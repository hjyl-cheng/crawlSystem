import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {createInterface} from 'node:readline';
import net from 'node:net';
import {createLocalRotaClient} from '../../src/remoteNodes/localRotaClient.js';

export async function fullCrawlRelayFixture(t,{nodeId,publicKey,observe}){
  const folder=await mkdtemp(join(tmpdir(),'full-relay-'));
  const token='p3-local-control-test-only-'.repeat(3);
  await writeFile(join(folder,'public.pem'),publicKey.export({type:'spki',format:'pem'}),{mode:0o600});
  await writeFile(join(folder,'token'),token,{mode:0o600});
  const child=spawn(process.env.REMOTE_NODE_ROTA_TEST_BINARY,['-slots','full-crawl-1','-node-id',nodeId,'-public-key-file',join(folder,'public.pem'),
    '-control-token-file',join(folder,'token'),'-proxy-listen','127.0.0.1:0','-control-listen','127.0.0.1:0'],
    {env:{PATH:process.env.PATH,GOMAXPROCS:'2'},stdio:['ignore','pipe','pipe']});
  const exited=once(child,'exit');let stderr='';child.stderr.on('data',b=>{stderr=(stderr+b).slice(-2000);});
  const lines=createInterface({input:child.stdout});
  const startup=await Promise.race([once(lines,'line',{signal:AbortSignal.timeout(5000)}).then(([line])=>JSON.parse(line)),exited.then(()=>{throw Error(stderr);})]);
  const sockets=new Set(),calls=[];
  const upstream=net.createServer(socket=>{
    sockets.add(socket);socket.on('close',()=>sockets.delete(socket));socket.on('error',()=>{});
    let bytes=Buffer.alloc(0),connected=false;
    socket.on('data',data=>{
      bytes=Buffer.concat([bytes,data]);
      if(!connected){const end=bytes.indexOf('\r\n\r\n');if(end<0)return;
        calls.push(bytes.subarray(0,end).toString());bytes=bytes.subarray(end+4);connected=true;
        socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');}
      const end=bytes.indexOf(10);if(end<0)return;
      const request=JSON.parse(bytes.subarray(0,end));bytes=bytes.subarray(end+1);
      Promise.resolve().then(()=>observe(request)).then(value=>socket.end(JSON.stringify(value)+'\n'),()=>socket.destroy());
    });
  });
  upstream.listen(0,'127.0.0.1');await once(upstream,'listening');
  const localRota=createLocalRotaClient({nodeId,token,proxyUrl:`http://${startup.proxy_address}`,controlUrl:`http://${startup.control_address}`});
  t.after(async()=>{for(const socket of sockets)socket.destroy();await new Promise(r=>upstream.close(r));if(child.exitCode===null)child.kill('SIGTERM');await exited;await rm(folder,{recursive:true,force:true});});
  let proxy;
  const gateway={async prepare({proxyUrl}){proxy=new URL(proxyUrl);},async snapshot(){return {cookies:[]};},async close(){},
    async fetch(_profile,input){
      const socket=net.connect({host:proxy.hostname,port:Number(proxy.port)});socket.on('error',()=>{});
      await once(socket,'connect');
      const credential=Buffer.from(`${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`).toString('base64');
      socket.write(`CONNECT www.youtube.com:443 HTTP/1.1\r\nHost: www.youtube.com:443\r\nProxy-Authorization: Basic ${credential}\r\n\r\n${JSON.stringify(input)}\n`);
      let bytes=Buffer.alloc(0);
      try{for await(const chunk of socket){bytes=Buffer.concat([bytes,chunk]);const head=bytes.indexOf('\r\n\r\n');
        if(head<0)continue;const body=bytes.subarray(head+4),end=body.indexOf(10);if(end>=0)return JSON.parse(body.subarray(0,end));}
        throw Error('FIXTURE_NETWORK_TRUNCATED');}finally{socket.destroy();}
    }};
  return {localRota,gateway,calls,upstream:{protocol:'http',address:`127.0.0.1:${upstream.address().port}`,username:'p3-fixture',password:'test-only'}};
}
