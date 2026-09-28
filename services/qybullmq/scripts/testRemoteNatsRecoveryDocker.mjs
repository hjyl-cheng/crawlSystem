// A disposable broker and TLS proxy; never reads production credentials.
import {execFileSync,spawn} from 'node:child_process';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
const root=await mkdtemp(join(tmpdir(),'qy-nats-recovery-'));
const name=`qy-nats-recovery-${process.pid}-${Date.now()}`;
const docker=args=>execFileSync('docker',args,{encoding:'utf8',timeout:60000});
try{
  execFileSync('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',join(root,'key.pem'),'-out',join(root,'cert.pem'),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1'],{stdio:'ignore'});
  await writeFile(join(root,'server.conf'),'port: 4222\nwebsocket { port: 9222, no_tls: true, compression: false }\n');
  docker(['run','--rm','-d','--name',name,'--memory','128m','--cpus','1','-p','127.0.0.1::4222','-p','127.0.0.1::9222','-v',`${root}/server.conf:/etc/nats/probe.conf:ro`,'nats:2.12-alpine','-c','/etc/nats/probe.conf']);
  const port=p=>{const address=docker(['port',name,String(p)]).trim();if(!/^127\.0\.0\.1:\d+$/.test(address))throw Error('test broker must use loopback');return address.split(':')[1];};
  const env={...process.env,REMOTE_NATS_RECOVERY_TCP_PORT:port(4222),REMOTE_NATS_RECOVERY_WS_PORT:port(9222),REMOTE_NATS_RECOVERY_CA:join(root,'cert.pem'),REMOTE_NATS_RECOVERY_KEY:join(root,'key.pem')};
  const useNode20=process.env.REMOTE_NATS_RECOVERY_NODE20==='true';
  const command=useNode20?'docker':process.execPath;
  const args=useNode20?['run','--rm','--network','host','--memory','384m','--cpus','1','-v',`${process.cwd()}:/app:ro`,'-v',`${root}:${root}:ro`,'-w','/app',
    ...Object.entries(env).filter(([key])=>/^REMOTE_NATS_RECOVERY_(TCP_PORT|WS_PORT|CA|KEY)$/.test(key)).flatMap(([key,value])=>['-e',`${key}=${value}`]),
    'node:20-bookworm-slim','node','--test','test/remoteNatsRecovery.integration.test.js']:['--test','test/remoteNatsRecovery.integration.test.js'];
  await new Promise((resolve,reject)=>{const child=spawn(command,args,{env,stdio:'inherit',timeout:30000});child.once('error',reject);child.once('exit',code=>code===0?resolve():reject(Error(`recovery test failed: ${code}`)));});
}finally{try{docker(['rm','-f',name]);}finally{await rm(root,{recursive:true,force:true});}}
