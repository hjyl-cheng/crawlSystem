import {connect,wsconnect} from '@nats-io/transport-node';
import {WebSocket,Agent} from 'undici';
import {readFile} from 'node:fs/promises';

// Official NATS protocols on both paths. WSS uses the existing public TLS
// gateway; it is one persistent socket, not an HTTP polling fallback.
export async function connectNats(options){
  if(!String(options.servers).startsWith('wss:'))return connect(options);
  const {tls,...rest}=options;
  const dispatcher=new Agent({connect:tls?.caFile?{ca:await readFile(tls.caFile)}:{}});
  try{
    const nc=await wsconnect({...rest,wsFactory:async url=>({socket:new WebSocket(url,{dispatcher}),encrypted:true})});
    void nc.closed().then(()=>dispatcher.close()).catch(()=>{});
    return nc;
  }catch(error){await dispatcher.close();throw error;}
}
