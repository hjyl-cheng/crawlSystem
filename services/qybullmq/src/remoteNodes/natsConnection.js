import {connect,wsconnect} from '@nats-io/transport-node';
import {WebSocket,Agent} from 'undici';
import {readFile} from 'node:fs/promises';

const transports=new WeakMap();

// Closing a broken WebSocket must not wait for a peer that is unreachable.
// Close the NATS protocol and destroy its owned HTTP agent before replacing it.
export async function disposeNatsConnection(nc){
  const closing=nc.close();
  const destroy=transports.get(nc);
  if(destroy)await Promise.all([closing,destroy()]);
  else await closing;
}

// Official NATS protocols on both paths. WSS uses the existing public TLS
// gateway; it is one persistent socket, not an HTTP polling fallback.
export async function connectNats(options){
  if(!String(options.servers).startsWith('wss:'))return connect(options);
  const {tls,...rest}=options;
  const dispatcher=new Agent({connect:tls?.caFile?{ca:await readFile(tls.caFile)}:{}});
  const sockets=new Set();
  // Undici detaches upgraded WebSockets from Agent ownership. Agent.destroy()
  // alone therefore cannot terminate a socket whose peer has stopped replying.
  const tracked={dispatch(request,handler){return dispatcher.dispatch(request,new Proxy(handler,{
    get(target,key){
      if(key==='onUpgrade')return (status,headers,socket)=>{
        sockets.add(socket);socket.once('close',()=>sockets.delete(socket));
        return target.onUpgrade(status,headers,socket);
      };
      const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
    },
  }));}};
  const destroy=async()=>{for(const socket of sockets)socket.destroy();await dispatcher.destroy();};
  try{
    const nc=await wsconnect({...rest,wsFactory:async url=>({socket:new WebSocket(url,{dispatcher:tracked}),encrypted:true})});
    transports.set(nc,destroy);
    void nc.closed().then(()=>dispatcher.close()).catch(()=>{});
    return nc;
  }catch(error){await destroy();throw error;}
}
