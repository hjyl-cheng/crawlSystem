import {disposeNatsConnection} from './natsConnection.js';

// One connection per worker. Never replay an ambiguous business operation here:
// its caller retains the original lease, request ID and durable spool.
export async function createRecoveringNatsConnection({connect,dispose=disposeNatsConnection,healthTimeoutMs=5000,
  onRecovery=reason=>console.warn(JSON.stringify({event:'remote_nats_connection_recovery',reason}))}){
  let current=await connect(),opening=null,closing=null,checking=null,stopped=false;
  const discard=async(nc,reason)=>{
    if(current!==nc)return closing;
    current=null;
    onRecovery(reason);
    closing=Promise.resolve().then(()=>dispose(nc));
    // A failed dispose remains a barrier: never open a second connection when
    // the previous connection's shutdown has not been confirmed.
    await closing;closing=null;
  };
  const get=async()=>{
    if(stopped)throw new Error('NATS_CLIENT_CLOSED');
    if(current&&!current.isClosed())return current;
    if(!opening){
      opening=(async()=>{
        if(current)await discard(current,'connection_closed');
        else if(closing)await closing;
        if(stopped)throw new Error('NATS_CLIENT_CLOSED');
        const nc=await connect();
        if(stopped){await dispose(nc);throw new Error('NATS_CLIENT_CLOSED');}
        current=nc;return nc;
      })();
      opening.finally(()=>{opening=null;}).catch(()=>{});
    }
    return opening;
  };
  return {
    get,
    async failed(nc,{heartbeat=false}={}){
      if(stopped||current!==nc)return;
      if(nc.isClosed()){await discard(nc,'connection_closed');return;}
      if(!heartbeat)return;
      // An RPC timeout can be caused by a busy center. Only replace a live
      // connection if the broker's own PING/PONG also fails.
      if(!checking){
        checking=(async()=>{
          let timer;
          try{await Promise.race([nc.flush(),new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('NATS_PING_TIMEOUT')),healthTimeoutMs);})]);}
          catch{await discard(nc,'broker_ping_failed');}
          finally{clearTimeout(timer);}
        })();
        checking.finally(()=>{checking=null;}).catch(()=>{});
      }
      await checking;
    },
    async close(){
      stopped=true;
      await opening?.catch(()=>{});
      if(current){const nc=current;current=null;await dispose(nc);}
      if(closing)await closing;
    },
  };
}
