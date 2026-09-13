import pg from 'pg';

// One LISTEN session per center, never a connection held per waiting Worker.
// A lost notification cannot lose work: every wait has a bounded SQL recheck.
export async function createTransportSignals({connectionString, maxWaiters=4096}) {
  const watchers=new Map();let count=0,closed=false,client=null,timer=null;
  const subscribers=new Set();
  const notify=key=>{for(const callback of subscribers)try{callback(key);}catch{}};
  const wake=key=>{for(const resolve of [...(watchers.get(key)??[])])resolve();};
  const wakeAll=()=>{for(const key of [...watchers.keys()])wake(key);};
  async function listen(){
    if(closed)return;
    const next=new pg.Client({connectionString,application_name:'remote-node-nats-notifications',connectionTimeoutMillis:5000});
    client=next;
    const lost=()=>{if(client!==next||closed)return;client=null;wakeAll();notify(null);void next.end().catch(()=>{});clearTimeout(timer);timer=setTimeout(()=>void listen(),1000);};
    next.on('error',lost);next.on('end',lost);
    next.on('notification',value=>{if(value.channel==='qy_remote_transport'){wake(value.payload);notify(value.payload);}});
    try{await next.connect();await next.query('LISTEN qy_remote_transport');notify(null);}catch{lost();}
  }
  await listen();
  return {
    subscribe(callback){subscribers.add(callback);return ()=>subscribers.delete(callback);},
    watch(key,{timeoutMs=10000,signal}={}){
      if(signal?.aborted)return {wait:Promise.resolve(),cancel:()=>{}};
      // Saturation/disconnect must not turn the caller into a busy SQL loop.
      if(closed||count>=maxWaiters){
        let finish;const wait=new Promise(resolve=>{
          const timer=setTimeout(()=>finish(),timeoutMs);
          finish=()=>{clearTimeout(timer);signal?.removeEventListener('abort',finish);resolve();};
          signal?.addEventListener('abort',finish,{once:true});
        });
        return {wait,cancel:finish};
      }
      let cancel;
      const wait=new Promise(resolve=>{
        let done=false;const finish=()=>{if(done)return;done=true;clearTimeout(deadline);signal?.removeEventListener('abort',finish);watchers.get(key)?.delete(finish);if(!watchers.get(key)?.size)watchers.delete(key);count--;resolve();};
        const deadline=setTimeout(finish,timeoutMs);cancel=finish;
        if(!watchers.has(key))watchers.set(key,new Set());watchers.get(key).add(finish);count++;
        signal?.addEventListener('abort',finish,{once:true});
      });
      return {wait,cancel};
    },
    async close(){closed=true;clearTimeout(timer);wakeAll();subscribers.clear();await client?.end();client=null;},
  };
}
