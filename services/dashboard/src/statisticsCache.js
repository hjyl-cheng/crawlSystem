// Shared by viewers and pages. A refresh never holds a list request open.
export function createStatisticsCache({ttlMs=30000,staleMs=120000,retryMs=5000,maxEntries=128,maxPending=8,now=Date.now}={}) {
  const entries=new Map();const waiting=[];let active=false;
  const busy=()=>Object.assign(new Error('Statistics busy'),{code:'STATISTICS_BUSY'});
  async function acquire(){
    if(!active){active=true;return;}
    if(waiting.length>=maxPending)throw busy();
    await new Promise(resolve=>waiting.push(resolve));
  }
  function release(){const next=waiting.shift();if(next)next();else active=false;}
  function result(entry){return {value:entry.value,generatedAt:new Date(entry.at).toISOString(),stale:now()-entry.at>=ttlMs};}
  return {
    async get(key,load){
      let entry=entries.get(key);
      if(!entry){
        if(entries.size>=maxEntries){const victim=[...entries].find(([,v])=>!v.pending);if(!victim)throw busy();entries.delete(victim[0]);}
        entry={at:null,value:null,pending:null,retryAt:0};entries.set(key,entry);
      }else {entries.delete(key);entries.set(key,entry);}
      const hasValue=entry.at!==null;
      if(hasValue&&now()-entry.at<ttlMs)return result(entry);
      if(!entry.pending&&now()>=entry.retryAt){
        entry.pending=(async()=>{
          let acquired=false;
          try{await acquire();acquired=true;const value=await load();entry.value=value;entry.at=now();entry.error=null;entry.retryAt=0;}
          catch(error){entry.error=error;entry.retryAt=now()+retryMs;throw error;}
          finally{if(acquired)release();}
        })().finally(()=>{entry.pending=null;});
        // Stale callers return immediately; a failed refresh stays observable
        // to cold callers without creating an unhandled rejection.
        entry.pending.catch(()=>{});
      }
      if(hasValue&&now()-entry.at<ttlMs+staleMs)return result(entry);
      if(entry.pending)await entry.pending;else throw entry.error??busy();
      return result(entry);
    },
  };
}
