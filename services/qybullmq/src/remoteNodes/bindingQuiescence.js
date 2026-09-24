// One cache per managed attempt, never per slot or center. A binding UUID is
// immutable; only its durable zero-in-flight receipt may be reused. Failed or
// uncertain work is not cached, and concurrent callers share the same promise.
export function createBindingQuiescence({routes,timeoutMs=45000}) {
  const pending=new Map();
  return bindingId=>{
    if(pending.has(bindingId))return pending.get(bindingId);
    const promise=(async()=>{
      await routes.requestStop(bindingId);
      const quiet=await routes.waitQuiesced(bindingId,{signal:AbortSignal.timeout(timeoutMs)});
      if(quiet?.active_managed_requests!==0)throw Object.assign(new Error('REMOTE_NETWORK_NOT_QUIESCED'),{code:'REMOTE_NETWORK_NOT_QUIESCED'});
      return quiet;
    })();
    pending.set(bindingId,promise);
    // Consume the cleanup branch without creating an unhandled rejection.
    void promise.catch(()=>{if(pending.get(bindingId)===promise)pending.delete(bindingId);});
    return promise;
  };
}
