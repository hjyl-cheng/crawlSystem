import {performance} from 'node:perf_hooks';

export function remoteDeadlineError(code) {
  return Object.assign(new Error(code), {code});
}

// Only for short, read-only operations. Never use the supervision lock pool.
// The deadline includes checkout, BEGIN/settings, query and COMMIT. On unknown
// protocol state destroy the client; a timed-out query must not rejoin the pool.
export function boundedPostgresRead(pool, {text, values=[], signal, timeoutMs=5000, acquireTimeoutMs=2000}) {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs<1 || !Number.isSafeInteger(acquireTimeoutMs) || acquireTimeoutMs<1)
    throw new TypeError('positive read deadlines required');
  return new Promise((resolve,reject)=>{
    let client, released=false, settled=false, timer;
    const deadline=performance.now()+timeoutMs;
    const release=error=>{
      if(!client || released)return;
      released=true;
      try{client.release(error);}catch{/* Already disconnected; never reuse. */}
    };
    const finish=(error,result)=>{
      if(settled)return;
      settled=true;clearTimeout(timer);signal?.removeEventListener('abort',aborted);
      release(error);if(error)reject(error);else resolve(result);
    };
    const aborted=()=>finish(signal.reason??remoteDeadlineError('REMOTE_EXECUTION_ABORTED'));
    if(signal?.aborted){aborted();return;}
    signal?.addEventListener('abort',aborted,{once:true});
    timer=setTimeout(()=>finish(remoteDeadlineError('REMOTE_DB_ACQUIRE_TIMEOUT')),Math.min(timeoutMs,acquireTimeoutMs));
    const query=async(text,values)=>{
      if(settled)throw remoteDeadlineError('REMOTE_DB_READ_TIMEOUT');
      const result=await client.query(text,values);
      if(settled)throw remoteDeadlineError('REMOTE_DB_READ_TIMEOUT');
      signal?.throwIfAborted();
      return result;
    };
    // Both rejection and late checkout are consumed, even after cancellation.
    Promise.resolve().then(()=>pool.connect()).then(async borrowed=>{
      client=borrowed;
      if(settled){release();return;}
      clearTimeout(timer);
      const remaining=Math.floor(deadline-performance.now());
      if(remaining<=0){finish(remoteDeadlineError('REMOTE_DB_READ_TIMEOUT'));return;}
      timer=setTimeout(()=>finish(remoteDeadlineError('REMOTE_DB_READ_TIMEOUT')),remaining);
      await query('BEGIN READ ONLY');
      await query("SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true)",[`${Math.max(1,Math.floor(deadline-performance.now()))}ms`]);
      const result=await query(text,values);
      await query('COMMIT');
      finish(null,result);
    }).catch(error=>finish(error));
  });
}
