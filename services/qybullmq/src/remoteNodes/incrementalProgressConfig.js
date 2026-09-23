export function incrementalProgressConfig(env={}) {
  const mode=env.REMOTE_INCREMENTAL_PROGRESS_MODE??'observe';
  if(!['observe','enforce'].includes(mode))throw new TypeError('invalid REMOTE_INCREMENTAL_PROGRESS_MODE');
  const positive=(name,fallback)=>{
    const value=env[name]===undefined?fallback:Number(env[name]);
    if(!Number.isSafeInteger(value)||value<1||value>600000)throw new TypeError(`invalid ${name}`);
    return value;
  };
  const allowlist=JSON.parse(env.REMOTE_INCREMENTAL_PROGRESS_SLOTS??'[]');
  if(!Array.isArray(allowlist)||allowlist.some(value=>typeof value!=='string'||! /^[a-f0-9-]{36}\/incremental-[0-9]+$/.test(value))
    ||new Set(allowlist).size!==allowlist.length)throw new TypeError('invalid REMOTE_INCREMENTAL_PROGRESS_SLOTS');
  if(mode==='enforce'&&!allowlist.length)throw new TypeError('enforce requires explicit incremental slot allowlist');
  return {mode,allowlist,claimTimeoutMs:positive('REMOTE_INCREMENTAL_CLAIM_TIMEOUT_MS',30000),
    readTimeoutMs:positive('REMOTE_INCREMENTAL_READ_TIMEOUT_MS',5000),
    stopTimeoutMs:positive('REMOTE_INCREMENTAL_STOP_TIMEOUT_MS',45000),
    admissionTimeoutMs:positive('REMOTE_INCREMENTAL_ADMISSION_TIMEOUT_MS',30000)};
}
