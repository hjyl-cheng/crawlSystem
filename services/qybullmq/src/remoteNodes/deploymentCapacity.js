import {randomUUID} from 'node:crypto';
import {setTimeout as sleep} from 'node:timers/promises';
import {RemoteProtocolError} from './protocol.js';

export const CAPACITY_FAILURE_REASONS = new Set([
  'RESOURCE_SYNC_DEFERRED', 'CAPACITY_TIMEOUT', 'CAPACITY_AUTH_FAILED',
  'CAPACITY_INVALID_REQUEST', 'CAPACITY_INVALID_RESPONSE', 'CAPACITY_SERVICE_UNAVAILABLE',
]);

function failure(reason, attempts) {
  return Object.assign(new RemoteProtocolError('REMOTE_NETWORK_CAPACITY_UNAVAILABLE',503),{reason,attempts});
}
function reasonFor(error) {
  if(error?.code==='CAPACITY_TIMEOUT')return 'CAPACITY_TIMEOUT';
  if(error?.status===503 && error?.code==='RESOURCE_SYNC_DEFERRED')return 'RESOURCE_SYNC_DEFERRED';
  if([401,403].includes(error?.status))return 'CAPACITY_AUTH_FAILED';
  if(error?.status>=400 && error?.status<500)return 'CAPACITY_INVALID_REQUEST';
  return 'CAPACITY_SERVICE_UNAVAILABLE';
}

// Retry an absolute lower bound, never deployment registration or intake writes.
// A single budget covers all HTTP attempts and backoff, below the outer 15s timeout.
export function createDeploymentCapacity({pool,client,localChannelSlots,
  timeoutMs=12000,sleepImpl=sleep,now=()=>performance.now(),random=Math.random,report=()=>{}}) {
  if(!Number.isInteger(localChannelSlots)||localChannelSlots<0||localChannelSlots>500)throw new TypeError('explicit local channel slot count required');
  if(!Number.isFinite(timeoutMs)||timeoutMs<1||timeoutMs>12000)throw new TypeError('capacity timeout must be between 1 and 12000ms');
  return {
    async ensure({nodeId=null,operation=null}={}) {
      const {rows}=await pool.query(`SELECT COALESCE(sum(d.worker_count),0)::int AS workers
        FROM remote_ingestion.node_deployments d JOIN remote_ingestion.nodes n USING(node_id) WHERE n.state='active'`);
      const required=localChannelSlots+rows[0].workers;
      if(required>500)throw new RemoteProtocolError('REMOTE_NETWORK_CAPACITY_LIMIT',409);
      if(required===0)return;
      const started=now(),deadline=started+timeoutMs,requestId=randomUUID();
      const record=value=>{try{report({event:'remote_network_capacity',request_id:requestId,node_id:nodeId,operation,required,elapsed_ms:Math.round(now()-started),...value});}catch{}};
      const failed=(reason,attempts)=>{record({ok:false,reason,attempts});return failure(reason,attempts);};
      for(let attempt=1;attempt<=3;attempt++) {
        const remaining=Math.floor(deadline-now());
        if(remaining<=0)throw failed('CAPACITY_TIMEOUT',attempt-1);
        let result;
        try {
          result=await client.ensureCapacity({role:'channel',minimum_slots:required},{timeoutMs:remaining,maxAttempts:1});
        }catch(error){
          const reason=reasonFor(error);
          if(reason!=='RESOURCE_SYNC_DEFERRED'||attempt===3)throw failed(reason,attempt);
          const delay=[200,500][attempt-1]+Math.floor(random()*100);
          if(deadline-now()<=delay)throw failed('CAPACITY_TIMEOUT',attempt);
          await sleepImpl(delay);
          continue;
        }
        if(now()>deadline)throw failed('CAPACITY_TIMEOUT',attempt);
        if(result?.ok!==true || result.role!=='channel' || !Number.isInteger(result.provisioned) || result.provisioned<required)throw failed('CAPACITY_INVALID_RESPONSE',attempt);
        record({ok:true,attempts:attempt,provisioned:result.provisioned});
        return {required,provisioned:result.provisioned};
      }
    },
  };
}
