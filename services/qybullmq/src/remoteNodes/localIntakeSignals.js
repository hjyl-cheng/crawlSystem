import {connectNats as connect} from './natsConnection.js';
import {natsEndpoint} from './natsProtocol.js';
import {readNodeFile} from './workerConfig.js';

export const LOCAL_INTAKE_SUBJECT='qy.local-intake.changed';

// One center LISTEN session relays committed hints. Messages grant no authority:
// each Worker still rereads its own persisted intent and execution identity.
export function forwardLocalIntakeSignals(signals,nc) {
  return signals?.subscribe?.(key=>{
    if(key!==null&&!/^local-intake:[a-zA-Z0-9_-]{1,160}$/.test(key))return;
    try{nc.publish(LOCAL_INTAKE_SUBJECT,Buffer.from(key??'*'));}catch{}
  })??(()=>{});
}

export async function createLocalIntakeSignals({url,password,workerId,tls,allowLoopback=false}) {
  if(typeof password!=='string'||password.length<32||!/^[a-zA-Z0-9_-]{1,160}$/.test(workerId))throw new TypeError('local intake credentials and identity required');
  const nc=await connect({servers:natsEndpoint(url,{allowLoopback}),user:'local-intake',pass:password,tls,
    maxReconnectAttempts:-1,reconnectTimeWait:1000,reconnectJitter:500,timeout:5000});
  const waiters=new Set();let closed=false;
  const wake=()=>{for(const finish of [...waiters])finish();};
  nc.subscribe(LOCAL_INTAKE_SUBJECT,{callback:(error,msg)=>{
    if(error){wake();return;}
    const key=new TextDecoder().decode(msg.data);
    if(key==='*'||key===`local-intake:${workerId}`)wake();
  }});
  // Flush installs the subscription before the caller reads its initial state.
  try{await nc.flush();}catch(error){await nc.close();throw error;}
  const statuses=(async()=>{for await(const status of nc.status()){
    if(['disconnect','reconnect'].includes(status.type))wake();
  }})().catch(wake);
  return {
    watch(key,{timeoutMs=5000,signal}={}) {
      if(key!==`local-intake:${workerId}`)throw new TypeError('notification belongs to another local Worker');
      if(closed||signal?.aborted)return {wait:Promise.resolve(),cancel:()=>{}};
      let finish;const wait=new Promise(resolve=>{
        finish=()=>{clearTimeout(timer);signal?.removeEventListener('abort',finish);waiters.delete(finish);resolve();};
        const timer=setTimeout(finish,timeoutMs);waiters.add(finish);signal?.addEventListener('abort',finish,{once:true});
      });
      return {wait,cancel:finish};
    },
    async close(){closed=true;wake();await nc.close();await statuses;},
  };
}

export async function localIntakeSignalsFromEnv(workerId,env=process.env) {
  if(!env.LOCAL_INCREMENTAL_NATS_URL||!env.LOCAL_INCREMENTAL_NATS_PASSWORD_FILE)throw new Error('LOCAL_INTAKE_NATS_CONFIG_REQUIRED');
  const password=(await readNodeFile(env.LOCAL_INCREMENTAL_NATS_PASSWORD_FILE,{secret:true})).toString().trim();
  return createLocalIntakeSignals({url:env.LOCAL_INCREMENTAL_NATS_URL,password,workerId});
}
