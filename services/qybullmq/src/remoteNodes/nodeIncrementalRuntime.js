import { randomUUID } from 'node:crypto';
import { uptime } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { loadWorkerFiles, readNodeFile } from './workerConfig.js';
import { runNodeProcess, nodeHealthFile } from './nodeConnectionRuntime.js';
import { createRemoteIncrementalWorker } from './incrementalWorker.js';
import { RemoteResultSpool } from './spool.js';
import { REMOTE_RUNTIME_REVISION } from './workerActivationStore.js';

export async function checkNodeIncrementalHealth(path = nodeHealthFile) {
  const state=JSON.parse((await readNodeFile(path,{maxBytes:4096})).toString());
  if(state.version!==1 || !['connected_waiting_activation','ready','draining'].includes(state.state)
    || state.ready_for_tasks!==(state.state==='ready') || !Number.isFinite(state.valid_until_uptime)
    || state.valid_until_uptime<=uptime())throw new Error('NODE_INCREMENTAL_UNHEALTHY');
}

// Separate from connect_only. Starting the image is not activation. Both the
// local readiness lease and the center's transactional claim gate are required.
export class RemoteIncrementalProcess {
  constructor({config,client,localRota,spool,report=async()=>{},intervalMs=10000,
    createWorker=createRemoteIncrementalWorker}) {
    if(config.mode!=='incremental_collect' || !Number.isInteger(intervalMs) || intervalMs<50)throw new TypeError('collecting configuration required');
    Object.assign(this,{config,client,localRota,spool,report,intervalMs});
    this.instanceId=randomUUID();this.connection=null;this.readyUntil=0;this.stopping=false;this.fatal=null;
    this.worker=createWorker({client:{...client,claim:async(claimId,slot)=>{
      if(this.stopping || !this.connection || this.readyUntil<=uptime())return null;
      return client.claim(claimId,slot,this.connection);
    }},localRota,spool,slot:config.slot});
  }

  async probe() {
    const started=Date.now();const startedUptime=uptime();
    const boot=await this.localRota.boot();
    const value={version:1,mode:this.config.mode,node_id:this.config.node_id,slot:this.config.slot,
      deployment_id:this.config.deployment_id,config_hash:this.config.config_hash,instance_id:this.instanceId,
      relay_boot_id:boot.boot_id,runtime_revision:REMOTE_RUNTIME_REVISION,accepting:!this.stopping};
    const ack=await this.client.workerHeartbeat(value);
    const expected=ack?.ready_for_tasks===true?'ready':!value.accepting?'draining':'connected_waiting_activation';
    if(Object.keys(value).some(key=>ack?.[key]!==value[key]) || typeof ack.ready_for_tasks!=='boolean'
      || ack.state!==expected || (!value.accepting && ack.ready_for_tasks))throw new Error('NODE_CONNECTION_RECEIPT_MISMATCH');
    const serverTime=Date.parse(ack.server_time);const remaining=Date.parse(ack.connected_until)-serverTime;
    if(!Number.isFinite(serverTime) || serverTime<started-5000 || serverTime>Date.now()+5000
      || !Number.isFinite(remaining) || remaining<1000 || remaining>120000
      || startedUptime+remaining/1000<=uptime())throw new Error('NODE_CONNECTION_CLOCK_OR_LEASE_INVALID');
    this.connection=value;this.readyUntil=ack.ready_for_tasks?startedUptime+remaining/1000:0;
    return {version:1,node_id:value.node_id,slot:value.slot,deployment_id:value.deployment_id,
      state:ack.state,ready_for_tasks:ack.ready_for_tasks,valid_until_uptime:startedUptime+remaining/1000};
  }

  async run({signal}) {
    const stopped=new AbortController();let finished=false;
    const stop=()=>{this.stopping=true;this.readyUntil=0;this.worker.stop();};
    signal.addEventListener('abort',stop,{once:true});if(signal.aborted)stop();
    const connection=(async()=>{
      let failures=0;
      while(!finished){
        try {await this.report(await this.probe());failures=0;}
        catch(error){
          this.readyUntil=0;failures++;
          await this.report({version:1,state:'disconnected',ready_for_tasks:false,valid_until_uptime:0});
          if([400,401,403,404].includes(error.status) || error.message==='NODE_CONNECTION_RECEIPT_MISMATCH'){
            this.fatal=new Error('NODE_CONNECTION_REJECTED');stop();return;
          }
        }
        await delay(Math.min(15000,this.intervalMs*2**Math.min(failures,3)),null,{signal:stopped.signal}).catch(error=>{if(!finished)throw error;});
      }
    })().catch(()=>{
      // A failed health write must stop intake too; handle this immediately,
      // even if an in-flight collection takes minutes to drain.
      this.fatal??=new Error('NODE_CONNECTION_LOOP_FAILED');stop();
    });
    try {
      await this.worker.run({pollMs:1000,onStatus:status=>{
        if(status.status==='blocked'){this.fatal=new Error('NODE_RESULT_SPOOL_BLOCKED');stop();}
      }});
      if(!this.stopping)this.fatal??=new Error('NODE_EXECUTOR_STOPPED');
    } finally {
      finished=true;stopped.abort();signal.removeEventListener('abort',stop);
      await connection;
      await this.report({version:1,state:'stopped',ready_for_tasks:false,valid_until_uptime:0});
    }
    if(this.fatal)throw this.fatal;
  }
}

export function runNodeIncremental({spoolDirectory='/var/lib/qy-node/spool',...options}={}) {
  return runNodeProcess({...options,loadFiles:files=>loadWorkerFiles(files,{mode:'incremental_collect'}),
    runWorker:({signal,...args})=>new RemoteIncrementalProcess({...args,spool:new RemoteResultSpool({directory:spoolDirectory})}).run({signal}),
  });
}
