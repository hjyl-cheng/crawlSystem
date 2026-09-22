import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {DelayedError} from 'bullmq';
import {RemoteProtocolError} from './remoteNodes/protocol.js';
import {selectIntakeWorkers,intakeStatus} from './remoteNodes/intakeSelection.js';
import {readIntakeControl,saveIntakeControl,changeIntakeControl} from './remoteNodes/intakeControl.js';
import {publishWorkerIntake} from './workerIntakeTelemetry.js';

export function createLocalIntakeAdmin({query,transaction}) {
  return {
    async status() {
      const rows=(await query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.local_incremental_workers ORDER BY worker_id`)).rows;
      const control=await readIntakeControl({query},'local-center',rows.filter(r=>r.activation_requested).length,rows.length);
      const workers=rows.map(r=>({slot:r.worker_id,connected:r.alive,requested:control.intakeEnabled&&r.activation_requested,
        enabled:r.alive&&r.accepting,active:r.alive&&r.active,
        readyForTasks:control.intakeEnabled&&r.alive&&r.accepting&&r.activation_requested}));
      return {nodeId:'local-center',executionAvailable:true,workers,...intakeStatus(workers),
        ...control};
    },
    async setExecution(value) {
      if(Number.isInteger(value?.allowedCount)||Number.isInteger(value?.expectedAllowedCount))throw new RemoteProtocolError('EXECUTION_COUNT_CONTROL_REMOVED',400);
      if(!value || Object.keys(value).some(k=>!['workerCount','enabled','expectedRequested'].includes(k))
        || typeof value.enabled!=='boolean'||typeof value.expectedRequested!=='boolean' || !Number.isSafeInteger(value.workerCount))throw new RemoteProtocolError('INVALID_EXECUTION_CONTROL',400);
      await transaction(async client=>{
        await client.query('SELECT pg_advisory_xact_lock(781138020)');
        const rows=(await client.query(`SELECT *,worker_id AS slot,connected_until>clock_timestamp() AS alive
          FROM remote_ingestion.local_incremental_workers ORDER BY worker_id FOR UPDATE`)).rows;
        if(rows.length!==value.workerCount)throw new RemoteProtocolError('WORKER_DEPLOYMENT_MISMATCH');
        const current=rows.filter(r=>r.activation_requested).length;
        const control=changeIntakeControl(await readIntakeControl(client,'local-center',current,rows.length),value,rows.length);
        const selected=selectIntakeWorkers(rows,control.effectiveCount);
        if(selected.some(r=>!r.activation_requested&&!r.alive))throw new RemoteProtocolError('WORKER_NOT_READY');
        await client.query(`UPDATE remote_ingestion.local_incremental_workers
          SET activation_requested=(worker_id=ANY($1::text[])),updated_at=clock_timestamp()`,[selected.map(r=>r.worker_id)]);
        await saveIntakeControl(client,'local-center',control);
      });
      return this.status();
    },
  };
}

// Admission around the existing processor only; no channel/Clock/queue identity changes.
export class LocalIncrementalIntake {
  constructor({worker,query,workerId,instanceId=randomUUID(),intervalMs=2000,signals=null,report=()=>{}}) {
    if(!/^[a-zA-Z0-9_-]{1,160}$/.test(workerId)||intervalMs<20)throw new TypeError('valid local Worker identity required');
    Object.assign(this,{worker,query,workerId,instanceId,intervalMs,signals,report});
    this.stopping=false;this.timer=new AbortController();
  }
  async start() {
    const registered=await this.query(`INSERT INTO remote_ingestion.local_incremental_workers(worker_id,instance_id,connected_until)
      VALUES($1,$2,clock_timestamp()+interval '15 seconds') ON CONFLICT(worker_id) DO UPDATE
      SET instance_id=$2,accepting=false,active=false,connected_until=clock_timestamp()+interval '15 seconds'
      WHERE local_incremental_workers.connected_until<=clock_timestamp() OR local_incremental_workers.instance_id=$2
      RETURNING worker_id`,[this.workerId,this.instanceId]);
    if(!registered.rowCount)throw new Error('LOCAL_WORKER_INSTANCE_BUSY');
    // A Worker added while the center is accepting work joins automatically;
    // a paused center leaves it in standby until the next explicit enable.
    const intake=(await this.query('SELECT intake_enabled FROM remote_ingestion.intake_controls WHERE node_key=$1',['local-center'])).rows[0];
    if(intake?.intake_enabled===true)await this.query(`UPDATE remote_ingestion.local_incremental_workers
      SET activation_requested=true,updated_at=clock_timestamp() WHERE worker_id=$1 AND instance_id=$2`,[this.workerId,this.instanceId]);
    const first=this.signals?.watch(`local-intake:${this.workerId}`,{timeoutMs:5000,signal:this.timer.signal});
    try{await this.tick();}catch(error){first?.cancel();throw error;}
    this.loop=(async()=>{let notification=first;while(!this.stopping){
      try{
        if(notification)await notification.wait;
        else await delay(this.intervalMs,null,{signal:this.timer.signal}).catch(()=>{});
      }finally{notification?.cancel();}
      if(this.stopping)break;
      notification=this.signals?.watch(`local-intake:${this.workerId}`,{timeoutMs:5000,signal:this.timer.signal});
      try{await this.tick();}catch{
        await this.worker.pause(true);await publishWorkerIntake(this.worker,false).catch(()=>{});
        this.report({event:'local_intake_control_unavailable',worker_id:this.workerId});
      }
    }})();
  }
  async tick() {
    const row=(await this.query(`UPDATE remote_ingestion.local_incremental_workers
      SET connected_until=clock_timestamp()+interval '15 seconds',updated_at=clock_timestamp()
      WHERE worker_id=$1 AND instance_id=$2 RETURNING activation_requested`,[this.workerId,this.instanceId])).rows[0];
    if(!row)throw new Error('LOCAL_WORKER_INSTANCE_STALE');
    const accepting=row.activation_requested&&!this.draining;
    if(!accepting)await this.worker.pause(true);
    await publishWorkerIntake(this.worker,accepting);
    await this.query(`UPDATE remote_ingestion.local_incremental_workers SET accepting=$3
      WHERE worker_id=$1 AND instance_id=$2 AND accepting IS DISTINCT FROM $3`,[this.workerId,this.instanceId,accepting]);
    if(accepting&&!this.stopping)this.worker.resume();
  }
  async process(job,token,execute) {
    let allowed;
    try{allowed=await this.query(`UPDATE remote_ingestion.local_incremental_workers SET active=true
      WHERE worker_id=$1 AND instance_id=$2 AND activation_requested AND connected_until>clock_timestamp()
      RETURNING worker_id`,[this.workerId,this.instanceId]);}catch{allowed={rowCount:0};}
    if(!allowed.rowCount || this.draining){
      await this.worker.pause(true);await publishWorkerIntake(this.worker,false);
      await job.moveToDelayed(Date.now()+1000,token);throw new DelayedError();
    }
    try{return await execute();}finally{
      await this.query(`UPDATE remote_ingestion.local_incremental_workers SET active=false
        WHERE worker_id=$1 AND instance_id=$2`,[this.workerId,this.instanceId]).catch(()=>{});
    }
  }
  async stop() {
    if(this.stopping)return;
    // Keep heartbeats alive until the active processor has finished.
    this.draining=true;
    await this.worker.pause(true);
    await this.worker.close();
    this.stopping=true;this.timer.abort();await this.loop;
    await publishWorkerIntake(this.worker,false).catch(()=>{});
    await this.query(`UPDATE remote_ingestion.local_incremental_workers
      SET accepting=false,active=false,connected_until=clock_timestamp()
      WHERE worker_id=$1 AND instance_id=$2`,[this.workerId,this.instanceId]);
  }
}
