import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {DelayedError} from 'bullmq';
import {RemoteProtocolError} from './remoteNodes/protocol.js';
import {selectIntakeWorkers,intakeStatus} from './remoteNodes/intakeSelection.js';
import {publishWorkerIntake} from './workerIntakeTelemetry.js';

export function createLocalIntakeAdmin({query,transaction}) {
  return {
    async status() {
      const rows=(await query(`SELECT *,connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.local_incremental_workers ORDER BY worker_id`)).rows;
      const workers=rows.map(r=>({slot:r.worker_id,connected:r.alive,requested:r.activation_requested,
        enabled:r.alive&&r.accepting,active:r.alive&&r.active,
        readyForTasks:r.alive&&r.accepting&&r.activation_requested}));
      return {nodeId:'local-center',executionAvailable:true,workers,...intakeStatus(workers)};
    },
    async setExecution(value) {
      if(!value || Object.keys(value).some(k=>!['allowedCount','expectedAllowedCount','workerCount'].includes(k))
        || !Number.isInteger(value.expectedAllowedCount) || !Number.isInteger(value.workerCount))throw new RemoteProtocolError('INVALID_EXECUTION_CONTROL',400);
      await transaction(async client=>{
        await client.query('SELECT pg_advisory_xact_lock(781138020)');
        const rows=(await client.query(`SELECT *,worker_id AS slot,connected_until>clock_timestamp() AS alive
          FROM remote_ingestion.local_incremental_workers ORDER BY worker_id FOR UPDATE`)).rows;
        if(rows.length!==value.workerCount)throw new RemoteProtocolError('WORKER_DEPLOYMENT_MISMATCH');
        const current=rows.filter(r=>r.activation_requested).length;
        const selected=selectIntakeWorkers(rows,value.allowedCount);
        if(current!==value.expectedAllowedCount && current!==value.allowedCount)throw new RemoteProtocolError('EXECUTION_CONTROL_CHANGED');
        if(selected.some(r=>!r.activation_requested&&!r.alive))throw new RemoteProtocolError('WORKER_NOT_READY');
        await client.query(`UPDATE remote_ingestion.local_incremental_workers
          SET activation_requested=(worker_id=ANY($1::text[])),updated_at=clock_timestamp()`,[selected.map(r=>r.worker_id)]);
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
