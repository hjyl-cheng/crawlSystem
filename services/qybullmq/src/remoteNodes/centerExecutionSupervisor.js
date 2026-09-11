import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'bullmq';
import { INCREMENTAL_QUEUE } from '../incrementalPlan.js';
import { RotaSlotAdapter } from '../rotaSlotAdapter.js';
import { RemoteManagedIncrementalRuntime } from './managedIncrementalRuntime.js';
import { createCenterIncrementalProcessor } from './centerIncrementalProcessor.js';
import { REMOTE_RUNTIME_REVISION } from './workerActivationStore.js';
import { recoverRemoteSlot, remoteSlotUnsettled, supervisionLockKey } from './centerExecutionRecovery.js';
import { createRemoteYoutubeCheckpointConsumer } from './youtubeProfileCheckpoint.js';

const key = row => `${row.node_id}/${row.slot}`;
const same = (a,b) => ['node_id','slot','deployment_id','config_hash','instance_id','relay_boot_id'].every(field=>a[field]===b[field]);
export { supervisionLockKey };
const identity = row => ({version:1,mode:'incremental_collect',node_id:row.node_id,slot:row.slot,
  deployment_id:row.deployment_id,config_hash:row.config_hash,instance_id:row.instance_id,
  relay_boot_id:row.relay_boot_id,runtime_revision:REMOTE_RUNTIME_REVISION,accepting:true});

// One central queue consumer/Rota adapter for each admitted remote process.
// A dedicated PG session lock excludes another center from owning the same
// slot. All business policy stays in the original managed attempt and runner.
export class RemoteCenterExecutionSupervisor {
  constructor({store,channelStore,routes,youtubeSessions,activation,guardPool,connection,prefix,
    allowedNodeIds,resolvedPolicy,profileSecret,rotaClient,proxyBaseUrl,proxyPassword,
    createApiFallback=null,intervalMs=1000,maxSlots=32,report=()=>{},WorkerClass=Worker,createRota=args=>new RotaSlotAdapter(args),
    createRuntime=args=>new RemoteManagedIncrementalRuntime(args),createProcessor=createCenterIncrementalProcessor}) {
    if(!Array.isArray(allowedNodeIds)||!allowedNodeIds.length||allowedNodeIds.some(id=>!/^[a-f0-9-]{36}$/.test(id)))throw new TypeError('explicit admitted node IDs required');
    if(!guardPool || !connection || typeof prefix!=='string' || !prefix || intervalMs<50 || !Number.isInteger(maxSlots) || maxSlots<1 || maxSlots>32)throw new TypeError('explicit supervision database, Redis connection and queue prefix required');
    Object.assign(this,{store,channelStore,routes,youtubeSessions,activation,guardPool,connection,prefix,allowedNodeIds,
      resolvedPolicy,profileSecret,rotaClient,proxyBaseUrl,proxyPassword,createApiFallback,intervalMs,maxSlots,report,WorkerClass,createRota,createRuntime,createProcessor});
    this.entries=new Map();this.stopping=false;this.loop=null;
    this.recoveryCheckpoints=youtubeSessions?createRemoteYoutubeCheckpointConsumer({sessions:youtubeSessions,profileSecret}):null;
  }
  // Called under the node/connection row locks by activate, heartbeat and claim.
  async verifyExecution(client,row) {
    const entry=this.entries.get(key(row));
    if(!entry?.owned || !entry.queueReady || !same(entry.row,row) || entry.aborting || entry.redis?.status!=='ready')return false;
    const state=entry.rota.status();
    if(!state.started || state.closing || !state.assignment?.ready || (entry.closing && !entry.processing))return false;
    if(entry.rota.workerId!==entry.row.rota_worker_id || entry.rota.workerInstanceId!==entry.supervisorId)return false;
    // Verify that this precise session still holds the advisory lock, rather
    // than trusting a TCP error event that may arrive after a backend restart.
    const guard=await client.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=$1 AND granted
      AND classid=781138012::oid AND objid=(hashtext($2)::bigint & 4294967295)::oid AND objsubid=2`,[entry.backendPid,supervisionLockKey(row)]);
    return guard.rowCount===1;
  }
  async ready(entry) {
    if(this.stopping || entry.closing || !entry.owned || !entry.queueReady)return false;
    return this.store.transaction(async client=>{
      const row=(await client.query(`SELECT w.*,n.state AS node_state,w.connected_until>clock_timestamp() AS alive
        FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
        WHERE w.node_id=$1 AND w.slot=$2`,[entry.row.node_id,entry.row.slot])).rows[0];
      return !!row && row.node_state==='active' && row.alive && row.accepting && row.enabled && await this.verifyExecution(client,row);
    });
  }
  async startEntry(row) {
    const entry={row,supervisorId:randomUUID(),owned:false,queueReady:false,closing:false};
    this.entries.set(key(row),entry);
    try {
      entry.guard=await this.guardPool.connect();
      entry.guard.on('error',()=>{entry.owned=false;void this.closeEntry(entry,{abort:true}).catch(()=>{});});
      const locked=(await entry.guard.query('SELECT pg_try_advisory_lock(781138012,hashtext($1)) AS locked,pg_backend_pid() AS pid',[supervisionLockKey(row)])).rows[0];
      if(!locked.locked){entry.guard.release();entry.guard=null;this.entries.delete(key(row));return;}
      entry.owned=true;entry.backendPid=locked.pid;
      if(await this.unsettled(row)){
        entry.blocked=true;
        await this.activation.drain(row.node_id,row.slot,{keepRequested:true});
        this.report({event:'remote_center_previous_execution_unsettled',node_id:row.node_id,slot:row.slot});
        return;
      }
      if(!row.alive || !row.accepting || !row.activation_requested){await this.closeEntry(entry);return;}
      const runtime=this.createRuntime({channelStore:this.channelStore,routes:this.routes,youtubeSessions:this.youtubeSessions,
        nodeId:row.node_id,slot:row.slot,profileSecret:this.profileSecret,createApiFallback:this.createApiFallback,
        assertAdmission:async client=>{
          const current=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
            FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2`,[row.node_id,row.slot])).rows[0];
          return !!current?.alive && current.enabled && same(entry.row,current) && await this.verifyExecution(client,current);
        }});
      entry.rota=this.createRota({client:this.rotaClient,role:'channel',workerId:row.rota_worker_id,workerInstanceId:entry.supervisorId,
        resolvedPolicy:this.resolvedPolicy,proxyBaseUrl:this.proxyBaseUrl,proxyPassword:this.proxyPassword,identityRuntime:runtime});
      const process=this.createProcessor({channelStore:this.channelStore,runtime,rota:entry.rota,resolvedPolicy:this.resolvedPolicy,
        createApiFallback:this.createApiFallback,ready:()=>this.ready(entry),report:this.report});
      entry.worker=new this.WorkerClass(INCREMENTAL_QUEUE,async(job,token)=>{
        entry.processing=true;
        try{return await process(job,token);}finally{entry.processing=false;}
      },{connection:this.connection,prefix:this.prefix,
        concurrency:1,autorun:false});
      entry.worker.on('error',()=>this.report({event:'remote_center_queue_error',node_id:row.node_id,slot:row.slot}));
      entry.worker.on('failed',(job,error)=>this.report({event:'remote_center_job_failed',node_id:row.node_id,slot:row.slot,job_id:job?.id,code:error?.code??error?.name}));
      await entry.worker.waitUntilReady();
      // BullMQ waitUntilReady returns its blocking dequeue connection. That
      // connection is deliberately closed during a graceful drain; the main
      // client still renews the active job lock until execution finishes.
      entry.redis=await entry.worker.client;
      await entry.worker.pause(true);
      // start() can wait for an available Rota route. Do not block other slots.
      entry.starting=entry.rota.start().then(async()=>{
        if(entry.closing||this.stopping)return;
        entry.queueReady=true;
        entry.running=entry.worker.run().catch(()=>this.closeEntry(entry,{abort:true}));
        await this.activation.activate(identity(row));
        if(await this.ready(entry))entry.worker.resume();
      }).catch(()=>this.closeEntry(entry,{abort:true}));
    } catch {
      await this.closeEntry(entry,{abort:true});
      this.report({event:'remote_center_slot_start_failed',node_id:row.node_id,slot:row.slot});
    }
  }
  async unsettled(row) {
    return remoteSlotUnsettled(this.store.pool,row);
  }
  closeEntry(entry,{abort=false}={}) {
    if(entry.closed)return entry.closed;
    entry.closing=true;entry.aborting=abort;
    entry.closed=(async()=>{
      await entry.recovering?.catch(()=>{});
      await entry.worker?.pause(true);
      // Normal stop preserves in-flight channel/country/API work. Unexpected
      // loss of exclusive ownership aborts the original Rota execution.
      if(abort)await entry.rota?.close();
      await entry.worker?.close();
      await entry.rota?.close();
      if(entry.owned)await this.activation.drain(entry.row.node_id,entry.row.slot,{keepRequested:true});
    })().finally(async()=>{
      entry.owned=false;
      if(entry.guard){
        await entry.guard.query('SELECT pg_advisory_unlock(781138012,hashtext($1))',[supervisionLockKey(entry.row)]).catch(()=>{});
        entry.guard.release(true);entry.guard=null;
      }
      if(this.entries.get(key(entry.row))===entry)this.entries.delete(key(entry.row));
    });
    return entry.closed;
  }
  async tick() {
    if(this.stopping)return;
    const rows=(await this.store.pool.query(`SELECT w.*,s.rota_worker_id,w.connected_until>clock_timestamp() AS alive
      FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
      JOIN remote_ingestion.network_slots s USING(node_id,slot)
      WHERE w.node_id=ANY($1::uuid[]) AND n.state='active' AND w.mode='incremental_collect'`,[this.allowedNodeIds])).rows;
    for(const row of rows){
      const entry=this.entries.get(key(row));
      if(entry && !same(entry.row,row)){void this.closeEntry(entry,{abort:true}).catch(()=>{});continue;}
      if(!entry){if(this.entries.size<this.maxSlots
        && ((row.activation_requested&&row.alive&&row.accepting)||await this.unsettled(row)))await this.startEntry(row);continue;}
      if(entry.blocked){
        if(entry.closing||entry.recovering)continue;
        if(entry.recovered || !await this.unsettled(row)){await this.closeEntry(entry);continue;}
        // A slow recovery waits only on this slot; other queue consumers keep
        // reconciling. Do not release its guard until its transaction finishes.
        entry.recovering=recoverRemoteSlot({guard:entry.guard,row,lockKey:supervisionLockKey(row),profileSecret:this.profileSecret,checkpoints:this.recoveryCheckpoints})
          .then(result=>{entry.recovered=result.settled;
            if(result.closed || result.settled)this.report({event:'remote_center_execution_recovered',node_id:row.node_id,slot:row.slot,
              attempts_closed:result.closed,settled:result.settled});})
          .catch(()=>this.report({event:'remote_center_recovery_waiting',node_id:row.node_id,slot:row.slot}))
          .finally(()=>{entry.recovering=null;});
        continue;
      }
      if(!entry.queueReady||entry.closing)continue;
      if(!row.alive || !row.accepting){await entry.worker.pause(true);continue;}
      // Explicit drain is respected; enabling is performed once per attachment,
      // never by a polling loop that would undo an operator's stop request.
      if(await this.ready(entry))entry.worker.resume();else await entry.worker.pause(true);
    }
    const present=new Set(rows.map(key));
    for(const [id,entry] of this.entries)if(!present.has(id))void this.closeEntry(entry,{abort:true}).catch(()=>{});
  }
  start() {
    if(this.loop)return;
    this.loop=(async()=>{while(!this.stopping){try{await this.tick();}catch{for(const entry of this.entries.values())await entry.worker?.pause(true);this.report({event:'remote_center_reconcile_failed'});}
      if(!this.stopping)await delay(this.intervalMs);}})();
  }
  async stop() {
    this.stopping=true;await this.loop;
    await Promise.all([...this.entries.values()].map(entry=>this.closeEntry(entry)));
  }
}
