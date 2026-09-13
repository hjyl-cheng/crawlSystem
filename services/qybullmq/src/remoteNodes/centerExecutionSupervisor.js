import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'bullmq';
import { INCREMENTAL_QUEUE } from '../incrementalPlan.js';
import { RotaSlotAdapter } from '../rotaSlotAdapter.js';
import { RemoteManagedIncrementalRuntime } from './managedIncrementalRuntime.js';
import { createCenterIncrementalProcessor } from './centerIncrementalProcessor.js';
import { WHOLE_CHANNEL_RUNTIME_REVISION } from './workerActivationStore.js';
import { recoverRemoteSlot, remoteSlotUnsettled, supervisionLockKey } from './centerExecutionRecovery.js';
import { createRemoteYoutubeCheckpointConsumer } from './youtubeProfileCheckpoint.js';
import {intakeWorkerName,publishWorkerIntake} from '../workerIntakeTelemetry.js';
import {SupervisionGuards} from './supervisionGuards.js';

const key = row => `${row.node_id}/${row.slot}`;
const same = (a,b) => ['node_id','slot','deployment_id','config_hash','instance_id','relay_boot_id','runtime_revision'].every(field=>a[field]===b[field]);
export { supervisionLockKey };
const identity = row => ({version:1,mode:'incremental_collect',node_id:row.node_id,slot:row.slot,
  deployment_id:row.deployment_id,config_hash:row.config_hash,instance_id:row.instance_id,
  relay_boot_id:row.relay_boot_id,runtime_revision:row.runtime_revision,accepting:true});

// One central queue consumer/Rota adapter for each admitted remote process.
// Independent PG advisory locks, held on a bounded group of sessions, exclude
// another center from owning the same slot. All business policy stays in the original managed attempt and runner.
export class RemoteCenterExecutionSupervisor {
  constructor({store,channelStore,routes,youtubeSessions,activation,guardPool,connection,prefix,
    allowedNodeIds=[],dashboardManaged=false,resolvedPolicy,profileSecret,rotaClient,proxyBaseUrl,proxyPassword,
    createApiFallback=null,wholeChannels=null,loadWholeApiPolicy=null,intervalMs=1000,maxSlots=null,guardGroups=4,report=()=>{},WorkerClass=Worker,createRota=args=>new RotaSlotAdapter(args),
    createRuntime=args=>new RemoteManagedIncrementalRuntime(args),createProcessor=createCenterIncrementalProcessor}) {
    if(!Array.isArray(allowedNodeIds)||(!allowedNodeIds.length&&!dashboardManaged)||allowedNodeIds.some(id=>!/^[a-f0-9-]{36}$/.test(id)))throw new TypeError('explicit admitted node IDs required');
    if(!guardPool || !connection || typeof prefix!=='string' || !prefix || intervalMs<50 || (maxSlots!==null && (!Number.isSafeInteger(maxSlots) || maxSlots<1)))throw new TypeError('explicit supervision database, Redis connection and queue prefix required');
    Object.assign(this,{store,channelStore,routes,youtubeSessions,activation,guardPool,connection,prefix,allowedNodeIds,dashboardManaged,
      resolvedPolicy,profileSecret,rotaClient,proxyBaseUrl,proxyPassword,createApiFallback,wholeChannels,loadWholeApiPolicy,intervalMs,maxSlots,report,WorkerClass,createRota,createRuntime,createProcessor});
    this.entries=new Map();this.stopping=false;this.loop=null;this.waitAbort=new AbortController();
    this.guards=new SupervisionGuards({pool:guardPool,groups:guardGroups});
    this.recoveryCheckpoints=youtubeSessions?createRemoteYoutubeCheckpointConsumer({sessions:youtubeSessions,profileSecret}):null;
  }
  allowsNode(nodeId) { return this.dashboardManaged || this.allowedNodeIds.includes(nodeId); }
  isProcessing(row) { return this.entries.get(key(row))?.processing === true; }
  preparationState(row) {
    const entry=this.entries.get(key(row));
    if(!row.activation_requested || !entry || entry.closing || entry.blocked)return null;
    return entry.starting && !entry.rota.status().started ? 'waiting_network' : null;
  }
  async networkCapacity() {
    if(this.capacityUntil>Date.now())return this.capacityValue;
    if(!this.capacityRead)this.capacityRead=(async()=>{
      try {
        const role=(await this.rotaClient.capacity()).roles?.channel;
        this.capacityValue=Number.isInteger(role?.provisioned)&&Number.isInteger(role?.claimed)
          ?{provisioned:role.provisioned,claimed:role.claimed,available:Math.max(0,role.provisioned-role.claimed)}:null;
      } catch {this.capacityValue=null;}
      this.capacityUntil=Date.now()+10000;
      return this.capacityValue;
    })().finally(()=>{this.capacityRead=null;});
    return this.capacityRead;
  }
  // Called under the node/connection row locks by activate, heartbeat and claim.
  executionReady(entry,row) {
    if(!entry?.owned || !entry.queueReady || !same(entry.row,row) || entry.aborting || entry.redis?.status!=='ready')return false;
    const state=entry.rota.status();
    if(!state.started || state.closing || !state.assignment?.ready || (entry.closing && !entry.processing))return false;
    if(entry.rota.workerId!==entry.row.rota_worker_id || entry.rota.workerInstanceId!==entry.supervisorId)return false;
    return true;
  }
  async verifyExecution(client,row) {
    const entry=this.entries.get(key(row));
    if(!this.executionReady(entry,row))return false;
    // Check the exact backend lock, not only delayed connection error events.
    const guard=await client.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=$1 AND granted
      AND classid=781138012::oid AND objid=(hashtext($2)::bigint & 4294967295)::oid AND objsubid=2`,[entry.backendPid,supervisionLockKey(row)]);
    return guard.rowCount===1;
  }
  async ready(entry) {
    if(this.stopping || entry.closing || !entry.owned || !entry.queueReady)return false;
    const row=(await this.store.pool.query(`SELECT w.*,n.state AS node_state,
      w.connected_until>clock_timestamp() AS alive,
      EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=$3 AND granted
        AND classid=781138012::oid AND objid=(hashtext($4)::bigint & 4294967295)::oid AND objsubid=2) AS guard_owned
      FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
      WHERE w.node_id=$1 AND w.slot=$2`,[entry.row.node_id,entry.row.slot,entry.backendPid,supervisionLockKey(entry.row)])).rows[0];
    return !!row && row.node_state==='active' && row.alive && row.accepting && row.activation_requested
      && row.enabled && row.guard_owned && this.executionReady(entry,row);
  }

  async startEntry(row) {
    const entry={row,supervisorId:randomUUID(),owned:false,queueReady:false,closing:false};
    this.entries.set(key(row),entry);
    try {
      entry.guard=await this.guards.acquire(supervisionLockKey(row),()=>{entry.owned=false;void this.closeEntry(entry,{abort:true}).catch(()=>{});});
      if(!entry.guard){this.entries.delete(key(row));return;}
      if(!entry.guard.alive)throw new Error('SUPERVISION_SESSION_LOST');
      entry.owned=true;entry.backendPid=entry.guard.backendPid;
      const unsettled=await this.unsettled(row);
      // A session can be lost while the initial SQL read is pending. Cleanup
      // may already have run; never create consumers after that point.
      if(!entry.owned || entry.closing || this.stopping){await this.closeEntry(entry);return;}
      if(unsettled){
        entry.blocked=true;
        await this.activation.drain(row.node_id,row.slot,{keepRequested:true});
        this.report({event:'remote_center_previous_execution_unsettled',node_id:row.node_id,slot:row.slot});
        return;
      }
      if(!row.alive || !row.accepting || !row.activation_requested){await this.closeEntry(entry);return;}
      const runtime=this.createRuntime({channelStore:this.channelStore,routes:this.routes,youtubeSessions:this.youtubeSessions,
        nodeId:row.node_id,slot:row.slot,profileSecret:this.profileSecret,createApiFallback:this.createApiFallback,
        wholeChannels:row.runtime_revision===WHOLE_CHANNEL_RUNTIME_REVISION?this.wholeChannels:null,
        loadWholeApiPolicy:this.loadWholeApiPolicy,
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
        concurrency:1,autorun:false,name:intakeWorkerName('remote',`${row.node_id}-${row.slot}`)});
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
        await this.activation.activate(identity(row),{requireRequested:true});
        const ready=await this.ready(entry);await publishWorkerIntake(entry.worker,ready);
        if(ready)entry.worker.resume();
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
    const cleanup=async(stage,action)=>{
      try { await action(); } catch(error) {
        // Failed cleanup belongs to this slot. Its persisted execution remains
        // fenced and the replacement supervisor must recover it before intake.
        this.report({event:'remote_center_cleanup_failed',node_id:entry.row.node_id,
          slot:entry.row.slot,stage,code:error?.code??error?.name??'Error'});
      }
    };
    entry.closed=(async()=>{
      await entry.recovering?.catch(()=>{});
      await cleanup('pause',()=>entry.worker?.pause(true));
      if(entry.worker)await publishWorkerIntake(entry.worker,false).catch(()=>{});
      // Normal stop preserves in-flight channel/country/API work. Unexpected
      // loss of exclusive ownership aborts the original Rota execution.
      if(abort)await cleanup('abort_route',()=>entry.rota?.close());
      await cleanup('close_worker',()=>entry.worker?.close());
      await cleanup('close_route',()=>entry.rota?.close());
      if(entry.owned)await cleanup('drain',()=>this.activation.drain(entry.row.node_id,entry.row.slot,{keepRequested:true}));
    })().finally(async()=>{
      entry.owned=false;
      if(entry.guard){
        await entry.guard.release().catch(()=>{});entry.guard=null;
      }
      if(this.entries.get(key(entry.row))===entry)this.entries.delete(key(entry.row));
    });
    return entry.closed;
  }
  async tick() {
    if(this.stopping)return;
    const owners=[...this.entries.values()].filter(e=>e.owned).map(e=>({pid:e.backendPid,lock_key:supervisionLockKey(e.row)}));
    const [connections,locks]=await Promise.all([this.store.pool.query(`SELECT w.*,s.rota_worker_id,w.connected_until>clock_timestamp() AS alive
      FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
      JOIN remote_ingestion.network_slots s USING(node_id,slot)
      WHERE (w.node_id=ANY($1::uuid[]) OR ($2::boolean AND EXISTS(
        SELECT 1 FROM remote_ingestion.node_deployments d WHERE d.node_id=w.node_id AND d.deployment_id=w.deployment_id)))
        AND n.state='active' AND w.mode='incremental_collect'`,[this.allowedNodeIds,this.dashboardManaged]),
      this.store.pool.query(`SELECT owner.pid,owner.lock_key FROM pg_locks AS guard
        JOIN jsonb_to_recordset($1::jsonb) AS owner(pid integer,lock_key text)
          ON guard.pid=owner.pid AND guard.objid=(hashtext(owner.lock_key)::bigint & 4294967295)::oid
        WHERE guard.locktype='advisory' AND guard.granted AND guard.classid=781138012::oid AND guard.objsubid=2`,[JSON.stringify(owners)])]);
    const rows=connections.rows,owned=new Set(locks.rows.map(r=>`${r.pid}/${r.lock_key}`));
    const updates=[];
    // Refresh existing consumers together. Slow admission or recovery of one
    // slot cannot hold back the other slots' 15-second intake advertisements.
    for(const row of rows){
      const entry=this.entries.get(key(row));
      if(!entry || !same(entry.row,row) || entry.blocked || !entry.queueReady || entry.closing)continue;
      const ready=!this.stopping && row.activation_requested && row.alive && row.accepting && row.enabled
        && owned.has(`${entry.backendPid}/${supervisionLockKey(row)}`) && this.executionReady(entry,row);
      updates.push((async()=>{
        if(!ready)await entry.worker.pause(true);
        await publishWorkerIntake(entry.worker,ready);
        if(ready && !entry.closing && !this.stopping)entry.worker.resume();
      })());
    }
    const refreshed=await Promise.allSettled(updates);
    const failed=refreshed.find(r=>r.status==='rejected');if(failed)throw failed.reason;
    for(const row of rows){
      const entry=this.entries.get(key(row));
      if(entry && !same(entry.row,row)){void this.closeEntry(entry,{abort:true}).catch(()=>{});continue;}
      if(!entry){if((this.maxSlots===null || this.entries.size<this.maxSlots) && this.guards.canAcquire(supervisionLockKey(row))
        && ((row.activation_requested&&row.alive&&row.accepting)||await this.unsettled(row)))await this.startEntry(row);continue;}
      if(entry.blocked){
        if(entry.closing||entry.recovering)continue;
        if(entry.recovered){void this.closeEntry(entry).catch(()=>{});continue;}
        // A slow recovery waits only on this slot; other queue consumers keep
        // reconciling. Do not release its guard until its transaction finishes.
        entry.recovering=entry.guard.withSession(guard=>recoverRemoteSlot({guard,row,lockKey:supervisionLockKey(row),profileSecret:this.profileSecret,checkpoints:this.recoveryCheckpoints}))
          .then(result=>{entry.recovered=result.settled;
            if(result.closed || result.settled)this.report({event:'remote_center_execution_recovered',node_id:row.node_id,slot:row.slot,
              attempts_closed:result.closed,settled:result.settled});})
          .catch(()=>this.report({event:'remote_center_recovery_waiting',node_id:row.node_id,slot:row.slot}))
          .finally(()=>{entry.recovering=null;});
        continue;
      }
      // Closing a consumer drains its current job; it must not block the
      // shared refresh loop while that channel finishes.
      if(!row.activation_requested)void this.closeEntry(entry).catch(()=>{});
    }
    const present=new Set(rows.map(key));
    for(const [id,entry] of this.entries)if(!present.has(id))void this.closeEntry(entry,{abort:true}).catch(()=>{});
  }
  start() {
    if(this.loop)return;
    this.loop=(async()=>{while(!this.stopping){
      // Subscribe before reading: a concurrent committed pause cannot be lost.
      const notification=this.channelStore.transportSignals?.watch('supervisor',{timeoutMs:5000,signal:this.waitAbort.signal});
      try {
        try{await this.tick();}catch{
          for(const entry of this.entries.values())await entry.worker?.pause(true);
          this.report({event:'remote_center_reconcile_failed'});
        }
        if(!this.stopping){
          if(notification){await notification.wait;await delay(100,null,{signal:this.waitAbort.signal}).catch(()=>{});}
          else await delay(this.intervalMs,null,{signal:this.waitAbort.signal}).catch(()=>{});
        }
      } finally {notification?.cancel();}
    }})();
  }
  async stop() {
    this.stopping=true;this.waitAbort.abort();await this.loop;
    await Promise.all([...this.entries.values()].map(entry=>this.closeEntry(entry)));
    await this.guards.close();
  }
}
