import { incrementalProgressConfig } from './incrementalProgressConfig.js';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { Worker } from 'bullmq';
import { collectingWorkload } from './collectingWorkload.js';
import { RotaSlotAdapter } from '../rotaSlotAdapter.js';
import { RemoteManagedIncrementalRuntime } from './managedIncrementalRuntime.js';
import { createCenterIncrementalProcessor } from './centerIncrementalProcessor.js';
import { WHOLE_CHANNEL_RUNTIME_REVISION } from './workerActivationStore.js';
import { recoverRemoteSlot, remoteSlotUnsettled, supervisionLockKey, settleTerminalRemoteHandoffs } from './centerExecutionRecovery.js';
import { createRemoteYoutubeCheckpointConsumer } from './youtubeProfileCheckpoint.js';
import {intakeWorkerName,publishWorkerIntake} from '../workerIntakeTelemetry.js';
import {SupervisionGuards} from './supervisionGuards.js';
import {reconcileIntakeRequests} from './intakeRequests.js';

const key = row => `${row.node_id}/${row.slot}`;
const same = (a,b) => ['node_id','slot','deployment_id','config_hash','instance_id','relay_boot_id','runtime_revision'].every(field=>a[field]===b[field]);
export { supervisionLockKey };
const identity = row => ({version:1,mode:row.mode,node_id:row.node_id,slot:row.slot,
  deployment_id:row.deployment_id,config_hash:row.config_hash,instance_id:row.instance_id,
  relay_boot_id:row.relay_boot_id,runtime_revision:row.runtime_revision,accepting:true});

// One central queue consumer/Rota adapter for each admitted remote process.
// Independent PG advisory locks, held on a bounded group of sessions, exclude
// another center from owning the same slot. All business policy stays in the original managed attempt and runner.
export class RemoteCenterExecutionSupervisor {
  constructor({store,channelStore,routes,youtubeSessions,activation,guardPool,connection,prefix,
    allowedNodeIds=[],dashboardManaged=false,pausedWorkers=[],resolvedPolicy,profileSecret,rotaClient,proxyBaseUrl,proxyPassword,
    createApiFallback=null,wholeChannels=null,loadWholeApiPolicy=null,intervalMs=1000,maxSlots=null,guardGroups=4,report=()=>{},WorkerClass=Worker,createRota=args=>new RotaSlotAdapter(args),
    incrementalProgress=incrementalProgressConfig(),mode='incremental_collect',createRuntime=null,createProcessor=null,recoverSlot=null,settleHandoffs=null,slotUnsettled=null}) {
    const workload=collectingWorkload(mode);
    if(!workload)throw new TypeError('unknown collecting workload');
    if(mode==='full_crawl_collect' && ([createRuntime,createProcessor,recoverSlot,slotUnsettled].some(fn=>typeof fn!=='function')||(settleHandoffs!==false&&typeof settleHandoffs!=='function')))throw new TypeError('explicit full-crawl runtime, processor and recovery required');
    createRuntime??=args=>new RemoteManagedIncrementalRuntime(args);createProcessor??=createCenterIncrementalProcessor;
    recoverSlot??=recoverRemoteSlot;settleHandoffs??=settleTerminalRemoteHandoffs;slotUnsettled??=remoteSlotUnsettled;
    Object.assign(this,{workload,recoverSlot,settleHandoffs,slotUnsettled,incrementalProgress});
    if(!Array.isArray(allowedNodeIds)||(!allowedNodeIds.length&&!dashboardManaged)||allowedNodeIds.some(id=>!/^[a-f0-9-]{36}$/.test(id)))throw new TypeError('explicit admitted node IDs required');
    if(!guardPool || !connection || typeof prefix!=='string' || !prefix || intervalMs<50 || (maxSlots!==null && (!Number.isSafeInteger(maxSlots) || maxSlots<1)))throw new TypeError('explicit supervision database, Redis connection and queue prefix required');
    Object.assign(this,{store,channelStore,routes,youtubeSessions,activation,guardPool,connection,prefix,allowedNodeIds,dashboardManaged,
      resolvedPolicy,profileSecret,rotaClient,proxyBaseUrl,proxyPassword,createApiFallback,wholeChannels,loadWholeApiPolicy,intervalMs,maxSlots,report,WorkerClass,createRota,createRuntime,createProcessor});
    this.entries=new Map();this.stopping=false;this.loop=null;this.waitAbort=new AbortController();
    this.pausedWorkers=new Set(pausedWorkers);
    this.guards=new SupervisionGuards({pool:guardPool,groups:guardGroups});
    this.recoveryCheckpoints=mode==='incremental_collect'&&youtubeSessions?createRemoteYoutubeCheckpointConsumer({sessions:youtubeSessions,profileSecret}):null;
  }
  allowsNode(nodeId) { return this.dashboardManaged || this.allowedNodeIds.includes(nodeId); }
  isWorkerPaused(row) { return this.pausedWorkers?.has(key(row))===true; }
  isProcessing(row) { return this.entries.get(key(row))?.processing === true; }
  executionSnapshot(row) {
    const entry=this.entries.get(key(row));
    const snapshot=entry?.runtime?.executionSnapshot?.();
    if(!snapshot)return null;
    return {...snapshot,acceptingNewJobs:!!entry.queueReady && !entry.progressRecovery && !entry.closing
      && !entry.blocked && !this.isWorkerPaused(row) && !!row.activation_requested && !!row.enabled
      && row.accepting!==false && row.connected!==false && this.executionReady(entry,row),
      recoveryAttempts:entry.progressRecovery?1:0};
  }
  inspectProgress() {
    if(this.stopping || this.workload.mode!=='incremental_collect')return;
    for(const entry of this.entries.values()) {
      const snapshot=entry.runtime?.executionSnapshot?.();
      if(!snapshot || !entry.owned || entry.closing)continue;
      const status=`${snapshot.attemptId}/${snapshot.progressHealth}/${snapshot.executionPhase}`;
      if(status!==entry.progressStatus) {
        entry.progressStatus=status;
        this.report({event:'remote_incremental_progress',node_id:entry.row.node_id,slot:entry.row.slot,...snapshot});
      }
      if(entry.progressRecovery) {
        const route=entry.rota?.status();
        if(snapshot.attemptId===entry.progressRecovery && snapshot.executionPhase==='finished' && !entry.processing
          && route?.started && !route.closing && !route.active_job && !route.active_task_id && !route.recovery_pending
          && !route.control_in_flight && !route.reclaim_in_flight)entry.progressRecovery=null;
        continue;
      }
      if(this.incrementalProgress.mode!=='enforce' || !this.incrementalProgress.allowlist.includes(key(entry.row))
        || !snapshot.canAbort)continue;
      // Gate only new jobs. Keep old claim/heartbeat/receipt/cleanup paths live.
      // The original Rota attempt remains the sole cleanup owner.
      entry.progressRecovery=snapshot.attemptId;
      void Promise.resolve().then(()=>entry.worker?.pause(true))
        .then(()=>entry.worker && publishWorkerIntake(entry.worker,false))
        .catch(error=>this.report({event:'remote_incremental_pause_failed',node_id:entry.row.node_id,slot:entry.row.slot,code:error?.code??error?.name}));
      const result=entry.runtime.requestAbort(snapshot.attemptId,'REMOTE_EXECUTION_OVERDUE');
      if(!['requested','already_requested'].includes(result))entry.progressRecovery=null;
    }
  }
  preparationState(row) {
    const entry=this.entries.get(key(row));
    if(this.isWorkerPaused(row) || !row.activation_requested || !entry || entry.closing || entry.blocked)return null;
    if(!entry.rota)return 'preparing';
    const state=entry.rota.status();
    return !state.started ? 'waiting_network' : !state.assignment?.ready ? 'network_unready' : null;
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
    if(this.workload.role==='fullcrawl' && entry.runtime?.readyForTasks()!==true)return false;
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
    if(this.stopping || this.isWorkerPaused(entry.row) || entry.progressRecovery || entry.closing || !entry.owned || !entry.queueReady)return false;
    const row=(await this.store.pool.query(`SELECT w.*,n.state AS node_state,
      w.connected_until>clock_timestamp() AS alive,
      COALESCE((SELECT w.slot=ANY(p.selected_slots) FROM remote_ingestion.node_intake_requests p
        WHERE p.node_id=w.node_id AND p.deployment_id=w.deployment_id),w.activation_requested) AS intake_requested,
      EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=$3 AND granted
        AND classid=781138012::oid AND objid=(hashtext($4)::bigint & 4294967295)::oid AND objsubid=2) AS guard_owned
      FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
      WHERE w.node_id=$1 AND w.slot=$2`,[entry.row.node_id,entry.row.slot,entry.backendPid,supervisionLockKey(entry.row)])).rows[0];
    return !entry.progressRecovery && !!row && !row.retirement_id && row.node_state==='active' && row.alive && row.accepting && row.activation_requested && row.intake_requested
      && row.enabled && row.guard_owned && this.executionReady(entry,row);
  }

  async startEntry(row) {
    const entry={row,supervisorId:randomUUID(),owned:false,queueReady:false,closing:false};
    let stage='supervision_guard';
    const reportFailure=error=>this.report({event:'remote_center_slot_start_failed',node_id:row.node_id,slot:row.slot,stage,
      code:error?.code??(stage==='supervision_guard'&&error?.message==='timeout exceeded when trying to connect'
        ?'SUPERVISION_POOL_TIMEOUT':error?.name)});
    this.entries.set(key(row),entry);
    try {
      entry.guard=await this.guards.acquire(supervisionLockKey(row),()=>{entry.owned=false;void this.closeEntry(entry,{abort:true}).catch(()=>{});});
      if(!entry.guard){this.entries.delete(key(row));return;}
      if(!entry.guard.alive)throw new Error('SUPERVISION_SESSION_LOST');
      entry.owned=true;entry.backendPid=entry.guard.backendPid;
      stage='previous_execution';
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
      if(this.isWorkerPaused(row) || !row.alive || !row.accepting || !row.activation_requested){await this.closeEntry(entry);return;}
      stage='runtime';
      const runtime=this.createRuntime({channelStore:this.channelStore,routes:this.routes,youtubeSessions:this.youtubeSessions,
        workerConnection:identity(row),nodeId:row.node_id,slot:row.slot,profileSecret:this.profileSecret,createApiFallback:this.createApiFallback,
        wholeChannels:row.runtime_revision===WHOLE_CHANNEL_RUNTIME_REVISION?this.wholeChannels:null,
        loadWholeApiPolicy:this.loadWholeApiPolicy,
        ...(this.workload.mode==='incremental_collect'?{...this.incrementalProgress,report:this.report}:{}),
        assertOwnership:async client=>{
          if(!entry.owned || this.entries.get(key(row))!==entry)return false;
          const owned=await client.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=$1 AND granted
            AND classid=781138012::oid AND objid=(hashtext($2)::bigint & 4294967295)::oid AND objsubid=2`,
            [entry.backendPid,supervisionLockKey(row)]);
          return owned.rowCount===1 && entry.owned;
        },
        assertAdmission:async client=>{
          const current=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
            FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2`,[row.node_id,row.slot])).rows[0];
          return !!current?.alive && !this.isWorkerPaused(current) && !current.retirement_id && current.enabled && same(entry.row,current) && await this.verifyExecution(client,current);
        }});
      entry.runtime=runtime;
      entry.rota=this.createRota({client:this.rotaClient,role:'channel',workerId:row.rota_worker_id,workerInstanceId:entry.supervisorId,
        resolvedPolicy:this.resolvedPolicy,proxyBaseUrl:this.proxyBaseUrl,proxyPassword:this.proxyPassword,identityRuntime:runtime});
      const process=this.createProcessor({channelStore:this.channelStore,runtime,rota:entry.rota,resolvedPolicy:this.resolvedPolicy,
        createApiFallback:this.createApiFallback,ready:()=>this.ready(entry),report:this.report});
      entry.worker=new this.WorkerClass(this.workload.queue,async(job,token)=>{
        entry.processing=true;
        try{return await process(job,token);}finally{entry.processing=false;}
      },{connection:this.connection,prefix:this.prefix,
        concurrency:1,autorun:false,name:intakeWorkerName('remote',`${row.node_id}-${row.slot}`)});
      entry.worker.on('error',()=>this.report({event:'remote_center_queue_error',node_id:row.node_id,slot:row.slot}));
      entry.worker.on('failed',(job,error)=>this.report({event:'remote_center_job_failed',node_id:row.node_id,slot:row.slot,job_id:job?.id,code:error?.code??error?.name}));
      stage='queue';
      await entry.worker.waitUntilReady();
      // BullMQ waitUntilReady returns its blocking dequeue connection. That
      // connection is deliberately closed during a graceful drain; the main
      // client still renews the active job lock until execution finishes.
      entry.redis=await entry.worker.client;
      await entry.worker.pause(true);
      // start() can wait for an available Rota route. Do not block other slots.
      stage='rota';
      entry.starting=entry.rota.start().then(async()=>{
        if(entry.closing||this.stopping)return;
        entry.queueReady=true;
        entry.running=entry.worker.run().catch(()=>this.closeEntry(entry,{abort:true}));
        stage='activation';
        await this.activation.activate(identity(row),{requireRequested:true});
        stage='readiness';
        const ready=await this.ready(entry);await publishWorkerIntake(entry.worker,ready);
        if(ready && !entry.progressRecovery)entry.worker.resume();
      }).catch(error=>{reportFailure(error);return this.closeEntry(entry,{abort:true});});
    } catch(error) {
      await this.closeEntry(entry,{abort:true});
      reportFailure(error);
    }
  }
  async unsettled(row) {
    return this.slotUnsettled(this.store.pool,row);
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
    this.inspectProgress();
    if(this.settleHandoffs && !this.handoffRecovery && (!this.handoffCheckedAt || Date.now()-this.handoffCheckedAt>=30000)){
      this.handoffCheckedAt=Date.now();
      this.handoffRecovery=this.settleHandoffs(this.store)
        .then(count=>{if(count)this.report({event:'remote_center_terminal_handoffs_settled',count});})
        .catch(error=>this.report({event:'remote_center_terminal_handoff_recovery_failed',code:error?.code??error?.name}))
        .finally(()=>{this.handoffRecovery=null;});
    }
    await reconcileIntakeRequests(this.store);
    const owners=[...this.entries.values()].filter(e=>e.owned).map(e=>({pid:e.backendPid,lock_key:supervisionLockKey(e.row)}));
    const [connections,locks]=await Promise.all([this.store.pool.query(`SELECT w.*,s.rota_worker_id,w.connected_until>clock_timestamp() AS alive
      FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
      JOIN remote_ingestion.network_slots s USING(node_id,slot)
      WHERE (w.node_id=ANY($1::uuid[]) OR ($2::boolean AND EXISTS(
        SELECT 1 FROM remote_ingestion.node_deployments d WHERE d.node_id=w.node_id AND d.deployment_id=w.deployment_id)))
        AND n.state='active' AND w.mode=$3 AND w.role=$4 AND w.retired_at IS NULL`,[this.allowedNodeIds,this.dashboardManaged,this.workload.mode,this.workload.role]),
      this.store.pool.query(`SELECT owner.pid,owner.lock_key FROM pg_locks AS guard
        JOIN jsonb_to_recordset($1::jsonb) AS owner(pid integer,lock_key text)
          ON guard.pid=owner.pid AND guard.objid=(hashtext(owner.lock_key)::bigint & 4294967295)::oid
        WHERE guard.locktype='advisory' AND guard.granted AND guard.classid=781138012::oid AND guard.objsubid=2`,[JSON.stringify(owners)])]);
    const rows=connections.rows,owned=new Set(locks.rows.map(r=>`${r.pid}/${r.lock_key}`));
    // A maintenance pause overrides even an older persisted intake request.
    // Keep the row visible so existing executions can finish recovery.
    for(const row of rows)if(this.isWorkerPaused(row))row.activation_requested=false;
    const updates=[];
    // Refresh existing consumers together. Slow admission or recovery of one
    // slot cannot hold back the other slots' 15-second intake advertisements.
    for(const row of rows){
      const entry=this.entries.get(key(row));
      if(!entry || !same(entry.row,row) || entry.blocked || !entry.queueReady || entry.closing)continue;
      const ready=!this.stopping && !entry.progressRecovery && row.activation_requested && row.alive && row.accepting && row.enabled
        && owned.has(`${entry.backendPid}/${supervisionLockKey(row)}`) && this.executionReady(entry,row);
      if(ready)entry.notReadySince=null;
      else entry.notReadySince??=Date.now();
      updates.push((async()=>{
        if(!ready)await entry.worker.pause(true);
        await publishWorkerIntake(entry.worker,ready);
        if(ready && !entry.progressRecovery && !entry.closing && !this.stopping)entry.worker.resume();
      })());
    }
    const refreshed=await Promise.allSettled(updates);
    const failed=refreshed.find(r=>r.status==='rejected');if(failed)throw failed.reason;
    for(const row of rows){
      const entry=this.entries.get(key(row));
      if(entry && !same(entry.row,row)){void this.closeEntry(entry,{abort:true}).catch(()=>{});continue;}
      if(!entry){if((this.maxSlots===null || this.entries.size<this.maxSlots) && this.guards.canAcquire(supervisionLockKey(row))
        && ((row.activation_requested&&row.alive&&row.accepting)||await this.unsettled(row)))await this.startEntry(row);continue;}
      // A non-retryable Renew failure stops the adapter's renewal loop. An
      // otherwise healthy node heartbeat cannot restart it. Recreate only an
      // idle, quiesced owner; never recycle an active/finalizing attempt or a
      // healthy owner merely waiting for reserve capacity.
      const route=entry.rota?.status();
      const controlState=route?.control_state??route?.assignment?.control_state;
      const abandonedReclaim=controlState==='RECLAIMING' && !route?.assignment
        && route?.reclaim_in_flight===false;
      if(!entry.closing && !entry.blocked && entry.queueReady && !entry.processing
        && route?.started && !route.closing && !route.reclaim_in_flight && !route.control_in_flight
        && !route?.active_job && !route?.active_task_id && !route?.recovery_pending
        && row.activation_requested && row.alive && row.accepting
        && entry.notReadySince && Date.now()-entry.notReadySince>=30000
        && (abandonedReclaim || ['RENEW_FAILED','LEASE_GONE','LEASE_CONFLICT'].includes(controlState))){
        this.report({event:'remote_center_idle_route_recovering',node_id:row.node_id,slot:row.slot,
          reason:controlState,last_recovery_error:route.last_recovery_error??null});
        void this.closeEntry(entry).catch(()=>{});continue;
      }
      if(entry.blocked){
        if(entry.closing||entry.recovering)continue;
        if(entry.recovered){void this.closeEntry(entry).catch(()=>{});continue;}
        // A slow recovery waits only on this slot; other queue consumers keep
        // reconciling. Do not release its guard until its transaction finishes.
        entry.recovering=entry.guard.withSession(guard=>this.recoverSlot({guard,row,lockKey:supervisionLockKey(row),profileSecret:this.profileSecret,checkpoints:this.recoveryCheckpoints}))
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
    // This timer must not wait behind tick's database reads or slot cleanup.
    this.progressTimer=setInterval(()=>this.inspectProgress(),1000);
    this.progressTimer.unref?.();
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
    this.stopping=true;clearInterval(this.progressTimer);this.waitAbort.abort();await this.loop;
    await this.handoffRecovery;
    await Promise.all([...this.entries.values()].map(entry=>this.closeEntry(entry)));
    await this.guards.close();
  }
}
