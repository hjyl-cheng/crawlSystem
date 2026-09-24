import { ExecutionProgress } from './executionProgress.js';
import { performance } from 'node:perf_hooks';
import { createBindingQuiescence } from './bindingQuiescence.js';
import { remoteDeadlineError } from './boundedPostgresRead.js';
import { ChannelExecutionMetrics } from '../channelExecutionContext.js';
import { failureDecisions } from '../channelExecutionRuntime.js';
import { runRemoteIncrementalPlan } from './incrementalCoordinator.js';
import { assertRemoteIncrementalBusinessFence } from './incrementalBusinessFence.js';
import { RemoteChannelExecutionStore } from './channelExecutionStore.js';
import { createRemoteRotaChannelRuntime } from './rotaChannelRuntimeAdapter.js';
import { createRemoteYoutubeCheckpointConsumer } from './youtubeProfileCheckpoint.js';

// Fits the same runtime seam as ChannelExecutionRuntimeAdapter. The caller
// still uses RotaSlotAdapter + executeManagedWorkerAttempt + API resumable jobs.
// This adapter replaces where requests run, never their retry/country policy.
export class RemoteManagedIncrementalRuntime {
  constructor({ channelStore, routes, youtubeSessions, nodeId, slot, profileSecret,
    createApiFallback = null, wholeChannels = null, loadWholeApiPolicy = null, assertAdmission = null, assertOwnership = null, readTimeoutMs = 5000, admissionTimeoutMs = 30000, report = () => {}, pollMs = 100, claimTimeoutMs = 30000, stopTimeoutMs = 45000 }) {
    Object.assign(this, { channelStore, routes, youtubeSessions, nodeId, slot, createApiFallback, wholeChannels, loadWholeApiPolicy, assertAdmission, pollMs, claimTimeoutMs, stopTimeoutMs, admissionTimeoutMs, report });
    this.executions = new RemoteChannelExecutionStore({ channelStore, profileSecret, assertAdmission, assertOwnership, readTimeoutMs });
    this.checkpoints = createRemoteYoutubeCheckpointConsumer({ sessions: youtubeSessions, profileSecret });
    this.active = null;this.current = null;
  }

  executionSnapshot() {
    const snapshot=this.current?.progress?.snapshot();
    if(!snapshot)return null;
    const pool=this.channelStore.store.pool;
    return {...snapshot,planId:this.current.job?.data?.plan_id??null,runId:this.current.args.prepared?.businessRunId??null,
      poolTotal:pool.totalCount??null,poolIdle:pool.idleCount??null,poolWaiting:pool.waitingCount??null};
  }

  stage(handle, phase, operation=null) {
    const metrics=this.channelStore.store.pool.centerPerformance;
    if(metrics?.enabled){
      const now=performance.now();
      if(handle.metricStage)metrics.record(`stage.${handle.metricStage.phase}.ms`,now-handle.metricStage.at);
      handle.metricStage={phase,at:now};
    }
    handle.progress.move(phase,operation);
    this.report({event:'remote_incremental_stage',node_id:this.nodeId,slot:this.slot,...handle.progress.snapshot()});
  }

  requestAbort(expectedAttemptId, reason='REMOTE_EXECUTION_OVERDUE') {
    const handle=this.current;
    if(!handle || handle.progress.phase==='finished')return 'no_execution';
    if(handle.progress.attemptId!==expectedAttemptId)return 'superseded';
    if(handle.controller.signal.aborted)return 'already_requested';
    handle.progress.cancel(reason);handle.error??=remoteDeadlineError(reason);
    handle.controller.abort(handle.error);
    return 'requested';
  }

  async acquire(args) {
    const controller=new AbortController();
    args={...args,abortSignal:AbortSignal.any([args.abortSignal,controller.signal])};
    const handle = { args, controller, admission: null, lease: null, inner: null, adapter: null,
      error: null, quiescence: null, finished: false, job: null };
    handle.quiesceBinding=createBindingQuiescence({routes:this.routes,timeoutMs:this.stopTimeoutMs});
    handle.progress=new ExecutionProgress({attemptId:`channel-attempt:${args.task.task_id}`,jobId:null,
      claimTimeoutMs:this.claimTimeoutMs,stopTimeoutMs:this.stopTimeoutMs,admissionTimeoutMs:this.admissionTimeoutMs});
    handle.execute = async ({ job, attempt }, invoke) => {
      if (this.active || (this.current && this.current.progress.phase!=='finished')) throw new Error('REMOTE_CENTRAL_RUNTIME_BUSY');
      this.active = handle; this.current=handle;handle.job = job;
      handle.progress=new ExecutionProgress({attemptId:`channel-attempt:${args.task.task_id}`,jobId:job.id,
        claimTimeoutMs:this.claimTimeoutMs,stopTimeoutMs:this.stopTimeoutMs,admissionTimeoutMs:this.admissionTimeoutMs});
      this.stage(handle,'admitting','pending_country_handoff');
      try {
        args.abortSignal.throwIfAborted();
        const country=await this.executions.pendingCountryHandoff(job,{signal:args.abortSignal});
        args.abortSignal.throwIfAborted();
        if(country)return {kind:'country_recheck',country};
        this.stage(handle,'admitting','prepare');
        handle.admission = await this.executions.prepare({ ...args, job, nodeId: this.nodeId, slot: this.slot, resumeMode: attempt.resumeMode });
        handle.progress.taskId=handle.admission.taskId;
        args.abortSignal.throwIfAborted();
        this.stage(handle,'awaiting_claim','wait_claim');
        const claimController=new AbortController();
        const claimTimer=setTimeout(()=>claimController.abort(remoteDeadlineError('REMOTE_CLAIM_TIMEOUT')),this.claimTimeoutMs);
        const signal = AbortSignal.any([args.abortSignal, claimController.signal]);
        try {
          handle.lease = await this.executions.waitClaim(handle.admission, { nodeId: this.nodeId, slot: this.slot, signal, pollMs: this.pollMs });
          signal.throwIfAborted();
        } finally {clearTimeout(claimTimer);}
        handle.progress.generation=handle.lease.generation;
        this.stage(handle,'binding','acquire_binding');
        handle.adapter = createRemoteRotaChannelRuntime({ routes: this.routes, nodeId: this.nodeId, slot: this.slot,
          quiesceBinding:handle.quiesceBinding,
          lease: handle.lease, stopTimeoutMs: this.stopTimeoutMs, youtubeSessions: this.youtubeSessions,
          youtubeSession: { profileGroup: handle.admission.profileGroup, attemptId: handle.admission.attemptId },
          youtubeCheckpointConsumer: this.checkpoints });
        handle.inner = await handle.adapter.acquire(args);
        return await handle.inner.execute({ job }, invoke);
      } catch (error) { handle.error ??= error; throw error; }
      finally { if(this.active===handle)this.active = null; }
    };
    return handle;
  }

  // Call from the existing executeManagedWorkerAttempt's execute callback.
  async executePlan() {
    const handle = this.active;
    if (!handle?.lease) throw new Error('REMOTE_CENTRAL_EXECUTION_REQUIRED');
    try {
      this.stage(handle,'collecting','execute_plan');
      return await runRemoteIncrementalPlan({ channelStore: this.channelStore, lease: handle.lease,
        assertBusinessFence: async (client,task)=>{
          if(this.assertAdmission && await this.assertAdmission(client,this.nodeId,this.slot)!==true) {
            const error=new Error('REMOTE_SUPERVISOR_NOT_READY');error.code='REMOTE_SUPERVISOR_NOT_READY';throw error;
          }
          return assertRemoteIncrementalBusinessFence(client,task);
        }, createApiFallback: this.createApiFallback, wholeChannels: this.wholeChannels, loadWholeApiPolicy: this.loadWholeApiPolicy,
        onProgress:(phase,operation)=>this.stage(handle,phase,operation),
        pollMs: this.pollMs, signal: handle.args.abortSignal });
    } catch (error) {
      handle.error = error;
      // Match local execution: quiesce and gather evidence before the existing
      // managed attempt classifies the failure and chooses its next route.
      await this.quiesce(handle);
      error.channel_execution_attempt = { attempt_id: handle.admission.attemptId,
        youtube_requests: handle.metrics, failure_decisions: handle.decisions };
      throw error;
    }
  }

  quiesce(handle) {
    if (!handle) return Promise.resolve({ active_managed_requests: 0 });
    if (handle.finished) return Promise.resolve({active_managed_requests:0});
    if(handle.quiescenceWait)return handle.quiescenceWait;
    if (!handle.quiescence) {
      this.stage(handle,'stopping','find_admission');
      handle.quiescence = (async () => {
      handle.admission ??= handle.job ? await this.executions.find(handle.job, handle.args.task) : null;
      let quiet={active_managed_requests:0};
      if (!handle.admission) {handle.finished=true;this.stage(handle,'recovering','rota_completion');return quiet;}
      handle.progress.taskId=handle.admission.taskId;
      this.stage(handle,'stopping','stop_task');
      // stop serializes with claim/bind/grant on the task row. Check persisted
      // bindings even when acquire/bind never returned an inner handle.
      await this.executions.stop(handle.admission, handle.error);
      this.stage(handle,'stopping','quiesce_network');
      const bindings=await this.executions.bindings(handle.admission);
      for(const binding of bindings) {
        if(binding.state==='retired' && binding.release_receipt?.in_flight===0)continue;
        await handle.quiesceBinding(binding.binding_id);
      }
      if(handle.inner)quiet=await handle.adapter.quiesce(handle.inner);
      if(quiet?.active_managed_requests!==0)throw remoteDeadlineError('REMOTE_NETWORK_NOT_QUIESCED');
      handle.metrics = handle.inner?.youtubeCheckpoint?.metrics ?? new ChannelExecutionMetrics().snapshot();
      const aborted = handle.args.abortSignal.aborted;
      const countryHandoff = handle.error?.code === 'UPLOADS_COUNTRY_RECHECK';
      handle.decisions = failureDecisions(handle.metrics, aborted || countryHandoff ? null : handle.error);
      this.stage(handle,'stopping','finish_attempt');
      await this.executions.finish(handle.admission, {
        status: aborted ? 'aborted' : handle.error && !countryHandoff ? 'failed' : 'success',
        error: countryHandoff ? null : handle.error,
        result: { youtube_requests: handle.metrics, failure_decisions: handle.decisions,
          ...(countryHandoff ? { uploads_country_recheck: handle.error.country } : {}),
          ...(handle.inner?.binding ? { remote_binding_id: handle.inner.binding.binding_id } : {}) },
      });
      handle.finished = true;
      this.stage(handle,'recovering','rota_completion');
      return quiet;
      })().catch(error => {
        handle.quiescence=null;handle.quiescenceWait=null;handle.progress.recoveryReason=error.code??error.name;
        this.stage(handle,'blocked',handle.progress.operation);throw error;
      });
    }
    // Timing out the caller does not abandon or duplicate an uncertain write.
    // Rota retains its finalization and retries this SAME cleanup promise until
    // it settles. No new admission is allowed while finalization is pending.
    handle.quiescenceWait=new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>{
        handle.progress.recoveryReason='REMOTE_CLEANUP_TIMEOUT';
        this.stage(handle,'blocked',handle.progress.operation);
        reject(remoteDeadlineError('REMOTE_CLEANUP_TIMEOUT'));
      },this.stopTimeoutMs);
      handle.quiescence.then(value=>{clearTimeout(timer);handle.quiescenceWait=null;resolve(value);},error=>{clearTimeout(timer);handle.quiescenceWait=null;reject(error);});
    });
    return handle.quiescenceWait;
  }

  async checkpoint(handle) {
    const result=await this.quiesce(handle);
    this.stage(handle,'finished');return result;
  }
  retire(handle) { return this.checkpoint(handle); }
}
