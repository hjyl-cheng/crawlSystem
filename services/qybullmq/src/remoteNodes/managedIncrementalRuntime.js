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
    createApiFallback = null, assertAdmission = null, pollMs = 100, claimTimeoutMs = 30000, stopTimeoutMs = 45000 }) {
    Object.assign(this, { channelStore, routes, youtubeSessions, nodeId, slot, createApiFallback, assertAdmission, pollMs, claimTimeoutMs, stopTimeoutMs });
    this.executions = new RemoteChannelExecutionStore({ channelStore, profileSecret, assertAdmission });
    this.checkpoints = createRemoteYoutubeCheckpointConsumer({ sessions: youtubeSessions, profileSecret });
    this.active = null;
  }

  async acquire(args) {
    const handle = { args, admission: null, lease: null, inner: null, adapter: null,
      error: null, quiescence: null, finished: false, job: null };
    handle.execute = async ({ job, attempt }, invoke) => {
      if (this.active) throw new Error('REMOTE_CENTRAL_RUNTIME_BUSY');
      this.active = handle; handle.job = job;
      try {
        args.abortSignal.throwIfAborted();
        const country=await this.executions.pendingCountryHandoff(job);
        if(country)return {kind:'country_recheck',country};
        handle.admission = await this.executions.prepare({ ...args, job, nodeId: this.nodeId, slot: this.slot, resumeMode: attempt.resumeMode });
        const signal = AbortSignal.any([args.abortSignal, AbortSignal.timeout(this.claimTimeoutMs)]);
        handle.lease = await this.executions.waitClaim(handle.admission, { nodeId: this.nodeId, slot: this.slot, signal, pollMs: this.pollMs });
        handle.adapter = createRemoteRotaChannelRuntime({ routes: this.routes, nodeId: this.nodeId, slot: this.slot,
          lease: handle.lease, stopTimeoutMs: this.stopTimeoutMs, youtubeSessions: this.youtubeSessions,
          youtubeSession: { profileGroup: handle.admission.profileGroup, attemptId: handle.admission.attemptId },
          youtubeCheckpointConsumer: this.checkpoints });
        handle.inner = await handle.adapter.acquire(args);
        return await handle.inner.execute({ job }, invoke);
      } catch (error) { handle.error ??= error; throw error; }
      finally { this.active = null; }
    };
    return handle;
  }

  // Call from the existing executeManagedWorkerAttempt's execute callback.
  async executePlan() {
    const handle = this.active;
    if (!handle?.lease) throw new Error('REMOTE_CENTRAL_EXECUTION_REQUIRED');
    try {
      return await runRemoteIncrementalPlan({ channelStore: this.channelStore, lease: handle.lease,
        assertBusinessFence: async (client,task)=>{
          if(this.assertAdmission && await this.assertAdmission(client,this.nodeId,this.slot)!==true) {
            const error=new Error('REMOTE_SUPERVISOR_NOT_READY');error.code='REMOTE_SUPERVISOR_NOT_READY';throw error;
          }
          return assertRemoteIncrementalBusinessFence(client,task);
        }, createApiFallback: this.createApiFallback,
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
    if (handle.quiescence) return handle.quiescence;
    handle.quiescence = (async () => {
      handle.admission ??= handle.job ? await this.executions.find(handle.job, handle.args.task) : null;
      if (!handle.admission) return { active_managed_requests: 0 };
      // Also covers a lost admission/bind acknowledgement and cancellation
      // while awaiting the remote claim. Only the original attempt is stopped.
      await this.executions.stop(handle.admission, handle.error);
      const quiet = handle.inner ? await handle.adapter.quiesce(handle.inner) : { active_managed_requests: 0 };
      handle.metrics = handle.inner?.youtubeCheckpoint?.metrics ?? new ChannelExecutionMetrics().snapshot();
      const aborted = handle.args.abortSignal.aborted;
      const countryHandoff = handle.error?.code === 'UPLOADS_COUNTRY_RECHECK';
      handle.decisions = failureDecisions(handle.metrics, aborted || countryHandoff ? null : handle.error);
      await this.executions.finish(handle.admission, {
        status: aborted ? 'aborted' : handle.error && !countryHandoff ? 'failed' : 'success',
        error: countryHandoff ? null : handle.error,
        result: { youtube_requests: handle.metrics, failure_decisions: handle.decisions,
          ...(countryHandoff ? { uploads_country_recheck: handle.error.country } : {}),
          ...(handle.inner?.binding ? { remote_binding_id: handle.inner.binding.binding_id } : {}) },
      });
      handle.finished = true;
      return quiet;
    })().catch(error => { handle.quiescence = null; throw error; });
    return handle.quiescence;
  }

  checkpoint(handle) { return this.quiesce(handle); }
  retire(handle) { return this.quiesce(handle); }
}
