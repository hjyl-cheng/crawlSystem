import { createBindingQuiescence } from './bindingQuiescence.js';
import { remoteDeadlineError } from './boundedPostgresRead.js';
import { fromChannelWire } from './channelWire.js';
import { DISCOVER_WORKLOAD } from './collectingWorkload.js';
import { discoverAttemptId, discoverPageFromWire } from './discoverPageContract.js';
import { createRemoteRotaChannelRuntime } from './rotaChannelRuntimeAdapter.js';

// Fits RotaSlotAdapter's identity-runtime seam for one remote Discover slot.
// Rota still owns BeginTask, observations, retries and CompleteTask; this
// runtime only moves the page's single search request to the node and proves
// the node's network stopped before Rota completes the attempt.
export class RemoteManagedDiscoverRuntime {
  constructor({ pages, routes, nodeId, slot, assertAdmission = null, pollMs = 100,
    claimTimeoutMs = 30000, resultTimeoutMs = 180000, stopTimeoutMs = 45000 }) {
    Object.assign(this, { pages, routes, nodeId, slot, assertAdmission, pollMs, claimTimeoutMs, resultTimeoutMs, stopTimeoutMs });
    this.active = null;
  }

  executionSnapshot() { return null; }

  async acquire({ assignment, policy, task, prepared, abortSignal }) {
    const role = DISCOVER_WORKLOAD.rotaRole;
    if (policy.role !== role || assignment.role !== role
      || assignment.identity_policy_id !== policy.id
      || Number(assignment.identity_policy_version) !== Number(policy.version)
      || assignment.identity_policy_hash !== policy.hash) {
      throw new Error('discover Runtime Assignment conflicts with its Identity Policy');
    }
    const controller = new AbortController();
    const handle = {
      assignment, rotaTask: task, prepared, controller,
      signal: abortSignal ? AbortSignal.any([abortSignal, controller.signal]) : controller.signal,
      admission: null, lease: null, network: null, networkHandle: null, error: null, quiet: null,
      quiesceBinding: createBindingQuiescence({ routes: this.routes, timeoutMs: this.stopTimeoutMs }),
    };
    handle.execute = async (_context, invoke) => {
      if (this.active) throw new Error('REMOTE_CENTRAL_RUNTIME_BUSY');
      this.active = handle;
      try { return await invoke(); }
      catch (error) { handle.error ??= error; throw error; }
      finally { if (this.active === handle) this.active = null; }
    };
    return handle;
  }

  // The Discover processor's requestPage. Runs inside handle.execute.
  async requestPage(request) {
    const handle = this.active;
    if (!handle) throw new Error('REMOTE_DISCOVER_EXECUTION_REQUIRED');
    if (handle.admission) throw new Error('REMOTE_DISCOVER_REQUEST_REPEATED');
    const page = handle.prepared?.page;
    handle.admission = await this.pages.admit({
      nodeId: this.nodeId, slot: this.slot, attemptId: discoverAttemptId(handle.rotaTask.task_id),
      rotaWorkerId: handle.assignment.worker_id, pageId: page?.page_id, pageIntentHash: page?.page_intent_hash,
      request, assertAdmission: this.assertAdmission,
    });
    handle.lease = await this.withDeadline(handle, this.claimTimeoutMs, 'REMOTE_CLAIM_TIMEOUT',
      signal => this.pages.waitClaim(handle.admission, { nodeId: this.nodeId, slot: this.slot, signal, pollMs: this.pollMs }));
    handle.network = createRemoteRotaChannelRuntime({ routes: this.routes, nodeId: this.nodeId, lease: handle.lease,
      slot: this.slot, stopTimeoutMs: this.stopTimeoutMs, quiesceBinding: handle.quiesceBinding });
    handle.networkHandle = await handle.network.acquire({ assignment: handle.assignment, task: handle.rotaTask,
      prepared: handle.prepared, abortSignal: handle.signal });
    // Publishes the route binding the node's network session waits for.
    await handle.networkHandle.execute({ job: { data: {} } }, async () => {});
    const commandId = await this.pages.request(handle.lease, { assertAdmission: this.assertAdmission });
    const value = await this.withDeadline(handle, this.resultTimeoutMs, 'REMOTE_RESULT_TIMEOUT',
      signal => this.pages.awaitResult(handle.lease, commandId, { signal, pollMs: this.pollMs }));
    // The node's own error, rebuilt with the evidence Rota classifies.
    if (value.outcome === 'failure') throw fromChannelWire(value.error);
    const result = discoverPageFromWire(value.data, request);
    await this.pages.complete(handle.lease, { command_id: commandId, status: result.status });
    return result;
  }

  async withDeadline(handle, timeoutMs, code, action) {
    const deadline = new AbortController();
    const timer = setTimeout(() => deadline.abort(remoteDeadlineError(code)), timeoutMs);
    try { return await action(AbortSignal.any([handle.signal, deadline.signal])); }
    finally { clearTimeout(timer); }
  }

  // Idempotent. Stops the exact delivery, then waits until the node has
  // released its route, so Rota never completes a task with requests in flight.
  quiesce(handle) {
    handle.quiet ??= (async () => {
      if (!handle.admission) return { active_managed_requests: 0 };
      await this.pages.stop(handle.admission, handle.error);
      if (handle.network) return handle.network.quiesce(handle.networkHandle);
      for (const binding of await this.pages.bindings(handle.admission)) {
        if (binding.state === 'retired' && binding.release_receipt?.in_flight === 0) continue;
        await handle.quiesceBinding(binding.binding_id);
      }
      return { active_managed_requests: 0 };
    })().catch(error => { handle.quiet = null; throw error; });
    return handle.quiet;
  }

  checkpoint(handle) { return this.quiesce(handle); }
  retire(handle) { return this.quiesce(handle); }
}
