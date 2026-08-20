import { ProxyAgent } from "undici";
import { ManagedRequestTracker } from "./executionRuntimeSupport.js";
import { proxyAssignmentKey, sameProxyAssignment } from "./proxyAssignment.js";
import { runWithProxyIdentity } from "./proxyIdentity.js";

function required(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${field} is required`);
  return normalized;
}

export class DiscoverExecutionRuntimeAdapter {
  constructor({ role = "discover", createDispatcher = (proxyUrl) => new ProxyAgent(proxyUrl) } = {}) {
    this.role = required(role, "role").toLowerCase();
    this.createDispatcher = createDispatcher;
  }

  async acquire({ assignment, policy, proxyUrl, task, prepared, abortSignal, reusableRuntime }) {
    required(proxyAssignmentKey(assignment), "assignment identity");
    if (policy.role !== this.role || assignment.role !== this.role
        || assignment.identity_policy_id !== policy.id
        || Number(assignment.identity_policy_version) !== Number(policy.version)
        || assignment.identity_policy_hash !== policy.hash) {
      throw new Error(`${this.role} Runtime Assignment conflicts with its Identity Policy`);
    }
    if (reusableRuntime && !sameProxyAssignment(reusableRuntime.assignment, assignment)) {
      await this.retire(reusableRuntime, reusableRuntime.assignment);
      reusableRuntime = null;
    }
    const value = reusableRuntime ?? {
      dispatcher: this.createDispatcher(proxyUrl),
      tracker: new ManagedRequestTracker(),
      retired: false,
    };
    if (value.retired) throw new Error(`a retired ${this.role} identity Runtime cannot be reused`);
    value.tracker.reset();
    value.assignment = Object.freeze({ ...assignment });
    value.policy = policy;
    value.proxyUrl = proxyUrl;
    value.task = task;
    value.prepared = prepared;
    value.attemptController = new AbortController();
    value.abortSignal = abortSignal
      ? AbortSignal.any([abortSignal, value.attemptController.signal])
      : value.attemptController.signal;
    value.execute = (_context, callback) => this.#execute(value, callback);
    return value;
  }

  #execute(value, callback) {
    if (value.retired) throw new Error(`${this.role} identity Runtime is retired`);
    return runWithProxyIdentity({
      ...value.assignment,
      proxy_url: value.proxyUrl,
      dispatcher: value.dispatcher,
      abort_signal: value.abortSignal,
      managed_request_tracker: value.tracker,
    }, callback);
  }

  async quiesce(value) {
    if (!value.attemptController.signal.aborted) {
      value.attemptController.abort(new Error(`${this.role} network Attempt quiesced`));
    }
    return value.tracker.quiesce();
  }

  async retire(value) {
    if (!value || value.retired) return;
    value.retired = true;
    await this.quiesce(value);
    try {
      await value.dispatcher?.close?.();
    } catch {
      value.dispatcher?.destroy?.();
    }
  }

  async checkpoint(value, decision) {
    value.lastCheckpoint = decision?.kind ?? null;
  }
}
