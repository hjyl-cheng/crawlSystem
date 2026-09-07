import { ChannelExecutionRuntime } from "./channelExecutionRuntime.js";
import { ManagedRequestTracker } from "./executionRuntimeSupport.js";
import { proxyAssignmentKey, sameProxyAssignment } from "./proxyAssignment.js";
import { closeYoutubeHttpProxyAgent } from "./youtube.js";

function required(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${field} is required`);
  return normalized;
}

function assertAssignment(assignment, policy) {
  required(proxyAssignmentKey(assignment), "assignment identity");
  if (assignment.identity_policy_id !== policy.id
      || Number(assignment.identity_policy_version) !== Number(policy.version)
      || assignment.identity_policy_hash !== policy.hash) {
    throw new Error("channel Runtime Assignment conflicts with its Identity Policy");
  }
}

export class ChannelExecutionRuntimeAdapter {
  constructor({
    runtime = null,
    workerId,
    incrementalExecutor,
  } = {}) {
    this.runtime = runtime ?? new ChannelExecutionRuntime({ incrementalExecutor });
    this.workerId = required(workerId, "workerId");
  }

  async acquire({ assignment, policy, proxyUrl, task, prepared, abortSignal, reusableRuntime }) {
    assertAssignment(assignment, policy);
    if (reusableRuntime && !sameProxyAssignment(reusableRuntime.assignment, assignment)) {
      await this.retire(reusableRuntime, reusableRuntime.assignment);
      reusableRuntime = null;
    }
    const value = reusableRuntime ?? {
      tracker: new ManagedRequestTracker(),
      retired: false,
      telemetry: null,
    };
    if (value.retired) throw new Error("a retired channel identity Runtime cannot be reused");
    value.tracker.reset();
    value.assignment = Object.freeze({ ...assignment });
    value.policy = policy;
    value.proxyUrl = proxyUrl;
    value.task = task;
    value.prepared = prepared;
    value.abortSignal = abortSignal;
    value.execute = (context, callback) => this.#execute(value, context, callback);
    return value;
  }

  async #execute(value, { job, prepared }, callback) {
    if (value.retired) throw new Error("channel identity Runtime is retired");
    const execution = await this.runtime.run({
      job,
      proxy: value.assignment,
      getProxySnapshot: () => value.assignment,
      proxyUrl: value.proxyUrl,
      workerId: this.workerId,
      language: value.policy.youtube_language,
      country: value.policy.youtube_country,
      timezone: value.policy.browser_profile_timezone,
      task: value.task,
      prepared,
      abortSignal: value.abortSignal,
      managedRequestTracker: value.tracker,
    }, callback);
    value.telemetry = execution;
    return execution.result;
  }

  quiesce(value) {
    return value.tracker.quiesce();
  }

  async retire(value) {
    if (!value || value.retired) return;
    value.retired = true;
    await value.tracker.quiesce();
    await Promise.allSettled([
      this.runtime.close(),
      closeYoutubeHttpProxyAgent(),
    ]);
  }

  async checkpoint(value, decision) {
    value.lastCheckpoint = decision?.kind ?? null;
  }
}
