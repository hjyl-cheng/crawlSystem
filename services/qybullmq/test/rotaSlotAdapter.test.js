import assert from "node:assert/strict";
import test from "node:test";
import { resolveWorkerIdentityPolicy } from "../src/identityPolicyCatalog.js";
import {
  RotaSlotAdapter,
  RotaSlotDeferredError,
} from "../src/rotaSlotAdapter.js";

const resolvedPolicy = resolveWorkerIdentityPolicy({
  role: "channel",
  policyId: "qy-br-channel-anonymous-v1",
  expectedWorkloadScope: "qy-production",
  environment: {},
});

function assignment(generation, overrides = {}) {
  return {
    ok: true,
    ready: true,
    control_state: "leased_idle",
    workload_scope: "qy-production",
    protocol_version: 2,
    role: "channel",
    worker_id: "qy-channel-01",
    worker_instance_id: "instance-01",
    slot_name: "bullmq-channel-01",
    proxy_user: `bullmq-channel-01-g${generation}-opaque`,
    lease_id: "lease-01",
    lease_remaining_ms: 60_000,
    server_time: "2026-08-13T00:00:00.000Z",
    route_generation: generation,
    credential_generation: generation + 10,
    network_identity_key: `network-${generation}`,
    profile_epoch: generation - 1,
    identity_policy_id: resolvedPolicy.policy.id,
    identity_policy_version: resolvedPolicy.policy.version,
    identity_policy_hash: resolvedPolicy.policy.hash,
    identity_action: generation === 1 ? "keep" : "rotate_profile",
    egress_country: "BR",
    ...overrides,
  };
}

function job() {
  return {
    id: "job-01",
    queueName: "youtube-channel-crawl",
    attemptsMade: 0,
    data: { channel_id: "UCtest", run_id: "run-01" },
  };
}

function createFixture({
  challenge = false,
  noReserve = false,
  clientOverrides = {},
  adapterOverrides = {},
} = {}) {
  const calls = [];
  let taskNumber = 0;
  let currentAssignment = assignment(1);
  const client = {
    async claim(request) {
      calls.push({ command: "claim", request });
      return currentAssignment;
    },
    async renew(request) {
      calls.push({ command: "renew", request });
      if (challenge && !noReserve && request.known_route_generation === 1) {
        currentAssignment = assignment(2, { route_changed: true });
      }
      return noReserve
        ? assignment(1, {
          ready: false,
          control_state: "PAUSED_NO_RESERVE",
          proxy_user: "",
          network_identity_key: "",
        })
        : currentAssignment;
    },
    async beginTask(request) {
      calls.push({ command: "begin", request });
      taskNumber += 1;
      return {
        ok: true,
        task_id: `task-${taskNumber}`,
        attempt_request_id: request.attempt_request_id,
        business_run_id: request.business_run_id,
        job_execution_id: request.job_execution_id,
        attempt_number: taskNumber,
        slot_name: request.slot_name,
        route_generation: request.route_generation,
        started_at: "2026-08-13T00:00:00.000Z",
      };
    },
    async observe(request) {
      calls.push({ command: "observe", request });
      return {
        ok: true,
        observation_id: request.observation_id,
        task_id: request.task_id,
        action: "rotate_profile",
        incident_id: "incident-01",
        created_at: "2026-08-13T00:00:01.000Z",
      };
    },
    async completeTask(request) {
      calls.push({ command: "complete", request });
      if (request.outcome === "failed") {
        return {
          ok: true,
          task_completed: true,
          completion_request_id: request.completion_request_id,
          task_id: request.task_id,
          slot_name: request.slot_name,
          lease_id: request.lease_id,
          control_state: noReserve ? "PAUSED_NO_RESERVE" : "PENDING_NEW_ROUTE",
          ready: false,
          completed_task_route_generation: request.route_generation,
          pending_route_generation: request.route_generation + 1,
          pending_identity_action: "rotate_profile",
          retry_after_ms: 1,
          reason_code: noReserve ? "NO_POLICY_ELIGIBLE_RESERVE" : "WAITING_FOR_ROUTE_REFRESH",
        };
      }
      return {
        ok: true,
        task_completed: true,
        completion_request_id: request.completion_request_id,
        task_id: request.task_id,
        slot_name: request.slot_name,
        lease_id: request.lease_id,
        control_state: "READY_KEEP_ROUTE",
        ready: true,
        completed_task_route_generation: request.route_generation,
      };
    },
    async release(request) {
      calls.push({ command: "release", request });
      return {
        ok: true,
        released: true,
        release_request_id: request.release_request_id,
        lease_id: request.lease_id,
        slot_name: request.slot_name,
        route_generation: request.known_route_generation,
        status: "released",
        released_at: "2026-08-13T00:00:02.000Z",
        reason: request.reason,
      };
    },
    ...clientOverrides,
  };
  const runtimeCalls = [];
  const identityRuntime = {
    async acquire(context) {
      runtimeCalls.push({ action: "acquire", context });
      return { generation: context.assignment.route_generation };
    },
    async quiesce(handle) {
      runtimeCalls.push({ action: "quiesce", handle });
      return { active_managed_requests: 0 };
    },
    async checkpoint(handle, decision) {
      runtimeCalls.push({ action: "checkpoint", handle, decision });
    },
    async retire(handle, assignmentValue) {
      runtimeCalls.push({ action: "retire", handle, assignment: assignmentValue });
    },
  };
  let uuid = 0;
  const adapter = new RotaSlotAdapter({
    client,
    role: "channel",
    workerId: "qy-channel-01",
    workerInstanceId: "instance-01",
    resolvedPolicy,
    proxyBaseUrl: "http://rota:8000",
    proxyPassword: "proxy-password-secret",
    identityRuntime,
    renewIntervalMs: 60_000,
    routeReadyWaitMs: noReserve ? 2 : 100,
    sleepImpl: async () => {},
    randomUUID: () => `uuid-${++uuid}`,
    ...adapterOverrides,
  });
  return { adapter, calls, runtimeCalls };
}

function prepared() {
  return {
    kind: "ready",
    businessRunId: "run-01",
    workloadKind: "channel_full",
    identityPolicyId: resolvedPolicy.policy.id,
    identityPolicyVersion: resolvedPolicy.policy.version,
    identityPolicyHash: resolvedPolicy.policy.hash,
    initialResumeMode: "initial",
  };
}

test("a normal managed job uses one fenced Rota task", async () => {
  const { adapter, calls, runtimeCalls } = createFixture();
  await adapter.start();
  const attempts = [];
  const result = await adapter.executeJob(job(), {
    prepare: async () => prepared(),
    executeAttempt: async (_prepared, attempt) => {
      attempts.push(attempt);
      return {
        kind: "managed_work_complete",
        businessState: "terminal",
        result: { channel_id: "UCtest" },
      };
    },
  });
  await adapter.close();

  assert.deepEqual(result, { channel_id: "UCtest" });
  assert.equal(attempts.length, 1);
  assert.equal(attempts[0].businessRunId, "run-01");
  assert.equal(attempts[0].jobExecutionId, "youtube-channel-crawl:job-01:1");
  assert.equal(attempts[0].number, 1);
  assert.equal(attempts[0].resumeMode, "initial");
  assert.deepEqual(calls.map((call) => call.command), ["claim", "begin", "complete", "release"]);
  assert.equal(calls[2].request.attempt_quiesced, true);
  assert.equal(calls[2].request.active_managed_requests, 0);
  assert.deepEqual(runtimeCalls.map((call) => call.action), ["acquire", "quiesce", "checkpoint", "retire"]);
});

test("Rota's production Business Run budget code becomes a typed checkpoint deferral", async () => {
  const { adapter } = createFixture({
    clientOverrides: {
      async beginTask() {
        const error = new Error("proxy control business run budget exhausted");
        error.code = "BUSINESS_RUN_BUDGET_EXHAUSTED";
        error.payload = {};
        throw error;
      },
    },
  });
  await adapter.start();
  await assert.rejects(
    adapter.executeJob(job(), {
      prepare: async () => prepared(),
      executeAttempt: async () => ({ kind: "managed_work_complete", businessState: "terminal" }),
    }),
    (error) => error instanceof RotaSlotDeferredError
      && error.reason === "business_run_budget_exhausted",
  );
  await adapter.close();
});

test("a challenge rotates the same Slot and resumes the same business run", async () => {
  const { adapter, calls, runtimeCalls } = createFixture({ challenge: true });
  await adapter.start();
  const attempts = [];
  const result = await adapter.executeJob(job(), {
    prepare: async () => prepared(),
    executeAttempt: async (_prepared, attempt) => {
      attempts.push(attempt);
      if (attempt.number === 1) {
        return {
          kind: "retryable_network_failure",
          observation: "youtube_challenge",
          source: "youtubejs_player",
          failedStage: "content_detail",
          checkpointPersisted: true,
        };
      }
      return {
        kind: "managed_work_complete",
        businessState: "terminal",
        result: { resumed: true },
      };
    },
  });
  await adapter.close();

  assert.deepEqual(result, { resumed: true });
  assert.equal(attempts.length, 2);
  assert.deepEqual(attempts.map((value) => value.routeGeneration), [1, 2]);
  assert.deepEqual(attempts.map((value) => value.businessRunId), ["run-01", "run-01"]);
  assert.deepEqual(attempts.map((value) => value.jobExecutionId), [
    "youtube-channel-crawl:job-01:1",
    "youtube-channel-crawl:job-01:1",
  ]);
  assert.deepEqual(calls.map((call) => call.command), [
    "claim", "begin", "observe", "complete", "renew", "begin", "complete", "release",
  ]);
  assert.equal(calls.filter((call) => call.command === "claim").length, 1);
  assert.ok(calls
    .filter((call) => ["begin", "observe", "complete", "renew", "release"].includes(call.command))
    .every((call) => call.request.slot_name === "bullmq-channel-01"
      && call.request.lease_id === "lease-01"));
  assert.equal(runtimeCalls.filter((call) => call.action === "retire").length, 2);
});

test("no policy-eligible warm standby returns a bounded defer without reusing the failed route", async () => {
  const { adapter, calls } = createFixture({ challenge: true, noReserve: true });
  await adapter.start();
  await assert.rejects(
    adapter.executeJob(job(), {
      prepare: async () => prepared(),
      executeAttempt: async () => ({
        kind: "retryable_network_failure",
        observation: "youtube_challenge",
        source: "youtubejs_player",
        failedStage: "content_detail",
        checkpointPersisted: true,
      }),
    }),
    (error) => error instanceof RotaSlotDeferredError && error.reason === "no_reserve",
  );
  await adapter.close();
  assert.equal(calls.filter((call) => call.command === "begin").length, 1);
  assert.equal(calls.filter((call) => call.command === "claim").length, 1);
});

test("prepared work with a different Policy is rejected before BeginTask", async () => {
  const { adapter, calls } = createFixture();
  await adapter.start();
  await assert.rejects(
    adapter.executeJob(job(), {
      prepare: async () => ({
        ...prepared(),
        identityPolicyHash: "sha256:stale",
      }),
      executeAttempt: async () => assert.fail("attempt must not start"),
    }),
    (error) => error instanceof RotaSlotDeferredError && error.reason === "policy_unavailable",
  );
  await adapter.close();
  assert.equal(calls.some((call) => call.command === "begin"), false);
});

test("a ready Assignment with a stale or foreign Lease fence fails closed", async () => {
  for (const invalid of [
    assignment(1, { workload_scope: "qy-test" }),
    assignment(1, { identity_policy_hash: "sha256:stale" }),
  ]) {
    const { adapter } = createFixture();
    adapter.client.claim = async () => invalid;
    await assert.rejects(adapter.start(), /conflicts with the Worker identity policy/);
  }
});

test("a ready Assignment without its initial profile epoch fails during Claim", async () => {
  const { adapter } = createFixture();
  adapter.client.claim = async () => {
    const value = assignment(1);
    delete value.profile_epoch;
    return value;
  };

  await assert.rejects(
    adapter.start(),
    /assignment\.profile_epoch must be a non-negative integer/,
  );
});

test("a preferred egress country never becomes a Worker hard gate", async () => {
  for (const egressCountry of ["US", ""]) {
    const { adapter } = createFixture();
    adapter.client.claim = async () => assignment(1, { egress_country: egressCountry });
    await adapter.start();
    await adapter.close();
  }
});

test("a failed periodic Renew fences the Slot before another job can begin", async () => {
  let scheduledRenew = null;
  let renewAttempted;
  const renewStarted = new Promise((resolve) => { renewAttempted = resolve; });
  const { adapter, calls } = createFixture({
    clientOverrides: {
      async renew(request) {
        calls.push({ command: "renew", request });
        renewAttempted();
        const error = new Error("lease is no longer authoritative");
        error.retryable = false;
        throw error;
      },
    },
    adapterOverrides: {
      setTimeoutImpl(callback) {
        scheduledRenew = callback;
        return { unref() {} };
      },
      clearTimeoutImpl() {},
    },
  });
  await adapter.start();
  assert.equal(typeof scheduledRenew, "function");
  scheduledRenew();
  await renewStarted;
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(adapter.status().assignment.ready, false);
  await assert.rejects(
    adapter.executeJob(job(), {
      prepare: async () => prepared(),
      executeAttempt: async () => assert.fail("attempt must not start"),
    }),
    (error) => error instanceof RotaSlotDeferredError && error.reason === "slot_not_ready",
  );
  assert.equal(calls.some((call) => call.command === "begin"), false);
  await adapter.close();
});

test("a failed Renew aborts the active network Attempt", async () => {
  let scheduledRenew = null;
  let renewAttempted;
  const renewStarted = new Promise((resolve) => { renewAttempted = resolve; });
  let attemptEntered;
  const attemptStarted = new Promise((resolve) => { attemptEntered = resolve; });
  let observedSignal = null;
  const { adapter, calls } = createFixture({
    clientOverrides: {
      async renew(request) {
        calls.push({ command: "renew", request });
        renewAttempted();
        const error = new Error("lease is no longer authoritative");
        error.retryable = false;
        throw error;
      },
    },
    adapterOverrides: {
      setTimeoutImpl(callback) {
        scheduledRenew = callback;
        return { unref() {} };
      },
      clearTimeoutImpl() {},
    },
  });
  await adapter.start();
  const execution = adapter.executeJob(job(), {
    prepare: async () => prepared(),
    executeAttempt: async (_prepared, attempt) => {
      observedSignal = attempt.abortSignal;
      attemptEntered();
      await Promise.race([
        new Promise((resolve, reject) => {
          attempt.abortSignal.addEventListener("abort", () => {
            reject(attempt.abortSignal.reason);
          }, { once: true });
        }),
        new Promise((resolve, reject) => {
          setTimeout(() => reject(new Error("active Attempt was not aborted")), 50);
        }),
      ]);
      return { kind: "managed_work_complete", businessState: "terminal" };
    },
  });
  const executionSettled = assert.rejects(execution, /lease is no longer authoritative/);
  await attemptStarted;
  scheduledRenew();
  await renewStarted;
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(observedSignal.aborted, true);
  await executionSettled;
  await adapter.close();
});

test("an uncertain CompleteTask response is retried with the same idempotency key", async () => {
  const { adapter, calls } = createFixture();
  const completeTask = adapter.client.completeTask.bind(adapter.client);
  let completeAttempts = 0;
  adapter.client.completeTask = async (request) => {
    completeAttempts += 1;
    if (completeAttempts === 1) {
      calls.push({ command: "complete", request });
      const error = new Error("complete response was lost");
      error.retryable = true;
      throw error;
    }
    return completeTask(request);
  };

  await adapter.start();
  const result = await adapter.executeJob(job(), {
    prepare: async () => prepared(),
    executeAttempt: async () => ({
      kind: "managed_work_complete",
      businessState: "terminal",
      result: { completed: true },
    }),
  });
  await adapter.close();

  assert.deepEqual(result, { completed: true });
  const completions = calls.filter((call) => call.command === "complete");
  assert.equal(completions.length, 2);
  assert.equal(
    completions[0].request.completion_request_id,
    completions[1].request.completion_request_id,
  );
  assert.deepEqual(completions[0].request, completions[1].request);
  assert.equal(calls.filter((call) => call.command === "begin").length, 1);
});

test("close cancels and completes an active task before releasing its Lease", async () => {
  const { adapter, calls, runtimeCalls } = createFixture();
  let attemptEntered;
  const attemptStarted = new Promise((resolve) => { attemptEntered = resolve; });
  let observedSignal = null;
  await adapter.start();
  const execution = adapter.executeJob(job(), {
    prepare: async () => prepared(),
    executeAttempt: async (_prepared, attempt) => {
      observedSignal = attempt.abortSignal;
      attemptEntered();
      await Promise.race([
        new Promise((resolve, reject) => {
          attempt.abortSignal.addEventListener("abort", () => {
            reject(attempt.abortSignal.reason);
          }, { once: true });
        }),
        new Promise((resolve, reject) => {
          setTimeout(() => reject(new Error("shutdown did not abort active Attempt")), 100);
        }),
      ]);
      return { kind: "managed_work_complete", businessState: "terminal" };
    },
  });
  const executionSettled = assert.rejects(execution, /adapter_closing/);
  await attemptStarted;
  await adapter.close();
  await executionSettled;

  assert.equal(observedSignal.aborted, true);
  assert.deepEqual(calls.map((call) => call.command), [
    "claim", "begin", "complete", "release",
  ]);
  assert.equal(calls[2].request.outcome, "cancelled");
  assert.deepEqual(runtimeCalls.map((call) => call.action), [
    "acquire", "quiesce", "retire",
  ]);
});

test("an unresolved CompleteTask fences the Slot and close does not release an active Lease", async () => {
  const { adapter, calls } = createFixture({
    clientOverrides: {
      async completeTask(request) {
        calls.push({ command: "complete", request });
        const error = new Error("complete outcome remains uncertain");
        error.retryable = true;
        throw error;
      },
    },
    adapterOverrides: { maxCompletionAttempts: 2 },
  });
  await adapter.start();
  await assert.rejects(
    adapter.executeJob(job(), {
      prepare: async () => prepared(),
      executeAttempt: async () => ({
        kind: "managed_work_complete",
        businessState: "terminal",
      }),
    }),
    /complete outcome remains uncertain/,
  );

  assert.equal(adapter.status().assignment.ready, false);
  await assert.rejects(
    adapter.executeJob(job(), {
      prepare: async () => prepared(),
      executeAttempt: async () => assert.fail("second Attempt must not start"),
    }),
    (error) => error instanceof RotaSlotDeferredError && error.reason === "slot_not_ready",
  );
  await adapter.close();

  const completions = calls.filter((call) => call.command === "complete");
  assert.ok(completions.length >= 2);
  assert.equal(new Set(completions.map(
    (call) => call.request.completion_request_id,
  )).size, 1);
  assert.equal(calls.some((call) => call.command === "release"), false);
});

test("a periodic Renew and route-ready poll share one coalesced control request", async () => {
  let scheduledRenew = null;
  const { adapter, calls } = createFixture({
    challenge: true,
    adapterOverrides: {
      setTimeoutImpl(callback) {
        scheduledRenew = callback;
        return { unref() {} };
      },
      clearTimeoutImpl() {},
    },
  });
  const completeTask = adapter.client.completeTask.bind(adapter.client);
  adapter.client.completeTask = async (request) => {
    const result = await completeTask(request);
    if (request.outcome === "failed") scheduledRenew();
    return result;
  };

  await adapter.start();
  const result = await adapter.executeJob(job(), {
    prepare: async () => prepared(),
    executeAttempt: async (_prepared, attempt) => (
      attempt.number === 1
        ? {
          kind: "retryable_network_failure",
          observation: "youtube_challenge",
          source: "youtubejs_player",
          failedStage: "content_detail",
          checkpointPersisted: true,
        }
        : {
          kind: "managed_work_complete",
          businessState: "terminal",
          result: { resumed: true },
        }
    ),
  });
  await adapter.close();

  assert.deepEqual(result, { resumed: true });
  assert.equal(calls.filter((call) => call.command === "renew").length, 1);
});

test("a stale Renew response cannot overwrite or fence a newer Assignment", async () => {
  const scheduled = [];
  let renewCount = 0;
  let resolveRenew;
  let renewObserved = new Promise((resolve) => { resolveRenew = resolve; });
  const { adapter, calls } = createFixture({
    clientOverrides: {
      async renew(request) {
        calls.push({ command: "renew", request });
        renewCount += 1;
        const result = renewCount === 1 ? assignment(2) : assignment(1);
        resolveRenew();
        return result;
      },
    },
    adapterOverrides: {
      setTimeoutImpl(callback) {
        scheduled.push(callback);
        return { unref() {} };
      },
      clearTimeoutImpl() {},
    },
  });
  await adapter.start();

  scheduled.shift()();
  await renewObserved;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(adapter.status().assignment.route_generation, 2);

  renewObserved = new Promise((resolve) => { resolveRenew = resolve; });
  scheduled.shift()();
  await renewObserved;
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(adapter.status().assignment.route_generation, 2);
  assert.equal(adapter.status().assignment.ready, true);
  await adapter.close();
});
