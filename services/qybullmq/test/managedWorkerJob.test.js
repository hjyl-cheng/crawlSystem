import assert from "node:assert/strict";
import test from "node:test";
import { queuesByRole } from "../src/queues.js";
import {
  RotaBusinessRunBudgetExhaustedError,
  RotaExecutionBudgetExhaustedError,
  RotaSlotDeferredError,
} from "../src/rotaSlotAdapter.js";
import {
  activeChannelCandidateAttemptFence,
  channelCandidateFailureDisposition,
  clearChannelCandidateJobAttempt,
  failedChannelCandidateAttemptFence,
  markChannelCandidateJobAttemptActive,
  processManagedWorkerJob,
  recordChannelCandidateJobFailure,
  settleChannelCandidateJobFailure,
} from "../src/managedWorkerJob.js";

function channelJob() {
  return {
    id: "channel-job-01",
    queueName: queuesByRole.channelCrawl,
    data: { candidate_id: 1, run_id: "run-01" },
  };
}

test("an Execution budget error consumes the current BullMQ attempt", async () => {
  const expected = new RotaExecutionBudgetExhaustedError();
  let deferred = false;
  let terminated = false;
  await assert.rejects(
    processManagedWorkerJob({
      job: channelJob(),
      token: "bullmq-lock-token",
      execute: async () => { throw expected; },
      terminateBusinessRun: async () => { terminated = true; },
      deferForSlotPause: async () => { deferred = true; },
    }),
    (error) => error === expected,
  );
  assert.equal(deferred, false);
  assert.equal(terminated, false);
});

test("a Business Run budget error is sent to terminal recovery", async () => {
  const budgetError = new RotaBusinessRunBudgetExhaustedError();
  const terminalError = Object.assign(new Error("terminal"), { code: "BUSINESS_RUN_BUDGET_EXHAUSTED" });
  let deferred = false;
  let terminated = false;
  await assert.rejects(
    processManagedWorkerJob({
      job: channelJob(),
      token: "bullmq-lock-token",
      execute: async () => { throw budgetError; },
      terminateBusinessRun: async (job, error) => {
        assert.equal(job.id, "channel-job-01");
        assert.equal(error, budgetError);
        terminated = true;
        throw terminalError;
      },
      deferForSlotPause: async () => { deferred = true; },
    }),
    (error) => error === terminalError,
  );
  assert.equal(terminated, true);
  assert.equal(deferred, false);
});

test("no Reserve remains a BullMQ delay without consuming an attempt", async () => {
  const deferredError = new RotaSlotDeferredError("no_reserve", { retryAfterMs: 2500 });
  const calls = [];
  const result = await processManagedWorkerJob({
    job: channelJob(),
    token: "bullmq-lock-token",
    execute: async () => { throw deferredError; },
    terminateBusinessRun: async () => assert.fail("Business Run must not terminate"),
    deferForSlotPause: async (job, token, options) => {
      calls.push({ job, token, options });
      return { delayed: true };
    },
  });
  assert.deepEqual(result, { delayed: true });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].job.id, "channel-job-01");
  assert.equal(calls[0].token, "bullmq-lock-token");
  assert.deepEqual(calls[0].options, { delayMs: 2500 });
});

test("a temporarily unavailable Route remains a BullMQ delay without consuming an attempt", async () => {
  const deferredError = new RotaSlotDeferredError("route_not_ready");
  let terminated = false;
  let delayed = false;
  const result = await processManagedWorkerJob({
    job: channelJob(),
    token: "bullmq-lock-token",
    execute: async () => { throw deferredError; },
    terminateBusinessRun: async () => { terminated = true; },
    deferForSlotPause: async (_job, _token, { delayMs }) => {
      delayed = true;
      assert.equal(delayMs, 5000);
      return { delayed: true };
    },
  });

  assert.deepEqual(result, { delayed: true });
  assert.equal(delayed, true);
  assert.equal(terminated, false);
});

test("the failed listener preserves only a Business Run terminal budget state", () => {
  assert.equal(channelCandidateFailureDisposition({
    error: new RotaBusinessRunBudgetExhaustedError(),
    attemptsMade: 1,
    maxAttempts: 3,
  }), "preserve");
  assert.equal(channelCandidateFailureDisposition({
    error: new RotaExecutionBudgetExhaustedError(),
    attemptsMade: 1,
    maxAttempts: 3,
  }), "queued");
  assert.equal(channelCandidateFailureDisposition({
    error: new Error("last attempt failed"),
    attemptsMade: 3,
    maxAttempts: 3,
  }), "failed");
});

test("Candidate attempt Fences use the active and failed BullMQ attempt clocks", () => {
  const job = {
    id: "channel-job-01",
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  };

  assert.deepEqual(activeChannelCandidateAttemptFence(job), {
    candidateId: 42,
    dispatchGeneration: 7,
    jobId: "channel-job-01",
    bullmqAttempt: 3,
  });
  assert.deepEqual(failedChannelCandidateAttemptFence(job), {
    candidateId: 42,
    dispatchGeneration: 7,
    jobId: "channel-job-01",
    bullmqAttempt: 2,
  });
});

test("a Candidate attempt claim is monotonic within one dispatch generation", async () => {
  let statement = null;
  const updated = await markChannelCandidateJobAttemptActive(async (sql, params) => {
    statement = { sql, params };
    return { rowCount: 1, rows: [{}] };
  }, {
    id: "channel-job-01",
    attemptsMade: 1,
    data: { candidate_id: 42, dispatch_generation: 7 },
  });

  assert.equal(updated, true);
  assert.match(statement.sql, /snapshot_active_job_attempt<=\$3/);
  assert.match(statement.sql, /snapshot_dispatch_generation=\$4/);
  assert.match(statement.sql, /status IN \('discovered','queued','validating','accepted'\)/);
  assert.deepEqual(statement.params, [42, "channel-job-01", 2, 7]);
});

test("a completed Job only clears its own active Candidate attempt", async () => {
  let statement = null;
  const cleared = await clearChannelCandidateJobAttempt(async (sql, params) => {
    statement = { sql, params };
    return { rowCount: 1, rows: [{}] };
  }, {
    id: "channel-job-01",
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  });

  assert.equal(cleared, true);
  assert.match(statement.sql, /snapshot_dispatch_generation=\$2/);
  assert.match(statement.sql, /snapshot_active_job_id=\$3/);
  assert.match(statement.sql, /snapshot_active_job_attempt=\$4/);
  assert.deepEqual(statement.params, [42, 7, "channel-job-01", 2]);
});

test("a preserved terminal failure still releases its own Candidate attempt Fence", async () => {
  const statements = [];
  const result = await settleChannelCandidateJobFailure(async (sql, params) => {
    statements.push({ sql, params });
    return { rowCount: 1, rows: [{ candidate_id: 42 }] };
  }, {
    id: "channel-job-01",
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  }, {
    disposition: "preserve",
    message: "Business Run terminal state is already persisted",
  });

  assert.deepEqual(result, { recorded: false, fenceCleared: true });
  assert.equal(statements.length, 1);
  assert.match(statements[0].sql, /snapshot_active_job_id=\$3/);
  assert.deepEqual(statements[0].params, [42, 7, "channel-job-01", 2]);
});

test("a Candidate failed event is fenced by terminal state, generation and newer BullMQ attempt", async () => {
  let statement = null;
  const updated = await recordChannelCandidateJobFailure(async (sql, params) => {
    statement = { sql, params };
    return { rowCount: 0, rows: [] };
  }, {
    id: "channel-job-01",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 2,
    data: { candidate_id: 42, dispatch_generation: 7 },
  }, {
    disposition: "queued",
    message: "late failure",
    snapshotPatch: { failure_kind: "unknown" },
  });

  assert.equal(updated, false);
  assert.match(statement.sql, /snapshot_dispatch_generation=\$5/);
  assert.match(statement.sql, /status IN \('discovered','queued','validating'\)/);
  assert.match(statement.sql, /snapshot_active_job_id=\$6/);
  assert.match(statement.sql, /snapshot_active_job_attempt=\$7/);
  assert.deepEqual(statement.params, [
    42,
    "queued",
    "late failure",
    JSON.stringify({ failure_kind: "unknown" }),
    7,
    "channel-job-01",
    2,
  ]);
});
