import assert from "node:assert/strict";
import test from "node:test";
import { queuesByRole } from "../src/queues.js";
import {
  RotaBusinessRunBudgetExhaustedError,
  RotaExecutionBudgetExhaustedError,
  RotaSlotDeferredError,
} from "../src/rotaSlotAdapter.js";
import {
  channelCandidateFailureDisposition,
  processManagedWorkerJob,
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
