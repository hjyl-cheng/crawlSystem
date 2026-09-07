import assert from "node:assert/strict";
import test from "node:test";
import { retryableSystemFailureDecision } from "../src/managedWorkerJob.js";
import { applyFailureRetryDecision } from "../src/queues.js";
import { runChannelCandidateWorkerJobWithDurableSettlement } from "../src/channelCandidateWorkerLifecycle.js";

test("stale execution is an internal fence error and discards automatic Bull retries", () => {
  for (const code of ["CONTENT_DETAIL_EXECUTION_FENCE_STALE", "CANDIDATE_ATTEMPT_FENCE_STALE", "MIGRATION_RETRY_INTENT_FENCE_STALE"]) {
    const error = Object.assign(new Error("superseded"), { code });
    const decision = retryableSystemFailureDecision(error);
    assert.equal(decision?.evidence.category, "fence");
    assert.equal(decision.retry_mode, "none");
    assert.equal(decision.proxy_action, "none");
    let discarded = false;
    assert.equal(applyFailureRetryDecision({ discard() { discarded = true; } }, decision).retry, false);
    assert.equal(discarded, true);
  }
});

test("superseded attempt does not turn a rejected failure write into another retryable persistence error", async () => {
  const error = Object.assign(new Error("detail superseded"), { code: "CONTENT_DETAIL_EXECUTION_FENCE_STALE" });
  const writes = [];
  await assert.rejects(runChannelCandidateWorkerJobWithDurableSettlement({
    query: async (sql, params) => { writes.push({ sql, params }); return { rowCount: 0, rows: [] }; },
    job: { id: "original", queueName: "youtube-channel-crawl", attemptsMade: 0, attemptsStarted: 2,
      opts: { attempts: 3 }, data: { candidate_id: 12, dispatch_generation: 2, dispatch_batch_id: "batch" } },
    execute: async () => { throw error; },
  }), value => value === error);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].params[6], true);
});
