import assert from "node:assert/strict";
import test from "node:test";

import {
  completeChannelCandidateWorkerJob,
  failChannelCandidateWorkerJob,
  runChannelCandidateWorkerJobWithDurableSettlement,
} from "../src/channelCandidateWorkerLifecycle.js";

function completedJob() {
  return {
    id: "channel-snapshot__legacy-results-canary__UC0Noar__g2",
    attemptsMade: 1,
    data: { candidate_id: 482, dispatch_generation: 2 },
  };
}

test("Worker completion clears its Candidate Fence and resolves the retry item atomically", async () => {
  const calls = [];
  const result = await completeChannelCandidateWorkerJob(async (sql, params) => {
    calls.push({ sql, params });
    return { rowCount: 1, rows: [{ candidate_id: 482, resolved_count: 1 }] };
  }, completedJob());

  assert.deepEqual(result, { cleared: true, resolved: 1 });
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /WITH cleared_candidate AS \(/);
  assert.match(calls[0].sql, /UPDATE crawler\.channel_candidates/);
  assert.match(calls[0].sql, /resolved_retry AS \([\s\S]*UPDATE crawler\.migration_system_retry_items/);
  assert.match(calls[0].sql, /FROM cleared_candidate/);
  assert.deepEqual(calls[0].params, [
    482,
    2,
    completedJob().id,
    1,
    "job_completed",
  ]);
});

test("terminal business failure settles the Candidate and retry item in one transaction", async () => {
  const transactionStatements = [];
  let outsideQueryCalled = false;
  const result = await failChannelCandidateWorkerJob({
    query: async () => {
      outsideQueryCalled = true;
      throw new Error("Candidate settlement must use the transaction client");
    },
    withTransaction: async (action) => action({
      async query(sql, params) {
        transactionStatements.push({ sql, params });
        if (sql.includes("UPDATE crawler.channel_candidates")) {
          return {
            rowCount: 1,
            rows: [{ candidate_id: 482, status: "failed", snapshot_dispatch_generation: 2 }],
          };
        }
        if (sql.includes("UPDATE crawler.migration_system_retry_items")) {
          return { rowCount: 1, rows: [{ system_retry_id: 801 }] };
        }
        throw new Error(`unexpected transaction SQL: ${sql}`);
      },
    }),
    job: {
      ...completedJob(),
      opts: { attempts: 1 },
      data: {
        ...completedJob().data,
        dispatch_batch_id: "legacy-results-canary",
      },
    },
    error: new Error("terminal channel business failure"),
    failure: {
      message: "terminal channel business failure",
      parserFailure: false,
      terminalChannel: null,
      businessRunBudgetTerminal: false,
      systemFailure: null,
      parserDetails: null,
      failureDecision: { retry_mode: "none" },
      permanentFailure: true,
    },
    refreshDispatchCandidateCounts: async () => {},
    signalReadyDiscoveryPageQualifications: async () => {},
    finishMigrationRetryIntent: async () => {},
  });

  assert.equal(outsideQueryCalled, false);
  assert.equal(result.terminal, true);
  assert.equal(result.resolved, 1);
  assert.equal(transactionStatements.length, 2);
  assert.match(transactionStatements[0].sql, /UPDATE crawler\.channel_candidates/);
  assert.match(transactionStatements[1].sql, /UPDATE crawler\.migration_system_retry_items/);
});

test("a system failure is durable under the current attempt Fence before the processor rejects", async () => {
  const statements = [];
  const error = Object.assign(new Error("lease changed after Candidate acceptance"), {
    code: "LEASE_CONFLICT",
    status: 409,
  });
  const job = {
    id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
    queueName: "youtube-channel-crawl",
    attemptsMade: 2,
    opts: { attempts: 3 },
    data: {
      candidate_id: 482,
      dispatch_generation: 1,
      dispatch_batch_id: "legacy-results-canary",
    },
  };

  await assert.rejects(
    runChannelCandidateWorkerJobWithDurableSettlement({
      query: async (sql, params) => {
        statements.push({ sql, params });
        return {
          rowCount: 1,
          rows: [{
            candidate_id: 482,
            status: "accepted",
            snapshot_active_job_id: job.id,
            snapshot_active_job_attempt: 3,
            system_retry_id: 801,
          }],
        };
      },
      job,
      execute: async () => { throw error; },
    }),
    (rejected) => rejected === error,
  );

  assert.equal(statements.length, 1);
  assert.match(statements[0].sql, /INSERT INTO crawler\.migration_system_retry_items/);
  assert.deepEqual(statements[0].params.slice(3, 8), [
    1,
    job.id,
    3,
    true,
    "legacy-results-canary",
  ]);
});

test("a successful Candidate processor clears the current attempt Fence before returning", async () => {
  let statement = null;
  const job = {
    id: "channel-snapshot__legacy-results-canary__UC0Noar__g2",
    queueName: "youtube-channel-crawl",
    attemptsMade: 1,
    opts: { attempts: 3 },
    data: {
      candidate_id: 482,
      dispatch_generation: 2,
      dispatch_batch_id: "legacy-results-canary",
    },
  };

  const result = await runChannelCandidateWorkerJobWithDurableSettlement({
    query: async (sql, params) => {
      statement = { sql, params };
      return { rowCount: 1, rows: [{ candidate_id: 482, resolved_count: 1 }] };
    },
    job,
    execute: async () => ({ accepted: true }),
  });

  assert.deepEqual(result, { accepted: true });
  assert.match(statement.sql, /WITH cleared_candidate AS/);
  assert.deepEqual(statement.params, [482, 2, job.id, 2, "job_completed"]);
});
