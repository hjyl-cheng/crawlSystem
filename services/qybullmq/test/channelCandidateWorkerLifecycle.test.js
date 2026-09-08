import assert from "node:assert/strict";
import test from "node:test";

test("an exhausted About-only repair does not invalidate completed video work", async () => {
  const value = completedJob();
  value.opts = { attempts: 1 };
  Object.assign(value.data, { run_id: "run:about", publication_gap_root_run_id: "run:about",
    publication_gap_domains: ["channel"], publication_gap_scope: "about_only",
    require_complete_about_metrics: true });
  const result = await failChannelCandidateWorkerJob({
    query: async () => assert.fail("must use transaction"),
    withTransaction: async action => action({ query: async sql => {
      assert.equal(sql.includes("UPDATE crawler.channel_runs"), false);
      if (sql.includes("UPDATE crawler.channel_candidates")) return {
        rowCount: 1, rows: [{ candidate_id: value.data.candidate_id, snapshot_active_job_id: null,
          snapshot_active_job_attempt: null }],
      };
      return { rowCount: 0, rows: [] };
    } }),
    job: value, error: new Error("About metrics incomplete"),
    failure: { message: "About metrics incomplete", terminalChannel: null,
      businessRunBudgetTerminal: false, systemFailure: null, parserDetails: null,
      permanentFailure: true },
    refreshDispatchCandidateCounts: async () => {},
    signalReadyDiscoveryPageQualifications: async () => {},
    finishMigrationRetryIntent: async () => {},
  });
  assert.equal(result.terminal, true);
  assert.equal(result.runFailureRecorded, false);
});

import {
  completeChannelCandidateWorkerJob,
  failChannelCandidateWorkerJob,
  runChannelCandidateWorkerJobWithDurableSettlement,
} from "../src/channelCandidateWorkerLifecycle.js";

function completedJob() {
  return {
    id: "channel-snapshot__legacy-results-canary__UC0Noar__g2",
    attemptsMade: 1,
    attemptsStarted: 1,
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
  assert.doesNotMatch(calls[0].sql, /retry_dispatch_generation/);
  assert.match(calls[0].sql, /FROM cleared_candidate/);
  assert.deepEqual(calls[0].params, [
    482,
    2,
    completedJob().id,
    1,
    "job_completed",
  ]);
});

test("Worker completion clears Candidate and finishes Retry Intent in one transaction", async () => {
  const job = {
    ...completedJob(),
    data: {
      ...completedJob().data,
      retry_intent_id: "retry-intent-current-attempt",
    },
  };
  const transactionStatements = [];
  let outsideQueryCalled = false;
  const result = await completeChannelCandidateWorkerJob(
    async () => {
      outsideQueryCalled = true;
      throw new Error("completion must use the transaction client");
    },
    job,
    {
      withTransaction: async (action) => action({
        async query(sql, params) {
          transactionStatements.push({ sql, params });
          if (sql === "finish retry intent") {
            return { rowCount: 1, rows: [{ retry_intent_id: job.data.retry_intent_id }] };
          }
          return { rowCount: 1, rows: [{ candidate_id: 482, resolved_count: 1 }] };
        },
      }),
      finishMigrationRetryIntent: async (transactionQuery) => {
        const finished = await transactionQuery("finish retry intent", []);
        return finished.rowCount === 1;
      },
    },
  );

  assert.equal(outsideQueryCalled, false);
  assert.deepEqual(result, { cleared: true, resolved: 1, intentFinished: true });
  assert.equal(transactionStatements.length, 2);
  assert.match(transactionStatements[0].sql, /WITH cleared_candidate AS/);
  assert.equal(transactionStatements[1].sql, "finish retry intent");
});

test("stale completed attempt cannot finish its Retry Intent", async () => {
  const job = {
    ...completedJob(),
    data: {
      ...completedJob().data,
      retry_intent_id: "retry-intent-newer-attempt",
    },
  };
  let finishCalls = 0;
  const result = await completeChannelCandidateWorkerJob(
    async () => { throw new Error("completion must use the transaction client"); },
    job,
    {
      withTransaction: async (action) => action({
        query: async () => ({ rowCount: 0, rows: [] }),
      }),
      finishMigrationRetryIntent: async () => {
        finishCalls += 1;
        return true;
      },
    },
  );

  assert.deepEqual(result, { cleared: false, resolved: 0, intentFinished: false });
  assert.equal(finishCalls, 0);
});

test("Retry Intent fence rejection aborts Candidate completion", async () => {
  const job = {
    ...completedJob(),
    data: {
      ...completedJob().data,
      retry_intent_id: "retry-intent-conflict",
    },
  };
  await assert.rejects(
    completeChannelCandidateWorkerJob(
      async () => { throw new Error("completion must use the transaction client"); },
      job,
      {
        withTransaction: async (action) => action({
          query: async () => ({
            rowCount: 1,
            rows: [{ candidate_id: 482, resolved_count: 1 }],
          }),
        }),
        finishMigrationRetryIntent: async () => false,
      },
    ),
    (error) => error?.code === "MIGRATION_RETRY_INTENT_FENCE_STALE",
  );
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
        if (sql.includes("UPDATE crawler.channel_runs")) {
          return { rowCount: 1, rows: [{ run_id: "run:terminal-business-failure" }] };
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
        run_id: "run:terminal-business-failure",
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
  assert.equal(result.runFailureRecorded, true);
  assert.equal(transactionStatements.length, 3);
  assert.match(transactionStatements[0].sql, /UPDATE crawler\.channel_candidates/);
  assert.match(transactionStatements[1].sql, /UPDATE crawler\.migration_system_retry_items/);
  assert.match(transactionStatements[2].sql, /UPDATE crawler\.channel_runs/);
  assert.match(transactionStatements[2].sql, /candidate_id=\$4/);
});

test("parser failure writes Candidate evidence and Run failure under one exact attempt Fence", async () => {
  const statements = [];
  const job = {
    ...completedJob(),
    opts: { attempts: 1 },
    data: {
      ...completedJob().data,
      run_id: "run:parser-current",
      dispatch_batch_id: "legacy-results-canary",
    },
  };
  const result = await failChannelCandidateWorkerJob({
    query: async () => ({ rowCount: 0, rows: [] }),
    withTransaction: async (action) => action({
      async query(sql, params) {
        statements.push({ sql, params });
        if (sql.includes("SET status=CASE WHEN status='accepted'")) {
          return { rowCount: 1, rows: [{ candidate_id: 482, status: "accepted" }] };
        }
        if (sql.includes("SET snapshot_active_job_id=NULL")) {
          return { rowCount: 1, rows: [{ candidate_id: 482 }] };
        }
        if (sql.includes("UPDATE crawler.migration_system_retry_items")) {
          return { rowCount: 0, rows: [] };
        }
        if (sql.includes("UPDATE crawler.channel_runs")) {
          return { rowCount: 1, rows: [{ run_id: job.data.run_id }] };
        }
        throw new Error(`unexpected transaction SQL: ${sql}`);
      },
    }),
    job,
    error: new Error("localized parser contract failed"),
    failure: {
      message: "localized parser contract failed",
      parserFailure: true,
      terminalChannel: null,
      businessRunBudgetTerminal: false,
      systemFailure: null,
      parserDetails: { contract: "channel_about", selector: "title" },
      failureDecision: { retry_mode: "none" },
      permanentFailure: true,
    },
    refreshDispatchCandidateCounts: async () => {},
    signalReadyDiscoveryPageQualifications: async () => {},
    finishMigrationRetryIntent: async () => {},
  });

  assert.deepEqual(result.settlement, { recorded: true, fenceCleared: true });
  assert.equal(result.runFailureRecorded, true);
  assert.equal(statements.length, 4);
  assert.match(statements[0].sql, /snapshot_json=.*parser_contract_error/s);
  assert.match(statements[1].sql, /snapshot_active_job_id=NULL/);
  assert.match(statements[2].sql, /migration_system_retry_items/);
  assert.match(statements[3].sql, /channel_runs/);
});

test("stale parser failure cannot write either Candidate evidence or Run failure", async () => {
  const statements = [];
  const result = await failChannelCandidateWorkerJob({
    query: async () => ({ rowCount: 0, rows: [] }),
    withTransaction: async (action) => action({
      async query(sql, params) {
        statements.push({ sql, params });
        return { rowCount: 0, rows: [] };
      },
    }),
    job: {
      ...completedJob(),
      opts: { attempts: 1 },
      data: {
        ...completedJob().data,
        run_id: "run:parser-current",
        dispatch_batch_id: "legacy-results-canary",
      },
    },
    error: new Error("stale localized parser contract failed"),
    failure: {
      message: "stale localized parser contract failed",
      parserFailure: true,
      terminalChannel: null,
      businessRunBudgetTerminal: false,
      systemFailure: null,
      parserDetails: { contract: "channel_about", selector: "title" },
      failureDecision: { retry_mode: "none" },
      permanentFailure: true,
    },
    refreshDispatchCandidateCounts: async () => {},
    signalReadyDiscoveryPageQualifications: async () => {},
    finishMigrationRetryIntent: async () => {},
  });

  assert.deepEqual(result.settlement, { recorded: false, fenceCleared: false });
  assert.equal(result.runFailureRecorded, false);
  assert.equal(statements.length, 1);
  assert.match(statements[0].sql, /crawler.channel_candidates/);
});

test("a stale terminal attempt cannot fail its Migration Retry Intent", async () => {
  let finishCalls = 0;
  const job = {
    ...completedJob(),
    opts: { attempts: 1 },
    data: {
      ...completedJob().data,
      retry_intent_id: "retry-intent-stale-attempt",
      dispatch_batch_id: "legacy-results-canary",
      run_id: "run:stale-retry-intent-attempt",
    },
  };
  const result = await failChannelCandidateWorkerJob({
    query: async () => ({ rowCount: 0, rows: [] }),
    withTransaction: async (action) => action({
      async query() {
        return { rowCount: 0, rows: [] };
      },
    }),
    job,
    error: new Error("stale terminal parser failure"),
    failure: {
      message: "stale terminal parser failure",
      parserFailure: true,
      terminalChannel: null,
      businessRunBudgetTerminal: false,
      systemFailure: null,
      parserDetails: { contract: "channel_about", selector: "title" },
      failureDecision: { retry_mode: "none" },
      permanentFailure: true,
    },
    refreshDispatchCandidateCounts: async () => {},
    signalReadyDiscoveryPageQualifications: async () => {},
    finishMigrationRetryIntent: async () => {
      finishCalls += 1;
      return true;
    },
  });

  assert.deepEqual(result.settlement, { recorded: false, fenceCleared: false });
  assert.equal(result.terminal, true);
  assert.equal(finishCalls, 0);
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
    attemptsStarted: 3,
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
    attemptsStarted: 2,
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
