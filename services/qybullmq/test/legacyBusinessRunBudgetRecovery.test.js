import assert from "node:assert/strict";
import test from "node:test";

import {
  LEGACY_BUDGET_EXHAUSTION_REASON,
  recoverLegacyBusinessRunBudgetEvidence,
} from "../src/legacyBusinessRunBudgetRecovery.js";

const runId = "run:legacy-budget";
const jobId = "final-repair__run_legacy-budget__3";

function failedJob(overrides = {}) {
  return {
    id: jobId,
    name: "channel-crawl-repair",
    queueName: "youtube-channel-crawl",
    data: {
      run_id: runId,
      repair_parent_run_id: runId,
      repair_round: 3,
    },
    attemptsMade: 3,
    failedReason: LEGACY_BUDGET_EXHAUSTION_REASON,
    async getState() { return "failed"; },
    ...overrides,
  };
}

function databaseQuery({ alreadyRecorded = false } = {}) {
  const calls = [];
  const query = async (sql, params) => {
    calls.push({ sql, params });
    if (/SELECT run_id/.test(sql)) {
      return {
        rowCount: 1,
        rows: [{
          run_id: runId,
          status: "failed",
          detail_status: "failed",
          result_json: {
            final_repair: { job_id: jobId, rounds: 3 },
            ...(alreadyRecorded
              ? { proxy_control: { status: "business_run_budget_exhausted" } }
              : {}),
          },
        }],
      };
    }
    if (/SELECT business_run_key/.test(sql)) {
      return {
        rowCount: 1,
        rows: [{
          business_run_key: "full-channel:legacy-budget",
          business_run_id: runId,
          status: "materialized",
          terminal_reason: null,
          channel_id: null,
          candidate_id: null,
        }],
      };
    }
    if (/UPDATE crawler\.channel_runs/.test(sql)) {
      return { rowCount: 1, rows: [{ run_id: params[0] }] };
    }
    if (/UPDATE crawler\.business_run_bindings/.test(sql)) {
      return {
        rowCount: 1,
        rows: [{
          business_run_key: params[0],
          business_run_id: params[1],
          status: "terminal",
          terminal_reason: params[2],
        }],
      };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  };
  return {
    calls,
    query,
    withTransaction: (action) => action({ query }),
  };
}

test("legacy budget evidence dry-run validates without changing the Run", async () => {
  const db = databaseQuery();
  const result = await recoverLegacyBusinessRunBudgetEvidence({
    query: db.query,
    queue: { async getJob(id) { assert.equal(id, jobId); return failedJob(); } },
    runId,
    jobId,
    apply: false,
  });

  assert.equal(result.action, "record_budget_exhaustion");
  assert.equal(result.applied, false);
  assert.equal(db.calls.length, 1);
});

test("legacy budget evidence is persisted only after exact validation", async () => {
  const db = databaseQuery();
  const result = await recoverLegacyBusinessRunBudgetEvidence({
    query: db.query,
    withTransaction: db.withTransaction,
    queue: { async getJob() { return failedJob(); } },
    runId,
    jobId,
    apply: true,
  });

  assert.equal(result.action, "recorded_budget_exhaustion");
  assert.equal(result.applied, true);
  assert.equal(db.calls.length, 5);
  const runUpdate = db.calls.find(({ sql }) => /UPDATE crawler\.channel_runs/.test(sql));
  assert.ok(runUpdate);
  assert.equal(
    JSON.parse(runUpdate.params[1]).source,
    "bullmq_failed_job_reconciliation",
  );
  assert.equal(
    db.calls.some(({ sql }) => /UPDATE crawler\.business_run_bindings/.test(sql)),
    true,
  );
});

test("legacy recovery rejects mismatched or non-budget BullMQ evidence", async () => {
  const cases = [
    failedJob({ id: "different-job" }),
    failedJob({ failedReason: "ordinary failure" }),
    failedJob({ data: { run_id: "run:other", repair_parent_run_id: runId, repair_round: 3 } }),
    failedJob({ async getState() { return "waiting"; } }),
  ];

  for (const job of cases) {
    const db = databaseQuery();
    await assert.rejects(
      recoverLegacyBusinessRunBudgetEvidence({
        query: db.query,
        queue: { async getJob() { return job; } },
        runId,
        jobId,
        apply: true,
      }),
      /legacy budget evidence/i,
    );
    assert.equal(db.calls.length, 0);
  }
});

test("already-recorded legacy evidence is idempotent", async () => {
  const db = databaseQuery({ alreadyRecorded: true });
  const result = await recoverLegacyBusinessRunBudgetEvidence({
    query: db.query,
    queue: { async getJob() { return failedJob(); } },
    runId,
    jobId,
    apply: true,
  });

  assert.equal(result.action, "already_recorded");
  assert.equal(result.applied, false);
  assert.equal(db.calls.length, 1);
});
