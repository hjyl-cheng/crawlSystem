import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { settleCompletedMigrationBatch } from "../src/migrationBatchCompletion.js";
import { schedulerConflict } from "../src/manualMigrationDispatch.js";

test("99 accepted and one system failure settle atomically and release the next Batch", async () => {
  const batchId = "legacy-results-canary-99-plus-1";
  const completedAt = "2026-08-30T12:00:00.000Z";
  const state = {
    scheduler: { status: "finishing", pipeline_cycle_id: batchId },
    batch: null,
  };
  const candidates = [
    ...Array.from({ length: 99 }, (_, index) => ({
      candidate_id: index + 1,
      status: "accepted",
      snapshot_json: {},
    })),
    {
      candidate_id: 100,
      dispatch_batch_id: batchId,
      status: "failed",
      has_system_failure: true,
      has_active_system_retry: true,
      snapshot_json: {
        failure_type: "retryable_system_failure",
        failed_dispatch_batch_id: batchId,
        system_failure: { code: "LEASE_CONFLICT", category: "lease" },
      },
    },
  ];
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ value_json: { ...state.scheduler } }] };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return { rowCount: candidates.length, rows: candidates };
      }
      if (sql.includes("UPDATE crawler.query_dispatch_batches")) {
        state.batch = {
          dispatch_batch_id: params[0],
          status: params[1],
          outcome: params[2],
          total_channel_count: params[3],
          accepted_channel_count: params[4],
          rejected_channel_count: params[5],
          failed_channel_count: params[6],
        };
        return { rowCount: 1, rows: [{ ...state.batch }] };
      }
      if (sql.includes("UPDATE crawler.settings")) {
        state.scheduler = { ...state.scheduler, ...JSON.parse(params[1]) };
        return { rowCount: 1, rows: [{ value_json: { ...state.scheduler } }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const completion = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId,
    completedAt,
    schedulerMetadata: {
      parser_failure_counts: { channel: 0, content: 0, total: 0 },
      status: "must-not-override-completion",
    },
  });

  assert.deepEqual(state.batch, {
    dispatch_batch_id: batchId,
    status: "completed",
    outcome: "completed_with_system_failures",
    total_channel_count: 100,
    accepted_channel_count: 99,
    rejected_channel_count: 0,
    failed_channel_count: 1,
  });
  assert.equal(completion.status, "completed");
  assert.equal(completion.outcome, "completed_with_system_failures");
  assert.equal(state.scheduler.status, "stopped");
  assert.deepEqual(state.scheduler.parser_failure_counts, {
    channel: 0,
    content: 0,
    total: 0,
  });
  assert.equal(schedulerConflict(state.scheduler, "legacy-results-canary-next"), null);
});

test("a resolved system retry still preserves the accepted Candidate Batch outcome", async () => {
  const batchId = "legacy-results-canary-accepted-system-failure";
  const candidate = {
    candidate_id: 482,
    status: "accepted",
    snapshot_json: {},
    has_system_failure: true,
  };
  let batchParams = null;
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{ value_json: { status: "finishing", pipeline_cycle_id: batchId } }],
        };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        assert.match(
          sql,
          /EXISTS \([\s\S]*FROM crawler\.migration_system_retry_items retry[\s\S]*\) AS has_system_failure/,
        );
        assert.match(
          sql,
          /retry\.failed_dispatch_batch_id=candidate\.dispatch_batch_id/,
        );
        return { rowCount: 1, rows: [candidate] };
      }
      if (sql.includes("UPDATE crawler.query_dispatch_batches")) {
        batchParams = params;
        return { rowCount: 1, rows: [{ dispatch_batch_id: batchId }] };
      }
      if (sql.includes("UPDATE crawler.settings")) {
        return { rowCount: 1, rows: [{ value_json: JSON.parse(params[1]) }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const completion = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId,
  });

  assert.equal(completion.status, "completed");
  assert.equal(completion.outcome, "completed_with_system_failures");
  assert.deepEqual({
    total: completion.total,
    accepted: completion.accepted,
    rejected: completion.rejected,
    failed: completion.failed,
  }, { total: 1, accepted: 1, rejected: 0, failed: 0 });
  assert.deepEqual(batchParams.slice(1, 7), [
    "completed",
    "completed_with_system_failures",
    1,
    1,
    0,
    0,
  ]);
});

test("historical retry and snapshot evidence cannot contaminate a later Batch outcome", async () => {
  const batchId = "legacy-results-canary-later-batch";
  let batchOutcome = null;
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{ value_json: { status: "finishing", pipeline_cycle_id: batchId } }],
        };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        assert.match(
          sql,
          /retry\.failed_dispatch_batch_id=candidate\.dispatch_batch_id/,
        );
        return {
          rowCount: 1,
          rows: [{
            candidate_id: 482,
            dispatch_batch_id: batchId,
            status: "accepted",
            snapshot_json: {
              failure_type: "retryable_system_failure",
              failed_dispatch_batch_id: "legacy-results-canary-earlier-batch",
              system_failure: { code: "LEASE_CONFLICT", category: "lease" },
            },
            has_system_failure: false,
          }],
        };
      }
      if (sql.includes("UPDATE crawler.query_dispatch_batches")) {
        batchOutcome = params[2];
        return { rowCount: 1, rows: [{ dispatch_batch_id: batchId }] };
      }
      if (sql.includes("UPDATE crawler.settings")) {
        return { rowCount: 1, rows: [{ value_json: JSON.parse(params[1]) }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const completion = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId,
  });

  assert.equal(batchOutcome, "completed");
  assert.equal(completion.outcome, "completed");
});

test("Migration Batch completion rechecks Candidate terminal state inside its transaction", async () => {
  const calls = [];
  const client = {
    async query(sql) {
      calls.push(sql);
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            value_json: {
              status: "finishing",
              pipeline_cycle_id: "legacy-results-canary-open-candidate",
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 2,
          rows: [
            { candidate_id: 481, status: "accepted", snapshot_json: {} },
            { candidate_id: 482, status: "queued", snapshot_json: {} },
          ],
        };
      }
      throw new Error(`completion must not continue after an open Candidate: ${sql}`);
    },
  };

  const completion = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId: "legacy-results-canary-open-candidate",
  });

  assert.equal(completion, null);
  assert.equal(calls.some((sql) => sql.includes("UPDATE crawler.query_dispatch_batches")), false);
  assert.equal(calls.some((sql) => sql.includes("UPDATE crawler.settings")), false);
});

test("a resolved historical system failure cannot hide a retry-eligible failed Candidate", async () => {
  const batchId = "legacy-results-canary-awaiting-g-plus-one";
  let batchUpdated = false;
  let schedulerUpdated = false;
  let snapshotAttempts = 1;
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{ value_json: { status: "finishing", pipeline_cycle_id: batchId } }],
        };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            candidate_id: 482,
            dispatch_batch_id: batchId,
            status: "failed",
            snapshot_attempts: snapshotAttempts,
            snapshot_json: {},
            snapshot_active_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
            snapshot_active_job_attempt: 1,
            has_system_failure: true,
            has_active_system_retry: false,
          }],
        };
      }
      if (sql.includes("UPDATE crawler.query_dispatch_batches")) {
        batchUpdated = true;
        return {
          rowCount: 1,
          rows: [{
            dispatch_batch_id: params[0],
            status: params[1],
            outcome: params[2],
            total_channel_count: params[3],
            accepted_channel_count: params[4],
            rejected_channel_count: params[5],
            failed_channel_count: params[6],
          }],
        };
      }
      if (sql.includes("UPDATE crawler.settings")) {
        schedulerUpdated = true;
        return { rowCount: 1, rows: [{ value_json: JSON.parse(params[1]) }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const completion = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId,
    maxSnapshotAttempts: 6,
  });

  assert.equal(completion, null);
  assert.equal(batchUpdated, false);
  assert.equal(schedulerUpdated, false);

  snapshotAttempts = 6;
  const exhausted = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId,
    maxSnapshotAttempts: 6,
  });

  assert.equal(exhausted.status, "completed");
  assert.equal(exhausted.failed, 1);
  assert.equal(batchUpdated, true);
  assert.equal(schedulerUpdated, true);
});

test("an accepted Candidate with an active Fence cannot settle before system failure evidence", async () => {
  const calls = [];
  const client = {
    async query(sql) {
      calls.push(sql);
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            value_json: {
              status: "finishing",
              pipeline_cycle_id: "legacy-results-canary-persistence-window",
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            candidate_id: 482,
            status: "accepted",
            snapshot_json: {},
            snapshot_active_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
            snapshot_active_job_attempt: 1,
            has_system_failure: false,
          }],
        };
      }
      throw new Error(`completion must wait for durable failure evidence: ${sql}`);
    },
  };

  const completion = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId: "legacy-results-canary-persistence-window",
  });

  assert.equal(completion, null);
  assert.equal(calls.some((sql) => sql.includes("UPDATE crawler.query_dispatch_batches")), false);
});

test("Migration Batch completion cannot overwrite a stopped or failed Batch", async () => {
  const client = {
    async query(sql) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            value_json: {
              status: "finishing",
              pipeline_cycle_id: "legacy-results-canary-stopped",
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{ candidate_id: 482, status: "existing", snapshot_json: {} }],
        };
      }
      if (sql.includes("UPDATE crawler.query_dispatch_batches")) {
        assert.match(
          sql,
          /status IN \('running','discovery_closed','validation_closed','finishing'\)/,
        );
        return { rowCount: 0, rows: [] };
      }
      throw new Error(`scheduler must remain unchanged when the Batch Fence rejects completion: ${sql}`);
    },
  };

  const completion = await settleCompletedMigrationBatch({
    withTransaction: (action) => action(client),
    batchId: "legacy-results-canary-stopped",
  });

  assert.equal(completion, null);
});

test("completed Migration Batch statistics are fenced from later Candidate refreshes", async () => {
  const sources = await Promise.all([
    readFile(new URL("../src/worker.js", import.meta.url), "utf8"),
    readFile(new URL("../src/pipelineV2.js", import.meta.url), "utf8"),
    readFile(new URL("../src/manualMigrationDispatch.js", import.meta.url), "utf8"),
  ]);
  for (const source of sources) {
    assert.match(
      source,
      /UPDATE crawler\.query_dispatch_batches batch[\s\S]{0,700}WHERE batch\.dispatch_batch_id=\$1\s+AND batch\.status<>'completed'/,
    );
  }
});
