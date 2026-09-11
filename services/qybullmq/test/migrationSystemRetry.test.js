import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { retryMigrationSystemFailure } from "../src/migrationSystemRetry.js";

function completedScheduler() {
  return {
    rowCount: 1,
    rows: [{
      value_json: {
        status: "stopped",
        stop_reason: "pipeline_complete",
        pipeline_cycle_id: "legacy-results-canary",
      },
    }],
  };
}

test("controlled Migration mode exposes only the existing controlled write surfaces", async () => {
  const server = await readFile(new URL("../src/server.js", import.meta.url), "utf8");

  const guard = await readFile(new URL("../src/controlledMigrationGuard.js", import.meta.url), "utf8");
  assert.match(server, /app\.use\(controlledMigrationGuard/);
  assert.match(guard, /req\.path\.startsWith\(['"]\/api\/migration\/channels['"]\)/);
  assert.match(guard, /\^\\\/api\\\/migration\\\/system-retries/);
  assert.doesNotMatch(guard, /req\.path\.startsWith\("\/api\/migration\/"\)/);
});

test("controlled system retry allocates exactly one G+1 Outbox", async () => {
  const state = {
    retry: {
      system_retry_id: "801",
      migration_intent_id: "25",
      candidate_id: "482",
      failed_dispatch_batch_id: "legacy-results-canary",
      failed_dispatch_generation: "1",
      failed_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
      failed_job_attempt: 3,
      failure_code: "LEASE_CONFLICT",
      failure_category: "lease",
      failure_evidence: { failure_type: "retryable_system_failure" },
      status: "pending",
      retry_dispatch_generation: null,
      dispatch_batch_id: "legacy-results-canary",
      pipeline_cycle_id: "legacy-results-canary",
      channel_id: "UC0NoarYHkSxek05QDqhtoYw",
      channel_url: "https://www.youtube.com/channel/UC0NoarYHkSxek05QDqhtoYw",
      priority: 100,
      candidate_status: "failed",
      snapshot_dispatch_generation: "1",
      snapshot_active_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
      snapshot_active_job_attempt: 3,
      snapshot_json: { failure_type: "retryable_system_failure" },
      intent_dispatch_attempts: 1,
    },
    createdOutboxes: 0,
  };
  const lockOrder = [];
  const client = {
    async query(sql, params) {
      for (const resource of ["candidate", "retry", "intent"]) {
        if (sql.includes(`migration-system-retry-dispatch-lock:${resource}`)) {
          lockOrder.push(resource);
        }
      }
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return completedScheduler();
      }
      if (sql.includes("FROM crawler.migration_system_retry_items") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ ...state.retry }] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("snapshot_json")) {
        return { rowCount: 1, rows: [{ candidate_id: params[0] }] };
      }
      if (sql.includes("UPDATE crawler.migration_system_retry_items")) {
        state.retry = {
          ...state.retry,
          status: "dispatched",
          retry_dispatch_generation: params[1],
          snapshot_dispatch_generation: String(params[1]),
          candidate_status: "queued",
          snapshot_active_job_id: params[2],
          snapshot_active_job_attempt: 0,
          intent_dispatch_attempts: params[1],
        };
        return { rowCount: 1, rows: [{ ...state.retry }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };
  const allocateOutbox = async (_client, options) => {
    const created = state.createdOutboxes === 0;
    if (created) state.createdOutboxes += 1;
    assert.equal(options.expectedGeneration, 1);
    assert.equal(options.previousJobAttempt, 3);
    assert.equal(options.candidate.snapshot_dispatch_generation, 2);
    return {
      created,
      candidate: {
        ...options.candidate,
        status: "queued",
        snapshot_active_job_id: options.jobId,
        snapshot_active_job_attempt: 0,
      },
      outbox: {
        dispatch_id: "channel-snapshot-dispatch:482:g2:test",
        deterministic_job_id: options.jobId,
        payload_json: options.payload,
      },
    };
  };
  const dependencies = {
    systemRetryId: 801,
    withTransaction: (action) => action(client),
    allocateOutbox,
  };

  const first = await retryMigrationSystemFailure(dependencies);
  const repeated = await retryMigrationSystemFailure(dependencies);

  assert.equal(first.created, true);
  assert.equal(repeated.created, false);
  assert.equal(first.dispatch_generation, 2);
  assert.equal(repeated.dispatch_generation, 2);
  assert.equal(state.createdOutboxes, 1);
  assert.equal(state.retry.status, "dispatched");
  assert.deepEqual(lockOrder, [
    "candidate", "retry", "intent",
    "candidate", "retry", "intent",
  ]);
});

test("controlled system retry pins a historical unknown Batch before allocating G+1", async () => {
  const failedJobId = "channel-snapshot__legacy-results-canary__UC0Noar__g1";
  const state = {
    retry: {
      system_retry_id: "801",
      migration_intent_id: "25",
      candidate_id: "482",
      failed_dispatch_batch_id: null,
      failed_dispatch_generation: "1",
      failed_job_id: failedJobId,
      failed_job_attempt: 3,
      failure_code: "LEASE_CONFLICT",
      failure_category: "lease",
      failure_evidence: { failure_type: "retryable_system_failure" },
      status: "pending",
      retry_dispatch_generation: null,
      dispatch_batch_id: "legacy-results-canary",
      pipeline_cycle_id: "legacy-results-canary",
      channel_id: "UC0NoarYHkSxek05QDqhtoYw",
      channel_url: "https://www.youtube.com/channel/UC0NoarYHkSxek05QDqhtoYw",
      priority: 100,
      candidate_status: "failed",
      snapshot_dispatch_generation: "1",
      snapshot_active_job_id: failedJobId,
      snapshot_active_job_attempt: 3,
      snapshot_json: { failure_type: "retryable_system_failure" },
      intent_dispatch_attempts: 1,
    },
  };
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return completedScheduler();
      }
      if (sql.includes("FROM crawler.migration_system_retry_items") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ ...state.retry }] };
      }
      if (sql.includes("SET failed_dispatch_batch_id=$2")) {
        assert.deepEqual(params, [801, "legacy-results-canary", 1, failedJobId, 3]);
        state.retry.failed_dispatch_batch_id = params[1];
        return { rowCount: 1, rows: [{ failed_dispatch_batch_id: params[1] }] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("snapshot_json")) {
        return { rowCount: 1, rows: [{ candidate_id: 482 }] };
      }
      if (sql.includes("UPDATE crawler.migration_system_retry_items")) {
        state.retry.status = "dispatched";
        state.retry.retry_dispatch_generation = params[1];
        return { rowCount: 1, rows: [{ system_retry_id: 801 }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };
  let allocated = false;

  const result = await retryMigrationSystemFailure({
    systemRetryId: 801,
    withTransaction: (action) => action(client),
    allocateOutbox: async (_client, options) => {
      allocated = true;
      assert.equal(state.retry.failed_dispatch_batch_id, "legacy-results-canary");
      assert.equal(options.candidate.dispatch_batch_id, "legacy-results-canary");
      return {
        created: true,
        candidate: options.candidate,
        outbox: {
          dispatch_id: "channel-snapshot-dispatch:482:g2:test",
          deterministic_job_id: options.jobId,
          payload_json: options.payload,
          status: "pending",
        },
      };
    },
  });

  assert.equal(allocated, true);
  assert.equal(state.retry.failed_dispatch_batch_id, "legacy-results-canary");
  assert.equal(result.dispatch_generation, 2);
});

test("controlled retry accepts an Intent-fenced system failure after Candidate acceptance", async () => {
  const failedJobId = "channel-snapshot__legacy-results-canary__UC0Noar__g1";
  const row = {
    system_retry_id: "801",
    migration_intent_id: "25",
    candidate_id: "482",
    failed_dispatch_batch_id: "legacy-results-canary",
    failed_dispatch_generation: "1",
    failed_job_id: failedJobId,
    failed_job_attempt: 3,
    failure_code: "LEASE_CONFLICT",
    failure_category: "lease",
    failure_evidence: { failure_type: "retryable_system_failure" },
    status: "pending",
    retry_dispatch_generation: null,
    dispatch_batch_id: "legacy-results-canary",
    pipeline_cycle_id: "legacy-results-canary",
    channel_id: "UC0NoarYHkSxek05QDqhtoYw",
    channel_url: "https://www.youtube.com/channel/UC0NoarYHkSxek05QDqhtoYw",
    priority: 100,
    candidate_status: "accepted",
    snapshot_dispatch_generation: "1",
    snapshot_active_job_id: failedJobId,
    snapshot_active_job_attempt: 3,
    snapshot_json: { failure_type: "retryable_system_failure" },
    intent_dispatch_attempts: 1,
  };
  let allocatedCandidate = null;
  const client = {
    async query(sql) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return completedScheduler();
      }
      if (sql.includes("FROM crawler.migration_system_retry_items") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [row] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("snapshot_json")) {
        return { rowCount: 1, rows: [{ candidate_id: 482 }] };
      }
      if (sql.includes("UPDATE crawler.migration_system_retry_items")) {
        return { rowCount: 1, rows: [{ system_retry_id: 801 }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const result = await retryMigrationSystemFailure({
    systemRetryId: 801,
    withTransaction: (action) => action(client),
    allocateOutbox: async (_client, options) => {
      allocatedCandidate = options.candidate;
      return {
        created: true,
        candidate: { ...options.candidate, status: "accepted" },
        outbox: {
          dispatch_id: "channel-snapshot-dispatch:482:g2:test",
          deterministic_job_id: options.jobId,
          payload_json: options.payload,
          status: "pending",
        },
      };
    },
  });

  assert.equal(result.dispatch_generation, 2);
  assert.equal(allocatedCandidate.candidate_id, 482);
  assert.equal(allocatedCandidate.migration_intent_id, 25);
});

test("a dispatched retry rearms its exact sent G+1 Outbox without allocating G+2", async () => {
  const jobId = "channel-snapshot__legacy-results-canary__UC0NoarYHkSxek05QDqhtoYw__g2";
  const row = {
    system_retry_id: "801",
    migration_intent_id: "25",
    candidate_id: "482",
    failed_dispatch_batch_id: "legacy-results-canary",
    failed_dispatch_generation: "1",
    failed_job_id: "channel-snapshot__legacy-results-canary__UC0NoarYHkSxek05QDqhtoYw__g1",
    failed_job_attempt: 3,
    failure_code: "LEASE_CONFLICT",
    failure_category: "lease",
    failure_evidence: { failure_type: "retryable_system_failure" },
    status: "dispatched",
    retry_dispatch_generation: "2",
    dispatch_batch_id: "legacy-results-canary",
    pipeline_cycle_id: "legacy-results-canary",
    channel_id: "UC0NoarYHkSxek05QDqhtoYw",
    channel_url: "https://www.youtube.com/channel/UC0NoarYHkSxek05QDqhtoYw",
    priority: 100,
    candidate_status: "queued",
    snapshot_dispatch_generation: "2",
    snapshot_active_job_id: jobId,
    snapshot_active_job_attempt: 0,
    snapshot_json: {},
    intent_dispatch_attempts: 2,
  };
  const outbox = {
    dispatch_id: "channel-snapshot-dispatch:482:g2:test",
    aggregate_kind: "channel_snapshot",
    aggregate_id: "482",
    deterministic_job_id: jobId,
    intent_hash: "sha256:sent-g2",
    payload_json: { dispatch_generation: 2 },
    status: "sent",
    attempts: 1,
    sent_at: "2026-08-30T00:00:00.000Z",
  };
  let rearmed = false;
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return completedScheduler();
      }
      if (sql.includes("FROM crawler.migration_system_retry_items") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [row] };
      }
      if (sql.includes("UPDATE crawler.proxy_job_dispatch_outbox")) {
        assert.match(sql, /status='pending'/);
        assert.match(sql, /status='sent'/);
        assert.deepEqual(params, [outbox.dispatch_id, "482", 2, jobId, outbox.intent_hash]);
        rearmed = true;
        return {
          rowCount: 1,
          rows: [{ ...outbox, status: "pending", sent_at: null }],
        };
      }
      if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("snapshot_json")) {
        return { rowCount: 1, rows: [{ candidate_id: 482 }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };
  let allocations = 0;

  const result = await retryMigrationSystemFailure({
    systemRetryId: 801,
    withTransaction: (action) => action(client),
    allocateOutbox: async (_client, options) => {
      allocations += 1;
      assert.equal(options.expectedGeneration, 1);
      assert.equal(options.candidate.snapshot_dispatch_generation, 2);
      assert.equal(options.jobId, jobId);
      return { created: false, candidate: options.candidate, outbox };
    },
  });

  assert.equal(allocations, 1);
  assert.equal(rearmed, true);
  assert.equal(result.created, false);
  assert.equal(result.dispatch_generation, 2);
  assert.equal(result.outbox.status, "pending");
});

test("controlled retry rearms a dead G+1 Outbox without allocating G+2 or demoting acceptance", async () => {
  const state = {
    retry: {
      system_retry_id: "801",
      migration_intent_id: "25",
      candidate_id: "482",
      failed_dispatch_batch_id: "legacy-results-canary",
      failed_dispatch_generation: "1",
      failed_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
      failed_job_attempt: 3,
      failure_code: "OUTBOX_DELIVERY_EXHAUSTED",
      failure_category: "outbox",
      failure_evidence: { failure_type: "retryable_system_failure" },
      status: "pending",
      retry_dispatch_generation: "2",
      dispatch_batch_id: "legacy-results-canary",
      pipeline_cycle_id: "legacy-results-canary",
      channel_id: "UC0NoarYHkSxek05QDqhtoYw",
      channel_url: "https://www.youtube.com/channel/UC0NoarYHkSxek05QDqhtoYw",
      priority: 100,
      candidate_status: "accepted",
      snapshot_dispatch_generation: "2",
      snapshot_active_job_id: "channel-snapshot__legacy-results-canary__UC0NoarYHkSxek05QDqhtoYw__g2",
      snapshot_active_job_attempt: 0,
      snapshot_json: { failure_type: "retryable_system_failure" },
      intent_dispatch_attempts: 2,
    },
    outbox: {
      dispatch_id: "channel-snapshot-dispatch:482:g2:test",
      aggregate_kind: "channel_snapshot",
      aggregate_id: "482",
      deterministic_job_id: "channel-snapshot__legacy-results-canary__UC0NoarYHkSxek05QDqhtoYw__g2",
      intent_hash: "sha256:dead-g2",
      payload_json: { dispatch_generation: 2 },
      status: "dead",
      attempts: 8,
    },
    rearmedCandidate: false,
  };
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return completedScheduler();
      }
      if (sql.includes("FROM crawler.migration_system_retry_items") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ ...state.retry }] };
      }
      if (sql.includes("UPDATE crawler.proxy_job_dispatch_outbox")) {
        state.outbox = { ...state.outbox, status: "pending", attempts: 0 };
        return { rowCount: 1, rows: [{ ...state.outbox }] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("next_retry_at=NULL")) {
        assert.match(
          sql,
          /status=CASE WHEN status='accepted' THEN 'accepted' ELSE 'queued' END/,
        );
        assert.match(
          sql,
          /validation_finished_at=CASE WHEN status='accepted' THEN validation_finished_at ELSE NULL END/,
        );
        assert.match(sql, /status IN \('failed','accepted'\)/);
        const retainedOutboxFenceAccepted = sql.includes("snapshot_active_job_id=$3")
          && sql.includes("snapshot_active_job_attempt=0");
        if (!retainedOutboxFenceAccepted) return { rowCount: 0, rows: [] };
        state.rearmedCandidate = true;
        state.retry = {
          ...state.retry,
          candidate_status: "accepted",
          snapshot_active_job_id: params[2],
          snapshot_active_job_attempt: 0,
        };
        return { rowCount: 1, rows: [{ candidate_id: 482 }] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("snapshot_json")) {
        return {
          rowCount: state.rearmedCandidate ? 1 : 0,
          rows: state.rearmedCandidate ? [{ candidate_id: 482 }] : [],
        };
      }
      if (sql.includes("UPDATE crawler.migration_system_retry_items")) {
        state.retry = { ...state.retry, status: "dispatched" };
        return { rowCount: 1, rows: [{ ...state.retry }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };
  let allocations = 0;
  const result = await retryMigrationSystemFailure({
    systemRetryId: 801,
    withTransaction: (action) => action(client),
    allocateOutbox: async (_client, options) => {
      allocations += 1;
      assert.equal(options.expectedGeneration, 1);
      assert.equal(options.candidate.snapshot_dispatch_generation, 2);
      assert.equal(options.jobId, state.outbox.deterministic_job_id);
      return { created: false, candidate: options.candidate, outbox: { ...state.outbox } };
    },
  });

  assert.equal(allocations, 1);
  assert.equal(result.created, false);
  assert.equal(result.dispatch_generation, 2);
  assert.equal(state.outbox.status, "pending");
  assert.equal(state.outbox.attempts, 0);
  assert.equal(state.rearmedCandidate, true);
  assert.equal(state.retry.candidate_status, "accepted");
  assert.equal(state.retry.status, "dispatched");
});
