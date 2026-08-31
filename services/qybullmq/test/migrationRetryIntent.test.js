import assert from "node:assert/strict";
import test from "node:test";

import {
  InMemoryMigrationRetryIntentRepository,
  enterMigrationRetryIntentWorkerJob,
  MigrationRetryIntentConflictError,
  MigrationRetryIntentJobReconciler,
  MigrationRetryIntentStore,
  finishMigrationRetryIntent,
  markMigrationRetryIntentRunning,
} from "../src/migrationRetryIntent.js";

function fixture() {
  const repository = new InMemoryMigrationRetryIntentRepository({
    candidates: [{
      candidate_id: 42,
      channel_id: "UC1234567890123456789012",
      channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
      dispatch_batch_id: "legacy-results-manual-v1",
      pipeline_cycle_id: "legacy-results-manual-v1",
      priority: 100,
      status: "failed",
      snapshot_dispatch_generation: 4,
      source_json: { source: "legacy_results_db" },
    }],
    bindings: [{
      business_run_key: "full-candidate:42",
      business_run_id: "run:old",
      candidate_id: 42,
      channel_id: "UC1234567890123456789012",
      status: "terminal",
      terminal_reason: "proxy_control_business_run_budget_exhausted",
    }],
  });
  const uuids = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
  ];
  const store = new MigrationRetryIntentStore({
    repository,
    randomUUID: () => uuids.shift(),
    now: () => new Date("2026-08-26T12:00:00.000Z"),
  });
  return { repository, store };
}

test("a Recovery Intent creates a fresh budget and dispatch boundary from whitelisted data", async () => {
  const { repository, store } = fixture();
  const result = await store.prepare({
    requestKey: "operator-ticket-20260826-candidate-42",
    candidateId: 42,
    previousBusinessRunId: "run:old",
    reason: "controlled retry after proxy fixes",
  });

  assert.equal(result.created, true);
  assert.equal(result.intent.retry_intent_id, "11111111-1111-4111-8111-111111111111");
  assert.equal(result.intent.new_business_run_id, "run:22222222-2222-4222-8222-222222222222");
  assert.equal(
    result.intent.new_business_run_key,
    "full-candidate:42:recovery:11111111-1111-4111-8111-111111111111",
  );
  assert.equal(result.intent.dispatch_generation, 5);
  assert.match(result.intent.new_job_id, /channel-recovery__42__11111111-1111-4111-8111-111111111111__g5/);
  assert.deepEqual(Object.keys(result.outbox.payload_json).sort(), [
    "candidate_id",
    "channel_id",
    "channel_url",
    "crawl_mode",
    "dispatch_batch_id",
    "dispatch_generation",
    "enforce_min_subscribers",
    "min_subscriber_count",
    "pipeline_cycle_id",
    "query_id",
    "query_text",
    "recovery_business_run_id",
    "recovery_reason",
    "reject_if_no_recent_content",
    "retry_intent_id",
  ]);
  assert.equal(result.outbox.payload_json.run_id, undefined);
  assert.equal(result.outbox.payload_json.business_run_key, undefined);
  assert.equal(result.outbox.payload_json.full_intent_id, undefined);
  assert.equal(repository.candidates.get(42).snapshot_dispatch_generation, 5);
  assert.equal(repository.candidates.get(42).status, "queued");
  assert.equal(repository.bindings.get("run:old").status, "terminal");
  assert.equal(repository.outbox.size, 1);
});

test("the same request key replays one Intent while conflicting reuse fails closed", async () => {
  const { repository, store } = fixture();
  const input = {
    requestKey: "operator-ticket-20260826-candidate-42",
    candidateId: 42,
    previousBusinessRunId: "run:old",
    reason: "controlled retry after proxy fixes",
  };
  const first = await store.prepare(input);
  const replay = await store.prepare(input);

  assert.equal(replay.created, false);
  assert.deepEqual(replay.intent, first.intent);
  assert.equal(repository.candidates.get(42).snapshot_dispatch_generation, 5);
  assert.equal(repository.outbox.size, 1);
  await assert.rejects(
    store.prepare({ ...input, reason: "different reason" }),
    MigrationRetryIntentConflictError,
  );
});

test("a request key cannot replay with a different subscriber threshold", async () => {
  const { store } = fixture();
  const input = {
    requestKey: "operator-ticket-20260826-candidate-42",
    candidateId: 42,
    previousBusinessRunId: "run:old",
    reason: "controlled retry after proxy fixes",
    minSubscriberCount: 1000,
  };
  await store.prepare(input);

  await assert.rejects(
    store.prepare({ ...input, minSubscriberCount: 2000 }),
    MigrationRetryIntentConflictError,
  );
});

test("an Intent without its transactional Outbox record fails closed", async () => {
  const { repository, store } = fixture();
  const input = {
    requestKey: "operator-ticket-20260826-candidate-42",
    candidateId: 42,
    previousBusinessRunId: "run:old",
    reason: "controlled retry after proxy fixes",
  };
  await store.prepare(input);
  repository.outbox.clear();

  await assert.rejects(
    store.prepare(input),
    MigrationRetryIntentConflictError,
  );
});

test("Worker lifecycle updates are fenced by Intent, Job, and dispatch generation", async () => {
  const calls = [];
  const query = async (sql, params) => {
    calls.push({ sql, params });
    return { rowCount: 1, rows: [{ retry_intent_id: "intent-1" }] };
  };
  const job = {
    id: "channel-recovery__42__intent-1__g5",
    attemptsStarted: 2,
    data: { retry_intent_id: "intent-1", dispatch_generation: 5 },
  };

  assert.equal(await markMigrationRetryIntentRunning(query, job), true);
  assert.equal(await finishMigrationRetryIntent(query, job, { outcome: "finished" }), true);
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].params, ["intent-1", job.id, 5]);
  assert.match(calls[1].sql, /terminal_job_attempt=COALESCE\(terminal_job_attempt,\$6\)/);
  assert.match(calls[1].sql, /status=\$4 AND terminal_job_attempt=\$6/);
  assert.deepEqual(calls[1].params, ["intent-1", job.id, 5, "finished", null, 2]);
});

test("Worker entry replays only the exact finished Intent identity without executing", async () => {
  const calls = [];
  const job = {
    id: "channel-recovery__42__intent-1__g5",
    attemptsStarted: 3,
    data: { retry_intent_id: "intent-1", dispatch_generation: 5 },
  };
  const result = await enterMigrationRetryIntentWorkerJob(async (sql, params) => {
    calls.push({ sql, params });
    if (sql.includes("UPDATE crawler.migration_retry_intents")) {
      return { rowCount: 0, rows: [] };
    }
    return { rowCount: 1, rows: [{ terminal_job_attempt: "2" }] };
  }, job);

  assert.deepEqual(result, { action: "finished_replay", terminalJobAttempt: 2 });
  assert.equal(calls.length, 2);
  assert.match(calls[1].sql, /retry_intent_id=\$1 AND new_job_id=\$2 AND dispatch_generation=\$3/);
  assert.match(calls[1].sql, /terminal_job_attempt<=\$4/);
  assert.deepEqual(calls[1].params, ["intent-1", job.id, 5, 3]);
});

test("Worker entry fails closed when the finished Intent identity is not exact", async () => {
  const result = await enterMigrationRetryIntentWorkerJob(async () => ({
    rowCount: 0,
    rows: [],
  }), {
    id: "channel-recovery__42__wrong-job__g5",
    attemptsStarted: 3,
    data: { retry_intent_id: "intent-1", dispatch_generation: 5 },
  });

  assert.deepEqual(result, { action: "rejected" });
});

test("terminal BullMQ Jobs replay a missed Recovery Intent lifecycle event", async () => {
  const terminal = [
    {
      retry_intent_id: "intent-completed",
      new_job_id: "channel-recovery__42__intent-completed__g5",
      dispatch_generation: 5,
      state: "completed",
    },
    {
      retry_intent_id: "intent-failed",
      new_job_id: "channel-recovery__43__intent-failed__g6",
      dispatch_generation: 6,
      state: "failed",
      failedReason: "snapshot attempts exhausted",
    },
  ];
  const finished = [];
  const repository = {
    async loadActive() {
      return terminal.map(({ state, failedReason, ...intent }) => intent);
    },
    async markRunning() { return true; },
    async finish(intent, outcome) {
      finished.push({ intent, ...outcome });
      return true;
    },
  };
  const jobs = new Map(terminal.map((item) => [item.new_job_id, {
    id: item.new_job_id,
    attemptsStarted: 2,
    data: {
      retry_intent_id: item.retry_intent_id,
      dispatch_generation: item.dispatch_generation,
    },
    failedReason: item.failedReason,
    async getState() { return item.state; },
  }]));
  const reconciler = new MigrationRetryIntentJobReconciler({
    repository,
    queue: { getJob: async (jobId) => jobs.get(jobId) ?? null },
  });

  const result = await reconciler.reconcileAvailable({ limit: 10 });

  assert.deepEqual(result, { scanned: 2, finished: 1, failed: 1, pending: 0, missing: 0 });
  assert.deepEqual(finished.map(({ intent, outcome, error }) => ({
    retry_intent_id: intent.retry_intent_id,
    outcome,
    error: error?.message ?? null,
  })), [
    { retry_intent_id: "intent-completed", outcome: "finished", error: null },
    { retry_intent_id: "intent-failed", outcome: "failed", error: "snapshot attempts exhausted" },
  ]);
});
