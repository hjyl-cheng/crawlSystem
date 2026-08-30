import assert from "node:assert/strict";
import test from "node:test";
import {
  InMemoryManagedJobDispatchRepository,
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "../src/managedJobDispatchOutbox.js";
import { channelSnapshotDispatchIntentHash } from "../src/channelSnapshotDispatch.js";

function dispatch(overrides = {}) {
  return {
    dispatch_id: "dispatch-1",
    aggregate_kind: "discover_page",
    aggregate_id: "page-1",
    intent_hash: "sha256:intent",
    queue_registry_key: "youtube-discover-page",
    deterministic_job_id: "discover-page__page-1",
    payload_json: { page_id: "page-1", intent_schema_version: 1, dispatch_generation: 1 },
    status: "pending",
    attempts: 0,
    ...overrides,
  };
}

function migrationRecoveryPayload(overrides = {}) {
  return {
    candidate_id: 42,
    retry_intent_id: "intent-1",
    recovery_business_run_id: "run:recovery-1",
    dispatch_generation: 5,
    dispatch_batch_id: "legacy-results-manual-v1",
    channel_id: "UCtest",
    channel_url: "https://www.youtube.com/channel/UCtest",
    crawl_mode: "full",
    query_id: null,
    query_text: "controlled migration recovery",
    pipeline_cycle_id: "legacy-results-manual-v1",
    enforce_min_subscribers: true,
    min_subscriber_count: 1000,
    reject_if_no_recent_content: true,
    recovery_reason: "recover exhausted Business Run",
    ...overrides,
  };
}

function channelSnapshotPayload(overrides = {}) {
  return {
    candidate_id: 42,
    dispatch_generation: 4,
    dispatch_batch_id: "manual-batch",
    channel_id: "UCtest",
    channel_url: "https://www.youtube.com/channel/UCtest",
    crawl_mode: "full",
    query_id: null,
    query_text: "results.db migration",
    pipeline_cycle_id: "manual-batch",
    enforce_min_subscribers: true,
    min_subscriber_count: 1000,
    reject_if_no_recent_content: true,
    ...overrides,
  };
}

function deduplicatingQueue() {
  const jobs = new Map();
  return {
    jobs,
    async getJob(jobId) {
      return jobs.get(jobId) ?? null;
    },
    async add(name, data, options) {
      const existing = jobs.get(options.jobId);
      if (existing) return existing;
      const job = { id: options.jobId, name, data };
      jobs.set(job.id, job);
      return job;
    },
  };
}

test("Outbox dispatch marks the persistent aggregate enqueued only after BullMQ accepts it", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const queue = deduplicatingQueue();
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-discover-page": queue },
  });

  const result = await dispatcher.dispatchAvailable({ limit: 10 });

  assert.deepEqual(result, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  assert.equal(queue.jobs.size, 1);
  assert.equal(repository.rows.get("dispatch-1").status, "sent");
  assert.deepEqual(repository.aggregates.get("discover_page:page-1"), {
    dispatch_status: "enqueued",
    dispatched_job_id: "discover-page__page-1",
  });
});

test("replaying the queue-add/mark-sent crash window cannot create a second BullMQ job", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const queue = deduplicatingQueue();
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-discover-page": queue },
  });
  repository.failNextMarkSent = true;

  await assert.rejects(dispatcher.dispatchAvailable({ limit: 1 }), /simulated mark-sent crash/);
  assert.equal(queue.jobs.size, 1);
  repository.recoverSending("dispatch-1");
  const replay = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.equal(replay.sent, 1);
  assert.equal(queue.jobs.size, 1);
  assert.equal(repository.rows.get("dispatch-1").status, "sent");
});

test("an ambiguous queue.add failure is accepted when the deterministic Job already exists", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const queue = deduplicatingQueue();
  const acceptedAdd = queue.add.bind(queue);
  queue.getJob = async (jobId) => queue.jobs.get(jobId) ?? null;
  queue.add = async (...args) => {
    await acceptedAdd(...args);
    throw new Error("connection closed before Redis reply");
  };
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-discover-page": queue },
    maxAttempts: 1,
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  assert.equal(queue.jobs.size, 1);
  assert.equal(repository.rows.get("dispatch-1").status, "sent");
});

test("an ambiguous queue.add failure is not accepted when the deterministic Job conflicts", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const queue = deduplicatingQueue();
  queue.jobs.set("discover-page__page-1", {
    id: "discover-page__page-1",
    name: "discover-page",
    data: { page_id: "different-page", intent_schema_version: 1 },
  });
  queue.getJob = async (jobId) => queue.jobs.get(jobId) ?? null;
  queue.add = async () => { throw new Error("connection closed before Redis reply"); };
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-discover-page": queue },
    maxAttempts: 1,
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 0, failed: 0, dead: 1 });
  const persisted = repository.rows.get("dispatch-1");
  assert.equal(persisted.status, "dead");
  assert.match(persisted.last_error, /deterministic BullMQ Job conflicts/);
});

test("a duplicate queue.add result is not accepted when the deterministic Job conflicts", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const queue = deduplicatingQueue();
  queue.jobs.set("discover-page__page-1", {
    id: "discover-page__page-1",
    name: "discover-page",
    data: { page_id: "different-page", intent_schema_version: 1 },
  });
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-discover-page": queue },
    maxAttempts: 1,
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 0, failed: 0, dead: 1 });
  const persisted = repository.rows.get("dispatch-1");
  assert.equal(persisted.status, "dead");
  assert.match(persisted.last_error, /deterministic BullMQ Job conflicts/);
});

test("a repeatedly rejected dispatch becomes dead and its aggregate becomes terminal", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: {
      "youtube-discover-page": {
        async add() { throw new Error("redis unavailable"); },
      },
    },
    maxAttempts: 2,
    retryDelayMs: () => 0,
  });

  const first = await dispatcher.dispatchAvailable({ limit: 1 });
  const second = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.equal(first.failed, 1);
  assert.equal(second.dead, 1);
  assert.equal(repository.rows.get("dispatch-1").status, "dead");
  assert.equal(repository.aggregates.get("discover_page:page-1").dispatch_status, "terminal");
});

test("a Migration Recovery Outbox dispatches the whitelisted Channel recovery job", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({
    rows: [dispatch({
      dispatch_id: "migration-retry-dispatch:intent-1",
      aggregate_kind: "migration_retry",
      aggregate_id: "intent-1",
      queue_registry_key: "youtube-channel-crawl",
      deterministic_job_id: "channel-recovery__42__intent-1__g5",
      payload_json: migrationRecoveryPayload(),
    })],
  });
  const queue = deduplicatingQueue();
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-channel-crawl": queue },
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.equal(result.sent, 1);
  assert.equal(queue.jobs.values().next().value.name, "channel-snapshot-recovery");
  assert.deepEqual(repository.aggregates.get("migration_retry:intent-1"), {
    dispatch_status: "enqueued",
    dispatched_job_id: "channel-recovery__42__intent-1__g5",
  });
});

test("replaying a current Channel snapshot Outbox accepts a matching terminal Job", async () => {
  const payload = channelSnapshotPayload({ query_id: 17, query_text: "creator search" });
  const row = dispatch({
    dispatch_id: "channel-snapshot-dispatch:42:g4",
    aggregate_kind: "channel_snapshot",
    aggregate_id: "42",
    intent_hash: channelSnapshotDispatchIntentHash(payload),
    queue_registry_key: "youtube-channel-crawl",
    deterministic_job_id: "channel-snapshot__manual-batch__UCtest__g4",
    payload_json: payload,
  });
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [row] });
  const queue = deduplicatingQueue();
  queue.jobs.set(row.deterministic_job_id, {
    id: row.deterministic_job_id,
    name: "channel-snapshot",
    data: row.payload_json,
    async getState() { return "completed"; },
  });

  const result = await new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-channel-crawl": queue },
  }).dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  assert.equal(repository.rows.get(row.dispatch_id).status, "sent");
});

test("an intent-bound Channel snapshot Outbox tolerates Worker-enriched Job data", async () => {
  const payload = channelSnapshotPayload({ migration_intent_id: 25 });
  const row = dispatch({
    dispatch_id: "channel-snapshot-dispatch:42:g4:intent-bound",
    aggregate_kind: "channel_snapshot",
    aggregate_id: "42",
    intent_hash: channelSnapshotDispatchIntentHash(payload),
    queue_registry_key: "youtube-channel-crawl",
    deterministic_job_id: "channel-snapshot__manual-batch__UCtest__g4",
    payload_json: payload,
  });
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [row] });
  const queue = deduplicatingQueue();
  const add = queue.add.bind(queue);
  queue.add = async (...args) => {
    const job = await add(...args);
    job.data = {
      ...job.data,
      run_id: "run:worker-enriched",
      business_run_id: "run:proxy-control",
      worker_runtime: { prepared_at: "2026-08-30T00:00:00.000Z" },
    };
    return job;
  };

  const result = await new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-channel-crawl": queue },
  }).dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  assert.equal(repository.rows.get(row.dispatch_id).status, "sent");
});

test("a terminal Recovery Outbox update rolls back when the Candidate fence is lost", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("UPDATE crawler.proxy_job_dispatch_outbox")) {
        return {
          rowCount: 1,
          rows: [dispatch({
            dispatch_id: "migration-retry-dispatch:intent-1",
            aggregate_kind: "migration_retry",
            aggregate_id: "intent-1",
            intent_hash: "sha256:migration-intent",
            status: "dead",
            attempts: 1,
          })],
        };
      }
      if (sql.includes("UPDATE crawler.migration_retry_intents")) {
        return { rowCount: 1, rows: [{ candidate_id: 42, dispatch_generation: "5" }] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return { rowCount: 0, rows: [] };
      }
      throw new Error("unexpected Outbox repository query");
    },
  };
  const repository = new PostgresManagedJobDispatchRepository({
    withTransaction: (action) => action(client),
  });

  await assert.rejects(
    repository.markFailed({
      dispatchId: "migration-retry-dispatch:intent-1",
      attempt: 1,
      error: "Redis delivery exhausted",
      terminal: true,
      nextAttemptAt: null,
    }),
    /lost its Candidate fence/,
  );

  const candidateUpdate = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_candidates"));
  assert.match(candidateUpdate.sql, /snapshot_dispatch_generation=\$3/);
  assert.match(candidateUpdate.sql, /snapshot_active_job_id IS NULL/);
  assert.match(candidateUpdate.sql, /snapshot_active_job_attempt IS NULL/);
  assert.deepEqual(candidateUpdate.params, [42, "Redis delivery exhausted", "5"]);
});

test("a dead first-generation Migration Channel Outbox creates a controlled retry item", async () => {
  const calls = [];
  const row = dispatch({
    dispatch_id: "channel-snapshot-dispatch:482:g1",
    aggregate_kind: "channel_snapshot",
    aggregate_id: "482",
    queue_registry_key: "youtube-channel-crawl",
    deterministic_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g1",
    payload_json: channelSnapshotPayload({
      candidate_id: 482,
      dispatch_generation: 1,
      dispatch_batch_id: "legacy-results-canary",
      pipeline_cycle_id: "legacy-results-canary",
      channel_id: "UC0NoarYHkSxek05QDqhtoYw",
      migration_intent_id: 25,
    }),
    status: "dead",
    attempts: 8,
  });
  row.intent_hash = channelSnapshotDispatchIntentHash(row.payload_json);
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("UPDATE crawler.proxy_job_dispatch_outbox")) {
        return { rowCount: 1, rows: [row] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return { rowCount: 1, rows: [{ candidate_id: 482 }] };
      }
      if (sql.includes("crawler.migration_system_retry_items")) {
        return { rowCount: 1, rows: [{ system_retry_id: 801 }] };
      }
      throw new Error(`unexpected Outbox repository query: ${sql}`);
    },
  };

  await new PostgresManagedJobDispatchRepository({
    withTransaction: (action) => action(client),
  }).markFailed({
    dispatchId: row.dispatch_id,
    attempt: 8,
    error: "Redis delivery exhausted",
    terminal: true,
    nextAttemptAt: null,
  });

  const candidateUpdate = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_candidates"));
  assert.doesNotMatch(candidateUpdate.sql, /snapshot_active_job_id=NULL/);
  assert.doesNotMatch(candidateUpdate.sql, /snapshot_active_job_attempt=NULL/);
  assert.match(candidateUpdate.sql, /snapshot_active_job_id=\$4/);
  assert.match(candidateUpdate.sql, /snapshot_active_job_attempt=0/);

  const retryWrite = calls.find(({ sql }) => sql.includes("crawler.migration_system_retry_items"));
  assert.match(retryWrite.sql, /UPDATE crawler\.migration_system_retry_items/);
  assert.match(retryWrite.sql, /INSERT INTO crawler\.migration_system_retry_items/);
  assert.match(
    retryWrite.sql,
    /migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation/,
  );
  assert.match(retryWrite.sql, /FROM crawler\.migration_channel_intents intent/);
  assert.match(retryWrite.sql, /intent\.target_candidate_id=\$1/);
  assert.match(retryWrite.sql, /AND NOT EXISTS \(SELECT 1 FROM requeued_retry\)/);
  assert.match(retryWrite.sql, /'pending'/);
  assert.deepEqual(retryWrite.params.slice(0, 7), [
    482,
    1,
    "Redis delivery exhausted",
    row.dispatch_id,
    row.deterministic_job_id,
    8,
    "legacy-results-canary",
  ]);
});

test("a dead system-retry Channel Outbox returns its item to the controlled retry list", async () => {
  const calls = [];
  const row = dispatch({
    dispatch_id: "channel-snapshot-dispatch:482:g2",
    aggregate_kind: "channel_snapshot",
    aggregate_id: "482",
    queue_registry_key: "youtube-channel-crawl",
    deterministic_job_id: "channel-snapshot__legacy-results-canary__UC0Noar__g2",
    payload_json: channelSnapshotPayload({ dispatch_generation: 2 }),
    status: "dead",
    attempts: 8,
  });
  row.intent_hash = channelSnapshotDispatchIntentHash(row.payload_json);
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("UPDATE crawler.proxy_job_dispatch_outbox")) {
        return { rowCount: 1, rows: [row] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return { rowCount: 1, rows: [{ candidate_id: 482 }] };
      }
      if (sql.includes("crawler.migration_system_retry_items")) {
        return { rowCount: 1, rows: [{ system_retry_id: 801 }] };
      }
      throw new Error(`unexpected Outbox repository query: ${sql}`);
    },
  };
  const repository = new PostgresManagedJobDispatchRepository({
    withTransaction: (action) => action(client),
  });

  await repository.markFailed({
    dispatchId: row.dispatch_id,
    attempt: 8,
    error: "Redis delivery exhausted",
    terminal: true,
    nextAttemptAt: null,
  });

  const candidateUpdate = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_candidates"));
  assert.match(
    candidateUpdate.sql,
    /status=CASE WHEN status='accepted' THEN 'accepted' ELSE 'failed' END/,
  );
  assert.match(
    candidateUpdate.sql,
    /validation_finished_at=CASE WHEN status='accepted' THEN validation_finished_at ELSE now\(\) END/,
  );
  assert.match(candidateUpdate.sql, /status IN \('queued','accepted'\)/);
  assert.match(candidateUpdate.sql, /retryable_system_failure/);
  assert.match(candidateUpdate.sql, /OUTBOX_DELIVERY_EXHAUSTED/);
  const retryUpdate = calls.find(({ sql }) => (
    sql.includes("UPDATE crawler.migration_system_retry_items")
  ));
  assert.ok(retryUpdate);
  assert.match(retryUpdate.sql, /SET[\s\S]*status='pending'/);
  assert.match(retryUpdate.sql, /retry_dispatch_generation=\$2/);
  assert.match(retryUpdate.sql, /INSERT INTO crawler\.migration_system_retry_items/);
  assert.deepEqual(retryUpdate.params.slice(0, 3), [482, 2, "Redis delivery exhausted"]);
});

test("a Migration Recovery Outbox rejects an incomplete payload before BullMQ delivery", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({
    rows: [dispatch({
      dispatch_id: "migration-retry-dispatch:intent-1",
      aggregate_kind: "migration_retry",
      aggregate_id: "intent-1",
      queue_registry_key: "youtube-channel-crawl",
      deterministic_job_id: "channel-recovery__42__intent-1__g5",
      payload_json: { candidate_id: 42, retry_intent_id: "intent-1", dispatch_generation: 5 },
    })],
  });
  let addCalled = false;
  const invalidPayload = { candidate_id: 42, retry_intent_id: "intent-1", dispatch_generation: 5 };
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: {
      "youtube-channel-crawl": {
        async getJob() {
          return {
            id: "channel-recovery__42__intent-1__g5",
            name: "channel-snapshot-recovery",
            data: invalidPayload,
          };
        },
        async add() {
          addCalled = true;
          assert.fail("invalid payload must not reach BullMQ");
        },
      },
    },
    maxAttempts: 1,
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 0, failed: 0, dead: 1 });
  assert.equal(addCalled, false);
  assert.match(repository.rows.get("migration-retry-dispatch:intent-1").last_error, /payload/i);
});

test("a Discover Outbox rejects payload fields outside its aggregate contract", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({
    rows: [dispatch({
      payload_json: {
        page_id: "page-1",
        intent_schema_version: 1,
        dispatch_generation: 1,
        business_run_key: "stale-runtime-field",
      },
    })],
  });
  let addCalled = false;
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: {
      "youtube-discover-page": {
        async add() { addCalled = true; },
      },
    },
    maxAttempts: 1,
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 0, failed: 0, dead: 1 });
  assert.equal(addCalled, false);
});

test("a Query Quality Outbox validates its aggregate identity before BullMQ delivery", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({
    rows: [dispatch({
      aggregate_kind: "query_quality_chunk",
      aggregate_id: "quality-chunk-1",
      queue_registry_key: "youtube-query-quality",
      deterministic_job_id: "query-quality__quality-chunk-1",
      payload_json: {
        quality_chunk_id: "different-quality-chunk",
        intent_schema_version: 1,
        dispatch_generation: 1,
      },
    })],
  });
  let addCalled = false;
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: {
      "youtube-query-quality": {
        async add() { addCalled = true; },
      },
    },
    maxAttempts: 1,
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 0, failed: 0, dead: 1 });
  assert.equal(addCalled, false);
  assert.match(repository.rows.get("dispatch-1").last_error, /aggregate_id/);
});

test("an aggregate cannot dispatch through another managed queue", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({
    rows: [dispatch({ queue_registry_key: "youtube-query-quality" })],
  });
  let addCalled = false;
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: {
      "youtube-query-quality": {
        async add() { addCalled = true; },
      },
    },
    maxAttempts: 1,
  });

  const result = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.deepEqual(result, { claimed: 1, sent: 0, failed: 0, dead: 1 });
  assert.equal(addCalled, false);
  assert.match(repository.rows.get("dispatch-1").last_error, /queue registry key/);
});
