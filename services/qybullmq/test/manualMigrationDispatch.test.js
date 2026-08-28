import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_MANUAL_MIGRATION_BATCH_ID,
  dispatchManualMigrationBatch,
  dispatchManualMigrationChannel,
  normalizeManualMigrationBatchSelection,
  schedulerConflict,
  validateMigrationSourceSnapshot,
} from "../src/manualMigrationDispatch.js";
import { sourceSnapshotHash } from "../src/migrationSource.js";

function sourceSnapshot(overrides = {}) {
  const value = {
    source_id: "qy-migration-v1",
    source_database: "bullmq_crawler_migration",
    source_database_oid: "16384",
    source_candidate_id: "42",
    source_candidate_status: "discovered",
    source_dispatch_batch_id: "legacy-results-full-v1",
    channel_id: "UC1234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
    handle: "@example",
    title: "Example",
    description: null,
    avatar_url: null,
    search_subscriber_count: "1200",
    search_subscriber_count_text: "1.2K",
    is_verified: false,
    priority: 100,
    snapshot_json: {},
    source_json: { source: "legacy_results_db" },
    source_created_at: "2026-08-01T00:00:00.000Z",
    source_updated_at: "2026-08-02T00:00:00.000Z",
    ...overrides,
  };
  return { ...value, snapshot_sha256: sourceSnapshotHash(value) };
}

test("manual migration does not replace another active crawler pipeline", () => {
  assert.deepEqual(
    schedulerConflict({ status: "finishing", pipeline_cycle_id: "other-pipeline" }),
    { code: "pipeline_busy", message: "crawler pipeline other-pipeline is finishing" },
  );
  assert.equal(
    schedulerConflict({ status: "finishing", pipeline_cycle_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID }),
    null,
  );
  assert.equal(schedulerConflict({ status: "stopped", pipeline_cycle_id: "old-batch" }), null);
});

test("canary batch selections are deliberately capped at approved sizes up to 2000", () => {
  assert.deepEqual(normalizeManualMigrationBatchSelection("100"), { selection: "100", limit: 100 });
  assert.deepEqual(normalizeManualMigrationBatchSelection("200"), { selection: "200", limit: 200 });
  assert.deepEqual(normalizeManualMigrationBatchSelection("500"), { selection: "500", limit: 500 });
  assert.deepEqual(normalizeManualMigrationBatchSelection("1000"), { selection: "1000", limit: 1000 });
  assert.deepEqual(normalizeManualMigrationBatchSelection("2000"), { selection: "2000", limit: 2000 });
  for (const forbidden of ["all", "300", "5000", "10000"]) {
    assert.throws(
      () => normalizeManualMigrationBatchSelection(forbidden),
      (error) => error.code === "invalid_batch_selection",
    );
  }
});

test("Migration Source snapshots reject changed payloads before Target writes", () => {
  const snapshot = sourceSnapshot();
  assert.equal(validateMigrationSourceSnapshot(snapshot), snapshot);
  assert.throws(
    () => validateMigrationSourceSnapshot({ ...snapshot, title: "changed" }),
    /snapshot hash mismatch/,
  );
  const accepted = sourceSnapshot({ source_candidate_status: "accepted" });
  assert.throws(
    () => validateMigrationSourceSnapshot(accepted),
    (error) => error.code === "source_candidate_not_pending",
  );
});

test("one-channel dispatch closes Source before opening the independent Target transaction", async () => {
  const events = [];
  const snapshot = sourceSnapshot();
  const queueAdds = [];
  let persistedQueueJob = null;
  const result = await dispatchManualMigrationChannel({
    channelId: snapshot.channel_id,
    candidateId: 42,
    queue: {
      name: "youtube-channel-crawl",
      async getJob(jobId) {
        return persistedQueueJob?.id === jobId ? persistedQueueJob : null;
      },
      async add(name, data, options) {
        queueAdds.push({ name, data, options });
        persistedQueueJob = {
          id: options.jobId,
          name,
          data,
          async getState() { return "waiting"; },
        };
        return persistedQueueJob;
      },
    },
    sourceLoader: async () => {
      events.push("source-begin");
      events.push("source-commit");
      return snapshot;
    },
    transaction: async (action) => {
      events.push("target-begin");
      const value = await action({ target: true });
      events.push("target-commit");
      return value;
    },
    targetPreparer: async (client, options) => {
      assert.equal(client.target, true);
      assert.equal(options.sourceSnapshot, snapshot);
      events.push("target-materialize");
      return {
        candidate: {
          candidate_id: 91,
          channel_id: snapshot.channel_id,
          channel_url: snapshot.channel_url,
          priority: 100,
          status: "queued",
          snapshot_dispatch_generation: 4,
        },
        batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
        previousStatus: null,
        shouldEnqueue: true,
        alreadyInProgress: false,
        intentId: 7,
      };
    },
    dbQuery: async () => ({ rowCount: 1 }),
  });

  assert.deepEqual(events, [
    "source-begin",
    "source-commit",
    "target-begin",
    "target-materialize",
    "target-commit",
  ]);
  assert.equal(result.source_candidate_id, 42);
  assert.equal(result.candidate_id, 91);
  assert.equal(result.migration_intent_id, 7);
  assert.equal(queueAdds.length, 1);
  assert.equal(queueAdds[0].data.crawl_mode, "full");
  assert.equal(queueAdds[0].data.dispatch_generation, 4);
  assert.match(queueAdds[0].options.jobId, /__g4$/);
});

test("a repeated click reuses the Target intent and does not duplicate the queue job", async () => {
  let queueCalls = 0;
  const snapshot = sourceSnapshot();
  const result = await dispatchManualMigrationChannel({
    channelId: snapshot.channel_id,
    candidateId: 42,
    queue: {
      name: "youtube-channel-crawl",
      async getJob() { queueCalls += 1; return null; },
      async add() { queueCalls += 1; },
    },
    sourceLoader: async () => snapshot,
    transaction: (action) => action({}),
    targetPreparer: async () => ({
      candidate: {
        candidate_id: 91,
        channel_id: snapshot.channel_id,
        channel_url: snapshot.channel_url,
        status: "queued",
      },
      batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      shouldEnqueue: false,
      alreadyInProgress: true,
      intentId: 7,
    }),
  });

  assert.equal(result.created, false);
  assert.equal(result.already_in_progress, true);
  assert.equal(queueCalls, 0);
});

test("a represented deterministic Job must match the manual migration identity", async () => {
  const snapshot = sourceSnapshot();
  const jobId = `channel-snapshot__${DEFAULT_MANUAL_MIGRATION_BATCH_ID}__${snapshot.channel_id}__g1`;
  const conflictingJob = {
    id: jobId,
    name: "unrelated-channel-job",
    data: { candidate_id: 999, dispatch_generation: 1 },
    async getState() { return "waiting"; },
  };

  await assert.rejects(
    dispatchManualMigrationChannel({
      channelId: snapshot.channel_id,
      candidateId: 42,
      queue: {
        name: "youtube-channel-crawl",
        async getJob(candidateJobId) {
          return candidateJobId === jobId ? conflictingJob : null;
        },
        async add() {
          assert.fail("a represented deterministic Job must not be added again");
        },
      },
      sourceLoader: async () => snapshot,
      transaction: (action) => action({}),
      targetPreparer: async () => ({
        candidate: {
          candidate_id: 91,
          channel_id: snapshot.channel_id,
          channel_url: snapshot.channel_url,
          priority: 100,
          status: "queued",
          snapshot_dispatch_generation: 1,
        },
        batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
        previousStatus: "failed",
        shouldEnqueue: true,
        intentId: 7,
      }),
      dbQuery: async () => ({ rowCount: 1 }),
    }),
    (error) => error?.code === "job_identity_conflict",
  );
});

test("batch dispatch reads Target exclusions, closes Source, then materializes only fresh intents", async () => {
  const events = [];
  const snapshots = [sourceSnapshot(), sourceSnapshot({
    source_candidate_id: "43",
    channel_id: "UC2234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC2234567890123456789012",
  })].map((snapshot) => ({ ...snapshot, snapshot_sha256: sourceSnapshotHash(snapshot) }));

  const result = await dispatchManualMigrationBatch({
    selection: "100",
    sourceId: "qy-migration-v1",
    exclusionLoader: async () => {
      events.push("target-exclusions");
      return { sourceCandidateIds: ["40"], channelIds: ["UCold"] };
    },
    sourceBatchLoader: async (options) => {
      events.push("source-read-commit");
      assert.equal(options.limit, 100);
      assert.deepEqual(options.excludeSourceCandidateIds, ["40"]);
      return snapshots;
    },
    transaction: async (action) => {
      events.push("target-begin");
      const value = await action({});
      events.push("target-commit");
      return value;
    },
    targetBatchPreparer: async (_client, options) => {
      events.push("target-materialize");
      assert.equal(options.sourceSnapshots, snapshots);
      return {
        batchId: "manual-canary-100",
        targetCount: 2,
        reusedCount: 0,
        firstSourceCandidateId: "42",
        lastSourceCandidateId: "43",
      };
    },
    batchId: "manual-canary-100",
  });

  assert.deepEqual(events, [
    "target-exclusions",
    "source-read-commit",
    "target-begin",
    "target-materialize",
    "target-commit",
  ]);
  assert.equal(result.target_count, 2);
  assert.equal(result.created, true);
});

test("a Redis delivery failure records compensation only in the Target database", async () => {
  const snapshot = sourceSnapshot();
  const compensation = [];
  await assert.rejects(
    dispatchManualMigrationChannel({
      channelId: snapshot.channel_id,
      candidateId: 42,
      queue: {
        name: "youtube-channel-crawl",
        async getJob() { return null; },
        async add() { throw new Error("redis unavailable"); },
      },
      sourceLoader: async () => snapshot,
      transaction: (action) => action({}),
      targetPreparer: async () => ({
        candidate: {
          candidate_id: 91,
          channel_id: snapshot.channel_id,
          channel_url: snapshot.channel_url,
          priority: 100,
          status: "queued",
          snapshot_dispatch_generation: 1,
        },
        batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
        previousStatus: null,
        shouldEnqueue: true,
        intentId: 7,
      }),
      dbQuery: async (sql, params) => {
        compensation.push({ sql, params });
        return { rowCount: 1 };
      },
    }),
    /redis unavailable/,
  );
  assert.equal(compensation.length, 1);
  assert.match(compensation[0].sql, /crawler\.channel_candidates/);
  assert.match(compensation[0].sql, /crawler\.migration_channel_intents/);
  assert.match(compensation[0].sql, /snapshot_dispatch_generation=\$3/);
  assert.match(compensation[0].sql, /snapshot_active_job_id IS NULL/);
  assert.match(compensation[0].sql, /dispatch_attempts=\$3/);
  assert.equal(compensation[0].params[0], 91);
  assert.equal(compensation[0].params[2], 1);
});

test("an ambiguous queue delivery is recovered from the persisted deterministic Job", async () => {
  const snapshot = sourceSnapshot();
  const persistedJob = {
    id: `channel-snapshot__${DEFAULT_MANUAL_MIGRATION_BATCH_ID}__${snapshot.channel_id}__g1`,
    name: "channel-snapshot",
    data: {
      candidate_id: 91,
      dispatch_generation: 1,
      dispatch_batch_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      channel_id: snapshot.channel_id,
      channel_url: snapshot.channel_url,
      crawl_mode: "full",
      query_id: null,
      query_text: "results.db migration",
      pipeline_cycle_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      enforce_min_subscribers: true,
      min_subscriber_count: 1000,
      reject_if_no_recent_content: true,
    },
    async getState() { return "waiting"; },
  };
  const compensation = [];
  const result = await dispatchManualMigrationChannel({
    channelId: snapshot.channel_id,
    candidateId: 42,
    queue: {
      name: "youtube-channel-crawl",
      async getJob(jobId) {
        return jobId === persistedJob.id ? persistedJob : null;
      },
      async add() {
        throw new Error("connection closed after Redis accepted the Job");
      },
    },
    sourceLoader: async () => snapshot,
    transaction: (action) => action({}),
    targetPreparer: async () => ({
      candidate: {
        candidate_id: 91,
        channel_id: snapshot.channel_id,
        channel_url: snapshot.channel_url,
        priority: 100,
        status: "queued",
        snapshot_dispatch_generation: 1,
      },
      batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      previousStatus: null,
      shouldEnqueue: true,
      intentId: 7,
    }),
    dbQuery: async (...args) => {
      compensation.push(args);
      return { rowCount: 1 };
    },
  });

  assert.equal(result.created, false);
  assert.equal(result.job.id, persistedJob.id);
  assert.equal(result.job.state, "waiting");
  assert.deepEqual(compensation, []);
});

test("a matching terminal Job for the current exact Outbox is delivery, not another generation", async () => {
  const snapshot = sourceSnapshot();
  const jobId = `channel-snapshot__${DEFAULT_MANUAL_MIGRATION_BATCH_ID}__${snapshot.channel_id}__g5`;
  const payload = {
    candidate_id: 91,
    dispatch_generation: 5,
    dispatch_batch_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
    channel_id: snapshot.channel_id,
    channel_url: snapshot.channel_url,
    crawl_mode: "full",
    query_id: null,
    query_text: "results.db migration",
    pipeline_cycle_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
    enforce_min_subscribers: true,
    min_subscriber_count: 1000,
    reject_if_no_recent_content: true,
  };
  const job = {
    id: jobId,
    name: "channel-snapshot",
    data: payload,
    async getState() { return "completed"; },
  };
  const marked = [];

  const result = await dispatchManualMigrationChannel({
    channelId: snapshot.channel_id,
    candidateId: 42,
    queue: {
      name: "youtube-channel-crawl",
      async getJob(candidateJobId) { return candidateJobId === jobId ? job : null; },
      async add() { assert.fail("the exact persisted Job must be reused"); },
    },
    sourceLoader: async () => snapshot,
    transaction: (action) => action({}),
    targetPreparer: async () => ({
      candidate: {
        candidate_id: 91,
        channel_id: snapshot.channel_id,
        channel_url: snapshot.channel_url,
        priority: 100,
        status: "queued",
        snapshot_dispatch_generation: 5,
      },
      batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      previousStatus: "failed",
      shouldEnqueue: true,
      intentId: 7,
      outbox: {
        dispatch_id: "channel-snapshot-dispatch:91:g5:test",
        aggregate_kind: "channel_snapshot",
        aggregate_id: "91",
        intent_hash: "sha256:test",
        queue_registry_key: "youtube-channel-crawl",
        deterministic_job_id: jobId,
        payload_json: payload,
        status: "pending",
      },
    }),
    terminalDispatchAdvancer: async () => assert.fail("current Outbox must not allocate G+1"),
    dbQuery: async (sql, params) => {
      marked.push({ sql, params });
      return { rowCount: 1 };
    },
  });

  assert.equal(result.created, false);
  assert.equal(result.job.id, jobId);
  assert.equal(result.job.state, "completed");
  assert.equal(marked.length, 1);
  assert.match(marked[0].sql, /status='sent'/);
});

test("all raced deterministic Job reuse paths reject conflicting identity", async (t) => {
  const snapshot = sourceSnapshot();
  const jobId = `channel-snapshot__${DEFAULT_MANUAL_MIGRATION_BATCH_ID}__${snapshot.channel_id}__g1`;
  const conflicting = () => ({
    id: jobId,
    name: "channel-snapshot",
    data: { candidate_id: 999, dispatch_generation: 1 },
    async getState() { return "waiting"; },
  });
  const dispatch = (queue, overrides = {}) => dispatchManualMigrationChannel({
    channelId: snapshot.channel_id,
    candidateId: 42,
    queue: { name: "youtube-channel-crawl", ...queue },
    sourceLoader: async () => snapshot,
    transaction: (action) => action({}),
    targetPreparer: async () => ({
      candidate: {
        candidate_id: 91,
        channel_id: snapshot.channel_id,
        channel_url: snapshot.channel_url,
        priority: 100,
        status: "queued",
        snapshot_dispatch_generation: 1,
      },
      batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      previousStatus: "failed",
      shouldEnqueue: true,
      intentId: 7,
    }),
    dbQuery: async () => ({ rowCount: 1 }),
    ...overrides,
  });

  await t.test("a conflicting terminal Job is never removed", async () => {
    for (const state of ["completed", "failed"]) {
      let removed = false;
      await assert.rejects(
        dispatch({
          async getJob() {
            return {
              ...conflicting(),
              async getState() { return state; },
              async remove() {
                removed = true;
                throw new Error("conflicting terminal Job must not be removed");
              },
            };
          },
          async add() { assert.fail("conflicting terminal Job must not be replaced"); },
        }),
        (error) => error?.code === "job_identity_conflict",
        state,
      );
      assert.equal(removed, false, state);
    }
  });

  await t.test("a matching terminal G Job is preserved while Target allocates G+1", async () => {
    const matchingPayload = {
      candidate_id: 91,
      dispatch_generation: 1,
      dispatch_batch_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      channel_id: snapshot.channel_id,
      channel_url: snapshot.channel_url,
      crawl_mode: "full",
      query_id: null,
      query_text: "results.db migration",
      pipeline_cycle_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
      enforce_min_subscribers: true,
      min_subscriber_count: 1000,
      reject_if_no_recent_content: true,
    };
    let removeCalled = false;
    const terminalJob = {
      id: jobId,
      name: "channel-snapshot",
      data: matchingPayload,
      async getState() { return "completed"; },
      async remove() {
        removeCalled = true;
        throw new Error("terminal audit Job must never be removed");
      },
    };
    const jobs = new Map([[jobId, terminalJob]]);
    const nextPayload = { ...matchingPayload, dispatch_generation: 2 };
    const nextJobId = `channel-snapshot__${DEFAULT_MANUAL_MIGRATION_BATCH_ID}__${snapshot.channel_id}__g2`;

    const result = await dispatch({
        async getJob(candidateJobId) { return jobs.get(candidateJobId) ?? null; },
        async add(name, data, options) {
          const persisted = {
            id: options.jobId,
            name,
            data,
            async getState() { return "waiting"; },
          };
          jobs.set(options.jobId, persisted);
          return persisted;
        },
      }, {
        terminalDispatchAdvancer: async (_client, { prepared }) => {
          assert.equal(prepared.candidate.snapshot_dispatch_generation, 1);
          return {
            created: true,
            candidate: { ...prepared.candidate, snapshot_dispatch_generation: 2 },
            outbox: {
              dispatch_id: "channel-snapshot-dispatch:91:g2:test",
              aggregate_kind: "channel_snapshot",
              aggregate_id: "91",
              intent_hash: "sha256:test",
              queue_registry_key: "youtube-channel-crawl",
              deterministic_job_id: nextJobId,
              payload_json: nextPayload,
              status: "pending",
            },
          };
        },
      });

    assert.equal(removeCalled, false);
    assert.equal(result.dispatch_generation, 2);
    assert.equal(result.job.id, nextJobId);
    assert.equal(jobs.get(jobId), terminalJob);
    assert.deepEqual(jobs.get(jobId).data, matchingPayload);
    assert.deepEqual(jobs.get(nextJobId).data, nextPayload);
  });

  await t.test("a Job that wins the remove race", async () => {
    let reads = 0;
    const replaced = {
      id: jobId,
      async getState() { return "completed"; },
      async remove() { throw new Error("Job became active before remove"); },
    };
    await assert.rejects(
      dispatch({
        async getJob() {
          reads += 1;
          return reads === 1 ? replaced : conflicting();
        },
        async add() { assert.fail("remove-race Job must not be added again"); },
      }),
      (error) => error?.code === "job_identity_conflict",
    );
  });

  await t.test("a Job recovered after an ambiguous queue.add", async () => {
    let reads = 0;
    await assert.rejects(
      dispatch({
        async getJob() {
          reads += 1;
          return reads === 1 ? null : conflicting();
        },
        async add() { throw new Error("connection closed after Redis reply"); },
      }),
      (error) => error?.code === "job_identity_conflict",
    );
  });

  await t.test("a Job that wins between getJob and a successful queue.add", async () => {
    let reads = 0;
    await assert.rejects(
      dispatch({
        async getJob() {
          reads += 1;
          return reads === 1 ? null : conflicting();
        },
        async add(name, data, options) {
          return { id: options.jobId, name, data };
        },
      }),
      (error) => error?.code === "job_identity_conflict",
    );
  });
});
