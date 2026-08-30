import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  channelSnapshotDispatchIntentHash,
  channelSnapshotJobPayload,
} from "../src/channelSnapshotDispatch.js";
import { reconcileChannelCandidateQueue } from "../src/channelSnapshotReconciliation.js";

test("Controller expires stale Channel terminal samples from pressure metrics", async () => {
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");

  assert.match(
    source,
    /created_at >= now\(\) - \(\$3::int \* interval '1 second'\)/,
  );
  assert.match(
    source,
    /\[queuesByRole\.channelCrawl, channelPressureSampleSize, channelPressureWindowSeconds\]/,
  );
});

test("Controller allocates G+1 for an ordinary Query terminal Job with canonical identity", async () => {
  const currentJobId = "channel-snapshot__query-batch__UCquery__g1";
  const nextJobId = "channel-snapshot__query-batch__UCquery__g2";
  const candidate = {
    candidate_id: "42",
    channel_id: "UCquery",
    channel_url: "https://www.youtube.com/channel/UCquery",
    pipeline_cycle_id: "query-cycle",
    priority: 100,
    status: "failed",
    snapshot_dispatch_generation: "1",
    snapshot_active_job_id: currentJobId,
    snapshot_active_job_attempt: 2,
    candidate_source: "youtube_search_discovery",
    query_id: 91,
    query_text: "quiet coding channels",
    migration_intent_id: null,
  };
  const currentPayload = channelSnapshotJobPayload(candidate, "query-batch", {
    minSubscriberCount: 1000,
  });
  const actions = [];
  const transactionCalls = [];
  const client = {
    async query(sql, params) {
      transactionCalls.push({ sql, params });
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [candidate] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return {
          rowCount: 1,
          rows: [{
            ...candidate,
            status: "queued",
            snapshot_dispatch_generation: "2",
            snapshot_active_job_id: nextJobId,
            snapshot_active_job_attempt: 0,
          }],
        };
      }
      if (sql.includes("INSERT INTO crawler.proxy_job_dispatch_outbox")) {
        return { rowCount: 1, rows: [{ dispatch_id: params[0] }] };
      }
      throw new Error(`unexpected transaction SQL: ${sql}`);
    },
  };

  const enqueued = await reconcileChannelCandidateQueue({
    actions,
    dispatchBatchId: "query-batch",
    query: async () => ({ rows: [candidate] }),
    queue: {
      async getJob(jobId) {
        if (jobId !== currentJobId) return null;
        return {
          id: currentJobId,
          name: "channel-snapshot",
          data: currentPayload,
          async getState() { return "failed"; },
        };
      },
    },
    withTransaction: async (callback) => callback(client),
    safeJobId: (...parts) => parts.join("__"),
    minSubscriberCount: 1000,
    staleSeconds: 900,
    maxAttempts: 3,
    retrySeconds: 30,
    limit: 10,
  });

  assert.equal(enqueued, 1);
  assert.equal(actions.some((action) => action.action.includes("identity-conflict")), false);
  assert.equal(actions.at(-1)?.action, "reconcile-channel-snapshots");
  const candidateUpdate = transactionCalls.find(({ sql }) => (
    sql.includes("UPDATE crawler.channel_candidates")
  ));
  assert.equal(candidateUpdate.params[2], nextJobId);
});

test("Controller allocates exactly one G+1 after a Worker enriches Migration Job Data", async () => {
  const batchId = "legacy-results-canary-1788072676068-25df5b5e";
  const channelId = "UC0NoarYHkSxek05QDqhtoYw";
  const currentJobId = `channel-snapshot__${batchId}__${channelId}__g1`;
  const nextJobId = `channel-snapshot__${batchId}__${channelId}__g2`;
  const candidate = {
    candidate_id: "482",
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    pipeline_cycle_id: batchId,
    priority: 100,
    status: "failed",
    snapshot_dispatch_generation: "1",
    snapshot_active_job_id: currentJobId,
    snapshot_active_job_attempt: 1,
    candidate_source: "legacy_results_db",
    query_id: null,
    query_text: "Migration PostgreSQL controlled canary",
    migration_intent_id: "25",
  };
  const currentPayload = channelSnapshotJobPayload(candidate, batchId, {
    minSubscriberCount: 1000,
  });
  candidate.snapshot_outbox_job_id = currentJobId;
  candidate.snapshot_outbox_payload = currentPayload;
  candidate.snapshot_outbox_intent_hash = channelSnapshotDispatchIntentHash(currentPayload);
  const enrichedData = {
    ...currentPayload,
    run_id: "run:worker-enriched",
    business_run_id: "run:proxy-control",
    business_run_key: "full-candidate:482",
    full_intent_id: "full-intent:worker-enriched",
    worker_runtime: { prepared_at: "2026-08-30T00:00:00.000Z" },
  };
  const actions = [];
  let transactionCount = 0;
  let outboxWrites = 0;
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [candidate] };
      }
      if (sql.includes("UPDATE crawler.migration_channel_intents")) {
        return { rowCount: 1, rows: [{ dispatch_attempts: 2 }] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return {
          rowCount: 1,
          rows: [{
            ...candidate,
            status: "queued",
            snapshot_dispatch_generation: "2",
            snapshot_active_job_id: nextJobId,
            snapshot_active_job_attempt: 0,
          }],
        };
      }
      if (sql.includes("INSERT INTO crawler.proxy_job_dispatch_outbox")) {
        outboxWrites += 1;
        return { rowCount: 1, rows: [{ dispatch_id: params[0] }] };
      }
      throw new Error(`unexpected transaction SQL: ${sql}`);
    },
  };

  const enqueued = await reconcileChannelCandidateQueue({
    actions,
    dispatchBatchId: batchId,
    query: async () => ({ rows: [candidate] }),
    queue: {
      async getJob(jobId) {
        if (jobId !== currentJobId) return null;
        return {
          id: currentJobId,
          name: "channel-snapshot",
          data: enrichedData,
          async getState() { return "failed"; },
        };
      },
    },
    withTransaction: async (callback) => {
      transactionCount += 1;
      return callback(client);
    },
    safeJobId: (...parts) => parts.join("__"),
    minSubscriberCount: 2000,
    staleSeconds: 900,
    maxAttempts: 3,
    retrySeconds: 30,
    limit: 10,
  });

  assert.equal(enqueued, 1);
  assert.equal(transactionCount, 1);
  assert.equal(outboxWrites, 1);
  assert.equal(actions.some((action) => action.action.includes("identity-conflict")), false);
  assert.equal(actions.at(-1)?.action, "reconcile-channel-snapshots");
});

test("Controller refuses G+1 after the observed terminal Job starts a newer attempt", async () => {
  const currentJobId = "channel-snapshot__query-batch__UCrace__g1";
  const observed = {
    candidate_id: "43",
    channel_id: "UCrace",
    channel_url: "https://www.youtube.com/channel/UCrace",
    pipeline_cycle_id: "query-cycle",
    priority: 100,
    status: "validating",
    snapshot_dispatch_generation: "1",
    snapshot_active_job_id: currentJobId,
    snapshot_active_job_attempt: 2,
    candidate_source: "youtube_search_discovery",
    query_id: 92,
    query_text: "race channels",
    migration_intent_id: null,
  };
  const raced = { ...observed, snapshot_active_job_attempt: 3 };
  const actions = [];
  let outboxWrites = 0;
  const client = {
    async query(sql, params) {
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [raced] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        const expectedPreviousAttempt = params[4];
        const exactAttemptFence = sql.includes("snapshot_active_job_attempt=$5");
        if (exactAttemptFence && expectedPreviousAttempt !== Number(raced.snapshot_active_job_attempt)) {
          return { rowCount: 0, rows: [] };
        }
        return {
          rowCount: 1,
          rows: [{
            ...raced,
            status: "queued",
            snapshot_dispatch_generation: "2",
            snapshot_active_job_id: "channel-snapshot__query-batch__UCrace__g2",
            snapshot_active_job_attempt: 0,
          }],
        };
      }
      if (sql.includes("INSERT INTO crawler.proxy_job_dispatch_outbox")) {
        outboxWrites += 1;
        return { rowCount: 1, rows: [{ dispatch_id: params[0] }] };
      }
      throw new Error(`unexpected transaction SQL: ${sql}`);
    },
  };

  const enqueued = await reconcileChannelCandidateQueue({
    actions,
    dispatchBatchId: "query-batch",
    query: async () => ({ rows: [observed] }),
    queue: {
      async getJob(jobId) {
        if (jobId !== currentJobId) return null;
        return {
          id: currentJobId,
          name: "channel-snapshot",
          data: channelSnapshotJobPayload(observed, "query-batch", {
            minSubscriberCount: 1000,
          }),
          async getState() { return "failed"; },
        };
      },
    },
    withTransaction: async (callback) => callback(client),
    safeJobId: (...parts) => parts.join("__"),
    minSubscriberCount: 1000,
    staleSeconds: 900,
    maxAttempts: 3,
    retrySeconds: 30,
    limit: 10,
  });

  assert.equal(enqueued, 0);
  assert.equal(outboxWrites, 0);
  assert.equal(actions.at(-1)?.action, "hold-channel-snapshot-dispatch-conflict");
});

test("Controller leaves retryable system failures on the independent retry list", async () => {
  let transactionCount = 0;
  const actions = [];
  const enqueued = await reconcileChannelCandidateQueue({
    actions,
    dispatchBatchId: "legacy-results-canary-system-failure",
    query: async (sql) => {
      assert.match(
        sql,
        /NOT EXISTS \([\s\S]*FROM crawler\.migration_system_retry_items active_system_retry[\s\S]*active_system_retry\.status IN \('retrying','pending','dispatched'\)/,
      );
      assert.doesNotMatch(
        sql,
        /COALESCE\(candidate\.snapshot_json->>'failure_type',''\)<>'retryable_system_failure'/,
      );
      return { rows: [] };
    },
    queue: { async getJob() { assert.fail("system retry list must own redispatch"); } },
    withTransaction: async () => { transactionCount += 1; },
    safeJobId: (...parts) => parts.join("__"),
    minSubscriberCount: 1000,
    staleSeconds: 900,
    maxAttempts: 3,
    retrySeconds: 30,
    limit: 10,
  });

  assert.equal(enqueued, 0);
  assert.equal(transactionCount, 0);
  assert.deepEqual(actions, []);
});
