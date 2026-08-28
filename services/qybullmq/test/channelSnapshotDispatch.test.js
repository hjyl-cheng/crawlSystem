import assert from "node:assert/strict";
import test from "node:test";

import {
  allocateChannelSnapshotDispatchOutbox,
  allocateDiscoveredChannelSnapshotDispatches,
  buildDiscoveredChannelSnapshotJob,
  buildChannelSnapshotRedispatchAllocation,
  buildChannelSnapshotOutbox,
  channelSnapshotJobPayload,
  stageChannelSnapshotOutbox,
} from "../src/channelSnapshotDispatch.js";
import { channelSnapshotPayload } from "../src/migrationDispatchPolicy.js";

test("Discover persists first generations before bulk queue delivery", async () => {
  const candidates = await allocateDiscoveredChannelSnapshotDispatches(async (sql, params) => {
    assert.match(sql, /status='discovered'/);
    assert.deepEqual(params, [[41, 42]]);
    return {
      rows: [
        { candidate_id: "41", snapshot_dispatch_generation: "1" },
        { candidate_id: "42", snapshot_dispatch_generation: "1" },
      ],
    };
  }, [41, 42]);

  assert.deepEqual(candidates, [
    { candidate_id: 41, snapshot_dispatch_generation: 1 },
    { candidate_id: 42, snapshot_dispatch_generation: 1 },
  ]);
});

test("Discover and Controller reconstruction use one canonical Channel snapshot payload", () => {
  const first = buildDiscoveredChannelSnapshotJob({
    candidate: {
      candidate_id: 42,
      snapshot_dispatch_generation: 1,
    },
    channel: {
      channel_id: "UCquery",
      channel_url: "https://www.youtube.com/channel/UCquery",
    },
    dispatchBatchId: "query-batch",
    pipelineCycleId: "query-cycle",
    queryId: 91,
    queryText: "quiet coding channels",
    minSubscriberCount: 1000,
    jobId: "channel-snapshot__query-batch__UCquery__g1",
  });
  const reconstructed = channelSnapshotJobPayload({
    candidate_id: 42,
    snapshot_dispatch_generation: 1,
    channel_id: "UCquery",
    channel_url: "https://www.youtube.com/channel/UCquery",
    query_id: 91,
    query_text: "quiet coding channels",
    pipeline_cycle_id: "query-cycle",
    candidate_source: "youtube_search_discovery",
  }, "query-batch", { minSubscriberCount: 1000 });

  assert.equal(first.name, "channel-snapshot");
  assert.equal(first.opts.jobId, "channel-snapshot__query-batch__UCquery__g1");
  assert.deepEqual(first.data, reconstructed);
  assert.equal(first.data.reject_if_no_recent_content, false);
});

function snapshotCandidate(generation = 4) {
  return {
    candidate_id: 42,
    dispatch_batch_id: "manual-batch",
    pipeline_cycle_id: "manual-batch",
    channel_id: "UCsnapshot",
    channel_url: "https://www.youtube.com/channel/UCsnapshot",
    priority: 100,
    status: "queued",
    snapshot_dispatch_generation: generation,
  };
}

function snapshotPayload(generation = 4) {
  return {
    candidate_id: 42,
    dispatch_generation: generation,
    dispatch_batch_id: "manual-batch",
    channel_id: "UCsnapshot",
    channel_url: "https://www.youtube.com/channel/UCsnapshot",
    crawl_mode: "full",
    query_id: null,
    query_text: "results.db migration",
    pipeline_cycle_id: "manual-batch",
    enforce_min_subscribers: true,
    min_subscriber_count: 1000,
    reject_if_no_recent_content: true,
  };
}

test("Controller migration redispatch preserves the shared payload and Intent generation fence", () => {
  const candidate = {
    ...snapshotCandidate(3),
    migration_intent_id: "7",
    query_id: "91",
    query_text: "Migration PostgreSQL controlled canary",
    pipeline_cycle_id: "misleading-cycle",
    candidate_source: "legacy_results_db",
  };
  const expectedCurrent = channelSnapshotPayload(candidate, "manual-batch", {
    minSubscriberCount: 1000,
  });
  assert.deepEqual(
    channelSnapshotJobPayload(candidate, "manual-batch", { minSubscriberCount: 1000 }),
    expectedCurrent,
  );

  const allocation = buildChannelSnapshotRedispatchAllocation(candidate, "manual-batch", {
    expectedGeneration: 3,
    previousJobId: "channel-snapshot__manual-batch__UCsnapshot__g3",
    previousJobAttempt: 2,
    jobId: "channel-snapshot__manual-batch__UCsnapshot__g4",
    minSubscriberCount: 1000,
  });
  assert.equal(allocation.migrationIntentId, 7);
  assert.deepEqual(
    allocation.payload,
    channelSnapshotPayload({
      ...candidate,
      snapshot_dispatch_generation: 4,
    }, "manual-batch", { minSubscriberCount: 1000 }),
  );
});

test("a prepared generation and its exact Outbox identity share the Candidate fence", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return {
          rowCount: 1,
          rows: [{
            candidate_id: "42",
            snapshot_dispatch_generation: "4",
            snapshot_active_job_id: "channel-snapshot__manual-batch__UCsnapshot__g4",
            snapshot_active_job_attempt: 0,
          }],
        };
      }
      if (sql.includes("INSERT INTO crawler.proxy_job_dispatch_outbox")) {
        return { rowCount: 1, rows: [{ dispatch_id: params[0] }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const staged = await stageChannelSnapshotOutbox(client, {
    candidate: snapshotCandidate(),
    payload: snapshotPayload(),
    jobId: "channel-snapshot__manual-batch__UCsnapshot__g4",
  });

  assert.equal(staged.outbox.aggregate_kind, "channel_snapshot");
  assert.equal(staged.outbox.aggregate_id, "42");
  assert.equal(staged.outbox.deterministic_job_id, "channel-snapshot__manual-batch__UCsnapshot__g4");
  assert.equal(staged.outbox.payload_json.dispatch_generation, 4);
  assert.match(calls[0].sql, /snapshot_active_job_attempt=CASE/);
  assert.match(calls[0].sql, /snapshot_dispatch_generation=\$2/);
  assert.deepEqual(calls[0].params, [
    42,
    4,
    "channel-snapshot__manual-batch__UCsnapshot__g4",
  ]);
});

test("the terminal-Job CAS winner allocates G+1 and persists its exact Outbox", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            ...snapshotCandidate(3),
            snapshot_dispatch_generation: "3",
            snapshot_active_job_id: "channel-snapshot__manual-batch__UCsnapshot__g3",
            snapshot_active_job_attempt: 2,
          }],
        };
      }
      if (sql.includes("UPDATE crawler.migration_channel_intents")) {
        return { rowCount: 1, rows: [{ dispatch_attempts: 4 }] };
      }
      if (sql.includes("UPDATE crawler.channel_candidates")) {
        return {
          rowCount: 1,
          rows: [{
            ...snapshotCandidate(4),
            snapshot_dispatch_generation: "4",
            snapshot_active_job_id: "channel-snapshot__manual-batch__UCsnapshot__g4",
            snapshot_active_job_attempt: 0,
          }],
        };
      }
      if (sql.includes("INSERT INTO crawler.proxy_job_dispatch_outbox")) {
        return { rowCount: 1, rows: [{ dispatch_id: params[0] }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const allocated = await allocateChannelSnapshotDispatchOutbox(client, {
    candidate: snapshotCandidate(4),
    expectedGeneration: 3,
    previousJobId: "channel-snapshot__manual-batch__UCsnapshot__g3",
    previousJobAttempt: 2,
    migrationIntentId: 7,
    payload: snapshotPayload(4),
    jobId: "channel-snapshot__manual-batch__UCsnapshot__g4",
  });

  assert.equal(allocated.created, true);
  assert.equal(allocated.candidate.snapshot_dispatch_generation, 4);
  assert.equal(allocated.outbox.payload_json.dispatch_generation, 4);
  const candidateUpdate = calls.find(({ sql }) => sql.includes("UPDATE crawler.channel_candidates"));
  assert.match(candidateUpdate.sql, /snapshot_dispatch_generation=\$2 \+ 1/);
  assert.match(candidateUpdate.sql, /snapshot_active_job_attempt=0/);
  assert.match(candidateUpdate.sql, /snapshot_dispatch_generation=\$2/);
});

test("a generation CAS loser reuses only the exact G+1 Outbox", async () => {
  const outbox = buildChannelSnapshotOutbox({
    candidate: snapshotCandidate(4),
    payload: snapshotPayload(4),
    jobId: "channel-snapshot__manual-batch__UCsnapshot__g4",
  });
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            ...snapshotCandidate(4),
            snapshot_dispatch_generation: "4",
            snapshot_active_job_id: outbox.deterministic_job_id,
            snapshot_active_job_attempt: 0,
          }],
        };
      }
      if (sql.includes("FROM crawler.proxy_job_dispatch_outbox")) {
        assert.deepEqual(params, ["42", 4]);
        return { rowCount: 1, rows: [outbox] };
      }
      if (sql.includes("FROM crawler.migration_channel_intents")) {
        assert.deepEqual(params, [7, 42, 4]);
        return {
          rowCount: 1,
          rows: [{ migration_intent_id: 7, target_candidate_id: 42, dispatch_attempts: 4 }],
        };
      }
      throw new Error(`CAS loser must not mutate state: ${sql}`);
    },
  };

  const allocated = await allocateChannelSnapshotDispatchOutbox(client, {
    candidate: snapshotCandidate(4),
    expectedGeneration: 3,
    previousJobId: "channel-snapshot__manual-batch__UCsnapshot__g3",
    previousJobAttempt: 2,
    migrationIntentId: 7,
    payload: snapshotPayload(4),
    jobId: outbox.deterministic_job_id,
  });

  assert.equal(allocated.created, false);
  assert.equal(allocated.outbox.dispatch_id, outbox.dispatch_id);
  assert.equal(calls.filter(({ sql }) => sql.includes("UPDATE ")).length, 0);
  assert.equal(
    calls.some(({ sql }) => sql.includes("FROM crawler.migration_channel_intents")),
    true,
  );
});

test("a generation CAS loser fails closed when the G+1 Migration Intent is missing", async () => {
  const outbox = buildChannelSnapshotOutbox({
    candidate: snapshotCandidate(4),
    payload: snapshotPayload(4),
    jobId: "channel-snapshot__manual-batch__UCsnapshot__g4",
  });
  const client = {
    async query(sql) {
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            ...snapshotCandidate(4),
            snapshot_dispatch_generation: "4",
            snapshot_active_job_id: outbox.deterministic_job_id,
            snapshot_active_job_attempt: 0,
          }],
        };
      }
      if (sql.includes("FROM crawler.proxy_job_dispatch_outbox")) {
        return { rowCount: 1, rows: [outbox] };
      }
      if (sql.includes("FROM crawler.migration_channel_intents")) {
        return { rowCount: 0, rows: [] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  await assert.rejects(
    allocateChannelSnapshotDispatchOutbox(client, {
      candidate: snapshotCandidate(4),
      expectedGeneration: 3,
      previousJobId: "channel-snapshot__manual-batch__UCsnapshot__g3",
      previousJobAttempt: 2,
      migrationIntentId: 7,
      payload: snapshotPayload(4),
      jobId: outbox.deterministic_job_id,
    }),
    /Migration Intent conflicts with the allocated Channel snapshot generation/,
  );
});
