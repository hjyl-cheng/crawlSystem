import assert from "node:assert/strict";
import test from "node:test";

import {
  allocateChannelSnapshotDispatchOutbox,
  allocateDiscoveredChannelSnapshotDispatches,
  buildChannelSnapshotOutbox,
  stageChannelSnapshotOutbox,
} from "../src/channelSnapshotDispatch.js";

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
            snapshot_active_job_id: null,
            snapshot_active_job_attempt: null,
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
      throw new Error(`CAS loser must not mutate state: ${sql}`);
    },
  };

  const allocated = await allocateChannelSnapshotDispatchOutbox(client, {
    candidate: snapshotCandidate(4),
    expectedGeneration: 3,
    previousJobId: "channel-snapshot__manual-batch__UCsnapshot__g3",
    migrationIntentId: 7,
    payload: snapshotPayload(4),
    jobId: outbox.deterministic_job_id,
  });

  assert.equal(allocated.created, false);
  assert.equal(allocated.outbox.dispatch_id, outbox.dispatch_id);
  assert.equal(calls.filter(({ sql }) => sql.includes("UPDATE ")).length, 0);
});
