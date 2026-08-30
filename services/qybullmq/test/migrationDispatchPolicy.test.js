import assert from "node:assert/strict";
import test from "node:test";
import {
  channelDispatchCapacity,
  channelQueuePressure,
  channelSnapshotPayload,
  migrationBatchCompletion,
  migrationBatchHasOpenWork,
} from "../src/migrationDispatchPolicy.js";

test("migration dispatcher counts every non-terminal channel queue state", () => {
  const counts = {
    waiting: 2,
    active: 1,
    delayed: 1,
    prioritized: 1,
    paused: 0,
    "waiting-children": 1,
    completed: 100,
    failed: 20,
  };
  assert.equal(channelQueuePressure(counts), 6);
  assert.equal(channelDispatchCapacity(counts, { highWater: 6, refill: 2 }), 0);
});

test("migration dispatcher refills two jobs without crossing the high-water mark", () => {
  assert.equal(channelDispatchCapacity({ waiting: 1, active: 2 }, { highWater: 6, refill: 2 }), 2);
  assert.equal(channelDispatchCapacity({ waiting: 4, active: 1 }, { highWater: 6, refill: 2 }), 1);
  assert.equal(channelDispatchCapacity({}, { highWater: 6, refill: 2, paused: true }), 0);
});

test("migration watcher waits for the latest Full Run after Candidate validation", () => {
  assert.equal(migrationBatchHasOpenWork({ open_count: 1, open_run_count: 0 }), true);
  assert.equal(migrationBatchHasOpenWork({ open_count: 0, open_run_count: 1 }), true);
  assert.equal(migrationBatchHasOpenWork({ open_count: 0, open_run_count: 0 }), false);
});

test("99 accepted plus one system failure completes with explicit Batch outcome", () => {
  assert.deepEqual(migrationBatchCompletion({
    total: 100,
    accepted: 99,
    rejected: 0,
    failed: 1,
    systemFailures: 1,
  }), {
    status: "completed",
    outcome: "completed_with_system_failures",
    total: 100,
    accepted: 99,
    rejected: 0,
    failed: 1,
  });
});

test("an accepted Candidate can retain a system-failure Batch outcome", () => {
  assert.deepEqual(migrationBatchCompletion({
    total: 1,
    accepted: 1,
    rejected: 0,
    failed: 0,
    systemFailures: 1,
  }), {
    status: "completed",
    outcome: "completed_with_system_failures",
    total: 1,
    accepted: 1,
    rejected: 0,
    failed: 0,
  });
});

test("migration channel jobs start at the post-Discover channel snapshot contract", () => {
  assert.deepEqual(channelSnapshotPayload({
    candidate_id: "42",
    migration_intent_id: "25",
    channel_id: "UC1234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
    snapshot_dispatch_generation: "3",
  }, "legacy-results-pilot-30-v1"), {
    candidate_id: 42,
    migration_intent_id: 25,
    dispatch_generation: 3,
    dispatch_batch_id: "legacy-results-pilot-30-v1",
    channel_id: "UC1234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
    crawl_mode: "full",
    query_id: null,
    query_text: "results.db migration",
    pipeline_cycle_id: "legacy-results-pilot-30-v1",
    enforce_min_subscribers: true,
    min_subscriber_count: 1000,
    reject_if_no_recent_content: true,
  });
});
