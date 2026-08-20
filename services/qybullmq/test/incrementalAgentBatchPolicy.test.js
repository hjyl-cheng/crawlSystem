import assert from "node:assert/strict";
import test from "node:test";
import { decideIncrementalAgentBatch } from "../src/incrementalAgentBatchPolicy.js";

test("Agent dispatches immediately only when 30 due Channels are accumulated", () => {
  assert.deepEqual(decideIncrementalAgentBatch({ pendingChannelCount: 30 }), {
    dispatch: true,
    limit: 30,
    partial: false,
    reason: "full_batch",
  });
  assert.equal(decideIncrementalAgentBatch({ pendingChannelCount: 29 }).dispatch, false);
});

test("Agent tail dispatches immediately when every active Agent Plan is registered", () => {
  const now = new Date("2026-07-20T12:30:00.000Z");
  assert.deepEqual(decideIncrementalAgentBatch({
    pendingChannelCount: 7,
    unregisteredPlanCount: 0,
    newestPendingAt: "2026-07-20T12:29:59.000Z",
    now,
    tailQuietMs: 10 * 60 * 1000,
  }), {
    dispatch: true,
    limit: 7,
    partial: true,
    reason: "planned_tail_ready",
  });
});

test("Agent tail waits while active Agent Plans have not registered requests", () => {
  const now = new Date("2026-07-20T12:30:00.000Z");
  assert.deepEqual(decideIncrementalAgentBatch({
    pendingChannelCount: 7,
    unregisteredPlanCount: 2,
    newestPendingAt: "2026-07-20T12:20:00.000Z",
    now,
    tailQuietMs: 10 * 60 * 1000,
  }), {
    dispatch: false,
    limit: 0,
    partial: false,
    reason: "awaiting_planned_requests",
  });
});

test("Agent quiet window remains a fallback when Plan state is unavailable", () => {
  const now = new Date("2026-07-20T12:30:00.000Z");
  assert.equal(decideIncrementalAgentBatch({
    pendingChannelCount: 7,
    newestPendingAt: "2026-07-20T12:20:01.000Z",
    now,
    tailQuietMs: 10 * 60 * 1000,
  }).dispatch, false);

  assert.equal(decideIncrementalAgentBatch({
    pendingChannelCount: 7,
    newestPendingAt: "2026-07-20T12:20:00.000Z",
    now,
    tailQuietMs: 10 * 60 * 1000,
  }).reason, "tail_quiet_window_elapsed");
});

test("Agent batching counts due Channels, not requests for the same Channel", () => {
  const decision = decideIncrementalAgentBatch({ pendingChannelCount: 1 });
  assert.equal(decision.reason, "collecting_tail");
  assert.equal(decision.dispatch, false);
});
