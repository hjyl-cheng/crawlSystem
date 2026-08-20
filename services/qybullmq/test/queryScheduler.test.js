import assert from "node:assert/strict";
import test from "node:test";
import { normalizeQueryScheduler, querySchedulerAllowsDiscovery } from "../src/queryScheduler.js";

test("automatic finalization states do not schedule more discovery", () => {
  for (const status of ["finishing", "repairing"]) {
    const scheduler = normalizeQueryScheduler({ status, stop_reason: "upstream_drained" });
    assert.equal(scheduler.status, status);
    assert.equal(scheduler.stop_reason, "upstream_drained");
    assert.equal(querySchedulerAllowsDiscovery(scheduler), false);
  }
  assert.equal(querySchedulerAllowsDiscovery({ status: "running" }), true);
});

test("pause state retains the phase that should resume", () => {
  const scheduler = normalizeQueryScheduler({
    status: "paused",
    paused_from_status: "repairing",
    completed_at: "2026-07-12T00:00:00Z",
    pipeline_cycle_id: "pipeline:test",
  });
  assert.equal(scheduler.paused_from_status, "repairing");
  assert.equal(scheduler.completed_at, "2026-07-12T00:00:00Z");
  assert.equal(scheduler.pipeline_cycle_id, "pipeline:test");
});
