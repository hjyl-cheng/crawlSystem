import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeQueryScheduler,
  querySchedulerAllowsDiscovery,
  reconcileAutomaticDiscoveryClosure,
} from "../src/queryScheduler.js";

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

test("automatic finalization repairs a discovery closure interrupted by restart", async () => {
  const calls = [];
  const changed = await reconcileAutomaticDiscoveryClosure(async (sql, params) => {
    calls.push({ sql, params });
    return { rows: [{ dispatch_batch_id: "pipeline:test" }] };
  }, {
    status: "repairing",
    pipeline_cycle_id: "pipeline:test",
  });

  assert.equal(changed, true);
  assert.deepEqual(calls[0].params, ["pipeline:test"]);
  assert.match(calls[0].sql, /discovery_closed_at=COALESCE\(discovery_closed_at,now\(\)\)/);
  assert.match(calls[0].sql, /discovery_closed_at IS NULL/);
});

test("running discovery does not close its batch", async () => {
  let called = false;
  const changed = await reconcileAutomaticDiscoveryClosure(async () => {
    called = true;
    return { rows: [] };
  }, {
    status: "running",
    pipeline_cycle_id: "pipeline:test",
  });

  assert.equal(changed, false);
  assert.equal(called, false);
});
