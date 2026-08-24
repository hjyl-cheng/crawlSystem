import assert from "node:assert/strict";
import test from "node:test";
import { dispatchContentEnrichForController } from "../src/controllerContentEnrichDispatch.js";

test("Content Enrich dispatch failure is recorded without blocking later Controller work", async () => {
  const dispatchFailure = new Error("isolated Enrich database outage");
  const actions = [];
  const errors = [];
  let laterWorkRan = false;

  const result = await dispatchContentEnrichForController({
    dispatcher: {
      async dispatchAvailable() {
        throw dispatchFailure;
      },
    },
    actions,
    logger: { error: (entry) => errors.push(JSON.parse(entry)) },
  });
  laterWorkRan = true;

  assert.equal(result.ok, false);
  assert.equal(laterWorkRan, true);
  assert.deepEqual(actions, [{
    action: "dispatch-content-enrich-failed",
    error_message: "isolated Enrich database outage",
  }]);
  assert.deepEqual(errors, [{
    event: "content_enrich_dispatch_failed",
    error: "isolated Enrich database outage",
  }]);
});

test("successful Content Enrich dispatch preserves the Controller action summary", async () => {
  const actions = [];
  const summary = {
    enqueued: 2,
    recovered: 1,
    existing: 0,
    released: 0,
    failed: 0,
  };

  const result = await dispatchContentEnrichForController({
    dispatcher: { dispatchAvailable: async () => summary },
    actions,
  });

  assert.deepEqual(result, { ok: true, summary });
  assert.deepEqual(actions, [{ action: "dispatch-content-enrich", ...summary }]);
});

test("Controller persists Content Enrich operational metrics and threshold alerts", async () => {
  const actions = [];
  const errors = [];
  const operational = {
    observed_at: "2026-08-23T12:00:00.000Z",
    task_backlog: 46_103,
    oldest_queued_age_seconds: 172_800,
    mutex_contention: true,
    outcome_rates: {
      window_seconds: 300,
      claimed_per_minute: 12,
      success_per_minute: 9,
      retry_per_minute: 2,
      terminal_per_minute: 1,
    },
    alerts: {
      active: [
        { code: "content_enrich_backlog_high", value: 46_103, threshold: 10_000 },
      ],
      notifications: [
        { code: "content_enrich_backlog_high", state: "raised", value: 46_103, threshold: 10_000 },
      ],
    },
  };

  const result = await dispatchContentEnrichForController({
    dispatcher: {
      dispatchAvailable: async () => ({
        enqueued: 0,
        recovered: 0,
        existing: 0,
        released: 0,
        failed: 0,
        reason: "dispatch_locked",
      }),
    },
    monitor: { observe: async () => operational },
    queueCounts: { waiting: 4, active: 1 },
    actions,
    logger: { error: (entry) => errors.push(JSON.parse(entry)) },
  });

  assert.equal(result.operational, operational);
  assert.deepEqual(actions, [{
    action: "content-enrich-alert",
    ...operational.alerts.notifications[0],
  }]);
  assert.deepEqual(errors, [{
    event: "content_enrich_alert",
    ...operational.alerts.notifications[0],
  }]);
});

test("Content Enrich observation failure is isolated from later Controller work", async () => {
  const actions = [];
  const errors = [];
  let laterWorkRan = false;

  const result = await dispatchContentEnrichForController({
    dispatcher: { dispatchAvailable: async () => ({ enqueued: 0 }) },
    monitor: {
      async observe() {
        throw new Error("isolated Enrich observability outage");
      },
    },
    actions,
    logger: { error: (entry) => errors.push(JSON.parse(entry)) },
  });
  laterWorkRan = true;

  assert.equal(result.ok, true);
  assert.equal(result.operational, null);
  assert.equal(laterWorkRan, true);
  assert.deepEqual(actions, [{
    action: "observe-content-enrich-failed",
    error_message: "isolated Enrich observability outage",
  }]);
  assert.deepEqual(errors, [{
    event: "content_enrich_observation_failed",
    error: "isolated Enrich observability outage",
  }]);
});
