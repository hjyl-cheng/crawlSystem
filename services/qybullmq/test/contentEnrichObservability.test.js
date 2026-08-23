import assert from "node:assert/strict";
import test from "node:test";
import {
  ContentEnrichMonitor,
  PostgresContentEnrichObservabilityRepository,
} from "../src/contentEnrichObservability.js";

const NOW = "2026-08-23T12:00:00.000Z";

test("Content Enrich monitor exposes backlog age, outcome rates, mutex contention, and alerts", async () => {
  let snapshot = {
    task_counts: {
      queued: 100,
      leased: 5,
      running: 3,
      failed: 12,
      terminal: 4,
      dead_letter: 2,
      done: 80,
      skipped: 1,
    },
    oldest_queued_at: "2026-08-23T10:00:00.000Z",
    outcome_counts: {
      claimed: 60,
      success: 45,
      retry: 10,
      terminal: 3,
      dead_letter: 2,
    },
  };
  const monitor = new ContentEnrichMonitor({
    repository: { loadSnapshot: async () => snapshot },
    now: () => new Date(NOW),
    windowMs: 5 * 60_000,
    backlogAlertThreshold: 100,
    queuedAgeAlertSeconds: 3_600,
    alertRepeatMs: 60 * 60_000,
  });

  const raised = await monitor.observe({
    queueCounts: { waiting: 4, active: 1, completed: 20 },
    dispatchSummary: { reason: "dispatch_locked" },
  });

  assert.deepEqual(raised.task_counts, snapshot.task_counts);
  assert.equal(raised.task_backlog, 120);
  assert.equal(raised.oldest_queued_age_seconds, 7_200);
  assert.equal(raised.queue_open_jobs, 5);
  assert.equal(raised.mutex_contention, true);
  assert.deepEqual(raised.outcome_rates, {
    window_seconds: 300,
    claimed: 60,
    success: 45,
    retry: 10,
    terminal: 3,
    dead_letter: 2,
    claimed_per_minute: 12,
    success_per_minute: 9,
    retry_per_minute: 2,
    terminal_per_minute: 0.6,
    dead_letter_per_minute: 0.4,
    success_ratio: 0.75,
    retry_ratio: 10 / 60,
    terminal_ratio: 3 / 60,
    dead_letter_ratio: 2 / 60,
  });
  assert.deepEqual(
    raised.alerts.notifications.map(({ code, state }) => ({ code, state })),
    [
      { code: "content_enrich_backlog_high", state: "raised" },
      { code: "content_enrich_queued_age_high", state: "raised" },
    ],
  );

  const repeated = await monitor.observe({
    queueCounts: { waiting: 4, active: 1 },
    dispatchSummary: { reason: "dispatch_locked" },
  });
  assert.deepEqual(repeated.alerts.notifications, []);

  snapshot = {
    ...snapshot,
    task_counts: { ...snapshot.task_counts, queued: 1, leased: 0, running: 0, failed: 0 },
    oldest_queued_at: "2026-08-23T11:59:00.000Z",
  };
  const resolved = await monitor.observe({
    queueCounts: {},
    dispatchSummary: { reason: "high_water" },
  });
  assert.deepEqual(
    resolved.alerts.notifications.map(({ code, state }) => ({ code, state })),
    [
      { code: "content_enrich_backlog_high", state: "resolved" },
      { code: "content_enrich_queued_age_high", state: "resolved" },
    ],
  );
});

test("PostgreSQL observability repository returns player-refresh state and Worker outcome totals", async () => {
  const calls = [];
  const repository = new PostgresContentEnrichObservabilityRepository({
    queryFn: async (sql, params) => {
      calls.push({ sql, params });
      return {
        rows: [{
          queued: "101",
          leased: "5",
          running: "3",
          failed: "12",
          terminal: "4",
          dead_letter: "2",
          done: "80",
          skipped: "1",
          oldest_queued_at: "2026-08-20T12:00:00.000Z",
          claimed: "60",
          success: "45",
          retry: "10",
          outcome_terminal: "3",
          outcome_dead_letter: "2",
        }],
      };
    },
  });

  const snapshot = await repository.loadSnapshot({ windowMs: 300_000 });

  assert.deepEqual(snapshot, {
    task_counts: {
      queued: 101,
      leased: 5,
      running: 3,
      failed: 12,
      terminal: 4,
      dead_letter: 2,
      done: 80,
      skipped: 1,
    },
    oldest_queued_at: "2026-08-20T12:00:00.000Z",
    outcome_counts: {
      claimed: 60,
      success: 45,
      retry: 10,
      terminal: 3,
      dead_letter: 2,
    },
  });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].params, [300_000]);
});
