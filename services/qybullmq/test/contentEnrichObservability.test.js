import assert from "node:assert/strict";
import test from "node:test";
import {
  ContentEnrichMonitor,
  PostgresContentEnrichObservabilityRepository,
} from "../src/contentEnrichObservability.js";

const NOW = "2026-08-23T12:00:00.000Z";

test("Content Enrich monitor exposes backlog age, outcome rates, mutex contention, and alerts", async () => {
  let currentTime = new Date(NOW);
  let snapshotLoads = 0;
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
    repository: {
      async loadSnapshot() {
        snapshotLoads += 1;
        return snapshot;
      },
    },
    now: () => new Date(currentTime),
    windowMs: 5 * 60_000,
    sampleIntervalMs: 60_000,
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

  currentTime = new Date("2026-08-23T12:00:15.000Z");
  const repeated = await monitor.observe({
    queueCounts: { waiting: 7, active: 1 },
    dispatchSummary: { reason: "dispatch_locked" },
  });
  assert.equal(snapshotLoads, 1);
  assert.equal(repeated.observed_at, raised.observed_at);
  assert.equal(repeated.queue_open_jobs, 8);
  assert.deepEqual(repeated.alerts.notifications, []);

  snapshot = {
    ...snapshot,
    task_counts: { ...snapshot.task_counts, queued: 1, leased: 0, running: 0, failed: 0 },
    oldest_queued_at: "2026-08-23T11:59:00.000Z",
  };
  currentTime = new Date("2026-08-23T12:01:00.000Z");
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
  assert.equal(snapshotLoads, 2);
  assert.equal(resolved.observed_at, "2026-08-23T12:01:00.000Z");
});

test("Content Enrich monitor times out a stalled refresh and serves the last database snapshot", async () => {
  let currentTime = new Date(NOW);
  let snapshotLoads = 0;
  let releaseStalledRefresh = null;
  const snapshot = {
    task_counts: {
      queued: 7,
      leased: 0,
      running: 0,
      failed: 1,
      terminal: 0,
      dead_letter: 0,
      done: 3,
      skipped: 0,
    },
    oldest_queued_at: "2026-08-23T11:00:00.000Z",
    outcome_counts: {
      claimed: 4,
      success: 3,
      retry: 1,
      terminal: 0,
      dead_letter: 0,
    },
  };
  const monitor = new ContentEnrichMonitor({
    repository: {
      async loadSnapshot() {
        snapshotLoads += 1;
        if (snapshotLoads === 1) return snapshot;
        return new Promise((resolve) => {
          releaseStalledRefresh = () => resolve(snapshot);
        });
      },
    },
    now: () => new Date(currentTime),
    sampleIntervalMs: 1_000,
    queryTimeoutMs: 5,
  });

  const initial = await monitor.observe({ queueCounts: { waiting: 1 } });
  currentTime = new Date("2026-08-23T12:00:01.000Z");
  const controllerDeadline = Symbol("controller deadline");
  const refresh = monitor.observe({
    queueCounts: { waiting: 4, active: 2 },
    dispatchSummary: { reason: "dispatch_locked" },
  });
  const observed = await Promise.race([
    refresh,
    new Promise((resolve) => setTimeout(() => resolve(controllerDeadline), 100)),
  ]);

  currentTime = new Date("2026-08-23T12:00:02.000Z");
  const repeated = await monitor.observe({ queueCounts: { waiting: 8 } });
  releaseStalledRefresh?.();
  await refresh;

  assert.notEqual(observed, controllerDeadline, "a stalled metrics query blocked the Controller");
  assert.equal(snapshotLoads, 2);
  assert.equal(observed.observed_at, initial.observed_at);
  assert.deepEqual(observed.task_counts, initial.task_counts);
  assert.equal(observed.queue_open_jobs, 6);
  assert.equal(observed.mutex_contention, true);
  assert.equal(repeated.queue_open_jobs, 8);
  assert.deepEqual(repeated.task_counts, initial.task_counts);
});

test("Content Enrich monitor serves the last database snapshot when a refresh fails", async () => {
  let currentTime = new Date(NOW);
  let snapshotLoads = 0;
  const snapshot = {
    task_counts: { queued: 5, failed: 2, done: 1 },
    oldest_queued_at: "2026-08-23T11:00:00.000Z",
    outcome_counts: { claimed: 3, success: 1, retry: 2 },
  };
  const monitor = new ContentEnrichMonitor({
    repository: {
      async loadSnapshot() {
        snapshotLoads += 1;
        if (snapshotLoads === 1) return snapshot;
        throw new Error("isolated metrics database failure");
      },
    },
    now: () => new Date(currentTime),
    sampleIntervalMs: 1_000,
    queryTimeoutMs: 50,
  });

  const initial = await monitor.observe({ queueCounts: { waiting: 1 } });
  currentTime = new Date("2026-08-23T12:00:01.000Z");
  const observed = await monitor.observe({ queueCounts: { waiting: 3, active: 1 } });

  assert.equal(snapshotLoads, 2);
  assert.equal(observed.observed_at, initial.observed_at);
  assert.deepEqual(observed.task_counts, initial.task_counts);
  assert.equal(observed.queue_open_jobs, 4);
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
  assert.match(calls[0].sql, /status IN \('claimed','checkpointed'\)/);
  assert.doesNotMatch(calls[0].sql, /status IN \('completed','failed'\)/);
});

test("PostgreSQL observability repository bounds the aggregate with a transaction-local timeout", async () => {
  const queries = [];
  let transactions = 0;
  const repository = new PostgresContentEnrichObservabilityRepository({
    queryFn: async () => {
      throw new Error("unbounded query path was used");
    },
    withTransaction: async (action) => {
      transactions += 1;
      return action({
        async query(sql, params = []) {
          queries.push({ sql, params });
          if (sql.includes("set_config('statement_timeout'")) return { rows: [{}] };
          return {
            rows: [{
              queued: "0",
              leased: "0",
              running: "0",
              failed: "0",
              terminal: "0",
              dead_letter: "0",
              done: "0",
              skipped: "0",
              oldest_queued_at: null,
              claimed: "0",
              success: "0",
              retry: "0",
              outcome_terminal: "0",
              outcome_dead_letter: "0",
            }],
          };
        },
      });
    },
    queryTimeoutMs: 1_234,
  });

  await repository.loadSnapshot({ windowMs: 300_000 });

  assert.equal(transactions, 1);
  assert.equal(queries.length, 2);
  assert.match(queries[0].sql, /set_config\('statement_timeout'/);
  assert.deepEqual(queries[0].params, ["1234ms"]);
  assert.match(queries[1].sql, /FROM crawler\.content_enrich_tasks/);
  assert.deepEqual(queries[1].params, [300_000]);
});
