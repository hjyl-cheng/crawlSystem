import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  querySchedulerPauseTransition,
  querySchedulerResumeTransition,
  querySchedulerStartTransition,
  querySchedulerStopTransition,
  stopQuerySchedulerBatch,
  updateQuerySchedulerWithMigrationFence,
} from "./querySchedulerControl.js";

function normalized(value = {}) {
  return {
    status: String(value.status ?? "stopped"),
    stop_reason: value.stop_reason ?? null,
    pipeline_cycle_id: value.pipeline_cycle_id ?? null,
    completed_at: value.completed_at ?? null,
    updated_at: value.updated_at ?? null,
    updated_by: value.updated_by ?? null,
  };
}

function fakePool({ scheduler, activeRetry = null }) {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql === "BEGIN" || sql === "COMMIT" || sql === "ROLLBACK") {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ value_json: scheduler }] };
      }
      if (sql.includes("FROM crawler.migration_system_retry_items")) {
        return { rowCount: activeRetry == null ? 0 : 1, rows: activeRetry == null ? [] : [activeRetry] };
      }
      if (sql.includes("UPDATE crawler.settings")) {
        return { rowCount: 1, rows: [{ value_json: JSON.parse(params[1]) }] };
      }
      if (sql.includes("UPDATE crawler.query_dispatch_batches")) {
        return { rowCount: 1, rows: [{ dispatch_batch_id: params[0] }] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
    release() {},
  };
  return {
    calls,
    pool: { async connect() { return client; } },
  };
}

test("Dashboard cannot start a new Scheduler cycle while controlled recovery is active", async () => {
  const observed = {
    status: "stopped",
    stop_reason: "pipeline_complete",
    pipeline_cycle_id: "completed-batch",
    completed_at: "2026-08-30T00:00:00.000Z",
  };
  const fixture = fakePool({
    scheduler: observed,
    activeRetry: {
      system_retry_id: "801",
      candidate_id: "482",
      failed_dispatch_batch_id: "completed-batch",
      status: "dispatched",
    },
  });
  const result = await updateQuerySchedulerWithMigrationFence({
    pool: fixture.pool,
    normalize: normalized,
    now: "2026-08-30T01:00:00.000Z",
    mutate: (current) => {
      const transition = querySchedulerStartTransition(current, { expected: observed });
      return transition.allowed
        ? {
          startsNewCycle: transition.startsNewCycle,
          settings: { ...current, status: "running", pipeline_cycle_id: "next-batch" },
        }
        : { rejection: transition };
    },
  });

  assert.equal(result.updated, false);
  assert.equal(result.admission.code, "migration_system_retry_recovery_active");
  assert.equal(result.admission.active_system_retry.system_retry_id, 801);
  assert.equal(fixture.calls.some(({ sql }) => sql.includes("UPDATE crawler.settings")), false);
  assert.deepEqual(
    fixture.calls.find(({ sql }) => sql.includes("migration_system_retry_items")).params,
    [["retrying", "dispatched"]],
  );
});

test("Dashboard may resume the same Scheduler cycle without claiming a new recovery admission", async () => {
  const observed = {
    status: "paused",
    stop_reason: null,
    pipeline_cycle_id: "current-batch",
    completed_at: null,
  };
  const fixture = fakePool({
    scheduler: observed,
    activeRetry: {
      system_retry_id: "801",
      candidate_id: "482",
      failed_dispatch_batch_id: "current-batch",
      status: "dispatched",
    },
  });
  const result = await updateQuerySchedulerWithMigrationFence({
    pool: fixture.pool,
    normalize: normalized,
    mutate: (current) => {
      const transition = querySchedulerResumeTransition(current, { expected: observed });
      return transition.allowed
        ? {
          startsNewCycle: transition.startsNewCycle,
          settings: { ...current, status: transition.resumeStatus },
        }
        : { rejection: transition };
    },
  });

  assert.equal(result.updated, true);
  assert.equal(result.scheduler.status, "running");
  assert.equal(
    fixture.calls.some(({ sql }) => sql.includes("migration_system_retry_items")),
    false,
  );
  assert.deepEqual(
    fixture.calls.map(({ sql }) => sql === "BEGIN" || sql === "COMMIT" ? sql : null).filter(Boolean),
    ["BEGIN", "COMMIT"],
  );
  assert.ok(fixture.calls.findIndex(({ sql }) => sql.includes("FOR UPDATE"))
    < fixture.calls.findIndex(({ sql }) => sql.includes("UPDATE crawler.settings")));
});

test("Dashboard cannot turn a concurrently completed Scheduler back into a resumed cycle", async () => {
  const observed = {
    status: "paused",
    stop_reason: null,
    pipeline_cycle_id: "current-batch",
    completed_at: null,
  };
  const fixture = fakePool({
    scheduler: {
      status: "stopped",
      stop_reason: "pipeline_complete",
      pipeline_cycle_id: "current-batch",
      completed_at: "2026-08-30T02:00:00.000Z",
    },
    activeRetry: {
      system_retry_id: "801",
      candidate_id: "482",
      failed_dispatch_batch_id: "current-batch",
      status: "dispatched",
    },
  });

  const result = await updateQuerySchedulerWithMigrationFence({
    pool: fixture.pool,
    normalize: normalized,
    mutate: (current) => {
      const transition = querySchedulerResumeTransition(current, { expected: observed });
      return transition.allowed
        ? {
          startsNewCycle: transition.startsNewCycle,
          settings: { ...current, status: transition.resumeStatus },
        }
        : { rejection: transition };
    },
  });

  assert.equal(result.updated, false);
  assert.equal(result.rejection.code, "query_scheduler_resume_state_changed");
  assert.equal(result.scheduler.status, "stopped");
  assert.equal(fixture.calls.some(({ sql }) => sql.includes("migration_system_retry_items")), false);
  assert.equal(fixture.calls.some(({ sql }) => sql.includes("UPDATE crawler.settings")), false);
});

test("Dashboard cannot replace a Scheduler cycle that started before its row lock", async () => {
  const observed = {
    status: "stopped",
    stop_reason: "pipeline_complete",
    pipeline_cycle_id: "completed-batch",
    completed_at: "2026-08-30T02:00:00.000Z",
    updated_at: "2026-08-30T02:00:00.000Z",
  };
  const fixture = fakePool({
    scheduler: {
      status: "running",
      stop_reason: null,
      pipeline_cycle_id: "concurrent-batch",
      completed_at: null,
      updated_at: "2026-08-30T02:01:00.000Z",
    },
  });

  const result = await updateQuerySchedulerWithMigrationFence({
    pool: fixture.pool,
    normalize: normalized,
    mutate: (current) => {
      const transition = querySchedulerStartTransition(current, { expected: observed });
      return transition.allowed
        ? {
          startsNewCycle: transition.startsNewCycle,
          settings: { ...current, status: "running", pipeline_cycle_id: "requested-batch" },
        }
        : { rejection: transition };
    },
  });

  assert.equal(result.updated, false);
  assert.equal(result.rejection.code, "query_scheduler_start_state_changed");
  assert.equal(result.scheduler.pipeline_cycle_id, "concurrent-batch");
  assert.equal(fixture.calls.some(({ sql }) => sql.includes("migration_system_retry_items")), false);
  assert.equal(fixture.calls.some(({ sql }) => sql.includes("UPDATE crawler.settings")), false);
});

for (const [action, transition] of [
  ["pause", querySchedulerPauseTransition],
  ["stop", querySchedulerStopTransition],
]) {
  test(`Dashboard cannot ${action} over a concurrently completed Scheduler`, async () => {
    const observed = {
      status: "running",
      stop_reason: null,
      pipeline_cycle_id: "completing-batch",
      completed_at: null,
      updated_at: "2026-08-30T02:00:00.000Z",
    };
    const fixture = fakePool({
      scheduler: {
        status: "stopped",
        stop_reason: "pipeline_complete",
        pipeline_cycle_id: "completing-batch",
        completed_at: "2026-08-30T02:01:00.000Z",
        updated_at: "2026-08-30T02:01:00.000Z",
      },
      activeRetry: {
        system_retry_id: "801",
        candidate_id: "482",
        failed_dispatch_batch_id: "completing-batch",
        status: "dispatched",
      },
    });

    const result = await updateQuerySchedulerWithMigrationFence({
      pool: fixture.pool,
      normalize: normalized,
      mutate: (current) => {
        const proposed = transition(current, { expected: observed });
        return proposed.allowed
          ? { startsNewCycle: false, settings: { ...current, status: action === "pause" ? "paused" : "stopped" } }
          : { rejection: proposed };
      },
    });

    assert.equal(result.updated, false);
    assert.equal(result.rejection.code, `query_scheduler_${action}_state_changed`);
    assert.equal(result.scheduler.stop_reason, "pipeline_complete");
    assert.equal(result.scheduler.completed_at, "2026-08-30T02:01:00.000Z");
    assert.equal(fixture.calls.some(({ sql }) => sql.includes("migration_system_retry_items")), false);
    assert.equal(fixture.calls.some(({ sql }) => sql.includes("UPDATE crawler.settings")), false);
  });
}

test("Dashboard pause and stop transitions only accept live execution sources", () => {
  assert.equal(querySchedulerPauseTransition({ status: "finishing" }).allowed, true);
  assert.equal(querySchedulerPauseTransition({ status: "stopped" }).allowed, false);
  assert.equal(querySchedulerStopTransition({ status: "paused" }).allowed, true);
  assert.equal(querySchedulerStopTransition({
    status: "stopped",
    stop_reason: "pipeline_complete",
    completed_at: "2026-08-30T02:01:00.000Z",
  }).allowed, false);
});

test("Dashboard commits Scheduler and Batch stop under one row-lock transaction", async () => {
  const observed = {
    status: "running",
    stop_reason: null,
    pipeline_cycle_id: "running-batch",
    completed_at: null,
    updated_at: "2026-08-30T03:00:00.000Z",
  };
  const fixture = fakePool({ scheduler: observed });
  const result = await updateQuerySchedulerWithMigrationFence({
    pool: fixture.pool,
    normalize: normalized,
    mutate: (current) => {
      const transition = querySchedulerStopTransition(current, { expected: observed });
      return transition.allowed
        ? {
          startsNewCycle: false,
          settings: { ...current, status: "stopped", stop_reason: "user_requested" },
        }
        : { rejection: transition };
    },
    afterUpdate: ({ client, scheduler }) => stopQuerySchedulerBatch(client, scheduler),
  });

  assert.equal(result.updated, true);
  const statements = fixture.calls.map(({ sql }) => sql);
  const schedulerUpdate = statements.findIndex((sql) => sql.includes("UPDATE crawler.settings"));
  const batchUpdate = statements.findIndex((sql) => sql.includes("UPDATE crawler.query_dispatch_batches"));
  const commit = statements.indexOf("COMMIT");
  assert.ok(schedulerUpdate >= 0 && schedulerUpdate < batchUpdate);
  assert.ok(batchUpdate < commit);
  assert.deepEqual(fixture.calls[batchUpdate].params, ["running-batch"]);
});

test("Dashboard route preserves Batch state on pause and stops it atomically on stop", async () => {
  const server = await readFile(new URL("./server.js", import.meta.url), "utf8");
  const pauseStart = server.indexOf('} else if (action === "pause") {');
  const stopStart = server.indexOf('} else if (action === "stop") {', pauseStart);
  const unknownActionStart = server.indexOf("} else {", stopStart);
  assert.ok(pauseStart >= 0 && stopStart > pauseStart && unknownActionStart > stopStart);

  const pauseHandler = server.slice(pauseStart, stopStart);
  const stopHandler = server.slice(stopStart, unknownActionStart);
  assert.doesNotMatch(pauseHandler, /stopQuerySchedulerBatch/);
  assert.match(
    stopHandler,
    /afterUpdate:\s*\(\{ client, scheduler \}\) => stopQuerySchedulerBatch\(client, scheduler\)/,
  );
});
