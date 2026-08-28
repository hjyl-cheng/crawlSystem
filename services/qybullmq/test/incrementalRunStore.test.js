import assert from "node:assert/strict";
import test from "node:test";
import { IncrementalRunStore } from "../src/incrementalRunStore.js";

function plan() {
  return {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: "incremental__UCutc__20260720__clock_7__5d62c032cbbc",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_mode: "standard",
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: "UCutc",
    task_mask: { about: true, video: false, agent: false },
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
    clock_version: 7,
    policy_version: "v16-rule-1",
    planner_config_version: "video-plan-1",
  };
}

test("incremental Run stores the precise UTC scheduled_at", async () => {
  let insertSql = null;
  let insertParams = null;
  const store = new IncrementalRunStore({
    withTransaction: async (action) => action({
      async query(sql, params) {
        insertSql = sql;
        insertParams = params;
        return {
          rowCount: 1,
          rows: [{ run_id: `incremental:${plan().plan_id}`, status: "running" }],
        };
      },
    }),
  });

  const result = await store.claim(plan());

  assert.equal(result.created, true);
  assert.equal(insertParams[3], "2026-07-20");
  assert.equal(insertParams[5], "2026-07-20T13:25:40.000Z");
  assert.match(insertSql, /\$6::timestamptz/);
  assert.deepEqual(JSON.parse(insertParams[11]).domains, {
    about: { status: "pending" },
    video: { status: "not_due" },
    agent: { status: "not_due" },
  });
});

test("successful retry clears a previous failed detail status", async () => {
  let finishSql = null;
  const store = new IncrementalRunStore({
    withTransaction: async (action) => action({
      async query(sql) {
        finishSql = sql;
        return {
          rowCount: 1,
          rows: [{
            run_id: "incremental:retry",
            status: "done",
            detail_status: "done",
            error_message: null,
          }],
        };
      },
    }),
  });

  const result = await store.finish("incremental:retry");

  assert.equal(result.status, "done");
  assert.equal(result.detail_status, "done");
  assert.equal(result.error_message, null);
  assert.match(finishSql, /detail_status='done'/);
  assert.doesNotMatch(
    finishSql,
    /detail_status=CASE\s+WHEN detail_status='failed' THEN detail_status ELSE 'done' END/,
  );
});
