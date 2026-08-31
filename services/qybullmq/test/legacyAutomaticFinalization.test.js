import assert from "node:assert/strict";
import test from "node:test";

import { resumeLegacyAutomaticFinalizationWithFence } from "../src/legacyAutomaticFinalization.js";

function legacyScheduler() {
  return {
    status: "stopped",
    stop_reason: "no_schedulable_query",
    pipeline_cycle_id: "legacy-cycle",
  };
}

test("legacy automatic Finalization stays stopped while system recovery owns shared consumers", async () => {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ value_json: legacyScheduler() }] };
      }
      if (sql.includes("FROM crawler.migration_system_retry_items")) {
        return {
          rowCount: 1,
          rows: [{
            system_retry_id: "801",
            candidate_id: "482",
            failed_dispatch_batch_id: "legacy-cycle",
            status: "retrying",
          }],
        };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const result = await resumeLegacyAutomaticFinalizationWithFence({
    scheduler: legacyScheduler(),
    withTransaction: (action) => action(client),
  });

  assert.equal(result.resumed, false);
  assert.equal(result.scheduler.status, "stopped");
  assert.equal(result.admission.code, "migration_system_retry_recovery_active");
  assert.equal(calls.some(({ sql }) => sql.includes("UPDATE crawler.settings")), false);
});

test("legacy automatic Finalization updates the Scheduler under its row lock when recovery is idle", async () => {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ value_json: legacyScheduler() }] };
      }
      if (sql.includes("FROM crawler.migration_system_retry_items")) {
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("UPDATE crawler.settings")) {
        return {
          rowCount: 1,
          rows: [{
            value_json: {
              ...legacyScheduler(),
              ...JSON.parse(params[1]),
            },
          }],
        };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const result = await resumeLegacyAutomaticFinalizationWithFence({
    scheduler: legacyScheduler(),
    withTransaction: (action) => action(client),
    now: "2026-08-30T01:00:00.000Z",
  });

  assert.equal(result.resumed, true);
  assert.equal(result.scheduler.status, "finishing");
  assert.equal(result.scheduler.stop_reason, "upstream_drained");
  assert.ok(calls.findIndex(({ sql }) => sql.includes("FOR UPDATE"))
    < calls.findIndex(({ sql }) => sql.includes("UPDATE crawler.settings")));
});
