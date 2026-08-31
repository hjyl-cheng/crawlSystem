import assert from "node:assert/strict";
import test from "node:test";

import { maybeStartMetadataDiscoveryCycle } from "../src/metadataDiscoveryLoop.js";

test("metadata auto-cycle does not replace an active controlled system recovery", async () => {
  const calls = [];
  const client = {
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            value_json: {
              status: "stopped",
              stop_reason: "pipeline_complete",
              completed_at: "2026-08-30T00:00:00.000Z",
              pipeline_cycle_id: "completed-batch",
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.migration_system_retry_items")) {
        return {
          rowCount: 1,
          rows: [{
            system_retry_id: "801",
            candidate_id: "482",
            failed_dispatch_batch_id: "completed-batch",
            status: "dispatched",
          }],
        };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const result = await maybeStartMetadataDiscoveryCycle(
    (action) => action(client),
    { environment: { QUERY_METADATA_AUTO_CYCLE_ENABLED: "true" } },
  );

  assert.equal(result.started, false);
  assert.equal(result.reason, "migration_system_retry_recovery_active");
  assert.equal(result.scheduler.pipeline_cycle_id, "completed-batch");
  assert.equal(calls.some(({ sql }) => sql.includes("FROM crawler.query_terms")), false);
  assert.deepEqual(calls[1].params, [["retrying", "dispatched"]]);
});
