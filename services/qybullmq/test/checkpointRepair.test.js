import assert from "node:assert/strict";
import test from "node:test";
import {
  finishCheckpointRepairExecution,
  materializeCheckpointRepairRun,
  prepareCheckpointRepairCandidates,
} from "../src/checkpointRepair.js";

test("checkpoint repair materializes a new budget Run without replacing the target Run", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("FROM crawler.channel_runs") && sql.includes("FOR SHARE")) {
        return {
          rowCount: 1,
          rows: [{
            run_id: "run:parent",
            channel_id: "UCtest",
            crawl_mode: "full",
            content_limit: 30,
          }],
        };
      }
      if (sql.includes("INSERT INTO crawler.channel_runs")) return { rowCount: 1, rows: [] };
      if (sql.includes("FOR UPDATE")) {
        return {
          rowCount: 1,
          rows: [{
            run_id: "run:repair",
            channel_id: "UCtest",
            result_json: { checkpoint_repair: { target_run_id: "run:parent", repair_round: 1 } },
          }],
        };
      }
      return { rowCount: 1, rows: [{ run_id: "run:repair" }] };
    },
  };

  const result = await materializeCheckpointRepairRun(client, {
    repairRunId: "run:repair",
    targetRunId: "run:parent",
    businessRunKey: "full-repair:auto:run:parent:1:UCtest",
    channelId: "UCtest",
    repairRound: 1,
    jobId: "final-repair__run-parent__1",
  });

  assert.equal(result.repair_run_id, "run:repair");
  assert.equal(result.target_run_id, "run:parent");
  assert.equal(calls.some(({ sql }) => /latest_run_id/.test(sql)), false);
  assert.equal(calls.some(({ sql }) => /materialized/.test(sql)), true);
});

test("checkpoint repair resets only unfinished candidates from the target Run", async () => {
  const calls = [];
  const result = await prepareCheckpointRepairCandidates(async (sql, params) => {
    calls.push({ sql, params });
    return { rowCount: 3, rows: [] };
  }, {
    targetRunId: "run:parent",
    repairRunId: "run:repair",
    repairRound: 1,
  });
  assert.equal(result.prepared_count, 3);
  assert.match(calls[0].sql, /detail_status IN \('queued','failed'\)/);
  assert.match(calls[0].sql, /classified_only/);
  assert.match(calls[0].sql, /NOT IN \('done','api_pending'\)/);
  assert.equal(calls[0].params[0], "run:parent");
});

test("checkpoint execution records the target summary on its own Run", async () => {
  const calls = [];
  await finishCheckpointRepairExecution(async (sql, params) => {
    calls.push({ sql, params });
    return { rowCount: 1, rows: [{ run_id: params[0] }] };
  }, {
    repairRunId: "run:repair",
    targetRunId: "run:parent",
    status: "done",
    summary: { total: 30, terminal: 30, failed: 0, api_open: 0 },
  });
  assert.match(calls[0].sql, /checkpoint_repair/);
  assert.equal(calls[0].params[0], "run:repair");
});
