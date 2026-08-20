import assert from "node:assert/strict";
import test from "node:test";
import {
  isBusinessRunBudgetExhausted,
  recordBusinessRunBudgetExhaustion,
  terminateExhaustedBusinessRun,
} from "../src/businessRunBudgetRecovery.js";
import { UnrecoverableError } from "bullmq";

test("Rota's exact production budget code is recognized", () => {
  assert.equal(isBusinessRunBudgetExhausted({
    code: "BUSINESS_RUN_BUDGET_EXHAUSTED",
  }), true);
  assert.equal(isBusinessRunBudgetExhausted({
    reason: "business_run_budget_exhausted",
  }), true);
  assert.equal(isBusinessRunBudgetExhausted({
    code: "EXECUTION_ROUTE_BUDGET_EXHAUSTED",
  }), false);
});

test("an exhausted Run becomes terminal in BullMQ after its checkpoint is recorded", async () => {
  const calls = [];
  await assert.rejects(
    terminateExhaustedBusinessRun(async (sql, params) => {
      calls.push({ sql, params });
      return { rowCount: 1, rows: [{ run_id: params[0] }] };
    }, {
      id: "channel-job",
      queueName: "youtube-channel-crawl",
      name: "channel-crawl",
      data: { run_id: "run:exhausted", channel_id: "UCtest" },
    }, {
      reason: "business_run_budget_exhausted",
    }),
    (error) => error instanceof UnrecoverableError
      && error.code === "BUSINESS_RUN_BUDGET_EXHAUSTED",
  );
  assert.equal(calls.length, 1);
});

test("budget exhaustion records both the repair execution and its checkpoint target", async () => {
  const calls = [];
  const result = await recordBusinessRunBudgetExhaustion(async (sql, params) => {
    calls.push({ sql, params });
    return { rowCount: 1, rows: [{ run_id: params[0] }] };
  }, {
    id: "final-repair__run-parent__1",
    queueName: "youtube-channel-crawl",
    name: "channel-checkpoint-repair",
    data: {
      run_id: "run:repair-budget",
      repair_parent_run_id: "run:parent",
      channel_id: "UCtest",
      repair_round: 1,
    },
  });

  assert.equal(result.recorded, true);
  assert.equal(result.execution_run_id, "run:repair-budget");
  assert.equal(result.target_run_id, "run:parent");
  assert.equal(calls.length, 2);
  assert.equal(JSON.parse(calls[0].params[1]).status, "business_run_budget_exhausted");
  assert.match(calls[1].sql, /checkpoint_repair/);
});
