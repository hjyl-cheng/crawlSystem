import assert from "node:assert/strict";
import test from "node:test";
import {
  isBusinessRunBudgetExhausted,
  recordBusinessRunBudgetExhaustion,
  terminateExhaustedBusinessRun,
} from "../src/businessRunBudgetRecovery.js";
import { UnrecoverableError } from "bullmq";

function budgetFixture({
  runId,
  businessRunKey,
  bindingStatus = "materialized",
  runMaterialized = true,
  candidateId = null,
  candidateGeneration = 0,
  candidateJobId = null,
  candidateJobAttempt = null,
} = {}) {
  const fixture = { calls: [], committed: false };
  fixture.client = {
    async query(sql, params) {
      fixture.calls.push({ sql, params });
      if (/FROM crawler\.business_run_bindings/.test(sql) && /FOR UPDATE/.test(sql)) {
        return {
          rowCount: 1,
          rows: [{
            business_run_key: businessRunKey,
            business_run_id: runId,
            status: bindingStatus,
            terminal_reason: null,
            channel_id: "UCtest",
            candidate_id: candidateId,
          }],
        };
      }
      if (/FROM crawler\.channel_candidates/.test(sql) && /FOR UPDATE/.test(sql)) {
        return {
          rowCount: 1,
          rows: [{
            candidate_id: candidateId,
            status: "queued",
            channel_id: "UCtest",
            snapshot_dispatch_generation: candidateGeneration,
            snapshot_active_job_id: candidateJobId,
            snapshot_active_job_attempt: candidateJobAttempt,
          }],
        };
      }
      if (/FROM crawler\.channel_runs/.test(sql) && /FOR UPDATE/.test(sql)) {
        return {
          rowCount: runMaterialized ? params[0].length : 0,
          rows: runMaterialized
            ? params[0].map((id) => ({ run_id: id, status: "failed", detail_status: "failed" }))
            : [],
        };
      }
      if (/UPDATE crawler\.channel_runs/.test(sql)) {
        return runMaterialized ? { rowCount: 1, rows: [{ run_id: params[0] }] } : { rowCount: 0, rows: [] };
      }
      if (/UPDATE crawler\.business_run_bindings/.test(sql)) {
        return { rowCount: 1, rows: [{ status: "terminal" }] };
      }
      if (/UPDATE crawler\.channel_candidates/.test(sql)) {
        return { rowCount: 1, rows: [{ candidate_id: candidateId, status: "failed" }] };
      }
      if (/UPDATE crawler\.migration_system_retry_items/.test(sql)) {
        return { rowCount: 0, rows: [] };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };
  fixture.withTransaction = async (action) => {
    const result = await action(fixture.client);
    fixture.committed = true;
    return result;
  };
  return fixture;
}

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
  const fixture = budgetFixture({
    runId: "run:exhausted",
    businessRunKey: "full-intent:exhausted",
  });
  await assert.rejects(
    terminateExhaustedBusinessRun(fixture.withTransaction, {
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
  assert.equal(fixture.committed, true);
  assert.equal(fixture.calls.some((call) => /UPDATE crawler\.business_run_bindings/.test(call.sql)), true);
});

test("budget exhaustion records both the repair execution and its checkpoint target", async () => {
  const fixture = budgetFixture({
    runId: "run:repair-budget",
    businessRunKey: "full-repair:auto:run-parent:1:UCtest",
  });
  const result = await recordBusinessRunBudgetExhaustion(fixture.client, {
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
  const runUpdates = fixture.calls.filter((call) => /UPDATE crawler\.channel_runs/.test(call.sql));
  assert.equal(runUpdates.length, 2);
  assert.equal(JSON.parse(runUpdates[0].params[1]).status, "business_run_budget_exhausted");
  assert.match(runUpdates[1].sql, /checkpoint_repair/);
});

test("a reserved Binding without a materialized Run still terminates atomically", async () => {
  const calls = [];
  let committed = false;
  const withTransaction = async (action) => {
    assert.equal(typeof action, "function");
    const client = {
      async query(sql, params) {
        calls.push({ sql, params });
        if (/FROM crawler\.business_run_bindings/.test(sql) && /FOR UPDATE/.test(sql)) {
          return {
            rowCount: 1,
            rows: [{
              business_run_key: "full-candidate:1",
              business_run_id: "run:reserved",
              status: "reserved",
              terminal_reason: null,
              channel_id: "UCtest",
              candidate_id: 1,
            }],
          };
        }
        if (/FROM crawler\.channel_candidates/.test(sql) && /FOR UPDATE/.test(sql)) {
          return {
            rowCount: 1,
            rows: [{
              candidate_id: 1,
              status: "queued",
              channel_id: "UCtest",
              snapshot_dispatch_generation: 1,
              snapshot_active_job_id: "channel-job",
              snapshot_active_job_attempt: 1,
            }],
          };
        }
        if (/FROM crawler\.channel_runs/.test(sql) && /FOR UPDATE/.test(sql)) {
          return { rowCount: 0, rows: [] };
        }
        if (/UPDATE crawler\.channel_runs/.test(sql)) return { rowCount: 0, rows: [] };
        if (/UPDATE crawler\.business_run_bindings/.test(sql)) {
          return { rowCount: 1, rows: [{ status: "terminal" }] };
        }
        if (/UPDATE crawler\.channel_candidates/.test(sql)) {
          return { rowCount: 1, rows: [{ candidate_id: 1, status: "failed" }] };
        }
        if (/UPDATE crawler\.migration_system_retry_items/.test(sql)) {
          return { rowCount: 0, rows: [] };
        }
        throw new Error(`unexpected SQL: ${sql}`);
      },
    };
    const result = await action(client);
    committed = true;
    return result;
  };

  await assert.rejects(
    terminateExhaustedBusinessRun(withTransaction, {
      id: "channel-job",
      queueName: "youtube-channel-crawl",
      name: "channel-crawl",
      attemptsStarted: 1,
      data: {
        business_run_key: "full-candidate:1",
        candidate_id: 1,
        channel_id: "UCtest",
        dispatch_generation: 1,
      },
    }, {
      code: "BUSINESS_RUN_BUDGET_EXHAUSTED",
    }),
    (error) => error instanceof UnrecoverableError
      && error.code === "BUSINESS_RUN_BUDGET_EXHAUSTED",
  );

  assert.equal(committed, true);
  assert.equal(calls.some((call) => /UPDATE crawler\.channel_runs/.test(call.sql)), true);
  assert.equal(calls.some((call) => /UPDATE crawler\.business_run_bindings/.test(call.sql)), true);
  assert.equal(calls.some((call) => /UPDATE crawler\.channel_candidates/.test(call.sql)), true);
});

test("a late budget error cannot terminate a newer Candidate generation", async () => {
  const fixture = budgetFixture({
    runId: "run:generation-5",
    businessRunKey: "full-candidate:42:recovery:generation-5",
    candidateId: 42,
    candidateGeneration: 6,
  });

  await assert.rejects(
    recordBusinessRunBudgetExhaustion(fixture.client, {
      id: "channel-job:generation-5",
      queueName: "youtube-channel-crawl",
      name: "channel-crawl",
      data: {
        run_id: "run:generation-5",
        business_run_key: "full-candidate:42:recovery:generation-5",
        candidate_id: 42,
        channel_id: "UCtest",
        dispatch_generation: 5,
      },
    }),
    /Candidate dispatch generation changed: expected 5, got 6/,
  );

  assert.equal(
    fixture.calls.some((call) => /^\s*UPDATE crawler\./.test(call.sql)),
    false,
  );
});

test("Candidate budget recovery refuses a Job without a persisted dispatch generation", async () => {
  const fixture = budgetFixture({
    runId: "run:missing-generation",
    businessRunKey: "full-candidate:43",
    candidateId: 43,
    candidateGeneration: 1,
  });

  await assert.rejects(
    recordBusinessRunBudgetExhaustion(fixture.client, {
      id: "channel-job:missing-generation",
      queueName: "youtube-channel-crawl",
      name: "channel-crawl",
      data: {
        run_id: "run:missing-generation",
        business_run_key: "full-candidate:43",
        candidate_id: 43,
        channel_id: "UCtest",
      },
    }),
    /job.data.dispatch_generation is required for Candidate budget recovery/,
  );

  assert.equal(
    fixture.calls.some((call) => /^\s*UPDATE crawler\./.test(call.sql)),
    false,
  );
});

test("Candidate budget termination carries its dispatch generation into the write fence", async () => {
  const fixture = budgetFixture({
    runId: "run:generation-7",
    businessRunKey: "full-candidate:44:recovery:generation-7",
    candidateId: 44,
    candidateGeneration: 7,
    candidateJobId: "channel-job:generation-7",
    candidateJobAttempt: 1,
  });

  await recordBusinessRunBudgetExhaustion(fixture.client, {
    id: "channel-job:generation-7",
    queueName: "youtube-channel-crawl",
    name: "channel-crawl",
    attemptsStarted: 1,
    data: {
      run_id: "run:generation-7",
      business_run_key: "full-candidate:44:recovery:generation-7",
      candidate_id: 44,
      channel_id: "UCtest",
      dispatch_generation: 7,
    },
  });

  const candidateUpdate = fixture.calls.find(
    (call) => /^\s*UPDATE crawler\.channel_candidates/.test(call.sql),
  );
  assert.match(candidateUpdate.sql, /snapshot_dispatch_generation=\$3/);
  assert.match(candidateUpdate.sql, /snapshot_active_job_id=\$4/);
  assert.match(candidateUpdate.sql, /snapshot_active_job_attempt=\$5/);
  assert.equal(candidateUpdate.params[2], 7);
  assert.equal(candidateUpdate.params[3], "channel-job:generation-7");
  assert.equal(candidateUpdate.params[4], 1);
});
