import assert from "node:assert/strict";
import test from "node:test";

import {
  migrationSystemRetryDispatchAdmission,
  sharedCrawlerSchedulerActivationAdmission,
} from "../src/migrationSystemRetryAdmission.js";

test("G+1 dispatch is admitted only after the Scheduler completed a pipeline", () => {
  assert.deepEqual(
    migrationSystemRetryDispatchAdmission({
      status: "stopped",
      stop_reason: "pipeline_complete",
      pipeline_cycle_id: "completed-batch",
    }),
    {
      allowed: true,
      code: null,
      scheduler_status: "stopped",
      scheduler_stop_reason: "pipeline_complete",
      scheduler_pipeline_cycle_id: "completed-batch",
    },
  );

  for (const scheduler of [
    { status: "paused", stop_reason: null },
    { status: "stopped", stop_reason: "user_requested" },
    { status: "finishing", stop_reason: "controlled_migration_dispatch" },
  ]) {
    const admission = migrationSystemRetryDispatchAdmission(scheduler);
    assert.equal(admission.allowed, false);
    assert.equal(admission.code, "migration_system_retry_scheduler_blocked");
    assert.equal(admission.scheduler_status, scheduler.status);
    assert.equal(admission.scheduler_stop_reason, scheduler.stop_reason);
  }
});

test("only retrying and dispatched system recovery blocks a stopped Scheduler activation", async () => {
  const calls = [];
  const allowed = await sharedCrawlerSchedulerActivationAdmission({
    async query(sql, params) {
      calls.push({ sql, params });
      return { rowCount: 0, rows: [] };
    },
  });
  assert.deepEqual(allowed, {
    allowed: true,
    code: null,
    active_system_retry: null,
  });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].params, [["retrying", "dispatched"]]);
  assert.match(calls[0].sql, /status=ANY\(\$1::text\[\]\)/);

  const blocked = await sharedCrawlerSchedulerActivationAdmission({
    async query() {
      return {
        rowCount: 1,
        rows: [{
          system_retry_id: "801",
          candidate_id: "482",
          failed_dispatch_batch_id: "completed-batch",
          status: "dispatched",
        }],
      };
    },
  });
  assert.deepEqual(blocked, {
    allowed: false,
    code: "migration_system_retry_recovery_active",
    active_system_retry: {
      system_retry_id: 801,
      candidate_id: 482,
      failed_dispatch_batch_id: "completed-batch",
      status: "dispatched",
    },
  });
});
