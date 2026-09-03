import assert from "node:assert/strict";
import test from "node:test";

import { FinalRepairExecutionRecovery } from "../src/finalRepairExecutionRecovery.js";

test("Final Repair takes over a terminal Snapshot Detail lease with a new epoch", async () => {
  const prepared = [];
  const repository = {
    async loadExecutionState() {
      return {
        run_id: "run:repair",
        channel_id: "UCrepair",
        detail_job_epoch: 0,
        detail_active_job_id: "channel-snapshot__cycle__UCrepair__g2",
        detail_active_job_attempt: 1,
        detail_active_scope_key: "snapshot-scope",
        detail_active_job_epoch: 0,
      };
    },
    async prepareDetailDispatch(input) {
      prepared.push(input);
      return { content_detail_job_epoch: 1, lease_replaced: true };
    },
  };
  const recovery = new FinalRepairExecutionRecovery({
    repository,
    findJob: async (jobId) => ({
      id: jobId,
      async getState() { return "completed"; },
    }),
  });

  const result = await recovery.prepareDetailDispatch({
    runId: "run:repair",
    channelId: "UCrepair",
    repairRound: 1,
    jobId: "final-repair__run_repair__1",
  });

  assert.deepEqual(result, {
    data: { content_detail_job_epoch: 1 },
    lease_replaced: true,
  });
  assert.equal(prepared.length, 1);
  assert.equal(prepared[0].expectedActiveJobId, "channel-snapshot__cycle__UCrepair__g2");
  assert.equal(prepared[0].expectedActiveJobState, "completed");
  assert.equal(prepared[0].expectedJobEpoch, 0);
});

test("Final Repair completion requires Candidate, Finalize, and Publication closure", async () => {
  const states = [
    {
      open_candidate_count: 0,
      detail_status: "done",
      publication_finalized_status: "ready_auto",
      publication_finalized_at: "2026-09-02T00:00:00.000Z",
      publication_open: false,
    },
    {
      open_candidate_count: 1,
      detail_status: "done",
      publication_finalized_status: "ready_auto",
      publication_finalized_at: "2026-09-02T00:00:00.000Z",
      publication_open: false,
    },
    {
      open_candidate_count: 0,
      detail_status: "done",
      publication_finalized_status: null,
      publication_finalized_at: null,
      publication_open: false,
    },
    {
      open_candidate_count: 0,
      detail_status: "done",
      publication_finalized_status: "ready_auto",
      publication_finalized_at: "2026-09-02T00:00:00.000Z",
      publication_open: true,
    },
  ];
  const repository = {
    async loadExecutionState() { return null; },
    async prepareDetailDispatch() { return null; },
    async loadBusinessState() { return states.shift(); },
  };
  const recovery = new FinalRepairExecutionRecovery({
    repository,
    findJob: async () => null,
  });
  const input = { runId: "run:repair", channelId: "UCrepair" };

  assert.equal(await recovery.isBusinessComplete(input), true);
  assert.equal(await recovery.isBusinessComplete(input), false);
  assert.equal(await recovery.isBusinessComplete(input), false);
  assert.equal(await recovery.isBusinessComplete(input), false);
});
