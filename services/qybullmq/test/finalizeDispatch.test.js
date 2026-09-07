import assert from "node:assert/strict";
import test from "node:test";
import { dispatchFinalizeForRun } from "../src/finalizeDispatch.js";

test("Finalize dispatch derives a stable source revision and deterministic Job identity", async () => {
  const added = [];
  const source = {
    channel_id: "UCfinalize",
    latest_run_id: "run-finalize",
    channel_status: "active",
    agent_status: "pending",
    detail_status: "done",
    expected_content_count: 2,
    candidate_count: 2,
    content_count: 2,
    pipeline_cycle_id: "batch-finalize",
  };
  const input = {
    query: async () => ({ rows: [{ ...source }] }),
    queue: {
      async add(name, data, options) {
        added.push({ name, data, options });
      },
    },
    channelId: "UCfinalize",
    runId: "run-finalize",
    reason: "channel-full-fetch-complete",
  };

  const first = await dispatchFinalizeForRun(input);
  const replay = await dispatchFinalizeForRun(input);

  assert.deepEqual(first, replay);
  assert.equal(added.length, 2);
  assert.equal(added[0].name, "finalize-channel");
  assert.equal(added[0].data.source_revision, first.sourceRevision);
  assert.equal(added[0].data.pipeline_cycle_id, "batch-finalize");
  assert.equal(added[0].options.jobId, first.jobId);
  assert.equal(added[1].options.jobId, first.jobId);
});
