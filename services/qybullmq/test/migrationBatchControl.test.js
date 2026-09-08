import test from "node:test";
import assert from "node:assert/strict";
import {
  batchSelection,
  batchTransition,
  startControlledMigrationChannel,
} from "../src/migrationBatchControl.js";
test("All is unbounded selection, not a huge queue request; fixed limits remain validated", () => {
  assert.deepEqual(batchSelection("all"), { selection: "all", limit: null });
  assert.deepEqual(batchSelection("200"), { selection: "200", limit: 200 });
  assert.throws(() => batchSelection("400000"), /有效/);
});
test("pause and stop drain first; only paused batches can resume and ended batches cannot", () => {
  assert.equal(batchTransition({ status: "running" }, "pause"), "pausing");
  assert.equal(
    batchTransition({ status: "paused", frozen_at: "now" }, "resume"),
    "running",
  );
  assert.equal(
    batchTransition({ status: "paused", frozen_at: null }, "resume"),
    "preparing",
  );
  assert.equal(batchTransition({ status: "paused" }, "stop"), "stopping");
  assert.throws(() => batchTransition({ status: "ended" }, "resume"), /状态/);
});
test("queued start jobs after pause/stop never create a candidate or use network", async () => {
  for (const batch_status of [
    "pausing",
    "paused",
    "stopping",
    "ended",
    "completed",
  ]) {
    const result = await startControlledMigrationChannel({
      batchId: "b",
      channelId: "c",
      query: async () => ({ rows: [{ state: "pending", batch_status }] }),
      withTransaction() {
        throw Error("must not materialize");
      },
    });
    assert.equal(result.started, false);
  }
});
