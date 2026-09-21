import test from "node:test";
import assert from "node:assert/strict";
import {
  batchSelection,
  batchTransition,
  maintainMigrationControl,
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

test("settlement SQL recognizes durable terminal Run and retry evidence", async () => {
  let settlementSql = "";
  const batch = {
    batch_id: "migration-test-terminal-evidence",
    status: "running",
    selection: "100",
    source_id: "source",
    total_count: 1,
  };
  const client = {
    async query(sql) {
      if (sql.includes("FROM crawler.settings")) return { rows: [{ value_json: {} }] };
      if (sql.includes("FROM crawler.migration_control_batches")) return { rows: [batch] };
      if (sql.includes("WITH started AS MATERIALIZED")) {
        settlementSql = sql;
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("SELECT count(*) FILTER")) {
        return { rows: [{ pending: 0, started: 1, terminal: 0, failed: 0 }] };
      }
      if (sql.includes("FROM publication.outbox")) return { rows: [{ count: "0" }] };
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };
  await maintainMigrationControl({
    query: async () => ({ rows: [batch] }),
    withTransaction: async action => action(client),
    queue: { add: async () => {} },
  });
  assert.match(settlementSql, /manual\.terminal/);
  assert.match(settlementSql, /channel_run_terminal_failure/);
  assert.match(settlementSql, /recovery_terminal_business_outcome/);
});
