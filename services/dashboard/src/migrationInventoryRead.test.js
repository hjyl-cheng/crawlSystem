import assert from "node:assert/strict";
import test from "node:test";
import { readMigrationInventory } from "./migrationInventoryRead.js";

function limitedSharedMemoryPool() {
  let inTransaction = false;
  let parallelWorkers = 2;
  let timeout = null;
  let released = false;
  const events = [];
  const client = {
    async query(sql, params) {
      events.push(sql);
      if (sql === "BEGIN READ ONLY") inTransaction = true;
      else if (sql === "SET LOCAL max_parallel_workers_per_gather=0") {
        assert.ok(inTransaction);
        parallelWorkers = 0;
      } else if (sql === "SET LOCAL statement_timeout='60s'") {
        assert.ok(inTransaction);
        timeout = 60_000;
      } else if (sql === "COMMIT" || sql === "ROLLBACK") {
        inTransaction = false;
        parallelWorkers = 2;
        timeout = null;
      } else {
        // The observed failure: the inventory aggregate's parallel plan cannot
        // allocate its shared hash table in the production 64 MiB /dev/shm.
        if (parallelWorkers > 0) {
          throw Object.assign(new Error("could not resize shared memory segment: No space left on device"), { code: "53100" });
        }
        assert.equal(timeout, 60_000);
        if (params?.[0] === "fail") throw Object.assign(new Error("query failed"), { code: "57014" });
        return { rows: [{ total: "400000" }] };
      }
      return { rows: [] };
    },
    release() { released = true; },
  };
  return {
    pool: { connect: async () => client, query: client.query },
    events,
    state: () => ({ inTransaction, parallelWorkers, timeout, released }),
  };
}

test("inventory queries avoid the shared-memory parallel plan and restore pooled session settings", async () => {
  const fixture = limitedSharedMemoryPool();
  const result = await readMigrationInventory(fixture.pool, "SELECT count(*) FROM inventory", ["source"]);
  assert.equal(result.rows[0].total, "400000");
  assert.deepEqual(fixture.state(), { inTransaction: false, parallelWorkers: 2, timeout: null, released: true });
  assert.equal(fixture.events.at(-1), "COMMIT");
});

test("a failed inventory read rolls back before returning the connection to its pool", async () => {
  const fixture = limitedSharedMemoryPool();
  await assert.rejects(readMigrationInventory(fixture.pool, "SELECT count(*) FROM inventory", ["fail"]), { code: "57014" });
  assert.deepEqual(fixture.state(), { inTransaction: false, parallelWorkers: 2, timeout: null, released: true });
  assert.equal(fixture.events.at(-1), "ROLLBACK");
});
