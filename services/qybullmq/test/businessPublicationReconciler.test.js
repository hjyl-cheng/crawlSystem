import assert from "node:assert/strict";
import test from "node:test";
import { PostgresBusinessPublicationReconciler } from "../src/businessPublicationReconciler.js";

const DSM_EXHAUSTED = [
  "could not resize shared memory segment",
  '"/PostgreSQL.1234567890" to 8388608 bytes:',
  "No space left on device",
].join(" ");

class SharedMemoryLimitedPool {
  constructor({ maximumConcurrentAudits = 2 } = {}) {
    this.maximumConcurrentAudits = maximumConcurrentAudits;
    this.activeAudits = 0;
    this.peakConcurrentAudits = 0;
  }

  async connect() {
    return {
      query: (sql, params) => this.query(sql, params),
      release() {},
    };
  }

  async query(sql) {
    const statement = String(sql);
    if (statement.includes("business-publication-reconciler:claim")) {
      return { rowCount: 0, rows: [] };
    }
    if (!/WITH\s+(?:current_rows|issues)\s+AS/i.test(statement)) {
      return { rowCount: 0, rows: [] };
    }

    this.activeAudits += 1;
    this.peakConcurrentAudits = Math.max(this.peakConcurrentAudits, this.activeAudits);
    try {
      if (this.activeAudits > this.maximumConcurrentAudits) {
        throw new Error(DSM_EXHAUSTED);
      }
      await new Promise((resolve) => setImmediate(resolve));
      return {
        rowCount: 1,
        rows: [{ count: 0, oldest_at: null, samples: [] }],
      };
    } finally {
      this.activeAudits -= 1;
    }
  }
}

test("Reconciler audit stays within one PostgreSQL shared-memory budget", async () => {
  const pool = new SharedMemoryLimitedPool();
  const reconciler = new PostgresBusinessPublicationReconciler(pool, {
    activator: { async activateReady() { assert.fail("there are no claimed channels"); } },
    auditIntervalSeconds: 0,
  });

  const summary = await reconciler.runOnce();

  assert.equal(summary.audit.performed, true);
  assert.equal(summary.audit.issue_count, 0);
  assert.equal(pool.peakConcurrentAudits, 1);
});
