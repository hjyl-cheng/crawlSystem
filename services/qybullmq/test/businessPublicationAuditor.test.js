import assert from "node:assert/strict";
import test from "node:test";
import { PostgresBusinessPublicationAuditor } from "../src/businessPublicationAuditor.js";

const FINDING_KEYS = [
  "cursor_current_mismatch",
  "cursor_revision_mismatch",
  "ownership_cursor_mismatch",
  "long_lived_gap",
  "open_quarantine",
  "old_stream_pending",
  "active_without_activation",
  "projection_stuck",
  "projection_dead_letter",
  "blocked_activation",
  "activation_error",
];

class RecordingPool {
  constructor({ failAudits = false } = {}) {
    this.failAudits = failAudits;
    this.connectCount = 0;
    this.statements = [];
    this.activeAudits = 0;
    this.peakConcurrentAudits = 0;
  }

  async connect() {
    this.connectCount += 1;
    let transactionStarted = false;
    let parallelDisabled = false;
    let debugParallelDisabled = false;
    return {
      query: async (sql) => {
        const statement = String(sql);
        this.statements.push(statement);
        if (statement === "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY") {
          transactionStarted = true;
          return { rows: [] };
        }
        if (statement === "SET LOCAL max_parallel_workers_per_gather = 0") {
          assert.equal(transactionStarted, true);
          parallelDisabled = true;
          return { rows: [] };
        }
        if (statement === "SET LOCAL debug_parallel_query = off") {
          assert.equal(transactionStarted, true);
          debugParallelDisabled = true;
          return { rows: [] };
        }
        if (statement === "COMMIT" || statement === "ROLLBACK") return { rows: [] };
        assert.equal(transactionStarted, true);
        assert.equal(parallelDisabled, true);
        assert.equal(debugParallelDisabled, true);
        this.activeAudits += 1;
        this.peakConcurrentAudits = Math.max(
          this.peakConcurrentAudits,
          this.activeAudits,
        );
        try {
          if (this.failAudits) {
            throw new Error(
              "could not resize shared memory segment to 8388608 bytes: No space left on device",
            );
          }
          await new Promise((resolve) => setImmediate(resolve));
          return { rows: [{ count: 0, oldest_at: null, samples: [] }] };
        } finally {
          this.activeAudits -= 1;
        }
      },
      release() {},
    };
  }
}

test("Auditor uses one read-only transaction with local parallelism disabled", async () => {
  const pool = new RecordingPool();
  const now = new Date("2026-08-22T04:00:00.000Z");
  const auditor = new PostgresBusinessPublicationAuditor(pool, { clock: () => now });

  const audit = await auditor.runIfDue();

  assert.equal(audit.status, "succeeded");
  assert.equal(audit.query_count, 11);
  assert.equal(audit.parallel_workers_per_gather, 0);
  assert.equal(audit.debug_parallel_query, "off");
  assert.equal(audit.attempts_total, 1);
  assert.equal(audit.successes_total, 1);
  assert.equal(audit.failures_total, 0);
  assert.equal(audit.shared_memory_failures_total, 0);
  assert.equal(audit.failure_kind, null);
  assert.equal(audit.next_attempt_at, "2026-08-22T04:05:00.000Z");
  assert.deepEqual(Object.keys(audit.findings), FINDING_KEYS);
  assert.equal(pool.connectCount, 1);
  assert.equal(pool.peakConcurrentAudits, 1);
  assert.deepEqual(pool.statements.slice(0, 3), [
    "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
    "SET LOCAL max_parallel_workers_per_gather = 0",
    "SET LOCAL debug_parallel_query = off",
  ]);
  assert.deepEqual(
    pool.statements.slice(3, -1).map((statement) => (
      /business-publication-auditor:([a-z_]+)/.exec(statement)?.[1]
    )),
    FINDING_KEYS,
  );
  assert.equal(pool.statements.at(-1), "COMMIT");
});

test("Auditor contains failures behind a 30/60/120/300 second retry schedule", async () => {
  const pool = new RecordingPool({ failAudits: true });
  let nowMs = Date.parse("2026-08-22T05:00:00.000Z");
  const auditor = new PostgresBusinessPublicationAuditor(pool, {
    clock: () => new Date(nowMs),
    errorRetrySeconds: 30,
    maximumErrorRetrySeconds: 300,
  });

  for (const [index, retrySeconds] of [30, 60, 120, 300].entries()) {
    const failed = await auditor.runIfDue();
    assert.equal(failed.status, "failed");
    assert.equal(failed.consecutive_failures, index + 1);
    assert.equal(failed.failed_query, "cursor_current_mismatch");
    assert.equal(failed.query_count, 0);
    assert.deepEqual(failed.query_duration_ms, {});
    assert.equal(failed.attempts_total, index + 1);
    assert.equal(failed.successes_total, 0);
    assert.equal(failed.failures_total, index + 1);
    assert.equal(failed.shared_memory_failures_total, index + 1);
    assert.equal(failed.failure_kind, "dynamic_shared_memory_exhausted");
    assert.match(failed.error, /shared memory segment.*No space left on device/);
    assert.equal(
      failed.next_attempt_at,
      new Date(nowMs + retrySeconds * 1000).toISOString(),
    );
    assert.equal((await auditor.runIfDue()).status, "not_due");
    nowMs += retrySeconds * 1000;
  }
});

test("Auditor admits only one in-flight audit", async () => {
  let startAudit;
  const auditStarted = new Promise((resolve) => { startAudit = resolve; });
  let finishAudit;
  const auditGate = new Promise((resolve) => { finishAudit = resolve; });
  const pool = new RecordingPool();
  const originalConnect = pool.connect.bind(pool);
  pool.connect = async () => {
    const client = await originalConnect();
    const originalQuery = client.query;
    let firstAudit = true;
    client.query = async (sql) => {
      if (String(sql).includes("business-publication-auditor:") && firstAudit) {
        firstAudit = false;
        startAudit();
        await auditGate;
      }
      return originalQuery(sql);
    };
    return client;
  };
  const auditor = new PostgresBusinessPublicationAuditor(pool);

  const first = auditor.runIfDue();
  await auditStarted;
  const overlapping = await auditor.runIfDue();

  assert.equal(overlapping.status, "in_progress");
  assert.equal(overlapping.performed, false);
  assert.equal(pool.connectCount, 1);
  finishAudit();
  assert.equal((await first).status, "succeeded");
});

test("Auditor exposes cumulative recovery metrics after a failed audit", async () => {
  const pool = new RecordingPool({ failAudits: true });
  let nowMs = Date.parse("2026-08-22T06:00:00.000Z");
  const auditor = new PostgresBusinessPublicationAuditor(pool, {
    clock: () => new Date(nowMs),
    errorRetrySeconds: 30,
  });

  const failed = await auditor.runIfDue();
  assert.equal(failed.failure_kind, "dynamic_shared_memory_exhausted");

  pool.failAudits = false;
  nowMs += 30_000;
  const recovered = await auditor.runIfDue();

  assert.equal(recovered.status, "succeeded");
  assert.equal(recovered.attempts_total, 2);
  assert.equal(recovered.successes_total, 1);
  assert.equal(recovered.failures_total, 1);
  assert.equal(recovered.shared_memory_failures_total, 1);
  assert.equal(recovered.consecutive_failures, 0);
  assert.equal(recovered.failure_kind, null);
});
