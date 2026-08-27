import assert from "node:assert/strict";
import test from "node:test";
import { loadMigrationRunDiagnostics } from "./migrationRunDiagnostics.js";

const databaseUrl = String(
  process.env.MIGRATION_RUN_DIAGNOSTICS_POSTGRES_TEST_URL || "",
).trim();

function assertDedicatedTestDatabase(value) {
  const url = new URL(value);
  const databaseName = decodeURIComponent(url.pathname.replace(/^\//, ""));
  assert.match(databaseName, /test/i, "integration database name must contain test");
  assert.ok(
    ["127.0.0.1", "localhost", "::1"].includes(url.hostname),
    "integration database must be local",
  );
}

test("PostgreSQL migration diagnostics follow the latest recovery Binding", {
  skip: databaseUrl ? false : "MIGRATION_RUN_DIAGNOSTICS_POSTGRES_TEST_URL is not configured",
}, async (t) => {
  assertDedicatedTestDatabase(databaseUrl);
  const pg = await import("pg");
  const Pool = pg.default?.Pool || pg.Pool;
  const pool = new Pool({ connectionString: databaseUrl, max: 1 });
  t.after(async () => {
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await pool.end();
  });

  await pool.query(`DROP SCHEMA IF EXISTS crawler CASCADE;
    CREATE SCHEMA crawler;
    CREATE TABLE crawler.channel_candidates (
      candidate_id bigint PRIMARY KEY,
      channel_id text NOT NULL,
      snapshot_dispatch_generation bigint NOT NULL
    );
    CREATE TABLE crawler.business_run_bindings (
      business_run_key text PRIMARY KEY,
      business_run_id text NOT NULL UNIQUE,
      candidate_id bigint,
      status text NOT NULL,
      terminal_reason text,
      created_at timestamptz NOT NULL
    );
    CREATE TABLE crawler.channel_execution_attempts (
      attempt_id text PRIMARY KEY,
      channel_id text NOT NULL,
      business_run_id text,
      attempt_number integer,
      job_attempt integer NOT NULL,
      job_id text,
      status text NOT NULL,
      started_at timestamptz NOT NULL
    );
    INSERT INTO crawler.channel_candidates
      (candidate_id,channel_id,snapshot_dispatch_generation)
      VALUES (42,'UC-diagnostics',6);
    INSERT INTO crawler.business_run_bindings
      (business_run_key,business_run_id,candidate_id,status,terminal_reason,created_at)
      VALUES
      ('full-candidate:42','run:old',42,'terminal','old_budget','2026-08-25T00:00:00Z'),
      ('full-candidate:42:recovery:retry-6','run:new',42,'materialized',NULL,'2026-08-26T00:00:00Z');
    INSERT INTO crawler.channel_execution_attempts
      (attempt_id,channel_id,business_run_id,attempt_number,job_attempt,job_id,status,started_at)
      VALUES
      ('old-attempt','UC-diagnostics','run:old',9,2,'old-job','failed','2026-08-26T03:00:00Z'),
      ('new-attempt-1','UC-diagnostics','run:new',1,0,'new-job','failed','2026-08-26T01:00:00Z'),
      ('new-attempt-2','UC-diagnostics','run:new',2,1,'new-job','running','2026-08-26T02:00:00Z');`);

  const diagnostics = await loadMigrationRunDiagnostics({
    read: pool.query.bind(pool),
    candidateId: 42,
  });
  assert.deepEqual(diagnostics, {
    candidate_id: 42,
    dispatch_generation: 6,
    binding_status: "materialized",
    terminal_reason: null,
    business_run_key: "full-candidate:42:recovery:retry-6",
    business_run_id: "run:new",
    rota_attempt: 2,
    bullmq_attempt: 2,
    latest_job_id: "new-job",
    latest_attempt_status: "running",
  });
});
