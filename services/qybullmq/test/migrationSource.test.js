import assert from "node:assert/strict";
import test from "node:test";
import {
  loadMigrationSourceBatch,
  loadMigrationSourceChannel,
  migrationSourceRuntimeConfig,
  sourceSnapshotHash,
  withMigrationSourceReadTransaction,
} from "../src/migrationSource.js";

const environment = {
  MIGRATION_DATABASE_URL: "postgresql://migration_reader:secret@migration-postgres:5432/bullmq_crawler_migration",
  MIGRATION_SOURCE_ID: "qy-migration-v1",
  EXPECTED_MIGRATION_DATABASE: "bullmq_crawler_migration",
  EXPECTED_MIGRATION_DATABASE_OID: "16384",
  EXPECTED_MIGRATION_DATABASE_USER: "migration_reader",
  EXPECTED_CRAWLER_DATABASE: "newcrawler_crawler",
};

function identityRow() {
  return {
    database_name: "bullmq_crawler_migration",
    database_oid: "16384",
    database_user: "migration_reader",
    default_transaction_read_only: "on",
    transaction_read_only: "on",
    candidates_ready: true,
    channels_ready: true,
    candidate_write: false,
    channel_write: false,
  };
}

test("Migration Source config requires a pinned source identity distinct from Target", () => {
  assert.deepEqual(migrationSourceRuntimeConfig(environment), {
    databaseUrl: environment.MIGRATION_DATABASE_URL,
    sourceId: "qy-migration-v1",
    expectedDatabase: "bullmq_crawler_migration",
    expectedDatabaseOid: "16384",
    expectedUser: "migration_reader",
    targetDatabase: "newcrawler_crawler",
    statementTimeoutMs: 10000,
  });
  assert.throws(
    () => migrationSourceRuntimeConfig({ ...environment, EXPECTED_MIGRATION_DATABASE_OID: "" }),
    /EXPECTED_MIGRATION_DATABASE_OID/,
  );
});

test("every Migration Source query runs after identity verification in one explicit read-only snapshot", async () => {
  const statements = [];
  const client = {
    async query(sql) {
      const normalized = sql.replace(/\s+/g, " ").trim();
      statements.push(normalized);
      if (normalized.includes("current_database() AS database_name")) {
        return { rows: [identityRow()] };
      }
      if (normalized === "SELECT 'source-row' AS value") {
        return { rows: [{ value: "source-row" }] };
      }
      return { rows: [] };
    },
    release() { statements.push("RELEASE"); },
  };
  const pool = { async connect() { return client; } };

  const result = await withMigrationSourceReadTransaction(
    (sourceClient) => sourceClient.query("SELECT 'source-row' AS value"),
    { pool, environment },
  );

  assert.deepEqual(result.rows, [{ value: "source-row" }]);
  assert.equal(statements[0], "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  assert.equal(statements[1], "SET LOCAL statement_timeout=10000");
  assert.match(statements[2], /current_database\(\) AS database_name/);
  assert.equal(statements.at(-2), "COMMIT");
  assert.equal(statements.at(-1), "RELEASE");
});

test("source snapshots are canonical and carry an immutable hash", async () => {
  const candidate = {
    candidate_id: "42",
    dispatch_batch_id: "legacy-results-full-v1",
    channel_id: "UC1234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
    handle: "@example",
    title: "Example",
    description: "Source description",
    avatar_url: null,
    search_subscriber_count: "1200",
    search_subscriber_count_text: "1.2K",
    is_verified: false,
    priority: 100,
    source_candidate_status: "discovered",
    snapshot_json: { b: 2, a: 1 },
    source_json: { source: "legacy_results_db" },
    created_at: new Date("2026-08-01T00:00:00.000Z"),
    updated_at: new Date("2026-08-02T00:00:00.000Z"),
  };
  const client = {
    async query(sql) {
      if (sql.includes("current_database() AS database_name")) return { rows: [identityRow()] };
      if (sql.includes("FROM crawler.channel_candidates")) return { rows: [candidate] };
      return { rows: [] };
    },
    release() {},
  };
  const snapshot = await loadMigrationSourceChannel({
    channelId: candidate.channel_id,
    candidateId: 42,
    pool: { async connect() { return client; } },
    environment,
  });

  assert.equal(snapshot.source_id, "qy-migration-v1");
  assert.equal(snapshot.source_database, "bullmq_crawler_migration");
  assert.equal(snapshot.source_database_oid, "16384");
  assert.equal(snapshot.source_candidate_id, "42");
  assert.equal(snapshot.source_candidate_status, "discovered");
  assert.equal(snapshot.snapshot_sha256, sourceSnapshotHash(snapshot));
  assert.match(snapshot.snapshot_sha256, /^[a-f0-9]{64}$/);
  assert.equal(sourceSnapshotHash({ ...snapshot, snapshot_sha256: "ignored" }), snapshot.snapshot_sha256);
});

test("single and batch Source reads select only canonical pending migration candidates", async () => {
  const sourceSql = [];
  const candidate = {
    candidate_id: "42",
    dispatch_batch_id: "legacy-results-full-v1",
    channel_id: "UC1234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
    priority: 100,
    source_candidate_status: "discovered",
    snapshot_json: {},
    source_json: { source: "legacy_results_db" },
  };
  const client = {
    async query(sql) {
      if (sql.includes("current_database() AS database_name")) return { rows: [identityRow()] };
      if (sql.includes("FROM crawler.channel_candidates")) {
        sourceSql.push(sql.replace(/\s+/g, " "));
        return { rows: [candidate] };
      }
      return { rows: [] };
    },
    release() {},
  };
  const pool = { async connect() { return client; } };

  await loadMigrationSourceChannel({
    channelId: candidate.channel_id,
    candidateId: candidate.candidate_id,
    pool,
    environment,
  });
  await loadMigrationSourceBatch({ limit: 100, pool, environment });

  assert.equal(sourceSql.length, 2);
  for (const sql of sourceSql) {
    assert.match(sql, /channel_rank=1/);
    assert.match(
      sql,
      /source_candidate_status IN \('discovered','queued','validating','failed'\)/,
    );
  }
  assert.match(sourceSql[1], /source_page AS \(/);
  assert.match(
    sourceSql[1],
    /FROM source_page page JOIN crawler\.channel_candidates candidate/,
  );
  assert.doesNotMatch(
    sourceSql[1].slice(0, sourceSql[1].indexOf("source_page AS")),
    /snapshot_json|source_json,created_at/,
  );
});
