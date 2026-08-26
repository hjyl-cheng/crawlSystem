import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import test from "node:test";
import {
  assertMigrationChannelInventorySchema,
  loadMigrationChannelInventorySchemaSql,
} from "../src/migrationInventorySchema.js";
import { syncMigrationChannelInventory } from "../src/migrationInventorySync.js";

const sourceAdminUrl = String(
  process.env.MIGRATION_INVENTORY_SOURCE_POSTGRES_ADMIN_TEST_URL || "",
).trim();
const sourceReaderUrl = String(
  process.env.MIGRATION_INVENTORY_SOURCE_POSTGRES_TEST_URL || "",
).trim();
const targetUrl = String(
  process.env.MIGRATION_INVENTORY_TARGET_POSTGRES_TEST_URL || "",
).trim();
const configured = Boolean(sourceAdminUrl && sourceReaderUrl && targetUrl);

function parseDedicatedTestDatabase(value, name) {
  const url = new URL(value);
  const database = decodeURIComponent(url.pathname.replace(/^\//, ""));
  const user = decodeURIComponent(url.username);
  assert.match(database, /test/i, `${name} database name must contain test`);
  assert.ok(
    ["127.0.0.1", "localhost", "::1"].includes(url.hostname),
    `${name} database must be local`,
  );
  return { database, user };
}

function quotedIdentifier(value) {
  assert.match(value, /^[a-z_][a-z0-9_]*$/i, "test role must be a simple SQL identifier");
  return `"${value.replaceAll('"', '""')}"`;
}

test("pre-deploy gate syncs 410292 Source rows through the real PostgreSQL cursor", {
  skip: configured ? false : "Migration inventory Source/Target PostgreSQL test URLs are not configured",
  timeout: 240_000,
}, async (t) => {
  const sourceAdminIdentity = parseDedicatedTestDatabase(sourceAdminUrl, "Source admin");
  const sourceReaderIdentity = parseDedicatedTestDatabase(sourceReaderUrl, "Source reader");
  const targetIdentity = parseDedicatedTestDatabase(targetUrl, "Target");
  assert.equal(sourceAdminIdentity.database, sourceReaderIdentity.database);
  assert.notEqual(sourceAdminIdentity.database, targetIdentity.database);
  assert.match(sourceReaderIdentity.user, /test/i, "Source reader role name must contain test");
  assert.notEqual(sourceAdminIdentity.user, sourceReaderIdentity.user);

  const pg = await import("pg");
  const Pool = pg.default?.Pool || pg.Pool;
  const sourceAdminPool = new Pool({ connectionString: sourceAdminUrl, max: 1 });
  const sourceReaderPool = new Pool({ connectionString: sourceReaderUrl, max: 1 });
  const targetPool = new Pool({ connectionString: targetUrl, max: 1 });
  t.after(async () => {
    await targetPool.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await sourceAdminPool.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await Promise.all([
      sourceReaderPool.end(),
      sourceAdminPool.end(),
      targetPool.end(),
    ]);
  });

  await sourceAdminPool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await sourceAdminPool.query(`CREATE SCHEMA crawler;
    CREATE TABLE crawler.channel_candidates (
      candidate_id bigint PRIMARY KEY,
      channel_id text NOT NULL,
      channel_url text NOT NULL,
      handle text,
      title text,
      avatar_url text,
      search_subscriber_count bigint,
      priority integer NOT NULL,
      status text NOT NULL,
      updated_at timestamptz NOT NULL,
      source_json jsonb NOT NULL
    );
    CREATE TABLE crawler.channels (
      channel_id text PRIMARY KEY
    );
    CREATE INDEX migration_inventory_source_test_page
      ON crawler.channel_candidates (channel_id,priority DESC,candidate_id DESC)
      WHERE source_json->>'source'='legacy_results_db';`);
  await sourceAdminPool.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,channel_id,channel_url,handle,title,search_subscriber_count,
       priority,status,updated_at,source_json
     )
     SELECT series.value,
            'UC'||lpad(series.value::text,22,'0'),
            'https://www.youtube.com/channel/UC'||lpad(series.value::text,22,'0'),
            '@source'||series.value::text,
            'Source '||series.value::text,
            CASE WHEN series.value%2=0 THEN series.value*10 ELSE NULL END,
            100,'discovered',now(),'{"source":"legacy_results_db"}'::jsonb
     FROM generate_series(1,410292) AS series(value)`,
  );
  const sourceReaderRole = quotedIdentifier(sourceReaderIdentity.user);
  await sourceAdminPool.query(`GRANT USAGE ON SCHEMA crawler TO ${sourceReaderRole}`);
  await sourceAdminPool.query(
    `GRANT SELECT ON crawler.channel_candidates,crawler.channels TO ${sourceReaderRole}`,
  );
  await sourceAdminPool.query("ANALYZE crawler.channel_candidates");

  await targetPool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await targetPool.query("CREATE SCHEMA crawler");
  await targetPool.query(await loadMigrationChannelInventorySchemaSql());
  await assertMigrationChannelInventorySchema({ query: targetPool.query.bind(targetPool) });

  const sourceDatabaseOidResult = await sourceAdminPool.query(
    "SELECT oid::text AS database_oid FROM pg_database WHERE datname=current_database()",
  );
  const environment = {
    MIGRATION_DATABASE_URL: sourceReaderUrl,
    MIGRATION_SOURCE_ID: "postgres-sync-gate-v1",
    EXPECTED_MIGRATION_DATABASE: sourceReaderIdentity.database,
    EXPECTED_MIGRATION_DATABASE_OID: sourceDatabaseOidResult.rows[0].database_oid,
    EXPECTED_MIGRATION_DATABASE_USER: sourceReaderIdentity.user,
    EXPECTED_CRAWLER_DATABASE: targetIdentity.database,
    MIGRATION_POSTGRES_STATEMENT_TIMEOUT_MS: "10000",
    MIGRATION_INVENTORY_SOURCE_STATEMENT_TIMEOUT_MS: "120000",
  };
  const sourceStatements = [];
  const observedSourcePool = {
    async connect() {
      const client = await sourceReaderPool.connect();
      return {
        query(sql, params) {
          sourceStatements.push(String(sql).replace(/\s+/g, " ").trim());
          return client.query(sql, params);
        },
        release() {
          client.release();
        },
      };
    },
  };
  const progress = [];
  const startedAt = performance.now();
  const result = await syncMigrationChannelInventory({
    targetPool,
    sourcePool: observedSourcePool,
    environment,
    batchSize: 5000,
    onProgress: (entry) => progress.push(entry.eligible_count),
  });
  const elapsedMs = performance.now() - startedAt;
  t.diagnostic(`Real 410292-row Source inventory sync: ${elapsedMs.toFixed(1)}ms`);

  assert.equal(result.status, "ready");
  assert.equal(result.skipped, false);
  assert.equal(result.eligible_count, 410292);
  assert.ok(sourceStatements.includes("SET LOCAL statement_timeout=120000"));
  assert.ok(sourceStatements.some((sql) => sql.startsWith("DECLARE migration_channel_inventory_source")));
  assert.equal(
    sourceStatements.filter((sql) => sql.startsWith("FETCH FORWARD 5000")).length,
    Math.ceil(410292 / 5000) + 1,
  );
  assert.equal(progress.at(-1), 410292);

  const published = await targetPool.query(
    `SELECT sync.status,sync.eligible_count::text,
            count(inventory.source_candidate_id)::text AS inventory_count
     FROM crawler.migration_channel_inventory_syncs sync
     LEFT JOIN crawler.migration_channel_inventory inventory
       ON inventory.source_id=sync.source_id
     WHERE sync.source_id=$1
     GROUP BY sync.source_id,sync.status,sync.eligible_count`,
    [environment.MIGRATION_SOURCE_ID],
  );
  assert.deepEqual(published.rows, [{
    status: "ready",
    eligible_count: "410292",
    inventory_count: "410292",
  }]);
});
