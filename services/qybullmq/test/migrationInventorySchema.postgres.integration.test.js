import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  assertMigrationChannelInventorySchemaState,
  loadMigrationChannelInventorySchemaSql,
  loadMigrationChannelInventorySchemaState,
  migrationChannelInventorySchemaBlock,
} from "../src/migrationInventorySchema.js";

const databaseUrl = String(
  process.env.MIGRATION_INVENTORY_SCHEMA_POSTGRES_TEST_URL || "",
).trim();

function assertDedicatedTestDatabase(value) {
  const url = new URL(value);
  const database = decodeURIComponent(url.pathname.replace(/^\//, ""));
  assert.match(database, /test/i, "integration database name must contain test");
  assert.ok(
    ["127.0.0.1", "localhost", "::1"].includes(url.hostname),
    "integration database must be local",
  );
}

async function resetPublishedSchema(pool, schemaSql) {
  await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await pool.query("CREATE SCHEMA crawler");
  await pool.query(schemaSql);
}

async function schemaState(pool) {
  return loadMigrationChannelInventorySchemaState(pool.query.bind(pool));
}

async function constraintName(pool, tableName, constraintType, expressionSearch = "") {
  const result = await pool.query(
    `SELECT constraint_record.conname
     FROM pg_constraint constraint_record
     WHERE constraint_record.conrelid=$1::regclass
       AND constraint_record.contype=$2
       AND ($3='' OR COALESCE(
         pg_get_expr(constraint_record.conbin,constraint_record.conrelid,false),''
       ) ILIKE '%'||$3||'%')`,
    [`crawler.${tableName}`, constraintType, expressionSearch],
  );
  assert.equal(result.rows.length, 1);
  return `"${result.rows[0].conname.replaceAll('"', '""')}"`;
}

test("PostgreSQL catalog check rejects incomplete Migration inventory contracts", {
  skip: databaseUrl ? false : "MIGRATION_INVENTORY_SCHEMA_POSTGRES_TEST_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedTestDatabase(databaseUrl);
  const pg = await import("pg");
  const Pool = pg.default?.Pool || pg.Pool;
  const pool = new Pool({ connectionString: databaseUrl, max: 1 });
  const schemaSql = await loadMigrationChannelInventorySchemaSql();
  const bootstrapSql = migrationChannelInventorySchemaBlock(await readFile(
    new URL("../../../database/bootstrap/crawler.sql", import.meta.url),
    "utf8",
  ));
  t.after(async () => {
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await pool.end();
  });

  await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await pool.query(`CREATE SCHEMA crawler;
    CREATE TABLE crawler.migration_channel_inventory_syncs (
      source_id text,source_database text,source_database_oid oid,status text,
      sync_token uuid,eligible_count bigint,started_at timestamptz,
      completed_at timestamptz,last_error text,updated_at timestamptz
    );
    CREATE TABLE crawler.migration_channel_inventory (
      source_id text,source_candidate_id bigint,channel_id text,channel_url text,
      handle text,title text,avatar_url text,search_subscriber_count bigint,
      priority integer,source_candidate_status text,source_updated_at timestamptz,
      sync_token uuid,synced_at timestamptz
    );
    CREATE INDEX idx_crawler_migration_inventory_page
      ON crawler.migration_channel_inventory (
        source_id,priority DESC,source_candidate_id ASC
      );`);
  const incomplete = await schemaState(pool);
  assert.equal(incomplete.sync_table_ready, true);
  assert.equal(incomplete.inventory_table_ready, true);
  assert.equal(incomplete.page_index_match, true);
  assert.equal(incomplete.sync_columns_match, false);
  assert.equal(incomplete.inventory_constraints_match, false);
  assert.throws(
    () => assertMigrationChannelInventorySchemaState(incomplete),
    (error) => error?.code === "migration_inventory_schema_not_ready",
  );

  await resetPublishedSchema(pool, schemaSql);
  const published = await schemaState(pool);
  assert.doesNotThrow(
    () => assertMigrationChannelInventorySchemaState(published),
  );

  await pool.query(
    "ALTER TABLE crawler.migration_channel_inventory ALTER COLUMN handle TYPE varchar(255)",
  );
  assert.equal((await schemaState(pool)).inventory_columns_match, false);

  await resetPublishedSchema(pool, schemaSql);
  await pool.query(
    "ALTER TABLE crawler.migration_channel_inventory ALTER COLUMN channel_url DROP NOT NULL",
  );
  assert.equal((await schemaState(pool)).inventory_columns_match, false);

  await resetPublishedSchema(pool, schemaSql);
  await pool.query(
    "ALTER TABLE crawler.migration_channel_inventory ALTER COLUMN priority SET DEFAULT 101",
  );
  assert.equal((await schemaState(pool)).inventory_columns_match, false);

  await resetPublishedSchema(pool, schemaSql);
  const primaryKeyName = await constraintName(
    pool,
    "migration_channel_inventory",
    "p",
  );
  await pool.query(
    `ALTER TABLE crawler.migration_channel_inventory DROP CONSTRAINT ${primaryKeyName}`,
  );
  assert.equal((await schemaState(pool)).inventory_constraints_match, false);

  await resetPublishedSchema(pool, schemaSql);
  const uniqueName = await constraintName(
    pool,
    "migration_channel_inventory",
    "u",
  );
  await pool.query(
    `ALTER TABLE crawler.migration_channel_inventory DROP CONSTRAINT ${uniqueName}`,
  );
  assert.equal((await schemaState(pool)).inventory_constraints_match, false);

  await resetPublishedSchema(pool, schemaSql);
  const foreignKeyName = await constraintName(
    pool,
    "migration_channel_inventory",
    "f",
  );
  await pool.query(
    `ALTER TABLE crawler.migration_channel_inventory DROP CONSTRAINT ${foreignKeyName}`,
  );
  await pool.query(`ALTER TABLE crawler.migration_channel_inventory
    ADD FOREIGN KEY (source_id)
    REFERENCES crawler.migration_channel_inventory_syncs(source_id)`);
  assert.equal((await schemaState(pool)).inventory_constraints_match, false);

  await resetPublishedSchema(pool, schemaSql);
  const statusCheckName = await constraintName(
    pool,
    "migration_channel_inventory_syncs",
    "c",
    "status",
  );
  await pool.query(
    `ALTER TABLE crawler.migration_channel_inventory_syncs
     DROP CONSTRAINT ${statusCheckName}`,
  );
  await pool.query(`ALTER TABLE crawler.migration_channel_inventory_syncs
    ADD CHECK (status IN ('syncing','ready','failed','unknown'))`);
  assert.equal((await schemaState(pool)).sync_constraints_match, false);

  await resetPublishedSchema(pool, schemaSql);
  await pool.query("DROP INDEX crawler.idx_crawler_migration_inventory_page");
  await pool.query(`CREATE INDEX idx_crawler_migration_inventory_page
    ON crawler.migration_channel_inventory (
      source_id,source_candidate_id,priority DESC
    )`);
  assert.equal((await schemaState(pool)).page_index_match, false);

  await resetPublishedSchema(pool, schemaSql);
  await pool.query("DROP INDEX crawler.idx_crawler_migration_inventory_page");
  await pool.query(`CREATE INDEX idx_crawler_migration_inventory_page
    ON crawler.migration_channel_inventory (
      source_id,priority ASC,source_candidate_id ASC
    )`);
  assert.equal((await schemaState(pool)).page_index_match, false);

  await resetPublishedSchema(pool, bootstrapSql);
  const bootstrapState = await schemaState(pool);
  assert.doesNotThrow(
    () => assertMigrationChannelInventorySchemaState(bootstrapState),
  );
});
