import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { migrationInventorySchemaApplyGuard } from "../scripts/applyMigrationChannelInventorySchema.mjs";
import {
  assertMigrationChannelInventorySchema,
  assertMigrationChannelInventorySchemaState,
  migrationChannelInventorySchemaBlock,
} from "../src/migrationInventorySchema.js";

function readySchemaState(overrides = {}) {
  return {
    sync_table_ready: true,
    inventory_table_ready: true,
    sync_columns_match: true,
    inventory_columns_match: true,
    sync_constraints_match: true,
    inventory_constraints_match: true,
    page_index_match: true,
    ...overrides,
  };
}

test("only the controlled publisher and fresh bootstrap include inventory DDL", async () => {
  const [runtime, controlled, bootstrap] = await Promise.all([
    readFile(new URL("../src/schema.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/migrationInventorySchema.sql", import.meta.url), "utf8"),
    readFile(new URL("../../../database/bootstrap/crawler.sql", import.meta.url), "utf8"),
  ]);

  assert.doesNotMatch(runtime, /migration_channel_inventory/);
  const controlledBlock = migrationChannelInventorySchemaBlock(controlled);
  for (const schema of [controlledBlock, bootstrap]) {
    assert.match(
      schema,
      /CREATE TABLE(?: IF NOT EXISTS)? crawler\.migration_channel_inventory_syncs/,
    );
    assert.match(
      schema,
      /CREATE TABLE(?: IF NOT EXISTS)? crawler\.migration_channel_inventory/,
    );
    assert.match(schema, /PRIMARY KEY \(source_id, channel_id\)/);
    assert.match(schema, /UNIQUE \(source_id, source_candidate_id\)/);
    assert.match(
      schema,
      /CREATE INDEX(?: IF NOT EXISTS)? idx_crawler_migration_inventory_page[^;]*source_id[^;]*priority DESC[^;]*source_candidate_id(?: ASC)?\s*\)/,
    );
  }
});

test("runtime inventory schema check is read-only and accepts the published contract", async () => {
  const calls = [];
  const state = await assertMigrationChannelInventorySchema({
    query: async (sql, params) => {
      calls.push({ sql: sql.replace(/\s+/g, " ").trim(), params });
      return { rows: [readySchemaState()] };
    },
  });

  assert.equal(state.inventory_table_ready, true);
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /^(?:WITH|SELECT) /);
  assert.doesNotMatch(
    calls[0].sql,
    /\b(?:CREATE|ALTER|DROP|TRUNCATE|INSERT|UPDATE|DELETE)\b/i,
  );
  assert.equal(JSON.parse(calls[0].params[0]).length, 10);
  assert.equal(JSON.parse(calls[0].params[1]).length, 13);
});

test("runtime inventory schema check blocks missing or invalid schema", () => {
  for (const state of [
    readySchemaState({ sync_table_ready: false }),
    readySchemaState({ sync_columns_match: false }),
    readySchemaState({ inventory_columns_match: false }),
    readySchemaState({ sync_constraints_match: false }),
    readySchemaState({ inventory_constraints_match: false }),
    readySchemaState({ page_index_match: false }),
  ]) {
    assert.throws(
      () => assertMigrationChannelInventorySchemaState(state),
      (error) => error?.code === "migration_inventory_schema_not_ready",
    );
  }
});

test("controlled inventory schema publication requires an exact database confirmation", () => {
  const environment = {
    EXPECTED_CRAWLER_DATABASE: "newcrawler_crawler",
    EXPECTED_MIGRATION_INVENTORY_ROW_COUNT: "410292",
  };

  assert.throws(
    () => migrationInventorySchemaApplyGuard(environment, ["node", "script"]),
    /pass --apply explicitly/,
  );
  assert.throws(
    () => migrationInventorySchemaApplyGuard(environment, ["node", "script", "--apply"]),
    /must equal EXPECTED_CRAWLER_DATABASE/,
  );
  assert.throws(
    () => migrationInventorySchemaApplyGuard({
      ...environment,
      CONFIRM_MIGRATION_INVENTORY_SCHEMA_APPLY: "newcrawler_crawler",
      EXPECTED_MIGRATION_INVENTORY_ROW_COUNT: "unknown",
    }, ["node", "script", "--apply"]),
    /must be an explicit non-negative integer/,
  );

  const guard = migrationInventorySchemaApplyGuard({
    ...environment,
    CONFIRM_MIGRATION_INVENTORY_SCHEMA_APPLY: "newcrawler_crawler",
  }, ["node", "script", "--apply"]);
  assert.equal(guard.confirmedDatabase, "newcrawler_crawler");
  assert.equal(guard.expectedInventoryRowCount, 410292);
});

test("Compose exposes only the manual controlled inventory schema publisher", async () => {
  const compose = await readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8");
  const start = compose.indexOf("  migration-inventory-schema-publisher:");
  const end = compose.indexOf("\n  local-agent-config:", start);
  assert.ok(start >= 0 && end > start);
  const service = compose.slice(start, end);

  assert.match(service, /profiles: \[manual-migration-inventory-schema\]/);
  assert.match(service, /applyMigrationChannelInventorySchema\.mjs", "--apply"/);
  assert.match(
    service,
    /CONFIRM_MIGRATION_INVENTORY_SCHEMA_APPLY: \$\{CONFIRM_MIGRATION_INVENTORY_SCHEMA_APPLY:-\}/,
  );
  assert.match(service, /POSTGRES_HOST: crawler-postgres/);
  assert.match(service, /POSTGRES_PORT: 5432/);
});
