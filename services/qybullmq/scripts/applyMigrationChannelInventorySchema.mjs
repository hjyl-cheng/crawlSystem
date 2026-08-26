import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { databaseUrl } from "../src/databaseConnection.js";
import { verifyCrawlerWriterDatabase } from "../src/databaseIdentity.js";
import {
  assertMigrationChannelInventorySchema,
  loadMigrationChannelInventorySchemaSql,
  loadMigrationChannelInventorySchemaState,
} from "../src/migrationInventorySchema.js";

const { Client } = pg;
const MIGRATION_LOCK = 781137244;

function nonnegativeInteger(environment, name) {
  const raw = String(environment[name] ?? "").trim();
  const value = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(value),
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

export function migrationInventorySchemaApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const expectedDatabase = String(environment.EXPECTED_CRAWLER_DATABASE || "").trim();
  const confirmedDatabase = String(
    environment.CONFIRM_MIGRATION_INVENTORY_SCHEMA_APPLY || "",
  ).trim();
  if (!expectedDatabase) throw new Error("EXPECTED_CRAWLER_DATABASE is required");
  if (confirmedDatabase !== expectedDatabase) {
    throw new Error(
      "CONFIRM_MIGRATION_INVENTORY_SCHEMA_APPLY must equal EXPECTED_CRAWLER_DATABASE",
    );
  }
  return {
    confirmedDatabase,
    databaseUrl: databaseUrl(environment),
    expectedInventoryRowCount: nonnegativeInteger(
      environment,
      "EXPECTED_MIGRATION_INVENTORY_ROW_COUNT",
    ),
  };
}

async function inventoryRowCount(client, schemaState) {
  if (schemaState?.inventory_table_ready !== true) return 0;
  const result = await client.query(
    "SELECT count(*)::bigint AS inventory_count FROM crawler.migration_channel_inventory",
  );
  return Number(result.rows[0]?.inventory_count || 0);
}

async function main() {
  const guard = migrationInventorySchemaApplyGuard();
  const client = new Client({
    connectionString: guard.databaseUrl,
    application_name: "migration-inventory-schema-publisher-v1",
  });
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='300s'");
    await client.query(`SELECT pg_advisory_xact_lock(${MIGRATION_LOCK})`);
    const identity = await verifyCrawlerWriterDatabase(
      client.query.bind(client),
      process.env,
    );
    assert.equal(identity.database, guard.confirmedDatabase, "unexpected Crawler database");

    const beforeState = await loadMigrationChannelInventorySchemaState(
      client.query.bind(client),
    );
    const beforeCount = await inventoryRowCount(client, beforeState);
    assert.equal(
      beforeCount,
      guard.expectedInventoryRowCount,
      "unexpected Migration inventory row count",
    );

    await client.query(await loadMigrationChannelInventorySchemaSql());
    await assertMigrationChannelInventorySchema({ query: client.query.bind(client) });
    const afterCount = await inventoryRowCount(client, { inventory_table_ready: true });
    assert.equal(afterCount, beforeCount, "Schema publication changed Migration inventory rows");
    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: identity.database,
      inventory_row_count: afterCount,
      migration: "migration-channel-inventory-schema-v1",
    }));
  } catch (error) {
    if (began) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
