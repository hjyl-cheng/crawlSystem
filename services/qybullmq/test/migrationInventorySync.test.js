import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  migrationInventoryForceSyncEnabled,
  syncMigrationChannelInventory,
} from "../src/migrationInventorySync.js";

const environment = {
  MIGRATION_DATABASE_URL: "postgresql://migration_reader:secret@migration-postgres:5432/bullmq_crawler_migration",
  MIGRATION_SOURCE_ID: "qy-migration-v1",
  EXPECTED_MIGRATION_DATABASE: "bullmq_crawler_migration",
  EXPECTED_MIGRATION_DATABASE_OID: "16384",
  EXPECTED_MIGRATION_DATABASE_USER: "migration_reader",
  EXPECTED_CRAWLER_DATABASE: "newcrawler_crawler",
  MIGRATION_POSTGRES_STATEMENT_TIMEOUT_MS: "2500",
  MIGRATION_INVENTORY_SOURCE_STATEMENT_TIMEOUT_MS: "120000",
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

function normalized(sql) {
  return sql.replace(/\s+/g, " ").trim();
}

test("Migration inventory sync streams Source rows and atomically publishes Target inventory", async () => {
  const sourceStatements = [];
  const sourceRows = [
    {
      candidate_id: "11",
      channel_id: "UC11",
      channel_url: "https://www.youtube.com/channel/UC11",
      handle: "@eleven",
      title: "Eleven",
      avatar_url: null,
      search_subscriber_count: "1200",
      priority: 100,
      source_candidate_status: "discovered",
      updated_at: "2026-08-01T00:00:00.000Z",
    },
    {
      candidate_id: "12",
      channel_id: "UC12",
      channel_url: "https://www.youtube.com/channel/UC12",
      handle: "@twelve",
      title: "Twelve",
      avatar_url: null,
      search_subscriber_count: null,
      priority: 90,
      source_candidate_status: "failed",
      updated_at: "2026-08-02T00:00:00.000Z",
    },
  ];
  let fetchCount = 0;
  const sourceClient = {
    async query(sql) {
      const statement = normalized(sql);
      sourceStatements.push(statement);
      if (statement.includes("current_database() AS database_name")) {
        return { rows: [identityRow()] };
      }
      if (statement.startsWith("FETCH FORWARD")) {
        fetchCount += 1;
        return { rows: fetchCount === 1 ? sourceRows : [] };
      }
      return { rows: [] };
    },
    release() {
      sourceStatements.push("RELEASE");
    },
  };
  const targetStatements = [];
  const targetClient = {
    async query(sql, params = []) {
      const statement = normalized(sql);
      targetStatements.push({ sql: statement, params });
      if (statement.includes("FROM crawler.migration_channel_inventory_syncs")) {
        return { rows: [] };
      }
      if (statement === "ANALYZE crawler.migration_channel_inventory") {
        throw new Error("best-effort analyze failed");
      }
      return { rows: [] };
    },
    release() {},
  };
  const progress = [];

  const result = await syncMigrationChannelInventory({
    targetPool: { async connect() { return targetClient; } },
    sourcePool: { async connect() { return sourceClient; } },
    environment,
    batchSize: 2,
    onProgress: (entry) => progress.push(entry),
  });

  assert.equal(result.status, "ready");
  assert.equal(result.eligible_count, 2);
  assert.equal(result.skipped, false);
  assert.equal(targetStatements[0].sql, "BEGIN");
  assert.match(targetStatements[1].sql, /pg_advisory_xact_lock/);
  assert.equal(targetStatements.at(-2).sql, "COMMIT");
  assert.equal(targetStatements.at(-1).sql, "ANALYZE crawler.migration_channel_inventory");
  const upsert = targetStatements.find((entry) => (
    entry.sql.startsWith("INSERT INTO crawler.migration_channel_inventory (")
  ));
  assert.ok(upsert);
  assert.equal(JSON.parse(upsert.params[2]).length, 2);
  assert.match(upsert.sql, /jsonb_to_recordset/);
  assert.match(upsert.sql, /ON CONFLICT \(source_id,channel_id\) DO UPDATE/);
  assert.ok(targetStatements.some((entry) => (
    entry.sql.startsWith("DELETE FROM crawler.migration_channel_inventory ")
  )));
  assert.ok(targetStatements.some((entry) => /SET status='ready'/.test(entry.sql)));
  assert.ok(sourceStatements.some((statement) => statement.startsWith("DECLARE migration_channel_inventory_source")));
  assert.ok(sourceStatements.includes("SET LOCAL statement_timeout=120000"));
  assert.equal(sourceStatements.filter((statement) => statement.startsWith("FETCH FORWARD")).length, 2);
  assert.deepEqual(progress, [{ source_id: "qy-migration-v1", eligible_count: 2 }]);
});

test("Migration inventory force refresh accepts only explicit true or false", () => {
  assert.equal(migrationInventoryForceSyncEnabled({}), false);
  assert.equal(migrationInventoryForceSyncEnabled({ MIGRATION_INVENTORY_FORCE_SYNC: "false" }), false);
  assert.equal(migrationInventoryForceSyncEnabled({ MIGRATION_INVENTORY_FORCE_SYNC: " TRUE " }), true);
  assert.throws(
    () => migrationInventoryForceSyncEnabled({ MIGRATION_INVENTORY_FORCE_SYNC: "treu" }),
    /MIGRATION_INVENTORY_FORCE_SYNC must be true or false/,
  );
});

test("ready Migration inventory skips Source without opening a connection", async () => {
  let sourceConnections = 0;
  const targetStatements = [];
  const targetClient = {
    async query(sql) {
      const statement = normalized(sql);
      targetStatements.push(statement);
      if (statement.includes("FROM crawler.migration_channel_inventory_syncs")) {
        return {
          rows: [{
            source_database: "bullmq_crawler_migration",
            source_database_oid: "16384",
            status: "ready",
            eligible_count: "410292",
            inventory_count: "410292",
            completed_at: "2026-08-26T00:00:00.000Z",
          }],
        };
      }
      return { rows: [] };
    },
    release() {},
  };

  const result = await syncMigrationChannelInventory({
    targetPool: { async connect() { return targetClient; } },
    sourcePool: { async connect() { sourceConnections += 1; throw new Error("unexpected"); } },
    environment,
  });

  assert.equal(result.status, "ready");
  assert.equal(result.skipped, true);
  assert.equal(result.eligible_count, 410292);
  assert.equal(sourceConnections, 0);
  assert.deepEqual(targetStatements.slice(0, 2), [
    "BEGIN",
    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
  ]);
  assert.equal(targetStatements.at(-1), "COMMIT");
});

test("QYBullMQ API prepares inventory before becoming healthy", async () => {
  const server = await readFile(new URL("../src/server.js", import.meta.url), "utf8");
  const assertIndex = server.indexOf("await assertMigrationChannelInventorySchema");
  const syncIndex = server.indexOf("await syncMigrationChannelInventory");
  const listenIndex = server.indexOf("app.listen(");

  assert.ok(assertIndex >= 0);
  assert.ok(syncIndex > assertIndex);
  assert.ok(listenIndex > syncIndex);
  assert.match(server, /migrationInventorySyncConfigured\(\)/);
  assert.match(server, /migrationInventoryForceSyncEnabled\(\)/);
  assert.doesNotMatch(server, /MIGRATION_INVENTORY_FORCE_SYNC \|\| ""/);
  assert.doesNotMatch(server, /ensureMigrationChannelInventorySchema/);
  assert.doesNotMatch(server, /loadMigrationChannelInventorySchemaSql/);

  const runtimeSchema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  assert.doesNotMatch(runtimeSchema, /migration_channel_inventory/);
});
