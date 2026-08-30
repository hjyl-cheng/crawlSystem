import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { loadMigrationChannelInventory } from "./migrationInventory.js";

function normalized(sql) {
  return sql.replace(/\s+/g, " ").trim();
}

test("Migration list filters, orders, and paginates in the Target database before content counts", async () => {
  const calls = [];
  const read = async (sql, params) => {
    const statement = normalized(sql);
    calls.push({ sql: statement, params });
    if (statement.includes("FROM crawler.migration_channel_inventory_syncs")) {
      return {
        rows: [{
          source_database: "bullmq_crawler_migration",
          source_database_oid: "16384",
          status: "ready",
          eligible_count: "410292",
          completed_at: "2026-08-26T00:00:00.000Z",
        }],
      };
    }
    if (statement.includes("AS filtered_count")) {
      return {
        rows: [{
          total: "410000",
          discovered: "409980",
          queued: "3",
          validating: "2",
          finishing: "4",
          failed: "11",
          migration_done: "292",
          final_done: "292",
          filtered_count: "7",
        }],
      };
    }
    return {
      rows: [{
        candidate_id: "51",
        target_candidate_id: "482",
        active_system_retry_id: "801",
        channel_id: "UC51",
        title: "Lisa",
        subscriber_count: "1200",
        candidate_status: "discovered",
      }],
    };
  };

  const result = await loadMigrationChannelInventory({
    read,
    sourceId: "qy-migration-v1",
    expectedSourceDatabase: "bullmq_crawler_migration",
    expectedSourceDatabaseOid: "16384",
    filters: {
      search: "Lisa",
      channelStatus: "all",
      agentStatus: "",
      finalStatus: "",
      limit: 50,
      offset: 100,
    },
  });

  assert.equal(calls.length, 3);
  assert.equal(result.total, 7);
  assert.equal(result.channels.length, 1);
  assert.equal(result.channels[0].active_system_retry_id, "801");
  assert.equal(result.stats.total, 410000);
  assert.equal(result.stats.migration_done, 292);
  const page = calls.find((call) => call.sql.includes("filtered_page AS"));
  assert.ok(page);
  assert.match(page.sql, /FROM crawler\.migration_channel_inventory inventory/);
  assert.match(page.sql, /crawler\.migration_system_retry_items/);
  assert.match(page.sql, /system_retry\.candidate_id=intent\.target_candidate_id/);
  assert.match(page.sql, /AS active_system_retry_id/);
  assert.match(page.sql, /WHERE [\s\S]*state\.candidate_status IN/);
  assert.match(
    page.sql,
    /ORDER BY state\.priority DESC,state\.source_candidate_id ASC LIMIT \$\d+::int OFFSET \$\d+::int/,
  );
  assert.deepEqual(page.params.slice(-2), [50, 100]);
  assert.ok(
    page.sql.indexOf("LIMIT") < page.sql.indexOf("FROM crawler.contents content"),
    "content aggregation must run only for the selected page",
  );
});

test("Migration list refuses to read partially synchronized inventory", async () => {
  let calls = 0;
  await assert.rejects(
    loadMigrationChannelInventory({
      read: async () => {
        calls += 1;
        return { rows: [{ status: "syncing", eligible_count: "100" }] };
      },
      sourceId: "qy-migration-v1",
      expectedSourceDatabase: "bullmq_crawler_migration",
      expectedSourceDatabaseOid: "16384",
      filters: { limit: 50, offset: 0 },
    }),
    /inventory is syncing/,
  );
  assert.equal(calls, 1);
});

test("Migration list rejects inventory copied from a different pinned Source", async () => {
  await assert.rejects(
    loadMigrationChannelInventory({
      read: async () => ({
        rows: [{
          source_database: "bullmq_crawler_migration",
          source_database_oid: "99999",
          status: "ready",
          eligible_count: "410292",
        }],
      }),
      sourceId: "qy-migration-v1",
      expectedSourceDatabase: "bullmq_crawler_migration",
      expectedSourceDatabaseOid: "16384",
      filters: { limit: 50, offset: 0 },
    }),
    /source_identity_mismatch/,
  );
});

test("Dashboard Migration list route cannot fall back to Source-side pagination", async () => {
  const server = await readFile(new URL("./server.js", import.meta.url), "utf8");
  const start = server.indexOf("async function migrationChannelListData(req)");
  const end = server.indexOf("async function migrationChannelDetailData(channelId)", start);
  assert.ok(start >= 0 && end > start);
  const listFunction = server.slice(start, end);

  assert.match(listFunction, /loadMigrationChannelInventory/);
  assert.match(listFunction, /loadMigrationSystemRetriesSafely\(\{ read: db \}\)/);
  assert.match(listFunction, /intValue\(req\.query\.limit, 50, 1, 50\)/);
  assert.doesNotMatch(listFunction, /migrationRead|loadMigrationSource|limit:\s*500/);
  assert.match(server, /<th>源库订阅<\/th>/);
  assert.match(server, /const hasActiveSystemRetry = channel\.active_system_retry_id != null/);
  assert.match(
    server,
    /\["discovered", "failed"\]\.includes\(candidateStatus\) && !hasActiveSystemRetry/,
  );
});
