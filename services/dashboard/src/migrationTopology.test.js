import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  assertCrawlerDashboardIdentity,
  assertMigrationSourceIdentity,
  filterAndPageMigrationCandidates,
  mergeMigrationCandidate,
  mergeMigrationCandidates,
  migrationReadModelStats,
} from "./migrationTopology.js";

test("Dashboard Crawler connection rejects the legacy Writer database", () => {
  assert.throws(
    () => assertCrawlerDashboardIdentity({
      database_name: "bullmq_crawler_migration",
      database_user: "bullmq",
      transaction_read_only: "off",
      identity_kind: "crawler",
      identity_database: "bullmq_crawler_migration",
    }, {
      expectedDatabase: "bullmq_crawler_migration",
      forbiddenDatabase: "bullmq_crawler_migration",
    }),
    /forbidden Crawler database/,
  );
});

test("Dashboard Migration Source requires pinned OID, role, and read-only transaction", () => {
  const row = {
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
  assert.deepEqual(assertMigrationSourceIdentity(row, {
    expectedDatabase: "bullmq_crawler_migration",
    expectedDatabaseOid: "16384",
    expectedUser: "migration_reader",
    targetDatabase: "newcrawler_crawler",
  }), {
    database: "bullmq_crawler_migration",
    databaseOid: "16384",
    user: "migration_reader",
  });
  assert.throws(
    () => assertMigrationSourceIdentity({ ...row, transaction_read_only: "off" }, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    /read-only/,
  );
  assert.throws(
    () => assertMigrationSourceIdentity({ ...row, default_transaction_read_only: "off" }, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    /default to read-only/,
  );
  assert.throws(
    () => assertMigrationSourceIdentity({ ...row, channel_write: true }, {
      expectedDatabase: "bullmq_crawler_migration",
      expectedDatabaseOid: "16384",
      expectedUser: "migration_reader",
      targetDatabase: "newcrawler_crawler",
    }),
    /write privileges/,
  );
});

test("Migration read model keeps source facts and overlays only Target lifecycle state", () => {
  const source = {
    candidate_id: "42",
    channel_id: "UC123",
    channel_url: "https://youtube.example/source",
    title: "Source title",
    handle: "@source",
    search_subscriber_count: "1200",
    source_json: { source: "legacy_results_db" },
    updated_at: "2026-08-01T00:00:00.000Z",
  };
  const pending = mergeMigrationCandidate(source, null);
  assert.equal(pending.candidate_id, "42");
  assert.equal(pending.title, "Source title");
  assert.equal(pending.candidate_status, "discovered");
  assert.equal(pending.migration_started, false);

  const completed = mergeMigrationCandidate(source, {
    migration_intent_id: "7",
    target_candidate_id: "91",
    target_candidate_status: "accepted",
    target_channel_status: "active",
    registry_promotion_candidate_id: "91",
    agent_status: "done",
    final_status: "ready_auto",
    run_status: "completed",
    content_count: "10",
    updated_at: "2026-08-02T00:00:00.000Z",
  });
  assert.equal(completed.candidate_id, "42");
  assert.equal(completed.target_candidate_id, "91");
  assert.equal(completed.title, "Source title");
  assert.equal(completed.candidate_status, "accepted");
  assert.equal(completed.final_status, "ready_auto");
  assert.equal(completed.migration_started, true);
  assert.equal(completed.migration_done, true);
});

test("Migration Target state falls back from source candidate ID to channel ID", () => {
  const source = [
    { candidate_id: "42", channel_id: "UC-exact", title: "Exact" },
    { candidate_id: "84", channel_id: "UC-fallback", title: "Fallback" },
  ];
  const target = [
    {
      source_candidate_id: "42",
      channel_id: "UC-exact",
      migration_intent_id: "1",
      target_candidate_status: "queued",
    },
    {
      source_candidate_id: "21",
      channel_id: "UC-fallback",
      migration_intent_id: "2",
      target_candidate_status: "accepted",
      target_channel_status: "active",
      target_candidate_id: "99",
      registry_promotion_candidate_id: "99",
      final_status: "ready_auto",
    },
  ];

  const merged = mergeMigrationCandidates(source, target);
  assert.equal(merged[0].candidate_status, "queued");
  assert.equal(merged[0].migration_intent_id, "1");
  assert.equal(merged[1].candidate_status, "accepted");
  assert.equal(merged[1].migration_intent_id, "2");
  assert.equal(merged[1].migration_done, true);
});

test("Migration stats count source inventory separately from Target results", () => {
  assert.deepEqual(migrationReadModelStats(3, [
    { candidate_status: "discovered", migration_started: false },
    { candidate_status: "failed", migration_started: true },
    { candidate_status: "accepted", migration_started: true, migration_done: true, final_status: "ready_auto" },
  ]), {
    total: 3,
    discovered: 1,
    queued: 0,
    validating: 0,
    finishing: 0,
    failed: 1,
    migration_done: 1,
    final_done: 1,
  });
});

test("Migration status filters are applied globally before pagination", () => {
  const candidates = [
    { channel_id: "UC1", candidate_status: "discovered", agent_status: "pending", final_status: "pending" },
    { channel_id: "UC2", candidate_status: "queued", agent_status: "pending", final_status: "pending" },
    { channel_id: "UC3", candidate_status: "failed", agent_status: "failed", final_status: "pending" },
    { channel_id: "UC4", candidate_status: "failed", agent_status: "failed", final_status: "pending" },
    { channel_id: "UC5", candidate_status: "failed", agent_status: "done", final_status: "ready_auto" },
  ];
  assert.deepEqual(filterAndPageMigrationCandidates(candidates, {
    channelStatus: "failed",
    agentStatus: "failed",
    finalStatus: "pending",
    offset: 1,
    limit: 1,
  }), {
    rows: [candidates[3]],
    total: 2,
  });
});

test("Dashboard source and target SQL stay on separate connection helpers", async () => {
  const server = await readFile(new URL("./server.js", import.meta.url), "utf8");
  assert.match(server, /BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY/);
  assert.match(server, /assertMigrationSourceIdentity/);
  assert.match(server, /crawler\.migration_channel_intents/);
  assert.doesNotMatch(server, /migrationRead\([\s\S]{0,120}UPDATE\s+crawler\./);
});
