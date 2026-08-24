import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  assertCrawlerDashboardIdentity,
  assertMigrationSourceIdentity,
  loadMigrationSourceCount,
  loadMigrationSourcePage,
  migrationCandidateMatches,
  migrationFiltersIncludeUnstarted,
  mergeMigrationCandidate,
  mergeMigrationCandidates,
  migrationReadModelStatsFromSummary,
  migrationSourceCandidateFromIntent,
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

test("Migration Source applies pagination in SQL before loading wide candidate rows", async () => {
  const calls = [];
  const expectedRows = [{ candidate_id: "51", channel_id: "UC-page" }];
  const rows = await loadMigrationSourcePage({
    read: async (sql, params) => {
      calls.push({ sql, params });
      return { rows: expectedRows };
    },
    search: "Lisa",
    limit: 25,
    offset: 50,
  });

  assert.equal(calls.length, 1);
  const normalizedSql = calls[0].sql.replace(/\s+/g, " ");
  assert.match(normalizedSql, /source_page AS \(/);
  assert.match(
    normalizedSql,
    /source_candidate_status IN \('discovered','queued','validating','failed'\)/,
  );
  assert.match(normalizedSql, /LIMIT \$2::int OFFSET \$3::int/);
  assert.match(
    normalizedSql,
    /FROM source_page page JOIN crawler\.channel_candidates candidate/,
  );
  assert.doesNotMatch(
    normalizedSql.slice(0, normalizedSql.indexOf("source_page AS")),
    /candidate\.\*/,
  );
  assert.deepEqual(calls[0].params, ["%Lisa%", 25, 50]);
  assert.equal(rows, expectedRows);
});

test("Migration Source total uses a narrow aggregate query", async () => {
  const calls = [];
  const total = await loadMigrationSourceCount({
    read: async (sql, params) => {
      calls.push({ sql, params });
      return { rows: [{ total: 410_292 }] };
    },
  });

  assert.equal(total, 410_292);
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /count\(\*\)::int AS total/);
  assert.match(calls[0].sql, /candidate\.channel_rank=1/);
  assert.match(
    calls[0].sql,
    /candidate\.source_candidate_status IN \('discovered','queued','validating','failed'\)/,
  );
  assert.doesNotMatch(calls[0].sql, /snapshot_json|candidate\.\*/);
  assert.deepEqual(calls[0].params, []);
});

test("Migration filter planning includes unstarted rows only when their default state matches", () => {
  assert.equal(migrationFiltersIncludeUnstarted({
    channelStatus: "all",
    agentStatus: "",
    finalStatus: "",
  }), true);
  assert.equal(migrationFiltersIncludeUnstarted({
    channelStatus: "discovered",
    agentStatus: "pending",
    finalStatus: "pending",
  }), true);
  assert.equal(migrationFiltersIncludeUnstarted({
    channelStatus: "failed",
    agentStatus: "",
    finalStatus: "",
  }), false);
  assert.equal(migrationCandidateMatches({
    candidate_status: "accepted",
    agent_status: "done",
    final_status: "ready_auto",
  }, {
    channelStatus: "all",
    agentStatus: "",
    finalStatus: "",
  }), false);
});

test("Migration Target snapshot restores the immutable Source candidate fields", () => {
  assert.deepEqual(migrationSourceCandidateFromIntent({
    source_candidate_id: "42",
    channel_id: "UC42",
    source_snapshot: {
      source_dispatch_batch_id: "legacy-batch",
      channel_url: "https://www.youtube.com/channel/UC42",
      handle: "@fortytwo",
      title: "Forty Two",
      avatar_url: "https://example.test/avatar.jpg",
      search_subscriber_count: "1200",
      source_candidate_status: "discovered",
      snapshot_json: { channel_header: { title: "Forty Two" } },
      source_json: {
        source: "legacy_results_db",
        source_rowid: "99",
        legacy_import: { country: "BR" },
      },
      source_created_at: "2026-08-01T00:00:00.000Z",
      source_updated_at: "2026-08-02T00:00:00.000Z",
    },
  }), {
    candidate_id: "42",
    dispatch_batch_id: "legacy-batch",
    channel_id: "UC42",
    channel_url: "https://www.youtube.com/channel/UC42",
    handle: "@fortytwo",
    title: "Forty Two",
    avatar_url: "https://example.test/avatar.jpg",
    search_subscriber_count: "1200",
    source_candidate_status: "discovered",
    snapshot_json: { channel_header: { title: "Forty Two" } },
    source_json: {
      source: "legacy_results_db",
      source_rowid: "99",
      legacy_import: { country: "BR" },
    },
    legacy_country: "BR",
    legacy_target_reason: null,
    legacy_evidence_score: null,
    legacy_evidence_reasons: null,
    legacy_discovered_at: null,
    source_rowid: "99",
    created_at: "2026-08-01T00:00:00.000Z",
    updated_at: "2026-08-02T00:00:00.000Z",
  });
});

test("Migration stats combine Source inventory with Target aggregates", () => {
  assert.deepEqual(migrationReadModelStatsFromSummary(1000, {
    started: 20,
    discovered: 1,
    queued: 3,
    validating: 2,
    finishing: 4,
    failed: 2,
    migration_done: 5,
    final_done: 6,
  }), {
    total: 992,
    discovered: 981,
    queued: 3,
    validating: 2,
    finishing: 4,
    failed: 2,
    migration_done: 5,
    final_done: 6,
  });
});

test("Dashboard source and target SQL stay on separate connection helpers", async () => {
  const server = await readFile(new URL("./server.js", import.meta.url), "utf8");
  assert.match(server, /BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY/);
  assert.match(server, /assertMigrationSourceIdentity/);
  assert.match(server, /crawler\.migration_channel_intents/);
  for (const selection of ["100", "200", "500", "1000", "2000"]) {
    assert.match(server, new RegExp(`\\["${selection}", "${selection}"\\]`));
  }
  assert.doesNotMatch(server, /migrationRead\([\s\S]{0,120}UPDATE\s+crawler\./);
});
