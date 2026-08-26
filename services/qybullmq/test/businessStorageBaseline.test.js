import assert from "node:assert/strict";
import test from "node:test";
import { businessStorageBaselineCommand } from "../scripts/reportBusinessStorageBaseline.mjs";
import {
  BusinessStorageBaselineReporter,
  businessStorageBaselineConfig,
} from "../src/businessStorageBaseline.js";

function environment(overrides = {}) {
  return {
    BUSINESS_DATABASE_URL: "postgres://business-reader@business/business_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "197",
    ...overrides,
  };
}

function fakePool() {
  const calls = [];
  const client = {
    async query(sql) {
      calls.push(sql);
      if (sql.includes("business-storage-baseline:identity")) {
        return { rows: [{
          database_name: "business_test",
          database_user: "business_reader",
          identity_kind: "business",
          identity_database: "business_test",
          channel_count: 197,
          database_bytes: "1048576",
          wal_lsn: "0/12345678",
          stats_reset: "2026-08-25T00:00:00.000Z",
        }] };
      }
      if (sql.includes("business-storage-baseline:relations")) {
        return { rows: [{
          schema_name: "publication",
          relation_name: "inbox",
          heap_bytes: "4096",
          toast_heap_bytes: "6144",
          toast_index_bytes: "2048",
          toast_total_bytes: "8192",
          index_bytes: "2048",
          total_bytes: "14336",
          estimated_live_rows: "615",
          estimated_dead_rows: "2",
          tuples_inserted: "620",
          tuples_updated: "5",
          tuples_deleted: "0",
          tuples_hot_updated: "2",
          vacuum_count: "0",
          autovacuum_count: "3",
          analyze_count: "0",
          autoanalyze_count: "3",
          last_vacuum: null,
          last_autovacuum: "2026-08-25T01:00:00.000Z",
          last_analyze: null,
          last_autoanalyze: "2026-08-25T01:00:00.000Z",
        }] };
      }
      if (sql.includes("business-storage-baseline:indexes")) {
        return { rows: [{
          schema_name: "publication",
          relation_name: "inbox",
          index_name: "inbox_pkey",
          index_bytes: "16384",
          idx_scan: "0",
          idx_tup_read: "0",
          idx_tup_fetch: "0",
          is_primary: true,
          is_unique: true,
          index_definition: "CREATE UNIQUE INDEX inbox_pkey ON publication.inbox USING btree (revision_id)",
        }] };
      }
      if (sql.includes("business-storage-baseline:search")) {
        return { rows: [{
          write_mode: "shadow",
          read_mode: "legacy",
          active_watermark: "publication_projection_abc",
          live_rows: "197",
          active_legacy_rows: "197",
          legacy_rows: "5280",
          change_rows: "208",
          release_rows: "43",
          cumulative_changed_channels: "208",
        }] };
      }
      if (sql.includes("business-storage-baseline:entities")) {
        return { rows: [
          { entity_name: "channels", row_count: "197" },
          { entity_name: "content_items", row_count: "3633" },
          { entity_name: "revisions", row_count: "615" },
        ] };
      }
      if (sql.includes("business-storage-baseline:snapshots")) {
        return { rows: [{
          content_count: "100",
          snapshot_rows: "100",
          versions_p50: "1",
          versions_p95: "1",
          versions_p99: "1",
          versions_max: "1",
          raw_item_logical_bytes: "2048",
          raw_item_stored_bytes: "1024",
          raw_channel_logical_bytes: "4096",
          raw_channel_stored_bytes: "2048",
        }] };
      }
      if (sql.includes("business-storage-baseline:projection-batches")) {
        return { rows: [{
          status: "published",
          batch_count: "43",
          projected_channel_count: "208",
          duration_ms_p50: "12",
          duration_ms_p95: "40",
          duration_ms_p99: "55",
          duration_ms_max: "60",
        }] };
      }
      if (sql.includes("business-storage-baseline:projection")) {
        return { rows: [{ status: "delivered", row_count: "208", maximum_attempts: "1" }] };
      }
      return { rows: [] };
    },
    release() { calls.push("RELEASE"); },
  };
  return { calls, pool: { async connect() { return client; } } };
}

test("Business storage baseline CLI has no write mode", () => {
  assert.deepEqual(businessStorageBaselineCommand([]), { help: false, output: null });
  assert.deepEqual(businessStorageBaselineCommand(["--output", "baseline.json"]), {
    help: false,
    output: "baseline.json",
  });
  assert.throws(() => businessStorageBaselineCommand(["--apply"]), /unknown option/);
});

test("Business storage baseline config pins database identity and Channel count", () => {
  assert.deepEqual(businessStorageBaselineConfig(environment()), {
    databaseUrl: "postgres://business-reader@business/business_test",
    expectedDatabase: "business_test",
    expectedBusinessChannelCount: 197,
  });
  assert.throws(
    () => businessStorageBaselineConfig(environment({ EXPECTED_BUSINESS_CHANNEL_COUNT: "197.0" })),
    /explicit non-negative integer/,
  );
});

test("Business storage baseline runs one read-only snapshot and preserves index counters", async () => {
  const config = businessStorageBaselineConfig(environment());
  const fixture = fakePool();
  const reporter = new BusinessStorageBaselineReporter({ pool: fixture.pool, config });
  const report = await reporter.capture();
  assert.deepEqual(report.measurement, {
    kind: "point_in_time_snapshot",
    computes_inter_round_deltas: false,
    toast_scope: "physical_relation_level",
  });
  assert.equal(report.database.database_name, "business_test");
  assert.equal(report.database.stats_reset, "2026-08-25T00:00:00.000Z");
  assert.equal(report.relations[0].toast_heap_bytes, 6144);
  assert.equal(report.relations[0].toast_index_bytes, 2048);
  assert.equal(report.relations[0].toast_total_bytes, 8192);
  assert.equal(report.relations[0].autovacuum_count, 3);
  assert.deepEqual(report.indexes[0], {
    schema_name: "publication",
    relation_name: "inbox",
    index_name: "inbox_pkey",
    index_bytes: 16384,
    idx_scan: 0,
    idx_tup_read: 0,
    idx_tup_fetch: 0,
    is_primary: true,
    is_unique: true,
    index_definition: "CREATE UNIQUE INDEX inbox_pkey ON publication.inbox USING btree (revision_id)",
  });
  assert.equal(report.snapshots.versions_p99, 1);
  assert.deepEqual(report.entities, {
    channels: 197,
    content_items: 3633,
    revisions: 615,
  });
  assert.equal(report.projection_batches[0].duration_ms_p95, 40);
  assert.ok(fixture.calls.some((sql) => /BEGIN.*READ ONLY/i.test(sql)));
  assert.ok(fixture.calls.includes("ROLLBACK"));
  assert.ok(!fixture.calls.some((sql) => /\b(?:INSERT|UPDATE|DELETE|TRUNCATE)\b/i.test(sql)));
});
