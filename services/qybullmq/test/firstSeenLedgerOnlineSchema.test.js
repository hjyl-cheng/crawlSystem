import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  firstSeenLedgerOnlineSchemaApplyGuard,
} from "../scripts/applyFirstSeenLedgerOnlineSchema.mjs";

test("First-Seen ledger separates startup compatibility DDL from its online migration", async () => {
  const [schema, bootstrap, migration, packageJson] = await Promise.all([
    readFile(new URL("../src/schema.sql", import.meta.url), "utf8"),
    readFile(new URL("../../../database/bootstrap/crawler.sql", import.meta.url), "utf8"),
    readFile(new URL("../scripts/applyFirstSeenLedgerOnlineSchema.mjs", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);

  assert.match(
    schema,
    /ADD CONSTRAINT content_candidates_first_seen_ledger_shape_check[\s\S]*?NOT VALID/,
  );
  assert.doesNotMatch(
    schema,
    /DROP CONSTRAINT IF EXISTS content_candidates_first_seen_ledger_(?:shape_check|observation_id_fkey)/,
  );
  assert.doesNotMatch(
    schema,
    /CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_first_seen_ledger_pending/,
  );

  assert.match(migration, /CREATE INDEX CONCURRENTLY/);
  assert.match(migration, /DROP INDEX CONCURRENTLY/);
  assert.match(migration, /ON crawler\.content_candidates \(channel_id,candidate_id\)/);
  assert.match(
    migration,
    /VALIDATE CONSTRAINT content_candidates_first_seen_ledger_shape_check/,
  );
  assert.match(
    migration,
    /VALIDATE CONSTRAINT content_candidates_first_seen_ledger_observation_id_fkey/,
  );
  assert.match(migration, /pg_try_advisory_lock/);
  assert.match(migration, /lock_timeout/);
  assert.match(migration, /statement_timeout/);
  assert.match(migration, /invalid_shape_count/);
  assert.match(migration, /orphan_observation_count/);

  assert.match(
    bootstrap,
    /CREATE INDEX idx_crawler_content_candidates_first_seen_ledger_pending[\s\S]*?\(channel_id, candidate_id\)/,
  );
  assert.match(packageJson, /schema:first-seen-ledger-online/);
});

test("First-Seen ledger online migration requires explicit database and row-count guards", () => {
  const base = {
    DATABASE_URL: "postgres://crawler-admin:secret@crawler/crawler_test",
    CONFIRM_FIRST_SEEN_LEDGER_SCHEMA_APPLY: "crawler_test",
    EXPECTED_CRAWLER_CANDIDATE_MIN_ROWS: "1058693",
    EXPECTED_FIRST_SEEN_LEDGER_PENDING_COUNT: "17",
  };

  assert.throws(
    () => firstSeenLedgerOnlineSchemaApplyGuard(base, ["node", "script"]),
    /pass --apply explicitly/,
  );
  assert.throws(() => firstSeenLedgerOnlineSchemaApplyGuard({
    ...base,
    CONFIRM_FIRST_SEEN_LEDGER_SCHEMA_APPLY: "",
  }, ["node", "script", "--apply"]), /CONFIRM_FIRST_SEEN_LEDGER_SCHEMA_APPLY/);
  for (const name of [
    "EXPECTED_CRAWLER_CANDIDATE_MIN_ROWS",
    "EXPECTED_FIRST_SEEN_LEDGER_PENDING_COUNT",
  ]) {
    assert.throws(() => firstSeenLedgerOnlineSchemaApplyGuard({
      ...base,
      [name]: "1junk",
    }, ["node", "script", "--apply"]), new RegExp(name));
  }
  assert.deepEqual(
    firstSeenLedgerOnlineSchemaApplyGuard(base, ["node", "script", "--apply"]),
    {
      databaseUrl: base.DATABASE_URL,
      confirmedDatabase: "crawler_test",
      expectedMinimumCandidateRows: 1058693,
      expectedPendingCount: 17,
    },
  );
});
