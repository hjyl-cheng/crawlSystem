import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  constraintsHaveFinalShape,
  firstSeenLedgerOnlineSchemaApplyGuard,
  indexHasFinalShape,
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
  assert.match(migration, /FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL/);
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
    DATABASE_URL: "postgres://crawler-writer:secret@crawler-pgbouncer:6432/crawler_test",
    FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL:
      "postgres://crawler-admin:secret@crawler-postgres:5432/crawler_test",
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
      databaseUrl: base.FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL,
      confirmedDatabase: "crawler_test",
      expectedMinimumCandidateRows: 1058693,
      expectedPendingCount: 17,
      expectedServerPort: 5432,
    },
  );

  assert.throws(() => firstSeenLedgerOnlineSchemaApplyGuard({
    ...base,
    FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL: "",
  }, ["node", "script", "--apply"]), /FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL/);
  assert.throws(() => firstSeenLedgerOnlineSchemaApplyGuard({
    ...base,
    FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL:
      "postgres://crawler-admin:secret@crawler-pgbouncer:6432/crawler_test",
  }, ["node", "script", "--apply"]), /direct PostgreSQL endpoint/);
});

test("First-Seen ledger postflight checks complete constraint and index definitions", () => {
  const constraints = [
    {
      constraint_name: "content_candidates_first_seen_ledger_shape_check",
      constraint_type: "c",
      validated: true,
      check_expression: `
        (((first_seen_ledger_status='not_applicable')
            AND (first_seen_ledger_observation_id IS NULL))
          OR ((first_seen_ledger_status='pending')
            AND (first_seen_ledger_observation_id IS NULL))
          OR ((first_seen_ledger_status='consumed')
            AND (first_seen_ledger_observation_id IS NOT NULL)))`,
      key_columns: [],
      referenced_schema: null,
      referenced_table: null,
      referenced_columns: [],
      delete_action: " ",
      update_action: " ",
      match_type: " ",
      deferrable: false,
      initially_deferred: false,
      no_inherit: false,
    },
    {
      constraint_name: "content_candidates_first_seen_ledger_observation_id_fkey",
      constraint_type: "f",
      validated: true,
      check_expression: null,
      key_columns: ["first_seen_ledger_observation_id"],
      referenced_schema: "crawler",
      referenced_table: "crawl_observations",
      referenced_columns: ["observation_id"],
      delete_action: "r",
      update_action: "a",
      match_type: "s",
      deferrable: true,
      initially_deferred: true,
      no_inherit: false,
    },
  ];
  assert.equal(constraintsHaveFinalShape(constraints), true);
  assert.equal(constraintsHaveFinalShape([
    { ...constraints[0], check_expression: "first_seen_ledger_status <> 'pending'" },
    constraints[1],
  ]), false);
  assert.equal(constraintsHaveFinalShape([
    constraints[0],
    { ...constraints[1], referenced_table: "channel_runs" },
  ]), false);

  const index = {
    indisvalid: true,
    indisready: true,
    indislive: true,
    indisunique: false,
    table_schema: "crawler",
    table_name: "content_candidates",
    key_columns: ["channel_id", "candidate_id"],
    predicate: "first_seen_ledger_status = 'pending'::text",
  };
  assert.equal(indexHasFinalShape(index), true);
  assert.equal(indexHasFinalShape({
    ...index,
    predicate: "first_seen_ledger_status <> 'pending'::text",
  }), false);
});
