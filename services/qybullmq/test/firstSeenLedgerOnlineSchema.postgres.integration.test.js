import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";
import test from "node:test";
import pg from "pg";

const { Pool } = pg;
const execFileAsync = promisify(execFile);
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

function quoteIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function databaseUrl(connectionString, databaseName) {
  const url = new URL(connectionString);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

test("First-Seen ledger online migration replaces the legacy index and validates constraints", {
  skip: !integrationUrl,
  timeout: 60_000,
}, async () => {
  const databaseName = `qy_first_seen_ledger_${randomUUID().replaceAll("-", "")}_test`;
  assert.match(databaseName, /_test$/i);
  const adminPool = new Pool({ connectionString: databaseUrl(integrationUrl, "postgres"), max: 1 });
  let pool;
  try {
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
    const targetUrl = databaseUrl(integrationUrl, databaseName);
    pool = new Pool({ connectionString: targetUrl, max: 2 });
    await pool.query(`
      CREATE SCHEMA crawler;
      CREATE TABLE crawler.crawl_observations (observation_id uuid PRIMARY KEY);
      CREATE TABLE crawler.content_candidates (
        candidate_id bigserial PRIMARY KEY,
        run_id text NOT NULL,
        channel_id text NOT NULL,
        first_seen_ledger_status text NOT NULL DEFAULT 'not_applicable',
        first_seen_ledger_observation_id uuid
      );
      INSERT INTO crawler.crawl_observations (observation_id)
      VALUES ('11111111-1111-4111-8111-111111111111');
      INSERT INTO crawler.content_candidates (run_id,channel_id)
      SELECT 'legacy-run-'||position,'migration-channel-'||(position % 10)
      FROM generate_series(1,1000) position;
      INSERT INTO crawler.content_candidates (
        run_id,channel_id,first_seen_ledger_status
      ) VALUES ('pending-run','migration-channel-1','pending');
      INSERT INTO crawler.content_candidates (
        run_id,channel_id,first_seen_ledger_status,first_seen_ledger_observation_id
      ) VALUES (
        'consumed-run','migration-channel-2','consumed',
        '11111111-1111-4111-8111-111111111111'
      );
      ALTER TABLE crawler.content_candidates
      ADD CONSTRAINT content_candidates_first_seen_ledger_shape_check
      CHECK (
        (first_seen_ledger_status='not_applicable'
          AND first_seen_ledger_observation_id IS NULL)
        OR (first_seen_ledger_status='pending'
          AND first_seen_ledger_observation_id IS NULL)
        OR (first_seen_ledger_status='consumed'
          AND first_seen_ledger_observation_id IS NOT NULL)
      ) NOT VALID;
      ALTER TABLE crawler.content_candidates
      ADD CONSTRAINT content_candidates_first_seen_ledger_observation_id_fkey
      FOREIGN KEY (first_seen_ledger_observation_id)
      REFERENCES crawler.crawl_observations(observation_id)
      ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED NOT VALID;
      CREATE INDEX idx_crawler_content_candidates_first_seen_ledger_pending
      ON crawler.content_candidates (run_id,channel_id,candidate_id)
      WHERE first_seen_ledger_status='pending';
    `);

    const environment = {
      ...process.env,
      DATABASE_URL: targetUrl,
      DATABASE_URL_FILE: "",
      CONFIRM_FIRST_SEEN_LEDGER_SCHEMA_APPLY: databaseName,
      EXPECTED_CRAWLER_CANDIDATE_MIN_ROWS: "1002",
      EXPECTED_FIRST_SEEN_LEDGER_PENDING_COUNT: "1",
    };
    const script = new URL(
      "../scripts/applyFirstSeenLedgerOnlineSchema.mjs",
      import.meta.url,
    );
    const first = await execFileAsync(process.execPath, [script.pathname, "--apply"], {
      env: environment,
    });
    const second = await execFileAsync(process.execPath, [script.pathname, "--apply"], {
      env: environment,
    });
    assert.equal(JSON.parse(first.stdout.trim()).ok, true);
    assert.equal(JSON.parse(second.stdout.trim()).ok, true);

    const state = (await pool.query(
      `SELECT
         (SELECT count(*)::int FROM crawler.content_candidates) AS candidate_count,
         (SELECT count(*)::int FROM crawler.content_candidates
          WHERE first_seen_ledger_status='pending') AS pending_count,
         (SELECT count(*)=2 AND bool_and(convalidated)
          FROM pg_constraint
          WHERE conrelid='crawler.content_candidates'::regclass
            AND conname IN (
              'content_candidates_first_seen_ledger_shape_check',
              'content_candidates_first_seen_ledger_observation_id_fkey'
            )) AS constraints_validated,
         index_state.indisvalid,index_state.indisready,index_state.indislive,
         pg_get_expr(index_state.indpred,index_state.indrelid) AS predicate,
         ARRAY(
           SELECT attribute.attname
           FROM unnest(index_state.indkey) WITH ORDINALITY key(attnum,position)
           JOIN pg_attribute attribute
             ON attribute.attrelid=index_state.indrelid
            AND attribute.attnum=key.attnum
           WHERE key.position<=index_state.indnkeyatts
           ORDER BY key.position
         )::text[] AS key_columns
       FROM pg_class index_class
       JOIN pg_index index_state ON index_state.indexrelid=index_class.oid
       WHERE index_class.oid=to_regclass(
         'crawler.idx_crawler_content_candidates_first_seen_ledger_pending'
       )`,
    )).rows[0] ?? {};
    assert.equal(state.candidate_count, 1002);
    assert.equal(state.pending_count, 1);
    assert.equal(state.constraints_validated, true);
    assert.equal(state.indisvalid, true);
    assert.equal(state.indisready, true);
    assert.equal(state.indislive, true);
    assert.deepEqual(state.key_columns, ["channel_id", "candidate_id"]);
    assert.match(state.predicate, /first_seen_ledger_status.*pending/);
  } finally {
    if (pool) await pool.end().catch(() => {});
    await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} WITH (FORCE)`)
      .catch(() => {});
    await adminPool.end().catch(() => {});
  }
});
