import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promisify } from "node:util";
import test from "node:test";
import pg from "pg";

const { Pool } = pg;
const execFileAsync = promisify(execFile);
const integrationUrl = process.env.PUBLICATION_BUSINESS_PROJECTION_POSTGRES_TEST_URL;

function quoteIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function databaseUrl(connectionString, databaseName) {
  const url = new URL(connectionString);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

test("Business Projection Claim index migration is idempotent in PostgreSQL", {
  skip: !integrationUrl,
  timeout: 60000,
}, async () => {
  const databaseName = `qy_claim_index_${randomUUID().replaceAll("-", "")}_test`;
  assert.match(databaseName, /_test$/i);
  const adminPool = new Pool({ connectionString: databaseUrl(integrationUrl, "postgres"), max: 1 });
  let pool;
  try {
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
    const targetUrl = databaseUrl(integrationUrl, databaseName);
    pool = new Pool({ connectionString: targetUrl, max: 2 });
    await pool.query(`
      CREATE SCHEMA publication;
      CREATE TABLE publication.channel_ownership (channel_id text PRIMARY KEY);
      CREATE TABLE publication.projection_outbox (
        projection_id uuid PRIMARY KEY,
        publication_stream_id uuid NOT NULL,
        channel_id text NOT NULL,
        status text NOT NULL,
        created_at timestamptz NOT NULL
      );
      INSERT INTO publication.projection_outbox (
        projection_id,publication_stream_id,channel_id,status,created_at
      )
      SELECT (substr(hash,1,8)||'-'||substr(hash,9,4)||'-4'||substr(hash,14,3)
                ||'-8'||substr(hash,18,3)||'-'||substr(hash,21,12))::uuid,
             '11111111-1111-4111-8111-111111111111'::uuid,
             'migration-channel-'||position,
             CASE WHEN position<=25 THEN 'pending' ELSE 'delivered' END,
             timestamp '2000-01-01T00:00:00Z'+position*interval '1 second'
      FROM (
        SELECT position,md5('migration-'||position) AS hash
        FROM generate_series(1,1000) position
      ) source;
    `);
    const environment = {
      ...process.env,
      BUSINESS_DATABASE_URL: targetUrl,
      BUSINESS_DATABASE_URL_FILE: "",
      CONFIRM_BUSINESS_PUBLICATION_CLAIM_INDEX_APPLY: databaseName,
      EXPECTED_BUSINESS_PROJECTION_OUTBOX_MIN_ROWS: "1000",
    };
    const script = new URL(
      "../scripts/applyBusinessPublicationProjectionClaimIndex.mjs",
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
      `SELECT index_state.indisvalid,index_state.indisready,index_state.indislive,
              pg_get_expr(index_state.indpred,index_state.indrelid) AS predicate,
              table_class.reloptions,
              (SELECT count(*)::int FROM publication.projection_outbox) AS outbox_rows
       FROM pg_class index_class
       JOIN pg_index index_state ON index_state.indexrelid=index_class.oid
       JOIN pg_class table_class ON table_class.oid=index_state.indrelid
       WHERE index_class.oid=to_regclass(
         'publication.idx_business_publication_projection_predecessor'
       )`,
    )).rows[0] ?? {};
    assert.equal(state.indisvalid, true);
    assert.equal(state.indisready, true);
    assert.equal(state.indislive, true);
    assert.equal(state.predicate, null);
    assert.equal(Number(state.outbox_rows), 1000);
    assert.ok(state.reloptions.includes("autovacuum_analyze_scale_factor=0.02"));
    assert.ok(state.reloptions.includes("autovacuum_analyze_threshold=100"));

    await pool.query(`
      DROP INDEX publication.idx_business_publication_projection_predecessor;
      CREATE INDEX idx_business_publication_projection_predecessor
      ON publication.projection_outbox (channel_id);
    `);
    await assert.rejects(
      execFileAsync(process.execPath, [script.pathname, "--apply"], { env: environment }),
      /key_columns|strictly deep-equal/,
    );
    const wrongIndex = await pool.query(
      `SELECT pg_get_indexdef(
         'publication.idx_business_publication_projection_predecessor'::regclass
       ) AS definition,
       (SELECT count(*)::int FROM publication.projection_outbox) AS outbox_rows`,
    );
    assert.match(wrongIndex.rows[0].definition, /\(channel_id\)$/);
    assert.equal(Number(wrongIndex.rows[0].outbox_rows), 1000);
  } finally {
    if (pool) await pool.end().catch(() => {});
    await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} WITH (FORCE)`)
      .catch(() => {});
    await adminPool.end().catch(() => {});
  }
});
