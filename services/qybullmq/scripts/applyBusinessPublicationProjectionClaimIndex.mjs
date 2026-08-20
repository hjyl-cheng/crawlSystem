import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";

const { Client } = pg;

export const BUSINESS_PROJECTION_PREDECESSOR_INDEX =
  "idx_business_publication_projection_predecessor";

export const CREATE_BUSINESS_PROJECTION_PREDECESSOR_INDEX_SQL = `
  CREATE INDEX CONCURRENTLY idx_business_publication_projection_predecessor
  ON publication.projection_outbox (
    channel_id,publication_stream_id,created_at,projection_id
  )`;

function nonnegativeInteger(environment, name) {
  const raw = String(environment[name] ?? "").trim();
  const value = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(value),
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

export function businessPublicationProjectionClaimIndexApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_BUSINESS_PUBLICATION_CLAIM_INDEX_APPLY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_BUSINESS_PUBLICATION_CLAIM_INDEX_APPLY must equal the target database name",
    );
  }
  return {
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    confirmedDatabase,
    expectedMinimumOutboxRows: nonnegativeInteger(
      environment,
      "EXPECTED_BUSINESS_PROJECTION_OUTBOX_MIN_ROWS",
    ),
  };
}

async function indexState(client) {
  return (await client.query(
    `SELECT index_state.indisvalid,index_state.indisready,index_state.indislive,
            index_state.indisunique,
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
     WHERE index_class.oid=to_regclass($1)`,
    [`publication.${BUSINESS_PROJECTION_PREDECESSOR_INDEX}`],
  )).rows[0] ?? null;
}

function verifyIndex(state) {
  assert.equal(state?.indisvalid, true, "Business Projection predecessor index is not valid");
  assert.equal(state?.indisready, true, "Business Projection predecessor index is not ready");
  assert.equal(state?.indislive, true, "Business Projection predecessor index is not live");
  assert.equal(state?.indisunique, false, "Business Projection predecessor index must not be unique");
  assert.equal(state?.predicate, null, "Business Projection predecessor index must not be partial");
  assert.deepEqual(state?.key_columns, [
    "channel_id",
    "publication_stream_id",
    "created_at",
    "projection_id",
  ]);
}

async function main() {
  const guard = businessPublicationProjectionClaimIndexApplyGuard();
  const client = new Client({
    connectionString: guard.databaseUrl,
    application_name: "business-publication-claim-index-v1",
  });
  let advisoryLock = false;
  const startedAt = Date.now();
  try {
    await client.connect();
    await client.query("SET TIME ZONE 'UTC'");
    await client.query("SET lock_timeout='5s'");
    await client.query("SET statement_timeout='600s'");
    advisoryLock = (await client.query(
      "SELECT pg_try_advisory_lock(hashtext('business-publication-claim-index-v1')) AS acquired",
    )).rows[0]?.acquired === true;
    assert.equal(advisoryLock, true, "another Business Projection Claim index migration is running");

    const identity = (await client.query(
      `SELECT current_database() AS database_name,
              to_regclass('publication.projection_outbox') IS NOT NULL AS outbox_ready,
              to_regclass('publication.channel_ownership') IS NOT NULL AS ownership_ready`,
    )).rows[0] ?? {};
    assert.equal(identity.database_name, guard.confirmedDatabase, "unexpected Business database");
    assert.equal(identity.outbox_ready, true, "Business Projection Outbox is missing");
    assert.equal(identity.ownership_ready, true, "Business Publication ownership is missing");
    const outboxRowsBefore = Number((await client.query(
      "SELECT count(*)::bigint AS outbox_rows FROM publication.projection_outbox",
    )).rows[0]?.outbox_rows);
    assert.ok(
      outboxRowsBefore >= guard.expectedMinimumOutboxRows,
      "Business Projection Outbox row count is below the confirmed minimum",
    );

    const existing = await indexState(client);
    if (existing && (!existing.indisvalid || !existing.indisready || !existing.indislive)) {
      await client.query(
        `DROP INDEX CONCURRENTLY IF EXISTS publication.${BUSINESS_PROJECTION_PREDECESSOR_INDEX}`,
      );
    } else if (existing) {
      verifyIndex(existing);
    }
    if (!existing || !existing.indisvalid || !existing.indisready || !existing.indislive) {
      await client.query(CREATE_BUSINESS_PROJECTION_PREDECESSOR_INDEX_SQL);
    }

    await client.query(
      `ALTER TABLE publication.projection_outbox SET (
         autovacuum_analyze_scale_factor=0.02,
         autovacuum_analyze_threshold=100
       )`,
    );
    await client.query("ANALYZE publication.projection_outbox");

    verifyIndex(await indexState(client));
    const verified = (await client.query(
      `SELECT reloptions,
              (SELECT count(*)::bigint FROM publication.projection_outbox) AS outbox_rows
       FROM pg_class
       WHERE oid='publication.projection_outbox'::regclass`,
    )).rows[0] ?? {};
    assert.ok(
      verified.reloptions?.includes("autovacuum_analyze_scale_factor=0.02"),
      "Business Projection Outbox analyze scale factor was not applied",
    );
    assert.ok(
      verified.reloptions?.includes("autovacuum_analyze_threshold=100"),
      "Business Projection Outbox analyze threshold was not applied",
    );
    console.log(JSON.stringify({
      ok: true,
      database: identity.database_name,
      outbox_rows_before: outboxRowsBefore,
      outbox_rows_after: Number(verified.outbox_rows),
      index: BUSINESS_PROJECTION_PREDECESSOR_INDEX,
      duration_ms: Date.now() - startedAt,
    }));
  } finally {
    if (advisoryLock) {
      await client.query(
        "SELECT pg_advisory_unlock(hashtext('business-publication-claim-index-v1'))",
      ).catch(() => {});
    }
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
