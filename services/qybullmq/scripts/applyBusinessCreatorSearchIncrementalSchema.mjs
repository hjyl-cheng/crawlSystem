import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";

const { Client } = pg;

function exactCount(environment, name) {
  const raw = String(environment[name] ?? "").trim();
  const value = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(value),
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

export function businessCreatorSearchIncrementalSchemaApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_BUSINESS_CREATOR_SEARCH_INCREMENTAL_SCHEMA_APPLY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_BUSINESS_CREATOR_SEARCH_INCREMENTAL_SCHEMA_APPLY must equal the target database name",
    );
  }
  return {
    confirmedDatabase,
    expectedChannelCount: exactCount(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT"),
    expectedActiveSearchCount: exactCount(
      environment,
      "EXPECTED_BUSINESS_ACTIVE_SEARCH_COUNT",
    ),
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
  };
}

async function main() {
  const guard = businessCreatorSearchIncrementalSchemaApplyGuard();
  const schema = await readFile(
    new URL("../src/businessCreatorSearchIncrementalSchema.sql", import.meta.url),
    "utf8",
  );
  const client = new Client({ connectionString: guard.databaseUrl });
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    began = true;
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='600s'");
    await client.query("SELECT pg_advisory_xact_lock(781137234)");
    await client.query(
      "SELECT pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'))",
    );

    const preflight = (await client.query(
      `SELECT current_database() AS database_name,
              to_regclass('public.creator_search_current') IS NOT NULL
                AS legacy_search_ready,
              to_regprocedure('public.refresh_creator_search_release_v8(text,text[],text[])')
                IS NOT NULL AS v8_ready,
              (SELECT count(*)::int FROM public.channels) AS channel_count,
              (SELECT count(*)::int
               FROM public.creator_search_active active
               JOIN public.creator_search_current search USING(watermark))
                AS active_search_count`,
    )).rows[0] ?? {};
    assert.equal(preflight.database_name, guard.confirmedDatabase, "unexpected Business database");
    assert.equal(preflight.legacy_search_ready, true, "Legacy Creator Search is missing");
    assert.equal(preflight.v8_ready, true, "Creator Search v8 publisher is missing");
    assert.equal(Number(preflight.channel_count), guard.expectedChannelCount, "unexpected Channel count");
    assert.equal(
      Number(preflight.active_search_count),
      guard.expectedActiveSearchCount,
      "unexpected active Creator Search count",
    );

    await client.query(schema);
    const verified = (await client.query(
      `WITH legacy AS (
         SELECT search.*
         FROM public.creator_search_active active
         JOIN public.creator_search_current search USING(watermark)
       ), parity AS (
         SELECT legacy.channel_id AS legacy_channel_id,
                live.channel_id AS live_channel_id,
                CASE WHEN legacy.channel_id IS NULL THEN NULL ELSE
                  to_jsonb(legacy)-'watermark' END
                  AS legacy_document,
                CASE WHEN live.channel_id IS NULL THEN NULL ELSE
                  to_jsonb(live)-'watermark' END
                  AS live_document
         FROM legacy FULL JOIN public.creator_search_live live USING(channel_id)
       )
       SELECT
         (SELECT count(*)::int FROM public.creator_search_live) AS live_count,
         (SELECT count(*)::int FROM parity
          WHERE legacy_channel_id IS NULL OR live_channel_id IS NULL
             OR legacy_document IS DISTINCT FROM live_document) AS parity_errors,
         (SELECT write_mode FROM publication.creator_search_storage_state
          WHERE singleton=true) AS write_mode,
         (SELECT read_mode FROM publication.creator_search_storage_state
          WHERE singleton=true) AS read_mode,
         to_regprocedure('public.refresh_creator_search_release_v9(text,text[],text[])')
           IS NOT NULL AS v9_ready,
         to_regprocedure('public.restore_creator_search_live_from_legacy_v1(text)')
           IS NOT NULL AS legacy_restore_ready`,
    )).rows[0] ?? {};
    assert.equal(Number(verified.live_count), guard.expectedActiveSearchCount, "Live count mismatch");
    assert.equal(Number(verified.parity_errors), 0, "Live differs from active Legacy Search");
    assert.equal(verified.write_mode, "shadow", "migration must start in shadow write mode");
    assert.equal(verified.read_mode, "legacy", "migration must not cut reads");
    assert.equal(verified.v9_ready, true, "Creator Search v9 publisher is missing");
    assert.equal(verified.legacy_restore_ready, true, "Creator Search Legacy restore is missing");

    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: preflight.database_name,
      channel_count: Number(preflight.channel_count),
      live_count: Number(verified.live_count),
      parity_errors: Number(verified.parity_errors),
      write_mode: verified.write_mode,
      read_mode: verified.read_mode,
      migration: "business-creator-search-incremental-schema",
    }));
  } catch (error) {
    if (began) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
