import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { databaseUrl } from "../src/databaseConnection.js";
import { incrementalYoutubeJsVideoCheckpointSchemaBlock } from "../src/incrementalYoutubeJsVideoSchema.js";

const { Client } = pg;

export function incrementalYoutubeJsVideoCheckpointSchemaApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_INCREMENTAL_YOUTUBEJS_CHECKPOINT_SCHEMA_APPLY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_INCREMENTAL_YOUTUBEJS_CHECKPOINT_SCHEMA_APPLY must equal the target database name",
    );
  }
  const runCountText = String(environment.EXPECTED_INCREMENTAL_RUN_MIN_COUNT ?? "").trim();
  const expectedMinimumRunCount = Number(runCountText);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(runCountText) && Number.isSafeInteger(expectedMinimumRunCount),
    "EXPECTED_INCREMENTAL_RUN_MIN_COUNT must be an explicit non-negative integer",
  );
  return {
    confirmedDatabase,
    expectedMinimumRunCount,
    databaseUrl: databaseUrl(environment),
  };
}

async function main() {
  const guard = incrementalYoutubeJsVideoCheckpointSchemaApplyGuard();
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const client = new Client({ connectionString: guard.databaseUrl });
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL TIME ZONE 'UTC'");
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='60s'");
    await client.query("SELECT pg_advisory_xact_lock(1909032026)");
    const before = (await client.query(
      `SELECT current_database() AS database_name,
              count(*) FILTER (WHERE crawl_mode='incremental')::bigint AS incremental_run_count
       FROM crawler.channel_runs`,
    )).rows[0] ?? {};
    assert.equal(before.database_name, guard.confirmedDatabase, "unexpected Crawler database");
    assert.ok(
      Number(before.incremental_run_count) >= guard.expectedMinimumRunCount,
      "Incremental Run count is below EXPECTED_INCREMENTAL_RUN_MIN_COUNT",
    );
    await client.query(incrementalYoutubeJsVideoCheckpointSchemaBlock(schema));
    const verified = (await client.query(
      `SELECT
         to_regclass('crawler.incremental_youtubejs_video_batches') IS NOT NULL AS batches_ready,
         to_regclass('crawler.incremental_youtubejs_video_items') IS NOT NULL AS items_ready`,
    )).rows[0] ?? {};
    assert.equal(Object.values(verified).every(Boolean), true, "checkpoint schema verification failed");
    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: before.database_name,
      incremental_run_count: Number(before.incremental_run_count),
      migration: "incremental-youtubejs-video-checkpoint-schema-v1",
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
