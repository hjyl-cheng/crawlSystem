import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";
import { youtubeBusinessEmailCrawlerSchemaBlock } from "../src/youtubeBusinessEmailSchema.js";

const { Client } = pg;

export function youtubeBusinessEmailCrawlerSchemaApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_YOUTUBE_BUSINESS_EMAIL_CRAWLER_SCHEMA_APPLY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_YOUTUBE_BUSINESS_EMAIL_CRAWLER_SCHEMA_APPLY must equal the target database name",
    );
  }
  const countValue = String(environment.EXPECTED_CRAWLER_CHANNEL_COUNT ?? "").trim();
  const expectedChannelCount = Number(countValue);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(countValue) && Number.isSafeInteger(expectedChannelCount),
    "EXPECTED_CRAWLER_CHANNEL_COUNT must be an explicit non-negative integer",
  );
  return {
    confirmedDatabase,
    expectedChannelCount,
    databaseUrl: environmentValue("DATABASE_URL", { environment }),
  };
}

async function main() {
  const guard = youtubeBusinessEmailCrawlerSchemaApplyGuard();
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
    await client.query("SELECT pg_advisory_xact_lock(781137235)");
    const before = (await client.query(
      `SELECT current_database() AS database_name,
              (SELECT count(*)::int FROM crawler.channels) AS channel_count`,
    )).rows[0];
    assert.equal(before.database_name, guard.confirmedDatabase, "unexpected Crawler database");
    assert.equal(
      Number(before.channel_count),
      guard.expectedChannelCount,
      "unexpected Crawler Channel count",
    );
    await client.query(youtubeBusinessEmailCrawlerSchemaBlock(schema));
    const verified = (await client.query(
      `SELECT
         EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='crawler' AND table_name='channels'
             AND column_name='youtube_business_email_available'
             AND data_type='boolean'
         ) AS availability_ready,
         EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='crawler' AND table_name='channels'
             AND column_name='youtube_business_email_observed_at'
             AND data_type='timestamp with time zone'
         ) AS observed_at_ready,
         EXISTS (
           SELECT 1 FROM pg_constraint
           WHERE conrelid='crawler.channels'::regclass
             AND conname='channels_youtube_business_email_shape'
         ) AS shape_ready`,
    )).rows[0];
    assert.equal(Object.values(verified).every(Boolean), true, "crawler email schema verification failed");
    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: before.database_name,
      channel_count: Number(before.channel_count),
      migration: "youtube-business-email-crawler-schema-v1",
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
