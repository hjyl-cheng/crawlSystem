import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";
import { youtubeCommentFirstPageSchemaBlock } from "../src/youtubeCommentPageSchema.js";

const { Client } = pg;

export function youtubeCommentFirstPageSchemaApplyGuard(
  environment = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(
    environment.CONFIRM_YOUTUBE_COMMENT_FIRST_PAGE_SCHEMA_APPLY ?? "",
  ).trim();
  if (!confirmedDatabase) {
    throw new Error(
      "CONFIRM_YOUTUBE_COMMENT_FIRST_PAGE_SCHEMA_APPLY must equal the target database name",
    );
  }
  const countValue = String(environment.EXPECTED_CRAWLER_CONTENT_COUNT ?? "").trim();
  const expectedContentCount = Number(countValue);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(countValue) && Number.isSafeInteger(expectedContentCount),
    "EXPECTED_CRAWLER_CONTENT_COUNT must be an explicit non-negative integer",
  );
  return {
    confirmedDatabase,
    expectedContentCount,
    databaseUrl: environmentValue("DATABASE_URL", { environment }),
  };
}

async function main() {
  const guard = youtubeCommentFirstPageSchemaApplyGuard();
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
    await client.query("SELECT pg_advisory_xact_lock(781137236)");
    const before = (await client.query(
      `SELECT current_database() AS database_name,
              (SELECT count(*)::int FROM crawler.contents) AS content_count`,
    )).rows[0];
    assert.equal(before.database_name, guard.confirmedDatabase, "unexpected Crawler database");
    assert.equal(
      Number(before.content_count),
      guard.expectedContentCount,
      "unexpected Crawler Content count",
    );
    await client.query(youtubeCommentFirstPageSchemaBlock(schema));
    const verified = (await client.query(
      `SELECT
         EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='crawler' AND table_name='contents'
             AND column_name='comments_first_page' AND data_type='jsonb'
         ) AS column_ready,
         EXISTS (
           SELECT 1 FROM pg_constraint
           WHERE conrelid='crawler.contents'::regclass
             AND conname='contents_comments_first_page_shape_check'
         ) AS shape_ready`,
    )).rows[0];
    assert.equal(Object.values(verified).every(Boolean), true, "comment page schema verification failed");
    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: before.database_name,
      content_count: Number(before.content_count),
      migration: "youtube-comment-first-page-schema-v1",
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
