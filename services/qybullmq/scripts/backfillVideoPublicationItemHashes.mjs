import assert from "node:assert/strict";
import pg from "pg";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { refreshVideoPublicationItemHashes } from "../src/videoPublicationItemStore.js";

const { Client } = pg;

function databaseUrl() {
  return process.env.DATABASE_URL || [
    "postgres://",
    encodeURIComponent(process.env.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(process.env.POSTGRES_PASSWORD || "bullmq"),
    "@",
    process.env.POSTGRES_HOST || "127.0.0.1",
    ":",
    process.env.POSTGRES_PORT || "5432",
    "/",
    process.env.POSTGRES_DB || "bullmq_crawler",
  ].join("");
}

if (!process.argv.includes("--apply")) {
  throw new Error("refusing to backfill: pass --apply explicitly");
}
const confirmedDatabase = String(process.env.CONFIRM_VIDEO_ITEM_HASH_BACKFILL ?? "").trim();
if (!confirmedDatabase) {
  throw new Error("CONFIRM_VIDEO_ITEM_HASH_BACKFILL must equal the target database name");
}
const expectedContentCount = Number.parseInt(
  String(process.env.EXPECTED_CRAWLER_CONTENT_COUNT ?? ""),
  10,
);
assert.ok(
  Number.isSafeInteger(expectedContentCount) && expectedContentCount >= 0,
  "EXPECTED_CRAWLER_CONTENT_COUNT must be an explicit non-negative integer",
);
const batchSize = Number.parseInt(String(process.env.VIDEO_ITEM_HASH_BATCH_SIZE ?? "500"), 10);
assert.ok(
  Number.isSafeInteger(batchSize) && batchSize >= 1 && batchSize <= 5000,
  "VIDEO_ITEM_HASH_BATCH_SIZE must be between 1 and 5000",
);

const client = new Client({ connectionString: databaseUrl() });
let transactionOpen = false;
let lockHeld = false;
let lastContentKey = null;
let processedCount = 0;
let readyCount = 0;
let incompleteCount = 0;
let changedCount = 0;
try {
  await client.connect();
  const preflight = await client.query(
    `SELECT current_database() AS database_name,
            count(*)::int AS content_count
     FROM crawler.contents`,
  );
  const actual = preflight.rows[0];
  assert.equal(actual.database_name, confirmedDatabase, "unexpected Crawler database");
  assert.equal(Number(actual.content_count), expectedContentCount, "unexpected Crawler Content count");
  await client.query("SELECT pg_advisory_lock(781137217)");
  lockHeld = true;

  while (true) {
    await client.query("BEGIN");
    transactionOpen = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '120s'");
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    const batch = await client.query(
      `SELECT content_key
       FROM crawler.contents
       WHERE ($1::text IS NULL OR content_key>$1)
       ORDER BY content_key
       LIMIT $2`,
      [lastContentKey, batchSize],
    );
    if (batch.rows.length === 0) {
      await client.query("COMMIT");
      transactionOpen = false;
      break;
    }
    const keys = batch.rows.map((row) => row.content_key);
    const result = await refreshVideoPublicationItemHashes(client, keys);
    await client.query("COMMIT");
    transactionOpen = false;
    lastContentKey = keys[keys.length - 1];
    processedCount += result.found_count;
    readyCount += result.ready_count;
    incompleteCount += result.incomplete_count;
    changedCount += result.changed_count;
  }

  console.log(JSON.stringify({
    ok: true,
    database: confirmedDatabase,
    expected_content_count: expectedContentCount,
    processed_count: processedCount,
    ready_count: readyCount,
    incomplete_count: incompleteCount,
    changed_count: changedCount,
  }));
} catch (error) {
  if (transactionOpen) await client.query("ROLLBACK").catch(() => {});
  throw error;
} finally {
  if (lockHeld) await client.query("SELECT pg_advisory_unlock(781137217)").catch(() => {});
  await client.end().catch(() => {});
}
