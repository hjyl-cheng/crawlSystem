import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";
import {
  collectQueryTerms,
  resolveMetadataQueryIdentityPolicy,
} from "../src/queryCollector.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

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

function expectedCount(name) {
  const value = Number.parseInt(String(process.env[name] ?? ""), 10);
  assert.ok(
    Number.isSafeInteger(value) && value >= 0,
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

if (!process.argv.includes("--apply")) {
  throw new Error("refusing to backfill: pass --apply explicitly");
}

const confirmedDatabase = String(process.env.CONFIRM_QUERY_METADATA_BACKFILL ?? "").trim();
if (!confirmedDatabase) {
  throw new Error("CONFIRM_QUERY_METADATA_BACKFILL must equal the target database name");
}
const expectedChannelCount = expectedCount("EXPECTED_CRAWLER_CHANNEL_COUNT");
const expectedContentCount = expectedCount("EXPECTED_CRAWLER_CONTENT_COUNT");
const batchSize = Number.parseInt(String(process.env.QUERY_METADATA_BACKFILL_BATCH_SIZE ?? "500"), 10);
assert.ok(
  Number.isSafeInteger(batchSize) && batchSize >= 1 && batchSize <= 5000,
  "QUERY_METADATA_BACKFILL_BATCH_SIZE must be between 1 and 5000",
);
const metadataIdentityPolicy = resolveMetadataQueryIdentityPolicy();

const client = new Client({ connectionString: databaseUrl() });
let transactionOpen = false;
let lockHeld = false;
let scannedChannelCount = 0;
let scannedContentCount = 0;
let insertedQueryCount = 0;

async function beginBatch() {
  await client.query("BEGIN");
  transactionOpen = true;
  await client.query("SET LOCAL lock_timeout = '10s'");
  await client.query("SET LOCAL statement_timeout = '120s'");
  await client.query(
    "SELECT set_config('publication.writer_version',$1,true)",
    [PUBLICATION_WRITER_VERSION],
  );
}

async function commitBatch() {
  await client.query("COMMIT");
  transactionOpen = false;
}

try {
  await client.connect();
  const preflight = await client.query(
    `SELECT current_database() AS database_name,
            (SELECT count(*)::int FROM crawler.channels) AS channel_count,
            (SELECT count(*)::int FROM crawler.contents) AS content_count`,
  );
  const actual = preflight.rows[0];
  assert.equal(actual.database_name, confirmedDatabase, "unexpected Crawler database");
  assert.equal(Number(actual.channel_count), expectedChannelCount, "unexpected Crawler Channel count");
  assert.equal(Number(actual.content_count), expectedContentCount, "unexpected Crawler Content count");

  await client.query("SELECT pg_advisory_lock(781137221)");
  lockHeld = true;

  let lastChannelId = null;
  while (true) {
    await beginBatch();
    const batch = await client.query(
      `SELECT channel_id,keywords
       FROM crawler.channels
       WHERE ($1::text IS NULL OR channel_id>$1)
         AND cardinality(keywords)>0
       ORDER BY channel_id
       LIMIT $2`,
      [lastChannelId, batchSize],
    );
    if (batch.rows.length === 0) {
      await commitBatch();
      break;
    }
    const collected = await collectQueryTerms(client, {
      qualityBatchId: `metadata-backfill:channel:${randomUUID()}`,
      identityPolicy: metadataIdentityPolicy,
      sources: [{
        kind: "channel_keyword",
        values: batch.rows.flatMap((row) => row.keywords ?? []),
      }],
    });
    await commitBatch();
    lastChannelId = batch.rows.at(-1).channel_id;
    scannedChannelCount += batch.rows.length;
    insertedQueryCount += collected.inserted_count;
  }

  let lastContentKey = null;
  while (true) {
    await beginBatch();
    const batch = await client.query(
      `SELECT content_key,keywords,hashtags
       FROM crawler.contents
       WHERE ($1::text IS NULL OR content_key>$1)
         AND (cardinality(keywords)>0 OR cardinality(hashtags)>0)
       ORDER BY content_key
       LIMIT $2`,
      [lastContentKey, batchSize],
    );
    if (batch.rows.length === 0) {
      await commitBatch();
      break;
    }
    const collected = await collectQueryTerms(client, {
      qualityBatchId: `metadata-backfill:video:${randomUUID()}`,
      identityPolicy: metadataIdentityPolicy,
      sources: [
        {
          kind: "video_keyword",
          values: batch.rows.flatMap((row) => row.keywords ?? []),
        },
        {
          kind: "video_hashtag",
          values: batch.rows.flatMap((row) => row.hashtags ?? []),
        },
      ],
    });
    await commitBatch();
    lastContentKey = batch.rows.at(-1).content_key;
    scannedContentCount += batch.rows.length;
    insertedQueryCount += collected.inserted_count;
  }

  console.log(JSON.stringify({
    ok: true,
    database: confirmedDatabase,
    scanned_channel_count: scannedChannelCount,
    scanned_content_count: scannedContentCount,
    inserted_query_count: insertedQueryCount,
  }));
} catch (error) {
  if (transactionOpen) await client.query("ROLLBACK").catch(() => {});
  throw error;
} finally {
  if (lockHeld) await client.query("SELECT pg_advisory_unlock(781137221)").catch(() => {});
  await client.end().catch(() => {});
}
