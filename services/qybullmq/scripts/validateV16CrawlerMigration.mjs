import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { normalizeAboutMetrics } from "../src/aboutMetrics.js";
import { IdempotencyConflict, recordAboutObservation } from "../src/aboutObservationStore.js";
import { V16_SCHEMA_VERIFICATION_SQL } from "../src/v16SchemaVerification.js";

const { Client } = pg;
const here = dirname(fileURLToPath(import.meta.url));

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

async function migrationSql() {
  const schema = await readFile(join(here, "../src/schema.sql"), "utf8");
  const start = schema.indexOf("-- v16-rule-clock-schema:start");
  const end = schema.indexOf("-- v16-rule-clock-schema:end");
  assert.ok(start >= 0 && end > start, "V16 schema markers are missing");
  return schema.slice(start, end);
}

function exactAbout(values = {}) {
  return normalizeAboutMetrics({
    aboutObserved: true,
    locale: "en",
    metadata: {
      subscriber_count: 1234,
      subscriber_count_text: "1,234 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "98,765 views",
      view_count_source: "youtube_about",
      video_count_text: "42 videos",
      video_count_source: "youtube_about",
      ...values,
    },
  });
}

function command({ key, channelId, runId, observedAt, about }) {
  return {
    idempotencyKey: key,
    channelId,
    runId,
    observedAt,
    triggerReason: "manual",
    startedAt: observedAt,
    finishedAt: observedAt,
    crawlerVersion: "v16-rollback-validation",
    extractorVersions: { youtubejs: "validation" },
    about,
    current: {
      aboutDescription: "rollback-only validation",
      country: "Brazil",
      joinedDateText: "Joined Jan 1, 2020",
      externalLinks: [],
    },
  };
}

const client = new Client({ connectionString: databaseUrl() });
let began = false;
try {
  await client.connect();
  await client.query("BEGIN");
  began = true;
  await client.query("SET LOCAL lock_timeout = '5s'");
  await client.query("SET LOCAL statement_timeout = '60s'");
  const expectedDatabase = String(
    process.env.EXPECTED_CRAWLER_DATABASE || "bullmq_crawler_migration",
  ).trim();
  const expectedChannelCount = Number.parseInt(
    String(process.env.EXPECTED_CRAWLER_CHANNEL_COUNT || "1552"),
    10,
  );
  assert.ok(expectedDatabase, "EXPECTED_CRAWLER_DATABASE is required");
  assert.ok(
    Number.isSafeInteger(expectedChannelCount) && expectedChannelCount >= 0,
    "EXPECTED_CRAWLER_CHANNEL_COUNT must be a non-negative integer",
  );
  const preflight = await client.query(
    `SELECT current_database() AS database_name,
            (SELECT count(*)::int FROM crawler.channels) AS channel_count`,
  );
  assert.equal(preflight.rows[0].database_name, expectedDatabase, "unexpected crawler database");
  assert.equal(
    Number(preflight.rows[0].channel_count),
    expectedChannelCount,
    "unexpected crawler Channel count",
  );
  await client.query(await migrationSql());

  const schemaObjects = await client.query(V16_SCHEMA_VERIFICATION_SQL);
  assert.ok(
    Object.values(schemaObjects.rows[0]).every(Boolean),
    "V16 crawler schema objects are incomplete",
  );

  const suffix = randomUUID();
  const channelId = `UCv16rollback${suffix}`;
  const runId = `run:v16-rollback:${suffix}`;
  await client.query(
    `INSERT INTO crawler.channels (channel_id,channel_url,title)
     VALUES ($1,$2,'V16 rollback validation')`,
    [channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (run_id,channel_id,status,crawl_mode,started_at)
     VALUES ($1,$2,'running','full',now())`,
    [runId, channelId],
  );

  const observedAt = "2026-07-20T12:00:00.000Z";
  const firstCommand = command({
    key: `about:${runId}`,
    channelId,
    runId,
    observedAt,
    about: exactAbout(),
  });
  const first = await recordAboutObservation(client, firstCommand);
  const duplicate = await recordAboutObservation(client, firstCommand);
  assert.equal(first.duplicate, false);
  assert.equal(first.snapshot_written, true);
  assert.equal(duplicate.duplicate, true);
  assert.equal(duplicate.observation_id, first.observation_id);

  await assert.rejects(
    recordAboutObservation(client, {
      ...firstCommand,
      about: exactAbout({ view_count_text: "98,766 views" }),
    }),
    IdempotencyConflict,
  );

  const failedAbout = normalizeAboutMetrics({
    aboutObserved: false,
    metadata: {
      subscriber_count: 9000,
      subscriber_count_text: "9K subscribers",
      subscriber_count_source: "youtube_channel_header",
    },
  });
  const failed = await recordAboutObservation(client, command({
    key: `about:${runId}:failed`,
    channelId,
    runId,
    observedAt: "2026-07-20T13:00:00.000Z",
    about: failedAbout,
  }));
  assert.equal(failed.snapshot_written, false);

  const counts = await client.query(
    `SELECT
       (SELECT count(*)::int FROM crawler.crawl_observations WHERE channel_id=$1) AS observations,
       (SELECT count(*)::int FROM crawler.channel_about_metric_snapshots WHERE channel_id=$1) AS snapshots,
       (SELECT count(*)::int FROM crawler.crawler_outbox WHERE aggregate_key=$2) AS outbox_events`,
    [channelId, `${channelId}:about`],
  );
  assert.deepEqual(counts.rows[0], { observations: 2, snapshots: 1, outbox_events: 2 });

  const current = await client.query(
    `SELECT subscriber_count,total_view_count,total_video_count,
            subscriber_count_status,total_view_count_status,total_video_count_status
     FROM crawler.channels WHERE channel_id=$1`,
    [channelId],
  );
  assert.deepEqual(current.rows[0], {
    subscriber_count: "1234",
    total_view_count: "98765",
    total_video_count: "42",
    subscriber_count_status: "exact",
    total_view_count_status: "exact",
    total_video_count_status: "exact",
  });

  const outbox = await client.query(
    `SELECT payload_json->'payload' AS payload
     FROM crawler.crawler_outbox
     WHERE observation_id=$1`,
    [first.observation_id],
  );
  assert.deepEqual(Object.keys(outbox.rows[0].payload).sort(), [
    "subscriber_count",
    "subscriber_count_status",
    "total_video_count",
    "total_video_count_status",
    "total_view_count",
    "total_view_count_status",
  ]);

  console.log(JSON.stringify({
    ok: true,
    mode: "rollback_only",
    database: expectedDatabase,
    channel_count: expectedChannelCount,
    observations: 2,
    snapshots: 1,
    outbox_events: 2,
    agent_requests_ready: true,
    plan_identity_ready: true,
    candidate_identity_index_removed: true,
    duplicate_observation_reused: true,
    idempotency_conflict_rejected: true,
  }));
} finally {
  if (began) await client.query("ROLLBACK");
  await client.end().catch(() => {});
}
