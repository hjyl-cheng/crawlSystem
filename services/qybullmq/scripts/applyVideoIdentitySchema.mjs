import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Client } = pg;

function nonnegativeInteger(env, name) {
  const raw = String(env[name] ?? "").trim();
  const value = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(value),
    `${name} must be an explicit non-negative integer`,
  );
  return value;
}

export function videoIdentitySchemaBlock(schema) {
  const startMarker = "-- video-identity-schema:start";
  const endMarker = "-- video-identity-schema:end";
  const start = schema.indexOf(startMarker);
  const end = schema.indexOf(endMarker);
  assert.ok(start >= 0 && end > start, "Video identity schema markers are missing");
  return schema.slice(start + startMarker.length, end);
}

export function videoIdentityApplyGuard(env = process.env, argv = process.argv) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(env.CONFIRM_VIDEO_IDENTITY_SCHEMA_APPLY ?? "").trim();
  if (!confirmedDatabase) {
    throw new Error("CONFIRM_VIDEO_IDENTITY_SCHEMA_APPLY must equal the target database name");
  }
  return {
    confirmedDatabase,
    expectedChannelCount: nonnegativeInteger(env, "EXPECTED_CRAWLER_CHANNEL_COUNT"),
    expectedContentCount: nonnegativeInteger(env, "EXPECTED_CRAWLER_CONTENT_COUNT"),
    expectedDuplicateGroupCount: nonnegativeInteger(
      env,
      "EXPECTED_VIDEO_IDENTITY_DUPLICATE_GROUP_COUNT",
    ),
  };
}

function databaseUrl(env = process.env) {
  return env.DATABASE_URL || [
    "postgres://",
    encodeURIComponent(env.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(env.POSTGRES_PASSWORD || "bullmq"),
    "@",
    env.POSTGRES_HOST || "127.0.0.1",
    ":",
    env.POSTGRES_PORT || "5432",
    "/",
    env.POSTGRES_DB || "bullmq_crawler",
  ].join("");
}

const PREFLIGHT_SQL = `WITH duplicate_identity AS (
  SELECT channel_id,source_content_id,count(*)::bigint AS copies
  FROM crawler.contents
  GROUP BY channel_id,source_content_id
  HAVING count(*)>1
), task_conflict AS (
  SELECT content.channel_id,content.source_content_id,task.job_type,count(*)::bigint AS copies
  FROM crawler.content_enrich_tasks task
  JOIN crawler.contents content USING (content_key)
  GROUP BY content.channel_id,content.source_content_id,task.job_type
  HAVING count(*)>1
)
SELECT
  current_database() AS database_name,
  (SELECT count(*)::bigint FROM crawler.channels) AS channel_count,
  (SELECT count(*)::bigint FROM crawler.contents) AS content_count,
  (SELECT count(*)::bigint FROM duplicate_identity) AS duplicate_group_count,
  COALESCE((SELECT sum(copies-1) FROM duplicate_identity),0)::bigint
    AS duplicate_excess_row_count,
  (SELECT count(*)::bigint FROM crawler.content_candidates) AS candidate_count,
  (SELECT count(*)::bigint FROM crawler.content_candidates WHERE content_key IS NOT NULL)
    AS candidate_content_ref_count,
  (SELECT count(*)::bigint FROM crawler.content_enrich_tasks) AS enrich_task_count,
  COALESCE((SELECT sum(copies-1) FROM task_conflict),0)::bigint
    AS enrich_task_conflict_count`;

const POSTFLIGHT_SQL = `SELECT
  (SELECT count(*)::bigint FROM crawler.contents) AS content_count,
  (SELECT count(*)::bigint FROM crawler.content_candidates) AS candidate_count,
  (SELECT count(*)::bigint FROM crawler.content_candidates WHERE content_key IS NOT NULL)
    AS candidate_content_ref_count,
  (SELECT count(*)::bigint FROM crawler.content_enrich_tasks) AS enrich_task_count,
  EXISTS (
    SELECT 1
    FROM pg_class index_class
    JOIN pg_index index_state ON index_state.indexrelid=index_class.oid
    WHERE index_class.oid=to_regclass('crawler.ux_crawler_contents_channel_source')
      AND index_state.indisunique
  ) AS video_identity_index_ready,
  NOT EXISTS (
    SELECT 1
    FROM crawler.contents
    GROUP BY channel_id,source_content_id
    HAVING count(*)>1
  ) AS video_identity_deduplicated,
  EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='crawler.contents'::regclass
      AND conname='contents_access_status_check'
      AND pg_get_constraintdef(oid) LIKE '%unlisted%'
  ) AS unlisted_access_status_ready`;

async function main() {
  const guard = videoIdentityApplyGuard();
  const schemaPath = fileURLToPath(new URL("../src/schema.sql", import.meta.url));
  const ddl = videoIdentitySchemaBlock(await readFile(schemaPath, "utf8"));
  const client = new Client({
    connectionString: databaseUrl(),
    application_name: PUBLICATION_WRITER_VERSION,
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const startedAt = Date.now();
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN");
    began = true;
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '300s'");
    await client.query("SELECT pg_advisory_xact_lock(781137223)");
    await client.query(
      "LOCK TABLE crawler.contents,crawler.content_candidates,crawler.content_enrich_tasks IN SHARE ROW EXCLUSIVE MODE",
    );

    const before = (await client.query(PREFLIGHT_SQL)).rows[0];
    assert.equal(before.database_name, guard.confirmedDatabase, "unexpected Crawler database");
    assert.equal(Number(before.channel_count), guard.expectedChannelCount, "unexpected Crawler Channel count");
    assert.equal(Number(before.content_count), guard.expectedContentCount, "unexpected Crawler Content count");
    assert.equal(
      Number(before.duplicate_group_count),
      guard.expectedDuplicateGroupCount,
      "unexpected Video identity duplicate group count",
    );

    await client.query(ddl);
    const after = (await client.query(POSTFLIGHT_SQL)).rows[0];
    const expectedContentCountAfter = Number(before.content_count)
      - Number(before.duplicate_excess_row_count);
    const expectedTaskCountAfter = Number(before.enrich_task_count)
      - Number(before.enrich_task_conflict_count);
    assert.equal(Number(after.content_count), expectedContentCountAfter, "unexpected Content merge count");
    assert.equal(Number(after.candidate_count), Number(before.candidate_count), "Candidate rows were lost");
    assert.equal(
      Number(after.candidate_content_ref_count),
      Number(before.candidate_content_ref_count),
      "Candidate Content references were lost",
    );
    assert.equal(Number(after.enrich_task_count), expectedTaskCountAfter, "unexpected Enrich Task merge count");
    assert.equal(after.video_identity_index_ready, true, "Video identity unique index is missing");
    assert.equal(after.video_identity_deduplicated, true, "Video identity duplicates remain");
    assert.equal(after.unlisted_access_status_ready, true, "unlisted access status is not allowed");

    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: before.database_name,
      channel_count: Number(before.channel_count),
      content_count_before: Number(before.content_count),
      content_count_after: Number(after.content_count),
      duplicate_group_count: Number(before.duplicate_group_count),
      duplicate_excess_row_count: Number(before.duplicate_excess_row_count),
      enrich_task_conflict_count: Number(before.enrich_task_conflict_count),
      duration_ms: Date.now() - startedAt,
      migration: "video-identity-schema",
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
