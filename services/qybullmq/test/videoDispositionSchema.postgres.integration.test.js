import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

async function applySchema(pool, schema) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(781137199)");
    await client.query(schema);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

test("schema backfills legacy Video dispositions and remains idempotent", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UClegacydisposition${suffix}`;
  const runId = `full:legacy-disposition:${suffix}`;
  const storedVideoId = `stored-${suffix}`;
  const privateVideoId = `private-${suffix}`;
  const failedVideoId = `failed-${suffix}`;
  const oldStoredVideoId = `old-stored-${suffix}`;
  const upcomingStoredVideoId = `upcoming-stored-${suffix}`;
  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Legacy disposition integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (run_id,channel_id,status,crawl_mode,detail_status)
       VALUES ($1,$2,'waiting_detail','full','running')`,
      [runId, channelId],
    );
    const storedContentKey = `${channelId}:video:${storedVideoId}`;
    const oldStoredContentKey = `${channelId}:video:${oldStoredVideoId}`;
    const upcomingStoredContentKey = `${channelId}:video:${upcomingStoredVideoId}`;
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,position,title,url,first_seen_at,last_seen_at
       ) VALUES
       ($1,$2,$3,'video','youtube_watch_canonical',$4,1,'Stored',$5,now(),now()),
       ($6,$2,$3,'video','youtube_watch_canonical',$7,4,'Old stored',$8,now(),now()),
       ($9,$2,$3,'video','youtube_watch_canonical',$10,5,'Upcoming stored',$11,now(),now())`,
      [
        storedContentKey,
        channelId,
        runId,
        storedVideoId,
        `https://www.youtube.com/watch?v=${storedVideoId}`,
        oldStoredContentKey,
        oldStoredVideoId,
        `https://www.youtube.com/watch?v=${oldStoredVideoId}`,
        upcomingStoredContentKey,
        upcomingStoredVideoId,
        `https://www.youtube.com/watch?v=${upcomingStoredVideoId}`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,content_type,type_status,
         detail_status,api_status,content_key,result_json,error_message,finished_at
       ) VALUES
       ($1,$2,$3,1,'video','resolved','done','not_needed',$4,'{}'::jsonb,NULL,now()-interval '10 days'),
       ($1,$2,$5,2,NULL,'unavailable','unavailable','unavailable',NULL,
         '{"access":{"access_status":"private","access_status_source":"youtubejs_player"}}'::jsonb,
         NULL,now()-interval '10 days'),
       ($1,$2,$6,3,NULL,'unresolved','failed','not_needed',NULL,
         '{"detail":{"id":"legacy-failed"},"errors":["Player timeout"]}'::jsonb,
         NULL,NULL),
       ($1,$2,$7,4,'video','resolved','done','not_needed',$8,
         '{"scope":{"status":"excluded","reason":"older_than_max_age"}}'::jsonb,
         NULL,now()-interval '10 days'),
       ($1,$2,$9,5,'live','resolved','done','not_needed',$10,
         '{"scope":{"status":"excluded","reason":"upcoming_live"}}'::jsonb,
         NULL,now()-interval '10 days')`,
      [
        runId,
        channelId,
        storedVideoId,
        storedContentKey,
        privateVideoId,
        failedVideoId,
        oldStoredVideoId,
        oldStoredContentKey,
        upcomingStoredVideoId,
        upcomingStoredContentKey,
      ],
    );

    await applySchema(pool, schema);
    const loadRows = () => pool.query(
      `SELECT source_content_id,disposition,next_attempt_at,error_message,
              result_json->'disposition' AS evidence,
              result_json->'legacy_video_disposition_backfill' AS backfill
       FROM crawler.content_candidates
       WHERE run_id=$1
       ORDER BY position`,
      [runId],
    );
    const first = (await loadRows()).rows;
    assert.deepEqual(first.map((row) => ({
      source_content_id: row.source_content_id,
      disposition: row.disposition,
      reason_code: row.evidence.reason_code,
      retry_class: row.evidence.retry_class,
      retryable: row.evidence.retryable,
      scheduled: row.next_attempt_at != null,
      has_backfill: row.backfill != null,
    })), [
      {
        source_content_id: storedVideoId,
        disposition: "stored",
        reason_code: "legacy_content_already_stored",
        retry_class: null,
        retryable: false,
        scheduled: false,
        has_backfill: true,
      },
      {
        source_content_id: privateVideoId,
        disposition: "terminal_excluded",
        reason_code: "access_private",
        retry_class: "low_frequency_access_recheck",
        retryable: false,
        scheduled: true,
        has_backfill: true,
      },
      {
        source_content_id: failedVideoId,
        disposition: "deferred",
        reason_code: "detail_collection_failed",
        retry_class: "player_retry",
        retryable: true,
        scheduled: true,
        has_backfill: true,
      },
      {
        source_content_id: oldStoredVideoId,
        disposition: "terminal_excluded",
        reason_code: "outside_content_window",
        retry_class: "low_frequency_policy_recheck",
        retryable: false,
        scheduled: true,
        has_backfill: true,
      },
      {
        source_content_id: upcomingStoredVideoId,
        disposition: "terminal_excluded",
        reason_code: "upcoming_live",
        retry_class: "low_frequency_access_recheck",
        retryable: false,
        scheduled: true,
        has_backfill: true,
      },
    ]);
    assert.match(first[2].error_message, /requires recovery/);

    const firstEvidence = first.map((row) => row.evidence);
    await applySchema(pool, schema);
    const second = (await loadRows()).rows;
    assert.deepEqual(second.map((row) => row.evidence), firstEvidence);
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
