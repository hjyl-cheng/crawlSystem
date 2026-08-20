import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { upsertFullVideoContent } from "../src/fullVideoContentStore.js";

const { Pool } = pg;
const integrationUrl = process.env.VIDEO_IDENTITY_POSTGRES_TEST_URL;

function identityMigration(schema) {
  const startMarker = "-- video-identity-schema:start";
  const endMarker = "-- video-identity-schema:end";
  const start = schema.indexOf(startMarker);
  const end = schema.indexOf(endMarker);
  assert.ok(start >= 0 && end > start, "Video identity schema markers are missing");
  return schema.slice(start + startMarker.length, end);
}

test("Video identity migration merges legacy type duplicates without losing references", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const channelId = "UCvideoidentityintegration";
  const videoId = "same-youtube-video-id";
  const videoKey = `${channelId}:video:${videoId}`;
  const shortKey = `${channelId}:short:${videoId}`;

  try {
    const database = (await pool.query("SELECT current_database() AS name")).rows[0].name;
    assert.match(database, /test/i, "integration test refuses to reset a non-test database");
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await pool.query(schema);

    await pool.query("DROP INDEX crawler.ux_crawler_contents_channel_source");
    await pool.query(
      `ALTER TABLE crawler.contents
       ADD CONSTRAINT contents_channel_id_content_type_source_content_id_key
       UNIQUE (channel_id,content_type,source_content_id)`,
    );
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Video identity integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status
       ) VALUES ('identity-integration-run',$1,'done','full',30,'done')`,
      [channelId],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,title,url,description,description_status,description_source,
         hashtags,published_at,published_at_status,published_at_precision,
         view_count,view_count_text,view_count_status,view_count_source,
         access_status,access_status_source,raw_json,first_seen_at,last_seen_at,
         last_enriched_at,player_last_observed_at,publication_item_hash
       ) VALUES
       (
         $1,$2,'identity-integration-run','video','youtube_uploads_default:video',
         $3,'Generic upload','https://www.youtube.com/watch?v=' || $3,
         'Trusted description','exact','youtubejs_player',ARRAY['trusted'],
         '2026-07-01T00:00:00Z','exact','second',100,'100','exact','youtubejs_player',
         'public','youtubejs_player','{"surface":"uploads"}'::jsonb,
         '2026-07-01T00:00:00Z','2026-07-02T00:00:00Z',
         '2026-07-02T00:00:00Z','2026-07-02T00:00:00Z',
         'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
       ),
       (
         $4,$2,'identity-integration-run','short','youtube_channel_tab:short',
         $3,'Confirmed Short','https://www.youtube.com/shorts/' || $3,
         NULL,'unresolved',NULL,'{}'::text[],
         '2026-07-01T00:00:00Z','exact','second',150,'150','exact','youtubejs_player',
         'public','youtubejs_player','{"surface":"shorts"}'::jsonb,
         '2026-07-03T00:00:00Z','2026-07-04T00:00:00Z',
         '2026-07-04T00:00:00Z','2026-07-04T00:00:00Z',NULL
       )`,
      [videoKey, channelId, videoId, shortKey],
    );
    await pool.query(
      `INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,content_type,type_status,
         detail_status,api_status,content_key
       ) VALUES
       ('identity-integration-run',$1,$2,1,'short','resolved','done','not_needed',$3)`,
      [channelId, videoId, videoKey],
    );
    await pool.query(
      `INSERT INTO crawler.content_enrich_tasks (
         task_id,content_key,channel_id,job_type,status,priority,attempts,result_json
       ) VALUES
       ('identity-video-player',$1,$3,'player-refresh','done',20,2,'{"video":true}'::jsonb),
       ('identity-short-player',$2,$3,'player-refresh','queued',10,1,'{"short":true}'::jsonb),
       ('identity-video-next',$1,$3,'next-refresh','queued',30,0,'{}'::jsonb)`,
      [videoKey, shortKey, channelId],
    );

    await pool.query("BEGIN");
    try {
      await pool.query(identityMigration(schema));
      await pool.query("COMMIT");
    } catch (error) {
      await pool.query("ROLLBACK");
      throw error;
    }

    const contents = await pool.query(
      `SELECT content_key,content_type,content_type_source,url,description,
              description_status,hashtags,view_count,first_seen_at,last_seen_at,
              publication_item_hash,raw_json
       FROM crawler.contents
       WHERE channel_id=$1 AND source_content_id=$2`,
      [channelId, videoId],
    );
    assert.equal(contents.rowCount, 1);
    assert.deepEqual({
      content_key: contents.rows[0].content_key,
      content_type: contents.rows[0].content_type,
      content_type_source: contents.rows[0].content_type_source,
      url: contents.rows[0].url,
      description: contents.rows[0].description,
      description_status: contents.rows[0].description_status,
      hashtags: contents.rows[0].hashtags,
      view_count: contents.rows[0].view_count,
      first_seen_at: contents.rows[0].first_seen_at.toISOString(),
      last_seen_at: contents.rows[0].last_seen_at.toISOString(),
      publication_item_hash: contents.rows[0].publication_item_hash,
    }, {
      content_key: shortKey,
      content_type: "short",
      content_type_source: "youtube_channel_tab:short",
      url: `https://www.youtube.com/shorts/${videoId}`,
      description: "Trusted description",
      description_status: "exact",
      hashtags: ["trusted"],
      view_count: "150",
      first_seen_at: "2026-07-01T00:00:00.000Z",
      last_seen_at: "2026-07-04T00:00:00.000Z",
      publication_item_hash: null,
    });
    assert.deepEqual(
      contents.rows[0].raw_json.identity_merge.source_content_keys,
      [shortKey, videoKey].sort(),
    );
    assert.deepEqual(
      Object.keys(contents.rows[0].raw_json.identity_merge.legacy_raw_by_content_key).sort(),
      [shortKey, videoKey].sort(),
    );

    const candidate = await pool.query(
      "SELECT content_key FROM crawler.content_candidates WHERE run_id='identity-integration-run'",
    );
    assert.equal(candidate.rows[0].content_key, shortKey);
    const tasks = await pool.query(
      `SELECT content_key,job_type,status,priority,attempts,result_json
       FROM crawler.content_enrich_tasks
       WHERE channel_id=$1
       ORDER BY job_type`,
      [channelId],
    );
    assert.equal(tasks.rowCount, 2);
    assert.equal(tasks.rows.every((row) => row.content_key === shortKey), true);
    assert.deepEqual(tasks.rows.map((row) => ({
      job_type: row.job_type,
      status: row.status,
      priority: row.priority,
      attempts: row.attempts,
    })), [
      { job_type: "next-refresh", status: "queued", priority: 30, attempts: 0 },
      { job_type: "player-refresh", status: "done", priority: 10, attempts: 2 },
    ]);
    assert.deepEqual(
      tasks.rows.find((row) => row.job_type === "player-refresh").result_json,
      { video: true, short: true },
    );

    const storedKey = await upsertFullVideoContent(pool, {
      candidate: {
        channel_id: channelId,
        run_id: "identity-integration-run",
        content_type: "video",
        type_source: "youtube_uploads_default:video",
        source_content_id: videoId,
        position: 1,
        title: "Generic upload refresh",
      },
      state: {
        detail: {
          title: "Generic upload refresh",
          view_count: 175,
          view_count_status: "exact",
          view_count_source: "youtubejs_player",
          extractor_version: "youtubei.js@integration",
        },
        access: {
          is_members_only: false,
          access_status: "public",
          access_status_source: "youtubejs_player",
        },
      },
      locale: "en",
    });
    assert.equal(storedKey, shortKey);
    const afterGenericRefresh = await pool.query(
      `SELECT content_key,content_type,content_type_source,url,count(*) OVER ()::int AS identity_count
       FROM crawler.contents
       WHERE channel_id=$1 AND source_content_id=$2`,
      [channelId, videoId],
    );
    assert.deepEqual(afterGenericRefresh.rows, [{
      content_key: shortKey,
      content_type: "short",
      content_type_source: "youtube_channel_tab:short",
      url: `https://www.youtube.com/shorts/${videoId}`,
      identity_count: 1,
    }]);

    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,source_content_id,access_status
       ) VALUES ($1,$2,'video','unlisted-id','unlisted')`,
      [`${channelId}:video:unlisted-id`, channelId],
    );
    await assert.rejects(
      pool.query(
        `INSERT INTO crawler.contents (
           content_key,channel_id,content_type,source_content_id
         ) VALUES ($1,$2,'video',$3)`,
        [`${channelId}:second:${videoId}`, channelId, videoId],
      ),
      (error) => error?.code === "23505",
    );
  } finally {
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await pool.end();
  }
});
