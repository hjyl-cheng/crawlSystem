import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { executeIncrementalVideo } from "../src/incrementalVideo.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

test("incremental Video commits Current and aggregate events atomically", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCv16video${suffix}`;
  const runId = `incremental:video:${suffix}`;
  const planId = randomUUID();
  const fetched = [];
  const withTransaction = async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };

  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'V16 Video integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status,
         plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
         clock_version,policy_version,planner_config_version,capacity_version,
         crawler_version,started_at
       ) VALUES (
         $1,$2,'running','incremental',0,'pending',$3,'2026-07-20','clock_due',
         '{"video":true}'::jsonb,
         '2026-07-20T00:00:00Z',7,'v16-rule-1','video-plan-1','capacity-1','test',now()
       )`,
      [runId, channelId, planId],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,source_content_id,title,url,
         published_at,published_at_status,published_at_precision,view_count,
         view_count_text,view_count_status,first_seen_at,last_seen_at,
         player_last_observed_at,video_change_probability
       ) VALUES
       (
         $1,$2,$3,'video','old-video','Old video','https://www.youtube.com/watch?v=old-video',
         '2026-07-18T00:00:00Z','exact','second',90,'90','exact',now(),now(),NULL,NULL
       ),
       (
         $4,$2,$3,'video','known-anchor','Known anchor','https://www.youtube.com/watch?v=known-anchor',
         '2026-07-19T00:00:00Z','exact','second',200,'200','exact',now(),now(),
         '2026-07-20T00:00:00Z',0
       ),
       (
         $5,$2,$3,'video','pending-detail','Pending detail','https://www.youtube.com/watch?v=pending-detail',
         NULL,'unresolved','unknown',NULL,NULL,'unresolved',now(),now(),NULL,NULL
       )`,
      [
        `${channelId}:video:old-video`,
        channelId,
        runId,
        `${channelId}:video:known-anchor`,
        `${channelId}:video:pending-detail`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.content_enrich_tasks (
         task_id,content_key,channel_id,job_type,status,priority,next_retry_at
       ) VALUES ($1,$2,$3,'player-refresh','queued',10,now())`,
      [
        `player-refresh:${suffix}`,
        `${channelId}:video:pending-detail`,
        channelId,
      ],
    );
    await pool.query(
      `UPDATE crawler.contents
       SET description='Trusted old description',description_status='exact',
           description_source='youtubejs_player',hashtags=ARRAY['trusted-hashtag'],
           keywords=ARRAY['trusted-keyword']
       WHERE channel_id=$1 AND source_content_id='old-video'`,
      [channelId],
    );
    await pool.query(
      `INSERT INTO crawler.channel_domain_cursors (
         channel_id,observation_kind,anchor_video_ids
       ) VALUES ($1,'video',ARRAY['known-anchor','old-video']::text[])`,
      [channelId],
    );
    await pool.query(
      `INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,title,content_type,
         type_status,detail_status,api_status
       ) VALUES ($2,$1,'candidate-only',2,'Candidate only','video','resolved','done','not_needed')`,
      [channelId, runId],
    );

    const result = await executeIncrementalVideo({
      plan: {
        job_id: `incremental__${channelId}__20260720__clock_7__hash`,
        plan_id: planId,
        plan_day: "2026-07-20",
        channel_id: channelId,
        capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
        planner_config_version: "video-plan-1",
      },
      runId,
      startedAt: "2026-07-20T00:00:00.000Z",
      query: (sql, params) => pool.query(sql, params),
      withTransaction,
      getChannelSnapshot: async () => ({
        async scanUploads(options) {
          assert.deepEqual(options.anchors, [
            { id: "known-anchor", published_day: "2026-07-19" },
            { id: "old-video", published_day: "2026-07-18" },
          ]);
          return {
            playlist_id: `UU${channelId.slice(2)}`,
            entries: [
              { id: "new-video", position: 1, content_type: "video", title: "New video" },
              { id: "candidate-only", position: 2, content_type: "video", title: "Candidate only" },
              { id: "known-anchor", position: 3, content_type: "video", title: "Anchor" },
            ],
            pages: 1,
            item_count: 3,
            parse_gap_count: 0,
            anchor_matched: true,
            matched_anchor_id: "known-anchor",
            stop_reason: "anchor_matched",
            terminal_reason: "anchor_matched",
            complete: true,
            raw: { engine: "youtubei.js@test" },
          };
        },
      }),
      fetchDetail: async (videoId) => {
        fetched.push(videoId);
        return {
          id: videoId,
          title: `Title ${videoId}`,
          published_at: videoId === "old-video"
            ? "2026-07-18T00:00:00.000Z"
            : "2026-07-19T00:00:00.000Z",
          published_at_precision: "second",
          view_count: videoId === "old-video" ? 100 : 10,
          comment_count: 2,
          comment_count_status: "exact",
          comment_count_source: "youtubejs_comments",
          comments_disabled: null,
          comments_first_page: {
            version: 1,
            collected_at: "2026-07-20T12:00:00.000Z",
            sort: "TOP_COMMENTS",
            total_count: 2,
            returned_count: 1,
            comments: [{ comment_id: `comment-${videoId}`, text: `Comment ${videoId}` }],
          },
          description: videoId === "old-video" ? "" : null,
          description_observed: videoId === "old-video",
          description_source: videoId === "old-video" ? "youtubejs_player" : null,
          hashtags: [],
          hashtags_observed: videoId === "old-video",
          keywords: [],
          keywords_observed: videoId === "old-video",
          availability: "public",
          access_status: "public",
          content_type_signals: {
            source: "youtubei_player",
            canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
            is_shorts_eligible: false,
            is_live_content: false,
            is_live: false,
            is_upcoming: false,
            is_live_now: false,
          },
          extractor_version: "youtubei.js@test",
        };
      },
      now: () => new Date("2026-07-20T12:00:00.000Z"),
      crawlerVersion: "qy-v16-integration-test",
    });

    assert.equal(result.outcome, "complete");
    assert.equal(result.first_seen_count, 2);
    assert.deepEqual(fetched, ["new-video", "candidate-only", "pending-detail", "old-video"]);

    const contents = await pool.query(
      `SELECT source_content_id,view_count,comments_disabled,comments_first_page,video_change_probability,
              description,description_status,hashtags,keywords,
              publication_item_hash
       FROM crawler.contents WHERE channel_id=$1 ORDER BY source_content_id`,
      [channelId],
    );
    assert.deepEqual(contents.rows.map((row) => row.source_content_id), [
      "candidate-only",
      "known-anchor",
      "new-video",
      "old-video",
      "pending-detail",
    ]);
    assert.equal(contents.rows.some((row) => row.source_content_id === "candidate-only"), true);
    assert.equal(contents.rows.find((row) => row.source_content_id === "old-video").view_count, "100");
    assert.deepEqual(
      (({ description, description_status, hashtags, keywords }) => ({
        description,
        description_status,
        hashtags,
        keywords,
      }))(contents.rows.find((row) => row.source_content_id === "old-video")),
      {
        description: "Trusted old description",
        description_status: "exact",
        hashtags: ["trusted-hashtag"],
        keywords: ["trusted-keyword"],
      },
    );
    assert.equal(
      Number(contents.rows.find((row) => row.source_content_id === "old-video").video_change_probability),
      1,
    );
    assert.equal(contents.rows.find((row) => row.source_content_id === "new-video").comments_disabled, null);
    assert.equal(
      contents.rows.find((row) => row.source_content_id === "new-video")
        .comments_first_page?.comments?.[0]?.comment_id,
      "comment-new-video",
    );
    assert.equal(
      contents.rows.find((row) => row.source_content_id === "old-video")
        .comments_first_page?.comments?.[0]?.comment_id,
      "comment-old-video",
    );
    assert.equal(
      contents.rows.find((row) => row.source_content_id === "pending-detail").view_count,
      "10",
    );
    for (const sourceContentId of ["candidate-only", "new-video", "old-video", "pending-detail"]) {
      assert.match(
        contents.rows.find((row) => row.source_content_id === sourceContentId).publication_item_hash,
        /^sha256:[0-9a-f]{64}$/,
      );
    }
    assert.equal(
      contents.rows.find((row) => row.source_content_id === "known-anchor").publication_item_hash,
      null,
    );
    const recoveredTask = await pool.query(
      `SELECT task.status,content.published_at
       FROM crawler.content_enrich_tasks task
       JOIN crawler.contents content USING (content_key)
       WHERE task.content_key=$1`,
      [`${channelId}:video:pending-detail`],
    );
    assert.equal(recoveredTask.rows[0].status, "done");
    assert.equal(new Date(recoveredTask.rows[0].published_at).toISOString(), "2026-07-19T00:00:00.000Z");

    const events = await pool.query(
      `SELECT observation_kind,outcome FROM crawler.crawl_observations
       WHERE channel_id=$1 ORDER BY kind_sequence,observation_kind`,
      [channelId],
    );
    assert.deepEqual(events.rows, [
      { observation_kind: "video", outcome: "complete" },
    ]);
    const snapshots = await pool.query(
      "SELECT count(*)::int AS count FROM crawler.channel_about_metric_snapshots WHERE channel_id=$1",
      [channelId],
    );
    assert.equal(snapshots.rows[0].count, 0);
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
