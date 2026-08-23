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

    const dispositions = await pool.query(
      `SELECT source_content_id,disposition,next_attempt_at,
              result_json->'disposition' AS disposition_evidence,content_key
       FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=ANY($2::text[])
       ORDER BY source_content_id`,
      [runId, ["candidate-only", "new-video"]],
    );
    assert.deepEqual(
      dispositions.rows.map((row) => ({
        source_content_id: row.source_content_id,
        disposition: row.disposition,
        next_attempt_at: row.next_attempt_at,
        reason_code: row.disposition_evidence.reason_code,
        content_key: row.content_key,
      })),
      [
        {
          source_content_id: "candidate-only",
          disposition: "stored",
          next_attempt_at: null,
          reason_code: "content_stored",
          content_key: `${channelId}:video:candidate-only`,
        },
        {
          source_content_id: "new-video",
          disposition: "stored",
          next_attempt_at: null,
          reason_code: "content_stored",
          content_key: `${channelId}:video:new-video`,
        },
      ],
    );

    const outbox = await pool.query(
      `SELECT payload_json
       FROM crawler.crawler_outbox
       WHERE aggregate_key=$1 AND payload_json->>'observation_kind'='video'`,
      [`${channelId}:video`],
    );
    const discovery = outbox.rows[0].payload_json.payload.discovery.payload;
    assert.equal(discovery.discovered_count, 2);
    assert.equal(discovery.stored_count, 2);
    assert.equal(discovery.deferred_count, 0);
    assert.equal(discovery.terminal_excluded_count, 0);
    assert.deepEqual(
      discovery.dispositions.map((item) => item.video_id).sort(),
      ["candidate-only", "new-video"],
    );

    const cursor = await pool.query(
      `SELECT latest_sequence,anchor_video_ids,source_cursor
       FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    );
    assert.equal(cursor.rows[0].latest_sequence, "1");
    assert.deepEqual(cursor.rows[0].anchor_video_ids.slice(0, 3), [
      "new-video",
      "candidate-only",
      "known-anchor",
    ]);
    assert.equal(cursor.rows[0].source_cursor.matched_anchor_id, "known-anchor");

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

test("Incremental Video consumes scheduled terminal rechecks only in Clock mode and only when due", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const clockChannelId = `UCenrichclock${suffix}`;
  const queueChannelId = `UCenrichqueue${suffix}`;
  const originalMode = (await pool.query(
    `SELECT value_json FROM crawler.settings WHERE setting_key='content_enrich_dispatch'`,
  )).rows[0]?.value_json ?? { mode: "clock" };
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

  async function prepareChannel(channelId, videos) {
    const runId = `incremental:enrich-owner:${channelId}`;
    const planId = randomUUID();
    const anchorId = `anchor-${channelId}`;
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Enrich owner integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status,
         plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
         clock_version,policy_version,planner_config_version,capacity_version,
         crawler_version,started_at
       ) VALUES (
         $1,$2,'running','incremental',0,'pending',$3,'2026-08-23','clock_due',
         '{"video":true}'::jsonb,
         '2026-08-23T00:00:00Z',7,'v16-rule-1','video-plan-1','capacity-1','test',now()
       )`,
      [runId, channelId, planId],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,title,url,published_at,published_at_status,
         published_at_source,published_at_precision,access_status,
         access_status_source,first_seen_at,last_seen_at,player_last_observed_at
       ) VALUES (
         $1,$2,$3,'video','youtube_watch_canonical',$4,'Anchor',$5,
         '2026-08-20T00:00:00Z','exact','youtubejs_player','second','public',
         'youtubejs_player',now(),now(),now()
       )`,
      [
        `${channelId}:video:${anchorId}`,
        channelId,
        runId,
        anchorId,
        `https://www.youtube.com/watch?v=${anchorId}`,
      ],
    );
    for (const video of videos) {
      const contentKey = `${channelId}:video:${video.videoId}`;
      await pool.query(
        `INSERT INTO crawler.contents (
           content_key,channel_id,run_id,content_type,content_type_source,
           source_content_id,title,url,published_at,published_at_status,
           published_at_source,published_at_precision,access_status,
           access_status_source,first_seen_at,last_seen_at
         ) VALUES (
           $1,$2,$3,'video','youtube_watch_canonical',$4,$4,$5,$6,
           'exact','youtubejs_player','second','private','youtubejs_player',now(),now()
         )`,
        [
          contentKey,
          channelId,
          runId,
          video.videoId,
          `https://www.youtube.com/watch?v=${video.videoId}`,
          video.publishedAt,
        ],
      );
      await pool.query(
        `INSERT INTO crawler.content_enrich_tasks (
           task_id,content_key,channel_id,job_type,status,priority,attempts,next_retry_at
         ) VALUES (
           $1,$2,$3,'player-refresh','terminal',10,5,
           CASE WHEN $4::boolean
             THEN clock_timestamp()-interval '1 second'
             ELSE clock_timestamp()+interval '7 days' END
         )`,
        [`player-refresh:${channelId}:${video.videoId}`, contentKey, channelId, video.due],
      );
    }
    await pool.query(
      `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind,anchor_video_ids)
       VALUES ($1,'video',ARRAY[$2]::text[])`,
      [channelId, anchorId],
    );
    return { anchorId, planId, runId };
  }

  async function executeScenario(channelId, prepared, fetched) {
    return executeIncrementalVideo({
      plan: {
        job_id: `incremental__enrich_owner__${channelId}`,
        plan_id: prepared.planId,
        plan_day: "2026-08-23",
        channel_id: channelId,
        capacity: { factor: 1, player_cap: 20, next_cap: 0, version: "capacity-1" },
        planner_config_version: "video-plan-1",
      },
      runId: prepared.runId,
      startedAt: "2026-08-23T00:00:00.000Z",
      query: (sql, params) => pool.query(sql, params),
      withTransaction,
      getChannelSnapshot: async () => ({
        async scanUploads() {
          return {
            playlist_id: `UU${channelId.slice(2)}`,
            entries: [{ id: prepared.anchorId, position: 1, title: "Anchor" }],
            pages: 1,
            item_count: 1,
            parse_gap_count: 0,
            anchor_matched: true,
            matched_anchor_id: prepared.anchorId,
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
          title: `Public ${videoId}`,
          published_at: "2026-01-01T00:00:00.000Z",
          published_at_precision: "second",
          view_count: 123,
          view_count_text: "123",
          duration_seconds: 90,
          access_status: "public",
          availability: "public",
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
      now: () => new Date("2026-08-23T00:00:00.000Z"),
      crawlerVersion: "qy-v16-integration-test",
    });
  }

  try {
    const queueVideoId = `queue-due-${suffix}`;
    const queuePrepared = await prepareChannel(queueChannelId, [{
      videoId: queueVideoId,
      publishedAt: "2026-01-01T00:00:00.000Z",
      due: true,
    }]);
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"queue"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    const queueFetched = [];
    await executeScenario(queueChannelId, queuePrepared, queueFetched);
    assert.equal(queueFetched.includes(queueVideoId), false);
    assert.deepEqual((await pool.query(
      `SELECT status,attempts FROM crawler.content_enrich_tasks WHERE channel_id=$1`,
      [queueChannelId],
    )).rows[0], { status: "terminal", attempts: 5 });

    const futureVideoId = `clock-future-${suffix}`;
    const dueVideoId = `clock-due-${suffix}`;
    const clockPrepared = await prepareChannel(clockChannelId, [
      {
        videoId: futureVideoId,
        publishedAt: "2026-08-22T00:00:00.000Z",
        due: false,
      },
      {
        videoId: dueVideoId,
        publishedAt: "2026-01-01T00:00:00.000Z",
        due: true,
      },
    ]);
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"clock"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    const clockFetched = [];
    await executeScenario(clockChannelId, clockPrepared, clockFetched);
    assert.equal(clockFetched.includes(futureVideoId), false);
    assert.equal(clockFetched.filter((videoId) => videoId === dueVideoId).length, 1);
    assert.deepEqual((await pool.query(
      `SELECT content.source_content_id,task.status,task.attempts,task.next_retry_at
       FROM crawler.content_enrich_tasks task
       JOIN crawler.contents content USING (content_key)
       WHERE task.channel_id=$1
       ORDER BY content.source_content_id`,
      [clockChannelId],
    )).rows.map((row) => ({
      source_content_id: row.source_content_id,
      status: row.status,
      attempts: row.attempts,
      has_next_retry: row.next_retry_at != null,
    })), [
      {
        source_content_id: dueVideoId,
        status: "done",
        attempts: 0,
        has_next_retry: false,
      },
      {
        source_content_id: futureVideoId,
        status: "terminal",
        attempts: 5,
        has_next_retry: true,
      },
    ]);
  } finally {
    await pool.query(
      `UPDATE crawler.settings SET value_json=$1::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
      [JSON.stringify(originalMode)],
    ).catch(() => {});
    await pool.query(
      `DELETE FROM crawler.channels WHERE channel_id=ANY($1::text[])`,
      [[clockChannelId, queueChannelId]],
    ).catch(() => {});
    await pool.end();
  }
});

test("Video disposition schema rejects invalid kind and retry schedules", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCdispositionconstraint${suffix}`;
  const runId = `incremental:disposition-constraint:${suffix}`;
  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Disposition constraint integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (run_id,channel_id,status,crawl_mode,detail_status)
       VALUES ($1,$2,'running','incremental','pending')`,
      [runId, channelId],
    );
    const insertCandidate = (disposition, nextAttemptAt) => pool.query(
      `INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,detail_status,api_status,
         disposition,next_attempt_at
       ) VALUES ($1,$2,'constraint-video',1,'done','not_needed',$3,$4)`,
      [runId, channelId, disposition, nextAttemptAt],
    );

    await assert.rejects(
      insertCandidate("discarded", null),
      (error) => error?.code === "23514"
        && error?.constraint === "content_candidates_disposition_kind_check",
    );
    await assert.rejects(
      insertCandidate("stored", "2026-07-21T00:00:00.000Z"),
      (error) => error?.code === "23514"
        && error?.constraint === "content_candidates_disposition_schedule_check",
    );
    await assert.rejects(
      insertCandidate("deferred", null),
      (error) => error?.code === "23514"
        && error?.constraint === "content_candidates_disposition_schedule_check",
    );
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});

test("incremental Video rechecks terminal exclusions only when their schedule is due", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCterminalrecheck${suffix}`;
  const privateVideoId = `private-${suffix}`;
  const anchorVideoId = `anchor-${suffix}`;
  const fetched = [];
  let privateRecheckFails = false;
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
  const insertRun = async (runId) => {
    const planId = randomUUID();
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
    return planId;
  };
  const execute = async ({ runId, jobId, startedAt, planId }) => executeIncrementalVideo({
    plan: {
      job_id: jobId,
      plan_id: planId,
      plan_day: "2026-07-20",
      scheduled_at: startedAt,
      channel_id: channelId,
      capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
      planner_config_version: "video-plan-1",
    },
    runId,
    startedAt,
    query: (sql, params) => pool.query(sql, params),
    withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: `UU${channelId.slice(2)}`,
          entries: [
            { id: privateVideoId, position: 1, title: "Private" },
            { id: anchorVideoId, position: 2, title: "Anchor" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: anchorVideoId,
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push({ videoId, startedAt });
      if (videoId === privateVideoId) {
        if (privateRecheckFails) throw new Error("temporary Player timeout");
        return {
          id: videoId,
          title: "Private",
          access_status: "private",
          availability: "private",
          extractor_version: "youtubei.js@test",
        };
      }
      return {
        id: videoId,
        title: "Anchor",
        published_at: "2026-07-19T00:00:00.000Z",
        published_at_precision: "second",
        view_count: 10,
        access_status: "public",
        availability: "public",
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
    now: () => new Date(startedAt),
    crawlerVersion: "qy-v16-integration-test",
  });

  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Terminal recheck integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const firstRunId = `incremental:terminal:first:${suffix}`;
    const firstPlanId = await insertRun(firstRunId);
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,position,title,url,published_at,published_at_status,
         published_at_source,published_at_precision,first_seen_at,last_seen_at
       ) VALUES ($1,$2,$3,'video','youtube_watch_canonical',$4,2,'Anchor',$5,
         '2026-07-19T00:00:00Z','exact','youtubejs_player','second',now(),now())`,
      [
        `${channelId}:video:${anchorVideoId}`,
        channelId,
        firstRunId,
        anchorVideoId,
        `https://www.youtube.com/watch?v=${anchorVideoId}`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind,anchor_video_ids)
       VALUES ($1,'video',ARRAY[$2]::text[])`,
      [channelId, anchorVideoId],
    );

    const first = await execute({
      runId: firstRunId,
      jobId: `incremental__terminal__first__${suffix}`,
      startedAt: "2026-07-20T00:00:00.000Z",
      planId: firstPlanId,
    });
    assert.equal(first.outcome, "complete");

    const earlyRunId = `incremental:terminal:early:${suffix}`;
    const early = await execute({
      runId: earlyRunId,
      jobId: `incremental__terminal__early__${suffix}`,
      startedAt: "2026-07-21T00:00:00.000Z",
      planId: await insertRun(earlyRunId),
    });
    assert.equal(early.outcome, "complete");
    assert.equal(fetched.filter((item) => item.videoId === privateVideoId).length, 1);

    const dueRunId = `incremental:terminal:due:${suffix}`;
    const due = await execute({
      runId: dueRunId,
      jobId: `incremental__terminal__due__${suffix}`,
      startedAt: "2026-07-27T00:00:00.000Z",
      planId: await insertRun(dueRunId),
    });
    assert.equal(due.outcome, "complete");
    assert.equal(fetched.filter((item) => item.videoId === privateVideoId).length, 2);

    privateRecheckFails = true;
    const failedRecheckRunId = `incremental:terminal:failed-recheck:${suffix}`;
    const failedRecheck = await execute({
      runId: failedRecheckRunId,
      jobId: `incremental__terminal__failed_recheck__${suffix}`,
      startedAt: "2026-08-03T00:00:00.000Z",
      planId: await insertRun(failedRecheckRunId),
    });
    assert.equal(failedRecheck.outcome, "complete");
    assert.equal(fetched.filter((item) => item.videoId === privateVideoId).length, 3);

    const candidates = await pool.query(
      `SELECT run_id,disposition,result_json#>>'{disposition,reason_code}' AS reason_code,
              result_json#>>'{collection_error,message}' AS collection_error_message
       FROM crawler.content_candidates
       WHERE channel_id=$1 AND source_content_id=$2
       ORDER BY candidate_id`,
      [channelId, privateVideoId],
    );
    assert.deepEqual(candidates.rows, [
      {
        run_id: firstRunId,
        disposition: "terminal_excluded",
        reason_code: "access_private",
        collection_error_message: null,
      },
      {
        run_id: dueRunId,
        disposition: "terminal_excluded",
        reason_code: "access_private",
        collection_error_message: null,
      },
      {
        run_id: failedRecheckRunId,
        disposition: "terminal_excluded",
        reason_code: "access_private",
        collection_error_message: "temporary Player timeout",
      },
    ]);
    const cursor = await pool.query(
      `SELECT latest_sequence,source_cursor
       FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    );
    assert.equal(cursor.rows[0].latest_sequence, "4");
    assert.notEqual(cursor.rows[0].source_cursor, null);
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
