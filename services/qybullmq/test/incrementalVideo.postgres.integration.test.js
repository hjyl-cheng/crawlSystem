import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { executeIncrementalVideo } from "../src/incrementalVideo.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

function incrementalPool(max = 4) {
  return new Pool({
    connectionString: integrationUrl,
    max,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
}

function clockPlan({ channelId, planId, jobId, playerCap = 1 }) {
  return {
    job_id: jobId,
    plan_id: planId,
    plan_day: "2026-08-24",
    scheduled_at: "2026-08-24T00:00:00.000Z",
    channel_id: channelId,
    capacity: { factor: 1, player_cap: playerCap, next_cap: 0, version: "capacity-1" },
    planner_config_version: "video-plan-1",
  };
}

function completePublicDetail(videoId, overrides = {}) {
  return {
    id: videoId,
    title: `Updated ${videoId}`,
    published_at: "2026-08-23T00:00:00.000Z",
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
    ...overrides,
  };
}

function postgresTransactionRunner(pool, { beforeTransaction = null, beforeQuery = null } = {}) {
  let transactionCount = 0;
  const runner = async (action) => {
    transactionCount += 1;
    await beforeTransaction?.(transactionCount);
    const client = await pool.connect();
    const transactionClient = {
      query(sql, params = []) {
        beforeQuery?.(String(sql), params, transactionCount);
        return client.query(sql, params);
      },
    };
    try {
      await client.query("BEGIN");
      const result = await action(transactionClient);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };
  runner.transactionCount = () => transactionCount;
  return runner;
}

async function insertClockBase(pool, { channelId, runId, planId, anchorVideoId }) {
  await pool.query(
    `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
     VALUES ($1,$2,'Clock failure-boundary integration','active')`,
    [channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  await pool.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,status,crawl_mode,content_limit,detail_status,
       plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
       clock_version,policy_version,planner_config_version,capacity_version,
       crawler_version,started_at
     ) VALUES (
       $1,$2,'running','incremental',0,'pending',$3,'2026-08-24','clock_due',
       '{"video":true}'::jsonb,
       '2026-08-24T00:00:00Z',7,'v16-rule-1','video-plan-1','capacity-1','test',now()
     )`,
    [runId, channelId, planId],
  );
  await pool.query(
    `INSERT INTO crawler.contents (
       content_key,channel_id,run_id,content_type,content_type_source,
       source_content_id,title,url,published_at,published_at_status,
       published_at_source,published_at_precision,access_status,
       access_status_source,first_seen_at,last_seen_at
     ) VALUES (
       $1,$2,$3,'video','youtube_watch_canonical',$4,'Original anchor',$5,
       '2026-01-01T00:00:00Z','exact','youtubejs_player','second','public',
       'youtubejs_player',now(),now()
     )`,
    [
      `${channelId}:video:${anchorVideoId}`,
      channelId,
      runId,
      anchorVideoId,
      `https://www.youtube.com/watch?v=${anchorVideoId}`,
    ],
  );
  await pool.query(
    `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind,anchor_video_ids)
     VALUES ($1,'video',ARRAY[$2]::text[])`,
    [channelId, anchorVideoId],
  );
}

function completeAnchorScan(channelId, anchorVideoId, entries = []) {
  return {
    playlist_id: `UU${channelId.slice(2)}`,
    entries: [...entries, { id: anchorVideoId, position: entries.length + 1, title: "Anchor" }],
    pages: 1,
    item_count: entries.length + 1,
    parse_gap_count: 0,
    anchor_matched: true,
    matched_anchor_id: anchorVideoId,
    stop_reason: "anchor_matched",
    terminal_reason: "anchor_matched",
    complete: true,
    raw: { engine: "youtubei.js@test" },
  };
}

function completeListEndScan(channelId, entries) {
  return {
    playlist_id: `UU${channelId.slice(2)}`,
    entries,
    pages: 1,
    item_count: entries.length,
    parse_gap_count: 0,
    anchor_matched: false,
    matched_anchor_id: null,
    stop_reason: "list_end",
    terminal_reason: "list_end",
    complete: true,
    raw: { engine: "youtubei.js@test" },
  };
}

function partialScan(channelId, entries) {
  return {
    playlist_id: `UU${channelId.slice(2)}`,
    entries,
    pages: 1,
    item_count: entries.length,
    parse_gap_count: 0,
    anchor_matched: false,
    matched_anchor_id: null,
    stop_reason: "page_cap",
    terminal_reason: null,
    complete: false,
    raw: { engine: "youtubei.js@test" },
  };
}

function firstSeenEntry(videoId, position = 1) {
  return {
    id: videoId,
    position,
    title: `First-Seen ${videoId}`,
    content_type: "video",
    published_day: "2026-08-23",
  };
}

async function insertPublicationOwnership(pool, { channelId, streamId = randomUUID() }) {
  await pool.query(
    `INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,
       minimum_writer_version,capture_enabled_at,created_by,created_reason,
       status_changed_by,status_reason
     ) VALUES (
       $1,$2,'{"database":"incremental-video-test"}'::jsonb,$3,now(),
       'integration-test','Incremental Video test','integration-test','capture enabled'
     )`,
    [streamId, `incremental-video-${channelId}`, PUBLICATION_WRITER_VERSION],
  );
  await pool.query(
    `INSERT INTO publication.channel_stream_state (
       publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
     ) VALUES ($1,$2,'bootstrap','integration-test','Incremental Video owner')`,
    [streamId, channelId],
  );
  await pool.query(
    `INSERT INTO publication.channel_delivery_state (
       destination,publication_stream_id,channel_id,state_changed_by,state_reason
     ) VALUES ('business',$1,$2,'integration-test','hold for baseline')`,
    [streamId, channelId],
  );
  return streamId;
}

function twoPartyBarrier() {
  let arrivals = 0;
  let release;
  const opened = new Promise((resolve) => { release = resolve; });
  return async () => {
    arrivals += 1;
    if (arrivals === 2) release();
    await opened;
  };
}

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
       ),
       (
         $6,$2,$3,'video','uploads-dated-video','Uploads dated',
         'https://www.youtube.com/watch?v=uploads-dated-video',
         NULL,'unresolved','unknown',NULL,NULL,'unresolved',now(),now(),NULL,NULL
       )`,
      [
        `${channelId}:video:old-video`,
        channelId,
        runId,
        `${channelId}:video:known-anchor`,
        `${channelId}:video:pending-detail`,
        `${channelId}:video:uploads-dated-video`,
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
              {
                id: "uploads-dated-video",
                position: 3,
                content_type: "video",
                title: "Uploads dated",
                published_day: "2026-07-19",
              },
              { id: "known-anchor", position: 4, content_type: "video", title: "Anchor" },
            ],
            pages: 1,
            item_count: 4,
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
          view_count_text: videoId === "old-video" ? "100" : "10",
          duration_seconds: 90,
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
    assert.deepEqual(fetched, [
      "new-video",
      "candidate-only",
      "pending-detail",
      "uploads-dated-video",
      "old-video",
    ]);

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
      "uploads-dated-video",
    ]);
    assert.equal(cursor.rows[0].source_cursor.matched_anchor_id, "known-anchor");

    const contents = await pool.query(
      `SELECT source_content_id,published_at,view_count,comments_disabled,comments_first_page,video_change_probability,
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
      "uploads-dated-video",
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
    assert.equal(
      new Date(contents.rows.find(
        (row) => row.source_content_id === "uploads-dated-video",
      ).published_at).toISOString(),
      "2026-07-19T00:00:00.000Z",
    );
    for (const sourceContentId of [
      "candidate-only",
      "new-video",
      "old-video",
      "pending-detail",
      "uploads-dated-video",
    ]) {
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

test("Clock keeps failure attempts but rolls back successful cycle state when Publication fails", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCclockatomic${suffix}`;
  const runId = `incremental:clock-atomic:${suffix}`;
  const planId = randomUUID();
  const anchorVideoId = `atomic-anchor-${suffix}`;
  const successVideoId = `atomic-success-${suffix}`;
  const failedVideoId = `atomic-failed-${suffix}`;
  const terminalVideoId = `atomic-terminal-${suffix}`;
  const publicationFailure = new Error("injected Publication failure");
  let publicationAttempted = false;
  const originalMode = (await pool.query(
    `SELECT value_json FROM crawler.settings WHERE setting_key='content_enrich_dispatch'`,
  )).rows[0]?.value_json ?? { mode: "clock" };
  const withTransaction = async (action) => {
    const client = await pool.connect();
    const transactionClient = {
      query(sql, params = []) {
        if (String(sql).includes("publication-reconciler:transaction-guard")) {
          publicationAttempted = true;
          throw publicationFailure;
        }
        return client.query(sql, params);
      },
    };
    try {
      await client.query("BEGIN");
      const result = await action(transactionClient);
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
      `UPDATE crawler.settings SET value_json='{"mode":"clock"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Clock atomicity integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status,
         plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
         clock_version,policy_version,planner_config_version,capacity_version,
         crawler_version,started_at
       ) VALUES (
         $1,$2,'running','incremental',0,'pending',$3,'2026-08-24','clock_due',
         '{"video":true}'::jsonb,
         '2026-08-24T00:00:00Z',7,'v16-rule-1','video-plan-1','capacity-1','test',now()
       )`,
      [runId, channelId, planId],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,title,url,published_at,published_at_status,
         published_at_source,published_at_precision,access_status,
         access_status_source,first_seen_at,last_seen_at
       ) VALUES
       ($1,$2,$3,'video','youtube_watch_canonical',$4,'Original anchor',$5,
        '2026-08-23T00:00:00Z','exact','youtubejs_player','second','public',
        'youtubejs_player',now(),now()),
       ($6,$2,$3,'video','youtube_watch_canonical',$7,'Original success',$8,
        '2026-01-01T00:00:00Z','exact','youtubejs_player','second','public',
        'youtubejs_player',now(),now()),
       ($9,$2,$3,'video','youtube_watch_canonical',$10,'Original failure',$11,
        '2026-01-02T00:00:00Z','exact','youtubejs_player','second','public',
        'youtubejs_player',now(),now())`,
      [
        `${channelId}:video:${anchorVideoId}`,
        channelId,
        runId,
        anchorVideoId,
        `https://www.youtube.com/watch?v=${anchorVideoId}`,
        `${channelId}:video:${successVideoId}`,
        successVideoId,
        `https://www.youtube.com/watch?v=${successVideoId}`,
        `${channelId}:video:${failedVideoId}`,
        failedVideoId,
        `https://www.youtube.com/watch?v=${failedVideoId}`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.content_enrich_tasks (
         task_id,content_key,channel_id,job_type,status,priority,attempts,next_retry_at
       ) VALUES
       ($1,$2,$3,'player-refresh','queued',10,0,clock_timestamp()-interval '1 second'),
       ($4,$5,$3,'player-refresh','failed',10,2,clock_timestamp()-interval '1 second')`,
      [
        `player-refresh:${successVideoId}`,
        `${channelId}:video:${successVideoId}`,
        channelId,
        `player-refresh:${failedVideoId}`,
        `${channelId}:video:${failedVideoId}`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,title,url,published_at,published_at_status,
         published_at_source,published_at_precision,access_status,
         access_status_source,first_seen_at,last_seen_at
       ) VALUES (
         $1,$2,$3,'video','youtube_watch_canonical',$4,'Original terminal',$5,
         '2026-01-03T00:00:00Z','exact','youtubejs_player','second','public',
         'youtubejs_player',now(),now()
       )`,
      [
        `${channelId}:video:${terminalVideoId}`,
        channelId,
        runId,
        terminalVideoId,
        `https://www.youtube.com/watch?v=${terminalVideoId}`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.content_enrich_tasks (
         task_id,content_key,channel_id,job_type,status,priority,attempts,next_retry_at
       ) VALUES (
         $1,$2,$3,'player-refresh','queued',10,0,clock_timestamp()-interval '1 second'
       )`,
      [
        `player-refresh:${terminalVideoId}`,
        `${channelId}:video:${terminalVideoId}`,
        channelId,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind,anchor_video_ids)
       VALUES ($1,'video',ARRAY[$2]::text[])`,
      [channelId, anchorVideoId],
    );

    await assert.rejects(
      executeIncrementalVideo({
        plan: {
          job_id: `incremental__clock_atomic__${suffix}`,
          plan_id: planId,
          plan_day: "2026-08-24",
          scheduled_at: "2026-08-24T00:00:00.000Z",
          channel_id: channelId,
          capacity: { factor: 1, player_cap: 20, next_cap: 0, version: "capacity-1" },
          planner_config_version: "video-plan-1",
        },
        runId,
        startedAt: "2026-08-24T00:00:00.000Z",
        query: (sql, params) => pool.query(sql, params),
        withTransaction,
        getChannelSnapshot: async () => ({
          async scanUploads() {
            return {
              playlist_id: `UU${channelId.slice(2)}`,
              entries: [{ id: anchorVideoId, position: 1, title: "Anchor" }],
              pages: 1,
              item_count: 1,
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
          if (videoId === failedVideoId) throw new Error("temporary Player timeout");
          if (videoId === terminalVideoId) {
            return {
              id: videoId,
              access_status: "private",
              access_status_source: "youtubei_player",
              availability: "private",
              extractor_version: "youtubei.js@test",
            };
          }
          return {
            id: videoId,
            title: `Updated ${videoId}`,
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
        now: () => new Date("2026-08-24T00:00:00.000Z"),
        crawlerVersion: "qy-v16-integration-test",
      }),
      (error) => error === publicationFailure,
    );

    assert.equal(publicationAttempted, true);
    assert.deepEqual((await pool.query(
      `SELECT content.source_content_id,task.status,task.attempts,task.next_retry_at,
              task.lease_owner,task.lease_expires_at,task.dispatch_generation
       FROM crawler.content_enrich_tasks task
       JOIN crawler.contents content USING (content_key)
       WHERE task.channel_id=$1
       ORDER BY content.source_content_id`,
      [channelId],
    )).rows.map((row) => ({
      source_content_id: row.source_content_id,
      status: row.status,
      attempts: row.attempts,
      has_next_retry: row.next_retry_at != null,
      lease_owner: row.lease_owner,
      lease_expires_at: row.lease_expires_at,
      dispatch_generation: Number(row.dispatch_generation),
    })), [
      {
        source_content_id: failedVideoId,
        status: "failed",
        attempts: 3,
        has_next_retry: true,
        lease_owner: null,
        lease_expires_at: null,
        dispatch_generation: 0,
      },
      {
        source_content_id: successVideoId,
        status: "queued",
        attempts: 0,
        has_next_retry: true,
        lease_owner: null,
        lease_expires_at: null,
        dispatch_generation: 1,
      },
      {
        source_content_id: terminalVideoId,
        status: "queued",
        attempts: 0,
        has_next_retry: true,
        lease_owner: null,
        lease_expires_at: null,
        dispatch_generation: 1,
      },
    ]);
    assert.deepEqual((await pool.query(
      `SELECT source_content_id,title,view_count,last_enriched_at,publication_item_hash,access_status
       FROM crawler.contents
       WHERE channel_id=$1
       ORDER BY source_content_id`,
      [channelId],
    )).rows, [
      {
        source_content_id: anchorVideoId,
        title: "Original anchor",
        view_count: null,
        last_enriched_at: null,
        publication_item_hash: null,
        access_status: "public",
      },
      {
        source_content_id: failedVideoId,
        title: "Original failure",
        view_count: null,
        last_enriched_at: null,
        publication_item_hash: null,
        access_status: "public",
      },
      {
        source_content_id: successVideoId,
        title: "Original success",
        view_count: null,
        last_enriched_at: null,
        publication_item_hash: null,
        access_status: "public",
      },
      {
        source_content_id: terminalVideoId,
        title: "Original terminal",
        view_count: null,
        last_enriched_at: null,
        publication_item_hash: null,
        access_status: "public",
      },
    ]);
    assert.deepEqual((await pool.query(
      `SELECT latest_sequence,source_cursor,anchor_video_ids
       FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0], {
      latest_sequence: "0",
      source_cursor: {},
      anchor_video_ids: [anchorVideoId],
    });
    assert.equal((await pool.query(
      `SELECT count(*)::int AS count FROM crawler.crawl_observations
       WHERE channel_id=$1 AND run_id=$2`,
      [channelId, runId],
    )).rows[0].count, 0);
    assert.equal((await pool.query(
      `SELECT count(*)::int AS count FROM crawler.crawler_outbox
       WHERE aggregate_key=$1`,
      [`${channelId}:video`],
    )).rows[0].count, 0);
    assert.deepEqual((await pool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.revision
          WHERE channel_id=$1) AS revisions,
         (SELECT count(*)::int FROM publication.domain_current
          WHERE channel_id=$1) AS currents,
         (SELECT count(*)::int
          FROM publication.outbox outbox
          JOIN publication.revision revision USING (revision_id)
          WHERE revision.channel_id=$1) AS outbox_rows`,
      [channelId],
    )).rows[0], { revisions: 0, currents: 0, outbox_rows: 0 });
  } finally {
    await pool.query(
      `UPDATE crawler.settings SET value_json=$1::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
      [JSON.stringify(originalMode)],
    ).catch(() => {});
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});

test("First-Seen failure checkpoint publishes exactly once through an owned Publication Stream", {
  skip: !integrationUrl,
}, async () => {
  const pool = incrementalPool(4);
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCfirstseenatomic${suffix}`;
  const runId = `incremental:first-seen-atomic:${suffix}`;
  const planId = randomUUID();
  const streamId = randomUUID();
  const anchorVideoId = `first-seen-anchor-${suffix}`;
  const incompleteVideoId = `first-seen-incomplete-${suffix}`;
  const incompleteContentKey = `${channelId}:video:${incompleteVideoId}`;
  const publicationFailure = new Error("injected First-Seen Publication failure");
  let publicationAttempts = 0;
  let incompleteDetailAttempts = 0;
  let scanAttempts = 0;
  const originalMode = (await pool.query(
    `SELECT value_json FROM crawler.settings WHERE setting_key='content_enrich_dispatch'`,
  )).rows[0]?.value_json ?? { mode: "clock" };
  const withTransaction = postgresTransactionRunner(pool, {
    beforeQuery(sql) {
      if (!sql.includes("publication-reconciler:transaction-guard")) return;
      publicationAttempts += 1;
      if (publicationAttempts === 1) throw publicationFailure;
    },
  });

  const execute = () => executeIncrementalVideo({
    plan: clockPlan({
      channelId,
      planId,
      jobId: `incremental__first_seen_atomic__${suffix}`,
    }),
    runId,
    startedAt: "2026-08-24T00:00:00.000Z",
    query: (sql, params) => pool.query(sql, params),
    withTransaction,
    getChannelSnapshot: async () => ({
      scanUploads: async () => {
        scanAttempts += 1;
        return completeListEndScan(channelId, scanAttempts === 1 ? [{
          ...firstSeenEntry(incompleteVideoId),
        }, {
          id: incompleteVideoId,
          position: 2,
          title: "First-Seen incomplete duplicate",
          content_type: "video",
          published_day: "2026-08-23",
        }, {
          id: anchorVideoId,
          position: 3,
          title: "Anchor",
          published_day: "2026-01-01",
        }] : [{
          id: anchorVideoId,
          position: 1,
          title: "Anchor",
          published_day: "2026-01-01",
        }]);
      },
    }),
    fetchDetail: async (videoId) => {
      if (videoId === incompleteVideoId) incompleteDetailAttempts += 1;
      return completePublicDetail(videoId, {
        duration_seconds: videoId === incompleteVideoId ? null : 90,
      });
    },
    now: () => new Date("2026-08-24T00:00:00.000Z"),
    crawlerVersion: "qy-v16-integration-test",
  });

  try {
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"clock"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    await insertClockBase(pool, { channelId, runId, planId, anchorVideoId });
    await insertPublicationOwnership(pool, { channelId, streamId });

    await assert.rejects(
      execute(),
      (error) => error === publicationFailure,
    );

    assert.equal(publicationAttempts, 1);
    assert.deepEqual((await pool.query(
      `SELECT detail_status,missing_fields,error_message,attempts,content_key,
              first_seen_ledger_status,first_seen_ledger_observation_id
       FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2`,
      [runId, incompleteVideoId],
    )).rows[0], {
      detail_status: "failed",
      missing_fields: ["detail"],
      error_message: "Content Enrich detail is missing the required public Video surface",
      attempts: 1,
      content_key: incompleteContentKey,
      first_seen_ledger_status: "pending",
      first_seen_ledger_observation_id: null,
    });
    assert.deepEqual((await pool.query(
      `SELECT status,attempts,last_success_at,lease_owner,lease_expires_at
       FROM crawler.content_enrich_tasks
       WHERE content_key=$1 AND job_type='player-refresh'`,
      [incompleteContentKey],
    )).rows[0], {
      status: "failed",
      attempts: 1,
      last_success_at: null,
      lease_owner: null,
      lease_expires_at: null,
    });
    assert.deepEqual((await pool.query(
      `SELECT last_enriched_at,last_observation_id,publication_item_hash
       FROM crawler.contents WHERE content_key=$1`,
      [incompleteContentKey],
    )).rows[0], {
      last_enriched_at: null,
      last_observation_id: null,
      publication_item_hash: null,
    });
    assert.deepEqual((await pool.query(
      `SELECT latest_sequence,source_cursor,anchor_video_ids
       FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0], {
      latest_sequence: "0",
      source_cursor: {},
      anchor_video_ids: [anchorVideoId],
    });
    assert.equal((await pool.query(
      `SELECT count(*)::int AS count FROM crawler.crawl_observations
       WHERE channel_id=$1 AND run_id=$2`,
      [channelId, runId],
    )).rows[0].count, 0);
    assert.equal((await pool.query(
      `SELECT count(*)::int AS count FROM crawler.crawler_outbox
       WHERE aggregate_key=$1`,
      [`${channelId}:video`],
    )).rows[0].count, 0);
    assert.deepEqual((await pool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.revision
          WHERE channel_id=$1) AS revisions,
         (SELECT count(*)::int FROM publication.domain_current
          WHERE channel_id=$1) AS currents,
         (SELECT count(*)::int
          FROM publication.outbox outbox
          JOIN publication.revision revision USING (revision_id)
          WHERE revision.channel_id=$1) AS outbox_rows`,
      [channelId],
    )).rows[0], { revisions: 0, currents: 0, outbox_rows: 0 });

    const detailAttemptsAfterCheckpoint = incompleteDetailAttempts;
    const recovered = await execute();

    assert.equal(publicationAttempts, 2);
    assert.equal(scanAttempts, 2);
    assert.equal(incompleteDetailAttempts, detailAttemptsAfterCheckpoint);
    assert.equal(recovered.outcome, "complete");
    assert.equal(recovered.first_seen_count, 1);

    const candidates = (await pool.query(
      `SELECT detail_status,missing_fields,error_message,attempts,content_key
       FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2
       ORDER BY candidate_id`,
      [runId, incompleteVideoId],
    )).rows;
    assert.equal(candidates.length, 1);
    assert.deepEqual(candidates[0], {
      detail_status: "failed",
      missing_fields: ["detail"],
      error_message: "Content Enrich detail is missing the required public Video surface",
      attempts: 1,
      content_key: incompleteContentKey,
    });
    assert.deepEqual((await pool.query(
      `SELECT status,attempts,last_success_at,lease_owner,lease_expires_at
       FROM crawler.content_enrich_tasks
       WHERE content_key=$1 AND job_type='player-refresh'`,
      [incompleteContentKey],
    )).rows[0], {
      status: "failed",
      attempts: 1,
      last_success_at: null,
      lease_owner: null,
      lease_expires_at: null,
    });

    const observations = (await pool.query(
      `SELECT observation_id,outcome,result_summary_json
       FROM crawler.crawl_observations
       WHERE channel_id=$1 AND run_id=$2 AND observation_kind='video'
       ORDER BY kind_sequence`,
      [channelId, runId],
    )).rows;
    assert.equal(observations.length, 1);
    assert.equal(observations[0].outcome, "complete");
    assert.equal(observations[0].result_summary_json.discovery.first_seen_count, 1);
    assert.equal(observations[0].result_summary_json.discovery.discovered_count, 1);
    assert.equal(observations[0].result_summary_json.discovery.stored_count, 1);
    assert.deepEqual((await pool.query(
      `SELECT first_seen_ledger_status,first_seen_ledger_observation_id
       FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2`,
      [runId, incompleteVideoId],
    )).rows[0], {
      first_seen_ledger_status: "consumed",
      first_seen_ledger_observation_id: observations[0].observation_id,
    });

    const outboxRows = (await pool.query(
      `SELECT observation_id,payload_json
       FROM crawler.crawler_outbox
       WHERE aggregate_key=$1 AND payload_json->>'observation_kind'='video'
       ORDER BY kind_sequence`,
      [`${channelId}:video`],
    )).rows;
    assert.equal(outboxRows.length, 1);
    assert.equal(outboxRows[0].observation_id, observations[0].observation_id);
    const discovery = outboxRows[0].payload_json.payload.discovery.payload;
    assert.deepEqual(discovery.first_seen.map((item) => item.video_id), [incompleteVideoId]);
    assert.deepEqual(discovery.dispositions.map((item) => item.video_id), [incompleteVideoId]);
    assert.equal(discovery.dispositions[0].kind, "stored");

    const content = (await pool.query(
      `SELECT last_enriched_at,last_observation_id,publication_item_hash
       FROM crawler.contents WHERE content_key=$1`,
      [incompleteContentKey],
    )).rows[0];
    assert.equal(content.last_enriched_at, null);
    assert.equal(content.last_observation_id, observations[0].observation_id);
    assert.match(content.publication_item_hash, /^sha256:[0-9a-f]{64}$/);

    assert.deepEqual((await pool.query(
      `SELECT latest_sequence,latest_observation_id,latest_complete_observation_id,
              source_cursor,anchor_video_ids
       FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0], {
      latest_sequence: "1",
      latest_observation_id: observations[0].observation_id,
      latest_complete_observation_id: observations[0].observation_id,
      source_cursor: {
        playlist_id: `UU${channelId.slice(2)}`,
        matched_anchor_id: null,
        crossed_anchor_ids: [],
        terminal_reason: "list_end",
      },
      anchor_video_ids: [anchorVideoId],
    });

    const published = (await pool.query(
      `SELECT current.readiness_status,current.data_sequence,
              revision.revision_type,revision.operation,outbox.status AS outbox_status
       FROM publication.domain_current current
       JOIN publication.revision revision
         ON revision.revision_id=current.current_revision_id
       JOIN publication.outbox outbox ON outbox.revision_id=revision.revision_id
       WHERE current.publication_stream_id=$1
         AND current.channel_id=$2
         AND current.domain='video'`,
      [streamId, channelId],
    )).rows;
    assert.deepEqual(published, [{
      readiness_status: "ready",
      data_sequence: "1",
      revision_type: "bootstrap",
      operation: "replace_window",
      outbox_status: "held",
    }]);
  } finally {
    await pool.query(
      `UPDATE crawler.settings SET value_json=$1::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
      [JSON.stringify(originalMode)],
    ).catch(() => {});
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});

test("an intervening partial Observation cannot consume a pending First-Seen checkpoint", {
  skip: !integrationUrl,
}, async () => {
  const pool = incrementalPool(5);
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCfirstseenpartial${suffix}`;
  const runId = `incremental:first-seen-partial:${suffix}`;
  const planId = randomUUID();
  const anchorVideoId = `first-seen-partial-anchor-${suffix}`;
  const incompleteVideoId = `first-seen-partial-video-${suffix}`;
  const incompleteContentKey = `${channelId}:video:${incompleteVideoId}`;
  const publicationFailure = new Error("injected partial-path Publication failure");
  let publicationAttempts = 0;
  let detailAttempts = 0;
  const originalMode = (await pool.query(
    `SELECT value_json FROM crawler.settings WHERE setting_key='content_enrich_dispatch'`,
  )).rows[0]?.value_json ?? { mode: "clock" };
  const withTransaction = postgresTransactionRunner(pool, {
    beforeQuery(sql) {
      if (!sql.includes("publication-reconciler:transaction-guard")) return;
      publicationAttempts += 1;
      if (publicationAttempts === 1) throw publicationFailure;
    },
  });
  const execute = ({ job, scan }) => executeIncrementalVideo({
    plan: clockPlan({
      channelId,
      planId,
      jobId: `incremental__first_seen_partial__${suffix}__${job}`,
      playerCap: 0,
    }),
    runId,
    startedAt: "2026-08-24T00:00:00.000Z",
    query: (sql, params) => pool.query(sql, params),
    withTransaction,
    getChannelSnapshot: async () => ({ scanUploads: async () => scan }),
    fetchDetail: async (videoId) => {
      if (videoId === incompleteVideoId) detailAttempts += 1;
      return completePublicDetail(videoId, {
        duration_seconds: videoId === incompleteVideoId ? null : 90,
      });
    },
    now: () => new Date("2026-08-24T00:00:00.000Z"),
    crawlerVersion: "qy-v16-integration-test",
  });

  try {
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"clock"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    await insertClockBase(pool, { channelId, runId, planId, anchorVideoId });

    await assert.rejects(
      execute({
        job: "checkpoint",
        scan: completeAnchorScan(channelId, anchorVideoId, [firstSeenEntry(incompleteVideoId)]),
      }),
      (error) => error === publicationFailure,
    );
    assert.equal(detailAttempts, 1);

    const partial = await execute({
      job: "partial",
      scan: partialScan(channelId, [firstSeenEntry(incompleteVideoId)]),
    });
    assert.equal(partial.outcome, "partial");
    assert.equal(partial.first_seen_count, 0);
    const partialObservationId = partial.observation_id;
    assert.equal((await pool.query(
      `SELECT last_observation_id FROM crawler.contents WHERE content_key=$1`,
      [incompleteContentKey],
    )).rows[0].last_observation_id, partialObservationId);
    assert.deepEqual((await pool.query(
      `SELECT first_seen_ledger_status,first_seen_ledger_observation_id
       FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2`,
      [runId, incompleteVideoId],
    )).rows[0], {
      first_seen_ledger_status: "pending",
      first_seen_ledger_observation_id: null,
    });

    const recovered = await execute({
      job: "complete",
      scan: completeAnchorScan(channelId, anchorVideoId),
    });
    assert.equal(recovered.outcome, "complete");
    assert.equal(recovered.first_seen_count, 1);
    assert.equal(detailAttempts, 1);

    const candidates = (await pool.query(
      `SELECT attempts,content_key FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2`,
      [runId, incompleteVideoId],
    )).rows;
    assert.deepEqual(candidates, [{ attempts: 1, content_key: incompleteContentKey }]);
    assert.deepEqual((await pool.query(
      `SELECT status,attempts FROM crawler.content_enrich_tasks
       WHERE content_key=$1 AND job_type='player-refresh'`,
      [incompleteContentKey],
    )).rows[0], { status: "failed", attempts: 1 });

    const observations = (await pool.query(
      `SELECT observation_id,outcome,result_summary_json
       FROM crawler.crawl_observations
       WHERE channel_id=$1 AND run_id=$2 AND observation_kind='video'
       ORDER BY kind_sequence`,
      [channelId, runId],
    )).rows;
    assert.equal(observations.length, 2);
    assert.deepEqual(observations.map((row) => row.outcome), ["partial", "complete"]);
    assert.deepEqual(
      observations.map((row) => row.result_summary_json.discovery.first_seen_count),
      [0, 1],
    );

    const discoveryPayloads = (await pool.query(
      `SELECT payload_json#>'{payload,discovery,payload}' AS discovery
       FROM crawler.crawler_outbox
       WHERE aggregate_key=$1 AND payload_json->>'observation_kind'='video'
       ORDER BY kind_sequence`,
      [`${channelId}:video`],
    )).rows.map((row) => row.discovery);
    assert.equal(discoveryPayloads.length, 2);
    assert.deepEqual(
      discoveryPayloads.flatMap((payload) => payload.first_seen.map((item) => item.video_id)),
      [incompleteVideoId],
    );
    assert.deepEqual(
      discoveryPayloads.flatMap((payload) => payload.dispositions.map((item) => item.video_id)),
      [incompleteVideoId],
    );
    assert.deepEqual((await pool.query(
      `SELECT first_seen_ledger_status,first_seen_ledger_observation_id
       FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2`,
      [runId, incompleteVideoId],
    )).rows[0], {
      first_seen_ledger_status: "consumed",
      first_seen_ledger_observation_id: recovered.observation_id,
    });
    assert.deepEqual((await pool.query(
      `SELECT latest_sequence,latest_observation_id,latest_complete_observation_id
       FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0], {
      latest_sequence: "2",
      latest_observation_id: recovered.observation_id,
      latest_complete_observation_id: recovered.observation_id,
    });
  } finally {
    await pool.query(
      `UPDATE crawler.settings SET value_json=$1::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
      [JSON.stringify(originalMode)],
    ).catch(() => {});
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});

test("concurrent Clock retries atomically consume a First-Seen checkpoint once", {
  skip: !integrationUrl,
}, async () => {
  const pool = incrementalPool(8);
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCfirstseenrace${suffix}`;
  const runId = `incremental:first-seen-race:${suffix}`;
  const planId = randomUUID();
  const anchorVideoId = `first-seen-race-anchor-${suffix}`;
  const incompleteVideoId = `first-seen-race-video-${suffix}`;
  const incompleteContentKey = `${channelId}:video:${incompleteVideoId}`;
  const publicationFailure = new Error("injected concurrent-path Publication failure");
  let detailAttempts = 0;
  const originalMode = (await pool.query(
    `SELECT value_json FROM crawler.settings WHERE setting_key='content_enrich_dispatch'`,
  )).rows[0]?.value_json ?? { mode: "clock" };
  let failPublication = true;
  const checkpointTransactions = postgresTransactionRunner(pool, {
    beforeQuery(sql) {
      if (failPublication && sql.includes("publication-reconciler:transaction-guard")) {
        failPublication = false;
        throw publicationFailure;
      }
    },
  });
  const execute = ({ job, withTransaction, scan }) => executeIncrementalVideo({
    plan: clockPlan({
      channelId,
      planId,
      jobId: `incremental__first_seen_race__${suffix}__${job}`,
      playerCap: 0,
    }),
    runId,
    startedAt: "2026-08-24T00:00:00.000Z",
    query: (sql, params) => pool.query(sql, params),
    withTransaction,
    getChannelSnapshot: async () => ({ scanUploads: async () => scan }),
    fetchDetail: async (videoId) => {
      if (videoId === incompleteVideoId) detailAttempts += 1;
      return completePublicDetail(videoId, {
        duration_seconds: videoId === incompleteVideoId ? null : 90,
      });
    },
    now: () => new Date("2026-08-24T00:00:00.000Z"),
    crawlerVersion: "qy-v16-integration-test",
  });

  try {
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"clock"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    await insertClockBase(pool, { channelId, runId, planId, anchorVideoId });
    await assert.rejects(
      execute({
        job: "checkpoint",
        withTransaction: checkpointTransactions,
        scan: completeAnchorScan(channelId, anchorVideoId, [firstSeenEntry(incompleteVideoId)]),
      }),
      (error) => error === publicationFailure,
    );
    assert.equal(detailAttempts, 1);

    const rendezvous = twoPartyBarrier();
    const transactionRunner = () => postgresTransactionRunner(pool, {
      beforeTransaction: async (transactionCount) => {
        if (transactionCount === 1) await rendezvous();
      },
    });
    const scan = completeAnchorScan(channelId, anchorVideoId);
    const results = await Promise.all([
      execute({ job: "one", withTransaction: transactionRunner(), scan }),
      execute({ job: "two", withTransaction: transactionRunner(), scan }),
    ]);
    assert.deepEqual(results.map((result) => result.first_seen_count).sort(), [0, 1]);
    assert.equal(detailAttempts, 1);

    const candidates = (await pool.query(
      `SELECT attempts,content_key FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2`,
      [runId, incompleteVideoId],
    )).rows;
    assert.deepEqual(candidates, [{ attempts: 1, content_key: incompleteContentKey }]);
    assert.deepEqual((await pool.query(
      `SELECT status,attempts FROM crawler.content_enrich_tasks
       WHERE content_key=$1 AND job_type='player-refresh'`,
      [incompleteContentKey],
    )).rows[0], { status: "failed", attempts: 1 });

    const discoveryPayloads = (await pool.query(
      `SELECT observation_id,payload_json#>'{payload,discovery,payload}' AS discovery
       FROM crawler.crawler_outbox
       WHERE aggregate_key=$1 AND payload_json->>'observation_kind'='video'
       ORDER BY kind_sequence`,
      [`${channelId}:video`],
    )).rows;
    assert.equal(discoveryPayloads.length, 2);
    assert.deepEqual(
      discoveryPayloads.flatMap((row) => row.discovery.first_seen.map((item) => item.video_id)),
      [incompleteVideoId],
    );
    assert.deepEqual(
      discoveryPayloads.flatMap((row) => row.discovery.dispositions.map((item) => item.video_id)),
      [incompleteVideoId],
    );
    const consumingObservation = discoveryPayloads.find(
      (row) => row.discovery.first_seen.length === 1,
    ).observation_id;
    assert.deepEqual((await pool.query(
      `SELECT first_seen_ledger_status,first_seen_ledger_observation_id
       FROM crawler.content_candidates
       WHERE run_id=$1 AND source_content_id=$2`,
      [runId, incompleteVideoId],
    )).rows[0], {
      first_seen_ledger_status: "consumed",
      first_seen_ledger_observation_id: consumingObservation,
    });
    assert.equal((await pool.query(
      `SELECT latest_sequence FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0].latest_sequence, "2");
  } finally {
    await pool.query(
      `UPDATE crawler.settings SET value_json=$1::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
      [JSON.stringify(originalMode)],
    ).catch(() => {});
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});

test("a reservation cleanup outage cannot fail an already committed Clock cycle", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCcleanupatomic${suffix}`;
  const runId = `incremental:cleanup-atomic:${suffix}`;
  const planId = randomUUID();
  const anchorVideoId = `cleanup-anchor-${suffix}`;
  const refreshVideoId = `cleanup-refresh-${suffix}`;
  const refreshContentKey = `${channelId}:video:${refreshVideoId}`;
  const cleanupFailure = new Error("injected reservation cleanup outage");
  const originalMode = (await pool.query(
    `SELECT value_json FROM crawler.settings WHERE setting_key='content_enrich_dispatch'`,
  )).rows[0]?.value_json ?? { mode: "clock" };
  const withTransaction = postgresTransactionRunner(pool, {
    async beforeTransaction(transactionCount) {
      if (transactionCount === 3) throw cleanupFailure;
    },
  });

  try {
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"clock"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    await insertClockBase(pool, { channelId, runId, planId, anchorVideoId });
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,title,url,published_at,published_at_status,
         published_at_source,published_at_precision,access_status,
         access_status_source,first_seen_at,last_seen_at
       ) VALUES (
         $1,$2,$3,'video','youtube_watch_canonical',$4,'Original refresh',$5,
         '2026-08-23T00:00:00Z','exact','youtubejs_player','second','public',
         'youtubejs_player',now(),now()
       )`,
      [
        refreshContentKey,
        channelId,
        runId,
        refreshVideoId,
        `https://www.youtube.com/watch?v=${refreshVideoId}`,
      ],
    );
    await pool.query(
      `INSERT INTO crawler.content_enrich_tasks (
         task_id,content_key,channel_id,job_type,status,priority,attempts,next_retry_at
       ) VALUES ($1,$2,$3,'player-refresh','queued',10,0,clock_timestamp()-interval '1 second')`,
      [`player-refresh:${refreshVideoId}`, refreshContentKey, channelId],
    );

    const result = await executeIncrementalVideo({
      plan: clockPlan({
        channelId,
        planId,
        jobId: `incremental__cleanup_atomic__${suffix}`,
      }),
      runId,
      startedAt: "2026-08-24T00:00:00.000Z",
      query: (sql, params) => pool.query(sql, params),
      withTransaction,
      getChannelSnapshot: async () => ({
        scanUploads: async () => completeAnchorScan(channelId, anchorVideoId),
      }),
      fetchDetail: async (videoId) => completePublicDetail(videoId),
      now: () => new Date("2026-08-24T00:00:00.000Z"),
      crawlerVersion: "qy-v16-integration-test",
    });

    assert.equal(withTransaction.transactionCount(), 3);
    assert.equal(result.outcome, "complete");
    assert.equal(result.reservation_cleanup_deferred, true);
    assert.deepEqual((await pool.query(
      `SELECT status,attempts,lease_owner,lease_expires_at
       FROM crawler.content_enrich_tasks
       WHERE content_key=$1 AND job_type='player-refresh'`,
      [refreshContentKey],
    )).rows[0], {
      status: "done",
      attempts: 0,
      lease_owner: null,
      lease_expires_at: null,
    });
    const content = (await pool.query(
      `SELECT title,last_enriched_at,publication_item_hash
       FROM crawler.contents WHERE content_key=$1`,
      [refreshContentKey],
    )).rows[0];
    assert.equal(content.title, `Updated ${refreshVideoId}`);
    assert.equal(new Date(content.last_enriched_at).toISOString(), "2026-08-24T00:00:00.000Z");
    assert.match(content.publication_item_hash, /^sha256:[0-9a-f]{64}$/);
    assert.equal((await pool.query(
      `SELECT latest_sequence FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0].latest_sequence, "1");
    assert.equal((await pool.query(
      `SELECT count(*)::int AS count FROM crawler.crawler_outbox
       WHERE aggregate_key=$1`,
      [`${channelId}:video`],
    )).rows[0].count, 1);
  } finally {
    await pool.query(
      `UPDATE crawler.settings SET value_json=$1::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
      [JSON.stringify(originalMode)],
    ).catch(() => {});
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
           $1,$2,$3,'player-refresh',$4,10,$5,
           CASE
             WHEN $4='dead_letter' THEN NULL
             WHEN $6::boolean THEN clock_timestamp()-interval '1 second'
             ELSE clock_timestamp()+interval '7 days'
           END
         )`,
        [
          `player-refresh:${channelId}:${video.videoId}`,
          contentKey,
          channelId,
          video.status ?? "terminal",
          video.attempts ?? 5,
          video.due,
        ],
      );
    }
    await pool.query(
      `INSERT INTO crawler.channel_domain_cursors (channel_id,observation_kind,anchor_video_ids)
       VALUES ($1,'video',ARRAY[$2]::text[])`,
      [channelId, anchorId],
    );
    return { anchorId, planId, runId };
  }

  async function executeScenario(channelId, prepared, fetched, fetchResult = null) {
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
        if (fetchResult) return fetchResult(videoId);
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
    const incompleteVideoId = `clock-incomplete-${suffix}`;
    const exhaustedVideoId = `clock-exhausted-${suffix}`;
    const recoveredDeadLetterVideoId = `clock-dead-recovered-${suffix}`;
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
      {
        videoId: incompleteVideoId,
        publishedAt: "2026-01-01T00:00:00.000Z",
        due: true,
        status: "queued",
        attempts: 2,
      },
      {
        videoId: exhaustedVideoId,
        publishedAt: "2026-01-01T00:00:00.000Z",
        due: true,
        status: "failed",
        attempts: 7,
      },
      {
        videoId: recoveredDeadLetterVideoId,
        publishedAt: "2026-08-22T00:00:00.000Z",
        due: false,
        status: "dead_letter",
        attempts: 8,
      },
    ]);
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"clock"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    const clockFetched = [];
    await executeScenario(clockChannelId, clockPrepared, clockFetched, async (videoId) => {
      if (videoId === exhaustedVideoId) throw new Error("temporary Player timeout");
      const complete = {
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
      return videoId === incompleteVideoId
        ? { ...complete, duration_seconds: null }
        : complete;
    });
    assert.equal(clockFetched.includes(futureVideoId), false);
    assert.equal(clockFetched.filter((videoId) => videoId === dueVideoId).length, 1);
    assert.deepEqual((await pool.query(
      `SELECT content.source_content_id,content.last_enriched_at,
              task.status,task.attempts,task.next_retry_at
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
      enriched: row.last_enriched_at != null,
    })), [
      {
        source_content_id: recoveredDeadLetterVideoId,
        status: "done",
        attempts: 8,
        has_next_retry: false,
        enriched: true,
      },
      {
        source_content_id: dueVideoId,
        status: "done",
        attempts: 0,
        has_next_retry: false,
        enriched: true,
      },
      {
        source_content_id: exhaustedVideoId,
        status: "dead_letter",
        attempts: 8,
        has_next_retry: false,
        enriched: false,
      },
      {
        source_content_id: futureVideoId,
        status: "terminal",
        attempts: 5,
        has_next_retry: true,
        enriched: false,
      },
      {
        source_content_id: incompleteVideoId,
        status: "failed",
        attempts: 3,
        has_next_retry: true,
        enriched: false,
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
        view_count_text: "10",
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
