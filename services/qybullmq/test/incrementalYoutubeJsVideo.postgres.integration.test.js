import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { executeIncrementalYoutubeJsVideo } from "../src/incrementalYoutubeJsVideo.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { BusinessRunBindingStore } from "../src/businessRunBindingStore.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";
import { createFullCrawlYoutubeJsExecutor } from "../src/fullCrawlYoutubeJsFactory.js";
import { FullCrawlYoutubeJsStore } from "../src/fullCrawlYoutubeJsStore.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

function transactionRunner(pool) {
  return async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  };
}

function publicDetail(videoId) {
  return {
    id: videoId,
    title: `YouTubeJS ${videoId}`,
    thumbnail_url: "https://i.ytimg.com/vi/checkpoint/default.jpg",
    published_at: "2026-09-02T12:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtubejs_player",
    duration_seconds: 90,
    duration_source: "youtubejs_player",
    view_count: 321,
    view_count_text: "321",
    view_count_source: "youtubejs_player",
    like_count: 12,
    like_count_source: "youtubejs_player",
    comment_count: 0,
    comment_count_status: "exact",
    comment_count_source: "youtubejs_comments",
    comments_disabled: false,
    comments_first_page: {
      version: 1,
      total_count: 0,
      returned_count: 0,
      comments: [],
    },
    description: "Captured once",
    description_status: "exact",
    description_source: "youtubejs_player",
    description_observed: true,
    hashtags: ["checkpoint"],
    hashtags_observed: true,
    keywords: ["youtubejs"],
    keywords_observed: true,
    availability: "public",
    access_status: "public",
    access_status_source: "youtubejs_player",
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
    source: "youtubejs_get_info",
  };
}

async function initializeThroughFullCrawl(pool, withTransaction, channelId, videoId, suffix) {
  const batchId = `shared-full-${suffix}`;
  const runId = `full:${suffix}`;
  const jobId = `channel-snapshot__${batchId}__${channelId}__g1`;
  const businessRunKey = `shared-full:${suffix}`;
  await pool.query(`INSERT INTO crawler.query_dispatch_batches
    (dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at)
    VALUES ($1,$1,'validation_closed',now())`, [batchId]);
  const candidateId = Number((await pool.query(`INSERT INTO crawler.channel_candidates
    (dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
     snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt)
    VALUES ($1,$1,$2,$3,'queued',1,$4,1) RETURNING candidate_id`,
  [batchId, channelId, `https://www.youtube.com/channel/${channelId}`, jobId])).rows[0].candidate_id);
  await new BusinessRunBindingStore({ withTransaction }).resolve({
    businessRunKey, explicitBusinessRunId: runId, requestedStatus: "reserved", runKind: "full",
    channelId, candidateId, policy: { id: "shared-test", version: 1, hash: "sha256:shared-test" },
    intent: { job_name: "channel-snapshot", crawl_mode: "full", fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT },
  });
  const detail = { ...publicDetail(videoId), title: "Stored recent Video",
    thumbnail_url: "https://i.ytimg.com/vi/stored/default.jpg", description: "Stored description",
    keywords: ["stored"], published_at: "2026-09-01T00:00:00Z",
    published_at_precision: "date_only", duration_seconds: 45 };
  const execute = createFullCrawlYoutubeJsExecutor({
    store: new FullCrawlYoutubeJsStore({ query: pool.query.bind(pool), withTransaction }),
    clock: () => new Date("2026-09-02T00:00:00Z"),
    youtube: {
      fetchChannel: async () => ({ about_requested: true, about_observed: true,
        metadata: { channel_id: channelId, channel_url: `https://www.youtube.com/channel/${channelId}`,
          handle: "@shared-test", title: "Full to Incremental", country: "Brazil",
          subscriber_count: 10000, subscriber_count_text: "10,000", subscriber_count_source: "youtube_about" },
        raw: { engine: "youtubei.js@test" } }),
      fetchUploads: async () => ({ playlist_id: `UU${channelId.slice(2)}`,
        entries: [{ video_id: videoId, position: 1, title: detail.title,
          url: `https://www.youtube.com/watch?v=${videoId}`, published_at: detail.published_at,
          published_at_status: "exact", published_at_precision: "date_only", published_at_source: "youtubejs_player" }],
        activity_evidence_complete: true,
        scan: { complete: true, stop_reason: "list_end", terminal_reason: "list_end",
          pages: 1, inspected_count: 1, parse_gap_count: 0 } }),
      fetchDetail: async () => detail,
    },
  });
  const result = await execute({ id: jobId, name: "channel-snapshot", attemptsStarted: 1,
    queueName: "youtube-channel-crawl", data: { channel_id: channelId, candidate_id: candidateId,
      run_id: runId, business_run_key: businessRunKey, dispatch_batch_id: batchId,
      dispatch_generation: 1, fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } });
  assert.equal(result.ok, true);
  assert.equal(result.detail_processed, 1);
}

async function checkpointReplayScenario(enrichPending) {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 4,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCyoutubejscheckpoint${suffix}`;
  const planId = randomUUID();
  const runId = `incremental:${planId}`;
  const jobId = `incremental__${channelId}__20260903__clock_1__test`;
  const videoId = `youtubejs-checkpoint-${suffix}`;
  const recentVideoId = `youtubejs-recent-${suffix}`;
  const recentContentKey = `${channelId}:video:${recentVideoId}`;
  const plan = {
    job_id: jobId,
    plan_id: planId,
    plan_day: "2026-09-03",
    scheduled_at: "2026-09-03T00:00:00.000Z",
    channel_id: channelId,
    task_mask: { about: false, video: true, agent: false },
    capacity: { factor: 1, player_cap: 4, next_cap: 0, version: "capacity-1" },
    planner_config_version: "video-plan-1",
  };
  const withTransaction = transactionRunner(pool);
  const fetched = [];
  let scans = 0;

  try {
    await initializeThroughFullCrawl(pool, withTransaction, channelId, recentVideoId, suffix);
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status,
         plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
         clock_version,policy_version,planner_config_version,capacity_version,
         crawler_version,started_at,result_json
       ) VALUES (
         $1,$2,'running','incremental',0,'pending',$3,'2026-09-03','clock_due',
         $4::jsonb,'2026-09-03T00:00:00Z',1,'v16-rule-1','video-plan-1',
         'capacity-1','test','2026-09-03T00:00:00Z',jsonb_build_object('job_id',$5::text)
       )`,
      [runId, channelId, planId, JSON.stringify(plan.task_mask), jobId],
    );
    if (enrichPending) {
      await pool.query(
        `INSERT INTO crawler.content_enrich_tasks (
           task_id,content_key,channel_id,job_type,status,priority,next_retry_at
         ) VALUES ($1,$2,$3,'player-refresh','queued',10,now())`,
        [`player-refresh:${suffix}`, recentContentKey, channelId],
      );
    }

    const result = await executeIncrementalYoutubeJsVideo({
      plan,
      runId,
      startedAt: "2026-09-03T00:00:00.000Z",
      query: (sql, params) => pool.query(sql, params),
      withTransaction,
      getChannelSnapshot: async () => ({
        async scanUploads({ anchors }) {
          scans += 1;
          assert.deepEqual(anchors, [{ id: recentVideoId, published_day: "2026-09-01" }]);
          return {
            playlist_id: `UU${channelId.slice(2)}`,
            entries: [
              {
                id: videoId,
                position: 1,
                title: `Uploads ${videoId}`,
                content_type: "video",
                published_day: "2026-09-02",
              },
              {
                id: recentVideoId,
                position: 2,
                title: `Uploads ${recentVideoId}`,
                content_type: "video",
                published_day: "2026-09-01",
              },
            ],
            pages: 1,
            first_page_item_count: 2,
            catch_up_item_count: 0,
            item_count: 2,
            parse_gap_count: 0,
            anchor_matched: true,
            matched_anchor_id: recentVideoId,
            crossed_anchor_ids: [],
            stop_reason: "anchor_matched",
            terminal_reason: "anchor_matched",
            complete: true,
            raw: { engine: "youtubei.js@test" },
          };
        },
      }),
      fetchDetail: async (requestedVideoId) => {
        fetched.push(requestedVideoId);
        return publicDetail(requestedVideoId);
      },
      now: () => new Date("2026-09-03T01:00:00.000Z"),
      crawlerVersion: "youtubejs-checkpoint-integration",
    });

    assert.equal(result.outcome, "complete");
    assert.equal(result.first_seen_count, 1);
    assert.equal(result.selected_count, 1);
    assert.equal(result.duplicate, false);
    assert.equal(scans, 1);
    assert.deepEqual(fetched, [videoId, recentVideoId]);

    const batch = (await pool.query(
      `SELECT status,cycle_key,final_observation_id,final_result_json
       FROM crawler.incremental_youtubejs_video_batches
       WHERE run_id=$1`,
      [runId],
    )).rows[0];
    assert.equal(batch.status, "finalized");
    assert.equal(batch.cycle_key, "base");
    assert.equal(batch.final_observation_id, result.observation_id);
    assert.deepEqual(batch.final_result_json, result);

    const items = (await pool.query(
      `SELECT phase,video_id,status,attempt_count,detail_json,field_status_json,error_json,target_json
       FROM crawler.incremental_youtubejs_video_items
       WHERE run_id=$1
       ORDER BY CASE phase WHEN 'first_seen' THEN 0 ELSE 1 END,ordinal`,
      [runId],
    )).rows;
    assert.deepEqual(items.map((item) => [item.phase, item.video_id]), [
      ["first_seen", videoId],
      ["recent", recentVideoId],
    ]);
    const item = items[0];
    assert.equal(item.status, "captured");
    assert.equal(Number(item.attempt_count), 1);
    assert.equal(item.detail_json.description, "Captured once");
    assert.equal(item.field_status_json.description, "exact");
    assert.equal(item.field_status_json.comment_count, "exact");
    assert.equal(item.error_json, null);
    assert.equal(items[1].status, "captured");
    assert.equal(Number(items[1].attempt_count), 1);
    assert.equal(items[1].detail_json.id, recentVideoId);
    assert.equal(items[1].target_json.enrich_pending, enrichPending);

    const content = (await pool.query(
      `SELECT title,description,view_count,comment_count,publication_item_hash
       FROM crawler.contents
       WHERE channel_id=$1 AND source_content_id=$2`,
      [channelId, videoId],
    )).rows[0];
    assert.equal(content.title, `YouTubeJS ${videoId}`);
    assert.equal(content.description, "Captured once");
    assert.equal(content.view_count, "321");
    assert.equal(content.comment_count, "0");
    assert.match(content.publication_item_hash, /^sha256:[a-f0-9]{64}$/);
    const recentContent = (await pool.query(
      `SELECT title,thumbnail_url,description,keywords,duration_seconds,
              view_count,like_count,comment_count,published_at,
              player_last_observed_at,publication_item_hash
       FROM crawler.contents
       WHERE content_key=$1`,
      [recentContentKey],
    )).rows[0];
    assert.equal(recentContent.title, enrichPending ? `YouTubeJS ${recentVideoId}` : "Stored recent Video");
    assert.equal(recentContent.thumbnail_url, enrichPending
      ? "https://i.ytimg.com/vi/checkpoint/default.jpg" : "https://i.ytimg.com/vi/stored/default.jpg");
    assert.equal(recentContent.description, enrichPending ? "Captured once" : "Stored description");
    assert.deepEqual(recentContent.keywords, enrichPending ? ["youtubejs"] : ["stored"]);
    assert.equal(recentContent.duration_seconds, enrichPending ? 90 : 45);
    assert.equal(recentContent.view_count, "321");
    assert.equal(recentContent.like_count, "12");
    assert.equal(recentContent.comment_count, "0");
    assert.equal(
      new Date(recentContent.published_at).toISOString(),
      enrichPending ? "2026-09-02T12:00:00.000Z" : "2026-09-01T00:00:00.000Z",
    );
    assert.equal(
      new Date(recentContent.player_last_observed_at).toISOString(),
      "2026-09-03T01:00:00.000Z",
    );
    assert.match(recentContent.publication_item_hash, /^sha256:[a-f0-9]{64}$/);
    const recentTask = (await pool.query(
      `SELECT status,lease_owner,lease_expires_at,result_json->'last_outcome' AS last_outcome
       FROM crawler.content_enrich_tasks
       WHERE content_key=$1 AND job_type='player-refresh'`,
      [recentContentKey],
    )).rows[0];
    if (enrichPending) {
      assert.equal(recentTask.status, "done");
      assert.equal(recentTask.lease_owner, null);
      assert.equal(recentTask.lease_expires_at, null);
      assert.equal(recentTask.last_outcome.kind, "done");
    } else {
      assert.equal(recentTask, undefined);
    }

    const observation = (await pool.query(
      `SELECT observation.observation_id,key.idempotency_key,
              observation.outcome,observation.extractor_versions,
              observation.result_summary_json
       FROM crawler.crawl_observations observation
       JOIN crawler.crawl_observation_keys key
         ON key.observation_id=observation.observation_id
       WHERE observation.run_id=$1 AND observation.observation_kind='video'`,
      [runId],
    )).rows;
    assert.equal(observation.length, 1);
    assert.equal(observation[0].observation_id, result.observation_id);
    assert.equal(observation[0].idempotency_key, `video:${runId}:youtubejs:base`);
    assert.equal(observation[0].outcome, "complete");
    assert.deepEqual(observation[0].extractor_versions, { youtubejs: "youtubei.js@test" });

    const cursor = (await pool.query(
      `SELECT anchor_video_ids,source_cursor
       FROM crawler.channel_domain_cursors
       WHERE channel_id=$1 AND observation_kind='video'`,
      [channelId],
    )).rows[0];
    assert.equal(cursor.anchor_video_ids[0], videoId);
    assert.equal(cursor.source_cursor.terminal_reason, "anchor_matched");
    const outbox = (await pool.query(
      `SELECT payload_json
       FROM crawler.crawler_outbox
       WHERE aggregate_key=$1 AND payload_json->>'observation_kind'='video'`,
      [`${channelId}:video`],
    )).rows;
    assert.equal(outbox.length, 1);
    const activityEvidence = outbox[0].payload_json.payload.activity_evidence;
    const {
      lifecycle_status: _lifecycleStatus,
      conclusive: _conclusive,
      ...summaryEvidence
    } = observation[0].result_summary_json.activity;
    assert.deepEqual(summaryEvidence, activityEvidence);

    const replayed = await executeIncrementalYoutubeJsVideo({
      plan,
      runId,
      startedAt: "2026-09-03T02:00:00.000Z",
      query: (sql, params) => pool.query(sql, params),
      withTransaction,
      getChannelSnapshot: async () => {
        throw new Error("finalized replay must not scan");
      },
      fetchDetail: async () => {
        throw new Error("finalized replay must not fetch Detail");
      },
      now: () => new Date("2026-09-03T02:00:00.000Z"),
      crawlerVersion: "a-replay-must-use-the-stored-version",
    });
    assert.deepEqual(replayed, result);
    assert.equal(scans, 1);
    assert.deepEqual(fetched, [videoId, recentVideoId]);
    assert.equal((await pool.query(
      `SELECT count(*)::int AS count
       FROM crawler.crawl_observations
       WHERE run_id=$1 AND observation_kind='video'`,
      [runId],
    )).rows[0].count, 1);
  } finally {
    await pool.query(
      "DELETE FROM crawler.incremental_youtubejs_video_items WHERE run_id=$1",
      [runId],
    ).catch(() => {});
    await pool.query(
      "DELETE FROM crawler.incremental_youtubejs_video_batches WHERE run_id=$1",
      [runId],
    ).catch(() => {});
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
}

for (const enrichPending of [false, true]) {
  test(`Full Crawl feeds Incremental ${enrichPending ? "pending repair" : "ordinary metrics"} and checkpoint replay without network`, {
    skip: integrationUrl ? false : "INCREMENTAL_POSTGRES_TEST_URL is not configured",
  }, () => checkpointReplayScenario(enrichPending));
}
