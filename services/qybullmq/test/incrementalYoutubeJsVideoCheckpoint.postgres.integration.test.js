import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import pg from "pg";
import {
  INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL,
  INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL,
  executeIncrementalYoutubeJsVideo,
  incrementalYoutubeJsVideoTargetHash,
} from "../src/incrementalYoutubeJsVideo.js";
import { incrementalYoutubeJsVideoCheckpointSchemaBlock } from "../src/incrementalYoutubeJsVideoSchema.js";

const { Client } = pg;
const integrationUrl = process.env.INCREMENTAL_YOUTUBEJS_CHECKPOINT_POSTGRES_TEST_URL;

async function expectDatabaseError(client, sql, params, code) {
  await client.query("SAVEPOINT expected_database_error");
  let caught = null;
  try {
    await client.query(sql, params);
  } catch (error) {
    caught = error;
  }
  await client.query("ROLLBACK TO SAVEPOINT expected_database_error");
  await client.query("RELEASE SAVEPOINT expected_database_error");
  assert.ok(caught, "expected PostgreSQL statement to fail");
  assert.equal(caught.code, code);
}

test("checkpoint schema enforces Batch finalization and Item claim shapes", {
  skip: integrationUrl ? false : "INCREMENTAL_YOUTUBEJS_CHECKPOINT_POSTGRES_TEST_URL is not configured",
}, async () => {
  const client = new Client({ connectionString: integrationUrl });
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const previousHeartbeatMs = process.env.INCREMENTAL_YOUTUBEJS_ITEM_HEARTBEAT_MS;
  await client.connect();
  try {
    process.env.INCREMENTAL_YOUTUBEJS_ITEM_HEARTBEAT_MS = "1000";
    await client.query("BEGIN");
    const identity = (await client.query(
      "SELECT current_database() AS database_name,to_regnamespace('crawler') AS crawler_schema",
    )).rows[0];
    assert.equal(identity.crawler_schema, null, "integration database must not contain crawler schema");
    await client.query("CREATE SCHEMA crawler");
    await client.query(
      `CREATE TABLE crawler.channel_runs (
         run_id text PRIMARY KEY,
         channel_id text,
         plan_id uuid,
         crawl_mode text,
         task_mask jsonb,
         result_json jsonb
       )`,
    );
    await client.query("CREATE TABLE crawler.crawl_observations (observation_id uuid PRIMARY KEY)");
    await client.query(incrementalYoutubeJsVideoCheckpointSchemaBlock(schema));

    const shape = (await client.query(
      `SELECT to_regclass('crawler.incremental_youtubejs_video_batches') IS NOT NULL AS batches,
              to_regclass('crawler.incremental_youtubejs_video_items') IS NOT NULL AS items`,
    )).rows[0];
    assert.deepEqual(shape, { batches: true, items: true });

    await client.query("INSERT INTO crawler.channel_runs(run_id) VALUES ('run-a')");
    await client.query(
      `INSERT INTO crawler.incremental_youtubejs_video_batches (
         run_id,cycle_key,plan_id,channel_id,status,cycle_observed_at,started_at,
         scan_json,anchors_json,discovery_entries_json,pending_deferred_video_ids,
         sampling_plan_json,sampling_config_json,target_hash,
         first_seen_checkpoint_status,first_seen_checkpoints_json
       ) VALUES (
         'run-a','base','11111111-1111-4111-8111-111111111111','UCtest','fetching',now(),now(),
         '{"entries":[]}'::jsonb,'[]'::jsonb,'[]'::jsonb,'[]'::jsonb,
         '{"rows":[]}'::jsonb,'{}'::jsonb,'sha256:test','pending','[]'::jsonb
       )`,
    );
    await client.query(
      `INSERT INTO crawler.incremental_youtubejs_video_items (
         run_id,cycle_key,phase,ordinal,video_id,target_json
       ) VALUES ('run-a','base','first_seen',0,'video-a','{"id":"video-a"}'::jsonb)`,
    );
    await client.query(
      `INSERT INTO crawler.incremental_youtubejs_video_items (
         run_id,cycle_key,phase,ordinal,video_id,target_json
       ) VALUES ('run-a','base','recent',0,'video-b','{"content_key":"key-b"}'::jsonb)`,
    );

    await expectDatabaseError(
      client,
      `UPDATE crawler.incremental_youtubejs_video_items
       SET status='captured',captured_at=now()
       WHERE run_id='run-a' AND cycle_key='base' AND video_id='video-a'`,
      [],
      "23514",
    );

    const claimToken = "22222222-2222-4222-8222-222222222222";
    const recentBlockedBeforeFirstSeen = await client.query(
      INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL,
      ["run-a", "base", "recent", "55555555-5555-4555-8555-555555555555", 300_000],
    );
    assert.equal(recentBlockedBeforeFirstSeen.rowCount, 0);
    const claimed = await client.query(INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL, [
      "run-a",
      "base",
      "first_seen",
      claimToken,
      300_000,
    ]);
    assert.equal(claimed.rowCount, 1);
    const secondClaimBlocked = await client.query(INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL, [
      "run-a",
      "base",
      "recent",
      "66666666-6666-4666-8666-666666666666",
      300_000,
    ]);
    assert.equal(secondClaimBlocked.rowCount, 0);
    const stale = await client.query(INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL, [
      "run-a",
      "base",
      "first_seen",
      "video-a",
      "33333333-3333-4333-8333-333333333333",
      "captured",
      JSON.stringify({ id: "video-a" }),
      JSON.stringify({ id: "exact" }),
      null,
    ]);
    assert.equal(stale.rowCount, 0);
    const captured = await client.query(INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL, [
      "run-a",
      "base",
      "first_seen",
      "video-a",
      claimToken,
      "captured",
      JSON.stringify({ id: "video-a" }),
      JSON.stringify({ id: "exact" }),
      null,
    ]);
    assert.equal(captured.rowCount, 1);
    const recentStillBlocked = await client.query(INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL, [
      "run-a",
      "base",
      "recent",
      "77777777-7777-4777-8777-777777777777",
      300_000,
    ]);
    assert.equal(recentStillBlocked.rowCount, 0);
    await client.query(
      `UPDATE crawler.incremental_youtubejs_video_batches
       SET first_seen_checkpoint_status='complete'
       WHERE run_id='run-a' AND cycle_key='base'`,
    );
    const recentToken = "88888888-8888-4888-8888-888888888888";
    const recentClaimed = await client.query(INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL, [
      "run-a",
      "base",
      "recent",
      recentToken,
      300_000,
    ]);
    assert.equal(recentClaimed.rowCount, 1);
    const recentCaptured = await client.query(INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL, [
      "run-a",
      "base",
      "recent",
      "video-b",
      recentToken,
      "captured",
      JSON.stringify({ id: "video-b" }),
      JSON.stringify({ id: "exact" }),
      null,
    ]);
    assert.equal(recentCaptured.rowCount, 1);

    await expectDatabaseError(
      client,
      `INSERT INTO crawler.incremental_youtubejs_video_items (
         run_id,cycle_key,phase,ordinal,video_id,target_json
       ) VALUES ('run-a','base','recent',1,'video-a','{"content_key":"key"}'::jsonb)`,
      [],
      "23505",
    );
    await expectDatabaseError(
      client,
      `UPDATE crawler.incremental_youtubejs_video_batches
       SET status='finalized'
       WHERE run_id='run-a' AND cycle_key='base'`,
      [],
      "23514",
    );

    const observationId = "44444444-4444-4444-8444-444444444444";
    await client.query(
      "INSERT INTO crawler.crawl_observations(observation_id) VALUES ($1::uuid)",
      [observationId],
    );
    await client.query(
      `UPDATE crawler.incremental_youtubejs_video_batches
       SET status='ready'
       WHERE run_id='run-a' AND cycle_key='base'`,
    );
    await expectDatabaseError(
      client,
      `UPDATE crawler.incremental_youtubejs_video_batches
       SET status='finalized',final_observation_id=$1::uuid,
           final_result_json='{"observation_id":null}'::jsonb,finalized_at=now()
       WHERE run_id='run-a' AND cycle_key='base'`,
      [observationId],
      "23514",
    );
    const finalized = await client.query(
      `UPDATE crawler.incremental_youtubejs_video_batches
       SET status='finalized',final_observation_id=$1::uuid,
           final_result_json=jsonb_build_object('observation_id',$1::text),finalized_at=now()
       WHERE run_id='run-a' AND cycle_key='base'`,
      [observationId],
    );
    assert.equal(finalized.rowCount, 1);

    const checkpointPlan = {
      plan_id: "99999999-9999-4999-8999-999999999999",
      channel_id: "UCcheckpoint",
      job_id: "incremental-checkpoint-test",
      plan_day: "2026-09-03",
      scheduled_at: "2026-09-03T00:00:00.000Z",
      planner_config_version: "video-plan-1",
      task_mask: { about: false, video: true, agent: false },
      capacity: { version: "test", factor: 1, player_cap: 3, next_cap: 0 },
    };
    const checkpointRunId = "incremental:99999999-9999-4999-8999-999999999999";
    const entries = ["parser-gap", "captured", "retryable"].map((id, ordinal) => ({
      id,
      ordinal,
      position: ordinal + 1,
      title: id,
      content_type: "video",
    }));
    const targets = entries.map((entry, ordinal) => ({
      phase: "first_seen",
      ordinal,
      video_id: entry.id,
      target_json: entry,
    }));
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,plan_id,crawl_mode,task_mask,result_json
       ) VALUES ($1,$2,$3::uuid,'incremental',$4::jsonb,'{}'::jsonb)`,
      [
        checkpointRunId,
        checkpointPlan.channel_id,
        checkpointPlan.plan_id,
        JSON.stringify(checkpointPlan.task_mask),
      ],
    );
    await client.query(
      `INSERT INTO crawler.incremental_youtubejs_video_batches (
         run_id,cycle_key,plan_id,channel_id,status,cycle_observed_at,started_at,
         scan_json,anchors_json,discovery_entries_json,pending_deferred_video_ids,
         sampling_plan_json,sampling_config_json,target_hash,
         first_seen_checkpoint_status,first_seen_checkpoints_json
       ) VALUES (
         $1,'base',$2::uuid,$3,'fetching','2026-09-03T01:00:00.000Z',
         '2026-09-03T00:59:00.000Z',$4::jsonb,'[]'::jsonb,$5::jsonb,'[]'::jsonb,
         '{"rows":[]}'::jsonb,$6::jsonb,$7,'pending','[]'::jsonb
       )`,
      [
        checkpointRunId,
        checkpointPlan.plan_id,
        checkpointPlan.channel_id,
        JSON.stringify({ complete: true, entries, pages: 1, stop_reason: "anchor_matched" }),
        JSON.stringify(entries),
        JSON.stringify({
          version: "video-plan-1",
          sampling_plan_input: checkpointPlan,
          crawler_version: "test",
        }),
        incrementalYoutubeJsVideoTargetHash(targets),
      ],
    );
    await client.query(
      `INSERT INTO crawler.incremental_youtubejs_video_items (
         run_id,cycle_key,phase,ordinal,video_id,target_json
       )
       SELECT $1,'base',item.phase,item.ordinal,item.video_id,item.target_json
       FROM jsonb_to_recordset($2::jsonb)
         AS item(phase text,ordinal integer,video_id text,target_json jsonb)`,
      [checkpointRunId, JSON.stringify(targets)],
    );

    let transactionDepth = 0;
    let claimHeartbeatCount = 0;
    const withTransaction = async (action) => {
      transactionDepth += 1;
      try {
        return await action({
          query(sql, params) {
            if (String(sql).includes("SET claim_expires_at=clock_timestamp()")) {
              claimHeartbeatCount += 1;
            }
            return client.query(sql, params);
          },
        });
      } finally {
        transactionDepth -= 1;
      }
    };
    const firstAttemptCalls = [];
    const routeFailure = Object.assign(new Error("429 Too Many Requests"), { status: 429 });
    const parserCause = new TypeError(
      "Cannot read properties of undefined (reading 'contents')",
    );
    const parserFailure = Object.assign(
      new Error("YouTube.js required comments surface failed", { cause: parserCause }),
      {
        name: "YoutubeJsRequiredSurfaceError",
        required_surface: "comments",
        partial_detail: {
          id: "parser-gap",
          title: "Parser gap",
          access_status: "public",
          description: "observed player detail",
          description_observed: true,
        },
      },
    );
    const execute = (fetchDetail) => executeIncrementalYoutubeJsVideo({
      plan: checkpointPlan,
      runId: checkpointRunId,
      getChannelSnapshot: async () => {
        throw new Error("a frozen Batch must not scan again");
      },
      query: client.query.bind(client),
      withTransaction,
      startedAt: "2026-09-03T00:59:00.000Z",
      fetchDetail,
      crawlerVersion: "test",
      now: () => new Date("2026-09-03T01:05:00.000Z"),
    });
    await assert.rejects(
      execute(async (videoId, { signal }) => {
        assert.equal(transactionDepth, 0, "Video HTTP must run outside a logical transaction");
        assert.equal(signal?.aborted, false);
        firstAttemptCalls.push(videoId);
        if (videoId === "parser-gap") throw parserFailure;
        if (videoId === "retryable") throw routeFailure;
        await delay(1_200);
        return {
          id: videoId,
          title: "Captured",
          access_status: "public",
          description: "",
          description_observed: true,
        };
      }),
      (error) => error === routeFailure,
    );
    assert.deepEqual(firstAttemptCalls, ["parser-gap", "captured", "retryable"]);
    assert.ok(claimHeartbeatCount >= 1, "a slow Detail request must renew its Item claim");

    const firstAttemptItems = (await client.query(
      `SELECT video_id,status,attempt_count,detail_json,field_status_json,error_json
       FROM crawler.incremental_youtubejs_video_items
       WHERE run_id=$1 AND cycle_key='base'
       ORDER BY ordinal`,
      [checkpointRunId],
    )).rows;
    assert.deepEqual(firstAttemptItems.map((item) => item.status), [
      "settled_error",
      "captured",
      "pending",
    ]);
    assert.equal(firstAttemptItems[0].detail_json.description, "observed player detail");
    assert.equal(firstAttemptItems[0].field_status_json.comment_count, "parser_gap");
    assert.equal(firstAttemptItems[0].error_json.decision.kind, "parser_runtime");
    assert.equal(firstAttemptItems[1].detail_json.id, "captured");
    assert.equal(firstAttemptItems[2].detail_json, null);

    const resumedCalls = [];
    const transientFailure = new Error("socket connection reset by peer");
    await assert.rejects(
      execute(async (videoId) => {
        assert.equal(transactionDepth, 0, "resumed Video HTTP must run outside a transaction");
        resumedCalls.push(videoId);
        throw transientFailure;
      }),
      (error) => error === transientFailure,
    );
    assert.deepEqual(resumedCalls, ["retryable"]);
    const resumedItems = (await client.query(
      `SELECT video_id,status,attempt_count
       FROM crawler.incremental_youtubejs_video_items
       WHERE run_id=$1 AND cycle_key='base'
       ORDER BY ordinal`,
      [checkpointRunId],
    )).rows;
    assert.deepEqual(resumedItems.map((item) => [
      item.video_id,
      item.status,
      Number(item.attempt_count),
    ]), [
      ["parser-gap", "settled_error", 1],
      ["captured", "captured", 1],
      ["retryable", "pending", 2],
    ]);
  } finally {
    if (previousHeartbeatMs === undefined) {
      delete process.env.INCREMENTAL_YOUTUBEJS_ITEM_HEARTBEAT_MS;
    } else {
      process.env.INCREMENTAL_YOUTUBEJS_ITEM_HEARTBEAT_MS = previousHeartbeatMs;
    }
    await client.query("ROLLBACK").catch(() => {});
    await client.end();
  }
});
