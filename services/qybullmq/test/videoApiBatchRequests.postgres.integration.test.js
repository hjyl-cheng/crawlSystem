import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { requestVideoApiDetail, dispatchVideoApiRequests, waitForVideoApiDetail, completeVideoApiRequests } from "../src/videoApiBatchRequests.js";

const databaseUrl = process.env.VIDEO_API_TEST_DATABASE_URL;
test("shared API task delivery, batching, replay and bounded failures in PostgreSQL", { skip: !databaseUrl }, async t => {
  const url = new URL(databaseUrl);
  assert.match(url.pathname, /test/);
  assert.ok(["localhost", "127.0.0.1"].includes(url.hostname));
  const pool = new pg.Pool({ connectionString: databaseUrl });
  t.after(() => pool.end());
  const query = pool.query.bind(pool);
  const transaction = async action => {
    const client = await pool.connect();
    try { await client.query("BEGIN"); const result = await action(client); await client.query("COMMIT"); return result; }
    catch (error) { await client.query("ROLLBACK"); throw error; }
    finally { client.release(); }
  };
  await query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await query(crawlerRuntimeSchema(await readFile(new URL("../src/schema.sql", import.meta.url), "utf8")));
  await query(`INSERT INTO crawler.channels(channel_id,channel_url,title) VALUES ('channel1','https://youtube.com/channel/channel1','test');
    INSERT INTO crawler.channel_runs(run_id,channel_id,started_at) VALUES ('full-run','channel1',now()),('incremental-run','channel1',now())`);
  const jobs = new Map();
  const queue = {
    getJob: async id => jobs.get(id),
    add: async (name, data, options) => {
      const job = { name, data, id: options.jobId, attemptsStarted: 1, getState: async () => "waiting" };
      jobs.set(job.id, job); return job;
    },
  };
  const dispatch = () => dispatchVideoApiRequests({ query, withTransaction: transaction, queue });
  const subscribe = (requestId, videoId, consumer = "full", extra = {}) => requestVideoApiDetail(transaction, {
    requestId, videoId, consumer, runId: consumer === "full" ? "full-run" : "incremental-run", ...extra,
  });
  const [full, incremental] = await Promise.all([
    subscribe("full1", "video1"), subscribe("inc1", "video1", "incremental"),
  ]);
  assert.equal(full.task_id, incremental.task_id);
  assert.equal((await subscribe("full1", "video1")).request_id, "full1");
  await assert.rejects(subscribe("full1", "different-video"), /identity conflicts/);
  await subscribe("missing1", "missing-video");
  assert.equal((await dispatch()).dispatched, 1);
  assert.equal(jobs.size, 1);
  const firstJob = [...jobs.values()][0];
  assert.deepEqual(firstJob.data.video_ids, ["video1", "missing-video"]);
  assert.equal((await dispatch()).dispatched, 1);
  assert.equal(jobs.size, 1, "reconciliation must reuse the original Job");

  Object.assign(process.env, { DATABASE_URL: databaseUrl, DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: url.pathname.slice(1), SKIP_SCHEMA_MIGRATION: "true", S3_ENDPOINT: "",
    YOUTUBE_DATA_API_KEY: "test-only-key", YOUTUBE_DATA_API_DAILY_REQUEST_LIMIT: "20" });
  const { processDataApiBatchV2, closePipelineV2Queues } = await import("../src/pipelineV2.js");
  t.after(() => closePipelineV2Queues());
  const { closeDb } = await import("../src/db.js");
  t.after(() => closeDb());
  let calls = 0;
  const result = await processDataApiBatchV2(firstJob, { fetchDetails: async ids => {
    calls += 1;
    assert.equal(ids.length, 2);
    return { detailsById: new Map([["video1", { title: "API observed", privacy_status: "public",
      comments_disabled: true, view_count_text: "321" }]]), raw: { items: [] }, returnedCount: 1 };
  } });
  assert.equal(result.ok, true);
  assert.equal(calls, 1);
  assert.equal((await waitForVideoApiDetail(query, "full1")).title, "API observed");
  assert.equal((await waitForVideoApiDetail(query, "inc1")).view_count_text, "321");
  await assert.rejects(waitForVideoApiDetail(query, "missing1"), { code: "VIDEO_API_FALLBACK_UNRESOLVED" });
  const usage = (await query("SELECT request_count,requested_video_count FROM crawler.youtube_api_daily_usage")).rows[0];
  assert.equal(Number(usage.request_count), 1);
  assert.equal(Number(usage.requested_video_count), 2);
  const stale = await processDataApiBatchV2(firstJob, { fetchDetails: async () => assert.fail("completed Batch must reject stale delivery") });
  assert.equal(stale.skipped, true);

  const cached = await subscribe("cached1", "video1");
  assert.equal(cached.status, "done");
  assert.equal((await dispatch()).pending, 0);
  await subscribe("failed1", "failed-video");
  await query("UPDATE crawler.youtube_api_tasks SET status='failed',attempts=3 WHERE source_content_id='failed-video'");
  await subscribe("failed2", "failed-video", "incremental");
  await dispatch();
  assert.equal((await query("SELECT attempts FROM crawler.youtube_api_tasks WHERE source_content_id='failed-video'")).rows[0].attempts, 3);
  await assert.rejects(waitForVideoApiDetail(query, "failed2"), { code: "VIDEO_API_FALLBACK_UNRESOLVED" });

  const comments = await subscribe("comments1", "comment-video", "incremental", { requireComments: true });
  await transaction(client => completeVideoApiRequests(client, comments.task_id, { title: "metadata only" }, true));
  assert.equal((await query("SELECT status FROM crawler.youtube_api_detail_requests WHERE request_id='comments1'")).rows[0].status, "pending");
  await transaction(client => completeVideoApiRequests(client, comments.task_id, { comments_first_page: { status: "done" } }, true));
  assert.ok((await waitForVideoApiDetail(query, "comments1")).comments_first_page);
  const quotaRequest = await subscribe("quota1", "quota-video");
  await query("UPDATE crawler.youtube_api_daily_usage SET request_count=20");
  await dispatch();
  const quotaJob = [...jobs.values()].find(job => job.data.video_ids.includes("quota-video"));
  const quota = await processDataApiBatchV2(quotaJob, {
    fetchDetails: async () => assert.fail("exhausted daily quota must prevent API calls"),
  });
  assert.equal(quota.deferred, true);
  assert.equal((await query("SELECT attempts FROM crawler.youtube_api_tasks WHERE task_id=$1", [quotaRequest.task_id])).rows[0].attempts, 0);
  await assert.rejects(waitForVideoApiDetail(query, "quota1"), /deferred until/);
});
