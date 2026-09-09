import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import pg from "pg";
import { Queue, Worker } from "bullmq";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { createVideoDetailApiFallback } from "../src/videoDetailApiFallback.js";
import { completeVideoApiRequests, dispatchVideoApiRequests } from "../src/videoApiBatchRequests.js";
import { gateVideoApiJob, runVideoApiResumable } from "../src/videoApiContinuation.js";
import { executeManagedWorkerAttempt } from "../src/managedWorkerExecution.js";
import { validateYoutubeJsVideoDetail } from "../src/youtubeJsVideoDetailContract.js";

const databaseUrl = process.env.VIDEO_API_TEST_DATABASE_URL;
const redisUrl = process.env.VIDEO_API_TEST_REDIS_URL;
const holdMs = Number(process.env.API_CONTINUATION_DELAY_TEST_MS || 100);

test("real workers release the channel slot, survive restart, and consume late shared API results without network retries", {
  skip: !databaseUrl || !redisUrl, timeout: holdMs + 30000,
}, async t => {
  const db = new URL(databaseUrl), redis = new URL(redisUrl);
  assert.match(db.pathname, /test$/);
  for (const url of [db, redis]) assert.ok(["localhost", "127.0.0.1"].includes(url.hostname));
  const pool = new pg.Pool({ connectionString: databaseUrl });
  const query = pool.query.bind(pool);
  const withTransaction = async action => {
    const client = await pool.connect();
    try { await client.query("BEGIN"); const result = await action(client); await client.query("COMMIT"); return result; }
    catch (error) { await client.query("ROLLBACK"); throw error; }
    finally { client.release(); }
  };
  const connection = { host: redis.hostname, port: Number(redis.port), maxRetriesPerRequest: null };
  const prefix = `api-continuation-${randomUUID()}`;
  const queue = new Queue("channels", { connection, prefix });
  const apiQueue = new Queue("api", { connection, prefix });
  let worker;
  t.after(async () => { await worker?.close(); await queue.obliterate({ force: true });
    await apiQueue.obliterate({ force: true }); await queue.close(); await apiQueue.close(); await pool.end(); });
  await query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await query(crawlerRuntimeSchema(await readFile(new URL("../src/schema.sql", import.meta.url), "utf8")));
  await query(`INSERT INTO crawler.channels(channel_id,channel_url,title) VALUES ('channel','https://youtube.com/channel/channel','test');
    INSERT INTO crawler.channel_runs(run_id,channel_id,started_at) VALUES ('full-run','channel',now()),('incremental-run','channel',now())`);
  const fallback = createVideoDetailApiFallback({ query, withTransaction,
    loadSettings: async () => ({ apiKeys: ["test"], dailyRequestLimit: 100, fallbackMode: "emergency" }) });
  let fetches = 0, networkTasks = 0;
  const results = [];
  const errors = [];
  const execute = async job => {
    if (job.name === "ordinary") return { ordinary: true };
    return fallback({ runId: job.data.run_id, requestId: job.id, videoId: "video1", consumer: job.data.consumer,
      fetch: async () => { fetches++;
        throw Object.assign(new Error("required surface missing"), { name: "YoutubeJsRequiredSurfaceError",
          partial_detail: { content_type_signals: { canonical_url: "https://www.youtube.com/shorts/video1" } } }); },
      validate: detail => validateYoutubeJsVideoDetail("video1", detail, { optionalComments: true }) });
  };
  const startWorker = () => {
    const instance = new Worker("channels", async (job, token) => {
      await gateVideoApiJob({ query, job, token, delayMs: 50 });
      return runVideoApiResumable({ job, token, delayMs: 50,
        executeReplay: () => execute(job), execute: async () => {
          networkTasks++;
          return (await executeManagedWorkerAttempt({ job, execute: () => execute(job),
            persistRetryableCheckpoint: async () => assert.fail("must not retry a network route while awaiting API") })).result;
        } });
    }, { connection, prefix, concurrency: 1 });
    instance.on("completed", job => results.push(job.id));
    instance.on("failed", (job, error) => errors.push({ id: job?.id, error: error.message }));
    return instance;
  };
  const until = async condition => {
    const deadline = Date.now() + 10000;
    while (!await condition()) { assert.ok(Date.now() < deadline, "condition timeout"); await delay(20); }
  };
  await queue.add("full", { run_id: "full-run", consumer: "full" }, { jobId: "full", attempts: 1 });
  await queue.add("ordinary", {}, { jobId: "next-channel", attempts: 1 });
  await queue.add("incremental", { run_id: "incremental-run", consumer: "incremental" }, { jobId: "incremental", attempts: 1 });
  worker = startWorker();
  await until(async () => results.includes("next-channel") && Number((await query(
    "SELECT count(*) FROM crawler.youtube_api_detail_requests WHERE status='pending'")).rows[0].count) === 2);
  assert.equal(fetches, 6);
  assert.deepEqual(errors, []);
  assert.equal((await queue.getJob("full")).attemptsMade, 0);
  await worker.close();
  worker = startWorker();
  await delay(holdMs);
  assert.equal(fetches, 6, "pending polling after restart never repeats YouTubeJS");
  assert.equal(networkTasks, 3, "pending polling never acquires another network Task");
  assert.deepEqual(results, ["next-channel"], "pending channels are neither completed nor failed");
  await dispatchVideoApiRequests({ query, withTransaction, queue: apiQueue });
  const tasks = (await query("SELECT task_id FROM crawler.youtube_api_tasks")).rows;
  assert.equal(tasks.length, 1, "full and incremental share one batch API task");
  await withTransaction(client => completeVideoApiRequests(client, tasks[0].task_id, {
    title: "API result", published_at: "2026-09-01T00:00:00Z", view_count_text: "123",
    duration_seconds: 60, privacy_status: "public", comments_disabled: true,
  }, true));
  await until(() => results.length === 3);
  assert.deepEqual(errors, []);
  assert.equal(fetches, 6);
  assert.equal(networkTasks, 3, "API evidence consumption must work even when network budget is exhausted");
  for (const id of ["full", "incremental"]) {
    const job = await queue.getJob(id);
    assert.equal(await job.getState(), "completed");
    assert.equal(job.attemptsMade, 1, "deferrals must not consume failure attempts");
    assert.equal(job.returnvalue.detail.view_count, 123);
    assert.equal(job.returnvalue.classification.content_type, "short");
  }
});
