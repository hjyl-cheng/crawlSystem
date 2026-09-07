import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Queue } from "bullmq";
import pg from "pg";
import { BusinessRunBindingStore } from "../src/businessRunBindingStore.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";
import { FullCrawlYoutubeJsStore } from "../src/fullCrawlYoutubeJsStore.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { FullCrawlRoutingQueue } from "../src/fullCrawlCanary.js";

const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();
const integrationOptions = {
  skip: databaseUrl && redisUrl ? false : "Isolated PostgreSQL and Redis URLs are required",
  timeout: 60_000,
};

function startWorker(environment) {
  const child = fork(fileURLToPath(new URL("./support/fullCrawlRecoveryWorker.mjs", import.meta.url)), {
    execArgv: ["--experimental-loader", fileURLToPath(new URL("./support/fullCrawlRecoveryYoutubeLoader.mjs", import.meta.url))],
    env: environment,
    stdio: ["ignore", "pipe", "pipe", "ipc"],
  });
  const messages = [];
  const waiters = new Set();
  let output = "";
  let exit = null;
  child.stdout.on("data", (chunk) => { output = (output + chunk).slice(-12_000); });
  child.stderr.on("data", (chunk) => { output = (output + chunk).slice(-12_000); });
  child.on("message", (message) => {
    messages.push(message);
    for (const notify of [...waiters]) notify();
  });
  const exited = new Promise((resolve) => {
    child.once("exit", (code, signal) => {
      exit = { code, signal };
      for (const notify of [...waiters]) notify();
      resolve(exit);
    });
  });
  function waitFor(predicate, label, after = 0) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => finish(new Error(`Timed out: ${label}\n${output}\n${JSON.stringify(messages)}`)), 20_000);
      function finish(error, message) {
        clearTimeout(timer);
        waiters.delete(notify);
        if (error) reject(error);
        else resolve(message);
      }
      function notify() {
        const found = messages.slice(after).find(predicate);
        if (found) return finish(null, found);
        const failure = messages.slice(after).find((message) => ["failed", "worker_error", "shutdown_error"].includes(message.event));
        if (failure || exit) finish(new Error(`${label}: ${JSON.stringify(failure ?? exit)}\n${output}`));
      }
      waiters.add(notify);
      notify();
    });
  }
  return {
    child, messages, exited, waitFor,
    async stop() {
      if (exit) return;
      const forceTimer = setTimeout(() => child.kill("SIGKILL"), 5000);
      try {
        child.send({ event: "shutdown" });
        await exited;
      } finally {
        clearTimeout(forceTimer);
      }
    },
  };
}

async function scenario(mode, action) {
  const name = decodeURIComponent(new URL(databaseUrl).pathname).slice(1);
  assert.match(name, /_test$/i, "Only an explicitly named test database is allowed");
  const redis = new URL(redisUrl);
  const connection = {
    host: redis.hostname,
    port: Number(redis.port || 6379),
    password: redis.password || undefined,
    maxRetriesPerRequest: null,
  };
  const suffix = randomUUID().replaceAll("-", "");
  const prefix = `fullcrawl-recovery-${suffix}`;
  const channelId = `UC${suffix.slice(0, 22)}`;
  const batchId = mode === "Migration" ? `fullcrawl-youtubejs-canary-${suffix}` : `fullcrawl-recovery-${suffix}`;
  const runId = `run:fullcrawl-recovery:${suffix}`;
  const businessRunKey = `fullcrawl-recovery:${suffix}`;
  const videoPrefix = suffix.slice(0, 8);
  const jobId = `channel-snapshot__${batchId}__${channelId}__g1`;
  const pool = new pg.Pool({
    connectionString: databaseUrl,
    max: 4,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const queues = ["youtube-channel-crawl", "youtube-finalize", "youtube-discover-page"]
    .map((queueName) => new (queueName === "youtube-channel-crawl" ? FullCrawlRoutingQueue : Queue)(queueName, { connection, prefix }));
  const [queue, finalizeQueue] = queues;
  const workers = [];
  async function withTransaction(operation) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await operation(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
  try {
    const actual = await pool.query("SELECT current_database() AS name");
    assert.equal(actual.rows[0].name, name);
    await pool.query(crawlerRuntimeSchema(await readFile(new URL("../src/schema.sql", import.meta.url), "utf8")));
    await pool.query(
      `INSERT INTO crawler.query_dispatch_batches
       (dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at)
       VALUES ($1,$1,'validation_closed',now())`, [batchId],
    );
    const inserted = await pool.query(
      `INSERT INTO crawler.channel_candidates
       (dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,snapshot_dispatch_generation)
       VALUES ($1,$1,$2,$3,'queued',1) RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidateId = Number(inserted.rows[0].candidate_id);
    await new BusinessRunBindingStore({ withTransaction }).resolve({
      businessRunKey, explicitBusinessRunId: runId, requestedStatus: "reserved",
      runKind: "full", channelId, candidateId,
      policy: { id: "fullcrawl-test", version: 1, hash: "sha256:fullcrawl-test" },
      intent: { job_name: "channel-snapshot", crawl_mode: "full", fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT },
    });
    const job = await queue.add("channel-snapshot", {
      channel_id: channelId, candidate_id: candidateId, run_id: runId,
      business_run_key: businessRunKey, dispatch_batch_id: batchId, dispatch_generation: 1,
      pipeline_cycle_id: batchId, crawl_mode: "full", query_id: null,
      fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
      reject_if_no_recent_content: mode === "Migration",
    }, { jobId, attempts: 3, removeOnComplete: false, removeOnFail: false });
    const ordinaryProbe = new Queue("youtube-channel-crawl", { connection, prefix });
    try {
      const ordinaryJob = await ordinaryProbe.getJob(job.id);
      assert.equal(Boolean(ordinaryJob), mode !== "Migration");
      assert.equal(Boolean(await queue.canaryQueue.getJob(job.id)), mode === "Migration");
    } finally {
      await ordinaryProbe.close();
    }
    const environment = {
      PATH: process.env.PATH,
      DATABASE_URL: databaseUrl,
      EXPECTED_CRAWLER_DATABASE: name,
      FORBIDDEN_CRAWLER_DATABASE: "not_the_fullcrawl_test_database",
      SKIP_SCHEMA_MIGRATION: "true",
      REDIS_HOST: connection.host,
      REDIS_PORT: String(connection.port),
      REDIS_PASSWORD: connection.password ?? "",
      BULLMQ_PREFIX: prefix,
      FULL_CRAWL_CANARY_WORKER: String(mode === "Migration"),
      YOUTUBE_CHANNEL_CONTENT_LIMIT: "10",
      FULL_CRAWL_TEST_CHANNEL_ID: channelId,
      FULL_CRAWL_TEST_VIDEO_PREFIX: videoPrefix,
      FULL_CRAWL_TEST_PUBLISHED_AT: new Date(Date.now() - 86400_000).toISOString(),
    };
    const store = new FullCrawlYoutubeJsStore({ query: pool.query.bind(pool), withTransaction });
    await action({
      job, queue, finalizeQueue, store, runId, videoPrefix,
      start(extra = {}) {
        const worker = startWorker({ ...environment, ...extra });
        workers.push(worker);
        return worker;
      },
    });
  } finally {
    await Promise.all(workers.map((worker) => worker.stop()));
    for (const ownedQueue of queues) {
      try {
        await ownedQueue.obliterate({ force: true });
        if (ownedQueue.canaryQueue) await ownedQueue.canaryQueue.obliterate({ force: true });
      }
      finally { await ownedQueue.close(); }
    }
    await pool.end();
  }
}

for (const mode of ["Query", "Migration"]) {
  test(`${mode} Full Crawl resumes the same Run after SIGKILL without refetching committed videos`, integrationOptions, async () => {
    await scenario(mode, async ({ job, queue, finalizeQueue, store, runId, videoPrefix, start }) => {
      const crashed = start({ FULL_CRAWL_TEST_BLOCK_VIDEO_ID: `${videoPrefix}5` });
      await crashed.waitFor((message) => message.event === "youtube_request" && message.videoId === `${videoPrefix}5`, "fifth video request");
      const interruptedJob = await queue.getJob(job.id);
      const checkpoint = await store.restore(interruptedJob);
      assert.equal(checkpoint.phase, "detail");
      assert.equal(checkpoint.candidates.filter((candidate) => candidate.detail_status === "done").length, 4);
      assert.equal(checkpoint.identity.runId, runId);
      crashed.child.kill("SIGKILL");
      assert.equal((await crashed.exited).signal, "SIGKILL");

      const restarted = start();
      const completed = await restarted.waitFor((message) => message.event === "completed", "stalled job recovery");
      assert.ok(restarted.messages.some((message) => message.event === "stalled" && message.jobId === job.id));
      assert.ok(completed.attempt > 1);
      assert.equal(completed.result.run_id, runId);
      assert.equal(completed.result.resumed, true);
      assert.equal(completed.result.candidate_count, 10);
      assert.equal(completed.result.detail_processed, 6);
      assert.equal(completed.result.migration_activity_gate.decision, mode === "Migration" ? "passed" : "not_required");
      assert.deepEqual(
        restarted.messages.filter((message) => message.event === "youtube_request")
          .map((message) => `${message.surface}:${message.videoId}`),
        [5, 6, 7, 8, 9, 10].map((position) => `detail:${videoPrefix}${position}`),
      );
      const finished = await store.restore(await queue.getJob(job.id));
      assert.equal(finished.phase, "handoff");
      assert.equal(finished.candidates.filter((candidate) => candidate.detail_status === "done").length, 10);
      assert.equal(finished.uploads.receipt.target_hash, checkpoint.uploads.receipt.target_hash);
      assert.equal(finished.channel.ready_for_agent, true);
      const finalizations = await finalizeQueue.getJobs(["waiting", "active", "completed", "delayed", "failed"]);
      assert.equal(finalizations.length, 1);
      assert.equal(finalizations[0].data.run_id, runId);
      await restarted.stop();
      assert.deepEqual(await restarted.exited, { code: 0, signal: null });
      assert.ok(restarted.messages.some((message) => message.event === "closed"));
    });
  });

  test(`${mode} Full Crawl replays committed handoff after SIGKILL and duplicate delivery without fetching or duplicate Finalize jobs`, integrationOptions, async () => {
    await scenario(mode, async ({ job, queue, finalizeQueue, store, runId, start }) => {
      const crashed = start({ FULL_CRAWL_TEST_BLOCK_AFTER_EXECUTION: "true" });
      const unacknowledged = await crashed.waitFor((message) => message.event === "before_queue_ack", "committed handoff before queue acknowledgement");
      assert.equal(unacknowledged.result.run_id, runId);
      assert.equal(unacknowledged.result.detail_processed, 10);
      assert.equal((await store.restore(await queue.getJob(job.id))).phase, "handoff");
      const originalFinalizations = await finalizeQueue.getJobs(["waiting", "active", "completed", "delayed", "failed"]);
      assert.equal(originalFinalizations.length, 1);
      const originalFinalizeId = originalFinalizations[0].id;
      const originalRevision = originalFinalizations[0].data.source_revision;

      const duplicateActive = await queue.add(job.name, job.data, { jobId: job.id });
      assert.equal(duplicateActive.id, job.id);
      assert.equal(await duplicateActive.getState(), "active");
      assert.equal(await queue.getWaitingCount(), 0);
      crashed.child.kill("SIGKILL");
      assert.equal((await crashed.exited).signal, "SIGKILL");

      const restarted = start();
      const recovered = await restarted.waitFor((message) => message.event === "completed", "handoff-only recovery");
      assert.equal(recovered.result.run_id, runId);
      assert.deepEqual(recovered.result.executed_phases, ["handoff"]);
      assert.equal(recovered.result.detail_processed, 0);
      assert.equal(recovered.result.candidate_count, 10);
      assert.ok(restarted.messages.some((message) => message.event === "stalled"));
      assert.equal(restarted.messages.filter((message) => message.event === "youtube_request").length, 0);

      const duplicateCompleted = await queue.add(job.name, job.data, { jobId: job.id });
      assert.equal(await duplicateCompleted.getState(), "completed");
      assert.equal(await queue.getCompletedCount(), 1);
      const after = restarted.messages.length;
      const replay = await queue.getJob(job.id);
      await replay.retry("completed");
      const replayed = await restarted.waitFor((message) => message.event === "completed", "explicit delivery replay", after);
      assert.equal(replayed.result.run_id, runId);
      assert.deepEqual(replayed.result.executed_phases, ["handoff"]);
      assert.equal(replayed.result.detail_processed, 0);
      assert.equal(restarted.messages.filter((message) => message.event === "youtube_request").length, 0);
      const finalizations = await finalizeQueue.getJobs(["waiting", "active", "completed", "delayed", "failed"]);
      assert.equal(finalizations.length, 1);
      assert.equal(finalizations[0].id, originalFinalizeId);
      assert.equal(finalizations[0].data.source_revision, originalRevision);
      assert.equal(finalizations[0].data.run_id, runId);
      assert.equal((await store.restore(await queue.getJob(job.id))).candidates.length, 10);
      await restarted.stop();
      assert.deepEqual(await restarted.exited, { code: 0, signal: null });
    });
  });
}
