import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents } from "bullmq";
import pg from "pg";

import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { queuesByRole } from "../src/queues.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();

function localDatabaseName(value) {
  const url = new URL(value);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
  const database = decodeURIComponent(url.pathname).replace(/^\/+/, "");
  assert.match(database, /test/i);
  return database;
}

function redisConnection(value) {
  const url = new URL(value);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
  return {
    host: url.hostname,
    port: Number(url.port),
    password: url.password || undefined,
    maxRetriesPerRequest: null,
  };
}

async function within(promise, label, timeoutMs = 20_000) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function capture(child) {
  let output = "";
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", (chunk) => { output = `${output}${chunk}`.slice(-20_000); });
  }
  return {
    output: () => output,
    waitFor: (fragment) => waitFor(() => output.includes(fragment), fragment),
  };
}

async function waitFor(check, label, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`timed out waiting for ${label}`);
}

async function stopChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await within(exited, "Worker shutdown", 10_000);
}

function workerEnvironment({ database, prefix, redis, crashAfterCommit }) {
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "migration_retry_post_commit_forbidden",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: prefix,
    WORKER_QUEUES: queuesByRole.channelCrawl,
    PROXY_SLOT_ROLE: "",
    ROTA_FIXED_PROXY_USER: "post-commit-test-worker",
    ROTA_BULLMQ_PROXY_PASSWORD: "post-commit-test-password",
    ROTA_PROXY_BASE_URL: "http://127.0.0.1:9",
    BULLMQ_LOCK_DURATION_MS: "500",
    BULLMQ_STALLED_INTERVAL_MS: "100",
    MIGRATION_RETRY_TEST_EXIT_AFTER_COMMIT: crashAfterCommit ? "true" : "false",
  };
}

test("real Worker replays a committed Retry Intent after exiting before BullMQ ACK", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  const database = localDatabaseName(databaseUrl);
  const redis = redisConnection(redisUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const prefix = `retry-post-commit-${suffix}`;
  const batchId = `retry-post-commit:${suffix}`;
  const channelId = `UCretrypostcommit${suffix}`;
  const retryIntentId = `retry-intent:${suffix}`;
  const previousRunId = `run:previous:${suffix}`;
  const jobId = `channel-recovery__${suffix}__g1`;
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.channelCrawl, { connection: redis, prefix });
  const queueEvents = new QueueEvents(queuesByRole.channelCrawl, { connection: redis, prefix });
  const loader = new URL("./support/migrationRetryIntentPostCommitLoader.mjs", import.meta.url);
  let firstWorker = null;
  let secondWorker = null;

  t.after(async () => {
    await Promise.all([stopChild(firstWorker), stopChild(secondWorker)]);
    await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close(), queueEvents.close()]);
    await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await setup.end().catch(() => {});
  });

  await setup.connect();
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  await setup.query(
    `INSERT INTO crawler.query_dispatch_batches (dispatch_batch_id,pipeline_cycle_id,status)
     VALUES ($1,$1,'running')`,
    [batchId],
  );
  const candidateId = Number((await setup.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json
     ) VALUES ($1,$1,$2,$3,'queued',1,'{}'::jsonb,'{}'::jsonb)
     RETURNING candidate_id`,
    [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
  )).rows[0].candidate_id);
  await setup.query(
    `INSERT INTO crawler.business_run_bindings (
       business_run_key,business_run_id,intent_hash,intent_json,
       identity_policy_id,identity_policy_version,identity_policy_hash,
       run_kind,channel_id,candidate_id,status,terminal_reason
     ) VALUES ($1,$2,repeat('a',64),'{}'::jsonb,'test-policy',1,
               repeat('b',64),'full',$3,$4,'terminal','post_commit_test')`,
    [`full-candidate:${candidateId}:previous`, previousRunId, channelId, candidateId],
  );
  await setup.query(
    `INSERT INTO crawler.migration_retry_intents (
       retry_intent_id,request_key,candidate_id,previous_business_run_id,
       new_business_run_id,new_business_run_key,new_job_id,dispatch_generation,
       reason,intent_hash,job_payload_json,status,dispatch_status,dispatched_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,1,'post-commit replay test',repeat('c',64),
               '{}'::jsonb,'dispatched','enqueued',now())`,
    [
      retryIntentId,
      `post-commit-request:${suffix}`,
      candidateId,
      previousRunId,
      `run:recovery:${suffix}`,
      `full-candidate:${candidateId}:recovery:${suffix}`,
      jobId,
    ],
  );
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  const job = await queue.add("channel-snapshot", {
    candidate_id: candidateId,
    retry_intent_id: retryIntentId,
    dispatch_generation: 1,
    dispatch_batch_id: batchId,
    pipeline_cycle_id: batchId,
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    crawl_mode: "full",
  }, {
    jobId,
    attempts: 3,
    removeOnComplete: false,
    removeOnFail: false,
  });

  firstWorker = spawn(process.execPath, [
    "--no-warnings",
    "--experimental-loader",
    loader.pathname,
    "src/worker.js",
  ], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ database, prefix, redis, crashAfterCommit: true }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const firstOutput = capture(firstWorker);
  await firstOutput.waitFor(`worker started queue=${queuesByRole.channelCrawl}`);
  await within(new Promise((resolve) => firstWorker.once("exit", resolve)), "post-commit exit");
  assert.equal(firstWorker.signalCode, "SIGKILL", firstOutput.output());
  const committed = (await setup.query(
    `SELECT candidate.status,candidate.snapshot_active_job_id,
            candidate.snapshot_json->>'post_commit_test_fetch_count' AS fetch_count,
            intent.status AS intent_status,intent.dispatch_status,
            intent.terminal_job_attempt
     FROM crawler.channel_candidates candidate
     JOIN crawler.migration_retry_intents intent ON intent.candidate_id=candidate.candidate_id
     WHERE candidate.candidate_id=$1`,
    [candidateId],
  )).rows[0];
  assert.deepEqual(committed, {
    status: "accepted",
    snapshot_active_job_id: null,
    fetch_count: "1",
    intent_status: "finished",
    dispatch_status: "terminal",
    terminal_job_attempt: "1",
  });

  secondWorker = spawn(process.execPath, [
    "--no-warnings",
    "--experimental-loader",
    loader.pathname,
    "src/worker.js",
  ], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ database, prefix, redis, crashAfterCommit: false }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const secondOutput = capture(secondWorker);
  await secondOutput.waitFor(`worker started queue=${queuesByRole.channelCrawl}`);
  const result = await within(job.waitUntilFinished(queueEvents), "post-commit replay completion");
  assert.deepEqual(result, {
    recovered_post_commit: true,
    retry_intent_id: retryIntentId,
    terminal_job_attempt: 1,
  });
  await secondOutput.waitFor("migration_retry_intent_post_commit_replayed");
  const replayed = (await setup.query(
    `SELECT candidate.snapshot_json->>'post_commit_test_fetch_count' AS fetch_count,
            candidate.snapshot_active_job_id,intent.status AS intent_status,
            intent.terminal_job_attempt
     FROM crawler.channel_candidates candidate
     JOIN crawler.migration_retry_intents intent ON intent.candidate_id=candidate.candidate_id
     WHERE candidate.candidate_id=$1`,
    [candidateId],
  )).rows[0];
  assert.deepEqual(replayed, {
    fetch_count: "1",
    snapshot_active_job_id: null,
    intent_status: "finished",
    terminal_job_attempt: "1",
  });
  assert.equal(Number((await queue.getJob(jobId)).attemptsStarted), 2);
});
