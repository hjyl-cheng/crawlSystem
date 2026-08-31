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
const prefix = `migration-content-detail-fence-${randomUUID()}`;

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

function localRedisConfiguration(value) {
  const url = new URL(value);
  assert.equal(url.protocol, "redis:");
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
  assert.equal(url.username, "");
  assert.equal(decodeURIComponent(url.pathname).replace(/^\/+/, "") || "0", "0");
  return {
    host: url.hostname,
    port: Number(url.port || 6379),
    password: url.password ? decodeURIComponent(url.password) : undefined,
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

async function transaction(client, action) {
  await client.query("BEGIN");
  try {
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
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

function captureChildOutput(child) {
  let output = "";
  let exited = false;
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", (chunk) => { output = `${output}${chunk}`.slice(-20_000); });
  }
  child.once("exit", () => { exited = true; });
  return {
    output: () => output,
    waitFor: (fragment) => waitFor(() => {
      if (output.includes(fragment)) return true;
      if (exited) throw new Error(`Worker exited before ${fragment}\n${output}`);
      return false;
    }, `Worker output ${fragment}`),
  };
}

async function stopChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await within(exited, "Worker shutdown", 10_000);
}

function workerEnvironment({
  skipLockRenewal = false,
  queueName = queuesByRole.contentDetail,
} = {}) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "content_detail_fence_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: prefix,
    WORKER_QUEUES: queueName,
    PROXY_SLOT_ROLE: "",
    ROTA_PROXY_CONTROL_URL: "",
    ROTA_PROXY_CONTROL_TOKEN: "",
    ROTA_FIXED_PROXY_USER: "content-detail-test",
    ROTA_BULLMQ_PROXY_PASSWORD: "content-detail-test-password",
    ROTA_PROXY_BASE_URL: "http://127.0.0.1:9",
    S3_ENDPOINT: "",
    S3_ACCESS_KEY: "",
    S3_SECRET_KEY: "",
    YOUTUBEJS_EXTRACTOR_MODE: "disabled",
    YOUTUBE_DATA_API_FALLBACK_MODE: "disabled",
    YOUTUBE_CHANNEL_INLINE_DETAILS: "true",
    BULLMQ_LOCK_DURATION_MS: "500",
    BULLMQ_STALLED_INTERVAL_MS: "500",
    BULLMQ_SKIP_LOCK_RENEWAL: skipLockRenewal ? "true" : "false",
  };
}

async function seedInlineRetryScenario(client) {
  const candidateId = 482;
  const channelId = "UC0NoarYHkSxek05QDqhtoYw";
  const runId = "content-detail-inline-real-worker-run";
  const pipelineCycleId = "content-detail-inline-real-worker-cycle";
  const jobId = "content-detail-inline-real-worker-job";
  const businessRunKey = "content-detail-inline-real-worker-business-run";
  const channelUrl = `https://www.youtube.com/channel/${channelId}`;
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'running',1,1)`,
    [pipelineCycleId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
       snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES (
       $1,$2,$2,$3,$4,'accepted',1,$5,1,'{}'::jsonb,
       '{"source":"legacy_results_db"}'::jsonb,now(),now()
     )`,
    [candidateId, pipelineCycleId, channelId, channelUrl, jobId],
  );
  const migrationIntentId = Number((await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,channel_id,
       source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id,
       dispatch_attempts,last_dispatch_at
     ) VALUES (
       'content-detail-inline-real-worker',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$1,$2,
       '{}'::jsonb,repeat('e',64),$1,$3,1,now()
     ) RETURNING migration_intent_id`,
    [candidateId, channelId, pipelineCycleId],
  )).rows[0].migration_intent_id);
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Inline retry Detail Worker',2000,'active',true,'pending',$3,$4,$3)`,
      [channelId, channelUrl, runId, candidateId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES ($1,$2,$3,'waiting_detail','full','queued',1,now(),$4::jsonb)`,
      [
        runId,
        channelId,
        candidateId,
        JSON.stringify({
          dispatch_batch_id: pipelineCycleId,
          pipeline_cycle_id: pipelineCycleId,
          content_max_age_days: 90,
        }),
      ],
    );
  });
  await client.query(
    `INSERT INTO crawler.business_run_bindings (
       business_run_key,business_run_id,intent_hash,intent_json,
       identity_policy_id,identity_policy_version,identity_policy_hash,
       run_kind,channel_id,candidate_id,status
     ) VALUES (
       $1,$2,'sha256:inline-retry','{}'::jsonb,
       'channel-inline-retry-v1',1,'sha256:inline-retry-policy',
       'full',$3,$4,'reserved'
     )`,
    [businessRunKey, runId, channelId, candidateId],
  );
  const contentCandidateId = Number((await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,title,source_url,
       content_type,type_status,type_source,detail_status,api_status,result_json
     ) VALUES (
       $1,$2,'old-inline-video',1,'Old inline video',
       'https://www.youtube.com/watch?v=old-inline-video',
       NULL,'unresolved',NULL,'queued','not_needed',$3::jsonb
     ) RETURNING candidate_id`,
    [
      runId,
      channelId,
      JSON.stringify({
        flat: {
          video_id: "old-inline-video",
          title: "Old inline video",
          published_at: "2020-01-01T00:00:00.000Z",
          published_at_status: "exact",
          published_at_precision: "second",
          published_at_source: "yt_dlp_flat_timestamp",
        },
      }),
    ],
  )).rows[0].candidate_id);
  const systemRetryId = Number((await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES (
       $1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'retrying'
     ) RETURNING system_retry_id`,
    [migrationIntentId, candidateId, pipelineCycleId, jobId],
  )).rows[0].system_retry_id);
  return {
    businessRunKey,
    candidateId,
    channelId,
    channelUrl,
    contentCandidateId,
    jobId,
    migrationIntentId,
    pipelineCycleId,
    runId,
    systemRetryId,
  };
}

async function seedScenario(client) {
  const channelId = "UC0NoarYHkSxek05QDqhtoYw";
  const runId = "content-detail-real-worker-run";
  const pipelineCycleId = "content-detail-real-worker-cycle";
  await client.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
       agent_status,latest_run_id
     ) VALUES ($1,$2,'Content Detail Worker Fence',2000,'active',true,'pending',$3)`,
    [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,status,crawl_mode,detail_status,expected_content_count,
       started_at,result_json
     ) VALUES ($1,$2,'waiting_detail','full','queued',1,'2026-08-30T00:00:00Z',$3::jsonb)`,
    [runId, channelId, JSON.stringify({ pipeline_cycle_id: pipelineCycleId })],
  );
  const candidateId = Number((await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,title,source_url,
       content_type,type_status,type_source,detail_status,api_status,result_json
     ) VALUES (
       $1,$2,'old-video',1,'Old video','https://www.youtube.com/watch?v=old-video',
       NULL,'unresolved',NULL,'queued','not_needed',$3::jsonb
     ) RETURNING candidate_id`,
    [
      runId,
      channelId,
      JSON.stringify({
        flat: {
          video_id: "old-video",
          title: "Old video",
          published_at: "2020-01-01T00:00:00.000Z",
          published_at_status: "exact",
          published_at_precision: "second",
          published_at_source: "yt_dlp_flat_timestamp",
        },
      }),
    ],
  )).rows[0].candidate_id);
  return { channelId, runId, pipelineCycleId, candidateId };
}

test("a real stalled Content Detail Worker cannot commit after attemptsStarted takeover", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const locker = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.contentDetail, { connection, prefix });
  const finalizeQueue = new Queue(queuesByRole.finalize, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.contentDetail, { connection, prefix });
  let firstWorker = null;
  let takeoverWorker = null;
  let schemaInitialized = false;
  let redisReady = false;
  let settingsLocked = false;
  t.after(async () => {
    if (settingsLocked) await locker.query("ROLLBACK").catch(() => {});
    await Promise.all([
      stopChild(firstWorker).catch(() => {}),
      stopChild(takeoverWorker).catch(() => {}),
    ]);
    if (redisReady) {
      await Promise.all([
        queue.obliterate({ force: true }).catch(() => {}),
        finalizeQueue.obliterate({ force: true }).catch(() => {}),
      ]);
    }
    await Promise.all([
      queue.close().catch(() => {}),
      finalizeQueue.close().catch(() => {}),
      queueEvents.close().catch(() => {}),
    ]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([
      setup.end().catch(() => {}),
      locker.end().catch(() => {}),
      observer.end().catch(() => {}),
    ]);
  });

  await Promise.all([setup.connect(), locker.connect(), observer.connect()]);
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  await Promise.all([
    queue.obliterate({ force: true }),
    finalizeQueue.obliterate({ force: true }),
  ]);
  await Promise.all([
    queue.waitUntilReady(),
    finalizeQueue.waitUntilReady(),
    queueEvents.waitUntilReady(),
  ]);
  redisReady = true;
  const scenario = await seedScenario(setup);

  await locker.query("BEGIN");
  await locker.query("LOCK TABLE crawler.settings IN ACCESS EXCLUSIVE MODE");
  settingsLocked = true;
  const job = await queue.add("content-detail-batch", {
    channel_id: scenario.channelId,
    run_id: scenario.runId,
    pipeline_cycle_id: scenario.pipelineCycleId,
    content_max_age_days: 90,
    api_fallback_mode: "disabled",
  }, {
    jobId: `content-detail__${scenario.runId}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

  firstWorker = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ skipLockRenewal: true }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const firstOutput = captureChildOutput(firstWorker);
  await firstOutput.waitFor(`worker started queue=${queuesByRole.contentDetail}`);
  await waitFor(async () => Number((await observer.query(
    "SELECT detail_active_job_attempt FROM crawler.channel_runs WHERE run_id=$1",
    [scenario.runId],
  )).rows[0]?.detail_active_job_attempt) === 1, "Content Detail attempt 1 claim");

  takeoverWorker = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const takeoverOutput = captureChildOutput(takeoverWorker);
  await takeoverOutput.waitFor(`worker started queue=${queuesByRole.contentDetail}`);
  await waitFor(async () => Number((await observer.query(
    "SELECT detail_active_job_attempt FROM crawler.channel_runs WHERE run_id=$1",
    [scenario.runId],
  )).rows[0]?.detail_active_job_attempt) === 2, "Content Detail attempt 2 takeover", 30_000);

  await locker.query("COMMIT");
  settingsLocked = false;
  const result = await within(job.waitUntilFinished(queueEvents), "takeover Detail completion", 30_000);
  assert.equal(result.ok, true);
  const state = (await observer.query(
    `SELECT run.detail_active_job_attempt,run.detail_status AS run_detail_status,
            run.status AS run_status,candidate.attempts,
            candidate.detail_status AS candidate_detail_status,candidate.disposition,
            candidate.result_json#>>'{scope,reason}' AS scope_reason
     FROM crawler.channel_runs run
     JOIN crawler.content_candidates candidate ON candidate.candidate_id=$2
     WHERE run.run_id=$1`,
    [scenario.runId, scenario.candidateId],
  )).rows[0];
  assert.deepEqual(state, {
    detail_active_job_attempt: "2",
    run_detail_status: "done",
    run_status: "waiting_agent",
    attempts: 1,
    candidate_detail_status: "done",
    disposition: "terminal_excluded",
    scope_reason: "older_than_max_age",
  });
  const completed = await queue.getJob(job.id);
  assert.equal(completed.attemptsStarted, 2);
  assert.equal(completed.attemptsMade, 1);

  await Promise.all([stopChild(firstWorker), stopChild(takeoverWorker)]);
  assert.equal(firstWorker.exitCode, 0, firstOutput.output());
  assert.equal(takeoverWorker.exitCode, 0, takeoverOutput.output());
});

test("a real standalone Detail Job remains authorized while its parent Job is retrying", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.contentDetail, { connection, prefix });
  const finalizeQueue = new Queue(queuesByRole.finalize, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.contentDetail, { connection, prefix });
  let workerProcess = null;
  let schemaInitialized = false;
  let redisReady = false;
  t.after(async () => {
    await stopChild(workerProcess).catch(() => {});
    if (redisReady) {
      await Promise.all([
        queue.obliterate({ force: true }).catch(() => {}),
        finalizeQueue.obliterate({ force: true }).catch(() => {}),
      ]);
    }
    await Promise.all([
      queue.close().catch(() => {}),
      finalizeQueue.close().catch(() => {}),
      queueEvents.close().catch(() => {}),
    ]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([setup.end().catch(() => {}), observer.end().catch(() => {})]);
  });

  await Promise.all([setup.connect(), observer.connect()]);
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  await Promise.all([
    queue.obliterate({ force: true }),
    finalizeQueue.obliterate({ force: true }),
  ]);
  await Promise.all([
    queue.waitUntilReady(),
    finalizeQueue.waitUntilReady(),
    queueEvents.waitUntilReady(),
  ]);
  redisReady = true;
  const scenario = await seedInlineRetryScenario(setup);
  await setup.query(
    `UPDATE crawler.channel_runs
     SET result_json=result_json || jsonb_build_object('job_id',$2::text)
     WHERE run_id=$1`,
    [scenario.runId, scenario.jobId],
  );
  const queued = await queue.add("content-detail-batch", {
    channel_id: scenario.channelId,
    run_id: scenario.runId,
    pipeline_cycle_id: scenario.pipelineCycleId,
    content_max_age_days: 90,
    api_fallback_mode: "disabled",
  }, {
    jobId: `content-detail__${scenario.runId}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${queuesByRole.contentDetail}`);
  const result = await within(
    queued.waitUntilFinished(queueEvents),
    "standalone Detail completion during parent retry",
    30_000,
  );
  assert.equal(result.ok, true);
  assert.equal(result.skipped, undefined);

  const state = (await observer.query(
    `SELECT candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            retry.status AS retry_status,retry.resolution,
            content.attempts AS content_attempts,
            content.detail_status AS content_detail_status,
            content.result_json#>>'{scope,reason}' AS scope_reason,
            run.detail_status AS run_detail_status
     FROM crawler.channel_candidates candidate
     JOIN crawler.migration_system_retry_items retry ON retry.system_retry_id=$2
     JOIN crawler.channel_runs run ON run.run_id=$3
     JOIN crawler.content_candidates content ON content.candidate_id=$4
     WHERE candidate.candidate_id=$1`,
    [
      scenario.candidateId,
      scenario.systemRetryId,
      scenario.runId,
      scenario.contentCandidateId,
    ],
  )).rows[0];
  assert.deepEqual(state, {
    snapshot_active_job_id: scenario.jobId,
    snapshot_active_job_attempt: 1,
    retry_status: "retrying",
    resolution: null,
    content_attempts: 1,
    content_detail_status: "done",
    scope_reason: "older_than_max_age",
    run_detail_status: "done",
  });

  await stopChild(workerProcess);
  assert.equal(workerProcess.exitCode, 0, workerOutput.output());
});

test("a real Channel Worker resumes inline Detail on the same Job after a retrying system failure", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.channelCrawl, { connection, prefix });
  const finalizeQueue = new Queue(queuesByRole.finalize, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.channelCrawl, { connection, prefix });
  let workerProcess = null;
  let schemaInitialized = false;
  let redisReady = false;
  t.after(async () => {
    await stopChild(workerProcess).catch(() => {});
    if (redisReady) {
      await Promise.all([
        queue.obliterate({ force: true }).catch(() => {}),
        finalizeQueue.obliterate({ force: true }).catch(() => {}),
      ]);
    }
    await Promise.all([
      queue.close().catch(() => {}),
      finalizeQueue.close().catch(() => {}),
      queueEvents.close().catch(() => {}),
    ]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([setup.end().catch(() => {}), observer.end().catch(() => {})]);
  });

  await Promise.all([setup.connect(), observer.connect()]);
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  await Promise.all([
    queue.obliterate({ force: true }),
    finalizeQueue.obliterate({ force: true }),
  ]);
  await Promise.all([
    queue.waitUntilReady(),
    finalizeQueue.waitUntilReady(),
    queueEvents.waitUntilReady(),
  ]);
  redisReady = true;
  const scenario = await seedInlineRetryScenario(setup);
  const queued = await queue.add("channel-snapshot", {
    candidate_id: scenario.candidateId,
    migration_intent_id: scenario.migrationIntentId,
    dispatch_generation: 1,
    dispatch_batch_id: scenario.pipelineCycleId,
    pipeline_cycle_id: scenario.pipelineCycleId,
    channel_id: scenario.channelId,
    channel_url: scenario.channelUrl,
    run_id: scenario.runId,
    business_run_key: scenario.businessRunKey,
    crawl_mode: "full",
  }, {
    jobId: scenario.jobId,
    attempts: 2,
    removeOnComplete: false,
    removeOnFail: false,
  });
  const redis = await queue.client;
  await redis.hset(`${queue.qualifiedName}:${queued.id}`, "atm", "1", "ats", "1");

  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ queueName: queuesByRole.channelCrawl }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${queuesByRole.channelCrawl}`);
  const result = await within(
    queued.waitUntilFinished(queueEvents),
    "inline Detail retry completion",
    30_000,
  );
  assert.equal(result.ok, true);
  assert.equal(result.resumed, true);

  const state = (await observer.query(
    `SELECT candidate.status AS candidate_status,
            candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            retry.status AS retry_status,retry.resolution,
            content.attempts AS content_attempts,
            content.detail_status AS content_detail_status,
            content.disposition AS content_disposition,
            content.result_json#>>'{scope,reason}' AS scope_reason,
            run.detail_status AS run_detail_status
     FROM crawler.channel_candidates candidate
     JOIN crawler.migration_system_retry_items retry ON retry.system_retry_id=$2
     JOIN crawler.channel_runs run ON run.run_id=$3
     JOIN crawler.content_candidates content ON content.candidate_id=$4
     WHERE candidate.candidate_id=$1`,
    [
      scenario.candidateId,
      scenario.systemRetryId,
      scenario.runId,
      scenario.contentCandidateId,
    ],
  )).rows[0];
  assert.deepEqual(state, {
    candidate_status: "accepted",
    snapshot_active_job_id: null,
    snapshot_active_job_attempt: null,
    retry_status: "resolved",
    resolution: "job_completed",
    content_attempts: 1,
    content_detail_status: "done",
    content_disposition: "terminal_excluded",
    scope_reason: "older_than_max_age",
    run_detail_status: "done",
  });
  const completed = await queue.getJob(queued.id);
  assert.equal(await completed.getState(), "completed");
  assert.equal(completed.attemptsStarted, 2);
  assert.equal(completed.attemptsMade, 2);

  await stopChild(workerProcess);
  assert.equal(workerProcess.exitCode, 0, workerOutput.output());
});
