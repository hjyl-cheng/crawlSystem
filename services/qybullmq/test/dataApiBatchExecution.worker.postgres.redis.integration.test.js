import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { queuesByRole } from "../src/queues.js";
import { STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID } from "../src/youtubeDataApiEvidence.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();
const prefix = `migration-data-api-fence-${randomUUID()}`;
const batchFinalizeLock = "data-api-batch-worker-finalize-test";

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

function workerEnvironment({ skipLockRenewal = false } = {}) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "data_api_fence_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: prefix,
    WORKER_QUEUES: queuesByRole.dataApiBatch,
    PROXY_SLOT_ROLE: "",
    ROTA_PROXY_CONTROL_URL: "",
    ROTA_PROXY_CONTROL_TOKEN: "",
    ROTA_FIXED_PROXY_USER: "",
    ROTA_BULLMQ_PROXY_PASSWORD: "",
    S3_ENDPOINT: "",
    S3_ACCESS_KEY: "",
    S3_SECRET_KEY: "",
    BULLMQ_LOCK_DURATION_MS: "500",
    BULLMQ_STALLED_INTERVAL_MS: "500",
    BULLMQ_SKIP_LOCK_RENEWAL: skipLockRenewal ? "true" : "false",
  };
}

async function seedRecovery(client, suffix, { resolved = false } = {}) {
  const batchId = `data-api-worker-${suffix}`;
  const channelCandidateId = suffix === "active" ? 482 : 483;
  const channelId = suffix === "active"
    ? "UC0NoarYHkSxek05QDqhtoYw"
    : "UC0NoarYHkSxek05QDqhtoYx";
  const runId = `data-api-worker-${suffix}-run`;
  const outsideRunId = `data-api-worker-${suffix}-outside-run`;
  const videoId = `data-api-worker-${suffix}-video`;
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'completed',1,1)`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$2,$2,$3,$4,'accepted',2,'{}'::jsonb,
               '{"source":"legacy_results_db"}'::jsonb,now(),now())`,
    [channelCandidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  const migrationIntentId = Number((await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,channel_id,
       source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id,
       dispatch_attempts,last_dispatch_at
     ) VALUES (
       'legacy-results-v1',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$1,$2,
       '{}'::jsonb,repeat('b',64),$1,$3,2,now()
     ) RETURNING migration_intent_id`,
    [channelCandidateId, channelId, batchId],
  )).rows[0].migration_intent_id);
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Data API Recovery',2000,'active',true,'pending',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId, channelCandidateId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES
         ($1,$3,$4,'waiting_detail','full','api_pending',1,now(),$5::jsonb),
         ($2,$3,NULL,'waiting_detail','full','api_pending',1,now(),$6::jsonb)`,
      [
        runId,
        outsideRunId,
        channelId,
        channelCandidateId,
        JSON.stringify({ dispatch_batch_id: batchId, pipeline_cycle_id: batchId }),
        JSON.stringify({ dispatch_batch_id: `${batchId}-ordinary`, pipeline_cycle_id: `${batchId}-ordinary` }),
      ],
    );
  });
  const resultJson = JSON.stringify({
    flat: {
      video_id: videoId,
      content_type: "video",
      type_source: "uploads_playlist",
    },
    detail: {
      access_status: "unknown",
      content_type_signals: {
        source: "youtube_watch_player",
        canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
        is_shorts_eligible: false,
        is_live_content: false,
        is_live: false,
        is_upcoming: false,
        is_live_now: false,
      },
    },
    access: { access_status: "unknown", access_status_source: null },
  });
  const contentRows = await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,content_type,type_status,type_source,
       detail_status,api_status,missing_fields,result_json
     ) VALUES
       ($1,$3,$4,1,'video','resolved','uploads_playlist','api_pending','queued',
        ARRAY['access_status']::text[],$5::jsonb),
       ($2,$3,$4,1,'video','resolved','uploads_playlist','api_pending','pending',
        ARRAY['access_status']::text[],$5::jsonb)
     RETURNING candidate_id,run_id`,
    [runId, outsideRunId, channelId, videoId, resultJson],
  );
  const recoveryCandidateId = Number(
    contentRows.rows.find(({ run_id: value }) => value === runId).candidate_id,
  );
  const outsideCandidateId = Number(
    contentRows.rows.find(({ run_id: value }) => value === outsideRunId).candidate_id,
  );
  const taskEvidence = {
    title: "Recovered Data API video",
    description: "Stored API detail evidence",
    description_status: "exact",
    published_at: "2026-08-17T10:00:00.000Z",
    published_at_precision: "second",
    duration_seconds: 120,
    view_count_text: "100",
    like_count: 5,
    comment_count: 0,
    comments_disabled: false,
    privacy_status: "public",
    source: "youtube_data_api_videos_list",
    api_verification: { videos_list: { returned: true } },
    stored_data_api_evidence_recovery: {
      operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
      run_id: runId,
      candidate_ids: [recoveryCandidateId],
    },
  };
  const taskId = Number((await client.query(
    `INSERT INTO crawler.youtube_api_tasks (
       source_content_id,status,missing_fields,candidate_ids,result_json
     ) VALUES ($1,'queued',ARRAY['access_status']::text[],$2::bigint[],$3::jsonb)
     RETURNING task_id`,
    [videoId, [recoveryCandidateId, outsideCandidateId], JSON.stringify(taskEvidence)],
  )).rows[0].task_id);
  const systemRetryId = Number((await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status,
       retry_dispatch_generation,recovery_run_id,dispatched_at,resolution,resolved_at
     ) VALUES (
       $1,$2,$3,1,'failed-channel-job',1,'LEASE_CONFLICT','lease','{}'::jsonb,
       $4,2,$5,now(),$6,CASE WHEN $4='resolved' THEN now() ELSE NULL END
     ) RETURNING system_retry_id`,
    [
      migrationIntentId,
      channelCandidateId,
      batchId,
      resolved ? "resolved" : "dispatched",
      runId,
      resolved ? "recovery_finalized" : null,
    ],
  )).rows[0].system_retry_id);
  await client.query(
    `INSERT INTO crawler.youtube_api_batches (
       batch_id,status,task_ids,video_ids,result_json
     ) VALUES ($1,'queued',ARRAY[$2]::bigint[],ARRAY[$3]::text[],$4::jsonb)`,
    [
      batchId,
      taskId,
      videoId,
      JSON.stringify({
        dispatch_intent: {
          pipeline_cycle_id: batchId,
          migration_system_retry_ids: [systemRetryId],
          recovery_run_ids: [runId],
        },
      }),
    ],
  );
  return {
    batchId,
    runId,
    videoId,
    taskId,
    systemRetryId,
    recoveryCandidateId,
    outsideCandidateId,
  };
}

function jobData(scenario) {
  return {
    batch_id: scenario.batchId,
    task_ids: [scenario.taskId],
    video_ids: [scenario.videoId],
    pipeline_cycle_id: scenario.batchId,
    migration_system_retry_ids: [scenario.systemRetryId],
    recovery_run_ids: [scenario.runId],
    stored_evidence_replay: {
      operation_id: STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
      run_id: scenario.runId,
      expected_candidate_count: 1,
      task_ids: [scenario.taskId],
    },
  };
}

test("a real stalled Data API Worker cannot commit after attemptsStarted takeover", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const locker = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const finalizerLocker = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.dataApiBatch, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.dataApiBatch, { connection, prefix });
  let firstWorker = null;
  let takeoverWorker = null;
  let schemaInitialized = false;
  let redisReady = false;
  let settingsLocked = false;
  let finalizerLocked = false;
  t.after(async () => {
    if (settingsLocked) await locker.query("ROLLBACK").catch(() => {});
    if (finalizerLocked) {
      await finalizerLocker.query(
        "SELECT pg_advisory_unlock(hashtext($1))",
        [batchFinalizeLock],
      ).catch(() => {});
    }
    await Promise.all([
      stopChild(firstWorker).catch(() => {}),
      stopChild(takeoverWorker).catch(() => {}),
    ]);
    if (redisReady) await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close().catch(() => {}), queueEvents.close().catch(() => {})]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([
      setup.end().catch(() => {}),
      locker.end().catch(() => {}),
      finalizerLocker.end().catch(() => {}),
      observer.end().catch(() => {}),
    ]);
  });

  await Promise.all([
    setup.connect(),
    locker.connect(),
    finalizerLocker.connect(),
    observer.connect(),
  ]);
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  await setup.query(
    `CREATE FUNCTION crawler.block_data_api_batch_finalize_for_test()
     RETURNS trigger LANGUAGE plpgsql AS $$
     BEGIN
       IF OLD.status='running' AND NEW.status='done' THEN
         PERFORM pg_advisory_xact_lock(hashtext('${batchFinalizeLock}'));
       END IF;
       RETURN NEW;
     END
     $$`,
  );
  await setup.query(
    `CREATE TRIGGER block_data_api_batch_finalize_for_test
     BEFORE UPDATE ON crawler.youtube_api_batches
     FOR EACH ROW EXECUTE FUNCTION crawler.block_data_api_batch_finalize_for_test()`,
  );
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  redisReady = true;
  const scenario = await seedRecovery(setup, "active");

  await locker.query("BEGIN");
  await locker.query("LOCK TABLE crawler.settings IN ACCESS EXCLUSIVE MODE");
  settingsLocked = true;
  const job = await queue.add("youtube-data-api-batch", jobData(scenario), {
    jobId: `youtube-data-api__${scenario.batchId}`,
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
  await firstOutput.waitFor(`worker started queue=${queuesByRole.dataApiBatch}`);
  await waitFor(async () => {
    const row = (await observer.query(
      `SELECT active_job_attempt FROM crawler.youtube_api_batches WHERE batch_id=$1`,
      [scenario.batchId],
    )).rows[0];
    return Number(row?.active_job_attempt) === 1;
  }, "Data API attempt 1 claim");

  takeoverWorker = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const takeoverOutput = captureChildOutput(takeoverWorker);
  await takeoverOutput.waitFor(`worker started queue=${queuesByRole.dataApiBatch}`);
  await waitFor(async () => {
    const row = (await observer.query(
      `SELECT active_job_attempt FROM crawler.youtube_api_batches WHERE batch_id=$1`,
      [scenario.batchId],
    )).rows[0];
    return Number(row?.active_job_attempt) === 2;
  }, "Data API attempt 2 takeover", 30_000);

  await finalizerLocker.query(
    "SELECT pg_advisory_lock(hashtext($1))",
    [batchFinalizeLock],
  );
  finalizerLocked = true;
  await locker.query("COMMIT");
  settingsLocked = false;
  await waitFor(async () => {
    const row = (await observer.query(
      `SELECT batch.status AS batch_status,task.status AS task_status,
              task.candidate_ids,recovery.detail_status AS recovery_detail_status
       FROM crawler.youtube_api_batches batch
       JOIN crawler.youtube_api_tasks task ON task.task_id=ANY(batch.task_ids)
       JOIN crawler.content_candidates recovery ON recovery.candidate_id=$2
       WHERE batch.batch_id=$1`,
      [scenario.batchId, scenario.recoveryCandidateId],
    )).rows[0];
    return row?.batch_status === "running"
      && row?.task_status === "running"
      && row?.recovery_detail_status === "done"
      && row?.candidate_ids?.map(Number).length === 1
      && Number(row.candidate_ids[0]) === scenario.outsideCandidateId;
  }, "shared Data API task held until Batch terminal", 30_000);
  await finalizerLocker.query(
    "SELECT pg_advisory_unlock(hashtext($1))",
    [batchFinalizeLock],
  );
  finalizerLocked = false;
  const result = await within(job.waitUntilFinished(queueEvents), "takeover Data API completion", 30_000);
  assert.equal(result.ok, true);
  const state = (await observer.query(
    `SELECT batch.status AS batch_status,batch.active_job_attempt,
            task.status AS task_status,task.attempts,
            recovery.detail_status AS recovery_detail_status,
            outside.detail_status AS outside_detail_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=ANY(batch.task_ids)
     JOIN crawler.content_candidates recovery ON recovery.candidate_id=$2
     JOIN crawler.content_candidates outside ON outside.candidate_id=$3
     WHERE batch.batch_id=$1`,
    [scenario.batchId, scenario.recoveryCandidateId, scenario.outsideCandidateId],
  )).rows[0];
  assert.deepEqual(state, {
    batch_status: "done",
    active_job_attempt: "2",
    task_status: "pending",
    attempts: 1,
    recovery_detail_status: "done",
    outside_detail_status: "api_pending",
  });
  const completed = await queue.getJob(job.id);
  assert.equal(completed.attemptsStarted, 2);
  assert.equal(completed.attemptsMade, 1);

  await Promise.all([stopChild(firstWorker), stopChild(takeoverWorker)]);
  assert.equal(firstWorker.exitCode, 0, firstOutput.output());
  assert.equal(takeoverWorker.exitCode, 0, takeoverOutput.output());
});

test("the real Worker rejects a stale recovery Data API Job and isolates a shared task", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.dataApiBatch, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.dataApiBatch, { connection, prefix });
  let worker = null;
  let schemaInitialized = false;
  let redisReady = false;
  t.after(async () => {
    await stopChild(worker).catch(() => {});
    if (redisReady) await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close().catch(() => {}), queueEvents.close().catch(() => {})]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await setup.end().catch(() => {});
  });

  await setup.connect();
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  redisReady = true;

  const stale = await seedRecovery(setup, "stale", { resolved: true });
  const active = await seedRecovery(setup, "active");
  const staleJob = await queue.add("youtube-data-api-batch", jobData(stale), {
    jobId: `youtube-data-api__${stale.batchId}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  const activeJob = await queue.add("youtube-data-api-batch", jobData(active), {
    jobId: `youtube-data-api__${active.batchId}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

  worker = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const output = captureChildOutput(worker);
  await output.waitFor(`worker started queue=${queuesByRole.dataApiBatch}`);
  const staleResult = await within(staleJob.waitUntilFinished(queueEvents), "stale Data API Job");
  const activeResult = await within(activeJob.waitUntilFinished(queueEvents), "active Data API Job");

  assert.deepEqual({
    ok: staleResult.ok,
    skipped: staleResult.skipped,
    reason: staleResult.reason,
  }, {
    ok: true,
    skipped: true,
    reason: "data_api_batch_execution_fence_stale",
  });
  assert.equal(activeResult.ok, true);

  const staleState = (await setup.query(
    `SELECT batch.status AS batch_status,batch.active_job_id,batch.active_job_attempt,
            task.status AS task_status,recovery.api_status AS recovery_api_status,
            outside.api_status AS outside_api_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=ANY(batch.task_ids)
     JOIN crawler.content_candidates recovery ON recovery.candidate_id=$2
     JOIN crawler.content_candidates outside ON outside.candidate_id=$3
     WHERE batch.batch_id=$1`,
    [stale.batchId, stale.recoveryCandidateId, stale.outsideCandidateId],
  )).rows[0];
  assert.deepEqual(staleState, {
    batch_status: "queued",
    active_job_id: null,
    active_job_attempt: null,
    task_status: "queued",
    recovery_api_status: "queued",
    outside_api_status: "pending",
  });

  const activeState = (await setup.query(
    `SELECT batch.status AS batch_status,batch.active_job_id,batch.active_job_attempt,
            task.status AS task_status,task.candidate_ids,
            recovery.detail_status AS recovery_detail_status,
            recovery.api_status AS recovery_api_status,
            outside.detail_status AS outside_detail_status,
            outside.api_status AS outside_api_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=ANY(batch.task_ids)
     JOIN crawler.content_candidates recovery ON recovery.candidate_id=$2
     JOIN crawler.content_candidates outside ON outside.candidate_id=$3
     WHERE batch.batch_id=$1`,
    [active.batchId, active.recoveryCandidateId, active.outsideCandidateId],
  )).rows[0];
  activeState.candidate_ids = activeState.candidate_ids.map(Number);
  assert.deepEqual(activeState, {
    batch_status: "done",
    active_job_id: `youtube-data-api__${active.batchId}`,
    active_job_attempt: "1",
    task_status: "pending",
    candidate_ids: [active.outsideCandidateId],
    recovery_detail_status: "done",
    recovery_api_status: "done",
    outside_detail_status: "api_pending",
    outside_api_status: "pending",
  });

  await stopChild(worker);
  assert.equal(worker.exitCode, 0, output.output());
});

test("the real Worker settles a final-attempt crash after the last Candidate commit", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.dataApiBatch, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.dataApiBatch, { connection, prefix });
  let worker = null;
  let schemaInitialized = false;
  let redisReady = false;
  t.after(async () => {
    await stopChild(worker).catch(() => {});
    if (redisReady) await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close().catch(() => {}), queueEvents.close().catch(() => {})]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await setup.end().catch(() => {});
  });

  await setup.connect();
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  redisReady = true;
  const scenario = await seedRecovery(setup, "active");
  await setup.query(
    `CREATE FUNCTION crawler.fail_data_api_batch_terminal_once()
     RETURNS trigger LANGUAGE plpgsql AS $$
     BEGIN
       IF OLD.status='running' AND NEW.status='done' THEN
         RAISE EXCEPTION 'injected crash after Data API Candidate commit';
       END IF;
       RETURN NEW;
     END
     $$`,
  );
  await setup.query(
    `CREATE TRIGGER fail_data_api_batch_terminal_once
     BEFORE UPDATE ON crawler.youtube_api_batches
     FOR EACH ROW EXECUTE FUNCTION crawler.fail_data_api_batch_terminal_once()`,
  );
  const job = await queue.add("youtube-data-api-batch", jobData(scenario), {
    jobId: `youtube-data-api__${scenario.batchId}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

  worker = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const output = captureChildOutput(worker);
  await output.waitFor(`worker started queue=${queuesByRole.dataApiBatch}`);
  await assert.rejects(
    within(job.waitUntilFinished(queueEvents), "terminal Data API failure", 30_000),
    /injected crash after Data API Candidate commit/,
  );
  await waitFor(async () => {
    const row = (await setup.query(
      `SELECT status,result_json->'execution_orphan_recovery' AS recovery
       FROM crawler.youtube_api_batches WHERE batch_id=$1`,
      [scenario.batchId],
    )).rows[0];
    return row?.status === "failed"
      && row?.recovery?.failure_code === "DATA_API_BATCH_EXECUTION_ORPHANED";
  }, "fenced Data API terminal failure settlement", 30_000);

  const state = (await setup.query(
    `SELECT batch.status AS batch_status,batch.active_job_id,batch.active_job_attempt,
            task.status AS task_status,
            recovery.detail_status AS recovery_detail_status,
            recovery.api_status AS recovery_api_status,
            outside.detail_status AS outside_detail_status,
            outside.api_status AS outside_api_status,
            run.status AS run_status,run.detail_status AS run_detail_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=ANY(batch.task_ids)
     JOIN crawler.content_candidates recovery ON recovery.candidate_id=$2
     JOIN crawler.content_candidates outside ON outside.candidate_id=$3
     JOIN crawler.channel_runs run ON run.run_id=$4
     WHERE batch.batch_id=$1`,
    [
      scenario.batchId,
      scenario.recoveryCandidateId,
      scenario.outsideCandidateId,
      scenario.runId,
    ],
  )).rows[0];
  assert.deepEqual(state, {
    batch_status: "failed",
    active_job_id: `youtube-data-api__${scenario.batchId}`,
    active_job_attempt: "1",
    task_status: "pending",
    recovery_detail_status: "done",
    recovery_api_status: "done",
    outside_detail_status: "api_pending",
    outside_api_status: "pending",
    run_status: "waiting_agent",
    run_detail_status: "done",
  });
  assert.equal(await job.getState(), "failed");
  assert.equal((await queue.getJob(job.id)).attemptsStarted, 1);

  await stopChild(worker);
  assert.equal(worker.exitCode, 0, output.output());
});

test("a real stalled takeover never replays a partially committed Data API Batch", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.dataApiBatch, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.dataApiBatch, { connection, prefix });
  let blockingWorker = null;
  let takeoverWorker = null;
  let releaseBlockingAttempt = null;
  let schemaInitialized = false;
  let redisReady = false;
  t.after(async () => {
    releaseBlockingAttempt?.();
    await Promise.all([
      blockingWorker?.close().catch(() => {}),
      stopChild(takeoverWorker).catch(() => {}),
    ]);
    if (redisReady) await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close().catch(() => {}), queueEvents.close().catch(() => {})]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await setup.end().catch(() => {});
  });

  await setup.connect();
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  redisReady = true;
  const scenario = await seedRecovery(setup, "active");
  const jobId = `youtube-data-api__${scenario.batchId}`;
  await setup.query(
    `UPDATE crawler.youtube_api_batches
     SET status='running',active_job_id=$2,active_job_attempt=1,started_at=now()
     WHERE batch_id=$1`,
    [scenario.batchId, jobId],
  );
  await setup.query(
    `UPDATE crawler.youtube_api_tasks
     SET status='running',candidate_ids=ARRAY[$2]::bigint[],attempts=1
     WHERE task_id=$1`,
    [scenario.taskId, scenario.outsideCandidateId],
  );
  await setup.query(
    `UPDATE crawler.content_candidates
     SET detail_status='done',api_status='done',missing_fields='{}'::text[],
         disposition='stored',error_message=NULL,finished_at=now()
     WHERE candidate_id=$1`,
    [scenario.recoveryCandidateId],
  );
  const job = await queue.add("youtube-data-api-batch", jobData(scenario), {
    jobId,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  const blockingAttempt = new Promise((resolve) => { releaseBlockingAttempt = resolve; });
  blockingWorker = new Worker(
    queuesByRole.dataApiBatch,
    () => blockingAttempt,
    {
      connection,
      prefix,
      concurrency: 1,
      lockDuration: 500,
      stalledInterval: 500,
      skipLockRenewal: true,
    },
  );
  await blockingWorker.waitUntilReady();
  await waitFor(async () => {
    const active = await queue.getJob(jobId);
    return active?.attemptsStarted === 1 && await active.getState() === "active";
  }, "synthetic first Data API attempt");

  takeoverWorker = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const output = captureChildOutput(takeoverWorker);
  await output.waitFor(`worker started queue=${queuesByRole.dataApiBatch}`);
  const result = await within(job.waitUntilFinished(queueEvents), "partial commit takeover", 30_000);
  assert.deepEqual({
    ok: result.ok,
    skipped: result.skipped,
    reason: result.reason,
  }, {
    ok: true,
    skipped: true,
    reason: "data_api_batch_execution_fence_stale",
  });
  await waitFor(async () => {
    const row = (await setup.query(
      `SELECT status,result_json->'execution_orphan_recovery' AS recovery
       FROM crawler.youtube_api_batches WHERE batch_id=$1`,
      [scenario.batchId],
    )).rows[0];
    return row?.status === "failed"
      && row?.recovery?.failure_code === "DATA_API_BATCH_EXECUTION_ORPHANED";
  }, "partial commit takeover settlement", 30_000);
  const state = (await setup.query(
    `SELECT batch.active_job_attempt,task.status AS task_status,task.attempts,
            recovery.detail_status AS recovery_detail_status,
            recovery.api_status AS recovery_api_status,
            run.status AS run_status,run.detail_status AS run_detail_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=$2
     JOIN crawler.content_candidates recovery ON recovery.candidate_id=$3
     JOIN crawler.channel_runs run ON run.run_id=$4
     WHERE batch.batch_id=$1`,
    [scenario.batchId, scenario.taskId, scenario.recoveryCandidateId, scenario.runId],
  )).rows[0];
  assert.deepEqual(state, {
    active_job_attempt: "2",
    task_status: "pending",
    attempts: 1,
    recovery_detail_status: "done",
    recovery_api_status: "done",
    run_status: "waiting_agent",
    run_detail_status: "done",
  });
  const completed = await queue.getJob(jobId);
  assert.equal(completed.attemptsStarted, 2);
  assert.equal(completed.attemptsMade, 1);

  releaseBlockingAttempt();
  await blockingWorker.close();
  blockingWorker = null;
  await stopChild(takeoverWorker);
  assert.equal(takeoverWorker.exitCode, 0, output.output());
});
