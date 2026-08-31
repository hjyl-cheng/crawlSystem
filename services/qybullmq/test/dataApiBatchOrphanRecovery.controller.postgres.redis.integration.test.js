import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue } from "bullmq";
import pg from "pg";

import {
  claimDataApiBatchExecution,
  dataApiBatchExecutionFence,
  lockDataApiBatchExecution,
} from "../src/dataApiBatchExecutionFence.js";
import {
  crawlerRuntimeSchema,
  publicationCaptureSchemaBlock,
  publicationCurrentSchemaBlock,
} from "../src/publicationCurrentSchema.js";
import { queuesByRole, safeJobId } from "../src/queues.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();

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

function captureChildOutput(child) {
  let output = "";
  let exited = false;
  const updates = new EventEmitter();
  for (const stream of [child.stdout, child.stderr]) {
    stream.setEncoding("utf8");
    stream.on("data", (chunk) => {
      output = `${output}${chunk}`.slice(-30_000);
      updates.emit("change");
    });
  }
  child.once("exit", () => {
    exited = true;
    updates.emit("change");
  });
  return {
    output: () => output,
    waitFor: (fragment) => within(new Promise((resolve, reject) => {
      const inspect = () => {
        if (output.includes(fragment)) {
          updates.off("change", inspect);
          resolve(true);
        } else if (exited) {
          updates.off("change", inspect);
          reject(new Error(`Controller exited before ${fragment}\n${output}`));
        }
      };
      updates.on("change", inspect);
      inspect();
    }), `Controller output ${fragment}`),
  };
}

async function stopChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  await within(exited, "Controller shutdown", 10_000);
}

function controllerEnvironment({ prefix }) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "data_api_orphan_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: prefix,
    ROTA_WORKLOAD_SCOPE_EXPECTED: "qy-production",
    ROTA_PROXY_CONTROL_URL: "",
    ROTA_PROXY_CONTROL_TOKEN: "",
    QUERY_METADATA_AUTO_CYCLE_ENABLED: "false",
    CHANNEL_CANDIDATE_DISPATCH_ENABLED: "false",
    CONTENT_ENRICH_DISPATCH_ENABLED: "false",
    YOUTUBE_DATA_API_FALLBACK_MODE: "emergency",
    YOUTUBE_CHANNEL_INLINE_DETAILS: "true",
    DATA_API_ORPHAN_GRACE_MS: "0",
    DATA_API_ORPHAN_MIN_OBSERVATIONS: "2",
    CONTROLLER_INTERVAL_MS: "300000",
  };
}

async function runControllerStartupTick({ prefix, label }) {
  const child = spawn(process.execPath, ["src/controller.js"], {
    cwd: new URL("..", import.meta.url),
    env: controllerEnvironment({ prefix }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const output = captureChildOutput(child);
  try {
    await output.waitFor("controller started interval_ms=300000");
    assert.doesNotMatch(output.output(), /controller_tick_failed/, `${label}\n${output.output()}`);
  } finally {
    await stopChild(child);
  }
  assert.equal(child.exitCode, 0, `${label}\n${output.output()}`);
  return output.output();
}

test("the real Controller confirms missing and identity-conflicting Data API Jobs before fenced recovery", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const prefix = `data-api-orphan-controller-${suffix}`;
  const connection = localRedisConfiguration(redisUrl);
  const queue = new Queue(queuesByRole.dataApiBatch, { connection, prefix });
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
    await queue.obliterate({ force: true }).catch(() => {});
    await queue.close().catch(() => {});
    if (schemaInitialized) {
      await client.query("DROP SCHEMA IF EXISTS feature_clock CASCADE").catch(() => {});
      await client.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await client.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await client.end().catch(() => {});
  });

  await client.connect();
  await client.query("DROP SCHEMA IF EXISTS feature_clock CASCADE");
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  await client.query(publicationCurrentSchemaBlock(schema));
  await client.query(publicationCaptureSchemaBlock(schema));
  await client.query(`
    CREATE SCHEMA feature_clock;
    CREATE TABLE feature_clock.daily_channel_plans (
      plan_id uuid PRIMARY KEY,
      channel_id text NOT NULL,
      run_agent boolean NOT NULL,
      plan_day date NOT NULL,
      status text NOT NULL
    )
  `);
  schemaInitialized = true;
  await queue.obliterate({ force: true });
  await queue.waitUntilReady();

  const pipelineCycleId = `data-api-orphan-cycle-${suffix}`;
  const batchId = `data-api-orphan-batch-${suffix}`;
  const channelId = `UCorphan${suffix}`;
  const runId = `data-api-orphan-run-${suffix}`;
  const videoId = `data-api-orphan-video-${suffix}`;
  await client.query(
    `INSERT INTO crawler.settings (setting_key,value_json)
     VALUES ('query_scheduler',$1::jsonb)
     ON CONFLICT (setting_key) DO UPDATE
     SET value_json=EXCLUDED.value_json,updated_at=now()`,
    [JSON.stringify({
      status: "stopped",
      stop_reason: "user_requested",
      pipeline_cycle_id: pipelineCycleId,
    })],
  );
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'validation_closed',0,0)`,
    [pipelineCycleId],
  );
  await client.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
       agent_status,latest_run_id
     ) VALUES ($1,$2,'Orphan Test',2000,'active',true,'pending',$3)`,
    [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,status,crawl_mode,detail_status,expected_content_count,
       started_at,result_json
     ) VALUES ($1,$2,'waiting_detail','full','api_pending',1,now(),$3::jsonb)`,
    [runId, channelId, JSON.stringify({ pipeline_cycle_id: pipelineCycleId })],
  );
  const candidateId = Number((await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,content_type,type_status,type_source,
       detail_status,api_status,missing_fields,result_json
     ) VALUES ($1,$2,$3,1,'video','resolved','uploads_playlist',
               'api_pending','running',ARRAY['description']::text[],'{}'::jsonb)
     RETURNING candidate_id`,
    [runId, channelId, videoId],
  )).rows[0].candidate_id);
  const taskId = Number((await client.query(
    `INSERT INTO crawler.youtube_api_tasks (
       source_content_id,status,missing_fields,candidate_ids,attempts
     ) VALUES ($1,'running',ARRAY['description']::text[],ARRAY[$2]::bigint[],1)
     RETURNING task_id`,
    [videoId, candidateId],
  )).rows[0].task_id);
  const jobId = safeJobId("youtube-data-api", batchId);
  await client.query(
    `INSERT INTO crawler.youtube_api_batches (
       batch_id,status,task_ids,video_ids,active_job_id,active_job_attempt,
       result_json,started_at
     ) VALUES ($1,'running',ARRAY[$2]::bigint[],ARRAY[$3]::text[],$4,1,$5::jsonb,now())`,
    [
      batchId,
      taskId,
      videoId,
      jobId,
      JSON.stringify({
        dispatch_intent: {
          pipeline_cycle_id: pipelineCycleId,
          migration_system_retry_ids: [],
          recovery_run_ids: [],
        },
      }),
    ],
  );
  assert.equal(await queue.getJob(jobId), undefined);

  await runControllerStartupTick({ prefix, label: "first missing observation" });
  const first = (await client.query(
    `SELECT status,result_json->'execution_orphan_observation' AS observation
     FROM crawler.youtube_api_batches WHERE batch_id=$1`,
    [batchId],
  )).rows[0];
  assert.equal(first.status, "running");
  assert.equal(first.observation.observation_kind, "missing");
  assert.equal(first.observation.observation_count, 1);
  assert.equal((await client.query(
    "SELECT status FROM crawler.youtube_api_tasks WHERE task_id=$1",
    [taskId],
  )).rows[0].status, "running");

  await runControllerStartupTick({ prefix, label: "confirmed missing observation" });
  const recovered = (await client.query(
    `SELECT batch.status AS batch_status,batch.active_job_id,batch.active_job_attempt,
            batch.result_json->'execution_orphan_recovery' AS recovery,
            task.status AS task_status,candidate.api_status AS candidate_api_status,
            run.detail_status AS run_detail_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=$2
     JOIN crawler.content_candidates candidate ON candidate.candidate_id=$3
     JOIN crawler.channel_runs run ON run.run_id=$4
     WHERE batch.batch_id=$1`,
    [batchId, taskId, candidateId, runId],
  )).rows[0];
  assert.equal(recovered.batch_status, "failed");
  assert.equal(recovered.active_job_id, jobId);
  assert.equal(Number(recovered.active_job_attempt), 1);
  assert.equal(recovered.recovery.failure_type, "retryable_system_failure");
  assert.equal(recovered.recovery.observation_kind, "missing");
  assert.equal(recovered.task_status, "pending");
  assert.equal(recovered.candidate_api_status, "pending");
  assert.equal(recovered.run_detail_status, "api_pending");
  assert.equal(await queue.getJob(jobId), undefined);

  const conflictBatchId = `data-api-conflict-batch-${suffix}`;
  const conflictChannelId = `UCconflict${suffix}`;
  const conflictRunId = `data-api-conflict-run-${suffix}`;
  const conflictVideoId = `data-api-conflict-video-${suffix}`;
  await client.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
       agent_status,latest_run_id
     ) VALUES ($1,$2,'Conflict Test',2000,'active',true,'pending',$3)`,
    [conflictChannelId, `https://www.youtube.com/channel/${conflictChannelId}`, conflictRunId],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,status,crawl_mode,detail_status,expected_content_count,
       started_at,result_json
     ) VALUES ($1,$2,'waiting_detail','full','api_pending',1,now(),$3::jsonb)`,
    [conflictRunId, conflictChannelId, JSON.stringify({ pipeline_cycle_id: pipelineCycleId })],
  );
  const conflictCandidateId = Number((await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,content_type,type_status,type_source,
       detail_status,api_status,missing_fields,result_json
     ) VALUES ($1,$2,$3,1,'video','resolved','uploads_playlist',
               'api_pending','running',ARRAY['description']::text[],'{}'::jsonb)
     RETURNING candidate_id`,
    [conflictRunId, conflictChannelId, conflictVideoId],
  )).rows[0].candidate_id);
  const conflictTaskId = Number((await client.query(
    `INSERT INTO crawler.youtube_api_tasks (
       source_content_id,status,missing_fields,candidate_ids,attempts
     ) VALUES ($1,'running',ARRAY['description']::text[],ARRAY[$2]::bigint[],1)
     RETURNING task_id`,
    [conflictVideoId, conflictCandidateId],
  )).rows[0].task_id);
  const conflictJobId = safeJobId("youtube-data-api", conflictBatchId);
  await client.query(
    `INSERT INTO crawler.youtube_api_batches (
       batch_id,status,task_ids,video_ids,active_job_id,active_job_attempt,
       result_json,started_at
     ) VALUES ($1,'running',ARRAY[$2]::bigint[],ARRAY[$3]::text[],$4,1,$5::jsonb,now())`,
    [
      conflictBatchId,
      conflictTaskId,
      conflictVideoId,
      conflictJobId,
      JSON.stringify({
        dispatch_intent: {
          pipeline_cycle_id: pipelineCycleId,
          migration_system_retry_ids: [],
          recovery_run_ids: [],
        },
      }),
    ],
  );
  const conflictingJobData = {
    batch_id: conflictBatchId,
    task_ids: [conflictTaskId],
    video_ids: [`${conflictVideoId}-wrong`],
    pipeline_cycle_id: pipelineCycleId,
  };
  const conflictingJob = await queue.add(
    "youtube-data-api-batch",
    conflictingJobData,
    { jobId: conflictJobId },
  );
  const conflictingFence = dataApiBatchExecutionFence({
    id: conflictingJob.id,
    name: conflictingJob.name,
    data: conflictingJob.data,
    attemptsStarted: 1,
  });
  assert.equal(
    await transaction(client, (tx) => claimDataApiBatchExecution(tx, conflictingFence)),
    null,
    "the conflicting Redis identity must not claim the authoritative Batch",
  );
  assert.equal(
    await transaction(client, (tx) => lockDataApiBatchExecution(tx, conflictingFence)),
    null,
    "the conflicting Redis identity must not acquire commit authorization",
  );

  await runControllerStartupTick({ prefix, label: "first identity-conflict observation" });
  const firstConflict = (await client.query(
    `SELECT batch.status,
            batch.result_json->'execution_orphan_observation' AS observation,
            task.status AS task_status,candidate.api_status AS candidate_api_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=$2
     JOIN crawler.content_candidates candidate ON candidate.candidate_id=$3
     WHERE batch.batch_id=$1`,
    [conflictBatchId, conflictTaskId, conflictCandidateId],
  )).rows[0];
  assert.equal(firstConflict.status, "running");
  assert.equal(firstConflict.observation.observation_kind, "identity_conflict");
  assert.equal(firstConflict.observation.observation_count, 1);
  assert.equal(firstConflict.task_status, "running");
  assert.equal(firstConflict.candidate_api_status, "running");
  assert.ok(await queue.getJob(conflictJobId), "the first observation must preserve raw Redis evidence");

  await runControllerStartupTick({ prefix, label: "confirmed identity-conflict observation" });
  const settledConflict = (await client.query(
    `SELECT batch.status AS batch_status,
            batch.result_json->'execution_orphan_recovery' AS recovery,
            task.status AS task_status,candidate.api_status AS candidate_api_status,
            run.detail_status AS run_detail_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks task ON task.task_id=$2
     JOIN crawler.content_candidates candidate ON candidate.candidate_id=$3
     JOIN crawler.channel_runs run ON run.run_id=$4
     WHERE batch.batch_id=$1`,
    [conflictBatchId, conflictTaskId, conflictCandidateId, conflictRunId],
  )).rows[0];
  assert.equal(settledConflict.batch_status, "failed");
  assert.equal(settledConflict.recovery.failure_type, "retryable_system_failure");
  assert.equal(settledConflict.recovery.observation_kind, "identity_conflict");
  assert.equal(settledConflict.task_status, "pending");
  assert.equal(settledConflict.candidate_api_status, "pending");
  assert.equal(settledConflict.run_detail_status, "api_pending");
  const retainedConflict = await queue.getJob(conflictJobId);
  assert.ok(retainedConflict, "fenced recovery must preserve the conflicting Redis Job as evidence");
  assert.deepEqual(retainedConflict.data, conflictingJobData);
});
