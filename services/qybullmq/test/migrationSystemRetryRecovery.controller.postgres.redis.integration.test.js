import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import { runChannelCandidateWorkerJobWithDurableSettlement } from "../src/channelCandidateWorkerLifecycle.js";
import { finalizeDispatchRevision } from "../src/finalizePolicy.js";
import {
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "../src/managedJobDispatchOutbox.js";
import { markChannelCandidateJobAttemptActive } from "../src/managedWorkerJob.js";
import { retryMigrationSystemFailure } from "../src/migrationSystemRetry.js";
import {
  crawlerRuntimeSchema,
  publicationCaptureSchemaBlock,
  publicationCurrentSchemaBlock,
} from "../src/publicationCurrentSchema.js";
import { queueNames, queuesByRole, safeJobId } from "../src/queues.js";

const { Client, Pool } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();
const recoveryQueueNames = Object.freeze([
  queuesByRole.channelCrawl,
  queuesByRole.dataApiBatch,
  queuesByRole.agentBatch,
  queuesByRole.finalize,
]);
const inFlightStates = Object.freeze([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "paused",
  "waiting-children",
]);
const retainedStates = Object.freeze([
  ...inFlightStates,
  "completed",
  "failed",
]);

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

function localRedisConfiguration(value) {
  const url = new URL(value);
  assert.equal(url.protocol, "redis:", "integration Redis must not require TLS");
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
  assert.equal(url.username, "", "Controller entry does not accept a Redis ACL username");
  assert.equal(
    decodeURIComponent(url.pathname).replace(/^\/+/, "") || "0",
    "0",
    "Controller entry integration must use Redis database 0",
  );
  assert.equal(url.search, "", "Controller entry does not accept Redis URL options");
  assert.equal(url.hash, "", "Controller entry does not accept a Redis URL fragment");
  return {
    host: url.hostname,
    port: Number(url.port || 6379),
    password: url.password ? decodeURIComponent(url.password) : undefined,
  };
}

function redisConnection(value) {
  return {
    ...localRedisConfiguration(value),
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
    waitFor: (fragment, timeoutMs = 20_000) => within(
      new Promise((resolve, reject) => {
        const inspect = () => {
          if (output.includes(fragment)) {
            updates.off("change", inspect);
            resolve(true);
          } else if (exited) {
            updates.off("change", inspect);
            reject(new Error(
              `Controller exited before ${JSON.stringify(fragment)}\n${output}`,
            ));
          }
        };
        updates.on("change", inspect);
        inspect();
      }),
      `Controller output ${JSON.stringify(fragment)}`,
      timeoutMs,
    ),
  };
}

async function stopChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  try {
    await within(exited, "Controller shutdown", 10_000);
  } catch (gracefulError) {
    child.kill("SIGKILL");
    try {
      await within(exited, "forced Controller shutdown", 5_000);
    } catch (forcedError) {
      throw new AggregateError(
        [gracefulError, forcedError],
        "Controller did not exit after SIGTERM or SIGKILL",
      );
    }
  }
}

function controllerEnvironment({ prefix }) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "controller_recovery_forbidden_database",
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

async function setScheduler(client, batchId, { status, stopReason }) {
  const value = {
    status,
    stop_reason: stopReason,
    pipeline_cycle_id: batchId,
    batch_outcome: "completed_with_system_failures",
    batch_statistics: { total: 100, accepted: 99, rejected: 0, failed: 1 },
    updated_by: "controller-recovery-test",
  };
  await client.query(
    `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
     VALUES ('query_scheduler',$1::jsonb,now())
     ON CONFLICT (setting_key) DO UPDATE
     SET value_json=EXCLUDED.value_json,updated_at=now()`,
    [JSON.stringify(value)],
  );
  return value;
}

async function initializeScenario(client, {
  batchId,
  candidateId,
  channelId,
  failedJobId,
}) {
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
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,outcome,total_channel_count,
       discovered_candidate_count,accepted_channel_count,rejected_channel_count,
       failed_channel_count,result_json,finished_at
     ) VALUES (
       $1,$1,'completed','completed_with_system_failures',100,100,99,0,1,
       jsonb_build_object(
         'outcome','completed_with_system_failures',
         'statistics',jsonb_build_object('total',100,'accepted',99,'rejected',0,'failed',1)
       ),now()
     )`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
       status,snapshot_dispatch_generation,snapshot_active_job_id,
       snapshot_active_job_attempt,snapshot_json,source_json,validation_finished_at
     ) VALUES (
       $1,$2,$2,$3,$4,'failed',1,$5,1,
       jsonb_build_object(
         'failure_type','retryable_system_failure',
         'failed_dispatch_batch_id',$2::text,
         'system_failure',jsonb_build_object('code','LEASE_CONFLICT','category','lease')
       ),
       '{"source":"legacy_results_db"}'::jsonb,now()
     )`,
    [candidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`, failedJobId],
  );
  const intent = await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts,last_dispatch_at,last_error
     ) VALUES (
       $1,current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
       $3,'{}'::jsonb,repeat('a',64),$2,$4,1,now(),'lease conflict'
     ) RETURNING migration_intent_id`,
    [`controller-system-recovery:${batchId}`, candidateId, channelId, batchId],
  );
  const retry = await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'pending')
     RETURNING system_retry_id`,
    [Number(intent.rows[0].migration_intent_id), candidateId, batchId, failedJobId],
  );
  return {
    migrationIntentId: Number(intent.rows[0].migration_intent_id),
    systemRetryId: Number(retry.rows[0].system_retry_id),
  };
}

async function pauseRecoveryQueues(queues) {
  await Promise.all(recoveryQueueNames.map((queueName) => queues[queueName].pause()));
}

async function assertRecoveryQueueState(queues, expected, label) {
  const actual = Object.fromEntries(await Promise.all(recoveryQueueNames.map(async (queueName) => (
    [queueName, await queues[queueName].isPaused()]
  ))));
  assert.deepEqual(actual, expected, label);
}

async function assertFrozenBatchAndScheduler(client, batchId, scheduler) {
  const state = await client.query(
    `SELECT batch.status,batch.outcome,batch.total_channel_count,
            batch.accepted_channel_count,batch.rejected_channel_count,
            batch.failed_channel_count,settings.value_json AS scheduler
     FROM crawler.query_dispatch_batches batch
     JOIN crawler.settings settings ON settings.setting_key='query_scheduler'
     WHERE batch.dispatch_batch_id=$1`,
    [batchId],
  );
  assert.deepEqual(state.rows[0], {
    status: "completed",
    outcome: "completed_with_system_failures",
    total_channel_count: 100,
    accepted_channel_count: 99,
    rejected_channel_count: 0,
    failed_channel_count: 1,
    scheduler,
  });
}

async function inFlightJobs(queue) {
  return queue.getJobs(inFlightStates, 0, 100, true);
}

async function assertJobNotStarted(job, label) {
  assert.ok(
    ["waiting", "paused", "prioritized", "delayed"].includes(await job.getState()),
    `${label} must remain queued without an active Worker attempt`,
  );
}

function jobTargetsChannel(job, channelId) {
  return String(job.data?.channel_id ?? "") === channelId
    || (job.data?.channel_ids ?? []).map(String).includes(channelId);
}

async function retainedJobsForChannel(queue, channelId) {
  return (await queue.getJobs(retainedStates, 0, 100, true))
    .filter((job) => jobTargetsChannel(job, channelId));
}

async function productionFinalizeIdentity(query, { channelId, runId }) {
  const revisionRows = await query(
    `SELECT
       c.channel_id,c.latest_run_id,c.status AS channel_status,c.agent_status,
       c.updated_at AS channel_updated_at,
       r.detail_status,r.expected_content_count,
       r.result_json->>'pipeline_cycle_id' AS pipeline_cycle_id,
       r.result_json->'final_repair' AS run_final_repair,
       (SELECT count(*)::int FROM crawler.content_candidates cc WHERE cc.run_id=$2)
         AS candidate_count,
       (SELECT max(cc.updated_at) FROM crawler.content_candidates cc WHERE cc.run_id=$2)
         AS candidate_updated_at,
       (SELECT count(*)::int FROM crawler.contents ct
        WHERE ct.channel_id=$1 AND ct.run_id=$2) AS content_count,
       (SELECT max(COALESCE(ct.last_enriched_at,ct.last_seen_at))
        FROM crawler.contents ct WHERE ct.channel_id=$1 AND ct.run_id=$2) AS content_updated_at,
       (SELECT ap.updated_at FROM crawler.agent_profiles ap
        WHERE ap.channel_id=$1 AND ap.agent_mode='basic' AND ap.status='success' LIMIT 1)
         AS agent_updated_at
     FROM crawler.channels c
     LEFT JOIN crawler.channel_runs r ON r.run_id=$2
     WHERE c.channel_id=$1
     LIMIT 1`,
    [channelId, runId],
  );
  const sourceRevision = finalizeDispatchRevision(
    revisionRows.rows[0] ?? { channel_id: channelId, run_id: runId },
  );
  return {
    sourceRevision,
    jobId: safeJobId("finalize", runId || channelId, sourceRevision),
  };
}

async function runObservedWorkerJob({
  queueName,
  queue,
  queueEvents,
  connection,
  prefix,
  job,
  processor,
  label,
}) {
  const workerErrors = [];
  const worker = new Worker(queueName, processor, { connection, prefix, concurrency: 1 });
  worker.on("error", (error) => workerErrors.push(error));
  try {
    await worker.waitUntilReady();
    await within(job.waitUntilFinished(queueEvents), label);
  } finally {
    await worker.close().catch(() => {});
  }
  assert.deepEqual(workerErrors, [], `${label} Worker errors`);
  assert.equal(await queue.isPaused(), false, `${label} queue must have been resumed by Controller`);
}

test("the real Controller only resumes queues demanded by a completed batch system retry", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 120_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  localRedisConfiguration(redisUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `controller-system-recovery:${suffix}`;
  const currentBatchId = `controller-current-batch:${suffix}`;
  const candidateId = 482;
  const channelId = `UCcontroller${suffix}`;
  const runId = `run:controller-system-recovery:${suffix}`;
  const outsideRunId = `run:controller-outside-data-api:${suffix}`;
  const failedJobId = safeJobId("channel-snapshot", batchId, channelId, "g1");
  const prefix = `migration-system-recovery-controller-${suffix}`;
  const connection = redisConnection(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const pool = new Pool({ connectionString: databaseUrl, max: 6 });
  const query = pool.query.bind(pool);
  const withTransaction = async (action) => {
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
  const queues = Object.fromEntries(queueNames.map((queueName) => [
    queueName,
    new Queue(queueName, { connection, prefix }),
  ]));
  const queueEvents = Object.fromEntries(recoveryQueueNames.map((queueName) => [
    queueName,
    new QueueEvents(queueName, { connection, prefix }),
  ]));
  let schemaInitialized = false;
  let recoveryContentCandidateId = null;
  let outsideContentCandidateId = null;

  await setup.connect();
  t.after(async () => {
    await Promise.all(Object.values(queues).map((queue) => (
      queue.obliterate({ force: true }).catch(() => {})
    )));
    await Promise.all([
      ...Object.values(queues).map((queue) => queue.close().catch(() => {})),
      ...Object.values(queueEvents).map((events) => events.close().catch(() => {})),
    ]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS feature_clock CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await setup.end().catch(() => {});
    await pool.end().catch(() => {});
  });

  schemaInitialized = true;
  const scenario = await initializeScenario(setup, {
    batchId,
    candidateId,
    channelId,
    failedJobId,
  });
  await setup.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,outcome,finished_at
     ) VALUES ($1,$1,'completed','completed',now())`,
    [currentBatchId],
  );
  await Promise.all([
    ...Object.values(queues).map((queue) => queue.waitUntilReady()),
    ...Object.values(queueEvents).map((events) => events.waitUntilReady()),
  ]);
  await Promise.all(Object.values(queues).map((queue) => queue.obliterate({ force: true })));
  await pauseRecoveryQueues(queues);

  await setScheduler(setup, currentBatchId, {
    status: "stopped",
    stopReason: "pipeline_complete",
  });
  const allocation = await retryMigrationSystemFailure({
    systemRetryId: scenario.systemRetryId,
    withTransaction,
  });
  assert.equal(allocation.dispatch_generation, 2);
  const dispatch = await new ManagedJobOutboxDispatcher({
    repository: new PostgresManagedJobDispatchRepository({ withTransaction }),
    queues,
  }).dispatchAvailable({ limit: 10 });
  assert.deepEqual(dispatch, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  const channelJob = await queues[queuesByRole.channelCrawl].getJob(
    allocation.outbox.deterministic_job_id,
  );
  assert.ok(channelJob);

  const pausedScheduler = await setScheduler(setup, currentBatchId, {
    status: "paused",
    stopReason: null,
  });
  await runControllerStartupTick({ prefix, label: "paused Scheduler" });
  await assertRecoveryQueueState(queues, {
    [queuesByRole.channelCrawl]: true,
    [queuesByRole.dataApiBatch]: true,
    [queuesByRole.agentBatch]: true,
    [queuesByRole.finalize]: false,
  }, "paused Scheduler must only preserve independent Finalize recovery");
  await assertJobNotStarted(channelJob, "paused Scheduler G+1 Job");
  await assertFrozenBatchAndScheduler(setup, batchId, pausedScheduler);

  const userStoppedScheduler = await setScheduler(setup, currentBatchId, {
    status: "stopped",
    stopReason: "user_requested",
  });
  await runControllerStartupTick({ prefix, label: "user-stopped Scheduler" });
  await assertRecoveryQueueState(queues, {
    [queuesByRole.channelCrawl]: true,
    [queuesByRole.dataApiBatch]: true,
    [queuesByRole.agentBatch]: true,
    [queuesByRole.finalize]: false,
  }, "user-stopped Scheduler must only preserve independent Finalize recovery");
  await assertJobNotStarted(channelJob, "user-stopped Scheduler G+1 Job");
  await assertFrozenBatchAndScheduler(setup, batchId, userStoppedScheduler);

  const completedScheduler = await setScheduler(setup, currentBatchId, {
    status: "stopped",
    stopReason: "pipeline_complete",
  });
  await runControllerStartupTick({ prefix, label: "completed Scheduler Channel recovery" });
  await assertRecoveryQueueState(queues, {
    [queuesByRole.channelCrawl]: false,
    [queuesByRole.dataApiBatch]: true,
    [queuesByRole.agentBatch]: true,
    [queuesByRole.finalize]: false,
  }, "Channel demand and independent Finalize recovery may resume after batch settlement");
  assert.equal((await inFlightJobs(queues[queuesByRole.channelCrawl])).length, 1);
  assert.equal((await inFlightJobs(queues[queuesByRole.agentBatch])).length, 0);
  assert.equal((await inFlightJobs(queues[queuesByRole.finalize])).length, 0);
  await assertFrozenBatchAndScheduler(setup, batchId, completedScheduler);

  await runObservedWorkerJob({
    queueName: queuesByRole.channelCrawl,
    queue: queues[queuesByRole.channelCrawl],
    queueEvents: queueEvents[queuesByRole.channelCrawl],
    connection,
    prefix,
    job: channelJob,
    label: "controlled G+1 Channel completion",
    processor: async (job) => {
      assert.equal(await markChannelCandidateJobAttemptActive(query, job), true);
      return runChannelCandidateWorkerJobWithDurableSettlement({
        query,
        job,
        execute: () => withTransaction(async (client) => {
          const accepted = await client.query(
            `UPDATE crawler.channel_candidates
             SET status='accepted',accepted_at=COALESCE(accepted_at,now()),
                 validation_finished_at=COALESCE(validation_finished_at,now()),updated_at=now()
             WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
               AND snapshot_active_job_id=$3 AND snapshot_active_job_attempt=$4
             RETURNING candidate_id`,
            [candidateId, 2, String(job.id), Number(job.attemptsStarted)],
          );
          assert.equal(accepted.rowCount, 1);
          await client.query(
            `INSERT INTO crawler.channels (
               channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
               agent_status,latest_run_id,registry_promotion_candidate_id,
               registry_promotion_run_id
             ) VALUES ($1,$2,'Recovered Channel',2000,'active',true,'pending',$3,$4,$3)`,
            [channelId, `https://www.youtube.com/channel/${channelId}`, runId, candidateId],
          );
          await client.query(
            `INSERT INTO crawler.channel_runs (
               run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
               expected_content_count,started_at,result_json
             ) VALUES ($1,$2,$3,'waiting_detail','full','api_pending',1,now(),$4::jsonb)`,
            [runId, channelId, candidateId, JSON.stringify({
              dispatch_batch_id: batchId,
            })],
          );
          const contentCandidate = await client.query(
            `INSERT INTO crawler.content_candidates (
               run_id,channel_id,source_content_id,position,content_type,
               type_status,type_source,detail_status,api_status,missing_fields,result_json
             ) VALUES (
               $1,$2,$3,1,'video','resolved','uploads_playlist',
               'api_pending','pending',ARRAY['description']::text[],'{}'::jsonb
             ) RETURNING candidate_id`,
            [runId, channelId, `video-${suffix}`],
          );
          recoveryContentCandidateId = Number(contentCandidate.rows[0].candidate_id);
          await client.query(
            `INSERT INTO crawler.channel_runs (
               run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
               expected_content_count,started_at,result_json
             ) VALUES ($1,$2,NULL,'waiting_detail','full','api_pending',1,now(),$3::jsonb)`,
            [outsideRunId, channelId, JSON.stringify({ dispatch_batch_id: currentBatchId })],
          );
          const outsideContentCandidate = await client.query(
            `INSERT INTO crawler.content_candidates (
               run_id,channel_id,source_content_id,position,content_type,
               type_status,type_source,detail_status,api_status,missing_fields,result_json
             ) VALUES (
               $1,$2,$3,1,'video','resolved','uploads_playlist',
               'api_pending','pending',ARRAY['description']::text[],'{}'::jsonb
             ) RETURNING candidate_id`,
            [outsideRunId, channelId, `video-${suffix}`],
          );
          outsideContentCandidateId = Number(outsideContentCandidate.rows[0].candidate_id);
          await client.query(
            `INSERT INTO crawler.youtube_api_tasks (
               source_content_id,status,missing_fields,candidate_ids
             ) VALUES ($1,'pending',ARRAY['description']::text[],$2::bigint[])`,
            [
              `video-${suffix}`,
              [recoveryContentCandidateId, outsideContentCandidateId],
            ],
          );
          return { accepted: true, run_id: runId };
        }),
      });
    },
  });

  const strandedTasks = await setup.query(
    `UPDATE crawler.youtube_api_tasks
     SET status='queued',updated_at=now()
     WHERE source_content_id=$1
     RETURNING task_id,source_content_id,candidate_ids`,
    [`video-${suffix}`],
  );
  assert.equal(strandedTasks.rowCount, 1);
  const strandedTaskId = Number(strandedTasks.rows[0].task_id);
  await setup.query(
    `UPDATE crawler.content_candidates
     SET api_status='queued',updated_at=now()
     WHERE candidate_id=$1`,
    [recoveryContentCandidateId],
  );
  assert.equal((await setup.query(
    `SELECT count(*)::int AS count
     FROM crawler.youtube_api_batches
     WHERE $1=ANY(task_ids)`,
    [strandedTaskId],
  )).rows[0].count, 0, "the first crash fixture must strand a queued task before Batch insert");

  await runControllerStartupTick({ prefix, label: "orphaned queued Data API task recovery" });
  const recoveredBatch = (await setup.query(
    `SELECT batch_id,status,task_ids,video_ids,result_json->'dispatch_intent' AS dispatch_intent
     FROM crawler.youtube_api_batches
     WHERE $1=ANY(task_ids)`,
    [strandedTaskId],
  )).rows[0];
  assert.ok(recoveredBatch);
  assert.equal(recoveredBatch.status, "queued");
  assert.deepEqual(recoveredBatch.task_ids.map(Number), [strandedTaskId]);
  assert.deepEqual(recoveredBatch.video_ids, [`video-${suffix}`]);
  assert.deepEqual(recoveredBatch.dispatch_intent, {
    pipeline_cycle_id: batchId,
    migration_system_retry_ids: [scenario.systemRetryId],
    recovery_run_ids: [runId],
  });
  assert.deepEqual((await setup.query(
    `SELECT candidate_id,api_status
     FROM crawler.content_candidates
     WHERE candidate_id=ANY($1::bigint[])
     ORDER BY candidate_id`,
    [[recoveryContentCandidateId, outsideContentCandidateId]],
  )).rows.map((row) => ({
    candidate_id: Number(row.candidate_id),
    api_status: row.api_status,
  })), [
    { candidate_id: recoveryContentCandidateId, api_status: "queued" },
    { candidate_id: outsideContentCandidateId, api_status: "pending" },
  ]);
  await setup.query(
    `UPDATE crawler.youtube_api_tasks
     SET candidate_ids=array_remove(candidate_ids,$2::bigint),updated_at=now()
     WHERE task_id=$1`,
    [strandedTaskId, outsideContentCandidateId],
  );
  await setup.query(
    "DELETE FROM crawler.content_candidates WHERE candidate_id=$1",
    [outsideContentCandidateId],
  );
  await setup.query("DELETE FROM crawler.channel_runs WHERE run_id=$1", [outsideRunId]);
  const strandedBatchId = recoveredBatch.batch_id;
  const recoveredJobId = safeJobId("youtube-data-api", strandedBatchId);
  const recoveredJob = await queues[queuesByRole.dataApiBatch].getJob(recoveredJobId);
  assert.ok(recoveredJob);
  await recoveredJob.remove();
  assert.equal(
    await queues[queuesByRole.dataApiBatch].getJob(recoveredJobId),
    undefined,
    "the second crash fixture must retain DB intent without its Redis Job",
  );

  await runControllerStartupTick({ prefix, label: "durable Data API Batch Job recovery" });
  await assertRecoveryQueueState(queues, {
    [queuesByRole.channelCrawl]: true,
    [queuesByRole.dataApiBatch]: false,
    [queuesByRole.agentBatch]: true,
    [queuesByRole.finalize]: false,
  }, "Data API demand must remain independently consumable after G+1 Channel completion");
  const dataApiJobs = await inFlightJobs(queues[queuesByRole.dataApiBatch]);
  assert.equal(dataApiJobs.length, 1, "exactly one recovery Data API Job must represent the Batch");
  assert.equal(dataApiJobs[0].data.batch_id, strandedBatchId);
  assert.equal(dataApiJobs[0].id, recoveredJobId);
  assert.equal(dataApiJobs[0].name, "youtube-data-api-batch");
  assert.equal(dataApiJobs[0].data.pipeline_cycle_id, batchId);
  assert.deepEqual(dataApiJobs[0].data.migration_system_retry_ids, [scenario.systemRetryId]);
  assert.deepEqual(dataApiJobs[0].data.recovery_run_ids, [runId]);
  assert.equal((await inFlightJobs(queues[queuesByRole.agentBatch])).length, 0);
  await assertFrozenBatchAndScheduler(setup, batchId, completedScheduler);

  await runObservedWorkerJob({
    queueName: queuesByRole.dataApiBatch,
    queue: queues[queuesByRole.dataApiBatch],
    queueEvents: queueEvents[queuesByRole.dataApiBatch],
    connection,
    prefix,
    job: dataApiJobs[0],
    label: "targeted Data API completion",
    processor: async (job) => {
      assert.deepEqual(job.data.migration_system_retry_ids, [scenario.systemRetryId]);
      await withTransaction(async (client) => {
        await client.query(
          `UPDATE crawler.youtube_api_batches
           SET status='done',finished_at=now(),updated_at=now()
           WHERE batch_id=$1`,
          [job.data.batch_id],
        );
        await client.query(
          `UPDATE crawler.youtube_api_tasks
           SET status='done',finished_at=now(),updated_at=now()
           WHERE task_id=ANY($1::bigint[])`,
          [job.data.task_ids],
        );
        await client.query(
          `UPDATE crawler.content_candidates
           SET detail_status='done',api_status='done',missing_fields='{}'::text[],
               disposition='stored',finished_at=now(),updated_at=now()
           WHERE run_id=$1 AND candidate_id=ANY(
             SELECT unnest(candidate_ids)
             FROM crawler.youtube_api_tasks
             WHERE task_id=ANY($2::bigint[])
           )`,
          [runId, job.data.task_ids],
        );
        await client.query(
          `UPDATE crawler.channel_runs
           SET status='waiting_agent',detail_status='done',updated_at=now()
           WHERE run_id=$1`,
          [runId],
        );
      });
      return { ok: true };
    },
  });

  await runControllerStartupTick({ prefix, label: "completed Scheduler Agent recovery" });
  await assertRecoveryQueueState(queues, {
    [queuesByRole.channelCrawl]: true,
    [queuesByRole.dataApiBatch]: true,
    [queuesByRole.agentBatch]: false,
    [queuesByRole.finalize]: false,
  }, "Agent demand and independent Finalize recovery may remain after G+1 Channel completion");
  const agentJobs = await inFlightJobs(queues[queuesByRole.agentBatch]);
  assert.equal(agentJobs.length, 1, "Controller and generic producer must not duplicate Agent work");
  assert.equal(agentJobs[0].name, "agent-profile-batch");
  assert.equal(agentJobs[0].data.migration_system_retry_id, scenario.systemRetryId);
  assert.equal(agentJobs[0].data.candidate_id, candidateId);
  assert.equal(agentJobs[0].data.dispatch_generation, 2);
  assert.deepEqual(agentJobs[0].data.channel_ids, [channelId]);
  assert.equal((await inFlightJobs(queues[queuesByRole.finalize])).length, 0);
  await assertFrozenBatchAndScheduler(setup, batchId, completedScheduler);

  await runObservedWorkerJob({
    queueName: queuesByRole.agentBatch,
    queue: queues[queuesByRole.agentBatch],
    queueEvents: queueEvents[queuesByRole.agentBatch],
    connection,
    prefix,
    job: agentJobs[0],
    label: "targeted Agent completion",
    processor: async (job) => {
      assert.equal(job.data.migration_system_retry_id, scenario.systemRetryId);
      await withTransaction(async (client) => {
        await client.query(
          `INSERT INTO crawler.agent_profiles (
             channel_id,agent_mode,input_url,status,metrics_json,prompt_variant,
             input_content_ids,input_content_hash,taxonomy_version,agent_version_hash,
             attempts,updated_at
           ) VALUES (
             $1,'basic',$2,'success','{}'::jsonb,'local_offline','{}'::text[],
             'sha256:' || repeat('a',64),'qy-taxonomy-v1','sha256:' || repeat('b',64),1,now()
           )`,
          [channelId, `https://www.youtube.com/channel/${channelId}`],
        );
        await client.query(
          `UPDATE crawler.channels
           SET agent_status='done',agent_attempts=agent_attempts+1,updated_at=now()
           WHERE channel_id=$1`,
          [channelId],
        );
      });
      return { ok: true };
    },
  });
  assert.equal(
    (await inFlightJobs(queues[queuesByRole.finalize])).length,
    0,
    "a recovery Agent completion must leave Finalize dispatch to the fenced reconciler",
  );
  const legacyGenericIdentity = await productionFinalizeIdentity(query, { channelId, runId });
  const legacyGenericFinalize = await queues[queuesByRole.finalize].add(
    "finalize-channel",
    {
      channel_id: channelId,
      run_id: runId,
      reason: "agent-complete",
      source_revision: legacyGenericIdentity.sourceRevision,
      pipeline_cycle_id: batchId,
    },
    { jobId: legacyGenericIdentity.jobId },
  );

  await runControllerStartupTick({ prefix, label: "completed Scheduler Finalize recovery" });
  await assertRecoveryQueueState(queues, {
    [queuesByRole.channelCrawl]: true,
    [queuesByRole.dataApiBatch]: true,
    [queuesByRole.agentBatch]: true,
    [queuesByRole.finalize]: false,
  }, "only Finalize demand may remain after targeted Agent completion");
  const channelFinalizeJobs = (await inFlightJobs(queues[queuesByRole.finalize]))
    .filter((job) => jobTargetsChannel(job, channelId));
  assert.equal(
    channelFinalizeJobs.length,
    2,
    "the legacy generic Job must not represent the independent fenced recovery Job",
  );
  const recoveryFinalizeJob = channelFinalizeJobs.find(
    (job) => job.data.migration_system_retry_id === scenario.systemRetryId,
  );
  assert.ok(recoveryFinalizeJob);
  assert.notEqual(recoveryFinalizeJob.id, legacyGenericFinalize.id);
  assert.equal(recoveryFinalizeJob.name, "finalize-channel");
  assert.equal(recoveryFinalizeJob.data.run_id, runId);
  assert.equal(recoveryFinalizeJob.data.candidate_id, candidateId);
  assert.equal(recoveryFinalizeJob.data.dispatch_generation, 2);
  assert.equal(recoveryFinalizeJob.data.dispatch_batch_id, batchId);
  await legacyGenericFinalize.remove();
  await assertFrozenBatchAndScheduler(setup, batchId, completedScheduler);

  await runObservedWorkerJob({
    queueName: queuesByRole.finalize,
    queue: queues[queuesByRole.finalize],
    queueEvents: queueEvents[queuesByRole.finalize],
    connection,
    prefix,
    job: recoveryFinalizeJob,
    label: "targeted Finalize completion",
    processor: async (job) => {
      assert.equal(job.data.run_id, runId);
      await withTransaction(async (client) => {
        await client.query(
          `INSERT INTO crawler.finalized_profiles (
             channel_id,run_id,status,profile_json,quality_json,finalized_at,updated_at
           ) VALUES ($1,$2,'ready_auto','{}'::jsonb,'{}'::jsonb,now(),now())
           ON CONFLICT (channel_id) DO UPDATE
           SET run_id=EXCLUDED.run_id,status=EXCLUDED.status,
               profile_json=EXCLUDED.profile_json,quality_json=EXCLUDED.quality_json,
               finalized_at=EXCLUDED.finalized_at,updated_at=now()`,
          [channelId, runId],
        );
        await client.query(
          `UPDATE crawler.channel_runs
           SET status='done',detail_status='done',publication_finalized_status='ready_auto',
               publication_finalized_at=now(),finished_at=now(),updated_at=now()
           WHERE run_id=$1`,
          [runId],
        );
      });
      return { ok: true };
    },
  });

  await runControllerStartupTick({ prefix, label: "completed Scheduler terminal recovery" });
  await assertRecoveryQueueState(queues, {
    [queuesByRole.channelCrawl]: true,
    [queuesByRole.dataApiBatch]: true,
    [queuesByRole.agentBatch]: true,
    [queuesByRole.finalize]: false,
  }, "targeted queues must pause while independent Finalize recovery remains available");
  const terminal = await setup.query(
    `SELECT status,resolution,recovery_run_id,retry_dispatch_generation
     FROM crawler.migration_system_retry_items
     WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  );
  assert.deepEqual({
    ...terminal.rows[0],
    retry_dispatch_generation: Number(terminal.rows[0].retry_dispatch_generation),
  }, {
    status: "resolved",
    resolution: "recovery_finalized",
    recovery_run_id: runId,
    retry_dispatch_generation: 2,
  });
  await assertFrozenBatchAndScheduler(setup, batchId, completedScheduler);
  assert.equal(
    (await retainedJobsForChannel(queues[queuesByRole.agentBatch], channelId)).length,
    1,
    "exactly one Agent Job may represent the controlled retry",
  );
  assert.equal(
    (await retainedJobsForChannel(queues[queuesByRole.finalize], channelId)).length,
    1,
    "exactly one Finalize Job may represent the controlled retry",
  );
  for (const queueName of [
    queuesByRole.discoverPage,
    queuesByRole.contentDetail,
    queuesByRole.dataApiBatch,
  ]) {
    assert.equal((await inFlightJobs(queues[queueName])).length, 0, `${queueName} must remain empty`);
  }
});
