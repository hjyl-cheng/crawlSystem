import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import { runChannelCandidateWorkerJobWithDurableSettlement } from "../src/channelCandidateWorkerLifecycle.js";
import {
  claimContentDetailExecution,
  contentDetailExecutionFence,
  lockContentDetailExecution,
} from "../src/contentDetailExecutionFence.js";
import { loadIdentityPolicyCatalog } from "../src/identityPolicyCatalog.js";
import { markChannelCandidateJobAttemptActive } from "../src/managedWorkerJob.js";
import { MigrationSystemRetryRecoveryReconciler } from "../src/migrationSystemRetryRecovery.js";
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

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

function redisConnection(value) {
  const url = new URL(value);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
  return {
    host: url.hostname,
    port: Number(url.port || 6379),
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
    stream.on("data", (chunk) => {
      output = `${output}${chunk}`.slice(-30_000);
    });
  }
  child.once("exit", () => { exited = true; });
  return {
    output: () => output,
    waitFor(fragment, timeoutMs = 20_000) {
      return waitFor(() => {
        if (output.includes(fragment)) return true;
        if (exited) {
          throw new Error(`Controller exited before ${JSON.stringify(fragment)}\n${output}`);
        }
        return false;
      }, `Controller output ${JSON.stringify(fragment)}`, timeoutMs);
    },
  };
}

async function stopChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  try {
    await within(exited, "Controller shutdown", 10_000);
  } catch (error) {
    child.kill("SIGKILL");
    await within(exited, "forced Controller shutdown", 5_000);
    throw error;
  }
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return server.address().port;
}

async function closeServer(server) {
  if (!server.listening) return;
  await new Promise((resolve, reject) => server.close((error) => (
    error ? reject(error) : resolve()
  )));
}

function controllerEnvironment({ prefix, capacityUrl }) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = redisConnection(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "migration_recovery_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: prefix,
    ROTA_WORKLOAD_SCOPE_EXPECTED: "qy-production",
    ROTA_PROXY_CONTROL_URL: capacityUrl,
    ROTA_PROXY_CONTROL_TOKEN: "migration-recovery-test-token",
    ROTA_PROXY_CONTROL_TIMEOUT_MS: "1000",
    ROTA_PROXY_CONTROL_MAX_ATTEMPTS: "1",
    QUERY_METADATA_AUTO_CYCLE_ENABLED: "false",
    AGENT_BATCH_SIZE: "1",
    CONTROLLER_INTERVAL_MS: "300000",
    CONTROLLER_WAKEUP_DELAY_MS: "1000",
    CHANNEL_CANDIDATE_DISPATCH_ENABLED: "true",
    YOUTUBE_CHANNEL_INLINE_DETAILS: "true",
  };
}

async function runControllerTick(environment) {
  const child = spawn(
    process.execPath,
    [fileURLToPath(new URL("../src/controller.js", import.meta.url))],
    { env: environment, stdio: ["ignore", "pipe", "pipe"] },
  );
  const captured = captureChildOutput(child);
  try {
    await captured.waitFor("controller started");
    assert.doesNotMatch(captured.output(), /controller_tick_failed/);
    return captured.output();
  } finally {
    await stopChild(child);
  }
}

async function initializeScenario(client, {
  batchId,
  candidateId,
  channelId,
  g1JobId,
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
       failed_channel_count,result_json,discovery_closed_at,validation_closed_at,finished_at
     ) VALUES (
       $1,$1,'completed','completed_with_system_failures',100,100,99,0,1,
       '{"outcome":"completed_with_system_failures"}'::jsonb,now(),now(),now()
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
    [candidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`, g1JobId],
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
    [`migration-system-recovery:${batchId}`, candidateId, channelId, batchId],
  );
  const retry = await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'pending')
     RETURNING system_retry_id`,
    [Number(intent.rows[0].migration_intent_id), candidateId, batchId, g1JobId],
  );
  await client.query(
    `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
     VALUES ('query_scheduler',$1::jsonb,now())
     ON CONFLICT (setting_key) DO UPDATE
     SET value_json=EXCLUDED.value_json,updated_at=now()`,
    [JSON.stringify({
      status: "stopped",
      stop_reason: "pipeline_complete",
      pipeline_cycle_id: batchId,
      batch_outcome: "completed_with_system_failures",
    })],
  );
  return {
    migrationIntentId: Number(intent.rows[0].migration_intent_id),
    systemRetryId: Number(retry.rows[0].system_retry_id),
  };
}

test("a controlled system retry remains owned through deterministic Agent and Finalize recovery", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 90_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `completed-system-recovery:${suffix}`;
  const candidateId = 482;
  const channelId = `UCrecovery${suffix}`;
  const g1JobId = `channel-snapshot__${batchId}__${channelId}__g1`;
  const runId = `run:system-recovery:${suffix}`;
  const prefix = `migration-system-recovery-${suffix}`;
  const connection = redisConnection(redisUrl);
  const setup = new Client({ connectionString: databaseUrl });
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
  const events = Object.fromEntries(Object.keys(queues).map((queueName) => [
    queueName,
    new QueueEvents(queueName, { connection, prefix }),
  ]));
  let channelWorker;
  let contentDetailWorker;
  let agentWorker;
  let genericAgentWorker;
  let finalizeWorker;
  const policyCatalog = loadIdentityPolicyCatalog();
  const policyByRole = new Map(
    [...policyCatalog.policies.values()].map((policy) => [policy.role, policy]),
  );
  const capacityServer = createServer((request, response) => {
    if (request.method !== "GET" || request.url !== "/capacity") {
      response.writeHead(404).end();
      return;
    }
    const roles = Object.fromEntries([
      "discover",
      "channel",
      "query_quality",
      "detail",
    ].map((role) => {
      const policy = policyByRole.get(role) ?? null;
      return [role, {
        desired: 1,
        provisioned: 1,
        eligible: 1,
        assigned: 1,
        ready: 1,
        claimed: 1,
        reserve: 0,
        identity_policy_id: policy?.id ?? null,
        identity_policy_version: policy?.version ?? null,
        identity_policy_hash: policy?.hash ?? null,
      }];
    }));
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      ok: true,
      workload_scope: policyCatalog.workload_scope,
      catalog_version: policyCatalog.catalog_version,
      catalog_digest: policyCatalog.digest,
      active: 4,
      cooldown: 0,
      total: 4,
      reserve: 0,
      running: 0,
      minimum_reserve: 0,
      reserve_below_minimum: false,
      roles,
    }));
  });
  const capacityPort = await listen(capacityServer);
  const capacityUrl = `http://127.0.0.1:${capacityPort}`;

  await setup.connect();
  t.after(async () => {
    await Promise.all([
      channelWorker?.close().catch(() => {}),
      contentDetailWorker?.close().catch(() => {}),
      agentWorker?.close().catch(() => {}),
      genericAgentWorker?.close().catch(() => {}),
      finalizeWorker?.close().catch(() => {}),
    ]);
    await Promise.all(Object.values(queues).map((queue) => (
      queue.obliterate({ force: true }).catch(() => {})
    )));
    await Promise.all([
      ...Object.values(queues).map((queue) => queue.close().catch(() => {})),
      ...Object.values(events).map((queueEvents) => queueEvents.close().catch(() => {})),
    ]);
    await setup.query("DROP SCHEMA IF EXISTS feature_clock CASCADE").catch(() => {});
    await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await setup.end();
    await pool.end();
    await closeServer(capacityServer);
  });

  const scenario = await initializeScenario(setup, {
    batchId,
    candidateId,
    channelId,
    g1JobId,
  });
  await Promise.all([
    ...Object.values(queues).map((queue) => queue.waitUntilReady()),
    ...Object.values(events).map((queueEvents) => queueEvents.waitUntilReady()),
  ]);
  const reconciler = new MigrationSystemRetryRecoveryReconciler({
    query,
    withTransaction,
    queues,
  });

  const pending = await reconciler.reconcileAvailable({ limit: 10 });
  assert.deepEqual(pending.requiredQueues, []);
  assert.equal((await queues[queuesByRole.channelCrawl].getJobCounts("waiting")).waiting, 0);
  assert.equal((await queues[queuesByRole.agentBatch].getJobCounts("waiting")).waiting, 0);

  const allocation = await retryMigrationSystemFailure({
    systemRetryId: scenario.systemRetryId,
    withTransaction,
  });
  assert.equal(allocation.dispatch_generation, 2);
  const allocatedDemand = await reconciler.reconcileAvailable({ limit: 10 });
  assert.ok(allocatedDemand.requiredQueues.includes(queuesByRole.channelCrawl));
  await Promise.all([
    queues[queuesByRole.channelCrawl].pause(),
    queues[queuesByRole.contentDetail].pause(),
    queues[queuesByRole.dataApiBatch].pause(),
    queues[queuesByRole.agentBatch].pause(),
  ]);
  const controllerEnv = controllerEnvironment({ prefix, capacityUrl });
  const setScheduler = (status, stopReason) => query(
    `UPDATE crawler.settings
     SET value_json=$1::jsonb,updated_at=now()
     WHERE setting_key='query_scheduler'`,
    [JSON.stringify({
      status,
      stop_reason: stopReason,
      pipeline_cycle_id: batchId,
      completed_at: status === "stopped" ? new Date().toISOString() : null,
      updated_by: "migration-system-recovery-test",
    })],
  );

  await setScheduler("paused", null);
  await runControllerTick(controllerEnv);
  assert.equal(await queues[queuesByRole.channelCrawl].isPaused(), true);
  assert.equal(await queues[queuesByRole.agentBatch].isPaused(), true);

  await setScheduler("stopped", "user_requested");
  await runControllerTick(controllerEnv);
  assert.equal(await queues[queuesByRole.channelCrawl].isPaused(), true);
  assert.equal(await queues[queuesByRole.agentBatch].isPaused(), true);

  await setScheduler("stopped", "pipeline_complete");
  await runControllerTick(controllerEnv);
  assert.equal(await queues[queuesByRole.channelCrawl].isPaused(), false);
  assert.equal(await queues[queuesByRole.contentDetail].isPaused(), true);
  assert.equal(await queues[queuesByRole.dataApiBatch].isPaused(), true);
  assert.equal(await queues[queuesByRole.agentBatch].isPaused(), true);
  const controllerDispatchedChannelJobs = await queues[queuesByRole.channelCrawl].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused"],
    0,
    100,
    true,
  );
  assert.equal(controllerDispatchedChannelJobs.length, 1);
  assert.equal(controllerDispatchedChannelJobs[0].id, allocation.outbox.deterministic_job_id);
  assert.equal(controllerDispatchedChannelJobs[0].name, "channel-snapshot");
  assert.equal(Number(controllerDispatchedChannelJobs[0].data.candidate_id), candidateId);
  assert.deepEqual((await query(
    `SELECT status FROM crawler.proxy_job_dispatch_outbox WHERE dispatch_id=$1`,
    [allocation.outbox.dispatch_id],
  )).rows[0], { status: "sent" });
  for (const queueName of [
    queuesByRole.discoverPage,
    queuesByRole.contentDetail,
    queuesByRole.dataApiBatch,
    queuesByRole.agentBatch,
    queuesByRole.finalize,
  ]) {
    const jobs = await queues[queueName].getJobs(
      ["waiting", "active", "delayed", "prioritized", "paused"],
      0,
      100,
      true,
    );
    assert.equal(jobs.length, 0, `Controller unexpectedly produced ${queueName} work`);
  }

  const pendingCandidateId = candidateId + 100;
  const pendingChannelId = `${channelId}pending`;
  const pendingRunId = `${runId}:pending`;
  const pendingJobId = `pending-system-failure-${suffix}`;
  await query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
       snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES (
       $1,$2,$2,$3,$4,'accepted',1,$5,1,
       jsonb_build_object(
         'failure_type','retryable_system_failure',
         'failed_dispatch_batch_id',$2::text,
         'system_failure',jsonb_build_object('code','LEASE_CONFLICT','category','lease')
       ),'{}'::jsonb,now(),now()
     )`,
    [
      pendingCandidateId,
      batchId,
      pendingChannelId,
      `https://www.youtube.com/channel/${pendingChannelId}`,
      pendingJobId,
    ],
  );
  const pendingIntent = await query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
     ) VALUES (
       $1,current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
       $3,'{}'::jsonb,repeat('d',64),$2,$4,1,now()
     ) RETURNING migration_intent_id`,
    [`migration-pending-agent:${suffix}`, pendingCandidateId, pendingChannelId, batchId],
  );
  await query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'pending')`,
    [Number(pendingIntent.rows[0].migration_intent_id), pendingCandidateId, batchId, pendingJobId],
  );
  await withTransaction(async (client) => {
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,
         registry_promotion_run_id
       ) VALUES ($1,$2,'Pending controlled retry',2000,'active',true,'pending',$3,$4,$3)`,
      [
        pendingChannelId,
        `https://www.youtube.com/channel/${pendingChannelId}`,
        pendingRunId,
        pendingCandidateId,
      ],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES ($1,$2,$3,'waiting_agent','full','done',0,now(),$4::jsonb)`,
      [pendingRunId, pendingChannelId, pendingCandidateId, JSON.stringify({
        dispatch_batch_id: batchId,
        pipeline_cycle_id: batchId,
      })],
    );
  });
  await query(
    `UPDATE crawler.agent_configs
     SET provider='local-offline',batch_size=1,min_batch_size=1,max_workers=1,
         enabled=true,updated_at=now()`,
  );
  let genericAgentExecutions = 0;
  genericAgentWorker = new Worker(queuesByRole.agentBatch, async () => {
    genericAgentExecutions += 1;
    return { ok: true };
  }, { connection, prefix, concurrency: 1 });
  genericAgentWorker.on("error", () => {});
  await genericAgentWorker.waitUntilReady();
  await setScheduler("finishing", "upstream_drained");
  await runControllerTick(controllerEnv);
  assert.equal((await query(
    `SELECT agent_status FROM crawler.channels WHERE channel_id=$1`,
    [pendingChannelId],
  )).rows[0].agent_status, "pending");
  assert.equal(genericAgentExecutions, 0);
  assert.equal((await queues[queuesByRole.agentBatch].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "completed", "failed"],
    0,
    100,
    true,
  )).length, 0);
  await genericAgentWorker.close();
  genericAgentWorker = null;
  await setScheduler("stopped", "pipeline_complete");
  await query(
    `UPDATE crawler.migration_system_retry_items
     SET status='resolved',resolution='job_completed',resolved_at=now(),updated_at=now()
     WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  );
  const legacyReopened = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(legacyReopened.legacyReopened, 1);
  assert.ok(legacyReopened.requiredQueues.includes(queuesByRole.channelCrawl));
  assert.deepEqual((await query(
    `SELECT status,resolution,resolved_at
     FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0], { status: "dispatched", resolution: null, resolved_at: null });

  channelWorker = new Worker(queuesByRole.channelCrawl, async (job) => {
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
           ) VALUES ($1,$2,$3,'waiting_detail','full','queued',1,now(),$4::jsonb)`,
          [runId, channelId, candidateId, JSON.stringify({
            dispatch_batch_id: batchId,
            pipeline_cycle_id: batchId,
            content_max_age_days: 90,
          })],
        );
        await client.query(
          `INSERT INTO crawler.content_candidates (
             run_id,channel_id,source_content_id,position,content_type,
             type_status,type_source,detail_status,api_status,result_json
           ) VALUES (
             $1,$2,$3,1,'video','resolved','uploads_playlist',
             'queued','not_needed','{}'::jsonb
           )`,
          [runId, channelId, `detail-video-${suffix}`],
        );
        return { accepted: true, run_id: runId };
      }),
    });
  }, { connection, prefix, concurrency: 1 });
  channelWorker.on("error", () => {});
  await channelWorker.waitUntilReady();
  const channelJob = await queues[queuesByRole.channelCrawl].getJob(
    allocation.outbox.deterministic_job_id,
  );
  await within(
    channelJob.waitUntilFinished(events[queuesByRole.channelCrawl]),
    "G+1 Channel completion",
  );

  const afterChannel = (await query(
    `SELECT retry.status,retry.resolution,candidate.status AS candidate_status,
            candidate.snapshot_active_job_id,run.status AS run_status,run.detail_status,
            channel.agent_status
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_candidates candidate ON candidate.candidate_id=retry.candidate_id
     JOIN crawler.channels channel ON channel.registry_promotion_candidate_id=candidate.candidate_id
     JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
     WHERE retry.system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0];
  assert.deepEqual(afterChannel, {
    status: "dispatched",
    resolution: null,
    candidate_status: "accepted",
    snapshot_active_job_id: null,
    run_status: "waiting_detail",
    detail_status: "queued",
    agent_status: "pending",
  });

  const firstDetailRecovery = await reconciler.reconcileAvailable({ limit: 10 });
  assert.ok(firstDetailRecovery.requiredQueues.includes(queuesByRole.contentDetail));
  assert.equal(firstDetailRecovery.detailEnqueued, 1);
  const detailJobId = safeJobId("content-detail", runId);
  const firstDetailJob = await queues[queuesByRole.contentDetail].getJob(detailJobId);
  assert.ok(firstDetailJob);
  assert.equal(firstDetailJob.name, "content-detail-batch");
  assert.deepEqual(firstDetailJob.data, {
    channel_id: channelId,
    run_id: runId,
    migration_system_retry_id: scenario.systemRetryId,
    candidate_id: candidateId,
    dispatch_generation: 2,
    dispatch_batch_id: batchId,
    pipeline_cycle_id: batchId,
    content_max_age_days: 90,
  });
  await runControllerTick(controllerEnv);
  assert.equal(await queues[queuesByRole.contentDetail].isPaused(), false);

  contentDetailWorker = new Worker(queuesByRole.contentDetail, async () => {
    throw new Error("synthetic terminal detail failure");
  }, { connection, prefix, concurrency: 1 });
  contentDetailWorker.on("error", () => {});
  await contentDetailWorker.waitUntilReady();
  await assert.rejects(
    within(
      firstDetailJob.waitUntilFinished(events[queuesByRole.contentDetail]),
      "terminal Content Detail failure",
    ),
    /synthetic terminal detail failure/,
  );
  await contentDetailWorker.close();
  contentDetailWorker = null;

  const orphanedDetailFence = contentDetailExecutionFence({
    id: firstDetailJob.id,
    name: firstDetailJob.name,
    attemptsStarted: 2,
    data: firstDetailJob.data,
  });
  await query(
    `UPDATE crawler.channel_runs
     SET detail_active_job_id=$2,detail_active_job_attempt=$3,
         detail_active_scope_key=$4,updated_at=now()
     WHERE run_id=$1`,
    [runId, orphanedDetailFence.jobId, 2, orphanedDetailFence.scopeKey],
  );

  const terminalDetailRecovery = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(terminalDetailRecovery.detailEnqueued, 1);
  assert.equal(terminalDetailRecovery.terminalJobsRequeued, 1);
  const requeuedDetailJob = await queues[queuesByRole.contentDetail].getJob(detailJobId);
  assert.ok(requeuedDetailJob);
  assert.equal(await requeuedDetailJob.getState(), "waiting");
  assert.deepEqual((await query(
    `SELECT detail_active_job_id,detail_active_job_attempt,detail_active_scope_key
     FROM crawler.channel_runs WHERE run_id=$1`,
    [runId],
  )).rows[0], {
    detail_active_job_id: null,
    detail_active_job_attempt: null,
    detail_active_scope_key: null,
  });
  const idempotentDetailRecovery = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(idempotentDetailRecovery.detailEnqueued, 0);
  assert.equal((await queues[queuesByRole.contentDetail].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "completed", "failed"],
    0,
    10,
    true,
  )).filter((job) => job.id === detailJobId).length, 1);

  contentDetailWorker = new Worker(queuesByRole.contentDetail, async (job) => {
    assert.equal(job.id, detailJobId);
    assert.deepEqual(job.data, firstDetailJob.data);
    assert.equal(job.attemptsStarted, 1);
    const executionFence = contentDetailExecutionFence(job);
    assert.ok(await withTransaction((client) => (
      claimContentDetailExecution(client, executionFence)
    )));
    await withTransaction(async (client) => {
      assert.ok(await lockContentDetailExecution(client, executionFence));
      await client.query(
        `UPDATE crawler.content_candidates
         SET detail_status='unavailable',api_status='unavailable',finished_at=now(),updated_at=now()
         WHERE run_id=$1`,
        [runId],
      );
      await client.query(
        `UPDATE crawler.channel_runs
         SET status='waiting_agent',detail_status='done',updated_at=now()
         WHERE run_id=$1`,
        [runId],
      );
    });
    return { ok: true };
  }, { connection, prefix, concurrency: 1 });
  contentDetailWorker.on("error", () => {});
  await contentDetailWorker.waitUntilReady();
  await within(
    requeuedDetailJob.waitUntilFinished(events[queuesByRole.contentDetail]),
    "requeued Content Detail completion",
  );
  await contentDetailWorker.close();
  contentDetailWorker = null;

  await withTransaction(async (client) => {
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at,updated_at
       ) VALUES ($1,$2,'ready_partial','{}'::jsonb,'{}'::jsonb,now(),now())`,
      [channelId, runId],
    );
    await client.query(
      `UPDATE crawler.channel_runs
       SET status='done',detail_status='done',publication_finalized_status='ready_partial',
           publication_finalized_at=now(),finished_at=now(),updated_at=now()
       WHERE run_id=$1`,
      [runId],
    );
  });

  const agentJobId = safeJobId(
    "migration-system-retry-agent",
    scenario.systemRetryId,
    "g2",
    runId,
  );
  const conflictingAgentJob = await queues[queuesByRole.agentBatch].add(
    "agent-profile-batch",
    {
      migration_system_retry_id: scenario.systemRetryId,
      recovery_agent_job_epoch: 0,
      dispatch_generation: 2,
      dispatch_batch_id: batchId,
      run_id: `${runId}:wrong`,
      channel_ids: [channelId],
    },
    { jobId: agentJobId },
  );
  const conflictingAgentReconcile = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(conflictingAgentReconcile.resolved, 0);
  assert.equal(conflictingAgentReconcile.queueConflicts, 1);
  assert.ok(conflictingAgentReconcile.requiredQueues.includes(queuesByRole.agentBatch));
  await conflictingAgentJob.remove();

  const firstAgentReconcile = await reconciler.reconcileAvailable({ limit: 10 });
  const secondAgentReconcile = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(firstAgentReconcile.resolved, 0);
  assert.ok(firstAgentReconcile.requiredQueues.includes(queuesByRole.agentBatch));
  assert.ok(secondAgentReconcile.requiredQueues.includes(queuesByRole.agentBatch));
  assert.equal((await query(
    `SELECT status FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0].status, "dispatched");
  const agentJobs = await queues[queuesByRole.agentBatch].getJobs(["waiting"], 0, 10, true);
  assert.equal(agentJobs.length, 1);
  const firstAllocatedAgentJobId = safeJobId(
    "migration-system-retry-agent",
    scenario.systemRetryId,
    "g2",
    runId,
    "e1",
  );
  assert.equal(agentJobs[0].id, firstAllocatedAgentJobId);
  assert.deepEqual(agentJobs[0].data, {
    batch_id: `migration-system-retry:${scenario.systemRetryId}:g2:${runId}`,
    channel_ids: [channelId],
    agent_mode: "basic",
    migration_system_retry_id: scenario.systemRetryId,
    recovery_agent_job_epoch: 1,
    candidate_id: candidateId,
    dispatch_generation: 2,
    pipeline_cycle_id: batchId,
    dispatch_batch_id: batchId,
    run_id: runId,
  });
  await runControllerTick(controllerEnv);
  assert.equal(await queues[queuesByRole.agentBatch].isPaused(), false);
  assert.equal((await queues[queuesByRole.agentBatch].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused"],
    0,
    100,
    true,
  )).length, 1);

  agentWorker = new Worker(queuesByRole.agentBatch, async (job) => {
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
         )
         ON CONFLICT (channel_id,agent_mode) DO UPDATE
         SET status='success',metrics_json=EXCLUDED.metrics_json,
             attempts=crawler.agent_profiles.attempts+1,error_message=NULL,updated_at=now()`,
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
  }, { connection, prefix, concurrency: 1 });
  agentWorker.on("error", () => {});
  await agentWorker.waitUntilReady();
  await within(
    agentJobs[0].waitUntilFinished(events[queuesByRole.agentBatch]),
    "targeted Agent completion",
  );

  await queues[queuesByRole.agentBatch].pause();
  await query(
    `UPDATE crawler.channels SET agent_status='failed',updated_at=now() WHERE channel_id=$1`,
    [channelId],
  );
  const retainedAgentRecovery = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(retainedAgentRecovery.terminalJobsRequeued, 1);
  assert.equal(retainedAgentRecovery.agentEnqueued, 1);
  const requeuedAgentJobId = safeJobId(
    "migration-system-retry-agent",
    scenario.systemRetryId,
    "g2",
    runId,
    "e2",
  );
  const requeuedAgentJob = await queues[queuesByRole.agentBatch].getJob(requeuedAgentJobId);
  assert.ok(requeuedAgentJob);
  assert.notEqual(requeuedAgentJob.id, agentJobId);
  assert.equal(requeuedAgentJob.data.recovery_agent_job_epoch, 2);
  assert.ok(["waiting", "paused"].includes(await requeuedAgentJob.getState()));
  assert.deepEqual((await query(
    `SELECT recovery_agent_job_epoch,recovery_agent_active_job_id,
            recovery_agent_active_job_attempt
     FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0], {
    recovery_agent_job_epoch: "2",
    recovery_agent_active_job_id: null,
    recovery_agent_active_job_attempt: null,
  });
  await queues[queuesByRole.agentBatch].resume();
  await within(
    requeuedAgentJob.waitUntilFinished(events[queuesByRole.agentBatch]),
    "retained completed Agent recovery",
  );

  const firstFinalizeReconcile = await reconciler.reconcileAvailable({ limit: 10 });
  assert.ok(firstFinalizeReconcile.requiredQueues.includes(queuesByRole.finalize));
  const finalizeJobs = await queues[queuesByRole.finalize].getJobs(["waiting"], 0, 10, true);
  assert.equal(finalizeJobs.length, 1);
  assert.deepEqual(finalizeJobs[0].data, {
    channel_id: channelId,
    run_id: runId,
    reason: "migration-system-retry-recovery",
    source_revision: finalizeJobs[0].data.source_revision,
    migration_system_retry_id: scenario.systemRetryId,
    candidate_id: candidateId,
    dispatch_generation: 2,
    pipeline_cycle_id: batchId,
    dispatch_batch_id: batchId,
  });
  assert.match(finalizeJobs[0].data.source_revision, /^[a-f0-9]{64}$/);
  await finalizeJobs[0].remove();
  const productionFinalize = await queues[queuesByRole.finalize].add(
    "finalize-channel",
    {
      channel_id: channelId,
      run_id: runId,
      reason: "agent-complete",
      source_revision: finalizeJobs[0].data.source_revision,
      pipeline_cycle_id: batchId,
    },
    { jobId: safeJobId("finalize-reconcile", runId, "legacy-controller") },
  );
  assert.notEqual(productionFinalize.id, finalizeJobs[0].id);
  assert.equal(productionFinalize.data.migration_system_retry_id, undefined);
  const secondFinalizeReconcile = await reconciler.reconcileAvailable({ limit: 10 });
  assert.ok(secondFinalizeReconcile.requiredQueues.includes(queuesByRole.finalize));
  assert.equal(secondFinalizeReconcile.finalizeEnqueued, 1);
  const representedFinalizeJobs = await queues[queuesByRole.finalize].getJobs(
    ["waiting"],
    0,
    10,
    true,
  );
  assert.equal(representedFinalizeJobs.length, 2);
  const recoveryFinalize = representedFinalizeJobs.find(
    (job) => job.data.migration_system_retry_id === scenario.systemRetryId,
  );
  assert.ok(recoveryFinalize, "generic Finalize must not represent the recovery Fence");
  assert.equal(recoveryFinalize.data.candidate_id, candidateId);
  assert.equal(recoveryFinalize.data.dispatch_generation, 2);
  assert.equal(recoveryFinalize.data.dispatch_batch_id, batchId);
  await productionFinalize.remove();

  const finalizeProcessor = async (job) => {
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
             publication_finalized_at=COALESCE(publication_finalized_at,now()),
             finished_at=now(),updated_at=now()
         WHERE run_id=$1`,
        [runId],
      );
    });
    return { ok: true };
  };
  finalizeWorker = new Worker(queuesByRole.finalize, finalizeProcessor, {
    connection,
    prefix,
    concurrency: 1,
  });
  finalizeWorker.on("error", () => {});
  await finalizeWorker.waitUntilReady();
  await within(
    recoveryFinalize.waitUntilFinished(events[queuesByRole.finalize]),
    "targeted Finalize completion",
  );

  await finalizeWorker.close();
  finalizeWorker = null;
  await query(
    `UPDATE crawler.content_candidates
     SET updated_at=clock_timestamp()
     WHERE run_id=$1`,
    [runId],
  );
  const lateCandidateRevision = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(lateCandidateRevision.resolved, 0);
  assert.equal(lateCandidateRevision.finalizeEnqueued, 1);
  assert.equal((await query(
    `SELECT status FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0].status, "dispatched");
  const refreshedFinalizeJobs = await queues[queuesByRole.finalize].getJobs(
    ["waiting"],
    0,
    10,
    true,
  );
  assert.equal(refreshedFinalizeJobs.length, 1);
  assert.notEqual(
    refreshedFinalizeJobs[0].data.source_revision,
    finalizeJobs[0].data.source_revision,
  );
  finalizeWorker = new Worker(queuesByRole.finalize, finalizeProcessor, {
    connection,
    prefix,
    concurrency: 1,
  });
  finalizeWorker.on("error", () => {});
  await finalizeWorker.waitUntilReady();
  await within(
    refreshedFinalizeJobs[0].waitUntilFinished(events[queuesByRole.finalize]),
    "refreshed Finalize completion",
  );

  const terminal = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(terminal.resolved, 1);
  const final = (await query(
    `SELECT retry.status AS retry_status,retry.resolution,
            candidate.status AS candidate_status,
            candidate.snapshot_dispatch_generation::int AS generation,
            candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            channel.agent_status,run.status AS run_status,run.detail_status,
            run.publication_finalized_status,run.publication_finalized_at IS NOT NULL AS finalized,
            finalized.run_id AS finalized_run_id,batch.status AS batch_status,batch.outcome,
            batch.total_channel_count,batch.accepted_channel_count,
            batch.rejected_channel_count,batch.failed_channel_count,
            scheduler.value_json->>'status' AS scheduler_status
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_candidates candidate ON candidate.candidate_id=retry.candidate_id
     JOIN crawler.channels channel ON channel.registry_promotion_candidate_id=candidate.candidate_id
     JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
     JOIN crawler.finalized_profiles finalized ON finalized.channel_id=channel.channel_id
     JOIN crawler.query_dispatch_batches batch
       ON batch.dispatch_batch_id=retry.failed_dispatch_batch_id
     JOIN crawler.settings scheduler ON scheduler.setting_key='query_scheduler'
     WHERE retry.system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0];
  assert.deepEqual(final, {
    retry_status: "resolved",
    resolution: "recovery_finalized",
    candidate_status: "accepted",
    generation: 2,
    snapshot_active_job_id: null,
    snapshot_active_job_attempt: null,
    agent_status: "done",
    run_status: "done",
    detail_status: "done",
    publication_finalized_status: "ready_auto",
    finalized: true,
    finalized_run_id: runId,
    batch_status: "completed",
    outcome: "completed_with_system_failures",
    total_channel_count: 100,
    accepted_channel_count: 99,
    rejected_channel_count: 0,
    failed_channel_count: 1,
    scheduler_status: "stopped",
  });

  await query(
    `UPDATE crawler.migration_system_retry_items
     SET resolution='job_completed',updated_at=now()
     WHERE system_retry_id=$1 AND status='resolved'`,
    [scenario.systemRetryId],
  );
  const normalizedLegacyTerminal = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(normalizedLegacyTerminal.legacyNormalized, 1);
  assert.equal(normalizedLegacyTerminal.legacyReopened, 0);
  assert.deepEqual((await query(
    `SELECT status,resolution
     FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0], { status: "resolved", resolution: "recovery_finalized" });

  const agentJobsBeforeBusinessOutcomes = (await queues[queuesByRole.agentBatch].getJobs(
    ["waiting", "active", "delayed", "completed", "failed"],
    0,
    100,
    true,
  )).length;
  const finalizeJobsBeforeBusinessOutcomes = (await queues[queuesByRole.finalize].getJobs(
    ["waiting", "active", "delayed", "completed", "failed"],
    0,
    100,
    true,
  )).length;
  for (const [offset, candidateStatus] of ["rejected", "existing"].entries()) {
    const businessCandidateId = candidateId + offset + 1;
    const businessChannelId = `${channelId}${candidateStatus}`;
    await query(
      `INSERT INTO crawler.channel_candidates (
         candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
         snapshot_json,source_json,validation_finished_at
       ) VALUES ($1,$2,$2,$3,$4,$5,2,NULL,NULL,'{}'::jsonb,'{}'::jsonb,now())`,
      [
        businessCandidateId,
        batchId,
        businessChannelId,
        `https://www.youtube.com/channel/${businessChannelId}`,
        candidateStatus,
      ],
    );
    const businessIntent = await query(
      `INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) VALUES (
         $1,current_database(),
         (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
         $3,'{}'::jsonb,repeat($4,64),$2,$5,2,now()
       ) RETURNING migration_intent_id`,
      [
        `migration-terminal-business:${candidateStatus}:${suffix}`,
        businessCandidateId,
        businessChannelId,
        candidateStatus === "rejected" ? "b" : "c",
        batchId,
      ],
    );
    await query(
      `INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,retry_dispatch_generation
       ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'dispatched',2)`,
      [
        Number(businessIntent.rows[0].migration_intent_id),
        businessCandidateId,
        batchId,
        `terminal-business-${candidateStatus}-${suffix}`,
      ],
    );
  }

  const terminalBusinessOutcomes = await reconciler.reconcileAvailable({ limit: 10 });
  assert.equal(terminalBusinessOutcomes.resolved, 2);
  assert.deepEqual(terminalBusinessOutcomes.requiredQueues, []);
  assert.deepEqual((await query(
    `SELECT candidate.status AS candidate_status,retry.status,retry.resolution
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_candidates candidate ON candidate.candidate_id=retry.candidate_id
     WHERE candidate.candidate_id=ANY($1::bigint[])
     ORDER BY candidate.candidate_id`,
    [[candidateId + 1, candidateId + 2]],
  )).rows, [
    {
      candidate_status: "rejected",
      status: "resolved",
      resolution: "recovery_terminal_business_outcome",
    },
    {
      candidate_status: "existing",
      status: "resolved",
      resolution: "recovery_terminal_business_outcome",
    },
  ]);
  assert.equal((await queues[queuesByRole.agentBatch].getJobs(
    ["waiting", "active", "delayed", "completed", "failed"],
    0,
    100,
    true,
  )).length, agentJobsBeforeBusinessOutcomes);
  assert.equal((await queues[queuesByRole.finalize].getJobs(
    ["waiting", "active", "delayed", "completed", "failed"],
    0,
    100,
    true,
  )).length, finalizeJobsBeforeBusinessOutcomes);

  const supersededCandidateId = candidateId + 200;
  const supersededRetry = await query(
    `WITH inserted_candidate AS (
       INSERT INTO crawler.channel_candidates (
         candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_json,source_json,validation_finished_at
       ) VALUES ($1,$2,$2,$3,$4,'failed',2,'{}'::jsonb,'{}'::jsonb,now())
       RETURNING candidate_id
     ), inserted_intent AS (
       INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) SELECT $5,current_database(),
                (SELECT oid FROM pg_database WHERE datname=current_database()),
                candidate_id,$3,'{}'::jsonb,repeat('e',64),candidate_id,$2,2,now()
         FROM inserted_candidate
       RETURNING migration_intent_id,target_candidate_id
     )
     INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) SELECT migration_intent_id,target_candidate_id,$2,1,$6,1,
              'LEASE_CONFLICT','lease','{}'::jsonb,'retrying'
       FROM inserted_intent
     RETURNING system_retry_id`,
    [
      supersededCandidateId,
      batchId,
      `${channelId}superseded`,
      `https://www.youtube.com/channel/${channelId}superseded`,
      `migration-superseded:${suffix}`,
      `superseded-job:${suffix}`,
    ],
  );
  const superseded = await reconciler.reconcileAvailable({ limit: 10 });
  assert.ok(superseded.resolved >= 1);
  assert.deepEqual((await query(
    `SELECT status,resolution
     FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [Number(supersededRetry.rows[0].system_retry_id)],
  )).rows[0], {
    status: "resolved",
    resolution: "recovery_fence_superseded",
  });

  const ownedCandidateId = candidateId + 201;
  const ownedRetries = await query(
    `WITH inserted_candidate AS (
       INSERT INTO crawler.channel_candidates (
         candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
         snapshot_json,source_json,validation_finished_at
       ) VALUES ($1,$2,$2,$3,$4,'failed',1,$6,1,'{}'::jsonb,'{}'::jsonb,now())
       RETURNING candidate_id
     ), inserted_intent AS (
       INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) SELECT $5,current_database(),
                (SELECT oid FROM pg_database WHERE datname=current_database()),
                candidate_id,$3,'{}'::jsonb,repeat('f',64),candidate_id,$2,1,now()
         FROM inserted_candidate
       RETURNING migration_intent_id,target_candidate_id
     ), legacy AS (
       INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,resolution,resolved_at
       ) SELECT migration_intent_id,target_candidate_id,$2,1,$7,1,
                'LEASE_CONFLICT','lease','{}'::jsonb,'resolved','job_completed',now()
         FROM inserted_intent
       RETURNING system_retry_id
     ), active AS (
       INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status
       ) SELECT migration_intent_id,target_candidate_id,$2,1,$6,1,
                'LEASE_CONFLICT','lease','{}'::jsonb,'pending'
         FROM inserted_intent
       RETURNING system_retry_id
     )
     SELECT legacy.system_retry_id AS legacy_retry_id,
            active.system_retry_id AS active_retry_id
     FROM legacy,active`,
    [
      ownedCandidateId,
      batchId,
      `${channelId}owned`,
      `https://www.youtube.com/channel/${channelId}owned`,
      `migration-owned:${suffix}`,
      `active-owner-job:${suffix}`,
      `legacy-owner-job:${suffix}`,
    ],
  );
  const owned = await reconciler.reconcileAvailable({ limit: 10 });
  assert.ok(owned.legacyNormalized >= 1);
  assert.deepEqual((await query(
    `SELECT status,resolution
     FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [Number(ownedRetries.rows[0].legacy_retry_id)],
  )).rows[0], {
    status: "resolved",
    resolution: "recovery_superseded_by_active_retry",
  });
  assert.equal((await query(
    `SELECT status FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [Number(ownedRetries.rows[0].active_retry_id)],
  )).rows[0].status, "pending");

  const backlogCandidateId = candidateId + 202;
  const fairLegacyCandidateId = candidateId + 203;
  const fairness = await query(
    `WITH backlog_candidate AS (
       INSERT INTO crawler.channel_candidates (
         candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_json,source_json,validation_finished_at
       ) VALUES ($1,$3,$3,$4,$5,'failed',1,'{}'::jsonb,'{}'::jsonb,now())
       RETURNING candidate_id
     ), backlog_intent AS (
       INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) SELECT $6,current_database(),
                (SELECT oid FROM pg_database WHERE datname=current_database()),
                candidate_id,$4,'{}'::jsonb,repeat('1',64),candidate_id,$3,2,now()
         FROM backlog_candidate
       RETURNING migration_intent_id,target_candidate_id
     ), backlog_retry AS (
       INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status
       ) SELECT migration_intent_id,target_candidate_id,$3,2,$7,1,
                'LEASE_CONFLICT','lease','{}'::jsonb,'retrying'
         FROM backlog_intent
       RETURNING system_retry_id
     ), legacy_candidate AS (
       INSERT INTO crawler.channel_candidates (
         candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_json,source_json,validation_finished_at
       ) VALUES ($2,$3,$3,$8,$9,'failed',2,'{}'::jsonb,'{}'::jsonb,now())
       RETURNING candidate_id
     ), legacy_intent AS (
       INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) SELECT $10,current_database(),
                (SELECT oid FROM pg_database WHERE datname=current_database()),
                candidate_id,$8,'{}'::jsonb,repeat('2',64),candidate_id,$3,2,now()
         FROM legacy_candidate
       RETURNING migration_intent_id,target_candidate_id
     ), legacy_retry AS (
       INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,resolution,resolved_at
       ) SELECT migration_intent_id,target_candidate_id,$3,1,$11,1,
                'LEASE_CONFLICT','lease','{}'::jsonb,'resolved','job_completed',now()
         FROM legacy_intent
       RETURNING system_retry_id
     )
     SELECT backlog_retry.system_retry_id AS backlog_retry_id,
            legacy_retry.system_retry_id AS legacy_retry_id
     FROM backlog_retry,legacy_retry`,
    [
      backlogCandidateId,
      fairLegacyCandidateId,
      batchId,
      `${channelId}backlog`,
      `https://www.youtube.com/channel/${channelId}backlog`,
      `migration-backlog:${suffix}`,
      `backlog-job:${suffix}`,
      `${channelId}fairlegacy`,
      `https://www.youtube.com/channel/${channelId}fairlegacy`,
      `migration-fair-legacy:${suffix}`,
      `fair-legacy-job:${suffix}`,
    ],
  );
  const fairReconciler = new MigrationSystemRetryRecoveryReconciler({
    query,
    withTransaction,
    queues,
  });
  const fairnessPasses = [];
  for (let index = 0; index < 5; index += 1) {
    fairnessPasses.push(await fairReconciler.reconcileAvailable({ limit: 1 }));
  }
  assert.deepEqual(fairnessPasses.map((pass) => pass.legacyNormalized), [0, 0, 0, 0, 1]);
  assert.deepEqual((await query(
    `SELECT system_retry_id,status,resolution
     FROM crawler.migration_system_retry_items
     WHERE system_retry_id=ANY($1::bigint[])
     ORDER BY system_retry_id`,
    [[
      Number(fairness.rows[0].backlog_retry_id),
      Number(fairness.rows[0].legacy_retry_id),
    ]],
  )).rows, [
    {
      system_retry_id: fairness.rows[0].backlog_retry_id,
      status: "retrying",
      resolution: null,
    },
    {
      system_retry_id: fairness.rows[0].legacy_retry_id,
      status: "resolved",
      resolution: "recovery_fence_superseded",
    },
  ]);
});
