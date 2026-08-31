import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import {
  prepareMigrationSystemRetryAgentJobRequeue,
  lockMigrationSystemRetryAgentJobFence,
  migrationSystemRetryAgentJobFence,
} from "../src/migrationSystemRetryRecovery.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { queuesByRole } from "../src/queues.js";

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
  assert.equal(url.search, "");
  assert.equal(url.hash, "");
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

async function within(promise, label, timeoutMs = 15_000) {
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

async function waitFor(check, label, timeoutMs = 15_000) {
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
      output = `${output}${chunk}`.slice(-20_000);
    });
  }
  child.once("exit", () => { exited = true; });
  return {
    output: () => output,
    waitFor: (fragment, timeoutMs = 15_000) => waitFor(() => {
      if (output.includes(fragment)) return true;
      if (exited) {
        throw new Error(`Worker exited before ${JSON.stringify(fragment)}\n${output}`);
      }
      return false;
    }, `Worker output ${JSON.stringify(fragment)}`, timeoutMs),
  };
}

async function stopChild(child) {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  try {
    await within(exited, "Worker shutdown", 10_000);
  } catch (gracefulError) {
    child.kill("SIGKILL");
    await within(exited, "forced Worker shutdown", 5_000).catch(() => {});
    throw gracefulError;
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
  server.closeIdleConnections?.();
  server.closeAllConnections?.();
  await new Promise((resolve, reject) => server.close((error) => (
    error ? reject(error) : resolve()
  )));
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

function workerEnvironment({ prefix, stalled = false }) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "full_agent_guard_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: prefix,
    WORKER_QUEUES: queuesByRole.agentBatch,
    PROXY_SLOT_ROLE: "",
    ROTA_PROXY_CONTROL_URL: "",
    ROTA_PROXY_CONTROL_TOKEN: "",
    ROTA_FIXED_PROXY_USER: "",
    ROTA_BULLMQ_PROXY_PASSWORD: "",
    AGENT_API_KEY: "",
    S3_ENDPOINT: "",
    S3_ACCESS_KEY: "",
    S3_SECRET_KEY: "",
    BULLMQ_LOCK_DURATION_MS: stalled ? "500" : "",
    BULLMQ_STALLED_INTERVAL_MS: stalled ? "500" : "",
    BULLMQ_SKIP_LOCK_RENEWAL: "false",
  };
}

function completeAgentRow(inputUrl) {
  const ageRanges = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"];
  return {
    input_url: inputUrl,
    country: "Brazil",
    creator_gender: "brand_team",
    creator_age_range: 30,
    creator_language: "Portuguese",
    audience_region: [
      { region: "Brazil", percentage: 50 },
      { region: "Portugal", percentage: 15 },
      { region: "United States", percentage: 10 },
      { region: "Mexico", percentage: 10 },
      { region: "Argentina", percentage: 5 },
      { region: "Other", percentage: 10 },
    ],
    audience_age_gender: ageRanges.map((ageRange, index) => ({
      age_range: ageRange,
      male: [8, 14, 10, 7, 4, 7][index],
      female: [8, 14, 10, 7, 4, 7][index],
    })),
    audience_language: [{ language: "Portuguese", percentage: 100 }],
    active_subscriber_ratio: 40,
    channel_tags: {
      tags: Array.from({ length: 10 }, (_, index) => `Topic ${index + 1}`),
      top_5_distribution: [
        { tag: "Topic 1", percentage: 25 },
        { tag: "Topic 2", percentage: 20 },
        { tag: "Topic 3", percentage: 15 },
        { tag: "Topic 4", percentage: 15 },
        { tag: "Topic 5", percentage: 15 },
        { tag: "Other", percentage: 10 },
      ],
    },
    channel_categories: { level_1: "Uncategorized", level_2: ["Uncategorized"] },
  };
}

async function initializeScenario(client, { endpoint, suffix, bindLatestRun = true }) {
  const batchId = `full-agent-stale:${suffix}`;
  const channelId = `UCfullagent${suffix}`;
  const channelUrl = `https://www.youtube.com/channel/${channelId}`;
  const runId = `run:full-agent-stale:${suffix}`;
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,total_channel_count,discovered_candidate_count
     ) VALUES ($1,$1,'running',1,1)`,
    [batchId],
  );
  const candidateId = Number((await client.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$1,$2,$3,'accepted',1,'{}'::jsonb,
               '{"source":"legacy_results_db"}'::jsonb,now(),now())
     RETURNING candidate_id`,
    [batchId, channelId, channelUrl],
  )).rows[0].candidate_id);
  const migrationIntentId = Number((await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
     ) VALUES (
       $1,current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
       $3,'{}'::jsonb,repeat('a',64),$2,$4,1,now()
     ) RETURNING migration_intent_id`,
    [`full-agent-stale:${suffix}`, candidateId, channelId, batchId],
  )).rows[0].migration_intent_id);
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Stale full Agent channel',2000,'active',true,'queued',$3,$4,$5)`,
      [
        channelId,
        channelUrl,
        bindLatestRun ? runId : null,
        bindLatestRun ? candidateId : null,
        bindLatestRun ? runId : null,
      ],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES ($1,$2,$3,'waiting_agent','full','done',0,now(),$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify({
        dispatch_batch_id: batchId,
        pipeline_cycle_id: batchId,
      })],
    );
  });
  const templateId = Number((await client.query(
    `INSERT INTO crawler.agent_prompt_templates (
       name,version,template_text,output_schema_json,status,is_default
     ) VALUES ($1,1,'Analyze {{input_urls_json}}','{}'::jsonb,'active',true)
     RETURNING template_id`,
    [`full-agent-stale-template-${suffix}`],
  )).rows[0].template_id);
  const agentConfigId = Number((await client.query(
    `INSERT INTO crawler.agent_configs (
       name,provider,model,endpoint,prompt_template_id,batch_size,min_batch_size,
       max_workers,timeout_ms,max_retries,tools_json,enabled,is_default
     ) VALUES ($1,'openai-compatible','test-agent',$2,$3,1,1,1,5000,0,'[]'::jsonb,true,false)
     RETURNING config_id`,
    [`full-agent-stale-config-${suffix}`, endpoint, templateId],
  )).rows[0].config_id);
  return {
    agentConfigId,
    batchId,
    candidateId,
    channelId,
    channelUrl,
    migrationIntentId,
    runId,
  };
}

test("a running generic full Agent cannot write after Candidate generation advances", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  localRedisConfiguration(redisUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const prefix = `full-agent-migration-retry-${suffix}`;
  const connection = redisConnection(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const agentQueue = new Queue(queuesByRole.agentBatch, { connection, prefix });
  const agentEvents = new QueueEvents(queuesByRole.agentBatch, { connection, prefix });
  const finalizeQueue = new Queue(queuesByRole.finalize, { connection, prefix });
  let workerProcess = null;
  let schemaInitialized = false;
  let redisReady = false;
  let agentRequestCount = 0;
  let observeAgentRequest;
  let releaseAgentResponse;
  let observeRunFenceRequest;
  let releaseRunFenceResponse;
  const agentRequestStarted = new Promise((resolve) => {
    observeAgentRequest = resolve;
  });
  const agentResponseReleased = new Promise((resolve) => {
    releaseAgentResponse = resolve;
  });
  const runFenceRequestStarted = new Promise((resolve) => {
    observeRunFenceRequest = resolve;
  });
  const runFenceResponseReleased = new Promise((resolve) => {
    releaseRunFenceResponse = resolve;
  });
  let scenario;
  const agentServer = createServer(async (request, response) => {
    if (request.method !== "POST") {
      response.writeHead(404).end();
      return;
    }
    agentRequestCount += 1;
    for await (const _chunk of request) {
      // Drain the request so the production HTTP client can reuse or close its connection.
    }
    if (agentRequestCount === 1) {
      observeAgentRequest();
      await agentResponseReleased;
    } else {
      observeRunFenceRequest();
      await runFenceResponseReleased;
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      choices: [{ message: { content: JSON.stringify([completeAgentRow(scenario.channelUrl)]) } }],
    }));
  });

  t.after(async () => {
    releaseAgentResponse();
    releaseRunFenceResponse();
    let workerStopError = null;
    try {
      await stopChild(workerProcess);
    } catch (error) {
      workerStopError = error;
    }
    if (redisReady) {
      await Promise.all([
        agentQueue.obliterate({ force: true }).catch(() => {}),
        finalizeQueue.obliterate({ force: true }).catch(() => {}),
      ]);
    }
    await Promise.all([
      agentQueue.close().catch(() => {}),
      agentEvents.close().catch(() => {}),
      finalizeQueue.close().catch(() => {}),
    ]);
    await closeServer(agentServer).catch(() => {});
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([setup.end().catch(() => {}), observer.end().catch(() => {})]);
    if (workerStopError) throw workerStopError;
  });

  await Promise.all([setup.connect(), observer.connect()]);
  const agentPort = await listen(agentServer);
  scenario = await initializeScenario(setup, {
    endpoint: `http://127.0.0.1:${agentPort}/v1`,
    suffix,
  });
  schemaInitialized = true;
  await Promise.all([
    agentQueue.obliterate({ force: true }),
    finalizeQueue.obliterate({ force: true }),
  ]);
  await Promise.all([
    agentQueue.waitUntilReady(),
    agentEvents.waitUntilReady(),
    finalizeQueue.waitUntilReady(),
  ]);
  redisReady = true;

  const staleJob = await agentQueue.add("agent-profile-batch", {
    batch_id: `agent-batch:${scenario.batchId}:stale`,
    channel_ids: [scenario.channelId],
    agent_mode: "basic",
    agent_config_id: scenario.agentConfigId,
    external_agent_opt_in: true,
    pipeline_cycle_id: scenario.batchId,
    dispatch_batch_id: scenario.batchId,
  }, {
    jobId: `generic_full_agent_stale_${suffix}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  assert.equal(await staleJob.getState(), "waiting");

  await setup.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_dispatch_generation=2,updated_at=now()
     WHERE candidate_id=$1`,
    [scenario.candidateId],
  );

  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ prefix }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${queuesByRole.agentBatch}`);
  await within(agentRequestStarted, "full Agent external request");

  await setup.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_dispatch_generation=3,updated_at=now()
     WHERE candidate_id=$1`,
    [scenario.candidateId],
  );
  const systemRetryId = Number((await setup.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status,
       retry_dispatch_generation,recovery_run_id,dispatched_at
     ) VALUES (
       $1,$2,$3,2,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,
       'dispatched',3,$5,now()
     ) RETURNING system_retry_id`,
    [
      scenario.migrationIntentId,
      scenario.candidateId,
      scenario.batchId,
      `failed_channel_job_${suffix}`,
      scenario.runId,
    ],
  )).rows[0].system_retry_id);
  await setup.query(
    `UPDATE crawler.migration_system_retry_items
     SET status='resolved',resolution='recovery_fence_superseded',resolved_at=now(),updated_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId],
  );
  releaseAgentResponse();
  const result = await within(
    staleJob.waitUntilFinished(agentEvents),
    "generic full Agent Fence rejection",
  );

  assert.deepEqual({
    ok: result.ok,
    skipped: result.skipped,
    reason: result.reason,
  }, {
    ok: true,
    skipped: true,
    reason: "migration_system_retry_agent_fence_required",
  });
  assert.equal(agentRequestCount, 1, "the Candidate must advance while Agent work is in flight");
  assert.deepEqual((await observer.query(
    `SELECT channel.agent_status,channel.agent_attempts,
            (SELECT count(*)::int FROM crawler.agent_profiles profile
             WHERE profile.channel_id=channel.channel_id) AS profile_count,
            retry.status AS retry_status,retry.recovery_run_id
     FROM crawler.channels channel
     JOIN crawler.migration_system_retry_items retry ON retry.system_retry_id=$2
     WHERE channel.channel_id=$1`,
    [scenario.channelId, systemRetryId],
  )).rows[0], {
    agent_status: "running",
    agent_attempts: 0,
    profile_count: 0,
    retry_status: "resolved",
    recovery_run_id: scenario.runId,
  });

  await setup.query(
    "UPDATE crawler.channels SET agent_status='queued',updated_at=now() WHERE channel_id=$1",
    [scenario.channelId],
  );
  const staleRunJob = await agentQueue.add("agent-profile-batch", {
    batch_id: `agent-batch:${scenario.batchId}:stale-run`,
    channel_ids: [scenario.channelId],
    agent_mode: "basic",
    agent_config_id: scenario.agentConfigId,
    external_agent_opt_in: true,
    pipeline_cycle_id: scenario.batchId,
    dispatch_batch_id: scenario.batchId,
  }, {
    jobId: `generic_full_agent_stale_run_${suffix}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  await within(runFenceRequestStarted, "full Agent Run Fence external request");
  await setup.query(
    `UPDATE crawler.channel_runs
     SET result_json=result_json || '{"dispatch_batch_id":"replacement-batch"}'::jsonb,
         updated_at=now()
     WHERE run_id=$1`,
    [scenario.runId],
  );
  releaseRunFenceResponse();
  const staleRunResult = await within(
    staleRunJob.waitUntilFinished(agentEvents),
    "generic full Agent Run Fence rejection",
  );
  assert.deepEqual({
    ok: staleRunResult.ok,
    skipped: staleRunResult.skipped,
    reason: staleRunResult.reason,
  }, {
    ok: true,
    skipped: true,
    reason: "migration_system_retry_agent_fence_required",
  });
  assert.equal(agentRequestCount, 2);
  assert.deepEqual((await observer.query(
    `SELECT channel.agent_status,
            (SELECT count(*)::int FROM crawler.agent_profiles profile
             WHERE profile.channel_id=channel.channel_id) AS profile_count
     FROM crawler.channels channel WHERE channel.channel_id=$1`,
    [scenario.channelId],
  )).rows[0], { agent_status: "running", profile_count: 0 });
  assert.equal(
    (await finalizeQueue.getJobCounts("waiting", "active", "delayed")).waiting,
    0,
    "a stale in-flight Agent result must not enqueue generic Finalize",
  );
  assert.doesNotMatch(workerOutput.output(), /worker failed|unhandled/i);
});

test("a real full Agent Worker rejects a Channel without a current Run before HTTP", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  localRedisConfiguration(redisUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const prefix = `full-agent-missing-run-${suffix}`;
  const connection = redisConnection(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const agentQueue = new Queue(queuesByRole.agentBatch, { connection, prefix });
  const agentEvents = new QueueEvents(queuesByRole.agentBatch, { connection, prefix });
  let workerProcess = null;
  let schemaInitialized = false;
  let redisReady = false;
  let agentRequestCount = 0;
  const agentServer = createServer((_request, response) => {
    agentRequestCount += 1;
    response.writeHead(500).end();
  });

  t.after(async () => {
    await stopChild(workerProcess).catch(() => {});
    if (redisReady) await agentQueue.obliterate({ force: true }).catch(() => {});
    await Promise.all([
      agentQueue.close().catch(() => {}),
      agentEvents.close().catch(() => {}),
    ]);
    await closeServer(agentServer).catch(() => {});
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await setup.end().catch(() => {});
  });

  await setup.connect();
  const agentPort = await listen(agentServer);
  const scenario = await initializeScenario(setup, {
    endpoint: `http://127.0.0.1:${agentPort}/v1`,
    suffix,
    bindLatestRun: false,
  });
  schemaInitialized = true;
  await agentQueue.obliterate({ force: true });
  await Promise.all([agentQueue.waitUntilReady(), agentEvents.waitUntilReady()]);
  redisReady = true;
  const job = await agentQueue.add("agent-profile-batch", {
    batch_id: `agent-batch:${scenario.batchId}:missing-run`,
    channel_ids: [scenario.channelId],
    agent_mode: "basic",
    agent_config_id: scenario.agentConfigId,
    external_agent_opt_in: true,
    pipeline_cycle_id: scenario.batchId,
    dispatch_batch_id: scenario.batchId,
  }, {
    jobId: `generic_full_agent_missing_run_${suffix}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ prefix }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${queuesByRole.agentBatch}`);
  const result = await within(job.waitUntilFinished(agentEvents), "missing Run Fence rejection");

  assert.deepEqual({
    ok: result.ok,
    skipped: result.skipped,
    reason: result.reason,
  }, {
    ok: true,
    skipped: true,
    reason: "full_agent_run_fence_required",
  });
  assert.equal(agentRequestCount, 0);
  assert.deepEqual((await setup.query(
    `SELECT channel.agent_status,
            (SELECT count(*)::int FROM crawler.agent_profiles profile
             WHERE profile.channel_id=channel.channel_id) AS profile_count
     FROM crawler.channels channel WHERE channel.channel_id=$1`,
    [scenario.channelId],
  )).rows[0], { agent_status: "queued", profile_count: 0 });
  assert.doesNotMatch(workerOutput.output(), /worker failed|unhandled/i);
});

test("a real Recovery Agent Worker takes ownership from a stalled BullMQ attempt", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  localRedisConfiguration(redisUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const prefix = `recovery-agent-stalled-${suffix}`;
  const connection = redisConnection(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const agentQueue = new Queue(queuesByRole.agentBatch, { connection, prefix });
  const agentEvents = new QueueEvents(queuesByRole.agentBatch, { connection, prefix });
  const finalizeQueue = new Queue(queuesByRole.finalize, { connection, prefix });
  let blockingWorker = null;
  let takeoverWorker = null;
  let releaseBlockingAttempt;
  let firstAttemptJob = null;
  let schemaInitialized = false;
  let redisReady = false;
  let agentRequestCount = 0;
  let scenario;
  let systemRetryId;
  const blockingAttempt = new Promise((resolve) => {
    releaseBlockingAttempt = resolve;
  });
  let observeFirstAttempt;
  const firstAttemptStarted = new Promise((resolve) => {
    observeFirstAttempt = resolve;
  });
  const agentServer = createServer(async (request, response) => {
    if (request.method !== "POST") {
      response.writeHead(404).end();
      return;
    }
    agentRequestCount += 1;
    for await (const _chunk of request) {
      // Drain the request before returning the deterministic Agent response.
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      choices: [{ message: { content: JSON.stringify([completeAgentRow(scenario.channelUrl)]) } }],
    }));
  });

  t.after(async () => {
    releaseBlockingAttempt();
    await Promise.all([
      blockingWorker?.close().catch(() => {}),
      stopChild(takeoverWorker).catch(() => {}),
    ]);
    if (redisReady) {
      await Promise.all([
        agentQueue.obliterate({ force: true }).catch(() => {}),
        finalizeQueue.obliterate({ force: true }).catch(() => {}),
      ]);
    }
    await Promise.all([
      agentQueue.close().catch(() => {}),
      agentEvents.close().catch(() => {}),
      finalizeQueue.close().catch(() => {}),
    ]);
    await closeServer(agentServer).catch(() => {});
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([setup.end().catch(() => {}), observer.end().catch(() => {})]);
  });

  await Promise.all([setup.connect(), observer.connect()]);
  const agentPort = await listen(agentServer);
  scenario = await initializeScenario(setup, {
    endpoint: `http://127.0.0.1:${agentPort}/v1`,
    suffix,
  });
  schemaInitialized = true;
  await setup.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_dispatch_generation=2,updated_at=now()
     WHERE candidate_id=$1`,
    [scenario.candidateId],
  );
  systemRetryId = Number((await setup.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status,
       retry_dispatch_generation,recovery_run_id,dispatched_at
     ) VALUES (
       $1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,
       'dispatched',2,$5,now()
     ) RETURNING system_retry_id`,
    [
      scenario.migrationIntentId,
      scenario.candidateId,
      scenario.batchId,
      `stalled_failed_channel_job_${suffix}`,
      scenario.runId,
    ],
  )).rows[0].system_retry_id);
  await Promise.all([
    agentQueue.obliterate({ force: true }),
    finalizeQueue.obliterate({ force: true }),
  ]);
  await Promise.all([
    agentQueue.waitUntilReady(),
    agentEvents.waitUntilReady(),
    finalizeQueue.waitUntilReady(),
  ]);
  redisReady = true;

  blockingWorker = new Worker(queuesByRole.agentBatch, async (job) => {
    firstAttemptJob = job;
    await setup.query(
      `UPDATE crawler.migration_system_retry_items
       SET recovery_agent_active_job_id=$2,recovery_agent_active_job_attempt=$3,updated_at=now()
       WHERE system_retry_id=$1`,
      [systemRetryId, String(job.id), Number(job.attemptsStarted)],
    );
    observeFirstAttempt();
    return blockingAttempt;
  }, {
    connection,
    prefix,
    concurrency: 1,
    lockDuration: 500,
    stalledInterval: 500,
    skipLockRenewal: true,
  });
  blockingWorker.on("error", () => {});
  await blockingWorker.waitUntilReady();

  const recoveryJob = await agentQueue.add("agent-profile-batch", {
    batch_id: `migration-system-retry:${systemRetryId}:g2:${scenario.runId}`,
    channel_ids: [scenario.channelId],
    agent_mode: "basic",
    agent_config_id: scenario.agentConfigId,
    external_agent_opt_in: true,
    migration_system_retry_id: systemRetryId,
    recovery_agent_job_epoch: 0,
    candidate_id: scenario.candidateId,
    dispatch_generation: 2,
    pipeline_cycle_id: scenario.batchId,
    dispatch_batch_id: scenario.batchId,
    run_id: scenario.runId,
  }, {
    jobId: `migration_system_retry_agent_stalled_${suffix}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  await within(firstAttemptStarted, "Recovery Agent attempt 1 ownership");
  assert.equal(firstAttemptJob.attemptsStarted, 1);

  takeoverWorker = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ prefix, stalled: true }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const takeoverOutput = captureChildOutput(takeoverWorker);
  await takeoverOutput.waitFor(`worker started queue=${queuesByRole.agentBatch}`);
  const result = await within(
    recoveryJob.waitUntilFinished(agentEvents),
    "Recovery Agent attempt 2 completion",
    30_000,
  );
  assert.equal(result.ok, true);
  assert.equal(agentRequestCount, 1);
  assert.deepEqual((await observer.query(
    `SELECT retry.recovery_agent_active_job_id,retry.recovery_agent_active_job_attempt,
            channel.agent_status,
            (SELECT count(*)::int FROM crawler.agent_profiles profile
             WHERE profile.channel_id=channel.channel_id AND profile.status='success') AS profile_count
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_candidates candidate ON candidate.candidate_id=retry.candidate_id
     JOIN crawler.channels channel ON channel.channel_id=candidate.channel_id
     WHERE retry.system_retry_id=$1`,
    [systemRetryId],
  )).rows[0], {
    recovery_agent_active_job_id: String(recoveryJob.id),
    recovery_agent_active_job_attempt: "2",
    agent_status: "done",
    profile_count: 1,
  });
  assert.equal((await agentQueue.getJob(recoveryJob.id)).attemptsStarted, 2);
  assert.equal(await transaction(observer, (client) => (
    lockMigrationSystemRetryAgentJobFence(
      client,
      migrationSystemRetryAgentJobFence(firstAttemptJob),
    )
  )), false, "the resumed attempt 1 cannot acquire a result-write lock");
  assert.equal(
    (await finalizeQueue.getJobCounts("waiting", "active", "delayed")).waiting,
    0,
    "Recovery Agent completion leaves Finalize to the recovery reconciler",
  );

  await setup.query(
    `UPDATE crawler.channels SET agent_status='failed',updated_at=now() WHERE channel_id=$1`,
    [scenario.channelId],
  );
  await recoveryJob.remove();
  const preparedRequeue = await transaction(observer, (client) => (
    prepareMigrationSystemRetryAgentJobRequeue(
      client,
      migrationSystemRetryAgentJobFence(firstAttemptJob),
      { findExistingJob: () => agentQueue.getJob(recoveryJob.id) },
    )
  ));
  assert.deepEqual(preparedRequeue, {
    ready: true,
    cleared: true,
    existingJob: null,
    jobEpoch: 1,
  });
  const reincarnatedJob = await agentQueue.add("agent-profile-batch", {
    ...recoveryJob.data,
    recovery_agent_job_epoch: preparedRequeue.jobEpoch,
  }, {
    jobId: `migration_system_retry_agent_stalled_${suffix}_e1`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  const reincarnatedResult = await within(
    reincarnatedJob.waitUntilFinished(agentEvents),
    "Recovery Agent incarnation 1 completion",
    30_000,
  );
  assert.equal(reincarnatedResult.ok, true);
  assert.equal(agentRequestCount, 2);
  assert.deepEqual((await observer.query(
    `SELECT recovery_agent_job_epoch,recovery_agent_active_job_id,
            recovery_agent_active_job_attempt
     FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [systemRetryId],
  )).rows[0], {
    recovery_agent_job_epoch: "1",
    recovery_agent_active_job_id: String(reincarnatedJob.id),
    recovery_agent_active_job_attempt: "1",
  });
  assert.equal(await transaction(observer, (client) => (
    lockMigrationSystemRetryAgentJobFence(
      client,
      migrationSystemRetryAgentJobFence(firstAttemptJob),
    )
  )), false, "a deleted incarnation's stalled callback remains fenced forever");

  releaseBlockingAttempt();
  await blockingWorker.close();
  blockingWorker = null;
  await stopChild(takeoverWorker);
  assert.equal(takeoverWorker.exitCode, 0, takeoverOutput.output());
});
