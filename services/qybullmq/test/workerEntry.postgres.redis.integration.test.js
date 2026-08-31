import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents } from "bullmq";
import pg from "pg";

import {
  querySchedulerStartTransition,
  updateQuerySchedulerWithMigrationFence,
} from "../../dashboard/src/querySchedulerControl.js";
import { finalizeDispatchRevision } from "../src/finalizePolicy.js";
import { resolveWorkerIdentityPolicy } from "../src/identityPolicyCatalog.js";
import {
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "../src/managedJobDispatchOutbox.js";
import { settleCompletedMigrationBatch } from "../src/migrationBatchCompletion.js";
import { retryMigrationSystemFailure } from "../src/migrationSystemRetry.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { queuesByRole } from "../src/queues.js";

const { Client, Pool } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();
const batchId = "worker-entry-system-failure-batch";
const candidateId = 482;
const channelId = "UC0NoarYHkSxek05QDqhtoYw";
const jobId = `channel-snapshot__${batchId}__${channelId}__g1`;
const queueName = queuesByRole.channelCrawl;
const bullmqPrefix = `migration-reliability-worker-entry-${randomUUID()}`;

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

function localRedisConfiguration(value) {
  const url = new URL(value);
  assert.equal(url.protocol, "redis:", "integration Redis must not require TLS");
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
  assert.equal(url.username, "", "Worker entry does not accept a Redis ACL username");
  assert.equal(
    decodeURIComponent(url.pathname).replace(/^\/+/, "") || "0",
    "0",
    "Worker entry integration must use Redis database 0",
  );
  assert.equal(url.search, "", "Worker entry does not accept Redis URL options");
  assert.equal(url.hash, "", "Worker entry does not accept a Redis URL fragment");
  return {
    host: url.hostname,
    port: Number(url.port || 6379),
    password: url.password ? decodeURIComponent(url.password) : undefined,
  };
}

function assertDedicatedLocalRedis(value) {
  localRedisConfiguration(value);
}

function normalizeQueryScheduler(value = {}) {
  return {
    ...value,
    status: String(value.status ?? "stopped"),
    stop_reason: value.stop_reason ?? null,
    pipeline_cycle_id: value.pipeline_cycle_id ?? null,
    completed_at: value.completed_at ?? null,
    updated_at: value.updated_at ?? null,
    updated_by: value.updated_by ?? null,
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

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
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
        throw new Error(
          `Worker exited before ${JSON.stringify(fragment)}\n${output}`,
        );
      }
      return false;
    },
      `Worker output ${JSON.stringify(fragment)}`,
      timeoutMs,
    ),
  };
}

async function stopChild(child) {
  if (!child) return;
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once("exit", resolve));
  child.kill("SIGTERM");
  try {
    await within(exited, "Worker shutdown", 10_000);
  } catch (gracefulShutdownError) {
    child.kill("SIGKILL");
    try {
      await within(exited, "forced Worker shutdown", 5_000);
    } catch (forcedShutdownError) {
      throw new AggregateError(
        [gracefulShutdownError, forcedShutdownError],
        "Worker did not exit after SIGTERM or SIGKILL",
      );
    }
    throw gracefulShutdownError;
  }
}

function assignment(request, policy, {
  routeGeneration = 1,
  controlState = "READY_KEEP_ROUTE",
  leaseId = "worker-entry-lease-1",
} = {}) {
  return {
    ok: true,
    ready: true,
    control_state: controlState,
    workload_scope: "qy-production",
    protocol_version: 2,
    role: "channel",
    worker_id: request.worker_id,
    worker_instance_id: request.worker_instance_id,
    slot_name: "worker-entry-channel-slot",
    proxy_user: `worker-entry-channel-proxy-g${routeGeneration}`,
    lease_id: leaseId,
    lease_remaining_ms: 300_000,
    server_time: "2026-08-30T00:00:00.000Z",
    route_generation: routeGeneration,
    credential_generation: routeGeneration,
    network_identity_key: `worker-entry-network-${routeGeneration}`,
    profile_epoch: routeGeneration - 1,
    identity_policy_id: policy.id,
    identity_policy_version: policy.version,
    identity_policy_hash: policy.hash,
    identity_action: "keep",
    egress_country: "BR",
  };
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

async function initializeScenario(client) {
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status
     ) VALUES ($1,$1,'running')`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
       status,snapshot_dispatch_generation,snapshot_active_job_id,
       snapshot_active_job_attempt,snapshot_json,source_json
     ) VALUES ($1,$2,$2,$3,$4,'queued',1,$5,0,'{}'::jsonb,
               '{"source":"legacy_results_db"}'::jsonb)`,
    [candidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`, jobId],
  );
  await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
     ) VALUES (
       'legacy-results-v1',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$1,
       $2,'{}'::jsonb,repeat('a',64),$1,$3,1,now()
     )`,
    [candidateId, channelId, batchId],
  );
}

function workerEnvironment({ controlUrl, policy }) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "worker_entry_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: bullmqPrefix,
    WORKER_QUEUES: queueName,
    PROXY_SLOT_ROLE: "channel",
    PROXY_WORKER_ID: "worker-entry-channel-1",
    ROTA_PROXY_CONTROL_URL: controlUrl,
    ROTA_PROXY_CONTROL_TOKEN: "worker-entry-control-token",
    ROTA_PROXY_CONTROL_TIMEOUT_MS: "1000",
    ROTA_PROXY_CONTROL_MAX_ATTEMPTS: "1",
    ROTA_PROXY_BASE_URL: controlUrl,
    ROTA_BULLMQ_PROXY_PASSWORD: "worker-entry-proxy-password",
    ROTA_FIXED_PROXY_USER: "",
    ROTA_IDENTITY_POLICY_ID: policy.id,
    ROTA_WORKLOAD_SCOPE_EXPECTED: "qy-production",
    ROTA_SLOT_RENEW_INTERVAL_MS: "60000",
    ROTA_LEASE_SAFETY_MARGIN_MS: "1000",
    ...resolveWorkerIdentityPolicy({
      role: "channel",
      policyId: policy.id,
      expectedWorkloadScope: "qy-production",
      environment: {},
    }).environment,
  };
}

function unmanagedWorkerEnvironment(workerQueueName) {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "worker_entry_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: bullmqPrefix,
    WORKER_QUEUES: workerQueueName,
    PROXY_SLOT_ROLE: "",
    ROTA_PROXY_CONTROL_URL: "",
    ROTA_PROXY_CONTROL_TOKEN: "",
    ROTA_FIXED_PROXY_USER: "",
    ROTA_BULLMQ_PROXY_PASSWORD: "",
    S3_ENDPOINT: "",
    S3_ACCESS_KEY: "",
    S3_SECRET_KEY: "",
  };
}

async function productionFinalizeRevision(client, { channelId: targetChannelId, runId }) {
  const state = (await client.query(
    `SELECT channel.channel_id,channel.latest_run_id,
            channel.status AS channel_status,channel.agent_status,
            channel.updated_at AS channel_updated_at,
            run.detail_status,run.expected_content_count,
            run.result_json->>'pipeline_cycle_id' AS pipeline_cycle_id,
            (SELECT count(*)::int
             FROM crawler.content_candidates candidate
             WHERE candidate.run_id=run.run_id) AS candidate_count,
            (SELECT max(candidate.updated_at)
             FROM crawler.content_candidates candidate
             WHERE candidate.run_id=run.run_id) AS candidate_updated_at,
            (SELECT count(*)::int
             FROM crawler.contents content
             WHERE content.channel_id=channel.channel_id AND content.run_id=run.run_id)
              AS content_count,
            (SELECT max(COALESCE(content.last_enriched_at,content.last_seen_at))
             FROM crawler.contents content
             WHERE content.channel_id=channel.channel_id AND content.run_id=run.run_id)
              AS content_updated_at,
            (SELECT agent.updated_at
             FROM crawler.agent_profiles agent
             WHERE agent.channel_id=channel.channel_id
               AND agent.agent_mode='basic' AND agent.status='success'
             LIMIT 1) AS agent_updated_at
     FROM crawler.channels channel
     JOIN crawler.channel_runs run ON run.run_id=$2
     WHERE channel.channel_id=$1`,
    [targetChannelId, runId],
  )).rows[0];
  assert.ok(state, "Finalize revision source state must exist");
  return finalizeDispatchRevision({
    channel_id: state.channel_id,
    latest_run_id: state.latest_run_id,
    channel_status: state.channel_status,
    agent_status: state.agent_status,
    channel_updated_at: state.channel_updated_at,
    detail_status: state.detail_status,
    expected_content_count: Number(state.expected_content_count ?? 0),
    pipeline_cycle_id: state.pipeline_cycle_id,
    candidate_count: Number(state.candidate_count ?? 0),
    candidate_updated_at: state.candidate_updated_at,
    content_count: Number(state.content_count ?? 0),
    content_updated_at: state.content_updated_at,
    agent_updated_at: state.agent_updated_at,
  });
}

test("the real Worker entry recovers a BeginTask Renew gap without consuming a BullMQ attempt", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  assertDedicatedLocalRedis(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const locker = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const connection = redisConnection(redisUrl);
  const queue = new Queue(queueName, { connection, prefix: bullmqPrefix });
  const queueEvents = new QueueEvents(queueName, { connection, prefix: bullmqPrefix });
  const resolvedPolicy = resolveWorkerIdentityPolicy({
    role: "channel",
    policyId: "qy-br-channel-anonymous-v1",
    expectedWorkloadScope: "qy-production",
    environment: {},
  });
  const controlRequests = [];
  const renewReached = deferred();
  const releaseRenew = deferred();
  const controlServer = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const payload = chunks.length > 0 ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
    controlRequests.push({ path: request.url, payload });
    let body;
    if (request.url === "/claim") {
      const claimCount = controlRequests.filter(({ path }) => path === "/claim").length;
      body = assignment(payload, resolvedPolicy.policy, claimCount === 1
        ? {}
        : {
          routeGeneration: 2,
          controlState: "READY_NEW_ROUTE",
          leaseId: "worker-entry-lease-2",
        });
    } else if (request.url === "/tasks/begin") {
      const beginCount = controlRequests.filter(({ path }) => path === "/tasks/begin").length;
      response.writeHead(409, { "content-type": "application/json" });
      response.end(JSON.stringify(beginCount === 1
        ? { ok: false, code: "LEASE_CONFLICT", error: "stale route generation" }
        : {
            ok: false,
            code: "TASK_FENCE_CONFLICT",
            error: "injected terminal control failure",
          }));
      return;
    } else if (request.url === "/renew") {
      renewReached.resolve(payload);
      await releaseRenew.promise;
      response.writeHead(409, { "content-type": "application/json" });
      response.end(JSON.stringify({
        ok: false,
        code: "LEASE_CONFLICT",
        error: "authoritative Lease owner changed during Renew",
      }));
      return;
    } else if (request.url === "/release") {
      body = {
        ok: true,
        released: true,
        release_request_id: payload.release_request_id,
        slot_name: payload.slot_name,
        lease_id: payload.lease_id,
        route_generation: payload.known_route_generation,
        status: "released",
        released_at: "2026-08-30T00:00:01.000Z",
        reason: payload.reason,
      };
    } else {
      response.writeHead(500, { "content-type": "application/json" });
      response.end(JSON.stringify({ ok: false, error: `unexpected control path ${request.url}` }));
      return;
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  });
  let workerProcess = null;
  let lockHeld = false;
  let redisReady = false;
  let schemaInitialized = false;

  t.after(async () => {
    let workerStopError = null;
    releaseRenew.resolve();
    if (lockHeld) await locker.query("ROLLBACK").catch(() => {});
    try {
      await stopChild(workerProcess);
    } catch (error) {
      workerStopError = error;
    }
    if (redisReady) await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close().catch(() => {}), queueEvents.close().catch(() => {})]);
    await closeServer(controlServer).catch(() => {});
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([
      setup.end().catch(() => {}),
      locker.end().catch(() => {}),
      observer.end().catch(() => {}),
    ]);
    if (workerStopError) throw workerStopError;
  });

  await Promise.all([setup.connect(), locker.connect(), observer.connect()]);
  const lockerPid = Number((await locker.query("SELECT pg_backend_pid() AS pid")).rows[0].pid);
  schemaInitialized = true;
  await initializeScenario(setup);
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  redisReady = true;
  const controlPort = await listen(controlServer);
  const controlUrl = `http://127.0.0.1:${controlPort}`;
  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ controlUrl, policy: resolvedPolicy.policy }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${queueName}`);

  await locker.query("BEGIN");
  lockHeld = true;
  await locker.query("LOCK TABLE crawler.migration_system_retry_items IN ACCESS EXCLUSIVE MODE");
  const queued = await queue.add("channel-snapshot", {
    candidate_id: candidateId,
    migration_intent_id: 1,
    dispatch_generation: 1,
    dispatch_batch_id: batchId,
    pipeline_cycle_id: batchId,
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    crawl_mode: "full",
  }, {
    jobId,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

  const renewRequest = await within(renewReached.promise, "BeginTask Lease conflict Renew");
  const activeJob = await queue.getJob(jobId);
  const activeCandidate = (await observer.query(
    `SELECT status,snapshot_attempts,snapshot_active_job_id,snapshot_active_job_attempt,
            (SELECT count(*)::int
             FROM crawler.channel_execution_attempts execution
             WHERE execution.channel_id=candidate.channel_id) AS execution_attempt_count
     FROM crawler.channel_candidates candidate
     WHERE candidate_id=$1`,
    [candidateId],
  )).rows[0];
  assert.deepEqual({
    job_state: await activeJob.getState(),
    attempts_made: activeJob.attemptsMade,
    attempts_started: activeJob.attemptsStarted,
    candidate_status: activeCandidate.status,
    snapshot_attempts: activeCandidate.snapshot_attempts,
    snapshot_active_job_id: activeCandidate.snapshot_active_job_id,
    snapshot_active_job_attempt: activeCandidate.snapshot_active_job_attempt,
    execution_attempt_count: activeCandidate.execution_attempt_count,
    known_route_generation: renewRequest.known_route_generation,
  }, {
    job_state: "active",
    attempts_made: 0,
    attempts_started: 1,
    candidate_status: "queued",
    snapshot_attempts: 0,
    snapshot_active_job_id: jobId,
    snapshot_active_job_attempt: 1,
    execution_attempt_count: 0,
    known_route_generation: 1,
  });
  releaseRenew.resolve();

  const persistenceGate = await waitFor(async () => {
    const blocked = await observer.query(
      `SELECT 1
       FROM pg_locks waiting
       JOIN pg_stat_activity activity ON activity.pid=waiting.pid
       WHERE waiting.relation='crawler.migration_system_retry_items'::regclass
         AND waiting.granted=false
         AND $1=ANY(pg_blocking_pids(waiting.pid))
         AND activity.query LIKE '%WITH matching_intent AS (%'
         AND activity.query LIKE '%UPDATE crawler.channel_candidates candidate%'
         AND EXISTS (
           SELECT 1
           FROM crawler.channel_candidates candidate
           WHERE candidate.candidate_id=$2
             AND candidate.snapshot_active_job_id=$3
             AND candidate.snapshot_active_job_attempt=1
         )
       LIMIT 1`,
      [lockerPid, candidateId, jobId],
    );
    const state = await queued.getState();
    if (blocked.rowCount === 1) return { blocked: true, state };
    if (state === "failed") return { blocked: false, state };
    return false;
  }, "Worker system failure persistence lock wait");

  await locker.query("COMMIT");
  lockHeld = false;
  assert.equal(
    persistenceGate.blocked,
    true,
    "the production Worker must execute the durable system-retry CTE before failing",
  );
  assert.equal(
    persistenceGate.state,
    "active",
    "BullMQ must not fail the job before the system retry ledger is durable",
  );
  await assert.rejects(
    within(queued.waitUntilFinished(queueEvents), "terminal system failure"),
    /injected terminal control failure/,
  );

  const persisted = await waitFor(async () => {
    const result = await observer.query(
      `SELECT candidate.status,candidate.snapshot_active_job_id,
              candidate.snapshot_active_job_attempt,
              retry.status AS retry_status,retry.failure_code,
              retry.failed_job_attempt::int AS failed_job_attempt
       FROM crawler.channel_candidates candidate
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       WHERE candidate.candidate_id=$1`,
      [candidateId],
    );
    return result.rows[0] ?? null;
  }, "system retry ledger row");
  assert.deepEqual(persisted, {
    status: "failed",
    snapshot_active_job_id: jobId,
    snapshot_active_job_attempt: 1,
    retry_status: "pending",
    failure_code: "TASK_FENCE_CONFLICT",
    failed_job_attempt: 1,
  });
  assert.equal((await queued.getState()), "failed");
  const failedJob = await queue.getJob(jobId);
  assert.equal(failedJob.attemptsMade, 1);
  assert.equal(failedJob.attemptsStarted, 1);
  const beginRequests = controlRequests.filter(({ path }) => path === "/tasks/begin");
  assert.deepEqual(
    beginRequests.map(({ payload }) => ({
      business_run_id: payload.business_run_id,
      job_execution_id: payload.job_execution_id,
      route_generation: payload.route_generation,
    })),
    [
      {
        business_run_id: beginRequests[0].payload.business_run_id,
        job_execution_id: beginRequests[0].payload.job_execution_id,
        route_generation: 1,
      },
      {
        business_run_id: beginRequests[0].payload.business_run_id,
        job_execution_id: beginRequests[0].payload.job_execution_id,
        route_generation: 2,
      },
    ],
  );
  assert.deepEqual(controlRequests.map((request) => request.path), [
    "/claim",
    "/tasks/begin",
    "/renew",
    "/claim",
    "/tasks/begin",
  ]);
  await waitFor(async () => {
    const result = await observer.query(
      `SELECT candidate.updated_at AS candidate_updated_at,
              intent.updated_at AS intent_updated_at,
              retry.updated_at AS retry_updated_at,
              batch.updated_at AS batch_updated_at,
              batch.discovered_candidate_count,
              batch.total_channel_count,
              batch.accepted_channel_count,
              batch.rejected_channel_count,
              batch.failed_channel_count,
              (SELECT count(*)::int
               FROM crawler.task_events event
               WHERE event.queue_name=$2 AND event.job_id=$3 AND event.status='failed'
              ) AS failed_event_count,
              (SELECT max(event.created_at)
               FROM crawler.task_events event
               WHERE event.queue_name=$2 AND event.job_id=$3 AND event.status='failed'
              ) AS failed_event_created_at
       FROM crawler.channel_candidates candidate
       JOIN crawler.migration_channel_intents intent
         ON intent.target_candidate_id=candidate.candidate_id
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       JOIN crawler.query_dispatch_batches batch
         ON batch.dispatch_batch_id=candidate.dispatch_batch_id
       WHERE candidate.candidate_id=$1`,
      [candidateId, queueName, jobId],
    );
    const row = result.rows[0];
    const eventAt = row?.failed_event_created_at?.getTime?.();
    if (!Number.isFinite(eventAt) || Number(row.failed_event_count) !== 1) return false;
    const candidateAt = row.candidate_updated_at?.getTime?.();
    const intentAt = row.intent_updated_at?.getTime?.();
    const retryAt = row.retry_updated_at?.getTime?.();
    const batchAt = row.batch_updated_at?.getTime?.();
    const durableWrites = [candidateAt, intentAt, retryAt, eventAt];
    return Number(row.discovered_candidate_count) === 1
      && Number(row.total_channel_count) === 1
      && Number(row.accepted_channel_count) === 0
      && Number(row.rejected_channel_count) === 0
      && Number(row.failed_channel_count) === 1
      && durableWrites.every(Number.isFinite)
      && batchAt >= Math.max(...durableWrites);
  }, "production failed-listener durable writes");

  await stopChild(workerProcess);
  assert.equal(workerProcess.exitCode, 0, workerOutput.output());
  assert.deepEqual(controlRequests.map((request) => request.path), [
    "/claim",
    "/tasks/begin",
    "/renew",
    "/claim",
    "/tasks/begin",
    "/release",
  ]);
});

test("a controlled G+1 Outbox reaches the real Worker entry and resolves its exact retry", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  assertDedicatedLocalRedis(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const dashboardPool = new Pool({ connectionString: databaseUrl, max: 1 });
  const connection = redisConnection(redisUrl);
  const queue = new Queue(queueName, { connection, prefix: bullmqPrefix });
  const queueEvents = new QueueEvents(queueName, { connection, prefix: bullmqPrefix });
  const resolvedPolicy = resolveWorkerIdentityPolicy({
    role: "channel",
    policyId: "qy-br-channel-anonymous-v1",
    expectedWorkloadScope: "qy-production",
    environment: {},
  });
  const controlRequests = [];
  const controlServer = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const payload = chunks.length > 0 ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
    controlRequests.push({ path: request.url, payload });
    if (request.url === "/claim") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify(assignment(payload, resolvedPolicy.policy)));
      return;
    }
    if (request.url === "/tasks/begin") {
      response.writeHead(409, { "content-type": "application/json" });
      response.end(JSON.stringify({
        ok: false,
        code: "BUSINESS_RUN_BUDGET_EXHAUSTED",
        error: "injected G+1 Business Run budget exhaustion",
      }));
      return;
    }
    if (request.url === "/release") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({
        ok: true,
        released: true,
        release_request_id: payload.release_request_id,
        slot_name: payload.slot_name,
        lease_id: payload.lease_id,
        route_generation: payload.known_route_generation,
        status: "released",
        released_at: "2026-08-30T00:00:01.000Z",
        reason: payload.reason,
      }));
      return;
    }
    response.writeHead(500, { "content-type": "application/json" });
    response.end(JSON.stringify({ ok: false, error: `unexpected control path ${request.url}` }));
  });
  let workerProcess = null;
  let redisReady = false;
  let schemaInitialized = false;

  t.after(async () => {
    let workerStopError = null;
    try {
      await stopChild(workerProcess);
    } catch (error) {
      workerStopError = error;
    }
    if (redisReady) await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close().catch(() => {}), queueEvents.close().catch(() => {})]);
    await closeServer(controlServer).catch(() => {});
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([
      setup.end().catch(() => {}),
      observer.end().catch(() => {}),
      dashboardPool.end().catch(() => {}),
    ]);
    if (workerStopError) throw workerStopError;
  });

  await Promise.all([setup.connect(), observer.connect()]);
  schemaInitialized = true;
  await initializeScenario(setup);
  await setup.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json,accepted_at,
       validation_finished_at
     )
     SELECT $1,$1,'UCworkeraccepted' || ordinal::text,
            'https://www.youtube.com/channel/UCworkeraccepted' || ordinal::text,
            'accepted',1,'{}'::jsonb,'{}'::jsonb,now(),now()
     FROM generate_series(1,99) AS ordinal`,
    [batchId],
  );
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  redisReady = true;
  const intent = (await setup.query(
    `SELECT migration_intent_id
     FROM crawler.migration_channel_intents
     WHERE target_candidate_id=$1`,
    [candidateId],
  )).rows[0];
  await setup.query(
    `UPDATE crawler.query_dispatch_batches
     SET status='finishing',discovered_candidate_count=1,updated_at=now()
     WHERE dispatch_batch_id=$1`,
    [batchId],
  );
  await setup.query(
    `UPDATE crawler.channel_candidates
     SET status='failed',snapshot_active_job_attempt=1,
         snapshot_json=$2::jsonb,error_message='injected G1 system failure',updated_at=now()
     WHERE candidate_id=$1`,
    [
      candidateId,
      JSON.stringify({
        failure_type: "retryable_system_failure",
        failed_dispatch_batch_id: batchId,
        system_failure: { code: "LEASE_CONFLICT", category: "lease" },
      }),
    ],
  );
  const retryItem = await setup.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease',$5::jsonb,'pending')
     RETURNING system_retry_id`,
    [
      Number(intent.migration_intent_id),
      candidateId,
      batchId,
      jobId,
      JSON.stringify({ failure_type: "retryable_system_failure" }),
    ],
  );
  await setup.query(
    `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
     VALUES ('query_scheduler',$1::jsonb,now())
     ON CONFLICT (setting_key) DO UPDATE
     SET value_json=EXCLUDED.value_json,updated_at=now()`,
    [JSON.stringify({ status: "finishing", pipeline_cycle_id: batchId })],
  );
  const withTransaction = (action) => transaction(setup, action);
  const completion = await settleCompletedMigrationBatch({ withTransaction, batchId });
  assert.deepEqual({
    status: completion.status,
    outcome: completion.outcome,
    total: completion.total,
    accepted: completion.accepted,
    rejected: completion.rejected,
    failed: completion.failed,
  }, {
    status: "completed",
    outcome: "completed_with_system_failures",
    total: 100,
    accepted: 99,
    rejected: 0,
    failed: 1,
  });

  const systemRetryId = Number(retryItem.rows[0].system_retry_id);
  const allocation = await retryMigrationSystemFailure({ systemRetryId, withTransaction });
  const repeated = await retryMigrationSystemFailure({ systemRetryId, withTransaction });
  assert.equal(allocation.created, true);
  assert.equal(allocation.dispatch_generation, 2);
  assert.equal(repeated.created, false);
  assert.equal(repeated.outbox.dispatch_id, allocation.outbox.dispatch_id);
  const dispatch = await new ManagedJobOutboxDispatcher({
    repository: new PostgresManagedJobDispatchRepository({ withTransaction }),
    queues: { [queueName]: queue },
  }).dispatchAvailable({ limit: 10 });
  assert.deepEqual(dispatch, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  const g2JobId = allocation.outbox.deterministic_job_id;
  const waitingG2 = await queue.getJob(g2JobId);
  assert.equal(await waitingG2.getState(), "prioritized");
  assert.equal(Number(waitingG2.data.dispatch_generation), 2);

  const controlPort = await listen(controlServer);
  const controlUrl = `http://127.0.0.1:${controlPort}`;
  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment({ controlUrl, policy: resolvedPolicy.policy }),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${queueName}`);
  await assert.rejects(
    within(waitingG2.waitUntilFinished(queueEvents), "G+1 budget exhaustion"),
    /Business Run budget exhausted/i,
  );

  const terminalG2 = await queue.getJob(g2JobId);
  const state = await waitFor(async () => {
    const result = await observer.query(
      `SELECT candidate.status AS candidate_status,
              candidate.snapshot_dispatch_generation::int AS generation,
              candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
              intent.dispatch_attempts::int AS intent_generation,
              retry.status AS retry_status,retry.resolution,
              retry.retry_dispatch_generation::int AS retry_generation,
              binding.status AS binding_status,binding.terminal_reason,
              batch.status AS batch_status,batch.outcome,
              batch.total_channel_count,batch.accepted_channel_count,
              batch.rejected_channel_count,batch.failed_channel_count,
              scheduler.value_json->>'status' AS scheduler_status,
              scheduler.value_json->>'stop_reason' AS scheduler_stop_reason,
              count(outbox.dispatch_id) FILTER (
                WHERE (outbox.payload_json->>'dispatch_generation')::int=2
              )::int AS g2_outbox_count
       FROM crawler.channel_candidates candidate
       JOIN crawler.migration_channel_intents intent
         ON intent.target_candidate_id=candidate.candidate_id
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       JOIN crawler.business_run_bindings binding
         ON binding.candidate_id=candidate.candidate_id
       JOIN crawler.query_dispatch_batches batch
         ON batch.dispatch_batch_id=candidate.dispatch_batch_id
       JOIN crawler.settings scheduler ON scheduler.setting_key='query_scheduler'
       LEFT JOIN crawler.proxy_job_dispatch_outbox outbox
         ON outbox.aggregate_kind='channel_snapshot'
        AND outbox.aggregate_id=candidate.candidate_id::text
       WHERE candidate.candidate_id=$1 AND retry.system_retry_id=$2
       GROUP BY candidate.candidate_id,intent.migration_intent_id,retry.system_retry_id,
                binding.business_run_key,batch.dispatch_batch_id,scheduler.setting_key`,
      [candidateId, systemRetryId],
    );
    return result.rows[0]?.retry_status === "resolved" ? result.rows[0] : null;
  }, "G+1 atomic terminal state");
  assert.deepEqual(state, {
    candidate_status: "failed",
    generation: 2,
    snapshot_active_job_id: null,
    snapshot_active_job_attempt: null,
    intent_generation: 2,
    retry_status: "resolved",
    resolution: "retry_job_terminal_business_run_budget_exhausted",
    retry_generation: 2,
    binding_status: "terminal",
    terminal_reason: "proxy_control_business_run_budget_exhausted",
    batch_status: "completed",
    outcome: "completed_with_system_failures",
    total_channel_count: 100,
    accepted_channel_count: 99,
    rejected_channel_count: 0,
    failed_channel_count: 1,
    scheduler_status: "stopped",
    scheduler_stop_reason: "pipeline_complete",
    g2_outbox_count: 1,
  });
  assert.equal(await terminalG2.getState(), "failed");
  assert.equal(terminalG2.attemptsStarted, 1);
  assert.equal(terminalG2.attemptsMade, 1);
  assert.equal(controlRequests.filter(({ path }) => path === "/tasks/begin").length, 1);
  assert.equal(
    controlRequests.find(({ path }) => path === "/tasks/begin").payload.route_generation,
    1,
  );

  await stopChild(workerProcess);
  assert.equal(workerProcess.exitCode, 0, workerOutput.output());
  assert.deepEqual(controlRequests.map(({ path }) => path), [
    "/claim",
    "/tasks/begin",
    "/release",
  ]);

  const observedScheduler = (await observer.query(
    "SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler'",
  )).rows[0].value_json;
  const nextBatchId = `${batchId}-next`;
  const activation = await updateQuerySchedulerWithMigrationFence({
    pool: dashboardPool,
    normalize: normalizeQueryScheduler,
    mutate: (lockedCurrent) => {
      const transition = querySchedulerStartTransition(lockedCurrent, {
        expected: observedScheduler,
      });
      return transition.allowed
        ? {
          startsNewCycle: transition.startsNewCycle,
          settings: {
            ...lockedCurrent,
            status: "running",
            stop_reason: null,
            completed_at: null,
            pipeline_cycle_id: nextBatchId,
          },
        }
        : { rejection: transition };
    },
  });
  assert.equal(activation.updated, true);
  assert.equal(activation.admission.allowed, true);
  assert.equal(activation.scheduler.status, "running");
  assert.equal(activation.scheduler.pipeline_cycle_id, nextBatchId);
});

test("the real Worker entry rejects a controlled Finalize Job after its source revision becomes stale", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  assertDedicatedLocalRedis(redisUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const connection = redisConnection(redisUrl);
  const finalizeQueueName = queuesByRole.finalize;
  const queue = new Queue(finalizeQueueName, { connection, prefix: bullmqPrefix });
  const queueEvents = new QueueEvents(finalizeQueueName, { connection, prefix: bullmqPrefix });
  const runId = "worker-entry-system-recovery-run-g2";
  let workerProcess = null;
  let redisReady = false;
  let schemaInitialized = false;

  t.after(async () => {
    let workerStopError = null;
    try {
      await stopChild(workerProcess);
    } catch (error) {
      workerStopError = error;
    }
    if (redisReady) await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close().catch(() => {}), queueEvents.close().catch(() => {})]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([setup.end().catch(() => {}), observer.end().catch(() => {})]);
    if (workerStopError) throw workerStopError;
  });

  await Promise.all([setup.connect(), observer.connect()]);
  schemaInitialized = true;
  await initializeScenario(setup);
  await queue.obliterate({ force: true });
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  redisReady = true;
  const migrationIntentId = Number((await setup.query(
    `SELECT migration_intent_id
     FROM crawler.migration_channel_intents
     WHERE target_candidate_id=$1`,
    [candidateId],
  )).rows[0].migration_intent_id);
  await setup.query(
    `UPDATE crawler.query_dispatch_batches
     SET status='completed',outcome='completed_with_system_failures',
         discovered_candidate_count=1,total_channel_count=1,accepted_channel_count=0,
         rejected_channel_count=0,failed_channel_count=1,
         finished_at=now(),updated_at=now()
     WHERE dispatch_batch_id=$1`,
    [batchId],
  );
  await setup.query(
    `UPDATE crawler.channel_candidates
     SET status='accepted',snapshot_dispatch_generation=2,
         snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,
         accepted_at=now(),validation_finished_at=now(),updated_at=now()
     WHERE candidate_id=$1`,
    [candidateId],
  );
  await setup.query(
    `UPDATE crawler.migration_channel_intents
     SET dispatch_attempts=2,last_dispatch_at=now(),last_error=NULL,updated_at=now()
     WHERE migration_intent_id=$1`,
    [migrationIntentId],
  );
  await transaction(setup, async (client) => {
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Recovered Worker Entry Channel',2000,'active',true,'done',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId, candidateId],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES ($1,$2,$3,'finalizing','full','done',1,now(),$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify({
        dispatch_batch_id: batchId,
        pipeline_cycle_id: batchId,
        content_max_age_days: 90,
      })],
    );
  });
  const contentCandidateId = Number((await setup.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,content_type,
       type_status,type_source,detail_status,api_status,result_json
     ) VALUES (
       $1,$2,'stale-finalize-video',1,'video','resolved','uploads_playlist',
       'unavailable','unavailable','{}'::jsonb
     ) RETURNING candidate_id`,
    [runId, channelId],
  )).rows[0].candidate_id);
  await setup.query(
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
  const systemRetryId = Number((await setup.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status,
       retry_dispatch_generation,recovery_run_id,dispatched_at
     ) VALUES (
       $1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,
       'dispatched',2,$5,now()
     ) RETURNING system_retry_id`,
    [migrationIntentId, candidateId, batchId, jobId, runId],
  )).rows[0].system_retry_id);

  const queuedRevision = await productionFinalizeRevision(setup, { channelId, runId });
  const finalizeJob = await queue.add("finalize-channel", {
    channel_id: channelId,
    run_id: runId,
    reason: "migration-system-retry-recovery",
    source_revision: queuedRevision,
    migration_system_retry_id: systemRetryId,
    candidate_id: candidateId,
    dispatch_generation: 2,
    pipeline_cycle_id: batchId,
    dispatch_batch_id: batchId,
  }, {
    jobId: `migration-system-retry-finalize__${systemRetryId}__g2`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  await setup.query(
    `UPDATE crawler.content_candidates
     SET updated_at=updated_at + interval '1 minute'
     WHERE candidate_id=$1`,
    [contentCandidateId],
  );
  assert.notEqual(
    await productionFinalizeRevision(setup, { channelId, runId }),
    queuedRevision,
    "the queued Finalize revision must be stale before the Worker starts",
  );

  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: unmanagedWorkerEnvironment(finalizeQueueName),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${finalizeQueueName}`);
  const result = await within(
    finalizeJob.waitUntilFinished(queueEvents),
    "stale controlled Finalize completion",
  );
  assert.deepEqual({
    ok: result.ok,
    skipped: result.skipped,
    skip_reason: result.skip_reason,
  }, {
    ok: true,
    skipped: true,
    skip_reason: "migration_system_retry_finalize_fence_stale",
  });

  const state = (await observer.query(
    `SELECT retry.status AS retry_status,retry.resolution,retry.resolved_at,
            run.status AS run_status,run.publication_finalized_status,
            run.publication_finalized_at,
            (SELECT count(*)::int FROM crawler.finalized_profiles) AS finalized_count
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_runs run ON run.run_id=retry.recovery_run_id
     WHERE retry.system_retry_id=$1`,
    [systemRetryId],
  )).rows[0];
  assert.deepEqual(state, {
    retry_status: "dispatched",
    resolution: null,
    resolved_at: null,
    run_status: "finalizing",
    publication_finalized_status: null,
    publication_finalized_at: null,
    finalized_count: 0,
  });

  const genericFinalizeJob = await queue.add("finalize-channel", {
    channel_id: channelId,
    run_id: runId,
    reason: "agent-complete",
    source_revision: await productionFinalizeRevision(setup, { channelId, runId }),
    pipeline_cycle_id: batchId,
  }, {
    jobId: `legacy-generic-finalize__${systemRetryId}`,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  const genericResult = await within(
    genericFinalizeJob.waitUntilFinished(queueEvents),
    "generic Finalize blocked by active system retry",
  );
  assert.deepEqual({
    ok: genericResult.ok,
    skipped: genericResult.skipped,
    skip_reason: genericResult.skip_reason,
  }, {
    ok: true,
    skipped: true,
    skip_reason: "migration_system_retry_finalize_fence_required",
  });
  assert.equal((await observer.query(
    `SELECT count(*)::int AS finalized_count
     FROM crawler.finalized_profiles`,
  )).rows[0].finalized_count, 0);

  await stopChild(workerProcess);
  assert.equal(workerProcess.exitCode, 0, workerOutput.output());
});
