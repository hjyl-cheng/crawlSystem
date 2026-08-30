import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents } from "bullmq";
import pg from "pg";

import { resolveWorkerIdentityPolicy } from "../src/identityPolicyCatalog.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { queuesByRole } from "../src/queues.js";

const { Client } = pg;
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

function assignment(request, policy) {
  return {
    ok: true,
    ready: true,
    control_state: "READY_KEEP_ROUTE",
    workload_scope: "qy-production",
    protocol_version: 2,
    role: "channel",
    worker_id: request.worker_id,
    worker_instance_id: request.worker_instance_id,
    slot_name: "worker-entry-channel-slot",
    proxy_user: "worker-entry-channel-proxy",
    lease_id: "worker-entry-lease-1",
    lease_remaining_ms: 300_000,
    server_time: "2026-08-30T00:00:00.000Z",
    route_generation: 1,
    credential_generation: 1,
    network_identity_key: "worker-entry-network-1",
    profile_epoch: 0,
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

async function initializeScenario(client) {
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'running',0,1)`,
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

test("the real Worker entry durably records a terminal system failure before BullMQ fails the job", {
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
  const controlServer = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const payload = chunks.length > 0 ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
    controlRequests.push({ path: request.url, payload });
    let body;
    if (request.url === "/claim") {
      body = assignment(payload, resolvedPolicy.policy);
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
    business_run_key: "conflicting-business-run-key",
    crawl_mode: "full",
  }, {
    jobId,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

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
    /cached Business Run identity conflicts/,
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
    failure_code: "SYSTEM_IDENTITY",
    failed_job_attempt: 1,
  });
  assert.equal((await queued.getState()), "failed");
  assert.equal((await queue.getJob(jobId)).attemptsMade, 1);
  assert.deepEqual(controlRequests.map((request) => request.path), ["/claim"]);
  await waitFor(async () => {
    const result = await observer.query(
      `SELECT candidate.updated_at AS candidate_updated_at,
              intent.updated_at AS intent_updated_at,
              retry.updated_at AS retry_updated_at,
              batch.updated_at AS batch_updated_at,
              batch.discovered_candidate_count,
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
      && durableWrites.every(Number.isFinite)
      && batchAt >= Math.max(...durableWrites);
  }, "production failed-listener durable writes");

  await stopChild(workerProcess);
  assert.equal(workerProcess.exitCode, 0, workerOutput.output());
  assert.deepEqual(controlRequests.map((request) => request.path), ["/claim", "/release"]);
});
