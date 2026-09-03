import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import {
  FinalRepairExecutionRecovery,
  PostgresFinalRepairExecutionRecoveryRepository,
} from "../src/finalRepairExecutionRecovery.js";
import { ensureFinalRepairJob } from "../src/finalRepairJobRecovery.js";
import { queuesByRole } from "../src/queues.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();
const prefix = `final-repair-recovery-${randomUUID()}`;

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

async function within(promise, label, timeoutMs = 30_000) {
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

async function waitFor(check, label, timeoutMs = 30_000) {
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

function workerEnvironment() {
  const database = decodeURIComponent(new URL(databaseUrl).pathname).replace(/^\//, "");
  const redis = localRedisConfiguration(redisUrl);
  return {
    ...process.env,
    DATABASE_URL: databaseUrl,
    DATABASE_URL_FILE: "",
    EXPECTED_CRAWLER_DATABASE: database,
    FORBIDDEN_CRAWLER_DATABASE: "final_repair_forbidden_database",
    SKIP_SCHEMA_MIGRATION: "true",
    POSTGRES_POOL_MIN: "0",
    POSTGRES_STARTUP_ATTEMPTS: "1",
    REDIS_HOST: redis.host,
    REDIS_PORT: String(redis.port),
    REDIS_PASSWORD: redis.password ?? "",
    BULLMQ_PREFIX: prefix,
    WORKER_QUEUES: queuesByRole.channelCrawl,
    PROXY_SLOT_ROLE: "",
    ROTA_PROXY_CONTROL_URL: "",
    ROTA_PROXY_CONTROL_TOKEN: "",
    ROTA_FIXED_PROXY_USER: "final-repair-test",
    ROTA_BULLMQ_PROXY_PASSWORD: "final-repair-test-password",
    ROTA_PROXY_BASE_URL: "http://127.0.0.1:9",
    S3_ENDPOINT: "",
    S3_ACCESS_KEY: "",
    S3_SECRET_KEY: "",
    YOUTUBEJS_EXTRACTOR_MODE: "disabled",
    YOUTUBE_DATA_API_FALLBACK_MODE: "disabled",
  };
}

test("a real Channel Worker closes Final Repair from stored comment evidence", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const connection = localRedisConfiguration(redisUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const queue = new Queue(queuesByRole.channelCrawl, { connection, prefix });
  const finalizeQueue = new Queue(queuesByRole.finalize, { connection, prefix });
  const queueEvents = new QueueEvents(queuesByRole.channelCrawl, { connection, prefix });
  let terminalWorker = null;
  let workerProcess = null;
  let schemaInitialized = false;
  let redisReady = false;
  t.after(async () => {
    await stopChild(workerProcess).catch(() => {});
    await terminalWorker?.close().catch(() => {});
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
      await client.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await client.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await client.end().catch(() => {});
  });

  await client.connect();
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(schema);
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

  const channelId = "UCfinalRepairStoredEvidence";
  const runId = "run:final-repair-stored-evidence";
  const cycleId = "cycle:final-repair-stored-evidence";
  const ownerJobId = "channel-snapshot__stored-evidence__g2";
  const repairJobId = "final-repair__stored-evidence__1";
  const contentKey = `${channelId}:video:stored-video`;
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id
       ) VALUES ($1,$2,'Stored evidence repair',2000,'active',true,'done',$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,detail_status,expected_content_count,
         started_at,result_json,detail_job_epoch,detail_active_job_id,
         detail_active_job_attempt,detail_active_scope_key,detail_active_job_epoch
       ) VALUES (
         $1,$2,'waiting_detail','full','failed',1,now(),$3::jsonb,0,$4,1,$5,0
       )`,
      [
        runId,
        channelId,
        JSON.stringify({
          pipeline_cycle_id: cycleId,
          final_repair: { rounds: 1, job_id: repairJobId, mode: "detail" },
        }),
        ownerJobId,
        "snapshot-scope",
      ],
    );
    await tx.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,
         source_content_id,title,url,published_at,published_at_status,
         published_at_precision,duration_seconds,duration_status,view_count_text,
         view_count_status,like_count,like_count_status,comment_count,
         comment_count_status,comments_disabled,comments_first_page,access_status
       ) VALUES (
         $1,$2,$3,'video','youtube_watch_canonical','stored-video','Stored video',
         'https://www.youtube.com/watch?v=stored-video',now(),'exact','second',90,
         'exact','100','exact',3,'exact',1,'exact',false,$4::jsonb,'public'
       )`,
      [
        contentKey,
        channelId,
        runId,
        JSON.stringify({
          version: 1,
          sort: "TOP_COMMENTS",
          returned_count: 1,
          comments: [{ comment_id: "stored-comment", text: "Already stored" }],
        }),
      ],
    );
    await tx.query(
      `INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,title,source_url,content_key,
         content_type,type_status,type_source,detail_status,api_status,
         missing_fields,attempts,disposition,result_json,error_message,finished_at
       ) VALUES (
         $1,$2,'stored-video',1,'Stored video',
         'https://www.youtube.com/watch?v=stored-video',$3,
         'video','resolved','youtube_watch_canonical','done','unavailable',
         ARRAY['comments_first_page']::text[],3,'stored',$4::jsonb,
         'missing comments_first_page',now()
       )`,
      [
        runId,
        channelId,
        contentKey,
        JSON.stringify({
          access: { access_status: "public" },
          detail: { comment_count: 1, comment_count_status: "exact" },
        }),
      ],
    );
  });

  terminalWorker = new Worker(queuesByRole.channelCrawl, async (job) => (
    job.id === repairJobId
      ? { ok: true, skipped: true, reason: "content_detail_execution_fence_stale" }
      : { ok: true, terminal_owner: true }
  ), { connection, prefix, concurrency: 1 });
  await terminalWorker.waitUntilReady();
  const ownerJob = await queue.add("test-terminal-owner", {}, {
    jobId: ownerJobId,
    removeOnComplete: false,
  });
  const repairData = {
    dispatch_generation: 1,
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    run_id: runId,
    crawl_mode: "full",
    enforce_min_subscribers: false,
    repair_reason: "automatic_final_reconciliation",
    repair_round: 1,
    pipeline_cycle_id: cycleId,
  };
  const repairJob = await queue.add("channel-detail-repair", repairData, {
    jobId: repairJobId,
    attempts: 3,
    removeOnComplete: false,
    removeOnFail: false,
  });
  await Promise.all([
    within(ownerJob.waitUntilFinished(queueEvents), "terminal owner completion"),
    within(repairJob.waitUntilFinished(queueEvents), "stale Final Repair completion"),
  ]);
  await terminalWorker.close();
  terminalWorker = null;

  const recovery = new FinalRepairExecutionRecovery({
    repository: new PostgresFinalRepairExecutionRecoveryRepository({
      withTransaction: (action) => transaction(client, action),
    }),
    findJob: (jobId) => queue.getJob(jobId),
  });
  const ensured = await ensureFinalRepairJob(queue, {
    name: "channel-detail-repair",
    data: repairData,
    options: {
      jobId: repairJobId,
      attempts: 3,
      backoff: { type: "exponential", delay: 5000, jitter: 0.5 },
    },
    beforeDispatch: () => recovery.prepareDetailDispatch({
      runId,
      channelId,
      repairRound: 1,
      jobId: repairJobId,
    }),
    isBusinessComplete: () => recovery.isBusinessComplete({ runId, channelId }),
  });
  assert.equal(ensured.action, "retried_incomplete");

  workerProcess = spawn(process.execPath, ["src/worker.js"], {
    cwd: new URL("..", import.meta.url),
    env: workerEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  const workerOutput = captureChildOutput(workerProcess);
  await workerOutput.waitFor(`worker started queue=${queuesByRole.channelCrawl}`);
  const result = await within(
    repairJob.waitUntilFinished(queueEvents),
    "recovered Final Repair completion",
  );
  assert.equal(result.ok, true);
  assert.equal(result.processed, 0);
  assert.equal(result.youtubejs_request_count, 0);

  const state = (await client.query(
    `SELECT run.detail_status,run.status,run.detail_job_epoch,
            run.detail_active_job_id,run.detail_active_job_attempt,
            candidate.detail_status AS candidate_detail_status,
            candidate.api_status,candidate.missing_fields,candidate.attempts,
            candidate.error_message,
            candidate.result_json#>>'{detail,comments_first_page_source}' AS page_source
     FROM crawler.channel_runs run
     JOIN crawler.content_candidates candidate ON candidate.run_id=run.run_id
     WHERE run.run_id=$1`,
    [runId],
  )).rows[0];
  assert.deepEqual(state, {
    detail_status: "done",
    status: "waiting_agent",
    detail_job_epoch: "1",
    detail_active_job_id: repairJobId,
    detail_active_job_attempt: "2",
    candidate_detail_status: "done",
    api_status: "done",
    missing_fields: [],
    attempts: 3,
    error_message: null,
    page_source: "stored_content_reconciliation",
  });
  const finalizeJobs = await finalizeQueue.getJobs(["waiting", "prioritized", "delayed", "active"]);
  assert.equal(finalizeJobs.length, 1);
  assert.equal(finalizeJobs[0].name, "finalize-channel");
  assert.equal(finalizeJobs[0].data.run_id, runId);

  await stopChild(workerProcess);
  assert.equal(workerProcess.exitCode, 0, workerOutput.output());
});
