import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

const { Pool } = pg;
const outputPath = process.argv[2];
const scenario = String(process.argv[3] ?? "uploads");
if (!new Set(["uploads", "dormant"]).has(scenario)) {
  throw new Error(`unsupported Channel Snapshot Fence scenario: ${scenario}`);
}
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();
const suffix = randomUUID().replaceAll("-", "");
const batchId = `snapshot-fence:${suffix}`;
const channelId = `UCsnapshotFence${suffix}`;
const runId = `run:snapshot-fence:${suffix}`;
const businessRunKey = `full:snapshot-fence:${suffix}`;
const jobId = `channel-snapshot__snapshot-fence-${suffix}__${channelId}__g1`;
const bullmqPrefix = `snapshot-fence-${suffix}`;

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
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

function redisConnection(value) {
  const url = new URL(value);
  return {
    host: url.hostname,
    port: Number(url.port),
    password: url.password || undefined,
    maxRetriesPerRequest: null,
  };
}

function databaseName(value) {
  return decodeURIComponent(new URL(value).pathname).replace(/^\//, "");
}

function report(value) {
  writeFileSync(outputPath, JSON.stringify(value), "utf8");
}

function executionContext(attempt) {
  const proxy = {
    slot_name: "snapshot-fence-slot",
    lease_id: "snapshot-fence-lease",
    route_generation: 1,
    identity_policy_id: "snapshot-fence-test",
    network_identity_key: "snapshot-fence-network",
    profile_epoch: 0,
  };
  return {
    attempt_id: `snapshot-fence:${attempt}`,
    proxy,
    get_proxy_snapshot: () => proxy,
  };
}

const redis = redisConnection(redisUrl);
Object.assign(process.env, {
  DATABASE_URL: databaseUrl,
  DATABASE_URL_FILE: "",
  EXPECTED_CRAWLER_DATABASE: databaseName(databaseUrl),
  FORBIDDEN_CRAWLER_DATABASE: "snapshot_fence_forbidden_database",
  SKIP_SCHEMA_MIGRATION: "true",
  POSTGRES_POOL_MIN: "0",
  POSTGRES_STARTUP_ATTEMPTS: "1",
  REDIS_HOST: redis.host,
  REDIS_PORT: String(redis.port),
  REDIS_PASSWORD: redis.password ?? "",
  BULLMQ_PREFIX: bullmqPrefix,
  YOUTUBE_CHANNEL_INLINE_DETAILS: "false",
  YOUTUBE_DATA_API_FALLBACK_MODE: "disabled",
  S3_ENDPOINT: "",
  S3_ACCESS_KEY: "",
  S3_SECRET_KEY: "",
});

const firstFetchStarted = deferred();
const releaseFirstFetch = deferred();
const secondFetchStarted = deferred();
const releaseSecondFetch = deferred();
globalThis.__channelSnapshotAttemptFenceState = {
  scenario,
  fetchCount: 0,
  firstFetchStarted,
  releaseFirstFetch,
  secondFetchStarted,
  releaseSecondFetch,
};

const pool = new Pool({ connectionString: databaseUrl, max: 8 });
const query = pool.query.bind(pool);
const queue = new Queue("youtube-channel-crawl", { connection: redis, prefix: bullmqPrefix });
const queueEvents = new QueueEvents("youtube-channel-crawl", { connection: redis, prefix: bullmqPrefix });
const detailQueue = new Queue("youtube-content-detail", { connection: redis, prefix: bullmqPrefix });
const finalizeQueue = new Queue("youtube-finalize", { connection: redis, prefix: bullmqPrefix });
const firstOutcome = deferred();
const secondOutcome = deferred();
let workerA;
let workerB;
let pipelineCloseDb;

async function initialize() {
  await query("DROP SCHEMA IF EXISTS publication CASCADE");
  await query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../../src/schema.sql", import.meta.url), "utf8");
  await query(schema);
  await query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,total_channel_count
     ) VALUES ($1,$1,'running',1)`,
    [batchId],
  );
  const candidate = await query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json
     ) VALUES ($1,$1,$2,$3,'queued',1,'{}'::jsonb,'{}'::jsonb)
     RETURNING candidate_id`,
    [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  const candidateId = Number(candidate.rows[0].candidate_id);
  await query(
    `INSERT INTO crawler.business_run_bindings (
       business_run_key,business_run_id,intent_schema_version,intent_hash,intent_json,
       identity_policy_id,identity_policy_version,identity_policy_hash,run_kind,
       channel_id,candidate_id,status
     ) VALUES ($1,$2,1,$3,'{}'::jsonb,$4,1,$5,'full',$6,$7,'reserved')`,
    [businessRunKey, runId, `sha256:${"a".repeat(64)}`, "snapshot-fence-test", `sha256:${"b".repeat(64)}`, channelId, candidateId],
  );
  return candidateId;
}

async function seedCurrentContent() {
  await query(
    `INSERT INTO crawler.contents (
       content_key,channel_id,run_id,content_type,source_content_id,title,is_recent
     ) VALUES ($1,$2,$3,'video',$4,'Previously current',true)`,
    [`${channelId}:video:previously-current`, channelId, runId, "previously-current"],
  );
}

async function snapshotState() {
  const result = await query(
    `SELECT run.status,run.detail_status,run.expected_content_count,
            channel.status AS channel_status,channel.dormant_reason,
            existing.is_recent AS existing_is_recent,
            array_agg(candidate.source_content_id ORDER BY candidate.source_content_id)
              FILTER (WHERE candidate.candidate_id IS NOT NULL) AS video_ids,
            (SELECT count(*)::int
             FROM crawler.crawl_observations observation
             WHERE observation.run_id=run.run_id) AS observation_count,
            (SELECT count(*)::int
             FROM crawler.crawler_outbox outbox
             JOIN crawler.crawl_observations observation
               ON observation.observation_id=outbox.observation_id
             WHERE observation.run_id=run.run_id) AS outbox_count
     FROM crawler.channel_runs run
     JOIN crawler.channels channel ON channel.channel_id=run.channel_id
     LEFT JOIN crawler.contents existing
       ON existing.channel_id=run.channel_id
      AND existing.source_content_id='previously-current'
     LEFT JOIN crawler.content_candidates candidate ON candidate.run_id=run.run_id
     WHERE run.run_id=$1
     GROUP BY run.run_id,run.status,run.detail_status,run.expected_content_count,
              channel.status,channel.dormant_reason,existing.is_recent`,
    [runId],
  );
  const finalizeCounts = await finalizeQueue.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
    "paused",
    "waiting-children",
  );
  const finalizeJobs = await finalizeQueue.getJobs([
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
    "paused",
    "waiting-children",
  ], 0, -1, true);
  const detailJobs = await detailQueue.getJobs([
    "waiting",
    "active",
    "delayed",
    "completed",
    "failed",
    "paused",
    "waiting-children",
  ], 0, -1, true);
  return {
    ...result.rows[0],
    finalize_job_count: Object.values(finalizeCounts).reduce(
      (total, count) => total + Number(count ?? 0),
      0,
    ),
    finalize_reasons: finalizeJobs
      .map((job) => String(job.data?.reason ?? ""))
      .sort(),
    detail_jobs: detailJobs.map((job) => ({
      id: job.id,
      origin_candidate_id: Number(job.data?.origin_candidate_id),
      origin_dispatch_generation: Number(job.data?.origin_dispatch_generation),
      origin_snapshot_job_id: job.data?.origin_snapshot_job_id ?? null,
      origin_snapshot_job_attempt: Number(job.data?.origin_snapshot_job_attempt),
    })),
  };
}

try {
  const candidateId = await initialize();
  const [
    { processChannelCrawlV2 },
    { closeDb },
    { markChannelCandidateJobAttemptActive },
    { runWithChannelExecution },
  ] = await Promise.all([
    import("../../src/pipelineV2.js"),
    import("../../src/db.js"),
    import("../../src/managedWorkerJob.js"),
    import("../../src/channelExecutionContext.js"),
  ]);
  pipelineCloseDb = closeDb;

  await Promise.all([
    queue.obliterate({ force: true }),
    detailQueue.obliterate({ force: true }),
    finalizeQueue.obliterate({ force: true }),
  ]);
  await Promise.all([
    queue.waitUntilReady(),
    queueEvents.waitUntilReady(),
    detailQueue.waitUntilReady(),
    finalizeQueue.waitUntilReady(),
  ]);
  workerA = new Worker("youtube-channel-crawl", async (job) => {
    try {
      if (!(await markChannelCandidateJobAttemptActive(query, job))) {
        throw new Error("attempt 1 failed to claim its Candidate Fence");
      }
      const value = await runWithChannelExecution(
        executionContext(job.attemptsStarted),
        () => processChannelCrawlV2(job),
      );
      firstOutcome.resolve({ status: "resolved", value });
    } catch (error) {
      firstOutcome.resolve({
        status: "rejected",
        code: error?.code ?? null,
        message: error?.message ?? String(error),
      });
    }
    return { worker: "A" };
  }, {
    connection: redis,
    prefix: bullmqPrefix,
    concurrency: 1,
    lockDuration: 500,
    stalledInterval: 100,
    skipLockRenewal: true,
  });
  workerA.on("error", () => {});
  await workerA.waitUntilReady();

  const queued = await queue.add("channel-snapshot", {
    candidate_id: candidateId,
    dispatch_generation: 1,
    dispatch_batch_id: batchId,
    pipeline_cycle_id: batchId,
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    run_id: runId,
    business_run_key: businessRunKey,
    crawl_mode: "full",
    reject_if_no_recent_content: scenario === "dormant",
  }, {
    jobId,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });

  await within(Promise.race([
    firstFetchStarted.promise,
    firstOutcome.promise.then((outcome) => {
      throw new Error(`attempt 1 ended before Uploads fetch: ${JSON.stringify(outcome)}`);
    }),
  ]), "attempt 1 Uploads fetch");
  await seedCurrentContent();
  await workerA.close(true);

  workerB = new Worker("youtube-channel-crawl", async (job) => {
    try {
      if (!(await markChannelCandidateJobAttemptActive(query, job))) {
        throw new Error(`attempt ${job.attemptsStarted} failed to claim its Candidate Fence`);
      }
      const value = await runWithChannelExecution(
        executionContext(job.attemptsStarted),
        () => processChannelCrawlV2(job),
      );
      secondOutcome.resolve({ status: "resolved", value });
      return value;
    } catch (error) {
      secondOutcome.resolve({
        status: "rejected",
        code: error?.code ?? null,
        message: error?.message ?? String(error),
      });
      throw error;
    }
  }, {
    connection: redis,
    prefix: bullmqPrefix,
    concurrency: 1,
    lockDuration: 5_000,
    stalledInterval: 100,
  });
  workerB.on("error", () => {});
  await workerB.waitUntilReady();
  await new Promise((resolve) => setTimeout(resolve, 750));
  await workerB.moveStalledJobsToWait();
  await new Promise((resolve) => setTimeout(resolve, 150));
  await workerB.moveStalledJobsToWait();
  await within(Promise.race([
    secondFetchStarted.promise,
    secondOutcome.promise.then((outcome) => {
      throw new Error(`attempt 2 ended before Uploads fetch: ${JSON.stringify(outcome)}`);
    }),
  ]), "attempt 2 Uploads fetch");

  releaseFirstFetch.resolve();
  const staleOutcome = await within(firstOutcome.promise, "stale attempt outcome");
  const beforeCurrentAttempt = await snapshotState();

  releaseSecondFetch.resolve();
  await within(queued.waitUntilFinished(queueEvents), "current attempt completion");
  const afterCurrentAttempt = await snapshotState();
  if (scenario === "uploads") {
    delete beforeCurrentAttempt.channel_status;
    delete beforeCurrentAttempt.dormant_reason;
    delete beforeCurrentAttempt.observation_count;
    delete beforeCurrentAttempt.outbox_count;
    delete beforeCurrentAttempt.finalize_job_count;
    delete beforeCurrentAttempt.finalize_reasons;
    delete afterCurrentAttempt.channel_status;
    delete afterCurrentAttempt.dormant_reason;
    delete afterCurrentAttempt.observation_count;
    delete afterCurrentAttempt.outbox_count;
    delete afterCurrentAttempt.finalize_job_count;
    delete afterCurrentAttempt.finalize_reasons;
  } else {
    delete beforeCurrentAttempt.existing_is_recent;
    delete afterCurrentAttempt.existing_is_recent;
  }
  report({ staleOutcome, beforeCurrentAttempt, afterCurrentAttempt });
} catch (error) {
  report({ harnessError: error?.stack ?? error?.message ?? String(error) });
  process.exitCode = 1;
} finally {
  releaseFirstFetch.resolve();
  releaseSecondFetch.resolve();
  await Promise.all([
    workerA?.close().catch(() => {}),
    workerB?.close().catch(() => {}),
  ]);
  await queue.obliterate({ force: true }).catch(() => {});
  await detailQueue.obliterate({ force: true }).catch(() => {});
  await finalizeQueue.obliterate({ force: true }).catch(() => {});
  await Promise.all([
    queue.close().catch(() => {}),
    queueEvents.close().catch(() => {}),
    detailQueue.close().catch(() => {}),
    finalizeQueue.close().catch(() => {}),
  ]);
  await pipelineCloseDb?.().catch(() => {});
  await query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
  await query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
  await pool.end().catch(() => {});
}

process.exit();
