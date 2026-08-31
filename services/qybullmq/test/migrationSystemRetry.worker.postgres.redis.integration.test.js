import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import {
  completeChannelCandidateWorkerJob,
  describeChannelCandidateWorkerFailure,
  failChannelCandidateWorkerJob,
} from "../src/channelCandidateWorkerLifecycle.js";
import { activeChannelCandidateAttemptFence } from "../src/channelCandidateAttemptFence.js";
import { beginChannelCandidateValidation } from "../src/channelCandidateAttemptMutations.js";
import { dataApiCircuitState } from "../src/dataApiCircuit.js";
import {
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "../src/managedJobDispatchOutbox.js";
import { hasOpenPipelineCrawlerWork } from "../src/finalizeRecoveryPolicy.js";
import { markChannelCandidateJobAttemptActive } from "../src/managedWorkerJob.js";
import { settleCompletedMigrationBatch } from "../src/migrationBatchCompletion.js";
import { retryMigrationSystemFailure } from "../src/migrationSystemRetry.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { queuesByRole } from "../src/queues.js";

const { Client, Pool } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();
const batchId = "legacy-results-canary-1788072676068-25df5b5e";
const candidateId = 482;
const channelId = "UC0NoarYHkSxek05QDqhtoYw";

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

function assertDedicatedLocalRedis(value) {
  const url = new URL(value);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
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

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
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

async function initializeScenario(client) {
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,
       accepted_channel_count,rejected_channel_count,failed_channel_count,
       total_channel_count,result_json
     ) VALUES ($1,$1,'finishing',100,99,0,0,100,'{}'::jsonb)`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
       status,snapshot_dispatch_generation,snapshot_json,source_json,accepted_at
     )
     SELECT ordinal,$1,$1,'UCaccepted' || ordinal::text,
            'https://www.youtube.com/channel/UCaccepted' || ordinal::text,
            'accepted',1,'{}'::jsonb,'{}'::jsonb,now()
     FROM generate_series(1,99) AS ordinal`,
    [batchId],
  );
  const g1JobId = `channel-snapshot__${batchId}__${channelId}__g1`;
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
       status,snapshot_dispatch_generation,snapshot_active_job_id,
       snapshot_active_job_attempt,snapshot_json,source_json
     ) VALUES ($1,$2,$2,$3,$4,'queued',1,$5,0,'{}'::jsonb,
               '{"source":"legacy_results_db"}'::jsonb)`,
    [candidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`, g1JobId],
  );
  const intent = await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
     ) VALUES (
       'legacy-results-v1',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$1,
       $2,'{}'::jsonb,repeat('a',64),$1,$3,1,now()
     ) RETURNING migration_intent_id`,
    [candidateId, channelId, batchId],
  );
  await client.query(
    `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
     VALUES ('query_scheduler',$1::jsonb,now())
     ON CONFLICT (setting_key) DO UPDATE SET value_json=EXCLUDED.value_json,updated_at=now()`,
    [JSON.stringify({ status: "finishing", pipeline_cycle_id: batchId })],
  );
  return {
    g1JobId,
    migrationIntentId: Number(intent.rows[0].migration_intent_id),
  };
}

test("a completed Batch retains its dispatched G+1 recovery while the Redis Job is absent", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  assertDedicatedLocalRedis(redisUrl);
  const setup = new Client({ connectionString: databaseUrl });
  const pool = new Pool({ connectionString: databaseUrl, max: 4 });
  const connection = redisConnection(redisUrl);
  const prefix = "migration-completion-gap-e2e";
  const queue = new Queue(queuesByRole.channelCrawl, { connection, prefix });
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

  await setup.connect();
  t.after(async () => {
    await queue.obliterate({ force: true }).catch(() => {});
    await queue.close();
    await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await setup.end();
    await pool.end();
  });

  const scenario = await initializeScenario(setup);
  const runId = `run:migration-system-retry:${candidateId}`;
  await setup.query(
    `UPDATE crawler.channel_candidates
     SET status='accepted',validation_finished_at=now(),accepted_at=now(),
         snapshot_json=$2::jsonb,updated_at=now()
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
  await setup.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,status,agent_status,latest_run_id
     ) VALUES ($1,$2,'Migration retry gap','active','pending',$3)`,
    [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
  );
  await setup.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
       started_at,result_json
     ) VALUES ($1,$2,$3,'waiting_detail','full','pending',now(),$4::jsonb)`,
    [
      runId,
      channelId,
      candidateId,
      JSON.stringify({ dispatch_batch_id: batchId, pipeline_cycle_id: batchId }),
    ],
  );
  const retryItem = await setup.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES ($1,$2,$3,1,$4,0,'LEASE_CONFLICT','lease','{}'::jsonb,'pending')
     RETURNING system_retry_id`,
    [scenario.migrationIntentId, candidateId, batchId, scenario.g1JobId],
  );
  await Promise.all([queue.waitUntilReady()]);

  const settled = await settleCompletedMigrationBatch({ withTransaction, batchId });
  assert.equal(settled.status, "completed");
  assert.equal(settled.outcome, "completed_with_system_failures");

  const retry = await retryMigrationSystemFailure({
    systemRetryId: Number(retryItem.rows[0].system_retry_id),
    withTransaction,
  });
  const redisJob = await queue.getJob(retry.outbox.deterministic_job_id);
  const open = await hasOpenPipelineCrawlerWork(query, batchId);
  const completion = await settleCompletedMigrationBatch({ withTransaction, batchId });
  const persisted = (await query(
    `SELECT retry.status AS retry_status,outbox.status AS outbox_status,
            scheduler.value_json->>'status' AS scheduler_status
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.proxy_job_dispatch_outbox outbox
       ON outbox.dispatch_id=$2
     JOIN crawler.settings scheduler
       ON scheduler.setting_key='query_scheduler'
     WHERE retry.system_retry_id=$1`,
    [Number(retryItem.rows[0].system_retry_id), retry.outbox.dispatch_id],
  )).rows[0];

  assert.deepEqual({
    ...persisted,
    redis_job: redisJob == null ? "missing" : "present",
    open,
    completion: completion == null ? null : completion.status,
  }, {
    retry_status: "dispatched",
    outbox_status: "pending",
    scheduler_status: "stopped",
    redis_job: "missing",
    open: true,
    completion: null,
  });
});

test("real Worker recovers the field Candidate through one controlled G+1 dispatch", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  assertDedicatedLocalRedis(redisUrl);
  const setup = new Client({ connectionString: databaseUrl });
  const pool = new Pool({ connectionString: databaseUrl, max: 4 });
  const connection = redisConnection(redisUrl);
  const prefix = "migration-reliability-e2e";
  const queueName = queuesByRole.channelCrawl;
  const queue = new Queue(queueName, { connection, prefix });
  const queueEvents = new QueueEvents(queueName, { connection, prefix });
  const failedPersisted = deferred();
  const completedPersisted = deferred();
  const workerErrors = [];
  let worker;

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

  await setup.connect();
  t.after(async () => {
    await worker?.close().catch(() => {});
    await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close(), queueEvents.close()]);
    await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await setup.end();
    await pool.end();
  });

  const scenario = await initializeScenario(setup);
  await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
  worker = new Worker(queueName, async (job) => {
    assert.equal(await markChannelCandidateJobAttemptActive(query, job), true);
    const fence = activeChannelCandidateAttemptFence(job);
    const claimed = await beginChannelCandidateValidation(query, fence);
    assert.equal(Number(claimed.candidate_id), candidateId);
    if (Number(job.data.dispatch_generation) === 1) {
      const error = new Error("lease changed in the Renew-to-BeginTask gap");
      error.code = "LEASE_CONFLICT";
      error.status = 409;
      error.channel_execution_attempt = { youtube_requests: { failure_count: 25 } };
      throw error;
    }
    const accepted = await query(
      `UPDATE crawler.channel_candidates
       SET status='accepted',accepted_at=COALESCE(accepted_at,now()),updated_at=now()
       WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
         AND snapshot_active_job_id=$3 AND snapshot_active_job_attempt=$4
         AND status='validating'
       RETURNING candidate_id`,
      [fence.candidateId, fence.dispatchGeneration, fence.jobId, fence.bullmqAttempt],
    );
    assert.equal(accepted.rowCount, 1);
    return { accepted: true };
  }, { connection, prefix, concurrency: 1 });
  worker.on("error", (error) => workerErrors.push(error));
  worker.on("failed", (job, error) => {
    void (async () => {
      const failure = describeChannelCandidateWorkerFailure(error);
      await query(
        `INSERT INTO crawler.task_events (
           queue_name,job_id,job_name,entity_key,status,payload_json,error_message
         ) VALUES ($1,$2,$3,$4,'failed',$5::jsonb,$6)`,
        [
          queueName,
          String(job.id),
          String(job.name),
          channelId,
          JSON.stringify({
            ...job.data,
            channel_execution_attempt: error.channel_execution_attempt,
            youtube_failure_decision: failure.failureDecision,
          }),
          failure.message,
        ],
      );
      await failChannelCandidateWorkerJob({
        query,
        withTransaction,
        job,
        error,
        failure,
        refreshDispatchCandidateCounts: async () => {},
        signalReadyDiscoveryPageQualifications: async () => {},
        finishMigrationRetryIntent: async () => {},
      });
      failedPersisted.resolve();
    })().catch(failedPersisted.reject);
  });
  worker.on("completed", (job) => {
    void completeChannelCandidateWorkerJob(query, job)
      .then(completedPersisted.resolve, completedPersisted.reject);
  });

  const g1 = await queue.add("channel-snapshot", {
    candidate_id: candidateId,
    migration_intent_id: scenario.migrationIntentId,
    dispatch_generation: 1,
    dispatch_batch_id: batchId,
    pipeline_cycle_id: batchId,
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    crawl_mode: "full",
    query_id: null,
    query_text: "results.db migration",
    enforce_min_subscribers: true,
    min_subscriber_count: 1000,
    reject_if_no_recent_content: true,
  }, {
    jobId: scenario.g1JobId,
    attempts: 1,
    removeOnComplete: false,
    removeOnFail: false,
  });
  await assert.rejects(
    within(g1.waitUntilFinished(queueEvents), "G1 failure"),
    /Renew-to-BeginTask gap/,
  );
  await within(failedPersisted.promise, "G1 system failure persistence");

  const failed = (await query(
    `SELECT candidate.status,candidate.snapshot_attempts,
            candidate.snapshot_json->>'failure_type' AS failure_type,
            retry.system_retry_id,retry.status AS retry_status,
            retry.failure_code,retry.failed_dispatch_batch_id,
            retry.failed_dispatch_generation::int AS failed_generation
     FROM crawler.channel_candidates candidate
     JOIN crawler.migration_system_retry_items retry
       ON retry.candidate_id=candidate.candidate_id
     WHERE candidate.candidate_id=$1`,
    [candidateId],
  )).rows[0];
  assert.deepEqual(failed, {
    status: "failed",
    snapshot_attempts: 0,
    failure_type: "retryable_system_failure",
    system_retry_id: "1",
    retry_status: "pending",
    failure_code: "LEASE_CONFLICT",
    failed_dispatch_batch_id: batchId,
    failed_generation: 1,
  });

  const completion = await settleCompletedMigrationBatch({ withTransaction, batchId });
  assert.equal(completion.status, "completed");
  assert.equal(completion.outcome, "completed_with_system_failures");
  assert.deepEqual(
    {
      total: completion.total,
      accepted: completion.accepted,
      rejected: completion.rejected,
      failed: completion.failed,
    },
    { total: 100, accepted: 99, rejected: 0, failed: 1 },
  );
  assert.equal((await query(
    "SELECT value_json->>'status' AS status FROM crawler.settings WHERE setting_key='query_scheduler'",
  )).rows[0].status, "stopped");

  const circuit = await dataApiCircuitState({
    query,
    proxyCapacity: { active: 4, roles: { channel: { ready: 2 } } },
    detailExecutionQueue: queueName,
    detailExecutionRole: "channel",
    failureThreshold: 1,
  });
  assert.equal(circuit.recent_detail_failures, 0);
  assert.equal(circuit.open, false);

  const retry = await retryMigrationSystemFailure({
    systemRetryId: Number(failed.system_retry_id),
    withTransaction,
  });
  const repeatedRetry = await retryMigrationSystemFailure({
    systemRetryId: Number(failed.system_retry_id),
    withTransaction,
  });
  assert.equal(retry.dispatch_generation, 2);
  assert.equal(retry.created, true);
  assert.equal(repeatedRetry.dispatch_generation, 2);
  assert.equal(repeatedRetry.created, false);

  const dispatched = await new ManagedJobOutboxDispatcher({
    repository: new PostgresManagedJobDispatchRepository({ withTransaction }),
    queues: { [queueName]: queue },
  }).dispatchAvailable({ limit: 10 });
  assert.deepEqual(dispatched, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  await within(completedPersisted.promise, "G2 completion persistence");

  const final = (await query(
    `SELECT candidate.status,candidate.snapshot_dispatch_generation::int AS generation,
            candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            intent.dispatch_attempts,retry.status AS retry_status,retry.resolution,
            batch.status AS batch_status,batch.outcome,
            batch.total_channel_count,batch.accepted_channel_count,
            batch.rejected_channel_count,batch.failed_channel_count,
            count(outbox.dispatch_id) FILTER (
              WHERE (outbox.payload_json->>'dispatch_generation')::int=2
            )::int AS g2_outbox_count
     FROM crawler.channel_candidates candidate
     JOIN crawler.migration_channel_intents intent
       ON intent.target_candidate_id=candidate.candidate_id
     JOIN crawler.migration_system_retry_items retry
       ON retry.candidate_id=candidate.candidate_id
     JOIN crawler.query_dispatch_batches batch
       ON batch.dispatch_batch_id=candidate.dispatch_batch_id
     LEFT JOIN crawler.proxy_job_dispatch_outbox outbox
       ON outbox.aggregate_kind='channel_snapshot'
      AND outbox.aggregate_id=candidate.candidate_id::text
     WHERE candidate.candidate_id=$1
     GROUP BY candidate.candidate_id,intent.migration_intent_id,retry.system_retry_id,
              batch.dispatch_batch_id`,
    [candidateId],
  )).rows[0];
  assert.deepEqual(final, {
    status: "accepted",
    generation: 2,
    snapshot_active_job_id: null,
    snapshot_active_job_attempt: null,
    dispatch_attempts: 2,
    retry_status: "dispatched",
    resolution: null,
    batch_status: "completed",
    outcome: "completed_with_system_failures",
    total_channel_count: 100,
    accepted_channel_count: 99,
    rejected_channel_count: 0,
    failed_channel_count: 1,
    g2_outbox_count: 1,
  });

  assert.deepEqual(workerErrors, []);
});
