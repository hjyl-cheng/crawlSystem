import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import { failChannelCandidateWorkerJob } from "../src/channelCandidateWorkerLifecycle.js";
import { activeChannelCandidateAttemptFence } from "../src/channelCandidateAttemptFence.js";
import { beginChannelCandidateValidation } from "../src/channelCandidateAttemptMutations.js";
import { markChannelCandidateJobAttemptActive } from "../src/managedWorkerJob.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";

const { Pool } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();
const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();

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

test("a stalled Worker cannot settle the Candidate or its dispatched system retry", {
  skip: databaseUrl && redisUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL and MANAGED_JOB_TEST_REDIS_URL are not configured",
  timeout: 60_000,
}, async () => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  assertDedicatedLocalRedis(redisUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `stalled-fence:${suffix}`;
  const channelId = `UCstalled${suffix}`;
  const jobId = `channel-snapshot__stalled-fence-${suffix}__${channelId}__g2`;
  const queueName = `stalled-candidate-${suffix}`;
  const prefix = `stalled-fence-${suffix}`;
  const connection = redisConnection(redisUrl);
  const pool = new Pool({ connectionString: databaseUrl, max: 4 });
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
  const queue = new Queue(queueName, { connection, prefix });
  const queueEvents = new QueueEvents(queueName, { connection, prefix });
  const firstStarted = deferred();
  const releaseFirst = deferred();
  const secondStarted = deferred();
  const releaseSecond = deferred();
  const oldMutationFinished = deferred();
  const starts = [];
  let workerA;
  let workerB;
  let candidateId;
  let migrationIntentId;
  let systemRetryId;

  try {
    await query("DROP SCHEMA IF EXISTS publication CASCADE");
    await query("DROP SCHEMA IF EXISTS crawler CASCADE");
    const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
    await query(crawlerRuntimeSchema(schema));
    const batch = await query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status
       ) VALUES ($1,$1,'running')
       RETURNING dispatch_batch_id`,
      [batchId],
    );
    assert.equal(batch.rowCount, 1);
    const candidate = await query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,'queued',2,$4,0)
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`, jobId],
    );
    candidateId = Number(candidate.rows[0].candidate_id);
    const intent = await query(
      `INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) VALUES (
         $1,current_database(),
         (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
         $3,'{}'::jsonb,repeat('a',64),$2,$4,2,now()
       ) RETURNING migration_intent_id`,
      [`stalled-system-retry:${suffix}`, candidateId, channelId, batchId],
    );
    migrationIntentId = Number(intent.rows[0].migration_intent_id);
    const systemRetry = await query(
      `INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,
         retry_dispatch_generation,dispatched_at
       ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,
                 'dispatched',2,now())
       RETURNING system_retry_id`,
      [migrationIntentId, candidateId, batchId, `${jobId}:g1`],
    );
    systemRetryId = Number(systemRetry.rows[0].system_retry_id);

    await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
    workerA = new Worker(queueName, async (job) => {
      starts.push({ worker: "A", made: job.attemptsMade, started: job.attemptsStarted });
      try {
        assert.equal(await markChannelCandidateJobAttemptActive(query, job), true);
        firstStarted.resolve();
        await releaseFirst.promise;
        try {
          const failure = {
            message: "terminal business failure from stale activation",
            parserFailure: false,
            terminalChannel: null,
            businessRunBudgetTerminal: false,
            systemFailure: null,
            parserDetails: null,
            failureDecision: { retry_mode: "none" },
            permanentFailure: true,
          };
          const settlement = await failChannelCandidateWorkerJob({
            query,
            withTransaction,
            job,
            error: new Error(failure.message),
            failure,
            refreshDispatchCandidateCounts: async () => {},
            signalReadyDiscoveryPageQualifications: async () => {},
            finishMigrationRetryIntent: async () => {},
          });
          oldMutationFinished.resolve({ settlement, error: null });
        } catch (error) {
          oldMutationFinished.resolve({ settlement: null, error });
        }
        return { worker: "A" };
      } catch (error) {
        firstStarted.reject(error);
        oldMutationFinished.reject(error);
        throw error;
      }
    }, {
      connection,
      prefix,
      concurrency: 1,
      lockDuration: 500,
      stalledInterval: 100,
      skipLockRenewal: true,
    });
    workerA.on("error", () => {});
    await workerA.waitUntilReady();

    const queued = await queue.add("channel-snapshot", {
      candidate_id: candidateId,
      dispatch_generation: 2,
      dispatch_batch_id: batchId,
    }, {
      jobId,
      attempts: 1,
      removeOnComplete: false,
      removeOnFail: false,
    });
    await within(firstStarted.promise, "first Worker start");
    await workerA.pause(true);

    workerB = new Worker(queueName, async (job) => {
      starts.push({ worker: "B", made: job.attemptsMade, started: job.attemptsStarted });
      try {
        assert.equal(await markChannelCandidateJobAttemptActive(query, job), true);
        await beginChannelCandidateValidation(query, activeChannelCandidateAttemptFence(job));
        secondStarted.resolve();
        await releaseSecond.promise;
        return { worker: "B" };
      } catch (error) {
        secondStarted.reject(error);
        throw error;
      }
    }, {
      connection,
      prefix,
      concurrency: 1,
      lockDuration: 5_000,
      stalledInterval: 100,
    });
    workerB.on("error", () => {});
    await workerB.waitUntilReady();
    await within(secondStarted.promise, "stalled Worker restart");

    releaseFirst.resolve();
    const oldMutation = await within(oldMutationFinished.promise, "stale Worker mutation");
    assert.deepEqual(starts, [
      { worker: "A", made: 0, started: 1 },
      { worker: "B", made: 0, started: 2 },
    ]);
    assert.equal(oldMutation.error, null);
    assert.deepEqual(oldMutation.settlement, {
      disposition: "failed",
      settlement: { recorded: false, fenceCleared: false },
      resolved: 0,
      terminal: true,
      attemptsMade: 0,
      maxAttempts: 1,
    });
    const state = (await query(
      `SELECT candidate.status,candidate.snapshot_active_job_id,
              candidate.snapshot_active_job_attempt,
              retry.status AS retry_status,retry.resolution
       FROM crawler.channel_candidates candidate
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       WHERE candidate.candidate_id=$1 AND retry.system_retry_id=$2`,
      [candidateId, systemRetryId],
    )).rows[0];
    assert.deepEqual(state, {
      status: "validating",
      snapshot_active_job_id: jobId,
      snapshot_active_job_attempt: 2,
      retry_status: "dispatched",
      resolution: null,
    });

    releaseSecond.resolve();
    await within(queued.waitUntilFinished(queueEvents), "new Worker completion");
  } finally {
    releaseFirst.resolve();
    releaseSecond.resolve();
    await Promise.all([
      workerA?.close().catch(() => {}),
      workerB?.close().catch(() => {}),
    ]);
    await queue.obliterate({ force: true }).catch(() => {});
    await Promise.all([queue.close(), queueEvents.close()]);
    if (systemRetryId != null) {
      await query(
        "DELETE FROM crawler.migration_system_retry_items WHERE system_retry_id=$1",
        [systemRetryId],
      ).catch(() => {});
    }
    if (migrationIntentId != null) {
      await query(
        "DELETE FROM crawler.migration_channel_intents WHERE migration_intent_id=$1",
        [migrationIntentId],
      ).catch(() => {});
    }
    if (candidateId != null) {
      await query("DELETE FROM crawler.channel_candidates WHERE candidate_id=$1", [candidateId])
        .catch(() => {});
    }
    await query(
      "DELETE FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=$1",
      [batchId],
    ).catch(() => {});
    await query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await pool.end();
  }
});
