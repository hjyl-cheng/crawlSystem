import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { Queue, QueueEvents, Worker } from "bullmq";
import pg from "pg";

import {
  completeChannelCandidateWorkerJob,
  failChannelCandidateWorkerJob,
} from "../src/channelCandidateWorkerLifecycle.js";
import { activeChannelCandidateAttemptFence } from "../src/channelCandidateAttemptFence.js";
import { beginChannelCandidateValidation } from "../src/channelCandidateAttemptMutations.js";
import { markChannelCandidateJobAttemptActive } from "../src/managedWorkerJob.js";
import { finishMigrationRetryIntent } from "../src/migrationRetryIntent.js";
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
  const runId = `run:stalled-fence:${suffix}`;
  const previousBusinessRunId = `run:stalled-fence-previous:${suffix}`;
  const retryIntentId = `retry-intent:${suffix}`;
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
  let firstAttemptJob;
  let secondAttemptJob;

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
    await query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,ready_for_agent
       ) VALUES ($1,$2,'Stalled Candidate Fence','active',false)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         detail_status,expected_content_count,result_json
       ) VALUES ($1,$2,$3,'waiting_detail','full',30,'queued',7,$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify({ sentinel: "current_attempt" })],
    );
    await query(
      `INSERT INTO crawler.business_run_bindings (
         business_run_key,business_run_id,intent_hash,intent_json,
         identity_policy_id,identity_policy_version,identity_policy_hash,
         run_kind,channel_id,candidate_id,status,terminal_reason
       ) VALUES ($1,$2,repeat('b',64),'{}'::jsonb,'test-policy',1,
                 repeat('c',64),'full',$3,$4,'terminal','test_previous_run')`,
      [`full-candidate:${candidateId}:previous`, previousBusinessRunId, channelId, candidateId],
    );
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
    await query(
      `INSERT INTO crawler.migration_retry_intents (
         retry_intent_id,request_key,candidate_id,previous_business_run_id,
         new_business_run_id,new_business_run_key,new_job_id,dispatch_generation,
         reason,intent_hash,job_payload_json,status,dispatch_status,dispatched_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,2,'stalled attempt fence test',
                 repeat('d',64),'{}'::jsonb,'running','enqueued',now())`,
      [
        retryIntentId,
        `stalled-attempt:${suffix}`,
        candidateId,
        previousBusinessRunId,
        `run:stalled-fence-recovery:${suffix}`,
        `full-candidate:${candidateId}:recovery:${suffix}`,
        jobId,
      ],
    );

    await Promise.all([queue.waitUntilReady(), queueEvents.waitUntilReady()]);
    workerA = new Worker(queueName, async (job) => {
      firstAttemptJob = job;
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
            finishMigrationRetryIntent,
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
      run_id: runId,
      retry_intent_id: retryIntentId,
    }, {
      jobId,
      attempts: 1,
      removeOnComplete: false,
      removeOnFail: false,
    });
    await within(firstStarted.promise, "first Worker start");
    await workerA.pause(true);

    workerB = new Worker(queueName, async (job) => {
      secondAttemptJob = job;
      starts.push({ worker: "B", made: job.attemptsMade, started: job.attemptsStarted });
      try {
        assert.equal(await markChannelCandidateJobAttemptActive(query, job), true);
        await beginChannelCandidateValidation(query, activeChannelCandidateAttemptFence(job));
        secondStarted.resolve();
        await releaseSecond.promise;
        const fence = activeChannelCandidateAttemptFence(job);
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
        const completed = await completeChannelCandidateWorkerJob(query, job, {
          withTransaction,
          finishMigrationRetryIntent,
        });
        assert.deepEqual(completed, {
          cleared: true,
          resolved: 0,
          intentFinished: true,
        });
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
      runFailureRecorded: false,
      terminal: true,
      attemptsMade: 0,
      maxAttempts: 1,
    });
    const staleCompletion = await completeChannelCandidateWorkerJob(query, firstAttemptJob, {
      withTransaction,
      finishMigrationRetryIntent,
    });
    assert.deepEqual(staleCompletion, {
      cleared: false,
      resolved: 0,
      intentFinished: false,
    });
    await query(
      "UPDATE crawler.migration_retry_intents SET new_job_id=$2 WHERE retry_intent_id=$1",
      [retryIntentId, `${jobId}:conflict`],
    );
    await assert.rejects(
      completeChannelCandidateWorkerJob(query, secondAttemptJob, {
        withTransaction,
        finishMigrationRetryIntent,
      }),
      (error) => error?.code === "MIGRATION_RETRY_INTENT_FENCE_STALE",
    );
    const rolledBackCandidate = (await query(
      `SELECT snapshot_active_job_id,snapshot_active_job_attempt
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0];
    assert.deepEqual(rolledBackCandidate, {
      snapshot_active_job_id: jobId,
      snapshot_active_job_attempt: 2,
    });
    await query(
      "UPDATE crawler.migration_retry_intents SET new_job_id=$2 WHERE retry_intent_id=$1",
      [retryIntentId, jobId],
    );
    const state = (await query(
      `SELECT candidate.status,candidate.snapshot_active_job_id,
              candidate.snapshot_active_job_attempt,
              retry.status AS retry_status,retry.resolution,
              intent.status AS intent_status,intent.finished_at AS intent_finished_at,
              run.status AS run_status,run.detail_status AS run_detail_status,
              run.expected_content_count,run.error_message AS run_error_message,
              run.result_json AS run_result_json
       FROM crawler.channel_candidates candidate
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       JOIN crawler.channel_runs run ON run.candidate_id=candidate.candidate_id
       JOIN crawler.migration_retry_intents intent ON intent.candidate_id=candidate.candidate_id
       WHERE candidate.candidate_id=$1 AND retry.system_retry_id=$2`,
      [candidateId, systemRetryId],
    )).rows[0];
    assert.deepEqual(state, {
      status: "validating",
      snapshot_active_job_id: jobId,
      snapshot_active_job_attempt: 2,
      retry_status: "dispatched",
      resolution: null,
      intent_status: "running",
      intent_finished_at: null,
      run_status: "waiting_detail",
      run_detail_status: "queued",
      expected_content_count: 7,
      run_error_message: null,
      run_result_json: { sentinel: "current_attempt" },
    });

    releaseSecond.resolve();
    await within(queued.waitUntilFinished(queueEvents), "new Worker completion");
    const finalState = (await query(
      `SELECT candidate.status,candidate.snapshot_active_job_id,
              candidate.snapshot_active_job_attempt,
              retry.status AS retry_status,retry.resolution,
              intent.status AS intent_status,intent.dispatch_status,
              intent.finished_at IS NOT NULL AS intent_finished,
              intent.terminal_job_attempt
       FROM crawler.channel_candidates candidate
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=candidate.candidate_id
       JOIN crawler.migration_retry_intents intent
         ON intent.candidate_id=candidate.candidate_id
       WHERE candidate.candidate_id=$1 AND retry.system_retry_id=$2`,
      [candidateId, systemRetryId],
    )).rows[0];
    assert.deepEqual(finalState, {
      status: "accepted",
      snapshot_active_job_id: null,
      snapshot_active_job_attempt: null,
      retry_status: "dispatched",
      resolution: null,
      intent_status: "finished",
      dispatch_status: "terminal",
      intent_finished: true,
      terminal_job_attempt: "2",
    });
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
    await query(
      "DELETE FROM crawler.migration_retry_intents WHERE retry_intent_id=$1",
      [retryIntentId],
    ).catch(() => {});
    await query(
      "DELETE FROM crawler.business_run_bindings WHERE business_run_id=$1",
      [previousBusinessRunId],
    ).catch(() => {});
    if (candidateId != null) {
      await query("DELETE FROM crawler.channel_runs WHERE run_id=$1", [runId]).catch(() => {});
      await query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
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
