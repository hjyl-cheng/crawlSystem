import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import {
  assertInlineContentDetailExecutionCurrent,
  claimContentDetailExecution,
  contentDetailExecutionFence,
  lockContentDetailExecution,
  prepareContentDetailExecutionRequeue,
} from "../src/contentDetailExecutionFence.js";
import {
  lockMigrationSystemRetryFinalizeJobFence,
  migrationSystemRetryFinalizeJobFence,
} from "../src/migrationSystemRetryRecovery.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { retryFullCrawlSnapshotJob } from "../src/finalRepairJobRecovery.js";
import { markChannelCandidateJobAttemptActive } from "../src/managedWorkerJob.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
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

function detailJob({
  runId,
  channelId,
  pipelineCycleId,
  attemptsStarted,
  systemRetryId = null,
  candidateId = null,
  dispatchGeneration = null,
  jobEpoch = 0,
  originCandidateId = null,
  originDispatchGeneration = null,
  originSnapshotJobId = null,
  originSnapshotJobAttempt = null,
}) {
  const origin = originCandidateId == null
    ? {}
    : {
        origin_candidate_id: originCandidateId,
        origin_dispatch_generation: originDispatchGeneration,
        origin_snapshot_job_id: originSnapshotJobId,
        origin_snapshot_job_attempt: originSnapshotJobAttempt,
      };
  return {
    id: `content-detail__${runId}${originCandidateId == null ? "" : `__g${originDispatchGeneration}__a${originSnapshotJobAttempt}`}`,
    name: "content-detail-batch",
    queueName: "youtube-content-detail",
    attemptsStarted,
    data: {
      run_id: runId,
      channel_id: channelId,
      pipeline_cycle_id: pipelineCycleId,
      content_detail_job_epoch: jobEpoch,
      ...origin,
      ...(systemRetryId == null
        ? {}
        : {
            migration_system_retry_id: systemRetryId,
            candidate_id: candidateId,
            dispatch_generation: dispatchGeneration,
            dispatch_batch_id: pipelineCycleId,
          }),
    },
  };
}

test("queued Content Detail follows the originating Snapshot attempt Fence", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
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
  await client.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;

  const candidateId = 481;
  const channelId = "UCqueuedDetailOriginFence";
  const batchId = "queued-detail-origin-fence";
  const runId = "run-queued-detail-origin-fence";
  const snapshotJobId = "channel-snapshot-origin-fence";
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'running',1,1)`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
       snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$2,$2,$3,$4,'accepted',1,$5,2,'{}'::jsonb,'{}'::jsonb,now(),now())`,
    [candidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`, snapshotJobId],
  );
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Queued Detail Origin Fence',2000,'active',true,'pending',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId, candidateId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES ($1,$2,$3,'waiting_detail','full','queued',1,now(),$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify({ pipeline_cycle_id: batchId })],
    );
  });

  const queuedFence = (originAttempt) => contentDetailExecutionFence(detailJob({
    runId,
    channelId,
    pipelineCycleId: batchId,
    attemptsStarted: 1,
    originCandidateId: candidateId,
    originDispatchGeneration: 1,
    originSnapshotJobId: snapshotJobId,
    originSnapshotJobAttempt: originAttempt,
  }));
  const stale = queuedFence(1);
  const current = queuedFence(2);
  assert.equal(
    await transaction(client, (tx) => claimContentDetailExecution(tx, stale)),
    null,
    "attempt 1 Detail cannot write after Snapshot attempt 2 takes over",
  );
  assert.ok(await transaction(client, (tx) => claimContentDetailExecution(tx, current)));

  await client.query(
    `UPDATE crawler.channel_runs
     SET detail_active_job_id=NULL,detail_active_job_attempt=NULL,
         detail_active_scope_key=NULL,detail_active_job_epoch=NULL
     WHERE run_id=$1`,
    [runId],
  );
  await client.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL
     WHERE candidate_id=$1`,
    [candidateId],
  );
  assert.ok(
    await transaction(client, (tx) => claimContentDetailExecution(tx, current)),
    "the queued Detail remains valid after its parent Snapshot completes",
  );
});

test("an inline stale Detail result stops the parent Candidate before follow-up writes", () => {
  const candidateAttemptFence = {
    candidateId: 482,
    dispatchGeneration: 1,
    jobId: "channel-snapshot-inline-job",
    bullmqAttempt: 1,
  };
  assert.throws(
    () => assertInlineContentDetailExecutionCurrent({
      ok: true,
      skipped: true,
      reason: "content_detail_execution_fence_stale",
    }, candidateAttemptFence),
    (error) => error?.name === "StaleChannelCandidateAttemptError"
      && error?.code === "CANDIDATE_ATTEMPT_FENCE_STALE",
  );
  const current = { ok: true, processed: 1 };
  assert.equal(
    assertInlineContentDetailExecutionCurrent(current, candidateAttemptFence),
    current,
  );
});

test("Migration recovery Content Detail uses Candidate to Retry to Run to Channel lock order", async () => {
  const statements = [];
  const job = detailJob({
    runId: "run-lock-order",
    channelId: "UClockorder",
    pipelineCycleId: "batch-lock-order",
    attemptsStarted: 1,
    systemRetryId: 19,
    candidateId: 482,
    dispatchGeneration: 2,
  });
  const fence = contentDetailExecutionFence(job);
  const client = {
    async query(sql, params) {
      statements.push({ sql: String(sql), params });
      if (String(sql).includes("content-detail-lock:candidate")) {
        return { rows: [{
          candidate_id: "482",
          status: "accepted",
          dispatch_batch_id: "batch-lock-order",
          pipeline_cycle_id: "batch-lock-order",
          snapshot_dispatch_generation: "2",
          snapshot_active_job_id: null,
          snapshot_active_job_attempt: null,
        }], rowCount: 1 };
      }
      if (String(sql).includes("content-detail-lock:retry")) {
        return { rows: [{
          system_retry_id: "19",
          candidate_id: "482",
          failed_dispatch_batch_id: "batch-lock-order",
          failed_dispatch_generation: "1",
          status: "dispatched",
          retry_dispatch_generation: "2",
          recovery_run_id: "run-lock-order",
        }], rowCount: 1 };
      }
      if (String(sql).includes("content-detail-lock:run")) {
        return { rows: [{
          run_id: "run-lock-order",
          channel_id: "UClockorder",
          candidate_id: "482",
          status: "waiting_detail",
          detail_status: "queued",
          result_json: {
            dispatch_batch_id: "batch-lock-order",
            pipeline_cycle_id: "batch-lock-order",
          },
          detail_active_job_id: null,
          detail_active_job_attempt: null,
          detail_active_scope_key: null,
          detail_job_epoch: "0",
          detail_active_job_epoch: null,
        }], rowCount: 1 };
      }
      if (String(sql).includes("content-detail-lock:channel")) {
        return { rows: [{ latest_run_id: "run-lock-order", channel_status: "active" }], rowCount: 1 };
      }
      if (String(sql).includes("UPDATE crawler.channel_runs")) {
        return { rows: [{ run_id: "run-lock-order" }], rowCount: 1 };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  assert.ok(await claimContentDetailExecution(client, fence));
  assert.match(statements[0].sql, /content-detail-lock:candidate[\s\S]*FOR UPDATE/);
  assert.match(statements[1].sql, /content-detail-lock:retry[\s\S]*FOR UPDATE/);
  assert.match(statements[2].sql, /content-detail-lock:run[\s\S]*FOR UPDATE/);
  assert.match(statements[3].sql, /content-detail-lock:channel[\s\S]*FOR UPDATE/);
});

async function seedCandidate(client, {
  candidateId,
  channelId,
  batchId,
  oldRunId,
  recoveryRunId,
}) {
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'completed',1,1)`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$2,$2,$3,$4,'accepted',2,'{}'::jsonb,
               '{"source":"legacy_results_db"}'::jsonb,now(),now())`,
    [candidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  const migrationIntentId = Number((await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,channel_id,
       source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id,
       dispatch_attempts,last_dispatch_at
     ) VALUES (
       'legacy-results-v1',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$1,$2,
       '{}'::jsonb,repeat('c',64),$1,$3,2,now()
     ) RETURNING migration_intent_id`,
    [candidateId, channelId, batchId],
  )).rows[0].migration_intent_id);
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Content Detail Fence',2000,'active',true,'pending',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, oldRunId, candidateId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json,
         publication_finalized_status,publication_finalized_at
       ) VALUES
         ($1,$3,$4,'done','full','done',1,now(),$5::jsonb,'ready_auto',now()),
         ($2,$3,$4,'waiting_detail','full','queued',1,now(),$6::jsonb,NULL,NULL)`,
      [
        oldRunId,
        recoveryRunId,
        channelId,
        candidateId,
        JSON.stringify({ pipeline_cycle_id: `${batchId}:old` }),
        JSON.stringify({ pipeline_cycle_id: batchId, dispatch_batch_id: batchId }),
      ],
    );
  });
  return { migrationIntentId };
}

test("Content Detail ownership rejects superseded ordinary and stalled executions", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
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
  await client.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;

  const candidateId = 482;
  const channelId = "UC0NoarYHkSxek05QDqhtoYw";
  const batchId = "content-detail-fence-recovery";
  const oldRunId = "content-detail-fence-old-run";
  const recoveryRunId = "content-detail-fence-recovery-run";
  const { migrationIntentId } = await seedCandidate(client, {
    candidateId,
    channelId,
    batchId,
    oldRunId,
    recoveryRunId,
  });

  const ordinary = contentDetailExecutionFence(detailJob({
    runId: oldRunId,
    channelId,
    pipelineCycleId: `${batchId}:old`,
    attemptsStarted: 1,
  }));
  assert.ok(await transaction(client, (tx) => claimContentDetailExecution(tx, ordinary)));

  const systemRetryId = Number((await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status,
       retry_dispatch_generation,recovery_run_id
     ) VALUES (
       $1,$2,$3,1,'failed-channel-job',1,'LEASE_CONFLICT','lease','{}'::jsonb,
       'pending',2,$4
     ) RETURNING system_retry_id`,
    [migrationIntentId, candidateId, batchId, recoveryRunId],
  )).rows[0].system_retry_id);
  await client.query(
    "UPDATE crawler.channels SET latest_run_id=$2,updated_at=now() WHERE channel_id=$1",
    [channelId, recoveryRunId],
  );

  assert.equal(
    await transaction(client, (tx) => lockContentDetailExecution(tx, ordinary)),
    null,
    "pinning an active Migration System Retry must invalidate an old ordinary Detail Job",
  );
  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET status='dispatched',dispatched_at=now(),updated_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId],
  );

  const finalizeFence = migrationSystemRetryFinalizeJobFence({
    name: "finalize-channel",
    data: {
      migration_system_retry_id: systemRetryId,
      candidate_id: candidateId,
      dispatch_generation: 2,
      dispatch_batch_id: batchId,
      run_id: recoveryRunId,
      channel_id: channelId,
      source_revision: "content-detail-lock-order-test",
    },
  });
  const ordinaryClient = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const finalizeClient = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  await Promise.all([ordinaryClient.connect(), finalizeClient.connect()]);
  try {
    await Promise.all([
      transaction(ordinaryClient, async (tx) => {
        await tx.query("SET LOCAL deadlock_timeout='50ms'");
        await tx.query("SET LOCAL statement_timeout='3s'");
        return lockContentDetailExecution(tx, ordinary);
      }),
      transaction(finalizeClient, async (tx) => {
        await tx.query("SET LOCAL deadlock_timeout='50ms'");
        await tx.query("SET LOCAL statement_timeout='3s'");
        return lockMigrationSystemRetryFinalizeJobFence(tx, finalizeFence);
      }),
    ]);
  } finally {
    await Promise.all([ordinaryClient.end(), finalizeClient.end()]);
  }

  const firstRecovery = contentDetailExecutionFence(detailJob({
    runId: recoveryRunId,
    channelId,
    pipelineCycleId: batchId,
    attemptsStarted: 1,
    systemRetryId,
    candidateId,
    dispatchGeneration: 2,
  }));
  const takeover = contentDetailExecutionFence(detailJob({
    runId: recoveryRunId,
    channelId,
    pipelineCycleId: batchId,
    attemptsStarted: 2,
    systemRetryId,
    candidateId,
    dispatchGeneration: 2,
  }));
  assert.deepEqual(
    await transaction(client, (tx) => prepareContentDetailExecutionRequeue(
      tx,
      firstRecovery,
      { findExistingJob: async () => null },
    )),
    { ready: true, cleared: false, existingJob: null, jobEpoch: 0 },
    "a recovery Job may supersede a different ordinary execution scope without clearing it",
  );
  assert.ok(await transaction(client, (tx) => claimContentDetailExecution(tx, firstRecovery)));
  assert.ok(await transaction(client, (tx) => claimContentDetailExecution(tx, takeover)));
  assert.equal(
    await transaction(client, (tx) => lockContentDetailExecution(tx, firstRecovery)),
    null,
    "attemptsStarted=1 must not commit after attemptsStarted=2 takes ownership",
  );
  assert.ok(await transaction(client, (tx) => lockContentDetailExecution(tx, takeover)));

  const representedJob = { id: takeover.jobId };
  const retained = await transaction(client, (tx) => prepareContentDetailExecutionRequeue(
    tx,
    firstRecovery,
    { findExistingJob: async () => representedJob },
  ));
  assert.deepEqual(retained, {
    ready: false,
    cleared: false,
    existingJob: representedJob,
    jobEpoch: 0,
  });
  assert.ok(
    await transaction(client, (tx) => lockContentDetailExecution(tx, takeover)),
    "a represented Redis Job must retain its active PostgreSQL attempt",
  );

  const prepared = await transaction(client, (tx) => prepareContentDetailExecutionRequeue(
    tx,
    firstRecovery,
    { findExistingJob: async () => null },
  ));
  assert.deepEqual(prepared, {
    ready: true,
    cleared: true,
    existingJob: null,
    jobEpoch: 1,
  });
  const recreatedRecovery = contentDetailExecutionFence(detailJob({
    runId: recoveryRunId,
    channelId,
    pipelineCycleId: batchId,
    attemptsStarted: 1,
    systemRetryId,
    candidateId,
    dispatchGeneration: 2,
    jobEpoch: prepared.jobEpoch,
  }));
  assert.equal(
    await transaction(client, (tx) => lockContentDetailExecution(tx, takeover)),
    null,
    "advancing the Job epoch must fence the terminal incarnation",
  );
  assert.ok(
    await transaction(client, (tx) => claimContentDetailExecution(tx, recreatedRecovery)),
    "a terminal Job recreated at attemptsStarted=1 must claim after its orphan is cleared",
  );

  const persisted = (await client.query(
    `SELECT detail_active_job_id,detail_active_job_attempt,
            detail_job_epoch,detail_active_job_epoch
     FROM crawler.channel_runs WHERE run_id=$1`,
    [recoveryRunId],
  )).rows[0];
  assert.deepEqual(persisted, {
    detail_active_job_id: `content-detail__${recoveryRunId}`,
    detail_active_job_attempt: "1",
    detail_job_epoch: "1",
    detail_active_job_epoch: "1",
  });
});

test("inline Content Detail uses the parent Channel Candidate attemptsStarted Fence", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
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
  await client.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;

  const candidateId = 483;
  const channelId = "UC0NoarYHkSxek05QDqhtoYx";
  const batchId = "content-detail-inline-fence";
  const runId = "content-detail-inline-run";
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'running',1,1)`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
       snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$2,$2,$3,$4,'accepted',1,$5,1,'{}'::jsonb,'{}'::jsonb,now(),now())`,
    [
      candidateId,
      batchId,
      channelId,
      `https://www.youtube.com/channel/${channelId}`,
      "channel-snapshot-inline-job",
    ],
  );
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Inline Content Detail Fence',2000,'active',true,'pending',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId, candidateId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES ($1,$2,$3,'waiting_detail','full','queued',1,now(),$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify({ pipeline_cycle_id: batchId })],
    );
  });

  const parentJob = (attemptsStarted) => ({
    id: "channel-snapshot-inline-job",
    name: "channel-snapshot",
    queueName: "youtube-channel-crawl",
    attemptsStarted,
    data: {
      run_id: runId,
      channel_id: channelId,
      candidate_id: candidateId,
      dispatch_generation: 1,
      dispatch_batch_id: batchId,
      pipeline_cycle_id: batchId,
    },
  });
  const inlineFence = (attemptsStarted) => contentDetailExecutionFence(
    parentJob(attemptsStarted),
    {
      executionMode: "channel_inline",
      candidateAttemptFence: {
        candidateId,
        dispatchGeneration: 1,
        jobId: "channel-snapshot-inline-job",
        bullmqAttempt: attemptsStarted,
      },
    },
  );
  const first = inlineFence(1);
  assert.ok(await transaction(client, (tx) => claimContentDetailExecution(tx, first)));
  const migrationIntentId = Number((await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,channel_id,
       source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id,
       dispatch_attempts,last_dispatch_at
     ) VALUES (
       'content-detail-inline-retry',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$1,$2,
       '{}'::jsonb,repeat('d',64),$1,$3,1,now()
     ) RETURNING migration_intent_id`,
    [candidateId, channelId, batchId],
  )).rows[0].migration_intent_id);
  const systemRetryId = Number((await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES (
       $1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'retrying'
     ) RETURNING system_retry_id`,
    [migrationIntentId, candidateId, batchId, first.jobId],
  )).rows[0].system_retry_id);
  assert.equal(
    await transaction(client, (tx) => lockContentDetailExecution(tx, first)),
    null,
    "the failed attempt cannot commit after its retrying evidence is durable",
  );
  await client.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_active_job_attempt=2,updated_at=now()
     WHERE candidate_id=$1`,
    [candidateId],
  );
  const second = inlineFence(2);
  assert.ok(await transaction(client, (tx) => claimContentDetailExecution(tx, second)));
  assert.equal(await transaction(client, (tx) => lockContentDetailExecution(tx, first)), null);
  assert.ok(await transaction(client, (tx) => lockContentDetailExecution(tx, second)));

  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET failed_job_id='different-channel-job',updated_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId],
  );
  assert.equal(
    await transaction(client, (tx) => lockContentDetailExecution(tx, second)),
    null,
    "retrying evidence for another BullMQ Job cannot authorize this attempt",
  );
  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET failed_job_id=$2,failed_dispatch_generation=2,updated_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId, second.jobId],
  );
  assert.equal(
    await transaction(client, (tx) => lockContentDetailExecution(tx, second)),
    null,
    "retrying evidence for another dispatch generation cannot authorize this attempt",
  );
  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET failed_dispatch_generation=1,updated_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId],
  );
  assert.ok(await transaction(client, (tx) => lockContentDetailExecution(tx, second)));

  await client.query(`UPDATE crawler.migration_system_retry_items
    SET status='pending',failed_job_attempt=2 WHERE system_retry_id=$1`, [systemRetryId]);
  await client.query(`UPDATE crawler.channel_runs SET result_json=result_json ||
    jsonb_build_object('job_id',$2::text,'fetch_contract',$3::jsonb) WHERE run_id=$1`,
  [runId, second.jobId, JSON.stringify(YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT)]);
  // The controller pins the same run before retrying its original Snapshot Job.
  await client.query(`UPDATE crawler.migration_system_retry_items SET recovery_run_id=$2
    WHERE system_retry_id=$1`, [systemRetryId, runId]);
  assert.equal(await transaction(client, tx => lockContentDetailExecution(tx, second)), null,
    "a pending terminal failure blocks the old attempt from writing");
  await client.query(`UPDATE crawler.channel_candidates SET snapshot_active_job_id=NULL,
    snapshot_active_job_attempt=NULL WHERE candidate_id=$1`, [candidateId]);
  const original = { ...parentJob(2), attemptsMade: 3,
    data: { ...parentJob(2).data, fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT },
    getState: async () => "failed", retry: async (_state, options) => {
      assert.deepEqual(options, { resetAttemptsMade: true });
      original.attemptsMade = 0;
    } };
  await retryFullCrawlSnapshotJob({ getJob: async () => original }, {
    run: { run_id: runId, channel_id: channelId, candidate_id: candidateId,
      result_json: { job_id: original.id, pipeline_cycle_id: batchId,
        fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } },
    candidate: { status: "accepted", snapshot_dispatch_generation: 1 }, round: 3,
  });
  original.attemptsStarted++;
  assert.equal(await markChannelCandidateJobAttemptActive(client.query.bind(client), original), true);
  assert.equal((await client.query('SELECT status FROM crawler.migration_system_retry_items WHERE system_retry_id=$1',
    [systemRetryId])).rows[0].status, 'retrying',
  'claiming the resumed original Job must atomically hand off its pending recovery');
  const resumed = inlineFence(original.attemptsStarted);
  assert.ok(await transaction(client, tx => claimContentDetailExecution(tx, resumed)),
    "replaying the original Snapshot Job takes over the persisted detail checkpoint");
  assert.equal(await transaction(client, tx => lockContentDetailExecution(tx, second)), null,
    "the old Snapshot attempt still cannot write after recovery");
  assert.equal((await client.query('SELECT recovery_run_id FROM crawler.migration_system_retry_items WHERE system_retry_id=$1',
    [systemRetryId])).rows[0].recovery_run_id, null,
  'the original Job reclaims recovery ownership atomically');
  await client.query(`UPDATE crawler.migration_system_retry_items SET recovery_run_id=$2
    WHERE system_retry_id=$1`, [systemRetryId, runId]);
  assert.equal(await markChannelCandidateJobAttemptActive(client.query.bind(client), original), true);
  assert.ok(await transaction(client, tx => lockContentDetailExecution(tx, resumed)),
    'a retrying record pinned by the controller also hands back the same run');
  await client.query(`INSERT INTO crawler.channel_runs(run_id,channel_id,candidate_id,status,crawl_mode,detail_status)
    VALUES($1,$2,$3,'waiting_detail','full','queued')`, [runId + '-other', channelId, candidateId]);

  // Activating a Job must never approve unrelated pending recovery evidence.
  for (const patch of [
    { failed_job_id: 'another-job' },
    { failed_dispatch_generation: 2 },
    { failed_dispatch_batch_id: 'another-batch' },
    { failed_job_attempt: original.attemptsStarted },
    { retry_dispatch_generation: 2 },
    { recovery_run_id: runId + '-other' },
  ]) {
    const row = { failed_job_id: original.id, failed_dispatch_generation: 1,
      failed_dispatch_batch_id: batchId, failed_job_attempt: 2,
      retry_dispatch_generation: null, recovery_run_id: null, ...patch };
    await client.query(`UPDATE crawler.migration_system_retry_items SET status='pending',
      failed_job_id=$2,failed_dispatch_generation=$3,failed_dispatch_batch_id=$4,
      failed_job_attempt=$5,retry_dispatch_generation=$6,recovery_run_id=$7 WHERE system_retry_id=$1`,
    [systemRetryId,row.failed_job_id,row.failed_dispatch_generation,row.failed_dispatch_batch_id,
      row.failed_job_attempt,row.retry_dispatch_generation,row.recovery_run_id]);
    await markChannelCandidateJobAttemptActive(client.query.bind(client), original);
    assert.equal((await client.query('SELECT status FROM crawler.migration_system_retry_items WHERE system_retry_id=$1',
      [systemRetryId])).rows[0].status, 'pending', JSON.stringify(patch));
    assert.equal(await transaction(client, tx => lockContentDetailExecution(tx, resumed)), null);
  }
});
