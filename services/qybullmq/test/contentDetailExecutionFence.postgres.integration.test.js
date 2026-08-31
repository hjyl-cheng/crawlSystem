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
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";

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
}) {
  return {
    id: `content-detail__${runId}`,
    name: "content-detail-batch",
    queueName: "youtube-content-detail",
    attemptsStarted,
    data: {
      run_id: runId,
      channel_id: channelId,
      pipeline_cycle_id: pipelineCycleId,
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
    { ready: true, cleared: false, existingJob: null },
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
  });
  assert.ok(
    await transaction(client, (tx) => claimContentDetailExecution(tx, firstRecovery)),
    "a terminal Job recreated at attemptsStarted=1 must claim after its orphan is cleared",
  );

  const persisted = (await client.query(
    `SELECT detail_active_job_id,detail_active_job_attempt
     FROM crawler.channel_runs WHERE run_id=$1`,
    [recoveryRunId],
  )).rows[0];
  assert.deepEqual(persisted, {
    detail_active_job_id: `content-detail__${recoveryRunId}`,
    detail_active_job_attempt: "1",
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
});
