import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import {
  lockGenericFullAgentAgainstMigrationSystemRetry,
  lockGenericFullAgentBatchAgainstMigrationSystemRetry,
} from "../src/fullAgentMigrationRetryGuard.js";
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

test("PostgreSQL fences a generic full Agent against every active retry generation", {
  skip: databaseUrl
    ? false
    : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const contender = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `full-agent-guard:${suffix}`;
  const channelId = `UCfullagentguard${suffix}`;
  const channelUrl = `https://www.youtube.com/channel/${channelId}`;
  const runId = `run:full-agent-guard:${suffix}`;
  const replacementRunId = `run:full-agent-replacement:${suffix}`;
  const ordinaryChannelId = `UCordinaryguard${suffix}`;
  const ordinaryRunId = `run:ordinary-agent:${suffix}`;
  let initialized = false;

  t.after(async () => {
    if (initialized) {
      await client.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await client.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([client.end().catch(() => {}), contender.end().catch(() => {})]);
  });

  await Promise.all([client.connect(), contender.connect()]);
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  initialized = true;
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
       $3,'{}'::jsonb,repeat('b',64),$2,$4,1,now()
     ) RETURNING migration_intent_id`,
    [`full-agent-guard:${suffix}`, candidateId, channelId, batchId],
  )).rows[0].migration_intent_id);
  await client.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
       agent_status,latest_run_id
     ) VALUES
       ($1,$2,'Migration Agent guard',2000,'active',true,'queued',$3),
       ($4,$5,'Ordinary Agent guard',2000,'active',true,'queued',$6)`,
    [
      channelId,
      channelUrl,
      runId,
      ordinaryChannelId,
      `https://www.youtube.com/channel/${ordinaryChannelId}`,
      ordinaryRunId,
    ],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
       expected_content_count,started_at,result_json
     ) VALUES
       ($1,$2,$3,'waiting_agent','full','done',0,now(),$4::jsonb),
       ($5,$6,NULL,'waiting_agent','full','done',0,now(),'{}'::jsonb)`,
    [
      runId,
      channelId,
      candidateId,
      JSON.stringify({ dispatch_batch_id: batchId, pipeline_cycle_id: batchId }),
      ordinaryRunId,
      ordinaryChannelId,
    ],
  );
  const systemRetryId = Number((await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'retrying')
     RETURNING system_retry_id`,
    [migrationIntentId, candidateId, batchId, `failed_agent_guard_${suffix}`],
  )).rows[0].system_retry_id);
  const scopeG1 = {
    channelId,
    runId,
    candidateId,
    dispatchBatchId: batchId,
    dispatchGeneration: 1,
  };

  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG1)
  )), false, "the original failed generation remains owned while BullMQ is retrying");

  await client.query(
    `UPDATE crawler.channel_candidates SET snapshot_dispatch_generation=2 WHERE candidate_id=$1`,
    [candidateId],
  );
  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET status='dispatched',retry_dispatch_generation=2,recovery_run_id=$2,dispatched_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId, runId],
  );
  const scopeG2 = {
    ...scopeG1,
    dispatchGeneration: 2,
  };
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG1)
  )), false, "a Worker-start G1 scope is stale after Candidate advances to G2");
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
  )), false, "the allocated G+1 recovery generation blocks a generic Agent");

  await client.query(
    `UPDATE crawler.migration_system_retry_items SET status='pending' WHERE system_retry_id=$1`,
    [systemRetryId],
  );
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
  )), false, "an Outbox-requeued G+1 remains fenced by retry_dispatch_generation");

  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET status='resolved',resolution='recovery_finalized',resolved_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId],
  );
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
  )), true, "a terminal retry no longer owns Agent writes");
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG1)
  )), false, "a terminal Retry does not make the old Candidate generation current again");

  await client.query(
    "UPDATE crawler.channel_runs SET candidate_id=NULL WHERE run_id=$1",
    [runId],
  );
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
  )), false, "a Worker-start Run cannot write after its Candidate binding is cleared");
  await client.query(
    "UPDATE crawler.channel_runs SET candidate_id=$2 WHERE run_id=$1",
    [runId, candidateId],
  );

  await client.query(
    `UPDATE crawler.channel_runs
     SET result_json=result_json || '{"dispatch_batch_id":"replacement-batch"}'::jsonb
     WHERE run_id=$1`,
    [runId],
  );
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
  )), false, "a Worker-start Run cannot write after its dispatch Batch evidence changes");
  await client.query(
    `UPDATE crawler.channel_runs
     SET result_json=result_json || jsonb_build_object('dispatch_batch_id',$2::text)
     WHERE run_id=$1`,
    [runId, batchId],
  );

  await assert.rejects(
    client.query(
      "UPDATE crawler.channel_runs SET channel_id=$2 WHERE run_id=$1",
      [runId, ordinaryChannelId],
    ),
    (error) => error?.code === "23503"
      && error?.constraint === "channel_runs_candidate_channel_fk",
  );
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
  )), true, "an invalid Channel-only rebind cannot alter the current Run Fence");

  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
       expected_content_count,started_at,result_json
     ) VALUES ($1,$2,$3,'waiting_agent','full','done',0,now(),$4::jsonb)`,
    [replacementRunId, channelId, candidateId, JSON.stringify({
      dispatch_batch_id: batchId,
      pipeline_cycle_id: batchId,
    })],
  );
  await client.query(
    "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
    [channelId, replacementRunId],
  );
  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
  )), false, "a terminal Retry does not authorize a Worker-start Run after latest_run_id advances");
  await client.query(
    "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
    [channelId, runId],
  );

  let writerTransactionOpen = false;
  let racingGuard = null;
  try {
    await client.query("BEGIN");
    writerTransactionOpen = true;
    await client.query(
      "UPDATE crawler.channel_candidates SET updated_at=clock_timestamp() WHERE candidate_id=$1",
      [candidateId],
    );
    const racingRetryId = Number((await client.query(
      `INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,
         retry_dispatch_generation,recovery_run_id,dispatched_at
       ) VALUES (
         $1,$2,$3,1,$4,2,'LEASE_CONFLICT','lease','{}'::jsonb,
         'dispatched',2,$5,now()
       ) RETURNING system_retry_id`,
      [
        migrationIntentId,
        candidateId,
        batchId,
        `read_committed_race_${suffix}`,
        runId,
      ],
    )).rows[0].system_retry_id);
    const contenderPid = Number((await contender.query(
      "SELECT pg_backend_pid() AS pid",
    )).rows[0].pid);
    racingGuard = transaction(contender, (tx) => (
      lockGenericFullAgentAgainstMigrationSystemRetry(tx, scopeG2)
    ));
    const waitDeadline = Date.now() + 3000;
    let candidateLockWaitObserved = false;
    while (Date.now() < waitDeadline) {
      const waiting = (await client.query(
        `SELECT wait_event_type='Lock' AS waiting
         FROM pg_stat_activity WHERE pid=$1`,
        [contenderPid],
      )).rows[0]?.waiting === true;
      if (waiting) {
        candidateLockWaitObserved = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(
      candidateLockWaitObserved,
      true,
      "the guard must begin before the Candidate writer commits",
    );
    await client.query("COMMIT");
    writerTransactionOpen = false;
    assert.equal(
      await racingGuard,
      false,
      "the post-wait Retry query must use a fresh READ COMMITTED command snapshot",
    );
    await client.query(
      `UPDATE crawler.migration_system_retry_items
       SET status='resolved',resolution='recovery_fence_superseded',resolved_at=now()
       WHERE system_retry_id=$1`,
      [racingRetryId],
    );
  } catch (error) {
    if (writerTransactionOpen) await client.query("ROLLBACK").catch(() => {});
    await racingGuard?.catch(() => {});
    throw error;
  }

  let guardTransactionOpen = false;
  let reopened = null;
  try {
    await client.query("BEGIN");
    guardTransactionOpen = true;
    assert.equal(
      await lockGenericFullAgentAgainstMigrationSystemRetry(client, scopeG2),
      true,
    );
    const contenderPid = Number((await contender.query(
      "SELECT pg_backend_pid() AS pid",
    )).rows[0].pid);
    reopened = contender.query(
      `UPDATE crawler.migration_system_retry_items
       SET status='dispatched',resolution=NULL,resolved_at=NULL
       WHERE system_retry_id=$1`,
      [systemRetryId],
    );
    const waitDeadline = Date.now() + 3000;
    let retryLockWaitObserved = false;
    while (Date.now() < waitDeadline) {
      const waiting = (await client.query(
        `SELECT wait_event_type='Lock' AS waiting
         FROM pg_stat_activity WHERE pid=$1`,
        [contenderPid],
      )).rows[0]?.waiting === true;
      if (waiting) {
        retryLockWaitObserved = true;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(
      retryLockWaitObserved,
      true,
      "a legacy retry reopen must wait for the guarded Agent transaction",
    );
    await client.query("COMMIT");
    guardTransactionOpen = false;
    await reopened;
  } catch (error) {
    if (guardTransactionOpen) await client.query("ROLLBACK").catch(() => {});
    await reopened?.catch(() => {});
    throw error;
  }

  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentAgainstMigrationSystemRetry(tx, {
      channelId: ordinaryChannelId,
      runId: ordinaryRunId,
      candidateId: null,
      dispatchBatchId: null,
      dispatchGeneration: null,
    })
  )), true, "a Run without a Migration Candidate is unchanged");

  assert.equal(await transaction(client, (tx) => (
    lockGenericFullAgentBatchAgainstMigrationSystemRetry(tx, [
      { channelId: ordinaryChannelId, runId: ordinaryRunId },
      scopeG2,
    ])
  )), false, "one owned Migration Run rejects the generic batch preflight");
});
