import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import {
  claimMigrationSystemRetryAgentJobFence,
  lockMigrationSystemRetryAgentJobFence,
  migrationSystemRetryAgentJobFence,
  prepareMigrationSystemRetryAgentJobRequeue,
} from "../src/migrationSystemRetryRecovery.js";
import { lockGenericFullAgentAgainstMigrationSystemRetry } from "../src/fullAgentMigrationRetryGuard.js";
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

async function waitFor(check, label, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`timed out waiting for ${label}`);
}

function recoveryAgentJob({
  jobId,
  jobAttempt,
  systemRetryId,
  candidateId,
  batchId,
  runId,
  channelId,
  jobEpoch = 0,
}) {
  return {
    id: jobId,
    name: "agent-profile-batch",
    attemptsStarted: jobAttempt,
    data: {
      migration_system_retry_id: systemRetryId,
      recovery_agent_job_epoch: jobEpoch,
      candidate_id: candidateId,
      dispatch_generation: 2,
      dispatch_batch_id: batchId,
      run_id: runId,
      channel_ids: [channelId],
    },
  };
}

test("PostgreSQL gives one physical Recovery Agent attempt exclusive write ownership", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const contender = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
    if (schemaInitialized) {
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
  schemaInitialized = true;

  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `recovery-agent-fence:${suffix}`;
  const channelId = `UCrecoveryagent${suffix}`;
  const runId = `run:recovery-agent:${suffix}`;
  const jobId = `migration-system-retry-agent:${suffix}`;
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,total_channel_count,discovered_candidate_count
     ) VALUES ($1,$1,'completed',1,1)`,
    [batchId],
  );
  const candidateId = Number((await client.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$1,$2,$3,'accepted',2,'{}'::jsonb,
               '{"source":"legacy_results_db"}'::jsonb,now(),now())
     RETURNING candidate_id`,
    [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
  )).rows[0].candidate_id);
  const migrationIntentId = Number((await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
     ) VALUES (
       $1,current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
       $3,'{}'::jsonb,repeat('a',64),$2,$4,2,now()
     ) RETURNING migration_intent_id`,
    [`recovery-agent-fence:${suffix}`, candidateId, channelId, batchId],
  )).rows[0].migration_intent_id);
  await client.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
       agent_status,latest_run_id
     ) VALUES ($1,$2,'Recovery Agent Fence',2000,'active',true,'pending',$3)`,
    [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
       expected_content_count,started_at,result_json
     ) VALUES ($1,$2,$3,'waiting_agent','full','done',0,now(),$4::jsonb)`,
    [runId, channelId, candidateId, JSON.stringify({
      dispatch_batch_id: batchId,
      pipeline_cycle_id: batchId,
    })],
  );
  const systemRetryId = Number((await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status,
       retry_dispatch_generation,recovery_run_id,dispatched_at
     ) VALUES (
       $1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,
       'dispatched',2,$5,now()
     ) RETURNING system_retry_id`,
    [migrationIntentId, candidateId, batchId, `failed:${suffix}`, runId],
  )).rows[0].system_retry_id);
  const fence = (jobAttempt, physicalJobId = jobId, jobEpoch = 0) => (
    migrationSystemRetryAgentJobFence(
    recoveryAgentJob({
      jobId: physicalJobId,
      jobAttempt,
      systemRetryId,
      candidateId,
      batchId,
      runId,
      channelId,
      jobEpoch,
    }),
    )
  );
  const first = fence(1);
  const takeover = fence(2);

  const initialAllocation = await transaction(client, (tx) => (
    prepareMigrationSystemRetryAgentJobRequeue(tx, first, {
      findExistingJob: async () => null,
    })
  ));
  assert.deepEqual(initialAllocation, {
    ready: true,
    cleared: true,
    existingJob: null,
    jobEpoch: 1,
  }, "a first physical queue.add must allocate a persisted incarnation");
  assert.equal(
    await transaction(client, (tx) => claimMigrationSystemRetryAgentJobFence(tx, first)),
    false,
    "epoch zero is stale before the first physical Job is added",
  );
  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET recovery_agent_job_epoch=0
     WHERE system_retry_id=$1`,
    [systemRetryId],
  );

  assert.equal(await transaction(client, (tx) => (
    claimMigrationSystemRetryAgentJobFence(tx, first)
  )), true);
  assert.equal(await transaction(client, (tx) => (
    claimMigrationSystemRetryAgentJobFence(tx, takeover)
  )), true);
  assert.equal(
    await transaction(client, (tx) => lockMigrationSystemRetryAgentJobFence(tx, first)),
    false,
    "attemptsStarted=1 cannot write after attemptsStarted=2 takes ownership",
  );
  assert.equal(await transaction(client, (tx) => (
    lockMigrationSystemRetryAgentJobFence(tx, takeover)
  )), true);
  assert.equal(
    await transaction(client, (tx) => (
      claimMigrationSystemRetryAgentJobFence(tx, fence(3, `${jobId}:different`))
    )),
    false,
    "a different physical Job cannot steal an active Recovery Agent execution",
  );

  const representedJob = { id: jobId };
  assert.deepEqual(await transaction(client, (tx) => (
    prepareMigrationSystemRetryAgentJobRequeue(tx, first, {
      findExistingJob: async () => representedJob,
    })
  )), { ready: false, cleared: false, existingJob: representedJob, jobEpoch: 0 });
  assert.equal(await transaction(client, (tx) => (
    lockMigrationSystemRetryAgentJobFence(tx, takeover)
  )), true, "a represented Redis Job retains its PostgreSQL ownership");

  const preparedRequeue = await transaction(client, (tx) => (
    prepareMigrationSystemRetryAgentJobRequeue(tx, first, {
      findExistingJob: async () => null,
    })
  ));
  assert.deepEqual(preparedRequeue, {
    ready: true,
    cleared: true,
    existingJob: null,
    jobEpoch: 1,
  });
  assert.equal(
    await transaction(client, (tx) => claimMigrationSystemRetryAgentJobFence(tx, first)),
    false,
    "a removed Job incarnation can never reclaim ownership",
  );
  const recreatedJobId = `${jobId}:epoch-1`;
  const recreated = fence(1, recreatedJobId, preparedRequeue.jobEpoch);
  assert.equal(
    await transaction(client, (tx) => claimMigrationSystemRetryAgentJobFence(tx, recreated)),
    true,
    "a new Job incarnation can claim from attemptsStarted=1",
  );
  assert.equal(
    await transaction(client, (tx) => lockMigrationSystemRetryAgentJobFence(tx, first)),
    false,
    "the old stalled callback stays stale after the new incarnation claims",
  );
  assert.deepEqual((await client.query(
    `SELECT recovery_agent_job_epoch,recovery_agent_active_job_id,
            recovery_agent_active_job_attempt
     FROM crawler.migration_system_retry_items WHERE system_retry_id=$1`,
    [systemRetryId],
  )).rows[0], {
    recovery_agent_job_epoch: "1",
    recovery_agent_active_job_id: recreatedJobId,
    recovery_agent_active_job_attempt: "1",
  });

  await client.query("BEGIN");
  let recoveryTransactionOpen = true;
  let genericGuard;
  try {
    assert.equal(await lockMigrationSystemRetryAgentJobFence(client, recreated), true);
    const contenderPid = Number((await contender.query(
      "SELECT pg_backend_pid() AS pid",
    )).rows[0].pid);
    genericGuard = transaction(contender, (tx) => (
      lockGenericFullAgentAgainstMigrationSystemRetry(tx, {
        channelId,
        runId,
        candidateId,
        dispatchBatchId: batchId,
        dispatchGeneration: 2,
      })
    ));
    await waitFor(async () => (
      (await client.query(
        `SELECT wait_event_type='Lock' AS waiting
         FROM pg_stat_activity WHERE pid=$1`,
        [contenderPid],
      )).rows[0]?.waiting === true
    ), "Generic Agent Candidate lock wait");
    await client.query("COMMIT");
    recoveryTransactionOpen = false;
    assert.equal(await genericGuard, false);
  } catch (error) {
    if (recoveryTransactionOpen) await client.query("ROLLBACK").catch(() => {});
    await genericGuard?.catch(() => {});
    throw error;
  }

  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET status='retrying',failed_dispatch_generation=2,
         retry_dispatch_generation=NULL,
         recovery_agent_active_job_id=NULL,
         recovery_agent_active_job_attempt=NULL,
         recovery_agent_job_epoch=0,updated_at=now()
     WHERE system_retry_id=$1`,
    [systemRetryId],
  );
  const orphanedOriginalAttempt = fence(1);
  assert.equal(
    await transaction(client, (tx) => (
      claimMigrationSystemRetryAgentJobFence(tx, orphanedOriginalAttempt)
    )),
    true,
    "an accepted original generation can resume downstream work from an orphaned retrying marker",
  );
  assert.equal(
    await transaction(client, (tx) => (
      lockMigrationSystemRetryAgentJobFence(tx, orphanedOriginalAttempt)
    )),
    true,
    "the orphaned original generation retains its Recovery Agent write Fence",
  );
});
