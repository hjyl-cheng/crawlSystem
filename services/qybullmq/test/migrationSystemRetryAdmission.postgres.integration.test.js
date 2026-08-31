import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import { allocateChannelSnapshotDispatchOutbox } from "../src/channelSnapshotDispatch.js";
import { prepareManualMigrationBatch } from "../src/manualMigrationDispatch.js";
import { sourceSnapshotHash } from "../src/migrationSource.js";
import { retryMigrationSystemFailure } from "../src/migrationSystemRetry.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";

const { Client, Pool } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function sourceSnapshot({ suffix, sourceCandidateId, channelId }) {
  const raw = {
    source_id: `system-retry-admission:${suffix}`,
    source_database: "migration_source_test",
    source_database_oid: "16384",
    source_candidate_id: String(sourceCandidateId),
    source_candidate_status: "discovered",
    source_dispatch_batch_id: "legacy-source-batch",
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    handle: null,
    title: "Independent next batch Candidate",
    description: null,
    avatar_url: null,
    search_subscriber_count: "1200",
    search_subscriber_count_text: "1.2K",
    is_verified: false,
    priority: 100,
    snapshot_json: {},
    source_json: { source: "legacy_results_db" },
    source_created_at: "2026-08-01T00:00:00.000Z",
    source_updated_at: "2026-08-02T00:00:00.000Z",
  };
  return { ...raw, snapshot_sha256: sourceSnapshotHash(raw) };
}

async function initializeScenario(client, { suffix }) {
  const failedBatchId = `completed-system-failure:${suffix}`;
  const candidateId = 482;
  const channelId = `UCretryadmission${suffix}`;
  const failedJobId = `channel-snapshot__${failedBatchId}__${channelId}__g1`;
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,outcome,total_channel_count,
       discovered_candidate_count,accepted_channel_count,rejected_channel_count,
       failed_channel_count,result_json,finished_at
     ) VALUES ($1,$1,'completed','completed_with_system_failures',1,1,0,0,1,
               '{"outcome":"completed_with_system_failures"}'::jsonb,now())`,
    [failedBatchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
       snapshot_json,source_json,validation_finished_at
     ) VALUES ($1,$2,$2,$3,$4,'failed',1,$5,1,$6::jsonb,
               '{"source":"legacy_results_db"}'::jsonb,now())`,
    [
      candidateId,
      failedBatchId,
      channelId,
      `https://www.youtube.com/channel/${channelId}`,
      failedJobId,
      JSON.stringify({
        failure_type: "retryable_system_failure",
        failed_dispatch_batch_id: failedBatchId,
        system_failure: { code: "LEASE_CONFLICT", category: "lease" },
      }),
    ],
  );
  const intent = await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,channel_id,
       source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id,
       dispatch_attempts,last_dispatch_at,last_error
     ) VALUES ($1,current_database(),
               (SELECT oid FROM pg_database WHERE datname=current_database()),$2,$3,
               '{}'::jsonb,repeat('a',64),$2,$4,1,now(),'lease conflict')
     RETURNING migration_intent_id`,
    [`system-retry-admission:${suffix}`, candidateId, channelId, failedBatchId],
  );
  const retry = await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status
     ) VALUES ($1,$2,$3,1,$4,1,'LEASE_CONFLICT','lease','{}'::jsonb,'pending')
     RETURNING system_retry_id`,
    [Number(intent.rows[0].migration_intent_id), candidateId, failedBatchId, failedJobId],
  );
  await client.query(
    `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
     VALUES ('query_scheduler',$1::jsonb,now())
     ON CONFLICT (setting_key) DO UPDATE
     SET value_json=EXCLUDED.value_json,updated_at=now()`,
    [JSON.stringify({
      status: "stopped",
      stop_reason: "user_requested",
      pipeline_cycle_id: failedBatchId,
    })],
  );
  return {
    failedBatchId,
    candidateId,
    systemRetryId: Number(retry.rows[0].system_retry_id),
  };
}

test("Scheduler admission serializes a controlled G+1 retry against the next Migration Batch", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const setup = new Client({ connectionString: databaseUrl });
  const pool = new Pool({ connectionString: databaseUrl, max: 4 });
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
    await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await setup.end().catch(() => {});
    await pool.end().catch(() => {});
  });

  const scenario = await initializeScenario(setup, { suffix });
  const nextBatchId = `next-migration-batch:${suffix}`;
  const nextSnapshot = sourceSnapshot({
    suffix,
    sourceCandidateId: 9001,
    channelId: `UCnextbatch${suffix}`,
  });

  await assert.rejects(
    retryMigrationSystemFailure({
      systemRetryId: scenario.systemRetryId,
      withTransaction,
    }),
    (error) => error?.code === "migration_system_retry_scheduler_blocked",
  );
  assert.deepEqual((await setup.query(
    `SELECT retry.status,candidate.snapshot_dispatch_generation,
            count(outbox.dispatch_id)::int AS outbox_count
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_candidates candidate ON candidate.candidate_id=retry.candidate_id
     LEFT JOIN crawler.proxy_job_dispatch_outbox outbox
       ON outbox.aggregate_kind='channel_snapshot'
      AND outbox.aggregate_id=candidate.candidate_id::text
      AND (outbox.payload_json->>'dispatch_generation')::int=2
     WHERE retry.system_retry_id=$1
     GROUP BY retry.status,candidate.snapshot_dispatch_generation`,
    [scenario.systemRetryId],
  )).rows[0], {
    status: "pending",
    snapshot_dispatch_generation: "1",
    outbox_count: 0,
  });

  const pendingDoesNotBlock = await withTransaction((client) => prepareManualMigrationBatch(client, {
    sourceSnapshots: [nextSnapshot],
    selection: "100",
    batchId: nextBatchId,
  }));
  assert.equal(pendingDoesNotBlock.targetCount, 1);
  await setup.query("DELETE FROM crawler.channel_candidate_sources WHERE candidate_id<>$1", [scenario.candidateId]);
  await setup.query(
    "DELETE FROM crawler.migration_channel_intents WHERE target_candidate_id<>$1",
    [scenario.candidateId],
  );
  await setup.query("DELETE FROM crawler.channel_candidates WHERE candidate_id<>$1", [scenario.candidateId]);
  await setup.query("DELETE FROM crawler.query_pages WHERE dispatch_batch_id=$1", [nextBatchId]);
  await setup.query("DELETE FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=$1", [nextBatchId]);
  await setup.query(
    `UPDATE crawler.settings
     SET value_json=jsonb_build_object(
       'status','stopped','stop_reason','pipeline_complete','pipeline_cycle_id',$1::text
     ),updated_at=now()
     WHERE setting_key='query_scheduler'`,
    [scenario.failedBatchId],
  );

  const allocationEntered = deferred();
  const releaseAllocation = deferred();
  const retryPromise = retryMigrationSystemFailure({
    systemRetryId: scenario.systemRetryId,
    withTransaction,
    allocateOutbox: async (client, options) => {
      const allocation = await allocateChannelSnapshotDispatchOutbox(client, options);
      allocationEntered.resolve();
      await releaseAllocation.promise;
      return allocation;
    },
  });
  await allocationEntered.promise;
  const nextBatchPromise = withTransaction((client) => prepareManualMigrationBatch(client, {
    sourceSnapshots: [nextSnapshot],
    selection: "100",
    batchId: nextBatchId,
  }));
  releaseAllocation.resolve();

  const retry = await retryPromise;
  assert.equal(retry.status, "dispatched");
  await assert.rejects(
    nextBatchPromise,
    (error) => error?.code === "migration_system_retry_recovery_active",
  );
  assert.deepEqual((await setup.query(
    `SELECT value_json->>'status' AS status,
            value_json->>'stop_reason' AS stop_reason,
            value_json->>'pipeline_cycle_id' AS pipeline_cycle_id
     FROM crawler.settings WHERE setting_key='query_scheduler'`,
  )).rows[0], {
    status: "stopped",
    stop_reason: "pipeline_complete",
    pipeline_cycle_id: scenario.failedBatchId,
  });

  await setup.query(
    `UPDATE crawler.migration_system_retry_items
     SET status='resolved',resolution='recovery_finalized',resolved_at=now(),updated_at=now()
     WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  );
  await setup.query(
    `UPDATE crawler.channel_candidates
     SET status='accepted',snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,
         accepted_at=COALESCE(accepted_at,now()),updated_at=now()
     WHERE candidate_id=$1`,
    [scenario.candidateId],
  );
  await setup.query(
    `UPDATE crawler.proxy_job_dispatch_outbox
     SET status='sent',sent_at=COALESCE(sent_at,now()),updated_at=now()
     WHERE dispatch_id=$1`,
    [retry.outbox.dispatch_id],
  );
  const afterRecovery = await withTransaction((client) => prepareManualMigrationBatch(client, {
    sourceSnapshots: [nextSnapshot],
    selection: "100",
    batchId: nextBatchId,
  }));
  assert.equal(afterRecovery.targetCount, 1);
  assert.deepEqual((await setup.query(
    `SELECT value_json->>'status' AS status,
            value_json->>'pipeline_cycle_id' AS pipeline_cycle_id
     FROM crawler.settings WHERE setting_key='query_scheduler'`,
  )).rows[0], {
    status: "finishing",
    pipeline_cycle_id: nextBatchId,
  });
});

test("controlled retry backfills a historical unknown Batch before G+1 dispatch", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const suffix = randomUUID().replaceAll("-", "");
  const setup = new Client({ connectionString: databaseUrl });
  const pool = new Pool({ connectionString: databaseUrl, max: 2 });
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
    await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await setup.end().catch(() => {});
    await pool.end().catch(() => {});
  });

  const scenario = await initializeScenario(setup, { suffix });
  await setup.query(
    "ALTER TABLE crawler.migration_system_retry_items ALTER COLUMN failed_dispatch_batch_id DROP NOT NULL",
  );
  await setup.query(
    "UPDATE crawler.migration_system_retry_items SET failed_dispatch_batch_id=NULL WHERE system_retry_id=$1",
    [scenario.systemRetryId],
  );
  await setup.query(
    `UPDATE crawler.settings
     SET value_json=jsonb_build_object(
       'status','stopped','stop_reason','pipeline_complete','pipeline_cycle_id',$1::text
     ),updated_at=now()
     WHERE setting_key='query_scheduler'`,
    [scenario.failedBatchId],
  );

  const result = await retryMigrationSystemFailure({
    systemRetryId: scenario.systemRetryId,
    withTransaction,
  });
  const persisted = (await setup.query(
    `SELECT retry.failed_dispatch_batch_id,retry.status,
            outbox.payload_json->>'dispatch_batch_id' AS outbox_dispatch_batch_id
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.proxy_job_dispatch_outbox outbox
       ON outbox.aggregate_kind='channel_snapshot'
      AND outbox.aggregate_id=retry.candidate_id::text
      AND (outbox.payload_json->>'dispatch_generation')::int=2
     WHERE retry.system_retry_id=$1`,
    [scenario.systemRetryId],
  )).rows[0];

  assert.equal(result.dispatch_generation, 2);
  assert.deepEqual(persisted, {
    failed_dispatch_batch_id: scenario.failedBatchId,
    status: "dispatched",
    outbox_dispatch_batch_id: scenario.failedBatchId,
  });
});
