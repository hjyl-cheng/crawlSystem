import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import {
  rotaWorkerV2SchemaBlock,
  verifyRotaWorkerV2Schema,
} from "../scripts/applyRotaWorkerV2Schema.mjs";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

test("Rota Worker V2 schema block applies transactionally and is idempotent", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  t.after(async () => {
    await client.query("ROLLBACK").catch(() => {});
    await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await client.end();
  });

  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,
       accepted_channel_count,rejected_channel_count,total_channel_count
     ) VALUES (
       'schema-backfill-batch','schema-backfill-cycle','discovery_closed',3,2,1,3
     )`,
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status
     ) VALUES
       ('schema-backfill-batch','schema-backfill-cycle','UCschemaqueued',
        'https://www.youtube.com/channel/UCschemaqueued','queued'),
       ('schema-backfill-batch','schema-backfill-cycle','UCschemadiscovered',
        'https://www.youtube.com/channel/UCschemadiscovered','discovered'),
       ('schema-backfill-batch','schema-backfill-cycle','UCschemaintent',
        'https://www.youtube.com/channel/UCschemaintent','discovered')`,
  );
  await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts
     ) SELECT
       'schema-backfill-source',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),42,
       candidate.channel_id,'{}'::jsonb,repeat('a',64),candidate.candidate_id,
       candidate.dispatch_batch_id,4
     FROM crawler.channel_candidates AS candidate
     WHERE candidate.channel_id='UCschemaintent'`,
  );
  // Reproduce a production database from before generation-fenced recovery shipped.
  await client.query("DROP TABLE crawler.migration_retry_intents");
  await client.query("DROP TABLE crawler.migration_system_retry_items");
  await client.query(
    `ALTER TABLE crawler.query_dispatch_batches
     DROP COLUMN IF EXISTS failed_channel_count,
     DROP COLUMN IF EXISTS total_channel_count,
     DROP COLUMN IF EXISTS outcome`,
  );
  await client.query(
    `ALTER TABLE crawler.channel_candidates
     DROP COLUMN IF EXISTS snapshot_dispatch_generation,
     DROP COLUMN IF EXISTS snapshot_active_job_id,
     DROP COLUMN IF EXISTS snapshot_active_job_attempt`,
  );
  await client.query(
    `ALTER TABLE crawler.youtube_api_batches
     DROP COLUMN IF EXISTS active_job_id,
     DROP COLUMN IF EXISTS active_job_attempt`,
  );
  await client.query(
    `ALTER TABLE crawler.channel_runs
     DROP COLUMN IF EXISTS detail_active_job_id,
     DROP COLUMN IF EXISTS detail_active_job_attempt,
     DROP COLUMN IF EXISTS detail_active_scope_key,
     DROP COLUMN IF EXISTS detail_job_epoch,
     DROP COLUMN IF EXISTS detail_active_job_epoch`,
  );
  await client.query(
    "ALTER TABLE crawler.channel_execution_attempts DROP COLUMN IF EXISTS dispatch_generation",
  );
  // Production databases created before the managed-job schema do not have this index.
  await client.query("DROP TABLE crawler.query_quality_chunk_members");
  await client.query("DROP INDEX crawler.ux_crawler_query_quality_tasks_batch_task");
  const block = rotaWorkerV2SchemaBlock(schema);
  await client.query("BEGIN");
  try {
    await client.query(block);
    await client.query(block);
    await verifyRotaWorkerV2Schema(client);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }

  const verified = await client.query(
    `SELECT
       to_regclass('crawler.business_run_bindings') IS NOT NULL AS binding,
       to_regclass('crawler.proxy_job_dispatch_outbox') IS NOT NULL AS outbox,
       to_regclass('crawler.migration_retry_intents') IS NOT NULL AS retry_intents,
       to_regclass('crawler.migration_system_retry_items') IS NOT NULL AS system_retry_items,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='failed_dispatch_batch_id'
       ) AS system_retry_batch_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_run_id' AND data_type='text'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS system_retry_recovery_run_id,
       EXISTS (
         SELECT 1
         FROM pg_constraint fk
         JOIN pg_attribute source_column
           ON source_column.attrelid=fk.conrelid
          AND source_column.attname='recovery_run_id'
          AND NOT source_column.attisdropped
         JOIN pg_attribute target_column
           ON target_column.attrelid=fk.confrelid
          AND target_column.attname='run_id'
          AND NOT target_column.attisdropped
         WHERE fk.conrelid=to_regclass('crawler.migration_system_retry_items')
           AND fk.conname='migration_system_retry_items_recovery_run_id_fkey'
           AND fk.contype='f' AND fk.convalidated AND fk.confdeltype='r'
           AND fk.confrelid=to_regclass('crawler.channel_runs')
           AND fk.conkey=ARRAY[source_column.attnum]
           AND fk.confkey=ARRAY[target_column.attnum]
       ) AS system_retry_recovery_run_reference,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_agent_active_job_id' AND data_type='text'
       ) AS recovery_agent_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_agent_active_job_attempt' AND data_type='bigint'
       ) AS recovery_agent_active_job_attempt,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_agent_job_epoch' AND data_type='bigint'
             AND column_default='0' AND is_nullable='NO'
       ) AS recovery_agent_job_epoch,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.migration_system_retry_items')
           AND conname='migration_system_retry_items_recovery_agent_active_job_check'
           AND contype='c' AND convalidated
       ) AS recovery_agent_active_job_check,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_candidates'
           AND column_name='snapshot_dispatch_generation'
       ) AS candidate_generation,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conname='channel_candidates_snapshot_dispatch_generation_check'
           AND convalidated
       ) AS candidate_generation_check,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_candidates'
           AND column_name='snapshot_active_job_id'
       ) AS candidate_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='youtube_api_batches'
           AND column_name='active_job_id' AND data_type='text'
       ) AS data_api_batch_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='youtube_api_batches'
           AND column_name='active_job_attempt' AND data_type='bigint'
       ) AS data_api_batch_active_job_attempt,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.youtube_api_batches')
           AND conname='youtube_api_batches_active_job_check'
           AND contype='c' AND convalidated
       ) AS data_api_batch_active_job_check,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='detail_active_job_id' AND data_type='text'
       ) AS content_detail_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='detail_active_job_attempt' AND data_type='bigint'
       ) AS content_detail_active_job_attempt,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='detail_active_scope_key' AND data_type='text'
       ) AS content_detail_active_scope_key,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='detail_job_epoch' AND data_type='bigint'
           AND is_nullable='NO' AND column_default='0'
       ) AS content_detail_job_epoch,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='detail_active_job_epoch' AND data_type='bigint'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS content_detail_active_job_epoch,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.channel_runs')
           AND conname='channel_runs_detail_active_job_check'
           AND contype='c' AND convalidated
       ) AS content_detail_active_job_check,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_candidates'
           AND column_name='snapshot_active_job_attempt'
       ) AS candidate_active_job_attempt,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.channel_candidates')
           AND conname='channel_candidates_snapshot_active_job_check'
           AND contype='c' AND convalidated
       ) AS candidate_active_job_check,
       to_regclass(
         'crawler.ux_crawler_proxy_job_dispatch_outbox_channel_snapshot_generation'
       ) IS NOT NULL AS channel_snapshot_outbox_key,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_execution_attempts'
           AND column_name='dispatch_generation'
       ) AS execution_dispatch_generation,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.channel_execution_attempts')
           AND conname='channel_execution_attempts_dispatch_generation_check'
           AND contype='c' AND convalidated
       ) AS execution_dispatch_generation_check,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_retry_intents'
           AND column_name='terminal_job_attempt' AND data_type='bigint'
       ) AS retry_intent_terminal_job_attempt,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.migration_retry_intents')
           AND conname='migration_retry_intents_terminal_job_attempt_check'
           AND contype='c' AND convalidated
       ) AS retry_intent_terminal_job_attempt_check,
       to_regclass('crawler.ux_crawler_migration_retry_intents_active_candidate') IS NOT NULL
         AS active_retry_intent_key,
       to_regclass('crawler.idx_crawler_migration_retry_intents_status') IS NOT NULL
         AS retry_intent_status_index,
       to_regclass('crawler.ux_crawler_migration_system_retry_active_candidate') IS NOT NULL
         AS active_system_retry_key,
       to_regclass('crawler.idx_crawler_migration_system_retry_status') IS NOT NULL
         AS system_retry_status_index,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.query_dispatch_batches')
           AND conname='query_dispatch_batches_completion_count_check'
           AND contype='c' AND convalidated
       ) AS batch_completion_count_check,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.query_dispatch_batches')
           AND conname='query_dispatch_batches_outcome_check'
           AND contype='c' AND convalidated
       ) AS batch_outcome_check,
       EXISTS (
         SELECT 1 FROM pg_trigger
         WHERE tgname='trg_guard_query_quality_chunk_members' AND NOT tgisinternal
       ) AS member_guard,
       to_regclass('crawler.ux_crawler_query_quality_tasks_batch_task') IS NOT NULL
         AS legacy_parent_key_repaired`,
  );
  assert.deepEqual(verified.rows[0], {
    binding: true,
    outbox: true,
    retry_intents: true,
    system_retry_items: true,
    system_retry_batch_id: true,
    system_retry_recovery_run_id: true,
    system_retry_recovery_run_reference: true,
    recovery_agent_active_job_id: true,
    recovery_agent_active_job_attempt: true,
    recovery_agent_job_epoch: true,
    recovery_agent_active_job_check: true,
    candidate_generation: true,
    candidate_generation_check: true,
    candidate_active_job_id: true,
    data_api_batch_active_job_id: true,
    data_api_batch_active_job_attempt: true,
    data_api_batch_active_job_check: true,
    content_detail_active_job_id: true,
    content_detail_active_job_attempt: true,
    content_detail_active_scope_key: true,
    content_detail_job_epoch: true,
    content_detail_active_job_epoch: true,
    content_detail_active_job_check: true,
    candidate_active_job_attempt: true,
    candidate_active_job_check: true,
    channel_snapshot_outbox_key: true,
    execution_dispatch_generation: true,
    execution_dispatch_generation_check: true,
    retry_intent_terminal_job_attempt: true,
    retry_intent_terminal_job_attempt_check: true,
    active_retry_intent_key: true,
    retry_intent_status_index: true,
    active_system_retry_key: true,
    system_retry_status_index: true,
    batch_completion_count_check: true,
    batch_outcome_check: true,
    member_guard: true,
    legacy_parent_key_repaired: true,
  });

  assert.deepEqual((await client.query(
    `SELECT discovered_candidate_count,accepted_channel_count,rejected_channel_count,
            failed_channel_count,total_channel_count,outcome
     FROM crawler.query_dispatch_batches
     WHERE dispatch_batch_id='schema-backfill-batch'`,
  )).rows[0], {
    discovered_candidate_count: 3,
    accepted_channel_count: 2,
    rejected_channel_count: 1,
    failed_channel_count: 0,
    total_channel_count: 3,
    outcome: null,
  });

  const generations = await client.query(
    `SELECT channel_id,snapshot_dispatch_generation::text AS generation
     FROM crawler.channel_candidates
     WHERE channel_id IN ('UCschemaqueued','UCschemadiscovered','UCschemaintent')
     ORDER BY channel_id`,
  );
  assert.deepEqual(generations.rows, [
    { channel_id: "UCschemadiscovered", generation: "0" },
    { channel_id: "UCschemaintent", generation: "4" },
    { channel_id: "UCschemaqueued", generation: "1" },
  ]);

  // Upgrade databases where the recovery Run pin has not been deployed at all.
  await client.query(
    "ALTER TABLE crawler.migration_system_retry_items DROP COLUMN recovery_run_id",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_recovery_run_id/,
  );
  await client.query(block);
  await verifyRotaWorkerV2Schema(client);

  // Repair a partially deployed FK that can erase an already pinned recovery Run.
  await client.query(
    `ALTER TABLE crawler.migration_system_retry_items
     DROP CONSTRAINT migration_system_retry_items_recovery_run_id_fkey`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_recovery_run_reference/,
  );
  await client.query(
    `ALTER TABLE crawler.migration_system_retry_items
     ADD CONSTRAINT migration_system_retry_items_recovery_run_id_fkey
     FOREIGN KEY (recovery_run_id) REFERENCES crawler.channel_runs(run_id)
     ON DELETE SET NULL`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_recovery_run_reference/,
  );
  await client.query(block);
  await verifyRotaWorkerV2Schema(client);
  assert.deepEqual((await client.query(
    `SELECT confdeltype,convalidated
     FROM pg_constraint
     WHERE conrelid='crawler.migration_system_retry_items'::regclass
       AND conname='migration_system_retry_items_recovery_run_id_fkey'`,
  )).rows[0], { confdeltype: "r", convalidated: true });

  await client.query(
    "ALTER TABLE crawler.migration_system_retry_items DROP COLUMN failed_dispatch_batch_id",
  );
  await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_generation,
       failed_job_id,failed_job_attempt,failure_code,failure_category,
       failure_evidence,status
     )
     SELECT intent.migration_intent_id,intent.target_candidate_id,4,
            'historical-system-failure',1,'LEASE_CONFLICT','lease','{}'::jsonb,'resolved'
     FROM crawler.migration_channel_intents intent
     WHERE intent.channel_id='UCschemaintent'`,
  );
  await client.query(block);
  assert.deepEqual((await client.query(
    `SELECT failed_dispatch_batch_id
     FROM crawler.migration_system_retry_items
     WHERE failed_job_id='historical-system-failure'`,
  )).rows[0], { failed_dispatch_batch_id: null });
  await verifyRotaWorkerV2Schema(client);

  await client.query(
    `UPDATE crawler.migration_channel_intents
     SET dispatch_attempts=5
     WHERE channel_id='UCschemaintent'`,
  );
  await client.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_active_job_id='legacy-job',snapshot_active_job_attempt=4
     WHERE channel_id='UCschemaintent'`,
  );
  await client.query(block);
  assert.deepEqual((await client.query(
    `SELECT snapshot_dispatch_generation::text AS generation,
            snapshot_active_job_id,snapshot_active_job_attempt
     FROM crawler.channel_candidates
     WHERE channel_id='UCschemaintent'`,
  )).rows[0], {
    generation: "5",
    snapshot_active_job_id: null,
    snapshot_active_job_attempt: null,
  });

  await client.query("BEGIN");
  await assert.rejects(
    client.query(
      `UPDATE crawler.channel_candidates
       SET snapshot_active_job_id='job:incomplete'
       WHERE channel_id='UCschemaqueued'`,
    ),
    /channel_candidates_snapshot_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.migration_system_retry_items
     DROP CONSTRAINT migration_system_retry_items_recovery_agent_active_job_check`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_recovery_agent_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.migration_system_retry_items
     DROP CONSTRAINT migration_system_retry_items_recovery_agent_active_job_check,
     ADD CONSTRAINT migration_system_retry_items_recovery_agent_active_job_check
       CHECK (recovery_agent_job_epoch >= 0 OR recovery_agent_active_job_attempt > 0 OR TRUE)`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_recovery_agent_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await assert.rejects(
    client.query(
      `UPDATE crawler.migration_system_retry_items
       SET recovery_agent_active_job_id='incomplete-agent-job'
       WHERE failed_job_id='historical-system-failure'`,
    ),
    /migration_system_retry_items_recovery_agent_active_job_check/,
  );
  await client.query("ROLLBACK");
  await client.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_active_job_id='job:allocated',snapshot_active_job_attempt=0
     WHERE channel_id='UCschemaqueued'`,
  );

  await client.query("BEGIN");
  await assert.rejects(
    client.query(
      `UPDATE crawler.channel_candidates
       SET snapshot_active_job_attempt=-1
       WHERE channel_id='UCschemaqueued'`,
    ),
    /channel_candidates_snapshot_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query("DROP INDEX crawler.idx_crawler_migration_retry_intents_status");
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_retry_intents_status_index/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    "ALTER TABLE crawler.migration_system_retry_items DROP COLUMN failed_dispatch_batch_id",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_failed_dispatch_batch_id/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query("DROP TABLE crawler.migration_system_retry_items");
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_items/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    "DROP INDEX crawler.ux_crawler_migration_system_retry_active_candidate",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_active_candidate_index/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `DROP INDEX crawler.ux_crawler_migration_system_retry_active_candidate;
     CREATE INDEX ux_crawler_migration_system_retry_active_candidate
       ON crawler.migration_system_retry_items (system_retry_id)
       WHERE status IN ('pending','dispatched')`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_active_candidate_index/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query("DROP INDEX crawler.idx_crawler_migration_system_retry_status");
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: migration_system_retry_status_index/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.query_dispatch_batches
     DROP CONSTRAINT query_dispatch_batches_completion_count_check,
     ADD CONSTRAINT query_dispatch_batches_completion_count_check CHECK (
       failed_channel_count >= 0
       OR total_channel_count >= 0
       OR accepted_channel_count >= 0
       OR rejected_channel_count >= 0
       OR TRUE
     )`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: query_dispatch_batch_completion_count_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.query_dispatch_batches
     DROP CONSTRAINT query_dispatch_batches_completion_count_check`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: query_dispatch_batch_completion_count_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.query_dispatch_batches
     DROP CONSTRAINT query_dispatch_batches_outcome_check,
     ADD CONSTRAINT query_dispatch_batches_outcome_check CHECK (
       outcome IS NULL OR outcome='completed_with_system_failures' OR TRUE
     )`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: query_dispatch_batch_outcome_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.query_dispatch_batches
     DROP CONSTRAINT query_dispatch_batches_outcome_check`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: query_dispatch_batch_outcome_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.youtube_api_batches
     DROP CONSTRAINT youtube_api_batches_active_job_check,
     ADD CONSTRAINT youtube_api_batches_active_job_check CHECK (
       active_job_id IS NULL OR active_job_attempt > 0 OR TRUE
     )`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: data_api_batch_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    "ALTER TABLE crawler.query_dispatch_batches DROP COLUMN total_channel_count",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: query_dispatch_batch_completion_count_columns/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    "ALTER TABLE crawler.query_dispatch_batches DROP COLUMN outcome",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: query_dispatch_batch_outcome_column/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    "ALTER TABLE crawler.channel_execution_attempts DROP COLUMN dispatch_generation",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: channel_execution_attempt_dispatch_generation/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.channel_candidates
     DROP CONSTRAINT channel_candidates_snapshot_active_job_check`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: candidate_snapshot_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    "ALTER TABLE crawler.channel_runs DROP COLUMN detail_active_job_epoch",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: content_detail_active_job_epoch, content_detail_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    "ALTER TABLE crawler.channel_runs DROP COLUMN detail_job_epoch",
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: content_detail_job_epoch, content_detail_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.channel_runs
     DROP CONSTRAINT channel_runs_detail_active_job_check`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: content_detail_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `ALTER TABLE crawler.channel_runs
     DROP CONSTRAINT channel_runs_detail_active_job_check,
     ADD CONSTRAINT channel_runs_detail_active_job_check CHECK (
       detail_job_epoch >= 0
       OR detail_active_job_attempt > 0
       OR detail_active_scope_key IS NOT NULL
       OR detail_active_job_epoch = detail_job_epoch
     )`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: content_detail_active_job_check/,
  );
  await client.query("ROLLBACK");

  await client.query("BEGIN");
  await client.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_dispatch_generation=2
     WHERE channel_id='UCschemaintent'`,
  );
  await assert.rejects(
    verifyRotaWorkerV2Schema(client),
    /missing: candidate_snapshot_dispatch_generation_alignment/,
  );
  await client.query("ROLLBACK");

  await client.query(
    `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
     VALUES ('UCschemarecovery','https://www.youtube.com/channel/UCschemarecovery',
             'Pinned recovery schema test','active')`,
  );
  const recoveryCandidate = await client.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation
     ) VALUES (
       'schema-backfill-batch','schema-backfill-cycle','UCschemarecovery',
       'https://www.youtube.com/channel/UCschemarecovery','accepted',1
     ) RETURNING candidate_id`,
  );
  const recoveryCandidateId = Number(recoveryCandidate.rows[0].candidate_id);
  const recoveryIntent = await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
       first_dispatch_batch_id,dispatch_attempts
     ) VALUES (
       'schema-recovery-source',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),43,
       'UCschemarecovery','{}'::jsonb,repeat('b',64),$1,
       'schema-backfill-batch',1
     ) RETURNING migration_intent_id`,
    [recoveryCandidateId],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,detail_status
     ) VALUES ('run:schema-recovery','UCschemarecovery',$1,'running','full','pending')`,
    [recoveryCandidateId],
  );
  await client.query(
    `UPDATE crawler.channels
     SET latest_run_id='run:schema-recovery'
     WHERE channel_id='UCschemarecovery'`,
  );
  await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,status,retry_dispatch_generation,recovery_run_id
     ) VALUES ($1,$2,'schema-backfill-batch',1,'schema-recovery-job',1,
               'LEASE_CONFLICT','lease','dispatched',2,'run:schema-recovery')`,
    [Number(recoveryIntent.rows[0].migration_intent_id), recoveryCandidateId],
  );
  await assert.rejects(
    client.query("DELETE FROM crawler.channel_runs WHERE run_id='run:schema-recovery'"),
    /migration_system_retry_items_recovery_run_id_fkey/,
  );
  assert.deepEqual((await client.query(
    `SELECT recovery_run_id
     FROM crawler.migration_system_retry_items
     WHERE failed_job_id='schema-recovery-job'`,
  )).rows[0], { recovery_run_id: "run:schema-recovery" });
});
