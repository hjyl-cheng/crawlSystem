import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";

const { Client } = pg;
const START_MARKER = "-- qy-rota-worker-v2-schema:start";
const END_MARKER = "-- qy-rota-worker-v2-schema:end";

function nonNegativeInteger(value, field) {
  if (value === null || value === undefined || String(value).trim() === "") {
    throw new Error(`${field} must be an explicit non-negative integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${field} must be an explicit non-negative integer`);
  }
  return parsed;
}

export function rotaWorkerV2SchemaBlock(schema) {
  const start = schema.indexOf(START_MARKER);
  const end = schema.indexOf(END_MARKER);
  if (start < 0 || end < 0 || end <= start) {
    throw new Error("Rota Worker V2 schema markers are missing or invalid");
  }
  return schema.slice(start + START_MARKER.length, end).trim();
}

export function guardedRotaWorkerSchemaConfig(environment = process.env) {
  if (String(environment.ROTA_WORKER_V2_SCHEMA_APPLY ?? "").trim().toLowerCase() !== "true") {
    throw new Error("ROTA_WORKER_V2_SCHEMA_APPLY=true is required");
  }
  const database = String(environment.POSTGRES_DB ?? "").trim();
  const confirmedDatabase = String(
    environment.CONFIRM_ROTA_WORKER_V2_DATABASE ?? "",
  ).trim();
  if (!database || confirmedDatabase !== database) {
    throw new Error("CONFIRM_ROTA_WORKER_V2_DATABASE must equal POSTGRES_DB");
  }
  const host = String(environment.POSTGRES_HOST ?? "").trim();
  if (!host) throw new Error("POSTGRES_HOST is required");
  return Object.freeze({
    connection: Object.freeze({
      host,
      port: Number(environment.POSTGRES_PORT || 5432),
      user: String(environment.POSTGRES_USER || "bullmq"),
      password: String(environment.POSTGRES_PASSWORD || ""),
      database,
    }),
    expectedChannelCount: nonNegativeInteger(
      environment.EXPECTED_CRAWLER_CHANNEL_COUNT,
      "EXPECTED_CRAWLER_CHANNEL_COUNT",
    ),
  });
}

export async function verifyRotaWorkerV2Schema(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("PostgreSQL client is required");
  }
  const verified = await client.query(
    `SELECT
       to_regclass('crawler.business_run_bindings') IS NOT NULL AS business_run_bindings,
       to_regclass('crawler.query_quality_chunks') IS NOT NULL AS query_quality_chunks,
       to_regclass('crawler.query_quality_chunk_members') IS NOT NULL AS query_quality_chunk_members,
       to_regclass('crawler.proxy_job_dispatch_outbox') IS NOT NULL AS proxy_job_dispatch_outbox,
       to_regclass('crawler.migration_retry_intents') IS NOT NULL AS migration_retry_intents,
       to_regclass('crawler.migration_system_retry_items') IS NOT NULL
         AS migration_system_retry_items,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='failed_dispatch_batch_id' AND data_type='text'
       ) AS migration_system_retry_failed_dispatch_batch_id,
       to_regclass('crawler.ux_crawler_migration_system_retry_active_candidate') IS NOT NULL
         AS migration_system_retry_active_candidate_index,
       to_regclass('crawler.idx_crawler_migration_system_retry_status') IS NOT NULL
         AS migration_system_retry_status_index,
       (
         SELECT count(*)=2
         FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='query_dispatch_batches'
           AND column_name IN ('failed_channel_count','total_channel_count')
           AND data_type='integer' AND is_nullable='NO' AND column_default='0'
       ) AS query_dispatch_batch_completion_count_columns,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='query_dispatch_batches'
           AND column_name='outcome' AND data_type='text' AND is_nullable='YES'
       ) AS query_dispatch_batch_outcome_column,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.query_dispatch_batches')
           AND conname='query_dispatch_batches_completion_count_check'
           AND contype='c' AND convalidated
           AND pg_get_constraintdef(oid) LIKE '%failed_channel_count%'
           AND pg_get_constraintdef(oid) LIKE '%total_channel_count%'
           AND pg_get_constraintdef(oid) LIKE '%accepted_channel_count%'
           AND pg_get_constraintdef(oid) LIKE '%rejected_channel_count%'
       ) AS query_dispatch_batch_completion_count_check,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.query_dispatch_batches')
           AND conname='query_dispatch_batches_outcome_check'
           AND contype='c' AND convalidated
           AND pg_get_constraintdef(oid) LIKE '%completed_with_system_failures%'
       ) AS query_dispatch_batch_outcome_check,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_candidates'
           AND column_name='snapshot_dispatch_generation'
           AND data_type='bigint' AND is_nullable='NO' AND column_default='0'
       ) AS candidate_snapshot_dispatch_generation,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.channel_candidates')
           AND conname='channel_candidates_snapshot_dispatch_generation_check'
           AND contype='c' AND convalidated
       ) AS candidate_snapshot_dispatch_generation_check,
       NOT EXISTS (
         SELECT 1
         FROM crawler.channel_candidates AS candidate
         LEFT JOIN (
           SELECT
             target_candidate_id,
             MAX(dispatch_attempts)::BIGINT AS expected_generation
           FROM crawler.migration_channel_intents
           WHERE target_candidate_id IS NOT NULL
           GROUP BY target_candidate_id
         ) AS intent
           ON intent.target_candidate_id = candidate.candidate_id
         WHERE candidate.snapshot_dispatch_generation < CASE
           WHEN intent.expected_generation IS NOT NULL THEN intent.expected_generation
           WHEN candidate.status <> 'discovered' THEN 1::BIGINT
           ELSE 0::BIGINT
         END
       ) AS candidate_snapshot_dispatch_generation_alignment,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_candidates'
           AND column_name='snapshot_active_job_id'
           AND data_type='text' AND is_nullable='YES' AND column_default IS NULL
       ) AS candidate_snapshot_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_candidates'
           AND column_name='snapshot_active_job_attempt'
           AND data_type='integer' AND is_nullable='YES' AND column_default IS NULL
       ) AS candidate_snapshot_active_job_attempt,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.channel_candidates')
           AND conname='channel_candidates_snapshot_active_job_check'
           AND contype='c' AND convalidated
           AND pg_get_constraintdef(oid) LIKE '%snapshot_active_job_attempt >= 0%'
       ) AS candidate_snapshot_active_job_check,
       to_regclass(
         'crawler.ux_crawler_proxy_job_dispatch_outbox_channel_snapshot_generation'
       ) IS NOT NULL AS channel_snapshot_outbox_generation_key,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_execution_attempts'
           AND column_name='dispatch_generation'
           AND data_type='bigint' AND is_nullable='YES' AND column_default IS NULL
       ) AS channel_execution_attempt_dispatch_generation,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.channel_execution_attempts')
           AND conname='channel_execution_attempts_dispatch_generation_check'
           AND contype='c' AND convalidated
       ) AS channel_execution_attempt_dispatch_generation_check,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.migration_retry_intents')
           AND conname='migration_retry_intents_candidate_id_dispatch_generation_key'
           AND contype='u' AND convalidated
       ) AS migration_retry_intents_generation_key,
       to_regclass('crawler.ux_crawler_migration_retry_intents_active_candidate') IS NOT NULL
         AS migration_retry_intents_active_candidate_index,
       to_regclass('crawler.idx_crawler_migration_retry_intents_status') IS NOT NULL
         AS migration_retry_intents_status_index,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='identity_policy_hash'
       ) AS channel_run_policy,
       EXISTS (
         SELECT 1 FROM pg_trigger
         WHERE tgname='trg_guard_managed_query_page_state' AND NOT tgisinternal
       ) AS discover_state_guard,
       EXISTS (
         SELECT 1 FROM pg_trigger
         WHERE tgname='trg_guard_query_quality_chunk_members' AND NOT tgisinternal
       ) AS query_quality_member_guard`,
  );
  const state = verified.rows[0] ?? {};
  const missing = Object.entries(state)
    .filter(([, present]) => present !== true)
    .map(([name]) => name);
  if (missing.length > 0) {
    throw new Error(`Rota Worker V2 schema verification failed; missing: ${missing.join(", ")}`);
  }
  return state;
}

async function main() {
  if (!process.argv.includes("--apply")) {
    throw new Error("refusing to apply: pass --apply explicitly");
  }
  const guarded = guardedRotaWorkerSchemaConfig();
  const schemaPath = fileURLToPath(new URL("../src/schema.sql", import.meta.url));
  const ddl = rotaWorkerV2SchemaBlock(await readFile(schemaPath, "utf8"));
  const client = new Client(guarded.connection);
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '120s'");
    await client.query("SELECT pg_advisory_xact_lock(781137218)");
    const preflight = await client.query(
      `SELECT current_database() AS database_name,
              (SELECT count(*)::int FROM crawler.channels) AS channel_count`,
    );
    const actual = preflight.rows[0];
    if (actual.database_name !== guarded.connection.database) {
      throw new Error(`database confirmation mismatch: ${actual.database_name}`);
    }
    if (Number(actual.channel_count) !== guarded.expectedChannelCount) {
      throw new Error(
        `Channel count mismatch: expected ${guarded.expectedChannelCount}, got ${actual.channel_count}`,
      );
    }
    await client.query(ddl);
    await verifyRotaWorkerV2Schema(client);
    await client.query("COMMIT");
    began = false;
    process.stdout.write(`${JSON.stringify({
      ok: true,
      database: actual.database_name,
      channel_count: Number(actual.channel_count),
      migration: "qy-rota-worker-v2-schema",
    })}\n`);
  } catch (error) {
    if (began) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
