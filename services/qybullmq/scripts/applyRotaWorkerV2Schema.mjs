import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { verifyCrawlerWriterDatabase } from "../src/databaseIdentity.js";

const { Client } = pg;
const START_MARKER = "-- qy-rota-worker-v2-schema:start";
const END_MARKER = "-- qy-rota-worker-v2-schema:end";
const CONTENT_DETAIL_ACTIVE_JOB_CONSTRAINT = `
  ((detail_job_epoch >= 0) AND (
    ((detail_active_job_id IS NULL)
      AND (detail_active_job_attempt IS NULL)
      AND (detail_active_scope_key IS NULL)
      AND (detail_active_job_epoch IS NULL))
    OR
    ((detail_active_job_id IS NOT NULL)
      AND (detail_active_job_attempt IS NOT NULL)
      AND (detail_active_job_attempt > 0)
      AND (detail_active_scope_key IS NOT NULL)
      AND (detail_active_job_epoch IS NOT NULL)
      AND (detail_active_job_epoch = detail_job_epoch))
  ))
`;
const REQUIRED_CHECK_EXPRESSIONS = Object.freeze({
  migration_system_retry_recovery_agent_active_job_check: `
    ((recovery_agent_job_epoch >= 0) AND (
      ((recovery_agent_active_job_id IS NULL)
        AND (recovery_agent_active_job_attempt IS NULL))
      OR
      ((recovery_agent_active_job_id IS NOT NULL)
        AND (recovery_agent_active_job_attempt IS NOT NULL)
        AND (recovery_agent_active_job_attempt > 0))
    ))
  `,
  query_dispatch_batch_completion_count_check: `
    ((failed_channel_count >= 0)
      AND (total_channel_count >= 0)
      AND (accepted_channel_count >= 0)
      AND (rejected_channel_count >= 0)
      AND (((accepted_channel_count + rejected_channel_count) + failed_channel_count)
        <= total_channel_count))
  `,
  query_dispatch_batch_outcome_check: `
    ((outcome IS NULL)
      OR (outcome = ANY (ARRAY['completed'::text,'completed_with_system_failures'::text])))
  `,
  data_api_batch_active_job_check: `
    (((active_job_id IS NULL) AND (active_job_attempt IS NULL))
      OR ((active_job_id IS NOT NULL)
        AND (active_job_attempt IS NOT NULL)
        AND (active_job_attempt > 0)))
  `,
  content_detail_active_job_check: CONTENT_DETAIL_ACTIVE_JOB_CONSTRAINT,
});
const ACTIVE_SYSTEM_RETRY_PREDICATE = `
  (status = ANY (ARRAY['retrying'::text,'pending'::text,'dispatched'::text]))
`;

export function normalizedCheckExpression(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/::(?:bigint|integer|text|boolean)/g, "")
    .replace(/\s+/g, "");
}

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
  const expectedDatabase = String(environment.EXPECTED_CRAWLER_DATABASE ?? "").trim();
  const forbiddenDatabase = String(
    environment.FORBIDDEN_CRAWLER_DATABASE ?? "bullmq_crawler_migration",
  ).trim();
  const confirmedDatabase = String(
    environment.CONFIRM_ROTA_WORKER_V2_DATABASE ?? "",
  ).trim();
  if (!expectedDatabase) throw new Error("EXPECTED_CRAWLER_DATABASE is required");
  if (!forbiddenDatabase) throw new Error("FORBIDDEN_CRAWLER_DATABASE is required");
  if (!database || database !== expectedDatabase) {
    throw new Error("POSTGRES_DB must equal EXPECTED_CRAWLER_DATABASE");
  }
  if (database === forbiddenDatabase) {
    throw new Error(`refusing forbidden Crawler database ${database}`);
  }
  if (confirmedDatabase !== expectedDatabase) {
    throw new Error(
      "CONFIRM_ROTA_WORKER_V2_DATABASE must equal EXPECTED_CRAWLER_DATABASE",
    );
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
    expectedDatabase,
    forbiddenDatabase,
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
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_run_id' AND data_type='text'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS migration_system_retry_recovery_run_id,
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
       ) AS migration_system_retry_recovery_run_reference,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_agent_active_job_id' AND data_type='text'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS migration_system_retry_recovery_agent_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_agent_active_job_attempt' AND data_type='bigint'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS migration_system_retry_recovery_agent_active_job_attempt,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_system_retry_items'
           AND column_name='recovery_agent_job_epoch' AND data_type='bigint'
           AND is_nullable='NO' AND column_default='0'
       ) AS migration_system_retry_recovery_agent_job_epoch,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.migration_system_retry_items')
           AND conname='migration_system_retry_items_recovery_agent_active_job_check'
           AND contype='c' AND convalidated
       ) AS migration_system_retry_recovery_agent_active_job_check,
       FALSE AS migration_system_retry_active_candidate_index,
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
       ) AS query_dispatch_batch_completion_count_check,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.query_dispatch_batches')
           AND conname='query_dispatch_batches_outcome_check'
           AND contype='c' AND convalidated
       ) AS query_dispatch_batch_outcome_check,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='youtube_api_batches'
           AND column_name='active_job_id' AND data_type='text'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS data_api_batch_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='youtube_api_batches'
           AND column_name='active_job_attempt' AND data_type='bigint'
           AND is_nullable='YES' AND column_default IS NULL
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
           AND is_nullable='YES' AND column_default IS NULL
       ) AS content_detail_active_job_id,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='detail_active_job_attempt' AND data_type='bigint'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS content_detail_active_job_attempt,
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='channel_runs'
           AND column_name='detail_active_scope_key' AND data_type='text'
           AND is_nullable='YES' AND column_default IS NULL
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
           AND pg_get_constraintdef(oid) LIKE '%detail_active_job_attempt > 0%'
           AND pg_get_constraintdef(oid) LIKE '%detail_active_scope_key%'
           AND pg_get_constraintdef(oid) LIKE '%detail_job_epoch >= 0%'
           AND pg_get_constraintdef(oid) LIKE '%detail_active_job_epoch = detail_job_epoch%'
       ) AS content_detail_active_job_check,
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
       EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_schema='crawler' AND table_name='migration_retry_intents'
           AND column_name='terminal_job_attempt' AND data_type='bigint'
           AND is_nullable='YES' AND column_default IS NULL
       ) AS migration_retry_intents_terminal_job_attempt,
       EXISTS (
         SELECT 1 FROM pg_constraint
         WHERE conrelid=to_regclass('crawler.migration_retry_intents')
           AND conname='migration_retry_intents_terminal_job_attempt_check'
           AND contype='c' AND convalidated
       ) AS migration_retry_intents_terminal_job_attempt_check,
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
  const checkExpressions = await client.query(
    `WITH required(state_key,relation_name,constraint_name) AS (
       VALUES
         ('migration_system_retry_recovery_agent_active_job_check',
          'crawler.migration_system_retry_items',
          'migration_system_retry_items_recovery_agent_active_job_check'),
         ('query_dispatch_batch_completion_count_check',
          'crawler.query_dispatch_batches',
          'query_dispatch_batches_completion_count_check'),
         ('query_dispatch_batch_outcome_check',
          'crawler.query_dispatch_batches',
          'query_dispatch_batches_outcome_check'),
         ('data_api_batch_active_job_check',
          'crawler.youtube_api_batches',
          'youtube_api_batches_active_job_check'),
         ('content_detail_active_job_check',
          'crawler.channel_runs',
          'channel_runs_detail_active_job_check')
     )
     SELECT required.state_key,
            pg_get_expr(constraint_state.conbin,constraint_state.conrelid) AS expression
     FROM required
     LEFT JOIN pg_constraint AS constraint_state
       ON constraint_state.conrelid=to_regclass(required.relation_name)
      AND constraint_state.conname=required.constraint_name
      AND constraint_state.contype='c'
      AND constraint_state.convalidated`,
  );
  for (const { state_key: stateKey, expression } of checkExpressions.rows) {
    state[stateKey] = expression !== null
      && normalizedCheckExpression(expression)
        === normalizedCheckExpression(REQUIRED_CHECK_EXPRESSIONS[stateKey]);
  }
  const activeSystemRetryIndex = await client.query(
    `SELECT
       index_state.indisunique AS is_unique,
       index_state.indisvalid AS is_valid,
       index_state.indisready AS is_ready,
       index_state.indnatts AS attribute_count,
       index_state.indnkeyatts AS key_attribute_count,
       ARRAY(
         SELECT attribute.attname::text
         FROM unnest(index_state.indkey) WITH ORDINALITY AS index_key(attnum,position)
         LEFT JOIN pg_attribute AS attribute
           ON attribute.attrelid=index_state.indrelid
          AND attribute.attnum=index_key.attnum
          AND NOT attribute.attisdropped
         ORDER BY index_key.position
       ) AS columns,
       pg_get_expr(index_state.indpred,index_state.indrelid) AS predicate
     FROM pg_index AS index_state
     WHERE index_state.indexrelid=
       to_regclass('crawler.ux_crawler_migration_system_retry_active_candidate')
       AND index_state.indrelid=to_regclass('crawler.migration_system_retry_items')`,
  );
  const activeIndex = activeSystemRetryIndex.rows[0];
  state.migration_system_retry_active_candidate_index = Boolean(
    activeIndex?.is_unique
      && activeIndex?.is_valid
      && activeIndex?.is_ready
      && Number(activeIndex?.attribute_count) === 1
      && Number(activeIndex?.key_attribute_count) === 1
      && Array.isArray(activeIndex?.columns)
      && activeIndex.columns.length === 1
      && activeIndex.columns[0] === "candidate_id"
      && normalizedCheckExpression(activeIndex?.predicate)
        === normalizedCheckExpression(ACTIVE_SYSTEM_RETRY_PREDICATE),
  );
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
    const identity = await verifyCrawlerWriterDatabase(
      client.query.bind(client),
      {
        EXPECTED_CRAWLER_DATABASE: guarded.expectedDatabase,
        FORBIDDEN_CRAWLER_DATABASE: guarded.forbiddenDatabase,
      },
    );
    if (identity.database !== guarded.expectedDatabase) {
      throw new Error(`database confirmation mismatch: ${identity.database}`);
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
