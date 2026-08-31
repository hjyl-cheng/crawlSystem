import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  guardedRotaWorkerSchemaConfig,
  normalizedCheckExpression,
  rotaWorkerV2SchemaBlock,
} from "../scripts/applyRotaWorkerV2Schema.mjs";

test("Rota Worker V2 deployment extracts the complete additive integration schema", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const block = rotaWorkerV2SchemaBlock(schema);
  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.business_run_bindings/);
  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.query_quality_chunks/);
  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.proxy_job_dispatch_outbox/);
  assert.match(block, /trg_guard_managed_query_page_state/);
  assert.match(block, /trg_guard_query_quality_chunk_members/);
  assert.match(
    block,
    /ALTER TABLE crawler\.channel_candidates[\s\S]*ADD COLUMN IF NOT EXISTS snapshot_dispatch_generation BIGINT NOT NULL DEFAULT 0/,
  );
  assert.match(block, /channel_candidates_snapshot_dispatch_generation_check/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS snapshot_active_job_id TEXT/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS snapshot_active_job_attempt INTEGER/);
  assert.match(block, /channel_candidates_snapshot_active_job_check/);
  assert.match(block, /snapshot_active_job_attempt >= 0/);
  assert.match(block, /ux_crawler_proxy_job_dispatch_outbox_channel_snapshot_generation/);
  assert.match(block, /payload_json->>'dispatch_generation'/);
  assert.match(block, /MAX\(intent\.dispatch_attempts\)/);
  assert.match(block, /LEFT JOIN crawler\.migration_channel_intents AS intent/);
  assert.match(
    block,
    /SET snapshot_dispatch_generation = expectation\.expected_generation,[\s\S]*snapshot_active_job_id = NULL,[\s\S]*snapshot_active_job_attempt = NULL/,
  );
  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.migration_retry_intents/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS terminal_job_attempt BIGINT/);
  assert.match(block, /migration_retry_intents_terminal_job_attempt_check/);
  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.migration_system_retry_items/);
  assert.match(block, /failed_dispatch_batch_id TEXT NOT NULL/);
  assert.match(
    block,
    /ALTER TABLE crawler\.migration_system_retry_items\s+ADD COLUMN IF NOT EXISTS failed_dispatch_batch_id TEXT/,
  );
  assert.match(block, /recovery_run_id TEXT/);
  assert.match(
    block,
    /ALTER TABLE crawler\.migration_system_retry_items\s+ADD COLUMN IF NOT EXISTS recovery_run_id TEXT/,
  );
  assert.match(
    block,
    /ADD CONSTRAINT migration_system_retry_items_recovery_run_id_fkey\s+FOREIGN KEY \(recovery_run_id\)\s+REFERENCES crawler\.channel_runs\(run_id\) ON DELETE RESTRICT/,
  );
  assert.match(block, /ADD COLUMN IF NOT EXISTS recovery_agent_active_job_id TEXT/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS recovery_agent_active_job_attempt BIGINT/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS recovery_agent_job_epoch BIGINT NOT NULL DEFAULT 0/);
  assert.match(block, /migration_system_retry_items_recovery_agent_active_job_check/);
  assert.match(block, /recovery_agent_active_job_attempt > 0/);
  assert.match(block, /recovery_agent_job_epoch >= 0/);
  assert.doesNotMatch(
    block,
    /UPDATE crawler\.migration_system_retry_items[\s\S]*channel_candidates/,
  );
  assert.match(block, /ux_crawler_migration_system_retry_active_candidate/);
  assert.match(block, /idx_crawler_migration_system_retry_status/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS failed_channel_count INTEGER NOT NULL DEFAULT 0/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS total_channel_count INTEGER NOT NULL DEFAULT 0/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS outcome TEXT/);
  assert.match(block, /query_dispatch_batches_completion_count_check/);
  assert.match(block, /query_dispatch_batches_outcome_check/);
  assert.match(
    block,
    /ALTER TABLE IF EXISTS crawler\.youtube_api_batches\s+ADD COLUMN IF NOT EXISTS active_job_id TEXT/,
  );
  assert.match(block, /ADD COLUMN IF NOT EXISTS active_job_attempt BIGINT/);
  assert.match(block, /youtube_api_batches_active_job_check/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS detail_active_job_id TEXT/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS detail_active_job_attempt BIGINT/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS detail_active_scope_key TEXT/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS detail_job_epoch BIGINT NOT NULL DEFAULT 0/);
  assert.match(block, /ADD COLUMN IF NOT EXISTS detail_active_job_epoch BIGINT/);
  assert.match(
    block,
    /UPDATE crawler\.channel_runs\s+SET detail_active_job_epoch=detail_job_epoch/,
  );
  assert.match(block, /channel_runs_detail_active_job_check/);
  assert.match(block, /detail_active_job_attempt > 0/);
  assert.match(block, /detail_job_epoch >= 0/);
  assert.match(block, /detail_active_job_epoch = detail_job_epoch/);
  assert.match(block, /channel_execution_attempts[\s\S]*dispatch_generation BIGINT/);
  assert.match(block, /UNIQUE \(candidate_id,dispatch_generation\)/);
  assert.match(block, /ux_crawler_migration_retry_intents_active_candidate/);
  assert.match(block, /idx_crawler_migration_retry_intents_status/);
  assert.doesNotMatch(block, /ALTER TABLE crawler\.channels ADD COLUMN IF NOT EXISTS country/);
});

test("Crawler bootstrap includes the Candidate attempt Fence shape", async () => {
  const bootstrap = await readFile(
    new URL("../../../database/bootstrap/crawler.sql", import.meta.url),
    "utf8",
  );
  assert.match(bootstrap, /snapshot_dispatch_generation bigint DEFAULT 0 NOT NULL/);
  assert.match(bootstrap, /snapshot_active_job_id text/);
  assert.match(bootstrap, /snapshot_active_job_attempt integer/);
  assert.match(bootstrap, /channel_candidates_snapshot_active_job_check/);
  assert.match(bootstrap, /snapshot_active_job_attempt >= 0/);
  assert.match(bootstrap, /ux_crawler_proxy_job_dispatch_outbox_channel_snapshot_generation/);
  assert.match(bootstrap, /channel_execution_attempts[\s\S]*dispatch_generation bigint/);
  assert.match(bootstrap, /migration_retry_intents[\s\S]*terminal_job_attempt bigint/);
  assert.match(bootstrap, /CREATE TABLE crawler\.migration_system_retry_items/);
  assert.match(bootstrap, /failed_dispatch_batch_id text NOT NULL/);
  assert.match(bootstrap, /recovery_run_id text/);
  assert.match(
    bootstrap,
    /CONSTRAINT migration_system_retry_items_recovery_run_id_fkey FOREIGN KEY \(recovery_run_id\) REFERENCES crawler\.channel_runs\(run_id\) ON DELETE RESTRICT/,
  );
  assert.match(bootstrap, /recovery_agent_active_job_id text/);
  assert.match(bootstrap, /recovery_agent_active_job_attempt bigint/);
  assert.match(bootstrap, /recovery_agent_job_epoch bigint DEFAULT 0 NOT NULL/);
  assert.match(bootstrap, /migration_system_retry_items_recovery_agent_active_job_check/);
  assert.match(bootstrap, /recovery_agent_active_job_attempt > 0/);
  assert.match(bootstrap, /recovery_agent_job_epoch >= 0/);
  assert.match(bootstrap, /ux_crawler_migration_system_retry_active_candidate/);
  assert.match(bootstrap, /idx_crawler_migration_system_retry_status/);
  assert.match(bootstrap, /failed_channel_count integer DEFAULT 0 NOT NULL/);
  assert.match(bootstrap, /total_channel_count integer DEFAULT 0 NOT NULL/);
  assert.match(bootstrap, /query_dispatch_batches_completion_count_check/);
  assert.match(bootstrap, /active_job_id text/);
  assert.match(bootstrap, /active_job_attempt bigint/);
  assert.match(bootstrap, /youtube_api_batches_active_job_check/);
  assert.match(bootstrap, /detail_active_job_id text/);
  assert.match(bootstrap, /detail_active_job_attempt bigint/);
  assert.match(bootstrap, /detail_active_scope_key text/);
  assert.match(bootstrap, /detail_job_epoch bigint DEFAULT 0 NOT NULL/);
  assert.match(bootstrap, /detail_active_job_epoch bigint/);
  assert.match(bootstrap, /channel_runs_detail_active_job_check/);
  assert.match(bootstrap, /detail_active_job_epoch = detail_job_epoch/);
});

test("Rota Worker V2 deployment requires explicit database and row-count confirmation", () => {
  const base = {
    ROTA_WORKER_V2_SCHEMA_APPLY: "true",
    POSTGRES_HOST: "crawler-pgbouncer",
    POSTGRES_PORT: "6432",
    POSTGRES_USER: "bullmq",
    POSTGRES_PASSWORD: "secret",
    POSTGRES_DB: "crawler_production",
    EXPECTED_CRAWLER_DATABASE: "crawler_production",
    FORBIDDEN_CRAWLER_DATABASE: "bullmq_crawler_migration",
    CONFIRM_ROTA_WORKER_V2_DATABASE: "crawler_production",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "22000",
  };
  assert.equal(guardedRotaWorkerSchemaConfig(base).expectedChannelCount, 22000);
  assert.throws(
    () => guardedRotaWorkerSchemaConfig({ ...base, ROTA_WORKER_V2_SCHEMA_APPLY: "false" }),
    /ROTA_WORKER_V2_SCHEMA_APPLY/,
  );
  assert.throws(
    () => guardedRotaWorkerSchemaConfig({
      ...base,
      CONFIRM_ROTA_WORKER_V2_DATABASE: "another_database",
    }),
    /must equal EXPECTED_CRAWLER_DATABASE/,
  );
  assert.throws(
    () => guardedRotaWorkerSchemaConfig({
      ...base,
      POSTGRES_DB: "bullmq_crawler_migration",
      EXPECTED_CRAWLER_DATABASE: "bullmq_crawler_migration",
      CONFIRM_ROTA_WORKER_V2_DATABASE: "bullmq_crawler_migration",
    }),
    /refusing forbidden Crawler database/,
  );
  assert.throws(
    () => guardedRotaWorkerSchemaConfig({
      ...base,
      EXPECTED_CRAWLER_DATABASE: "another_database",
    }),
    /POSTGRES_DB must equal EXPECTED_CRAWLER_DATABASE/,
  );
  assert.throws(
    () => guardedRotaWorkerSchemaConfig({ ...base, EXPECTED_CRAWLER_CHANNEL_COUNT: "" }),
    /explicit non-negative integer/,
  );
});

test("Rota Worker V2 detail ownership constraint comparison rejects a weak OR shape", () => {
  const required = `
    detail_job_epoch >= 0 AND (
      (detail_active_job_id IS NULL AND detail_active_job_attempt IS NULL
       AND detail_active_scope_key IS NULL AND detail_active_job_epoch IS NULL)
      OR
      (detail_active_job_id IS NOT NULL AND detail_active_job_attempt IS NOT NULL
       AND detail_active_job_attempt > 0 AND detail_active_scope_key IS NOT NULL
       AND detail_active_job_epoch IS NOT NULL
       AND detail_active_job_epoch = detail_job_epoch)
    )`;
  const weak = `
    detail_job_epoch >= 0 OR detail_active_job_attempt > 0
    OR detail_active_scope_key IS NOT NULL
    OR detail_active_job_epoch = detail_job_epoch`;
  assert.notEqual(normalizedCheckExpression(required), normalizedCheckExpression(weak));
});

test("Rota Worker V2 catalog expressions reject weak checks with the same keywords", () => {
  const requiredOutcome = `
    ((outcome IS NULL)
      OR (outcome = ANY (ARRAY['completed'::text,'completed_with_system_failures'::text])))`;
  const weakOutcome = `
    outcome IS NULL OR outcome='completed_with_system_failures' OR TRUE`;
  const requiredActiveIndex = `
    (status = ANY (ARRAY['retrying'::text,'pending'::text,'dispatched'::text]))`;
  const wrongActiveIndex = `
    (status = ANY (ARRAY['pending'::text,'dispatched'::text]))`;

  assert.notEqual(
    normalizedCheckExpression(requiredOutcome),
    normalizedCheckExpression(weakOutcome),
  );
  assert.notEqual(
    normalizedCheckExpression(requiredActiveIndex),
    normalizedCheckExpression(wrongActiveIndex),
  );
});
