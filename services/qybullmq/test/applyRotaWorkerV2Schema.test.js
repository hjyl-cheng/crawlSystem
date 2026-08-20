import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  guardedRotaWorkerSchemaConfig,
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
  assert.doesNotMatch(block, /ALTER TABLE crawler\.channels ADD COLUMN IF NOT EXISTS country/);
});

test("Rota Worker V2 deployment requires explicit database and row-count confirmation", () => {
  const base = {
    ROTA_WORKER_V2_SCHEMA_APPLY: "true",
    POSTGRES_HOST: "crawler-pgbouncer",
    POSTGRES_PORT: "6432",
    POSTGRES_USER: "bullmq",
    POSTGRES_PASSWORD: "secret",
    POSTGRES_DB: "crawler_production",
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
    /must equal POSTGRES_DB/,
  );
  assert.throws(
    () => guardedRotaWorkerSchemaConfig({ ...base, EXPECTED_CRAWLER_CHANNEL_COUNT: "" }),
    /explicit non-negative integer/,
  );
});
