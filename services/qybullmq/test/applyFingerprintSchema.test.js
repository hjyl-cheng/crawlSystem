import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  fingerprintSchemaBlock,
  guardedConnectionConfig,
} from "../scripts/applyFingerprintSchema.js";

test("fingerprint schema deployment extracts only the three qy runtime tables", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const block = fingerprintSchemaBlock(schema);

  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.browser_profile_groups/);
  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.browser_profiles/);
  assert.match(block, /CREATE TABLE IF NOT EXISTS crawler\.channel_execution_attempts/);
  assert.match(block, /channel_execution_attempts[\s\S]*dispatch_generation BIGINT/);
  assert.match(block, /profile_epoch >= 0/);
  assert.doesNotMatch(block, /ALTER TABLE crawler\.channels/);
});

test("fingerprint schema deployment refuses production-like connection targets", () => {
  const base = {
    QY_FINGERPRINT_SCHEMA_APPLY: "true",
    POSTGRES_USER: "bullmq",
    POSTGRES_PASSWORD: "secret",
    POSTGRES_PORT: "5432",
  };

  assert.throws(() => guardedConnectionConfig({
    ...base,
    POSTGRES_HOST: "postgres",
    POSTGRES_DB: "bullmq_crawler",
  }), /refusing/);
  assert.throws(() => guardedConnectionConfig({
    ...base,
    POSTGRES_HOST: "bullmq-crawler-migration-postgres",
    POSTGRES_DB: "bullmq_crawler_migration",
    QY_FINGERPRINT_SCHEMA_APPLY: "false",
  }), /QY_FINGERPRINT_SCHEMA_APPLY/);
  assert.equal(guardedConnectionConfig({
    ...base,
    POSTGRES_HOST: "bullmq-crawler-migration-postgres",
    POSTGRES_DB: "bullmq_crawler_migration",
  }).database, "bullmq_crawler_migration");
});
