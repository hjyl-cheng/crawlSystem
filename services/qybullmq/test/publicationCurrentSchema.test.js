import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  publicationCurrentApplyGuard,
  publicationCurrentSchemaBlock,
} from "../scripts/applyPublicationCurrentSchema.mjs";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";

test("Publication Current controlled deployment contains only the four Phase B state tables", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const block = publicationCurrentSchemaBlock(schema);

  for (const table of ["stream", "channel_stream_state", "channel_delivery_state", "domain_current"]) {
    assert.match(block, new RegExp(`CREATE TABLE IF NOT EXISTS publication\\.${table}`));
  }
  assert.doesNotMatch(block, /publication\.revision/);
  assert.doesNotMatch(block, /publication\.outbox/);
  assert.match(block, /domain IN \('channel', 'video', 'agent'\)/);
  assert.match(block, /data_sequence = 0 AND current_revision_id IS NULL/);
  assert.match(block, /readiness_status <> 'not_ready' OR jsonb_array_length\(readiness_reasons\) > 0/);
  assert.match(block, /ux_publication_channel_stream_owned/);
  assert.match(block, /guard_stream_lifecycle/);
  assert.match(block, /guard_channel_stream_lifecycle/);
  assert.match(block, /guard_channel_delivery_lifecycle/);
  assert.doesNotMatch(block, /crawler\.registry_promotion_is_complete/);
  assert.doesNotMatch(block, /crawler\.guard_channel_registry_promotion/);
  assert.doesNotMatch(block, /CREATE UNIQUE INDEX IF NOT EXISTS ux_publication_stream_active_deployment/);
});

test("runtime schema migration excludes the controlled Publication Current block", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const runtimeSchema = crawlerRuntimeSchema(schema);
  const dbSource = await readFile(new URL("../src/db.js", import.meta.url), "utf8");

  assert.doesNotMatch(runtimeSchema, /CREATE TABLE IF NOT EXISTS publication\./);
  assert.doesNotMatch(runtimeSchema, /publication-(?:current|capture)-schema:(?:start|end)/);
  assert.match(runtimeSchema, /CREATE OR REPLACE FUNCTION crawler\.registry_promotion_is_complete/);
  assert.match(runtimeSchema, /CREATE OR REPLACE FUNCTION crawler\.guard_channel_registry_promotion/);
  assert.match(runtimeSchema, /to_regclass\(required_table\.table_name\)/);
  const publicationTables = [
    "stream",
    "channel_stream_state",
    "channel_delivery_state",
    "domain_current",
    "revision",
    "outbox",
  ];
  for (const table of publicationTables) {
    assert.match(runtimeSchema, new RegExp(`'publication\\.${table}'`));
  }
  assert.match(runtimeSchema, /publication_table_count = 0[\s\S]*publication_table_count <> 6/);
  assert.match(runtimeSchema, /EXECUTE \$publication_promotion_complete\$/);
  assert.match(dbSource, /runSchemaMigration\(crawlerRuntimeSchema\(schema\)\)/);
});

test("Publication Current schema apply requires explicit database and Channel count confirmation", () => {
  const base = {
    CONFIRM_PUBLICATION_CURRENT_SCHEMA_APPLY: "bullmq_crawler_isolated",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "17",
  };

  assert.throws(() => publicationCurrentApplyGuard(base, ["node", "script"]), /--apply/);
  assert.throws(() => publicationCurrentApplyGuard({
    EXPECTED_CRAWLER_CHANNEL_COUNT: "17",
  }, ["node", "script", "--apply"]), /CONFIRM_PUBLICATION_CURRENT_SCHEMA_APPLY/);
  assert.throws(() => publicationCurrentApplyGuard({
    ...base,
    EXPECTED_CRAWLER_CHANNEL_COUNT: "unknown",
  }, ["node", "script", "--apply"]), /EXPECTED_CRAWLER_CHANNEL_COUNT/);
  for (const invalid of ["17junk", "17.0", "-1", "01", ""]) {
    assert.throws(() => publicationCurrentApplyGuard({
      ...base,
      EXPECTED_CRAWLER_CHANNEL_COUNT: invalid,
    }, ["node", "script", "--apply"]), /EXPECTED_CRAWLER_CHANNEL_COUNT/);
  }
  assert.deepEqual(
    publicationCurrentApplyGuard(base, ["node", "script", "--apply"]),
    { confirmedDatabase: "bullmq_crawler_isolated", expectedChannelCount: 17 },
  );
});
