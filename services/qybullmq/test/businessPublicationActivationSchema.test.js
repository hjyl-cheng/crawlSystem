import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  businessPublicationActivationSchemaApplyGuard,
} from "../scripts/applyBusinessPublicationActivationSchema.mjs";

test("Business Activation schema adds only isolated control and Current models", async () => {
  const schema = await readFile(
    new URL("../src/businessPublicationActivationSchema.sql", import.meta.url),
    "utf8",
  );
  for (const table of ["consumer_cursor", "activation", "activation_item", "projection_outbox"]) {
    assert.match(schema, new RegExp(`CREATE TABLE IF NOT EXISTS publication\\.${table}`));
  }
  for (const table of ["entity_current", "video_current", "content_current", "agent_current"]) {
    assert.match(schema, new RegExp(`CREATE TABLE IF NOT EXISTS result\\.${table}`));
  }
  assert.doesNotMatch(schema, /(?:INSERT|UPDATE|DELETE)\s+(?:INTO|FROM)?\s*public\./i);
  assert.doesNotMatch(schema, /raw_crawler\./i);
  assert.match(schema, /held_shadow/);
});

test("Business Activation schema apply has an independent deployment confirmation", () => {
  const base = {
    BUSINESS_DATABASE_URL: "postgres://business-test",
    CONFIRM_BUSINESS_PUBLICATION_ACTIVATION_SCHEMA_APPLY: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "17",
  };
  assert.throws(
    () => businessPublicationActivationSchemaApplyGuard(base, ["node", "script"]),
    /--apply/,
  );
  assert.deepEqual(
    businessPublicationActivationSchemaApplyGuard(base, ["node", "script", "--apply"]),
    {
      databaseUrl: "postgres://business-test",
      confirmedDatabase: "business_test",
      expectedChannelCount: 17,
    },
  );
});
