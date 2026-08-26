import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { businessPublicationSchemaApplyGuard } from "../scripts/applyBusinessPublicationSchema.mjs";

test("Business Publication schema is isolated from legacy public and raw_crawler models", async () => {
  const schema = await readFile(
    new URL("../src/businessPublicationSchema.sql", import.meta.url),
    "utf8",
  );
  for (const table of ["stream", "channel_ownership", "inbox", "revision", "inbox_conflict", "quarantine"]) {
    assert.match(schema, new RegExp(`CREATE TABLE IF NOT EXISTS publication\\.${table}`));
  }
  assert.doesNotMatch(schema, /(?:INSERT|UPDATE|DELETE)\s+(?:INTO|FROM)?\s*public\./i);
  assert.doesNotMatch(schema, /raw_crawler\./i);
  assert.match(schema, /Business Publication Revision Envelope is immutable/);
  assert.match(schema, /ALTER COLUMN received_envelope DROP NOT NULL/);
  assert.match(schema, /chk_business_publication_inbox_envelope_evidence/);
  assert.match(
    schema,
    /receive_status IN \('rejected',\s*'conflict'\)[\s\S]*received_envelope IS NOT NULL/,
  );
});

test("Business Publication schema apply requires database and row-count confirmation", () => {
  const base = {
    BUSINESS_DATABASE_URL: "postgres://business-test",
    CONFIRM_BUSINESS_PUBLICATION_SCHEMA_APPLY: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "17",
  };
  assert.throws(() => businessPublicationSchemaApplyGuard(base, ["node", "script"]), /--apply/);
  assert.throws(() => businessPublicationSchemaApplyGuard({
    ...base,
    BUSINESS_DATABASE_URL: "",
  }, ["node", "script", "--apply"]), /BUSINESS_DATABASE_URL/);
  for (const invalid of ["17junk", "17.0", "-1", "01", ""]) {
    assert.throws(() => businessPublicationSchemaApplyGuard({
      ...base,
      EXPECTED_BUSINESS_CHANNEL_COUNT: invalid,
    }, ["node", "script", "--apply"]), /EXPECTED_BUSINESS_CHANNEL_COUNT/);
  }
  assert.deepEqual(
    businessPublicationSchemaApplyGuard(base, ["node", "script", "--apply"]),
    {
      databaseUrl: "postgres://business-test",
      confirmedDatabase: "business_test",
      expectedChannelCount: 17,
    },
  );
});
