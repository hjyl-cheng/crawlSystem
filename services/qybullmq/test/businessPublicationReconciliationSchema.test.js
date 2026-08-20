import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  businessPublicationReconciliationSchemaApplyGuard,
} from "../scripts/applyBusinessPublicationReconciliationSchema.mjs";

test("Business Reconciliation schema adds only restartable control state", async () => {
  const schema = await readFile(
    new URL("../src/businessPublicationReconciliationSchema.sql", import.meta.url),
    "utf8",
  );
  assert.match(schema, /CREATE TABLE IF NOT EXISTS publication\.reconciliation_state/);
  assert.match(schema, /lease_owner TEXT/);
  assert.match(schema, /lease_expires_at TIMESTAMPTZ/);
  assert.match(schema, /next_attempt_at TIMESTAMPTZ/);
  assert.doesNotMatch(schema, /(?:INSERT|UPDATE|DELETE)\s+(?:INTO|FROM)?\s*public\./i);
  assert.doesNotMatch(schema, /(?:CREATE|ALTER|INSERT|UPDATE|DELETE).*result\./i);
  assert.doesNotMatch(schema, /raw_crawler\./i);
});

test("Business Reconciliation schema apply has an independent confirmation", () => {
  const base = {
    BUSINESS_DATABASE_URL: "postgres://business-test",
    CONFIRM_BUSINESS_PUBLICATION_RECONCILIATION_SCHEMA_APPLY: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "17",
  };
  assert.throws(
    () => businessPublicationReconciliationSchemaApplyGuard(base, ["node", "script"]),
    /--apply/,
  );
  assert.deepEqual(
    businessPublicationReconciliationSchemaApplyGuard(base, ["node", "script", "--apply"]),
    {
      databaseUrl: "postgres://business-test",
      confirmedDatabase: "business_test",
      expectedChannelCount: 17,
    },
  );
});
