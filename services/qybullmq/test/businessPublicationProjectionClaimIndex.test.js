import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  businessPublicationProjectionClaimIndexApplyGuard,
} from "../scripts/applyBusinessPublicationProjectionClaimIndex.mjs";

test("Business Projection Claim index is declared for fresh and live databases", async () => {
  const [schema, migration, packageJson] = await Promise.all([
    readFile(new URL("../src/businessPublicationActivationSchema.sql", import.meta.url), "utf8"),
    readFile(new URL("../scripts/applyBusinessPublicationProjectionClaimIndex.mjs", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);

  for (const source of [schema, migration]) {
    assert.match(source, /idx_business_publication_projection_predecessor/);
    assert.match(source, /channel_id,publication_stream_id,created_at,projection_id/);
  }
  assert.match(migration, /CREATE INDEX CONCURRENTLY/);
  assert.match(migration, /DROP INDEX CONCURRENTLY/);
  assert.match(migration, /ANALYZE publication\.projection_outbox/);
  assert.match(migration, /autovacuum_analyze_scale_factor=0\.02/);
  assert.match(migration, /autovacuum_analyze_threshold=100/);
  assert.match(packageJson, /schema:business-publication-claim-index/);
});

test("live Business Projection Claim index application requires explicit guards", () => {
  const base = {
    BUSINESS_DATABASE_URL: "postgres://business-admin:secret@business/business_test",
    CONFIRM_BUSINESS_PUBLICATION_CLAIM_INDEX_APPLY: "business_test",
    EXPECTED_BUSINESS_PROJECTION_OUTBOX_MIN_ROWS: "100000",
  };
  assert.throws(
    () => businessPublicationProjectionClaimIndexApplyGuard(base, ["node", "script"]),
    /--apply/,
  );
  assert.throws(() => businessPublicationProjectionClaimIndexApplyGuard({
    ...base,
    CONFIRM_BUSINESS_PUBLICATION_CLAIM_INDEX_APPLY: "",
  }, ["node", "script", "--apply"]), /CONFIRM_BUSINESS_PUBLICATION_CLAIM_INDEX_APPLY/);
  assert.throws(() => businessPublicationProjectionClaimIndexApplyGuard({
    ...base,
    EXPECTED_BUSINESS_PROJECTION_OUTBOX_MIN_ROWS: "many",
  }, ["node", "script", "--apply"]), /EXPECTED_BUSINESS_PROJECTION_OUTBOX_MIN_ROWS/);
  assert.deepEqual(
    businessPublicationProjectionClaimIndexApplyGuard(base, ["node", "script", "--apply"]),
    {
      databaseUrl: base.BUSINESS_DATABASE_URL,
      confirmedDatabase: "business_test",
      expectedMinimumOutboxRows: 100000,
    },
  );
});
