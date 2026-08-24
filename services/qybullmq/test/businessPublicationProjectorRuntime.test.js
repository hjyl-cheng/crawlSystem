import assert from "node:assert/strict";
import test from "node:test";
import {
  assertBusinessPublicationDatabase,
  businessPublicationProjectorRuntimeConfig,
} from "../src/runBusinessPublicationProjector.js";

function businessPreflight(overrides = {}) {
  return {
    database_name: "business_test",
    role_name: "business_publication_projector",
    projection_schema_ready: true,
    outbox_ready: true,
    predecessor_index_ready: true,
    current_ready: true,
    search_live_ready: true,
    search_storage_state_ready: true,
    search_release_ready: true,
    search_legacy_restore_ready: true,
    content_type_taxonomy_ready: true,
    ...overrides,
  };
}

test("Projector runtime requires its fixed role and accepts file-backed credentials", () => {
  const config = businessPublicationProjectorRuntimeConfig({
    BUSINESS_DATABASE_URL: "postgres://business_publication_projector:secret@business/business_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    BUSINESS_PUBLICATION_PROJECT_BATCH_SIZE: "30",
  });
  assert.equal(config.expectedRole, "business_publication_projector");
  assert.equal(config.batchSize, 30);
  assert.equal(config.poolMaximum, 4);
  assert.equal(config.claimStatementTimeoutMs, 15000);
});

test("Projector runtime validates the Claim statement timeout independently", () => {
  const base = {
    BUSINESS_DATABASE_URL: "postgres://business_publication_projector:secret@business/business_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
  };
  assert.equal(businessPublicationProjectorRuntimeConfig({
    ...base,
    BUSINESS_PUBLICATION_PROJECT_CLAIM_TIMEOUT_MS: "30000",
  }).claimStatementTimeoutMs, 30000);
  assert.throws(() => businessPublicationProjectorRuntimeConfig({
    ...base,
    BUSINESS_PUBLICATION_PROJECT_CLAIM_TIMEOUT_MS: "999",
  }), /BUSINESS_PUBLICATION_PROJECT_CLAIM_TIMEOUT_MS/);
});

test("Projector runtime rejects inverted retry limits", () => {
  assert.throws(() => businessPublicationProjectorRuntimeConfig({
    BUSINESS_DATABASE_URL: "postgres://business_publication_projector:secret@business/business_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    BUSINESS_PUBLICATION_PROJECT_RETRY_SECONDS: "60",
    BUSINESS_PUBLICATION_PROJECT_MAX_RETRY_SECONDS: "10",
  }), /must not be smaller/);
});

test("Projector refuses to start without the complete content taxonomy", async () => {
  const config = {
    expectedDatabase: "business_test",
    expectedRole: "business_publication_projector",
  };
  await assert.rejects(
    assertBusinessPublicationDatabase({
      query: async () => ({ rows: [businessPreflight({
        content_type_taxonomy_ready: false,
      })] }),
    }, config),
    /refusing to project/,
  );
  await assert.doesNotReject(assertBusinessPublicationDatabase({
    query: async () => ({ rows: [businessPreflight()] }),
  }, config));
});
