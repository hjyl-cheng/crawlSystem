import assert from "node:assert/strict";
import test from "node:test";
import {
  businessPublicationReconcilerRuntimeConfig,
} from "../src/runBusinessPublicationReconciler.js";

function environment(overrides = {}) {
  return {
    BUSINESS_DATABASE_URL: "postgres://business-test",
    BUSINESS_PUBLICATION_RECONCILER_ID: "reconciler-test",
    ...overrides,
  };
}

test("Business Reconciler runtime validates bounded settings before PostgreSQL", () => {
  assert.deepEqual(businessPublicationReconcilerRuntimeConfig(environment()), {
    databaseUrl: "postgres://business-test",
    expectedDatabase: "yewu_business",
    workerId: "reconciler-test",
    pollMs: 1000,
    poolMaximum: 12,
    batchSize: 100,
    concurrency: 4,
    leaseSeconds: 120,
    blockedRetrySeconds: 30,
    errorRetrySeconds: 10,
    maximumErrorRetrySeconds: 300,
    gapAlertSeconds: 900,
    projectionStuckSeconds: 300,
    auditIntervalSeconds: 300,
    auditSampleSize: 10,
  });
  for (const [name, value] of [
    ["BUSINESS_PUBLICATION_RECONCILE_POLL_MS", "99"],
    ["BUSINESS_PUBLICATION_RECONCILE_BATCH_SIZE", "0"],
    ["BUSINESS_PUBLICATION_RECONCILE_CONCURRENCY", "65"],
    ["BUSINESS_PUBLICATION_RECONCILE_LEASE_SECONDS", "4"],
    ["BUSINESS_PUBLICATION_GAP_ALERT_SECONDS", "1.5"],
    ["BUSINESS_PUBLICATION_RECONCILE_AUDIT_SAMPLE_SIZE", "101"],
  ]) {
    assert.throws(
      () => businessPublicationReconcilerRuntimeConfig(environment({ [name]: value })),
      new RegExp(name),
    );
  }
  assert.throws(
    () => businessPublicationReconcilerRuntimeConfig(environment({
      BUSINESS_PUBLICATION_RECONCILE_ERROR_RETRY_SECONDS: "20",
      BUSINESS_PUBLICATION_RECONCILE_MAX_ERROR_RETRY_SECONDS: "10",
    })),
    /MAX_ERROR_RETRY_SECONDS/,
  );
});
