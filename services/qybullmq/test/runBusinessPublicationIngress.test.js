import assert from "node:assert/strict";
import test from "node:test";
import { businessPublicationIngressRuntimeConfig } from "../src/runBusinessPublicationIngress.js";

function environment(overrides = {}) {
  return {
    BUSINESS_DATABASE_URL: "postgres://business-test",
    BUSINESS_PUBLICATION_INGRESS_TOKEN: "test-token",
    ...overrides,
  };
}

test("Business Ingress runtime validates all settings before opening PostgreSQL", () => {
  assert.deepEqual(businessPublicationIngressRuntimeConfig(environment()), {
    databaseUrl: "postgres://business-test",
    token: "test-token",
    expectedDatabase: "yewu_business",
    tls: null,
    host: "127.0.0.1",
    port: 8081,
    poolMaximum: 10,
    maximumBodyBytes: 4 * 1024 * 1024,
  });
  for (const [name, value] of [
    ["BUSINESS_PUBLICATION_INGRESS_PORT", "0"],
    ["BUSINESS_PUBLICATION_INGRESS_PORT", "8081junk"],
    ["BUSINESS_INGRESS_POSTGRES_POOL_MAX", "101"],
    ["BUSINESS_PUBLICATION_MAX_BODY_BYTES", "1023"],
  ]) {
    assert.throws(
      () => businessPublicationIngressRuntimeConfig(environment({ [name]: value })),
      new RegExp(name),
    );
  }
  assert.throws(
    () => businessPublicationIngressRuntimeConfig(environment({
      BUSINESS_PUBLICATION_INGRESS_TOKEN: "",
    })),
    /INGRESS_TOKEN/,
  );
  assert.throws(
    () => businessPublicationIngressRuntimeConfig(environment({
      BUSINESS_PUBLICATION_TLS_CERT: "certificate-only",
    })),
    /must be set together/,
  );
  assert.deepEqual(
    businessPublicationIngressRuntimeConfig(environment({
      BUSINESS_PUBLICATION_TLS_CERT: "certificate",
      BUSINESS_PUBLICATION_TLS_KEY: "private-key",
    })).tls,
    { cert: "certificate", key: "private-key" },
  );
});
