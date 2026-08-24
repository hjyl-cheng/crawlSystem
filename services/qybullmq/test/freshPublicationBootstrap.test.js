import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import {
  freshPublicationBootstrapConfirmation,
  freshPublicationBootstrapConfig,
  validateFreshPublicationBootstrapState,
} from "../src/freshPublicationBootstrap.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";
const WRITER_DIGEST = `sha256:${"1".repeat(64)}`;
const RUNTIME_DIGEST = `sha256:${"2".repeat(64)}`;

function environment(overrides = {}) {
  return {
    CRAWLER_ADMIN_DATABASE_URL: "postgresql://crawler_admin:secret@crawler:5432/crawler_test",
    BUSINESS_ADMIN_DATABASE_URL: "postgresql://business_admin:secret@business:5432/business_test",
    EXPECTED_CRAWLER_DATABASE: "crawler_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "0",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "0",
    PUBLICATION_STREAM_ID: STREAM_ID,
    PUBLICATION_SOURCE_DEPLOYMENT_KEY: "newcrawler-fresh-test-v1",
    PUBLICATION_SOURCE_IDENTITY_JSON: JSON.stringify({
      database: "crawler_test",
      deployment: "newcrawler-fresh-test-v1",
    }),
    PUBLICATION_DESTINATION: "business",
    PUBLICATION_BOOTSTRAP_PROJECTION_MODE: "online",
    PUBLICATION_OPERATOR: "fresh-publication-test",
    PUBLICATION_ACTION_REASON: "isolated first Channel test",
    PUBLICATION_WRITER_DEPLOYMENT_REF: WRITER_DIGEST,
    PUBLICATION_RUNTIME_DEPLOYMENT_REF: RUNTIME_DIGEST,
    ...overrides,
  };
}

function counts(overrides = {}) {
  return {
    channels: 0,
    candidates: 0,
    runs: 0,
    channel_ownerships: 0,
    deliveries: 0,
    domain_currents: 0,
    revisions: 0,
    outbox: 0,
    ...overrides,
  };
}

function businessCounts(overrides = {}) {
  return {
    channels: 0,
    channel_ownerships: 0,
    inbox: 0,
    revisions: 0,
    activations: 0,
    projection_outbox: 0,
    entity_current: 0,
    video_current: 0,
    agent_current: 0,
    snapshots: 0,
    search_current: 0,
    ...overrides,
  };
}

function state(config, phase = "empty") {
  const sourceStream = {
    publication_stream_id: config.streamId,
    source_deployment_key: config.sourceDeploymentKey,
    source_identity_json: config.sourceIdentity,
    status: "active",
    minimum_writer_version: PUBLICATION_WRITER_VERSION,
    capture_enabled_at: "2026-08-24T01:00:00.000Z",
    automatic_onboarding_destination: config.destination,
  };
  const businessStream = {
    publication_stream_id: config.streamId,
    source_deployment_key: config.sourceDeploymentKey,
    source_identity_json: config.sourceIdentity,
    status: "active",
    accepted_contract_versions: [1, 2],
    automatic_onboarding_projection_mode: config.projectionMode,
  };
  return {
    source: {
      streams: phase === "complete" ? [sourceStream] : [],
      counts: counts(),
    },
    business: {
      streams: new Set(["business_committed", "complete"]).has(phase)
        ? [businessStream]
        : [],
      counts: businessCounts(),
    },
  };
}

test("fresh Publication bootstrap config pins distinct non-legacy empty databases and immutable releases", () => {
  const config = freshPublicationBootstrapConfig(environment());

  assert.equal(config.expectedCrawlerDatabase, "crawler_test");
  assert.equal(config.expectedBusinessDatabase, "business_test");
  assert.equal(config.streamId, STREAM_ID);
  assert.equal(config.destination, "business");
  assert.equal(config.projectionMode, "online");
  assert.equal(config.writerDeploymentRef, WRITER_DIGEST);
  assert.equal(config.runtimeDeploymentRef, RUNTIME_DIGEST);
  assert.equal(config.sourceIdentity.database, "crawler_test");

  assert.throws(
    () => freshPublicationBootstrapConfig(environment({
      EXPECTED_CRAWLER_DATABASE: "bullmq_crawler_migration",
      PUBLICATION_SOURCE_IDENTITY_JSON: JSON.stringify({ database: "bullmq_crawler_migration" }),
    })),
    /forbidden Crawler database/,
  );
  assert.throws(
    () => freshPublicationBootstrapConfig(environment({
      EXPECTED_BUSINESS_DATABASE: "yewu_business",
    })),
    /forbidden Business database/,
  );
  assert.throws(
    () => freshPublicationBootstrapConfig(environment({ EXPECTED_CRAWLER_CHANNEL_COUNT: "1" })),
    /must be exactly 0/,
  );
});

test("fresh Publication bootstrap confirmation binds both databases, policy, identity, and releases", () => {
  const config = freshPublicationBootstrapConfig(environment());
  const confirmation = freshPublicationBootstrapConfirmation(config);
  const identityHash = `sha256:${createHash("sha256")
    .update(JSON.stringify(config.sourceIdentity))
    .digest("hex")}`;

  assert.equal(
    confirmation,
    [
      "BOOTSTRAP_FRESH_PUBLICATION",
      "crawler_test",
      "business_test",
      STREAM_ID,
      "newcrawler-fresh-test-v1",
      "business",
      "online",
      identityHash,
      WRITER_DIGEST,
      RUNTIME_DIGEST,
    ].join(":"),
  );
});

test("fresh Publication bootstrap accepts only empty, Business-committed, or complete exact states", () => {
  const config = freshPublicationBootstrapConfig(environment());

  assert.equal(validateFreshPublicationBootstrapState(state(config, "empty"), config), "empty");
  assert.equal(
    validateFreshPublicationBootstrapState(state(config, "business_committed"), config),
    "business_committed",
  );
  assert.equal(validateFreshPublicationBootstrapState(state(config, "complete"), config), "complete");

  const dirty = state(config, "empty");
  dirty.source.counts.channels = 1;
  assert.throws(
    () => validateFreshPublicationBootstrapState(dirty, config),
    /Crawler channels must be exactly 0/,
  );

  const crawlerOnly = state(config, "complete");
  crawlerOnly.business.streams = [];
  assert.throws(
    () => validateFreshPublicationBootstrapState(crawlerOnly, config),
    /Crawler Stream exists before the Business Stream/,
  );

  const divergent = state(config, "business_committed");
  divergent.business.streams[0].automatic_onboarding_projection_mode = "held_shadow";
  assert.throws(
    () => validateFreshPublicationBootstrapState(divergent, config),
    /Business automatic onboarding projection mode mismatch/,
  );
});
