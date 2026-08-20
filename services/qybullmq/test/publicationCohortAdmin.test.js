import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { publicationCohortCommand } from "../scripts/managePublicationCohort.mjs";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  publicationPayloadFixture,
  publicationPayloadResultHash,
  publicationPolicyVersionFixture,
} from "./support/publicationPayloadFixtures.js";
import {
  assertPublicationDeliveryReleaseState,
  assertPublicationCohortState,
  buildPublicationCohort,
  publicationCohortConfirmation,
  publicationCohortRuntimeConfig,
  readPublicationChannelSet,
  readPublicationReadinessEvidence,
} from "../src/publicationCohortAdmin.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const RUNTIME_DIGEST = `sha256:${"b".repeat(64)}`;

function hash(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function environment(overrides = {}) {
  return {
    CRAWLER_DATABASE_URL: "postgres://crawler-test",
    BUSINESS_DATABASE_URL: "postgres://business-test",
    EXPECTED_CRAWLER_DATABASE: "crawler_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "17",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "19",
    PUBLICATION_STREAM_ID: STREAM_ID,
    PUBLICATION_SOURCE_DEPLOYMENT_KEY: "crawler-test-canary-1",
    PUBLICATION_SOURCE_IDENTITY_JSON: JSON.stringify({
      database: "crawler_test",
      deployment: "canary-1",
    }),
    PUBLICATION_COHORT_KEY: "canary-001",
    PUBLICATION_CHANNEL_IDS_FILE: "/tmp/channels.txt",
    PUBLICATION_DESTINATION: "business",
    PUBLICATION_OPERATOR: "publication-test",
    PUBLICATION_ACTION_REASON: "controlled test",
    ...overrides,
  };
}

function fixture(command = "init") {
  const config = publicationCohortRuntimeConfig(environment({
    ...(command === "enable-capture"
      ? { PUBLICATION_WRITER_DEPLOYMENT_REF: IMAGE_DIGEST }
      : {}),
    ...(command === "release-delivery"
      ? {
          PUBLICATION_RUNTIME_DEPLOYMENT_REF: RUNTIME_DIGEST,
          PUBLICATION_READINESS_REPORT_FILE: "/tmp/readiness.json",
        }
      : {}),
  }), { command });
  const cohort = buildPublicationCohort(config, ["UC-alpha", "UC-beta"]);
  return { config, cohort };
}

function dataCounts(names) {
  return Object.fromEntries(names.map((name) => [name, 0]));
}

function releaseReason(config, cohort, evidence) {
  return JSON.stringify({
    action: "release_publication_delivery",
    cohort_key: cohort.cohortKey,
    channel_set_hash: cohort.channelSetHash,
    readiness_evidence_hash: evidence.evidence_hash,
    readiness_report_as_of: evidence.report_as_of,
    runtime_deployment_ref: config.runtimeDeploymentRef,
    operator_reason: config.reason,
  });
}

function emptyState() {
  return {
    source: {
      streams: [],
      ownerships: [],
      deliveries: [],
      dataCounts: dataCounts(["domain_current", "revision", "outbox"]),
    },
    business: {
      streams: [],
      ownerships: [],
      dataCounts: dataCounts(["inbox", "revision", "activation", "consumer_cursor"]),
    },
  };
}

function initializedState(config, cohort) {
  return {
    source: {
      streams: [{
        publication_stream_id: config.streamId,
        source_deployment_key: config.sourceDeploymentKey,
        source_identity_json: config.sourceIdentity,
        status: "active",
        minimum_writer_version: null,
        capture_enabled_at: null,
      }],
      ownerships: cohort.channelIds.map((channelId) => ({
        publication_stream_id: config.streamId,
        channel_id: channelId,
        status: "owned",
        onboarding_mode: "bootstrap",
        seed_status: "pending",
        ownership_reference: cohort.ownershipReference,
      })),
      deliveries: cohort.channelIds.map((channelId) => ({
        publication_stream_id: config.streamId,
        channel_id: channelId,
        destination: config.destination,
        mode: "hold",
        source_ownership_reference: cohort.ownershipReference,
        latest_successful_baseline_id: null,
        channel_watermark_sequence: null,
        video_watermark_sequence: null,
        agent_watermark_sequence: null,
        online_at: null,
        sealed_at: null,
      })),
      dataCounts: dataCounts(["domain_current", "revision", "outbox"]),
      domainCurrents: [],
      revisions: [],
      outbox: [],
    },
    business: {
      streams: [{
        publication_stream_id: config.streamId,
        source_deployment_key: config.sourceDeploymentKey,
        source_identity_json: config.sourceIdentity,
        status: "active",
        accepted_contract_versions: [1, 2],
      }],
      ownerships: cohort.channelIds.map((channelId) => ({
        channel_id: channelId,
        active_publication_stream_id: config.streamId,
        status: "active",
        previous_publication_stream_id: null,
        ownership_reference: cohort.ownershipReference,
        projection_mode: "held_shadow",
      })),
      dataCounts: dataCounts(["inbox", "revision", "activation", "consumer_cursor"]),
      targetDataCounts: dataCounts([
        "inbox",
        "revision",
        "activation",
        "activation_item",
        "consumer_cursor",
        "projection_outbox",
        "reconciliation_state",
        "inbox_conflict",
        "open_quarantine",
        "entity_current",
        "video_current",
        "content_current",
        "agent_current",
      ]),
    },
  };
}

function releaseFixture({ mode = "hold" } = {}) {
  const { config, cohort } = fixture("release-delivery");
  const state = initializedState(config, cohort);
  state.source.streams[0].minimum_writer_version = config.writerVersion;
  state.source.streams[0].capture_enabled_at = "2026-07-27T09:18:40.222Z";
  for (const ownership of state.source.ownerships) ownership.seed_status = "complete";
  for (const delivery of state.source.deliveries) {
    delivery.mode = mode;
    delivery.online_at = mode === "online" ? "2026-07-27T09:30:00.000Z" : null;
    delivery.state_changed_at = delivery.online_at;
  }
  const evidence = {
    report_version: "publication-readiness-report-v1",
    report_as_of: "2026-07-27T09:02:29.741Z",
    evidence_hash: hash("release-evidence"),
    channels: [],
  };
  let revisionNumber = 1;
  for (const channelId of cohort.channelIds) {
    const resultHashes = {};
    for (const domain of ["channel", "video", "agent"]) {
      const payload = publicationPayloadFixture(domain, channelId);
      const resultHash = publicationPayloadResultHash(domain, payload);
      const policyVersion = publicationPolicyVersionFixture(domain);
      const revisionId = `11111111-1111-4111-8${String(revisionNumber).padStart(3, "0")}-111111111111`;
      revisionNumber += 1;
      resultHashes[domain] = resultHash;
      state.source.domainCurrents.push({
        channel_id: channelId,
        domain,
        contract_version: 1,
        policy_version: policyVersion,
        readiness_status: "ready",
        result_hash: resultHash,
        data_sequence: "1",
        current_revision_id: revisionId,
      });
      state.source.revisions.push({
        revision_id: revisionId,
        publication_stream_id: config.streamId,
        channel_id: channelId,
        domain,
        data_sequence: "1",
        previous_data_sequence: null,
        revision_type: "bootstrap",
        operation: domain === "video" ? "replace_window" : "replace",
        contract_version: 1,
        policy_version: policyVersion,
        occurred_at: "2026-07-27T09:00:00.000Z",
        source_refs: {},
        previous_result_hash: null,
        result_hash: resultHash,
        payload_hash: observationFactsHash(payload),
        payload_json: payload,
      });
      state.source.outbox.push({
        destination: config.destination,
        revision_id: revisionId,
        channel_id: channelId,
        domain,
        data_sequence: "1",
        status: mode === "hold" ? "held" : "pending",
        attempts: 0,
        lease_owner: null,
        receipt_id: null,
        delivered_at: null,
      });
    }
    evidence.channels.push({ channel_id: channelId, result_hashes: resultHashes });
  }
  state.source.dataCounts = {
    domain_current: state.source.domainCurrents.length,
    revision: state.source.revisions.length,
    outbox: state.source.outbox.length,
  };
  if (mode === "online") {
    for (const delivery of state.source.deliveries) {
      delivery.state_reason = releaseReason(config, cohort, evidence);
    }
  }
  return { config, cohort, state, evidence };
}

test("Publication cohort runtime config requires explicit, distinct database identities", () => {
  const config = publicationCohortRuntimeConfig(environment());
  assert.equal(config.streamId, STREAM_ID);
  assert.equal(config.expectedCrawlerChannelCount, 17);
  assert.equal(config.expectedBusinessChannelCount, 19);
  assert.equal(config.writerDeploymentRef, null);

  for (const invalid of ["", "17junk", "17.0", "-1", "01"]) {
    assert.throws(() => publicationCohortRuntimeConfig(environment({
      EXPECTED_CRAWLER_CHANNEL_COUNT: invalid,
    })), /EXPECTED_CRAWLER_CHANNEL_COUNT/);
  }
  assert.throws(() => publicationCohortRuntimeConfig(environment({
    EXPECTED_BUSINESS_DATABASE: "crawler_test",
  })), /must be different/);
  assert.throws(() => publicationCohortRuntimeConfig(environment({
    PUBLICATION_STREAM_ID: "not-a-uuid",
  })), /must be a UUID/);
  assert.throws(() => publicationCohortRuntimeConfig(environment({
    PUBLICATION_SOURCE_IDENTITY_JSON: "[]",
  })), /JSON object/);
});

test("Capture activation requires an immutable deployed image digest", () => {
  assert.throws(() => publicationCohortRuntimeConfig(environment(), {
    command: "enable-capture",
  }), /PUBLICATION_WRITER_DEPLOYMENT_REF/);
  assert.throws(() => publicationCohortRuntimeConfig(environment({
    PUBLICATION_WRITER_DEPLOYMENT_REF: "latest",
  }), { command: "enable-capture" }), /immutable sha256 image digest/);
  const config = publicationCohortRuntimeConfig(environment({
    PUBLICATION_WRITER_DEPLOYMENT_REF: IMAGE_DIGEST,
  }), { command: "enable-capture" });
  assert.equal(config.writerVersion, "publication-reconciler-v1");
  assert.equal(config.writerDeploymentRef, IMAGE_DIGEST);
});

test("Delivery release requires an immutable Runtime image and Readiness evidence file", () => {
  assert.throws(() => publicationCohortRuntimeConfig(environment(), {
    command: "release-delivery",
  }), /PUBLICATION_RUNTIME_DEPLOYMENT_REF/);
  assert.throws(() => publicationCohortRuntimeConfig(environment({
    PUBLICATION_RUNTIME_DEPLOYMENT_REF: "latest",
    PUBLICATION_READINESS_REPORT_FILE: "/tmp/readiness.json",
  }), { command: "release-delivery" }), /immutable sha256 image digest/);
  const { config } = fixture("release-delivery");
  assert.equal(config.runtimeDeploymentRef, RUNTIME_DIGEST);
  assert.equal(config.readinessReportFile, "/tmp/readiness.json");
});

test("Channel cohort files are explicit, canonical, and reject duplicates", async () => {
  const directory = await mkdtemp(join(tmpdir(), "publication-cohort-"));
  try {
    const valid = join(directory, "valid.txt");
    await writeFile(valid, "# canary\nUC-beta\n\nUC-alpha\r\n", "utf8");
    assert.deepEqual(await readPublicationChannelSet(valid), ["UC-alpha", "UC-beta"]);

    const duplicate = join(directory, "duplicate.txt");
    await writeFile(duplicate, "UC-alpha\nUC-alpha\n", "utf8");
    await assert.rejects(readPublicationChannelSet(duplicate), /duplicate Channel ID/);

    const empty = join(directory, "empty.txt");
    await writeFile(empty, "# no implicit all-channel mode\n", "utf8");
    await assert.rejects(readPublicationChannelSet(empty), /at least one Channel ID/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Readiness evidence is exact, passing, and bound to the explicit Channel set", async () => {
  const { cohort } = fixture("release-delivery");
  const directory = await mkdtemp(join(tmpdir(), "publication-readiness-evidence-"));
  const reportPath = join(directory, "readiness.json");
  const report = {
    report_version: "publication-readiness-report-v1",
    report_as_of: "2026-07-27T09:02:29.741Z",
    contract_version: 1,
    policies: {
      publication: "publication-policy-v1",
      video_window: "video-window-v1",
      agent_taxonomy: "qy-taxonomy-v1",
    },
    scope: {
      requested_channel_count: cohort.channelCount,
      crawler_channel_count: cohort.channelCount,
      business_active_channel_count: cohort.channelCount,
      source_only_channel_count: 0,
      target_only_channel_count: 0,
    },
    summary: {
      total_channels: cohort.channelCount,
      baseline_eligible: cohort.channelCount,
      not_ready: 0,
      channel_ready: cohort.channelCount,
      video_ready: cohort.channelCount,
      agent_ready: cohort.channelCount,
      business_target_channels: cohort.channelCount,
      business_regression_passed: cohort.channelCount,
      business_regression_not_applicable: 0,
    },
    channels: [...cohort.channelIds].reverse().map((channelId) => ({
      channel_id: channelId,
      baseline_eligible: true,
      readiness_reasons: [],
      business_regression: {
        target_exists: true,
        passed: true,
        regressions: [],
      },
      domains: Object.fromEntries(["channel", "video", "agent"].map((domain) => [
        domain,
        {
          ready: true,
          contract_version: 1,
          policy_version: publicationPolicyVersionFixture(domain),
          result_hash: hash(`${channelId}:${domain}`),
        },
      ])),
    })),
  };
  try {
    await writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
    const evidence = await readPublicationReadinessEvidence(reportPath, cohort);
    assert.equal(evidence.report_as_of, report.report_as_of);
    assert.match(evidence.evidence_hash, /^sha256:[0-9a-f]{64}$/);
    assert.deepEqual(evidence.channels.map((row) => row.channel_id), cohort.channelIds);

    report.summary.not_ready = 1;
    await writeFile(reportPath, JSON.stringify(report), "utf8");
    await assert.rejects(
      readPublicationReadinessEvidence(reportPath, cohort),
      /not_ready mismatch/,
    );

    report.summary.not_ready = 0;
    report.channels[0].baseline_eligible = false;
    await writeFile(reportPath, JSON.stringify(report), "utf8");
    await assert.rejects(
      readPublicationReadinessEvidence(reportPath, cohort),
      /not baseline eligible/,
    );

    report.channels[0].baseline_eligible = true;
    report.channels[0].business_regression.passed = false;
    await writeFile(reportPath, JSON.stringify(report), "utf8");
    await assert.rejects(
      readPublicationReadinessEvidence(reportPath, cohort),
      /Business regression did not pass/,
    );

    report.channels[0].business_regression.passed = true;
    report.channels[0].domains.channel.contract_version = 2;
    await writeFile(reportPath, JSON.stringify(report), "utf8");
    await assert.rejects(
      readPublicationReadinessEvidence(reportPath, cohort),
      /contract version/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Confirmation binds the action to both databases, the exact cohort, and deployed writer", () => {
  const initialized = fixture("init");
  const initConfirmation = publicationCohortConfirmation(
    "init",
    initialized.config,
    initialized.cohort,
  );
  assert.ok(initConfirmation.startsWith("INITIALIZE_PUBLICATION_COHORT:"));
  assert.ok(initConfirmation.includes("crawler_test:business_test"));
  assert.ok(initConfirmation.includes(initialized.cohort.channelSetHash));

  const enabling = fixture("enable-capture");
  const captureConfirmation = publicationCohortConfirmation(
    "enable-capture",
    enabling.config,
    enabling.cohort,
  );
  assert.ok(captureConfirmation.startsWith("ENABLE_PUBLICATION_CAPTURE:"));
  assert.ok(captureConfirmation.includes("publication-reconciler-v1"));
  assert.ok(!captureConfirmation.includes(IMAGE_DIGEST));
  assert.notEqual(captureConfirmation, initConfirmation);

  const releasing = releaseFixture();
  const releaseConfirmation = publicationCohortConfirmation(
    "release-delivery",
    releasing.config,
    releasing.cohort,
    releasing.evidence,
  );
  assert.ok(releaseConfirmation.startsWith("RELEASE_PUBLICATION_DELIVERY:"));
  assert.ok(releaseConfirmation.includes(releasing.evidence.evidence_hash));
  assert.ok(!releaseConfirmation.includes(RUNTIME_DIGEST));
  assert.notEqual(releaseConfirmation, captureConfirmation);
});

test("Initialization state permits a clean plan and requires exact Bootstrap Shadow/Hold state", () => {
  const { config, cohort } = fixture();
  assert.doesNotThrow(() => assertPublicationCohortState(
    emptyState(),
    config,
    cohort,
    { complete: false, capture: "disabled", requireEmpty: true },
  ));
  const state = initializedState(config, cohort);
  assert.doesNotThrow(() => assertPublicationCohortState(
    state,
    config,
    cohort,
    { complete: true, capture: "disabled", requireEmpty: true },
  ));

  state.business.ownerships[0].projection_mode = "online";
  assert.throws(() => assertPublicationCohortState(
    state,
    config,
    cohort,
    { complete: true, capture: "disabled", requireEmpty: true },
  ), /projection mode/);
});

test("Cohort validation rejects ownership collisions and Channels outside the file", () => {
  const { config, cohort } = fixture();
  const collision = emptyState();
  collision.source.ownerships.push({
    publication_stream_id: "22222222-2222-4222-8222-222222222222",
    channel_id: cohort.channelIds[0],
    status: "owned",
  });
  assert.throws(() => assertPublicationCohortState(collision, config, cohort, {
    complete: false,
    capture: "disabled",
    requireEmpty: true,
  }), /owned by another active Stream/);

  const extra = initializedState(config, cohort);
  extra.source.ownerships.push({
    ...extra.source.ownerships[0],
    channel_id: "UC-not-in-file",
  });
  assert.throws(() => assertPublicationCohortState(extra, config, cohort, {
    complete: true,
    capture: "disabled",
    requireEmpty: true,
  }), /outside the explicit cohort/);
});

test("Capture can only be enabled for the complete held cohort and is idempotently valid afterward", () => {
  const { config, cohort } = fixture("enable-capture");
  const state = initializedState(config, cohort);
  assert.doesNotThrow(() => assertPublicationCohortState(state, config, cohort, {
    complete: true,
    capture: "enableable",
    requireEmpty: true,
  }));

  state.source.streams[0].minimum_writer_version = config.writerVersion;
  state.source.streams[0].capture_enabled_at = new Date("2026-07-27T00:00:00.000Z");
  state.source.dataCounts.revision = 3;
  state.business.dataCounts.inbox = 3;
  assert.doesNotThrow(() => assertPublicationCohortState(state, config, cohort, {
    complete: true,
    capture: "enabled",
    requireEmpty: false,
  }));

  state.source.streams[0].minimum_writer_version = "legacy-writer-v1";
  assert.throws(() => assertPublicationCohortState(state, config, cohort, {
    complete: true,
    capture: "enableable",
    requireEmpty: false,
  }), /writer version/);
});

test("Delivery release requires complete Bootstrap chains and exact Readiness hashes", () => {
  const release = releaseFixture();
  assert.equal(assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    release.evidence,
  ), "releaseable");

  const missingDomain = structuredClone(release.state);
  missingDomain.source.domainCurrents.pop();
  assert.throws(() => assertPublicationDeliveryReleaseState(
    missingDomain,
    release.config,
    release.cohort,
    release.evidence,
  ), /exactly one Domain Current/);

  const wrongHash = structuredClone(release.evidence);
  wrongHash.channels[0].result_hashes.channel = hash("different");
  assert.throws(() => assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    wrongHash,
  ), /Readiness Hash/);

  const businessAlreadyWritten = structuredClone(release.state);
  businessAlreadyWritten.business.targetDataCounts.inbox = 1;
  assert.throws(() => assertPublicationDeliveryReleaseState(
    businessAlreadyWritten,
    release.config,
    release.cohort,
    release.evidence,
  ), /must be empty before Delivery release/);

  const unsupportedContract = structuredClone(release.state);
  unsupportedContract.source.revisions[0].contract_version = 3;
  assert.throws(() => assertPublicationDeliveryReleaseState(
    unsupportedContract,
    release.config,
    release.cohort,
    release.evidence,
  ), /unsupported contract_version/);

  const corruptPayload = structuredClone(release.state);
  corruptPayload.source.revisions[0].payload_json.domain = "corrupt";
  assert.throws(() => assertPublicationDeliveryReleaseState(
    corruptPayload,
    release.config,
    release.cohort,
    release.evidence,
  ), /payload_hash does not match/);

  const invalidBusinessPayload = releaseFixture();
  const channelRevision = invalidBusinessPayload.state.source.revisions.find(
    (row) => row.domain === "channel",
  );
  delete channelRevision.payload_json.links;
  const invalidResultHash = observationFactsHash(channelRevision.payload_json);
  channelRevision.payload_hash = invalidResultHash;
  channelRevision.result_hash = invalidResultHash;
  const channelCurrent = invalidBusinessPayload.state.source.domainCurrents.find(
    (row) => row.revision_id === channelRevision.revision_id
      || row.current_revision_id === channelRevision.revision_id,
  );
  channelCurrent.result_hash = invalidResultHash;
  invalidBusinessPayload.evidence.channels.find(
    (row) => row.channel_id === channelRevision.channel_id,
  ).result_hashes.channel = invalidResultHash;
  assert.throws(() => assertPublicationDeliveryReleaseState(
    invalidBusinessPayload.state,
    invalidBusinessPayload.config,
    invalidBusinessPayload.cohort,
    invalidBusinessPayload.evidence,
  ), /keys differ from Contract V1/);
});

test("Delivery release binds the approved Current even after held incremental Revisions", () => {
  const release = releaseFixture();
  const channelId = release.cohort.channelIds[0];
  const domain = "channel";
  const current = release.state.source.domainCurrents.find((row) => (
    row.channel_id === channelId && row.domain === domain
  ));
  const first = release.state.source.revisions.find((row) => row.revision_id === current.current_revision_id);
  const revisionId = "22222222-2222-4222-8222-222222222222";
  const payload = publicationPayloadFixture(domain, channelId);
  payload.title = "Held incremental Current";
  const resultHash = publicationPayloadResultHash(domain, payload);
  release.state.source.revisions.push({
    revision_id: revisionId,
    publication_stream_id: release.config.streamId,
    channel_id: channelId,
    domain,
    data_sequence: "2",
    previous_data_sequence: "1",
    revision_type: "incremental",
    operation: "replace",
    contract_version: 1,
    policy_version: "publication-policy-v1",
    occurred_at: "2026-07-27T09:01:00.000Z",
    source_refs: {},
    previous_result_hash: first.result_hash,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
  release.state.source.outbox.push({
    destination: release.config.destination,
    revision_id: revisionId,
    channel_id: channelId,
    domain,
    data_sequence: "2",
    status: "held",
    attempts: 0,
    lease_owner: null,
    receipt_id: null,
    delivered_at: null,
  });
  current.data_sequence = "2";
  current.current_revision_id = revisionId;
  current.result_hash = resultHash;
  release.evidence.channels.find((row) => row.channel_id === channelId)
    .result_hashes[domain] = resultHash;
  release.state.source.dataCounts.revision += 1;
  release.state.source.dataCounts.outbox += 1;

  assert.equal(assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    release.evidence,
  ), "releaseable");

  for (const delivery of release.state.source.deliveries) {
    delivery.mode = "online";
    delivery.online_at = "2026-07-27T09:30:00.000Z";
    delivery.state_changed_at = delivery.online_at;
    delivery.state_reason = releaseReason(release.config, release.cohort, release.evidence);
  }
  for (const outbox of release.state.source.outbox) outbox.status = "pending";
  assert.equal(assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    release.evidence,
  ), "released");
});

test("Released Delivery is idempotently valid while Projection remains held shadow", () => {
  const release = releaseFixture({ mode: "online" });
  release.state.business.targetDataCounts.inbox = 6;
  release.state.business.targetDataCounts.revision = 6;
  release.state.business.targetDataCounts.activation = 2;
  release.state.business.targetDataCounts.activation_item = 6;
  release.state.business.targetDataCounts.consumer_cursor = 6;
  release.state.business.targetDataCounts.projection_outbox = 2;
  release.state.business.targetDataCounts.reconciliation_state = 2;
  release.state.business.targetDataCounts.entity_current = 2;
  release.state.business.targetDataCounts.video_current = 2;
  release.state.business.targetDataCounts.agent_current = 2;
  assert.equal(assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    release.evidence,
  ), "released");

  release.state.source.domainCurrents[0].readiness_status = "not_ready";
  release.state.source.outbox[0].status = "dead_letter";
  release.state.source.outbox[1].status = "covered_by_baseline";
  assert.equal(assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    release.evidence,
  ), "released");

  release.state.source.outbox[0].status = "held";
  assert.throws(() => assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    release.evidence,
  ), /Outbox status is invalid/);
});

test("Delivery release rejects mixed cohort modes, Sequence gaps, and integrity failures", () => {
  const release = releaseFixture();
  release.state.source.deliveries[0].mode = "online";
  release.state.source.deliveries[0].online_at = "2026-07-27T09:30:00.000Z";
  assert.throws(() => assertPublicationDeliveryReleaseState(
    release.state,
    release.config,
    release.cohort,
    release.evidence,
  ), /uniformly hold or online/);

  const brokenChain = releaseFixture();
  brokenChain.state.source.revisions[0].data_sequence = "2";
  assert.throws(() => assertPublicationDeliveryReleaseState(
    brokenChain.state,
    brokenChain.config,
    brokenChain.cohort,
    brokenChain.evidence,
  ), /Bootstrap must start at Sequence 1|Sequence is not contiguous/);

  const conflict = releaseFixture({ mode: "online" });
  conflict.state.business.targetDataCounts.inbox_conflict = 1;
  assert.throws(() => assertPublicationDeliveryReleaseState(
    conflict.state,
    conflict.config,
    conflict.cohort,
    conflict.evidence,
  ), /integrity failures/);

  const wrongReference = releaseFixture({ mode: "online" });
  const reason = JSON.parse(wrongReference.state.source.deliveries[0].state_reason);
  reason.readiness_evidence_hash = hash("other-evidence");
  wrongReference.state.source.deliveries[0].state_reason = JSON.stringify(reason);
  assert.throws(() => assertPublicationDeliveryReleaseState(
    wrongReference.state,
    wrongReference.config,
    wrongReference.cohort,
    wrongReference.evidence,
  ), /readiness_evidence_hash/);
});

test("Capture activation drains in-flight Source writes before changing the Stream switch", async () => {
  const source = await readFile(
    new URL("../src/publicationCohortAdmin.js", import.meta.url),
    "utf8",
  );
  const lock = source.indexOf("LOCK TABLE crawler.channels,crawler.contents,crawler.agent_profiles");
  const update = source.indexOf("UPDATE publication.stream", lock);
  assert.ok(lock > 0, "Source Writer tables must be locked");
  assert.ok(update > lock, "Capture must be enabled after the Source Writer lock");
  assert.match(source, /LOCK TABLE publication\.stream, publication\.channel_ownership IN SHARE MODE/);
  assert.match(
    source,
    /LOCK TABLE public\.channels,publication\.stream,publication\.channel_ownership/,
  );
  assert.match(source, /BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY/);
});

test("Publication cohort CLI has separate plan/apply actions", () => {
  assert.deepEqual(publicationCohortCommand(["init"]), {
    command: "init",
    apply: false,
    help: false,
  });
  assert.deepEqual(publicationCohortCommand(["enable-capture", "--apply"]), {
    command: "enable-capture",
    apply: true,
    help: false,
  });
  assert.deepEqual(publicationCohortCommand(["release-delivery"]), {
    command: "release-delivery",
    apply: false,
    help: false,
  });
  assert.throws(() => publicationCohortCommand(["init", "--all"]), /unknown option/);
  assert.throws(() => publicationCohortCommand(["enable-capture", "--apply", "--apply"]), /only/);
  assert.throws(() => publicationCohortCommand(["init-all"]), /init, enable-capture, or release-delivery/);
});
