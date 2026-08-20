import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import { environmentValue } from "./runtimeEnvironment.js";
import {
  AGENT_TAXONOMY_VERSION,
  PUBLICATION_CONTRACT_VERSION,
  PUBLICATION_POLICY_VERSION,
  VIDEO_WINDOW_POLICY_VERSION,
} from "./publicationContract.js";
import { validateBusinessPublicationEnvelope } from "./businessPublicationContract.js";
import { publicationEnvelopeFromRow } from "./publicationTransport.js";
import { PUBLICATION_WRITER_VERSION } from "./publicationWriterVersion.js";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_KEY = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const IMAGE_DIGEST = /^sha256:[0-9a-f]{64}$/;
const RESULT_HASH = /^sha256:[0-9a-f]{64}$/;
const DOMAINS = Object.freeze(["channel", "video", "agent"]);
const DOMAIN_POLICY_VERSIONS = Object.freeze({
  channel: PUBLICATION_POLICY_VERSION,
  video: VIDEO_WINDOW_POLICY_VERSION,
  agent: PUBLICATION_POLICY_VERSION,
});
const COMMANDS = new Set(["init", "enable-capture", "release-delivery"]);
const SOURCE_ADMIN_LOCK = 781137240;
const BUSINESS_ADMIN_LOCK = 781137241;

function requiredString(environment, name) {
  const value = String(environment[name] ?? "").trim();
  if (!value) throw new TypeError(`${name} is required`);
  return value;
}

function expectedCount(environment, name) {
  const raw = requiredString(environment, name);
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  return value;
}

function safeKey(environment, name) {
  const value = requiredString(environment, name);
  if (!SAFE_KEY.test(value)) {
    throw new TypeError(`${name} must use only letters, numbers, dot, underscore, or hyphen`);
  }
  return value;
}

function parseSourceIdentity(environment) {
  const raw = environmentValue("PUBLICATION_SOURCE_IDENTITY_JSON", { environment });
  let value;
  try {
    value = JSON.parse(raw);
  } catch (error) {
    throw new TypeError("PUBLICATION_SOURCE_IDENTITY_JSON must be valid JSON", { cause: error });
  }
  if (!value || Array.isArray(value) || typeof value !== "object") {
    throw new TypeError("PUBLICATION_SOURCE_IDENTITY_JSON must be a JSON object");
  }
  return value;
}

export function publicationCohortRuntimeConfig(
  environment = process.env,
  { command = "init" } = {},
) {
  if (!COMMANDS.has(command)) {
    throw new TypeError(`unsupported Publication cohort command: ${command}`);
  }
  const streamId = requiredString(environment, "PUBLICATION_STREAM_ID").toLowerCase();
  if (!UUID.test(streamId)) throw new TypeError("PUBLICATION_STREAM_ID must be a UUID");
  const expectedCrawlerDatabase = safeKey(environment, "EXPECTED_CRAWLER_DATABASE");
  const expectedBusinessDatabase = safeKey(environment, "EXPECTED_BUSINESS_DATABASE");
  if (expectedCrawlerDatabase === expectedBusinessDatabase) {
    throw new TypeError("Crawler and Business database names must be different");
  }
  const config = {
    command,
    crawlerDatabaseUrl: environmentValue("CRAWLER_DATABASE_URL", { environment }),
    businessDatabaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedCrawlerDatabase,
    expectedBusinessDatabase,
    expectedCrawlerChannelCount: expectedCount(environment, "EXPECTED_CRAWLER_CHANNEL_COUNT"),
    expectedBusinessChannelCount: expectedCount(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT"),
    streamId,
    sourceDeploymentKey: safeKey(environment, "PUBLICATION_SOURCE_DEPLOYMENT_KEY"),
    sourceIdentity: parseSourceIdentity(environment),
    cohortKey: safeKey(environment, "PUBLICATION_COHORT_KEY"),
    channelIdsFile: requiredString(environment, "PUBLICATION_CHANNEL_IDS_FILE"),
    destination: safeKey(environment, "PUBLICATION_DESTINATION"),
    actor: requiredString(environment, "PUBLICATION_OPERATOR"),
    reason: requiredString(environment, "PUBLICATION_ACTION_REASON"),
    writerVersion: PUBLICATION_WRITER_VERSION,
    writerDeploymentRef: null,
    runtimeDeploymentRef: null,
    readinessReportFile: null,
  };
  if (command === "enable-capture") {
    config.writerDeploymentRef = requiredString(environment, "PUBLICATION_WRITER_DEPLOYMENT_REF").toLowerCase();
    if (!IMAGE_DIGEST.test(config.writerDeploymentRef)) {
      throw new TypeError("PUBLICATION_WRITER_DEPLOYMENT_REF must be an immutable sha256 image digest");
    }
  }
  if (command === "release-delivery") {
    config.runtimeDeploymentRef = requiredString(
      environment,
      "PUBLICATION_RUNTIME_DEPLOYMENT_REF",
    ).toLowerCase();
    if (!IMAGE_DIGEST.test(config.runtimeDeploymentRef)) {
      throw new TypeError("PUBLICATION_RUNTIME_DEPLOYMENT_REF must be an immutable sha256 image digest");
    }
    config.readinessReportFile = requiredString(
      environment,
      "PUBLICATION_READINESS_REPORT_FILE",
    );
  }
  return config;
}

function sha256(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalJson);
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalJson(value[key])]),
  );
}

function compareChannelIds(left, right) {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

export async function readPublicationChannelSet(path) {
  const raw = await readFile(path, "utf8");
  const channelIds = [];
  const seen = new Map();
  const lines = raw.replace(/^\uFEFF/, "").split(/\r?\n/);
  for (let index = 0; index < lines.length; index += 1) {
    const channelId = lines[index].trim();
    if (!channelId || channelId.startsWith("#")) continue;
    if (/\s/.test(channelId) || /[\u0000-\u001f\u007f]/.test(channelId) || channelId.length > 255) {
      throw new TypeError(`invalid Channel ID at line ${index + 1}`);
    }
    if (seen.has(channelId)) {
      throw new TypeError(
        `duplicate Channel ID at lines ${seen.get(channelId)} and ${index + 1}: ${channelId}`,
      );
    }
    seen.set(channelId, index + 1);
    channelIds.push(channelId);
  }
  if (channelIds.length === 0) throw new TypeError("PUBLICATION_CHANNEL_IDS_FILE must contain at least one Channel ID");
  channelIds.sort(compareChannelIds);
  return channelIds;
}

export function buildPublicationCohort(config, channelIds) {
  const canonical = `${channelIds.join("\n")}\n`;
  const channelSetHash = sha256(canonical);
  return {
    streamId: config.streamId,
    cohortKey: config.cohortKey,
    destination: config.destination,
    channelIds,
    channelCount: channelIds.length,
    channelSetHash,
    sourceIdentityHash: sha256(JSON.stringify(canonicalJson(config.sourceIdentity))),
    ownershipReference: {
      cohort_key: config.cohortKey,
      channel_count: channelIds.length,
      channel_set_hash: channelSetHash,
    },
  };
}

function readinessCount(report, name, expected) {
  const actual = report?.summary?.[name];
  if (!Number.isSafeInteger(actual) || actual !== expected) {
    fail(`Publication Readiness ${name} mismatch`, { actual, expected });
  }
}

export async function readPublicationReadinessEvidence(path, cohort) {
  const raw = await readFile(path, "utf8");
  let report;
  try {
    report = JSON.parse(raw);
  } catch (error) {
    throw new TypeError("PUBLICATION_READINESS_REPORT_FILE must contain valid JSON", {
      cause: error,
    });
  }
  if (!report || Array.isArray(report) || typeof report !== "object") {
    throw new TypeError("PUBLICATION_READINESS_REPORT_FILE must contain a JSON object");
  }
  expect("Publication Readiness report version", report.report_version, "publication-readiness-report-v1");
  expect(
    "Publication Readiness contract version",
    report.contract_version,
    PUBLICATION_CONTRACT_VERSION,
  );
  expect(
    "Publication Readiness publication policy",
    report.policies?.publication,
    PUBLICATION_POLICY_VERSION,
  );
  expect(
    "Publication Readiness Video Window policy",
    report.policies?.video_window,
    VIDEO_WINDOW_POLICY_VERSION,
  );
  expect(
    "Publication Readiness Agent taxonomy",
    report.policies?.agent_taxonomy,
    AGENT_TAXONOMY_VERSION,
  );
  const reportAsOf = new Date(report.report_as_of);
  if (Number.isNaN(reportAsOf.getTime())) {
    fail("Publication Readiness report_as_of must be a valid timestamp");
  }
  expect(
    "Publication Readiness requested Channel count",
    report.scope?.requested_channel_count,
    cohort.channelCount,
  );
  expect(
    "Publication Readiness Crawler Channel count",
    report.scope?.crawler_channel_count,
    cohort.channelCount,
  );
  expect(
    "Publication Readiness Business Channel count",
    report.scope?.business_active_channel_count,
    cohort.channelCount,
  );
  expect(
    "Publication Readiness source-only Channel count",
    report.scope?.source_only_channel_count,
    0,
  );
  expect(
    "Publication Readiness target-only Channel count",
    report.scope?.target_only_channel_count,
    0,
  );
  for (const name of [
    "total_channels",
    "baseline_eligible",
    "channel_ready",
    "video_ready",
    "agent_ready",
    "business_target_channels",
    "business_regression_passed",
  ]) readinessCount(report, name, cohort.channelCount);
  readinessCount(report, "not_ready", 0);
  readinessCount(report, "business_regression_not_applicable", 0);

  if (!Array.isArray(report.channels)) fail("Publication Readiness channels must be an array");
  const seen = new Set();
  const channels = report.channels.map((row) => {
    const channelId = String(row?.channel_id ?? "").trim();
    if (!channelId || seen.has(channelId)) {
      fail("Publication Readiness Channel IDs must be non-empty and unique", channelId);
    }
    seen.add(channelId);
    if (row?.baseline_eligible !== true) {
      fail(`Publication Readiness Channel is not baseline eligible: ${channelId}`);
    }
    if (!Array.isArray(row?.readiness_reasons) || row.readiness_reasons.length !== 0) {
      fail(`Publication Readiness Channel has unresolved reasons: ${channelId}`);
    }
    if (
      row?.business_regression?.target_exists !== true
      || row.business_regression.passed !== true
      || !Array.isArray(row.business_regression.regressions)
      || row.business_regression.regressions.length !== 0
    ) {
      fail(`Publication Readiness Business regression did not pass: ${channelId}`);
    }
    const resultHashes = {};
    for (const domain of DOMAINS) {
      const candidate = row?.domains?.[domain];
      if (candidate?.ready !== true || !RESULT_HASH.test(String(candidate?.result_hash ?? ""))) {
        fail(`Publication Readiness ${domain} is not ready for ${channelId}`);
      }
      expect(
        `Publication Readiness ${domain} contract version for ${channelId}`,
        candidate.contract_version,
        PUBLICATION_CONTRACT_VERSION,
      );
      expect(
        `Publication Readiness ${domain} policy version for ${channelId}`,
        candidate.policy_version,
        DOMAIN_POLICY_VERSIONS[domain],
      );
      resultHashes[domain] = candidate.result_hash;
    }
    return { channel_id: channelId, result_hashes: resultHashes };
  }).sort((left, right) => compareChannelIds(left.channel_id, right.channel_id));
  expect(
    "Publication Readiness Channel set",
    channels.map((row) => row.channel_id),
    cohort.channelIds,
  );
  return {
    report_version: report.report_version,
    report_as_of: reportAsOf.toISOString(),
    evidence_hash: sha256(raw),
    channels,
  };
}

export function publicationCohortConfirmation(command, config, cohort, evidence = null) {
  const base = [
    config.expectedCrawlerDatabase,
    config.expectedBusinessDatabase,
    cohort.streamId,
    cohort.destination,
    cohort.cohortKey,
    String(cohort.channelCount),
    cohort.channelSetHash,
  ];
  if (command === "init") return ["INITIALIZE_PUBLICATION_COHORT", ...base].join(":");
  if (command === "enable-capture") {
    return [
      "ENABLE_PUBLICATION_CAPTURE",
      ...base,
      config.writerVersion,
      sha256(config.writerDeploymentRef),
    ].join(":");
  }
  if (command === "release-delivery") {
    if (!evidence?.evidence_hash) {
      throw new TypeError("Publication Readiness evidence is required for Delivery release");
    }
    return [
      "RELEASE_PUBLICATION_DELIVERY",
      ...base,
      sha256(config.runtimeDeploymentRef),
      evidence.evidence_hash,
    ].join(":");
  }
  throw new TypeError(`unsupported Publication cohort command: ${command}`);
}

function fail(message, detail = null) {
  const suffix = detail === null ? "" : `: ${JSON.stringify(detail)}`;
  throw new Error(`${message}${suffix}`);
}

function expect(label, actual, expected) {
  if (!isDeepStrictEqual(actual, expected)) fail(`${label} mismatch`, { actual, expected });
}

function id(value) {
  return String(value ?? "").toLowerCase();
}

function streamIdentity(row) {
  return {
    publication_stream_id: row.publication_stream_id,
    source_deployment_key: row.source_deployment_key,
    status: row.status,
    capture_enabled: row.capture_enabled_at != null,
  };
}

function validateStreamRows(rows, config, { business, allowMissing, capture }) {
  if (rows.length === 0) {
    if (allowMissing) return;
    fail(`${business ? "Business" : "Crawler"} Publication Stream is missing`);
  }
  const matching = rows.filter((row) => id(row.publication_stream_id) === config.streamId);
  const collisions = rows.filter((row) => id(row.publication_stream_id) !== config.streamId);
  if (matching.length !== 1) {
    fail("Publication Stream UUID is ambiguous", rows.map(streamIdentity));
  }
  if (collisions.length > 0) {
    fail(
      "source deployment key is already registered to another Stream",
      collisions.map(streamIdentity),
    );
  }
  const row = matching[0];
  expect("Publication source deployment key", row.source_deployment_key, config.sourceDeploymentKey);
  if (!isDeepStrictEqual(row.source_identity_json, config.sourceIdentity)) {
    fail("Publication source identity mismatch", {
      actual_hash: sha256(JSON.stringify(canonicalJson(row.source_identity_json))),
      expected_hash: sha256(JSON.stringify(canonicalJson(config.sourceIdentity))),
    });
  }
  expect("Publication Stream status", row.status, "active");
  if (business) {
    expect("Business accepted contract versions", row.accepted_contract_versions, [1, 2]);
    return;
  }
  const minimum = row.minimum_writer_version ?? null;
  const enabledAt = row.capture_enabled_at ?? null;
  if (capture === "disabled") {
    expect("Crawler minimum writer version", minimum, null);
    expect("Crawler Capture state", enabledAt, null);
  } else if (capture === "enableable") {
    if (minimum !== null && minimum !== config.writerVersion) {
      fail("Crawler minimum writer version cannot be enabled by this release", minimum);
    }
    if (enabledAt !== null && minimum !== config.writerVersion) {
      fail("Crawler Capture is already enabled with an incompatible writer version", minimum);
    }
  } else if (capture === "enabled") {
    expect("Crawler minimum writer version", minimum, config.writerVersion);
    if (enabledAt === null) fail("Crawler Capture was not enabled");
  }
}

function validateSourceOwnership(rows, config, cohort, {
  complete,
  seedStatus = "pending",
}) {
  const target = new Set(cohort.channelIds);
  const found = new Set();
  for (const row of rows) {
    const sameStream = id(row.publication_stream_id) === config.streamId;
    if (!sameStream) {
      if (target.has(row.channel_id) && row.status === "owned") {
        fail("Crawler Channel is already owned by another active Stream", row);
      }
      continue;
    }
    if (!target.has(row.channel_id)) fail("Crawler Stream owns a Channel outside the explicit cohort", row);
    found.add(row.channel_id);
    expect(`Crawler ownership status for ${row.channel_id}`, row.status, "owned");
    expect(`Crawler onboarding mode for ${row.channel_id}`, row.onboarding_mode, "bootstrap");
    expect(`Crawler seed status for ${row.channel_id}`, row.seed_status, seedStatus);
    expect(`Crawler ownership reference for ${row.channel_id}`, row.ownership_reference, cohort.ownershipReference);
  }
  if (complete && found.size !== target.size) {
    fail("Crawler ownership does not exactly match the explicit cohort", {
      expected: target.size,
      actual: found.size,
    });
  }
}

function validateDeliveries(rows, config, cohort, {
  complete,
  mode = "hold",
}) {
  const target = new Set(cohort.channelIds);
  const found = new Set();
  for (const row of rows) {
    if (row.destination !== config.destination) {
      fail("Crawler Stream has an unexpected Publication destination", row);
    }
    if (!target.has(row.channel_id)) fail("Crawler delivery includes a Channel outside the explicit cohort", row);
    found.add(row.channel_id);
    expect(`Crawler delivery mode for ${row.channel_id}`, row.mode, mode);
    expect(
      `Crawler delivery ownership reference for ${row.channel_id}`,
      row.source_ownership_reference,
      cohort.ownershipReference,
    );
    for (const field of [
      "latest_successful_baseline_id",
      "channel_watermark_sequence",
      "video_watermark_sequence",
      "agent_watermark_sequence",
      "sealed_at",
    ]) expect(`Crawler delivery ${field} for ${row.channel_id}`, row[field] ?? null, null);
    if (mode === "hold") {
      expect(`Crawler delivery online_at for ${row.channel_id}`, row.online_at ?? null, null);
    } else if (mode === "online" && row.online_at == null) {
      fail(`Crawler delivery online_at is missing for ${row.channel_id}`);
    }
  }
  if (complete && found.size !== target.size) {
    fail("Crawler delivery state does not exactly match the explicit cohort", {
      expected: target.size,
      actual: found.size,
    });
  }
}

function validateBusinessOwnership(rows, config, cohort, { complete }) {
  const target = new Set(cohort.channelIds);
  const found = new Set();
  for (const row of rows) {
    const sameStream = id(row.active_publication_stream_id) === config.streamId;
    if (!sameStream) {
      if (target.has(row.channel_id)) fail("Business Channel is owned by another Stream", row);
      continue;
    }
    if (!target.has(row.channel_id)) fail("Business Stream owns a Channel outside the explicit cohort", row);
    found.add(row.channel_id);
    expect(`Business ownership status for ${row.channel_id}`, row.status, "active");
    expect(`Business previous Stream for ${row.channel_id}`, row.previous_publication_stream_id ?? null, null);
    expect(`Business projection mode for ${row.channel_id}`, row.projection_mode, "held_shadow");
    expect(`Business ownership reference for ${row.channel_id}`, row.ownership_reference, cohort.ownershipReference);
  }
  if (complete && found.size !== target.size) {
    fail("Business ownership does not exactly match the explicit cohort", {
      expected: target.size,
      actual: found.size,
    });
  }
}

function assertEmptyData(state, side) {
  for (const [name, value] of Object.entries(state.dataCounts)) {
    if (Number(value) !== 0) fail(`${side} ${name} must be empty before Capture is enabled`, value);
  }
}

export function assertPublicationCohortState(
  state,
  config,
  cohort,
  { complete = false, capture = "disabled", requireEmpty = true } = {},
) {
  validateStreamRows(state.source.streams, config, {
    business: false,
    allowMissing: !complete,
    capture,
  });
  validateStreamRows(state.business.streams, config, {
    business: true,
    allowMissing: !complete,
    capture,
  });
  validateSourceOwnership(state.source.ownerships, config, cohort, { complete });
  validateDeliveries(state.source.deliveries, config, cohort, { complete });
  validateBusinessOwnership(state.business.ownerships, config, cohort, { complete });
  if (requireEmpty) {
    assertEmptyData(state.source, "Crawler");
    assertEmptyData(state.business, "Business");
  }
}

function releasePhase(deliveries, config, cohort) {
  const target = new Set(cohort.channelIds);
  const modes = new Set(deliveries
    .filter((row) => row.destination === config.destination && target.has(row.channel_id))
    .map((row) => row.mode));
  if (modes.size !== 1) {
    fail("Publication Delivery release state must be uniformly hold or online", [...modes]);
  }
  const mode = [...modes][0];
  if (!new Set(["hold", "online"]).has(mode)) {
    fail("Publication Delivery release state is not releasable", mode);
  }
  return mode === "hold" ? "releaseable" : "released";
}

function evidenceByChannel(evidence) {
  if (!evidence || !Array.isArray(evidence.channels) || !evidence.evidence_hash) {
    throw new TypeError("Publication Readiness evidence is required for Delivery release");
  }
  const rows = new Map(evidence.channels.map((row) => [row.channel_id, row]));
  if (rows.size !== evidence.channels.length) {
    fail("Publication Readiness evidence contains duplicate Channels");
  }
  return rows;
}

function deliveryReleaseReference(config, cohort, evidence) {
  return {
    action: "release_publication_delivery",
    cohort_key: cohort.cohortKey,
    channel_set_hash: cohort.channelSetHash,
    readiness_evidence_hash: evidence.evidence_hash,
    readiness_report_as_of: evidence.report_as_of,
    runtime_deployment_ref: config.runtimeDeploymentRef,
  };
}

function releaseTimestamp(value, label) {
  const parsed = new Date(value);
  if (value == null || Number.isNaN(parsed.getTime())) {
    fail(`${label} must be a valid timestamp`, value ?? null);
  }
  return parsed.toISOString();
}

function validateReleasedDeliveryReference(deliveries, config, cohort, evidence) {
  const expected = deliveryReleaseReference(config, cohort, evidence);
  const releaseBoundaries = new Set();
  for (const row of deliveries) {
    const onlineAt = releaseTimestamp(
      row.online_at,
      `Crawler delivery online_at for ${row.channel_id}`,
    );
    const changedAt = releaseTimestamp(
      row.state_changed_at,
      `Crawler delivery state_changed_at for ${row.channel_id}`,
    );
    expect(`Crawler delivery release timestamp for ${row.channel_id}`, changedAt, onlineAt);
    releaseBoundaries.add(onlineAt);
    let reason;
    try {
      reason = JSON.parse(row.state_reason);
    } catch (error) {
      throw new Error(
        `Crawler delivery release evidence is invalid for ${row.channel_id}`,
        { cause: error },
      );
    }
    if (!reason || Array.isArray(reason) || typeof reason !== "object") {
      fail(`Crawler delivery release evidence is invalid for ${row.channel_id}`);
    }
    for (const [name, value] of Object.entries(expected)) {
      expect(`Crawler delivery release ${name} for ${row.channel_id}`, reason[name], value);
    }
  }
  if (releaseBoundaries.size !== 1) {
    fail("Publication Delivery cohort does not share one release boundary", [...releaseBoundaries]);
  }
}

function validateReleaseTopology(source, config, cohort, evidence, phase) {
  const target = new Set(cohort.channelIds);
  const approved = evidenceByChannel(evidence);
  expect("Publication Readiness evidence Channel count", approved.size, cohort.channelCount);
  for (const channelId of approved.keys()) {
    if (!target.has(channelId)) {
      fail("Publication Readiness evidence includes a Channel outside the explicit cohort", channelId);
    }
  }
  const currents = Array.isArray(source.domainCurrents) ? source.domainCurrents : [];
  const revisions = Array.isArray(source.revisions) ? source.revisions : [];
  const outbox = Array.isArray(source.outbox) ? source.outbox : [];
  const currentByDomain = new Map();
  const revisionsByDomain = new Map();
  for (const row of currents) {
    if (!target.has(row.channel_id)) {
      fail("Publication release data includes a Channel outside the explicit cohort", row);
    }
    const key = `${row.channel_id}\u0000${row.domain}`;
    if (currentByDomain.has(key)) {
      fail("Publication release has duplicate Domain Current rows", row);
    }
    currentByDomain.set(key, row);
  }
  for (const row of revisions) {
    if (!target.has(row.channel_id)) {
      fail("Publication release data includes a Channel outside the explicit cohort", row);
    }
    validateBusinessPublicationEnvelope(publicationEnvelopeFromRow(row));
    const key = `${row.channel_id}\u0000${row.domain}`;
    const chain = revisionsByDomain.get(key) ?? [];
    chain.push(row);
    revisionsByDomain.set(key, chain);
  }
  for (const row of outbox) {
    if (!target.has(row.channel_id)) {
      fail("Publication release data includes a Channel outside the explicit cohort", row);
    }
  }
  const outboxByRevision = new Map();
  for (const row of outbox) {
    if (row.destination !== config.destination) {
      fail("Publication release data has an unexpected destination", row);
    }
    if (outboxByRevision.has(id(row.revision_id))) {
      fail("Publication Revision has duplicate Delivery rows", row.revision_id);
    }
    outboxByRevision.set(id(row.revision_id), row);
    const allowed = phase === "releaseable"
      ? new Set(["held"])
      : new Set([
          "pending",
          "leased",
          "retry_wait",
          "delivered",
          "covered_by_baseline",
          "dead_letter",
        ]);
    if (!allowed.has(row.status)) {
      fail(`Publication Outbox status is invalid for ${phase}`, row);
    }
    if (phase === "releaseable" && (
      Number(row.attempts) !== 0
      || row.lease_owner != null
      || row.receipt_id != null
      || row.delivered_at != null
    )) fail("held Publication Outbox has delivery side effects", row);
  }

  for (const channelId of cohort.channelIds) {
    const channelEvidence = approved.get(channelId);
    if (!channelEvidence) fail("Publication Readiness evidence is missing a Channel", channelId);
    for (const domain of DOMAINS) {
      const key = `${channelId}\u0000${domain}`;
      const current = currentByDomain.get(key);
      if (!current) {
        fail("Publication release requires exactly one Domain Current", {
          channel_id: channelId,
          domain,
          actual: 0,
        });
      }
      expect(
        `Publication Current contract version for ${channelId}/${domain}`,
        Number(current.contract_version),
        PUBLICATION_CONTRACT_VERSION,
      );
      expect(
        `Publication Current policy version for ${channelId}/${domain}`,
        current.policy_version,
        DOMAIN_POLICY_VERSIONS[domain],
      );
      if (phase === "releaseable") {
        expect(`Publication Current readiness for ${channelId}/${domain}`, current.readiness_status, "ready");
      } else if (!new Set(["ready", "not_ready"]).has(current.readiness_status)) {
        fail(`Publication Current readiness is invalid for ${channelId}/${domain}`);
      }
      const chain = (revisionsByDomain.get(key) ?? [])
        .sort((left, right) => Number(left.data_sequence) - Number(right.data_sequence));
      if (chain.length === 0) {
        fail("Publication release requires a Bootstrap Revision", { channel_id: channelId, domain });
      }
      let previous = null;
      for (const revision of chain) {
        const sequence = Number(revision.data_sequence);
        if (!Number.isSafeInteger(sequence) || sequence !== (previous ? Number(previous.data_sequence) + 1 : 1)) {
          fail("Publication Revision Sequence is not contiguous", revision);
        }
        if (!RESULT_HASH.test(String(revision.result_hash ?? ""))) {
          fail("Publication Revision result_hash is invalid", revision);
        }
        if (!previous) {
          expect(`Publication Bootstrap type for ${channelId}/${domain}`, revision.revision_type, "bootstrap");
          expect(`Publication Bootstrap previous Sequence for ${channelId}/${domain}`, revision.previous_data_sequence ?? null, null);
          expect(`Publication Bootstrap previous Hash for ${channelId}/${domain}`, revision.previous_result_hash ?? null, null);
          const operation = domain === "video" ? "replace_window" : "replace";
          expect(`Publication Bootstrap operation for ${channelId}/${domain}`, revision.operation, operation);
        } else {
          expect(
            `Publication previous Sequence for ${channelId}/${domain}/${sequence}`,
            Number(revision.previous_data_sequence),
            Number(previous.data_sequence),
          );
          expect(
            `Publication previous Hash for ${channelId}/${domain}/${sequence}`,
            revision.previous_result_hash,
            previous.result_hash,
          );
          if (revision.revision_type === "bootstrap") {
            fail("Publication Revision chain contains more than one Bootstrap", revision);
          }
        }
        if (!outboxByRevision.has(id(revision.revision_id))) {
          fail("Publication Revision is missing its Delivery row", revision.revision_id);
        }
        previous = revision;
      }
      expect(
        `Publication Current Sequence for ${channelId}/${domain}`,
        Number(current.data_sequence),
        Number(previous.data_sequence),
      );
      expect(
        `Publication Current Revision for ${channelId}/${domain}`,
        id(current.current_revision_id),
        id(previous.revision_id),
      );
      expect(
        `Publication Current Hash for ${channelId}/${domain}`,
        current.result_hash,
        previous.result_hash,
      );
      const approvedHash = channelEvidence.result_hashes?.[domain];
      if (phase === "releaseable") {
        expect(
          `Publication Readiness Hash for ${channelId}/${domain}`,
          current.result_hash,
          approvedHash,
        );
      } else if (!chain.some((revision) => revision.result_hash === approvedHash)) {
        fail(`Released Publication chain is missing the approved Hash for ${channelId}/${domain}`);
      }
    }
  }
  if (outboxByRevision.size !== revisions.length) {
    fail("Publication release requires exactly one Delivery row per Revision", {
      revisions: revisions.length,
      outbox: outboxByRevision.size,
    });
  }
}

function assertBusinessReleaseEmpty(business) {
  for (const [name, value] of Object.entries(business.targetDataCounts ?? {})) {
    if (Number(value) !== 0) {
      fail(`Business ${name} must be empty before Delivery release`, value);
    }
  }
}

export function assertPublicationDeliveryReleaseState(state, config, cohort, evidence, {
  expectedPhase = null,
} = {}) {
  validateStreamRows(state.source.streams, config, {
    business: false,
    allowMissing: false,
    capture: "enabled",
  });
  validateStreamRows(state.business.streams, config, {
    business: true,
    allowMissing: false,
    capture: "enabled",
  });
  validateSourceOwnership(state.source.ownerships, config, cohort, {
    complete: true,
    seedStatus: "complete",
  });
  const phase = releasePhase(state.source.deliveries, config, cohort);
  if (expectedPhase && phase !== expectedPhase) {
    fail("Publication Delivery release phase mismatch", { actual: phase, expected: expectedPhase });
  }
  validateDeliveries(state.source.deliveries, config, cohort, {
    complete: true,
    mode: phase === "releaseable" ? "hold" : "online",
  });
  if (phase === "released") {
    validateReleasedDeliveryReference(
      state.source.deliveries,
      config,
      cohort,
      evidence,
    );
  }
  validateBusinessOwnership(state.business.ownerships, config, cohort, { complete: true });
  validateReleaseTopology(state.source, config, cohort, evidence, phase);
  if (phase === "releaseable") assertBusinessReleaseEmpty(state.business);
  const conflictCount = Number(state.business.targetDataCounts?.inbox_conflict ?? 0);
  const quarantineCount = Number(state.business.targetDataCounts?.open_quarantine ?? 0);
  if (conflictCount !== 0 || quarantineCount !== 0) {
    fail("Business Publication release has unresolved integrity failures", {
      inbox_conflict: conflictCount,
      open_quarantine: quarantineCount,
    });
  }
  return phase;
}

async function databaseIdentity(client, side) {
  if (side === "source") {
    return (await client.query(
      `SELECT current_database() AS database_name,
              inet_server_addr()::text AS server_address,
              inet_server_port() AS server_port,
              to_regclass('crawler.channels') IS NOT NULL AS channels_ready,
              to_regclass('publication.stream') IS NOT NULL AS stream_ready,
              to_regclass('publication.channel_stream_state') IS NOT NULL AS ownership_ready,
              to_regclass('publication.channel_delivery_state') IS NOT NULL AS delivery_ready,
              to_regclass('publication.domain_current') IS NOT NULL AS current_ready,
              to_regclass('publication.revision') IS NOT NULL AS revision_ready,
              to_regclass('publication.outbox') IS NOT NULL AS outbox_ready,
              to_regprocedure('publication.writer_version_satisfies(text,text)') IS NOT NULL
                AS writer_barrier_ready`,
    )).rows[0] ?? {};
  }
  return (await client.query(
    `SELECT current_database() AS database_name,
            inet_server_addr()::text AS server_address,
            inet_server_port() AS server_port,
            to_regclass('public.channels') IS NOT NULL AS channels_ready,
            to_regclass('publication.stream') IS NOT NULL AS stream_ready,
            to_regclass('publication.channel_ownership') IS NOT NULL AS ownership_ready,
            to_regclass('publication.inbox') IS NOT NULL AS inbox_ready,
            to_regclass('publication.revision') IS NOT NULL AS revision_ready,
              to_regclass('publication.consumer_cursor') IS NOT NULL
                AND to_regclass('publication.activation') IS NOT NULL AS activation_ready,
            to_regclass('publication.reconciliation_state') IS NOT NULL AS reconciliation_ready,
            EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_schema='publication' AND table_name='channel_ownership'
                AND column_name='projection_mode'
            ) AS projection_mode_ready`,
  )).rows[0] ?? {};
}

function assertIdentity(identity, expectedDatabase, side) {
  expect(`${side} database`, identity.database_name, expectedDatabase);
  const flags = Object.entries(identity).filter(([name]) => name.endsWith("_ready"));
  const missing = flags.filter(([, ready]) => ready !== true).map(([name]) => name);
  if (missing.length > 0) fail(`${side} Publication schema preflight failed`, missing);
}

async function inspectSource(client, config, cohort) {
  const identity = await databaseIdentity(client, "source");
  assertIdentity(identity, config.expectedCrawlerDatabase, "Crawler");
  const count = Number((await client.query("SELECT count(*)::int AS count FROM crawler.channels")).rows[0]?.count);
  expect("Crawler Channel count", count, config.expectedCrawlerChannelCount);
  const present = (await client.query(
    "SELECT channel_id FROM crawler.channels WHERE channel_id=ANY($1::text[])",
    [cohort.channelIds],
  )).rows.map((row) => row.channel_id).sort(compareChannelIds);
  expect("Crawler cohort Channel set", present, cohort.channelIds);
  const streams = (await client.query(
    `SELECT publication_stream_id,source_deployment_key,source_identity_json,status,
            minimum_writer_version,capture_enabled_at,created_by,created_reason
     FROM publication.stream
     WHERE publication_stream_id=$1::uuid OR source_deployment_key=$2
     ORDER BY publication_stream_id`,
    [config.streamId, config.sourceDeploymentKey],
  )).rows;
  const ownerships = (await client.query(
    `SELECT publication_stream_id,channel_id,status,onboarding_mode,seed_status,ownership_reference
     FROM publication.channel_stream_state
     WHERE publication_stream_id=$1::uuid
        OR (channel_id=ANY($2::text[]) AND status='owned')
     ORDER BY channel_id,publication_stream_id`,
    [config.streamId, cohort.channelIds],
  )).rows;
  const deliveries = (await client.query(
    `SELECT destination,publication_stream_id,channel_id,mode,latest_successful_baseline_id,
            channel_watermark_sequence,video_watermark_sequence,agent_watermark_sequence,
            source_ownership_reference,online_at,sealed_at,state_changed_at,
            state_changed_by,state_reason
     FROM publication.channel_delivery_state
     WHERE publication_stream_id=$1::uuid
     ORDER BY destination,channel_id`,
    [config.streamId],
  )).rows;
  const dataCounts = (await client.query(
    `SELECT
       (SELECT count(*)::int FROM publication.domain_current WHERE publication_stream_id=$1::uuid)
         AS domain_current,
       (SELECT count(*)::int FROM publication.revision WHERE publication_stream_id=$1::uuid)
         AS revision,
       (SELECT count(*)::int
        FROM publication.outbox AS outbox
        JOIN publication.revision AS revision USING (revision_id)
        WHERE revision.publication_stream_id=$1::uuid) AS outbox`,
    [config.streamId],
  )).rows[0] ?? {};
  const domainCurrents = (await client.query(
    `SELECT channel_id,domain,contract_version,policy_version,readiness_status,
            result_hash,data_sequence,current_revision_id
     FROM publication.domain_current
     WHERE publication_stream_id=$1::uuid
     ORDER BY channel_id,domain`,
    [config.streamId],
  )).rows;
  const revisions = (await client.query(
    `SELECT revision_id,publication_stream_id,channel_id,domain,data_sequence,
            previous_data_sequence,revision_type,operation,contract_version,policy_version,
            occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
     FROM publication.revision
     WHERE publication_stream_id=$1::uuid
     ORDER BY channel_id,domain,data_sequence,revision_id`,
    [config.streamId],
  )).rows;
  const outbox = (await client.query(
    `SELECT outbox.destination,outbox.revision_id,revision.channel_id,revision.domain,
            revision.data_sequence,outbox.status,outbox.attempts,outbox.lease_owner,
            outbox.receipt_id,outbox.delivered_at
     FROM publication.outbox AS outbox
     JOIN publication.revision AS revision USING (revision_id)
     WHERE revision.publication_stream_id=$1::uuid
     ORDER BY revision.channel_id,revision.domain,revision.data_sequence,outbox.destination`,
    [config.streamId],
  )).rows;
  return {
    identity,
    channelCount: count,
    streams,
    ownerships,
    deliveries,
    dataCounts,
    domainCurrents,
    revisions,
    outbox,
  };
}

async function inspectBusiness(client, config, cohort) {
  const identity = await databaseIdentity(client, "business");
  assertIdentity(identity, config.expectedBusinessDatabase, "Business");
  const count = Number((await client.query("SELECT count(*)::int AS count FROM public.channels")).rows[0]?.count);
  expect("Business Channel count", count, config.expectedBusinessChannelCount);
  const present = (await client.query(
    "SELECT channel_id FROM public.channels WHERE channel_id=ANY($1::text[])",
    [cohort.channelIds],
  )).rows.map((row) => row.channel_id).sort(compareChannelIds);
  expect("Business cohort Channel set", present, cohort.channelIds);
  const streams = (await client.query(
    `SELECT publication_stream_id,source_deployment_key,source_identity_json,status,
            accepted_contract_versions,registered_by,registered_reason
     FROM publication.stream
     WHERE publication_stream_id=$1::uuid OR source_deployment_key=$2
     ORDER BY publication_stream_id`,
    [config.streamId, config.sourceDeploymentKey],
  )).rows;
  const ownerships = (await client.query(
    `SELECT channel_id,active_publication_stream_id,status,previous_publication_stream_id,
            ownership_reference,projection_mode
     FROM publication.channel_ownership
     WHERE active_publication_stream_id=$1::uuid OR channel_id=ANY($2::text[])
     ORDER BY channel_id`,
    [config.streamId, cohort.channelIds],
  )).rows;
  const dataCounts = (await client.query(
    `SELECT
       (SELECT count(*)::int FROM publication.inbox WHERE publication_stream_id=$1::uuid) AS inbox,
       (SELECT count(*)::int FROM publication.revision WHERE publication_stream_id=$1::uuid) AS revision,
       (SELECT count(*)::int FROM publication.activation WHERE publication_stream_id=$1::uuid) AS activation,
       (SELECT count(*)::int FROM publication.consumer_cursor WHERE publication_stream_id=$1::uuid)
         AS consumer_cursor`,
    [config.streamId],
  )).rows[0] ?? {};
  const targetDataCounts = (await client.query(
    `SELECT
       (SELECT count(*)::int FROM publication.inbox
        WHERE channel_id=ANY($1::text[])) AS inbox,
       (SELECT count(*)::int FROM publication.revision
        WHERE channel_id=ANY($1::text[])) AS revision,
       (SELECT count(*)::int FROM publication.activation
        WHERE channel_id=ANY($1::text[])) AS activation,
       (SELECT count(*)::int FROM publication.activation_item AS item
        JOIN publication.revision AS revision USING (revision_id)
        WHERE revision.channel_id=ANY($1::text[])) AS activation_item,
       (SELECT count(*)::int FROM publication.consumer_cursor
        WHERE channel_id=ANY($1::text[])) AS consumer_cursor,
       (SELECT count(*)::int FROM publication.projection_outbox
        WHERE channel_id=ANY($1::text[])) AS projection_outbox,
       (SELECT count(*)::int FROM publication.reconciliation_state
        WHERE channel_id=ANY($1::text[])) AS reconciliation_state,
       (SELECT count(*)::int FROM publication.inbox_conflict AS conflict
        JOIN publication.inbox AS inbox USING (revision_id)
        WHERE inbox.channel_id=ANY($1::text[]))
         AS inbox_conflict,
       (SELECT count(*)::int FROM publication.quarantine AS quarantine
        JOIN publication.inbox AS inbox USING (revision_id)
        WHERE inbox.channel_id=ANY($1::text[])
          AND quarantine.status='open') AS open_quarantine,
       (SELECT count(*)::int FROM result.entity_current
        WHERE channel_id=ANY($1::text[])) AS entity_current,
       (SELECT count(*)::int FROM result.video_current
        WHERE channel_id=ANY($1::text[])) AS video_current,
       (SELECT count(*)::int FROM result.content_current
        WHERE channel_id=ANY($1::text[])) AS content_current,
       (SELECT count(*)::int FROM result.agent_current
        WHERE channel_id=ANY($1::text[])) AS agent_current`,
    [cohort.channelIds],
  )).rows[0] ?? {};
  return {
    identity,
    channelCount: count,
    streams,
    ownerships,
    dataCounts,
    targetDataCounts,
  };
}

async function transaction(client, lockId, action) {
  let begun = false;
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
    begun = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '120s'");
    await client.query("SELECT pg_advisory_xact_lock($1)", [lockId]);
    const result = await action();
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function readOnlyTransaction(client, action) {
  let begun = false;
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    begun = true;
    await client.query("SET LOCAL statement_timeout = '120s'");
    const result = await action();
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function inspectBoth(sourcePromise, businessPromise) {
  const results = await Promise.allSettled([sourcePromise, businessPromise]);
  const failed = results.find((result) => result.status === "rejected");
  if (failed) throw failed.reason;
  return {
    source: results[0].value,
    business: results[1].value,
  };
}

async function lockPublicationDeliveryRows(client, config, cohort) {
  const result = await client.query(
    `SELECT destination,publication_stream_id,channel_id,mode
     FROM publication.channel_delivery_state
     WHERE destination=$1 AND publication_stream_id=$2::uuid
       AND channel_id=ANY($3::text[])
     ORDER BY channel_id
     FOR UPDATE`,
    [config.destination, config.streamId, cohort.channelIds],
  );
  if (result.rows.length !== cohort.channelCount) {
    fail("Publication Delivery lock does not cover the exact cohort", {
      expected: cohort.channelCount,
      actual: result.rows.length,
    });
  }
  return result.rows;
}

async function updatePublicationDeliverySource(client, {
  config,
  cohort,
  lockedDeliveries,
  expectedOutboxCount,
  stateReason,
}) {
  if (!client?.query) throw new TypeError("an active Crawler PostgreSQL client is required");
  if (!Array.isArray(lockedDeliveries) || lockedDeliveries.length !== cohort.channelCount) {
    throw new TypeError("the exact locked Publication Delivery cohort is required");
  }
  const modes = new Set(lockedDeliveries.map((row) => row.mode));
  if (modes.size !== 1 || !modes.has("hold")) {
    fail("Publication Delivery rows changed while releasing", [...modes]);
  }
  const expected = Number(expectedOutboxCount);
  if (!Number.isSafeInteger(expected) || expected < 1) {
    throw new TypeError("expectedOutboxCount must be a positive integer");
  }
  const releasedAtResult = await client.query("SELECT clock_timestamp() AS released_at");
  const releasedAt = new Date(releasedAtResult.rows[0]?.released_at);
  if (Number.isNaN(releasedAt.getTime())) fail("Crawler database did not return a release timestamp");
  const releasedAtIso = releasedAt.toISOString();
  const outbox = await client.query(
    `UPDATE publication.outbox AS outbox
     SET status='pending',next_attempt_at=$4::timestamptz,updated_at=$4::timestamptz
     FROM publication.revision AS revision
     WHERE outbox.revision_id=revision.revision_id
       AND outbox.destination=$1
       AND revision.publication_stream_id=$2::uuid
       AND revision.channel_id=ANY($3::text[])
       AND outbox.status='held'`,
    [config.destination, config.streamId, cohort.channelIds, releasedAtIso],
  );
  if (outbox.rowCount !== expected) {
    fail("Publication held Outbox count changed while releasing", {
      expected,
      actual: outbox.rowCount,
    });
  }
  const delivery = await client.query(
    `UPDATE publication.channel_delivery_state
     SET mode='online',online_at=$4::timestamptz,state_changed_at=$4::timestamptz,
         state_changed_by=$5,state_reason=$6,updated_at=$4::timestamptz
     WHERE destination=$1 AND publication_stream_id=$2::uuid
       AND channel_id=ANY($3::text[]) AND mode='hold'`,
    [
      config.destination,
      config.streamId,
      cohort.channelIds,
      releasedAtIso,
      config.actor,
      stateReason,
    ],
  );
  if (delivery.rowCount !== cohort.channelCount) {
    fail("Publication Delivery cohort changed while releasing", {
      expected: cohort.channelCount,
      actual: delivery.rowCount,
    });
  }
  return { delivery: delivery.rowCount, outbox: outbox.rowCount, releasedAt: releasedAtIso };
}

export class PublicationCohortAdministrator {
  constructor({ crawlerClient, businessClient, config, cohort }) {
    if (!crawlerClient?.query || !businessClient?.query) {
      throw new TypeError("Crawler and Business PostgreSQL clients are required");
    }
    this.crawler = crawlerClient;
    this.business = businessClient;
    this.config = config;
    this.cohort = cohort;
  }

  async inspect() {
    return inspectBoth(
      inspectSource(this.crawler, this.config, this.cohort),
      inspectBusiness(this.business, this.config, this.cohort),
    );
  }

  async inspectReadOnly() {
    return inspectBoth(
      readOnlyTransaction(
        this.crawler,
        () => inspectSource(this.crawler, this.config, this.cohort),
      ),
      readOnlyTransaction(
        this.business,
        () => inspectBusiness(this.business, this.config, this.cohort),
      ),
    );
  }

  async initialize() {
    const initial = await this.inspect();
    assertPublicationCohortState(initial, this.config, this.cohort, {
      complete: false,
      capture: "disabled",
      requireEmpty: true,
    });
    const source = await transaction(this.crawler, SOURCE_ADMIN_LOCK, async () => {
      const state = await inspectSource(this.crawler, this.config, this.cohort);
      assertPublicationCohortState(
        { source: state, business: initial.business },
        this.config,
        this.cohort,
        { complete: false, capture: "disabled", requireEmpty: true },
      );
      const stream = await this.crawler.query(
        `INSERT INTO publication.stream (
           publication_stream_id,source_deployment_key,source_identity_json,
           created_by,created_reason,status_changed_by,status_reason
         ) VALUES ($1::uuid,$2,$3::jsonb,$4,$5,$4,$5)
         ON CONFLICT (publication_stream_id) DO NOTHING`,
        [
          this.config.streamId,
          this.config.sourceDeploymentKey,
          JSON.stringify(this.config.sourceIdentity),
          this.config.actor,
          this.config.reason,
        ],
      );
      const ownership = await this.crawler.query(
        `INSERT INTO publication.channel_stream_state (
           publication_stream_id,channel_id,onboarding_mode,ownership_reference,
           state_changed_by,state_reason
         )
         SELECT $1::uuid,channel_id,'bootstrap',$3::jsonb,$4,$5
         FROM unnest($2::text[]) AS cohort(channel_id)
         ON CONFLICT (publication_stream_id,channel_id) DO NOTHING`,
        [
          this.config.streamId,
          this.cohort.channelIds,
          JSON.stringify(this.cohort.ownershipReference),
          this.config.actor,
          this.config.reason,
        ],
      );
      const delivery = await this.crawler.query(
        `INSERT INTO publication.channel_delivery_state (
           destination,publication_stream_id,channel_id,mode,source_ownership_reference,
           state_changed_by,state_reason
         )
         SELECT $3,$1::uuid,channel_id,'hold',$4::jsonb,$5,$6
         FROM unnest($2::text[]) AS cohort(channel_id)
         ON CONFLICT (destination,publication_stream_id,channel_id) DO NOTHING`,
        [
          this.config.streamId,
          this.cohort.channelIds,
          this.config.destination,
          JSON.stringify(this.cohort.ownershipReference),
          this.config.actor,
          this.config.reason,
        ],
      );
      const final = await inspectSource(this.crawler, this.config, this.cohort);
      validateStreamRows(final.streams, this.config, {
        business: false,
        allowMissing: false,
        capture: "disabled",
      });
      validateSourceOwnership(final.ownerships, this.config, this.cohort, { complete: true });
      validateDeliveries(final.deliveries, this.config, this.cohort, { complete: true });
      assertEmptyData(final, "Crawler");
      return { stream: stream.rowCount, ownership: ownership.rowCount, delivery: delivery.rowCount };
    });
    const business = await transaction(this.business, BUSINESS_ADMIN_LOCK, async () => {
      const state = await inspectBusiness(this.business, this.config, this.cohort);
      validateStreamRows(state.streams, this.config, {
        business: true,
        allowMissing: true,
        capture: "disabled",
      });
      validateBusinessOwnership(state.ownerships, this.config, this.cohort, { complete: false });
      assertEmptyData(state, "Business");
      const stream = await this.business.query(
        `INSERT INTO publication.stream (
           publication_stream_id,source_deployment_key,source_identity_json,
           accepted_contract_versions,registered_by,registered_reason,
           status_changed_by,status_reason
         ) VALUES ($1::uuid,$2,$3::jsonb,ARRAY[1,2]::integer[],$4,$5,$4,$5)
         ON CONFLICT (publication_stream_id) DO NOTHING`,
        [
          this.config.streamId,
          this.config.sourceDeploymentKey,
          JSON.stringify(this.config.sourceIdentity),
          this.config.actor,
          this.config.reason,
        ],
      );
      const ownership = await this.business.query(
        `INSERT INTO publication.channel_ownership (
           channel_id,active_publication_stream_id,status,previous_publication_stream_id,
           ownership_reference,projection_mode,state_changed_by,state_reason
         )
         SELECT channel_id,$1::uuid,'active',NULL,$3::jsonb,'held_shadow',$4,$5
         FROM unnest($2::text[]) AS cohort(channel_id)
         ON CONFLICT (channel_id) DO NOTHING`,
        [
          this.config.streamId,
          this.cohort.channelIds,
          JSON.stringify(this.cohort.ownershipReference),
          this.config.actor,
          this.config.reason,
        ],
      );
      const final = await inspectBusiness(this.business, this.config, this.cohort);
      validateStreamRows(final.streams, this.config, {
        business: true,
        allowMissing: false,
        capture: "disabled",
      });
      validateBusinessOwnership(final.ownerships, this.config, this.cohort, { complete: true });
      assertEmptyData(final, "Business");
      return { stream: stream.rowCount, ownership: ownership.rowCount };
    });
    const final = await this.inspect();
    assertPublicationCohortState(final, this.config, this.cohort, {
      complete: true,
      capture: "disabled",
      requireEmpty: true,
    });
    return { source, business, state: final };
  }

  async enableCapture() {
    const initial = await this.inspect();
    const existingStream = initial.source.streams.find(
      (row) => id(row.publication_stream_id) === this.config.streamId,
    );
    assertPublicationCohortState(initial, this.config, this.cohort, {
      complete: true,
      capture: "enableable",
      requireEmpty: existingStream?.capture_enabled_at == null,
    });
    if (existingStream.capture_enabled_at != null) {
      return {
        updated: 0,
        captureEnabledAt: existingStream.capture_enabled_at,
        state: initial,
      };
    }
    const result = await transaction(this.business, BUSINESS_ADMIN_LOCK, async () => {
      await this.business.query(
        "LOCK TABLE publication.stream, publication.channel_ownership IN SHARE MODE",
      );
      const business = await inspectBusiness(this.business, this.config, this.cohort);
      validateStreamRows(business.streams, this.config, {
        business: true,
        allowMissing: false,
        capture: "enableable",
      });
      validateBusinessOwnership(business.ownerships, this.config, this.cohort, { complete: true });
      return transaction(this.crawler, SOURCE_ADMIN_LOCK, async () => {
        // This creates the capture boundary: pre-existing writes finish before the
        // Stream switch, and subsequent writes observe the Writer Barrier.
        await this.crawler.query(
          `LOCK TABLE crawler.channels,crawler.contents,crawler.agent_profiles,
                      crawler.finalized_profiles IN SHARE MODE`,
        );
        await this.crawler.query(
          `SELECT publication_stream_id
           FROM publication.stream
           WHERE publication_stream_id=$1::uuid
           FOR UPDATE`,
          [this.config.streamId],
        );
        const source = await inspectSource(this.crawler, this.config, this.cohort);
        assertPublicationCohortState(
          { source, business },
          this.config,
          this.cohort,
          {
            complete: true,
            capture: "enableable",
            requireEmpty: source.streams.find(
              (row) => id(row.publication_stream_id) === this.config.streamId,
            )?.capture_enabled_at == null,
          },
        );
        const reason = JSON.stringify({
          action: "enable_publication_capture",
          cohort_key: this.cohort.cohortKey,
          channel_set_hash: this.cohort.channelSetHash,
          writer_deployment_ref: this.config.writerDeploymentRef,
          operator_reason: this.config.reason,
        });
        const updated = await this.crawler.query(
          `UPDATE publication.stream
           SET minimum_writer_version=$2,capture_enabled_at=now(),
               status_changed_at=now(),status_changed_by=$3,status_reason=$4
           WHERE publication_stream_id=$1::uuid AND capture_enabled_at IS NULL`,
          [this.config.streamId, this.config.writerVersion, this.config.actor, reason],
        );
        const final = await inspectSource(this.crawler, this.config, this.cohort);
        validateStreamRows(final.streams, this.config, {
          business: false,
          allowMissing: false,
          capture: "enabled",
        });
        validateSourceOwnership(final.ownerships, this.config, this.cohort, { complete: true });
        validateDeliveries(final.deliveries, this.config, this.cohort, { complete: true });
        const stream = final.streams.find(
          (row) => id(row.publication_stream_id) === this.config.streamId,
        );
        return { updated: updated.rowCount, captureEnabledAt: stream.capture_enabled_at };
      });
    });
    const final = await this.inspect();
    assertPublicationCohortState(final, this.config, this.cohort, {
      complete: true,
      capture: "enabled",
      requireEmpty: false,
    });
    return { ...result, state: final };
  }

  async releaseDelivery(evidence) {
    const initial = await this.inspectReadOnly();
    const initialPhase = assertPublicationDeliveryReleaseState(
      initial,
      this.config,
      this.cohort,
      evidence,
    );
    if (initialPhase === "released") {
      return {
        delivery: 0,
        outbox: 0,
        releasedAt: initial.source.deliveries[0]?.online_at ?? null,
        state: initial,
      };
    }
    let sourceCommitState = "not_started";
    let sourceCommitted = null;
    let result;
    try {
      result = await transaction(this.business, BUSINESS_ADMIN_LOCK, async () => {
        await this.business.query(
          `LOCK TABLE public.channels,publication.stream,publication.channel_ownership,
                      publication.inbox,publication.revision,publication.inbox_conflict,
                      publication.quarantine,publication.activation,publication.activation_item,
                      publication.consumer_cursor,publication.projection_outbox,
                      publication.reconciliation_state,result.entity_current,result.video_current,
                      result.content_current,result.agent_current IN SHARE MODE`,
        );
        const business = await inspectBusiness(this.business, this.config, this.cohort);
        sourceCommitted = await transaction(this.crawler, SOURCE_ADMIN_LOCK, async () => {
          const lockedDeliveries = await lockPublicationDeliveryRows(
            this.crawler,
            this.config,
            this.cohort,
          );
          const source = await inspectSource(this.crawler, this.config, this.cohort);
          assertPublicationDeliveryReleaseState(
            { source, business },
            this.config,
            this.cohort,
            evidence,
            { expectedPhase: "releaseable" },
          );
          const stateReason = JSON.stringify({
            ...deliveryReleaseReference(this.config, this.cohort, evidence),
            operator_reason: this.config.reason,
          });
          const updated = await updatePublicationDeliverySource(this.crawler, {
            config: this.config,
            cohort: this.cohort,
            lockedDeliveries,
            expectedOutboxCount: source.outbox.length,
            stateReason,
          });
          const finalSource = await inspectSource(this.crawler, this.config, this.cohort);
          assertPublicationDeliveryReleaseState(
            { source: finalSource, business },
            this.config,
            this.cohort,
            evidence,
            { expectedPhase: "released" },
          );
          sourceCommitState = "commit_pending";
          return updated;
        });
        sourceCommitState = "committed";
        return sourceCommitted;
      });
    } catch (error) {
      if (sourceCommitState === "committed") {
        throw new Error(
          `Publication Delivery release committed on Crawler but the Business release barrier failed: ${error.message}`,
          { cause: error },
        );
      }
      if (sourceCommitState === "commit_pending") {
        throw new Error(
          `Publication Delivery release may have committed on Crawler because the Source commit acknowledgement failed; rerun the read-only release plan before any other action: ${error.message}`,
          { cause: error },
        );
      }
      throw error;
    }
    const final = await this.inspectReadOnly();
    try {
      assertPublicationDeliveryReleaseState(final, this.config, this.cohort, evidence, {
        expectedPhase: "released",
      });
    } catch (error) {
      throw new Error(
        `Publication Delivery release committed but post-release verification failed: ${error.message}`,
        { cause: error },
      );
    }
    return { ...result, state: final };
  }
}

function valueCounts(rows, field) {
  const counts = {};
  for (const row of rows ?? []) {
    const value = String(row?.[field] ?? "unknown");
    counts[value] = (counts[value] ?? 0) + 1;
  }
  return counts;
}

export function publicationCohortSummary(state, config, cohort) {
  const sourceStream = state.source.streams.find(
    (row) => id(row.publication_stream_id) === config.streamId,
  );
  const businessStream = state.business.streams.find(
    (row) => id(row.publication_stream_id) === config.streamId,
  );
  return {
    publication_stream_id: cohort.streamId,
    cohort_key: cohort.cohortKey,
    destination: cohort.destination,
    channel_count: cohort.channelCount,
    channel_set_hash: cohort.channelSetHash,
    source_identity_hash: cohort.sourceIdentityHash,
    crawler: {
      database: state.source.identity.database_name,
      server_address: state.source.identity.server_address,
      server_port: state.source.identity.server_port,
      stream_registered: Boolean(sourceStream),
      capture_enabled_at: sourceStream?.capture_enabled_at ?? null,
      minimum_writer_version: sourceStream?.minimum_writer_version ?? null,
      ownership_count: state.source.ownerships.filter(
        (row) => id(row.publication_stream_id) === config.streamId,
      ).length,
      delivery_count: state.source.deliveries.length,
      seed_statuses: valueCounts(state.source.ownerships, "seed_status"),
      delivery_modes: valueCounts(state.source.deliveries, "mode"),
      current_readiness: valueCounts(state.source.domainCurrents, "readiness_status"),
      outbox_statuses: valueCounts(state.source.outbox, "status"),
      data_counts: state.source.dataCounts,
    },
    business: {
      database: state.business.identity.database_name,
      server_address: state.business.identity.server_address,
      server_port: state.business.identity.server_port,
      stream_registered: Boolean(businessStream),
      ownership_count: state.business.ownerships.filter(
        (row) => id(row.active_publication_stream_id) === config.streamId,
      ).length,
      projection_modes: valueCounts(state.business.ownerships, "projection_mode"),
      data_counts: state.business.dataCounts,
      target_data_counts: state.business.targetDataCounts ?? {},
    },
  };
}

export function publicationDeliveryReleaseSummary(state, config, cohort, evidence) {
  return {
    ...publicationCohortSummary(state, config, cohort),
    release_phase: releasePhase(state.source.deliveries, config, cohort),
    readiness_evidence: {
      report_version: evidence.report_version,
      report_as_of: evidence.report_as_of,
      evidence_hash: evidence.evidence_hash,
    },
    runtime_deployment_ref_hash: sha256(config.runtimeDeploymentRef),
  };
}
