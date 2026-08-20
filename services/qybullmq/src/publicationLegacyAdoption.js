import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { isDeepStrictEqual } from "node:util";
import { environmentValue } from "./runtimeEnvironment.js";
import {
  PUBLICATION_CONTRACT_VERSION,
} from "./publicationContract.js";
import { lockPublicationChannelMutation } from "./publicationChannelMutationLock.js";
import { generatePublicationReadinessReport } from "./publicationReadinessReport.js";
import { reconcilePublication } from "./publicationReconciler.js";
import {
  PUBLICATION_WRITER_VERSION,
  publicationWriterVersionSatisfies,
} from "./publicationWriterVersion.js";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_KEY = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const RESULT_HASH = /^sha256:[0-9a-f]{64}$/;
const DOMAINS = Object.freeze(["channel", "video", "agent"]);
const EVIDENCE_VERSION = "publication-legacy-adoption-evidence-v2";
const BUSINESS_ADMIN_LOCK = 781137243;
const ONBOARDING_MODE = "legacy_tracked_adoption";

export class PublicationLegacyAdoptionConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationLegacyAdoptionConflict";
    this.details = details;
  }
}

export class PublicationLegacyAdoptionPartialFailure extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationLegacyAdoptionPartialFailure";
    this.details = details;
  }
}

function fail(message, details = {}) {
  throw new PublicationLegacyAdoptionConflict(message, details);
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function safeKey(environment, name) {
  const value = requiredText(environment[name], name);
  if (!SAFE_KEY.test(value)) {
    throw new TypeError(`${name} must use only letters, numbers, dot, underscore, or hyphen`);
  }
  return value;
}

function explicitCount(environment, name) {
  const value = requiredText(environment[name], name);
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  return parsed;
}

function positiveInteger(value, fallback, field, maximum = 16) {
  if (value == null || String(value).trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new TypeError(`${field} must be an integer between 1 and ${maximum}`);
  }
  return parsed;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a timestamp`);
  return parsed.toISOString();
}

function optionalTimestamp(value, field) {
  return value == null ? null : timestamp(value, field);
}

function optionalText(value) {
  return value == null ? null : String(value);
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalJson);
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalJson(value[key])]),
  );
}

function sha256(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function canonicalHash(value) {
  return sha256(JSON.stringify(canonicalJson(value)));
}

function compareText(left, right) {
  const leftText = String(left);
  const rightText = String(right);
  if (leftText === rightText) return 0;
  return leftText < rightText ? -1 : 1;
}

function sortedUniqueChannelIds(channelIds) {
  if (!Array.isArray(channelIds) || channelIds.length === 0) {
    throw new TypeError("channelIds must be a non-empty array");
  }
  const normalized = channelIds.map((channelId) => requiredText(channelId, "channelId"));
  const unique = new Set(normalized);
  if (unique.size !== normalized.length) throw new TypeError("channelIds must be unique");
  return [...unique].sort(compareText);
}

export function publicationLegacyAdoptionConfig(environment = process.env, { apply = false } = {}) {
  const streamId = requiredText(environment.PUBLICATION_STREAM_ID, "PUBLICATION_STREAM_ID").toLowerCase();
  if (!UUID.test(streamId)) throw new TypeError("PUBLICATION_STREAM_ID must be a UUID");
  const expectedCrawlerDatabase = safeKey(environment, "EXPECTED_CRAWLER_DATABASE");
  const expectedBusinessDatabase = safeKey(environment, "EXPECTED_BUSINESS_DATABASE");
  if (expectedCrawlerDatabase === expectedBusinessDatabase) {
    throw new TypeError("Crawler and Business database names must be different");
  }
  const evidenceFile = String(environment.PUBLICATION_LEGACY_ADOPTION_EVIDENCE_FILE ?? "").trim() || null;
  if (apply && !evidenceFile) {
    throw new TypeError("PUBLICATION_LEGACY_ADOPTION_EVIDENCE_FILE is required with --apply");
  }
  return {
    crawlerDatabaseUrl: environmentValue("CRAWLER_DATABASE_URL", { environment }),
    businessDatabaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedCrawlerDatabase,
    expectedBusinessDatabase,
    expectedCrawlerChannelCount: explicitCount(environment, "EXPECTED_CRAWLER_CHANNEL_COUNT"),
    expectedBusinessChannelCount: explicitCount(environment, "EXPECTED_BUSINESS_CHANNEL_COUNT"),
    streamId,
    destination: safeKey(environment, "PUBLICATION_DESTINATION"),
    adoptionKey: safeKey(environment, "PUBLICATION_LEGACY_ADOPTION_KEY"),
    channelIdsFile: requiredText(
      environment.PUBLICATION_CHANNEL_IDS_FILE,
      "PUBLICATION_CHANNEL_IDS_FILE",
    ),
    asOf: timestamp(environment.PUBLICATION_LEGACY_ADOPTION_AS_OF, "PUBLICATION_LEGACY_ADOPTION_AS_OF"),
    actor: requiredText(environment.PUBLICATION_OPERATOR, "PUBLICATION_OPERATOR"),
    reason: requiredText(environment.PUBLICATION_ACTION_REASON, "PUBLICATION_ACTION_REASON"),
    concurrency: positiveInteger(
      environment.PUBLICATION_LEGACY_ADOPTION_CONCURRENCY,
      6,
      "PUBLICATION_LEGACY_ADOPTION_CONCURRENCY",
    ),
    evidenceFile,
  };
}

export function buildPublicationLegacyAdoptionTarget(config, channelIds) {
  const sorted = sortedUniqueChannelIds(channelIds);
  return {
    adoption_key: config.adoptionKey,
    publication_stream_id: config.streamId,
    destination: config.destination,
    as_of: config.asOf,
    channel_ids: sorted,
    channel_count: sorted.length,
    channel_set_hash: sha256(`${sorted.join("\n")}\n`),
  };
}

function normalizedReasons(issues) {
  return (Array.isArray(issues) ? issues : [])
    .map((issue) => canonicalJson(issue && typeof issue === "object" ? issue : {
      code: String(issue),
    }))
    .sort((left, right) => compareText(JSON.stringify(left), JSON.stringify(right)));
}

function domainEvidence(domain, candidate) {
  const ready = candidate?.ready === true;
  const resultHash = ready ? String(candidate?.result_hash ?? "") : null;
  if (ready && !RESULT_HASH.test(resultHash)) {
    fail("ready Publication Domain is missing a valid result hash", { domain });
  }
  return {
    domain,
    ready,
    result_hash: resultHash,
    readiness_reasons: ready ? [] : normalizedReasons(candidate?.issues),
  };
}

function finalizeByChannel(rows) {
  const output = new Map();
  for (const row of rows ?? []) {
    if (row.finalized_full_run_id == null) continue;
    output.set(String(row.channel_id), {
      run_id: requiredText(row.finalized_full_run_id, "finalized_full_run_id"),
      finalized_at: timestamp(row.finalized_at, "finalized_at"),
    });
  }
  return output;
}

function rowsByChannel(rows) {
  const output = new Map();
  for (const row of rows ?? []) {
    const channelId = String(row.channel_id);
    const values = output.get(channelId) ?? [];
    values.push(row);
    output.set(channelId, values);
  }
  return output;
}

function normalizeSourceOwnership(row) {
  return {
    publication_stream_id: String(row.publication_stream_id),
    status: requiredText(row.status, "source ownership status"),
    onboarding_mode: requiredText(row.onboarding_mode, "source ownership onboarding_mode"),
    seed_status: requiredText(row.seed_status, "source ownership seed_status"),
    ownership_reference: canonicalJson(row.ownership_reference ?? {}),
    owned_at: timestamp(row.owned_at, "source ownership owned_at"),
    seed_completed_at: optionalTimestamp(
      row.seed_completed_at,
      "source ownership seed_completed_at",
    ),
    sealed_at: optionalTimestamp(row.sealed_at, "source ownership sealed_at"),
  };
}

function normalizeBusinessOwnership(row) {
  return {
    active_publication_stream_id: String(row.active_publication_stream_id),
    status: requiredText(row.status, "business ownership status"),
    previous_publication_stream_id: optionalText(row.previous_publication_stream_id),
    projection_mode: requiredText(row.projection_mode, "business ownership projection_mode"),
    ownership_reference: canonicalJson(row.ownership_reference ?? {}),
  };
}

function normalizeDelivery(row) {
  return {
    destination: requiredText(row.destination, "delivery destination"),
    publication_stream_id: String(row.publication_stream_id),
    mode: requiredText(row.mode, "delivery mode"),
    source_ownership_reference: canonicalJson(row.source_ownership_reference ?? {}),
    online_at: optionalTimestamp(row.online_at, "delivery online_at"),
    sealed_at: optionalTimestamp(row.sealed_at, "delivery sealed_at"),
  };
}

function sortedSnapshots(rows, normalize) {
  return (rows ?? [])
    .map(normalize)
    .sort((left, right) => compareText(JSON.stringify(left), JSON.stringify(right)));
}

function expectedWrites(disposition, domains) {
  const adopt = disposition === "adopt";
  return {
    business_ownership: adopt,
    source_ownership: adopt,
    source_delivery: adopt,
    revisions: Object.fromEntries(DOMAINS.map((domain) => [
      domain,
      adopt && domains.find((item) => item.domain === domain)?.ready === true,
    ])),
  };
}

function expectedInsertCounts(channels) {
  const revisions = Object.fromEntries(DOMAINS.map((domain) => [domain, 0]));
  const counts = {
    business_ownerships: 0,
    source_ownerships: 0,
    source_deliveries: 0,
    revisions,
    outbox_rows: 0,
  };
  for (const channel of channels) {
    if (channel.expected_writes.business_ownership) counts.business_ownerships += 1;
    if (channel.expected_writes.source_ownership) counts.source_ownerships += 1;
    if (channel.expected_writes.source_delivery) counts.source_deliveries += 1;
    for (const domain of DOMAINS) {
      if (!channel.expected_writes.revisions[domain]) continue;
      counts.revisions[domain] += 1;
      counts.outbox_rows += 1;
    }
  }
  return counts;
}

function legacyRegistryEvidence(channel, captureEnabledAt) {
  const createdAt = timestamp(channel.created_at, "channel.created_at");
  const candidateId = optionalText(channel.registry_promotion_candidate_id);
  const promotionRunId = optionalText(channel.registry_promotion_run_id);
  const acceptedAt = optionalTimestamp(channel.promotion_accepted_at, "promotion.accepted_at");
  if ((candidateId == null) !== (promotionRunId == null)) {
    fail("Channel Registry Promotion evidence is incomplete", {
      channel_id: channel.channel_id,
    });
  }
  if (candidateId != null) {
    if (optionalText(channel.promotion_candidate_id) !== candidateId
        || channel.promotion_status !== "accepted"
        || acceptedAt == null) {
      fail("Channel Registry Promotion evidence is invalid", {
        channel_id: channel.channel_id,
      });
    }
    if (Date.parse(acceptedAt) >= Date.parse(captureEnabledAt)) {
      fail("Channel entered the Registry after Publication Capture was enabled", {
        channel_id: channel.channel_id,
        promotion_accepted_at: acceptedAt,
        capture_enabled_at: captureEnabledAt,
      });
    }
  } else {
    if (acceptedAt != null || channel.promotion_candidate_id != null) {
      fail("Channel without Registry Promotion pointers has Promotion evidence", {
        channel_id: channel.channel_id,
      });
    }
    if (Date.parse(createdAt) >= Date.parse(captureEnabledAt)) {
      fail("Channel without legacy Promotion evidence was created after Capture was enabled", {
        channel_id: channel.channel_id,
        registry_created_at: createdAt,
        capture_enabled_at: captureEnabledAt,
      });
    }
  }
  return {
    registry_created_at: createdAt,
    promotion_candidate_id: candidateId,
    promotion_run_id: promotionRunId,
    promotion_accepted_at: acceptedAt,
  };
}

function channelEvidenceRows({ target, source, captureEnabledAt, topologyByChannel }) {
  const channelRows = new Map((source.channels ?? []).map((row) => [String(row.channel_id), row]));
  const reportRows = new Map((source.readiness?.channels ?? []).map((row) => [String(row.channel_id), row]));
  const finalizes = finalizeByChannel(source.channels);
  return target.channel_ids.map((channelId) => {
    const channel = channelRows.get(channelId);
    if (!channel) fail("Crawler target Channel is missing", { channel_id: channelId });
    if (!new Set(["active", "dormant"]).has(channel.status)) {
      fail("Channel lifecycle is not eligible for Legacy Adoption", {
        channel_id: channelId,
        lifecycle_status: channel.status ?? null,
      });
    }
    const finalized = finalizes.get(channelId);
    if (!finalized) {
      fail("legacy Channel has no successful ready_auto Full Finalize evidence", {
        channel_id: channelId,
      });
    }
    if (Date.parse(finalized.finalized_at) > Date.parse(target.as_of)) {
      fail("legacy Channel Finalize is newer than the adoption boundary", {
        channel_id: channelId,
        finalized_at: finalized.finalized_at,
        adoption_as_of: target.as_of,
      });
    }
    const readiness = reportRows.get(channelId);
    if (!readiness) fail("Publication Readiness omitted a target Channel", { channel_id: channelId });
    const domains = DOMAINS.map((domain) => domainEvidence(domain, readiness.domains?.[domain]));
    const topology = topologyByChannel?.get(channelId);
    if (!topology) fail("Publication topology omitted a target Channel", { channel_id: channelId });
    return {
      channel_id: channelId,
      lifecycle_status: requiredText(channel.status, "channel.status"),
      registry: legacyRegistryEvidence(channel, captureEnabledAt),
      finalized_full_run_id: finalized.run_id,
      finalized_at: finalized.finalized_at,
      domains,
      topology,
      expected_writes: expectedWrites(topology.disposition, domains),
    };
  });
}

export function buildPublicationLegacyAdoptionEvidence({ config, target, source, business }) {
  const sourceStream = source.streams?.[0];
  const captureEnabledAt = timestamp(
    sourceStream?.capture_enabled_at,
    "publication.stream.capture_enabled_at",
  );
  const topologyByChannel = validateOwnershipTopology(source, business, target);
  const channels = channelEvidenceRows({
    target,
    source,
    captureEnabledAt,
    topologyByChannel,
  });
  const body = {
    evidence_version: EVIDENCE_VERSION,
    publication_stream_id: target.publication_stream_id,
    destination: target.destination,
    adoption_key: target.adoption_key,
    adoption_as_of: target.as_of,
    channel_count: target.channel_count,
    channel_set_hash: target.channel_set_hash,
    contract_version: PUBLICATION_CONTRACT_VERSION,
    writer_version: PUBLICATION_WRITER_VERSION,
    capture_enabled_at: captureEnabledAt,
    operator: config.actor,
    operator_reason: config.reason,
    expected_inserts: expectedInsertCounts(channels),
    channels,
  };
  return { ...body, evidence_hash: canonicalHash(body) };
}

function validateEvidenceTopology(row, target) {
  const topology = row?.topology;
  if (!topology || Array.isArray(topology) || typeof topology !== "object") {
    fail("Publication Legacy Adoption evidence topology is invalid", {
      channel_id: row?.channel_id ?? null,
    });
  }
  if (!new Set(["adopt", "preexisting"]).has(topology.disposition)) {
    fail("Publication Legacy Adoption evidence disposition is invalid", {
      channel_id: row.channel_id,
    });
  }
  if (!Array.isArray(topology.source_ownerships) || !Array.isArray(topology.deliveries)) {
    fail("Publication Legacy Adoption evidence topology arrays are invalid", {
      channel_id: row.channel_id,
    });
  }
  const sourceOwnerships = sortedSnapshots(topology.source_ownerships, normalizeSourceOwnership);
  const deliveries = sortedSnapshots(topology.deliveries, normalizeDelivery);
  const businessOwnership = topology.business_ownership == null
    ? null
    : normalizeBusinessOwnership(topology.business_ownership);
  if (!isDeepStrictEqual(sourceOwnerships, topology.source_ownerships)
      || !isDeepStrictEqual(deliveries, topology.deliveries)
      || !isDeepStrictEqual(businessOwnership, topology.business_ownership)) {
    fail("Publication Legacy Adoption evidence topology is not canonical", {
      channel_id: row.channel_id,
    });
  }
  const active = sourceOwnerships.filter((owner) => owner.status === "owned");
  if (topology.disposition === "adopt") {
    if (active.length !== 0 || businessOwnership != null
        || sourceOwnerships.some((owner) => owner.publication_stream_id === target.publication_stream_id)
        || deliveries.some((delivery) => delivery.mode !== "sealed")) {
      fail("Publication Legacy Adoption evidence adopt topology is not empty", {
        channel_id: row.channel_id,
      });
    }
  } else {
    if (active.length !== 1
        || active[0].publication_stream_id !== target.publication_stream_id
        || active[0].ownership_reference?.onboarding_mode === ONBOARDING_MODE
        || businessOwnership?.active_publication_stream_id !== target.publication_stream_id
        || businessOwnership.status !== "active"
        || !new Set(["held_shadow", "online"]).has(businessOwnership.projection_mode)) {
      fail("Publication Legacy Adoption evidence pre-existing topology is invalid", {
        channel_id: row.channel_id,
      });
    }
    validateDeliveryTopology(deliveries, active[0], target, row.channel_id);
  }
  const expected = expectedWrites(topology.disposition, row.domains ?? []);
  if (!isDeepStrictEqual(row.expected_writes, expected)) {
    fail("Publication Legacy Adoption evidence expected writes mismatch", {
      channel_id: row.channel_id,
    });
  }
}

function validateEvidenceShape(evidence, config, target) {
  if (!evidence || Array.isArray(evidence) || typeof evidence !== "object") {
    throw new TypeError("Publication Legacy Adoption evidence must be an object");
  }
  const expected = {
    evidence_version: EVIDENCE_VERSION,
    publication_stream_id: target.publication_stream_id,
    destination: target.destination,
    adoption_key: target.adoption_key,
    adoption_as_of: target.as_of,
    channel_count: target.channel_count,
    channel_set_hash: target.channel_set_hash,
    contract_version: PUBLICATION_CONTRACT_VERSION,
    writer_version: PUBLICATION_WRITER_VERSION,
    operator: config.actor,
    operator_reason: config.reason,
  };
  for (const [field, value] of Object.entries(expected)) {
    if (!isDeepStrictEqual(evidence[field], value)) {
      fail(`Publication Legacy Adoption evidence ${field} mismatch`, {
        actual: evidence[field],
        expected: value,
      });
    }
  }
  if (!Array.isArray(evidence.channels) || evidence.channels.length !== target.channel_count) {
    fail("Publication Legacy Adoption evidence Channel count mismatch");
  }
  const channelIds = evidence.channels.map((row) => String(row?.channel_id ?? ""));
  if (!isDeepStrictEqual(channelIds, target.channel_ids)) {
    fail("Publication Legacy Adoption evidence Channel set mismatch");
  }
  const captureEnabledAt = timestamp(
    evidence.capture_enabled_at,
    "evidence.capture_enabled_at",
  );
  if (captureEnabledAt !== evidence.capture_enabled_at) {
    fail("Publication Legacy Adoption evidence Capture timestamp is not canonical");
  }
  for (const row of evidence.channels) {
    requiredText(row.finalized_full_run_id, "evidence.finalized_full_run_id");
    const finalizedAt = timestamp(row.finalized_at, "evidence.finalized_at");
    if (Date.parse(finalizedAt) > Date.parse(target.as_of)) {
      fail("Publication Legacy Adoption evidence Finalize exceeds the adoption boundary", {
        channel_id: row.channel_id,
      });
    }
    if (!new Set(["active", "dormant"]).has(row.lifecycle_status)) {
      fail("Publication Legacy Adoption evidence lifecycle is not adoptable", {
        channel_id: row.channel_id,
        lifecycle_status: row.lifecycle_status,
      });
    }
    const registry = row.registry;
    if (!registry || Array.isArray(registry) || typeof registry !== "object") {
      fail("Publication Legacy Adoption evidence Registry entry is invalid", {
        channel_id: row.channel_id,
      });
    }
    const canonicalRegistry = {
      registry_created_at: timestamp(
        registry.registry_created_at,
        "evidence.registry.registry_created_at",
      ),
      promotion_candidate_id: optionalText(registry.promotion_candidate_id),
      promotion_run_id: optionalText(registry.promotion_run_id),
      promotion_accepted_at: optionalTimestamp(
        registry.promotion_accepted_at,
        "evidence.registry.promotion_accepted_at",
      ),
    };
    if (!isDeepStrictEqual(registry, canonicalRegistry)) {
      fail("Publication Legacy Adoption evidence Registry entry is not canonical", {
        channel_id: row.channel_id,
      });
    }
    if ((registry.promotion_candidate_id == null) !== (registry.promotion_run_id == null)
        || (registry.promotion_candidate_id == null) !== (registry.promotion_accepted_at == null)) {
      fail("Publication Legacy Adoption evidence Registry Promotion is incomplete", {
        channel_id: row.channel_id,
      });
    }
    const enteredAt = registry.promotion_accepted_at ?? registry.registry_created_at;
    if (Date.parse(enteredAt) >= Date.parse(captureEnabledAt)) {
      fail("Publication Legacy Adoption evidence is not from before Capture", {
        channel_id: row.channel_id,
      });
    }
    if (!Array.isArray(row.domains) || !isDeepStrictEqual(
      row.domains.map((domain) => domain.domain),
      DOMAINS,
    )) {
      fail("Publication Legacy Adoption evidence Domain set mismatch", {
        channel_id: row.channel_id,
      });
    }
    for (const domain of row.domains) {
      if (typeof domain.ready !== "boolean") fail("evidence Domain readiness must be boolean");
      if (domain.ready && !RESULT_HASH.test(String(domain.result_hash ?? ""))) {
        fail("evidence ready Domain result hash is invalid", {
          channel_id: row.channel_id,
          domain: domain.domain,
        });
      }
      if (!Array.isArray(domain.readiness_reasons)) {
        fail("evidence Domain readiness reasons must be an array");
      }
      if (domain.ready && domain.readiness_reasons.length !== 0) {
        fail("evidence ready Domain cannot contain readiness reasons");
      }
      if (!domain.ready && domain.result_hash !== null) {
        fail("evidence NotReady Domain cannot contain a result hash");
      }
      if (!isDeepStrictEqual(
        domain.readiness_reasons,
        normalizedReasons(domain.readiness_reasons),
      )) {
        fail("evidence Domain readiness reasons are not canonical");
      }
    }
    validateEvidenceTopology(row, target);
  }
  const insertCounts = expectedInsertCounts(evidence.channels);
  if (!isDeepStrictEqual(evidence.expected_inserts, insertCounts)) {
    fail("Publication Legacy Adoption evidence expected insert counts mismatch", {
      actual: evidence.expected_inserts,
      expected: insertCounts,
    });
  }
  const { evidence_hash: claimedHash, ...body } = evidence;
  const actualHash = canonicalHash(body);
  if (claimedHash !== actualHash) {
    fail("Publication Legacy Adoption evidence hash mismatch", {
      actual: claimedHash,
      expected: actualHash,
    });
  }
  if (config.expectedCrawlerDatabase === config.expectedBusinessDatabase) {
    fail("Crawler and Business database identities must differ");
  }
  return evidence;
}

export async function readPublicationLegacyAdoptionEvidence(path, config, target) {
  let parsed;
  try {
    parsed = JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    throw new TypeError("cannot read Publication Legacy Adoption evidence JSON", { cause: error });
  }
  return validateEvidenceShape(parsed.evidence ?? parsed, config, target);
}

export function publicationLegacyAdoptionConfirmation(config, target, evidence) {
  validateEvidenceShape(evidence, config, target);
  return [
    "ADOPT_LEGACY_TRACKED_CHANNELS",
    config.expectedCrawlerDatabase,
    config.expectedBusinessDatabase,
    target.publication_stream_id,
    target.destination,
    target.adoption_key,
    String(target.channel_count),
    target.channel_set_hash,
    sha256(target.as_of),
    evidence.evidence_hash,
  ].join(":");
}

async function beginReadOnly(client) {
  await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
  await client.query("SET LOCAL statement_timeout = '180s'");
}

async function inReadOnlyTransaction(pool, action) {
  const client = await pool.connect();
  let begun = false;
  try {
    await beginReadOnly(client);
    begun = true;
    const result = await action(client);
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function inTransaction(pool, lockId, action) {
  const client = await pool.connect();
  let begun = false;
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
    begun = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '180s'");
    if (lockId != null) await client.query("SELECT pg_advisory_xact_lock($1)", [lockId]);
    const result = await action(client);
    await client.query("COMMIT");
    begun = false;
    return result;
  } catch (error) {
    if (begun) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function sourcePlanState(client, config, target) {
  const identity = (await client.query(
    `/* publication-legacy-adoption:source-identity */
     SELECT current_database() AS database_name,
            inet_server_addr()::text AS server_address,
            inet_server_port() AS server_port,
            (SELECT count(*)::int FROM crawler.channels) AS channel_count,
            to_regclass('publication.stream') IS NOT NULL AS stream_ready,
            to_regclass('publication.channel_stream_state') IS NOT NULL AS ownership_ready,
            to_regclass('publication.channel_delivery_state') IS NOT NULL AS delivery_ready,
            to_regclass('publication.domain_current') IS NOT NULL AS current_ready,
            to_regclass('publication.revision') IS NOT NULL AS revision_ready,
            to_regclass('publication.outbox') IS NOT NULL AS outbox_ready`,
  )).rows[0] ?? {};
  const streams = (await client.query(
    `/* publication-legacy-adoption:source-stream */
     SELECT publication_stream_id,source_deployment_key,source_identity_json,status,
            minimum_writer_version,capture_enabled_at
     FROM publication.stream WHERE publication_stream_id=$1::uuid`,
    [target.publication_stream_id],
  )).rows;
  const channels = (await client.query(
    `/* publication-legacy-adoption:source-channels */
     SELECT target.channel_id,channel.status,channel.created_at,
            channel.registry_promotion_candidate_id::text AS registry_promotion_candidate_id,
            channel.registry_promotion_run_id,
            promotion.candidate_id::text AS promotion_candidate_id,
            promotion.status AS promotion_status,
            promotion.accepted_at AS promotion_accepted_at,
            finalized.run_id AS finalized_full_run_id,
            finalized.publication_finalized_at AS finalized_at
     FROM unnest($1::text[]) WITH ORDINALITY AS target(channel_id,position)
     LEFT JOIN crawler.channels AS channel ON channel.channel_id=target.channel_id
     LEFT JOIN crawler.channel_candidates AS promotion
       ON promotion.candidate_id=channel.registry_promotion_candidate_id
      AND promotion.channel_id=channel.channel_id
     LEFT JOIN LATERAL (
       SELECT run.run_id,run.publication_finalized_at
       FROM crawler.channel_runs AS run
       WHERE run.channel_id=target.channel_id
         AND run.crawl_mode='full'
         AND run.publication_finalized_status='ready_auto'
         AND run.publication_finalized_at IS NOT NULL
       ORDER BY run.publication_finalized_at DESC,run.run_id DESC
       LIMIT 1
     ) AS finalized ON true
     ORDER BY target.position`,
    [target.channel_ids],
  )).rows;
  const ownerships = (await client.query(
    `/* publication-legacy-adoption:source-ownerships */
     SELECT publication_stream_id,channel_id,status,onboarding_mode,seed_status,
            ownership_reference,owned_at,seed_completed_at,sealed_at
     FROM publication.channel_stream_state
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id,owned_at,publication_stream_id`,
    [target.channel_ids],
  )).rows;
  const deliveries = (await client.query(
    `/* publication-legacy-adoption:source-deliveries */
     SELECT destination,publication_stream_id,channel_id,mode,source_ownership_reference,
            online_at,sealed_at
     FROM publication.channel_delivery_state
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id,destination,publication_stream_id`,
    [target.channel_ids],
  )).rows;
  const readiness = await generatePublicationReadinessReport({
    crawlerQuery: client.query.bind(client),
    businessQuery: async () => ({ rows: [] }),
    asOf: target.as_of,
    channelIds: target.channel_ids,
  });
  return { identity, streams, channels, ownerships, deliveries, readiness };
}

async function businessPlanState(client, config, target) {
  const identity = (await client.query(
    `/* publication-legacy-adoption:business-identity */
     SELECT current_database() AS database_name,
            inet_server_addr()::text AS server_address,
            inet_server_port() AS server_port,
            (SELECT count(*)::int FROM public.channels) AS channel_count,
            to_regclass('publication.stream') IS NOT NULL AS stream_ready,
            to_regclass('publication.channel_ownership') IS NOT NULL AS ownership_ready,
            to_regclass('publication.inbox') IS NOT NULL AS inbox_ready,
            to_regclass('publication.consumer_cursor') IS NOT NULL AS cursor_ready,
            to_regclass('result.entity_current') IS NOT NULL AS entity_current_ready,
            to_regclass('result.video_current') IS NOT NULL AS video_current_ready`,
  )).rows[0] ?? {};
  const streams = (await client.query(
    `/* publication-legacy-adoption:business-stream */
     SELECT publication_stream_id,source_deployment_key,source_identity_json,status,
            accepted_contract_versions
     FROM publication.stream WHERE publication_stream_id=$1::uuid`,
    [target.publication_stream_id],
  )).rows;
  const ownerships = (await client.query(
    `/* publication-legacy-adoption:business-ownerships */
     SELECT channel_id,active_publication_stream_id,status,previous_publication_stream_id,
            ownership_reference,projection_mode
     FROM publication.channel_ownership
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id`,
    [target.channel_ids],
  )).rows;
  return { identity, streams, ownerships };
}

function validateIdentity(identity, expectedDatabase, expectedCount, side) {
  if (identity.database_name !== expectedDatabase) {
    fail(`${side} database identity mismatch`, {
      actual: identity.database_name,
      expected: expectedDatabase,
    });
  }
  if (Number(identity.channel_count) !== expectedCount) {
    fail(`${side} Channel count mismatch`, {
      actual: Number(identity.channel_count),
      expected: expectedCount,
    });
  }
  const missing = Object.entries(identity)
    .filter(([name, value]) => name.endsWith("_ready") && value !== true)
    .map(([name]) => name);
  if (missing.length > 0) fail(`${side} Publication schema preflight failed`, { missing });
}

function businessOwnerByChannel(rows) {
  const output = new Map();
  for (const row of rows ?? []) {
    const channelId = String(row.channel_id);
    if (output.has(channelId)) fail("duplicate Business Publication ownership found", { channel_id: channelId });
    output.set(channelId, row);
  }
  return output;
}

function validateStreams(source, business, config, target) {
  if (source.streams.length !== 1 || business.streams.length !== 1) {
    fail("Publication Stream must already exist exactly once in both databases");
  }
  const sourceStream = source.streams[0];
  const businessStream = business.streams[0];
  if (String(sourceStream.publication_stream_id) !== target.publication_stream_id
      || String(businessStream.publication_stream_id) !== target.publication_stream_id) {
    fail("Publication Stream identity mismatch");
  }
  if (sourceStream.status !== "active" || businessStream.status !== "active") {
    fail("Publication Stream must be active in both databases");
  }
  if (!sourceStream.capture_enabled_at) fail("Crawler Publication Capture is not enabled");
  if (!publicationWriterVersionSatisfies(
    PUBLICATION_WRITER_VERSION,
    sourceStream.minimum_writer_version,
  )) {
    fail("deployed Publication writer does not satisfy the Stream minimum", {
      writer_version: PUBLICATION_WRITER_VERSION,
      minimum_writer_version: sourceStream.minimum_writer_version,
    });
  }
  if (sourceStream.source_deployment_key !== businessStream.source_deployment_key
      || !isDeepStrictEqual(sourceStream.source_identity_json, businessStream.source_identity_json)) {
    fail("Crawler and Business Publication Stream identities diverge");
  }
  const accepted = (businessStream.accepted_contract_versions ?? []).map(Number);
  if (!accepted.includes(PUBLICATION_CONTRACT_VERSION)) {
    fail("Business Publication Stream does not accept the current contract version");
  }
}

function validateDeliveryTopology(deliveries, sourceOwner, target, channelId) {
  const expected = (deliveries ?? []).filter((row) => (
    String(row.publication_stream_id) === target.publication_stream_id
    && row.destination === target.destination
  ));
  const unexpected = (deliveries ?? []).filter((row) => (
    row.mode !== "sealed"
    && !(
      String(row.publication_stream_id) === target.publication_stream_id
      && row.destination === target.destination
    )
  ));
  if (expected.length !== 1 || expected[0].mode !== "online" || unexpected.length > 0) {
    fail("Crawler ownership does not have the exact online Delivery route", {
      channel_id: channelId,
      deliveries,
    });
  }
  if (!isDeepStrictEqual(
    canonicalJson(expected[0].source_ownership_reference ?? {}),
    canonicalJson(sourceOwner.ownership_reference ?? {}),
  )) {
    fail("Crawler Delivery is not bound to its Source ownership reference", {
      channel_id: channelId,
      destination: target.destination,
    });
  }
  return expected[0];
}

function validateOwnershipTopology(source, business, target) {
  const sourceByChannel = rowsByChannel(source.ownerships);
  const businessOwners = businessOwnerByChannel(business?.ownerships);
  const deliveriesByChannel = rowsByChannel(source.deliveries);
  const topologyByChannel = new Map();
  for (const channelId of target.channel_ids) {
    const sourceOwnerships = sourceByChannel.get(channelId) ?? [];
    const active = sourceOwnerships.filter((row) => row.status === "owned");
    if (active.length > 1) {
      fail("multiple owned Publication Streams found", { channel_id: channelId });
    }
    const sourceOwner = active[0] ?? null;
    const businessOwner = businessOwners.get(channelId);
    if (sourceOwner && String(sourceOwner.publication_stream_id) !== target.publication_stream_id) {
      fail("Crawler Channel is owned by another Publication Stream", {
        channel_id: channelId,
        publication_stream_id: sourceOwner.publication_stream_id,
      });
    }
    if (businessOwner && String(businessOwner.active_publication_stream_id) !== target.publication_stream_id) {
      fail("Business Channel is owned by another Publication Stream", {
        channel_id: channelId,
        publication_stream_id: businessOwner.active_publication_stream_id,
      });
    }
    if (businessOwner && businessOwner.status !== "active") {
      fail("Business Channel ownership is not active", { channel_id: channelId });
    }
    if (businessOwner && !new Set(["held_shadow", "online"]).has(businessOwner.projection_mode)) {
      fail("Business Channel projection mode is invalid", { channel_id: channelId });
    }
    if (sourceOwner && !businessOwner) {
      fail("Crawler ownership exists without Business ownership", { channel_id: channelId });
    }
    const deliveries = deliveriesByChannel.get(channelId) ?? [];
    if (sourceOwner) {
      if (sourceOwner.ownership_reference?.onboarding_mode === ONBOARDING_MODE) {
        fail("Legacy Adoption ownership already exists; resume with its original Evidence", {
          channel_id: channelId,
          evidence_hash: sourceOwner.ownership_reference?.evidence_hash ?? null,
        });
      }
      validateDeliveryTopology(deliveries, sourceOwner, target, channelId);
    } else {
      if (businessOwner) {
        fail("Business ownership exists without Crawler ownership; resume its original operation", {
          channel_id: channelId,
        });
      }
      if (sourceOwnerships.some((row) => (
        String(row.publication_stream_id) === target.publication_stream_id
      ))) {
        fail("Crawler Stream already has sealed ownership for the Channel", {
          channel_id: channelId,
        });
      }
      if (deliveries.some((row) => row.mode !== "sealed")) {
        fail("Crawler Delivery exists without active ownership", { channel_id: channelId });
      }
    }
    topologyByChannel.set(channelId, {
      disposition: sourceOwner ? "preexisting" : "adopt",
      source_ownerships: sortedSnapshots(sourceOwnerships, normalizeSourceOwnership),
      business_ownership: businessOwner ? normalizeBusinessOwnership(businessOwner) : null,
      deliveries: sortedSnapshots(deliveries, normalizeDelivery),
    });
  }
  return topologyByChannel;
}

function validatePlanState(state, config, target) {
  validateIdentity(
    state.source.identity,
    config.expectedCrawlerDatabase,
    config.expectedCrawlerChannelCount,
    "Crawler",
  );
  validateIdentity(
    state.business.identity,
    config.expectedBusinessDatabase,
    config.expectedBusinessChannelCount,
    "Business",
  );
  validateStreams(state.source, state.business, config, target);
  return validateOwnershipTopology(state.source, state.business, target);
}

function evidenceChannelMap(evidence) {
  return new Map(evidence.channels.map((row) => [row.channel_id, row]));
}

function ownershipReference(config, target, evidence, channel) {
  return {
    onboarding_mode: ONBOARDING_MODE,
    adoption_key: target.adoption_key,
    channel_set_hash: target.channel_set_hash,
    evidence_hash: evidence.evidence_hash,
    adoption_as_of: target.as_of,
    finalized_full_run_id: channel.finalized_full_run_id,
    finalized_at: channel.finalized_at,
    registry: channel.registry,
    capture_enabled_at: evidence.capture_enabled_at,
    operator: config.actor,
    operator_reason: config.reason,
  };
}

function stateReason(config, target, evidence) {
  return JSON.stringify({
    action: "adopt_legacy_tracked_channels",
    adoption_key: target.adoption_key,
    channel_set_hash: target.channel_set_hash,
    evidence_hash: evidence.evidence_hash,
    adoption_as_of: target.as_of,
    operator_reason: config.reason,
  });
}

function readinessCounts(evidence) {
  const ready = Object.fromEntries(DOMAINS.map((domain) => [domain, 0]));
  let any = 0;
  let channelAndVideo = 0;
  let complete = 0;
  for (const channel of evidence.channels) {
    const byDomain = new Map(channel.domains.map((domain) => [domain.domain, domain]));
    for (const domain of DOMAINS) if (byDomain.get(domain)?.ready) ready[domain] += 1;
    if (DOMAINS.some((domain) => byDomain.get(domain)?.ready)) any += 1;
    if (byDomain.get("channel")?.ready && byDomain.get("video")?.ready) channelAndVideo += 1;
    if (DOMAINS.every((domain) => byDomain.get(domain)?.ready)) complete += 1;
  }
  return { ready, any, channelAndVideo, complete };
}

export function publicationLegacyAdoptionSummary(state, target, evidence) {
  const readiness = readinessCounts(evidence);
  const preexisting = evidence.channels.filter((channel) => (
    channel.topology.disposition === "preexisting"
  )).length;
  return {
    publication_stream_id: target.publication_stream_id,
    destination: target.destination,
    adoption_key: target.adoption_key,
    adoption_as_of: target.as_of,
    channel_count: target.channel_count,
    channel_set_hash: target.channel_set_hash,
    evidence_hash: evidence.evidence_hash,
    finalized_channel_count: evidence.channels.length,
    domain_ready: readiness.ready,
    channels_with_any_ready_domain: readiness.any,
    waiting_for_first_ready_domain: target.channel_count - readiness.any,
    channel_and_video_ready: readiness.channelAndVideo,
    complete_three_domain_package: readiness.complete,
    expected_inserts: evidence.expected_inserts,
    crawler: {
      database: state.source.identity.database_name,
      server_address: state.source.identity.server_address,
      server_port: state.source.identity.server_port,
      preexisting_ownership_count: preexisting,
      ownerships_to_register: evidence.expected_inserts.source_ownerships,
    },
    business: {
      database: state.business.identity.database_name,
      server_address: state.business.identity.server_address,
      server_port: state.business.identity.server_port,
      preexisting_ownership_count: preexisting,
      ownerships_to_register: evidence.expected_inserts.business_ownerships,
      projection_mode_for_new_ownerships: "held_shadow",
    },
    crawler_refetch_required: false,
  };
}

function exactBusinessLegacyOwner(owner, target, reference) {
  return owner != null
    && String(owner.active_publication_stream_id) === target.publication_stream_id
    && owner.status === "active"
    && owner.previous_publication_stream_id == null
    && owner.projection_mode === "held_shadow"
    && isDeepStrictEqual(canonicalJson(owner.ownership_reference ?? {}), reference);
}

function exactSourceLegacyOwner(owner, target, reference) {
  return owner != null
    && String(owner.publication_stream_id) === target.publication_stream_id
    && owner.status === "owned"
    && owner.onboarding_mode === "bootstrap"
    && owner.ownership_reference?.onboarding_mode === ONBOARDING_MODE
    && isDeepStrictEqual(canonicalJson(owner.ownership_reference ?? {}), reference);
}

function validateApprovedSourceTopology(sourceRows, deliveries, config, target, evidence, expected) {
  const channelId = expected.channel_id;
  const topology = expected.topology;
  if (topology.disposition === "preexisting") {
    if (!isDeepStrictEqual(
      sortedSnapshots(sourceRows, normalizeSourceOwnership),
      topology.source_ownerships,
    ) || !isDeepStrictEqual(
      sortedSnapshots(deliveries, normalizeDelivery),
      topology.deliveries,
    )) {
      fail("pre-existing Crawler topology changed after the approved plan", {
        channel_id: channelId,
      });
    }
    const active = sourceRows.filter((row) => row.status === "owned");
    if (active.length !== 1) {
      fail("pre-existing Crawler ownership changed after the approved plan", {
        channel_id: channelId,
      });
    }
    validateDeliveryTopology(deliveries, active[0], target, channelId);
    return { stage: "preexisting", sourceOwner: active[0], delivery: null };
  }

  const reference = ownershipReference(config, target, evidence, expected);
  const legacyOwners = sourceRows.filter((row) => exactSourceLegacyOwner(row, target, reference));
  if (legacyOwners.length > 1) {
    fail("duplicate Legacy Adoption Source ownership found", { channel_id: channelId });
  }
  const sourceOwner = legacyOwners[0] ?? null;
  const baseSourceRows = sourceOwner
    ? sourceRows.filter((row) => row !== sourceOwner)
    : sourceRows;
  if (!isDeepStrictEqual(
    sortedSnapshots(baseSourceRows, normalizeSourceOwnership),
    topology.source_ownerships,
  )) {
    fail("Crawler ownership topology no longer matches the approved Evidence", {
      channel_id: channelId,
    });
  }

  const expectedDeliveries = deliveries.filter((row) => (
    String(row.publication_stream_id) === target.publication_stream_id
    && row.destination === target.destination
    && row.mode === "online"
    && isDeepStrictEqual(
      canonicalJson(row.source_ownership_reference ?? {}),
      reference,
    )
  ));
  if (expectedDeliveries.length > 1) {
    fail("duplicate Legacy Adoption Delivery found", { channel_id: channelId });
  }
  const expectedDelivery = expectedDeliveries[0] ?? null;
  const baseDeliveries = expectedDelivery
    ? deliveries.filter((row) => row !== expectedDelivery)
    : deliveries;
  if (!isDeepStrictEqual(
    sortedSnapshots(baseDeliveries, normalizeDelivery),
    topology.deliveries,
  )) {
    fail("Crawler Delivery topology no longer matches the approved Evidence", {
      channel_id: channelId,
    });
  }
  if (sourceOwner) {
    if (!expectedDelivery) {
      fail("completed Source adoption is missing its Delivery", { channel_id: channelId });
    }
    validateDeliveryTopology(deliveries, sourceOwner, target, channelId);
    return { stage: "completed", sourceOwner, delivery: expectedDelivery };
  }
  if (expectedDelivery) {
    fail("Legacy Adoption Delivery exists without its Source ownership", {
      channel_id: channelId,
    });
  }
  return { stage: "pending", sourceOwner: null, delivery: null };
}

function validateApprovedTopology(state, config, target, evidence) {
  const sourceByChannel = rowsByChannel(state.source.ownerships);
  const deliveriesByChannel = rowsByChannel(state.source.deliveries);
  const businessOwners = businessOwnerByChannel(state.business.ownerships);
  const stages = new Map();
  for (const expected of evidence.channels) {
    const channelId = expected.channel_id;
    const sourceRows = sourceByChannel.get(channelId) ?? [];
    const deliveries = deliveriesByChannel.get(channelId) ?? [];
    const businessOwner = businessOwners.get(channelId) ?? null;
    const topology = expected.topology;
    const source = validateApprovedSourceTopology(
      sourceRows,
      deliveries,
      config,
      target,
      evidence,
      expected,
    );
    if (topology.disposition === "preexisting") {
      if (!isDeepStrictEqual(
        businessOwner ? normalizeBusinessOwnership(businessOwner) : null,
        topology.business_ownership,
      )) {
        fail("pre-existing Publication topology changed after the approved plan", {
          channel_id: channelId,
        });
      }
      stages.set(channelId, "preexisting");
      continue;
    }

    const reference = ownershipReference(config, target, evidence, expected);
    if (businessOwner && !exactBusinessLegacyOwner(businessOwner, target, reference)) {
      fail("Business ownership is not bound to the approved Legacy Adoption Evidence", {
        channel_id: channelId,
      });
    }
    if (source.stage === "completed") {
      if (!businessOwner) {
        fail("completed Source adoption is missing its Business owner or Delivery", {
          channel_id: channelId,
        });
      }
      stages.set(channelId, "completed");
    } else {
      stages.set(channelId, businessOwner ? "business_registered" : "pending");
    }
  }
  return stages;
}

function validateLiveStateAgainstEvidence(state, config, target, evidence) {
  validateIdentity(
    state.source.identity,
    config.expectedCrawlerDatabase,
    config.expectedCrawlerChannelCount,
    "Crawler",
  );
  validateIdentity(
    state.business.identity,
    config.expectedBusinessDatabase,
    config.expectedBusinessChannelCount,
    "Business",
  );
  validateStreams(state.source, state.business, config, target);
  const captureEnabledAt = timestamp(
    state.source.streams[0].capture_enabled_at,
    "publication.stream.capture_enabled_at",
  );
  if (captureEnabledAt !== evidence.capture_enabled_at) {
    fail("Publication Capture boundary changed after the approved plan", {
      approved: evidence.capture_enabled_at,
      live: captureEnabledAt,
    });
  }
  const approvedTopology = new Map(evidence.channels.map((channel) => [
    channel.channel_id,
    channel.topology,
  ]));
  const liveChannels = channelEvidenceRows({
    target,
    source: state.source,
    captureEnabledAt,
    topologyByChannel: approvedTopology,
  });
  if (!isDeepStrictEqual(liveChannels, evidence.channels)) {
    fail("live Publication data no longer matches the approved adoption Evidence");
  }
  return validateApprovedTopology(state, config, target, evidence);
}

async function registerBusinessOwnerships(pool, config, target, evidence) {
  return inTransaction(pool, BUSINESS_ADMIN_LOCK, async (client) => {
    const stream = await client.query(
      `/* publication-legacy-adoption:lock-business-stream */
       SELECT publication_stream_id,status FROM publication.stream
       WHERE publication_stream_id=$1::uuid FOR SHARE`,
      [target.publication_stream_id],
    );
    if (stream.rows.length !== 1 || stream.rows[0].status !== "active") {
      fail("Business Publication Stream is no longer active");
    }
    const existing = await client.query(
      `/* publication-legacy-adoption:lock-business-ownerships */
       SELECT channel_id,active_publication_stream_id,status,previous_publication_stream_id,
              projection_mode,ownership_reference
       FROM publication.channel_ownership
       WHERE channel_id=ANY($1::text[])
       ORDER BY channel_id FOR UPDATE`,
      [target.channel_ids],
    );
    const owners = businessOwnerByChannel(existing.rows);
    const evidenceByChannel = evidenceChannelMap(evidence);
    const inserts = [];
    for (const channelId of target.channel_ids) {
      const owner = owners.get(channelId);
      const expected = evidenceByChannel.get(channelId);
      if (expected.topology.disposition === "preexisting") {
        if (!owner || !isDeepStrictEqual(
          normalizeBusinessOwnership(owner),
          expected.topology.business_ownership,
        )) {
          fail("pre-existing Business ownership changed before adoption", {
            channel_id: channelId,
          });
        }
        continue;
      }
      const reference = ownershipReference(config, target, evidence, expected);
      if (owner) {
        if (!exactBusinessLegacyOwner(owner, target, reference)) {
          fail("Business Channel ownership is not bound to this Evidence", {
            channel_id: channelId,
          });
        }
        continue;
      }
      inserts.push({
        channel_id: channelId,
        ownership_reference: reference,
      });
    }
    let inserted = 0;
    if (inserts.length > 0) {
      const result = await client.query(
        `/* publication-legacy-adoption:insert-business-ownerships */
         INSERT INTO publication.channel_ownership (
           channel_id,active_publication_stream_id,status,previous_publication_stream_id,
           ownership_reference,projection_mode,state_changed_by,state_reason
         )
         SELECT item.channel_id,$1::uuid,'active',NULL,item.ownership_reference,
                'held_shadow',$3,$4
         FROM jsonb_to_recordset($2::jsonb)
           AS item(channel_id text,ownership_reference jsonb)
         ON CONFLICT (channel_id) DO NOTHING`,
        [
          target.publication_stream_id,
          JSON.stringify(inserts),
          config.actor,
          stateReason(config, target, evidence),
        ],
      );
      inserted = result.rowCount;
    }
    const verified = await client.query(
      `/* publication-legacy-adoption:verify-business-ownerships */
       SELECT channel_id,active_publication_stream_id,status,previous_publication_stream_id,
              projection_mode,ownership_reference
       FROM publication.channel_ownership
       WHERE channel_id=ANY($1::text[])
       ORDER BY channel_id`,
      [target.channel_ids],
    );
    const verifiedByChannel = businessOwnerByChannel(verified.rows);
    if (verified.rows.length !== target.channel_count || target.channel_ids.some((channelId) => {
      const expected = evidenceByChannel.get(channelId);
      const row = verifiedByChannel.get(channelId);
      return expected.topology.disposition === "preexisting"
        ? !row || !isDeepStrictEqual(
          normalizeBusinessOwnership(row),
          expected.topology.business_ownership,
        )
        : !exactBusinessLegacyOwner(
          row,
          target,
          ownershipReference(config, target, evidence, expected),
        );
    })) {
      fail("Business ownership verification failed after registration");
    }
    return { inserted, existing: target.channel_count - inserted };
  });
}

function compareReconciliationToEvidence(result, expected) {
  const actual = new Map((result.domains ?? []).map((domain) => [domain.domain, domain]));
  if (actual.size !== DOMAINS.length) {
    fail("Publication reconciliation returned an unexpected Domain set", {
      channel_id: expected.channel_id,
    });
  }
  for (const domain of expected.domains) {
    const value = actual.get(domain.domain);
    if (!value) fail("Publication reconciliation omitted a Domain", {
      channel_id: expected.channel_id,
      domain: domain.domain,
    });
    if (domain.ready) {
      if (value.status !== "revision_created" || value.result_hash !== domain.result_hash) {
        fail("Publication Domain changed after the approved adoption plan", {
          channel_id: expected.channel_id,
          domain: domain.domain,
          expected_result_hash: domain.result_hash,
          actual_result_hash: value.result_hash ?? null,
          actual_status: value.status,
        });
      }
    } else if (value.status !== "not_ready"
      || !isDeepStrictEqual(normalizedReasons(value.readiness_reasons), domain.readiness_reasons)) {
      fail("Publication Domain readiness changed after the approved adoption plan", {
        channel_id: expected.channel_id,
        domain: domain.domain,
      });
    }
  }
  const expectedRevisions = expected.domains
    .filter((domain) => domain.ready)
    .map((domain) => domain.domain)
    .sort(compareText);
  const actualRevisions = (result.revisions ?? [])
    .map((revision) => String(revision.domain))
    .sort(compareText);
  if (!isDeepStrictEqual(actualRevisions, expectedRevisions)) {
    fail("Publication Revision inserts no longer match the approved plan", {
      channel_id: expected.channel_id,
      actual: actualRevisions,
      expected: expectedRevisions,
    });
  }
}

async function lockAndValidateSourceFacts(client, config, target, evidence, expected) {
  const stream = await client.query(
    `/* publication-legacy-adoption:lock-source-stream */
     SELECT publication_stream_id,status,minimum_writer_version,capture_enabled_at
     FROM publication.stream
     WHERE publication_stream_id=$1::uuid
     FOR SHARE`,
    [target.publication_stream_id],
  );
  const streamRow = stream.rows[0];
  if (stream.rows.length !== 1 || streamRow.status !== "active"
      || timestamp(streamRow.capture_enabled_at, "source Capture boundary")
        !== evidence.capture_enabled_at
      || !publicationWriterVersionSatisfies(
        PUBLICATION_WRITER_VERSION,
        streamRow.minimum_writer_version,
      )) {
    fail("Crawler Publication Stream changed after the approved plan", {
      channel_id: expected.channel_id,
    });
  }
  const channelResult = await client.query(
    `/* publication-legacy-adoption:lock-source-channel */
     SELECT channel_id,status,created_at,
            registry_promotion_candidate_id::text AS registry_promotion_candidate_id,
            registry_promotion_run_id
     FROM crawler.channels
     WHERE channel_id=$1
     FOR SHARE`,
    [expected.channel_id],
  );
  if (channelResult.rows.length !== 1) {
    fail("Crawler Channel disappeared after the approved plan", {
      channel_id: expected.channel_id,
    });
  }
  const channel = channelResult.rows[0];
  let promotion = null;
  if (channel.registry_promotion_candidate_id != null) {
    const promotionResult = await client.query(
      `/* publication-legacy-adoption:lock-source-promotion */
       SELECT candidate_id::text AS promotion_candidate_id,status AS promotion_status,
              accepted_at AS promotion_accepted_at
       FROM crawler.channel_candidates
       WHERE candidate_id=$1::bigint AND channel_id=$2
       FOR SHARE`,
      [channel.registry_promotion_candidate_id, expected.channel_id],
    );
    if (promotionResult.rows.length !== 1) {
      fail("Channel Registry Promotion disappeared after the approved plan", {
        channel_id: expected.channel_id,
      });
    }
    promotion = promotionResult.rows[0];
  }
  const finalized = await client.query(
    `/* publication-legacy-adoption:lock-source-finalize */
     SELECT run_id,publication_finalized_at
     FROM crawler.channel_runs
     WHERE channel_id=$1 AND crawl_mode='full'
       AND publication_finalized_status='ready_auto'
       AND publication_finalized_at IS NOT NULL
     ORDER BY publication_finalized_at DESC,run_id DESC
     LIMIT 1
     FOR SHARE`,
    [expected.channel_id],
  );
  const finalizedRow = finalized.rows[0];
  const live = {
    lifecycle_status: channel.status,
    registry: legacyRegistryEvidence(
      { ...channel, ...promotion },
      evidence.capture_enabled_at,
    ),
    finalized_full_run_id: finalizedRow?.run_id == null ? null : String(finalizedRow.run_id),
    finalized_at: finalizedRow?.publication_finalized_at == null
      ? null
      : timestamp(finalizedRow.publication_finalized_at, "latest Full Finalize"),
  };
  const approved = {
    lifecycle_status: expected.lifecycle_status,
    registry: expected.registry,
    finalized_full_run_id: expected.finalized_full_run_id,
    finalized_at: expected.finalized_at,
  };
  if (!isDeepStrictEqual(live, approved)) {
    fail("Channel Registry or Finalize changed after the approved plan", {
      channel_id: expected.channel_id,
      approved,
      live,
    });
  }
}

async function adoptSourceChannel(pool, config, target, evidence, expected, reconcile) {
  return inTransaction(pool, null, async (client) => {
    await lockPublicationChannelMutation(client, expected.channel_id);
    await lockAndValidateSourceFacts(client, config, target, evidence, expected);
    const owners = await client.query(
      `/* publication-legacy-adoption:lock-source-owner */
       SELECT publication_stream_id,channel_id,status,onboarding_mode,seed_status,
              ownership_reference,owned_at,seed_completed_at,sealed_at
       FROM publication.channel_stream_state
       WHERE channel_id=$1
       ORDER BY owned_at,publication_stream_id FOR UPDATE`,
      [expected.channel_id],
    );
    const initialDeliveries = await client.query(
      `/* publication-legacy-adoption:lock-source-deliveries */
       SELECT destination,publication_stream_id,channel_id,mode,source_ownership_reference,
              online_at,sealed_at
       FROM publication.channel_delivery_state
       WHERE channel_id=$1
       ORDER BY destination,publication_stream_id
       FOR SHARE`,
      [expected.channel_id],
    );
    const initial = validateApprovedSourceTopology(
      owners.rows,
      initialDeliveries.rows,
      config,
      target,
      evidence,
      expected,
    );
    if (initial.stage === "preexisting") {
      return {
        channel_id: expected.channel_id,
        status: "preexisting",
        ownership_inserted: 0,
        delivery_inserted: 0,
        revisions: [],
        not_ready_domains: [],
      };
    }
    if (initial.stage === "completed") {
      return {
        channel_id: expected.channel_id,
        status: "recovered",
        ownership_inserted: 0,
        delivery_inserted: 0,
        revisions: [],
        not_ready_domains: expected.domains
          .filter((domain) => !domain.ready)
          .map((domain) => domain.domain),
      };
    }
    const reference = ownershipReference(config, target, evidence, expected);
    const ownership = await client.query(
      `/* publication-legacy-adoption:insert-source-owner */
       INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,ownership_reference,
         state_changed_by,state_reason
       ) VALUES ($1::uuid,$2,'bootstrap',$3::jsonb,$4,$5)
       ON CONFLICT DO NOTHING`,
      [
        target.publication_stream_id,
        expected.channel_id,
        JSON.stringify(reference),
        config.actor,
        stateReason(config, target, evidence),
      ],
    );
    const ownershipInserted = ownership.rowCount;
    const owner = await client.query(
      `/* publication-legacy-adoption:verify-source-owner */
       SELECT publication_stream_id,channel_id,status,onboarding_mode,seed_status,
              ownership_reference,owned_at,seed_completed_at,sealed_at
       FROM publication.channel_stream_state
       WHERE channel_id=$1
       ORDER BY owned_at,publication_stream_id FOR UPDATE`,
      [expected.channel_id],
    );
    const delivery = await client.query(
      `/* publication-legacy-adoption:insert-source-delivery */
       INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,source_ownership_reference,
         online_at,state_changed_by,state_reason
       ) VALUES ($1,$2::uuid,$3,'online',$4::jsonb,now(),$5,$6)
       ON CONFLICT DO NOTHING`,
      [
        target.destination,
        target.publication_stream_id,
        expected.channel_id,
        JSON.stringify(reference),
        config.actor,
        stateReason(config, target, evidence),
      ],
    );
    const deliveryInserted = delivery.rowCount;
    const deliveries = await client.query(
      `/* publication-legacy-adoption:verify-source-delivery */
       SELECT destination,publication_stream_id,channel_id,mode,source_ownership_reference,
              online_at,sealed_at
       FROM publication.channel_delivery_state
       WHERE channel_id=$1
       ORDER BY destination,publication_stream_id FOR SHARE`,
      [expected.channel_id],
    );
    const completed = validateApprovedSourceTopology(
      owner.rows,
      deliveries.rows,
      config,
      target,
      evidence,
      expected,
    );
    if (completed.stage !== "completed") {
      fail("Crawler Delivery verification failed", { channel_id: expected.channel_id });
    }
    const result = await reconcile(client, {
      channelId: expected.channel_id,
      domains: DOMAINS,
      asOf: target.as_of,
      revisionType: "incremental",
    });
    compareReconciliationToEvidence(result, expected);
    return {
      channel_id: expected.channel_id,
      status: "adopted",
      ownership_inserted: ownershipInserted,
      delivery_inserted: deliveryInserted,
      revisions: result.revisions.map((revision) => revision.domain),
      not_ready_domains: result.domains
        .filter((domain) => domain.status === "not_ready")
        .map((domain) => domain.domain),
    };
  });
}

async function mapConcurrent(values, concurrency, action) {
  const results = new Array(values.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= values.length) return;
      try {
        results[index] = { ok: true, value: await action(values[index], index) };
      } catch (error) {
        results[index] = { ok: false, error };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, values.length) }, () => worker()));
  return results;
}

export class PublicationLegacyAdoptionAdministrator {
  constructor({ crawlerPool, businessPool, config, target, evidence = null, reconcile = reconcilePublication }) {
    if (!crawlerPool?.connect || !businessPool?.connect) {
      throw new TypeError("Crawler and Business PostgreSQL Pools are required");
    }
    if (typeof reconcile !== "function") throw new TypeError("reconcile must be a function");
    this.crawlerPool = crawlerPool;
    this.businessPool = businessPool;
    this.config = config;
    this.target = target;
    this.evidence = evidence;
    this.reconcile = reconcile;
  }

  async readState() {
    const [source, business] = await Promise.all([
      inReadOnlyTransaction(
        this.crawlerPool,
        (client) => sourcePlanState(client, this.config, this.target),
      ),
      inReadOnlyTransaction(
        this.businessPool,
        (client) => businessPlanState(client, this.config, this.target),
      ),
    ]);
    return { source, business };
  }

  async inspectReadOnly() {
    const state = await this.readState();
    validatePlanState(state, this.config, this.target);
    const evidence = buildPublicationLegacyAdoptionEvidence({
      config: this.config,
      target: this.target,
      source: state.source,
      business: state.business,
    });
    return {
      state,
      evidence,
      summary: publicationLegacyAdoptionSummary(state, this.target, evidence),
    };
  }

  async apply() {
    const approved = validateEvidenceShape(this.evidence, this.config, this.target);
    const state = await this.readState();
    validateLiveStateAgainstEvidence(state, this.config, this.target, approved);
    const business = await registerBusinessOwnerships(
      this.businessPool,
      this.config,
      this.target,
      approved,
    );
    const sourceResults = await mapConcurrent(
      approved.channels,
      this.config.concurrency,
      (channel) => adoptSourceChannel(
        this.crawlerPool,
        this.config,
        this.target,
        approved,
        channel,
        this.reconcile,
      ),
    );
    const succeeded = sourceResults.filter((result) => result.ok).map((result) => result.value);
    const failures = sourceResults.map((result, index) => ({ result, index }))
      .filter(({ result }) => !result.ok)
      .map(({ result, index }) => ({
        channel_id: approved.channels[index]?.channel_id ?? null,
        error: result.error?.message ?? String(result.error),
        details: result.error?.details ?? null,
      }));
    const result = {
      business,
      crawler: {
        succeeded: succeeded.length,
        failed: failures.length,
        ownerships_inserted: succeeded.reduce((sum, row) => sum + row.ownership_inserted, 0),
        deliveries_inserted: succeeded.reduce((sum, row) => sum + row.delivery_inserted, 0),
        preexisting: succeeded.filter((row) => row.status === "preexisting").length,
        recovered: succeeded.filter((row) => row.status === "recovered").length,
        revisions_created: Object.fromEntries(DOMAINS.map((domain) => [
          domain,
          succeeded.filter((row) => row.revisions.includes(domain)).length,
        ])),
        waiting_for_natural_updates: Object.fromEntries(DOMAINS.map((domain) => [
          domain,
          succeeded.filter((row) => row.not_ready_domains.includes(domain)).length,
        ])),
      },
      failures,
    };
    if (failures.length > 0) {
      throw new PublicationLegacyAdoptionPartialFailure(
        "Publication Legacy Adoption completed only partially; rerun the same approved command",
        result,
      );
    }
    return result;
  }
}
