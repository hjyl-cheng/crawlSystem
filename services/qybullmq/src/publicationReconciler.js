import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { observationFactsHash } from "./crawlObservationStore.js";
import {
  PUBLICATION_CONTRACT_VERSION,
  PUBLICATION_POLICY_VERSION,
} from "./publicationContract.js";
import {
  PUBLICATION_WRITER_VERSION,
  publicationWriterVersionSatisfies,
} from "./publicationWriterVersion.js";
import { normalizePublicationCurrentCandidate } from "./publicationCurrentStore.js";
import { publicationResultHash } from "./publicationResultHash.js";
import {
  AGENTS_SQL,
  CHANNELS_SQL,
  CONTENTS_SQL,
  SOURCES_SQL,
  buildAgentReadiness,
  buildChannelReadiness,
  buildVideoReadiness,
} from "./publicationReadinessReport.js";

const DOMAIN_ORDER = Object.freeze(["channel", "video", "agent"]);
const DOMAIN_SET = new Set(DOMAIN_ORDER);
const SEED_ONBOARDING_MODES = new Set(["baseline", "cutover"]);
const CHANGE_REVISION_TYPES = new Set(["incremental", "repair"]);
const PRESERVATION_BASELINE_STATUSES = new Set(["available", "not_found"]);
const PRESERVATION_POLICY = "legacy_business_active_snapshot_v1";
const RETRACTION_EVIDENCE_MAX_LENGTH = 2000;

export class PublicationReconciliationConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationReconciliationConflict";
    this.details = details;
  }
}

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function timestamp(value, field) {
  const parsed = value == null ? new Date() : new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a valid timestamp`);
  return parsed.toISOString();
}

function requiredTimestamp(value, field) {
  if (value == null || value === "") throw new TypeError(`${field} is required`);
  return timestamp(value, field);
}

function normalizePreservationBaseline(value, channelId, domain) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`preservationBaselines.${domain} must be an object`);
  }
  const status = text(value.status);
  if (!PRESERVATION_BASELINE_STATUSES.has(status)) {
    throw new TypeError(`preservationBaselines.${domain}.status is invalid`);
  }
  const source = object(value.source);
  if (!text(source.type)) {
    throw new TypeError(`preservationBaselines.${domain}.source.type is required`);
  }
  if (status === "not_found") {
    if (value.payload != null) {
      throw new TypeError(`preservationBaselines.${domain}.payload must be null when not_found`);
    }
    return { status, payload: null, source };
  }
  if (!value.payload || typeof value.payload !== "object" || Array.isArray(value.payload)) {
    throw new TypeError(`preservationBaselines.${domain}.payload must be an object when available`);
  }
  const payload = { ...value.payload };
  if (text(payload.channel_id) !== channelId) {
    throw new TypeError(`preservationBaselines.${domain}.payload.channel_id must match channelId`);
  }
  return { status, payload, source };
}

function normalizePreservationBaselines(value, channelId, domains) {
  if (value == null) return new Map();
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("preservationBaselines must be an object");
  }
  const unknown = Object.keys(value).filter((domain) => !DOMAIN_SET.has(domain));
  if (unknown.length > 0) {
    throw new TypeError(`unsupported preservation baseline Domain: ${unknown.join(", ")}`);
  }
  const unrequested = Object.keys(value).filter((domain) => !domains.includes(domain));
  if (unrequested.length > 0) {
    throw new TypeError(`preservation baseline Domain was not requested: ${unrequested.join(", ")}`);
  }
  const unsupported = Object.keys(value).filter((domain) => domain !== "channel");
  if (unsupported.length > 0) {
    throw new TypeError("only Channel preservation baselines are supported");
  }
  return new Map(Object.entries(value).map(([domain, baseline]) => [
    domain,
    normalizePreservationBaseline(baseline, channelId, domain),
  ]));
}

function normalizePolicyRemovalContentIds(value, domains, revisionType) {
  if (value == null) return [];
  if (!Array.isArray(value)) {
    throw new TypeError("policyRemovalContentIds must be an array");
  }
  const normalized = [...new Set(value.map(text).filter(Boolean))].sort();
  if (normalized.length !== value.length) {
    throw new TypeError("policyRemovalContentIds must contain unique non-empty values");
  }
  if (normalized.length > 0 && (revisionType !== "repair" || !domains.includes("video"))) {
    throw new TypeError("policyRemovalContentIds require a Video Repair reconciliation");
  }
  return normalized;
}

function normalizeInput(input) {
  const channelId = text(input?.channelId);
  if (!channelId) throw new TypeError("channelId is required");
  if (!Array.isArray(input?.domains) || input.domains.length === 0) {
    throw new TypeError("domains must be a non-empty array");
  }
  const unknown = input.domains.map(text).filter((domain) => !DOMAIN_SET.has(domain));
  if (unknown.length > 0) {
    throw new TypeError(`unsupported Publication Domain: ${[...new Set(unknown)].join(", ")}`);
  }
  const requested = new Set(input.domains.map(text));
  const revisionType = text(input.revisionType) ?? "incremental";
  if (!CHANGE_REVISION_TYPES.has(revisionType)) {
    throw new TypeError("revisionType must be incremental or repair");
  }
  const domains = DOMAIN_ORDER.filter((domain) => requested.has(domain));
  return {
    channelId,
    domains,
    asOf: timestamp(input.asOf, "asOf"),
    revisionType,
    policyRemovalContentIds: normalizePolicyRemovalContentIds(
      input?.policyRemovalContentIds,
      domains,
      revisionType,
    ),
    preservationBaselines: normalizePreservationBaselines(
      input?.preservationBaselines,
      channelId,
      domains,
    ),
  };
}

function normalizeChannelRetraction(input) {
  const channelId = text(input?.channelId);
  if (!channelId) throw new TypeError("channelId is required");
  const reasonCode = text(input?.reasonCode);
  if (!reasonCode) throw new TypeError("reasonCode is required");
  const source = text(input?.source);
  if (!source) throw new TypeError("source is required");
  const evidence = text(input?.evidence);
  if (!evidence) throw new TypeError("evidence is required");
  return {
    channelId,
    reasonCode,
    source,
    evidence: evidence.slice(0, RETRACTION_EVIDENCE_MAX_LENGTH),
    removedAt: requiredTimestamp(input?.removedAt, "removedAt"),
  };
}

function activeClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

function rows(result, queryName) {
  if (!result || !Array.isArray(result.rows)) {
    throw new TypeError(`${queryName} query must return { rows: [] }`);
  }
  return result.rows;
}

function sourceMap(sourceRows) {
  return new Map(sourceRows.map((row) => [
    `${text(row.channel_id)}\u0000${text(row.observation_kind)}`,
    row,
  ]));
}

async function loadCandidates(client, channelId, domains, asOf, existing = new Map(), {
  policyRemovalContentIds = [],
} = {}) {
  const filter = [channelId];
  const sourceResult = await client.query(SOURCES_SQL, [filter]);
  const sources = sourceMap(rows(sourceResult, "Publication source"));
  const candidates = new Map();

  if (domains.includes("channel")) {
    const channelResult = await client.query(CHANNELS_SQL, [filter]);
    const wrapper = rows(channelResult, "Publication Channel")[0];
    if (!wrapper) throw new PublicationReconciliationConflict("owned Publication Channel is missing");
    candidates.set("channel", buildChannelReadiness({
      row: object(wrapper.row ?? wrapper),
      source: sources.get(`${channelId}\u0000about`),
    }));
  }

  if (domains.includes("video")) {
    const contentResult = await client.query(CONTENTS_SQL, [filter, asOf]);
    const contentRows = rows(contentResult, "Publication Video")
      .map((wrapper) => object(wrapper.row ?? wrapper));
    const policyRemovalSource = policyRemovalContentIds.length === 0 ? [] : rows(
      await client.query(
        `/* publication-reconciler:policy-removal-source */
         SELECT source_content_id
         FROM crawler.contents
         WHERE channel_id=$1 AND source_content_id=ANY($2::text[])
         ORDER BY source_content_id`,
        [channelId, policyRemovalContentIds],
      ),
      "Publication policy removal source",
    );
    candidates.set("video", {
      ...buildVideoReadiness({
        rows: contentRows,
        source: sources.get(`${channelId}\u0000video`),
        channelId,
        asOf,
        previousCurrent: existing.get("video") ?? null,
      }),
      source_content_ids: [...new Set([
        ...contentRows.map((row) => text(row.source_content_id)),
        ...policyRemovalSource.map((row) => text(row.source_content_id)),
      ].filter(Boolean))],
    });
  }

  if (domains.includes("agent")) {
    const agentResult = await client.query(AGENTS_SQL, [filter]);
    const wrapper = rows(agentResult, "Publication Agent")[0] ?? {};
    candidates.set("agent", buildAgentReadiness({
      row: wrapper.row,
      config: wrapper.config,
      source: sources.get(`${channelId}\u0000agent`),
    }));
  }

  return candidates;
}

export async function inspectPublicationInitialPackage(clientValue, input) {
  const client = activeClient(clientValue);
  const {
    channelId,
    domains,
    asOf,
  } = normalizeInput({
    channelId: input?.channelId,
    domains: DOMAIN_ORDER,
    asOf: input?.asOf,
    revisionType: "incremental",
  });
  const rawCandidates = await loadCandidates(client, channelId, domains, asOf);
  const results = domains.map((domain) => {
    const candidate = normalizePublicationCurrentCandidate(
      channelId,
      domain,
      rawCandidates.get(domain),
    );
    return {
      domain,
      readiness_status: candidate.readiness_status,
      readiness_reasons: candidate.readiness_reasons,
      result_hash: candidate.result_hash,
      complete_observed_at: candidate.complete_observed_at,
    };
  });
  return {
    status: results.every((result) => result.readiness_status === "ready")
      ? "ready"
      : "not_ready",
    channel_id: channelId,
    as_of: asOf,
    domains: results,
  };
}

function currentByDomain(currentRows) {
  return new Map(currentRows.map((row) => [row.domain, row]));
}

function currentSequence(current) {
  const value = Number(current?.data_sequence ?? 0);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new PublicationReconciliationConflict("stored Publication Sequence is invalid", {
      domain: current?.domain,
      data_sequence: current?.data_sequence,
    });
  }
  return value;
}

function oldVideoItems(current) {
  const items = object(current?.payload_json).items;
  return Array.isArray(items) ? items.map(object) : [];
}

function carriedForwardFields(candidate) {
  const merge = object(object(candidate?.source_refs).publication_merge);
  return Array.isArray(merge.carried_forward_fields)
    ? merge.carried_forward_fields.map(text).filter(Boolean)
    : [];
}

function storedChannelPreservation(current) {
  const reference = object(object(current?.source_refs).publication_preservation);
  if (!text(reference.policy)) return null;
  if (reference.policy !== PRESERVATION_POLICY
      || current?.domain !== "channel"
      || current?.readiness_status !== "not_ready"
      || currentSequence(current) !== 0
      || current?.current_revision_id !== null
      || !current?.payload_json
      || !current?.result_hash) {
    throw new PublicationReconciliationConflict("stored Channel preservation Current is invalid", {
      domain: current?.domain,
      readiness_status: current?.readiness_status,
      data_sequence: current?.data_sequence,
      current_revision_id: current?.current_revision_id,
    });
  }
  const payload = object(current.payload_json);
  const payloadHash = observationFactsHash(payload);
  if (reference.payload_hash !== payloadHash
      || publicationResultHash("channel", payload) !== current.result_hash
      || !text(object(reference.source).type)) {
    throw new PublicationReconciliationConflict("stored Channel preservation evidence is invalid");
  }
  return {
    status: "available",
    payload,
    source: reference.source,
  };
}

function hasRevisionBase(current) {
  return Boolean(current?.result_hash) && !storedChannelPreservation(current);
}

function buildChannelPreservationCandidate(candidate, baseline) {
  const payload = { ...baseline.payload };
  return {
    ...candidate,
    payload_json: payload,
    result_hash: publicationResultHash("channel", payload),
    source_refs: {
      ...object(candidate.source_refs),
      publication_preservation: {
        policy: PRESERVATION_POLICY,
        source: baseline.source,
        payload_hash: observationFactsHash(payload),
      },
    },
    complete_observed_at: null,
  };
}

function assertRequestedPreservationMatches(stored, requested, channelId) {
  if (!stored || !requested) return;
  if (requested.status !== "available"
      || observationFactsHash(stored.payload) !== observationFactsHash(requested.payload)
      || !isDeepStrictEqual(stored.source, requested.source)) {
    throw new PublicationReconciliationConflict(
      "stored Channel preservation differs from the requested Business baseline",
      { channel_id: channelId },
    );
  }
}

function carryForwardChannelCandidate(rawCandidate, previous, preservationBaseline) {
  if (rawCandidate?.ready !== true) return rawCandidate;
  const storedPreservation = storedChannelPreservation(previous);
  const sourceCurrent = hasRevisionBase(previous) ? previous : null;
  const takeoverBaseline = !sourceCurrent
    ? storedPreservation ?? (preservationBaseline?.status === "available" ? preservationBaseline : null)
    : null;
  if (!sourceCurrent && !takeoverBaseline) return rawCandidate;
  const previousPayload = object(sourceCurrent?.payload_json ?? takeoverBaseline.payload);
  if (!text(previousPayload.channel_id)) return rawCandidate;

  const payload = { ...object(rawCandidate.payload) };
  const carried = new Set();
  const carry = (keys) => {
    for (const key of keys) {
      if (!Object.hasOwn(previousPayload, key)) continue;
      payload[key] = previousPayload[key];
      carried.add(key);
    }
  };
  const previousText = (key) => text(previousPayload[key]);
  const previousList = (key) => Array.isArray(previousPayload[key]) && previousPayload[key].length > 0;

  for (const key of ["title", "canonical_url", "vanity_channel_url", "handle", "rss_url"]) {
    if (!text(payload[key]) && previousText(key)) carry([key]);
  }
  if ((!Array.isArray(payload.avatar) || payload.avatar.length === 0) && previousList("avatar")) {
    carry(["avatar"]);
  }
  if ((!Array.isArray(payload.keywords) || payload.keywords.length === 0) && previousList("keywords")) {
    carry(["keywords"]);
  }
  if (typeof payload.is_family_safe !== "boolean"
      && typeof previousPayload.is_family_safe === "boolean") {
    carry(["is_family_safe"]);
  }
  if (typeof payload.is_verified !== "boolean"
      && typeof previousPayload.is_verified === "boolean") {
    carry(["is_verified", "is_verified_status"]);
  }
  if (["has_videos", "has_shorts", "has_live_streams"].every((key) => payload[key] !== true)
      && ["has_videos", "has_shorts", "has_live_streams"].some((key) => previousPayload[key] === true)) {
    carry(["has_videos", "has_shorts", "has_live_streams"]);
  }
  if (!text(payload.description) && previousText("description")) carry(["description"]);
  for (const [valueKey, statusKey] of [
    ["subscriber_count", "subscriber_count_status"],
    ["total_video_count", "total_video_count_status"],
    ["total_view_count", "total_view_count_status"],
  ]) {
    if (payload[valueKey] == null && previousPayload[valueKey] != null) {
      carry([valueKey, statusKey]);
    }
  }
  if (!text(payload.joined_date) && previousText("joined_date")) {
    carry(["joined_date", "joined_date_status", "joined_date_raw"]);
  }
  for (const key of ["country_code", "country_name"]) {
    if (!text(payload[key]) && previousText(key)) carry([key]);
  }
  if ((!Array.isArray(payload.links) || payload.links.length === 0) && previousList("links")) {
    carry(["links"]);
  }
  const mergeReference = sourceCurrent
    ? { previous_result_hash: sourceCurrent.result_hash }
    : {
      preservation_source: takeoverBaseline.source,
      preservation_payload_hash: observationFactsHash(previousPayload),
    };
  if (carried.size === 0 && !takeoverBaseline) return rawCandidate;
  return {
    ...rawCandidate,
    payload,
    result_hash: publicationResultHash("channel", payload),
    source_refs: {
      ...object(rawCandidate.source_refs),
      publication_merge: {
        policy: "preserve_trusted_nonempty_v1",
        ...mergeReference,
        carried_forward_fields: [...carried].sort(),
      },
    },
  };
}

function videoExitReason(candidate, item) {
  const contentId = text(item.content_id);
  const exclusion = Array.isArray(candidate.exclusions)
    ? candidate.exclusions.find((value) => text(value?.content_id) === contentId)
    : null;
  if (exclusion?.reason_code === "outside_90_day_window") return "aged_out";
  if (exclusion?.reason_code === "outside_limit") return "outside_limit";

  const policy = object(candidate.payload?.window_policy);
  const publishedAt = text(item.published_at);
  const publishedDate = text(item.published_date);
  if (publishedAt && text(policy.cutoff_at) && Date.parse(publishedAt) <= Date.parse(policy.cutoff_at)) {
    return "aged_out";
  }
  if (publishedDate && text(policy.cutoff_date) && publishedDate <= policy.cutoff_date) {
    return "aged_out";
  }
  return null;
}

function videoRetractionReason(candidate, item, policyRemovalContentIds) {
  const contentId = text(item.content_id);
  if (policyRemovalContentIds.has(contentId)) return "policy_removed";
  const exclusion = Array.isArray(candidate.exclusions)
    ? candidate.exclusions.find((value) => text(value?.content_id) === contentId)
    : null;
  const reasons = new Set(["source_unlisted", "source_private", "source_unavailable"]);
  return reasons.has(exclusion?.reason_code) ? exclusion.reason_code : null;
}

export function buildVideoRevisionDelta(current, candidate, {
  policyRemovalContentIds = [],
} = {}) {
  const previousItems = oldVideoItems(current);
  const nextItems = Array.isArray(candidate.payload?.items) ? candidate.payload.items.map(object) : [];
  const previousById = new Map(previousItems.map((item) => [text(item.content_id), item]));
  const nextById = new Map(nextItems.map((item) => [text(item.content_id), item]));
  const policyRemovals = new Set(policyRemovalContentIds.map(text).filter(Boolean));
  const upserts = nextItems.filter((item) => {
    const previous = previousById.get(text(item.content_id));
    return !previous
      || previous.item_hash !== item.item_hash
      || Number(previous.position) !== Number(item.position);
  });
  const windowExits = [];
  const retractions = [];
  const issues = [];
  for (const contentId of policyRemovals) {
    if (!previousById.has(contentId)) {
      issues.push({
        domain: "video",
        code: "video_policy_removal_not_in_current",
        content_id: contentId,
      });
    } else if (nextById.has(contentId)) {
      issues.push({
        domain: "video",
        code: "video_policy_removal_still_in_source",
        content_id: contentId,
      });
    }
  }
  for (const item of previousItems) {
    const contentId = text(item.content_id);
    if (nextById.has(contentId)) continue;
    const retractionReason = videoRetractionReason(candidate, item, policyRemovals);
    if (retractionReason) {
      retractions.push({ content_id: contentId, reason: retractionReason });
      continue;
    }
    const reason = videoExitReason(candidate, item);
    if (!reason) {
      issues.push({
        domain: "video",
        code: "video_window_exit_reason_unproven",
        content_id: contentId,
      });
      continue;
    }
    windowExits.push({ content_id: contentId, reason });
  }
  return {
    ready: issues.length === 0,
    issues,
    payload: {
      channel_id: candidate.payload.channel_id,
      window_policy: candidate.payload.window_policy,
      window_proof: candidate.payload.window_proof,
      upserts,
      window_exits: windowExits,
      retractions,
      result_hash: candidate.result_hash,
    },
  };
}

function revisionShape(
  domain,
  current,
  candidate,
  onboardingMode,
  revisionType,
  policyRemovalContentIds = [],
) {
  const sequence = currentSequence(current);
  const hasTrustedCurrent = hasRevisionBase(current);
  if (!hasTrustedCurrent && onboardingMode === "bootstrap") {
    return {
      revisionType: "bootstrap",
      operation: domain === "video" ? "replace_window" : "replace",
      dataSequence: 1,
      previousDataSequence: null,
      previousResultHash: null,
      payload: domain === "video"
        ? { ...candidate.payload, result_hash: candidate.result_hash }
        : candidate.payload,
    };
  }
  if (!hasTrustedCurrent) return null;
  if (domain === "video") {
    const delta = buildVideoRevisionDelta(current, candidate, { policyRemovalContentIds });
    if (!delta.ready) return { notReadyIssues: delta.issues };
    return {
      revisionType,
      operation: "apply_window_delta",
      dataSequence: sequence + 1,
      previousDataSequence: sequence,
      previousResultHash: current.result_hash,
      payload: delta.payload,
    };
  }
  return {
    revisionType,
    operation: "replace",
    dataSequence: sequence + 1,
    previousDataSequence: sequence,
    previousResultHash: current.result_hash,
    payload: candidate.payload,
  };
}

function policyRemovalCandidate(rawCandidate, previous, contentIds) {
  if (!hasRevisionBase(previous) || contentIds.length === 0) return rawCandidate;
  const removalIds = new Set(contentIds);
  const sourceIds = new Set(
    Array.isArray(rawCandidate.source_content_ids) ? rawCandidate.source_content_ids : [],
  );
  const stillInSource = contentIds.filter((contentId) => sourceIds.has(contentId));
  if (stillInSource.length > 0) {
    return {
      ...rawCandidate,
      ready: false,
      result_hash: null,
      issues: [
        ...(Array.isArray(rawCandidate.issues) ? rawCandidate.issues : []),
        ...stillInSource.map((contentId) => ({
          domain: "video",
          code: "video_policy_removal_still_in_source",
          content_id: contentId,
        })),
      ],
    };
  }
  const previousPayload = object(previous.payload_json);
  const previousItems = Array.isArray(previousPayload.items) ? previousPayload.items : [];
  const keptItems = previousItems
    .filter((item) => !removalIds.has(text(item?.content_id)))
    .map((item, index) => ({ ...item, position: index + 1 }));
  const removedCount = previousItems.length - keptItems.length;
  const previousProof = object(previousPayload.window_proof);
  const previousQualifiedCount = Number(previousProof.qualified_count);
  const previousExcludedCount = Number(previousProof.excluded_count);
  const payload = {
    ...previousPayload,
    window_proof: {
      ...previousProof,
      qualified_count: Number.isSafeInteger(previousQualifiedCount)
        ? Math.max(keptItems.length, previousQualifiedCount - removedCount)
        : keptItems.length,
      selected_count: keptItems.length,
      excluded_count: Number.isSafeInteger(previousExcludedCount)
        ? previousExcludedCount + removedCount
        : removedCount,
    },
    items: keptItems,
  };
  return {
    ready: true,
    contract_version: previous.contract_version,
    policy_version: previous.policy_version,
    result_hash: publicationResultHash("video", payload),
    payload,
    exclusions: [],
    source_refs: {
      ...object(previous.source_refs),
      publication_repair: {
        kind: "policy_removal",
        content_ids: [...contentIds],
        based_on_revision_id: previous.current_revision_id,
        based_on_result_hash: previous.result_hash,
      },
    },
    complete_observed_at: previous.complete_observed_at,
    issues: [],
  };
}

async function storeCurrent(client, {
  publicationStreamId,
  channelId,
  candidate,
  previous,
  dataSequence,
  revisionId,
  allowNotReadyPayload = false,
}) {
  const values = [
    publicationStreamId,
    channelId,
    candidate.domain,
    candidate.contract_version,
    candidate.policy_version,
    candidate.readiness_status,
    JSON.stringify(candidate.readiness_reasons),
    candidate.payload_json === null ? null : JSON.stringify(candidate.payload_json),
    candidate.result_hash,
    JSON.stringify(candidate.source_refs),
    candidate.complete_observed_at,
    dataSequence,
    revisionId,
  ];
  const updateParams = [
    ...values,
    currentSequence(previous),
    previous?.current_revision_id ?? null,
    allowNotReadyPayload,
  ];
  const result = previous
    ? await client.query(
      `/* publication-reconciler:store-current */
       UPDATE publication.domain_current AS current
       SET contract_version=CASE
             WHEN $6='ready' OR $16::boolean OR current.payload_json IS NULL
               THEN $4 ELSE current.contract_version END,
           policy_version=CASE
             WHEN $6='ready' OR $16::boolean OR current.payload_json IS NULL
               THEN $5 ELSE current.policy_version END,
           readiness_status=$6,
           readiness_reasons=$7::jsonb,
           payload_json=CASE
             WHEN $6='ready' OR $16::boolean THEN $8::jsonb ELSE current.payload_json END,
           result_hash=CASE
             WHEN $6='ready' OR $16::boolean THEN $9 ELSE current.result_hash END,
           source_refs=CASE
             WHEN $6='ready' OR $16::boolean OR current.payload_json IS NULL
               THEN $10::jsonb ELSE current.source_refs END,
           complete_observed_at=CASE
             WHEN $6='ready' OR $16::boolean OR current.payload_json IS NULL
               THEN $11::timestamptz ELSE current.complete_observed_at END,
           data_sequence=$12,
           current_revision_id=$13::uuid,
           updated_at=now()
       WHERE current.publication_stream_id=$1::uuid
         AND current.channel_id=$2
         AND current.domain=$3
         AND current.data_sequence=$14
         AND current.current_revision_id IS NOT DISTINCT FROM $15::uuid
         AND (
           NOT $16::boolean
           OR (
             current.data_sequence=0
             AND current.current_revision_id IS NULL
             AND current.readiness_status='not_ready'
             AND (current.result_hash IS NULL OR current.result_hash=$9)
           )
       )
       RETURNING domain,readiness_status,result_hash,data_sequence,current_revision_id`,
      updateParams,
    )
    : await client.query(
      `/* publication-reconciler:store-current */
       INSERT INTO publication.domain_current (
         publication_stream_id,channel_id,domain,contract_version,policy_version,
         readiness_status,readiness_reasons,payload_json,result_hash,source_refs,
         complete_observed_at,data_sequence,current_revision_id
       ) VALUES (
         $1::uuid,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10::jsonb,$11::timestamptz,$12,$13::uuid
       )
       ON CONFLICT (publication_stream_id,channel_id,domain) DO NOTHING
       RETURNING domain,readiness_status,result_hash,data_sequence,current_revision_id`,
      values,
    );
  if (result.rows.length !== 1) {
    throw new PublicationReconciliationConflict("Publication Current changed while reconciling", {
      publication_stream_id: publicationStreamId,
      channel_id: channelId,
      domain: candidate.domain,
    });
  }
  return result.rows[0];
}

async function insertRevision(client, {
  publicationStreamId,
  channelId,
  domain,
  candidate,
  shape,
}) {
  const revisionId = randomUUID();
  const payloadHash = observationFactsHash(shape.payload);
  const result = await client.query(
    `/* publication-reconciler:insert-revision */
     INSERT INTO publication.revision (
       revision_id,publication_stream_id,channel_id,domain,data_sequence,
       previous_data_sequence,revision_type,operation,contract_version,policy_version,
       occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
     ) VALUES (
       $1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,now(),$11::jsonb,$12,$13,$14,$15::jsonb
     )
     RETURNING occurred_at`,
    [
      revisionId,
      publicationStreamId,
      channelId,
      domain,
      shape.dataSequence,
      shape.previousDataSequence,
      shape.revisionType,
      shape.operation,
      candidate.contract_version,
      candidate.policy_version,
      JSON.stringify(candidate.source_refs),
      shape.previousResultHash,
      candidate.result_hash,
      payloadHash,
      JSON.stringify(shape.payload),
    ],
  );
  const stored = rows(result, "Publication Revision")[0];
  if (!stored) throw new PublicationReconciliationConflict("Publication Revision was not stored");
  return {
    revision_id: revisionId,
    publication_stream_id: publicationStreamId,
    channel_id: channelId,
    domain,
    data_sequence: shape.dataSequence,
    previous_data_sequence: shape.previousDataSequence,
    revision_type: shape.revisionType,
    operation: shape.operation,
    contract_version: candidate.contract_version,
    policy_version: candidate.policy_version,
    occurred_at: requiredTimestamp(stored.occurred_at, "stored Publication Revision occurred_at"),
    source: candidate.source_refs,
    previous_result_hash: shape.previousResultHash,
    result_hash: candidate.result_hash,
    payload_hash: payloadHash,
    payload: shape.payload,
  };
}

async function insertOutboxRows(client, revisionId, deliveries) {
  const stored = [];
  for (const delivery of deliveries) {
    if (delivery.mode === "sealed") continue;
    const status = delivery.mode === "online" ? "pending" : "held";
    await client.query(
      `/* publication-reconciler:insert-outbox */
       INSERT INTO publication.outbox (destination,revision_id,status)
       VALUES ($1,$2::uuid,$3)`,
      [delivery.destination, revisionId, status],
    );
    stored.push({ destination: delivery.destination, status });
  }
  return stored;
}

async function updateSeedStatus(client, publicationStreamId, channelId) {
  const result = await client.query(
    `/* publication-reconciler:seed-status */
     UPDATE publication.channel_stream_state AS channel_state
     SET seed_status=CASE
           WHEN channel_state.seed_status='complete' OR readiness.seed_complete THEN 'complete'
           ELSE 'pending'
         END,
         seed_completed_at=CASE
           WHEN channel_state.seed_status='complete' OR readiness.seed_complete
             THEN COALESCE(channel_state.seed_completed_at,now())
           ELSE NULL
         END,
         updated_at=now()
     FROM (
       SELECT count(*)=3
          AND count(*) FILTER (
            WHERE readiness_status='ready'
              AND payload_json IS NOT NULL
              AND result_hash IS NOT NULL
          )=3
            AS seed_complete
       FROM publication.domain_current
       WHERE publication_stream_id=$1::uuid AND channel_id=$2
     ) AS readiness
     WHERE channel_state.publication_stream_id=$1::uuid
       AND channel_state.channel_id=$2
     RETURNING channel_state.seed_status,channel_state.seed_completed_at`,
    [publicationStreamId, channelId],
  );
  if (result.rows.length !== 1) {
    throw new PublicationReconciliationConflict("Publication ownership disappeared while reconciling");
  }
  return result.rows[0];
}

function resultStatus(domainResults, revisions) {
  if (revisions.length > 0) return "revised";
  if (domainResults.some((item) => item.status === "seeded")) return "seeded";
  if (domainResults.some((item) => item.status === "preservation_seeded")) {
    return "preservation_seeded";
  }
  if (domainResults.every((item) => (
    item.status === "no_change" || item.status === "preservation_retained"
  ))) return "no_change";
  return "not_ready";
}

async function lockPublicationContext(client, channelId, domains) {
  await client.query("/* publication-reconciler:transaction-guard */ SAVEPOINT publication_reconcile_guard");
  await client.query("RELEASE SAVEPOINT publication_reconcile_guard");

  const discovered = await client.query(
    `/* publication-reconciler:find-owner */
     SELECT channel_state.publication_stream_id
     FROM publication.channel_stream_state AS channel_state
     JOIN publication.stream AS stream_state
       ON stream_state.publication_stream_id=channel_state.publication_stream_id
     WHERE channel_state.channel_id=$1 AND channel_state.status='owned'
     ORDER BY channel_state.owned_at,channel_state.publication_stream_id`,
    [channelId],
  );
  if (discovered.rows.length === 0) {
    return {
      result: { status: "not_owned", channel_id: channelId, domains: [], revisions: [] },
    };
  }
  if (discovered.rows.length !== 1) {
    throw new PublicationReconciliationConflict("multiple owned Publication Streams found", {
      channel_id: channelId,
    });
  }
  const publicationStreamId = String(discovered.rows[0].publication_stream_id);
  await client.query(
    `/* publication-reconciler:lock-domains */
     SELECT domain,contract_version,policy_version,readiness_status,readiness_reasons,
            payload_json,result_hash,source_refs,complete_observed_at,
            data_sequence,current_revision_id
     FROM publication.domain_current
     WHERE publication_stream_id=$1::uuid AND channel_id=$2 AND domain=ANY($3::text[])
     ORDER BY CASE domain WHEN 'channel' THEN 1 WHEN 'video' THEN 2 ELSE 3 END
     FOR UPDATE`,
    [publicationStreamId, channelId, domains],
  );
  const ownership = await client.query(
    `/* publication-reconciler:lock-owner */
     SELECT channel_state.status AS channel_status,channel_state.onboarding_mode,
            channel_state.seed_status,stream_state.status AS stream_status,
            stream_state.capture_enabled_at,stream_state.minimum_writer_version
     FROM publication.channel_stream_state AS channel_state
     JOIN publication.stream AS stream_state
       ON stream_state.publication_stream_id=channel_state.publication_stream_id
     WHERE channel_state.publication_stream_id=$1::uuid AND channel_state.channel_id=$2
     FOR SHARE OF stream_state
     FOR UPDATE OF channel_state`,
    [publicationStreamId, channelId],
  );
  const state = ownership.rows[0];
  if (!state || state.stream_status !== "active" || state.channel_status !== "owned") {
    return {
      result: {
        status: "not_owned",
        publication_stream_id: publicationStreamId,
        channel_id: channelId,
        domains: [],
        revisions: [],
      },
    };
  }
  if (!state.capture_enabled_at) {
    return {
      result: {
        status: "capture_disabled",
        publication_stream_id: publicationStreamId,
        channel_id: channelId,
        domains: [],
        revisions: [],
      },
    };
  }
  if (!publicationWriterVersionSatisfies(
    PUBLICATION_WRITER_VERSION,
    state.minimum_writer_version,
  )) {
    throw new PublicationReconciliationConflict(
      "Publication writer version does not satisfy the Stream minimum",
      {
        publication_stream_id: publicationStreamId,
        channel_id: channelId,
        writer_version: PUBLICATION_WRITER_VERSION,
        minimum_writer_version: state.minimum_writer_version,
      },
    );
  }

  const refreshed = await client.query(
    `/* publication-reconciler:refresh-domains */
     SELECT domain,contract_version,policy_version,readiness_status,readiness_reasons,
            payload_json,result_hash,source_refs,complete_observed_at,
            data_sequence,current_revision_id
     FROM publication.domain_current
     WHERE publication_stream_id=$1::uuid AND channel_id=$2 AND domain=ANY($3::text[])
     ORDER BY CASE domain WHEN 'channel' THEN 1 WHEN 'video' THEN 2 ELSE 3 END`,
    [publicationStreamId, channelId, domains],
  );
  return {
    publicationStreamId,
    state,
    existing: currentByDomain(refreshed.rows),
  };
}

async function lockPublicationDeliveries(client, publicationStreamId, channelId) {
  return client.query(
    `/* publication-reconciler:lock-deliveries */
     SELECT destination,mode
     FROM publication.channel_delivery_state
     WHERE publication_stream_id=$1::uuid AND channel_id=$2
     ORDER BY destination
     FOR SHARE`,
    [publicationStreamId, channelId],
  );
}

export async function reconcilePublication(clientValue, input) {
  const client = activeClient(clientValue);
  const {
    channelId,
    domains,
    asOf,
    revisionType,
    policyRemovalContentIds,
    preservationBaselines,
  } = normalizeInput(input);
  const context = await lockPublicationContext(client, channelId, domains);
  if (context.result) return context.result;
  const { publicationStreamId, state, existing } = context;
  const rawCandidates = await loadCandidates(client, channelId, domains, asOf, existing, {
    policyRemovalContentIds,
  });
  const deliveries = await lockPublicationDeliveries(client, publicationStreamId, channelId);

  const domainResults = [];
  const revisions = [];
  for (const domain of domains) {
    const rawCandidate = rawCandidates.get(domain);
    const previous = existing.get(domain);
    const requestedPreservation = preservationBaselines.get(domain);
    const storedPreservation = domain === "channel" ? storedChannelPreservation(previous) : null;
    assertRequestedPreservationMatches(storedPreservation, requestedPreservation, channelId);
    const effectiveCandidate = domain === "channel"
      ? carryForwardChannelCandidate(rawCandidate, previous, requestedPreservation)
      : domain === "video" && policyRemovalContentIds.length > 0
        ? policyRemovalCandidate(rawCandidate, previous, policyRemovalContentIds)
        : rawCandidate;
    const carriedFields = carriedForwardFields(effectiveCandidate);
    let candidate = normalizePublicationCurrentCandidate(channelId, domain, effectiveCandidate);

    if (candidate.readiness_status === "not_ready") {
      if (storedPreservation && requestedPreservation) {
        const stored = await storeCurrent(client, {
          publicationStreamId,
          channelId,
          candidate,
          previous,
          dataSequence: 0,
          revisionId: null,
        });
        domainResults.push({
          domain,
          status: "preservation_retained",
          readiness_reasons: candidate.readiness_reasons,
          result_hash: stored.result_hash,
          data_sequence: Number(stored.data_sequence),
          current_revision_id: stored.current_revision_id,
          carried_forward_fields: [],
        });
        continue;
      }
      const previousCanAcceptPreservation = !previous || (
        currentSequence(previous) === 0
        && previous.current_revision_id === null
        && previous.readiness_status === "not_ready"
        && !previous.result_hash
      );
      const canSeedPreservation = domain === "channel"
        && state.onboarding_mode === "bootstrap"
        && requestedPreservation?.status === "available"
        && previousCanAcceptPreservation;
      if (canSeedPreservation) {
        candidate = buildChannelPreservationCandidate(candidate, requestedPreservation);
        const stored = await storeCurrent(client, {
          publicationStreamId,
          channelId,
          candidate,
          previous,
          dataSequence: 0,
          revisionId: null,
          allowNotReadyPayload: true,
        });
        domainResults.push({
          domain,
          status: "preservation_seeded",
          readiness_reasons: candidate.readiness_reasons,
          result_hash: stored.result_hash,
          data_sequence: Number(stored.data_sequence),
          current_revision_id: stored.current_revision_id,
          carried_forward_fields: [],
        });
        continue;
      }
      const stored = await storeCurrent(client, {
        publicationStreamId,
        channelId,
        candidate,
        previous,
        dataSequence: currentSequence(previous),
        revisionId: previous?.current_revision_id ?? null,
      });
      domainResults.push({
        domain,
        status: "not_ready",
        readiness_reasons: candidate.readiness_reasons,
        data_sequence: Number(stored.data_sequence),
        current_revision_id: stored.current_revision_id,
        carried_forward_fields: carriedFields,
      });
      continue;
    }

    if (hasRevisionBase(previous) && previous.result_hash === candidate.result_hash) {
      const stored = await storeCurrent(client, {
        publicationStreamId,
        channelId,
        candidate,
        previous,
        dataSequence: currentSequence(previous),
        revisionId: previous.current_revision_id,
      });
      domainResults.push({
        domain,
        status: "no_change",
        result_hash: stored.result_hash,
        data_sequence: Number(stored.data_sequence),
        current_revision_id: stored.current_revision_id,
        carried_forward_fields: carriedFields,
      });
      continue;
    }

    if (!hasRevisionBase(previous) && SEED_ONBOARDING_MODES.has(state.onboarding_mode)) {
      const stored = await storeCurrent(client, {
        publicationStreamId,
        channelId,
        candidate,
        previous,
        dataSequence: 0,
        revisionId: null,
      });
      domainResults.push({
        domain,
        status: "seeded",
        result_hash: stored.result_hash,
        data_sequence: 0,
        current_revision_id: null,
        carried_forward_fields: carriedFields,
      });
      continue;
    }

    const shape = revisionShape(
      domain,
      previous,
      effectiveCandidate,
      state.onboarding_mode,
      revisionType,
      domain === "video" ? policyRemovalContentIds : [],
    );
    if (shape?.notReadyIssues) {
      candidate = {
        ...candidate,
        readiness_status: "not_ready",
        readiness_reasons: shape.notReadyIssues,
        payload_json: null,
        result_hash: null,
        complete_observed_at: null,
      };
      const stored = await storeCurrent(client, {
        publicationStreamId,
        channelId,
        candidate,
        previous,
        dataSequence: currentSequence(previous),
        revisionId: previous?.current_revision_id ?? null,
      });
      domainResults.push({
        domain,
        status: "not_ready",
        readiness_reasons: shape.notReadyIssues,
        data_sequence: Number(stored.data_sequence),
        current_revision_id: stored.current_revision_id,
        carried_forward_fields: carriedFields,
      });
      continue;
    }
    if (!shape) {
      throw new PublicationReconciliationConflict("Publication onboarding state cannot create a Revision", {
        publication_stream_id: publicationStreamId,
        channel_id: channelId,
        domain,
        onboarding_mode: state.onboarding_mode,
      });
    }
    const revision = await insertRevision(client, {
      publicationStreamId,
      channelId,
      domain,
      candidate,
      shape,
    });
    revision.outbox = await insertOutboxRows(client, revision.revision_id, deliveries.rows);
    await storeCurrent(client, {
      publicationStreamId,
      channelId,
      candidate,
      previous,
      dataSequence: shape.dataSequence,
      revisionId: revision.revision_id,
    });
    revisions.push(revision);
    domainResults.push({
      domain,
      status: "revision_created",
      result_hash: candidate.result_hash,
      data_sequence: shape.dataSequence,
      current_revision_id: revision.revision_id,
      outbox: revision.outbox,
      carried_forward_fields: carriedFields,
    });
  }

  const seed = await updateSeedStatus(client, publicationStreamId, channelId);
  return {
    status: resultStatus(domainResults, revisions),
    publication_stream_id: publicationStreamId,
    channel_id: channelId,
    seed_status: seed.seed_status,
    seed_completed_at: seed.seed_completed_at,
    domains: domainResults,
    revisions,
  };
}

function channelRetractionCandidate(previous, retraction) {
  const payload = {
    channel_id: retraction.channelId,
    retraction: {
      object_id: retraction.channelId,
      domain: "channel",
      reason_code: retraction.reasonCode,
      source: retraction.source,
      evidence: {
        type: "terminal_channel_response",
        source: retraction.source,
        detail: retraction.evidence,
      },
      removed_at: retraction.removedAt,
    },
  };
  return normalizePublicationCurrentCandidate(retraction.channelId, "channel", {
    ready: true,
    contract_version: Number(previous?.contract_version) || PUBLICATION_CONTRACT_VERSION,
    policy_version: text(previous?.policy_version) ?? PUBLICATION_POLICY_VERSION,
    payload,
    result_hash: publicationResultHash("channel", payload),
    source_refs: {
      terminal_channel: {
        removed_reason: retraction.reasonCode,
        removed_at: retraction.removedAt,
        removed_source: retraction.source,
        evidence_hash: observationFactsHash(retraction.evidence),
      },
    },
    complete_observed_at: retraction.removedAt,
    issues: [],
  });
}

export async function retractPublicationChannel(clientValue, input) {
  const client = activeClient(clientValue);
  const retraction = normalizeChannelRetraction(input);
  const context = await lockPublicationContext(client, retraction.channelId, ["channel"]);
  if (context.result) return context.result;
  const { publicationStreamId, existing } = context;
  const previous = existing.get("channel");
  if (!previous?.result_hash || !previous.current_revision_id || currentSequence(previous) === 0) {
    return {
      status: "not_previously_published",
      publication_stream_id: publicationStreamId,
      channel_id: retraction.channelId,
      domains: [],
      revisions: [],
    };
  }

  const candidate = channelRetractionCandidate(previous, retraction);
  const previousRetraction = object(previous.payload_json).retraction;
  if (previousRetraction && previous.result_hash !== candidate.result_hash) {
    throw new PublicationReconciliationConflict("published Channel Retraction cannot be changed", {
      publication_stream_id: publicationStreamId,
      channel_id: retraction.channelId,
    });
  }
  if (previous.result_hash === candidate.result_hash) {
    const stored = await storeCurrent(client, {
      publicationStreamId,
      channelId: retraction.channelId,
      candidate,
      previous,
      dataSequence: currentSequence(previous),
      revisionId: previous.current_revision_id,
    });
    return {
      status: "no_change",
      publication_stream_id: publicationStreamId,
      channel_id: retraction.channelId,
      domains: [{
        domain: "channel",
        status: "no_change",
        result_hash: stored.result_hash,
        data_sequence: Number(stored.data_sequence),
        current_revision_id: stored.current_revision_id,
      }],
      revisions: [],
    };
  }

  const sequence = currentSequence(previous) + 1;
  const shape = {
    revisionType: "retraction",
    operation: "retract_channel",
    dataSequence: sequence,
    previousDataSequence: sequence - 1,
    previousResultHash: previous.result_hash,
    payload: candidate.payload_json,
  };
  const deliveries = await lockPublicationDeliveries(
    client,
    publicationStreamId,
    retraction.channelId,
  );
  const revision = await insertRevision(client, {
    publicationStreamId,
    channelId: retraction.channelId,
    domain: "channel",
    candidate,
    shape,
  });
  revision.outbox = await insertOutboxRows(client, revision.revision_id, deliveries.rows);
  await storeCurrent(client, {
    publicationStreamId,
    channelId: retraction.channelId,
    candidate,
    previous,
    dataSequence: sequence,
    revisionId: revision.revision_id,
  });
  const seed = await updateSeedStatus(client, publicationStreamId, retraction.channelId);
  return {
    status: "revised",
    publication_stream_id: publicationStreamId,
    channel_id: retraction.channelId,
    seed_status: seed.seed_status,
    seed_completed_at: seed.seed_completed_at,
    domains: [{
      domain: "channel",
      status: "revision_created",
      result_hash: candidate.result_hash,
      data_sequence: sequence,
      current_revision_id: revision.revision_id,
      outbox: revision.outbox,
    }],
    revisions: [revision],
  };
}
