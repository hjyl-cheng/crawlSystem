import { createHash, randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { validateBusinessPublicationEnvelope } from "./businessPublicationContract.js";
import { observationFactsHash } from "./crawlObservationStore.js";
import { lockPublicationChannelMutation } from "./publicationChannelMutationLock.js";
import { publicationResultHash } from "./publicationResultHash.js";
import { PUBLICATION_WRITER_VERSION } from "./publicationWriterVersion.js";

const DOMAINS = Object.freeze(["channel", "video", "agent"]);
const HASH = /^sha256:[0-9a-f]{64}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const RECOVERY_MODE = "dead_letter_recovery_cutover";
const SOURCE_ADMIN_LOCK = 781137243;
const BUSINESS_ADMIN_LOCK = 781137244;
const ACTIVE_VIDEO_POSITION_ERROR = "Active Video positions must be contiguous from 1";
const VIDEO_RETRACTION_REASONS = new Set([
  "source_deleted",
  "source_unlisted",
  "source_private",
  "source_unavailable",
  "policy_removed",
]);

export class PublicationDeadLetterRecoveryConflict extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = "PublicationDeadLetterRecoveryConflict";
    this.details = details;
  }
}

function fail(message, details = {}) {
  throw new PublicationDeadLetterRecoveryConflict(message, details);
}

function text(value, field = "value") {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function id(value, field) {
  const output = text(value, field).toLowerCase();
  if (!UUID.test(output)) throw new TypeError(`${field} must be a UUID`);
  return output;
}

function hash(value, field) {
  const output = text(value, field);
  if (!HASH.test(output)) throw new TypeError(`${field} must be a SHA-256 hash`);
  return output;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a timestamp`);
  return parsed.toISOString();
}

function object(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${field} must be an object`);
  }
  return value;
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalJson);
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalJson(value[key])]),
  );
}

function canonicalHash(value) {
  return `sha256:${createHash("sha256")
    .update(JSON.stringify(canonicalJson(value)))
    .digest("hex")}`;
}

function integer(value, field, minimum = 0) {
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < minimum) {
    throw new TypeError(`${field} must be an integer >= ${minimum}`);
  }
  return output;
}

function sortedUnique(values) {
  return [...new Set((values ?? []).map((value) => text(value)))].sort();
}

function normalizedHistoricalRetractions(values = []) {
  if (!Array.isArray(values)) throw new TypeError("historicalRetractions must be an array");
  const byContentId = new Map();
  for (const value of values) {
    const item = object(value, "historical Retraction");
    const contentId = text(item.content_id, "historical Retraction content_id");
    const reason = text(item.reason, "historical Retraction reason");
    if (!VIDEO_RETRACTION_REASONS.has(reason)) {
      throw new TypeError(`unsupported historical Retraction reason: ${reason}`);
    }
    const previous = byContentId.get(contentId);
    if (previous && previous !== reason) {
      throw new TypeError(`historical Retraction reason conflicts for ${contentId}`);
    }
    byContentId.set(contentId, reason);
  }
  return [...byContentId.entries()]
    .map(([content_id, reason]) => ({ content_id, reason }))
    .sort((left, right) => left.content_id.localeCompare(right.content_id));
}

function publishableVideoItem(item) {
  return (item?.access_status === "public" && item?.is_members_only === false)
    || (item?.access_status === "members_only" && item?.is_members_only === true);
}

export function classifyRecoverablePublicationDeadLetter(row = {}) {
  const message = String(row.error_message ?? "");
  const base = row.domain === "video"
    && row.status === "dead_letter"
    && row.receipt_status === "rejected"
    && row.error_code === "payload_contract_invalid";
  const membersOnlyUpgrade = base
    && message.includes("may contain only public Content")
    && Number(row.members_only_item_count ?? 0) > 0
    && Number(row.non_publishable_item_count ?? 0) === 0;
  const retractionUpgrade = base
    && message.includes("retractions.reason is not allowed by Contract V1")
    && Number(row.retraction_reason_count ?? 0) > 0
    && Number(row.unsupported_retraction_reason_count ?? 0) === 0;
  const positionGapRecovery = row.domain === "video"
    && row.status === "delivered"
    && row.receipt_status === "accepted"
    && row.explicit_target === true
    && row.business_receive_status === "accepted"
    && row.business_validation_status === "quarantined"
    && row.business_activation_status === "quarantined"
    && row.quarantine_status === "open"
    && row.quarantine_issue_code === "video_current_invalid"
    && row.quarantine_message === ACTIVE_VIDEO_POSITION_ERROR;
  const reason = membersOnlyUpgrade
    ? "video_members_only_contract_upgrade"
    : retractionUpgrade
      ? "video_retraction_contract_upgrade"
      : positionGapRecovery
        ? "video_position_gap_activation_quarantine"
        : row.status === "dead_letter"
          ? "unsupported_dead_letter"
          : "unsupported_publication_failure";
  return {
    recoverable: membersOnlyUpgrade || retractionUpgrade || positionGapRecovery,
    reason,
  };
}

function normalizedCurrent(currentValue, expectedDomain = null) {
  const current = object(currentValue, "Publication Current");
  const domain = text(current.domain, "Publication Current domain");
  if (!DOMAINS.includes(domain) || (expectedDomain && domain !== expectedDomain)) {
    throw new TypeError(`unsupported Publication Current domain: ${domain}`);
  }
  if (current.readiness_status !== "ready") {
    throw new TypeError(`${domain} Publication Current must be ready`);
  }
  return {
    ...current,
    domain,
    contract_version: integer(current.contract_version, `${domain} contract_version`, 1),
    policy_version: text(current.policy_version, `${domain} policy_version`),
    data_sequence: integer(current.data_sequence, `${domain} data_sequence`, 1),
    current_revision_id: id(current.current_revision_id, `${domain} current_revision_id`),
    result_hash: hash(current.result_hash, `${domain} result_hash`),
    complete_observed_at: timestamp(
      current.complete_observed_at,
      `${domain} complete_observed_at`,
    ),
    source_refs: object(current.source_refs, `${domain} source_refs`),
    payload_json: object(current.payload_json, `${domain} payload_json`),
  };
}

export function buildVideoRecoverySnapshot(currentValue) {
  const current = normalizedCurrent(currentValue, "video");
  const payload = current.payload_json;
  const items = Array.isArray(payload.items) ? payload.items : null;
  if (!items) throw new TypeError("Video Publication Current items must be an array");
  const kept = items.filter(publishableVideoItem).map((item, index) => ({
    ...item,
    position: index + 1,
  }));
  const removed = items
    .filter((item) => !publishableVideoItem(item))
    .map((item) => text(item.content_id, "Video Content ID"))
    .sort();
  const previousProof = object(payload.window_proof, "Video window_proof");
  const oldQualified = integer(
    previousProof.qualified_count ?? items.length,
    "Video qualified_count",
  );
  const oldExcluded = integer(previousProof.excluded_count ?? 0, "Video excluded_count");
  const windowProof = {
    ...previousProof,
    qualified_count: Math.max(kept.length, oldQualified - removed.length),
    selected_count: kept.length,
    excluded_count: oldExcluded + removed.length,
  };
  const recoveryPayload = {
    ...payload,
    window_proof: windowProof,
    items: kept,
  };
  return {
    payload: recoveryPayload,
    result_hash: publicationResultHash("video", recoveryPayload),
    removed_content_ids: removed,
  };
}

function recoverySnapshot(current) {
  if (current.domain === "video") return buildVideoRecoverySnapshot(current);
  const actualHash = publicationResultHash(current.domain, current.payload_json);
  if (actualHash !== current.result_hash) {
    fail(`${current.domain} Current result hash does not match its Payload`, {
      domain: current.domain,
      expected: current.result_hash,
      actual: actualHash,
    });
  }
  return {
    payload: current.payload_json,
    result_hash: current.result_hash,
    removed_content_ids: [],
  };
}

export function buildPublicationRecoveryBootstrap({
  current: currentValue,
  channelId: channelIdValue,
  oldStreamId: oldStreamIdValue,
  newStreamId: newStreamIdValue,
  revisionId: revisionIdValue,
  occurredAt: occurredAtValue,
  evidenceHash: evidenceHashValue,
  deadRevisionIds = [],
  historicalRetractions = [],
}) {
  const current = normalizedCurrent(currentValue);
  const channelId = text(channelIdValue, "channelId");
  const oldStreamId = id(oldStreamIdValue, "oldStreamId");
  const newStreamId = id(newStreamIdValue, "newStreamId");
  const revisionId = id(revisionIdValue, "revisionId");
  const occurredAt = timestamp(occurredAtValue, "occurredAt");
  const evidenceHash = hash(evidenceHashValue, "evidenceHash");
  const normalizedRetractions = normalizedHistoricalRetractions(historicalRetractions);
  if (current.domain !== "video" && normalizedRetractions.length > 0) {
    throw new TypeError("historical Retractions may only be attached to Video recovery");
  }
  if (text(current.payload_json.channel_id, "Current payload channel_id") !== channelId) {
    throw new TypeError("Publication Current channel_id does not match recovery Channel");
  }
  const snapshot = recoverySnapshot(current);
  const snapshotContentIds = new Set(
    current.domain === "video"
      ? snapshot.payload.items.map((item) => text(item.content_id, "Video Content ID"))
      : [],
  );
  const retainedRetractions = normalizedRetractions.filter((item) => (
    snapshotContentIds.has(item.content_id)
  ));
  if (retainedRetractions.length > 0) {
    throw new TypeError("historical Retraction targets remain in the recovery Video snapshot");
  }
  const payload = current.domain === "video"
    ? { ...snapshot.payload, result_hash: snapshot.result_hash }
    : snapshot.payload;
  const source = {
    ...current.source_refs,
    dead_letter_recovery: {
      recovery_version: "publication-dead-letter-recovery-v1",
      evidence_hash: evidenceHash,
      old_publication_stream_id: oldStreamId,
      old_data_sequence: current.data_sequence,
      old_revision_id: current.current_revision_id,
      old_result_hash: current.result_hash,
      dead_revision_ids: sortedUnique(deadRevisionIds).map((value) => id(value, "deadRevisionId")),
      removed_content_ids: snapshot.removed_content_ids,
      historical_retractions: normalizedRetractions,
    },
  };
  const envelope = {
    revision_id: revisionId,
    publication_stream_id: newStreamId,
    revision_type: "bootstrap",
    channel_id: channelId,
    domain: current.domain,
    data_sequence: 1,
    previous_data_sequence: null,
    operation: current.domain === "video" ? "replace_window" : "replace",
    contract_version: current.contract_version,
    policy_version: current.policy_version,
    occurred_at: occurredAt,
    source,
    previous_result_hash: null,
    result_hash: snapshot.result_hash,
    payload_hash: observationFactsHash(payload),
    payload,
  };
  validateBusinessPublicationEnvelope(envelope);
  return {
    envelope,
    current_payload: snapshot.payload,
    current_source_refs: source,
    complete_observed_at: current.complete_observed_at,
    removed_content_ids: snapshot.removed_content_ids,
    historical_retractions: normalizedRetractions,
  };
}

function recoveryEvidenceBody(evidence) {
  return {
    format: evidence.format,
    destination: evidence.destination,
    generated_at: evidence.generated_at,
    databases: evidence.databases,
    recovery_stream: evidence.recovery_stream,
    channels: evidence.channels,
  };
}

export function publicationDeadLetterRecoveryEvidenceHash(evidence) {
  return canonicalHash(recoveryEvidenceBody(evidence));
}

export function publicationDeadLetterRecoveryConfirmation(evidence) {
  return `APPLY ${publicationDeadLetterRecoveryEvidenceHash(evidence)}`;
}

function rowPayloadItems(row) {
  const payload = object(row.payload_json, "dead-letter payload");
  const items = row.revision_type === "bootstrap" ? payload.items : payload.upserts;
  return Array.isArray(items) ? items : [];
}

function normalizedDeadLetter(row) {
  const items = rowPayloadItems(row);
  const payload = object(row.payload_json, "dead-letter payload");
  const retractions = Array.isArray(payload.retractions) ? payload.retractions : [];
  const retractionReasons = sortedUnique(retractions.map((item) => (
    text(item?.reason, "Video Retraction reason")
  )));
  const historicalRetractions = retractions.map((item) => ({
    content_id: text(item?.content_id, "Video Retraction content_id"),
    reason: text(item?.reason, "Video Retraction reason"),
  })).sort((left, right) => left.content_id.localeCompare(right.content_id));
  const receipt = row.receipt_json && typeof row.receipt_json === "object"
    ? row.receipt_json
    : {};
  const output = {
    destination: text(row.destination, "dead-letter destination"),
    revision_id: id(row.revision_id, "dead-letter revision_id"),
    publication_stream_id: id(row.publication_stream_id, "dead-letter stream"),
    channel_id: text(row.channel_id, "dead-letter channel_id"),
    domain: text(row.domain, "dead-letter domain"),
    data_sequence: integer(row.data_sequence, "dead-letter data_sequence", 1),
    revision_type: text(row.revision_type, "dead-letter revision_type"),
    status: text(row.status, "dead-letter status"),
    receipt_status: optionalText(row.receipt_status),
    error_code: optionalText(receipt.error_code),
    error_message: optionalText(receipt.error_message),
    payload_hash: hash(row.payload_hash, "dead-letter payload_hash"),
    result_hash: hash(row.result_hash, "dead-letter result_hash"),
    members_only_item_count: items.filter((item) => (
      item?.access_status === "members_only" && item?.is_members_only === true
    )).length,
    non_publishable_item_count: items.filter((item) => !publishableVideoItem(item)).length,
    non_publishable_access_states: sortedUnique(items
      .filter((item) => !publishableVideoItem(item))
      .map((item) => String(item.access_status ?? "unknown"))),
    retraction_reason_count: retractions.length,
    retraction_reasons: retractionReasons,
    unsupported_retraction_reason_count: retractionReasons.filter((reason) => (
      !VIDEO_RETRACTION_REASONS.has(reason)
    )).length,
    historical_retractions: historicalRetractions,
  };
  return { ...output, ...classifyRecoverablePublicationDeadLetter(output) };
}

function normalizedRevisionIds(values = []) {
  if (!Array.isArray(values)) throw new TypeError("revisionIds must be an array");
  return [...new Set(values.map((value) => id(value, "revisionId")))].sort();
}

async function sourceDeadLetters(client, destination, revisionIds = []) {
  const targets = normalizedRevisionIds(revisionIds);
  const result = await client.query(
    `/* publication-dead-letter-recovery:source-dead-letters */
     SELECT outbox.destination,outbox.revision_id,outbox.status,outbox.receipt_status,
            outbox.receipt_json,revision.publication_stream_id,revision.channel_id,
            revision.domain,revision.data_sequence,revision.revision_type,
            revision.payload_hash,revision.result_hash,revision.payload_json
     FROM publication.outbox AS outbox
     JOIN publication.revision AS revision USING(revision_id)
     WHERE outbox.destination=$1
       AND (
         outbox.status='dead_letter'
         OR (cardinality($3::uuid[])>0 AND outbox.status='delivered')
       )
       AND (cardinality($3::uuid[])=0 OR outbox.revision_id=ANY($3::uuid[]))
       AND NOT EXISTS (
         SELECT 1
         FROM publication.channel_delivery_state AS resolved_delivery
         JOIN publication.channel_stream_state AS resolved_owner
           ON resolved_owner.publication_stream_id=resolved_delivery.publication_stream_id
          AND resolved_owner.channel_id=resolved_delivery.channel_id
         WHERE resolved_delivery.destination=outbox.destination
           AND resolved_delivery.channel_id=revision.channel_id
           AND resolved_delivery.mode='online'
           AND resolved_owner.status='owned'
           AND resolved_delivery.source_ownership_reference->>'onboarding_mode'=$2
           AND resolved_delivery.source_ownership_reference->'dead_revision_ids'
                 ? outbox.revision_id::text
           AND resolved_delivery.cutover_reference @> '{"recovery_completed":true}'::jsonb
       )
     ORDER BY revision.channel_id,revision.domain,revision.data_sequence,revision.revision_id`,
    [destination, RECOVERY_MODE, targets],
  );
  return result.rows.map(normalizedDeadLetter);
}

async function businessQuarantines(client, revisionIds) {
  if (revisionIds.length === 0) return [];
  const result = await client.query(
    `/* publication-dead-letter-recovery:business-quarantines */
     SELECT quarantine.quarantine_id::text,quarantine.revision_id::text,
            quarantine.issue_code,quarantine.issue_hash,quarantine.details_json,
            quarantine.status,inbox.channel_id,inbox.domain,inbox.receive_status,
            revision.validation_status,revision.activation_status
     FROM publication.quarantine AS quarantine
     JOIN publication.inbox AS inbox USING(revision_id)
     LEFT JOIN publication.revision AS revision USING(revision_id)
     WHERE quarantine.revision_id=ANY($1::uuid[])
     ORDER BY quarantine.revision_id,quarantine.issue_code,quarantine.issue_hash`,
    [revisionIds],
  );
  return result.rows.map((row) => ({
    quarantine_id: id(row.quarantine_id, "Business quarantine_id"),
    revision_id: id(row.revision_id, "Business quarantine revision_id"),
    issue_code: text(row.issue_code, "Business quarantine issue_code"),
    issue_hash: hash(row.issue_hash, "Business quarantine issue_hash"),
    details_json: object(row.details_json, "Business quarantine details_json"),
    status: text(row.status, "Business quarantine status"),
    channel_id: text(row.channel_id, "Business quarantine channel_id"),
    domain: text(row.domain, "Business quarantine domain"),
    receive_status: text(row.receive_status, "Business Inbox receive_status"),
    validation_status: optionalText(row.validation_status),
    activation_status: optionalText(row.activation_status),
  }));
}

function recoveryQuarantineForFailure(failure, quarantines) {
  const rows = quarantines.filter((row) => (
    row.revision_id === failure.revision_id
    && row.status === "open"
    && (
      (failure.status === "dead_letter" && row.issue_code === failure.error_code)
      || (failure.status === "delivered"
        && row.issue_code === "video_current_invalid"
        && row.details_json.message === ACTIVE_VIDEO_POSITION_ERROR)
    )
  ));
  return rows.length === 1 ? rows[0] : null;
}

function attachBusinessQuarantine(failure, quarantines, explicitTargets) {
  const quarantine = recoveryQuarantineForFailure(failure, quarantines);
  const candidate = {
    ...failure,
    explicit_target: explicitTargets.has(failure.revision_id),
    business_receive_status: quarantine?.receive_status ?? null,
    business_validation_status: quarantine?.validation_status ?? null,
    business_activation_status: quarantine?.activation_status ?? null,
    quarantine_status: quarantine?.status ?? null,
    quarantine_issue_code: quarantine?.issue_code ?? null,
    quarantine_message: optionalText(quarantine?.details_json?.message),
    business_quarantine: quarantine ? {
      quarantine_id: quarantine.quarantine_id,
      revision_id: quarantine.revision_id,
      issue_code: quarantine.issue_code,
      issue_hash: quarantine.issue_hash,
      message: optionalText(quarantine.details_json.message),
    } : null,
  };
  const classification = classifyRecoverablePublicationDeadLetter(candidate);
  if (classification.recoverable && !candidate.business_quarantine) {
    return {
      ...candidate,
      recoverable: false,
      reason: "missing_business_quarantine_evidence",
    };
  }
  return { ...candidate, ...classification };
}

async function sourceChannelSnapshot(client, destination, channelId) {
  const owners = await client.query(
    `/* publication-dead-letter-recovery:source-owner */
     SELECT state.publication_stream_id,state.status,state.onboarding_mode,state.seed_status,
            state.ownership_reference,state.final_version_vector,
            stream.status AS stream_status,stream.source_deployment_key,
            stream.source_identity_json,stream.minimum_writer_version,stream.capture_enabled_at
     FROM publication.channel_stream_state AS state
     JOIN publication.stream AS stream USING(publication_stream_id)
     WHERE state.channel_id=$1
     ORDER BY state.owned_at,state.publication_stream_id`,
    [channelId],
  );
  const owned = owners.rows.filter((row) => row.status === "owned");
  if (owned.length !== 1) fail("Recovery Channel must have exactly one owned Source Stream", {
    channel_id: channelId,
    owned_stream_count: owned.length,
  });
  const owner = owned[0];
  const delivery = await client.query(
    `/* publication-dead-letter-recovery:source-delivery */
     SELECT destination,publication_stream_id,mode,source_ownership_reference,
            cutover_reference,online_at,sealed_at
     FROM publication.channel_delivery_state
     WHERE destination=$1 AND publication_stream_id=$2::uuid AND channel_id=$3`,
    [destination, owner.publication_stream_id, channelId],
  );
  const currents = await client.query(
    `/* publication-dead-letter-recovery:source-currents */
     SELECT domain,contract_version,policy_version,readiness_status,readiness_reasons,
            payload_json,result_hash,source_refs,complete_observed_at,
            data_sequence,current_revision_id
     FROM publication.domain_current
     WHERE publication_stream_id=$1::uuid AND channel_id=$2
     ORDER BY CASE domain WHEN 'channel' THEN 1 WHEN 'video' THEN 2 ELSE 3 END`,
    [owner.publication_stream_id, channelId],
  );
  return {
    owner: {
      publication_stream_id: String(owner.publication_stream_id),
      status: owner.status,
      onboarding_mode: owner.onboarding_mode,
      seed_status: owner.seed_status,
      ownership_reference: owner.ownership_reference,
      stream_status: owner.stream_status,
      source_deployment_key: owner.source_deployment_key,
      source_identity_json: owner.source_identity_json,
      minimum_writer_version: owner.minimum_writer_version,
      capture_enabled_at: owner.capture_enabled_at == null
        ? null
        : timestamp(owner.capture_enabled_at, "capture_enabled_at"),
    },
    delivery: delivery.rows[0] ? {
      destination: delivery.rows[0].destination,
      publication_stream_id: String(delivery.rows[0].publication_stream_id),
      mode: delivery.rows[0].mode,
      source_ownership_reference: delivery.rows[0].source_ownership_reference,
      cutover_reference: delivery.rows[0].cutover_reference,
      online_at: delivery.rows[0].online_at == null
        ? null
        : timestamp(delivery.rows[0].online_at, "delivery online_at"),
      sealed_at: delivery.rows[0].sealed_at == null
        ? null
        : timestamp(delivery.rows[0].sealed_at, "delivery sealed_at"),
    } : null,
    currents: currents.rows.map((row) => normalizedCurrent(row)),
  };
}

function businessVersionVector(rows) {
  const byDomain = new Map(rows.map((row) => [row.domain, row]));
  return Object.fromEntries(DOMAINS.map((domain) => {
    const row = byDomain.get(domain);
    return [domain, row ? {
      publication_stream_id: String(row.publication_stream_id),
      sequence: integer(row.active_sequence, `${domain} Business sequence`),
      revision_id: id(row.active_revision_id, `${domain} Business revision_id`),
      result_hash: hash(row.active_result_hash, `${domain} Business result_hash`),
    } : null];
  }));
}

async function businessChannelSnapshot(client, channelId) {
  const ownerResult = await client.query(
    `/* publication-dead-letter-recovery:business-owner */
     SELECT channel_id,active_publication_stream_id,status,previous_publication_stream_id,
            ownership_reference,projection_mode,state_changed_at
     FROM publication.channel_ownership WHERE channel_id=$1`,
    [channelId],
  );
  const cursorResult = await client.query(
    `/* publication-dead-letter-recovery:business-cursors */
     SELECT domain,publication_stream_id,active_sequence,active_revision_id,active_result_hash
     FROM publication.consumer_cursor WHERE channel_id=$1 ORDER BY domain`,
    [channelId],
  );
  const owner = ownerResult.rows[0];
  return {
    owner: owner ? {
      active_publication_stream_id: String(owner.active_publication_stream_id),
      status: owner.status,
      previous_publication_stream_id: owner.previous_publication_stream_id == null
        ? null
        : String(owner.previous_publication_stream_id),
      ownership_reference: owner.ownership_reference,
      projection_mode: owner.projection_mode,
      state_changed_at: timestamp(owner.state_changed_at, "Business ownership state_changed_at"),
    } : null,
    version_vector: businessVersionVector(cursorResult.rows),
  };
}

function currentVector(currents) {
  return Object.fromEntries(DOMAINS.map((domain) => {
    const row = currents.find((current) => current.domain === domain);
    return [domain, row ? {
      sequence: row.data_sequence,
      revision_id: row.current_revision_id,
      result_hash: row.result_hash,
      complete_observed_at: row.complete_observed_at,
    } : null];
  }));
}

function sourceSnapshotEvidence(snapshot) {
  return {
    owner: snapshot.owner,
    delivery: snapshot.delivery,
    current_vector: currentVector(snapshot.currents),
  };
}

function businessSnapshotEvidence(snapshot) {
  return {
    owner: snapshot.owner,
    version_vector: snapshot.version_vector,
  };
}

async function databaseName(client) {
  const result = await client.query("SELECT current_database() AS database_name");
  return String(result.rows[0].database_name);
}

function assertPlanChannel({ channelId, deadLetters, source, business }) {
  if (deadLetters.length === 0 || deadLetters.some((item) => !item.recoverable)) {
    fail("Recovery Channel includes an unsupported dead letter", { channel_id: channelId });
  }
  if (source.owner.status !== "owned" || source.owner.stream_status !== "active") {
    fail("Recovery Channel Source ownership is not active", { channel_id: channelId });
  }
  if (source.delivery?.mode !== "online") {
    fail("Recovery Channel Source delivery is not online", { channel_id: channelId });
  }
  if (source.currents.length !== DOMAINS.length
      || source.currents.some((current) => current.readiness_status !== "ready")) {
    fail("Recovery Channel requires three ready Source Currents", { channel_id: channelId });
  }
  if (!business.owner
      || business.owner.status !== "active"
      || business.owner.active_publication_stream_id !== source.owner.publication_stream_id
      || business.owner.projection_mode !== "online") {
    fail("Recovery Channel Business ownership is not the online Source owner", {
      channel_id: channelId,
    });
  }
  const cursorStreams = Object.values(business.version_vector)
    .filter(Boolean)
    .map((cursor) => cursor.publication_stream_id);
  if (cursorStreams.some((streamId) => streamId !== source.owner.publication_stream_id)) {
    fail("Recovery Channel Business Cursor belongs to another Stream", { channel_id: channelId });
  }
}

async function repeatableRead(pool, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export async function planPublicationDeadLetterRecovery({
  crawlerPool,
  businessPool,
  destination = "business",
  revisionIds = [],
  now = () => new Date(),
  uuid = randomUUID,
}) {
  const generatedAt = timestamp(now(), "generatedAt");
  const targets = normalizedRevisionIds(revisionIds);
  const rawSourceInspection = await repeatableRead(crawlerPool, async (client) => ({
    database_name: await databaseName(client),
    dead_letters: await sourceDeadLetters(client, destination, targets),
  }));
  const businessInspection = await repeatableRead(businessPool, async (client) => ({
    database_name: await databaseName(client),
    quarantines: await businessQuarantines(
      client,
      rawSourceInspection.dead_letters.map((item) => item.revision_id),
    ),
  }));
  const explicitTargets = new Set(targets);
  const sourceInspection = {
    ...rawSourceInspection,
    dead_letters: rawSourceInspection.dead_letters.map((item) => (
      attachBusinessQuarantine(item, businessInspection.quarantines, explicitTargets)
    )),
  };
  if (targets.length > 0) {
    const observed = sourceInspection.dead_letters.map((item) => item.revision_id).sort();
    if (!isDeepStrictEqual(observed, targets)) {
      fail("Recovery target Revision set does not match current dead letters", {
        requested: targets,
        observed,
      });
    }
  }
  const recoverable = sourceInspection.dead_letters.filter((item) => item.recoverable);
  const unsupported = sourceInspection.dead_letters.filter((item) => !item.recoverable);
  if (recoverable.length === 0) fail("no recoverable Publication dead letters were found");
  if (unsupported.length > 0) fail("unsupported Publication dead letters must be resolved separately", {
    unsupported: unsupported.map((item) => item.revision_id),
  });
  const channelIds = sortedUnique(recoverable.map((item) => item.channel_id));
  const channels = [];
  for (const channelId of channelIds) {
    const [source, business] = await Promise.all([
      repeatableRead(crawlerPool, (client) => sourceChannelSnapshot(client, destination, channelId)),
      repeatableRead(businessPool, (client) => businessChannelSnapshot(client, channelId)),
    ]);
    const deadLetters = recoverable.filter((item) => item.channel_id === channelId);
    assertPlanChannel({ channelId, deadLetters, source, business });
    const historicalRetractions = normalizedHistoricalRetractions(
      deadLetters.flatMap((item) => item.historical_retractions),
    );
    const revisionIds = Object.fromEntries(DOMAINS.map((domain) => [domain, uuid()]));
    const preview = Object.fromEntries(source.currents.map((current) => {
      const snapshot = recoverySnapshot(current);
      return [current.domain, {
        revision_id: revisionIds[current.domain],
        result_hash: snapshot.result_hash,
        removed_content_ids: snapshot.removed_content_ids,
        historical_retractions: current.domain === "video" ? historicalRetractions : [],
      }];
    }));
    const sourceEvidence = sourceSnapshotEvidence(source);
    const businessEvidence = businessSnapshotEvidence(business);
    channels.push({
      channel_id: channelId,
      old_publication_stream_id: source.owner.publication_stream_id,
      dead_letters: deadLetters.map((item) => ({
        revision_id: item.revision_id,
        data_sequence: item.data_sequence,
        revision_type: item.revision_type,
        payload_hash: item.payload_hash,
        result_hash: item.result_hash,
        failure_kind: item.reason,
        members_only_item_count: item.members_only_item_count,
        non_publishable_item_count: item.non_publishable_item_count,
        non_publishable_access_states: item.non_publishable_access_states,
        retraction_reason_count: item.retraction_reason_count,
        retraction_reasons: item.retraction_reasons,
        historical_retractions: item.historical_retractions,
        business_quarantine: item.business_quarantine,
      })),
      previous_source: sourceEvidence,
      previous_business: businessEvidence,
      source_snapshot_hash: canonicalHash(sourceEvidence),
      business_snapshot_hash: canonicalHash(businessEvidence),
      bootstrap_preview: preview,
    });
  }
  const newStreamId = uuid();
  const suffix = newStreamId.replaceAll("-", "").slice(0, 12);
  const parentStreams = sortedUnique(channels.map((item) => item.old_publication_stream_id));
  const evidence = {
    format: "publication-dead-letter-recovery-v1",
    generated_at: generatedAt,
    destination,
    databases: {
      crawler: sourceInspection.database_name,
      business: businessInspection.database_name,
    },
    recovery_stream: {
      publication_stream_id: newStreamId,
      source_deployment_key: `qy-dead-letter-recovery-${suffix}`,
      source_identity_json: {
        stream_role: "dead_letter_recovery",
        recovery_mode: RECOVERY_MODE,
        parent_publication_stream_ids: parentStreams,
      },
      minimum_writer_version: PUBLICATION_WRITER_VERSION,
      capture_enabled_at: generatedAt,
    },
    channels,
  };
  evidence.evidence_hash = publicationDeadLetterRecoveryEvidenceHash(evidence);
  evidence.summary = {
    channel_count: channels.length,
    dead_letter_count: recoverable.length,
    bootstrap_revision_count: channels.length * DOMAINS.length,
    removed_unpublishable_content_count: channels.reduce((total, channel) => (
      total + channel.bootstrap_preview.video.removed_content_ids.length
    ), 0),
  };
  evidence.can_apply = true;
  return evidence;
}

export function assertPublicationDeadLetterRecoveryEvidence(evidenceValue) {
  const evidence = object(evidenceValue, "Recovery evidence");
  if (evidence.format !== "publication-dead-letter-recovery-v1") {
    throw new TypeError("unsupported Publication dead-letter recovery evidence format");
  }
  const actual = publicationDeadLetterRecoveryEvidenceHash(evidence);
  if (evidence.evidence_hash !== actual) {
    fail("Publication dead-letter recovery evidence hash mismatch", {
      expected: evidence.evidence_hash,
      actual,
    });
  }
  if (!Array.isArray(evidence.channels) || evidence.channels.length === 0) {
    throw new TypeError("Recovery evidence must include Channels");
  }
  id(evidence.recovery_stream?.publication_stream_id, "recovery stream ID");
  return evidence;
}

async function transaction(pool, lockKey, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (lockKey != null) await client.query("SELECT pg_advisory_xact_lock($1)", [lockKey]);
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

function ownershipReference(evidence, channel) {
  return {
    onboarding_mode: RECOVERY_MODE,
    evidence_hash: evidence.evidence_hash,
    old_publication_stream_id: channel.old_publication_stream_id,
    new_publication_stream_id: evidence.recovery_stream.publication_stream_id,
    dead_revision_ids: channel.dead_letters.map((item) => item.revision_id).sort(),
    previous_version_vector: channel.previous_business.version_vector,
  };
}

async function ensureBusinessStream(client, evidence, actor, reason) {
  const stream = evidence.recovery_stream;
  await client.query(
    `/* publication-dead-letter-recovery:register-business-stream */
     INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,
       accepted_contract_versions,registered_by,registered_reason,
       status_changed_by,status_reason
     ) VALUES ($1::uuid,$2,$3::jsonb,ARRAY[1,2]::integer[],$4,$5,$4,$5)
     ON CONFLICT (publication_stream_id) DO NOTHING`,
    [
      stream.publication_stream_id,
      stream.source_deployment_key,
      JSON.stringify(stream.source_identity_json),
      actor,
      reason,
    ],
  );
  const stored = await client.query(
    `SELECT publication_stream_id,source_deployment_key,source_identity_json,status
     FROM publication.stream WHERE publication_stream_id=$1::uuid FOR SHARE`,
    [stream.publication_stream_id],
  );
  const row = stored.rows[0];
  if (!row
      || row.status !== "active"
      || row.source_deployment_key !== stream.source_deployment_key
      || !isDeepStrictEqual(row.source_identity_json, stream.source_identity_json)) {
    fail("Business recovery Stream differs from approved evidence");
  }
}

async function ensureSourceStream(client, evidence, actor, reason) {
  const stream = evidence.recovery_stream;
  await client.query(
    `/* publication-dead-letter-recovery:register-source-stream */
     INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,status,
       minimum_writer_version,capture_enabled_at,created_by,created_reason,
       status_changed_by,status_reason
     ) VALUES ($1::uuid,$2,$3::jsonb,'active',$4,$5::timestamptz,$6,$7,$6,$7)
     ON CONFLICT (publication_stream_id) DO NOTHING`,
    [
      stream.publication_stream_id,
      stream.source_deployment_key,
      JSON.stringify(stream.source_identity_json),
      stream.minimum_writer_version,
      stream.capture_enabled_at,
      actor,
      reason,
    ],
  );
  const stored = await client.query(
    `SELECT publication_stream_id,source_deployment_key,source_identity_json,status,
            minimum_writer_version,capture_enabled_at
     FROM publication.stream WHERE publication_stream_id=$1::uuid FOR SHARE`,
    [stream.publication_stream_id],
  );
  const row = stored.rows[0];
  if (!row
      || row.status !== "active"
      || row.source_deployment_key !== stream.source_deployment_key
      || row.minimum_writer_version !== stream.minimum_writer_version
      || timestamp(row.capture_enabled_at, "stored recovery capture_enabled_at")
        !== stream.capture_enabled_at
      || !isDeepStrictEqual(row.source_identity_json, stream.source_identity_json)) {
    fail("Source recovery Stream differs from approved evidence");
  }
}

function assertInitialSourceSnapshot(channel, snapshot) {
  const evidence = sourceSnapshotEvidence(snapshot);
  const actual = canonicalHash(evidence);
  if (actual !== channel.source_snapshot_hash) {
    fail("Source Channel changed after the approved recovery plan", {
      channel_id: channel.channel_id,
      expected: channel.source_snapshot_hash,
      actual,
    });
  }
}

async function storedRecoverySourceState(client, evidence, channel) {
  const streamId = evidence.recovery_stream.publication_stream_id;
  const result = await client.query(
    `/* publication-dead-letter-recovery:stored-source-state */
     SELECT state.status,state.onboarding_mode,state.seed_status,state.ownership_reference,
            delivery.mode,delivery.cutover_reference,
            (SELECT count(*)::int FROM publication.domain_current current
             WHERE current.publication_stream_id=state.publication_stream_id
               AND current.channel_id=state.channel_id AND current.data_sequence=1
               AND current.readiness_status='ready') AS current_count,
            (SELECT count(*)::int FROM publication.revision revision
             WHERE revision.publication_stream_id=state.publication_stream_id
               AND revision.channel_id=state.channel_id AND revision.data_sequence=1
               AND revision.revision_type='bootstrap') AS revision_count,
            (SELECT count(*)::int FROM publication.outbox outbox
             JOIN publication.revision revision USING(revision_id)
             WHERE revision.publication_stream_id=state.publication_stream_id
               AND revision.channel_id=state.channel_id AND outbox.destination=$3) AS outbox_count
     FROM publication.channel_stream_state state
     LEFT JOIN publication.channel_delivery_state delivery
       ON delivery.publication_stream_id=state.publication_stream_id
      AND delivery.channel_id=state.channel_id AND delivery.destination=$3
     WHERE state.publication_stream_id=$1::uuid AND state.channel_id=$2`,
    [streamId, channel.channel_id, evidence.destination],
  );
  return result.rows[0] ?? null;
}

function verifyStoredRecoverySourceState(row, reference, channelId) {
  if (!row
      || row.status !== "owned"
      || row.onboarding_mode !== "bootstrap"
      || row.seed_status !== "complete"
      || row.mode !== "online"
      || Number(row.current_count) !== DOMAINS.length
      || Number(row.revision_count) !== DOMAINS.length
      || Number(row.outbox_count) !== DOMAINS.length
      || !isDeepStrictEqual(row.ownership_reference, reference)) {
    fail("Stored Source recovery state is incomplete or divergent", { channel_id: channelId });
  }
}

async function prepareSourceChannel(client, evidence, channel, actor, reason) {
  await lockPublicationChannelMutation(client, channel.channel_id);
  const reference = ownershipReference(evidence, channel);
  const existing = await storedRecoverySourceState(client, evidence, channel);
  if (existing) {
    verifyStoredRecoverySourceState(existing, reference, channel.channel_id);
    return { channel_id: channel.channel_id, status: "already_prepared" };
  }
  const snapshot = await sourceChannelSnapshot(client, evidence.destination, channel.channel_id);
  assertInitialSourceSnapshot(channel, snapshot);
  const oldStreamId = channel.old_publication_stream_id;
  const newStreamId = evidence.recovery_stream.publication_stream_id;
  const deadRevisionIds = channel.dead_letters.map((item) => item.revision_id);
  const bootstraps = snapshot.currents.map((current) => buildPublicationRecoveryBootstrap({
    current,
    channelId: channel.channel_id,
    oldStreamId,
    newStreamId,
    revisionId: channel.bootstrap_preview[current.domain].revision_id,
    occurredAt: evidence.generated_at,
    evidenceHash: evidence.evidence_hash,
    deadRevisionIds,
    historicalRetractions: current.domain === "video"
      ? channel.bootstrap_preview.video.historical_retractions
      : [],
  }));
  for (const bootstrap of bootstraps) {
    const expected = channel.bootstrap_preview[bootstrap.envelope.domain];
    if (bootstrap.envelope.result_hash !== expected.result_hash
        || !isDeepStrictEqual(bootstrap.removed_content_ids, expected.removed_content_ids)
        || !isDeepStrictEqual(
          bootstrap.historical_retractions,
          expected.historical_retractions ?? [],
        )) {
      fail("Recovery Bootstrap no longer matches the approved preview", {
        channel_id: channel.channel_id,
        domain: bootstrap.envelope.domain,
      });
    }
  }
  const finalVector = currentVector(snapshot.currents);
  const cutoverReference = {
    recovery_mode: RECOVERY_MODE,
    evidence_hash: evidence.evidence_hash,
    replacement_publication_stream_id: newStreamId,
    dead_revision_ids: deadRevisionIds.sort(),
  };
  await client.query(
    `/* publication-dead-letter-recovery:seal-source-delivery */
     UPDATE publication.channel_delivery_state
     SET mode='sealed',cutover_reference=$4::jsonb,sealed_at=now(),
         state_changed_at=now(),state_changed_by=$5,state_reason=$6,updated_at=now()
     WHERE destination=$1 AND publication_stream_id=$2::uuid AND channel_id=$3
       AND mode='online'`,
    [
      evidence.destination,
      oldStreamId,
      channel.channel_id,
      JSON.stringify(cutoverReference),
      actor,
      reason,
    ],
  );
  const sealed = await client.query(
    `/* publication-dead-letter-recovery:seal-source-owner */
     UPDATE publication.channel_stream_state
     SET status='sealed',final_version_vector=$3::jsonb,sealed_at=now(),
         state_changed_at=now(),state_changed_by=$4,state_reason=$5,updated_at=now()
     WHERE publication_stream_id=$1::uuid AND channel_id=$2 AND status='owned'
     RETURNING channel_id`,
    [oldStreamId, channel.channel_id, JSON.stringify(finalVector), actor, reason],
  );
  if (sealed.rows.length !== 1) fail("Source owner could not be sealed", {
    channel_id: channel.channel_id,
  });
  await client.query(
    `/* publication-dead-letter-recovery:create-source-owner */
     INSERT INTO publication.channel_stream_state (
       publication_stream_id,channel_id,onboarding_mode,seed_status,ownership_reference,
       state_changed_by,state_reason
     ) VALUES ($1::uuid,$2,'bootstrap','pending',$3::jsonb,$4,$5)`,
    [newStreamId, channel.channel_id, JSON.stringify(reference), actor, reason],
  );
  await client.query(
    `/* publication-dead-letter-recovery:create-source-delivery */
     INSERT INTO publication.channel_delivery_state (
       destination,publication_stream_id,channel_id,mode,source_ownership_reference,
       cutover_reference,online_at,state_changed_by,state_reason
     ) VALUES ($1,$2::uuid,$3,'online',$4::jsonb,$5::jsonb,now(),$6,$7)`,
    [
      evidence.destination,
      newStreamId,
      channel.channel_id,
      JSON.stringify(reference),
      JSON.stringify({ evidence_hash: evidence.evidence_hash, source_stream_cutover: true }),
      actor,
      reason,
    ],
  );
  for (const bootstrap of bootstraps) {
    const envelope = bootstrap.envelope;
    await client.query(
      `/* publication-dead-letter-recovery:create-bootstrap */
       INSERT INTO publication.revision (
         revision_id,publication_stream_id,channel_id,domain,data_sequence,
         previous_data_sequence,revision_type,operation,contract_version,policy_version,
         occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
       ) VALUES (
         $1::uuid,$2::uuid,$3,$4,1,NULL,'bootstrap',$5,$6,$7,
         $8::timestamptz,$9::jsonb,NULL,$10,$11,$12::jsonb
       )`,
      [
        envelope.revision_id,
        newStreamId,
        channel.channel_id,
        envelope.domain,
        envelope.operation,
        envelope.contract_version,
        envelope.policy_version,
        envelope.occurred_at,
        JSON.stringify(envelope.source),
        envelope.result_hash,
        envelope.payload_hash,
        JSON.stringify(envelope.payload),
      ],
    );
    await client.query(
      `/* publication-dead-letter-recovery:create-current */
       INSERT INTO publication.domain_current (
         publication_stream_id,channel_id,domain,contract_version,policy_version,
         readiness_status,readiness_reasons,payload_json,result_hash,source_refs,
         complete_observed_at,data_sequence,current_revision_id
       ) VALUES (
         $1::uuid,$2,$3,$4,$5,'ready','[]'::jsonb,$6::jsonb,$7,$8::jsonb,
         $9::timestamptz,1,$10::uuid
       )`,
      [
        newStreamId,
        channel.channel_id,
        envelope.domain,
        envelope.contract_version,
        envelope.policy_version,
        JSON.stringify(bootstrap.current_payload),
        envelope.result_hash,
        JSON.stringify(bootstrap.current_source_refs),
        bootstrap.complete_observed_at,
        envelope.revision_id,
      ],
    );
    await client.query(
      `/* publication-dead-letter-recovery:create-outbox */
       INSERT INTO publication.outbox (destination,revision_id,status)
       VALUES ($1,$2::uuid,'pending')`,
      [evidence.destination, envelope.revision_id],
    );
  }
  await client.query(
    `/* publication-dead-letter-recovery:complete-source-seed */
     UPDATE publication.channel_stream_state
     SET seed_status='complete',seed_completed_at=now(),updated_at=now()
     WHERE publication_stream_id=$1::uuid AND channel_id=$2 AND seed_status='pending'`,
    [newStreamId, channel.channel_id],
  );
  const stored = await storedRecoverySourceState(client, evidence, channel);
  verifyStoredRecoverySourceState(stored, reference, channel.channel_id);
  return { channel_id: channel.channel_id, status: "prepared" };
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForDelivery(crawlerPool, businessPool, evidence, timeoutMs, pollMs) {
  const revisionIds = evidence.channels.flatMap((channel) => (
    DOMAINS.map((domain) => channel.bootstrap_preview[domain].revision_id)
  ));
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const [source, business] = await Promise.all([
      crawlerPool.query(
        `SELECT revision_id,status,receipt_status,last_error
         FROM publication.outbox
         WHERE destination=$1 AND revision_id=ANY($2::uuid[])
         ORDER BY revision_id`,
        [evidence.destination, revisionIds],
      ),
      businessPool.query(
        `SELECT revision_id,receive_status,error_code
         FROM publication.inbox WHERE revision_id=ANY($1::uuid[]) ORDER BY revision_id`,
        [revisionIds],
      ),
    ]);
    const dead = source.rows.find((row) => row.status === "dead_letter");
    if (dead) fail("Recovery Bootstrap entered dead letter", {
      revision_id: dead.revision_id,
      error: dead.last_error,
    });
    const delivered = source.rows.length === revisionIds.length
      && source.rows.every((row) => row.status === "delivered");
    const staged = business.rows.length === revisionIds.length
      && business.rows.every((row) => ["waiting_ownership", "accepted"].includes(row.receive_status));
    if (delivered && staged) return { delivered: source.rows.length, staged: business.rows.length };
    await sleep(pollMs);
  }
  fail("timed out waiting for recovery Bootstrap delivery", { timeout_ms: timeoutMs });
}

async function prepareBusinessOwnership(client, evidence, channel, actor, reason) {
  await client.query(
    `SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
    [channel.channel_id, BUSINESS_ADMIN_LOCK],
  );
  const ownershipLock = await client.query(
    `SELECT channel_id FROM publication.channel_ownership
     WHERE channel_id=$1 FOR UPDATE`,
    [channel.channel_id],
  );
  if (ownershipLock.rows.length !== 1) fail("Business recovery Ownership is missing", {
    channel_id: channel.channel_id,
  });
  const live = await businessChannelSnapshot(client, channel.channel_id);
  const reference = ownershipReference(evidence, channel);
  const newStreamId = evidence.recovery_stream.publication_stream_id;
  if (live.owner?.active_publication_stream_id === newStreamId) {
    if (!isDeepStrictEqual(live.owner.ownership_reference, reference)
        || !["active", "cutover_pending"].includes(live.owner.status)) {
      fail("Business recovery Ownership differs from approved evidence", {
        channel_id: channel.channel_id,
      });
    }
    return { channel_id: channel.channel_id, status: live.owner.status };
  }
  const liveEvidence = businessSnapshotEvidence(live);
  if (canonicalHash(liveEvidence) !== channel.business_snapshot_hash) {
    fail("Business Channel changed after the approved recovery plan", {
      channel_id: channel.channel_id,
    });
  }
  const revisionIds = DOMAINS.map((domain) => channel.bootstrap_preview[domain].revision_id);
  const staged = await client.query(
    `SELECT count(*)::int AS revision_count,
            count(DISTINCT domain)::int AS domain_count,
            bool_and(revision_type='bootstrap' AND data_sequence=1
              AND validation_status='valid'
              AND activation_status IN ('staged','waiting_ownership','superseded')
              AND source_json->'dead_letter_recovery'->>'recovery_version'
                    ='publication-dead-letter-recovery-v1'
              AND source_json->'dead_letter_recovery'->>'evidence_hash'=$4
              AND NOT EXISTS (
                SELECT 1 FROM publication.activation_item AS activated
                WHERE activated.revision_id=publication.revision.revision_id
              )) AS valid
     FROM publication.revision
     WHERE publication_stream_id=$1::uuid AND channel_id=$2
       AND revision_id=ANY($3::uuid[])`,
    [newStreamId, channel.channel_id, revisionIds, evidence.evidence_hash],
  );
  if (Number(staged.rows[0].revision_count) !== DOMAINS.length
      || Number(staged.rows[0].domain_count) !== DOMAINS.length
      || staged.rows[0].valid !== true) {
    fail("Business recovery Bootstrap package is incomplete", { channel_id: channel.channel_id });
  }
  await client.query(
    `/* publication-dead-letter-recovery:reopen-approved-bootstrap */
     UPDATE publication.revision
     SET activation_status='waiting_ownership',updated_at=now()
     WHERE publication_stream_id=$1::uuid AND channel_id=$2
       AND revision_id=ANY($3::uuid[]) AND activation_status='superseded'`,
    [newStreamId, channel.channel_id, revisionIds],
  );
  const updated = await client.query(
    `/* publication-dead-letter-recovery:prepare-business-owner */
     UPDATE publication.channel_ownership
     SET active_publication_stream_id=$2::uuid,status='cutover_pending',
         previous_publication_stream_id=$3::uuid,ownership_reference=$4::jsonb,
         state_changed_by=$5,state_reason=$6,state_changed_at=now(),updated_at=now()
     WHERE channel_id=$1 AND active_publication_stream_id=$3::uuid AND status='active'
     RETURNING channel_id`,
    [
      channel.channel_id,
      newStreamId,
      channel.old_publication_stream_id,
      JSON.stringify(reference),
      actor,
      reason,
    ],
  );
  if (updated.rows.length !== 1) fail("Business recovery Ownership could not enter Cutover", {
    channel_id: channel.channel_id,
  });
  return { channel_id: channel.channel_id, status: "cutover_pending" };
}

async function waitForProjection(businessPool, activationIds, timeoutMs, pollMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    const result = await businessPool.query(
      `SELECT activation_id,status,last_error
       FROM publication.projection_outbox
       WHERE activation_id=ANY($1::uuid[]) ORDER BY activation_id`,
      [activationIds],
    );
    const dead = result.rows.find((row) => row.status === "dead_letter");
    if (dead) fail("Recovery Projection entered dead letter", {
      activation_id: dead.activation_id,
      error: dead.last_error,
    });
    if (result.rows.length === activationIds.length
        && result.rows.every((row) => row.status === "delivered")) {
      return { delivered: result.rows.length };
    }
    await sleep(pollMs);
  }
  fail("timed out waiting for recovery Projection", { timeout_ms: timeoutMs });
}

async function recoveryActivationIds(businessPool, evidence) {
  const expectedByRevision = new Map();
  for (const channel of evidence.channels) {
    for (const domain of DOMAINS) {
      expectedByRevision.set(channel.bootstrap_preview[domain].revision_id, channel.channel_id);
    }
  }
  const revisionIds = [...expectedByRevision.keys()];
  const result = await businessPool.query(
    `/* publication-dead-letter-recovery:activation-evidence */
     SELECT activation.activation_id::text,activation.channel_id,item.revision_id::text
     FROM publication.activation AS activation
     JOIN publication.activation_item AS item USING(activation_id)
     WHERE activation.publication_stream_id=$1::uuid
       AND item.revision_id=ANY($2::uuid[])
     ORDER BY activation.channel_id,activation.activation_id,item.revision_id`,
    [evidence.recovery_stream.publication_stream_id, revisionIds],
  );
  const activationByChannel = new Map();
  const observedRevisions = new Set();
  for (const row of result.rows) {
    const expectedChannelId = expectedByRevision.get(String(row.revision_id));
    if (!expectedChannelId || expectedChannelId !== row.channel_id) {
      fail("Recovery Activation contains divergent Bootstrap evidence", {
        revision_id: row.revision_id,
        channel_id: row.channel_id,
      });
    }
    const activationId = id(row.activation_id, "recovery activation_id");
    const existing = activationByChannel.get(row.channel_id);
    if (existing && existing !== activationId) {
      fail("Recovery Bootstrap package was split across Activations", {
        channel_id: row.channel_id,
      });
    }
    activationByChannel.set(row.channel_id, activationId);
    observedRevisions.add(String(row.revision_id));
  }
  if (observedRevisions.size !== revisionIds.length
      || evidence.channels.some((channel) => !activationByChannel.has(channel.channel_id))) {
    fail("Recovery Activation audit is incomplete", {
      expected_revision_count: revisionIds.length,
      observed_revision_count: observedRevisions.size,
    });
  }
  return evidence.channels.map((channel) => activationByChannel.get(channel.channel_id));
}

async function markSourceRecoveryCompleted(
  client,
  evidence,
  channel,
  activationId,
  actor,
  reason,
) {
  await lockPublicationChannelMutation(client, channel.channel_id);
  const streamId = evidence.recovery_stream.publication_stream_id;
  const reference = ownershipReference(evidence, channel);
  const marker = {
    recovery_mode: RECOVERY_MODE,
    recovery_completed: true,
    evidence_hash: evidence.evidence_hash,
    activation_id: activationId,
    resolved_dead_revision_ids: channel.dead_letters.map((item) => item.revision_id).sort(),
  };
  const stored = await client.query(
    `SELECT mode,source_ownership_reference,cutover_reference
     FROM publication.channel_delivery_state
     WHERE destination=$1 AND publication_stream_id=$2::uuid AND channel_id=$3
     FOR UPDATE`,
    [evidence.destination, streamId, channel.channel_id],
  );
  const row = stored.rows[0];
  if (!row || row.mode !== "online" || !isDeepStrictEqual(row.source_ownership_reference, reference)) {
    fail("Source recovery Delivery differs before completion", {
      channel_id: channel.channel_id,
    });
  }
  if (row.cutover_reference?.recovery_completed === true) {
    for (const [key, value] of Object.entries(marker)) {
      if (!isDeepStrictEqual(row.cutover_reference[key], value)) {
        fail("Stored Source recovery completion differs from approved evidence", {
          channel_id: channel.channel_id,
          field: key,
        });
      }
    }
    return { channel_id: channel.channel_id, status: "already_completed" };
  }
  const updated = await client.query(
    `/* publication-dead-letter-recovery:complete-source-delivery */
     UPDATE publication.channel_delivery_state
     SET cutover_reference=COALESCE(cutover_reference,'{}'::jsonb) || $4::jsonb,
         state_changed_at=now(),state_changed_by=$5,state_reason=$6,updated_at=now()
     WHERE destination=$1 AND publication_stream_id=$2::uuid AND channel_id=$3
       AND mode='online'
     RETURNING channel_id`,
    [
      evidence.destination,
      streamId,
      channel.channel_id,
      JSON.stringify(marker),
      actor,
      reason,
    ],
  );
  if (updated.rows.length !== 1) fail("Source recovery completion could not be recorded", {
    channel_id: channel.channel_id,
  });
  return { channel_id: channel.channel_id, status: "completed" };
}

async function resolveBusinessQuarantines(client, evidence, channel, actor) {
  await client.query(
    `SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
    [channel.channel_id, BUSINESS_ADMIN_LOCK],
  );
  const references = channel.dead_letters.map((item) => {
    const stored = item.business_quarantine;
    if (!stored) {
      return {
        quarantine_id: null,
        revision_id: id(item.revision_id, "dead-letter revision_id"),
        issue_code: "payload_contract_invalid",
        issue_hash: null,
      };
    }
    return {
      quarantine_id: id(stored.quarantine_id, "Business quarantine_id"),
      revision_id: id(stored.revision_id, "Business quarantine revision_id"),
      issue_code: text(stored.issue_code, "Business quarantine issue_code"),
      issue_hash: hash(stored.issue_hash, "Business quarantine issue_hash"),
    };
  }).sort((left, right) => left.revision_id.localeCompare(right.revision_id));
  const revisionIds = references.map((item) => item.revision_id);
  const resolutionReason = `dead-letter recovery ${evidence.evidence_hash}`;
  const load = () => client.query(
    `SELECT quarantine.quarantine_id::text,quarantine.revision_id::text,
            quarantine.issue_code,quarantine.issue_hash,
            quarantine.status,quarantine.resolved_by,
            quarantine.resolution_reason,inbox.channel_id,inbox.domain
     FROM publication.quarantine AS quarantine
     JOIN publication.inbox AS inbox USING(revision_id)
     WHERE quarantine.revision_id=ANY($1::uuid[])
     ORDER BY quarantine.revision_id,quarantine.issue_code,quarantine.issue_hash
     FOR UPDATE OF quarantine`,
    [revisionIds],
  );
  const before = await load();
  const matched = references.map((reference) => before.rows.filter((row) => (
    String(row.revision_id) === reference.revision_id
    && row.issue_code === reference.issue_code
    && (reference.quarantine_id == null
      || String(row.quarantine_id) === reference.quarantine_id)
    && (reference.issue_hash == null || row.issue_hash === reference.issue_hash)
  )));
  if (matched.some((rows) => rows.length !== 1)
      || matched.flat().some((row) => (
        row.channel_id !== channel.channel_id || row.domain !== "video"
      ))) {
    fail("Business Quarantine evidence differs from the approved dead letters", {
      channel_id: channel.channel_id,
    });
  }
  const targetRows = matched.flat();
  const quarantineIds = targetRows.map((row) => String(row.quarantine_id));
  const alreadyResolved = targetRows.every((row) => (
    row.status === "resolved" && row.resolution_reason === resolutionReason
  ));
  if (!alreadyResolved) {
    if (targetRows.some((row) => row.status !== "open")) {
      fail("Business Quarantine was resolved by different evidence", {
        channel_id: channel.channel_id,
      });
    }
    await client.query(
      `/* publication-dead-letter-recovery:resolve-business-quarantine */
       UPDATE publication.quarantine
       SET status='resolved',resolved_at=now(),resolved_by=$2,resolution_reason=$3
       WHERE quarantine_id=ANY($1::uuid[]) AND status='open'`,
      [quarantineIds, actor, resolutionReason],
    );
  }
  const after = await load();
  const resolvedRows = references.map((reference) => after.rows.find((row) => (
    String(row.revision_id) === reference.revision_id
    && row.issue_code === reference.issue_code
    && (reference.quarantine_id == null
      || String(row.quarantine_id) === reference.quarantine_id)
    && (reference.issue_hash == null || row.issue_hash === reference.issue_hash)
  )));
  if (resolvedRows.some((row) => !row || (
    row.status !== "resolved"
    || !optionalText(row.resolved_by)
    || row.resolution_reason !== resolutionReason
  ))) {
    fail("Business Quarantine recovery was not recorded completely", {
      channel_id: channel.channel_id,
    });
  }
  return {
    channel_id: channel.channel_id,
    status: alreadyResolved ? "already_resolved" : "resolved",
    quarantine_count: resolvedRows.length,
  };
}

async function verifyCompletedChannel(
  crawlerPool,
  businessPool,
  evidence,
  channel,
  activationId,
) {
  const newStreamId = evidence.recovery_stream.publication_stream_id;
  const [source, business] = await Promise.all([
    crawlerPool.query(
      `SELECT
         (SELECT status FROM publication.channel_stream_state
          WHERE publication_stream_id=$1::uuid AND channel_id=$2) AS new_owner_status,
         (SELECT status FROM publication.channel_stream_state
          WHERE publication_stream_id=$3::uuid AND channel_id=$2) AS old_owner_status,
         (SELECT count(*)::int FROM publication.outbox outbox
          JOIN publication.revision revision USING(revision_id)
          WHERE revision.publication_stream_id=$1::uuid AND revision.channel_id=$2
            AND outbox.destination=$4 AND outbox.status='delivered') AS delivered_bootstraps`,
      [newStreamId, channel.channel_id, channel.old_publication_stream_id, evidence.destination],
    ),
    businessPool.query(
      `SELECT owner.status,owner.active_publication_stream_id,
              owner.previous_publication_stream_id,owner.ownership_reference,
              (SELECT count(*)::int FROM publication.consumer_cursor cursor
               WHERE cursor.channel_id=owner.channel_id
                 AND cursor.publication_stream_id=$2::uuid) AS new_cursors,
              (SELECT status FROM publication.projection_outbox
               WHERE activation_id=$3::uuid) AS projection_status
       FROM publication.channel_ownership owner WHERE owner.channel_id=$1`,
      [channel.channel_id, newStreamId, activationId],
    ),
  ]);
  const sourceRow = source.rows[0] ?? {};
  const businessRow = business.rows[0] ?? {};
  const complete = sourceRow.new_owner_status === "owned"
    && sourceRow.old_owner_status === "sealed"
    && Number(sourceRow.delivered_bootstraps) === DOMAINS.length
    && businessRow.status === "active"
    && String(businessRow.active_publication_stream_id) === newStreamId
    && String(businessRow.previous_publication_stream_id) === channel.old_publication_stream_id
    && Number(businessRow.new_cursors) === DOMAINS.length
    && businessRow.projection_status === "delivered"
    && isDeepStrictEqual(businessRow.ownership_reference, ownershipReference(evidence, channel));
  if (!complete) fail("Recovery Channel did not reach the completed topology", {
    channel_id: channel.channel_id,
  });
  return {
    channel_id: channel.channel_id,
    status: "recovered",
    resolved_dead_letters: channel.dead_letters.length,
  };
}

export class PublicationDeadLetterRecoveryAdministrator {
  constructor({
    crawlerPool,
    businessPool,
    activator,
    evidence,
    actor,
    reason,
    deliveryTimeoutMs = 120000,
    projectionTimeoutMs = 120000,
    pollMs = 1000,
    afterSourcePrepared = async () => {},
    afterBusinessActivated = async () => {},
  }) {
    this.crawlerPool = crawlerPool;
    this.businessPool = businessPool;
    this.activator = activator;
    this.evidence = assertPublicationDeadLetterRecoveryEvidence(evidence);
    this.actor = text(actor, "actor");
    this.reason = text(reason, "reason");
    this.deliveryTimeoutMs = integer(deliveryTimeoutMs, "deliveryTimeoutMs", 1000);
    this.projectionTimeoutMs = integer(projectionTimeoutMs, "projectionTimeoutMs", 1000);
    this.pollMs = integer(pollMs, "pollMs", 50);
    if (typeof afterSourcePrepared !== "function"
        || typeof afterBusinessActivated !== "function") {
      throw new TypeError("Recovery lifecycle hooks must be functions");
    }
    this.afterSourcePrepared = afterSourcePrepared;
    this.afterBusinessActivated = afterBusinessActivated;
  }

  async apply() {
    const evidence = this.evidence;
    await transaction(this.businessPool, BUSINESS_ADMIN_LOCK, (client) => (
      ensureBusinessStream(client, evidence, this.actor, this.reason)
    ));
    await transaction(this.crawlerPool, SOURCE_ADMIN_LOCK, (client) => (
      ensureSourceStream(client, evidence, this.actor, this.reason)
    ));
    const prepared = [];
    for (const channel of evidence.channels) {
      prepared.push(await transaction(this.crawlerPool, null, (client) => (
        prepareSourceChannel(client, evidence, channel, this.actor, this.reason)
      )));
    }
    await this.afterSourcePrepared({ evidence, prepared });
    const delivery = await waitForDelivery(
      this.crawlerPool,
      this.businessPool,
      evidence,
      this.deliveryTimeoutMs,
      this.pollMs,
    );
    const ownership = [];
    for (const channel of evidence.channels) {
      ownership.push(await transaction(this.businessPool, null, (client) => (
        prepareBusinessOwnership(client, evidence, channel, this.actor, this.reason)
      )));
    }
    const newlyActivated = [];
    for (const channel of evidence.channels) {
      const result = await this.activator.activateReady(channel.channel_id);
      if (!new Set(["cutover_activated", "idle"]).has(result.status)) {
        fail("Business Activator did not complete the approved Cutover", {
          channel_id: channel.channel_id,
          activation_status: result.status,
        });
      }
      if (result.activation_id) newlyActivated.push(result.activation_id);
    }
    const activations = await recoveryActivationIds(this.businessPool, evidence);
    await this.afterBusinessActivated({ evidence, activations, newlyActivated });
    const projection = await waitForProjection(
      this.businessPool,
      activations,
      this.projectionTimeoutMs,
      this.pollMs,
    );
    const channels = [];
    for (const [index, channel] of evidence.channels.entries()) {
      channels.push(await verifyCompletedChannel(
        this.crawlerPool,
        this.businessPool,
        evidence,
        channel,
        activations[index],
      ));
    }
    const businessResolutions = [];
    for (const channel of evidence.channels) {
      businessResolutions.push(await transaction(this.businessPool, null, (client) => (
        resolveBusinessQuarantines(client, evidence, channel, this.actor)
      )));
    }
    const completed = [];
    for (const [index, channel] of evidence.channels.entries()) {
      completed.push(await transaction(this.crawlerPool, null, (client) => (
        markSourceRecoveryCompleted(
          client,
          evidence,
          channel,
          activations[index],
          this.actor,
          this.reason,
        )
      )));
    }
    const unresolved = await repeatableRead(
      this.crawlerPool,
      (client) => sourceDeadLetters(
        client,
        evidence.destination,
        evidence.channels.flatMap((channel) => (
          channel.dead_letters.map((item) => item.revision_id)
        )),
      ),
    );
    if (unresolved.length > 0) {
      fail("Publication dead letters remain unresolved after recovery", {
        revision_ids: unresolved.map((item) => item.revision_id),
      });
    }
    return {
      format: "publication-dead-letter-recovery-result-v1",
      evidence_hash: evidence.evidence_hash,
      recovery_stream_id: evidence.recovery_stream.publication_stream_id,
      prepared,
      delivery,
      ownership,
      activations,
      projection,
      business_resolutions: businessResolutions,
      completed,
      summary: {
        recovered_channels: channels.length,
        resolved_dead_letters: channels.reduce(
          (total, channel) => total + channel.resolved_dead_letters,
          0,
        ),
        unresolved_dead_letters: 0,
      },
      channels,
    };
  }
}
