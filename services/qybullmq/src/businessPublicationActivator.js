import { randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { observationFactsHash } from "./crawlObservationStore.js";
import { publicationResultHash } from "./publicationResultHash.js";

const DOMAINS = Object.freeze(["channel", "video", "agent"]);
const DEAD_LETTER_RECOVERY_ONBOARDING_MODE = "dead_letter_recovery_cutover";
const HASH = /^sha256:[0-9a-f]{64}$/;
// Stored pre-ADR revisions may still contain this historical retraction reason.
const VIDEO_RETRACTION_REASONS = new Set([
  "source_deleted",
  "source_unlisted",
  "source_private",
  "source_unavailable",
  "policy_removed",
]);

export class BusinessPublicationActivationConflict extends Error {
  constructor(code, message, { revisionId = null } = {}) {
    super(message);
    this.name = "BusinessPublicationActivationConflict";
    this.code = code;
    this.revisionId = revisionId;
  }
}

function text(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function sequence(value, field) {
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < 0) {
    throw new BusinessPublicationActivationConflict(
      "stored_sequence_invalid",
      `${field} must be a non-negative safe integer`,
    );
  }
  return output;
}

function object(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new BusinessPublicationActivationConflict(
      "stored_payload_invalid",
      `${field} must be an object`,
    );
  }
  return value;
}

function normalizedRevision(row) {
  return {
    ...row,
    data_sequence: sequence(row.data_sequence, "revision.data_sequence"),
    previous_data_sequence: row.previous_data_sequence == null
      ? null
      : sequence(row.previous_data_sequence, "revision.previous_data_sequence"),
    payload_json: object(row.payload_json, "revision.payload_json"),
  };
}

function cursorByDomain(rows) {
  return new Map(rows.map((row) => [row.domain, {
    ...row,
    active_sequence: sequence(row.active_sequence, "cursor.active_sequence"),
  }]));
}

function versionVector(cursors) {
  return Object.fromEntries(DOMAINS.map((domain) => {
    const cursor = cursors.get(domain);
    return [domain, cursor ? {
      publication_stream_id: String(cursor.publication_stream_id),
      sequence: cursor.active_sequence,
      revision_id: String(cursor.active_revision_id),
      result_hash: cursor.active_result_hash,
    } : null];
  }));
}

function activationConflict(code, message, revision) {
  return new BusinessPublicationActivationConflict(code, message, {
    revisionId: revision?.revision_id ?? null,
  });
}

function selectContiguous(rows, cursor) {
  let expectedSequence = cursor ? cursor.active_sequence + 1 : 1;
  let expectedPreviousHash = cursor?.active_result_hash ?? null;
  const ready = [];
  const superseded = [];
  const waiting = [];
  let conflict = null;

  for (const row of rows) {
    if (row.data_sequence < expectedSequence) {
      superseded.push(row);
      continue;
    }
    if (conflict || row.data_sequence > expectedSequence) {
      waiting.push(row);
      continue;
    }
    if (!cursor && ready.length === 0) {
      if (row.revision_type !== "bootstrap"
          || row.previous_data_sequence !== null
          || row.previous_result_hash !== null) {
        conflict = activationConflict(
          "bootstrap_required",
          "The first active Revision must be a Bootstrap without a previous version",
          row,
        );
        continue;
      }
    } else if (row.revision_type === "bootstrap") {
      conflict = activationConflict(
        "unexpected_bootstrap",
        "Bootstrap cannot overwrite an existing Consumer Cursor",
        row,
      );
      continue;
    } else if (
      row.previous_data_sequence !== expectedSequence - 1
      || row.previous_result_hash !== expectedPreviousHash
    ) {
      conflict = activationConflict(
        "previous_result_hash_conflict",
        "Revision does not continue the active Consumer Cursor",
        row,
      );
      continue;
    } else if (row.result_hash === expectedPreviousHash) {
      conflict = activationConflict(
        "no_change_revision",
        "A Revision cannot advance Sequence without changing result_hash",
        row,
      );
      continue;
    }
    ready.push(row);
    expectedPreviousHash = row.result_hash;
    expectedSequence += 1;
  }
  return { ready, superseded, waiting, conflict };
}

async function quarantineRevision(client, revision, error) {
  const details = {
    code: error.code,
    message: error.message,
    revision_id: revision.revision_id,
    publication_stream_id: revision.publication_stream_id,
    channel_id: revision.channel_id,
    domain: revision.domain,
    data_sequence: revision.data_sequence,
  };
  await client.query(
    `/* business-publication-activator:quarantine-revision */
     UPDATE publication.revision
     SET validation_status='quarantined',activation_status='quarantined',updated_at=now()
     WHERE revision_id=$1::uuid`,
    [revision.revision_id],
  );
  await client.query(
    `/* business-publication-activator:quarantine-evidence */
     INSERT INTO publication.quarantine (
       quarantine_id,revision_id,issue_code,issue_hash,details_json
     ) VALUES ($1,$2::uuid,$3,$4,$5::jsonb)
     ON CONFLICT (revision_id,issue_code,issue_hash) DO UPDATE
     SET last_seen_at=now()`,
    [
      randomUUID(),
      revision.revision_id,
      error.code,
      observationFactsHash(details),
      JSON.stringify(details),
    ],
  );
}

async function markActivationStatuses(client, { superseded = [], waiting = [] }) {
  if (superseded.length > 0) {
    await client.query(
      `/* business-publication-activator:superseded */
       UPDATE publication.revision
       SET activation_status='superseded',updated_at=now()
       WHERE revision_id=ANY($1::uuid[])
         AND activation_status<>'quarantined'`,
      [superseded.map((row) => row.revision_id)],
    );
  }
  if (waiting.length > 0) {
    await client.query(
      `/* business-publication-activator:waiting-gap */
       UPDATE publication.revision
       SET activation_status='waiting_gap',updated_at=now()
       WHERE revision_id=ANY($1::uuid[])
         AND activation_status NOT IN ('active','quarantined','superseded')`,
      [waiting.map((row) => row.revision_id)],
    );
  }
}

function channelState(revision) {
  const actualHash = observationFactsHash(revision.payload_json);
  if (actualHash !== revision.result_hash) {
    throw activationConflict(
      "result_hash_mismatch",
      "Channel Payload no longer matches its immutable result_hash",
      revision,
    );
  }
  return {
    revision,
    payload: revision.payload_json,
    lifecycleStatus: revision.revision_type === "retraction"
      ? "removed"
      : revision.payload_json.lifecycle_status,
    retracted: revision.revision_type === "retraction",
  };
}

function agentState(revision) {
  const actualHash = observationFactsHash(revision.payload_json);
  if (actualHash !== revision.result_hash) {
    throw activationConflict(
      "result_hash_mismatch",
      "Agent Payload no longer matches its immutable result_hash",
      revision,
    );
  }
  return {
    revision,
    payload: revision.payload_json,
    retracted: revision.revision_type === "retraction",
  };
}

function contentRecord(row) {
  return {
    contentId: row.content_id,
    itemHash: row.item_hash,
    payload: object(row.payload_json, "result.content_current.payload_json"),
    position: row.position == null ? null : Number(row.position),
    status: row.window_status,
    reason: row.state_reason,
    sequence: sequence(row.active_sequence, "result.content_current.active_sequence"),
    revisionId: String(row.active_revision_id),
    activatedAt: new Date(row.activated_at).toISOString(),
    touched: false,
  };
}

function activeVideoItems(records) {
  return [...records.values()]
    .filter((record) => record.status === "active")
    .map((record) => record.payload)
    .sort((left, right) => Number(left.position) - Number(right.position));
}

function assertVideoResult(revision, records) {
  const items = activeVideoItems(records);
  if (items.some((item, index) => Number(item.position) !== index + 1)) {
    throw activationConflict(
      "video_current_invalid",
      "Active Video positions must be contiguous from 1",
      revision,
    );
  }
  let actualHash;
  try {
    actualHash = publicationResultHash("video", {
      channel_id: revision.channel_id,
      window_policy: revision.payload_json.window_policy,
      items,
    });
  } catch (error) {
    throw activationConflict(
      "video_current_invalid",
      `Video Current cannot be canonicalized: ${error.message}`,
      revision,
    );
  }
  if (actualHash !== revision.result_hash) {
    throw activationConflict(
      "result_hash_mismatch",
      "Applying the Video Revision does not produce its declared result_hash",
      revision,
    );
  }
}

function upsertVideoItem(records, item, revision) {
  records.set(item.content_id, {
    contentId: item.content_id,
    itemHash: item.item_hash,
    payload: item,
    position: Number(item.position),
    status: "active",
    reason: null,
    sequence: revision.data_sequence,
    revisionId: revision.revision_id,
    activatedAt: null,
    touched: true,
  });
}

function removeVideoItem(records, action, revision, status) {
  const existing = records.get(action.content_id);
  if (!existing || (status === "window_exit" && existing.status !== "active")) {
    throw activationConflict(
      "video_delta_target_missing",
      `Video ${status} references Content outside the active Current`,
      revision,
    );
  }
  records.set(action.content_id, {
    ...existing,
    position: null,
    status,
    reason: action.reason,
    sequence: revision.data_sequence,
    revisionId: revision.revision_id,
    touched: true,
  });
}

function recoveryBootstrapRetractions(revision, recoveryCutover) {
  if (!recoveryCutover || revision.revision_type !== "bootstrap") return new Map();
  const recovery = revision.source_json?.dead_letter_recovery;
  if (!recovery || recovery.recovery_version !== "publication-dead-letter-recovery-v1") {
    return new Map();
  }
  const values = recovery.historical_retractions ?? [];
  if (!Array.isArray(values)) {
    throw activationConflict(
      "recovery_evidence_invalid",
      "Recovery Bootstrap historical_retractions must be an array",
      revision,
    );
  }
  const output = new Map();
  for (const value of values) {
    const item = object(value, "Recovery Bootstrap historical Retraction");
    const contentId = text(item.content_id, "Recovery Bootstrap Retraction content_id");
    const reason = text(item.reason, "Recovery Bootstrap Retraction reason");
    if (!VIDEO_RETRACTION_REASONS.has(reason) || output.has(contentId)) {
      throw activationConflict(
        "recovery_evidence_invalid",
        "Recovery Bootstrap contains an invalid historical Retraction",
        revision,
      );
    }
    output.set(contentId, reason);
  }
  return output;
}

function applyVideoRevision(state, revision, { recoveryCutover = false } = {}) {
  const payload = revision.payload_json;
  if (revision.revision_type === "bootstrap") {
    const historicalRetractions = recoveryBootstrapRetractions(revision, recoveryCutover);
    for (const record of state.records.values()) {
      if (record.status === "active") {
        const historicalReason = historicalRetractions.get(record.contentId) ?? null;
        record.position = null;
        record.status = historicalReason ? "retracted" : "window_exit";
        record.reason = historicalReason ?? "bootstrap_replaced";
        record.sequence = revision.data_sequence;
        record.revisionId = revision.revision_id;
        record.touched = true;
      }
    }
    for (const item of payload.items) upsertVideoItem(state.records, item, revision);
  } else {
    if (!state.header) {
      throw activationConflict(
        "video_current_missing",
        "Video Delta cannot apply without an active Video Current",
        revision,
      );
    }
    for (const item of payload.upserts) upsertVideoItem(state.records, item, revision);
    for (const action of payload.window_exits) {
      removeVideoItem(state.records, action, revision, "window_exit");
    }
    for (const action of payload.retractions) {
      removeVideoItem(state.records, action, revision, "retracted");
    }
  }
  assertVideoResult(revision, state.records);
  state.header = {
    revision,
    windowPolicy: payload.window_policy,
    windowProof: payload.window_proof,
  };
  return state;
}

async function loadVideoState(client, channelId, revisions) {
  const referencedContentIds = [...new Set(revisions.flatMap((revision) => {
    const payload = revision.payload_json;
    return [
      ...(payload.items ?? []),
      ...(payload.upserts ?? []),
      ...(payload.window_exits ?? []),
      ...(payload.retractions ?? []),
    ].map((item) => item.content_id).filter(Boolean);
  }))];
  const headerResult = await client.query(
    `SELECT publication_stream_id,active_sequence,active_revision_id,result_hash,
            window_policy,window_proof
     FROM result.video_current WHERE channel_id=$1 FOR UPDATE`,
    [channelId],
  );
  const contentResult = await client.query(
    `SELECT content_id,item_hash,payload_json,position,window_status,state_reason,
            active_sequence,active_revision_id,activated_at
     FROM result.content_current
     WHERE channel_id=$1
       AND (window_status='active' OR content_id=ANY($2::text[]))
     ORDER BY content_id FOR UPDATE`,
    [channelId, referencedContentIds],
  );
  return {
    header: headerResult.rows[0] ?? null,
    records: new Map(contentResult.rows.map((row) => [row.content_id, contentRecord(row)])),
  };
}

async function simulateDomain(client, channelId, domain, revisions, {
  recoveryCutover = false,
} = {}) {
  let state = null;
  if (domain === "video") state = await loadVideoState(client, channelId, revisions);
  const applied = [];
  let error = null;
  for (const revision of revisions) {
    try {
      if (domain === "channel") state = channelState(revision);
      else if (domain === "video") {
        state = applyVideoRevision(state, revision, { recoveryCutover });
      }
      else state = agentState(revision);
      applied.push(revision);
    } catch (failure) {
      error = failure instanceof BusinessPublicationActivationConflict
        ? failure
        : activationConflict("activation_failed", failure.message || String(failure), revision);
      break;
    }
  }
  return { domain, state, applied, error };
}

async function persistChannelState(client, state, activatedAt) {
  const { revision } = state;
  await client.query(
    `/* business-publication-activator:entity-current */
     INSERT INTO result.entity_current (
       channel_id,publication_stream_id,active_sequence,active_revision_id,
       result_hash,payload_json,lifecycle_status,is_retracted,activated_at
     ) VALUES ($1,$2::uuid,$3,$4::uuid,$5,$6::jsonb,$7,$8,$9::timestamptz)
     ON CONFLICT (channel_id) DO UPDATE
     SET publication_stream_id=EXCLUDED.publication_stream_id,
         active_sequence=EXCLUDED.active_sequence,
         active_revision_id=EXCLUDED.active_revision_id,
         result_hash=EXCLUDED.result_hash,payload_json=EXCLUDED.payload_json,
         lifecycle_status=EXCLUDED.lifecycle_status,is_retracted=EXCLUDED.is_retracted,
         activated_at=EXCLUDED.activated_at,updated_at=now()`,
    [
      revision.channel_id,
      revision.publication_stream_id,
      revision.data_sequence,
      revision.revision_id,
      revision.result_hash,
      JSON.stringify(state.payload),
      state.lifecycleStatus,
      state.retracted,
      activatedAt,
    ],
  );
}

async function persistAgentState(client, state, activatedAt) {
  const { revision } = state;
  await client.query(
    `/* business-publication-activator:agent-current */
     INSERT INTO result.agent_current (
       channel_id,publication_stream_id,active_sequence,active_revision_id,
       result_hash,payload_json,is_retracted,activated_at
     ) VALUES ($1,$2::uuid,$3,$4::uuid,$5,$6::jsonb,$7,$8::timestamptz)
     ON CONFLICT (channel_id) DO UPDATE
     SET publication_stream_id=EXCLUDED.publication_stream_id,
         active_sequence=EXCLUDED.active_sequence,
         active_revision_id=EXCLUDED.active_revision_id,
         result_hash=EXCLUDED.result_hash,payload_json=EXCLUDED.payload_json,
         is_retracted=EXCLUDED.is_retracted,activated_at=EXCLUDED.activated_at,
         updated_at=now()`,
    [
      revision.channel_id,
      revision.publication_stream_id,
      revision.data_sequence,
      revision.revision_id,
      revision.result_hash,
      JSON.stringify(state.payload),
      state.retracted,
      activatedAt,
    ],
  );
}

async function persistVideoState(client, state, activatedAt) {
  const { revision } = state.header;
  await client.query(
    `/* business-publication-activator:clear-video-positions */
     UPDATE result.content_current
     SET position=NULL,window_status='window_exit',state_reason='window_rewrite',updated_at=now()
     WHERE channel_id=$1 AND window_status='active'`,
    [revision.channel_id],
  );
  for (const record of state.records.values()) {
    if (record.status !== "active" && !record.touched) continue;
    await client.query(
      `/* business-publication-activator:content-current */
       INSERT INTO result.content_current (
         channel_id,content_id,publication_stream_id,active_sequence,active_revision_id,
         item_hash,payload_json,position,window_status,state_reason,activated_at
       ) VALUES ($1,$2,$3::uuid,$4,$5::uuid,$6,$7::jsonb,$8,$9,$10,$11::timestamptz)
       ON CONFLICT (channel_id,content_id) DO UPDATE
       SET publication_stream_id=EXCLUDED.publication_stream_id,
           active_sequence=EXCLUDED.active_sequence,
           active_revision_id=EXCLUDED.active_revision_id,
           item_hash=EXCLUDED.item_hash,payload_json=EXCLUDED.payload_json,
           position=EXCLUDED.position,window_status=EXCLUDED.window_status,
           state_reason=EXCLUDED.state_reason,activated_at=EXCLUDED.activated_at,
           updated_at=now()`,
      [
        revision.channel_id,
        record.contentId,
        revision.publication_stream_id,
        record.sequence,
        record.revisionId,
        record.itemHash,
        JSON.stringify(record.payload),
        record.position,
        record.status,
        record.reason,
        record.touched ? activatedAt : record.activatedAt,
      ],
    );
  }
  await client.query(
    `/* business-publication-activator:video-current */
     INSERT INTO result.video_current (
       channel_id,publication_stream_id,active_sequence,active_revision_id,
       result_hash,window_policy,window_proof,activated_at
     ) VALUES ($1,$2::uuid,$3,$4::uuid,$5,$6::jsonb,$7::jsonb,$8::timestamptz)
     ON CONFLICT (channel_id) DO UPDATE
     SET publication_stream_id=EXCLUDED.publication_stream_id,
         active_sequence=EXCLUDED.active_sequence,
         active_revision_id=EXCLUDED.active_revision_id,
         result_hash=EXCLUDED.result_hash,window_policy=EXCLUDED.window_policy,
         window_proof=EXCLUDED.window_proof,activated_at=EXCLUDED.activated_at,
         updated_at=now()`,
    [
      revision.channel_id,
      revision.publication_stream_id,
      revision.data_sequence,
      revision.revision_id,
      revision.result_hash,
      JSON.stringify(state.header.windowPolicy),
      JSON.stringify(state.header.windowProof),
      activatedAt,
    ],
  );
}

async function persistCurrent(client, simulation, activatedAt) {
  if (simulation.domain === "channel") {
    await persistChannelState(client, simulation.state, activatedAt);
  } else if (simulation.domain === "video") {
    await persistVideoState(client, simulation.state, activatedAt);
  } else {
    await persistAgentState(client, simulation.state, activatedAt);
  }
}

async function upsertCursor(client, revision, activatedAt) {
  await client.query(
    `/* business-publication-activator:cursor */
     INSERT INTO publication.consumer_cursor (
       channel_id,domain,publication_stream_id,active_sequence,
       active_revision_id,active_result_hash,activated_at
     ) VALUES ($1,$2,$3::uuid,$4,$5::uuid,$6,$7::timestamptz)
     ON CONFLICT (channel_id,domain) DO UPDATE
     SET publication_stream_id=EXCLUDED.publication_stream_id,
         active_sequence=EXCLUDED.active_sequence,
         active_revision_id=EXCLUDED.active_revision_id,
         active_result_hash=EXCLUDED.active_result_hash,
         activated_at=EXCLUDED.activated_at,updated_at=now()`,
    [
      revision.channel_id,
      revision.domain,
      revision.publication_stream_id,
      revision.data_sequence,
      revision.revision_id,
      revision.result_hash,
      activatedAt,
    ],
  );
}

async function setAppliedRevisionStatuses(client, domain, revisions) {
  if (revisions.length === 0) return;
  const last = revisions.at(-1);
  await client.query(
    `/* business-publication-activator:accepted-gap */
     UPDATE publication.revision
     SET ingress_status='accepted',updated_at=now()
     WHERE revision_id=ANY($1::uuid[])`,
    [revisions.map((row) => row.revision_id)],
  );
  await client.query(
    `/* business-publication-activator:retire-active */
     UPDATE publication.revision
     SET activation_status='superseded',updated_at=now()
     WHERE publication_stream_id=$1::uuid AND channel_id=$2 AND domain=$3
       AND activation_status='active'`,
    [last.publication_stream_id, last.channel_id, domain],
  );
  if (revisions.length > 1) {
    await client.query(
      `/* business-publication-activator:applied-superseded */
       UPDATE publication.revision
       SET activation_status='superseded',updated_at=now()
       WHERE revision_id=ANY($1::uuid[])`,
      [revisions.slice(0, -1).map((row) => row.revision_id)],
    );
  }
  await client.query(
    `/* business-publication-activator:active */
     UPDATE publication.revision
     SET activation_status='active',updated_at=now()
     WHERE revision_id=$1::uuid`,
    [last.revision_id],
  );
}

async function insertActivationAudit(client, {
  activationId,
  streamId,
  channelId,
  ownership,
  beforeVector,
  afterVector,
  applied,
  actor,
  reason,
  activatedAt,
}) {
  await client.query(
    `/* business-publication-activator:activation */
     INSERT INTO publication.activation (
       activation_id,publication_stream_id,channel_id,ownership_reference,
       before_version_vector,after_version_vector,revision_count,
       projection_mode,actor,reason,activated_at
     ) VALUES ($1,$2::uuid,$3,$4::jsonb,$5::jsonb,$6::jsonb,$7,$8,$9,$10,$11::timestamptz)`,
    [
      activationId,
      streamId,
      channelId,
      JSON.stringify({
        active_publication_stream_id: String(ownership.active_publication_stream_id),
        previous_publication_stream_id: ownership.previous_publication_stream_id == null
          ? null
          : String(ownership.previous_publication_stream_id),
        status: ownership.status,
        projection_mode: ownership.projection_mode,
        state_changed_at: new Date(ownership.state_changed_at).toISOString(),
        reference: ownership.ownership_reference,
      }),
      JSON.stringify(beforeVector),
      JSON.stringify(afterVector),
      applied.length,
      ownership.projection_mode,
      actor,
      reason,
      activatedAt,
    ],
  );
  for (const revision of applied) {
    await client.query(
      `/* business-publication-activator:activation-item */
       INSERT INTO publication.activation_item (
         activation_id,revision_id,domain,previous_sequence,active_sequence,
         previous_result_hash,active_result_hash,activated_at
       ) VALUES ($1,$2::uuid,$3,$4,$5,$6,$7,$8::timestamptz)`,
      [
        activationId,
        revision.revision_id,
        revision.domain,
        revision.previous_data_sequence,
        revision.data_sequence,
        revision.previous_result_hash,
        revision.result_hash,
        activatedAt,
      ],
    );
  }
  const projectionStatus = ownership.projection_mode === "online" ? "pending" : "held_shadow";
  await client.query(
    `/* business-publication-activator:projection-outbox */
     INSERT INTO publication.projection_outbox (
       projection_id,activation_id,publication_stream_id,channel_id,
       version_vector,status
     ) VALUES ($1,$2,$3::uuid,$4,$5::jsonb,$6)`,
    [randomUUID(), activationId, streamId, channelId, JSON.stringify(afterVector), projectionStatus],
  );
  return projectionStatus;
}

export class PostgresBusinessPublicationActivator {
  constructor(pool, {
    actor = "business-publication-activator-v1",
    reason = "activate contiguous durable Revisions",
  } = {}) {
    if (!pool || typeof pool.connect !== "function") {
      throw new TypeError("a PostgreSQL Pool is required");
    }
    this.pool = pool;
    this.actor = text(actor, "actor");
    this.reason = text(reason, "reason");
  }

  async activateReady(channelIdValue) {
    const channelId = text(channelIdValue, "channelId");
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const ownershipResult = await client.query(
        `/* business-publication-activator:ownership */
         SELECT active_publication_stream_id,previous_publication_stream_id,status,
                projection_mode,ownership_reference,state_changed_at
         FROM publication.channel_ownership
         WHERE channel_id=$1
         FOR UPDATE`,
        [channelId],
      );
      const ownership = ownershipResult.rows[0];
      const recoveryCutover = ownership?.status === "cutover_pending"
        && ownership.ownership_reference?.onboarding_mode
          === DEAD_LETTER_RECOVERY_ONBOARDING_MODE
        && HASH.test(String(ownership.ownership_reference?.evidence_hash ?? ""))
        && ownership.ownership_reference?.previous_version_vector
        && typeof ownership.ownership_reference.previous_version_vector === "object";
      if (!ownership || (ownership.status !== "active" && !recoveryCutover)) {
        await client.query(
          `UPDATE publication.revision
           SET activation_status='waiting_ownership',updated_at=now()
           WHERE channel_id=$1
             AND activation_status IN ('staged','waiting_gap','waiting_ownership')`,
          [channelId],
        );
        await client.query("COMMIT");
        return { status: "waiting_ownership", channel_id: channelId, applied: [] };
      }
      const streamId = String(ownership.active_publication_stream_id);
      const cursorResult = await client.query(
        `/* business-publication-activator:cursors */
         SELECT channel_id,domain,publication_stream_id,active_sequence,
                active_revision_id,active_result_hash
         FROM publication.consumer_cursor
         WHERE channel_id=$1
         ORDER BY domain
         FOR UPDATE`,
        [channelId],
      );
      const cursors = cursorByDomain(cursorResult.rows);
      const previousStreamId = ownership.previous_publication_stream_id == null
        ? null
        : String(ownership.previous_publication_stream_id);
      if (recoveryCutover && (
        !previousStreamId
        || [...cursors.values()].some((cursor) => (
          String(cursor.publication_stream_id) !== previousStreamId
        ))
        || !isDeepStrictEqual(
          versionVector(cursors),
          ownership.ownership_reference.previous_version_vector,
        )
      )) {
        await client.query("COMMIT");
        return { status: "cutover_cursor_mismatch", channel_id: channelId, applied: [] };
      }
      if (!recoveryCutover && [...cursors.values()].some((cursor) => (
        String(cursor.publication_stream_id) !== streamId
      ))) {
        await client.query("COMMIT");
        return { status: "cutover_required", channel_id: channelId, applied: [] };
      }
      await client.query(
        `/* business-publication-activator:retire-other-streams */
         UPDATE publication.revision AS pending_revision
         SET activation_status='superseded',updated_at=now()
         WHERE pending_revision.channel_id=$1
           AND pending_revision.publication_stream_id<>$2::uuid
           AND pending_revision.activation_status IN ('staged','waiting_gap','waiting_ownership')
           AND NOT (
             COALESCE(
               pending_revision.source_json->'dead_letter_recovery'->>'recovery_version',
               ''
             )
               ='publication-dead-letter-recovery-v1'
             AND COALESCE(
               pending_revision.source_json->'dead_letter_recovery'->>'evidence_hash',
               ''
             )
               ~ '^sha256:[0-9a-f]{64}$'
           )`,
        [channelId, streamId],
      );
      const revisionResult = await client.query(
        `/* business-publication-activator:revisions */
         SELECT revision_id,publication_stream_id,channel_id,domain,data_sequence,
                previous_data_sequence,revision_type,operation,contract_version,
                policy_version,previous_result_hash,result_hash,payload_hash,
                payload_json,source_json,activation_status
         FROM publication.revision
         WHERE publication_stream_id=$1::uuid AND channel_id=$2
           AND validation_status='valid'
           AND activation_status IN ('staged','waiting_gap','waiting_ownership')
         ORDER BY domain,data_sequence,revision_id
         FOR UPDATE`,
        [streamId, channelId],
      );
      const revisions = revisionResult.rows.map(normalizedRevision);
      const selections = new Map();
      for (const domain of DOMAINS) {
        const selection = selectContiguous(
          revisions.filter((row) => row.domain === domain),
          recoveryCutover ? null : cursors.get(domain),
        );
        selections.set(domain, selection);
        await markActivationStatuses(client, selection);
        if (selection.conflict) {
          const revision = revisions.find((row) => row.revision_id === selection.conflict.revisionId);
          await quarantineRevision(client, revision, selection.conflict);
        }
      }

      const onboardingMode = ownership.ownership_reference?.onboarding_mode;
      const automaticBootstrap = onboardingMode === "automatic_bootstrap";
      const independentDomainBootstrap = onboardingMode === "legacy_tracked_adoption";
      const coupledCorePolicy = !recoveryCutover
        && !automaticBootstrap
        && !independentDomainBootstrap;
      const channelCursor = cursors.get("channel");
      const videoCursor = cursors.get("video");
      const activeCursorCount = DOMAINS.filter((domain) => cursors.has(domain)).length;
      if (automaticBootstrap && activeCursorCount !== 0 && activeCursorCount !== DOMAINS.length) {
        await client.query("COMMIT");
        return { status: "inconsistent_initial_package_cursor", channel_id: channelId, applied: [] };
      }
      if (coupledCorePolicy && Boolean(channelCursor) !== Boolean(videoCursor)) {
        await client.query("COMMIT");
        return { status: "inconsistent_core_cursor", channel_id: channelId, applied: [] };
      }
      const coreBootstrap = coupledCorePolicy && !channelCursor && !videoCursor;
      const initialPackageBootstrap = automaticBootstrap && activeCursorCount === 0;
      if (recoveryCutover && DOMAINS.some((domain) => (
        selections.get(domain).ready.length === 0
      ))) {
        await client.query("COMMIT");
        return { status: "waiting_cutover_package", channel_id: channelId, applied: [] };
      }
      if (initialPackageBootstrap && DOMAINS.some((domain) => (
        selections.get(domain).ready.length === 0
      ))) {
        await client.query("COMMIT");
        return { status: "waiting_initial_package", channel_id: channelId, applied: [] };
      }
      if (coreBootstrap && (
        selections.get("channel").ready.length === 0
        || selections.get("video").ready.length === 0
      )) {
        await client.query("COMMIT");
        return { status: "waiting_core_bootstrap", channel_id: channelId, applied: [] };
      }

      const simulations = new Map();
      for (const domain of DOMAINS) {
        if (coreBootstrap && domain === "agent") continue;
        const ready = selections.get(domain).ready;
        if (ready.length === 0) continue;
        const simulation = await simulateDomain(client, channelId, domain, ready, {
          recoveryCutover,
        });
        simulations.set(domain, simulation);
        if (simulation.error) {
          const failed = ready[simulation.applied.length];
          await quarantineRevision(client, failed, simulation.error);
          await markActivationStatuses(client, {
            waiting: ready.slice(simulation.applied.length + 1),
          });
        }
      }
      const initialPackageError = initialPackageBootstrap
        && DOMAINS.some((domain) => simulations.get(domain)?.error);
      const cutoverPackageError = recoveryCutover
        && DOMAINS.some((domain) => simulations.get(domain)?.error);
      if (cutoverPackageError) {
        await client.query("COMMIT");
        return { status: "cutover_package_quarantined", channel_id: channelId, applied: [] };
      }
      if (initialPackageError) {
        await client.query("COMMIT");
        return { status: "initial_package_quarantined", channel_id: channelId, applied: [] };
      }
      if (coreBootstrap && (
        simulations.get("channel")?.error
        || simulations.get("video")?.error
      )) {
        await client.query("COMMIT");
        return { status: "core_bootstrap_quarantined", channel_id: channelId, applied: [] };
      }
      if (coreBootstrap && selections.get("agent").ready.length > 0) {
        const agentSimulation = await simulateDomain(
          client,
          channelId,
          "agent",
          selections.get("agent").ready,
          { recoveryCutover },
        );
        simulations.set("agent", agentSimulation);
        if (agentSimulation.error) {
          const failed = selections.get("agent").ready[agentSimulation.applied.length];
          await quarantineRevision(client, failed, agentSimulation.error);
        }
      }
      const successful = [...simulations.values()].filter((simulation) => (
        simulation.applied.length > 0
      ));
      const applied = successful.flatMap((simulation) => simulation.applied);
      if (applied.length === 0) {
        await client.query("COMMIT");
        return {
          status: revisions.length === 0 ? "idle" : "waiting_gap",
          channel_id: channelId,
          applied: [],
        };
      }

      const activationId = randomUUID();
      const activatedAtResult = await client.query("SELECT clock_timestamp() AS activated_at");
      const activatedAt = new Date(activatedAtResult.rows[0].activated_at).toISOString();
      const beforeVector = versionVector(cursors);
      for (const simulation of successful) {
        await persistCurrent(client, simulation, activatedAt);
        const latest = simulation.applied.at(-1);
        await upsertCursor(client, latest, activatedAt);
        await setAppliedRevisionStatuses(client, simulation.domain, simulation.applied);
        cursors.set(simulation.domain, {
          publication_stream_id: latest.publication_stream_id,
          active_sequence: latest.data_sequence,
          active_revision_id: latest.revision_id,
          active_result_hash: latest.result_hash,
        });
      }
      if (recoveryCutover) {
        await client.query(
          `/* business-publication-activator:retire-cutover-source */
           UPDATE publication.revision
           SET activation_status='superseded',updated_at=now()
           WHERE channel_id=$1 AND publication_stream_id=$2::uuid
             AND activation_status='active'`,
          [channelId, previousStreamId],
        );
      }
      const afterVector = versionVector(cursors);
      const projectionStatus = await insertActivationAudit(client, {
        activationId,
        streamId,
        channelId,
        ownership,
        beforeVector,
        afterVector,
        applied,
        actor: this.actor,
        reason: this.reason,
        activatedAt,
      });
      if (recoveryCutover) {
        const finalizedOwnership = await client.query(
          `/* business-publication-activator:complete-cutover */
           UPDATE publication.channel_ownership
           SET status='active',state_changed_by=$4,state_reason=$5,
               state_changed_at=$6::timestamptz,updated_at=now()
           WHERE channel_id=$1 AND status='cutover_pending'
             AND active_publication_stream_id=$2::uuid
             AND previous_publication_stream_id=$3::uuid
           RETURNING channel_id`,
          [
            channelId,
            streamId,
            previousStreamId,
            this.actor,
            this.reason,
            activatedAt,
          ],
        );
        if (finalizedOwnership.rows.length !== 1) {
          throw new BusinessPublicationActivationConflict(
            "cutover_ownership_changed",
            "Business Publication Ownership changed while completing the Cutover",
          );
        }
      }
      await client.query("COMMIT");
      return {
        status: recoveryCutover ? "cutover_activated" : "activated",
        activation_id: activationId,
        channel_id: channelId,
        publication_stream_id: streamId,
        applied: applied.map((row) => ({
          revision_id: row.revision_id,
          domain: row.domain,
          data_sequence: row.data_sequence,
        })),
        before_version_vector: beforeVector,
        after_version_vector: afterVector,
        projection_status: projectionStatus,
      };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
