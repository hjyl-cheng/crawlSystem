import { observationFactsHash } from "./crawlObservationStore.js";
import { publicationResultHash } from "./publicationResultHash.js";

const DOMAINS = Object.freeze(["channel", "video", "agent"]);

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveSequence(value, field) {
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output <= 0) {
    throw new TypeError(`${field} must be a positive safe integer`);
  }
  return output;
}

function object(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${field} must be an object`);
  }
  return value;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a timestamp`);
  return parsed.toISOString();
}

export function normalizeBusinessPublicationVersionVector(value, field = "versionVector") {
  const source = object(value, field);
  return Object.fromEntries(DOMAINS.map((domain) => {
    const entry = source[domain];
    if (entry == null) return [domain, null];
    const normalized = object(entry, `${field}.${domain}`);
    return [domain, {
      publication_stream_id: requiredText(
        normalized.publication_stream_id,
        `${field}.${domain}.publication_stream_id`,
      ),
      sequence: positiveSequence(normalized.sequence, `${field}.${domain}.sequence`),
      revision_id: requiredText(normalized.revision_id, `${field}.${domain}.revision_id`),
      result_hash: requiredText(normalized.result_hash, `${field}.${domain}.result_hash`),
    }];
  }));
}

export function businessPublicationVersionVectorStreamId(vectorValue, field = "versionVector") {
  const vector = normalizeBusinessPublicationVersionVector(vectorValue, field);
  const streamIds = [...new Set(DOMAINS
    .map((domain) => vector[domain]?.publication_stream_id)
    .filter(Boolean))];
  if (streamIds.length !== 1) {
    throw new TypeError(`${field} must contain exactly one Publication Stream`);
  }
  return streamIds[0];
}

function revisionKey(revisionId) {
  return String(revisionId);
}

function assertRevisionMatchesVector(revision, { channelId, domain, vector }) {
  if (!revision) {
    throw new Error(`Version Vector Revision is missing: ${channelId}/${domain}/${vector.revision_id}`);
  }
  if (
    revision.channel_id !== channelId
      || revision.domain !== domain
      || String(revision.publication_stream_id) !== vector.publication_stream_id
      || Number(revision.data_sequence) !== vector.sequence
      || String(revision.revision_id) !== vector.revision_id
      || revision.result_hash !== vector.result_hash
  ) {
    throw new Error(`Version Vector does not match its immutable Revision: ${channelId}/${domain}`);
  }
}

function baseCurrentRow(revision) {
  const activatedAt = timestamp(revision.activated_at, "revision.activated_at");
  return {
    channel_id: revision.channel_id,
    publication_stream_id: String(revision.publication_stream_id),
    active_sequence: Number(revision.data_sequence),
    active_revision_id: String(revision.revision_id),
    result_hash: revision.result_hash,
    payload_json: object(revision.payload_json, "revision.payload_json"),
    activated_at: activatedAt,
    created_at: activatedAt,
    updated_at: activatedAt,
  };
}

function channelCurrentRow(revision) {
  const row = baseCurrentRow(revision);
  const retracted = revision.revision_type === "retraction";
  const sourceObservedAt = retracted
    ? revision.source_json?.terminal_channel?.removed_at
    : revision.source_json?.complete_observation?.observed_at;
  const sourceObservedAtField = retracted
    ? "revision.source_json.terminal_channel.removed_at"
    : "revision.source_json.complete_observation.observed_at";
  return {
    ...row,
    lifecycle_status: retracted ? "removed" : row.payload_json.lifecycle_status,
    is_retracted: retracted,
    source_observed_at: timestamp(sourceObservedAt, sourceObservedAtField),
  };
}

function agentCurrentRow(revision) {
  return {
    ...baseCurrentRow(revision),
    is_retracted: revision.revision_type === "retraction",
  };
}

function videoItems(records) {
  return [...records.values()]
    .filter((record) => record.window_status === "active")
    .sort((left, right) => Number(left.position) - Number(right.position));
}

function assertVideoState(revision, records) {
  const active = videoItems(records);
  if (active.some((record, index) => Number(record.position) !== index + 1)) {
    throw new Error(`Versioned Video positions are not contiguous: ${revision.channel_id}`);
  }
  const actualHash = publicationResultHash("video", {
    channel_id: revision.channel_id,
    window_policy: revision.payload_json.window_policy,
    items: active.map((record) => record.payload_json),
  });
  if (actualHash !== revision.result_hash) {
    throw new Error(`Versioned Video result_hash mismatch: ${revision.revision_id}`);
  }
}

function upsertVideoRecord(records, revision, itemValue) {
  const item = object(itemValue, "video item");
  const contentId = requiredText(item.content_id, "video item.content_id");
  const activatedAt = timestamp(revision.activated_at, "revision.activated_at");
  const existing = records.get(contentId);
  records.set(contentId, {
    channel_id: revision.channel_id,
    content_id: contentId,
    publication_stream_id: String(revision.publication_stream_id),
    active_sequence: Number(revision.data_sequence),
    active_revision_id: String(revision.revision_id),
    item_hash: requiredText(item.item_hash, "video item.item_hash"),
    payload_json: item,
    position: positiveSequence(item.position, "video item.position"),
    window_status: "active",
    state_reason: null,
    activated_at: activatedAt,
    created_at: existing?.created_at ?? activatedAt,
    updated_at: activatedAt,
  });
}

function removeVideoRecord(records, revision, actionValue, windowStatus) {
  const action = object(actionValue, `video ${windowStatus}`);
  const contentId = requiredText(action.content_id, `video ${windowStatus}.content_id`);
  const existing = records.get(contentId);
  if (!existing || (windowStatus === "window_exit" && existing.window_status !== "active")) {
    throw new Error(`Versioned Video ${windowStatus} target is missing: ${contentId}`);
  }
  records.set(contentId, {
    ...existing,
    active_sequence: Number(revision.data_sequence),
    active_revision_id: String(revision.revision_id),
    position: null,
    window_status: windowStatus,
    state_reason: requiredText(action.reason, `video ${windowStatus}.reason`),
    activated_at: timestamp(revision.activated_at, "revision.activated_at"),
    updated_at: timestamp(revision.activated_at, "revision.activated_at"),
  });
}

function replayVideoRevisions(revisions, target) {
  const records = new Map();
  let previous = null;
  for (const revision of revisions) {
    const sequence = Number(revision.data_sequence);
    if (sequence !== (previous ? Number(previous.data_sequence) + 1 : 1)) {
      throw new Error(`Versioned Video Revision gap: ${revision.channel_id}/${sequence}`);
    }
    if (!previous) {
      if (
        revision.revision_type !== "bootstrap"
          || revision.previous_data_sequence != null
          || revision.previous_result_hash != null
      ) {
        throw new Error(`Versioned Video must begin with Bootstrap: ${revision.channel_id}`);
      }
    } else if (
      Number(revision.previous_data_sequence) !== Number(previous.data_sequence)
        || revision.previous_result_hash !== previous.result_hash
    ) {
      throw new Error(`Versioned Video Revision chain mismatch: ${revision.revision_id}`);
    }

    const payload = object(revision.payload_json, "video revision.payload_json");
    if (revision.revision_type === "bootstrap") {
      records.clear();
      for (const item of payload.items ?? []) upsertVideoRecord(records, revision, item);
    } else {
      for (const item of payload.upserts ?? []) upsertVideoRecord(records, revision, item);
      for (const action of payload.window_exits ?? []) {
        removeVideoRecord(records, revision, action, "window_exit");
      }
      for (const action of payload.retractions ?? []) {
        removeVideoRecord(records, revision, action, "retracted");
      }
    }
    assertVideoState(revision, records);
    previous = revision;
  }
  assertRevisionMatchesVector(previous, {
    channelId: previous?.channel_id,
    domain: "video",
    vector: target,
  });
  const row = baseCurrentRow(previous);
  return {
    header: {
      channel_id: row.channel_id,
      publication_stream_id: row.publication_stream_id,
      active_sequence: row.active_sequence,
      active_revision_id: row.active_revision_id,
      result_hash: row.result_hash,
      window_policy: row.payload_json.window_policy,
      window_proof: row.payload_json.window_proof,
      activated_at: row.activated_at,
      created_at: row.created_at,
      updated_at: row.updated_at,
    },
    contents: videoItems(records),
  };
}

async function loadReferencedRevisions(client, targets) {
  if (targets.length === 0) return new Map();
  const result = await client.query(
    `/* business-publication-version-state:referenced-revisions */
     SELECT revision.revision_id,revision.publication_stream_id,revision.channel_id,
            revision.domain,revision.data_sequence,revision.previous_data_sequence,
            revision.revision_type,revision.previous_result_hash,revision.result_hash,
            revision.payload_json,revision.source_json,activation_item.activated_at
     FROM publication.revision revision
     JOIN publication.activation_item activation_item
       ON activation_item.revision_id=revision.revision_id
     WHERE revision.revision_id=ANY($1::uuid[])
     ORDER BY revision.revision_id`,
    [targets.map((target) => target.vector.revision_id)],
  );
  return new Map(result.rows.map((row) => [revisionKey(row.revision_id), row]));
}

async function loadVideoRevisionChains(client, targets) {
  if (targets.length === 0) return new Map();
  const parameters = targets.map((target) => ({
    channel_id: target.channelId,
    publication_stream_id: target.vector.publication_stream_id,
    target_sequence: target.vector.sequence,
  }));
  const result = await client.query(
    `/* business-publication-version-state:video-revision-chains */
     WITH target AS (
       SELECT * FROM jsonb_to_recordset($1::jsonb) AS value(
         channel_id text,publication_stream_id uuid,target_sequence bigint
       )
     )
     SELECT target.channel_id AS target_channel_id,
            revision.revision_id,revision.publication_stream_id,revision.channel_id,
            revision.domain,revision.data_sequence,revision.previous_data_sequence,
            revision.revision_type,revision.previous_result_hash,revision.result_hash,
            revision.payload_json,revision.source_json,activation_item.activated_at
     FROM target
     JOIN publication.revision revision
       ON revision.channel_id=target.channel_id
      AND revision.publication_stream_id=target.publication_stream_id
      AND revision.domain='video'
      AND revision.data_sequence<=target.target_sequence
     JOIN publication.activation_item activation_item
       ON activation_item.revision_id=revision.revision_id
     WHERE revision.validation_status='valid'
     ORDER BY target.channel_id,revision.data_sequence,revision.revision_id`,
    [JSON.stringify(parameters)],
  );
  const grouped = new Map(targets.map((target) => [target.channelId, []]));
  for (const row of result.rows) grouped.get(row.target_channel_id)?.push(row);
  return grouped;
}

export async function loadBusinessPublicationVersionState(client, versionVectorsValue) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("a PostgreSQL client is required");
  }
  const entries = Object.entries(object(versionVectorsValue, "versionVectors"));
  const versionVectors = Object.fromEntries(entries.map(([channelIdValue, vectorValue]) => {
    const channelId = requiredText(channelIdValue, "channelId");
    return [channelId, normalizeBusinessPublicationVersionVector(
      vectorValue,
      `versionVectors.${channelId}`,
    )];
  }));
  const targets = Object.entries(versionVectors).flatMap(([channelId, vector]) => (
    DOMAINS.flatMap((domain) => vector[domain] ? [{ channelId, domain, vector: vector[domain] }] : [])
  ));
  const referenced = await loadReferencedRevisions(client, targets);
  for (const target of targets) {
    assertRevisionMatchesVector(referenced.get(revisionKey(target.vector.revision_id)), target);
  }

  const channels = new Map();
  const agents = new Map();
  for (const target of targets) {
    const revision = referenced.get(revisionKey(target.vector.revision_id));
    if (target.domain === "channel") channels.set(target.channelId, channelCurrentRow(revision));
    if (target.domain === "agent") agents.set(target.channelId, agentCurrentRow(revision));
  }

  const videoTargets = targets.filter((target) => target.domain === "video");
  const chains = await loadVideoRevisionChains(client, videoTargets);
  const videos = new Map();
  const contents = new Map();
  for (const target of videoTargets) {
    const state = replayVideoRevisions(chains.get(target.channelId) ?? [], target.vector);
    videos.set(target.channelId, state.header);
    contents.set(target.channelId, state.contents);
  }

  for (const [channelId, vector] of Object.entries(versionVectors)) {
    for (const domain of DOMAINS) {
      const entry = vector[domain];
      if (!entry) continue;
      const revision = referenced.get(revisionKey(entry.revision_id));
      const actualHash = domain === "video"
        ? revision.result_hash
        : observationFactsHash(revision.payload_json);
      if (actualHash !== entry.result_hash) {
        throw new Error(`Versioned ${domain} payload hash mismatch: ${channelId}/${entry.sequence}`);
      }
    }
  }

  return { channels, videos, agents, contents, versionVectors };
}
