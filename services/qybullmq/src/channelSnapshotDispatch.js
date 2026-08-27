import { createHash } from "node:crypto";
import { canonicalJsonEqual, canonicalJsonString } from "./canonicalJson.js";
import { channelSnapshotPayload } from "./migrationDispatchPolicy.js";
import { queuesByRole } from "./queues.js";

export const CHANNEL_SNAPSHOT_AGGREGATE_KIND = "channel_snapshot";
export const CHANNEL_SNAPSHOT_JOB_NAME = "channel-snapshot";

export class ChannelSnapshotDispatchConflictError extends Error {
  constructor(message, details = null) {
    super(message);
    this.name = "ChannelSnapshotDispatchConflictError";
    this.code = "channel_snapshot_dispatch_conflict";
    this.details = details;
  }
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function dispatchIntentHash(payload) {
  return `sha256:${createHash("sha256")
    .update(canonicalJsonString({ job_name: CHANNEL_SNAPSHOT_JOB_NAME, payload }))
    .digest("hex")}`;
}

function normalizedCandidate(row) {
  return {
    ...row,
    candidate_id: positiveInteger(row?.candidate_id, "candidate_id"),
    snapshot_dispatch_generation: positiveInteger(
      row?.snapshot_dispatch_generation,
      "snapshot_dispatch_generation",
    ),
    snapshot_active_job_attempt: row?.snapshot_active_job_attempt == null
      ? null
      : nonNegativeInteger(row.snapshot_active_job_attempt, "snapshot_active_job_attempt"),
  };
}

export function channelSnapshotRedispatchPayload(candidate, batchId, {
  minSubscriberCount = 1000,
} = {}) {
  const generation = positiveInteger(
    candidate?.snapshot_dispatch_generation,
    "candidate.snapshot_dispatch_generation",
  );
  const normalizedBatchId = requiredText(batchId, "batchId");
  const migrationIntentId = candidate?.migration_intent_id == null
    ? null
    : positiveInteger(candidate.migration_intent_id, "candidate.migration_intent_id");
  if (migrationIntentId != null) {
    return channelSnapshotPayload(candidate, normalizedBatchId, { minSubscriberCount });
  }
  return {
    candidate_id: positiveInteger(candidate?.candidate_id, "candidate.candidate_id"),
    dispatch_generation: generation,
    dispatch_batch_id: normalizedBatchId,
    channel_id: requiredText(candidate?.channel_id, "candidate.channel_id"),
    channel_url: requiredText(candidate?.channel_url, "candidate.channel_url"),
    crawl_mode: "full",
    query_id: candidate?.query_id ?? null,
    query_text: candidate?.query_text ?? "results.db migration",
    pipeline_cycle_id: candidate?.pipeline_cycle_id || normalizedBatchId,
    enforce_min_subscribers: true,
    min_subscriber_count: Number(minSubscriberCount),
    reject_if_no_recent_content: candidate?.candidate_source === "legacy_results_db",
  };
}

export function buildChannelSnapshotRedispatchAllocation(candidate, batchId, {
  expectedGeneration,
  previousJobId = null,
  jobId,
  minSubscriberCount = 1000,
} = {}) {
  const normalizedExpected = nonNegativeInteger(expectedGeneration, "expectedGeneration");
  const nextCandidate = {
    ...candidate,
    snapshot_dispatch_generation: normalizedExpected + 1,
  };
  return Object.freeze({
    candidate: nextCandidate,
    expectedGeneration: normalizedExpected,
    previousJobId: previousJobId == null ? null : requiredText(previousJobId, "previousJobId"),
    migrationIntentId: candidate?.migration_intent_id == null
      ? null
      : positiveInteger(candidate.migration_intent_id, "candidate.migration_intent_id"),
    payload: channelSnapshotRedispatchPayload(nextCandidate, batchId, { minSubscriberCount }),
    jobId: requiredText(jobId, "jobId"),
  });
}

export function buildChannelSnapshotOutbox({ candidate, payload, jobId } = {}) {
  const candidateId = positiveInteger(candidate?.candidate_id, "candidate.candidate_id");
  const generation = positiveInteger(
    candidate?.snapshot_dispatch_generation,
    "candidate.snapshot_dispatch_generation",
  );
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new TypeError("channel snapshot payload must be an object");
  }
  if (positiveInteger(payload.candidate_id, "payload.candidate_id") !== candidateId
      || positiveInteger(payload.dispatch_generation, "payload.dispatch_generation") !== generation) {
    throw new ChannelSnapshotDispatchConflictError(
      "Channel snapshot payload conflicts with its Candidate generation",
      { candidate_id: candidateId, dispatch_generation: generation },
    );
  }
  const deterministicJobId = requiredText(jobId, "jobId");
  const intentHash = dispatchIntentHash(payload);
  const digest = intentHash.replace(/^sha256:/, "").slice(0, 24);
  return Object.freeze({
    dispatch_id: `channel-snapshot-dispatch:${candidateId}:g${generation}:${digest}`,
    aggregate_kind: CHANNEL_SNAPSHOT_AGGREGATE_KIND,
    aggregate_id: String(candidateId),
    intent_hash: intentHash,
    queue_registry_key: queuesByRole.channelCrawl,
    deterministic_job_id: deterministicJobId,
    payload_json: Object.freeze({ ...payload }),
    status: "pending",
  });
}

function assertMatchingChannelSnapshotOutbox(row, expected) {
  if (!row
      || row.dispatch_id !== expected.dispatch_id
      || row.aggregate_kind !== expected.aggregate_kind
      || String(row.aggregate_id) !== expected.aggregate_id
      || row.intent_hash !== expected.intent_hash
      || row.queue_registry_key !== expected.queue_registry_key
      || row.deterministic_job_id !== expected.deterministic_job_id
      || !canonicalJsonEqual(row.payload_json, expected.payload_json)) {
    throw new ChannelSnapshotDispatchConflictError(
      "Persisted Channel snapshot Outbox conflicts with the requested generation",
      {
        candidate_id: Number(expected.aggregate_id),
        dispatch_generation: expected.payload_json.dispatch_generation,
      },
    );
  }
  return { ...row, payload_json: { ...row.payload_json } };
}

async function loadExactChannelSnapshotOutbox(client, expected) {
  const existing = await client.query(
    `SELECT *
     FROM crawler.proxy_job_dispatch_outbox
     WHERE aggregate_kind='channel_snapshot' AND aggregate_id=$1
       AND (payload_json->>'dispatch_generation')::bigint=$2`,
    [expected.aggregate_id, expected.payload_json.dispatch_generation],
  );
  return assertMatchingChannelSnapshotOutbox(existing.rows?.[0], expected);
}

async function persistChannelSnapshotOutbox(client, expected) {
  const inserted = await client.query(
    `INSERT INTO crawler.proxy_job_dispatch_outbox (
       dispatch_id,aggregate_kind,aggregate_id,intent_hash,queue_registry_key,
       deterministic_job_id,payload_json,status,updated_at
     ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,'pending',now())
     ON CONFLICT DO NOTHING
     RETURNING dispatch_id`,
    [
      expected.dispatch_id,
      expected.aggregate_kind,
      expected.aggregate_id,
      expected.intent_hash,
      expected.queue_registry_key,
      expected.deterministic_job_id,
      JSON.stringify(expected.payload_json),
    ],
  );
  if (inserted.rowCount === 1) return { ...expected };
  return loadExactChannelSnapshotOutbox(client, expected);
}

export async function stageChannelSnapshotOutbox(client, {
  candidate,
  payload,
  jobId,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("PostgreSQL client is required");
  }
  const expected = buildChannelSnapshotOutbox({ candidate, payload, jobId });
  const candidateId = Number(expected.aggregate_id);
  const generation = expected.payload_json.dispatch_generation;
  const fenced = await client.query(
    `UPDATE crawler.channel_candidates
     SET snapshot_active_job_id=$3,
         snapshot_active_job_attempt=CASE
           WHEN snapshot_active_job_id=$3 THEN GREATEST(snapshot_active_job_attempt,0)
           ELSE 0
         END,
         updated_at=now()
     WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
       AND status IN ('discovered','queued','validating')
       AND (
         (snapshot_active_job_id IS NULL AND snapshot_active_job_attempt IS NULL)
         OR (snapshot_active_job_id=$3 AND snapshot_active_job_attempt>=0)
       )
     RETURNING candidate_id,snapshot_dispatch_generation,
               snapshot_active_job_id,snapshot_active_job_attempt`,
    [candidateId, generation, expected.deterministic_job_id],
  );
  if (fenced.rowCount !== 1) {
    throw new ChannelSnapshotDispatchConflictError(
      "Candidate lost its Channel snapshot dispatch fence while staging the Outbox",
      { candidate_id: candidateId, dispatch_generation: generation },
    );
  }
  const outbox = await persistChannelSnapshotOutbox(client, expected);
  return { candidate: normalizedCandidate(fenced.rows[0]), outbox, created: true };
}

export async function allocateChannelSnapshotDispatchOutbox(client, {
  candidate,
  expectedGeneration,
  previousJobId = null,
  migrationIntentId = null,
  payload,
  jobId,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("PostgreSQL client is required");
  }
  const candidateId = positiveInteger(candidate?.candidate_id, "candidate.candidate_id");
  const normalizedExpected = nonNegativeInteger(expectedGeneration, "expectedGeneration");
  const nextGeneration = normalizedExpected + 1;
  if (positiveInteger(candidate?.snapshot_dispatch_generation, "candidate.snapshot_dispatch_generation")
      !== nextGeneration) {
    throw new TypeError("candidate.snapshot_dispatch_generation must equal expectedGeneration + 1");
  }
  const expected = buildChannelSnapshotOutbox({ candidate, payload, jobId });
  const locked = await client.query(
    `SELECT candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
            priority,status,snapshot_dispatch_generation,
            snapshot_active_job_id,snapshot_active_job_attempt
     FROM crawler.channel_candidates
     WHERE candidate_id=$1
     FOR UPDATE`,
    [candidateId],
  );
  const current = locked.rows?.[0];
  if (!current) {
    throw new ChannelSnapshotDispatchConflictError("Channel snapshot Candidate does not exist", {
      candidate_id: candidateId,
    });
  }
  const currentGeneration = Number(current.snapshot_dispatch_generation);
  if (currentGeneration === nextGeneration) {
    const outbox = await loadExactChannelSnapshotOutbox(client, expected);
    return { candidate: normalizedCandidate(current), outbox, created: false };
  }
  if (currentGeneration !== normalizedExpected) {
    throw new ChannelSnapshotDispatchConflictError(
      "Channel snapshot dispatch generation changed before allocation",
      {
        candidate_id: candidateId,
        expected_generation: normalizedExpected,
        current_generation: currentGeneration,
      },
    );
  }
  if (migrationIntentId != null) {
    const intent = await client.query(
      `UPDATE crawler.migration_channel_intents
       SET dispatch_attempts=dispatch_attempts+1,last_dispatch_at=now(),updated_at=now()
       WHERE migration_intent_id=$1 AND target_candidate_id=$2 AND dispatch_attempts=$3
       RETURNING dispatch_attempts`,
      [positiveInteger(migrationIntentId, "migrationIntentId"), candidateId, normalizedExpected],
    );
    if (intent.rowCount !== 1 || Number(intent.rows[0].dispatch_attempts) !== nextGeneration) {
      throw new ChannelSnapshotDispatchConflictError(
        "Migration Intent lost its dispatch generation fence",
        { migration_intent_id: Number(migrationIntentId), candidate_id: candidateId },
      );
    }
  }
  const normalizedPreviousJobId = previousJobId == null
    ? null
    : requiredText(previousJobId, "previousJobId");
  const advanced = await client.query(
    `UPDATE crawler.channel_candidates
     SET status='queued',next_retry_at=NULL,validation_finished_at=NULL,
         snapshot_dispatch_generation=$2 + 1,
         snapshot_active_job_id=$3,snapshot_active_job_attempt=0,
         updated_at=now()
     WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
       AND status IN ('discovered','queued','validating','failed')
       AND (
         ($4::text IS NULL AND snapshot_active_job_id IS NULL
           AND snapshot_active_job_attempt IS NULL)
         OR (snapshot_active_job_id=$4 AND snapshot_active_job_attempt IS NOT NULL)
       )
     RETURNING candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
               priority,status,snapshot_dispatch_generation,
               snapshot_active_job_id,snapshot_active_job_attempt`,
    [candidateId, normalizedExpected, expected.deterministic_job_id, normalizedPreviousJobId],
  );
  if (advanced.rowCount !== 1) {
    throw new ChannelSnapshotDispatchConflictError(
      "Candidate lost its generation or active Job fence while allocating G+1",
      { candidate_id: candidateId, expected_generation: normalizedExpected },
    );
  }
  const outbox = await persistChannelSnapshotOutbox(client, expected);
  return { candidate: normalizedCandidate(advanced.rows[0]), outbox, created: true };
}

function nonNegativeInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return parsed;
}

export async function allocateDiscoveredChannelSnapshotDispatches(query, candidateIds = []) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const ids = [...new Set(candidateIds.map((value) => positiveInteger(value, "candidateId")))];
  if (ids.length === 0) return [];
  const result = await query(
    `UPDATE crawler.channel_candidates
     SET status='queued',
         snapshot_dispatch_generation=CASE
           WHEN snapshot_dispatch_generation=0 THEN 1
           ELSE snapshot_dispatch_generation
         END,
         snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,
         updated_at=now()
     WHERE candidate_id=ANY($1::bigint[]) AND status='discovered'
     RETURNING candidate_id,snapshot_dispatch_generation`,
    [ids],
  );
  return (result.rows ?? []).map((row) => ({
    candidate_id: positiveInteger(row.candidate_id, "candidate_id"),
    snapshot_dispatch_generation: positiveInteger(
      row.snapshot_dispatch_generation,
      "snapshot_dispatch_generation",
    ),
  })).sort((left, right) => left.candidate_id - right.candidate_id);
}
