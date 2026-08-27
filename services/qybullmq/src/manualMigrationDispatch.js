import { randomUUID } from "node:crypto";
import { canonicalJsonEqual } from "./canonicalJson.js";
import {
  allocateChannelSnapshotDispatchOutbox,
  stageChannelSnapshotOutbox,
} from "./channelSnapshotDispatch.js";
import { channelSnapshotPayload } from "./migrationDispatchPolicy.js";
import {
  loadMigrationSourceBatch,
  loadMigrationSourceChannel,
  migrationSourceCandidateIsPending,
  sourceSnapshotHash,
} from "./migrationSource.js";

export const DEFAULT_MANUAL_MIGRATION_BATCH_ID = "legacy-results-manual-v2";
export const MANUAL_MIGRATION_BATCH_SELECTIONS = Object.freeze([100, 200, 500, 1000, 2000]);

const ACTIVE_SCHEDULER_STATUSES = new Set(["running", "finishing", "repairing"]);
const IN_PROGRESS_CANDIDATE_STATUSES = new Set(["queued", "validating"]);
const RETRYABLE_CANDIDATE_STATUSES = new Set(["discovered", "failed"]);
const TERMINAL_CANDIDATE_STATUSES = new Set(["accepted", "rejected", "existing"]);
const REPRESENTED_JOB_STATES = new Set([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "paused",
  "waiting-children",
]);

export class ManualMigrationDispatchError extends Error {
  constructor(message, { statusCode = 400, code = "manual_migration_error", details = null } = {}) {
    super(message);
    this.name = "ManualMigrationDispatchError";
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function requiredText(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    throw new ManualMigrationDispatchError(`${name} is required`, { code: `missing_${name}` });
  }
  return normalized;
}

function positiveCandidateId(value) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new ManualMigrationDispatchError("candidate_id must be a positive integer", {
      code: "invalid_candidate_id",
    });
  }
  return parsed;
}

function safeJobId(...parts) {
  const id = parts
    .flat()
    .map((part) => String(part ?? "").trim())
    .filter(Boolean)
    .map((part) => part.replace(/[^a-zA-Z0-9_-]+/g, "_").replace(/^_+|_+$/g, ""))
    .filter(Boolean)
    .join("__");
  return (id || "job").slice(0, 240);
}

function assertMatchingMigrationJob(job, { jobId, payload }) {
  if (String(job?.id ?? "") !== jobId
      || job?.name !== "channel-snapshot"
      || !canonicalJsonEqual(job?.data, payload)) {
    throw new ManualMigrationDispatchError(
      `deterministic migration Job conflicts with ${jobId}`,
      {
        statusCode: 409,
        code: "job_identity_conflict",
        details: { job_id: jobId },
      },
    );
  }
  return job;
}

function manualChannelSnapshotIdentity(candidate, batchId, minSubscriberCount) {
  const dispatchGeneration = positiveCandidateId(candidate?.snapshot_dispatch_generation);
  if (dispatchGeneration == null) {
    throw new ManualMigrationDispatchError("snapshot dispatch generation is required", {
      code: "missing_dispatch_generation",
    });
  }
  return {
    jobId: safeJobId(
      "channel-snapshot",
      batchId,
      candidate.channel_id,
      `g${dispatchGeneration}`,
    ),
    payload: channelSnapshotPayload(candidate, batchId, { minSubscriberCount }),
  };
}

async function stagePreparedManualMigration(client, {
  candidate,
  batchId,
  minSubscriberCount,
}) {
  const identity = manualChannelSnapshotIdentity(candidate, batchId, minSubscriberCount);
  const staged = await stageChannelSnapshotOutbox(client, {
    candidate,
    payload: identity.payload,
    jobId: identity.jobId,
  });
  return {
    candidate: { ...candidate, ...staged.candidate },
    outbox: staged.outbox,
  };
}

async function defaultTargetTransaction(action) {
  const { withTransaction } = await import("./db.js");
  return withTransaction(action);
}

async function defaultTargetQuery(sql, params = []) {
  const { query } = await import("./db.js");
  return query(sql, params);
}

export function schedulerConflict(scheduler = {}, batchId = DEFAULT_MANUAL_MIGRATION_BATCH_ID) {
  const status = String(scheduler.status || "stopped");
  const pipelineCycleId = String(scheduler.pipeline_cycle_id || "").trim() || null;
  if (status === "paused") {
    return {
      code: "pipeline_paused",
      message: pipelineCycleId === batchId
        ? "manual migration pipeline is paused"
        : `crawler pipeline ${pipelineCycleId || "unknown"} is paused`,
    };
  }
  if (ACTIVE_SCHEDULER_STATUSES.has(status) && pipelineCycleId !== batchId) {
    return {
      code: "pipeline_busy",
      message: `crawler pipeline ${pipelineCycleId || "unknown"} is ${status}`,
    };
  }
  return null;
}

export function normalizeManualMigrationBatchSelection(value) {
  const normalized = String(value ?? "").trim();
  const limit = Number(normalized);
  if (!Number.isSafeInteger(limit) || !MANUAL_MIGRATION_BATCH_SELECTIONS.includes(limit)) {
    throw new ManualMigrationDispatchError(
      `selection must be one of: ${MANUAL_MIGRATION_BATCH_SELECTIONS.join(", ")}`,
      {
      code: "invalid_batch_selection",
      },
    );
  }
  return { selection: String(limit), limit };
}

export function validateMigrationSourceSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) {
    throw new ManualMigrationDispatchError("Migration Source snapshot is required", {
      code: "invalid_source_snapshot",
    });
  }
  for (const field of [
    "source_id",
    "source_database",
    "source_database_oid",
    "source_candidate_id",
    "source_candidate_status",
    "channel_id",
    "channel_url",
    "snapshot_sha256",
  ]) {
    requiredText(snapshot[field], field);
  }
  if (!migrationSourceCandidateIsPending(snapshot.source_candidate_status)) {
    throw new ManualMigrationDispatchError(
      `Migration Source candidate is not pending: ${snapshot.source_candidate_status}`,
      { code: "source_candidate_not_pending" },
    );
  }
  const expectedHash = sourceSnapshotHash(snapshot);
  if (snapshot.snapshot_sha256 !== expectedHash) {
    throw new ManualMigrationDispatchError("Migration Source snapshot hash mismatch", {
      code: "source_snapshot_hash_mismatch",
      details: { expected: expectedHash, received: snapshot.snapshot_sha256 },
    });
  }
  return snapshot;
}

function generatedManualBatchId() {
  return `legacy-results-canary-${Date.now()}-${randomUUID().slice(0, 8)}`;
}

function manualPageId(batchId) {
  return `${batchId}:page:1`;
}

async function lockMigrationChannel(client, channelId) {
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    [`manual-migration:${channelId}`],
  );
}

async function loadSchedulerForUpdate(client, batchId) {
  const result = await client.query(
    `SELECT value_json
     FROM crawler.settings
     WHERE setting_key='query_scheduler'
     LIMIT 1
     FOR UPDATE`,
  );
  if (result.rows.length !== 1) {
    throw new ManualMigrationDispatchError("query_scheduler setting is missing", {
      statusCode: 503,
      code: "scheduler_missing",
    });
  }
  const conflict = schedulerConflict(result.rows[0].value_json, batchId);
  if (conflict) {
    throw new ManualMigrationDispatchError(conflict.message, {
      statusCode: 409,
      code: conflict.code,
      details: result.rows[0].value_json,
    });
  }
}

async function ensureBatchScaffold(client, {
  batchId,
  selection,
  sourceId,
  targetCount,
  minSubscriberCount,
}) {
  const metadata = {
    source: "migration_postgresql",
    purpose: "controlled_migration_canary",
    migration_dispatcher: {
      version: 2,
      source_id: sourceId,
      selection,
      target_count: targetCount,
      min_subscriber_count: Number(minSubscriberCount),
    },
  };
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,
       discovery_closed_at,result_json,updated_at
     ) VALUES ($1,$1,'discovery_closed',$2,now(),$3::jsonb,now())
     ON CONFLICT (dispatch_batch_id) DO UPDATE
     SET status='discovery_closed',discovery_closed_at=now(),
         validation_closed_at=NULL,agent_tail_flushed_at=NULL,finished_at=NULL,
         result_json=crawler.query_dispatch_batches.result_json || EXCLUDED.result_json,
         updated_at=now()`,
    [batchId, targetCount, JSON.stringify(metadata)],
  );
  await client.query(
    `INSERT INTO crawler.query_pages (
       page_id,query_text,page_no,status,candidate_count,should_continue,
       stop_reason,result_json,dispatch_batch_id,finished_at,updated_at
     ) VALUES ($1,'Migration PostgreSQL controlled canary',1,'done',$2,false,
               'controlled_migration_canary',$3::jsonb,$4,now(),now())
     ON CONFLICT (page_id) DO UPDATE
     SET status='done',candidate_count=EXCLUDED.candidate_count,
         should_continue=false,stop_reason='controlled_migration_canary',
         result_json=crawler.query_pages.result_json || EXCLUDED.result_json,
         dispatch_batch_id=EXCLUDED.dispatch_batch_id,finished_at=now(),updated_at=now()`,
    [manualPageId(batchId), targetCount, JSON.stringify(metadata), batchId],
  );
}

async function loadIntentForUpdate(client, snapshot) {
  const result = await client.query(
    `SELECT intent.migration_intent_id,intent.source_id,intent.source_database,
            intent.source_database_oid::text AS source_database_oid,
            intent.source_candidate_id,intent.channel_id,intent.snapshot_sha256,
            intent.target_candidate_id,intent.first_dispatch_batch_id,
            candidate.dispatch_batch_id,candidate.pipeline_cycle_id,
            candidate.channel_url,candidate.priority,candidate.status,
            candidate.snapshot_attempts,candidate.snapshot_dispatch_generation,
            candidate.source_json
     FROM crawler.migration_channel_intents intent
     LEFT JOIN crawler.channel_candidates candidate
       ON candidate.candidate_id=intent.target_candidate_id
     WHERE intent.source_id=$1 AND intent.channel_id=$2
     FOR UPDATE OF intent`,
    [snapshot.source_id, snapshot.channel_id],
  );
  return result.rows[0] ?? null;
}

async function insertIntent(client, snapshot, batchId) {
  const result = await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,
       channel_id,source_snapshot,snapshot_sha256,first_dispatch_batch_id
     ) VALUES ($1,$2,$3::oid,$4::bigint,$5,$6::jsonb,$7,$8)
     ON CONFLICT (source_id,channel_id) DO NOTHING
     RETURNING migration_intent_id,source_id,source_database,
               source_database_oid::text AS source_database_oid,
               source_candidate_id,channel_id,snapshot_sha256,target_candidate_id,
               first_dispatch_batch_id`,
    [
      snapshot.source_id,
      snapshot.source_database,
      snapshot.source_database_oid,
      snapshot.source_candidate_id,
      snapshot.channel_id,
      JSON.stringify(snapshot),
      snapshot.snapshot_sha256,
      batchId,
    ],
  );
  return result.rows[0] ?? null;
}

function targetSourceJson(snapshot, batchId) {
  return {
    ...(snapshot.source_json && typeof snapshot.source_json === "object"
      ? snapshot.source_json
      : {}),
    source: "legacy_results_db",
    migration_source: {
      source_id: snapshot.source_id,
      database: snapshot.source_database,
      database_oid: snapshot.source_database_oid,
      candidate_id: snapshot.source_candidate_id,
      dispatch_batch_id: snapshot.source_dispatch_batch_id,
      snapshot_sha256: snapshot.snapshot_sha256,
      target_batch_id: batchId,
    },
  };
}

async function createTargetCandidate(client, snapshot, batchId, status) {
  const result = await client.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,handle,title,
       description,avatar_url,search_subscriber_count,
       search_subscriber_count_text,is_verified,priority,status,snapshot_json,
       source_json,updated_at
     ) VALUES (
       $1,$1,$2,$3,$4,$5,$6,$7,$8::bigint,$9,$10,$11,$12,'{}'::jsonb,$13::jsonb,now()
     )
     RETURNING candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,
               channel_url,priority,status,snapshot_attempts,
               snapshot_dispatch_generation,source_json`,
    [
      batchId,
      snapshot.channel_id,
      snapshot.channel_url,
      snapshot.handle,
      snapshot.title,
      snapshot.description,
      snapshot.avatar_url,
      snapshot.search_subscriber_count,
      snapshot.search_subscriber_count_text,
      snapshot.is_verified,
      Number(snapshot.priority ?? 100),
      status,
      JSON.stringify(targetSourceJson(snapshot, batchId)),
    ],
  );
  return result.rows[0];
}

async function attachIntentAndSource(client, {
  intentId,
  candidate,
  snapshot,
  batchId,
}) {
  const dispatched = await client.query(
    `WITH bumped AS (
       UPDATE crawler.migration_channel_intents
       SET target_candidate_id=$2,dispatch_attempts=dispatch_attempts+1,
           last_dispatch_at=now(),updated_at=now()
       WHERE migration_intent_id=$1
         AND (target_candidate_id IS NULL OR target_candidate_id=$2)
       RETURNING dispatch_attempts
     )
     UPDATE crawler.channel_candidates candidate
     SET snapshot_dispatch_generation=bumped.dispatch_attempts,
         snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,
         updated_at=now()
     FROM bumped
     WHERE candidate.candidate_id=$2
       AND candidate.snapshot_dispatch_generation<=bumped.dispatch_attempts
     RETURNING candidate.snapshot_dispatch_generation`,
    [intentId, candidate.candidate_id],
  );
  if (dispatched.rowCount !== 1) {
    throw new ManualMigrationDispatchError("Migration dispatch generation could not be allocated", {
      statusCode: 409,
      code: "dispatch_generation_conflict",
      details: { migration_intent_id: intentId, candidate_id: candidate.candidate_id },
    });
  }
  await client.query(
    `INSERT INTO crawler.channel_candidate_sources (
       candidate_id,page_id,query_text,discovery_strategy,source_json
     ) VALUES ($1,$2,'Migration PostgreSQL controlled canary','manual_migration',$3::jsonb)
     ON CONFLICT (candidate_id,page_id,discovery_strategy) DO NOTHING`,
    [
      candidate.candidate_id,
      manualPageId(batchId),
      JSON.stringify({
        source_id: snapshot.source_id,
        source_candidate_id: snapshot.source_candidate_id,
        snapshot_sha256: snapshot.snapshot_sha256,
      }),
    ],
  );
  return Number(dispatched.rows[0].snapshot_dispatch_generation);
}

async function refreshBatchCounts(client, batchId) {
  await client.query(
    `UPDATE crawler.query_dispatch_batches batch
     SET discovered_candidate_count=stats.total,
         accepted_channel_count=stats.accepted,
         rejected_channel_count=stats.rejected,
         updated_at=now()
     FROM (
       SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status='accepted')::int AS accepted,
              count(*) FILTER (WHERE status='rejected')::int AS rejected
       FROM crawler.channel_candidates
       WHERE dispatch_batch_id=$1
     ) stats
     WHERE batch.dispatch_batch_id=$1`,
    [batchId],
  );
  await client.query(
    `UPDATE crawler.query_pages page
     SET candidate_count=(
       SELECT count(*)::int
       FROM crawler.channel_candidates candidate
       WHERE candidate.dispatch_batch_id=$1
     ),updated_at=now()
     WHERE page.page_id=$2`,
    [batchId, manualPageId(batchId)],
  );
}

async function activateScheduler(client, batchId) {
  const now = new Date().toISOString();
  await client.query(
    `UPDATE crawler.settings
     SET value_json=value_json || jsonb_build_object(
           'status','finishing','pipeline_cycle_id',$2::text,
           'started_at',$3::text,'paused_at',NULL,'stopped_at',NULL,
           'completed_at',NULL,'stop_reason','controlled_migration_dispatch',
           'updated_at',$3::text,'updated_by','manualMigrationDispatchV2'
         ),updated_at=now()
     WHERE setting_key=$1`,
    ["query_scheduler", batchId, now],
  );
}

function existingCandidate(intent) {
  if (!intent?.target_candidate_id) return null;
  return {
    candidate_id: positiveCandidateId(intent.target_candidate_id),
    dispatch_batch_id: intent.dispatch_batch_id,
    pipeline_cycle_id: intent.pipeline_cycle_id,
    channel_id: intent.channel_id,
    channel_url: intent.channel_url,
    priority: intent.priority,
    status: intent.status,
    snapshot_attempts: intent.snapshot_attempts,
    snapshot_dispatch_generation: Number(intent.snapshot_dispatch_generation ?? 0),
    source_json: intent.source_json,
  };
}

export async function prepareManualMigration(client, {
  sourceSnapshot,
  batchId = DEFAULT_MANUAL_MIGRATION_BATCH_ID,
  minSubscriberCount = 1000,
} = {}) {
  const snapshot = validateMigrationSourceSnapshot(sourceSnapshot);
  const normalizedBatchId = requiredText(batchId, "batch_id");
  await lockMigrationChannel(client, snapshot.channel_id);

  let intent = await loadIntentForUpdate(client, snapshot);
  let candidate = existingCandidate(intent);
  if (intent && candidate) {
    if (IN_PROGRESS_CANDIDATE_STATUSES.has(candidate.status)
        || TERMINAL_CANDIDATE_STATUSES.has(candidate.status)) {
      return {
        candidate,
        batchId: candidate.dispatch_batch_id,
        previousStatus: candidate.status,
        shouldEnqueue: false,
        alreadyInProgress: IN_PROGRESS_CANDIDATE_STATUSES.has(candidate.status),
        alreadyTerminal: TERMINAL_CANDIDATE_STATUSES.has(candidate.status),
        intentId: intent.migration_intent_id,
        sourceChanged: intent.snapshot_sha256 !== snapshot.snapshot_sha256,
      };
    }
    if (!RETRYABLE_CANDIDATE_STATUSES.has(candidate.status)) {
      throw new ManualMigrationDispatchError(
        `Target Candidate cannot be retried from status: ${candidate.status}`,
        { statusCode: 409, code: "target_candidate_not_retryable" },
      );
    }
    await loadSchedulerForUpdate(client, normalizedBatchId);
    await ensureBatchScaffold(client, {
      batchId: normalizedBatchId,
      selection: "single",
      sourceId: snapshot.source_id,
      targetCount: 1,
      minSubscriberCount,
    });
    const retried = await client.query(
      `UPDATE crawler.channel_candidates
       SET dispatch_batch_id=$2,pipeline_cycle_id=$2,status='queued',
           snapshot_attempts=CASE WHEN status='failed' THEN 0 ELSE snapshot_attempts END,
           reject_reason=NULL,error_message=NULL,next_retry_at=NULL,
           validation_started_at=NULL,validation_finished_at=NULL,accepted_at=NULL,
           updated_at=now()
       WHERE candidate_id=$1
       RETURNING candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,
                 channel_url,priority,status,snapshot_attempts,source_json`,
      [candidate.candidate_id, normalizedBatchId],
    );
    candidate = retried.rows[0];
    const dispatchGeneration = await attachIntentAndSource(client, {
      intentId: intent.migration_intent_id,
      candidate,
      snapshot,
      batchId: normalizedBatchId,
    });
    candidate = { ...candidate, snapshot_dispatch_generation: dispatchGeneration };
    const staged = await stagePreparedManualMigration(client, {
      candidate,
      batchId: normalizedBatchId,
      minSubscriberCount,
    });
    candidate = staged.candidate;
    await refreshBatchCounts(client, normalizedBatchId);
    await activateScheduler(client, normalizedBatchId);
    return {
      candidate,
      batchId: normalizedBatchId,
      previousStatus: intent.status,
      shouldEnqueue: true,
      alreadyInProgress: false,
      intentId: intent.migration_intent_id,
      sourceChanged: intent.snapshot_sha256 !== snapshot.snapshot_sha256,
      outbox: staged.outbox,
    };
  }
  if (intent && !candidate) {
    throw new ManualMigrationDispatchError("Migration intent has no Target Candidate", {
      statusCode: 409,
      code: "migration_intent_incomplete",
      details: { migration_intent_id: intent.migration_intent_id },
    });
  }

  await loadSchedulerForUpdate(client, normalizedBatchId);
  await ensureBatchScaffold(client, {
    batchId: normalizedBatchId,
    selection: "single",
    sourceId: snapshot.source_id,
    targetCount: 1,
    minSubscriberCount,
  });
  intent = await insertIntent(client, snapshot, normalizedBatchId);
  if (!intent) {
    intent = await loadIntentForUpdate(client, snapshot);
    candidate = existingCandidate(intent);
    if (!intent || !candidate) {
      throw new ManualMigrationDispatchError("Migration intent conflict could not be resolved", {
        statusCode: 409,
        code: "migration_intent_conflict",
      });
    }
    return {
      candidate,
      batchId: candidate.dispatch_batch_id,
      previousStatus: candidate.status,
      shouldEnqueue: false,
      alreadyInProgress: true,
      intentId: intent.migration_intent_id,
      sourceChanged: intent.snapshot_sha256 !== snapshot.snapshot_sha256,
    };
  }
  candidate = await createTargetCandidate(client, snapshot, normalizedBatchId, "queued");
  const dispatchGeneration = await attachIntentAndSource(client, {
    intentId: intent.migration_intent_id,
    candidate,
    snapshot,
    batchId: normalizedBatchId,
  });
  candidate = { ...candidate, snapshot_dispatch_generation: dispatchGeneration };
  const staged = await stagePreparedManualMigration(client, {
    candidate,
    batchId: normalizedBatchId,
    minSubscriberCount,
  });
  candidate = staged.candidate;
  await refreshBatchCounts(client, normalizedBatchId);
  await activateScheduler(client, normalizedBatchId);
  return {
    candidate,
    batchId: normalizedBatchId,
    previousStatus: null,
    shouldEnqueue: true,
    alreadyInProgress: false,
    intentId: intent.migration_intent_id,
    sourceChanged: false,
    outbox: staged.outbox,
  };
}

export async function prepareManualMigrationBatch(client, {
  sourceSnapshots,
  selection,
  batchId = generatedManualBatchId(),
  minSubscriberCount = 1000,
} = {}) {
  const normalizedSelection = normalizeManualMigrationBatchSelection(selection);
  const normalizedBatchId = requiredText(batchId, "batch_id");
  const snapshots = [...new Map(
    (sourceSnapshots || []).map((value) => {
      const snapshot = validateMigrationSourceSnapshot(value);
      return [`${snapshot.source_id}:${snapshot.channel_id}`, snapshot];
    }),
  ).values()];
  if (snapshots.length > normalizedSelection.limit) {
    throw new ManualMigrationDispatchError("Source returned more candidates than the canary selection", {
      code: "source_batch_too_large",
    });
  }
  if (snapshots.length === 0) {
    return {
      batchId: null,
      targetCount: 0,
      reusedCount: 0,
      firstSourceCandidateId: null,
      lastSourceCandidateId: null,
    };
  }

  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    ["manual-migration-batch-dispatch-v2"],
  );
  const fresh = [];
  let reusedCount = 0;
  for (const snapshot of snapshots) {
    await lockMigrationChannel(client, snapshot.channel_id);
    const existing = await loadIntentForUpdate(client, snapshot);
    if (existing) reusedCount += 1;
    else fresh.push(snapshot);
  }
  if (fresh.length === 0) {
    return {
      batchId: null,
      targetCount: 0,
      reusedCount,
      firstSourceCandidateId: null,
      lastSourceCandidateId: null,
    };
  }

  await loadSchedulerForUpdate(client, normalizedBatchId);
  await ensureBatchScaffold(client, {
    batchId: normalizedBatchId,
    selection: normalizedSelection.selection,
    sourceId: fresh[0].source_id,
    targetCount: fresh.length,
    minSubscriberCount,
  });
  const materialized = [];
  for (const snapshot of fresh) {
    const intent = await insertIntent(client, snapshot, normalizedBatchId);
    if (!intent) {
      reusedCount += 1;
      continue;
    }
    const candidate = await createTargetCandidate(
      client,
      snapshot,
      normalizedBatchId,
      "discovered",
    );
    await attachIntentAndSource(client, {
      intentId: intent.migration_intent_id,
      candidate,
      snapshot,
      batchId: normalizedBatchId,
    });
    materialized.push(snapshot);
  }
  if (materialized.length > 0) {
    await refreshBatchCounts(client, normalizedBatchId);
    await activateScheduler(client, normalizedBatchId);
  }
  return {
    batchId: materialized.length > 0 ? normalizedBatchId : null,
    targetCount: materialized.length,
    reusedCount,
    firstSourceCandidateId: materialized[0]?.source_candidate_id ?? null,
    lastSourceCandidateId: materialized.at(-1)?.source_candidate_id ?? null,
  };
}

export async function loadMigrationIntentExclusions({
  sourceId = process.env.MIGRATION_SOURCE_ID,
  dbQuery = defaultTargetQuery,
} = {}) {
  const normalizedSourceId = requiredText(sourceId, "source_id");
  const result = await dbQuery(
    `SELECT source_candidate_id::text AS source_candidate_id,channel_id
     FROM crawler.migration_channel_intents
     WHERE source_id=$1`,
    [normalizedSourceId],
  );
  return {
    sourceCandidateIds: result.rows.map((row) => String(row.source_candidate_id)),
    channelIds: result.rows.map((row) => row.channel_id),
  };
}

export async function dispatchManualMigrationBatch({
  selection,
  batchId = generatedManualBatchId(),
  sourceId = process.env.MIGRATION_SOURCE_ID,
  minSubscriberCount = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000),
  exclusionLoader = loadMigrationIntentExclusions,
  sourceBatchLoader = loadMigrationSourceBatch,
  transaction = defaultTargetTransaction,
  targetBatchPreparer = prepareManualMigrationBatch,
} = {}) {
  const normalized = normalizeManualMigrationBatchSelection(selection);
  const exclusions = await exclusionLoader({ sourceId });
  const sourceSnapshots = await sourceBatchLoader({
    limit: normalized.limit,
    excludeSourceCandidateIds: exclusions.sourceCandidateIds,
    excludeChannelIds: exclusions.channelIds,
  });
  const prepared = await transaction((client) => targetBatchPreparer(client, {
    sourceSnapshots,
    selection: normalized.selection,
    batchId,
    minSubscriberCount,
  }));
  return {
    ok: true,
    created: prepared.targetCount > 0,
    selection: normalized.selection,
    source_count: sourceSnapshots.length,
    target_count: prepared.targetCount,
    reused_count: prepared.reusedCount,
    batch_id: prepared.batchId,
    first_source_candidate_id: prepared.firstSourceCandidateId,
    last_source_candidate_id: prepared.lastSourceCandidateId,
  };
}

async function addOrReuseJob(queue, { candidate, batchId, minSubscriberCount }) {
  const { jobId, payload } = manualChannelSnapshotIdentity(
    candidate,
    batchId,
    minSubscriberCount,
  );
  const existing = await queue.getJob(jobId);
  if (existing) {
    assertMatchingMigrationJob(existing, { jobId, payload });
    const state = await existing.getState();
    if (REPRESENTED_JOB_STATES.has(state)) {
      return {
        job: existing,
        jobId,
        state,
        created: false,
      };
    }
    throw new ManualMigrationDispatchError(
      `terminal migration Job requires a new dispatch generation: ${state}`,
      {
        statusCode: 409,
        code: "job_terminal_generation_conflict",
        details: { job_id: jobId, state },
      },
    );
  }
  let job;
  try {
    await queue.add(
      "channel-snapshot",
      payload,
      { jobId, priority: Number(candidate.priority ?? 100) },
    );
  } catch (error) {
    const persisted = await queue.getJob(jobId).catch(() => null);
    if (persisted) {
      assertMatchingMigrationJob(persisted, { jobId, payload });
      const persistedState = await persisted.getState().catch(() => null);
      if (REPRESENTED_JOB_STATES.has(persistedState)) {
        return {
          job: persisted,
          jobId,
          state: persistedState,
          created: false,
        };
      }
      throw new ManualMigrationDispatchError(
        `terminal migration Job requires a new dispatch generation: ${persistedState}`,
        {
          statusCode: 409,
          code: "job_terminal_generation_conflict",
          details: { job_id: jobId, state: persistedState },
        },
      );
    }
    throw error;
  }
  const persisted = await queue.getJob(jobId);
  if (!persisted) {
    throw new ManualMigrationDispatchError(
      `migration Job was not persisted for ${jobId}`,
      {
        statusCode: 503,
        code: "job_persistence_unconfirmed",
        details: { job_id: jobId },
      },
    );
  }
  job = assertMatchingMigrationJob(persisted, { jobId, payload });
  const state = await job.getState();
  if (!REPRESENTED_JOB_STATES.has(state)) {
    throw new ManualMigrationDispatchError(
      `terminal migration Job requires a new dispatch generation: ${state}`,
      {
        statusCode: 409,
        code: "job_terminal_generation_conflict",
        details: { job_id: jobId, state },
      },
    );
  }
  return { job, jobId, state, created: true };
}

async function markChannelSnapshotOutboxSent(dbQuery, outbox) {
  const marked = await dbQuery(
    `UPDATE crawler.proxy_job_dispatch_outbox
     SET status='sent',sent_at=COALESCE(sent_at,now()),next_attempt_at=NULL,
         last_error=NULL,updated_at=now()
     WHERE dispatch_id=$1 AND aggregate_kind='channel_snapshot'
       AND aggregate_id=$2
       AND (payload_json->>'dispatch_generation')::bigint=$3
       AND status IN ('pending','sending','sent')
     RETURNING dispatch_id`,
    [
      outbox.dispatch_id,
      String(outbox.aggregate_id),
      Number(outbox.payload_json?.dispatch_generation),
    ],
  );
  if (marked.rowCount !== 1) {
    throw new ManualMigrationDispatchError("Channel snapshot Outbox lost its delivery fence", {
      statusCode: 409,
      code: "channel_snapshot_outbox_fence_lost",
      details: { dispatch_id: outbox.dispatch_id },
    });
  }
}

export async function deliverExistingChannelSnapshotOutbox(queue, outbox, {
  priority = 100,
  dbQuery = defaultTargetQuery,
} = {}) {
  const jobId = requiredText(outbox?.deterministic_job_id, "outbox_job_id");
  const payload = outbox?.payload_json;
  let persisted = await queue.getJob(jobId);
  const created = !persisted;
  if (persisted) assertMatchingMigrationJob(persisted, { jobId, payload });
  if (!persisted) {
    try {
      await queue.add("channel-snapshot", payload, { jobId, priority: Number(priority ?? 100) });
    } catch (error) {
      persisted = await queue.getJob(jobId).catch(() => null);
      if (!persisted) throw error;
    }
    persisted = persisted ?? await queue.getJob(jobId);
  }
  if (!persisted) {
    throw new ManualMigrationDispatchError(`migration Job was not persisted for ${jobId}`, {
      statusCode: 503,
      code: "job_persistence_unconfirmed",
      details: { job_id: jobId },
    });
  }
  const job = assertMatchingMigrationJob(persisted, { jobId, payload });
  const state = await job.getState();
  await markChannelSnapshotOutboxSent(dbQuery, outbox);
  return { job, jobId, state, created };
}

async function advanceTerminalManualMigrationDispatch(client, {
  prepared,
  minSubscriberCount,
}) {
  const currentGeneration = positiveCandidateId(prepared?.candidate?.snapshot_dispatch_generation);
  if (currentGeneration == null) {
    throw new ManualMigrationDispatchError("snapshot dispatch generation is required", {
      code: "missing_dispatch_generation",
    });
  }
  const previous = manualChannelSnapshotIdentity(
    prepared.candidate,
    prepared.batchId,
    minSubscriberCount,
  );
  const candidate = {
    ...prepared.candidate,
    snapshot_dispatch_generation: currentGeneration + 1,
  };
  const next = manualChannelSnapshotIdentity(candidate, prepared.batchId, minSubscriberCount);
  return allocateChannelSnapshotDispatchOutbox(client, {
    candidate,
    expectedGeneration: currentGeneration,
    previousJobId: previous.jobId,
    migrationIntentId: prepared.intentId,
    payload: next.payload,
    jobId: next.jobId,
  });
}

export async function dispatchManualMigrationChannel({
  channelId,
  candidateId = null,
  queue,
  batchId = process.env.MIGRATION_MANUAL_BATCH_ID || DEFAULT_MANUAL_MIGRATION_BATCH_ID,
  minSubscriberCount = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000),
  sourceLoader = loadMigrationSourceChannel,
  transaction = defaultTargetTransaction,
  targetPreparer = prepareManualMigration,
  terminalDispatchAdvancer = advanceTerminalManualMigrationDispatch,
  outboxDeliverer = deliverExistingChannelSnapshotOutbox,
  dbQuery = defaultTargetQuery,
} = {}) {
  if (!queue || typeof queue.add !== "function" || typeof queue.getJob !== "function") {
    throw new ManualMigrationDispatchError("channel crawl queue is required", {
      statusCode: 503,
      code: "queue_missing",
    });
  }
  const normalizedChannelId = requiredText(channelId, "channel_id");
  const normalizedCandidateId = positiveCandidateId(candidateId);

  const sourceSnapshot = await sourceLoader({
    channelId: normalizedChannelId,
    candidateId: normalizedCandidateId,
  });
  validateMigrationSourceSnapshot(sourceSnapshot);
  const prepared = await transaction((client) => targetPreparer(client, {
    sourceSnapshot,
    batchId,
    minSubscriberCount,
  }));

  const responseBase = {
    ok: true,
    source_id: sourceSnapshot.source_id,
    source_candidate_id: Number(sourceSnapshot.source_candidate_id),
    candidate_id: Number(prepared.candidate.candidate_id),
    channel_id: prepared.candidate.channel_id,
    candidate_status: prepared.candidate.status,
    batch_id: prepared.batchId,
    migration_intent_id: Number(prepared.intentId),
    source_changed: prepared.sourceChanged === true,
  };
  if (!prepared.shouldEnqueue) {
    return {
      ...responseBase,
      created: false,
      already_in_progress: prepared.alreadyInProgress === true,
      already_terminal: prepared.alreadyTerminal === true,
      job: null,
    };
  }

  try {
    if (prepared.outbox) {
      const queued = await outboxDeliverer(queue, prepared.outbox, {
        priority: prepared.candidate.priority,
        dbQuery,
      });
      return {
        ...responseBase,
        created: queued.created,
        already_in_progress: !queued.created,
        already_terminal: false,
        previous_status: prepared.previousStatus,
        job: { queue: queue.name, id: queued.job.id, state: queued.state },
      };
    }
    const queued = await addOrReuseJob(queue, {
      candidate: prepared.candidate,
      batchId: prepared.batchId,
      minSubscriberCount,
    });
    return {
      ...responseBase,
      created: queued.created,
      already_in_progress: !queued.created,
      already_terminal: false,
      previous_status: prepared.previousStatus,
      job: {
        queue: queue.name,
        id: queued.job.id,
        state: queued.state,
      },
    };
  } catch (error) {
    if (error?.code === "job_terminal_generation_conflict") {
      const advanced = await transaction((client) => terminalDispatchAdvancer(client, {
        prepared,
        minSubscriberCount,
      }));
      const queued = await outboxDeliverer(queue, advanced.outbox, {
        priority: advanced.candidate.priority ?? prepared.candidate.priority,
        dbQuery,
      });
      return {
        ...responseBase,
        created: queued.created,
        already_in_progress: !queued.created,
        already_terminal: false,
        previous_status: prepared.previousStatus,
        dispatch_generation: advanced.candidate.snapshot_dispatch_generation,
        job: { queue: queue.name, id: queued.job.id, state: queued.state },
      };
    }
    if (prepared.outbox) throw error;
    await dbQuery(
      `WITH failed_candidate AS (
         UPDATE crawler.channel_candidates
         SET status='failed',error_message=$2,updated_at=now()
         WHERE candidate_id=$1 AND snapshot_dispatch_generation=$3
           AND snapshot_active_job_id IS NULL AND snapshot_active_job_attempt IS NULL
           AND status='queued'
         RETURNING candidate_id
       )
       UPDATE crawler.migration_channel_intents
       SET last_error=$2,updated_at=now()
       WHERE target_candidate_id=$1 AND dispatch_attempts=$3`,
      [
        prepared.candidate.candidate_id,
        `queue delivery failed: ${error?.message || String(error)}`,
        prepared.candidate.snapshot_dispatch_generation,
      ],
    ).catch(() => {});
    throw error;
  }
}
