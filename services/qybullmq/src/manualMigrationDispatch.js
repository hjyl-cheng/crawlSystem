import { randomUUID } from "node:crypto";
import { query, withTransaction } from "./db.js";
import { channelSnapshotPayload } from "./migrationDispatchPolicy.js";
import { safeJobId } from "./queues.js";

export const DEFAULT_MANUAL_MIGRATION_BATCH_ID = "legacy-results-manual-v1";
export const MANUAL_MIGRATION_BATCH_SELECTIONS = Object.freeze([500, 1000, 2000, 5000, 10000]);

const ACTIVE_SCHEDULER_STATUSES = new Set(["running", "finishing", "repairing"]);
const IN_PROGRESS_CANDIDATE_STATUSES = new Set(["queued", "validating"]);
const ELIGIBLE_CANDIDATE_STATUSES = new Set(["discovered", "failed"]);
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

function nonemptyText(value, name) {
  const parsed = String(value ?? "").trim();
  if (!parsed) {
    throw new ManualMigrationDispatchError(`${name} is required`, {
      code: `missing_${name}`,
    });
  }
  return parsed;
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
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized === "all") return { selection: "all", limit: null };
  const limit = Number(normalized);
  if (!Number.isSafeInteger(limit) || !MANUAL_MIGRATION_BATCH_SELECTIONS.includes(limit)) {
    throw new ManualMigrationDispatchError("selection must be one of: 500, 1000, 2000, 5000, 10000, all", {
      code: "invalid_batch_selection",
    });
  }
  return { selection: String(limit), limit };
}

function generatedManualBatchId() {
  return `legacy-results-manual-${Date.now()}-${randomUUID().slice(0, 8)}`;
}

function manualPageId(batchId) {
  return `${batchId}:page:1`;
}

async function refreshBatchCounts(client, batchId) {
  if (!batchId) return;
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
}

async function refreshSourcePageCounts(client, batchId) {
  if (!batchId) return;
  await client.query(
    `UPDATE crawler.query_pages page
     SET candidate_count=(
           SELECT count(DISTINCT source.candidate_id)::int
           FROM crawler.channel_candidate_sources source
           JOIN crawler.channel_candidates candidate ON candidate.candidate_id=source.candidate_id
           WHERE source.page_id=page.page_id
             AND candidate.dispatch_batch_id=$1
         ),
         updated_at=now()
     WHERE page.dispatch_batch_id=$1`,
    [batchId],
  );
}

async function loadCandidateForUpdate(client, channelId, candidateId) {
  const result = await client.query(
    `SELECT candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
            priority,status,snapshot_attempts,source_json
     FROM crawler.channel_candidates
     WHERE channel_id=$1
       AND source_json->>'source'='legacy_results_db'
       AND ($2::bigint IS NULL OR candidate_id=$2::bigint)
     ORDER BY candidate_id DESC
     LIMIT 1
     FOR UPDATE`,
    [channelId, candidateId],
  );
  return result.rows[0] ?? null;
}

export async function prepareManualMigrationBatch(client, {
  selection,
  batchId = generatedManualBatchId(),
  minSubscriberCount = 1000,
} = {}) {
  const normalizedSelection = normalizeManualMigrationBatchSelection(selection);
  const normalizedBatchId = nonemptyText(batchId, "batch_id");

  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    ["manual-migration-batch-dispatch"],
  );
  const schedulerRows = await client.query(
    `SELECT value_json
     FROM crawler.settings
     WHERE setting_key='query_scheduler'
     LIMIT 1
     FOR UPDATE`,
  );
  if (schedulerRows.rows.length === 0) {
    throw new ManualMigrationDispatchError("query_scheduler setting is missing", {
      statusCode: 503,
      code: "scheduler_missing",
    });
  }
  const conflict = schedulerConflict(schedulerRows.rows[0].value_json, normalizedBatchId);
  if (conflict) {
    throw new ManualMigrationDispatchError(conflict.message, {
      statusCode: 409,
      code: conflict.code,
      details: schedulerRows.rows[0].value_json,
    });
  }

  const candidates = await client.query(
    `SELECT candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
            priority,status,snapshot_attempts,source_json
     FROM crawler.channel_candidates
     WHERE source_json->>'source'='legacy_results_db'
       AND status IN ('discovered','failed')
     ORDER BY priority DESC,candidate_id
     LIMIT $1::int
     FOR UPDATE`,
    [normalizedSelection.limit],
  );
  if (candidates.rows.length === 0) {
    return {
      batchId: null,
      selection: normalizedSelection.selection,
      targetCount: 0,
      firstCandidateId: null,
      lastCandidateId: null,
    };
  }

  const candidateIds = candidates.rows.map((candidate) => candidate.candidate_id);
  const sourceBatchIds = [...new Set(
    candidates.rows.map((candidate) => String(candidate.dispatch_batch_id || "").trim()).filter(Boolean),
  )];
  const pageId = manualPageId(normalizedBatchId);
  const metadata = {
    source: "results.db",
    purpose: "manual_migration_batch",
    migration_dispatcher: {
      mode: "dashboard_batch",
      selection: normalizedSelection.selection,
      target_count: candidateIds.length,
      source_batch_ids: sourceBatchIds,
      min_subscriber_count: Number(minSubscriberCount),
      first_candidate_id: candidateIds[0],
      last_candidate_id: candidateIds.at(-1),
    },
  };

  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,
       discovery_closed_at,result_json,updated_at
     ) VALUES ($1,$1,'discovery_closed',$2,now(),$3::jsonb,now())`,
    [normalizedBatchId, candidateIds.length, JSON.stringify(metadata)],
  );
  await client.query(
    `INSERT INTO crawler.query_pages (
       page_id,query_text,page_no,status,candidate_count,should_continue,
       stop_reason,result_json,dispatch_batch_id,finished_at,updated_at
     ) VALUES ($1,'results.db batch migration',1,'done',$2,false,
               'manual_migration_batch',$3::jsonb,$4,now(),now())`,
    [pageId, candidateIds.length, JSON.stringify(metadata), normalizedBatchId],
  );
  const updatedCandidates = await client.query(
    `UPDATE crawler.channel_candidates
     SET dispatch_batch_id=$2,pipeline_cycle_id=$2,status='discovered',
         snapshot_attempts=CASE WHEN status='failed' THEN 0 ELSE snapshot_attempts END,
         reject_reason=NULL,error_message=NULL,next_retry_at=NULL,
         validation_started_at=NULL,validation_finished_at=NULL,accepted_at=NULL,
         source_json=source_json || jsonb_build_object(
           'manual_migration',jsonb_build_object(
             'requested_at',now(),
             'source_batch_id',dispatch_batch_id,
             'batch_id',$2::text,
             'mode','dashboard_batch'
           )
         ),
         updated_at=now()
     WHERE candidate_id=ANY($1::bigint[])
       AND status IN ('discovered','failed')
     RETURNING candidate_id`,
    [candidateIds, normalizedBatchId],
  );
  if (updatedCandidates.rowCount !== candidateIds.length) {
    throw new ManualMigrationDispatchError("migration candidate selection changed while preparing the batch", {
      statusCode: 409,
      code: "batch_selection_changed",
      details: { expected: candidateIds.length, updated: updatedCandidates.rowCount },
    });
  }

  await client.query(
    `UPDATE crawler.channel_candidate_sources
     SET page_id=$2,query_text='results.db batch migration',
         source_json=source_json || jsonb_build_object(
           'manual_migration_batch_id',$3::text,
           'manual_migration_requested_at',now()
         )
     WHERE candidate_id=ANY($1::bigint[])`,
    [candidateIds, pageId, normalizedBatchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidate_sources (
       candidate_id,page_id,query_text,discovery_strategy,source_json
     )
     SELECT selected.candidate_id,$2,'results.db batch migration','manual_migration',
            jsonb_build_object('manual_migration_batch_id',$3::text)
     FROM unnest($1::bigint[]) AS selected(candidate_id)
     WHERE NOT EXISTS (
       SELECT 1
       FROM crawler.channel_candidate_sources source
       WHERE source.candidate_id=selected.candidate_id
     )`,
    [candidateIds, pageId, normalizedBatchId],
  );
  await refreshBatchCounts(client, normalizedBatchId);
  for (const sourceBatchId of sourceBatchIds) {
    if (sourceBatchId === normalizedBatchId) continue;
    await refreshBatchCounts(client, sourceBatchId);
    await refreshSourcePageCounts(client, sourceBatchId);
  }

  const now = new Date().toISOString();
  await client.query(
    `UPDATE crawler.settings
     SET value_json=value_json || jsonb_build_object(
           'status','finishing',
           'pipeline_cycle_id',$2::text,
           'started_at',$3::text,
           'paused_at',NULL,
           'stopped_at',NULL,
           'completed_at',NULL,
           'stop_reason','manual_migration_batch_dispatch',
           'updated_at',$3::text,
           'updated_by','manualMigrationDispatch'
         ),updated_at=now()
     WHERE setting_key=$1`,
    ["query_scheduler", normalizedBatchId, now],
  );

  return {
    batchId: normalizedBatchId,
    selection: normalizedSelection.selection,
    targetCount: candidateIds.length,
    firstCandidateId: candidateIds[0],
    lastCandidateId: candidateIds.at(-1),
  };
}

export async function dispatchManualMigrationBatch({
  selection,
  batchId = generatedManualBatchId(),
  minSubscriberCount = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000),
  transaction = withTransaction,
} = {}) {
  const prepared = await transaction((client) => prepareManualMigrationBatch(client, {
    selection,
    batchId,
    minSubscriberCount,
  }));
  return {
    ok: true,
    created: prepared.targetCount > 0,
    selection: prepared.selection,
    target_count: prepared.targetCount,
    batch_id: prepared.batchId,
    first_candidate_id: prepared.firstCandidateId,
    last_candidate_id: prepared.lastCandidateId,
  };
}

export async function prepareManualMigration(client, {
  channelId,
  candidateId = null,
  batchId = DEFAULT_MANUAL_MIGRATION_BATCH_ID,
  minSubscriberCount = 1000,
} = {}) {
  const normalizedChannelId = nonemptyText(channelId, "channel_id");
  const normalizedCandidateId = positiveCandidateId(candidateId);
  const normalizedBatchId = nonemptyText(batchId, "batch_id");
  const pageId = manualPageId(normalizedBatchId);

  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    [`manual-migration:${normalizedChannelId}`],
  );
  const candidate = await loadCandidateForUpdate(client, normalizedChannelId, normalizedCandidateId);
  if (!candidate) {
    throw new ManualMigrationDispatchError(`migration candidate not found: ${normalizedChannelId}`, {
      statusCode: 404,
      code: "candidate_not_found",
    });
  }

  if (IN_PROGRESS_CANDIDATE_STATUSES.has(candidate.status)) {
    return {
      candidate,
      batchId: candidate.dispatch_batch_id,
      pageId: null,
      previousStatus: candidate.status,
      shouldEnqueue: false,
      alreadyInProgress: true,
    };
  }
  if (TERMINAL_CANDIDATE_STATUSES.has(candidate.status)) {
    throw new ManualMigrationDispatchError(
      `migration candidate is already terminal: ${candidate.status}`,
      { statusCode: 409, code: "candidate_terminal", details: { status: candidate.status } },
    );
  }
  if (!ELIGIBLE_CANDIDATE_STATUSES.has(candidate.status)) {
    throw new ManualMigrationDispatchError(`migration candidate cannot be queued from status: ${candidate.status}`, {
      statusCode: 409,
      code: "candidate_not_eligible",
      details: { status: candidate.status },
    });
  }

  const schedulerRows = await client.query(
    `SELECT value_json
     FROM crawler.settings
     WHERE setting_key='query_scheduler'
     LIMIT 1
     FOR UPDATE`,
  );
  if (schedulerRows.rows.length === 0) {
    throw new ManualMigrationDispatchError("query_scheduler setting is missing", {
      statusCode: 503,
      code: "scheduler_missing",
    });
  }
  const conflict = schedulerConflict(schedulerRows.rows[0].value_json, normalizedBatchId);
  if (conflict) {
    throw new ManualMigrationDispatchError(conflict.message, {
      statusCode: 409,
      code: conflict.code,
      details: schedulerRows.rows[0].value_json,
    });
  }

  const sourceBatchId = candidate.dispatch_batch_id;
  const metadata = {
    source: "results.db",
    purpose: "manual_migration_channel",
    migration_dispatcher: {
      mode: "dashboard_manual",
      source_batch_id: sourceBatchId,
      min_subscriber_count: Number(minSubscriberCount),
    },
  };
  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,
       discovery_closed_at,result_json,updated_at
     ) VALUES ($1,$1,'discovery_closed',0,now(),$2::jsonb,now())
     ON CONFLICT (dispatch_batch_id) DO UPDATE
     SET status='discovery_closed',
         discovery_closed_at=now(),
         validation_closed_at=NULL,
         agent_tail_flushed_at=NULL,
         finished_at=NULL,
         result_json=crawler.query_dispatch_batches.result_json || EXCLUDED.result_json,
         updated_at=now()`,
    [normalizedBatchId, JSON.stringify(metadata)],
  );
  await client.query(
    `INSERT INTO crawler.query_pages (
       page_id,query_text,page_no,status,candidate_count,should_continue,
       stop_reason,result_json,dispatch_batch_id,finished_at,updated_at
     ) VALUES ($1,'results.db manual migration',1,'done',0,false,
               'manual_migration_channel',$2::jsonb,$3,now(),now())
     ON CONFLICT (page_id) DO UPDATE
     SET status='done',should_continue=false,stop_reason='manual_migration_channel',
         result_json=crawler.query_pages.result_json || EXCLUDED.result_json,
         dispatch_batch_id=EXCLUDED.dispatch_batch_id,finished_at=now(),updated_at=now()`,
    [pageId, JSON.stringify(metadata), normalizedBatchId],
  );
  const updatedRows = await client.query(
    `UPDATE crawler.channel_candidates
     SET dispatch_batch_id=$2,pipeline_cycle_id=$2,status='queued',
         snapshot_attempts=CASE WHEN status='failed' THEN 0 ELSE snapshot_attempts END,
         reject_reason=NULL,error_message=NULL,next_retry_at=NULL,
         validation_started_at=NULL,validation_finished_at=NULL,accepted_at=NULL,
         source_json=source_json || jsonb_build_object(
           'manual_migration',jsonb_build_object(
             'requested_at',now(),
             'source_batch_id',$3::text,
             'batch_id',$2::text
           )
         ),
         updated_at=now()
     WHERE candidate_id=$1
     RETURNING candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
               priority,status,snapshot_attempts,source_json`,
    [candidate.candidate_id, normalizedBatchId, sourceBatchId],
  );
  const updatedCandidate = updatedRows.rows[0];

  await client.query(
    `UPDATE crawler.channel_candidate_sources
     SET page_id=$2,query_text='results.db manual migration',
         source_json=source_json || jsonb_build_object(
           'manual_migration_batch_id',$3::text,
           'manual_migration_requested_at',now()
         )
     WHERE candidate_id=$1`,
    [candidate.candidate_id, pageId, normalizedBatchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidate_sources (
       candidate_id,page_id,query_text,discovery_strategy,source_json
     )
     SELECT $1,$2,'results.db manual migration','manual_migration',
            jsonb_build_object('manual_migration_batch_id',$3::text)
     WHERE NOT EXISTS (
       SELECT 1 FROM crawler.channel_candidate_sources WHERE candidate_id=$1
     )`,
    [candidate.candidate_id, pageId, normalizedBatchId],
  );
  await client.query(
    `UPDATE crawler.query_pages
     SET candidate_count=(
           SELECT count(DISTINCT source.candidate_id)::int
           FROM crawler.channel_candidate_sources source
           WHERE source.page_id=$1
         ),updated_at=now()
     WHERE page_id=$1`,
    [pageId],
  );
  await refreshBatchCounts(client, normalizedBatchId);
  if (sourceBatchId !== normalizedBatchId) {
    await refreshBatchCounts(client, sourceBatchId);
    await refreshSourcePageCounts(client, sourceBatchId);
  }

  const now = new Date().toISOString();
  await client.query(
    `UPDATE crawler.settings
     SET value_json=value_json || jsonb_build_object(
           'status','finishing',
           'pipeline_cycle_id',$2::text,
           'started_at',COALESCE(value_json->'started_at',to_jsonb($3::text)),
           'paused_at',NULL,
           'stopped_at',NULL,
           'completed_at',NULL,
           'stop_reason','manual_migration_dispatch',
           'updated_at',$3::text,
           'updated_by','manualMigrationDispatch'
         ),updated_at=now()
     WHERE setting_key=$1`,
    ["query_scheduler", normalizedBatchId, now],
  );

  return {
    candidate: updatedCandidate,
    batchId: normalizedBatchId,
    pageId,
    previousStatus: candidate.status,
    shouldEnqueue: true,
    alreadyInProgress: false,
  };
}

async function addOrReuseJob(queue, { candidate, batchId, minSubscriberCount }) {
  const jobId = safeJobId("channel-snapshot", batchId, candidate.channel_id);
  const existing = await queue.getJob(jobId);
  if (existing) {
    const state = await existing.getState();
    if (REPRESENTED_JOB_STATES.has(state)) {
      return { job: existing, jobId, state, created: false };
    }
    try {
      await existing.remove();
    } catch {
      const current = await queue.getJob(jobId);
      const currentState = current ? await current.getState() : null;
      if (current && REPRESENTED_JOB_STATES.has(currentState)) {
        return { job: current, jobId, state: currentState, created: false };
      }
      throw new ManualMigrationDispatchError(`existing migration job cannot be replaced: ${state}`, {
        statusCode: 409,
        code: "job_not_replaceable",
        details: { job_id: jobId, state },
      });
    }
  }
  const job = await queue.add(
    "channel-snapshot",
    channelSnapshotPayload(candidate, batchId, { minSubscriberCount }),
    { jobId, priority: Number(candidate.priority ?? 100) },
  );
  return { job, jobId, state: "waiting", created: true };
}

export async function dispatchManualMigrationChannel({
  channelId,
  candidateId = null,
  queue,
  batchId = process.env.MIGRATION_MANUAL_BATCH_ID || DEFAULT_MANUAL_MIGRATION_BATCH_ID,
  minSubscriberCount = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000),
  transaction = withTransaction,
  dbQuery = query,
} = {}) {
  if (!queue || typeof queue.add !== "function" || typeof queue.getJob !== "function") {
    throw new ManualMigrationDispatchError("channel crawl queue is required", {
      statusCode: 503,
      code: "queue_missing",
    });
  }
  const prepared = await transaction((client) => prepareManualMigration(client, {
    channelId,
    candidateId,
    batchId,
    minSubscriberCount,
  }));

  if (!prepared.shouldEnqueue) {
    return {
      ok: true,
      created: false,
      already_in_progress: true,
      candidate_id: Number(prepared.candidate.candidate_id),
      channel_id: prepared.candidate.channel_id,
      candidate_status: prepared.candidate.status,
      batch_id: prepared.batchId,
      job: null,
    };
  }

  try {
    const queued = await addOrReuseJob(queue, {
      candidate: prepared.candidate,
      batchId: prepared.batchId,
      minSubscriberCount,
    });
    return {
      ok: true,
      created: queued.created,
      already_in_progress: !queued.created,
      candidate_id: Number(prepared.candidate.candidate_id),
      channel_id: prepared.candidate.channel_id,
      candidate_status: prepared.candidate.status,
      previous_status: prepared.previousStatus,
      batch_id: prepared.batchId,
      job: {
        queue: queue.name,
        id: queued.job.id,
        state: queued.state,
      },
    };
  } catch (error) {
    await dbQuery(
      `UPDATE crawler.channel_candidates
       SET status='failed',error_message=$2,updated_at=now()
       WHERE candidate_id=$1 AND status='queued' AND validation_started_at IS NULL`,
      [prepared.candidate.candidate_id, error?.message || String(error)],
    ).catch(() => {});
    throw error;
  }
}
