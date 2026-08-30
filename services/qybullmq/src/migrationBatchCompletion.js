import { migrationBatchCompletion } from "./migrationDispatchPolicy.js";
import { QUERY_SCHEDULER_KEY } from "./queryScheduler.js";

const COMPLETABLE_SCHEDULER_STATUSES = new Set(["finishing", "repairing"]);
const TERMINAL_CANDIDATE_STATUSES = new Set(["accepted", "rejected", "existing", "failed"]);

function hasSystemFailureEvidence(candidate) {
  return candidate?.has_system_failure === true
    || (
      candidate?.snapshot_json?.failure_type === "retryable_system_failure"
      && String(candidate.snapshot_json.failed_dispatch_batch_id ?? "")
        === String(candidate.dispatch_batch_id ?? "")
    );
}

function hasActiveSystemRetry(candidate) {
  return candidate?.has_active_system_retry === true;
}

function completionFromCandidates(candidates) {
  return migrationBatchCompletion({
    total: candidates.length,
    accepted: candidates.filter(({ status }) => status === "accepted").length,
    rejected: candidates.filter(({ status }) => status === "rejected").length,
    failed: candidates.filter(({ status }) => status === "failed").length,
    systemFailures: candidates.filter(hasSystemFailureEvidence).length,
  });
}

export async function settleCompletedMigrationBatch({
  withTransaction,
  batchId,
  completedAt = new Date().toISOString(),
  schedulerKey = QUERY_SCHEDULER_KEY,
  schedulerMetadata = {},
  maxSnapshotAttempts = 6,
} = {}) {
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  const normalizedBatchId = String(batchId ?? "").trim();
  if (!normalizedBatchId) throw new TypeError("batchId is required");
  const normalizedMaxSnapshotAttempts = Number(maxSnapshotAttempts);
  if (!Number.isSafeInteger(normalizedMaxSnapshotAttempts)
      || normalizedMaxSnapshotAttempts <= 0) {
    throw new TypeError("maxSnapshotAttempts must be a positive integer");
  }
  if (!schedulerMetadata || typeof schedulerMetadata !== "object" || Array.isArray(schedulerMetadata)) {
    throw new TypeError("schedulerMetadata must be an object");
  }

  return withTransaction(async (client) => {
    const schedulerRows = await client.query(
      `SELECT value_json
       FROM crawler.settings
       WHERE setting_key=$1
       FOR UPDATE`,
      [schedulerKey],
    );
    const scheduler = schedulerRows.rows[0]?.value_json ?? null;
    if (String(scheduler?.pipeline_cycle_id ?? "") !== normalizedBatchId
        || !COMPLETABLE_SCHEDULER_STATUSES.has(String(scheduler?.status ?? ""))) {
      return null;
    }

    const candidateRows = await client.query(
      `SELECT candidate.candidate_id,candidate.dispatch_batch_id,
              candidate.status,candidate.snapshot_attempts,candidate.snapshot_json,
              candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
              EXISTS (
                SELECT 1
                FROM crawler.migration_system_retry_items retry
                WHERE retry.candidate_id=candidate.candidate_id
                  AND retry.failed_dispatch_batch_id=candidate.dispatch_batch_id
              ) AS has_system_failure,
              EXISTS (
                SELECT 1
                FROM crawler.migration_system_retry_items retry
                WHERE retry.candidate_id=candidate.candidate_id
                  AND retry.failed_dispatch_batch_id=candidate.dispatch_batch_id
                  AND retry.status IN ('retrying','pending','dispatched')
              ) AS has_active_system_retry
       FROM crawler.channel_candidates candidate
       WHERE candidate.dispatch_batch_id=$1
       ORDER BY candidate.candidate_id
       FOR UPDATE OF candidate`,
      [normalizedBatchId],
    );
    if (candidateRows.rows.some(({ status }) => !TERMINAL_CANDIDATE_STATUSES.has(status))) {
      return null;
    }
    if (candidateRows.rows.some((candidate) => (
      candidate.status === "failed"
      && !hasActiveSystemRetry(candidate)
      && Number(candidate.snapshot_attempts) < normalizedMaxSnapshotAttempts
    ))) return null;
    if (candidateRows.rows.some((candidate) => (
      candidate.status === "accepted"
      && candidate.snapshot_active_job_id != null
      && !hasActiveSystemRetry(candidate)
    ))) return null;
    const completion = completionFromCandidates(candidateRows.rows);
    const statistics = {
      total: completion.total,
      accepted: completion.accepted,
      rejected: completion.rejected,
      failed: completion.failed,
    };
    const completedBatch = await client.query(
      `UPDATE crawler.query_dispatch_batches
       SET status=$2,outcome=$3,total_channel_count=$4,
           discovered_candidate_count=$4,accepted_channel_count=$5,
           rejected_channel_count=$6,failed_channel_count=$7,
           result_json=result_json || jsonb_build_object(
             'outcome',$3::text,'statistics',$8::jsonb
           ),
           finished_at=COALESCE(finished_at,now()),updated_at=now()
       WHERE dispatch_batch_id=$1
         AND status IN ('running','discovery_closed','validation_closed','finishing')
       RETURNING dispatch_batch_id,status,outcome,total_channel_count,
                 accepted_channel_count,rejected_channel_count,failed_channel_count`,
      [
        normalizedBatchId,
        completion.status,
        completion.outcome,
        completion.total,
        completion.accepted,
        completion.rejected,
        completion.failed,
        JSON.stringify(statistics),
      ],
    );
    if (completedBatch.rowCount !== 1) return null;

    const schedulerPatch = {
      ...schedulerMetadata,
      status: "stopped",
      stopped_at: completedAt,
      completed_at: completedAt,
      stop_reason: "pipeline_complete",
      batch_outcome: completion.outcome,
      batch_statistics: statistics,
      updated_at: completedAt,
      updated_by: "controller",
    };
    const stoppedScheduler = await client.query(
      `UPDATE crawler.settings
       SET value_json=value_json || $2::jsonb,updated_at=now()
       WHERE setting_key=$1
         AND value_json->>'pipeline_cycle_id'=$3
         AND value_json->>'status' IN ('finishing','repairing')
       RETURNING value_json`,
      [schedulerKey, JSON.stringify(schedulerPatch), normalizedBatchId],
    );
    if (stoppedScheduler.rowCount !== 1) {
      throw new Error(`Migration Scheduler fence changed during completion: ${normalizedBatchId}`);
    }
    return Object.freeze({
      ...completion,
      batch_id: normalizedBatchId,
      completed_at: completedAt,
      scheduler: stoppedScheduler.rows[0].value_json,
    });
  });
}
