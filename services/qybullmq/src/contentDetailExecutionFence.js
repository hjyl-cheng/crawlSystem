import { normalizeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";
import { StaleChannelCandidateAttemptError } from "./channelCandidateAttemptMutations.js";
import { claimVideoExecution, ownsVideoExecution as ownsExecution, videoExecutionScope as activeScope } from "./videoExecutionRecovery.js";

function text(value) {
  return String(value ?? "").trim() || null;
}

function requiredText(value, field) {
  const normalized = text(value);
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function positiveInteger(value, field) {
  const normalized = Number(value);
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return normalized;
}

function nonNegativeInteger(value, field) {
  const normalized = Number(value);
  if (!Number.isSafeInteger(normalized) || normalized < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return normalized;
}

function scopeKey(scope) {
  return JSON.stringify({
    version: 2,
    kind: scope.kind,
    run_id: scope.runId,
    channel_id: scope.channelId,
    pipeline_cycle_id: scope.pipelineCycleId,
    migration_system_retry_id: scope.migrationSystemRetryId,
    candidate_id: scope.candidateId,
    dispatch_generation: scope.dispatchGeneration,
    origin_snapshot_job_id: scope.originSnapshotJobId,
    origin_snapshot_job_attempt: scope.originSnapshotJobAttempt,
    content_detail_job_epoch: scope.jobEpoch,
  });
}

function originCandidateAttemptFence(job) {
  const values = [
    job?.data?.origin_candidate_id,
    job?.data?.origin_dispatch_generation,
    job?.data?.origin_snapshot_job_id,
    job?.data?.origin_snapshot_job_attempt,
  ];
  if (values.every((value) => value == null)) return null;
  return normalizeChannelCandidateAttemptFence({
    candidateId: job?.data?.origin_candidate_id,
    dispatchGeneration: job?.data?.origin_dispatch_generation,
    jobId: job?.data?.origin_snapshot_job_id,
    bullmqAttempt: job?.data?.origin_snapshot_job_attempt,
  });
}

export function contentDetailExecutionFence(job, {
  executionMode = "detail_queue",
  candidateAttemptFence = null,
} = {}) {
  const runId = requiredText(job?.data?.run_id, "Content Detail run_id");
  const channelId = requiredText(job?.data?.channel_id, "Content Detail channel_id");
  const jobId = requiredText(job?.id, "Content Detail Job id");
  const jobAttempt = positiveInteger(job?.attemptsStarted, "Content Detail attemptsStarted");
  const jobEpoch = nonNegativeInteger(
    job?.data?.content_detail_job_epoch ?? 0,
    "Content Detail content_detail_job_epoch",
  );
  const normalizedCandidateFence = candidateAttemptFence == null
    ? null
    : normalizeChannelCandidateAttemptFence(candidateAttemptFence);
  const normalizedOriginFence = originCandidateAttemptFence(job);
  if (normalizedCandidateFence && normalizedOriginFence) {
    throw new TypeError("Content Detail cannot be both inline and queued from a Snapshot attempt");
  }
  if (normalizedCandidateFence && (
    normalizedCandidateFence.jobId !== jobId
    || normalizedCandidateFence.bullmqAttempt !== jobAttempt
  )) {
    throw new TypeError("Content Detail inline Job and Candidate attempt Fence must match");
  }

  const migrationSystemRetryId = job?.data?.migration_system_retry_id == null
    ? null
    : positiveInteger(
        job.data.migration_system_retry_id,
        "Content Detail migration_system_retry_id",
      );
  const recovery = migrationSystemRetryId != null;
  if (recovery && normalizedOriginFence) {
    throw new TypeError("Migration recovery Content Detail cannot carry a Snapshot origin Fence");
  }
  const candidateId = recovery
    ? positiveInteger(job?.data?.candidate_id, "Content Detail candidate_id")
    : normalizedCandidateFence?.candidateId ?? normalizedOriginFence?.candidateId ?? null;
  const dispatchGeneration = recovery
    ? positiveInteger(
        job?.data?.dispatch_generation,
        "Content Detail dispatch_generation",
      )
    : normalizedCandidateFence?.dispatchGeneration
      ?? normalizedOriginFence?.dispatchGeneration
      ?? null;
  const pipelineCycleId = text(job?.data?.dispatch_batch_id)
    ?? text(job?.data?.pipeline_cycle_id);
  if (recovery && !pipelineCycleId) {
    throw new TypeError("Migration recovery Content Detail pipeline cycle is required");
  }

  const scope = {
    kind: normalizedCandidateFence
      ? "channel_inline"
      : recovery
        ? "migration_recovery"
        : normalizedOriginFence
          ? "channel_handoff"
          : "ordinary",
    runId,
    channelId,
    pipelineCycleId,
    migrationSystemRetryId,
    candidateId,
    dispatchGeneration,
    originSnapshotJobId: normalizedOriginFence?.jobId ?? null,
    originSnapshotJobAttempt: normalizedOriginFence?.bullmqAttempt ?? null,
    jobEpoch,
  };
  return Object.freeze({
    ...scope,
    scopeKey: scopeKey(scope),
    jobId,
    jobAttempt,
    executionMode: requiredText(executionMode, "Content Detail executionMode"),
    candidateAttemptFence: normalizedCandidateFence,
    originCandidateAttemptFence: normalizedOriginFence,
    recovery,
  });
}

export function assertInlineContentDetailExecutionCurrent(result, candidateAttemptFence) {
  if (result?.reason !== "content_detail_execution_fence_stale") return result;
  const fence = normalizeChannelCandidateAttemptFence(candidateAttemptFence);
  throw new StaleChannelCandidateAttemptError(
    "commit inline Content Detail result",
    fence.candidateId,
  );
}

async function loadRun(client, fence) {
  let candidateId = fence.candidateId;
  if (candidateId == null) {
    const identity = await client.query(
      `/* content-detail-lock:run-candidate-identity */
       SELECT candidate_id
       FROM crawler.channel_runs
       WHERE run_id=$1 AND channel_id=$2`,
      [fence.runId, fence.channelId],
    );
    candidateId = identity.rows[0]?.candidate_id == null
      ? null
      : Number(identity.rows[0].candidate_id);
  }
  const candidate = await loadCandidate(client, candidateId);
  const retry = await loadActiveRetry(client, candidateId);
  const runRows = await client.query(
    `/* content-detail-lock:run */
     SELECT run.run_id,run.channel_id,run.candidate_id,run.status,run.detail_status,
            run.result_json,run.detail_active_job_id,run.detail_active_job_attempt,
            run.detail_active_scope_key,run.detail_job_epoch,run.detail_active_job_epoch
     FROM crawler.channel_runs run
     WHERE run.run_id=$1 AND run.channel_id=$2
     ORDER BY run.run_id
     FOR UPDATE OF run`,
    [fence.runId, fence.channelId],
  );
  const run = runRows.rows[0] ?? null;
  if (!run || (candidateId != null && Number(run.candidate_id) !== candidateId)) return null;
  const channelRows = await client.query(
    `/* content-detail-lock:channel */
     SELECT channel.latest_run_id,channel.status AS channel_status
     FROM crawler.channels channel
     WHERE channel.channel_id=$1
     ORDER BY channel.channel_id
     FOR UPDATE OF channel`,
    [fence.channelId],
  );
  const channel = channelRows.rows[0] ?? null;
  return channel ? { run: { ...run, ...channel }, candidate, retry } : null;
}

async function loadCandidate(client, candidateId) {
  if (candidateId == null) return null;
  const rows = await client.query(
    `/* content-detail-lock:candidate */
     SELECT candidate_id,status,dispatch_batch_id,pipeline_cycle_id,
            snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
     FROM crawler.channel_candidates
     WHERE candidate_id=$1
     FOR UPDATE`,
    [candidateId],
  );
  return rows.rows[0] ?? null;
}

async function loadActiveRetry(client, candidateId) {
  if (candidateId == null) return null;
  const rows = await client.query(
    `/* content-detail-lock:retry */
     SELECT system_retry_id,candidate_id,failed_dispatch_batch_id,
            failed_dispatch_generation,failed_job_id,failed_job_attempt,status,
            retry_dispatch_generation,recovery_run_id
     FROM crawler.migration_system_retry_items
     WHERE candidate_id=$1 AND status IN ('retrying','pending','dispatched')
     ORDER BY system_retry_id DESC
     LIMIT 1
     FOR UPDATE`,
    [candidateId],
  );
  return rows.rows[0] ?? null;
}

function pipelineCycleMatches(run, fence) {
  if (!fence.pipelineCycleId) return true;
  const runCycle = text(run.result_json?.dispatch_batch_id)
    ?? text(run.result_json?.pipeline_cycle_id);
  return runCycle == null || runCycle === fence.pipelineCycleId;
}

function inlineCandidateMatches(candidate, fence) {
  const attempt = fence.candidateAttemptFence;
  return candidate
    && Number(candidate.candidate_id) === attempt.candidateId
    && Number(candidate.snapshot_dispatch_generation) === attempt.dispatchGeneration
    && text(candidate.snapshot_active_job_id) === attempt.jobId
    && Number(candidate.snapshot_active_job_attempt) === attempt.bullmqAttempt
    && ["validating", "accepted"].includes(text(candidate.status));
}

function exactRecoveryMatches({ run, candidate, retry, fence }) {
  const retryOwnsGeneration = retry?.status === "retrying"
    ? retry.retry_dispatch_generation == null
      && Number(retry.failed_dispatch_generation) === fence.dispatchGeneration
    : retry?.status === "dispatched"
      && Number(retry.retry_dispatch_generation) === fence.dispatchGeneration;
  return retry
    && candidate
    && Number(retry.system_retry_id) === fence.migrationSystemRetryId
    && Number(retry.candidate_id) === fence.candidateId
    && retryOwnsGeneration
    && text(retry.failed_dispatch_batch_id) === fence.pipelineCycleId
    && text(retry.recovery_run_id) === fence.runId
    && Number(run.candidate_id) === fence.candidateId
    && text(run.latest_run_id) === fence.runId
    && run.channel_status === "active"
    && (
      text(run.result_json?.dispatch_batch_id)
        ?? text(run.result_json?.pipeline_cycle_id)
    ) === fence.pipelineCycleId
    && candidate.status === "accepted"
    && Number(candidate.snapshot_dispatch_generation) === fence.dispatchGeneration
    && candidate.snapshot_active_job_id == null
    && candidate.snapshot_active_job_attempt == null;
}

function inlineRetryMatches(retry, fence) {
  if (!retry) return true;
  if (Number(retry.candidate_id) !== fence.candidateId) return false;
  if (text(retry.failed_dispatch_batch_id) !== fence.pipelineCycleId) return false;
  if (retry.status === "retrying") {
    const failedAttempt = Number(retry.failed_job_attempt);
    return Number(retry.failed_dispatch_generation) === fence.dispatchGeneration
      && text(retry.failed_job_id) === fence.jobId
      && Number.isSafeInteger(failedAttempt)
      && failedAttempt > 0
      && failedAttempt < fence.jobAttempt
      && retry.retry_dispatch_generation == null
      && retry.recovery_run_id == null;
  }
  return retry.status === "dispatched"
    && Number(retry.retry_dispatch_generation) === fence.dispatchGeneration
    && [null, fence.runId].includes(text(retry.recovery_run_id));
}

function ordinaryRetryMatches({ run, candidate, retry, fence }) {
  if (!candidate || retry?.status !== "retrying") return false;
  const failedAttempt = Number(retry.failed_job_attempt);
  const activeAttempt = Number(candidate.snapshot_active_job_attempt);
  const failedBatchId = text(retry.failed_dispatch_batch_id);
  const failedJobId = text(retry.failed_job_id);
  const runCycleId = text(run.result_json?.dispatch_batch_id)
    ?? text(run.result_json?.pipeline_cycle_id);
  return Number(retry.candidate_id) === Number(candidate.candidate_id)
    && Number(run.candidate_id) === Number(candidate.candidate_id)
    && failedBatchId != null
    && text(candidate.dispatch_batch_id) === failedBatchId
    && fence.pipelineCycleId != null
    && text(candidate.pipeline_cycle_id) === fence.pipelineCycleId
    && runCycleId === fence.pipelineCycleId
    && text(run.result_json?.job_id) === failedJobId
    && Number(candidate.snapshot_dispatch_generation)
      === Number(retry.failed_dispatch_generation)
    && text(candidate.snapshot_active_job_id) === failedJobId
    && Number.isSafeInteger(failedAttempt)
    && failedAttempt > 0
    && Number.isSafeInteger(activeAttempt)
    && activeAttempt >= failedAttempt
    && retry.retry_dispatch_generation == null
    && retry.recovery_run_id == null
    && text(run.latest_run_id) === fence.runId
    && run.channel_status === "active";
}

function queuedOriginMatches({ run, candidate, retry, fence }) {
  const origin = fence.originCandidateAttemptFence;
  if (!candidate || !origin) return false;
  const activeJobId = text(candidate.snapshot_active_job_id);
  const activeJobAttempt = candidate.snapshot_active_job_attempt == null
    ? null
    : Number(candidate.snapshot_active_job_attempt);
  const originOwnsCandidate = activeJobId === origin.jobId
    && activeJobAttempt === origin.bullmqAttempt;
  const parentCompleted = activeJobId == null && activeJobAttempt == null;
  if (!originOwnsCandidate && !parentCompleted) return false;
  if (retry && !ordinaryRetryMatches({ run, candidate, retry, fence })) return false;
  return Number(candidate.candidate_id) === origin.candidateId
    && Number(run.candidate_id) === origin.candidateId
    && Number(candidate.snapshot_dispatch_generation) === origin.dispatchGeneration
    && candidate.status === "accepted"
    && text(run.latest_run_id) === fence.runId
    && run.channel_status === "active"
    && pipelineCycleMatches(run, fence);
}

async function lockScope(locked, fence) {
  const { run, candidate, retry } = locked;
  if (Number(run.detail_job_epoch) !== fence.jobEpoch) return null;
  const runCandidateId = run.candidate_id == null ? null : Number(run.candidate_id);
  if (fence.candidateId != null && runCandidateId !== fence.candidateId) return null;

  if (fence.candidateAttemptFence) {
    if (!inlineCandidateMatches(candidate, fence)) return null;
    if (text(run.latest_run_id) !== fence.runId) return null;
    if (!pipelineCycleMatches(run, fence)) return null;
    if (!inlineRetryMatches(retry, fence)) return null;
    if (retry && (
      text(run.result_json?.dispatch_batch_id)
        ?? text(run.result_json?.pipeline_cycle_id)
    ) !== fence.pipelineCycleId) return null;
  } else if (fence.recovery) {
    if (!exactRecoveryMatches({ run, candidate, retry, fence })) return null;
  } else if (fence.originCandidateAttemptFence) {
    if (!queuedOriginMatches({ run, candidate, retry, fence })) return null;
  } else {
    if (retry && !ordinaryRetryMatches({ run, candidate, retry, fence })) return null;
    if (!pipelineCycleMatches(run, fence)) return null;
    if (fence.executionMode !== "checkpoint_repair" && text(run.latest_run_id) !== fence.runId) {
      return null;
    }
  }

  return Object.freeze({
    runId: fence.runId,
    channelId: fence.channelId,
    candidateId: fence.candidateId ?? runCandidateId,
    migrationSystemRetryId: fence.migrationSystemRetryId,
    dispatchGeneration: fence.dispatchGeneration,
    recovery: fence.recovery || (fence.candidateAttemptFence != null && retry != null),
  });
}

function supersedesQueuedOrigin(run, fence) {
  if (!fence.originCandidateAttemptFence) return false;
  const active = activeScope(run);
  return active?.kind === "channel_handoff"
    && Number(active.candidate_id) === fence.candidateId
    && Number(active.dispatch_generation) === fence.dispatchGeneration
    && text(active.origin_snapshot_job_id) === fence.originSnapshotJobId
    && Number(active.origin_snapshot_job_attempt) < fence.originSnapshotJobAttempt;
}

export async function claimContentDetailExecution(client, fence, { recoverPending = false } = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const locked = await loadRun(client, fence);
  if (!locked) return null;
  const { run } = locked;
  const scope = await lockScope(locked, fence);
  if (!scope) return null;
  const claimed = await claimVideoExecution(client, run, fence, {
    supersedesOwner: supersedesQueuedOrigin(run, fence),
    recovery: recoverPending ? { kind: "full" } : null,
  });
  return claimed ? scope : null;
}

export async function lockContentDetailExecution(client, fence) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const locked = await loadRun(client, fence);
  if (!locked || !ownsExecution(locked.run, fence)) return null;
  return lockScope(locked, fence);
}

export async function prepareContentDetailExecutionRequeue(client, fence, {
  findExistingJob,
  advanceEpoch = false,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  if (fence?.recovery !== true) {
    throw new TypeError("Content Detail requeue requires a Migration recovery Fence");
  }
  if (typeof findExistingJob !== "function") {
    throw new TypeError("findExistingJob is required");
  }
  if (typeof advanceEpoch !== "boolean") {
    throw new TypeError("advanceEpoch must be a boolean");
  }
  const locked = await loadRun(client, fence);
  if (!locked || !(await lockScope(locked, fence))) {
    return Object.freeze({ ready: false, cleared: false, existingJob: null, jobEpoch: null });
  }
  const { run } = locked;
  const existingJob = await findExistingJob();
  if (existingJob) {
    return Object.freeze({
      ready: false,
      cleared: false,
      existingJob,
      jobEpoch: Number(run.detail_job_epoch),
    });
  }
  const ownershipEmpty = run.detail_active_job_id == null
    && run.detail_active_job_attempt == null
    && run.detail_active_scope_key == null
    && run.detail_active_job_epoch == null;
  if (ownershipEmpty && !advanceEpoch) {
    return Object.freeze({
      ready: true,
      cleared: false,
      existingJob: null,
      jobEpoch: fence.jobEpoch,
    });
  }
  const advanced = await client.query(
    `UPDATE crawler.channel_runs
     SET detail_job_epoch=detail_job_epoch+1,
         detail_active_job_id=NULL,detail_active_job_attempt=NULL,
         detail_active_scope_key=NULL,detail_active_job_epoch=NULL,updated_at=now()
     WHERE run_id=$1
       AND detail_job_epoch=$2
       AND detail_active_job_id IS NOT DISTINCT FROM $3
       AND detail_active_job_attempt IS NOT DISTINCT FROM $4
       AND detail_active_scope_key IS NOT DISTINCT FROM $5
       AND detail_active_job_epoch IS NOT DISTINCT FROM $6
     RETURNING run_id,detail_job_epoch`,
    [
      fence.runId,
      fence.jobEpoch,
      run.detail_active_job_id,
      run.detail_active_job_attempt == null ? null : Number(run.detail_active_job_attempt),
      run.detail_active_scope_key,
      run.detail_active_job_epoch == null ? null : Number(run.detail_active_job_epoch),
    ],
  );
  return Object.freeze({
    ready: advanced.rowCount === 1,
    cleared: advanced.rowCount === 1 && !ownershipEmpty,
    existingJob: null,
    jobEpoch: advanced.rowCount === 1
      ? Number(advanced.rows[0].detail_job_epoch)
      : null,
  });
}
