import { normalizeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";
import { StaleChannelCandidateAttemptError } from "./channelCandidateAttemptMutations.js";

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

function scopeKey(scope) {
  return JSON.stringify({
    version: 1,
    kind: scope.kind,
    run_id: scope.runId,
    channel_id: scope.channelId,
    pipeline_cycle_id: scope.pipelineCycleId,
    migration_system_retry_id: scope.migrationSystemRetryId,
    candidate_id: scope.candidateId,
    dispatch_generation: scope.dispatchGeneration,
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
  const normalizedCandidateFence = candidateAttemptFence == null
    ? null
    : normalizeChannelCandidateAttemptFence(candidateAttemptFence);
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
  const candidateId = recovery
    ? positiveInteger(job?.data?.candidate_id, "Content Detail candidate_id")
    : normalizedCandidateFence?.candidateId ?? null;
  const dispatchGeneration = recovery
    ? positiveInteger(
        job?.data?.dispatch_generation,
        "Content Detail dispatch_generation",
      )
    : normalizedCandidateFence?.dispatchGeneration ?? null;
  const pipelineCycleId = text(job?.data?.dispatch_batch_id)
    ?? text(job?.data?.pipeline_cycle_id);
  if (recovery && !pipelineCycleId) {
    throw new TypeError("Migration recovery Content Detail pipeline cycle is required");
  }

  const scope = {
    kind: normalizedCandidateFence ? "channel_inline" : recovery ? "migration_recovery" : "ordinary",
    runId,
    channelId,
    pipelineCycleId,
    migrationSystemRetryId,
    candidateId,
    dispatchGeneration,
  };
  return Object.freeze({
    ...scope,
    scopeKey: scopeKey(scope),
    jobId,
    jobAttempt,
    executionMode: requiredText(executionMode, "Content Detail executionMode"),
    candidateAttemptFence: normalizedCandidateFence,
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
  const rows = await client.query(
    `SELECT run.run_id,run.channel_id,run.candidate_id,run.status,run.detail_status,
            run.result_json,run.detail_active_job_id,run.detail_active_job_attempt,
            run.detail_active_scope_key,channel.latest_run_id,
            channel.status AS channel_status
     FROM crawler.channel_runs run
     JOIN crawler.channels channel ON channel.channel_id=run.channel_id
     WHERE run.run_id=$1 AND run.channel_id=$2
     FOR UPDATE OF run,channel`,
    [fence.runId, fence.channelId],
  );
  return rows.rows[0] ?? null;
}

async function loadCandidate(client, candidateId) {
  if (candidateId == null) return null;
  const rows = await client.query(
    `SELECT candidate_id,status,dispatch_batch_id,pipeline_cycle_id,
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
    `SELECT system_retry_id,candidate_id,failed_dispatch_batch_id,
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
  return retry
    && candidate
    && Number(retry.system_retry_id) === fence.migrationSystemRetryId
    && Number(retry.candidate_id) === fence.candidateId
    && retry.status === "dispatched"
    && Number(retry.retry_dispatch_generation) === fence.dispatchGeneration
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

async function lockScope(client, run, fence) {
  const runCandidateId = run.candidate_id == null ? null : Number(run.candidate_id);
  if (fence.candidateId != null && runCandidateId !== fence.candidateId) return null;
  const candidate = await loadCandidate(client, fence.candidateId ?? runCandidateId);
  const retry = await loadActiveRetry(client, fence.candidateId ?? runCandidateId);

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

function ownsExecution(run, fence) {
  return text(run.detail_active_job_id) === fence.jobId
    && Number(run.detail_active_job_attempt) === fence.jobAttempt
    && text(run.detail_active_scope_key) === fence.scopeKey;
}

function canClaim(run, fence) {
  if (run.detail_active_job_id == null
      && run.detail_active_job_attempt == null
      && run.detail_active_scope_key == null) return true;
  if (ownsExecution(run, fence)) return true;
  if (text(run.detail_active_job_id) === fence.jobId
      && Number(run.detail_active_job_attempt) < fence.jobAttempt) return true;
  return fence.recovery && text(run.detail_active_scope_key) !== fence.scopeKey;
}

export async function claimContentDetailExecution(client, fence) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const run = await loadRun(client, fence);
  if (!run) return null;
  const scope = await lockScope(client, run, fence);
  if (!scope || !canClaim(run, fence)) return null;
  const claimed = await client.query(
    `UPDATE crawler.channel_runs
     SET detail_active_job_id=$2,detail_active_job_attempt=$3,
         detail_active_scope_key=$4,updated_at=now()
     WHERE run_id=$1
     RETURNING run_id`,
    [fence.runId, fence.jobId, fence.jobAttempt, fence.scopeKey],
  );
  return claimed.rowCount === 1 ? scope : null;
}

export async function lockContentDetailExecution(client, fence) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const run = await loadRun(client, fence);
  if (!run || !ownsExecution(run, fence)) return null;
  return lockScope(client, run, fence);
}

export async function prepareContentDetailExecutionRequeue(client, fence, {
  findExistingJob,
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
  const run = await loadRun(client, fence);
  if (!run || !(await lockScope(client, run, fence))) {
    return Object.freeze({ ready: false, cleared: false, existingJob: null });
  }
  const existingJob = await findExistingJob();
  if (existingJob) {
    return Object.freeze({ ready: false, cleared: false, existingJob });
  }
  const ownershipEmpty = run.detail_active_job_id == null
    && run.detail_active_job_attempt == null
    && run.detail_active_scope_key == null;
  if (ownershipEmpty) {
    return Object.freeze({ ready: true, cleared: false, existingJob: null });
  }
  if (text(run.detail_active_scope_key) !== fence.scopeKey) {
    return Object.freeze({ ready: true, cleared: false, existingJob: null });
  }
  if (text(run.detail_active_job_id) !== fence.jobId) {
    return Object.freeze({ ready: false, cleared: false, existingJob: null });
  }
  const cleared = await client.query(
    `UPDATE crawler.channel_runs
     SET detail_active_job_id=NULL,detail_active_job_attempt=NULL,
         detail_active_scope_key=NULL,updated_at=now()
     WHERE run_id=$1
       AND detail_active_job_id=$2
       AND detail_active_job_attempt=$3
       AND detail_active_scope_key=$4
     RETURNING run_id`,
    [
      fence.runId,
      run.detail_active_job_id,
      Number(run.detail_active_job_attempt),
      run.detail_active_scope_key,
    ],
  );
  return Object.freeze({
    ready: cleared.rowCount === 1,
    cleared: cleared.rowCount === 1,
    existingJob: null,
  });
}
