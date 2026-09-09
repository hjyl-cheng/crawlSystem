import { queuesByRole } from "./queues.js";
import { isBusinessRunBudgetExhausted } from "./businessRunBudgetRecovery.js";
import { RotaSlotDeferredError } from "./rotaSlotAdapter.js";
import {
  activeChannelCandidateAttemptFence,
  failedChannelCandidateAttemptFence,
} from "./channelCandidateAttemptFence.js";

export {
  activeChannelCandidateAttemptFence,
  failedChannelCandidateAttemptFence,
} from "./channelCandidateAttemptFence.js";

const SYSTEM_FAILURE_CODES = new Map([
  ["LEASE_CONFLICT", "lease"],
  ["LEASE_GONE", "lease"],
  ["ROUTE_NOT_READY", "route"],
  ["EXECUTION_ROUTE_BUDGET_EXHAUSTED", "route"],
  ["POLICY_UNAVAILABLE", "route"],
  ["MANAGED_POLICY_UNAVAILABLE", "route"],
  ["CANDIDATE_ATTEMPT_FENCE_STALE", "fence"],
  ["CONTENT_DETAIL_EXECUTION_FENCE_STALE", "fence"],
  ["MIGRATION_RETRY_INTENT_FENCE_STALE", "fence"],
  ["BUSINESS_RUN_BUDGET_RECOVERY_FAILED", "fence"],
  ["TASK_FENCE_CONFLICT", "fence"],
  ["TASK_COMPLETION_CONFLICT", "fence"],
  ["JOB_EXECUTION_ID_CONFLICT", "identity"],
  ["IDEMPOTENCY_KEY_REUSED", "identity"],
  ["BUSINESS_RUN_KEY_CONFLICT", "identity"],
  ["MANAGED_JOB_INTENT_CONFLICT", "identity"],
  ["MIGRATION_RETRY_INTENT_CONFLICT", "identity"],
  ["PROXY_IDENTITY_CHANGED", "identity"],
  ["CHANNEL_SNAPSHOT_DISPATCH_CONFLICT", "outbox"],
  ["FINGERPRINT_INVALID_TARGET_STATUS", "fingerprint_gateway"],
  ["ABORT_ERR", "cancellation"],
]);

function systemFailureNodes(error) {
  const pending = [error];
  const seen = new Set();
  const output = [];
  while (pending.length > 0) {
    const current = pending.shift();
    if (!current || seen.has(current)) continue;
    seen.add(current);
    output.push(current);
    if (current.cause) pending.push(current.cause);
    if (Array.isArray(current.errors)) pending.push(...current.errors);
  }
  return output;
}

function isMissingTargetHttpStatus(value) {
  return value === 0 || value === "0";
}

function fingerprintMissingHttpStatus(node) {
  if (isMissingTargetHttpStatus(node?.targetStatusRaw) || isMissingTargetHttpStatus(node?.target_status_raw)) {
    return true;
  }
  const body = String(node?.youtube_failure_evidence?.body ?? node?.body ?? "");
  return /"target_status_raw"\s*:\s*0\b/.test(body);
}

function failureCategory(node) {
  const code = String(node?.code ?? "").trim().toUpperCase();
  if (code === "FINGERPRINT_INVALID_TARGET_STATUS" && fingerprintMissingHttpStatus(node)) {
    return null;
  }
  if (SYSTEM_FAILURE_CODES.has(code)) return SYSTEM_FAILURE_CODES.get(code);
  const name = String(node?.name ?? "").trim();
  if (name === "ProxyControlRequestError") return "proxy_control";
  if (name === "RotaSlotContractError") return "route";
  if (name === "StaleChannelCandidateAttemptError") return "fence";
  if (name === "ChannelSnapshotDispatchConflictError") return "outbox";
  if (name === "AbortError") return "cancellation";
  if (name === "TimeoutError") return null;
  const message = String(node?.message ?? "");
  if (/\b(?:job|run|intent|candidate|dispatch)\b.{0,80}\bidentity conflicts?\b/i.test(message)) {
    return "identity";
  }
  if (/\boutbox\b.{0,80}\b(?:conflict|fence|dispatch)\b/i.test(message)) return "outbox";
  return null;
}

export function classifyRetryableSystemFailure(error) {
  for (const node of systemFailureNodes(error)) {
    const category = failureCategory(node);
    if (!category) continue;
    const code = String(node?.code ?? "").trim().toUpperCase()
      || `SYSTEM_${category.toUpperCase()}`;
    return Object.freeze({
      failure_type: "retryable_system_failure",
      category,
      code,
      name: String(node?.name ?? "Error"),
      message: String(node?.message ?? node ?? "system failure").slice(0, 2000),
      status: Number.isFinite(Number(node?.status)) ? Number(node.status) : null,
      retryable: true,
    });
  }
  return null;
}

export function retryableSystemFailureDecision(error) {
  const evidence = classifyRetryableSystemFailure(error);
  if (!evidence) return null;
  return Object.freeze({
    kind: "retryable_system_failure",
    retry_mode: isStaleExecutionFailure(error) ? "none" : "system_retry",
    proxy_action: "none",
    client_action: "none",
    terminal: false,
    status: evidence.status,
    evidence,
  });
}

export function isStaleExecutionFailure(error) {
  return systemFailureNodes(error).some(node => (
    ["CANDIDATE_ATTEMPT_FENCE_STALE", "CONTENT_DETAIL_EXECUTION_FENCE_STALE",
      "MIGRATION_RETRY_INTENT_FENCE_STALE"].includes(node?.code)
    || node?.name === "StaleChannelCandidateAttemptError"
  ));
}

export function channelCandidateFailureDisposition({
  error,
  terminalChannel = null,
  permanentFailure = false,
  attemptsMade = 0,
  maxAttempts = 1,
} = {}) {
  if (classifyRetryableSystemFailure(error)) return "retryable_system_failure";
  if (terminalChannel || isBusinessRunBudgetExhausted(error)) return "preserve";
  return permanentFailure || Number(attemptsMade) >= Math.max(1, Number(maxAttempts) || 1)
    ? "failed"
    : "queued";
}

export async function recordChannelCandidateSystemFailure(query, job, {
  message,
  error,
  systemFailureTerminal = false,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const failure = classifyRetryableSystemFailure(error);
  if (!failure) throw new TypeError("retryable system failure evidence is required");
  const fence = failedChannelCandidateAttemptFence(job);
  const failedDispatchBatchId = String(job?.data?.dispatch_batch_id ?? "").trim();
  if (!failedDispatchBatchId) {
    throw new TypeError("system failure Job dispatch_batch_id is required");
  }
  const evidence = {
    failure_type: "retryable_system_failure",
    failed_dispatch_batch_id: failedDispatchBatchId,
    system_failure: failure,
  };
  const updated = await query(
    `WITH matching_intent AS (
       SELECT migration_intent_id
       FROM crawler.migration_channel_intents
       WHERE target_candidate_id=$1
     ), failed_candidate AS (
       UPDATE crawler.channel_candidates candidate
       SET status=CASE
             WHEN status='accepted' THEN 'accepted'
             WHEN $7::boolean THEN 'failed'
             ELSE 'queued'
           END,
           error_message=$2,
           snapshot_json=COALESCE(snapshot_json,'{}'::jsonb) || $3::jsonb,
           snapshot_attempts=CASE
             WHEN status='validating' THEN GREATEST(snapshot_attempts-1,0)
             ELSE snapshot_attempts
           END,
           next_retry_at=NULL,
           validation_finished_at=CASE
             WHEN status='accepted' THEN validation_finished_at
             WHEN $7::boolean THEN COALESCE(validation_finished_at,now())
             ELSE validation_finished_at
           END,
           snapshot_active_job_id=CASE
             WHEN status='accepted' AND $7::boolean THEN NULL
             ELSE snapshot_active_job_id
           END,
           snapshot_active_job_attempt=CASE
             WHEN status='accepted' AND $7::boolean THEN NULL
             ELSE snapshot_active_job_attempt
           END,
           updated_at=now()
       WHERE candidate.candidate_id=$1 AND candidate.snapshot_dispatch_generation=$4
         AND candidate.snapshot_active_job_id=$5
         AND candidate.snapshot_active_job_attempt=$6
         AND candidate.status IN ('discovered','queued','validating','accepted')
       RETURNING candidate_id,status,snapshot_dispatch_generation,
                 snapshot_active_job_id,snapshot_active_job_attempt
     ), recorded_intent AS (
       UPDATE crawler.migration_channel_intents intent
       SET last_error=$2,updated_at=now()
       FROM failed_candidate candidate,matching_intent
       WHERE intent.migration_intent_id=matching_intent.migration_intent_id
       RETURNING intent.migration_intent_id,candidate.candidate_id,
                 candidate.snapshot_dispatch_generation,
                 $5::text AS snapshot_active_job_id,$6::integer AS snapshot_active_job_attempt
     ), closed_previous AS (
       UPDATE crawler.migration_system_retry_items retry
       SET status='resolved',resolution='superseded_by_newer_system_failure',
           resolved_at=COALESCE(resolved_at,now()),updated_at=now()
       FROM recorded_intent intent
       WHERE retry.candidate_id=intent.candidate_id
         AND retry.status IN ('retrying','pending','dispatched')
         AND NOT (
           retry.migration_intent_id=intent.migration_intent_id
           AND retry.failed_dispatch_generation=intent.snapshot_dispatch_generation
           AND retry.failed_job_id=intent.snapshot_active_job_id
           AND retry.failed_job_attempt=intent.snapshot_active_job_attempt
         )
       RETURNING retry.system_retry_id
     ), retry_item AS (
       INSERT INTO crawler.migration_system_retry_items AS existing_retry (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation,
         failed_job_id,failed_job_attempt,failure_code,failure_category,
         failure_evidence,status,updated_at
       )
       SELECT intent.migration_intent_id,intent.candidate_id,
              $8,intent.snapshot_dispatch_generation,intent.snapshot_active_job_id,
              intent.snapshot_active_job_attempt,
              $3::jsonb#>>'{system_failure,code}',
              $3::jsonb#>>'{system_failure,category}',$3::jsonb,
              CASE WHEN $7::boolean THEN 'pending' ELSE 'retrying' END,now()
       FROM recorded_intent intent
       CROSS JOIN (SELECT count(*) FROM closed_previous) closed
       ON CONFLICT (
         migration_intent_id,failed_dispatch_generation,failed_job_id,failed_job_attempt
       ) DO UPDATE
       SET failed_dispatch_batch_id=COALESCE(
             existing_retry.failed_dispatch_batch_id,
             EXCLUDED.failed_dispatch_batch_id
           ),
           failure_code=EXCLUDED.failure_code,
           failure_category=EXCLUDED.failure_category,
           failure_evidence=EXCLUDED.failure_evidence,
           status=EXCLUDED.status,resolution=NULL,resolved_at=NULL,updated_at=now()
       WHERE existing_retry.failed_dispatch_batch_id IS NULL
          OR existing_retry.failed_dispatch_batch_id=EXCLUDED.failed_dispatch_batch_id
       RETURNING system_retry_id,status
     )
     SELECT candidate.candidate_id,candidate.status,
            candidate.snapshot_dispatch_generation,
            candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            retry.system_retry_id,
            retry.status AS system_retry_status
     FROM failed_candidate candidate
     LEFT JOIN retry_item retry ON true`,
    [
      fence.candidateId,
      String(message ?? failure.message),
      JSON.stringify(evidence),
      fence.dispatchGeneration,
      fence.jobId,
      fence.bullmqAttempt,
      systemFailureTerminal === true,
      failedDispatchBatchId,
    ],
  );
  const row = updated?.rows?.[0];
  return Object.freeze({
    recorded: updated?.rowCount === 1,
    systemRetryRecorded: row?.system_retry_id != null,
    fenceCleared: updated?.rowCount === 1
      && row?.snapshot_active_job_id == null
      && row?.snapshot_active_job_attempt == null,
  });
}

export async function markChannelCandidateJobAttemptActive(query, job) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const fence = activeChannelCandidateAttemptFence(job);
  const updated = await query(
    `WITH claimed_candidate AS (
     UPDATE crawler.channel_candidates
     SET snapshot_active_job_id=$2,snapshot_active_job_attempt=$3,updated_at=now()
     WHERE candidate_id=$1 AND snapshot_dispatch_generation=$4
       AND status IN ('discovered','queued','validating','accepted')
       AND (
         snapshot_active_job_id IS NULL
         OR (
           snapshot_active_job_id=$2
           AND snapshot_active_job_attempt<=$3
         )
       )
     RETURNING candidate_id,snapshot_dispatch_generation,
               snapshot_active_job_id,snapshot_active_job_attempt,dispatch_batch_id
     ), resumed_retry AS (
       UPDATE crawler.migration_system_retry_items retry
       SET status='retrying',updated_at=now()
       FROM claimed_candidate candidate
       WHERE retry.candidate_id=candidate.candidate_id
         AND retry.status='pending'
         AND retry.failed_dispatch_batch_id=candidate.dispatch_batch_id
         AND retry.failed_dispatch_batch_id=$5
         AND retry.failed_dispatch_generation=$4
         AND retry.failed_job_id=$2
         AND retry.failed_job_attempt>0 AND retry.failed_job_attempt<$3
         AND retry.retry_dispatch_generation IS NULL
         AND retry.recovery_run_id IS NULL
         AND EXISTS (
           SELECT 1 FROM crawler.channel_runs run
           JOIN crawler.channels channel ON channel.channel_id=run.channel_id
           WHERE run.run_id=$6 AND run.candidate_id=candidate.candidate_id
             AND channel.latest_run_id=run.run_id
             AND run.result_json->>'job_id'=$2
             AND COALESCE(run.result_json->>'dispatch_batch_id',run.result_json->>'pipeline_cycle_id')=$5
         )
       RETURNING retry.system_retry_id
     )
     SELECT candidate_id,snapshot_dispatch_generation,
            snapshot_active_job_id,snapshot_active_job_attempt
     FROM claimed_candidate
     CROSS JOIN (SELECT count(*) FROM resumed_retry) resumed`,
    [fence.candidateId, fence.jobId, fence.bullmqAttempt, fence.dispatchGeneration,
      job.data?.dispatch_batch_id ?? job.data?.pipeline_cycle_id ?? null,
      job.data?.run_id ?? null],
  );
  return updated?.rowCount === 1;
}

export async function clearChannelCandidateJobAttempt(query, job) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const fence = failedChannelCandidateAttemptFence(job);
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,updated_at=now()
     WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
       AND snapshot_active_job_id=$3 AND snapshot_active_job_attempt=$4
     RETURNING candidate_id`,
    [fence.candidateId, fence.dispatchGeneration, fence.jobId, fence.bullmqAttempt],
  );
  return updated?.rowCount === 1;
}

export async function completeChannelCandidateJobAttempt(query, job, {
  resolution = "job_completed",
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const fence = failedChannelCandidateAttemptFence(job);
  const completed = await query(
    `WITH cleared_candidate AS (
       UPDATE crawler.channel_candidates
       SET snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,updated_at=now()
       WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
         AND snapshot_active_job_id=$3 AND snapshot_active_job_attempt=$4
       RETURNING candidate_id
     ), resolved_retry AS (
       UPDATE crawler.migration_system_retry_items retry
       SET status='resolved',resolution=$5,resolved_at=COALESCE(resolved_at,now()),
           updated_at=now()
       FROM cleared_candidate candidate
       WHERE retry.candidate_id=candidate.candidate_id
         AND retry.status='retrying'
         AND retry.failed_dispatch_generation=$2
         AND retry.failed_job_id=$3
       RETURNING retry.system_retry_id
     )
     SELECT candidate.candidate_id,
            (SELECT count(*)::int FROM resolved_retry) AS resolved_count
     FROM cleared_candidate candidate`,
    [
      fence.candidateId,
      fence.dispatchGeneration,
      fence.jobId,
      fence.bullmqAttempt,
      String(resolution),
    ],
  );
  const row = completed?.rows?.[0];
  return Object.freeze({
    cleared: completed?.rowCount === 1,
    resolved: Number(row?.resolved_count ?? 0),
  });
}

export async function resolveMigrationSystemRetryItems(query, job, {
  resolution = "job_completed",
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const fence = failedChannelCandidateAttemptFence(job);
  const updated = await query(
    `UPDATE crawler.migration_system_retry_items retry
     SET status='resolved',resolution=$4,resolved_at=COALESCE(resolved_at,now()),
         updated_at=now()
     WHERE retry.candidate_id=$1
       AND retry.status IN ('retrying','pending','dispatched')
       AND (
         (retry.failed_dispatch_generation=$2 AND retry.failed_job_id=$3)
         OR retry.retry_dispatch_generation=$2
       )
     RETURNING system_retry_id`,
    [fence.candidateId, fence.dispatchGeneration, fence.jobId, String(resolution)],
  );
  return Number(updated?.rowCount ?? 0);
}

export async function recordChannelCandidateJobFailure(query, job, {
  disposition,
  message,
  snapshotPatch = {},
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  if (!["queued", "failed"].includes(disposition)) {
    throw new TypeError("Candidate failure disposition must be queued or failed");
  }
  if (!snapshotPatch || typeof snapshotPatch !== "object" || Array.isArray(snapshotPatch)) {
    throw new TypeError("Candidate failure snapshotPatch must be an object");
  }
  const fence = failedChannelCandidateAttemptFence(job);
  const updated = await query(
     `UPDATE crawler.channel_candidates candidate
     SET status=$2,error_message=$3,
         snapshot_json=(COALESCE(snapshot_json,'{}'::jsonb)
           - 'failure_type' - 'system_failure') || $4::jsonb,
         next_retry_at=CASE WHEN $2='failed' THEN NULL ELSE now()+interval '30 seconds' END,
         validation_finished_at=CASE
           WHEN $2='failed' THEN COALESCE(validation_finished_at,now())
           ELSE validation_finished_at
         END,
         snapshot_active_job_id=CASE
           WHEN $2='failed' THEN snapshot_active_job_id ELSE NULL
         END,
         snapshot_active_job_attempt=CASE
           WHEN $2='failed' THEN snapshot_active_job_attempt ELSE NULL
         END,
         updated_at=now()
     WHERE candidate.candidate_id=$1 AND candidate.snapshot_dispatch_generation=$5
       AND candidate.snapshot_active_job_id=$6
       AND candidate.snapshot_active_job_attempt=$7
       AND candidate.status IN ('discovered','queued','validating')
     RETURNING candidate_id,status,snapshot_dispatch_generation`,
    [
      fence.candidateId,
      disposition,
      String(message ?? "unknown channel crawl failure"),
      JSON.stringify(snapshotPatch),
      fence.dispatchGeneration,
      fence.jobId,
      fence.bullmqAttempt,
    ],
  );
  return updated?.rowCount === 1;
}

export async function settleChannelCandidateJobFailure(query, job, options = {}) {
  const disposition = String(options?.disposition ?? "");
  if (!["preserve", "queued", "failed", "retryable_system_failure"].includes(disposition)) {
    throw new TypeError(
      "Candidate failure disposition must be preserve, queued, failed or retryable_system_failure",
    );
  }
  if (disposition === "retryable_system_failure") {
    return recordChannelCandidateSystemFailure(query, job, options);
  }
  if (disposition !== "preserve") {
    const recorded = await recordChannelCandidateJobFailure(query, job, options);
    if (recorded) return { recorded: true, fenceCleared: disposition !== "failed" };
  }
  return {
    recorded: false,
    fenceCleared: await clearChannelCandidateJobAttempt(query, job),
  };
}

export async function processManagedWorkerJob({
  job,
  token,
  execute,
  terminateBusinessRun,
  deferForSlotPause,
  defaultDelayMs = 5000,
  onDeferred = null,
} = {}) {
  if (typeof execute !== "function") throw new TypeError("execute is required");
  if (typeof terminateBusinessRun !== "function") {
    throw new TypeError("terminateBusinessRun is required");
  }
  if (typeof deferForSlotPause !== "function") {
    throw new TypeError("deferForSlotPause is required");
  }
  try {
    return await execute();
  } catch (error) {
    if (job?.queueName === queuesByRole.channelCrawl && isBusinessRunBudgetExhausted(error)) {
      return terminateBusinessRun(job, error);
    }
    if (!(error instanceof RotaSlotDeferredError)) throw error;
    const delayMs = Math.max(1000, Number(error.retryAfterMs) || Number(defaultDelayMs) || 5000);
    onDeferred?.({
      queue: job?.queueName,
      job_id: job?.id,
      reason: error.reason,
      delay_ms: delayMs,
    });
    return deferForSlotPause(job, token, { delayMs });
  }
}
