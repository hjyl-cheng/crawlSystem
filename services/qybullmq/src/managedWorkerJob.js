import { queuesByRole } from "./queues.js";
import { isBusinessRunBudgetExhausted } from "./businessRunBudgetRecovery.js";
import { RotaSlotDeferredError } from "./rotaSlotAdapter.js";

export function channelCandidateFailureDisposition({
  error,
  terminalChannel = null,
  permanentFailure = false,
  attemptsMade = 0,
  maxAttempts = 1,
} = {}) {
  if (terminalChannel || isBusinessRunBudgetExhausted(error)) return "preserve";
  return permanentFailure || Number(attemptsMade) >= Math.max(1, Number(maxAttempts) || 1)
    ? "failed"
    : "queued";
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
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
  const candidateId = positiveInteger(job?.data?.candidate_id, "job.data.candidate_id");
  const dispatchGeneration = positiveInteger(
    job?.data?.dispatch_generation,
    "job.data.dispatch_generation",
  );
  const updated = await query(
    `UPDATE crawler.channel_candidates
     SET status=$2,error_message=$3,
         snapshot_json=COALESCE(snapshot_json,'{}'::jsonb) || $4::jsonb,
         next_retry_at=CASE WHEN $2='failed' THEN NULL ELSE now()+interval '30 seconds' END,
         validation_finished_at=CASE
           WHEN $2='failed' THEN COALESCE(validation_finished_at,now())
           ELSE validation_finished_at
         END,
         updated_at=now()
     WHERE candidate_id=$1 AND snapshot_dispatch_generation=$5
       AND status IN ('discovered','queued','validating')
     RETURNING candidate_id,status,snapshot_dispatch_generation`,
    [
      candidateId,
      disposition,
      String(message ?? "unknown channel crawl failure"),
      JSON.stringify(snapshotPatch),
      dispatchGeneration,
    ],
  );
  return updated?.rowCount === 1;
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
