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
