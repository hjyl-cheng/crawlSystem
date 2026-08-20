import { DelayedError } from "bullmq";

const DEFAULT_SLOT_PAUSE_DELAY_MS = 5000;

export async function deferJobForSlotPause(job, token, {
  delayMs = DEFAULT_SLOT_PAUSE_DELAY_MS,
  now = Date.now,
} = {}) {
  if (!job || typeof job.moveToDelayed !== "function") {
    throw new TypeError("an active BullMQ Job is required for Channel pause deferral");
  }
  const normalizedDelay = Math.max(1, Math.floor(Number(delayMs) || DEFAULT_SLOT_PAUSE_DELAY_MS));
  await job.moveToDelayed(Number(now()) + normalizedDelay, token);
  throw new DelayedError();
}

export const deferChannelJobForSlotPause = deferJobForSlotPause;
