import { normalizePublicationGapDomains } from "./publicationGapRepairExecution.js";

const REPRESENTED_STATES = new Set([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
  "completed",
]);
const CONTROLLER_RETRY_ATTEMPTS_STARTED_FIELD =
  "final_repair_controller_retry_from_attempts_started";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function nonNegativeInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return parsed;
}

function sameOptionalIdentity(actual, expected) {
  if (expected == null || expected === "") return true;
  return String(actual ?? "") === String(expected);
}

function sameStrictIdentity(actual, expected) {
  return String(actual ?? "") === String(expected ?? "");
}

function normalizedIdentitySet(value) {
  try {
    return normalizePublicationGapDomains(value);
  } catch {
    return null;
  }
}

function sameIdentitySet(actual, expected) {
  const actualSet = normalizedIdentitySet(actual);
  const expectedSet = normalizedIdentitySet(expected);
  return actualSet !== null
    && expectedSet !== null
    && JSON.stringify(actualSet) === JSON.stringify(expectedSet);
}

function assertSameRepairIntent(job, { name, data, jobId }) {
  if (String(job?.id ?? "") !== jobId || String(job?.name ?? "") !== name) {
    throw new Error(`Final Repair Job identity conflicts with ${jobId}`);
  }
  for (const field of [
    "channel_id",
    "run_id",
    "candidate_id",
  ]) {
    if (!sameOptionalIdentity(job.data?.[field], data?.[field])) {
      throw new Error(`Final Repair Job ${jobId} has conflicting ${field}`);
    }
  }
  for (const field of [
    "dispatch_generation",
    "repair_parent_run_id",
    "checkpoint_target_run_id",
    "repair_round",
    "publication_gap_scope",
    "publication_gap_root_run_id",
    "require_complete_about_metrics",
  ]) {
    if (!sameStrictIdentity(job.data?.[field], data?.[field])) {
      throw new Error(`Final Repair Job ${jobId} has conflicting ${field}`);
    }
  }
  if (!sameIdentitySet(job.data?.publication_gap_domains, data?.publication_gap_domains)) {
    throw new Error(`Final Repair Job ${jobId} has conflicting publication_gap_domains`);
  }
}

export async function ensureFinalRepairJob(queue, {
  name,
  data,
  options,
  beforeDispatch = null,
} = {}) {
  if (!queue || typeof queue.getJob !== "function" || typeof queue.add !== "function") {
    throw new TypeError("a BullMQ queue is required");
  }
  const normalizedName = requiredText(name, "name");
  const jobId = requiredText(options?.jobId, "options.jobId");
  if (beforeDispatch !== null && typeof beforeDispatch !== "function") {
    throw new TypeError("beforeDispatch must be a function when provided");
  }
  const existing = await queue.getJob(jobId);
  if (!existing) {
    if (beforeDispatch) await beforeDispatch({ action: "enqueue", job: null });
    const job = await queue.add(normalizedName, data, options);
    return { action: "enqueued", job, attempts_made: Number(job?.attemptsMade ?? 0) };
  }

  assertSameRepairIntent(existing, { name: normalizedName, data, jobId });
  const state = await existing.getState();
  if (state === "failed") {
    if (typeof existing.retry !== "function") {
      throw new TypeError(`failed Final Repair Job ${jobId} cannot be retried`);
    }
    const attemptsMade = nonNegativeInteger(existing.attemptsMade ?? 0, "attemptsMade");
    const attemptsStarted = nonNegativeInteger(
      existing.attemptsStarted ?? existing.attemptsMade ?? 0,
      "attemptsStarted",
    );
    const recoveryMarker = existing.data?.[CONTROLLER_RETRY_ATTEMPTS_STARTED_FIELD];
    if (recoveryMarker != null) {
      const recoveryAttemptsStarted = nonNegativeInteger(
        recoveryMarker,
        CONTROLLER_RETRY_ATTEMPTS_STARTED_FIELD,
      );
      if (attemptsStarted < recoveryAttemptsStarted) {
        throw new Error(`Final Repair Job ${jobId} has regressed attempt history`);
      }
      if (attemptsStarted > recoveryAttemptsStarted) {
        return {
          action: "recovery_already_consumed",
          job: existing,
          attempts_made: attemptsMade,
        };
      }
    } else {
      if (typeof existing.updateData !== "function") {
        throw new TypeError(`failed Final Repair Job ${jobId} cannot persist recovery state`);
      }
      const nextData = {
        ...(existing.data ?? {}),
        [CONTROLLER_RETRY_ATTEMPTS_STARTED_FIELD]: attemptsStarted,
      };
      await existing.updateData(nextData);
      existing.data = nextData;
    }
    if (beforeDispatch) await beforeDispatch({ action: "retry_failed", job: existing });
    await existing.retry("failed");
    return { action: "retried_failed", job: existing, attempts_made: attemptsMade };
  }
  if (REPRESENTED_STATES.has(state)) {
    return {
      action: "already_represented",
      job: existing,
      state,
      attempts_made: Number(existing.attemptsMade ?? 0),
    };
  }
  throw new Error(`Final Repair Job ${jobId} has unsupported state: ${state}`);
}
