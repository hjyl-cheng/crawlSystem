import { hasCompletePublicVideoSurface, videoAccessStatus } from "./detailPolicy.js";
import { videoAccessRecheckAt } from "./videoDisposition.js";
import { decideYoutubeFailure, youtubeFailureText } from "./youtubeFailurePolicy.js";

const TERMINAL_ACCESS_STATUSES = new Set(["members_only", "private", "unavailable"]);

function positiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) return fallback;
  return Math.min(number, maximum);
}

function outcomeReference(task = {}) {
  return {
    task_id: task.task_id ?? null,
    dispatch_generation: task.dispatch_generation ?? null,
  };
}

export function contentEnrichRetryDelayMs(attemptNumber, {
  baseMs = 30_000,
  maxMs = 6 * 60 * 60_000,
} = {}) {
  const attempt = positiveInteger(attemptNumber, 1, 1_000_000);
  const base = positiveInteger(baseMs, 30_000, 24 * 60 * 60_000);
  const maximum = Math.max(base, positiveInteger(maxMs, 6 * 60 * 60_000, 30 * 24 * 60 * 60_000));
  return Math.min(maximum, base * (2 ** Math.min(30, attempt - 1)));
}

function terminalAccessFromError(error) {
  const message = youtubeFailureText(error).toLowerCase();
  if (/private/.test(message)) return "private";
  if (/member|subscriber/.test(message)) return "members_only";
  return "unavailable";
}

function terminalAccessSource(error) {
  const pending = [error];
  const seen = new Set();
  while (pending.length > 0 && seen.size < 20) {
    const value = pending.shift();
    if (!value || (typeof value !== "object" && typeof value !== "function")) continue;
    if (seen.has(value)) continue;
    seen.add(value);
    const source = String(value.youtube_failure_evidence?.source ?? "").trim();
    if (source) return source;
    if (value.cause != null) pending.push(value.cause);
    if (Array.isArray(value.errors)) pending.push(...value.errors);
  }
  return "youtube_failure_policy";
}

export function contentEnrichDetailOutcome(task, detail, observedAt, retryOptions) {
  const accessStatus = videoAccessStatus(detail);
  if (!TERMINAL_ACCESS_STATUSES.has(accessStatus) && !hasCompletePublicVideoSurface(detail)) {
    const error = Object.assign(
      new Error("Content Enrich detail is missing the required public Video surface"),
      {
        youtube_failure_decision: {
          kind: "incomplete_detail",
          retry_mode: "same_identity",
          reason_code: "required_public_surface_missing",
        },
      },
    );
    return contentEnrichFailureOutcome(task, error, observedAt, retryOptions);
  }
  return {
    ...outcomeReference(task),
    kind: TERMINAL_ACCESS_STATUSES.has(accessStatus) ? "terminal" : "done",
    detail,
    access_status: accessStatus,
    observed_at: observedAt.toISOString(),
    next_retry_at: videoAccessRecheckAt(accessStatus, observedAt),
    error_message: null,
  };
}

export function contentEnrichFailureOutcome(task, error, observedAt, retryOptions) {
  const decision = error?.youtube_failure_decision ?? decideYoutubeFailure({ error });
  if (decision.kind === "content_terminal") {
    const accessStatus = terminalAccessFromError(error);
    const accessStatusSource = terminalAccessSource(error);
    return {
      ...outcomeReference(task),
      kind: "terminal",
      detail: {
        access_status: accessStatus,
        access_status_source: accessStatusSource,
      },
      access_status: accessStatus,
      observed_at: observedAt.toISOString(),
      next_retry_at: videoAccessRecheckAt(accessStatus, observedAt),
      error_message: youtubeFailureText(error),
      failure_decision: decision,
    };
  }
  if (decision.retry_mode === "none") {
    return {
      ...outcomeReference(task),
      kind: "dead_letter",
      detail: null,
      access_status: null,
      observed_at: observedAt.toISOString(),
      next_retry_at: null,
      error_message: youtubeFailureText(error),
      failure_decision: {
        ...decision,
        terminal: false,
        dead_letter_reason: "non_retryable_engineering_failure",
      },
    };
  }
  const nextAttempt = Number(task.attempts ?? 0) + 1;
  const maxAttempts = positiveInteger(retryOptions?.maxAttempts, 8, 100);
  if (nextAttempt >= maxAttempts) {
    return {
      ...outcomeReference(task),
      kind: "dead_letter",
      detail: null,
      access_status: null,
      observed_at: observedAt.toISOString(),
      next_retry_at: null,
      error_message: youtubeFailureText(error),
      failure_decision: {
        ...decision,
        retry_exhausted: true,
        max_attempts: maxAttempts,
      },
    };
  }
  const delayMs = contentEnrichRetryDelayMs(nextAttempt, retryOptions);
  return {
    ...outcomeReference(task),
    kind: "retryable",
    detail: null,
    access_status: null,
    observed_at: observedAt.toISOString(),
    next_retry_at: new Date(observedAt.getTime() + delayMs).toISOString(),
    error_message: youtubeFailureText(error),
    failure_decision: decision,
  };
}
