export const VIDEO_DISPOSITION_VERSION = "video-disposition-v1";
const SCHEDULED_VIDEO_DISPOSITIONS = Object.freeze(["deferred", "terminal_excluded"]);

function text(value) {
  if (value === null || value === undefined) return null;
  const output = String(value).trim();
  return output || null;
}

function sqlAlias(value) {
  const alias = String(value ?? "").trim();
  if (!alias) return "";
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) {
    throw new TypeError("SQL alias must be an identifier");
  }
  return `${alias}.`;
}

export function videoDispositionEligibleForImmediateRepair(candidate = {}, {
  now = new Date(),
} = {}) {
  const disposition = text(candidate.disposition);
  if (!SCHEDULED_VIDEO_DISPOSITIONS.includes(disposition)) return true;
  const nextAttemptAt = text(candidate.next_attempt_at);
  if (!nextAttemptAt) return false;
  const nowMs = new Date(now).getTime();
  const nextAttemptMs = new Date(nextAttemptAt).getTime();
  return Number.isFinite(nowMs)
    && Number.isFinite(nextAttemptMs)
    && nextAttemptMs <= nowMs;
}

export function videoDispositionImmediateRepairSql(alias = "") {
  const prefix = sqlAlias(alias);
  return `(
    ${prefix}disposition IS NULL
    OR ${prefix}disposition NOT IN ('deferred','terminal_excluded')
    OR ${prefix}next_attempt_at<=now()
  )`;
}

function isoAfter(observedAt, milliseconds) {
  const timestamp = new Date(observedAt).getTime();
  if (!Number.isFinite(timestamp)) throw new TypeError("observedAt must be a valid timestamp");
  return new Date(timestamp + milliseconds).toISOString();
}

function retainedTerminalDisposition(priorDisposition, observedAt) {
  if (text(priorDisposition?.kind) !== "terminal_excluded") return null;
  const reasonCode = text(priorDisposition?.reason_code);
  if (!reasonCode) return null;
  const policyRecheck = reasonCode === "outside_content_window";
  const oneDayRecheck = ["upcoming_live", "live_in_progress"].includes(reasonCode);
  const retryAfter = policyRecheck
    ? 30 * 24 * 60 * 60 * 1000
    : oneDayRecheck ? 24 * 60 * 60 * 1000 : 7 * 24 * 60 * 60 * 1000;
  return {
    version: VIDEO_DISPOSITION_VERSION,
    kind: "terminal_excluded",
    reason_code: reasonCode,
    retry_class: policyRecheck
      ? "low_frequency_policy_recheck"
      : "low_frequency_access_recheck",
    retryable: false,
    observed_at: new Date(observedAt).toISOString(),
    next_attempt_at: isoAfter(observedAt, retryAfter),
  };
}

export function resolveVideoDisposition({
  storageAction,
  classification,
  access,
  detail,
  error = null,
  observedAt,
  terminalReason = null,
  deferredReason = null,
  priorDisposition = null,
} = {}) {
  const accessStatus = text(access?.access_status) ?? "unknown";
  if (["upcoming_live", "live_in_progress"].includes(terminalReason)) {
    const nextAttemptAt = isoAfter(observedAt, 24 * 60 * 60 * 1000);
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "terminal_excluded",
      reason_code: terminalReason,
      retry_class: "low_frequency_access_recheck",
      retryable: false,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: nextAttemptAt,
    };
  }
  if (terminalReason === "outside_content_window") {
    const nextAttemptAt = isoAfter(observedAt, 30 * 24 * 60 * 60 * 1000);
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "terminal_excluded",
      reason_code: terminalReason,
      retry_class: "low_frequency_policy_recheck",
      retryable: false,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: nextAttemptAt,
    };
  }
  const retainedTerminal = retainedTerminalDisposition(priorDisposition, observedAt);

  if (deferredReason === "discovery_scan_incomplete") {
    if (retainedTerminal) return retainedTerminal;
    const nextAttemptAt = isoAfter(observedAt, 60 * 60 * 1000);
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "deferred",
      reason_code: deferredReason,
      retry_class: "uploads_scan_retry",
      retryable: true,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: nextAttemptAt,
    };
  }

  if (storageAction?.kind === "update_access") {
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "stored",
      reason_code: "content_access_updated",
      retry_class: null,
      retryable: false,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: null,
    };
  }

  if (["private", "unavailable"].includes(accessStatus)) {
    const nextAttemptAt = isoAfter(observedAt, 7 * 24 * 60 * 60 * 1000);
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "terminal_excluded",
      reason_code: `access_${accessStatus}`,
      retry_class: "low_frequency_access_recheck",
      retryable: false,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: nextAttemptAt,
    };
  }

  if (error) {
    if (retainedTerminal) return retainedTerminal;
    const nextAttemptAt = isoAfter(observedAt, 60 * 60 * 1000);
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "deferred",
      reason_code: "detail_collection_failed",
      retry_class: "player_retry",
      retryable: true,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: nextAttemptAt,
    };
  }

  if (storageAction?.kind === "upsert") {
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "stored",
      reason_code: "content_stored",
      retry_class: null,
      retryable: false,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: null,
    };
  }

  if (retainedTerminal) return retainedTerminal;

  if (classification?.authoritative !== true) {
    const nextAttemptAt = isoAfter(observedAt, 6 * 60 * 60 * 1000);
    return {
      version: VIDEO_DISPOSITION_VERSION,
      kind: "deferred",
      reason_code: "authoritative_type_unresolved",
      retry_class: detail ? "alternate_player" : "detail_retry",
      retryable: true,
      observed_at: new Date(observedAt).toISOString(),
      next_attempt_at: nextAttemptAt,
    };
  }

  const nextAttemptAt = isoAfter(observedAt, 6 * 60 * 60 * 1000);
  return {
    version: VIDEO_DISPOSITION_VERSION,
    kind: "deferred",
    reason_code: `access_${accessStatus}`,
    retry_class: "access_recheck",
    retryable: true,
    observed_at: new Date(observedAt).toISOString(),
    next_attempt_at: nextAttemptAt,
  };
}

export function videoDispositionSummary(videoId, disposition) {
  return {
    video_id: text(videoId),
    kind: disposition.kind,
    reason_code: disposition.reason_code,
    retry_class: disposition.retry_class,
  };
}
