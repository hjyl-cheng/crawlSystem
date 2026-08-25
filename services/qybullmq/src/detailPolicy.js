import { isParserContractError } from "./localizedParsing.js";
import { decideYoutubeFailure, youtubeFailureText } from "./youtubeFailurePolicy.js";

const TERMINAL_ACCESS_STATUSES = new Set(["members_only", "private", "unlisted", "unavailable"]);
const CLASSIFIED_ONLY_TERMINAL_ACCESS_STATUSES = new Set(["private", "unavailable"]);

export function youtubeErrorText(error) {
  return youtubeFailureText(error);
}

function errorText(error) {
  return youtubeErrorText(error);
}

export function isYoutubeIpBlockedError(error) {
  return ["youtube_rate_limited", "youtube_challenge"]
    .includes(decideYoutubeFailure({ error }).kind);
}

export function isYoutubeNetworkRetryableError(error) {
  return ["proxy_transport", "upstream_transient"]
    .includes(decideYoutubeFailure({ error }).kind);
}

export function contentDetailFailureError(message, failures = []) {
  const cause = (Array.isArray(failures) ? failures : [])
    .map((failure) => failure?.error ?? failure)
    .find((error) => error instanceof Error
      && (isYoutubeIpBlockedError(error) || isYoutubeNetworkRetryableError(error)));
  return cause ? new Error(message, { cause }) : new Error(message);
}

export function isTerminalYoutubeError(error) {
  return decideYoutubeFailure({ error }).kind === "content_terminal";
}

export function isTransientYoutubeError(error) {
  return [
    "proxy_transport",
    "youtube_rate_limited",
    "youtube_challenge",
    "token_or_client",
    "upstream_transient",
  ].includes(decideYoutubeFailure({ error }).kind);
}

export function isUpcomingLiveDetail(detail) {
  const liveStatus = String(detail?.live_status ?? "").trim().toLowerCase();
  return detail?.is_upcoming === true || ["is_upcoming", "upcoming"].includes(liveStatus);
}

export function isLiveInProgress(detail) {
  const liveStatus = String(detail?.live_status ?? "").trim().toLowerCase();
  const currentSignal = detail?.is_live === true || liveStatus === "is_live";
  const replaySignal = detail?.was_live === true || ["was_live", "post_live"].includes(liveStatus);
  return currentSignal && !replaySignal;
}

export function isLiveReplay(detail) {
  const liveStatus = String(detail?.live_status ?? "").trim().toLowerCase();
  const currentSignal = detail?.is_live === true || liveStatus === "is_live";
  const replaySignal = detail?.was_live === true || ["was_live", "post_live"].includes(liveStatus);
  return replaySignal && !currentSignal;
}

export function unfinishedLiveReason(detail) {
  if (isUpcomingLiveDetail(detail)) return "upcoming_live";
  if (isLiveInProgress(detail)) return "live_in_progress";
  return null;
}

export function videoAccessStatus(detail, fallback = "unknown") {
  const explicit = String(detail?.access_status ?? "").trim().toLowerCase();
  if ([
    "public", "unlisted", "members_only", "private",
    "unavailable", "login_required",
  ].includes(explicit)) return explicit;
  const availability = String(detail?.availability ?? "").trim().toLowerCase();
  const privacyStatus = String(detail?.privacy_status ?? "").trim().toLowerCase();
  if (/subscriber|premium|member/.test(availability)) return "members_only";
  if (availability === "private" || privacyStatus === "private") return "private";
  if (availability === "unlisted" || privacyStatus === "unlisted" || detail?.is_unlisted === true) {
    return "unlisted";
  }
  if (privacyStatus === "public") return "public";
  if (/login|needs_auth/.test(availability)) return "login_required";
  if (/unavailable|removed/.test(availability)) return "unavailable";
  if (availability === "public" || availability === "age_restricted"
      || String(detail?.playability_status ?? "").toUpperCase() === "OK") return "public";
  return String(fallback || "unknown");
}

export function missingLikeIsZero(detail, accessStatus = "unknown") {
  if (detail?.like_count != null) return false;
  if (["members_only", "private", "unavailable", "login_required"].includes(String(accessStatus))) return false;
  return String(accessStatus) === "public"
    || String(detail?.privacy_status ?? "").toLowerCase() === "public"
    || String(detail?.playability_status ?? "").toUpperCase() === "OK"
    || detail?.view_count_text != null
    || detail?.comment_count != null
    || detail?.published_at != null;
}

export function positiveDurationSeconds(value) {
  if (value === null || value === undefined || value === "") return null;
  const seconds = Number(value);
  return Number.isFinite(seconds) && seconds > 0 ? Math.trunc(seconds) : null;
}

export function hasResolvedDuration(detail) {
  if (positiveDurationSeconds(detail?.duration_seconds) != null) return true;
  const parts = String(detail?.length_text ?? "").trim().split(":");
  if (parts.length < 2 || parts.length > 3 || parts.some((part) => !/^\d+$/.test(part))) return false;
  return parts.reduce((total, part) => total * 60 + Number(part), 0) > 0;
}

export function hasCompletePublicVideoSurface(detail = {}) {
  return Boolean(String(detail?.title ?? "").trim())
    && Boolean(detail?.published_at)
    && Boolean(detail?.view_count_text)
    && (hasResolvedDuration(detail) || isLiveInProgress(detail));
}

export function youtubeJsDetailFallbackReasons({
  missingFields = [],
  accessStatus = "unknown",
  detail = {},
} = {}) {
  const reasons = [...new Set(
    (Array.isArray(missingFields) ? missingFields : [])
      .map((field) => String(field ?? "").trim())
      .filter((field) => field && field !== "comment_count"),
  )];
  if (["members_only", "private", "unavailable", "login_required"].includes(String(accessStatus))) {
    reasons.push(`access:${accessStatus}`);
  }
  if (
    String(detail?.playability_retry_mode ?? "").toLowerCase() === "alternate_client"
    && !hasCompletePublicVideoSurface(detail)
  ) {
    reasons.push("playability:alternate_client");
  }
  const liveStatus = String(detail?.live_status ?? "").toLowerCase();
  if (
    detail?.is_live
    || detail?.was_live
    || detail?.is_upcoming
    || ["is_live", "was_live", "post_live", "is_upcoming", "upcoming"].includes(liveStatus)
  ) {
    reasons.push("live_state");
  }
  return [...new Set(reasons)];
}

export function detailResolutionAction({
  error = null,
  attemptNumber = 1,
  maxAttempts = 1,
  missingFields = [],
  apiFallbackMode = "disabled",
  accessStatus = "unknown",
  apiAlreadyAttempted = false,
} = {}) {
  if (missingFields.length === 0) return "done";
  if (error && isTransientYoutubeError(error) && attemptNumber < maxAttempts) return "retry";
  if (
    (!error || attemptNumber >= maxAttempts)
    && apiFallbackMode === "emergency"
    && !apiAlreadyAttempted
    && !TERMINAL_ACCESS_STATUSES.has(String(accessStatus))
  ) return "api";
  return "partial";
}

export function classifiedOnlyResolutionAction({
  accessStatus = "unknown",
  missingFields = [],
  attemptNumber = 1,
  maxAttempts = 1,
  apiFallbackMode = "disabled",
  apiAlreadyAttempted = false,
} = {}) {
  const normalizedAccess = String(accessStatus || "unknown").trim().toLowerCase() || "unknown";
  const normalizedMissing = [...new Set([
    "access_status",
    ...(Array.isArray(missingFields) ? missingFields : []),
  ].map((field) => String(field ?? "").trim()).filter(Boolean))];
  if (CLASSIFIED_ONLY_TERMINAL_ACCESS_STATUSES.has(normalizedAccess)) {
    return { action: "terminal", missingFields: normalizedMissing };
  }
  const resolution = detailResolutionAction({
    attemptNumber,
    maxAttempts,
    missingFields: normalizedMissing,
    apiFallbackMode,
    accessStatus: normalizedAccess,
    apiAlreadyAttempted,
  });
  if (resolution === "api") return { action: "api", missingFields: normalizedMissing };
  if (Number(attemptNumber) < Number(maxAttempts) && !apiAlreadyAttempted) {
    return { action: "retry", missingFields: normalizedMissing };
  }
  return { action: "terminal", missingFields: normalizedMissing };
}

export function unresolvedParserContractError(errors, {
  contentType = null,
  missingFields = [],
  accessStatus = "unknown",
} = {}) {
  const parserError = (errors || []).find((error) => isParserContractError(error));
  if (!parserError) return null;
  return !contentType || missingFields.length > 0 || accessStatus === "unknown"
    ? parserError
    : null;
}
