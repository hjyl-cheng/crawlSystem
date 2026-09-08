import {
  hasCompletePublicVideoSurface,
  isLiveInProgress,
  isUpcomingLiveDetail,
  videoAccessStatus,
} from "./detailPolicy.js";
import { resolveYoutubeContentType } from "./youtubeContentType.js";
import { assertYoutubeContentObservation } from "./youtubePlayability.js";

function text(value) {
  return String(value ?? "").trim() || null;
}

export function youtubeJsVideoAccess(detail) {
  const status = videoAccessStatus(detail);
  return Object.freeze({
    access_status: status,
    access_status_source: text(detail?.access_status_source)
      ?? text(detail?.source)
      ?? "youtubejs_playability",
    availability: text(detail?.availability),
    is_members_only: status === "members_only",
  });
}

function requiredSurfaceError(videoId, message, detail, surface = "player") {
  const error = new Error(message);
  error.name = "YoutubeJsRequiredSurfaceError";
  error.required_surface = surface;
  error.partial_detail = detail;
  error.video_id = videoId;
  return error;
}

function hasViewCount(detail) {
  const value = detail.view_count ?? detail.view_count_text;
  if (value == null || value === "") return false;
  const count = Number(value);
  return Number.isSafeInteger(count) && count >= 0;
}

export function validateYoutubeJsVideoDetail(videoIdValue, detailValue, {
  detailMode = "full",
  optionalComments = false,
  allowIncompleteLive = false,
} = {}) {
  const videoId = text(videoIdValue);
  if (!videoId) throw new TypeError("videoId is required");
  if (!["full", "metrics"].includes(detailMode)) {
    throw new TypeError(`Unsupported YouTube.js detail mode: ${detailMode}`);
  }
  const detail = assertYoutubeContentObservation(detailValue, {
    videoId,
    source: "youtubejs_player",
  });
  if (!detail || typeof detail !== "object" || Array.isArray(detail)) {
    throw new TypeError(`YouTube.js detail is missing for ${videoId}`);
  }
  const access = youtubeJsVideoAccess(detail);
  const terminalAccess = ["members_only", "private", "unavailable"]
    .includes(access.access_status);
  const upcoming = isUpcomingLiveDetail(detail);
  if (!terminalAccess && !upcoming
      && detail.comments_disabled !== true && text(detail.youtubejs_comments_error)) {
    throw requiredSurfaceError(videoId,
      `YouTube.js required comments surface failed for ${videoId}: ${detail.youtubejs_comments_error}`,
      detail, "comments");
  }
  const classification = resolveYoutubeContentType({ videoId, detail });
  // API metadata is useful even when YouTube omitted type evidence. The caller
  // persists unresolved classification as a deferred item, never a guessed type.
  const apiTypePending = detail.video_detail_fallback?.source === "youtube_data_api_batch"
    && detail.video_detail_fallback?.youtubejs_exhausted === true;
  if (!terminalAccess && detailMode === "metrics" && !hasViewCount(detail)) {
    throw requiredSurfaceError(videoId,
      `YouTube.js parser gap: required Video metrics surface is incomplete for ${videoId}`,
      detail);
  }
  // Full Crawl settles an ongoing live as an exclusion; Incremental still
  // needs its public metadata before recording the first-seen observation.
  const liveExclusion = allowIncompleteLive && isLiveInProgress(detail);
  if (!terminalAccess && detailMode === "full" && !upcoming && !liveExclusion
      && ((classification?.authoritative !== true && !apiTypePending) || !hasCompletePublicVideoSurface(detail))) {
    throw requiredSurfaceError(videoId,
      `YouTube.js parser gap: required public Video surface is incomplete for ${videoId}`,
      detail);
  }
  return Object.freeze({ detail, access, classification });
}
