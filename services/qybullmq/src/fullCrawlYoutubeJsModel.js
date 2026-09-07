import { createHash } from "node:crypto";
import {
  isLiveInProgress,
  isUpcomingLiveDetail,
} from "./detailPolicy.js";
import { classifyContentWindow } from "./contentWindow.js";
import { validateYoutubeJsVideoDetail } from "./youtubeJsVideoDetailContract.js";
export { youtubeJsVideoAccess as fullCrawlDetailAccess } from "./youtubeJsVideoDetailContract.js";

function canonicalValue(value) {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.keys(value).sort()
      .filter((key) => value[key] !== undefined)
      .map((key) => [key, canonicalValue(value[key])]),
  );
}

export function fullCrawlCanonicalHash(value) {
  return `sha256:${createHash("sha256")
    .update(JSON.stringify(canonicalValue(value)))
    .digest("hex")}`;
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function nonnegativeInteger(value) {
  if (value == null || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}

function positiveInteger(value, field) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return number;
}

export function normalizeFullCrawlUploadEntry(entry, fallbackPosition = null) {
  const videoId = text(entry?.video_id ?? entry?.id);
  if (!videoId) throw new TypeError("Full Crawl Upload entry video_id is required");
  const position = positiveInteger(
    entry?.position ?? fallbackPosition,
    `Full Crawl Upload ${videoId} position`,
  );
  const sourceUrl = text(entry?.source_url ?? entry?.url)
    ?? `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`;
  return Object.freeze({
    position,
    video_id: videoId,
    source_url: sourceUrl,
    title: text(entry?.title),
    thumbnail_url: text(entry?.thumbnail_url),
    duration_seconds: nonnegativeInteger(entry?.duration_seconds),
    view_count_text: text(entry?.view_count_text),
    published_text: text(entry?.published_text),
    published_at: text(entry?.published_at),
    published_at_status: text(entry?.published_at_status) ?? "unresolved",
    published_at_precision: text(entry?.published_at_precision) ?? "unknown",
    published_at_source: text(entry?.published_at_source),
    content_type: text(entry?.content_type),
    type_source: text(entry?.type_source),
    type_membership: Array.isArray(entry?.type_membership)
      ? [...new Set(entry.type_membership.map(text).filter(Boolean))].sort()
      : [],
    is_live: entry?.is_live === true,
    is_upcoming: entry?.is_upcoming === true,
    live_status: text(entry?.live_status),
    live_scheduled_at: text(entry?.live_scheduled_at),
  });
}

export function normalizeFullCrawlTargets(entries) {
  const targets = (Array.isArray(entries) ? entries : [])
    .map((entry, index) => normalizeFullCrawlUploadEntry(entry, index + 1))
    .sort((left, right) => left.position - right.position);
  const ids = new Set();
  const positions = new Set();
  for (const target of targets) {
    if (ids.has(target.video_id)) {
      throw new TypeError(`Full Crawl Upload contains duplicate video_id: ${target.video_id}`);
    }
    if (positions.has(target.position)) {
      throw new TypeError(`Full Crawl Upload contains duplicate position: ${target.position}`);
    }
    ids.add(target.video_id);
    positions.add(target.position);
  }
  return Object.freeze(targets);
}

export function fullCrawlTargetHash(entries) {
  return fullCrawlCanonicalHash(normalizeFullCrawlTargets(entries));
}

export function fullCrawlUploadsDocument(uploads) {
  const scan = uploads?.scan ?? {};
  return Object.freeze({
    version: 1,
    playlist_id: text(uploads?.playlist_id),
    entries: normalizeFullCrawlTargets(uploads?.entries),
    complete: scan.complete === true,
    stop_reason: text(scan.stop_reason),
    terminal_reason: text(scan.terminal_reason),
    pages: nonnegativeInteger(scan.pages) ?? 0,
    inspected_count: nonnegativeInteger(scan.inspected_count) ?? 0,
    parse_gap_count: nonnegativeInteger(scan.parse_gap_count) ?? 0,
    activity_evidence_complete: uploads?.activity_evidence_complete === true,
  });
}

export function fullCrawlUploadsHash(documentOrUploads) {
  const document = documentOrUploads?.version === 1
    ? documentOrUploads
    : fullCrawlUploadsDocument(documentOrUploads);
  return fullCrawlCanonicalHash(document);
}

export function validateFullCrawlYoutubeJsDetail(videoIdValue, detailValue, { optionalComments = false } = {}) {
  return validateYoutubeJsVideoDetail(videoIdValue, detailValue, {
    optionalComments,
    allowIncompleteLive: true,
  });
}

export function fullCrawlTargetDetail(targetValue) {
  const target = normalizeFullCrawlUploadEntry(targetValue, targetValue?.position);
  return {
    id: target.video_id,
    title: target.title,
    url: target.source_url,
    thumbnail_url: target.thumbnail_url,
    duration_seconds: target.duration_seconds,
    view_count_text: target.view_count_text,
    published_text: target.published_text,
    published_at: target.published_at,
    published_at_status: target.published_at_status,
    published_at_precision: target.published_at_precision,
    published_at_source: target.published_at_source,
    is_live: target.is_live,
    is_upcoming: target.is_upcoming,
    live_status: target.live_status,
    live_scheduled_at: target.live_scheduled_at,
    source: "youtubejs_uploads",
    access_status: "public",
    access_status_source: "youtubejs_uploads",
  };
}

export function classifyFullCrawlTargetBeforeDetail(target, {
  contentMaxAgeDays,
  observedAt,
} = {}) {
  const detail = fullCrawlTargetDetail(target);
  if (isUpcomingLiveDetail(detail)) {
    return Object.freeze({ terminalReason: "upcoming_live", detail });
  }
  if (isLiveInProgress(detail)) {
    return Object.freeze({ terminalReason: "live_in_progress", detail });
  }
  const window = classifyContentWindow(detail, contentMaxAgeDays, observedAt);
  if (Number(contentMaxAgeDays) > 0 && window.relation === "outside") {
    return Object.freeze({ terminalReason: "outside_content_window", detail, window });
  }
  return Object.freeze({ terminalReason: null, detail: null, window });
}
