import { positiveDurationSeconds, videoAccessStatus } from "./detailPolicy.js";
import { parseLocalizedCountDetails } from "./localizedCount.js";
import { publicationEvidenceFromFields } from "./publicationTimeEvidence.js";

function explicitFieldStatus(detail, field) {
  const has = Object.prototype.hasOwnProperty.call(detail, field);
  if (!has || detail[field] === undefined) return "unobserved";
  const value = detail[field];
  if (value === null) return "unobserved";
  if (Array.isArray(value) && value.length === 0) return "empty";
  if (typeof value === "string" && value === "") return "empty";
  return "exact";
}

function countFieldStatus(facts, field) {
  const status = facts[`${field}_status`];
  if (status === "unavailable") return status;
  return facts[field] == null || status === "unresolved" ? "unobserved" : status;
}

export function videoDetailFieldStatus(detailValue, errorValue = null) {
  const detail = detailValue && typeof detailValue === "object" && !Array.isArray(detailValue) ? detailValue : {};
  const facts = projectVideoDetail(detail);
  const commentFailure = errorValue?.required_surface === "comments";
  const playerFailure = errorValue?.required_surface === "player";
  const descriptionStatus = facts.description_status === "unavailable"
    ? "unavailable"
    : facts.description_observed
      ? facts.description_status
      : "unobserved";
  const hashtagsStatus = facts.hashtags_observed
    ? (facts.hashtags.length === 0 ? "empty" : "exact")
    : "unobserved";
  const keywordsStatus = facts.keywords_observed
    ? (facts.keywords.length === 0 ? "empty" : "exact")
    : "unobserved";
  const commentsDisabled = facts.comments_disabled === true;
  const commentStatus = commentFailure
    ? "parser_gap"
    : commentsDisabled
      ? "disabled"
      : countFieldStatus(facts, "comment_count");
  const pageStatus = commentFailure
    ? "parser_gap"
    : commentsDisabled
      ? "disabled"
      : explicitFieldStatus(detail, "comments_first_page");
  const accessStatus = !detail.access_status || detail.access_status === "unknown"
    ? "unobserved"
    : detail.access_status === "public" || detail.access_status === "unlisted"
      ? "exact"
      : "unavailable";
  const output = {
    version: 1,
    id: explicitFieldStatus(detail, "id"),
    title: explicitFieldStatus(detail, "title"),
    thumbnail_url: explicitFieldStatus(detail, "thumbnail_url"),
    published_at: detail.published_at_status === "unresolved"
      ? "unobserved"
      : explicitFieldStatus(detail, "published_at"),
    content_type_signals: explicitFieldStatus(detail, "content_type_signals"),
    duration_seconds: facts.duration_seconds == null ? "unobserved" : "exact",
    view_count: countFieldStatus(facts, "view_count"),
    like_count: countFieldStatus(facts, "like_count"),
    comment_count: commentStatus,
    comments_disabled: commentFailure
      ? "parser_gap"
      : commentsDisabled ? "disabled" : explicitFieldStatus(detail, "comments_disabled"),
    comments_first_page: pageStatus,
    description: descriptionStatus,
    hashtags: hashtagsStatus,
    keywords: keywordsStatus,
    access_status: accessStatus,
    live_scheduled_at: explicitFieldStatus(detail, "live_scheduled_at"),
    live_started_at: explicitFieldStatus(detail, "live_started_at"),
    live_ended_at: explicitFieldStatus(detail, "live_ended_at"),
    extractor_version: explicitFieldStatus(detail, "extractor_version"),
    source: explicitFieldStatus(detail, "source"),
  };
  if (playerFailure) {
    for (const field of [
      "title",
      "published_at",
      "content_type_signals",
      "duration_seconds",
      "view_count",
    ]) {
      if (["unobserved", "empty"].includes(output[field])) output[field] = "parser_gap";
    }
  }
  return output;
}

function text(value) {
  return String(value ?? "").trim() || null;
}

function integer(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}

function stringList(value) {
  return Array.isArray(value) ? [...new Set(value.map(text).filter(Boolean))] : [];
}

function timestamp(value) {
  if (!text(value)) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

export function normalizeVideoViewCount(detailValue, { locale } = {}) {
  const detail = detailValue && typeof detailValue === "object" ? detailValue : {};
  const explicit = integer(detail.view_count);
  const parsed = explicit == null
    ? parseLocalizedCountDetails(detail.view_count_text, { locale })
    : null;
  const value = explicit ?? parsed?.value ?? null;
  const compact = explicit == null && Number(parsed?.multiplier ?? 1) > 1;
  const declaredStatus = text(detail.view_count_status);
  const status = value == null
    ? ["unavailable", "unresolved"].includes(declaredStatus) ? declaredStatus : "unresolved"
    : compact
      ? "estimated"
      : ["exact", "estimated"].includes(declaredStatus) ? declaredStatus : "exact";
  return {
    value,
    text: text(detail.view_count_text) ?? (value == null ? null : String(value)),
    status,
    source: text(detail.view_count_source),
  };
}

function countEvidence(value, status, source, zeroStatuses = []) {
  const count = integer(value);
  return {
    value: count,
    status: ["unavailable", "unresolved"].includes(status) ? status
      : count == null ? "unresolved"
      : count === 0 && zeroStatuses.includes(status) ? status
        : status === "estimated" ? "estimated" : "exact",
    source: text(source),
  };
}

export function projectVideoDetail(detail, { locale, fallbackSource = null } = {}) {
  if (!detail) return null;
  const source = fallbackSource == null ? null : text(detail.source) ?? fallbackSource;
  const views = normalizeVideoViewCount(detail, { locale });
  const likes = countEvidence(detail.like_count, detail.like_count_status,
    detail.like_count_source ?? source, ["zero_from_empty"]);
  const disabled = detail.comments_disabled === true || detail.comment_count_status === "disabled";
  const comments = countEvidence(disabled ? 0 : detail.comment_count, detail.comment_count_status,
    detail.comment_count_source ?? detail.comments_status_source ?? source,
    ["zero_from_empty", "zero_from_surface", "zero_from_upcoming"]);
  const description = typeof detail.description === "string" ? detail.description : null;
  const descriptionObserved = description != null && detail.description_observed !== false
    && !["unavailable", "unresolved"].includes(detail.description_status);
  const duration = positiveDurationSeconds(detail.duration_seconds);
  return {
    title: text(detail.title),
    thumbnail_url: text(detail.thumbnail_url),
    ...publicationEvidenceFromFields(detail, { fallbackSource: source }),
    view_count: views.value,
    view_count_text: views.text,
    view_count_status: views.status,
    view_count_source: views.source ?? source,
    like_count: likes.value,
    like_count_status: likes.status,
    like_count_source: likes.source,
    comment_count: comments.value,
    comment_count_status: disabled ? "disabled" : comments.status,
    comment_count_source: comments.source,
    comments_disabled: disabled ? true : detail.comments_disabled == null ? null : false,
    comments_first_page: detail.comments_first_page ?? null,
    duration_seconds: duration,
    duration_status: duration == null ? detail.duration_status ?? "unresolved" : "exact",
    duration_source: text(detail.duration_source) ?? source,
    description,
    description_status: !descriptionObserved ? detail.description_status ?? "unresolved"
      : description === "" ? "empty" : "exact",
    description_source: text(detail.description_source) ?? source,
    description_observed: descriptionObserved,
    hashtags: stringList(detail.hashtags),
    hashtags_observed: detail.hashtags_observed === true || Array.isArray(detail.hashtags),
    keywords: stringList(detail.keywords),
    keywords_observed: detail.keywords_observed === true || Array.isArray(detail.keywords),
    access_status: videoAccessStatus(detail),
    access_status_source: text(detail.access_status_source) ?? source,
    live_scheduled_at: timestamp(detail.live_scheduled_at),
    live_started_at: timestamp(detail.live_started_at),
    live_ended_at: timestamp(detail.live_ended_at),
    extractor_version: text(detail.extractor_version),
  };
}
