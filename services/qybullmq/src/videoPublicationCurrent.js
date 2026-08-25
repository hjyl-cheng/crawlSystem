import { observationFactsHash } from "./crawlObservationStore.js";
import {
  PUBLICATION_CONTRACT_VERSION,
  VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL,
  VIDEO_WINDOW_POLICY_VERSION,
} from "./publicationContract.js";
import { buildPublicationSourceTrace } from "./publicationSourceTrace.js";
import { publicationResultHash } from "./publicationResultHash.js";
import {
  normalizePublicationImageUrl,
  normalizePublicationUrl,
} from "./publicationUrl.js";

const ALLOWED_CONTENT_TYPES = new Set(["video", "short", "live"]);
const PUBLISHABLE_ACCESS_STATUSES = new Set(["public", "unlisted", "members_only"]);
const UNPROVEN_ACCESS_STATUSES = new Set(["unknown", "login_required"]);
const ACCESS_RETRACTION_REASONS = new Map([
  ["private", "source_private"],
  ["unavailable", "source_unavailable"],
]);
const HASH_PATTERN = /^sha256:[0-9a-f]{64}$/;
const MILLISECONDS_PER_DAY = 86400000;
export const VIDEO_WINDOW_MAX_AGE_DAYS = 90;
export const VIDEO_WINDOW_MAX_ITEMS = 30;
const WINDOW_MAX_AGE_DAYS = VIDEO_WINDOW_MAX_AGE_DAYS;
const WINDOW_MAX_ITEMS = VIDEO_WINDOW_MAX_ITEMS;
const GAP_ABANDONMENT_STOP_REASON = "gap_abandoned_latest_30";
const GAP_ABANDONMENT_POLICY_VERSION = "latest-30-on-catchup-limit-v1";
const GAP_ABANDONMENT_SOURCE_STOP_REASON = "catchup_limit";
const GAP_ABANDONMENT_MAX_CATCH_UP_ITEMS = 50;
const FULL_CRAWL_VIDEO_COMPLETE_REASONS = new Set([
  "initial_full_video_complete",
  "repair_video_complete",
]);

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function integer(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function nonnegativeInteger(value) {
  const parsed = integer(value);
  return parsed !== null && parsed >= 0 ? parsed : null;
}

function timestamp(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function utcDay(value) {
  const normalized = timestamp(value);
  if (!normalized) return null;
  const parsed = new Date(normalized);
  return Math.floor(Date.UTC(
    parsed.getUTCFullYear(),
    parsed.getUTCMonth(),
    parsed.getUTCDate(),
  ) / MILLISECONDS_PER_DAY);
}

function compareText(left, right) {
  return Buffer.compare(Buffer.from(String(left ?? ""), "utf8"), Buffer.from(String(right ?? ""), "utf8"));
}

function uniqueTextList(value) {
  const values = Array.isArray(value) ? value : [];
  return [...new Set(values.map(text).filter(Boolean))].sort(compareText);
}

function compactIssue(issue) {
  return Object.fromEntries(Object.entries(issue).filter(([, value]) => value !== undefined));
}

function issueKey(issue) {
  return [issue.domain, issue.code, issue.field, issue.content_id]
    .map((value) => value ?? "")
    .join("\u0000");
}

function sortedIssues(issues) {
  const unique = new Map();
  for (const issue of issues) unique.set(issueKey(issue), compactIssue(issue));
  return [...unique.values()].sort((left, right) => (
    compareText(left.domain, right.domain)
    || compareText(left.code, right.code)
    || compareText(left.field, right.field)
    || compareText(left.content_id, right.content_id)
  ));
}

function countCurrent(row, name, {
  resolved,
  unresolved,
  observedAtRequired = false,
}) {
  const statusBase = name === "duration_seconds" ? "duration" : name;
  const status = text(row?.[`${statusBase}_status`]);
  const rawValue = row?.[name];
  const value = nonnegativeInteger(rawValue);
  const source = text(row?.[`${statusBase}_source`]);
  const observedAt = timestamp(
    row?.[`${statusBase}_observed_at`]
    ?? row?.player_last_observed_at
    ?? row?.next_last_observed_at
    ?? row?.last_enriched_at
    ?? row?.last_seen_at,
  );
  const isResolved = resolved.has(status);
  const isUnresolved = unresolved.has(status);
  const valid = isResolved
    ? value !== null && Boolean(source) && (!observedAtRequired || Boolean(observedAt))
    : isUnresolved && rawValue == null;
  return {
    value: isResolved ? value : null,
    status,
    source,
    observed_at: observedAt,
    valid,
  };
}

function hashPayload(payload) {
  return Object.fromEntries(Object.entries(payload).filter(([key]) => (
    !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
}

export function buildVideoPublicationItem(rowValue, { channelId } = {}) {
  const content = object(rowValue);
  const expectedChannelId = text(channelId);
  if (!expectedChannelId) throw new TypeError("channelId is required");
  const issues = [];
  const contentId = text(content.source_content_id);
  const contentKey = text(content.content_key);
  const kind = text(content.content_type);
  const precision = text(content.published_at_precision);
  const published = timestamp(content.published_at);
  const publishedDate = published?.slice(0, 10) ?? null;
  const publicationValid = Boolean(
    published
    && ["second", "date_only"].includes(precision)
    && content.published_at_status === "exact"
    && text(content.published_at_source),
  );
  const duration = countCurrent(content, "duration_seconds", {
    resolved: new Set(["exact"]),
    unresolved: new Set(["unavailable", "unresolved"]),
  });
  const views = countCurrent(content, "view_count", {
    resolved: new Set(["exact", "estimated"]),
    unresolved: new Set(["unavailable", "unresolved"]),
    observedAtRequired: true,
  });
  const likes = countCurrent(content, "like_count", {
    resolved: new Set(["exact", "zero_from_empty"]),
    unresolved: new Set(["unavailable", "unresolved"]),
    observedAtRequired: true,
  });
  const comments = countCurrent(content, "comment_count", {
    resolved: new Set(["exact", "zero_from_empty", "zero_from_surface", "zero_from_upcoming", "disabled"]),
    unresolved: new Set(["unavailable", "unresolved"]),
    observedAtRequired: true,
  });
  const descriptionStatus = text(content.description_status);
  const description = content.description == null ? null : String(content.description);
  const descriptionSource = text(content.description_source);
  const descriptionValid = (descriptionStatus === "exact" && Boolean(text(description)) && Boolean(descriptionSource))
    || (descriptionStatus === "empty" && description === "" && Boolean(descriptionSource))
    || (["unavailable", "unresolved"].includes(descriptionStatus) && description === null);
  const accessStatus = text(content.access_status);
  const accessSource = text(content.access_status_source);
  const isMembersOnly = content.is_members_only === true;
  const commentsDisabled = typeof content.comments_disabled === "boolean" ? content.comments_disabled : null;
  const checks = [
    ["channel_id", text(content.channel_id) === expectedChannelId],
    ["content_id", Boolean(contentId)],
    ["content_key", Boolean(contentKey)],
    ["kind", ALLOWED_CONTENT_TYPES.has(kind)],
    ["title", Boolean(text(content.title))],
    ["url", Boolean(normalizePublicationUrl(content.url))],
    ["published_at", publicationValid],
    ["duration", duration.valid],
    ["view_count", views.valid],
    ["like_count", likes.valid],
    ["comment_count", comments.valid],
    ["description", descriptionValid],
    ["access_status", ["public", "unlisted", "members_only", "private", "unavailable", "login_required", "unknown"].includes(accessStatus)
      && (accessStatus === "unknown" || Boolean(accessSource))],
  ];
  for (const [field, complete] of checks) {
    if (!complete) {
      issues.push({
        domain: "video",
        code: "video_item_field_incomplete",
        field,
        content_id: contentId,
      });
    }
  }
  if ((commentsDisabled === true) !== (comments.status === "disabled")
      || (comments.status === "disabled" && comments.value !== 0)) {
    issues.push({
      domain: "video",
      code: "video_item_comment_state_invalid",
      content_id: contentId,
    });
  }
  if (isMembersOnly !== (accessStatus === "members_only")) {
    issues.push({
      domain: "video",
      code: "video_item_access_state_invalid",
      content_id: contentId,
    });
  }
  const payload = {
    content_id: contentId,
    content_key: contentKey,
    kind,
    title: text(content.title),
    url: normalizePublicationUrl(content.url),
    thumbnail_url: normalizePublicationImageUrl(content.thumbnail_url),
    published_at: precision === "second" ? published : null,
    published_date: publishedDate,
    published_at_precision: precision,
    published_at_status: precision === "date_only" ? "date_exact" : "exact",
    published_at_source: text(content.published_at_source),
    duration_seconds: duration.value,
    duration_status: duration.status,
    duration_source: duration.source,
    view_count: views.value,
    view_count_status: views.status,
    view_count_source: views.source,
    view_count_observed_at: views.observed_at,
    like_count: likes.value,
    like_count_status: likes.status,
    like_count_source: likes.source,
    like_count_observed_at: likes.observed_at,
    comment_count: comments.value,
    comment_count_status: comments.status,
    comment_count_source: comments.source,
    comment_count_observed_at: comments.observed_at,
    comments_disabled: commentsDisabled,
    description,
    description_status: descriptionStatus,
    description_source: descriptionSource,
    hashtags: uniqueTextList(content.hashtags),
    keywords: uniqueTextList(content.keywords),
    access_status: accessStatus,
    access_status_source: accessSource,
    is_members_only: isMembersOnly,
    live_scheduled_at: timestamp(content.live_scheduled_at),
    live_started_at: timestamp(content.live_started_at),
    live_ended_at: timestamp(content.live_ended_at),
    extractor_version: text(content.extractor_version),
  };
  const readinessIssues = sortedIssues(issues);
  return {
    ready: readinessIssues.length === 0,
    payload,
    item_hash: readinessIssues.length === 0 ? observationFactsHash(hashPayload(payload)) : null,
    persisted_item_hash: HASH_PATTERN.test(text(content.publication_item_hash) ?? "")
      ? text(content.publication_item_hash)
      : null,
    issues: readinessIssues,
    source_refs: {
      run_id: text(content.run_id),
      observation_id: text(content.last_observation_id),
      playlist_last_seen_at: timestamp(content.playlist_last_seen_at),
      player_last_observed_at: timestamp(content.player_last_observed_at),
      next_last_observed_at: timestamp(content.next_last_observed_at),
    },
  };
}

function latestDiscovery(source) {
  return object(object(source?.complete_observation).result_summary_json).discovery ?? {};
}

function processedInitialCandidateLimit(source) {
  const completeObservation = object(source?.complete_observation);
  const discovery = object(latestDiscovery(source));
  const sourceCursor = object(object(source?.cursor).source_cursor);
  const run = object(source?.run);
  const uploadScan = object(object(run.result_json).upload_scan);
  const requestedLimit = nonnegativeInteger(uploadScan.requested_limit);
  const selectedCount = nonnegativeInteger(uploadScan.selected_count);
  const inspectedCount = nonnegativeInteger(uploadScan.inspected_count);
  const scanStoppedAtLimit = text(uploadScan.stop_reason) === "max_items"
    || text(uploadScan.terminal_reason) === "max_items";
  return completeObservation.outcome === "complete"
    && FULL_CRAWL_VIDEO_COMPLETE_REASONS.has(text(completeObservation.outcome_reason_code))
    && text(completeObservation.run_id) === text(run.run_id)
    && text(run.crawl_mode) === "full"
    && text(discovery.stop_reason) === VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL
    && text(sourceCursor.terminal_reason) === VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL
    && discovery.anchor_matched === false
    && nonnegativeInteger(discovery.parse_gap_count) === 0
    && nonnegativeInteger(discovery.detail_failure_count) === 0
    && nonnegativeInteger(discovery.items) === WINDOW_MAX_ITEMS
    && nonnegativeInteger(uploadScan.content_max_age_days) === WINDOW_MAX_AGE_DAYS
    && nonnegativeInteger(uploadScan.parse_gap_count) === 0
    && requestedLimit === WINDOW_MAX_ITEMS
    && selectedCount === requestedLimit
    && inspectedCount !== null
    && inspectedCount >= requestedLimit
    && scanStoppedAtLimit;
}

function boundedGapAbandonment(source) {
  const completeObservation = object(source?.complete_observation);
  const discovery = object(latestDiscovery(source));
  const sourceCursor = object(object(source?.cursor).source_cursor);
  const discoveryProof = object(discovery.gap_abandonment);
  const cursorProof = object(sourceCursor.gap_abandonment);
  const proofFields = [
    "policy_version",
    "source_stop_reason",
    "scanned_item_count",
    "first_page_item_count",
    "catch_up_item_count",
    "catch_up_item_limit",
    "selected_item_count",
  ];
  const proofMatches = proofFields.every((field) => discoveryProof[field] === cursorProof[field]);
  const scannedItemCount = nonnegativeInteger(discoveryProof.scanned_item_count);
  const firstPageItemCount = nonnegativeInteger(discoveryProof.first_page_item_count);
  const catchUpItemCount = nonnegativeInteger(discoveryProof.catch_up_item_count);
  const catchUpItemLimit = nonnegativeInteger(discoveryProof.catch_up_item_limit);
  const selectedItemCount = nonnegativeInteger(discoveryProof.selected_item_count);
  return completeObservation.outcome === "complete"
    && text(completeObservation.outcome_reason_code) === "video_cycle_gap_abandoned_latest_30"
    && text(discovery.stop_reason) === GAP_ABANDONMENT_STOP_REASON
    && text(sourceCursor.terminal_reason) === GAP_ABANDONMENT_STOP_REASON
    && discovery.anchor_matched === false
    && !text(sourceCursor.matched_anchor_id)
    && Array.isArray(sourceCursor.crossed_anchor_ids)
    && sourceCursor.crossed_anchor_ids.length === 0
    && nonnegativeInteger(discovery.parse_gap_count) === 0
    && proofMatches
    && text(discoveryProof.policy_version) === GAP_ABANDONMENT_POLICY_VERSION
    && text(discoveryProof.source_stop_reason) === GAP_ABANDONMENT_SOURCE_STOP_REASON
    && scannedItemCount !== null
    && firstPageItemCount !== null
    && catchUpItemCount !== null
    && catchUpItemLimit !== null
    && selectedItemCount === WINDOW_MAX_ITEMS
    && nonnegativeInteger(discovery.items) === selectedItemCount
    && scannedItemCount >= selectedItemCount
    && firstPageItemCount + catchUpItemCount === scannedItemCount
    && catchUpItemCount === catchUpItemLimit
    && catchUpItemLimit > 0
    && catchUpItemLimit <= GAP_ABANDONMENT_MAX_CATCH_UP_ITEMS;
}

function trustedAnchorContinuity({ rows, source, previousCurrent, channelId }) {
  const previous = object(previousCurrent);
  const previousProof = object(object(previous.payload_json).window_proof);
  const baselineObservedAt = timestamp(previous.complete_observed_at);
  const discovery = object(latestDiscovery(source));
  const completeObservation = object(source?.complete_observation);
  const sourceCursor = object(object(source?.cursor).source_cursor);
  const matchedAnchorId = text(sourceCursor.matched_anchor_id);
  const observationId = text(completeObservation.observation_id);
  if (nonnegativeInteger(previous.data_sequence) < 1
      || !text(previous.current_revision_id)
      || !HASH_PATTERN.test(text(previous.result_hash) ?? "")
      || previousProof.complete !== true
      || !baselineObservedAt
      || completeObservation.outcome !== "complete"
      || text(discovery.stop_reason) !== "anchor_matched"
      || discovery.anchor_matched !== true
      || nonnegativeInteger(discovery.parse_gap_count) !== 0
      || text(sourceCursor.terminal_reason) !== "anchor_matched"
      || !matchedAnchorId
      || !observationId) {
    return false;
  }
  const matchedAnchor = (Array.isArray(rows) ? rows : []).find((row) => (
    text(row?.channel_id) === channelId
    && text(row?.source_content_id) === matchedAnchorId
    && text(row?.last_observation_id) === observationId
  ));
  const firstSeenAt = timestamp(matchedAnchor?.first_seen_at);
  return Boolean(firstSeenAt && firstSeenAt <= baselineObservedAt);
}

function publicationRange(row, asOfMs, asOfDay) {
  const published = timestamp(row.published_at);
  const precision = text(row.published_at_precision);
  if (!published
      || row.published_at_status !== "exact"
      || !["second", "date_only"].includes(precision)) {
    return { trusted: false, afterAsOf: false, outsideWindow: false, sortTime: null };
  }
  const publishedMs = new Date(published).getTime();
  const publishedDay = utcDay(published);
  if (precision === "second") {
    return {
      trusted: true,
      afterAsOf: publishedMs > asOfMs,
      outsideWindow: publishedMs <= asOfMs - (WINDOW_MAX_AGE_DAYS * MILLISECONDS_PER_DAY),
      sortTime: publishedMs,
    };
  }
  return {
    trusted: true,
    afterAsOf: publishedDay > asOfDay,
    outsideWindow: publishedDay <= asOfDay - WINDOW_MAX_AGE_DAYS,
    sortTime: publishedDay * MILLISECONDS_PER_DAY,
  };
}

export function evaluateVideoPublicationWindowCoverage({ rows, channelId, asOf }) {
  const expectedChannelId = text(channelId);
  if (!expectedChannelId) throw new TypeError("channelId is required");
  const asOfIso = timestamp(asOf);
  if (!asOfIso) throw new TypeError("asOf must be a valid timestamp");
  const asOfMs = new Date(asOfIso).getTime();
  const asOfDay = utcDay(asOfIso);
  const seen = new Set();
  let qualifiedCount = 0;
  let excludedCount = 0;
  let ageBoundaryCrossed = false;
  for (const raw of Array.isArray(rows) ? rows : []) {
    const row = object(raw);
    const contentId = text(row.source_content_id);
    const kind = text(row.content_type);
    const range = publicationRange(row, asOfMs, asOfDay);
    if (contentId && seen.has(contentId)) {
      excludedCount += 1;
      continue;
    }
    if (contentId) seen.add(contentId);
    if (!ALLOWED_CONTENT_TYPES.has(kind) || !range.trusted || range.afterAsOf) {
      excludedCount += 1;
      continue;
    }
    if (range.outsideWindow) {
      ageBoundaryCrossed = true;
      excludedCount += 1;
      continue;
    }
    const accessStatus = text(row.access_status);
    if (ACCESS_RETRACTION_REASONS.has(accessStatus)) {
      excludedCount += 1;
      continue;
    }
    if (!PUBLISHABLE_ACCESS_STATUSES.has(accessStatus)) {
      excludedCount += 1;
      continue;
    }
    const item = buildVideoPublicationItem(row, { channelId: expectedChannelId });
    if (item.ready) qualifiedCount += 1;
    else excludedCount += 1;
  }
  return {
    catalog_candidate_count: Array.isArray(rows) ? rows.length : 0,
    unique_identity_count: seen.size,
    qualified_count: qualifiedCount,
    excluded_count: excludedCount,
    age_boundary_crossed: ageBoundaryCrossed,
  };
}

export function buildVideoPublicationCurrent({
  rows,
  source,
  channelId,
  asOf,
  previousCurrent = null,
}) {
  const expectedChannelId = text(channelId);
  if (!expectedChannelId) throw new TypeError("channelId is required");
  const asOfIso = timestamp(asOf);
  if (!asOfIso) throw new TypeError("asOf must be a valid timestamp");
  const asOfMs = new Date(asOfIso).getTime();
  const asOfDay = utcDay(asOfIso);
  const cutoffMs = asOfMs - (WINDOW_MAX_AGE_DAYS * MILLISECONDS_PER_DAY);
  const cutoffDay = asOfDay - WINDOW_MAX_AGE_DAYS;
  const completeObservation = object(source?.complete_observation);
  const completeObservationId = text(completeObservation.observation_id);
  const trace = buildPublicationSourceTrace(source, {
    observation_id: completeObservationId,
    observed_at: object(source?.cursor).latest_complete_observed_at,
    facts_hash: object(source?.cursor).current_facts_hash,
  }, "video");
  const issues = [...trace.issues];
  const exclusions = [];
  const candidates = [];
  const seen = new Set();
  let observedAgeBoundary = false;
  for (const raw of Array.isArray(rows) ? rows : []) {
    const row = object(raw);
    const contentId = text(row.source_content_id);
    const kind = text(row.content_type);
    const range = publicationRange(row, asOfMs, asOfDay);
    if (contentId && seen.has(contentId)) {
      exclusions.push({ content_id: contentId, reason_code: "duplicate_identity" });
      continue;
    }
    if (contentId) seen.add(contentId);
    if (!ALLOWED_CONTENT_TYPES.has(kind)) {
      exclusions.push({ content_id: contentId, reason_code: "unsupported_kind" });
      continue;
    }
    if (!range.trusted) {
      exclusions.push({ content_id: contentId, reason_code: "publication_date_unresolved" });
      continue;
    }
    if (range.afterAsOf) {
      exclusions.push({ content_id: contentId, reason_code: "publication_date_after_as_of" });
      continue;
    }
    if (range.outsideWindow) {
      if (text(row.last_observation_id) === completeObservationId) observedAgeBoundary = true;
      exclusions.push({ content_id: contentId, reason_code: "outside_90_day_window" });
      continue;
    }
    const accessStatus = text(row.access_status);
    const retractionReason = ACCESS_RETRACTION_REASONS.get(accessStatus);
    if (retractionReason) {
      exclusions.push({ content_id: contentId, reason_code: retractionReason });
      continue;
    }
    if (UNPROVEN_ACCESS_STATUSES.has(accessStatus)) {
      exclusions.push({ content_id: contentId, reason_code: "access_state_unproven" });
      issues.push({
        domain: "video",
        code: "video_access_state_unproven",
        content_id: contentId,
      });
      continue;
    }
    const item = buildVideoPublicationItem(row, { channelId: expectedChannelId });
    if (!item.ready) {
      exclusions.push({
        content_id: contentId,
        reason_code: "item_contract_incomplete",
        issues: item.issues,
      });
      continue;
    }
    if (!item.persisted_item_hash) {
      issues.push({ domain: "video", code: "video_item_hash_missing", content_id: contentId });
    } else if (item.persisted_item_hash !== item.item_hash) {
      issues.push({ domain: "video", code: "video_item_hash_mismatch", content_id: contentId });
    }
    candidates.push({ item, sort_time: range.sortTime });
  }
  candidates.sort((left, right) => (
    right.sort_time - left.sort_time
    || compareText(left.item.payload.content_id, right.item.payload.content_id)
  ));
  const selectedCandidates = candidates.slice(0, WINDOW_MAX_ITEMS);
  for (const candidate of candidates.slice(WINDOW_MAX_ITEMS)) {
    exclusions.push({
      content_id: candidate.item.payload.content_id,
      reason_code: "outside_limit",
    });
  }
  const discovery = object(latestDiscovery(source));
  const sourceCursor = object(object(source?.cursor).source_cursor);
  const listEnd = completeObservation.outcome === "complete"
    && (text(sourceCursor.terminal_reason) === "list_end" || text(discovery.stop_reason) === "list_end")
    && nonnegativeInteger(discovery.parse_gap_count) === 0;
  const ageBoundaryProof = completeObservation.outcome === "complete"
    && text(discovery.stop_reason) === "age_boundary_crossed"
    && nonnegativeInteger(discovery.parse_gap_count) === 0;
  const anchorContinuity = trustedAnchorContinuity({
    rows,
    source,
    previousCurrent,
    channelId: expectedChannelId,
  });
  const initialCandidateLimitProof = processedInitialCandidateLimit(source);
  const boundedGapProof = boundedGapAbandonment(source);
  let terminalCondition = null;
  if (selectedCandidates.length >= WINDOW_MAX_ITEMS) terminalCondition = "qualified_item_limit";
  else if (observedAgeBoundary || ageBoundaryProof) terminalCondition = "age_boundary_crossed";
  else if (listEnd) terminalCondition = "list_end_confirmed";
  else if (initialCandidateLimitProof) {
    terminalCondition = VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL;
  }
  else if (anchorContinuity) terminalCondition = "trusted_anchor_continuity";
  else if (boundedGapProof) terminalCondition = "bounded_gap_abandonment";
  if (!terminalCondition) issues.push({ domain: "video", code: "video_window_termination_unproven" });
  const items = selectedCandidates.map(({ item }, index) => ({
    position: index + 1,
    item_hash: item.item_hash,
    payload: item.payload,
    source_refs: item.source_refs,
  }));
  const readinessIssues = sortedIssues(issues);
  const ready = readinessIssues.length === 0;
  const windowPolicy = {
    policy_version: VIDEO_WINDOW_POLICY_VERSION,
    as_of: asOfIso,
    cutoff_at: new Date(cutoffMs).toISOString(),
    cutoff_date: new Date(cutoffDay * MILLISECONDS_PER_DAY).toISOString().slice(0, 10),
    max_age_days: WINDOW_MAX_AGE_DAYS,
    max_items: WINDOW_MAX_ITEMS,
  };
  const windowProof = {
    complete: Boolean(terminalCondition) && trace.traceable,
    terminal_condition: terminalCondition,
    catalog_candidate_count: Array.isArray(rows) ? rows.length : 0,
    qualified_count: candidates.length,
    selected_count: items.length,
    excluded_count: exclusions.length,
    latest_scan_items: nonnegativeInteger(discovery.items),
    latest_scan_pages: nonnegativeInteger(discovery.pages),
    latest_scan_stop_reason: text(discovery.stop_reason) ?? text(sourceCursor.terminal_reason),
    latest_scan_detail_failure_count: nonnegativeInteger(discovery.detail_failure_count),
  };
  const payload = {
    channel_id: expectedChannelId,
    window_policy: windowPolicy,
    window_proof: windowProof,
    items: items.map(({ position, item_hash, payload: itemPayload }) => ({
      position,
      item_hash,
      ...itemPayload,
    })),
  };
  return {
    ready,
    contract_version: PUBLICATION_CONTRACT_VERSION,
    policy_version: VIDEO_WINDOW_POLICY_VERSION,
    result_hash: ready ? publicationResultHash("video", payload) : null,
    payload,
    window_policy: windowPolicy,
    window_proof: windowProof,
    items,
    exclusions: exclusions.sort((left, right) => (
      compareText(left.content_id, right.content_id)
      || compareText(left.reason_code, right.reason_code)
    )),
    source_refs: trace,
    issues: readinessIssues,
  };
}
