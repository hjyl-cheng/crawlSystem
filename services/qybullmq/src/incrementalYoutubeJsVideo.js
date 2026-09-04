import { createHash, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { currentChannelExecutionAbortSignal } from "./channelExecutionContext.js";
import { combineAbortSignals, throwIfAborted } from "./abortSignal.js";
import {
  CONTENT_ENRICH_CLOCK_MODE,
  loadContentEnrichMode,
} from "./contentEnrichMode.js";
import {
  contentEnrichDetailOutcome,
  contentEnrichFailureOutcome,
} from "./contentEnrichPolicy.js";
import { recordCrawlerObservation } from "./crawlObservationStore.js";
import {
  hasCompletePublicVideoSurface,
  isUpcomingLiveDetail,
  unfinishedLiveReason,
  videoAccessStatus,
} from "./detailPolicy.js";
import {
  incrementalVideoPlannerConfig,
  orderedDiscoveryAnchors,
  planRecentVideoSampling,
} from "./incrementalVideoPlanner.js";
import { fetchYoutubeJsVideoDetail } from "./youtubeJs.js";
import { resolveYoutubeContentType } from "./youtubeContentType.js";
import { fullVideoStorageAction } from "./fullVideoContentStore.js";
import {
  normalizePublicationEvidence,
  publicationEvidenceFromFields,
  publicationEvidenceCandidateWinsSql,
  publicationEvidenceConflictPatchSql,
  publicationEvidenceConflictRecord,
  selectPublicationEvidence,
} from "./publicationTimeEvidence.js";
import {
  resolveVideoDisposition,
  videoAccessRecheckAt,
  videoDispositionSummary,
} from "./videoDisposition.js";
import { assertYoutubeContentObservation } from "./youtubePlayability.js";
import {
  selectYoutubeFailure,
  shouldReportProxyFailure,
  youtubeFailureText,
} from "./youtubeFailurePolicy.js";
import { reconcilePublication } from "./publicationReconciler.js";
import {
  applyVideoActivityLifecycle,
  buildVideoActivityEvidence,
} from "./videoActivityLifecycle.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";

const GAP_ABANDONMENT_STOP_REASON = "gap_abandoned_latest_30";
const GAP_ABANDONMENT_POLICY_VERSION = "latest-30-on-catchup-limit-v1";
const GAP_ABANDONMENT_ITEM_LIMIT = 30;
const CHECKPOINT_PHASES = ["first_seen", "recent"];
const CHECKPOINT_TERMINAL_ITEM_STATUSES = new Set(["captured", "settled_error"]);
const CHECKPOINT_SETTLED_FAILURE_KINDS = new Set(["content_terminal", "parser_runtime"]);
const PROBE_ROUTE_FAILURE_KINDS = new Set([
  "proxy_transport",
  "youtube_rate_limited",
  "youtube_challenge",
]);
const CHECKPOINT_FIELD_STATUS_VERSION = 1;
const CHECKPOINT_ACTIVE_CLAIM_POLL_MAX_MS = 5_000;
const CHECKPOINT_ACTIVE_CLAIM_POLL_MIN_MS = 50;
const CONTENT_UPSERT_PUBLICATION_WINS = publicationEvidenceCandidateWinsSql(
  "crawler.contents",
  "EXCLUDED",
);
const CONTENT_UPSERT_PUBLICATION_CONFLICT = publicationEvidenceConflictPatchSql(
  "crawler.contents",
  "EXCLUDED",
);
const SCAN_PUBLICATION_WINS = publicationEvidenceCandidateWinsSql("content", "input");
const SCAN_PUBLICATION_CONFLICT = publicationEvidenceConflictPatchSql("content", "input");

function objectValue(value) {
  if (value == null || typeof value !== "object" || Array.isArray(value)) return null;
  return value;
}

function parsedJsonObject(value, field) {
  let parsed = value;
  if (typeof parsed === "string") {
    try {
      parsed = JSON.parse(parsed);
    } catch (error) {
      throw new TypeError(`${field} must contain valid JSON`, { cause: error });
    }
  }
  const output = objectValue(parsed);
  if (!output) throw new TypeError(`${field} must be an object`);
  return output;
}

function canonicalValue(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalValue);
  return Object.fromEntries(
    Object.keys(value).sort()
      .filter((key) => value[key] !== undefined)
      .map((key) => [key, canonicalValue(value[key])]),
  );
}

function sha256Json(value) {
  return `sha256:${createHash("sha256")
    .update(JSON.stringify(canonicalValue(value)))
    .digest("hex")}`;
}

export function incrementalYoutubeJsVideoCycleKey(resultJsonValue) {
  const resultJson = parsedJsonObject(resultJsonValue ?? {}, "channel_runs.result_json");
  const recoveriesValue = Object.prototype.hasOwnProperty.call(resultJson, "controlled_recoveries")
    ? resultJson.controlled_recoveries
    : {};
  const recoveries = objectValue(recoveriesValue);
  if (!recoveries) {
    throw new TypeError("channel_runs.result_json.controlled_recoveries must be an object");
  }
  const markerKeys = Object.keys(recoveries).sort();
  for (const markerKey of markerKeys) {
    const marker = objectValue(recoveries[markerKey]);
    if (!marker || String(marker.operation_id ?? "").trim() !== markerKey) {
      throw new Error(`invalid controlled recovery marker: ${markerKey}`);
    }
  }
  if (markerKeys.length === 0) return "base";
  const digest = createHash("sha256")
    .update(JSON.stringify(markerKeys))
    .digest("hex");
  return `recovery:${digest}`;
}

export function incrementalYoutubeJsVideoTargetHash(items) {
  const targets = [...items]
    .map((item) => ({
      phase: String(item.phase),
      ordinal: Number(item.ordinal),
      video_id: String(item.video_id),
      target: item.target_json ?? item.target,
    }))
    .sort((left, right) => CHECKPOINT_PHASES.indexOf(left.phase)
      - CHECKPOINT_PHASES.indexOf(right.phase)
      || left.ordinal - right.ordinal
      || left.video_id.localeCompare(right.video_id));
  return sha256Json(targets);
}

function explicitFieldStatus(detail, field) {
  const has = Object.prototype.hasOwnProperty.call(detail, field);
  if (!has || detail[field] === undefined) return "unobserved";
  const value = detail[field];
  if (value === null) return "unobserved";
  if (Array.isArray(value) && value.length === 0) return "empty";
  if (typeof value === "string" && value === "") return "empty";
  return "exact";
}

export function incrementalYoutubeJsVideoFieldStatus(detailValue, errorValue = null) {
  const detail = objectValue(detailValue) ?? {};
  const commentFailure = errorValue?.required_surface === "comments";
  const playerFailure = errorValue?.required_surface === "player";
  const descriptionObserved = detail.description_observed === true
    || ["exact", "empty"].includes(detail.description_status);
  const descriptionStatus = detail.description_status === "unavailable"
    ? "unavailable"
    : descriptionObserved
      ? (detail.description === "" ? "empty" : "exact")
      : "unobserved";
  const hashtagsStatus = detail.hashtags_observed === true
    ? (Array.isArray(detail.hashtags) && detail.hashtags.length === 0 ? "empty" : "exact")
    : "unobserved";
  const keywordsStatus = detail.keywords_observed === true
    ? (Array.isArray(detail.keywords) && detail.keywords.length === 0 ? "empty" : "exact")
    : "unobserved";
  const commentsDisabled = detail.comments_disabled === true
    || detail.comment_count_status === "disabled";
  const commentStatus = commentFailure
    ? "parser_gap"
    : commentsDisabled
      ? "disabled"
      : explicitFieldStatus(detail, "comment_count");
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
    version: CHECKPOINT_FIELD_STATUS_VERSION,
    id: explicitFieldStatus(detail, "id"),
    title: explicitFieldStatus(detail, "title"),
    thumbnail_url: explicitFieldStatus(detail, "thumbnail_url"),
    published_at: detail.published_at_status === "unresolved"
      ? "unobserved"
      : explicitFieldStatus(detail, "published_at"),
    content_type_signals: explicitFieldStatus(detail, "content_type_signals"),
    duration_seconds: explicitFieldStatus(detail, "duration_seconds"),
    view_count: explicitFieldStatus(detail, "view_count"),
    like_count: explicitFieldStatus(detail, "like_count"),
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

function serializedCheckpointError(error) {
  const selected = selectYoutubeFailure({ error });
  const evidence = selected.evidence ?? {};
  return {
    name: String(error?.name || "Error").slice(0, 255),
    message: String(error?.message || error).slice(0, 2000),
    code: text(error?.code),
    required_surface: text(error?.required_surface),
    failure_text: youtubeFailureText(error),
    decision: selected.decision,
    evidence: {
      status: evidence.status ?? null,
      body: String(evidence.body ?? "").slice(0, 500),
      source: text(evidence.source),
      target_url: text(evidence.target_url),
      client: text(evidence.client),
    },
  };
}

function checkpointErrorFromJson(value) {
  const stored = objectValue(value) ?? {};
  const error = new Error(String(stored.message ?? "checkpointed YouTubeJS detail failure"));
  error.name = String(stored.name ?? "Error");
  if (stored.code != null) error.code = stored.code;
  if (stored.required_surface != null) error.required_surface = stored.required_surface;
  if (objectValue(stored.evidence)) error.youtube_failure_evidence = stored.evidence;
  if (objectValue(stored.decision)) error.youtube_failure_decision = stored.decision;
  return error;
}

function integer(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}

function resultRowCount(result) {
  if (Number.isInteger(result?.rowCount)) return result.rowCount;
  return Array.isArray(result?.rows) ? result.rows.length : 0;
}

function positiveInteger(value) {
  const number = integer(value);
  return number != null && number > 0 ? number : null;
}

function text(value) {
  if (value === null || value === undefined) return null;
  const output = String(value).trim();
  return output || null;
}

function stringList(value) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map(text).filter(Boolean))];
}

function extractorPublicationEvidence(detail, fallbackSource = null) {
  return publicationEvidenceFromFields(detail, { fallbackSource });
}

function mergedDiscoveryAnchorIds(entries, anchors, limit = 20) {
  const existingIds = anchors.map((anchor) => text(anchor?.id)).filter(Boolean);
  const existing = new Set(existingIds);
  const freshIds = entries
    .map((entry) => text(entry?.id))
    .filter((id) => id && !existing.has(id));
  return [...new Set([...freshIds, ...existingIds])].slice(0, Math.max(1, limit));
}

function applyCatchupGapAbandonment(scan, anchors, { catchUpMaxItems } = {}) {
  if (scan?.stop_reason !== "catchup_limit"
      || scan?.terminal_reason !== "catchup_limit"
      || scan?.anchor_matched !== false
      || scan?.complete !== false
      || Number(scan?.parse_gap_count ?? 0) !== 0) {
    return scan;
  }
  const scannedEntries = Array.isArray(scan?.entries) ? scan.entries : [];
  const firstPageItemCount = integer(scan?.first_page_item_count);
  const catchUpItemCount = integer(scan?.catch_up_item_count);
  const expectedCatchUpItemCount = positiveInteger(catchUpMaxItems);
  const rawItemCount = integer(scan?.item_count);
  const scannedVideoIds = scannedEntries.map((entry) => text(entry?.id));
  if (scannedEntries.length < GAP_ABANDONMENT_ITEM_LIMIT
      || firstPageItemCount == null
      || catchUpItemCount == null
      || expectedCatchUpItemCount == null
      || catchUpItemCount !== expectedCatchUpItemCount
      || rawItemCount !== scannedEntries.length
      || firstPageItemCount + catchUpItemCount !== scannedEntries.length
      || scannedVideoIds.some((videoId) => videoId == null)
      || new Set(scannedVideoIds).size !== scannedVideoIds.length) {
    return scan;
  }
  const selectedEntries = scannedEntries.slice(0, GAP_ABANDONMENT_ITEM_LIMIT);
  return {
    ...scan,
    entries: selectedEntries,
    item_count: selectedEntries.length,
    anchor_matched: false,
    matched_anchor_id: null,
    crossed_anchor_ids: [],
    stop_reason: GAP_ABANDONMENT_STOP_REASON,
    terminal_reason: GAP_ABANDONMENT_STOP_REASON,
    complete: true,
    gap_abandonment: {
      policy_version: GAP_ABANDONMENT_POLICY_VERSION,
      source_stop_reason: "catchup_limit",
      scanned_item_count: scannedEntries.length,
      first_page_item_count: firstPageItemCount,
      catch_up_item_count: catchUpItemCount,
      catch_up_item_limit: expectedCatchUpItemCount,
      selected_item_count: selectedEntries.length,
      scanned_video_ids: scannedVideoIds,
      selected_video_ids: scannedVideoIds.slice(0, GAP_ABANDONMENT_ITEM_LIMIT),
      abandoned_anchor_ids: stringList(anchors.map((anchor) => anchor?.id)),
    },
  };
}

function classifyVideoType(entry, detail = null) {
  return resolveYoutubeContentType({
    videoId: entry?.id ?? entry?.video_id ?? detail?.id,
    upload: entry,
    detail,
  });
}

function detailViewCount(detail) {
  return integer(detail?.view_count ?? detail?.view_count_text);
}

function detailAccess(detail) {
  return videoAccessStatus(detail);
}

function publishedAt(detail) {
  const value = text(detail?.published_at);
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function uploadsPublishedFacts(entry) {
  const published = text(entry?.published_at) ?? text(entry?.published_day);
  const evidence = publicationEvidenceFromFields({
    published_at: published,
    published_at_status: entry?.published_at_status,
    published_at_precision: entry?.published_at_precision,
    published_at_source: entry?.published_at_source,
  });
  return evidence.published_at ? evidence : null;
}

function detailSource(detail) {
  return text(detail?.source) ?? "youtubejs_player";
}

function detailFacts(detail) {
  if (!detail) return null;
  const source = detailSource(detail);
  const publication = extractorPublicationEvidence(detail, source);
  const commentsDisabled = detail.comments_disabled === true;
  return {
    title: text(detail.title),
    thumbnail_url: text(detail.thumbnail_url),
    ...publication,
    view_count: detailViewCount(detail),
    view_count_source: text(detail.view_count_source) ?? source,
    like_count: integer(detail.like_count),
    like_count_source: text(detail.like_count_source) ?? source,
    comment_count: commentsDisabled ? 0 : integer(detail.comment_count),
    comment_count_source: text(detail.comment_count_source ?? detail.comments_status_source) ?? source,
    comments_disabled: detail.comments_disabled == null
      ? null
      : commentsDisabled,
    comments_first_page: detail.comments_first_page ?? null,
    duration_seconds: positiveInteger(detail.duration_seconds),
    duration_source: text(detail.duration_source) ?? source,
    description: typeof detail.description === "string" ? detail.description : null,
    description_source: text(detail.description_source) ?? source,
    description_observed: typeof detail.description === "string",
    hashtags: stringList(detail.hashtags),
    hashtags_observed: detail.hashtags_observed === true || Array.isArray(detail.hashtags),
    keywords: stringList(detail.keywords),
    keywords_observed: detail.keywords_observed === true || Array.isArray(detail.keywords),
    access_status: detailAccess(detail),
    access_status_source: text(detail.access_status_source) ?? source,
    live_scheduled_at: publishedAt({ published_at: detail.live_scheduled_at }),
    live_started_at: publishedAt({ published_at: detail.live_started_at }),
    live_ended_at: publishedAt({ published_at: detail.live_ended_at }),
    extractor_version: text(detail.extractor_version),
  };
}

function missingStoredText(value) {
  return text(value) == null;
}

function recentStorageFacts(row, facts, { allowStaticRepair = false } = {}) {
  if (allowStaticRepair) {
    return {
      title: facts.title,
      thumbnail_url: facts.thumbnail_url,
      description: facts.description,
      description_source: facts.description_source,
      description_observed: facts.description_observed,
      hashtags: facts.hashtags,
      hashtags_observed: facts.hashtags_observed,
      keywords: facts.keywords,
      keywords_observed: facts.keywords_observed,
      duration_seconds: facts.duration_seconds,
      duration_source: facts.duration_source,
      live_scheduled_at: facts.live_scheduled_at,
      live_started_at: facts.live_started_at,
      live_ended_at: facts.live_ended_at,
    };
  }
  const descriptionResolved = ["exact", "empty"].includes(text(row.description_status));
  const descriptionMissing = missingStoredText(row.description) && !descriptionResolved;
  const durationMissing = positiveInteger(row.duration_seconds) == null;
  return {
    title: missingStoredText(row.title) ? facts.title : null,
    thumbnail_url: missingStoredText(row.thumbnail_url) ? facts.thumbnail_url : null,
    description: descriptionMissing ? facts.description : null,
    description_source: descriptionMissing ? facts.description_source : null,
    description_observed: descriptionMissing && facts.description_observed,
    hashtags: [],
    hashtags_observed: false,
    keywords: [],
    keywords_observed: false,
    duration_seconds: durationMissing ? facts.duration_seconds : null,
    duration_source: durationMissing ? facts.duration_source : null,
    live_scheduled_at: row.live_scheduled_at == null ? facts.live_scheduled_at : null,
    live_started_at: row.live_started_at == null ? facts.live_started_at : null,
    live_ended_at: row.live_ended_at == null ? facts.live_ended_at : null,
  };
}

export async function fetchIncrementalYoutubeJsVideoDetail(videoId, {
  fetchYoutubeJs = fetchYoutubeJsVideoDetail,
  signal = null,
  phase = "first_seen",
  target = null,
} = {}) {
  const effectiveSignal = combineAbortSignals(signal, currentChannelExecutionAbortSignal());
  const assertNotAborted = () => throwIfAborted(effectiveSignal);
  const requiresFullSurface = phase !== "recent" || target?.enrich_pending === true;
  assertNotAborted();
  const detail = assertYoutubeContentObservation(await fetchYoutubeJs(videoId, {
    signal: effectiveSignal,
    strictRequiredSurfaces: true,
    detailMode: requiresFullSurface ? "full" : "metrics",
  }), {
    videoId,
    source: "youtubejs_player",
  });
  assertNotAborted();
  if (!objectValue(detail)) {
    throw new TypeError(`YouTube.js detail is missing for ${videoId}`);
  }
  const commentsRequired = !["members_only", "private", "unavailable"]
    .includes(detail.access_status)
    && detail.is_upcoming !== true
    && detail.comments_disabled !== true;
  if (commentsRequired && text(detail.youtubejs_comments_error)) {
    const error = new Error(
      `YouTube.js required comments surface failed for ${videoId}: ${detail.youtubejs_comments_error}`,
    );
    error.name = "YoutubeJsRequiredSurfaceError";
    error.required_surface = "comments";
    error.partial_detail = detail;
    throw error;
  }
  const classification = resolveYoutubeContentType({ videoId, detail });
  const terminalAccess = ["members_only", "private", "unavailable"]
    .includes(detailAccess(detail));
  if (!requiresFullSurface && !terminalAccess && detailViewCount(detail) == null) {
    const error = new Error(
      `YouTube.js parser gap: required Video metrics surface is incomplete for ${videoId}`,
    );
    error.name = "YoutubeJsRequiredSurfaceError";
    error.required_surface = "player";
    error.partial_detail = detail;
    throw error;
  }
  if (!terminalAccess
      && requiresFullSurface
      && !isUpcomingLiveDetail(detail)
      && (classification?.authoritative !== true || !hasCompletePublicVideoSurface(detail))) {
    const error = new Error(
      `YouTube.js parser gap: required public Video surface is incomplete for ${videoId}`,
    );
    error.name = "YoutubeJsRequiredSurfaceError";
    error.required_surface = "player";
    error.partial_detail = detail;
    throw error;
  }
  return detail;
}

function probability(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 && number <= 1 ? number : null;
}

function nextVideoChangeProbability(row, facts, alpha) {
  const signals = [];
  const previousView = integer(row.view_count ?? row.view_count_text);
  if (previousView != null && facts.view_count != null) {
    signals.push({ weight: 0.70, changed: previousView !== facts.view_count });
  }
  const previousLike = integer(row.like_count);
  if (previousLike != null && facts.like_count != null) {
    signals.push({ weight: 0.15, changed: previousLike !== facts.like_count });
  }
  const previousComment = integer(row.comment_count);
  if (previousComment != null && facts.comment_count != null) {
    signals.push({ weight: 0.15, changed: previousComment !== facts.comment_count });
  }
  const previous = probability(row.video_change_probability);
  if (signals.length === 0) return previous;
  const weight = signals.reduce((total, signal) => total + signal.weight, 0);
  const observed = signals.reduce(
    (total, signal) => total + (signal.changed ? signal.weight : 0),
    0,
  ) / weight;
  return previous == null ? observed : (alpha * observed) + ((1 - alpha) * previous);
}

async function knownVideoIds(query, channelId, videoIds) {
  if (videoIds.length === 0) return new Set();
  const known = await query(
    `SELECT source_content_id AS video_id
     FROM crawler.contents
     WHERE channel_id=$1 AND source_content_id=ANY($2::text[])`,
    [channelId, videoIds],
  );
  return new Set(known.rows.map((row) => String(row.video_id)));
}

async function outstandingDeferredVideoIds(query, channelId) {
  const rows = await query(
    `WITH ranked AS (
       SELECT candidate.channel_id,candidate.source_content_id,candidate.candidate_id,
              candidate.disposition,
              row_number() OVER (
                PARTITION BY candidate.source_content_id
                ORDER BY candidate.candidate_id DESC
              ) AS disposition_rank
       FROM crawler.content_candidates candidate
       WHERE candidate.channel_id=$1
     )
     SELECT ranked.source_content_id AS video_id
     FROM ranked
     WHERE ranked.disposition_rank=1
       AND ranked.disposition='deferred'
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.contents content
         WHERE content.channel_id=ranked.channel_id
           AND content.source_content_id=ranked.source_content_id
       )
     ORDER BY ranked.candidate_id`,
    [channelId],
  );
  return stringList(rows.rows.map((row) => row.video_id));
}

async function latestVideoDispositionEntries(query, channelId, videoIds) {
  const ids = [...new Set(videoIds.map((videoId) => text(videoId)).filter(Boolean))];
  if (ids.length === 0) return new Map();
  const latest = await query(
    `SELECT DISTINCT ON (candidate.source_content_id)
            candidate.source_content_id,candidate.candidate_id,candidate.disposition,
            candidate.next_attempt_at,candidate.result_json,candidate.error_message
     FROM crawler.content_candidates candidate
     WHERE candidate.channel_id=$1
       AND candidate.source_content_id=ANY($2::text[])
     ORDER BY candidate.source_content_id,candidate.candidate_id DESC`,
    [channelId, ids],
  );
  return new Map(latest.rows.map((row) => [text(row.source_content_id), row]));
}

function dispositionRecheckEntry(entry, prior) {
  return {
    ...entry,
    disposition_recheck: {
      candidate_id: Number(prior.candidate_id),
      prior_kind: text(prior.disposition),
      prior_reason_code: text(prior.result_json?.disposition?.reason_code),
      scheduled_at: prior.next_attempt_at == null
        ? null
        : new Date(prior.next_attempt_at).toISOString(),
    },
  };
}

function scannedVideoDispositionWork(entries, priorByVideoId, observedAt, {
  allowDueRechecks = true,
} = {}) {
  const observedAtMs = Date.parse(observedAt);
  const pendingDeferredVideoIds = [];
  const workEntries = entries.flatMap((entry) => {
    const prior = priorByVideoId.get(entry.id);
    const priorKind = text(prior?.disposition);
    if (!["deferred", "terminal_excluded"].includes(priorKind)) return [entry];
    if (!allowDueRechecks) {
      if (priorKind === "deferred") pendingDeferredVideoIds.push(entry.id);
      return [];
    }
    const nextAttemptAtMs = Date.parse(prior.next_attempt_at);
    const due = !Number.isFinite(nextAttemptAtMs) || nextAttemptAtMs <= observedAtMs;
    if (due) return [dispositionRecheckEntry(entry, prior)];
    if (priorKind === "deferred") pendingDeferredVideoIds.push(entry.id);
    return [];
  });
  return { workEntries, pendingDeferredVideoIds };
}

function videoActivityEvidence(videoIdValue, contentTypeValue, publication, facts = null) {
  const videoId = text(videoIdValue);
  if (!videoId) return null;
  const contentType = text(contentTypeValue);
  return {
    source_content_id: videoId,
    content_type: ["video", "short", "live"].includes(contentType) ? contentType : "video",
    ...normalizePublicationEvidence(publication),
    live_ended_at: facts?.live_ended_at ?? null,
    duration_seconds: facts?.duration_seconds ?? null,
  };
}

function currentRunActivityEvidence(
  commandEntries,
  scanEntries,
  dispositions,
  storedActivityEvidence = [],
) {
  const evidenceByVideoId = new Map();
  for (const entry of Array.isArray(commandEntries) ? commandEntries : []) {
    const videoId = text(entry?.video_id);
    if (!videoId) continue;
    evidenceByVideoId.set(
      videoId,
      videoActivityEvidence(videoId, entry?.content_type, entry, entry),
    );
  }
  for (const evidence of Array.isArray(storedActivityEvidence) ? storedActivityEvidence : []) {
    const videoId = text(evidence?.source_content_id);
    if (!videoId) continue;
    evidenceByVideoId.set(videoId, evidence);
  }
  for (const entry of Array.isArray(scanEntries) ? scanEntries : []) {
    if (unfinishedLiveReason(entry) === "live_in_progress" && text(entry?.id)) {
      evidenceByVideoId.set(text(entry.id), {
        source_content_id: text(entry.id),
        content_type: "live",
        is_live: true,
        published_at: null,
        published_at_status: "unresolved",
        published_at_precision: "unknown",
        published_at_source: null,
      });
    }
  }
  for (const item of Array.isArray(dispositions) ? dispositions : []) {
    if (item?.reason_code === "live_in_progress" && text(item?.video_id)) {
      evidenceByVideoId.set(text(item.video_id), {
        source_content_id: text(item.video_id),
        content_type: "live",
        is_live: true,
        published_at: null,
        published_at_status: "unresolved",
        published_at_precision: "unknown",
        published_at_source: null,
      });
    }
  }
  return [...evidenceByVideoId.values()];
}

async function loadDueVideoDispositionEntries(query, channelId, observedAt, scanEntries, limit = 10) {
  const due = await query(
    `SELECT candidate.*
     FROM crawler.content_candidates candidate
     WHERE candidate.channel_id=$1
       AND candidate.disposition IN ('deferred','terminal_excluded')
       AND candidate.next_attempt_at<=$2::timestamptz
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.content_candidates newer
         WHERE newer.channel_id=candidate.channel_id
           AND newer.source_content_id=candidate.source_content_id
           AND newer.candidate_id>candidate.candidate_id
       )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.contents content
         WHERE content.channel_id=candidate.channel_id
           AND content.source_content_id=candidate.source_content_id
       )
     ORDER BY candidate.next_attempt_at,candidate.candidate_id
     LIMIT $3`,
    [channelId, observedAt, Math.max(1, limit)],
  );
  const scannedIds = new Set(scanEntries.map((entry) => text(entry?.id)).filter(Boolean));
  const maxPosition = scanEntries.reduce(
    (current, entry) => Math.max(current, integer(entry?.position) ?? 0),
    0,
  );
  return due.rows
    .filter((row) => !scannedIds.has(text(row.source_content_id)))
    .map((row, index) => {
      const flat = row.result_json?.flat ?? {};
      return {
        id: text(row.source_content_id),
        position: maxPosition + index + 1,
        title: text(row.title) ?? text(flat.title),
        thumbnail_url: text(row.thumbnail_url) ?? text(flat.thumbnail_url),
        published_day: text(flat.published_day),
        disposition_recheck: {
          candidate_id: Number(row.candidate_id),
          prior_kind: text(row.disposition),
          prior_reason_code: text(row.result_json?.disposition?.reason_code),
          scheduled_at: row.next_attempt_at == null
            ? null
            : new Date(row.next_attempt_at).toISOString(),
        },
      };
    })
    .filter((entry) => entry.id);
}

async function loadDiscoveryAnchors(query, channelId) {
  const cursor = await query(
    `SELECT COALESCE(
              jsonb_agg(
                jsonb_build_object(
                  'video_id',anchor.video_id,
                  'published_at',content.published_at
                ) ORDER BY anchor.ordinality
              ) FILTER (
                WHERE anchor.video_id IS NOT NULL AND content.published_at IS NOT NULL
              ),
              '[]'::jsonb
            ) AS anchors
     FROM crawler.channel_domain_cursors cursor
     LEFT JOIN LATERAL unnest(cursor.anchor_video_ids) WITH ORDINALITY
       AS anchor(video_id,ordinality) ON true
     LEFT JOIN LATERAL (
       SELECT known.published_at
       FROM crawler.contents known
       WHERE known.channel_id=cursor.channel_id
         AND known.source_content_id=anchor.video_id
         AND known.published_at IS NOT NULL
       ORDER BY known.published_at DESC,known.last_seen_at DESC
       LIMIT 1
     ) content ON true
     WHERE cursor.channel_id=$1 AND cursor.observation_kind='video'
     GROUP BY cursor.channel_id`,
    [channelId],
  );
  if (cursor.rows.length > 0) {
    const rows = Array.isArray(cursor.rows[0].anchors) ? cursor.rows[0].anchors : [];
    return orderedDiscoveryAnchors(rows);
  }

  const fallback = await query(
    `SELECT video_id,published_at
     FROM (
       SELECT DISTINCT ON (source_content_id)
              source_content_id AS video_id,published_at,last_seen_at
       FROM crawler.contents
       WHERE channel_id=$1
         AND content_type IN ('video','short','live')
         AND published_at IS NOT NULL
       ORDER BY source_content_id,published_at DESC,last_seen_at DESC
     ) known
     ORDER BY published_at DESC,video_id
     LIMIT 20`,
    [channelId],
  );
  return orderedDiscoveryAnchors(fallback.rows);
}

function refreshTaskId(contentKey, jobType) {
  const digest = createHash("sha256").update(`${contentKey}:${jobType}`).digest("hex").slice(0, 32);
  return `${jobType}:${digest}`;
}

function clockContentEnrichRetryOptions() {
  return {
    baseMs: Number(process.env.CONTENT_ENRICH_RETRY_BASE_MS || 30_000),
    maxMs: Number(process.env.CONTENT_ENRICH_RETRY_MAX_MS || 6 * 60 * 60_000),
    maxAttempts: Number(process.env.CONTENT_ENRICH_MAX_ATTEMPTS || 8),
  };
}

function clockContentEnrichReservationMs() {
  const value = Number(process.env.CONTENT_ENRICH_DISPATCH_LEASE_MS || 15 * 60_000);
  if (!Number.isSafeInteger(value) || value < 30_000) return 15 * 60_000;
  return Math.min(value, 24 * 60 * 60_000);
}

function clockContentEnrichLeaseOwner({ runId, cycleKey }) {
  const digest = createHash("sha256")
    .update(`${runId}:${cycleKey}`)
    .digest("hex")
    .slice(0, 32);
  return `clock-content-enrich:${digest}`;
}

async function prepareClockContentEnrichOutcome(client, {
  contentKey,
  jobType,
  detail = null,
  error = null,
}) {
  const locked = await client.query(
    `SELECT task.*,
            COALESCE(
              task.status IN ('leased','running')
                AND task.lease_expires_at>clock_timestamp(),
              false
            ) AS lease_live,
            COALESCE(
              task.next_retry_at<=clock_timestamp(),
              task.status IN ('queued','failed')
            ) AS retry_due
     FROM crawler.content_enrich_tasks task
     WHERE task.content_key=$1 AND task.job_type=$2
     FOR UPDATE`,
    [contentKey, jobType],
  );
  const currentTask = locked.rows[0] ?? null;
  if (currentTask?.lease_live === true) return { skipped: true, currentTask, outcome: null };
  if (["queued", "failed"].includes(currentTask?.status) && currentTask.retry_due !== true) {
    return { skipped: true, currentTask, outcome: null };
  }
  if (currentTask?.status === "terminal" && currentTask.retry_due !== true) {
    return { skipped: true, currentTask, outcome: null };
  }
  const completedAt = new Date();
  const task = {
    task_id: currentTask?.task_id ?? refreshTaskId(contentKey, jobType),
    dispatch_generation: currentTask?.dispatch_generation ?? 0,
    attempts: ["done", "terminal", "skipped"].includes(currentTask?.status)
      ? 0
      : Number(currentTask?.attempts ?? 0),
  };
  const retryOptions = clockContentEnrichRetryOptions();
  const outcome = detail == null
    ? contentEnrichFailureOutcome(task, error, completedAt, retryOptions)
    : contentEnrichDetailOutcome(task, detail, completedAt, retryOptions);
  return { skipped: false, currentTask, outcome };
}

async function persistClockContentEnrichOutcome(client, {
  currentTask,
  priorTaskStatus = currentTask?.status ?? null,
  outcome,
  contentKey,
  channelId,
  runId,
  observationId,
  jobType,
  observedAt,
}) {
  if (!currentTask && outcome.kind === "done") return;
  const status = outcome.kind === "retryable" ? "failed" : outcome.kind;
  const incrementsAttempts = ["retryable", "dead_letter"].includes(outcome.kind);
  const resultJson = JSON.stringify({
    kind: outcome.kind,
    observed_at: outcome.observed_at,
    access_status: outcome.access_status,
    failure_decision: outcome.failure_decision ?? null,
    consumer: "clock",
  });
  await client.query(
    `INSERT INTO crawler.content_enrich_tasks (
       task_id,content_key,channel_id,job_type,status,priority,attempts,
       result_json,error_message,requested_by_run_id,requested_observation_id,
       next_retry_at,last_attempt_at,last_success_at,updated_at
     ) VALUES (
       $1,$2,$3,$4,$5,10,$6,
       jsonb_build_object('last_outcome',$7::jsonb),$8,$9,$10,$11,$12,
       CASE WHEN $15::boolean THEN $13::timestamptz ELSE NULL END,now()
     )
     ON CONFLICT (content_key,job_type) DO UPDATE
     SET status=$5,
         priority=LEAST(crawler.content_enrich_tasks.priority,10),
         attempts=CASE
           WHEN $16::boolean THEN CASE WHEN $14::boolean THEN 1 ELSE 0 END
           WHEN $14::boolean THEN
             CASE WHEN crawler.content_enrich_tasks.status IN ('done','skipped')
               THEN 1 ELSE crawler.content_enrich_tasks.attempts+1 END
           ELSE crawler.content_enrich_tasks.attempts
         END,
         result_json=crawler.content_enrich_tasks.result_json
           || jsonb_build_object('last_outcome',$7::jsonb),
         error_message=$8,
         requested_by_run_id=$9,
         requested_observation_id=COALESCE($10,crawler.content_enrich_tasks.requested_observation_id),
         next_retry_at=$11,
         last_attempt_at=$12,
         last_success_at=CASE WHEN $15::boolean THEN $13
           ELSE crawler.content_enrich_tasks.last_success_at END,
         lease_owner=NULL,lease_expires_at=NULL,updated_at=now()`,
    [
      refreshTaskId(contentKey, jobType),
      contentKey,
      channelId,
      jobType,
      status,
      incrementsAttempts ? 1 : 0,
      resultJson,
      outcome.error_message,
      runId,
      observationId,
      outcome.next_retry_at ?? null,
      outcome.observed_at,
      observedAt,
      incrementsAttempts,
      outcome.detail != null,
      priorTaskStatus === "terminal",
    ],
  );
}

async function reserveClockContentEnrichPublication(client, {
  contentKey,
  currentTask,
  leaseOwner,
  outcome,
}) {
  if (!currentTask) return null;
  const dispatchGeneration = Number(currentTask.dispatch_generation ?? 0) + 1;
  const reserved = await client.query(
    `UPDATE crawler.content_enrich_tasks
     SET status='running',dispatch_generation=$3,lease_owner=$4,
         lease_expires_at=clock_timestamp()+($5::bigint*interval '1 millisecond'),
         last_attempt_at=$6,updated_at=clock_timestamp()
     WHERE task_id=$1 AND content_key=$2 AND job_type='player-refresh'
     RETURNING task_id,dispatch_generation`,
    [
      currentTask.task_id,
      contentKey,
      dispatchGeneration,
      leaseOwner,
      clockContentEnrichReservationMs(),
      outcome.observed_at,
    ],
  );
  if (resultRowCount(reserved) !== 1) {
    throw new Error(`failed to reserve Clock Enrich Task: ${currentTask.task_id}`);
  }
  return {
    task_id: currentTask.task_id,
    content_key: contentKey,
    dispatch_generation: dispatchGeneration,
    lease_owner: leaseOwner,
    prior_status: ["leased", "running"].includes(currentTask.status)
      ? "queued"
      : currentTask.status,
    prior_last_attempt_at: currentTask.last_attempt_at ?? null,
  };
}

async function lockClockContentEnrichPublication(client, { contentKey, fence }) {
  if (!fence) {
    const unexpected = await client.query(
      `SELECT task.*
       FROM crawler.content_enrich_tasks task
       WHERE task.content_key=$1 AND task.job_type='player-refresh'
       FOR UPDATE`,
      [contentKey],
    );
    return resultRowCount(unexpected) === 0
      ? { owned: true, currentTask: null }
      : { owned: false, currentTask: null };
  }
  const locked = await client.query(
    `SELECT task.*
     FROM crawler.content_enrich_tasks task
     WHERE task.content_key=$1 AND task.task_id=$2 AND task.job_type='player-refresh'
     FOR UPDATE`,
    [contentKey, fence.task_id],
  );
  if (resultRowCount(locked) !== 1) return { owned: false, currentTask: null };
  const live = await client.query(
    `SELECT task.*
     FROM crawler.content_enrich_tasks task
     WHERE task.content_key=$1 AND task.task_id=$2 AND task.job_type='player-refresh'
       AND task.status='running' AND task.lease_owner=$3 AND task.dispatch_generation=$4
       AND task.lease_expires_at>clock_timestamp()`,
    [contentKey, fence.task_id, fence.lease_owner, fence.dispatch_generation],
  );
  return resultRowCount(live) === 1
    ? { owned: true, currentTask: live.rows[0] }
    : { owned: false, currentTask: null };
}

async function releaseClockContentEnrichReservationsInTransaction(client, preparedSampling) {
  const fences = [...(preparedSampling?.preparedCaptures?.values?.() ?? [])]
    .map((prepared) => prepared?.fence)
    .filter(Boolean);
  if (fences.length === 0) return 0;
  let released = 0;
  for (const fence of fences) {
    const result = await client.query(
      `UPDATE crawler.content_enrich_tasks
       SET status=$5,lease_owner=NULL,lease_expires_at=NULL,
           last_attempt_at=$6,updated_at=clock_timestamp()
       WHERE task_id=$1 AND job_type='player-refresh'
         AND status='running' AND lease_owner=$2 AND dispatch_generation=$3
         AND content_key=$4`,
      [
        fence.task_id,
        fence.lease_owner,
        fence.dispatch_generation,
        fence.content_key,
        fence.prior_status,
        fence.prior_last_attempt_at,
      ],
    );
    released += Number(result.rowCount ?? 0);
  }
  return released;
}

export async function queueRefreshTask(client, {
  contentKey,
  channelId,
  runId,
  observationId,
  jobType,
  error,
}) {
  await client.query(
    `INSERT INTO crawler.content_enrich_tasks (
       task_id,content_key,channel_id,job_type,status,priority,attempts,
       result_json,error_message,requested_by_run_id,requested_observation_id,
       next_retry_at,updated_at
     ) VALUES ($1,$2,$3,$4,'queued',10,0,'{}'::jsonb,$7,$5,$6,now(),now())
     ON CONFLICT (content_key,job_type) DO UPDATE
     SET status=CASE
           WHEN crawler.content_enrich_tasks.status IN ('done','terminal','skipped') THEN 'queued'
           WHEN crawler.content_enrich_tasks.status='dead_letter' THEN 'dead_letter'
           WHEN crawler.content_enrich_tasks.status IN ('leased','running')
             AND crawler.content_enrich_tasks.lease_expires_at>now()
           THEN crawler.content_enrich_tasks.status
           WHEN crawler.content_enrich_tasks.status='failed' THEN 'failed'
           ELSE 'queued'
         END,
         priority=LEAST(crawler.content_enrich_tasks.priority,10),
         attempts=CASE
           WHEN crawler.content_enrich_tasks.status IN ('done','terminal','skipped') THEN 0
           ELSE crawler.content_enrich_tasks.attempts END,
         error_message=EXCLUDED.error_message,
         requested_by_run_id=EXCLUDED.requested_by_run_id,
         requested_observation_id=EXCLUDED.requested_observation_id,
         next_retry_at=CASE
           WHEN crawler.content_enrich_tasks.status IN ('done','terminal','skipped') THEN now()
           ELSE crawler.content_enrich_tasks.next_retry_at END,
         lease_owner=CASE
           WHEN crawler.content_enrich_tasks.status IN ('leased','running')
             AND crawler.content_enrich_tasks.lease_expires_at>now()
           THEN crawler.content_enrich_tasks.lease_owner ELSE NULL END,
         lease_expires_at=CASE
           WHEN crawler.content_enrich_tasks.status IN ('leased','running')
             AND crawler.content_enrich_tasks.lease_expires_at>now()
           THEN crawler.content_enrich_tasks.lease_expires_at ELSE NULL END,
         updated_at=now()`,
    [
      refreshTaskId(contentKey, jobType),
      contentKey,
      channelId,
      jobType,
      runId,
      observationId,
      String(error?.message || error || "detail collection deferred").slice(0, 2000),
    ],
  );
}

async function persistIncrementalCandidate(client, {
  runId,
  channelId,
  entry,
  detail,
  classification,
  disposition,
  missingFields,
  contentKey = null,
  detailStatus,
  apiStatus,
  typeStatus,
  resultJson,
  errorMessage = null,
  observedAt,
  attempted = false,
  firstSeenLedgerStatus = "not_applicable",
  firstSeenLedgerObservationId = null,
}) {
  const persisted = await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,title,source_url,thumbnail_url,
       content_type,type_status,type_source,detail_status,api_status,missing_fields,
       content_key,disposition,next_attempt_at,result_json,error_message,attempts,
       finished_at,updated_at,first_seen_ledger_status,first_seen_ledger_observation_id
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::text[],$14,$15,$16,$17::jsonb,$18,
       $19::integer,CASE WHEN $15='deferred' THEN NULL ELSE $20::timestamptz END,now(),
       $21,$22::uuid
     )
     ON CONFLICT (run_id,source_content_id) DO UPDATE
     SET position=EXCLUDED.position,title=COALESCE(EXCLUDED.title,crawler.content_candidates.title),
         source_url=EXCLUDED.source_url,
         thumbnail_url=COALESCE(EXCLUDED.thumbnail_url,crawler.content_candidates.thumbnail_url),
         content_type=EXCLUDED.content_type,type_status=EXCLUDED.type_status,
         type_source=EXCLUDED.type_source,detail_status=EXCLUDED.detail_status,
         api_status=EXCLUDED.api_status,missing_fields=EXCLUDED.missing_fields,
         content_key=EXCLUDED.content_key,disposition=EXCLUDED.disposition,
         next_attempt_at=EXCLUDED.next_attempt_at,
         result_json=CASE
           WHEN crawler.content_candidates.disposition='deferred'
             AND EXCLUDED.disposition IN ('stored','terminal_excluded')
           THEN EXCLUDED.result_json || jsonb_build_object(
             'recovery',jsonb_build_object(
               'from_disposition',crawler.content_candidates.result_json->'disposition',
               'deferred_evidence',crawler.content_candidates.result_json - 'disposition',
               'deferred_error_message',crawler.content_candidates.error_message,
               'resolved_at',EXCLUDED.result_json#>>'{disposition,observed_at}'
             )
           )
           ELSE EXCLUDED.result_json
         END,
         error_message=EXCLUDED.error_message,
         attempts=crawler.content_candidates.attempts+EXCLUDED.attempts,
         finished_at=CASE WHEN EXCLUDED.disposition='deferred' THEN NULL ELSE EXCLUDED.finished_at END,
         first_seen_ledger_status=CASE
           WHEN crawler.content_candidates.first_seen_ledger_status IN ('pending','consumed')
             THEN crawler.content_candidates.first_seen_ledger_status
           ELSE EXCLUDED.first_seen_ledger_status END,
         first_seen_ledger_observation_id=CASE
           WHEN crawler.content_candidates.first_seen_ledger_status IN ('pending','consumed')
             THEN crawler.content_candidates.first_seen_ledger_observation_id
           ELSE EXCLUDED.first_seen_ledger_observation_id END,
         updated_at=now()
     RETURNING candidate_id`,
    [
      runId,
      channelId,
      entry.id,
      entry.position,
      detail?.title ?? entry.title ?? null,
      classification?.canonical_url
        ?? `https://www.youtube.com/watch?v=${encodeURIComponent(entry.id)}`,
      detail?.thumbnail_url ?? entry.thumbnail_url ?? null,
      classification?.authoritative === true ? classification.content_type : null,
      typeStatus,
      classification?.authoritative === true ? classification.source : null,
      detailStatus,
      apiStatus,
      missingFields,
      contentKey,
      disposition.kind,
      disposition.next_attempt_at,
      JSON.stringify(resultJson),
      errorMessage,
      attempted ? 1 : 0,
      observedAt,
      firstSeenLedgerStatus,
      firstSeenLedgerObservationId,
    ],
  );
  const candidateId = text(persisted.rows?.[0]?.candidate_id);
  if (!candidateId) throw new Error(`Incremental Candidate was not persisted: ${entry.id}`);
  return candidateId;
}

function collectionErrorEvidence(error) {
  if (!error) return null;
  return {
    name: text(error.name) ?? "Error",
    code: text(error.code),
    message: text(error.message) ?? String(error),
    ...(error.youtube_failure_evidence && typeof error.youtube_failure_evidence === "object"
      ? { youtube_failure_evidence: error.youtube_failure_evidence }
      : {}),
    ...(Array.isArray(error.errors)
      ? {
          causes: error.errors.map((cause) => ({
            name: text(cause?.name) ?? "Error",
            code: text(cause?.code),
            message: text(cause?.message) ?? String(cause),
          })),
        }
      : {}),
  };
}

function resolveFirstSeenContent({ entry, capture, observedAt, discoveryDeferred = null }) {
  const detail = capture?.detail ?? null;
  const facts = detailFacts(detail);
  const uploadFacts = uploadsPublishedFacts(entry);
  const classification = classifyVideoType(entry, detail);
  const storageAction = fullVideoStorageAction({
    candidate: {},
    classification,
    access: { access_status: facts?.access_status ?? "unknown" },
  });
  const terminalReason = unfinishedLiveReason({
    ...detail,
    is_upcoming: entry.is_upcoming === true || detail?.is_upcoming === true,
    is_live: entry.is_live === true || detail?.is_live === true,
  });
  const disposition = resolveVideoDisposition({
    storageAction,
    classification,
    access: {
      access_status: facts?.access_status ?? "unknown",
      access_status_source: facts?.access_status_source ?? null,
    },
    detail,
    error: capture?.error ?? null,
    observedAt,
    terminalReason,
    deferredReason: discoveryDeferred?.reason_code ?? null,
    priorDisposition: entry.disposition_recheck
      ? {
          kind: entry.disposition_recheck.prior_kind,
          reason_code: entry.disposition_recheck.prior_reason_code,
        }
      : null,
  });
  return { detail, facts, uploadFacts, classification, disposition };
}

function firstSeenCheckpointOutcome(resolution, capture, observedAt) {
  if (resolution.disposition.kind !== "stored") return null;
  const task = { task_id: "first-seen-checkpoint", attempts: 0, dispatch_generation: 0 };
  const completedAt = new Date(observedAt);
  const retryOptions = clockContentEnrichRetryOptions();
  const outcome = resolution.detail == null
    ? contentEnrichFailureOutcome(task, capture?.error ?? null, completedAt, retryOptions)
    : contentEnrichDetailOutcome(task, resolution.detail, completedAt, retryOptions);
  return ["retryable", "dead_letter"].includes(outcome.kind) ? outcome : null;
}

async function upsertFirstSeenContent(client, {
  channelId,
  runId,
  observationId,
  observedAt,
  entry,
  capture,
  discoveryDeferred = null,
  resolution = null,
}) {
  const resolved = resolution ?? resolveFirstSeenContent({
    entry,
    capture,
    observedAt,
    discoveryDeferred,
  });
  const { detail, facts, uploadFacts, classification, disposition } = resolved;
  const publicationSelection = selectPublicationEvidence(uploadFacts, facts);
  const publication = publicationSelection.evidence;
  const publicationConflict = publicationEvidenceConflictRecord(publicationSelection);
  if (disposition.kind !== "stored") {
    const terminalExcluded = disposition.kind === "terminal_excluded";
    const collectionFailed = capture?.error != null;
    const scanIncomplete = disposition.reason_code === "discovery_scan_incomplete";
    const missingFields = terminalExcluded
      ? []
      : scanIncomplete
        ? ["discovery_scan", "detail", "content_type"]
      : collectionFailed
        ? ["detail", "content_type"]
        : classification?.authoritative === true ? ["access_status"] : ["content_type"];
    const errorMessage = terminalExcluded
      ? null
      : scanIncomplete
        ? `Uploads discovery scan incomplete: ${discoveryDeferred.stop_reason ?? "incomplete"}`
      : collectionFailed
        ? text(capture.error?.message) ?? String(capture.error)
        : classification?.authoritative === true
        ? `content access ${facts?.access_status ?? "unknown"} is not currently storable`
        : "authoritative content type evidence is missing";
    const resultJson = {
      flat: entry,
      detail,
      classification,
      access: {
        access_status: facts?.access_status ?? "unknown",
        access_status_source: facts?.access_status_source ?? null,
      },
      disposition,
      ...(publicationConflict ? { publication_evidence_conflict: publicationConflict } : {}),
      ...(scanIncomplete ? { discovery_deferred: discoveryDeferred } : {}),
      extractor: {
        source: detail ? detailSource(detail) : null,
        client: detail ? text(detail.youtubejs_client) ?? "WEB" : null,
        version: text(detail?.extractor_version),
      },
      ...(collectionFailed ? { collection_error: collectionErrorEvidence(capture.error) } : {}),
    };
    await persistIncrementalCandidate(client, {
      runId,
      channelId,
      entry,
      detail,
      classification,
      disposition,
      missingFields,
      detailStatus: terminalExcluded ? "unavailable" : detail ? "done" : "failed",
      apiStatus: terminalExcluded ? "unavailable" : "not_needed",
      typeStatus: classification?.authoritative === true
        ? "resolved"
        : terminalExcluded ? "unavailable" : "unresolved",
      resultJson,
      errorMessage,
      observedAt,
      attempted: detail != null || capture?.error != null,
    });
    return {
      disposition,
      classification,
      facts,
      enrichOutcomeKind: null,
      publishedAt: publication.published_at,
      publishedAtStatus: publication.published_at_status,
      publishedAtPrecision: publication.published_at_precision,
      publishedAtSource: publication.published_at_source,
    };
  }
  const contentType = classification.content_type;
  const contentKey = `${channelId}:${contentType}:${entry.id}`;
  const preparedEnrich = await prepareClockContentEnrichOutcome(client, {
    contentKey,
    jobType: "player-refresh",
    detail,
    error: capture?.error ?? null,
  });
  const detailComplete = preparedEnrich.skipped === false
    && preparedEnrich.outcome?.detail != null;
  const candidateDetailComplete = hasCompletePublicVideoSurface(detail);
  const url = contentType === "short"
    ? `https://www.youtube.com/shorts/${encodeURIComponent(entry.id)}`
    : `https://www.youtube.com/watch?v=${encodeURIComponent(entry.id)}`;
  const descriptionStatus = facts?.description == null
    ? "unresolved"
    : facts.description === "" ? "empty" : "exact";
  const published = publication.published_at;
  const publishedPrecision = publication.published_at_precision;
  const publishedSource = publication.published_at_source;
  const isRecent = published == null
    ? true
    : new Date(published).getTime() >= new Date(observedAt).getTime() - (30 * 86400000);
  const stored = await client.query(
    `INSERT INTO crawler.contents (
       content_key,channel_id,run_id,content_type,content_type_source,
       source_content_id,position,title,url,thumbnail_url,
       description,description_status,description_source,hashtags,keywords,
       published_at,published_at_status,published_at_source,published_at_precision,
       is_recent,duration_seconds,duration_status,duration_source,
       view_count,view_count_text,view_count_status,view_count_source,
       like_count,like_count_status,like_count_source,
       comment_count,comment_count_status,comments_disabled,comment_count_source,
       comments_first_page,
       is_members_only,access_status,access_status_source,extractor_version,
       raw_json,first_seen_at,last_seen_at,last_enriched_at,
       playlist_last_seen_at,player_last_observed_at,next_last_observed_at,last_observation_id
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
       $11,$12,$13,$14::text[],$15::text[],$16,$17,$18,$19,
       $20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$44::jsonb,
       $35,$36,$37,$38,$39::jsonb,$40::timestamptz,$40::timestamptz,
       CASE WHEN $41::boolean THEN $40::timestamptz ELSE NULL END,
       $40::timestamptz,CASE WHEN $41::boolean THEN $40::timestamptz ELSE NULL END,
       CASE WHEN $41::boolean THEN $40::timestamptz ELSE NULL END,$42
     )
     ON CONFLICT (channel_id,source_content_id) DO UPDATE
     SET run_id=EXCLUDED.run_id,position=EXCLUDED.position,
         content_type=CASE
           WHEN $43::boolean THEN EXCLUDED.content_type
           ELSE crawler.contents.content_type
         END,
         content_type_source=CASE
           WHEN $43::boolean THEN EXCLUDED.content_type_source
           ELSE crawler.contents.content_type_source
         END,
         url=CASE
           WHEN $43::boolean THEN EXCLUDED.url
           ELSE COALESCE(crawler.contents.url,EXCLUDED.url)
         END,
         title=COALESCE(EXCLUDED.title,crawler.contents.title),
         thumbnail_url=COALESCE(EXCLUDED.thumbnail_url,crawler.contents.thumbnail_url),
         description=CASE
           WHEN NOT $41::boolean THEN crawler.contents.description
           WHEN EXCLUDED.description_status='exact' THEN EXCLUDED.description
           WHEN EXCLUDED.description_status='empty'
             AND NULLIF(btrim(COALESCE(crawler.contents.description,'')),'') IS NULL
             THEN EXCLUDED.description
           ELSE crawler.contents.description END,
         description_status=CASE
           WHEN NOT $41::boolean THEN crawler.contents.description_status
           WHEN EXCLUDED.description_status='exact' THEN 'exact'
           WHEN EXCLUDED.description_status='empty'
             AND NULLIF(btrim(COALESCE(crawler.contents.description,'')),'') IS NULL
             THEN 'empty'
           ELSE crawler.contents.description_status END,
         description_source=CASE
           WHEN NOT $41::boolean THEN crawler.contents.description_source
           WHEN EXCLUDED.description_status='exact' THEN EXCLUDED.description_source
           WHEN EXCLUDED.description_status='empty'
             AND NULLIF(btrim(COALESCE(crawler.contents.description,'')),'') IS NULL
             THEN EXCLUDED.description_source
           ELSE crawler.contents.description_source END,
         hashtags=CASE
           WHEN $41::boolean AND (
             COALESCE(cardinality(EXCLUDED.hashtags),0)>0
             OR COALESCE(cardinality(crawler.contents.hashtags),0)=0
           ) THEN EXCLUDED.hashtags ELSE crawler.contents.hashtags END,
         keywords=CASE
           WHEN $41::boolean AND (
             COALESCE(cardinality(EXCLUDED.keywords),0)>0
             OR COALESCE(cardinality(crawler.contents.keywords),0)=0
           ) THEN EXCLUDED.keywords ELSE crawler.contents.keywords END,
         published_at=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at ELSE crawler.contents.published_at END,
         published_at_status=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at_status ELSE crawler.contents.published_at_status END,
         published_at_source=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at_source ELSE crawler.contents.published_at_source END,
         published_at_precision=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at_precision ELSE crawler.contents.published_at_precision END,
         duration_seconds=COALESCE(EXCLUDED.duration_seconds,crawler.contents.duration_seconds),
         duration_status=CASE WHEN EXCLUDED.duration_seconds IS NOT NULL THEN 'exact' ELSE crawler.contents.duration_status END,
         duration_source=COALESCE(EXCLUDED.duration_source,crawler.contents.duration_source),
         view_count=COALESCE(EXCLUDED.view_count,crawler.contents.view_count),
         view_count_text=COALESCE(EXCLUDED.view_count_text,crawler.contents.view_count_text),
         view_count_status=CASE WHEN EXCLUDED.view_count IS NOT NULL THEN 'exact' ELSE crawler.contents.view_count_status END,
         view_count_source=COALESCE(EXCLUDED.view_count_source,crawler.contents.view_count_source),
         like_count=COALESCE(EXCLUDED.like_count,crawler.contents.like_count),
         like_count_status=CASE WHEN EXCLUDED.like_count IS NOT NULL THEN 'exact' ELSE crawler.contents.like_count_status END,
         like_count_source=COALESCE(EXCLUDED.like_count_source,crawler.contents.like_count_source),
         comment_count=CASE
           WHEN EXCLUDED.comments_disabled THEN 0
           ELSE COALESCE(EXCLUDED.comment_count,crawler.contents.comment_count) END,
         comment_count_status=CASE
           WHEN EXCLUDED.comments_disabled OR EXCLUDED.comment_count IS NOT NULL
             THEN EXCLUDED.comment_count_status
           WHEN crawler.contents.comments_disabled OR crawler.contents.comment_count IS NOT NULL
             THEN crawler.contents.comment_count_status
           ELSE EXCLUDED.comment_count_status END,
         comments_disabled=CASE
           WHEN EXCLUDED.comments_disabled OR EXCLUDED.comment_count IS NOT NULL
             THEN EXCLUDED.comments_disabled
           WHEN crawler.contents.comments_disabled OR crawler.contents.comment_count IS NOT NULL
             THEN crawler.contents.comments_disabled
           ELSE EXCLUDED.comments_disabled END,
         comment_count_source=CASE
           WHEN EXCLUDED.comments_disabled OR EXCLUDED.comment_count IS NOT NULL
             THEN EXCLUDED.comment_count_source
           WHEN crawler.contents.comments_disabled OR crawler.contents.comment_count IS NOT NULL
             THEN crawler.contents.comment_count_source
           ELSE EXCLUDED.comment_count_source END,
         comments_first_page=CASE
           WHEN COALESCE((crawler.contents.comments_first_page->>'returned_count')::integer,0)>0
             THEN crawler.contents.comments_first_page
           WHEN COALESCE((EXCLUDED.comments_first_page->>'returned_count')::integer,0)>0
             THEN EXCLUDED.comments_first_page
           ELSE COALESCE(crawler.contents.comments_first_page,EXCLUDED.comments_first_page)
         END,
         is_members_only=CASE
           WHEN NOT $41::boolean THEN crawler.contents.is_members_only
           WHEN EXCLUDED.access_status='unknown'
             AND COALESCE(crawler.contents.access_status,'unknown')<>'unknown'
             THEN crawler.contents.is_members_only
           ELSE EXCLUDED.is_members_only END,
         access_status=CASE
           WHEN NOT $41::boolean THEN crawler.contents.access_status
           WHEN EXCLUDED.access_status='unknown'
             AND COALESCE(crawler.contents.access_status,'unknown')<>'unknown'
             THEN crawler.contents.access_status
           ELSE EXCLUDED.access_status END,
         access_status_source=CASE
           WHEN NOT $41::boolean THEN crawler.contents.access_status_source
           WHEN EXCLUDED.access_status='unknown'
             AND COALESCE(crawler.contents.access_status,'unknown')<>'unknown'
             THEN crawler.contents.access_status_source
           ELSE EXCLUDED.access_status_source END,
         extractor_version=COALESCE(EXCLUDED.extractor_version,crawler.contents.extractor_version),
         raw_json=crawler.contents.raw_json || EXCLUDED.raw_json
           || ${CONTENT_UPSERT_PUBLICATION_CONFLICT},
         last_seen_at=GREATEST(crawler.contents.last_seen_at,EXCLUDED.last_seen_at),
         last_enriched_at=CASE WHEN $41::boolean THEN $40::timestamptz ELSE crawler.contents.last_enriched_at END,
         playlist_last_seen_at=$40::timestamptz,
         player_last_observed_at=CASE WHEN $41::boolean THEN $40::timestamptz ELSE crawler.contents.player_last_observed_at END,
         next_last_observed_at=CASE WHEN $41::boolean THEN $40::timestamptz ELSE crawler.contents.next_last_observed_at END,
         last_observation_id=$42
     RETURNING content_key`,
    [
      contentKey,
      channelId,
      runId,
      contentType,
      classification.source,
      entry.id,
      entry.position,
      detail?.title ?? entry.title ?? null,
      url,
      detail?.thumbnail_url ?? entry.thumbnail_url ?? null,
      facts?.description ?? null,
      descriptionStatus,
      facts?.description == null ? null : detailSource(detail),
      facts?.hashtags ?? [],
      facts?.keywords ?? [],
      published,
      publication.published_at_status,
      publishedSource,
      publishedPrecision,
      isRecent,
      facts?.duration_seconds ?? null,
      facts?.duration_seconds == null ? "unresolved" : "exact",
      facts?.duration_seconds == null ? null : facts.duration_source,
      facts?.view_count ?? null,
      facts?.view_count == null ? null : String(facts.view_count),
      facts?.view_count == null ? "unresolved" : "exact",
      facts?.view_count == null ? null : facts.view_count_source,
      facts?.like_count ?? null,
      facts?.like_count == null ? "unresolved" : "exact",
      facts?.like_count == null ? null : facts.like_count_source,
      facts?.comments_disabled ? 0 : facts?.comment_count ?? null,
      facts?.comments_disabled ? "disabled" : facts?.comment_count == null ? "unresolved" : "exact",
      facts?.comments_disabled ?? null,
      facts?.comments_disabled || facts?.comment_count != null ? facts.comment_count_source : null,
      facts?.access_status === "members_only",
      facts?.access_status ?? "unknown",
      facts ? facts.access_status_source : null,
      facts?.extractor_version ?? null,
      JSON.stringify({
        incremental: {
          observation_id: observationId,
          playlist_position: entry.position,
          detail_collected: facts != null,
        },
        ...(publicationConflict ? { publication_evidence_conflict: publicationConflict } : {}),
      }),
      observedAt,
      detailComplete,
      observationId,
      classification.authoritative === true && facts?.access_status === "public",
      facts?.comments_first_page == null ? null : JSON.stringify(facts.comments_first_page),
    ],
  );
  const storedContentKey = text(stored.rows?.[0]?.content_key) ?? contentKey;
  const candidateId = await persistIncrementalCandidate(client, {
    runId,
    channelId,
    entry,
    detail,
    classification,
    disposition,
    missingFields: candidateDetailComplete ? [] : ["detail"],
    contentKey: storedContentKey,
    detailStatus: candidateDetailComplete ? "done" : "failed",
    apiStatus: "not_needed",
    typeStatus: "resolved",
    resultJson: {
      flat: entry,
      detail,
      classification,
      access: {
        access_status: facts?.access_status ?? "unknown",
        access_status_source: facts?.access_status_source ?? null,
      },
      disposition,
      ...(publicationConflict ? { publication_evidence_conflict: publicationConflict } : {}),
      extractor: {
        source: detail ? detailSource(detail) : null,
        client: detail ? text(detail.youtubejs_client) ?? "WEB" : null,
        version: text(detail?.extractor_version),
      },
      content_key: storedContentKey,
    },
    errorMessage: candidateDetailComplete
      ? null
      : preparedEnrich.outcome?.error_message
        ?? "Content Enrich detail is missing the required public Video surface",
    observedAt,
    attempted: detail != null || capture?.error != null,
    firstSeenLedgerStatus: observationId == null ? "pending" : "consumed",
    firstSeenLedgerObservationId: observationId,
  });
  if (!preparedEnrich.skipped) {
    await persistClockContentEnrichOutcome(client, {
      ...preparedEnrich,
      contentKey: storedContentKey,
      channelId,
      runId,
      observationId,
      jobType: "player-refresh",
      observedAt,
    });
  }
  return {
    disposition,
    candidateId,
    contentKey: storedContentKey,
    contentType,
    classification,
    facts,
    enrichOutcomeKind: preparedEnrich.skipped ? null : preparedEnrich.outcome?.kind ?? null,
    publishedAt: published,
    publishedAtStatus: publication.published_at_status,
    publishedAtPrecision: publishedPrecision,
    publishedAtSource: publishedSource,
  };
}

async function checkpointFirstSeenEnrichFailures({
  plan,
  runId,
  candidateEntries,
  captures,
  withTransaction,
  transactionClient = null,
  observedAt,
}) {
  const checkpoints = [];
  const seenVideoIds = new Set();
  for (const entry of candidateEntries) {
    if (seenVideoIds.has(entry.id)) continue;
    seenVideoIds.add(entry.id);
    if (!captures.has(entry.id)) continue;
    const capture = captures.get(entry.id);
    const resolution = resolveFirstSeenContent({ entry, capture, observedAt });
    if (!firstSeenCheckpointOutcome(resolution, capture, observedAt)) continue;
    checkpoints.push({ entry, capture, resolution });
  }
  if (checkpoints.length === 0) return [];
  const checkpoint = async (client) => {
    const alreadyKnown = await knownVideoIds(
      client.query.bind(client),
      plan.channel_id,
      checkpoints.map(({ entry }) => entry.id),
    );
    const checkpointed = [];
    for (const checkpoint of checkpoints) {
      if (alreadyKnown.has(checkpoint.entry.id)) continue;
      const persisted = await upsertFirstSeenContent(client, {
        channelId: plan.channel_id,
        runId,
        observationId: null,
        observedAt,
        ...checkpoint,
      });
      if (!["retryable", "dead_letter"].includes(persisted.enrichOutcomeKind)) {
        throw new Error(`First-Seen failure checkpoint changed outcome: ${checkpoint.entry.id}`);
      }
      checkpointed.push({ entry: checkpoint.entry, ...persisted });
    }
    return checkpointed;
  };
  if (transactionClient) return checkpoint(transactionClient);
  return withTransaction(checkpoint);
}

async function loadPendingFirstSeenCheckpoints(query, { channelId }) {
  const pending = await query(
    `SELECT candidate.candidate_id,candidate.source_content_id AS video_id,
            candidate.position,candidate.title,candidate.thumbnail_url,
            candidate.result_json,
            content.content_key,content.content_type,
            content.published_at,content.published_at_status,
            content.published_at_precision,content.published_at_source
     FROM crawler.content_candidates candidate
     JOIN crawler.contents content
       ON content.content_key=candidate.content_key
      AND content.channel_id=candidate.channel_id
     JOIN crawler.channel_runs run
       ON run.run_id=candidate.run_id
      AND run.channel_id=candidate.channel_id
     WHERE candidate.channel_id=$1
       AND run.crawl_mode='incremental'
       AND candidate.disposition='stored'
       AND candidate.detail_status='failed'
       AND candidate.result_json #>> '{disposition,kind}'='stored'
       AND candidate.first_seen_ledger_status='pending'
     ORDER BY candidate.candidate_id`,
    [channelId],
  );
  return pending.rows.map((row) => {
    const evidence = row.result_json && typeof row.result_json === "object"
      ? row.result_json
      : {};
    const flat = evidence.flat && typeof evidence.flat === "object" ? evidence.flat : {};
    const detail = evidence.detail && typeof evidence.detail === "object" ? evidence.detail : null;
    const disposition = evidence.disposition;
    const candidateId = text(row.candidate_id);
    const videoId = text(row.video_id);
    const contentKey = text(row.content_key);
    const contentType = text(row.content_type);
    if (!candidateId || !videoId || !contentKey || !contentType || disposition?.kind !== "stored") {
      throw new Error(`Invalid pending First-Seen checkpoint: ${videoId ?? contentKey ?? "unknown"}`);
    }
    return {
      candidateId,
      entry: {
        ...flat,
        id: videoId,
        position: Number(row.position),
        title: text(flat.title) ?? text(row.title),
        thumbnail_url: text(flat.thumbnail_url) ?? text(row.thumbnail_url),
      },
      disposition,
      classification: evidence.classification ?? null,
      facts: detailFacts(detail),
      contentKey,
      contentType,
      publishedAt: row.published_at == null
        ? null
        : new Date(row.published_at).toISOString(),
      publishedAtStatus: text(row.published_at_status) ?? "unresolved",
      publishedAtPrecision: text(row.published_at_precision) ?? "unknown",
      publishedAtSource: text(row.published_at_source),
    };
  });
}

async function claimPendingFirstSeenCheckpoints(transactionClient, {
  channelId,
  observationId,
  checkpoints,
}) {
  const candidateIds = stringList(checkpoints.map((checkpoint) => checkpoint.candidateId));
  if (candidateIds.length === 0) return [];
  const claimed = await transactionClient.query(
    `UPDATE crawler.content_candidates candidate
     SET first_seen_ledger_status='consumed',
         first_seen_ledger_observation_id=$3::uuid,
         updated_at=now()
     WHERE candidate.channel_id=$1
       AND candidate.candidate_id=ANY($2::bigint[])
       AND candidate.first_seen_ledger_status='pending'
     RETURNING candidate.candidate_id::text AS candidate_id`,
    [channelId, candidateIds, observationId],
  );
  const claimedIds = new Set(claimed.rows.map((row) => text(row.candidate_id)).filter(Boolean));
  return checkpoints.filter((checkpoint) => claimedIds.has(checkpoint.candidateId));
}

async function applyDiscovery({
  plan,
  runId,
  scan,
  candidateEntries,
  captures,
  transactionClient,
  observationId,
  observedAt,
  pendingDeferredVideoIds = [],
  checkpointedFirstSeen = [],
}) {
  const claimedFirstSeen = scan.complete === true
    ? await claimPendingFirstSeenCheckpoints(transactionClient, {
        channelId: plan.channel_id,
        observationId,
        checkpoints: checkpointedFirstSeen,
      })
    : [];
  const discoveryDeferred = scan.complete === true
    ? null
    : {
        reason_code: "discovery_scan_incomplete",
        complete: false,
        playlist_id: text(scan.playlist_id),
        pages: Number(scan.pages ?? 0),
        item_count: Number(scan.item_count ?? scan.entries.length),
        first_page_item_count: Number(scan.first_page_item_count ?? 0),
        catch_up_item_count: Number(scan.catch_up_item_count ?? 0),
        anchor_matched: scan.anchor_matched === true,
        stop_reason: text(scan.stop_reason) ?? "incomplete",
        terminal_reason: text(scan.terminal_reason),
        parse_gap_count: Number(scan.parse_gap_count ?? 0),
      };
  const scanInput = scan.entries.map((entry) => {
    const publication = uploadsPublishedFacts(entry)
      ?? normalizePublicationEvidence();
    return {
      video_id: entry.id,
      position: entry.position,
      ...publication,
    };
  });
  if (scanInput.length > 0) {
    await transactionClient.query(
           `WITH input AS (
             SELECT * FROM jsonb_to_recordset($3::jsonb)
               AS item(
                 video_id text,
                 position integer,
                 published_at timestamptz,
                 published_at_status text,
                 published_at_precision text,
                 published_at_source text
               )
           )
           UPDATE crawler.contents content
           SET playlist_last_seen_at=$2,last_seen_at=GREATEST(content.last_seen_at,$2::timestamptz),
               position=input.position,
               published_at=CASE WHEN ${SCAN_PUBLICATION_WINS}
                 THEN input.published_at ELSE content.published_at END,
               published_at_status=CASE WHEN ${SCAN_PUBLICATION_WINS}
                 THEN input.published_at_status ELSE content.published_at_status END,
               published_at_source=CASE WHEN ${SCAN_PUBLICATION_WINS}
                 THEN input.published_at_source ELSE content.published_at_source END,
               published_at_precision=CASE WHEN ${SCAN_PUBLICATION_WINS}
                 THEN input.published_at_precision ELSE content.published_at_precision END,
               raw_json=content.raw_json || ${SCAN_PUBLICATION_CONFLICT},
               last_observation_id=$4
           FROM input
           WHERE content.channel_id=$1 AND content.source_content_id=input.video_id`,
          [plan.channel_id, observedAt, JSON.stringify(scanInput), observationId],
    );
  }
  const checkpointedContentKeys = stringList(
    claimedFirstSeen.map((current) => current.contentKey),
  );
  if (checkpointedContentKeys.length > 0) {
    const linked = await transactionClient.query(
      `UPDATE crawler.contents content
       SET last_observation_id=$3::uuid,
           raw_json=COALESCE(content.raw_json,'{}'::jsonb)
             || jsonb_build_object(
                  'incremental',
                  COALESCE(content.raw_json->'incremental','{}'::jsonb)
                    || jsonb_build_object('observation_id',($3::uuid)::text)
                )
       WHERE content.channel_id=$1
         AND content.content_key=ANY($2::text[])`,
      [plan.channel_id, checkpointedContentKeys, observationId],
    );
    if (resultRowCount(linked) !== checkpointedContentKeys.length) {
      throw new Error("Pending First-Seen checkpoint Content changed before Observation commit");
    }
  }
  const candidateIds = candidateEntries.map((entry) => entry.id);
  const alreadyKnown = await knownVideoIds(
    transactionClient.query.bind(transactionClient),
    plan.channel_id,
    candidateIds,
  );
  const firstSeenEntries = candidateEntries.filter((entry) => !alreadyKnown.has(entry.id));
  const firstSeen = [];
  const activityEvidence = [];
  const dispositions = [];
  const recheckDispositions = [];
  const unresolvedVideoIds = [...new Set(pendingDeferredVideoIds)];
  const recheckDeferredVideoIds = [];
  let detailSuccessCount = 0;
  let detailFailureCount = 0;
  for (const current of claimedFirstSeen) {
    const entry = current.entry;
    const dispositionSummary = videoDispositionSummary(entry.id, current.disposition);
    if (entry.disposition_recheck) recheckDispositions.push(dispositionSummary);
    else dispositions.push(dispositionSummary);
    if (current.facts) detailSuccessCount += 1;
    else detailFailureCount += 1;
    firstSeen.push({
      video_id: entry.id,
      position: entry.position,
      content_type: current.contentType,
      published_at: current.publishedAt,
      published_at_status: current.publishedAtStatus,
      published_at_precision: current.publishedAtPrecision,
      published_at_source: current.publishedAtSource,
    });
    activityEvidence.push(videoActivityEvidence(
      entry.id,
      current.contentType ?? current.classification?.content_type ?? entry.content_type,
      {
        published_at: current.publishedAt,
        published_at_status: current.publishedAtStatus,
        published_at_precision: current.publishedAtPrecision,
        published_at_source: current.publishedAtSource,
      },
      current.facts,
    ));
  }
  for (const entry of firstSeenEntries) {
    const capture = captures.get(entry.id) ?? { detail: null, error: null };
    const detailAttempted = captures.has(entry.id);
    const current = await upsertFirstSeenContent(transactionClient, {
      channelId: plan.channel_id,
      runId,
      observationId,
      observedAt,
      entry,
      capture,
      discoveryDeferred,
    });
    const dispositionSummary = videoDispositionSummary(entry.id, current.disposition);
    if (entry.disposition_recheck) recheckDispositions.push(dispositionSummary);
    else dispositions.push(dispositionSummary);
    activityEvidence.push(videoActivityEvidence(
      entry.id,
      current.contentType ?? current.classification?.content_type ?? entry.content_type,
      {
        published_at: current.publishedAt,
        published_at_status: current.publishedAtStatus,
        published_at_precision: current.publishedAtPrecision,
        published_at_source: current.publishedAtSource,
      },
      current.facts,
    ));
    if (current.disposition.kind === "deferred") {
      if (entry.disposition_recheck) recheckDeferredVideoIds.push(entry.id);
      else unresolvedVideoIds.push(entry.id);
      if (capture.detail) detailSuccessCount += 1;
      else if (detailAttempted) detailFailureCount += 1;
      continue;
    }
    if (current.disposition.kind === "terminal_excluded") {
      if (capture.detail) detailSuccessCount += 1;
      else if (detailAttempted) detailFailureCount += 1;
      continue;
    }
    if (current.facts) detailSuccessCount += 1;
    else detailFailureCount += 1;
    firstSeen.push({
      video_id: entry.id,
      position: entry.position,
      content_type: current.contentType,
      published_at: current.publishedAt,
      published_at_status: current.publishedAtStatus,
      published_at_precision: current.publishedAtPrecision,
      published_at_source: current.publishedAtSource,
    });
  }
  const discoveredVideoIds = [...new Set(
    [...claimedFirstSeen.map((current) => current.entry), ...firstSeenEntries]
      .filter((entry) => !entry.disposition_recheck)
      .map((entry) => entry.id),
  )];
  const dispositionCounts = new Map();
  for (const item of dispositions) {
    dispositionCounts.set(item.video_id, (dispositionCounts.get(item.video_id) ?? 0) + 1);
  }
  const silentDropVideoIds = discoveredVideoIds.filter(
    (videoId) => !dispositionCounts.has(videoId),
  );
  const duplicateDispositionVideoIds = discoveredVideoIds.filter(
    (videoId) => (dispositionCounts.get(videoId) ?? 0) > 1,
  );
  if (silentDropVideoIds.length > 0 || duplicateDispositionVideoIds.length > 0) {
    throw new Error(
      `Video disposition ledger invariant failed: ${silentDropVideoIds.length} missing, ${duplicateDispositionVideoIds.length} duplicated`,
    );
  }
  const persistedBlockingDeferredVideoIds = await outstandingDeferredVideoIds(
    transactionClient.query.bind(transactionClient),
    plan.channel_id,
  );
  const blockingDeferredVideoIds = [...new Set([
    ...persistedBlockingDeferredVideoIds,
    ...unresolvedVideoIds,
    ...recheckDeferredVideoIds,
  ])];
  const payload = {
    pages: Number(scan.pages ?? 0),
    items: Number(scan.item_count ?? scan.entries.length),
    anchor_matched: scan.anchor_matched === true,
    stop_reason: scan.stop_reason,
    parse_gap_count: Number(scan.parse_gap_count ?? 0),
    first_seen: firstSeen,
    first_seen_count: firstSeen.length,
    discovered_count: discoveredVideoIds.length,
    silent_drop_count: silentDropVideoIds.length,
    silent_drop_video_ids: silentDropVideoIds,
    dispositions,
    recheck_dispositions: recheckDispositions,
    stored_count: dispositions.filter((item) => item.kind === "stored").length,
    deferred_count: dispositions.filter((item) => item.kind === "deferred").length,
    terminal_excluded_count: dispositions.filter((item) => item.kind === "terminal_excluded").length,
    unresolved_video_ids: unresolvedVideoIds,
    unresolved_count: unresolvedVideoIds.length,
    recheck_deferred_video_ids: recheckDeferredVideoIds,
    recheck_deferred_count: recheckDeferredVideoIds.length,
    pending_deferred_video_ids: [...new Set(pendingDeferredVideoIds)],
    pending_deferred_count: new Set(pendingDeferredVideoIds).size,
    blocking_deferred_video_ids: blockingDeferredVideoIds,
    recheck_stored_count: recheckDispositions.filter((item) => item.kind === "stored").length,
    recheck_terminal_excluded_count: recheckDispositions
      .filter((item) => item.kind === "terminal_excluded").length,
    detail_success_count: detailSuccessCount,
    detail_failure_count: detailFailureCount,
    ...(scan.gap_abandonment ? { gap_abandonment: scan.gap_abandonment } : {}),
  };
  return {
    outcome: scan.complete
      && blockingDeferredVideoIds.length === 0
      ? "complete"
      : "partial",
    payload,
    summary: {
      pages: payload.pages,
      items: payload.items,
      anchor_matched: payload.anchor_matched,
      stop_reason: payload.stop_reason,
      parse_gap_count: payload.parse_gap_count,
      first_seen_count: firstSeen.length,
      discovered_count: payload.discovered_count,
      silent_drop_count: payload.silent_drop_count,
      stored_count: payload.stored_count,
      deferred_count: payload.deferred_count,
      terminal_excluded_count: payload.terminal_excluded_count,
      detail_success_count: detailSuccessCount,
      detail_failure_count: detailFailureCount,
      unresolved_count: unresolvedVideoIds.length,
      recheck_deferred_count: recheckDeferredVideoIds.length,
      pending_deferred_count: payload.pending_deferred_count,
      blocking_deferred_count: blockingDeferredVideoIds.length,
      recheck_stored_count: payload.recheck_stored_count,
      recheck_terminal_excluded_count: payload.recheck_terminal_excluded_count,
      ...(scan.gap_abandonment ? {
        gap_abandonment: {
          policy_version: scan.gap_abandonment.policy_version,
          source_stop_reason: scan.gap_abandonment.source_stop_reason,
          scanned_item_count: scan.gap_abandonment.scanned_item_count,
          first_page_item_count: scan.gap_abandonment.first_page_item_count,
          catch_up_item_count: scan.gap_abandonment.catch_up_item_count,
          catch_up_item_limit: scan.gap_abandonment.catch_up_item_limit,
          selected_item_count: scan.gap_abandonment.selected_item_count,
        },
      } : {}),
    },
    firstSeen,
    claimedFirstSeen,
    activityEvidence,
  };
}

export async function applyIncrementalVideoDetail(client, {
  row,
  detail,
  observedAt,
  observationId = null,
  collectNext = false,
  changeAlpha = 0.4,
  detailMetadataKey = "incremental_detail",
  allowStaticRepair = true,
}) {
  if (!detail) throw new TypeError("detail is required");
  const facts = detailFacts(detail);
  const classification = resolveYoutubeContentType({
    videoId: row.source_content_id,
    detail,
  });
  const storageAction = fullVideoStorageAction({
    candidate: {
      known_content_key: row.content_key,
      known_content_type: row.content_type,
      known_content_type_source: row.content_type_source,
    },
    classification,
    access: { access_status: facts.access_status },
  });
  const previousView = integer(row.view_count ?? row.view_count_text);
  const viewDelta = previousView != null && facts.view_count != null
    ? facts.view_count - previousView
    : null;
  const previousLike = integer(row.like_count);
  const previousComment = integer(row.comment_count);
  const engagementChanged = (previousLike != null && facts.like_count != null
      && previousLike !== facts.like_count)
    || (previousComment != null && facts.comment_count != null
      && previousComment !== facts.comment_count);
  const changeProbability = nextVideoChangeProbability(row, facts, changeAlpha);
  const commentsObserved = facts.comments_disabled === true || facts.comment_count != null;
  const storageFacts = recentStorageFacts(row, facts, { allowStaticRepair });
  const storedPublication = normalizePublicationEvidence(row.stored_publication ?? row);
  const publicationSelection = allowStaticRepair || storedPublication.published_at == null
    ? selectPublicationEvidence(storedPublication, facts)
    : {
        evidence: storedPublication,
        selected: "current",
        reason_code: "immutable_publication_retained",
      };
  const publication = publicationSelection.evidence;
  const publicationConflict = publicationEvidenceConflictRecord(publicationSelection);
  const isRecent = publication.published_at == null
    ? null
    : new Date(publication.published_at).getTime() >= new Date(observedAt).getTime() - (30 * 86400000);
  await client.query(
    `UPDATE crawler.contents
     SET content_type=CASE WHEN $36::text IS NULL THEN content_type ELSE $36 END,
         content_type_source=CASE WHEN $36::text IS NULL THEN content_type_source ELSE $37 END,
         url=CASE
           WHEN $43::boolean THEN COALESCE($38,url)
           WHEN NULLIF(btrim(COALESCE(url,'')),'') IS NULL THEN COALESCE($38,url)
           ELSE url END,
         title=COALESCE($10,title),
         thumbnail_url=COALESCE($11,thumbnail_url),
         description=CASE
           WHEN $13::boolean AND (
             NULLIF(btrim(COALESCE($12::text,'')),'') IS NOT NULL
             OR NULLIF(btrim(COALESCE(description,'')),'') IS NULL
           ) THEN $12 ELSE description END,
         description_status=CASE
           WHEN NOT $13::boolean THEN description_status
           WHEN NULLIF(btrim(COALESCE($12::text,'')),'') IS NOT NULL THEN 'exact'
           WHEN NULLIF(btrim(COALESCE(description,'')),'') IS NULL THEN 'empty'
           ELSE description_status END,
         description_source=CASE
           WHEN $13::boolean AND (
             NULLIF(btrim(COALESCE($12::text,'')),'') IS NOT NULL
             OR NULLIF(btrim(COALESCE(description,'')),'') IS NULL
           ) THEN $14 ELSE description_source END,
         hashtags=CASE
           WHEN $16::boolean AND (
             COALESCE(cardinality($15::text[]),0)>0 OR COALESCE(cardinality(hashtags),0)=0
           ) THEN $15::text[] ELSE hashtags END,
         keywords=CASE
           WHEN $18::boolean AND (
             COALESCE(cardinality($17::text[]),0)>0 OR COALESCE(cardinality(keywords),0)=0
           ) THEN $17::text[] ELSE keywords END,
         published_at=CASE
           WHEN $43::boolean THEN $19::timestamptz
           ELSE COALESCE(published_at,$19::timestamptz) END,
         published_at_status=CASE
           WHEN $43::boolean THEN $41::text
           WHEN published_at IS NULL AND $19::timestamptz IS NOT NULL THEN $41
           ELSE published_at_status END,
         published_at_source=CASE
           WHEN $43::boolean THEN $20::text
           WHEN published_at IS NULL AND $19::timestamptz IS NOT NULL THEN $20
           ELSE published_at_source END,
         published_at_precision=CASE
           WHEN $43::boolean THEN $21::text
           WHEN published_at IS NULL AND $19::timestamptz IS NOT NULL THEN $21
           ELSE published_at_precision END,
         is_recent=COALESCE($22::boolean,is_recent),
         duration_seconds=COALESCE($23::integer,duration_seconds),
         duration_status=CASE WHEN $23::integer IS NULL THEN duration_status ELSE 'exact' END,
         duration_source=CASE WHEN $23::integer IS NULL THEN duration_source ELSE $24 END,
         view_count=COALESCE($3,view_count),
         view_count_text=CASE WHEN $3::bigint IS NULL THEN view_count_text ELSE $3::text END,
         view_count_status=CASE WHEN $3::bigint IS NULL THEN view_count_status ELSE 'exact' END,
         view_count_source=CASE WHEN $3::bigint IS NULL THEN view_count_source ELSE $25 END,
         like_count=COALESCE($4,like_count),
         like_count_status=CASE WHEN $4::bigint IS NULL THEN like_count_status ELSE 'exact' END,
         like_count_source=CASE WHEN $4::bigint IS NULL THEN like_count_source ELSE $26 END,
         comment_count=CASE
           WHEN $35::boolean AND $6::boolean THEN 0
           WHEN $5::bigint IS NOT NULL THEN $5 ELSE comment_count END,
         comment_count_status=CASE
           WHEN $35::boolean AND $6::boolean THEN 'disabled'
           WHEN $5::bigint IS NULL THEN comment_count_status ELSE 'exact' END,
         comments_disabled=CASE WHEN $35::boolean THEN $6 ELSE comments_disabled END,
         comment_count_source=CASE
           WHEN $35::boolean THEN $27
           ELSE comment_count_source END,
         comments_first_page=CASE
           WHEN COALESCE((comments_first_page->>'returned_count')::integer,0)>0
             THEN comments_first_page
           WHEN COALESCE(($39::jsonb->>'returned_count')::integer,0)>0
             THEN $39::jsonb
           ELSE COALESCE(comments_first_page,$39::jsonb)
         END,
         is_members_only=CASE
           WHEN $28 IN ('unknown','login_required') THEN is_members_only ELSE $28='members_only' END,
         access_status=CASE WHEN $28 IN ('unknown','login_required') THEN access_status ELSE $28 END,
         access_status_source=CASE WHEN $28 IN ('unknown','login_required') THEN access_status_source ELSE $29 END,
         live_scheduled_at=COALESCE($31::timestamptz,live_scheduled_at),
         live_started_at=COALESCE($32::timestamptz,live_started_at),
         live_ended_at=COALESCE($33::timestamptz,live_ended_at),
         extractor_version=COALESCE($30,extractor_version),
         raw_json=raw_json || jsonb_build_object(
           $40::text,jsonb_strip_nulls(jsonb_build_object(
             'observation_id',$8::uuid::text,
             'detail_collected',true,
             'source',$34::text,
             'publication_evidence_conflict',$42::jsonb
           ))
         ),
         player_last_observed_at=$2,
         next_last_observed_at=CASE WHEN $7::boolean THEN $2 ELSE next_last_observed_at END,
         last_observation_id=COALESCE($8::uuid,last_observation_id),last_enriched_at=$2,
         video_change_probability=COALESCE($9::double precision,video_change_probability)
     WHERE content_key=$1
       AND (player_last_observed_at IS NULL OR player_last_observed_at<=$2::timestamptz)`,
    [
      row.content_key,
      observedAt,
      facts.view_count,
      facts.like_count,
      facts.comment_count,
      facts.comments_disabled,
      collectNext,
      observationId,
      changeProbability,
      storageFacts.title,
      storageFacts.thumbnail_url,
      storageFacts.description,
      storageFacts.description_observed,
      storageFacts.description_source,
      storageFacts.hashtags,
      storageFacts.hashtags_observed,
      storageFacts.keywords,
      storageFacts.keywords_observed,
      publication.published_at,
      publication.published_at_source,
      publication.published_at_precision,
      isRecent,
      storageFacts.duration_seconds,
      storageFacts.duration_source,
      facts.view_count_source,
      facts.like_count_source,
      facts.comment_count_source,
      facts.access_status,
      facts.access_status_source,
      facts.extractor_version,
      storageFacts.live_scheduled_at,
      storageFacts.live_started_at,
      storageFacts.live_ended_at,
      detailSource(detail),
      commentsObserved,
      allowStaticRepair && storageAction.kind === "upsert" ? storageAction.content_type : null,
      allowStaticRepair && storageAction.kind === "upsert" ? storageAction.type_source : null,
      storageAction.kind === "upsert" ? classification.canonical_url : null,
      facts.comments_first_page == null ? null : JSON.stringify(facts.comments_first_page),
      detailMetadataKey,
      publication.published_at_status,
      publicationConflict == null ? null : JSON.stringify(publicationConflict),
      allowStaticRepair,
    ],
  );
  return {
    success: true,
    viewDelta,
    engagementChanged,
    changeProbability,
    accessStatus: facts.access_status,
    activityEvidence: videoActivityEvidence(
      row.source_content_id,
      storageAction.content_type ?? row.content_type,
      publication,
      facts,
    ),
  };
}

async function loadClockRecentSamplingRows(client, {
  channelId,
  planDay,
  recentWindowDays,
  clockOwnsPlayerRefresh,
  scanEntries,
}) {
  const observedUploads = scanEntries
    .map((entry) => ({
      video_id: text(entry?.id),
      published_at: uploadsPublishedFacts(entry)?.published_at ?? null,
    }))
    .filter((entry) => entry.video_id && entry.published_at);
  const recentRows = await client.query(
    `WITH observed_uploads AS (
       SELECT item.video_id,max(item.published_at) AS published_at
       FROM jsonb_to_recordset($5::jsonb)
         AS item(video_id text,published_at timestamptz)
       GROUP BY item.video_id
     ), candidate AS (
       SELECT content.*,
              COALESCE(content.published_at,observed.published_at) AS sampling_published_at,
              EXISTS (
                SELECT 1 FROM crawler.content_enrich_tasks task
                WHERE task.content_key=content.content_key
                  AND task.job_type IN ('player-refresh','next-refresh')
                  AND (task.job_type='next-refresh' OR $4::boolean)
                  AND (
                    (task.status IN ('queued','failed')
                      AND COALESCE(task.next_retry_at,now())<=now())
                    OR (task.status IN ('leased','running')
                      AND task.lease_expires_at<=now())
                    OR (task.status='terminal'
                      AND task.next_retry_at IS NOT NULL
                      AND task.next_retry_at<=now())
                  )
              ) AS enrich_pending,
              EXISTS (
                SELECT 1 FROM crawler.content_enrich_tasks task
                WHERE task.content_key=content.content_key
                  AND task.job_type='player-refresh'
                  AND (
                    task.status IN ('queued','failed','leased','running')
                    OR (task.status='terminal' AND task.next_retry_at IS NOT NULL)
                  )
              ) AS player_enrich_open,
              EXISTS (
                SELECT 1 FROM crawler.content_enrich_tasks task
                WHERE task.content_key=content.content_key
                  AND task.job_type='player-refresh'
                  AND task.status IN ('leased','running')
                  AND task.lease_expires_at>now()
              ) AS player_enrich_leased,
              EXISTS (
                SELECT 1 FROM crawler.content_enrich_tasks task
                WHERE task.content_key=content.content_key
                  AND task.job_type='player-refresh'
                  AND task.status='terminal'
                  AND task.next_retry_at>now()
              ) AS player_enrich_terminal_waiting,
              EXISTS (
                SELECT 1 FROM crawler.content_enrich_tasks task
                WHERE task.content_key=content.content_key
                  AND task.job_type='player-refresh'
                  AND task.status IN ('queued','failed')
                  AND task.next_retry_at>now()
              ) AS player_enrich_retry_waiting
       FROM crawler.contents content
       LEFT JOIN observed_uploads observed
         ON observed.video_id=content.source_content_id
       WHERE content.channel_id=$1
         AND content.content_type IN ('video','short','live')
     )
     SELECT candidate.*
     FROM candidate
     WHERE NOT candidate.player_enrich_leased
       AND NOT candidate.player_enrich_terminal_waiting
       AND NOT candidate.player_enrich_retry_waiting
       AND ($4::boolean OR NOT candidate.player_enrich_open)
       AND (
         candidate.enrich_pending
         OR candidate.sampling_published_at>=($2::date - ($3::int * interval '1 day'))
       )
     ORDER BY candidate.sampling_published_at DESC NULLS LAST,candidate.content_key`,
    [
      channelId,
      planDay,
      recentWindowDays,
      clockOwnsPlayerRefresh,
      JSON.stringify(observedUploads),
    ],
  );
  return recentRows.rows.map((row) => ({
    ...row,
    stored_publication: {
      published_at: row.published_at,
      published_at_status: row.published_at_status,
      published_at_precision: row.published_at_precision,
      published_at_source: row.published_at_source,
    },
    published_at: row.sampling_published_at ?? row.published_at,
  }));
}

function checkpointItemLeaseMs() {
  const value = Number(process.env.INCREMENTAL_YOUTUBEJS_ITEM_LEASE_MS || 5 * 60_000);
  if (!Number.isSafeInteger(value) || value < 30_000) return 5 * 60_000;
  return Math.min(value, 30 * 60_000);
}

function checkpointItemHeartbeatMs(leaseMs) {
  const normalizedLeaseMs = Number.isSafeInteger(Number(leaseMs))
    ? Number(leaseMs)
    : checkpointItemLeaseMs();
  const fallback = Math.max(1_000, Math.min(60_000, Math.floor(normalizedLeaseMs / 3)));
  const value = Number(process.env.INCREMENTAL_YOUTUBEJS_ITEM_HEARTBEAT_MS || fallback);
  if (!Number.isSafeInteger(value) || value < 1_000 || value >= normalizedLeaseMs) return fallback;
  return value;
}

function jsonValue(value, fallback) {
  if (value == null) return fallback;
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch (error) {
    throw new TypeError("checkpoint row contains invalid JSON", { cause: error });
  }
}

function jsonCheckpointValue(value) {
  if (value == null) return null;
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new TypeError("checkpoint value is not JSON serializable");
  return JSON.parse(serialized);
}

function checkpointLog(event, fields = {}) {
  console.log(JSON.stringify({
    event: `incremental_youtubejs_video.${event}`,
    ...fields,
  }));
}

function checkpointTaskSnapshot(task) {
  if (!task) return null;
  return {
    task_id: text(task.task_id),
    status: text(task.status),
    dispatch_generation: Number(task.dispatch_generation ?? 0),
    attempts: Number(task.attempts ?? 0),
    last_attempt_at: task.last_attempt_at ?? null,
  };
}

async function lockClockContentEnrichTask(client, contentKey) {
  const locked = await client.query(
    `SELECT task.*,
            COALESCE(
              task.status IN ('leased','running')
                AND task.lease_expires_at>clock_timestamp(),
              false
            ) AS lease_live,
            COALESCE(
              task.next_retry_at<=clock_timestamp(),
              task.status IN ('queued','failed')
            ) AS retry_due
     FROM crawler.content_enrich_tasks task
     WHERE task.content_key=$1 AND task.job_type='player-refresh'
     FOR UPDATE`,
    [contentKey],
  );
  const currentTask = locked.rows[0] ?? null;
  const skipped = currentTask?.lease_live === true
    || (["queued", "failed"].includes(currentTask?.status) && currentTask.retry_due !== true)
    || (currentTask?.status === "terminal" && currentTask.retry_due !== true);
  return { currentTask, skipped };
}

async function loadRunCycle(client, { plan, runId, lock = false }) {
  const result = await client.query(
    `SELECT run_id,channel_id,plan_id,crawl_mode,task_mask,result_json
     FROM crawler.channel_runs
     WHERE run_id=$1${lock ? " FOR UPDATE" : ""}`,
    [runId],
  );
  const run = result.rows[0];
  if (!run) throw new Error(`incremental Run not found: ${runId}`);
  if (run.crawl_mode !== "incremental"
      || run.channel_id !== plan.channel_id
      || String(run.plan_id) !== String(plan.plan_id)
      || jsonValue(run.task_mask, {})?.video !== true) {
    throw new Error(`incremental Video checkpoint Run identity mismatch: ${runId}`);
  }
  const resultJson = jsonValue(run.result_json, {});
  for (const [operationId, marker] of Object.entries(resultJson.controlled_recoveries ?? {})) {
    for (const [field, expected] of [
      ["operation_id", operationId],
      ["run_id", runId],
      ["channel_id", plan.channel_id],
      ["plan_id", plan.plan_id],
      ["job_id", plan.job_id],
    ]) {
      if (String(marker?.[field] ?? "") !== String(expected)) {
        throw new Error(`controlled recovery marker has conflicting ${field}: ${operationId}`);
      }
    }
  }
  return {
    run,
    cycleKey: incrementalYoutubeJsVideoCycleKey(resultJson),
  };
}

function normalizeCheckpointBatch(row, items) {
  return {
    ...row,
    scan_json: jsonValue(row.scan_json, {}),
    anchors_json: jsonValue(row.anchors_json, []),
    discovery_entries_json: jsonValue(row.discovery_entries_json, []),
    pending_deferred_video_ids: jsonValue(row.pending_deferred_video_ids, []),
    sampling_plan_json: jsonValue(row.sampling_plan_json, {}),
    sampling_config_json: jsonValue(row.sampling_config_json, {}),
    first_seen_checkpoints_json: jsonValue(row.first_seen_checkpoints_json, []),
    final_result_json: jsonValue(row.final_result_json, null),
    items: items.map((item) => ({
      ...item,
      ordinal: Number(item.ordinal),
      attempt_count: Number(item.attempt_count ?? 0),
      target_json: jsonValue(item.target_json, {}),
      detail_json: jsonValue(item.detail_json, null),
      field_status_json: jsonValue(item.field_status_json, null),
      error_json: jsonValue(item.error_json, null),
    })),
  };
}

function validateCheckpointBatch(batch, { plan, runId, cycleKey }) {
  if (batch.run_id !== runId
      || batch.cycle_key !== cycleKey
      || batch.channel_id !== plan.channel_id
      || String(batch.plan_id) !== String(plan.plan_id)) {
    throw new Error(`Incremental YouTubeJS checkpoint Batch identity mismatch: ${runId}/${cycleKey}`);
  }
  if (!Array.isArray(batch.scan_json?.entries)
      || !Array.isArray(batch.anchors_json)
      || !Array.isArray(batch.discovery_entries_json)
      || !Array.isArray(batch.pending_deferred_video_ids)
      || !Array.isArray(batch.sampling_plan_json?.rows)
      || !Array.isArray(batch.first_seen_checkpoints_json)) {
    throw new Error(`Incremental YouTubeJS checkpoint Batch payload is invalid: ${runId}/${cycleKey}`);
  }
  if (incrementalYoutubeJsVideoTargetHash(batch.items) !== batch.target_hash) {
    throw new Error(`Incremental YouTubeJS checkpoint target hash mismatch: ${runId}/${cycleKey}`);
  }
  if (["ready", "finalized"].includes(batch.status)
      && (batch.first_seen_checkpoint_status === "pending"
        || batch.items.some((item) => !CHECKPOINT_TERMINAL_ITEM_STATUSES.has(item.status)))) {
    throw new Error(`terminal Incremental YouTubeJS Batch has unfinished Items: ${runId}/${cycleKey}`);
  }
  if (batch.status === "finalized"
      && (!batch.final_observation_id || !objectValue(batch.final_result_json))) {
    throw new Error(`finalized Incremental YouTubeJS Batch is incomplete: ${runId}/${cycleKey}`);
  }
  if (batch.status === "finalized"
      && batch.final_result_json.observation_id !== batch.final_observation_id) {
    throw new Error(`finalized Incremental YouTubeJS Batch result is inconsistent: ${runId}/${cycleKey}`);
  }
  return batch;
}

async function loadCheckpointBatch(client, { plan, runId, cycleKey, lock = false }) {
  const batchResult = await client.query(
    `SELECT * FROM crawler.incremental_youtubejs_video_batches
     WHERE run_id=$1 AND cycle_key=$2${lock ? " FOR UPDATE" : ""}`,
    [runId, cycleKey],
  );
  const row = batchResult.rows[0];
  if (!row) return null;
  const itemResult = await client.query(
    `SELECT * FROM crawler.incremental_youtubejs_video_items
     WHERE run_id=$1 AND cycle_key=$2
     ORDER BY CASE phase WHEN 'first_seen' THEN 0 ELSE 1 END,ordinal,video_id`,
    [runId, cycleKey],
  );
  return validateCheckpointBatch(normalizeCheckpointBatch(row, itemResult.rows), {
    plan,
    runId,
    cycleKey,
  });
}

async function verifyFinalizedObservation(client, batch) {
  if (batch.status !== "finalized") return;
  const result = await client.query(
    `SELECT observation_id
     FROM crawler.crawl_observations
     WHERE observation_id=$1 AND run_id=$2 AND channel_id=$3
       AND observation_kind='video'`,
    [batch.final_observation_id, batch.run_id, batch.channel_id],
  );
  if (resultRowCount(result) !== 1) {
    throw new Error(`finalized Batch Observation is missing: ${batch.run_id}/${batch.cycle_key}`);
  }
}

async function reserveSamplingPlan(client, {
  samplePlan,
  runId,
  cycleKey,
  observedAt,
}) {
  const leaseOwner = clockContentEnrichLeaseOwner({ runId, cycleKey });
  const rows = [];
  for (const row of samplePlan.rows) {
    const eligibility = await lockClockContentEnrichTask(client, row.content_key);
    let fence = null;
    if (!eligibility.skipped) {
      fence = await reserveClockContentEnrichPublication(client, {
        contentKey: row.content_key,
        currentTask: eligibility.currentTask,
        leaseOwner,
        outcome: { observed_at: observedAt },
      });
    }
    rows.push({
      ...row,
      checkpoint_content_enrich: {
        skipped: eligibility.skipped,
        current_task: checkpointTaskSnapshot(eligibility.currentTask),
        fence,
      },
    });
  }
  return { ...samplePlan, rows };
}

function checkpointItems(discoveryEntries, samplingPlan) {
  const detailEligibleFirstSeen = discoveryEntries.filter(
    (entry) => entry.disposition_recheck
      || (entry.is_upcoming !== true && entry.is_live !== true),
  );
  const items = [
    ...detailEligibleFirstSeen.map((entry, ordinal) => ({
      phase: "first_seen",
      ordinal,
      video_id: entry.id,
      target_json: jsonCheckpointValue(entry),
    })),
    ...samplingPlan.rows.map((row, ordinal) => ({
      phase: "recent",
      ordinal,
      video_id: row.source_content_id,
      target_json: jsonCheckpointValue(row),
    })),
  ];
  const identities = new Set(items.map((item) => item.video_id));
  if (identities.size !== items.length) {
    throw new Error("Incremental YouTubeJS checkpoint target appears in more than one Phase");
  }
  return items;
}

async function createCheckpointBatch({
  plan,
  runId,
  cycleKey,
  scan,
  anchors,
  samplingPlanInput,
  config,
  withTransaction,
  startedAt,
  observedAt,
  crawlerVersion,
}) {
  return withTransaction(async (client) => {
    const current = await loadRunCycle(client, { plan, runId, lock: true });
    if (current.cycleKey !== cycleKey) {
      throw new Error(`controlled recovery markers changed while creating Video Batch: ${runId}`);
    }
    const existing = await loadCheckpointBatch(client, { plan, runId, cycleKey });
    if (existing) return existing;

    const ids = scan.entries.map((entry) => entry.id);
    const known = await knownVideoIds(client.query.bind(client), plan.channel_id, ids);
    const latestDispositions = await latestVideoDispositionEntries(
      client.query.bind(client),
      plan.channel_id,
      ids.filter((id) => !known.has(id)),
    );
    const scannedWork = scannedVideoDispositionWork(
      scan.entries.filter((entry) => !known.has(entry.id)),
      latestDispositions,
      observedAt,
      { allowDueRechecks: scan.complete === true },
    );
    let discoveryEntries = scannedWork.workEntries;
    let recoveredFirstSeen = [];
    let samplingPlan = {
      recent_count: 0,
      stale_ratio: 0,
      candidate_count: 0,
      suggested_player_quota: 0,
      player_quota: 0,
      next_quota: 0,
      rows: [],
    };
    if (scan.complete === true) {
      recoveredFirstSeen = await loadPendingFirstSeenCheckpoints(
        client.query.bind(client),
        { channelId: plan.channel_id },
      );
      const dueDispositionEntries = await loadDueVideoDispositionEntries(
        client.query.bind(client),
        plan.channel_id,
        observedAt,
        scan.entries,
      );
      discoveryEntries = [...discoveryEntries, ...dueDispositionEntries];
      const enrichMode = await loadContentEnrichMode(client, { lock: true });
      const recentRows = await loadClockRecentSamplingRows(client, {
        channelId: plan.channel_id,
        planDay: plan.plan_day,
        recentWindowDays: config.recentWindowDays,
        clockOwnsPlayerRefresh: enrichMode === CONTENT_ENRICH_CLOCK_MODE,
        scanEntries: scan.entries,
      });
      const planned = planRecentVideoSampling(recentRows, {
        plan: samplingPlanInput,
        config,
        excludeVideoIds: discoveryEntries.map((entry) => entry.id),
        now: new Date(observedAt),
      });
      samplingPlan = await reserveSamplingPlan(client, {
        samplePlan: planned,
        runId,
        cycleKey,
        observedAt,
      });
    }
    const items = scan.complete === true
      ? checkpointItems(discoveryEntries, samplingPlan)
      : [];
    const firstSeenCount = items.filter((item) => item.phase === "first_seen").length;
    const firstSeenStatus = scan.complete !== true
      ? "not_applicable"
      : firstSeenCount === 0 ? "complete" : "pending";
    const status = items.length === 0 && firstSeenStatus !== "pending" ? "ready" : "fetching";
    const targetHash = incrementalYoutubeJsVideoTargetHash(items);
    await client.query(
      `INSERT INTO crawler.incremental_youtubejs_video_batches (
         run_id,cycle_key,plan_id,channel_id,status,cycle_observed_at,started_at,
         scan_json,anchors_json,discovery_entries_json,pending_deferred_video_ids,
         sampling_plan_json,sampling_config_json,target_hash,
         first_seen_checkpoint_status,first_seen_checkpoints_json
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,
         $12::jsonb,$13::jsonb,$14,$15,$16::jsonb
       )`,
      [
        runId,
        cycleKey,
        plan.plan_id,
        plan.channel_id,
        status,
        observedAt,
        startedAt,
        JSON.stringify(scan),
        JSON.stringify(anchors),
        JSON.stringify(discoveryEntries),
        JSON.stringify(scannedWork.pendingDeferredVideoIds),
        JSON.stringify(samplingPlan),
        JSON.stringify({
          ...config,
          sampling_plan_input: samplingPlanInput,
          crawler_version: crawlerVersion,
        }),
        targetHash,
        firstSeenStatus,
        JSON.stringify(recoveredFirstSeen),
      ],
    );
    if (items.length > 0) {
      await client.query(
        `INSERT INTO crawler.incremental_youtubejs_video_items (
           run_id,cycle_key,phase,ordinal,video_id,target_json
         )
         SELECT $1,$2,item.phase,item.ordinal,item.video_id,item.target_json
         FROM jsonb_to_recordset($3::jsonb)
           AS item(phase text,ordinal integer,video_id text,target_json jsonb)`,
        [runId, cycleKey, JSON.stringify(items)],
      );
    }
    checkpointLog("batch_created", {
      run_id: runId,
      cycle_key: cycleKey,
      complete_scan: scan.complete === true,
      first_seen_targets: firstSeenCount,
      recent_targets: samplingPlan.rows.length,
    });
    return loadCheckpointBatch(client, { plan, runId, cycleKey });
  });
}

export const INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL = `WITH candidate AS (
  SELECT item.run_id,item.cycle_key,item.phase,item.video_id
  FROM crawler.incremental_youtubejs_video_items item
  JOIN crawler.incremental_youtubejs_video_batches batch
    ON batch.run_id=item.run_id AND batch.cycle_key=item.cycle_key
  WHERE item.run_id=$1 AND item.cycle_key=$2 AND item.phase=$3
    AND batch.status='fetching'
    AND NOT EXISTS (
      SELECT 1
      FROM crawler.incremental_youtubejs_video_items active
      WHERE active.run_id=item.run_id AND active.cycle_key=item.cycle_key
        AND active.status='claimed'
        AND active.claim_expires_at>clock_timestamp()
    )
    AND (
      $3<>'recent'
      OR (
        batch.first_seen_checkpoint_status='complete'
        AND NOT EXISTS (
          SELECT 1
          FROM crawler.incremental_youtubejs_video_items first_seen
          WHERE first_seen.run_id=item.run_id
            AND first_seen.cycle_key=item.cycle_key
            AND first_seen.phase='first_seen'
            AND first_seen.status NOT IN ('captured','settled_error')
        )
      )
    )
    AND (
      item.status='pending'
      OR (item.status='claimed' AND item.claim_expires_at<=clock_timestamp())
    )
  ORDER BY item.ordinal,item.video_id
  LIMIT 1
  FOR UPDATE OF item SKIP LOCKED
)
UPDATE crawler.incremental_youtubejs_video_items item
SET status='claimed',claim_token=$4::uuid,
    claim_expires_at=clock_timestamp()+($5::bigint*interval '1 millisecond'),
    attempt_count=item.attempt_count+1,updated_at=clock_timestamp()
FROM candidate
WHERE item.run_id=candidate.run_id AND item.cycle_key=candidate.cycle_key
  AND item.phase=candidate.phase AND item.video_id=candidate.video_id
RETURNING item.*`;

export const INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL = `UPDATE crawler.incremental_youtubejs_video_items item
SET status=$6,claim_token=NULL,claim_expires_at=NULL,
    detail_json=$7::jsonb,field_status_json=$8::jsonb,error_json=$9::jsonb,
    captured_at=clock_timestamp(),updated_at=clock_timestamp()
FROM crawler.incremental_youtubejs_video_batches batch
WHERE item.run_id=$1 AND item.cycle_key=$2 AND item.phase=$3 AND item.video_id=$4
  AND item.status='claimed' AND item.claim_token=$5::uuid
  AND batch.run_id=item.run_id AND batch.cycle_key=item.cycle_key
  AND batch.status='fetching'
RETURNING item.video_id`;

async function claimCheckpointItem(withTransaction, { runId, cycleKey, phase }) {
  const claimToken = randomUUID();
  const claimLeaseMs = checkpointItemLeaseMs();
  return withTransaction(async (client) => {
    const batch = await client.query(
      `SELECT status,first_seen_checkpoint_status
       FROM crawler.incremental_youtubejs_video_batches
       WHERE run_id=$1 AND cycle_key=$2
       FOR UPDATE`,
      [runId, cycleKey],
    );
    if (resultRowCount(batch) !== 1) {
      throw new Error(`Incremental YouTubeJS Batch is missing: ${runId}/${cycleKey}`);
    }
    if (batch.rows[0].status !== "fetching") return null;
    const result = await client.query(
      INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL,
      [runId, cycleKey, phase, claimToken, claimLeaseMs],
    );
    const row = result.rows[0];
    return row
      ? { ...normalizeCheckpointBatch({}, [row]).items[0], claim_lease_ms: claimLeaseMs }
      : null;
  });
}

async function renewCheckpointClaim(withTransaction, item) {
  const leaseMs = Number(item.claim_lease_ms) || checkpointItemLeaseMs();
  const result = await withTransaction((client) => client.query(
    `UPDATE crawler.incremental_youtubejs_video_items item
     SET claim_expires_at=clock_timestamp()+($6::bigint*interval '1 millisecond'),
         updated_at=clock_timestamp()
     FROM crawler.incremental_youtubejs_video_batches batch
     WHERE item.run_id=$1 AND item.cycle_key=$2 AND item.phase=$3 AND item.video_id=$4
       AND item.status='claimed' AND item.claim_token=$5::uuid
       AND batch.run_id=item.run_id AND batch.cycle_key=item.cycle_key
       AND batch.status='fetching'
     RETURNING item.video_id`,
    [
      item.run_id,
      item.cycle_key,
      item.phase,
      item.video_id,
      item.claim_token,
      leaseMs,
    ],
  ));
  return resultRowCount(result) === 1;
}

function startCheckpointClaimHeartbeat({ item, withTransaction, heartbeat, signal }) {
  const controller = new AbortController();
  const intervalMs = checkpointItemHeartbeatMs(item.claim_lease_ms);
  let stopped = false;
  let heartbeatError = null;
  let pending = Promise.resolve();
  const tick = () => {
    pending = pending.then(async () => {
      if (stopped || heartbeatError) return;
      const renewed = await renewCheckpointClaim(withTransaction, item);
      if (!renewed) {
        const error = new Error(
          `Incremental YouTubeJS Item claim was lost: ${item.video_id}`,
        );
        error.code = "INCREMENTAL_YOUTUBEJS_ITEM_CLAIM_LOST";
        throw error;
      }
      if (heartbeat) await heartbeat();
    }).catch((error) => {
      if (!heartbeatError) {
        heartbeatError = error;
        controller.abort(error);
      }
    });
  };
  const timer = setInterval(tick, intervalMs);
  timer.unref?.();
  return {
    signal: combineAbortSignals(signal, controller.signal),
    async stop() {
      stopped = true;
      clearInterval(timer);
      await pending;
      return heartbeatError;
    },
  };
}

async function fetchCheckpointItemDetail({ item, fetchDetail, withTransaction, heartbeat, signal }) {
  const liveClaim = startCheckpointClaimHeartbeat({
    item,
    withTransaction,
    heartbeat,
    signal,
  });
  let detail;
  let fetchError = null;
  try {
    detail = await fetchDetail(item.video_id, {
      signal: liveClaim.signal,
      phase: item.phase,
      target: item.target_json,
    });
    throwIfAborted(liveClaim.signal);
  } catch (error) {
    fetchError = error;
  }
  const heartbeatError = await liveClaim.stop();
  if (heartbeatError && heartbeatError !== fetchError) {
    throw new AggregateError(
      [fetchError, heartbeatError].filter(Boolean),
      `Incremental YouTubeJS Item heartbeat failed: ${item.video_id}`,
      { cause: fetchError ?? heartbeatError },
    );
  }
  if (fetchError) throw fetchError;
  return detail;
}

async function releaseCheckpointClaim(withTransaction, item) {
  const result = await withTransaction((client) => client.query(
    `UPDATE crawler.incremental_youtubejs_video_items
     SET status='pending',claim_token=NULL,claim_expires_at=NULL,updated_at=clock_timestamp()
     WHERE run_id=$1 AND cycle_key=$2 AND phase=$3 AND video_id=$4
       AND status='claimed' AND claim_token=$5::uuid`,
    [item.run_id, item.cycle_key, item.phase, item.video_id, item.claim_token],
  ));
  return resultRowCount(result) === 1;
}

async function settleCheckpointItem(withTransaction, item, {
  status,
  detail = null,
  error = null,
}) {
  const fieldStatus = incrementalYoutubeJsVideoFieldStatus(detail, error);
  const errorJson = error == null ? null : serializedCheckpointError(error);
  const result = await withTransaction((client) => client.query(
    INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL,
    [
      item.run_id,
      item.cycle_key,
      item.phase,
      item.video_id,
      item.claim_token,
      status,
      detail == null ? null : JSON.stringify(jsonCheckpointValue(detail)),
      JSON.stringify(fieldStatus),
      errorJson == null ? null : JSON.stringify(errorJson),
    ],
  ));
  return resultRowCount(result) === 1;
}

async function checkpointPhaseState(query, { runId, cycleKey, phase }) {
  const result = await query(
    `SELECT count(*)::int AS total_count,
            count(*) FILTER (WHERE status IN ('captured','settled_error'))::int AS settled_count,
            count(*) FILTER (WHERE status='claimed' AND claim_expires_at>clock_timestamp())::int
              AS active_claim_count,
            COALESCE(
              GREATEST(
                0,
                CEIL(EXTRACT(EPOCH FROM (
                  MIN(claim_expires_at) FILTER (
                    WHERE status='claimed' AND claim_expires_at>clock_timestamp()
                  ) - clock_timestamp()
                )) * 1000)
              ),
              0
            )::int AS active_claim_wait_ms
     FROM crawler.incremental_youtubejs_video_items
     WHERE run_id=$1 AND cycle_key=$2 AND phase=$3`,
    [runId, cycleKey, phase],
  );
  const row = result.rows[0] ?? {};
  return {
    total: Number(row.total_count ?? 0),
    settled: Number(row.settled_count ?? 0),
    activeClaims: Number(row.active_claim_count ?? 0),
    activeClaimWaitMs: Number(row.active_claim_wait_ms ?? 0),
  };
}

function activeClaimPollMs(state) {
  return Math.max(
    CHECKPOINT_ACTIVE_CLAIM_POLL_MIN_MS,
    Math.min(
      CHECKPOINT_ACTIVE_CLAIM_POLL_MAX_MS,
      Math.max(0, Number(state.activeClaimWaitMs) || 0) + 25,
    ),
  );
}

function checkpointReservationFences(batch) {
  return batch.sampling_plan_json.rows
    .map((row) => objectValue(row.checkpoint_content_enrich)?.fence)
    .filter((fence) => objectValue(fence));
}

async function renewCheckpointReservations({ batch, withTransaction }) {
  const fences = checkpointReservationFences(batch);
  if (fences.length === 0) return 0;
  const result = await withTransaction((client) => client.query(
    `UPDATE crawler.content_enrich_tasks task
     SET lease_expires_at=clock_timestamp()+($2::bigint*interval '1 millisecond'),
         updated_at=clock_timestamp()
     FROM jsonb_to_recordset($1::jsonb)
       AS fence(task_id text,content_key text,dispatch_generation integer,lease_owner text)
     WHERE task.task_id=fence.task_id
       AND task.content_key=fence.content_key
       AND task.job_type='player-refresh'
       AND task.status='running'
       AND task.dispatch_generation=fence.dispatch_generation
       AND task.lease_owner=fence.lease_owner`,
    [JSON.stringify(fences), clockContentEnrichReservationMs()],
  ));
  return resultRowCount(result);
}

export async function captureIncrementalYoutubeJsVideoCheckpointPhase({
  runId,
  cycleKey,
  phase,
  query,
  withTransaction,
  fetchDetail,
  signal,
  heartbeat = null,
}) {
  for (;;) {
    throwIfAborted(signal);
    if (heartbeat) await heartbeat();
    const item = await claimCheckpointItem(withTransaction, { runId, cycleKey, phase });
    if (!item) {
      const state = await checkpointPhaseState(query, { runId, cycleKey, phase });
      if (state.settled === state.total) return state;
      if (state.activeClaims > 0) {
        await delay(
          activeClaimPollMs(state),
          undefined,
          signal ? { signal } : undefined,
        );
        continue;
      }
      throw new Error(
        `Incremental YouTubeJS ${phase} Phase has ${state.activeClaims} active Item claim(s)`,
      );
    }
    const requestStartedAt = Date.now();
    let detail;
    try {
      detail = await fetchCheckpointItemDetail({
        item,
        fetchDetail,
        withTransaction,
        heartbeat,
        signal,
      });
    } catch (error) {
      const aborted = signal?.aborted === true;
      const selectedFailure = selectYoutubeFailure({ error });
      const routeFailure = selectedFailure.decision.proxy_action !== "none";
      const canSettle = CHECKPOINT_SETTLED_FAILURE_KINDS.has(selectedFailure.decision.kind);
      if (aborted || !canSettle) {
        try {
          await releaseCheckpointClaim(withTransaction, item);
        } catch (releaseError) {
          if (error && (typeof error === "object" || typeof error === "function")) {
            error.checkpoint_release_error = releaseError;
          }
        }
        checkpointLog(aborted
          ? "item_cancelled"
          : routeFailure ? "item_route_failure" : "item_retryable_failure", {
          run_id: runId,
          cycle_key: cycleKey,
          phase,
          video_id: item.video_id,
          failure_kind: selectedFailure.decision.kind,
          duration_ms: Date.now() - requestStartedAt,
        });
        throw error;
      }
      const detail = objectValue(error?.partial_detail);
      let settled;
      try {
        settled = await settleCheckpointItem(withTransaction, item, {
          status: "settled_error",
          detail,
          error,
        });
      } catch (settlementError) {
        await releaseCheckpointClaim(withTransaction, item).catch(() => {});
        throw settlementError;
      }
      checkpointLog(settled ? "item_error_settled" : "item_stale_settlement", {
        run_id: runId,
        cycle_key: cycleKey,
        phase,
        video_id: item.video_id,
          failure_kind: selectedFailure.decision.kind,
        duration_ms: Date.now() - requestStartedAt,
      });
      continue;
    }
    let settled;
    try {
      settled = await settleCheckpointItem(withTransaction, item, {
        status: "captured",
        detail,
      });
    } catch (settlementError) {
      await releaseCheckpointClaim(withTransaction, item).catch(() => {});
      throw settlementError;
    }
    checkpointLog(settled ? "item_captured" : "item_stale_settlement", {
      run_id: runId,
      cycle_key: cycleKey,
      phase,
      video_id: item.video_id,
      duration_ms: Date.now() - requestStartedAt,
    });
  }
}

function capturesFromCheckpointItems(items, phase) {
  return new Map(items
    .filter((item) => item.phase === phase && CHECKPOINT_TERMINAL_ITEM_STATUSES.has(item.status))
    .map((item) => [item.video_id, {
      detail: item.detail_json,
      error: item.error_json == null ? null : checkpointErrorFromJson(item.error_json),
    }]));
}

async function checkpointFirstSeenPhase({ plan, batch, withTransaction }) {
  if (batch.first_seen_checkpoint_status !== "pending") return batch;
  return withTransaction(async (client) => {
    const locked = await loadCheckpointBatch(client, {
      plan,
      runId: batch.run_id,
      cycleKey: batch.cycle_key,
      lock: true,
    });
    if (!locked || locked.first_seen_checkpoint_status !== "pending") return locked;
    const firstSeenItems = locked.items.filter((item) => item.phase === "first_seen");
    if (firstSeenItems.some((item) => !CHECKPOINT_TERMINAL_ITEM_STATUSES.has(item.status))) {
      throw new Error("First-Seen checkpoint cannot run before every Phase A Item settles");
    }
    const newlyCheckpointed = await checkpointFirstSeenEnrichFailures({
      plan,
      runId: locked.run_id,
      candidateEntries: locked.discovery_entries_json,
      captures: capturesFromCheckpointItems(locked.items, "first_seen"),
      withTransaction,
      transactionClient: client,
      observedAt: new Date(locked.cycle_observed_at).toISOString(),
    });
    const byCandidate = new Map(locked.first_seen_checkpoints_json
      .map((checkpoint) => [String(checkpoint.candidateId), checkpoint]));
    for (const checkpoint of newlyCheckpointed) {
      byCandidate.set(String(checkpoint.candidateId), checkpoint);
    }
    await client.query(
      `UPDATE crawler.incremental_youtubejs_video_batches
       SET first_seen_checkpoint_status='complete',first_seen_checkpoints_json=$3::jsonb,
           updated_at=clock_timestamp()
       WHERE run_id=$1 AND cycle_key=$2 AND status='fetching'
         AND first_seen_checkpoint_status='pending'`,
      [locked.run_id, locked.cycle_key, JSON.stringify([...byCandidate.values()])],
    );
    checkpointLog("first_seen_checkpoint_complete", {
      run_id: locked.run_id,
      cycle_key: locked.cycle_key,
      checkpoint_count: byCandidate.size,
    });
    return loadCheckpointBatch(client, {
      plan,
      runId: locked.run_id,
      cycleKey: locked.cycle_key,
    });
  });
}

async function markCheckpointBatchReady({ plan, batch, withTransaction }) {
  return withTransaction(async (client) => {
    const locked = await loadCheckpointBatch(client, {
      plan,
      runId: batch.run_id,
      cycleKey: batch.cycle_key,
      lock: true,
    });
    if (!locked || locked.status !== "fetching") return locked;
    if (locked.first_seen_checkpoint_status === "pending"
        || locked.items.some((item) => !CHECKPOINT_TERMINAL_ITEM_STATUSES.has(item.status))) {
      throw new Error("Incremental YouTubeJS Batch cannot become ready with unfinished Items");
    }
    await client.query(
      `UPDATE crawler.incremental_youtubejs_video_batches
       SET status='ready',updated_at=clock_timestamp()
       WHERE run_id=$1 AND cycle_key=$2 AND status='fetching'`,
      [locked.run_id, locked.cycle_key],
    );
    checkpointLog("batch_ready", {
      run_id: locked.run_id,
      cycle_key: locked.cycle_key,
    });
    return loadCheckpointBatch(client, {
      plan,
      runId: locked.run_id,
      cycleKey: locked.cycle_key,
    });
  });
}

function preparedSamplingFromCheckpoint(batch) {
  const samplePlan = batch.sampling_plan_json;
  const recentItems = new Map(batch.items
    .filter((item) => item.phase === "recent")
    .map((item) => [item.video_id, item]));
  const preparedCaptures = new Map();
  for (const row of samplePlan.rows) {
    const item = recentItems.get(String(row.source_content_id));
    if (!item || !CHECKPOINT_TERMINAL_ITEM_STATUSES.has(item.status)) {
      throw new Error(`Recent Sampling checkpoint Item is missing: ${row.source_content_id}`);
    }
    const checkpoint = objectValue(row.checkpoint_content_enrich) ?? {};
    if (checkpoint.skipped === true) {
      preparedCaptures.set(row.content_key, { state: "skipped", fence: null });
      continue;
    }
    const currentTask = objectValue(checkpoint.current_task);
    const task = currentTask ?? {
      task_id: refreshTaskId(row.content_key, "player-refresh"),
      attempts: 0,
      dispatch_generation: 0,
    };
    const completedAt = new Date(item.captured_at ?? batch.cycle_observed_at);
    if (Number.isNaN(completedAt.getTime())) {
      throw new Error(`Recent Sampling checkpoint time is invalid: ${row.source_content_id}`);
    }
    const error = item.error_json == null ? null : checkpointErrorFromJson(item.error_json);
    const outcome = item.status === "settled_error"
      ? contentEnrichFailureOutcome(task, error, completedAt, clockContentEnrichRetryOptions())
      : row.enrich_pending === true
        ? contentEnrichDetailOutcome(
            task,
            item.detail_json,
            completedAt,
            clockContentEnrichRetryOptions(),
          )
        : recentMetricsDetailOutcome(
            task,
            item.detail_json,
            completedAt,
            clockContentEnrichRetryOptions(),
          );
    preparedCaptures.set(row.content_key, {
      state: outcome.detail == null ? "failure" : "publication",
      currentTask,
      outcome,
      fence: objectValue(checkpoint.fence),
    });
  }
  return { samplePlan, preparedCaptures };
}

function recentMetricsDetailOutcome(task, detail, observedAt, retryOptions) {
  const accessStatus = detailAccess(detail);
  if (!["members_only", "private", "unavailable"].includes(accessStatus)
      && detailViewCount(detail) == null) {
    const error = Object.assign(
      new Error("Incremental Video detail is missing the required metrics surface"),
      {
        youtube_failure_decision: {
          kind: "incomplete_detail",
          retry_mode: "same_identity",
          reason_code: "required_metrics_surface_missing",
        },
      },
    );
    return contentEnrichFailureOutcome(task, error, observedAt, retryOptions);
  }
  return {
    task_id: task?.task_id ?? null,
    dispatch_generation: task?.dispatch_generation ?? null,
    kind: ["members_only", "private", "unavailable"].includes(accessStatus)
      ? "terminal"
      : "done",
    detail,
    access_status: accessStatus,
    observed_at: observedAt.toISOString(),
    next_retry_at: videoAccessRecheckAt(accessStatus, observedAt),
    error_message: null,
  };
}

async function applyRecentSampling({
  runId,
  preparedSampling,
  transactionClient,
  observationId,
  observedAt,
  changeAlpha,
}) {
  const { samplePlan, preparedCaptures } = preparedSampling;
  const keys = samplePlan.rows.map((row) => row.content_key);
  const locked = keys.length === 0
    ? { rows: [] }
    : await transactionClient.query(
          `SELECT * FROM crawler.contents
           WHERE content_key=ANY($1::text[])
           ORDER BY content_key
           FOR UPDATE`,
          [keys],
    );
  const planned = new Map(samplePlan.rows.map((row) => [row.content_key, row]));
  let successCount = 0;
  let failureCount = 0;
  let viewChangedCount = 0;
  let viewDeltaTotal = 0;
  let comparableViewCount = 0;
  let engagementChangedCount = 0;
  const activityEvidence = [];
  for (const row of locked.rows) {
    const spec = planned.get(row.content_key);
    const prepared = preparedCaptures.get(row.content_key);
    if (!prepared || prepared.state === "skipped") {
      failureCount += 1;
      continue;
    }
    const ownership = await lockClockContentEnrichPublication(transactionClient, {
      contentKey: row.content_key,
      fence: prepared.fence,
    });
    if (!ownership.owned) {
      failureCount += 1;
      continue;
    }
    if (prepared.state === "failure") {
      await persistClockContentEnrichOutcome(transactionClient, {
        currentTask: ownership.currentTask ?? prepared.currentTask,
        priorTaskStatus: prepared.fence?.prior_status
          ?? prepared.currentTask?.status
          ?? ownership.currentTask?.status
          ?? null,
        outcome: prepared.outcome,
        contentKey: row.content_key,
        channelId: row.channel_id,
        runId,
        observationId,
        jobType: "player-refresh",
        observedAt,
      });
      failureCount += 1;
      continue;
    }
    if (prepared.state !== "publication") {
      throw new Error(`invalid prepared Recent Sampling state: ${prepared.state}`);
    }
    const applied = await applyIncrementalVideoDetail(transactionClient, {
      row,
      detail: prepared.outcome.detail,
      observedAt,
      observationId,
      collectNext: spec?.collect_next === true,
      changeAlpha,
      allowStaticRepair: spec?.enrich_pending === true,
    });
    await persistClockContentEnrichOutcome(transactionClient, {
      currentTask: ownership.currentTask ?? prepared.currentTask,
      priorTaskStatus: prepared.fence?.prior_status ?? ownership.currentTask?.status ?? null,
      outcome: prepared.outcome,
      contentKey: row.content_key,
      channelId: row.channel_id,
      runId,
      observationId,
      jobType: "player-refresh",
      observedAt,
    });
    if (!applied.success) {
      failureCount += 1;
      continue;
    }
    successCount += 1;
    if (applied.activityEvidence) activityEvidence.push(applied.activityEvidence);
    if (applied.viewDelta != null) {
      comparableViewCount += 1;
      viewDeltaTotal += applied.viewDelta;
      if (applied.viewDelta !== 0) viewChangedCount += 1;
    }
    if (applied.engagementChanged) engagementChangedCount += 1;
  }
  failureCount += Math.max(0, samplePlan.rows.length - locked.rows.length);
  const outcome = failureCount === 0
    ? "complete"
    : successCount > 0 ? "partial" : "failed";
  const payload = {
    recent_count: samplePlan.recent_count,
    stale_ratio: samplePlan.stale_ratio,
    selected_count: samplePlan.rows.length,
    success_count: successCount,
    failure_count: failureCount,
    next_count: samplePlan.next_quota,
    comparable_view_count: comparableViewCount,
    view_changed_count: viewChangedCount,
    view_delta_total: viewDeltaTotal,
    engagement_changed_count: engagementChangedCount,
  };
  return {
    outcome,
    payload,
    activityEvidence,
    summary: {
      recent_count: payload.recent_count,
      candidate_count: samplePlan.candidate_count,
      selected_count: payload.selected_count,
      success_count: successCount,
      failure_count: failureCount,
    },
  };
}

function checkpointExecutorResult(recorded) {
  return {
    outcome: recorded.outcome,
    observation_id: recorded.observation_id ?? null,
    event_id: recorded.event_id ?? null,
    kind_sequence: recorded.kind_sequence ?? null,
    first_seen_count: recorded.result?.firstSeen?.length ?? 0,
    selected_count: recorded.result?.selectedCount ?? 0,
    lifecycle_status: recorded.result?.lifecycleStatus ?? null,
    dormant_recheck_day: recorded.result?.dormantRecheckDay ?? null,
    duplicate: recorded.duplicate === true,
  };
}

function normalizeProbeScan(rawScanValue, anchors, config) {
  let rawScan = rawScanValue;
  if (rawScan?.stop_reason === "pagination_error") {
    const error = rawScan.error instanceof Error
      ? rawScan.error
      : new Error(
          text(rawScan.error?.message)
            ?? "YouTube.js Uploads pagination failed during read-only probe",
          rawScan.error ? { cause: rawScan.error } : undefined,
        );
    const failure = selectYoutubeFailure({ error });
    if (!["parser_runtime", "content_terminal"].includes(failure.decision.kind)) {
      throw error;
    }
    rawScan = { ...rawScan, complete: false };
  }
  if (!objectValue(rawScan) || !Array.isArray(rawScan.entries)) {
    throw new TypeError("YouTube.js Uploads scan returned an invalid result");
  }
  return applyCatchupGapAbandonment(rawScan, anchors, {
    catchUpMaxItems: config.discoveryCatchUpMaxItems,
  });
}

function probeItemResult(item, { status, detail = null, error = null, elapsedMs }) {
  return {
    phase: item.phase,
    ordinal: item.ordinal,
    video_id: item.video_id,
    status,
    elapsed_ms: elapsedMs,
    target: item.target_json,
    detail: detail == null ? null : jsonCheckpointValue(detail),
    field_status: incrementalYoutubeJsVideoFieldStatus(detail, error),
    failure: error == null ? null : serializedCheckpointError(error),
  };
}

function probeHalt(failure, {
  phase,
  videoId = null,
  productionExecutorAction,
}) {
  const requestsRouteSwitch = PROBE_ROUTE_FAILURE_KINDS.has(failure?.decision?.kind);
  return {
    phase,
    video_id: videoId,
    executor_action: requestsRouteSwitch
      ? "request_bounded_diagnostic_route_switch"
      : "stop_read_only_probe_without_route_switch",
    production_executor_action: productionExecutorAction,
    retry_suppressed: !requestsRouteSwitch,
    failure,
  };
}

export function incrementalYoutubeJsVideoProbeAttemptOutcome(report) {
  if (!objectValue(report)
      || report.mode !== "incremental_youtubejs_video_read_only_probe"
      || report.writes_performed !== false) {
    throw new TypeError("a read-only Incremental YouTubeJS probe report is required");
  }
  const failure = objectValue(report.halted?.failure);
  const failureKind = text(failure?.decision?.kind);
  if (PROBE_ROUTE_FAILURE_KINDS.has(failureKind)) {
    return {
      kind: "retryable_network_failure",
      observation: failureKind,
      source: text(failure?.decision?.evidence?.source)
        ?? text(failure?.evidence?.source)
        ?? "youtube_managed_request",
      failedStage: text(report.halted?.phase) ?? "incremental_video_probe",
      // The probe has no mutable business state. Rota and Channel Runtime persist
      // diagnostic task/attempt evidence before the next route is allowed to run.
      checkpointPersisted: true,
    };
  }
  return {
    kind: "managed_work_complete",
    businessState: "terminal",
    result: {
      report_ready: true,
      fetch_flow_ok: report.verification?.fetch_flow_ok === true,
    },
  };
}

export async function probeIncrementalYoutubeJsVideoFetch({
  plan,
  query,
  getChannelSnapshot,
  fetchDetail = fetchIncrementalYoutubeJsVideoDetail,
  detailLimitPerPhase = 1,
  now = () => new Date(),
}) {
  if (!objectValue(plan)) throw new TypeError("plan is required for Incremental Video probe");
  if (typeof query !== "function") throw new TypeError("query is required for Incremental Video probe");
  if (typeof getChannelSnapshot !== "function") {
    throw new TypeError("getChannelSnapshot is required for Incremental Video probe");
  }
  if (typeof fetchDetail !== "function") {
    throw new TypeError("fetchDetail must be a function for Incremental Video probe");
  }
  const phaseLimit = Number(detailLimitPerPhase);
  if (!Number.isSafeInteger(phaseLimit) || phaseLimit < 0 || phaseLimit > 10) {
    throw new TypeError("detailLimitPerPhase must be an integer between 0 and 10");
  }
  const observedAtValue = new Date(now());
  if (Number.isNaN(observedAtValue.getTime())) throw new TypeError("now must return a valid date");
  const observedAt = observedAtValue.toISOString();
  const config = incrementalVideoPlannerConfig(plan);
  const storedVideoPlayerBudget = Math.floor(plan.capacity.player_cap * plan.capacity.factor);
  const samplingPlanInput = {
    ...plan,
    capacity: {
      ...plan.capacity,
      factor: 1,
      player_cap: storedVideoPlayerBudget,
    },
  };
  const signal = currentChannelExecutionAbortSignal();
  const assertNotAborted = () => throwIfAborted(signal);

  const anchors = await loadDiscoveryAnchors(query, plan.channel_id);
  assertNotAborted();
  let scanFailure = null;
  let scan = {
    playlist_id: null,
    entries: [],
    pages: 0,
    item_count: 0,
    parse_gap_count: 0,
    anchor_matched: false,
    matched_anchor_id: null,
    stop_reason: "probe_scan_failed",
    terminal_reason: "probe_scan_failed",
    complete: false,
  };
  try {
    const snapshot = await getChannelSnapshot();
    scan = normalizeProbeScan(await snapshot.scanUploads({
      anchors,
      maxPages: config.discoveryMaxPages,
      catchUpMaxItems: config.discoveryCatchUpMaxItems,
    }), anchors, config);
    assertNotAborted();
  } catch (error) {
    assertNotAborted();
    scanFailure = serializedCheckpointError(error);
  }

  let discoveryEntries = [];
  let pendingDeferredVideoIds = [];
  let recoveredFirstSeen = [];
  let samplingPlan = {
    recent_count: 0,
    stale_ratio: 0,
    candidate_count: 0,
    suggested_player_quota: 0,
    player_quota: 0,
    next_quota: 0,
    rows: [],
  };
  if (scan.complete === true) {
    const scanIds = scan.entries.map((entry) => entry.id);
    const known = await knownVideoIds(query, plan.channel_id, scanIds);
    const latestDispositions = await latestVideoDispositionEntries(
      query,
      plan.channel_id,
      scanIds.filter((videoId) => !known.has(videoId)),
    );
    const scannedWork = scannedVideoDispositionWork(
      scan.entries.filter((entry) => !known.has(entry.id)),
      latestDispositions,
      observedAt,
      { allowDueRechecks: true },
    );
    pendingDeferredVideoIds = scannedWork.pendingDeferredVideoIds;
    const dueDispositionEntries = await loadDueVideoDispositionEntries(
      query,
      plan.channel_id,
      observedAt,
      scan.entries,
    );
    discoveryEntries = [...scannedWork.workEntries, ...dueDispositionEntries];
    recoveredFirstSeen = await loadPendingFirstSeenCheckpoints(query, {
      channelId: plan.channel_id,
    });
    const queryClient = { query };
    const enrichMode = await loadContentEnrichMode(queryClient);
    const recentRows = await loadClockRecentSamplingRows(queryClient, {
      channelId: plan.channel_id,
      planDay: plan.plan_day,
      recentWindowDays: config.recentWindowDays,
      clockOwnsPlayerRefresh: enrichMode === CONTENT_ENRICH_CLOCK_MODE,
      scanEntries: scan.entries,
    });
    samplingPlan = planRecentVideoSampling(recentRows, {
      plan: samplingPlanInput,
      config,
      excludeVideoIds: discoveryEntries.map((entry) => entry.id),
      now: observedAtValue,
    });
  }

  const items = scan.complete === true
    ? checkpointItems(discoveryEntries, samplingPlan)
    : [];
  const phaseReports = [];
  let halted = scanFailure == null ? null : probeHalt(scanFailure, {
    phase: "uploads_scan",
    productionExecutorAction: "classify_and_retry_according_to_youtube_failure_policy",
  });
  phaseLoop:
  for (const phase of CHECKPOINT_PHASES) {
    if (scanFailure != null) break;
    const phaseItems = items.filter((item) => item.phase === phase);
    const sampledItems = phaseItems.slice(0, phaseLimit);
    const results = [];
    for (const item of sampledItems) {
      assertNotAborted();
      const startedAt = Date.now();
      try {
        const detail = await fetchDetail(item.video_id, { signal });
        results.push(probeItemResult(item, {
          status: "captured",
          detail,
          elapsedMs: Date.now() - startedAt,
        }));
      } catch (error) {
        const failure = selectYoutubeFailure({ error });
        const canSettle = CHECKPOINT_SETTLED_FAILURE_KINDS.has(failure.decision.kind);
        results.push(probeItemResult(item, {
          status: canSettle ? "settled_error" : "pending",
          detail: objectValue(error?.partial_detail),
          error,
          elapsedMs: Date.now() - startedAt,
        }));
        if (!canSettle) {
          halted = probeHalt(serializedCheckpointError(error), {
            phase,
            videoId: item.video_id,
            productionExecutorAction: "throw_and_retry_same_pending_item",
          });
          phaseReports.push({
            phase,
            target_count: phaseItems.length,
            sampled_count: sampledItems.length,
            results,
          });
          break phaseLoop;
        }
      }
    }
    phaseReports.push({
      phase,
      target_count: phaseItems.length,
      sampled_count: sampledItems.length,
      results,
    });
  }

  const resultRows = phaseReports.flatMap((phase) => phase.results);
  const capturedCount = resultRows.filter((row) => row.status === "captured").length;
  const settledErrorCount = resultRows.filter((row) => row.status === "settled_error").length;
  const pendingCount = resultRows.filter((row) => row.status === "pending").length;
  const phaseState = (phase) => {
    if (scanFailure != null) return "skipped_after_scan_failure";
    if (halted && CHECKPOINT_PHASES.indexOf(phase) > CHECKPOINT_PHASES.indexOf(halted.phase)) {
      return "skipped_after_retryable_failure";
    }
    const report = phaseReports.find((current) => current.phase === phase);
    if (!report || report.sampled_count === 0) return "no_sample_target";
    return report.results.some((row) => row.status === "pending")
      ? "would_retry"
      : "sample_complete";
  };

  return {
    schema_version: 1,
    mode: "incremental_youtubejs_video_read_only_probe",
    writes_performed: false,
    observed_at: observedAt,
    plan: {
      plan_id: plan.plan_id ?? null,
      plan_mode: plan.plan_mode ?? null,
      formal_daily_plan: plan.formal_daily_plan === true,
      plan_day: plan.plan_day,
      channel_id: plan.channel_id,
      task_mask: plan.task_mask ?? null,
      capacity: plan.capacity,
      planner_config_version: plan.planner_config_version,
    },
    flow: [
      { step: "clock_channel_sample", state: "selected_read_only" },
      {
        step: "uploads_scan",
        state: scanFailure == null
          ? (scan.complete === true ? "complete" : "incomplete")
          : PROBE_ROUTE_FAILURE_KINDS.has(scanFailure?.decision?.kind)
            ? "failed_route_switch_requested"
            : "failed_without_route_switch",
      },
      { step: "batch_snapshot", state: "computed_not_persisted" },
      { step: "phase_a_first_seen_detail", state: phaseState("first_seen") },
      { step: "first_seen_ledger", state: "not_written_by_read_only_probe" },
      { step: "phase_b_recent_detail", state: phaseState("recent") },
      { step: "finalize", state: "not_executed_by_read_only_probe" },
    ],
    scan: {
      playlist_id: scan.playlist_id ?? null,
      complete: scan.complete === true,
      pages: Number(scan.pages ?? 0),
      item_count: Number(scan.item_count ?? scan.entries.length),
      parse_gap_count: Number(scan.parse_gap_count ?? 0),
      anchor_matched: scan.anchor_matched === true,
      matched_anchor_id: scan.matched_anchor_id ?? null,
      stop_reason: scan.stop_reason ?? null,
      terminal_reason: scan.terminal_reason ?? null,
      failure: scanFailure,
      anchors,
      entries: jsonCheckpointValue(scan.entries),
    },
    targets: {
      first_seen_count: items.filter((item) => item.phase === "first_seen").length,
      recent_count: items.filter((item) => item.phase === "recent").length,
      pending_deferred_video_ids: pendingDeferredVideoIds,
      recovered_first_seen_count: recoveredFirstSeen.length,
      sampling: {
        recent_count: samplingPlan.recent_count,
        candidate_count: samplingPlan.candidate_count,
        player_quota: samplingPlan.player_quota,
        next_quota: samplingPlan.next_quota,
      },
      items: items.map((item) => ({
        phase: item.phase,
        ordinal: item.ordinal,
        video_id: item.video_id,
      })),
    },
    phases: phaseReports,
    halted,
    verification: {
      scan_complete: scan.complete === true,
      sampled_detail_count: resultRows.length,
      captured_count: capturedCount,
      settled_error_count: settledErrorCount,
      pending_count: pendingCount,
      fetch_flow_ok: halted == null,
      detail_surface_observed: capturedCount > 0,
      automatic_retry_or_route_switch_requested: PROBE_ROUTE_FAILURE_KINDS.has(
        halted?.failure?.decision?.kind,
      ),
      checkpoint_resume_tested: false,
      finalize_tested: false,
    },
  };
}

async function recordVideoCycle({
  plan,
  runId,
  cycleKey,
  scan,
  discoveryEntries,
  discoveryCaptures,
  checkpointedFirstSeen,
  pendingDeferredVideoIds,
  anchors,
  preparedSampling,
  samplingPlanInput,
  config,
  withTransaction,
  startedAt,
  observedAt,
  crawlerVersion,
}) {
  const commandEntries = scan.entries.map((entry) => {
    const facts = detailFacts(discoveryCaptures.get(entry.id)?.detail);
    const publicationSelection = selectPublicationEvidence(
      uploadsPublishedFacts(entry),
      facts,
    );
    const publicationConflict = publicationEvidenceConflictRecord(publicationSelection);
    return {
      video_id: entry.id,
      position: entry.position,
      content_type: entry.content_type,
      live_ended_at: facts?.live_ended_at ?? null,
      duration_seconds: facts?.duration_seconds ?? null,
      ...publicationSelection.evidence,
      ...(publicationConflict ? { publication_evidence_conflict: publicationConflict } : {}),
    };
  });
  return withTransaction(async (client) => {
      const current = await loadRunCycle(client, { plan, runId, lock: true });
      if (current.cycleKey !== cycleKey) {
        throw new Error(`controlled recovery markers changed before Video Finalization: ${runId}`);
      }
      const lockedBatch = await loadCheckpointBatch(client, {
        plan,
        runId,
        cycleKey,
        lock: true,
      });
      if (!lockedBatch) {
        throw new Error(`Incremental YouTubeJS Batch disappeared: ${runId}/${cycleKey}`);
      }
      if (lockedBatch.status === "finalized") {
        await verifyFinalizedObservation(client, lockedBatch);
        return lockedBatch.final_result_json;
      }
      if (lockedBatch.status !== "ready") {
        throw new Error(`Incremental YouTubeJS Batch is not ready: ${runId}/${cycleKey}`);
      }
      const observation = await recordCrawlerObservation(client, {
        idempotencyKey: `video:${runId}:youtubejs:${cycleKey}`,
        observationKind: "video",
        channelId: plan.channel_id,
        runId,
        observedAt,
        planId: plan.plan_id,
        planDay: plan.plan_day,
        triggerReason: "clock_due",
        scheduledAt: plan.scheduled_at,
        startedAt,
        finishedAt: observedAt,
        crawlerVersion,
        extractorVersions: { youtubejs: scan.raw?.engine ?? "youtubei.js@17.2.0" },
        command: {
          planner_config_version: plan.planner_config_version,
          anchors,
          scan: {
            pages: scan.pages,
            stop_reason: scan.stop_reason,
            entries: commandEntries,
            ...(scan.gap_abandonment ? { gap_abandonment: scan.gap_abandonment } : {}),
          },
          sampling: {
            basis: "post_discovery_current",
            recent_window_days: config.recentWindowDays,
            stale_after_days: config.staleAfterDays,
            minimum_refresh_score: config.minimumRefreshScore,
            default_collection_priority: config.defaultCollectionPriority,
            default_change_probability: config.defaultChangeProbability,
            default_interaction_need: config.defaultInteractionNeed,
            change_ewma_alpha: config.changeEwmaAlpha,
            remaining_player_cap: samplingPlanInput.capacity.player_cap,
            next_cap: samplingPlanInput.capacity.next_cap,
          },
        },
        prepare: async ({ client: transactionClient, observationId }) => {
          if (scan.complete !== true) {
            const discovery = await applyDiscovery({
              plan,
              runId,
              scan,
              candidateEntries: discoveryEntries,
              captures: discoveryCaptures,
              transactionClient,
              observationId,
              observedAt,
              pendingDeferredVideoIds,
              checkpointedFirstSeen,
            });
            const discoveryPayload = {
              ...discovery.payload,
              first_page_item_count: Number(scan.first_page_item_count ?? 0),
              catch_up_item_count: Number(scan.catch_up_item_count ?? 0),
              unclosed_video_ids: scan.entries.map((entry) => entry.id),
            };
            return {
              outcome: "partial",
              outcomeReasonCode: `video_cycle_discovery_${scan.stop_reason || "incomplete"}`,
              resultSummary: {
                discovery: {
                  pages: discoveryPayload.pages,
                  items: discoveryPayload.items,
                  anchor_matched: discoveryPayload.anchor_matched,
                  stop_reason: discoveryPayload.stop_reason,
                  parse_gap_count: discoveryPayload.parse_gap_count,
                  first_seen_count: discoveryPayload.first_seen_count,
                  discovered_count: discoveryPayload.discovered_count,
                  silent_drop_count: discoveryPayload.silent_drop_count,
                  stored_count: discoveryPayload.stored_count,
                  deferred_count: discoveryPayload.deferred_count,
                  terminal_excluded_count: discoveryPayload.terminal_excluded_count,
                  detail_success_count: discoveryPayload.detail_success_count,
                  detail_failure_count: discoveryPayload.detail_failure_count,
                },
                recent_sampling: {
                  selected_count: 0,
                  success_count: 0,
                  failure_count: 0,
                  skipped_reason: "discovery_incomplete",
                },
              },
              payload: {
                discovery: { outcome: "partial", payload: discoveryPayload },
                recent_sampling: {
                  outcome: "skipped",
                  payload: { skipped_reason: "discovery_incomplete" },
                },
              },
              anchorVideoIds: null,
              sourceCursor: null,
              result: {
                firstSeen: discovery.firstSeen,
                selectedCount: 0,
                lifecycleStatus: null,
                lifecycleTransitioned: false,
                dormantRecheckDay: null,
              },
            };
          }
          const discovery = await applyDiscovery({
            plan,
            runId,
            scan,
            candidateEntries: discoveryEntries,
            captures: discoveryCaptures,
            transactionClient,
            observationId,
            observedAt,
            pendingDeferredVideoIds,
            checkpointedFirstSeen,
          });
          const { samplePlan } = preparedSampling;
          const recentSampling = await applyRecentSampling({
            runId,
            preparedSampling,
            transactionClient,
            observationId,
            observedAt,
            changeAlpha: config.changeEwmaAlpha,
          });
          const publicationRows = await transactionClient.query(
            `SELECT content_key
             FROM crawler.contents
             WHERE channel_id=$1
               AND (
                 source_content_id=ANY($2::text[])
                 OR content_key=ANY($3::text[])
               )
             ORDER BY content_key`,
            [
              plan.channel_id,
              scan.entries.map((entry) => entry.id),
              [
                ...samplePlan.rows.map((row) => row.content_key),
                ...discovery.claimedFirstSeen.map((current) => current.contentKey),
              ],
            ],
          );
          await refreshVideoPublicationItemHashes(
            transactionClient,
            publicationRows.rows.map((row) => row.content_key),
          );
          const lifecycle = await applyVideoActivityLifecycle(transactionClient, {
            channelId: plan.channel_id,
            observedAt,
            discoveryComplete: scan.complete === true,
            runActivityEvidence: currentRunActivityEvidence(
              commandEntries,
              scan.entries,
              [
                ...discovery.payload.dispositions,
                ...discovery.payload.recheck_dispositions,
              ],
              [
                ...discovery.activityEvidence,
                ...recentSampling.activityEvidence,
              ],
            ),
          });
          let activityEvidence;
          try {
            activityEvidence = buildVideoActivityEvidence(lifecycle);
          } catch (error) {
            console.error(JSON.stringify({
              event: "producer_activity_evidence_invalid",
              channel_id: plan.channel_id,
              plan_id: plan.plan_id ?? null,
              policy_version: lifecycle?.policy_version ?? null,
              violated_rules: [error?.message || String(error)],
            }));
            throw error;
          }
          const outcome = discovery.outcome === "complete" && recentSampling.outcome === "complete"
            ? "complete"
            : "partial";
          return {
            outcome,
            outcomeReasonCode: outcome === "complete"
              ? scan.stop_reason === GAP_ABANDONMENT_STOP_REASON
                ? "video_cycle_gap_abandoned_latest_30"
                : "video_cycle_complete"
              : `video_cycle_${discovery.outcome}_${recentSampling.outcome}`,
            resultSummary: {
              discovery: discovery.summary,
              recent_sampling: recentSampling.summary,
              activity: {
                lifecycle_status: lifecycle.lifecycle_status,
                ...activityEvidence,
                conclusive: lifecycle.conclusive,
              },
            },
            payload: {
              discovery: { outcome: discovery.outcome, payload: discovery.payload },
              recent_sampling: {
                outcome: recentSampling.outcome,
                payload: recentSampling.payload,
              },
              activity_evidence: activityEvidence,
              ...(lifecycle.activity ? { activity: lifecycle.activity } : {}),
            },
            anchorVideoIds: discovery.outcome === "complete"
              ? mergedDiscoveryAnchorIds(scan.entries, anchors)
              : null,
            sourceCursor: discovery.outcome === "complete"
              ? {
                  playlist_id: scan.playlist_id,
                  matched_anchor_id: scan.matched_anchor_id,
                  crossed_anchor_ids: scan.crossed_anchor_ids ?? [],
                  terminal_reason: scan.terminal_reason,
                  ...(scan.gap_abandonment ? {
                    gap_abandonment: {
                      policy_version: scan.gap_abandonment.policy_version,
                      source_stop_reason: scan.gap_abandonment.source_stop_reason,
                      scanned_item_count: scan.gap_abandonment.scanned_item_count,
                      first_page_item_count: scan.gap_abandonment.first_page_item_count,
                      catch_up_item_count: scan.gap_abandonment.catch_up_item_count,
                      catch_up_item_limit: scan.gap_abandonment.catch_up_item_limit,
                      selected_item_count: scan.gap_abandonment.selected_item_count,
                    },
                  } : {}),
                }
              : null,
            result: {
              firstSeen: discovery.firstSeen,
              selectedCount: samplePlan.rows.length,
              lifecycleStatus: lifecycle.lifecycle_status,
              lifecycleTransitioned: lifecycle.transitioned === true,
              dormantRecheckDay: lifecycle.dormant_recheck_day ?? null,
            },
          };
        },
      });
      if (observation.duplicate === true) {
        throw new Error(
          `Video Observation exists while checkpoint Batch is not finalized: ${runId}/${cycleKey}`,
        );
      }
      if (scan.complete === true) {
        await reconcilePublication(client, {
          channelId: plan.channel_id,
          domains: ["channel", "video"],
          asOf: observedAt,
        });
      }
      await releaseClockContentEnrichReservationsInTransaction(client, preparedSampling);
      const finalResult = checkpointExecutorResult(observation);
      const finalized = await client.query(
        `UPDATE crawler.incremental_youtubejs_video_batches
         SET status='finalized',final_observation_id=$3::uuid,final_result_json=$4::jsonb,
             finalized_at=clock_timestamp(),updated_at=clock_timestamp()
         WHERE run_id=$1 AND cycle_key=$2 AND status='ready'
         RETURNING run_id`,
        [runId, cycleKey, observation.observation_id, JSON.stringify(finalResult)],
      );
      if (resultRowCount(finalized) !== 1) {
        throw new Error(`failed to finalize Incremental YouTubeJS Batch: ${runId}/${cycleKey}`);
      }
      checkpointLog("batch_finalized", {
        run_id: runId,
        cycle_key: cycleKey,
        observation_id: observation.observation_id,
        outcome: observation.outcome,
      });
      return finalResult;
    });
}

export async function executeIncrementalYoutubeJsVideo({
  plan,
  runId,
  getChannelSnapshot,
  query,
  withTransaction,
  startedAt,
  fetchDetail = null,
  crawlerVersion = String(process.env.CRAWLER_VERSION || "qy-v16"),
  now = () => new Date(),
}) {
  if (typeof query !== "function") throw new TypeError("query is required for incremental Video");
  if (typeof withTransaction !== "function") {
    throw new TypeError("withTransaction is required for incremental Video");
  }
  if (typeof getChannelSnapshot !== "function") {
    throw new TypeError("getChannelSnapshot is required for incremental Video");
  }
  if (typeof now !== "function") throw new TypeError("now must be a function");
  const config = incrementalVideoPlannerConfig(plan);
  const observedAtValue = new Date(now());
  if (Number.isNaN(observedAtValue.getTime())) throw new TypeError("now must return a valid date");
  const observedAt = observedAtValue.toISOString();
  const startedAtValue = new Date(startedAt);
  if (Number.isNaN(startedAtValue.getTime())) throw new TypeError("startedAt must be valid");
  const detailFetcher = fetchDetail ?? fetchIncrementalYoutubeJsVideoDetail;
  const signal = currentChannelExecutionAbortSignal();
  const assertNotAborted = () => throwIfAborted(signal);
  const storedVideoPlayerBudget = Math.floor(plan.capacity.player_cap * plan.capacity.factor);
  const samplingPlanInput = {
    ...plan,
    capacity: {
      ...plan.capacity,
      factor: 1,
      player_cap: storedVideoPlayerBudget,
    },
  };
  let cycleKey;
  let batch = await withTransaction(async (client) => {
    const current = await loadRunCycle(client, { plan, runId });
    cycleKey = current.cycleKey;
    const loaded = await loadCheckpointBatch(client, { plan, runId, cycleKey });
    if (loaded?.status === "finalized") await verifyFinalizedObservation(client, loaded);
    return loaded;
  });
  if (batch?.status === "finalized") return batch.final_result_json;

  if (!batch) {
    const anchors = await loadDiscoveryAnchors(query, plan.channel_id);
    assertNotAborted();
    let rawScan;
    try {
      const snapshot = await getChannelSnapshot();
      rawScan = await snapshot.scanUploads({
        anchors,
        maxPages: config.discoveryMaxPages,
        catchUpMaxItems: config.discoveryCatchUpMaxItems,
      });
    } catch (error) {
      assertNotAborted();
      checkpointLog("scan_failed", {
        run_id: runId,
        cycle_key: cycleKey,
        failure_kind: selectYoutubeFailure({ error }).decision.kind,
      });
      throw error;
    }
    assertNotAborted();
    if (rawScan?.stop_reason === "pagination_error") {
      const error = rawScan.error instanceof Error
        ? rawScan.error
        : new Error(
            text(rawScan.error?.message)
              ?? `YouTube.js Uploads pagination failed for ${plan.channel_id}`,
            rawScan.error ? { cause: rawScan.error } : undefined,
          );
      const failure = selectYoutubeFailure({ error });
      if (!["parser_runtime", "content_terminal"].includes(failure.decision.kind)) {
        checkpointLog(shouldReportProxyFailure({ error })
          ? "scan_route_failure"
          : "scan_retryable_failure", {
          run_id: runId,
          cycle_key: cycleKey,
          failure_kind: failure.decision.kind,
        });
        throw error;
      }
      rawScan = { ...rawScan, complete: false };
    }
    if (!objectValue(rawScan) || !Array.isArray(rawScan.entries)) {
      throw new TypeError("YouTube.js Uploads scan returned an invalid result");
    }
    const scan = applyCatchupGapAbandonment(rawScan, anchors, {
      catchUpMaxItems: config.discoveryCatchUpMaxItems,
    });
    batch = await createCheckpointBatch({
      plan,
      runId,
      cycleKey,
      scan,
      anchors,
      samplingPlanInput,
      config,
      withTransaction,
      startedAt: startedAtValue.toISOString(),
      observedAt,
      crawlerVersion,
    });
  } else {
    checkpointLog("batch_resumed", {
      run_id: runId,
      cycle_key: cycleKey,
      status: batch.status,
      pending_count: batch.items.filter((item) => item.status === "pending").length,
    });
  }

  if (batch.status === "fetching") {
    await captureIncrementalYoutubeJsVideoCheckpointPhase({
      runId,
      cycleKey,
      phase: "first_seen",
      query,
      withTransaction,
      fetchDetail: detailFetcher,
      signal,
      heartbeat: () => renewCheckpointReservations({ batch, withTransaction }),
    });
    batch = await withTransaction((client) => loadCheckpointBatch(client, {
      plan,
      runId,
      cycleKey,
    }));
    batch = await checkpointFirstSeenPhase({ plan, batch, withTransaction });
    await captureIncrementalYoutubeJsVideoCheckpointPhase({
      runId,
      cycleKey,
      phase: "recent",
      query,
      withTransaction,
      fetchDetail: detailFetcher,
      signal,
      heartbeat: () => renewCheckpointReservations({ batch, withTransaction }),
    });
    batch = await withTransaction((client) => loadCheckpointBatch(client, {
      plan,
      runId,
      cycleKey,
    }));
    batch = await markCheckpointBatchReady({ plan, batch, withTransaction });
  }
  if (batch.status === "finalized") {
    return batch.final_result_json;
  }
  if (batch.status !== "ready") {
    throw new Error(`Incremental YouTubeJS Batch did not reach ready: ${runId}/${cycleKey}`);
  }
  await renewCheckpointReservations({ batch, withTransaction });

  const scan = batch.scan_json;
  const storedConfig = batch.sampling_config_json;
  const finalSamplingPlanInput = objectValue(storedConfig.sampling_plan_input)
    ?? samplingPlanInput;
  const finalConfig = {
    version: storedConfig.version,
    discoveryMaxPages: storedConfig.discoveryMaxPages,
    discoveryCatchUpMaxItems: storedConfig.discoveryCatchUpMaxItems,
    recentWindowDays: storedConfig.recentWindowDays,
    staleAfterDays: storedConfig.staleAfterDays,
    defaultCollectionPriority: storedConfig.defaultCollectionPriority,
    defaultChangeProbability: storedConfig.defaultChangeProbability,
    defaultInteractionNeed: storedConfig.defaultInteractionNeed,
    minimumRefreshScore: storedConfig.minimumRefreshScore,
    changeEwmaAlpha: storedConfig.changeEwmaAlpha,
  };
  return recordVideoCycle({
    plan,
    runId,
    cycleKey,
    scan,
    discoveryEntries: batch.discovery_entries_json,
    discoveryCaptures: capturesFromCheckpointItems(batch.items, "first_seen"),
    checkpointedFirstSeen: batch.first_seen_checkpoints_json,
    pendingDeferredVideoIds: batch.pending_deferred_video_ids,
    anchors: batch.anchors_json,
    preparedSampling: scan.complete === true ? preparedSamplingFromCheckpoint(batch) : null,
    samplingPlanInput: finalSamplingPlanInput,
    config: finalConfig,
    withTransaction,
    startedAt: new Date(batch.started_at).toISOString(),
    observedAt: new Date(batch.cycle_observed_at).toISOString(),
    crawlerVersion: text(storedConfig.crawler_version) ?? crawlerVersion,
  });
}
