import { createHash } from "node:crypto";
import { currentChannelExecution } from "./channelExecutionContext.js";
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
  isLiveInProgress,
  isUpcomingLiveDetail,
  videoAccessStatus,
} from "./detailPolicy.js";
import {
  incrementalVideoPlannerConfig,
  orderedDiscoveryAnchors,
  planRecentVideoSampling,
} from "./incrementalVideoPlanner.js";
import { fetchVideoYtDlpDetail } from "./youtube.js";
import { fetchYoutubeJsVideoDetail } from "./youtubeJs.js";
import { resolveYoutubeContentType } from "./youtubeContentType.js";
import { fullVideoStorageAction } from "./fullVideoContentStore.js";
import {
  resolveVideoDisposition,
  videoAccessRecheckAt,
  videoDispositionSummary,
} from "./videoDisposition.js";
import {
  assertYoutubeContentObservation,
  isYoutubeCollectionFailureError,
} from "./youtubePlayability.js";
import { shouldReportProxyFailure } from "./youtubeFailurePolicy.js";
import { reconcilePublication } from "./publicationReconciler.js";
import { applyVideoActivityLifecycle } from "./videoActivityLifecycle.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";

const GAP_ABANDONMENT_STOP_REASON = "gap_abandoned_latest_30";
const GAP_ABANDONMENT_POLICY_VERSION = "latest-30-on-catchup-limit-v1";
const GAP_ABANDONMENT_ITEM_LIMIT = 30;
const EXPLICIT_CONTENT_ACCESS_STATUSES = new Set([
  "unlisted",
  "members_only",
  "private",
  "unavailable",
]);
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
  const day = text(entry?.published_day);
  if (!day || !/^\d{4}-\d{2}-\d{2}$/.test(day)) return null;
  const parsed = new Date(`${day}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== day) return null;
  return {
    published_at: parsed.toISOString(),
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads",
  };
}

function detailSource(detail) {
  const sources = [
    detail?.published_at_source,
    detail?.view_count_source,
    detail?.duration_source,
    detail?.like_count_source,
    detail?.comment_count_source,
    detail?.comments_status_source,
  ].map((value) => String(value ?? "").toLowerCase());
  return detail?.ytdlp_client || sources.some((value) => value.includes("yt_dlp"))
    ? "yt_dlp"
    : "youtubejs_player";
}

function detailFacts(detail) {
  if (!detail) return null;
  const source = detailSource(detail);
  return {
    title: text(detail.title),
    thumbnail_url: text(detail.thumbnail_url),
    published_at: publishedAt(detail),
    published_at_precision: ["second", "date_only"].includes(detail.published_at_precision)
      ? detail.published_at_precision
      : "unknown",
    published_at_source: text(detail.published_at_source) ?? source,
    view_count: detailViewCount(detail),
    view_count_source: text(detail.view_count_source) ?? source,
    like_count: integer(detail.like_count),
    like_count_source: text(detail.like_count_source) ?? source,
    comment_count: integer(detail.comment_count),
    comment_count_source: text(detail.comment_count_source ?? detail.comments_status_source) ?? source,
    comments_disabled: detail.comments_disabled == null
      ? null
      : detail.comments_disabled === true,
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

export async function fetchIncrementalVideoDetail(videoId, {
  fetchYoutubeJs = fetchYoutubeJsVideoDetail,
  fetchYtDlp = fetchVideoYtDlpDetail,
  signal = null,
} = {}) {
  const throwIfAborted = () => {
    if (!signal?.aborted) return;
    throw signal.reason instanceof Error
      ? signal.reason
      : new Error("incremental Video detail fetch was aborted");
  };
  throwIfAborted();
  const url = `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`;
  let youtubeJsDetail = null;
  try {
    youtubeJsDetail = assertYoutubeContentObservation(await fetchYoutubeJs(videoId, { signal }), {
      videoId,
      source: "youtubejs_player",
    });
    throwIfAborted();
    const classification = resolveYoutubeContentType({ videoId, detail: youtubeJsDetail });
    if (
      EXPLICIT_CONTENT_ACCESS_STATUSES.has(detailAccess(youtubeJsDetail))
      ||
      isUpcomingLiveDetail(youtubeJsDetail)
      || (
        classification?.authoritative === true
        && detailViewCount(youtubeJsDetail) != null
        && publishedAt(youtubeJsDetail) != null
      )
    ) {
      return youtubeJsDetail;
    }
  } catch (youtubeJsError) {
    throwIfAborted();
    try {
      const detail = assertYoutubeContentObservation(await fetchYtDlp(videoId, url, { signal }), {
        videoId,
        source: "yt_dlp_detail",
      });
      throwIfAborted();
      return detail;
    } catch (ytDlpError) {
      throwIfAborted();
      if (isYoutubeCollectionFailureError(ytDlpError)) throw ytDlpError;
      throw new AggregateError(
        [youtubeJsError, ytDlpError],
        `incremental Video detail failed for ${videoId}: YouTube.js and yt-dlp both failed`,
      );
    }
  }
  throwIfAborted();
  try {
    const detail = assertYoutubeContentObservation(await fetchYtDlp(videoId, url, { signal }), {
      videoId,
      source: "yt_dlp_detail",
    });
    throwIfAborted();
    return detail;
  } catch (ytDlpError) {
    throwIfAborted();
    if (isYoutubeCollectionFailureError(ytDlpError)) throw ytDlpError;
    throw new AggregateError(
      [new Error("YouTube.js detail was incomplete"), ytDlpError],
      `incremental Video detail failed for ${videoId}: YouTube.js was incomplete and yt-dlp failed`,
    );
  }
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

async function captureDetails(entries, fetchDetail, limit) {
  const output = new Map();
  for (const entry of entries.slice(0, Math.max(0, limit))) {
    try {
      output.set(entry.id, { detail: await fetchDetail(entry.id), error: null });
    } catch (error) {
      if (shouldReportProxyFailure({ error })) throw error;
      output.set(entry.id, { detail: null, error });
    }
  }
  return output;
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

function clockContentEnrichLeaseOwner({ runId, executionAttemptId }) {
  const digest = createHash("sha256")
    .update(`${runId}:${executionAttemptId}`)
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

async function releaseClockContentEnrichReservations(withTransaction, preparedSampling) {
  const fences = [...(preparedSampling?.preparedCaptures?.values?.() ?? [])]
    .map((prepared) => prepared?.fence)
    .filter(Boolean);
  if (fences.length === 0) return 0;
  return withTransaction(async (client) => {
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
  });
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
  const terminalReason = entry.is_upcoming === true || isUpcomingLiveDetail(detail)
    ? "upcoming_live"
    : entry.is_live === true || isLiveInProgress(detail) ? "live_in_progress" : null;
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
      ...(scanIncomplete ? { discovery_deferred: discoveryDeferred } : {}),
      extractor: {
        source: detail ? detailSource(detail) : null,
        client: text(detail?.ytdlp_client),
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
      publishedAt: facts?.published_at ?? uploadFacts?.published_at ?? null,
      publishedAtPrecision: facts?.published_at
        ? facts.published_at_precision
        : uploadFacts?.published_at_precision ?? "unknown",
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
  const published = facts?.published_at ?? uploadFacts?.published_at ?? null;
  const publishedPrecision = facts?.published_at
    ? facts.published_at_precision
    : uploadFacts?.published_at_precision ?? "unknown";
  const publishedSource = facts?.published_at
    ? facts.published_at_source
    : uploadFacts?.published_at_source ?? null;
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
         published_at=COALESCE(EXCLUDED.published_at,crawler.contents.published_at),
         published_at_status=CASE WHEN EXCLUDED.published_at IS NOT NULL THEN 'exact' ELSE crawler.contents.published_at_status END,
         published_at_source=COALESCE(EXCLUDED.published_at_source,crawler.contents.published_at_source),
         published_at_precision=CASE
           WHEN EXCLUDED.published_at IS NOT NULL THEN EXCLUDED.published_at_precision
           ELSE crawler.contents.published_at_precision END,
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
         comment_count=CASE WHEN EXCLUDED.comments_disabled THEN NULL ELSE COALESCE(EXCLUDED.comment_count,crawler.contents.comment_count) END,
         comment_count_status=CASE
           WHEN EXCLUDED.comments_disabled THEN 'disabled'
           WHEN EXCLUDED.comment_count IS NOT NULL THEN 'exact'
           ELSE crawler.contents.comment_count_status END,
         comments_disabled=CASE
           WHEN $41::boolean AND EXCLUDED.comments_disabled IS NOT NULL
             THEN EXCLUDED.comments_disabled
           ELSE crawler.contents.comments_disabled END,
         comment_count_source=COALESCE(EXCLUDED.comment_count_source,crawler.contents.comment_count_source),
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
         raw_json=crawler.contents.raw_json || EXCLUDED.raw_json,
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
      published == null ? "unresolved" : "exact",
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
      facts?.comments_disabled ? null : facts?.comment_count ?? null,
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
      extractor: {
        source: detail ? detailSource(detail) : null,
        client: text(detail?.ytdlp_client),
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
    publishedAtPrecision: publishedPrecision,
  };
}

async function checkpointFirstSeenEnrichFailures({
  plan,
  runId,
  candidateEntries,
  captures,
  withTransaction,
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
  return withTransaction(async (client) => {
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
  });
}

async function loadPendingFirstSeenCheckpoints(query, { channelId }) {
  const pending = await query(
    `SELECT candidate.candidate_id,candidate.source_content_id AS video_id,
            candidate.position,candidate.title,candidate.thumbnail_url,
            candidate.result_json,
            content.content_key,content.content_type,
            content.published_at,content.published_at_precision
     FROM crawler.content_candidates candidate
     JOIN crawler.contents content
       ON content.content_key=candidate.content_key
      AND content.channel_id=candidate.channel_id
      AND content.run_id=candidate.run_id
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
      publishedAtPrecision: text(row.published_at_precision) ?? "unknown",
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
  const scanInput = scan.entries.map((entry) => ({
    video_id: entry.id,
    position: entry.position,
    published_at: uploadsPublishedFacts(entry)?.published_at ?? null,
  }));
  if (scanInput.length > 0) {
    await transactionClient.query(
           `WITH input AS (
             SELECT * FROM jsonb_to_recordset($3::jsonb)
               AS item(video_id text,position integer,published_at timestamptz)
           )
           UPDATE crawler.contents content
           SET playlist_last_seen_at=$2,last_seen_at=GREATEST(content.last_seen_at,$2::timestamptz),
               position=input.position,
               published_at=COALESCE(content.published_at,input.published_at),
               published_at_status=CASE
                 WHEN content.published_at IS NULL AND input.published_at IS NOT NULL THEN 'exact'
                 ELSE content.published_at_status END,
               published_at_source=CASE
                 WHEN content.published_at IS NULL AND input.published_at IS NOT NULL THEN 'youtube_uploads'
                 ELSE content.published_at_source END,
               published_at_precision=CASE
                 WHEN content.published_at IS NULL AND input.published_at IS NOT NULL THEN 'date_only'
                 ELSE content.published_at_precision END,
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
      published_at_precision: current.publishedAtPrecision,
    });
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
      published_at_precision: current.publishedAtPrecision,
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
  const commentsObserved = facts.comments_disabled != null || facts.comment_count != null;
  const isRecent = facts.published_at == null
    ? null
    : new Date(facts.published_at).getTime() >= new Date(observedAt).getTime() - (30 * 86400000);
  await client.query(
    `UPDATE crawler.contents
     SET content_type=CASE WHEN $36::text IS NULL THEN content_type ELSE $36 END,
         content_type_source=CASE WHEN $36::text IS NULL THEN content_type_source ELSE $37 END,
         url=CASE WHEN $36::text IS NULL THEN url ELSE $38 END,
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
         published_at=COALESCE($19::timestamptz,published_at),
         published_at_status=CASE WHEN $19::timestamptz IS NULL THEN published_at_status ELSE 'exact' END,
         published_at_source=CASE WHEN $19::timestamptz IS NULL THEN published_at_source ELSE $20 END,
         published_at_precision=CASE WHEN $19::timestamptz IS NULL THEN published_at_precision ELSE $21 END,
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
           WHEN $35::boolean AND $6::boolean THEN NULL
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
             'source',$34::text
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
      facts.title,
      facts.thumbnail_url,
      facts.description,
      facts.description_observed,
      facts.description_source,
      facts.hashtags,
      facts.hashtags_observed,
      facts.keywords,
      facts.keywords_observed,
      facts.published_at,
      facts.published_at_source,
      facts.published_at_precision,
      isRecent,
      facts.duration_seconds,
      facts.duration_source,
      facts.view_count_source,
      facts.like_count_source,
      facts.comment_count_source,
      facts.access_status,
      facts.access_status_source,
      facts.extractor_version,
      facts.live_scheduled_at,
      facts.live_started_at,
      facts.live_ended_at,
      detailSource(detail),
      commentsObserved,
      storageAction.kind === "upsert" ? storageAction.content_type : null,
      storageAction.kind === "upsert" ? storageAction.type_source : null,
      storageAction.kind === "upsert" ? classification.canonical_url : null,
      facts.comments_first_page == null ? null : JSON.stringify(facts.comments_first_page),
      detailMetadataKey,
    ],
  );
  return {
    success: true,
    viewDelta,
    engagementChanged,
    changeProbability,
    accessStatus: facts.access_status,
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
    published_at: row.sampling_published_at ?? row.published_at,
  }));
}

async function prepareClockRecentSampling({
  plan,
  runId,
  scanEntries,
  excludeVideoIds,
  samplingPlanInput,
  config,
  fetchDetail,
  withTransaction,
  executionAttemptId,
  observedAt,
}) {
  const leaseOwner = clockContentEnrichLeaseOwner({ runId, executionAttemptId });
  return withTransaction(async (client) => {
    const enrichMode = await loadContentEnrichMode(client, { lock: true });
    const recentRows = await loadClockRecentSamplingRows(client, {
      channelId: plan.channel_id,
      planDay: plan.plan_day,
      recentWindowDays: config.recentWindowDays,
      clockOwnsPlayerRefresh: enrichMode === CONTENT_ENRICH_CLOCK_MODE,
      scanEntries,
    });
    const samplePlan = planRecentVideoSampling(recentRows, {
      plan: samplingPlanInput,
      config,
      excludeVideoIds,
      now: new Date(observedAt),
    });
    const captures = await captureDetails(
      samplePlan.rows.map((row) => ({ id: row.source_content_id })),
      fetchDetail,
      samplePlan.rows.length,
    );
    const preparedCaptures = new Map();
    for (const row of samplePlan.rows) {
      const capture = captures.get(row.source_content_id);
      const prepared = await prepareClockContentEnrichOutcome(client, {
        contentKey: row.content_key,
        jobType: "player-refresh",
        detail: capture?.detail ?? null,
        error: capture?.error ?? null,
      });
      if (prepared.skipped) {
        preparedCaptures.set(row.content_key, { state: "skipped", fence: null });
        continue;
      }
      if (prepared.outcome.detail == null) {
        await persistClockContentEnrichOutcome(client, {
          ...prepared,
          contentKey: row.content_key,
          channelId: row.channel_id,
          runId,
          observationId: null,
          jobType: "player-refresh",
          observedAt,
        });
        preparedCaptures.set(row.content_key, {
          state: "checkpointed",
          outcome: prepared.outcome,
          fence: null,
        });
        continue;
      }
      const fence = await reserveClockContentEnrichPublication(client, {
        contentKey: row.content_key,
        currentTask: prepared.currentTask,
        leaseOwner,
        outcome: prepared.outcome,
      });
      preparedCaptures.set(row.content_key, {
        state: "publication",
        outcome: prepared.outcome,
        fence,
      });
    }
    return { samplePlan, preparedCaptures };
  });
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
  for (const row of locked.rows) {
    const spec = planned.get(row.content_key);
    const prepared = preparedCaptures.get(row.content_key);
    if (prepared?.state !== "publication") {
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
    const applied = await applyIncrementalVideoDetail(transactionClient, {
      row,
      detail: prepared.outcome.detail,
      observedAt,
      observationId,
      collectNext: spec?.collect_next === true,
      changeAlpha,
    });
    await persistClockContentEnrichOutcome(transactionClient, {
      currentTask: ownership.currentTask,
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
    summary: {
      recent_count: payload.recent_count,
      candidate_count: samplePlan.candidate_count,
      selected_count: payload.selected_count,
      success_count: successCount,
      failure_count: failureCount,
    },
  };
}

async function recordVideoCycle({
  plan,
  runId,
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
  executionAttemptId,
}) {
  const commandEntries = scan.entries.map((entry) => ({
    video_id: entry.id,
    position: entry.position,
    content_type: entry.content_type,
    published_at: detailFacts(discoveryCaptures.get(entry.id)?.detail)?.published_at
      ?? uploadsPublishedFacts(entry)?.published_at
      ?? null,
  }));
  let recorded = null;
  let transactionError = null;
  try {
    recorded = await withTransaction(async (client) => {
      const observation = await recordCrawlerObservation(client, {
        idempotencyKey: `video:${runId}:${executionAttemptId}`,
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
          });
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
                recent_published_content_count: lifecycle.recent_published_content_count,
                conclusive: lifecycle.conclusive,
              },
            },
            payload: {
              discovery: { outcome: discovery.outcome, payload: discovery.payload },
              recent_sampling: {
                outcome: recentSampling.outcome,
                payload: recentSampling.payload,
              },
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
      if (scan.complete === true) {
        await reconcilePublication(client, {
          channelId: plan.channel_id,
          domains: ["channel", "video"],
          asOf: observedAt,
        });
      }
      return observation;
    });
  } catch (error) {
    transactionError = error;
  }
  let reservationCleanupDeferred = false;
  try {
    await releaseClockContentEnrichReservations(withTransaction, preparedSampling);
  } catch (releaseError) {
    if (transactionError && (
      typeof transactionError === "object" || typeof transactionError === "function"
    )) {
      transactionError.clock_content_enrich_release_error = releaseError;
    } else {
      reservationCleanupDeferred = true;
    }
  }
  if (transactionError) throw transactionError;
  return reservationCleanupDeferred
    ? { ...recorded, reservation_cleanup_deferred: true }
    : recorded;
}

export async function executeIncrementalVideo({
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
  if (typeof now !== "function") throw new TypeError("now must be a function");
  const config = incrementalVideoPlannerConfig(plan);
  const observedAtValue = new Date(now());
  if (Number.isNaN(observedAtValue.getTime())) throw new TypeError("now must return a valid date");
  const observedAt = observedAtValue.toISOString();
  const detailFetcher = fetchDetail ?? fetchIncrementalVideoDetail;
  const executionAttemptId = currentChannelExecution()?.attempt_id
    ?? `job-attempt:${plan.job_id}`;
  const anchors = await loadDiscoveryAnchors(query, plan.channel_id);
  const snapshot = await getChannelSnapshot();
  const rawScan = await snapshot.scanUploads({
    anchors,
    maxPages: config.discoveryMaxPages,
    catchUpMaxItems: config.discoveryCatchUpMaxItems,
  });
  const scan = applyCatchupGapAbandonment(rawScan, anchors, {
    catchUpMaxItems: config.discoveryCatchUpMaxItems,
  });
  const storedVideoPlayerBudget = Math.floor(plan.capacity.player_cap * plan.capacity.factor);
  const ids = scan.entries.map((entry) => entry.id);
  const known = await knownVideoIds(query, plan.channel_id, ids);
  const latestDispositions = await latestVideoDispositionEntries(
    query,
    plan.channel_id,
    ids.filter((id) => !known.has(id)),
  );
  const scannedWork = scannedVideoDispositionWork(
    scan.entries.filter((entry) => !known.has(entry.id)),
    latestDispositions,
    observedAt,
    { allowDueRechecks: scan.complete === true },
  );
  const samplingPlanInput = {
    ...plan,
    capacity: {
      ...plan.capacity,
      factor: 1,
      player_cap: storedVideoPlayerBudget,
    },
  };
  let recorded;
  if (scan.complete !== true) {
    recorded = await recordVideoCycle({
      plan,
      runId,
      scan,
      discoveryEntries: scannedWork.workEntries,
      discoveryCaptures: new Map(),
      checkpointedFirstSeen: [],
      pendingDeferredVideoIds: scannedWork.pendingDeferredVideoIds,
      anchors,
      preparedSampling: null,
      samplingPlanInput,
      config,
      withTransaction,
      startedAt,
      observedAt,
      crawlerVersion,
      executionAttemptId,
    });
  } else {
    const recoveredFirstSeen = await loadPendingFirstSeenCheckpoints(query, {
      channelId: plan.channel_id,
    });
    const dueDispositionEntries = await loadDueVideoDispositionEntries(
      query,
      plan.channel_id,
      observedAt,
      scan.entries,
    );
    const discoveryEntries = [...scannedWork.workEntries, ...dueDispositionEntries];
    const detailEligibleFirstSeen = discoveryEntries.filter(
      (entry) => entry.disposition_recheck
        || (entry.is_upcoming !== true && entry.is_live !== true),
    );
    const discoveryCaptures = await captureDetails(
      detailEligibleFirstSeen,
      detailFetcher,
      detailEligibleFirstSeen.length,
    );
    const newlyCheckpointedFirstSeen = await checkpointFirstSeenEnrichFailures({
      plan,
      runId,
      candidateEntries: discoveryEntries,
      captures: discoveryCaptures,
      withTransaction,
      observedAt,
    });
    const checkpointedFirstSeen = [
      ...recoveredFirstSeen,
      ...newlyCheckpointedFirstSeen,
    ];
    const preparedSampling = await prepareClockRecentSampling({
      plan,
      runId,
      scanEntries: scan.entries,
      excludeVideoIds: discoveryEntries.map((entry) => entry.id),
      samplingPlanInput,
      config,
      fetchDetail: detailFetcher,
      withTransaction,
      executionAttemptId,
      observedAt,
    });
    recorded = await recordVideoCycle({
      plan,
      runId,
      scan,
      discoveryEntries,
      discoveryCaptures,
      checkpointedFirstSeen,
      pendingDeferredVideoIds: scannedWork.pendingDeferredVideoIds,
      anchors,
      preparedSampling,
      samplingPlanInput,
      config,
      withTransaction,
      startedAt,
      observedAt,
      crawlerVersion,
      executionAttemptId,
    });
  }
  return {
    outcome: recorded.outcome,
    observation_id: recorded.observation_id ?? null,
    event_id: recorded.event_id ?? null,
    kind_sequence: recorded.kind_sequence ?? null,
    first_seen_count: recorded.result?.firstSeen?.length ?? 0,
    selected_count: recorded.result?.selectedCount ?? 0,
    lifecycle_status: recorded.result?.lifecycleStatus ?? null,
    dormant_recheck_day: recorded.result?.dormantRecheckDay ?? null,
    ...(recorded.reservation_cleanup_deferred === true
      ? { reservation_cleanup_deferred: true }
      : {}),
  };
}
