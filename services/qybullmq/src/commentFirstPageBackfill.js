import {
  commentFirstPageNeedsResolution,
  commentPageHasFirstPage,
  commentPageResolutionStatus,
} from "./youtubeCommentPage.js";
import { currentChannelExecutionAbortSignal } from "./channelExecutionContext.js";
import { throwIfAborted } from "./abortSignal.js";
import { fetchVideoYtDlpDetail } from "./youtube.js";

export const COMMENT_FIRST_PAGE_BACKFILL_SOURCE = "comment_first_page_backfill_v1";
export const DEFAULT_COMMENT_KEEP_BATCH_ID = "youtubejs-comment-keep-20-20260817-v1";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function nonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}

export function commentFirstPageNeedsBackfill(page, {
  commentsDisabled = null,
  commentCountStatus = null,
  commentCount = null,
} = {}) {
  if (commentsDisabled === true || commentCountStatus === "disabled") return false;
  if (page == null) return true;
  if (!commentPageHasFirstPage(page)) return true;
  if (Number(page.returned_count) > 0) return false;
  if (commentCountStatus === "zero_from_surface" || commentCountStatus === "zero_from_empty") {
    return false;
  }
  if (commentPageResolutionStatus(page) === "confirmed_no_visible_threads") {
    return commentFirstPageNeedsResolution({
      comment_count: nonnegativeInteger(commentCount) ?? page?.total_count,
      comments_disabled: commentsDisabled,
      comment_count_status: commentCountStatus,
      comments_first_page: page,
    });
  }
  return Number(page.total_count) > 0 || commentCountStatus === "unresolved" || page.total_count == null;
}

export function shouldPersistCommentBackfill(detail, { existingCommentCount = null } = {}) {
  if (!detail || typeof detail !== "object") return false;
  if (detail.youtubejs_comments_error) return false;
  if (!commentPageHasFirstPage(detail.comments_first_page)) return false;
  if (Number(detail.comments_first_page.returned_count) > 0) return true;
  if ((nonnegativeInteger(existingCommentCount) ?? 0) > 0) return false;
  if (detail.comments_disabled === true) return true;
  if (commentPageResolutionStatus(detail.comments_first_page) === "confirmed_no_visible_threads") return true;
  return ["disabled", "zero_from_surface", "zero_from_empty"].includes(detail.comment_count_status);
}

export function loadCommentBackfillTargetsSql({ selection = "batch", sharded = false } = {}) {
  if (!new Set(["batch", "plan_day"]).has(selection)) {
    throw new TypeError("selection must be batch or plan_day");
  }
  const shardClause = sharded
    ? "\n         AND mod(abs(hashtext(content.content_key)::bigint), $3::integer) = $4::integer"
    : "";
  const source = selection === "plan_day"
    ? `SELECT DISTINCT ON (content.content_key)
         content.content_key,
         content.channel_id,
         content.source_content_id,
         content.run_id,
         content.comment_count,
         content.comment_count_status,
         content.comments_disabled,
         content.comments_first_page
       FROM crawler.channel_runs run
       JOIN crawler.contents content
         ON content.run_id = run.run_id
       WHERE run.crawl_mode = 'incremental'
         AND run.plan_day = $1::date
         AND content.first_seen_at >= $1::date
         AND content.first_seen_at < ($1::date + 1)${shardClause}
       ORDER BY content.content_key, run.started_at DESC NULLS LAST`
    : `SELECT DISTINCT ON (content.channel_id, content.source_content_id)
         content.content_key,
         content.channel_id,
         content.source_content_id,
         content.run_id,
         content.comment_count,
         content.comment_count_status,
         content.comments_disabled,
         content.comments_first_page
       FROM crawler.channel_runs run
       JOIN crawler.content_candidates candidate
         ON candidate.run_id = run.run_id
       JOIN crawler.contents content
         ON content.channel_id = candidate.channel_id
        AND content.source_content_id = candidate.source_content_id
       WHERE COALESCE(run.result_json->>'dispatch_batch_id', run.result_json->>'pipeline_cycle_id') = $1${shardClause}
       ORDER BY content.channel_id, content.source_content_id, content.last_enriched_at DESC NULLS LAST`;
  return `
WITH latest AS (
  ${source}
)
SELECT target.*
FROM latest target
WHERE (
        target.comments_first_page IS NULL
        OR jsonb_typeof(target.comments_first_page->'comments') <> 'array'
        OR COALESCE((target.comments_first_page->>'returned_count')::int, 0) = 0
      )
  AND COALESCE(target.comments_disabled, false) = false
  AND COALESCE(target.comment_count_status, 'unresolved')
      NOT IN ('disabled', 'zero_from_surface', 'zero_from_empty')
  AND NOT (
    target.comments_first_page#>>'{resolution,status}' = 'confirmed_no_visible_threads'
    AND target.comment_count IS NOT NULL
    AND CASE
          WHEN COALESCE(target.comments_first_page#>>'{resolution,observed_comment_count}','') ~ '^[0-9]+$'
          THEN (target.comments_first_page#>>'{resolution,observed_comment_count}')::bigint = target.comment_count
          ELSE false
        END
    AND CASE
          WHEN pg_input_is_valid(
                 target.comments_first_page#>>'{resolution,next_retry_at}',
                 'timestamp with time zone'
               )
          THEN (target.comments_first_page#>>'{resolution,next_retry_at}')::timestamptz > now()
          ELSE false
        END
  )
ORDER BY target.channel_id, target.source_content_id
LIMIT $2
`;
}

export async function loadCommentBackfillTargets(queryFn, {
  batchId,
  planDay = null,
  limit = 500,
  shardCount = 1,
  shardIndex = 0,
} = {}) {
  if (typeof queryFn !== "function") throw new TypeError("query function is required");
  const normalizedPlanDay = text(planDay);
  const normalizedBatchId = text(
    batchId === undefined && normalizedPlanDay == null ? DEFAULT_COMMENT_KEEP_BATCH_ID : batchId,
  );
  if (normalizedBatchId && normalizedPlanDay) {
    throw new TypeError("batchId and planDay are mutually exclusive");
  }
  if (!normalizedBatchId && !normalizedPlanDay) {
    throw new TypeError("batchId or planDay is required");
  }
  if (normalizedPlanDay && !/^\d{4}-\d{2}-\d{2}$/.test(normalizedPlanDay)) {
    throw new TypeError("planDay must use YYYY-MM-DD");
  }
  const normalizedLimit = Number(limit);
  if (!Number.isInteger(normalizedLimit) || normalizedLimit < 1 || normalizedLimit > 10_000) {
    throw new TypeError("limit must be an integer between 1 and 10000");
  }
  const normalizedShardCount = Number(shardCount);
  const normalizedShardIndex = Number(shardIndex);
  if (!Number.isInteger(normalizedShardCount) || normalizedShardCount < 1 || normalizedShardCount > 100) {
    throw new TypeError("shardCount must be an integer between 1 and 100");
  }
  if (!Number.isInteger(normalizedShardIndex)
      || normalizedShardIndex < 0
      || normalizedShardIndex >= normalizedShardCount) {
    throw new TypeError("shardIndex must be an integer between 0 and shardCount - 1");
  }
  const selection = normalizedPlanDay ? "plan_day" : "batch";
  const selector = normalizedPlanDay ?? normalizedBatchId;
  const sharded = normalizedShardCount > 1;
  const params = sharded
    ? [selector, normalizedLimit, normalizedShardCount, normalizedShardIndex]
    : [selector, normalizedLimit];
  const result = await queryFn(loadCommentBackfillTargetsSql({ selection, sharded }), params);
  return (result?.rows ?? []).filter((row) => commentFirstPageNeedsBackfill(row.comments_first_page, {
    commentsDisabled: row.comments_disabled,
    commentCountStatus: row.comment_count_status,
    commentCount: row.comment_count,
  }));
}

export async function persistCommentFirstPage(client, {
  contentKey,
  channelId,
  sourceContentId,
  detail,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const storedKey = text(contentKey);
  const storedChannelId = text(channelId);
  const storedSourceId = text(sourceContentId);
  if (!storedKey || !storedChannelId || !storedSourceId) {
    throw new TypeError("content_key, channel_id and source_content_id are required");
  }
  if (!shouldPersistCommentBackfill(detail)) {
    return { updated: false, reason: "comment_page_unresolved" };
  }
  const stored = await client.query(
    `UPDATE crawler.contents
     SET comments_first_page = $4::jsonb,
         last_enriched_at = now(),
         raw_json = raw_json || $5::jsonb
     WHERE content_key = $1
       AND channel_id = $2
       AND source_content_id = $3
       AND COALESCE((comments_first_page->>'returned_count')::int, 0) = 0
       AND (
         COALESCE(($4::jsonb->>'returned_count')::int, 0) > 0
         OR COALESCE(comment_count, 0) = 0
       )
     RETURNING content_key`,
    [
      storedKey,
      storedChannelId,
      storedSourceId,
      JSON.stringify(detail.comments_first_page),
      JSON.stringify({
        source: COMMENT_FIRST_PAGE_BACKFILL_SOURCE,
        youtubejs_comments_error: detail.youtubejs_comments_error ?? null,
      }),
    ],
  );
  return {
    updated: stored.rowCount === 1,
    content_key: stored.rows[0]?.content_key ?? null,
    reason: stored.rowCount === 1 ? "updated" : "stale_or_conflicting_row",
  };
}

export async function backfillOneCommentFirstPage(target, {
  fetchDetail = fetchVideoYtDlpDetail,
  persist = persistCommentFirstPage,
  client,
} = {}) {
  const videoId = text(target?.source_content_id);
  if (!videoId) throw new TypeError("target.source_content_id is required");
  const signal = currentChannelExecutionAbortSignal();
  throwIfAborted(signal);
  let detail;
  try {
    detail = await fetchDetail(videoId, { signal });
    throwIfAborted(signal);
  } catch (error) {
    throwIfAborted(signal);
    return {
      content_key: target.content_key,
      channel_id: target.channel_id,
      source_content_id: videoId,
      updated: false,
      reason: "comment_detail_error",
      comment_count: null,
      returned_count: null,
      comments_error: String(error?.message || error),
    };
  }
  if (!shouldPersistCommentBackfill(detail, { existingCommentCount: target.comment_count })) {
    const storedPositiveCount = (nonnegativeInteger(target.comment_count) ?? 0) > 0;
    const returnedCount = nonnegativeInteger(detail?.comments_first_page?.returned_count) ?? 0;
    return {
      content_key: target.content_key,
      channel_id: target.channel_id,
      source_content_id: videoId,
      updated: false,
      reason: storedPositiveCount && returnedCount === 0
        ? "comment_page_conflicts_with_stored_count"
        : detail?.youtubejs_comments_error ? "youtubejs_comments_error" : "comment_page_unresolved",
      comment_count: detail?.comment_count ?? null,
      returned_count: detail?.comments_first_page?.returned_count ?? null,
      comments_error: detail?.youtubejs_comments_error ?? null,
    };
  }
  const persisted = await persist(client, {
    contentKey: target.content_key,
    channelId: target.channel_id,
    sourceContentId: videoId,
    detail,
  });
  return {
    content_key: target.content_key,
    channel_id: target.channel_id,
    source_content_id: videoId,
    updated: persisted.updated,
    reason: persisted.reason,
    comment_count: detail.comment_count ?? null,
    returned_count: detail.comments_first_page?.returned_count ?? null,
    comments_error: detail.youtubejs_comments_error ?? null,
  };
}
