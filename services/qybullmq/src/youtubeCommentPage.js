import { parseLocalizedCount } from "./localizedCount.js";
import { localizedEstimatedUtcTimestamp } from "./localizedTime.js";

const DEFAULT_LANGUAGE = process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en";

function text(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") {
    const output = value.trim();
    return !output || output === "N/A" ? null : output;
  }
  if (typeof value?.text === "string") {
    const output = value.text.trim();
    return !output || output === "N/A" ? null : output;
  }
  if (typeof value?.simpleText === "string") return value.simpleText.trim() || null;
  if (Array.isArray(value?.runs)) {
    const output = value.runs.map((run) => run?.text || "").join("").trim();
    return output || null;
  }
  if (typeof value?.content === "string") return value.content.trim() || null;
  if (typeof value?.toString === "function") {
    try {
      const rendered = value.toString();
      if (typeof rendered === "string") {
        const output = rendered.trim();
        if (output && output !== "[object Object]" && output !== "N/A") return output;
      }
    } catch {
      return null;
    }
  }
  return null;
}

function absoluteYoutubeUrl(value) {
  const path = String(value ?? "").trim();
  if (!path) return null;
  if (/^https?:\/\//i.test(path)) return path;
  if (path.startsWith("//")) return `https:${path}`;
  return `https://www.youtube.com${path.startsWith("/") ? "" : "/"}${path}`;
}

function bestImageUrl(value) {
  if (typeof value === "string") return absoluteYoutubeUrl(value);
  if (typeof value?.url === "string" && value.url.trim()) {
    return absoluteYoutubeUrl(value.url);
  }
  const sources = Array.isArray(value)
    ? value
    : Array.isArray(value?.sources)
      ? value.sources
      : Array.isArray(value?.thumbnails)
        ? value.thumbnails
        : [];
  return sources
    .filter((source) => typeof source?.url === "string" && source.url.trim())
    .sort((left, right) => (
      (Number(right?.width) || 0) * (Number(right?.height) || 0)
      - (Number(left?.width) || 0) * (Number(left?.height) || 0)
    ))
    .map((source) => absoluteYoutubeUrl(source.url))
    .find(Boolean) ?? null;
}

function allNodes(value, targetKey, output = []) {
  if (!value || typeof value !== "object") return output;
  if (Array.isArray(value)) {
    for (const item of value) allNodes(item, targetKey, output);
    return output;
  }
  for (const [key, child] of Object.entries(value)) {
    if (key === targetKey) output.push(child);
    allNodes(child, targetKey, output);
  }
  return output;
}

function firstNode(value, targetKey) {
  return allNodes(value, targetKey, [])[0] ?? null;
}

function nonnegativeCount(value, { locale = DEFAULT_LANGUAGE, missing = null } = {}) {
  const rendered = text(value);
  if (rendered == null) return missing;
  return parseLocalizedCount(rendered, { locale });
}

function commentSection(raw) {
  return allNodes(raw, "itemSectionRenderer")
    .find((section) => section?.targetId === "comments-section"
      || section?.sectionIdentifier === "comments-section") ?? null;
}

function explicitDisabledSignal(value, commentContext = false) {
  if (typeof value === "string") {
    return /comments? (?:are |have been )?(?:turned off|disabled)/i.test(value)
      || /coment[aá]rios? (?:foram |est[aã]o )?(?:desativad|desabilitad)/i.test(value)
      || /commentaires? (?:sont |ont [eé]t[eé] )?(?:d[eé]sactiv)/i.test(value);
  }
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) {
    return value.some((item) => explicitDisabledSignal(item, commentContext));
  }
  return Object.entries(value).some(([key, child]) => (
    explicitDisabledSignal(child, commentContext || /comment/i.test(key))
  ));
}

export function youtubeCommentSurface(raw) {
  const section = commentSection(raw);
  const continuation = firstNode(section, "continuationItemRenderer")
    ?.continuationEndpoint?.continuationCommand?.token;
  if (typeof continuation === "string" && continuation.trim()) {
    return { status: "available", continuation: continuation.trim() };
  }
  return {
    status: explicitDisabledSignal(raw) ? "disabled" : "absent",
    continuation: null,
  };
}

export function shouldRequestYoutubeCommentPage({
  collect = false,
  surface = null,
} = {}) {
  return collect === true
    && surface?.status === "available"
    && typeof surface?.continuation === "string"
    && surface.continuation.length > 0;
}

function normalizedCollectedAt(value) {
  const date = value instanceof Date ? value : new Date(value ?? Date.now());
  if (Number.isNaN(date.getTime())) throw new TypeError("collectedAt must be a valid date");
  return date.toISOString();
}

function optionalNonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}

function optionalIsoTimestamp(value) {
  if (value === null || value === undefined || value === "") return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function commentAuthorUrl(author) {
  const path = author?.channelCommand?.innertubeCommand
    ?.commandMetadata?.webCommandMetadata?.url;
  return absoluteYoutubeUrl(path)
    ?? (author?.channelId ? `https://www.youtube.com/channel/${encodeURIComponent(author.channelId)}` : null);
}

function commentThreadNodes(raw) {
  const threads = allNodes(raw, "commentThreadRenderer");
  if (threads.length > 0) return threads;
  return allNodes(raw, "commentViewModel");
}

function commentEntities(raw) {
  const entities = allNodes(raw, "commentEntityPayload");
  const byKey = new Map();
  const byCommentId = new Map();
  for (const entity of entities) {
    const key = text(entity?.key);
    const commentId = text(entity?.properties?.commentId);
    if (key) byKey.set(key, entity);
    if (commentId) byCommentId.set(commentId, entity);
  }
  return { byKey, byCommentId };
}

function normalizeComment(thread, entities, position, { collectedAt, locale }) {
  const model = thread?.commentViewModel?.commentViewModel
    ?? thread?.commentViewModel
    ?? thread;
  const entity = entities.byKey.get(text(model?.commentKey))
    ?? entities.byCommentId.get(text(model?.commentId ?? thread?.commentId));
  const properties = entity?.properties ?? {};
  const author = entity?.author ?? {};
  const toolbar = entity?.toolbar ?? {};
  const commentId = text(properties.commentId ?? model?.commentId ?? thread?.commentId);
  const replyLevel = Number(properties.replyLevel ?? 0);
  if (!commentId || !Number.isFinite(replyLevel) || replyLevel !== 0) return null;
  if (!entity && text(properties?.content?.content ?? properties?.content ?? model?.content) == null) {
    return null;
  }
  const publishedTextRaw = text(properties.publishedTime);
  const publishedAtUtc = publishedTextRaw
    ? localizedEstimatedUtcTimestamp(publishedTextRaw, {
        locale,
        now: Date.parse(collectedAt),
      })
    : null;
  return {
    comment_id: commentId,
    position,
    text: text(properties?.content?.content ?? properties?.content),
    author_name: text(author.displayName),
    author_channel_id: text(author.channelId),
    author_url: commentAuthorUrl(author),
    author_avatar_url: bestImageUrl(author.avatarThumbnailUrl)
      ?? bestImageUrl(entity?.avatar?.image),
    published_at_utc: publishedAtUtc,
    published_text_raw: publishedTextRaw,
    published_at_status: publishedAtUtc ? "estimated_relative" : "unresolved",
    is_edited: properties.isEdited === true || /\(\s*edited\s*\)/i.test(publishedTextRaw || ""),
    like_count: nonnegativeCount(toolbar.likeCountNotliked, { locale, missing: 0 }),
    reply_count: nonnegativeCount(toolbar.replyCount, { locale, missing: 0 }),
    is_pinned: Boolean(model.pinnedText || /PINNED/i.test(String(thread?.renderingPriority || ""))),
    is_channel_owner: author.isCreator === true,
    is_verified: author.isVerified === true,
    is_hearted: Boolean(toolbar.heartActiveTooltip),
  };
}

export function emptyYoutubeCommentPage({
  collectedAt = new Date(),
  totalCount = 0,
} = {}) {
  return {
    version: 1,
    collected_at: normalizedCollectedAt(collectedAt),
    sort: "TOP_COMMENTS",
    total_count: Number.isSafeInteger(totalCount) && totalCount >= 0 ? totalCount : 0,
    returned_count: 0,
    comments: [],
  };
}

export function commentPageResolutionStatus(page) {
  const status = String(page?.resolution?.status ?? "").trim().toLowerCase();
  return [
    "collected",
    "zero_comments",
    "disabled",
    "confirmed_no_visible_threads",
    "unresolved",
  ].includes(status) ? status : null;
}

export function confirmedNoVisibleThreadsPage({
  collectedAt = new Date(),
  checkedAt = collectedAt,
  retryAt = new Date(new Date(checkedAt).getTime() + (7 * 24 * 60 * 60 * 1000)),
  totalCount = 0,
  sources = [],
} = {}) {
  const observedCount = optionalNonnegativeInteger(totalCount) ?? 0;
  const checkedAtUtc = normalizedCollectedAt(checkedAt);
  const retryAtUtc = normalizedCollectedAt(retryAt);
  return {
    ...emptyYoutubeCommentPage({ collectedAt, totalCount: observedCount }),
    resolution: {
      status: "confirmed_no_visible_threads",
      observed_comment_count: observedCount,
      checked_at: checkedAtUtc,
      next_retry_at: retryAtUtc,
      sources: [...new Set((Array.isArray(sources) ? sources : [])
        .map((source) => text(source))
        .filter(Boolean))],
    },
  };
}

export function commentFirstPageNeedsResolution(detail = {}, { now = new Date() } = {}) {
  if (detail?.comments_disabled === true || detail?.comment_count_status === "disabled") return false;
  const count = optionalNonnegativeInteger(detail?.comment_count);
  if (count === 0 || ["zero_from_surface", "zero_from_empty", "zero_from_upcoming"]
    .includes(String(detail?.comment_count_status ?? ""))) return false;
  const page = detail?.comments_first_page;
  if (commentPageHasFirstPage(page) && Number(page.returned_count) > 0) return false;
  const status = commentPageResolutionStatus(page);
  if (["zero_comments", "disabled"].includes(status)) return false;
  if (status !== "confirmed_no_visible_threads") {
    return count > 0 || commentPageTotalCount(page) > 0;
  }
  const observedCount = optionalNonnegativeInteger(page?.resolution?.observed_comment_count);
  if (count != null && observedCount != null && count !== observedCount) return true;
  const nextRetryAt = optionalIsoTimestamp(page?.resolution?.next_retry_at);
  const nowUtc = optionalIsoTimestamp(now);
  if (!nextRetryAt || !nowUtc) return true;
  return Date.parse(nowUtc) >= Date.parse(nextRetryAt);
}

export function commentPageFromDataApiThreads(raw, {
  collectedAt = new Date(),
  totalCount = null,
} = {}) {
  const collectedAtUtc = normalizedCollectedAt(collectedAt);
  const comments = [];
  const seen = new Set();
  for (const thread of Array.isArray(raw?.items) ? raw.items : []) {
    const topLevel = thread?.snippet?.topLevelComment ?? {};
    const snippet = topLevel?.snippet ?? {};
    const commentId = text(topLevel?.id);
    if (!commentId || seen.has(commentId)) continue;
    seen.add(commentId);
    const publishedAt = optionalIsoTimestamp(snippet.publishedAt);
    const updatedAt = optionalIsoTimestamp(snippet.updatedAt);
    comments.push({
      comment_id: commentId,
      position: comments.length + 1,
      text: text(snippet.textOriginal ?? snippet.textDisplay) ?? "",
      author_name: text(snippet.authorDisplayName),
      author_channel_id: text(snippet.authorChannelId?.value),
      author_url: text(snippet.authorChannelUrl),
      author_avatar_url: text(snippet.authorProfileImageUrl),
      published_at_utc: publishedAt,
      published_text_raw: text(snippet.publishedAt),
      published_at_status: publishedAt ? "exact" : "unresolved",
      is_edited: Boolean(publishedAt && updatedAt && publishedAt !== updatedAt),
      like_count: optionalNonnegativeInteger(snippet.likeCount) ?? 0,
      reply_count: optionalNonnegativeInteger(thread?.snippet?.totalReplyCount) ?? 0,
      is_pinned: null,
      is_channel_owner: null,
      is_verified: null,
      is_hearted: null,
    });
  }
  return {
    version: 1,
    collected_at: collectedAtUtc,
    sort: "TOP_COMMENTS",
    total_count: optionalNonnegativeInteger(totalCount),
    returned_count: comments.length,
    comments,
    resolution: {
      status: comments.length > 0 ? "collected" : "unresolved",
      checked_at: collectedAtUtc,
      sources: ["youtube_data_api_comment_threads"],
    },
  };
}

export function isYoutubeJsCommentsResult(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  if (value.version === 1 && value.sort === "TOP_COMMENTS" && Array.isArray(value.comments)) {
    return false;
  }
  if (!Array.isArray(value.contents)) return false;
  if (value.contents.length === 0) {
    return value.header != null || Object.prototype.hasOwnProperty.call(value, "header");
  }
  const first = value.contents[0];
  return first?.comment != null
    || first?.comment_id != null
    || first?.type === "CommentThread";
}

function normalizeGetCommentsComment(thread, position, { collectedAt, locale }) {
  const view = thread?.comment ?? thread;
  const commentId = text(view?.comment_id ?? view?.commentId);
  if (!commentId) return null;
  const publishedTextRaw = text(view?.published_time ?? view?.publishedTime);
  const publishedAtUtc = publishedTextRaw
    ? localizedEstimatedUtcTimestamp(publishedTextRaw, {
        locale,
        now: Date.parse(collectedAt),
      })
    : null;
  const author = view?.author ?? {};
  const authorId = text(author.id);
  const authorName = text(author.name);
  const authorUrl = text(author.url);
  return {
    comment_id: commentId,
    position,
    text: text(view?.content) ?? "",
    author_name: authorName,
    author_channel_id: authorId,
    author_url: authorUrl && !/\/N\/A$/i.test(authorUrl)
      ? authorUrl
      : (authorId ? `https://www.youtube.com/channel/${encodeURIComponent(authorId)}` : null),
    author_avatar_url: bestImageUrl(author.best_thumbnail)
      ?? bestImageUrl(author.thumbnails)
      ?? bestImageUrl(view?.creator_thumbnail_url),
    published_at_utc: publishedAtUtc,
    published_text_raw: publishedTextRaw,
    published_at_status: publishedAtUtc ? "estimated_relative" : "unresolved",
    is_edited: /\(\s*edited\s*\)/i.test(publishedTextRaw || ""),
    like_count: nonnegativeCount(view?.like_count ?? view?.likeCount, { locale, missing: 0 }),
    reply_count: nonnegativeCount(view?.reply_count ?? view?.replyCount, { locale, missing: 0 }),
    is_pinned: view?.is_pinned === true,
    is_channel_owner: view?.author_is_channel_owner === true || author.isCreator === true,
    is_verified: author.is_verified === true || author.isVerified === true,
    is_hearted: view?.is_hearted === true,
  };
}

export function youtubeCommentPageFromGetComments(comments, {
  collectedAt = new Date(),
  locale = DEFAULT_LANGUAGE,
  totalCount = null,
} = {}) {
  const collectedAtUtc = normalizedCollectedAt(collectedAt);
  const seen = new Set();
  const rows = [];
  for (const thread of Array.isArray(comments?.contents) ? comments.contents : []) {
    const comment = normalizeGetCommentsComment(thread, rows.length + 1, {
      collectedAt: collectedAtUtc,
      locale,
    });
    if (!comment || seen.has(comment.comment_id)) continue;
    seen.add(comment.comment_id);
    comment.position = rows.length + 1;
    rows.push(comment);
  }
  const header = comments?.header;
  const parsedTotal = nonnegativeCount(header?.countText ?? header?.count, { locale })
    ?? nonnegativeCount(header?.commentsCount ?? header?.comments_count, { locale });
  const fallbackTotal = Number.isSafeInteger(totalCount) && totalCount >= 0 ? totalCount : null;
  return {
    version: 1,
    collected_at: collectedAtUtc,
    sort: "TOP_COMMENTS",
    total_count: parsedTotal ?? fallbackTotal,
    returned_count: rows.length,
    comments: rows,
  };
}

export function normalizeYoutubeCommentPage(raw, {
  collectedAt = new Date(),
  locale = DEFAULT_LANGUAGE,
  totalCount = null,
} = {}) {
  if (isYoutubeJsCommentsResult(raw)) {
    return youtubeCommentPageFromGetComments(raw, { collectedAt, locale, totalCount });
  }
  const collectedAtUtc = normalizedCollectedAt(collectedAt);
  const entities = commentEntities(raw);
  const seen = new Set();
  const comments = [];
  for (const thread of commentThreadNodes(raw)) {
    const comment = normalizeComment(thread, entities, comments.length + 1, {
      collectedAt: collectedAtUtc,
      locale,
    });
    if (!comment || seen.has(comment.comment_id)) continue;
    seen.add(comment.comment_id);
    comment.position = comments.length + 1;
    comments.push(comment);
  }
  const header = firstNode(raw, "commentsHeaderRenderer")
    ?? firstNode(raw, "commentsHeaderViewModel");
  const parsedTotal = nonnegativeCount(header?.countText, { locale })
    ?? nonnegativeCount(header?.commentsCount, { locale })
    ?? nonnegativeCount(header?.count, { locale })
    ?? nonnegativeCount(header?.commentsCount?.content, { locale });
  const fallbackTotal = Number.isSafeInteger(totalCount) && totalCount >= 0 ? totalCount : null;
  const disabled = youtubeCommentsDisabled(raw);
  const surface = youtubeCommentSurface(raw);
  return {
    version: 1,
    collected_at: collectedAtUtc,
    sort: "TOP_COMMENTS",
    total_count: parsedTotal ?? fallbackTotal,
    returned_count: comments.length,
    comments,
    comments_disabled: disabled,
    surface: disabled ? "disabled" : surface.status,
  };
}

export function commentPageTotalCount(page) {
  if (page?.total_count == null || page.total_count === "") return null;
  const total = Number(page.total_count);
  return Number.isSafeInteger(total) && total >= 0 ? total : null;
}

export function commentPageHasFirstPage(page) {
  return page != null
    && page.version === 1
    && page.sort === "TOP_COMMENTS"
    && Array.isArray(page.comments)
    && Number(page.returned_count) === page.comments.length;
}

export function youtubeCommentsDisabled(raw) {
  return explicitDisabledSignal(raw) === true;
}

export function classifyYoutubeCommentPage(page, {
  disabled = false,
  commentsError = null,
} = {}) {
  if (disabled === true || page?.comments_disabled === true) {
    return {
      comments_disabled: true,
      comment_count: 0,
      comment_count_status: "disabled",
      comment_count_source: "youtubejs_comments",
    };
  }
  if (commentsError) {
    return {
      comments_disabled: null,
      comment_count: commentPageTotalCount(page),
      comment_count_status: "unresolved",
      comment_count_source: null,
    };
  }
  const total = commentPageTotalCount(page);
  if (commentPageHasFirstPage(page) && page.returned_count > 0) {
    return {
      comments_disabled: false,
      comment_count: total ?? page.returned_count,
      comment_count_status: "exact",
      comment_count_source: "youtubejs_comments",
    };
  }
  if (total != null && total > 0 && (page?.returned_count ?? 0) === 0) {
    return {
      comments_disabled: false,
      comment_count: total,
      comment_count_status: "unresolved",
      comment_count_source: "youtubejs_comments",
    };
  }
  if (total === 0 || (commentPageHasFirstPage(page) && page.returned_count === 0 && page.surface !== "absent")) {
    return {
      comments_disabled: false,
      comment_count: 0,
      comment_count_status: "zero_from_surface",
      comment_count_source: "youtubejs_comments",
    };
  }
  return {
    comments_disabled: null,
    comment_count: null,
    comment_count_status: "unresolved",
    comment_count_source: null,
  };
}
