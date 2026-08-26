import { AsyncLocalStorage } from "node:async_hooks";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { Innertube, Log, Utils, YTNodes } from "youtubei.js";
import { ProxyAgent, fetch as undiciFetch } from "undici";
import { combineAbortSignals, throwIfAborted } from "./abortSignal.js";
import { youtubeErrorText } from "./detailPolicy.js";
import {
  recordChannelExecutionFailure,
  recordChannelExecutionRequest,
} from "./channelExecutionContext.js";
import { fetchWithFingerprint, fingerprintTransportRequired } from "./fingerprintFetch.js";
import {
  findSubscriberCountText,
  parseLocalizedCount,
  parseRequiredLocalizedCount,
} from "./localizedCount.js";
import { ParserContractError } from "./localizedParsing.js";
import { localizedPublishedUtcDay } from "./localizedTime.js";
import { publicationLinkTarget } from "./publicationLinks.js";
import { normalizeVideoKeywords, normalizeVideoTextMetadata } from "./videoMetadata.js";
import { annotateYoutubeFailure } from "./youtubeFailurePolicy.js";
import { observeYoutubeBusinessEmail } from "./youtubeBusinessEmailAvailability.js";
import { extractYoutubePlayerContentTypeSignals } from "./youtubeContentType.js";
import { currentManagedAbortSignal } from "./proxyIdentity.js";
import {
  classifyYoutubeCommentPage,
  commentPageHasFirstPage,
  emptyYoutubeCommentPage,
  isYoutubeJsCommentsResult,
  normalizeYoutubeCommentPage,
  youtubeCommentPageFromGetComments,
  youtubeCommentsDisabled,
} from "./youtubeCommentPage.js";
import {
  assertYoutubeContentObservation,
  resolveYoutubePlayability,
} from "./youtubePlayability.js";

const VERSION = "17.2.0";
const DEFAULT_LANGUAGE = process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en";
const DEFAULT_COUNTRY = process.env.YOUTUBE_COUNTRY || "BR";
const DEFAULT_TIMEOUT_MS = Math.max(1000, Number(process.env.YOUTUBEJS_TIMEOUT_MS || 30000));
const MAX_TAB_PAGES = Math.max(1, Number(process.env.YOUTUBEJS_MAX_TAB_PAGES || 20));

let runtime = null;
let runtimePromise = null;
let activeLease = null;
const operationSignalStorage = new AsyncLocalStorage();

function throwIfYoutubeJsOperationAborted() {
  throwIfAborted(combineAbortSignals(
    operationSignalStorage.getStore() ?? null,
    currentManagedAbortSignal(),
  ));
}

Log.setLevel(Log.Level.ERROR);

function extractorMode() {
  const mode = String(process.env.YOUTUBEJS_EXTRACTOR_MODE || "channel").trim().toLowerCase();
  return ["disabled", "channel", "full"].includes(mode) ? mode : "full";
}

export function youtubeJsChannelEnabled() {
  return extractorMode() !== "disabled";
}

export function youtubeJsDetailEnabled() {
  return extractorMode() === "full";
}

const require = createRequire(import.meta.url);
let commentsSectionParamsPromise = null;

function loadCommentsSectionParams() {
  if (!commentsSectionParamsPromise) {
    const paramsPath = join(
      dirname(require.resolve("youtubei.js/package.json")),
      "dist/protos/generated/misc/params.js",
    );
    commentsSectionParamsPromise = import(pathToFileURL(paramsPath).href)
      .then((module) => module.GetCommentsSectionParams);
  }
  return commentsSectionParamsPromise;
}

export async function inspectYoutubeJsCommentsSection(videoId) {
  const current = await getRuntime();
  if (!current?.client) throw new Error("YouTube.js runtime is not available");
  return fetchYoutubeJsCommentsSection(current.client, videoId);
}

export async function fetchYoutubeJsCommentFirstPage(videoId, {
  totalCount = null,
  signal = null,
} = {}) {
  const cleanVideoId = String(videoId ?? "").trim();
  if (!cleanVideoId) throw new Error("video_id is required");
  throwIfAborted(signal);
  const current = await getRuntime();
  return operationSignalStorage.run(signal, async () => {
    const raw = await fetchYoutubeJsCommentsSection(current.client, cleanVideoId);
    throwIfYoutubeJsOperationAborted();
    const disabled = youtubeCommentsDisabled(raw);
    const page = disabled
      ? emptyYoutubeCommentPage({ totalCount: 0 })
      : normalizeYoutubeCommentPage(raw, { totalCount });
    const classified = classifyYoutubeCommentPage(page, { disabled });
    return {
      comment_count: classified.comment_count ?? totalCount,
      comment_count_status: classified.comment_count_status,
      comments_disabled: classified.comments_disabled,
      comments_status_source: classified.comment_count_source,
      comment_count_source: classified.comment_count_source,
      comments_first_page: page,
      comments_first_page_status: disabled
        ? "disabled"
        : Number(page.returned_count) > 0 ? "collected" : "unresolved",
      comments_first_page_source: "youtubejs_comments",
      source: "youtubejs_comments",
    };
  });
}

export async function fetchYoutubeJsCommentsSection(client, videoId) {
  if (!client?.actions || typeof client.actions.execute !== "function") {
    throw new TypeError("YouTube.js client with actions.execute() is required");
  }
  const cleanVideoId = String(videoId ?? "").trim();
  if (!cleanVideoId) throw new Error("video_id is required");
  const GetCommentsSectionParams = await loadCommentsSectionParams();
  const token = GetCommentsSectionParams.encode({
    ctx: { videoId: cleanVideoId },
    unkParam: 6,
    params: {
      opts: {
        videoId: cleanVideoId,
        sortBy: 0,
        type: 2,
        commentId: "",
      },
      target: "comments-section",
    },
  });
  const endpoint = new YTNodes.NavigationEndpoint({
    continuationCommand: {
      request: "CONTINUATION_REQUEST_TYPE_WATCH_NEXT",
      token: encodeURIComponent(Utils.u8ToBase64(token.finish())),
    },
  });
  const response = await endpoint.call(client.actions);
  if (response?.success === false) {
    throw new Error(`YouTube comments request failed HTTP ${response.status_code ?? "unknown"}`);
  }
  return response?.data ?? response;
}

function currentProxyUrl() {
  return String(process.env.YOUTUBE_PROXY_URL || "").trim();
}

function renderedText(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value.trim() || null;
  try {
    const rendered = value.toString();
    return rendered && rendered !== "[object Object]" ? rendered.trim() || null : null;
  } catch {
    return null;
  }
}

function finiteInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? Math.trunc(number) : null;
}

function positiveInteger(value) {
  const number = finiteInteger(value);
  return number != null && number > 0 ? number : null;
}

function absoluteYoutubeUrl(value) {
  const url = String(value ?? "").trim();
  if (!url) return null;
  if (/^https?:\/\//i.test(url)) return url;
  if (url.startsWith("//")) return `https:${url}`;
  if (url.startsWith("/")) return `https://www.youtube.com${url}`;
  return `https://www.youtube.com/${url}`;
}

function bestThumbnail(...values) {
  const candidates = [];
  const visit = (value, depth = 0) => {
    if (!value || depth > 4) return;
    if (Array.isArray(value)) {
      for (const item of value) visit(item, depth + 1);
      return;
    }
    if (typeof value !== "object") return;
    if (typeof value.url === "string") {
      candidates.push({
        url: absoluteYoutubeUrl(value.url),
        area: Number(value.width || 0) * Number(value.height || 0),
      });
    }
    for (const key of ["thumbnails", "thumbnail", "image", "sources", "content_image", "contentImage"]) {
      if (value[key] && value[key] !== value) visit(value[key], depth + 1);
    }
  };
  for (const value of values) visit(value);
  candidates.sort((a, b) => b.area - a.area);
  return candidates.find((item) => item.url)?.url ?? null;
}

export function parseYoutubeJsCount(value, locale = DEFAULT_LANGUAGE) {
  if (typeof value === "number" && Number.isFinite(value)) return Math.round(value);
  const raw = renderedText(value);
  return raw ? parseLocalizedCount(raw, { locale }) : null;
}

function parseObservedYoutubeJsCount(value, {
  field,
  source,
  context = null,
} = {}) {
  if (typeof value === "number" && Number.isFinite(value)) return Math.round(value);
  const raw = renderedText(value);
  return raw == null
    ? null
    : parseRequiredLocalizedCount(raw, {
        locale: DEFAULT_LANGUAGE,
        field,
        source,
        context,
      });
}

function isoTimestamp(value) {
  if (!value) return { value: null, precision: "unknown" };
  const raw = value instanceof Date ? value.toISOString() : String(value).trim();
  if (!raw) return { value: null, precision: "unknown" };
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return { value: null, precision: "unknown" };
  const precision = value instanceof Date || /[T ]\d{2}:\d{2}/.test(raw) ? "second" : "date_only";
  return {
    value: precision === "second" ? parsed.toISOString() : parsed.toISOString().slice(0, 10),
    precision,
  };
}

function itemId(item) {
  const entityId = String(item?.entity_id || "").replace(/^shorts-shelf-item-/, "");
  return String(
    item?.video_id
      || item?.id
      || item?.content_id
      || item?.endpoint?.payload?.videoId
      || item?.on_tap_endpoint?.payload?.videoId
      || item?.on_tap_endpoint?.payload?.reelWatchEndpoint?.videoId
      || entityId
      || "",
  ).trim() || null;
}

function itemEndpoints(item) {
  return [
    item?.endpoint,
    item?.on_tap_endpoint,
    item?.renderer_context?.command_context?.on_tap,
    item?.inline_player_data,
  ].filter(Boolean);
}

function itemNavigationUrl(item, videoId) {
  const endpoints = itemEndpoints(item);
  for (const endpoint of endpoints) {
    const url = absoluteYoutubeUrl(endpoint?.metadata?.url);
    if (url) return url;
  }
  if (endpoints.some((endpoint) => endpoint?.name === "reelWatchEndpoint")) {
    return `https://www.youtube.com/shorts/${encodeURIComponent(videoId)}`;
  }
  return null;
}

function isShortsNavigationUrl(value, videoId) {
  if (!value) return false;
  try {
    const url = new URL(value, "https://www.youtube.com");
    const match = url.pathname.match(/^\/shorts\/([^/?#]+)/i);
    return Boolean(match && decodeURIComponent(match[1]) === videoId);
  } catch {
    return false;
  }
}

export function normalizeYoutubeJsFeedItem(item) {
  const videoId = itemId(item);
  if (!videoId) return null;
  const duration = finiteInteger(item?.duration?.seconds);
  const navigationUrl = itemNavigationUrl(item, videoId);
  const nodeType = item?.type || item?.constructor?.type || item?.constructor?.name || null;
  const isLive = Boolean(item?.is_live);
  const isUpcoming = Boolean(item?.is_upcoming);
  const isShort = isShortsNavigationUrl(navigationUrl, videoId)
    || item?.content_type === "SHORT"
    || ["ReelItem", "ShortsLockupView"].includes(nodeType);
  const contentType = isLive || isUpcoming ? "live" : isShort ? "short" : null;
  const typeSource = contentType === "live"
    ? "youtube_uploads_live_flag"
    : contentType === "short"
      ? "youtube_uploads_url:shorts"
      : null;
  return {
    id: videoId,
    title: renderedText(item?.title || item?.metadata?.title || item?.overlay_metadata?.primary_text),
    url: navigationUrl || `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`,
    thumbnail_url: bestThumbnail(item?.thumbnails, item?.thumbnail, item?.content_image),
    duration: duration != null && duration > 0 ? duration : null,
    view_count: parseYoutubeJsCount(
      item?.view_count || item?.short_view_count || item?.views || item?.overlay_metadata?.secondary_text,
    ),
    published_text: renderedText(item?.published),
    is_live: isLive,
    is_upcoming: isUpcoming,
    upcoming_at: item?.upcoming instanceof Date ? item.upcoming.toISOString() : null,
    content_type: contentType,
    type_source: typeSource,
    node_type: nodeType,
  };
}

async function collectFeed(feed, limit) {
  const entries = [];
  const seen = new Set();
  let page = feed;
  let pages = 0;
  let inspectedCount = 0;
  let parseGapCount = 0;
  while (page && entries.length < limit && pages < MAX_TAB_PAGES) {
    pages += 1;
    const before = entries.length;
    for (const node of Array.from(page.videos || page.items || [])) {
      inspectedCount += 1;
      const entry = normalizeYoutubeJsFeedItem(node);
      if (!entry) {
        parseGapCount += 1;
        continue;
      }
      if (seen.has(entry.id)) continue;
      seen.add(entry.id);
      entries.push(entry);
      if (entries.length >= limit) break;
    }
    if (!page.has_continuation || entries.length >= limit) break;
    page = await page.getContinuation();
    if (entries.length === before && !page?.has_continuation) break;
  }
  const terminalReason = !page?.has_continuation
    ? "list_end"
    : entries.length >= limit
      ? "max_items"
      : "max_pages";
  const stopReason = parseGapCount > 0 ? "parse_gap" : terminalReason;
  return {
    entries,
    pages,
    inspected_count: inspectedCount,
    parse_gap_count: parseGapCount,
    truncated: terminalReason !== "list_end",
    stop_reason: stopReason,
    terminal_reason: terminalReason,
    complete: stopReason === "list_end",
  };
}

export async function scanYoutubeJsFeed(feed, {
  anchors = [],
  maxPages = MAX_TAB_PAGES,
  catchUpMaxItems = 50,
  now = Date.now(),
  locale = DEFAULT_LANGUAGE,
} = {}) {
  const orderedAnchors = [];
  const seenAnchorIds = new Set();
  for (const anchor of anchors) {
    const id = String(anchor?.id ?? anchor?.video_id ?? "").trim();
    const publishedDay = String(anchor?.published_day ?? anchor?.published_at ?? "").slice(0, 10);
    if (!id || seenAnchorIds.has(id)) continue;
    seenAnchorIds.add(id);
    orderedAnchors.push({
      id,
      published_day: /^\d{4}-\d{2}-\d{2}$/.test(publishedDay) ? publishedDay : null,
    });
  }
  const anchorIndexes = new Map(
    orderedAnchors.map((anchor, index) => [anchor.id, index]),
  );
  const pageLimit = Math.max(1, Number.parseInt(String(maxPages), 10) || MAX_TAB_PAGES);
  const catchUpItemLimit = Math.max(
    1,
    Number.parseInt(String(catchUpMaxItems), 10) || 50,
  );
  const entries = [];
  const seen = new Set();
  let page = feed;
  let pages = 0;
  let firstPageItemCount = 0;
  let catchUpItemCount = 0;
  let sourcePosition = 0;
  let parseGapCount = 0;
  let matchedAnchorId = null;
  let activeAnchorIndex = 0;
  const crossedAnchorIds = [];
  let stopReason = null;
  let paginationError = null;

  while (page && !stopReason) {
    pages += 1;
    const nodes = Array.from(page.videos || page.items || []);
    for (const node of nodes) {
      sourcePosition += 1;
      const entry = normalizeYoutubeJsFeedItem(node);
      if (!entry) {
        parseGapCount += 1;
        continue;
      }
      if (seen.has(entry.id)) continue;
      if (pages > 1 && catchUpItemCount >= catchUpItemLimit) {
        stopReason = "catchup_limit";
        break;
      }
      seen.add(entry.id);
      if (pages === 1) firstPageItemCount += 1;
      else catchUpItemCount += 1;
      const publishedDay = localizedPublishedUtcDay(entry.published_text, { locale, now });
      entries.push({ ...entry, position: sourcePosition, published_day: publishedDay });
      const matchedAnchorIndex = anchorIndexes.get(entry.id);
      if (matchedAnchorIndex != null) {
        for (let index = 0; index < matchedAnchorIndex; index += 1) {
          crossedAnchorIds.push(orderedAnchors[index].id);
        }
        activeAnchorIndex = matchedAnchorIndex;
        matchedAnchorId = entry.id;
        stopReason = "anchor_matched";
      }
      if (stopReason) break;
    }
    if (stopReason) break;
    if (!page.has_continuation) {
      stopReason = "list_end";
      break;
    }
    if (pages >= pageLimit) {
      stopReason = "max_pages";
      break;
    }
    if (pages > 1 && catchUpItemCount >= catchUpItemLimit) {
      stopReason = "catchup_limit";
      break;
    }
    try {
      page = await page.getContinuation();
    } catch (error) {
      throwIfYoutubeJsOperationAborted();
      paginationError = error;
      stopReason = "pagination_error";
    }
  }
  if (!stopReason) stopReason = "list_end";
  const terminalCoverage = ["anchor_matched", "list_end"].includes(stopReason);
  const complete = terminalCoverage && parseGapCount === 0;
  return {
    entries,
    pages,
    first_page_item_count: firstPageItemCount,
    catch_up_item_count: catchUpItemCount,
    item_count: entries.length,
    parse_gap_count: parseGapCount,
    anchor_matched: matchedAnchorId !== null,
    matched_anchor_id: matchedAnchorId,
    crossed_anchor_ids: crossedAnchorIds,
    active_anchor_id: orderedAnchors[activeAnchorIndex]?.id ?? null,
    stop_reason: parseGapCount > 0 && terminalCoverage ? "parse_gap" : stopReason,
    terminal_reason: stopReason,
    complete,
    error: paginationError,
  };
}

export async function collectYoutubeJsUploadBundle(client, channelId, limit = 30, {
  hasContent = true,
  locale = DEFAULT_LANGUAGE,
  now = Date.now(),
} = {}) {
  const cleanLimit = Math.max(1, Math.min(Number(limit) || 30, 100));
  const playlistId = channelId.startsWith("UC") ? `UU${channelId.slice(2)}` : channelId;
  const uploads = await (hasContent
    ? client.getPlaylist(playlistId).then((feed) => collectFeed(feed, cleanLimit))
    : Promise.resolve({
        entries: [], pages: 0, inspected_count: 0, parse_gap_count: 0, absent: true, truncated: false,
        stop_reason: "list_end", terminal_reason: "list_end", complete: true,
      }));
  if (hasContent && uploads.entries.length === 0) {
    throw new Error(`YouTube.js uploads playlist ${playlistId} was empty while the channel exposes content tabs`);
  }
  const entries = uploads.entries.map((upload, index) => {
    const publishedDay = localizedPublishedUtcDay(upload.published_text, { locale, now });
    return {
      video_id: upload.id,
      title: upload.title,
      url: upload.url,
      source_url: upload.url,
      thumbnail_url: upload.thumbnail_url,
      duration_seconds: upload.duration,
      view_count_text: upload.view_count != null ? String(upload.view_count) : null,
      published_text: upload.published_text,
      published_at: publishedDay,
      published_at_precision: publishedDay ? "date_only" : "unknown",
      published_at_source: publishedDay ? "youtube_uploads_relative_time" : null,
      position: index + 1,
      content_type: upload.content_type,
      type_source: upload.type_source,
      type_membership: upload.content_type ? [upload.content_type] : [],
      is_live: upload.is_live,
      is_upcoming: upload.is_upcoming,
      live_status: upload.is_upcoming ? "is_upcoming" : upload.is_live ? "is_live" : null,
      live_scheduled_at: upload.upcoming_at,
    };
  });
  const contentTypeCounts = { video: 0, short: 0, live: 0, unresolved: 0 };
  for (const entry of entries) {
    if (entry.content_type) contentTypeCounts[entry.content_type] += 1;
    else contentTypeCounts.unresolved += 1;
  }
  return {
    playlistId,
    uploads,
    entries,
    contentTypeCounts,
    activityEvidenceComplete: uploads.parse_gap_count === 0,
  };
}

function channelHandle(metadata, header, about) {
  const explicit = renderedText(header?.channel_handle);
  if (explicit) return explicit.startsWith("@") ? explicit : `@${explicit}`;
  for (const value of [metadata?.vanity_channel_url, metadata?.url, about?.canonical_channel_url]) {
    const match = String(value || "").match(/\/@([^/?#]+)/);
    if (match) return `@${match[1]}`;
  }
  return null;
}

function longestText(...values) {
  return values.map(renderedText).filter(Boolean).sort((a, b) => b.length - a.length)[0] ?? null;
}

function pageHeaderMetadataParts(header) {
  return (header?.content?.metadata?.metadata_rows || [])
    .flatMap((row) => row?.metadata_parts || [])
    .map((part) => renderedText(part?.text))
    .filter(Boolean);
}

const VIDEO_COUNT_LABEL_PATTERN = /(?:\bvideos?\b|\bvídeos?\b|影片|视频|影片數|视频数|동영상|ビデオ|видео|فيديو|वीडियो)/iu;
const VIEW_COUNT_LABEL_PATTERN = /(?:\bviews?\b|visualiza(?:cao|ção|coes|ções)|vues?|aufrufe|观看|觀看|조회수|просмотр|مشاهد|बार देखा)/iu;

function handleShapedText(value) {
  const output = renderedText(value);
  return output?.startsWith("@") === true || /(?:^|\/)youtube\.com\/@/i.test(output ?? "");
}

function ambiguousHeaderSubscriberText(value, { locale, excludedTexts = [] } = {}) {
  const output = renderedText(value);
  if (!output || handleShapedText(output)) return null;
  if (excludedTexts.includes(output)
      || VIDEO_COUNT_LABEL_PATTERN.test(output)
      || VIEW_COUNT_LABEL_PATTERN.test(output)) return null;
  if (findSubscriberCountText([output], { locale })) return output;
  return !/\p{Letter}/u.test(output) && parseLocalizedCount(output, { locale }) != null
    ? output
    : null;
}

function firstSubscriberObservation({
  about,
  header,
  root,
  headerSubscriberText,
  headerVideoCountText,
  locale,
}) {
  const candidates = [
    { value: about?.subscriber_count, source: "youtube_about" },
    { value: headerSubscriberText, source: "youtube_channel_header" },
    { value: root?.subscribe_button?.subscribers, source: "youtube_subscribe_button" },
    {
      value: ambiguousHeaderSubscriberText(header?.subscribers, {
        locale,
        excludedTexts: [headerVideoCountText].filter(Boolean),
      }),
      source: "youtube_channel_header",
    },
  ];
  return candidates
    .map((item) => ({ ...item, text: renderedText(item.value) }))
    .find((item) => item.text !== null && !handleShapedText(item.text)) ?? null;
}

export function youtubeJsAboutLinks(about) {
  const links = about?.links || about?.primary_links || [];
  return Array.from(links).map((link, position) => {
    const targetUrl = absoluteYoutubeUrl(publicationLinkTarget(link));
    return {
      title: renderedText(link?.title),
      display_url: renderedText(link?.link),
      target_url: targetUrl,
      url: targetUrl,
      favicon_url: bestThumbnail(link?.favicon, link?.icon),
      position,
    };
  });
}

export function youtubeJsAboutLinkObservation(aboutResult) {
  const about = aboutResult?.metadata || aboutResult || {};
  return {
    links: youtubeJsAboutLinks(about),
    status: aboutResult == null ? "unresolved" : "observed",
  };
}

function verifiedBadge(value) {
  const style = String(value?.style ?? value?.badge_style ?? "").toUpperCase();
  const label = [value?.label, value?.tooltip, value?.accessibility_label]
    .map(renderedText)
    .filter(Boolean)
    .join(" ");
  return style === "BADGE_STYLE_TYPE_VERIFIED"
    || style === "BADGE_STYLE_TYPE_VERIFIED_ARTIST"
    || /(^|\W)verified(\W|$)/i.test(label);
}

export function youtubeJsChannelVerification(header) {
  if (!header || typeof header !== "object") return { value: null, status: "unknown" };
  if (typeof header?.author?.is_verified === "boolean") {
    return {
      value: header.author.is_verified,
      status: header.author.is_verified ? "verified" : "not_verified",
    };
  }
  const badgeCollections = [
    header.badges,
    header?.author?.badges,
    ...(header?.content?.metadata?.metadata_rows ?? []).map((row) => row?.badges),
  ].filter(Array.isArray);
  if (badgeCollections.length > 0) {
    const value = badgeCollections.some((badges) => badges.some(verifiedBadge));
    return { value, status: value ? "verified" : "not_verified" };
  }
  const titleRuns = header?.content?.title?.text?.runs;
  if (Array.isArray(titleRuns)) {
    const value = titleRuns.some((run) => Boolean(run?.attachment) || verifiedBadge(run));
    return { value, status: value ? "verified" : "not_verified" };
  }
  return { value: null, status: "unknown" };
}

function channelMetadata(channelId, root, aboutResult, { strictSubscriberCount = true } = {}) {
  const metadata = root?.metadata || {};
  const header = root?.header || {};
  const about = aboutResult?.metadata || aboutResult || {};
  const headerParts = pageHeaderMetadataParts(header);
  const headerVideoCountText = headerParts.find(
    (part) => !handleShapedText(part) && VIDEO_COUNT_LABEL_PATTERN.test(part),
  );
  const headerSubscriberText = findSubscriberCountText(
    headerParts.filter((part) => part !== headerVideoCountText),
    { locale: DEFAULT_LANGUAGE },
  );
  const aboutVideoCountText = renderedText(about?.video_count);
  const aboutViewCountText = renderedText(about?.view_count);
  const verification = youtubeJsChannelVerification(header);
  const linkObservation = youtubeJsAboutLinkObservation(aboutResult);
  const businessEmail = observeYoutubeBusinessEmail(aboutResult, {
    aboutObserved: aboutResult != null,
  });
  let subscriberObservation = firstSubscriberObservation({
    about,
    header,
    root,
    headerSubscriberText,
    headerVideoCountText,
    locale: DEFAULT_LANGUAGE,
  });
  let subscriberCount = subscriberObservation?.text
    ? strictSubscriberCount
      ? parseRequiredLocalizedCount(subscriberObservation.text, {
        locale: DEFAULT_LANGUAGE,
        field: "subscriber_count",
        source: subscriberObservation.source,
        context: { channel_id: channelId },
      })
      : parseLocalizedCount(subscriberObservation.text, { locale: DEFAULT_LANGUAGE })
    : null;
  if (!strictSubscriberCount && subscriberCount == null) subscriberObservation = null;
  const subscriberText = subscriberObservation?.text ?? null;
  return {
    channel_id: metadata.external_id || about.id || about.channel_id || header.channel_id || channelId,
    title: metadata.title || renderedText(header?.author?.name || header?.title) || header?.page_title || null,
    handle: channelHandle(metadata, header, about) || headerParts.find((part) => part.startsWith("@")) || null,
    channel_url: absoluteYoutubeUrl(metadata.url || about.canonical_channel_url || `/channel/${channelId}`),
    vanity_channel_url: absoluteYoutubeUrl(metadata.vanity_channel_url || about.canonical_channel_url),
    description: longestText(metadata.description, about.description, header.description),
    avatar_url: bestThumbnail(metadata.avatar, header?.author?.thumbnails, about.avatar, header?.content?.image),
    subscriber_count: subscriberCount,
    subscriber_count_text: subscriberText,
    subscriber_count_source: subscriberObservation?.source ?? null,
    video_count_text: aboutVideoCountText || renderedText(header?.videos_count || headerVideoCountText),
    video_count_source: aboutVideoCountText
      ? "youtube_about"
      : renderedText(header?.videos_count || headerVideoCountText) ? "youtube_channel_header" : null,
    view_count_text: aboutViewCountText,
    view_count_source: aboutViewCountText ? "youtube_about" : null,
    joined_date_text: renderedText(about?.joined_date),
    country: renderedText(about.country),
    external_links: linkObservation.links,
    external_links_status: linkObservation.status,
    youtube_business_email_available: businessEmail.available,
    youtube_business_email_status: businessEmail.status,
    rss_url: absoluteYoutubeUrl(metadata.rss_url),
    keywords: Array.isArray(metadata.keywords) ? metadata.keywords : renderedText(metadata.keywords),
    available_tabs: [
      root?.has_videos ? "videos" : null,
      root?.has_shorts ? "shorts" : null,
      root?.has_live_streams ? "live" : null,
    ].filter(Boolean),
    is_family_safe: metadata.is_family_safe ?? null,
    is_verified: verification.value,
    is_verified_status: verification.status,
    source: "youtubejs_channel_about",
  };
}

export { channelMetadata as youtubeJsChannelMetadata };

function youtubeChallengeTextSignal(text, { allowBareCaptcha = false } = {}) {
  const normalized = String(text ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  const explicit = /sorry\/index|detected unusual traffic|automated quer|sign in to confirm[^\n]{0,160}not a bot|confirm[^\n]{0,160}not a bot|faca login[^\n]{0,160}confirmar[^\n]{0,160}(?:nao e um (?:robo|bot)|not a bot)|verify you are human/i
    .test(normalized);
  return explicit || (allowBareCaptcha && /\bcaptcha\b/i.test(normalized));
}

function hasStructuredHttpChallenge(value) {
  if (!value || typeof value !== "object") return false;
  for (const [key, child] of Object.entries(value)) {
    if (/^(?:re)?captcha[a-z]*(?:renderer|challenge)$/i.test(key)) return true;
    if (
      /(?:error|alert|dialog|challenge|playability)/i.test(key)
      && youtubeChallengeTextSignal(JSON.stringify(child), { allowBareCaptcha: true })
    ) {
      return true;
    }
    if (hasStructuredHttpChallenge(child)) return true;
  }
  return false;
}

export function isYoutubeJsHttpBotChallenge(text) {
  const body = String(text ?? "");
  if (!/sorry\/index|unusual traffic|automated quer|not a bot|nao e um (?:robo|bot)|verify you are human|captcha/i.test(
    body.normalize("NFD").replace(/[\u0300-\u036f]/g, ""),
  )) {
    return false;
  }
  try {
    return hasStructuredHttpChallenge(JSON.parse(body));
  } catch {
    return youtubeChallengeTextSignal(body, { allowBareCaptcha: false });
  }
}

function requestParts(input, init) {
  const requestLike = typeof input === "object" && input !== null && !(input instanceof URL);
  const url = new URL(requestLike ? input.url : input);
  const method = init.method || (requestLike ? input.method : undefined) || "GET";
  return { requestLike, url, method };
}

function jsonRequestBody(body) {
  try {
    if (typeof body === "string") return JSON.parse(body);
    if (body instanceof ArrayBuffer) return JSON.parse(new TextDecoder().decode(body));
    if (ArrayBuffer.isView(body)) {
      return JSON.parse(new TextDecoder().decode(
        new Uint8Array(body.buffer, body.byteOffset, body.byteLength),
      ));
    }
  } catch {
    return null;
  }
  return null;
}

function capturePlayerTypeSurface(url, body, responseText, playerTypeSurfaces) {
  if (url.pathname !== "/youtubei/v1/player" || !responseText) return;
  const videoId = String(jsonRequestBody(body)?.videoId ?? "").trim();
  if (!videoId) return;
  try {
    playerTypeSurfaces.set(videoId, extractYoutubePlayerContentTypeSignals(
      JSON.parse(responseText),
      { source: "youtubei_player" },
    ));
  } catch {
    playerTypeSurfaces.delete(videoId);
  }
}

async function createFetch(proxyUrl, dispatcher, stats, playerTypeSurfaces) {
  return async (input, init = {}) => {
    const { requestLike, url, method } = requestParts(input, init);
    const startedAt = Date.now();
    stats.requests += 1;
    const callerSignal = init.signal || (requestLike ? input.signal : undefined);
    const operationSignal = operationSignalStorage.getStore() ?? null;
    const managedSignal = currentManagedAbortSignal();
    const externalSignal = combineAbortSignals(callerSignal, operationSignal, managedSignal);
    throwIfAborted(externalSignal);
    let body = init.body;
    if (body === undefined && requestLike && !["GET", "HEAD"].includes(method.toUpperCase())) {
      body = await input.clone().arrayBuffer();
    }
    throwIfAborted(externalSignal);
    const timeoutSignal = AbortSignal.timeout(DEFAULT_TIMEOUT_MS);
    const signal = combineAbortSignals(externalSignal, timeoutSignal);
    let recorded = false;
    try {
      const requestInit = {
        method,
        headers: init.headers || (requestLike ? input.headers : undefined),
        body,
        signal,
        redirect: init.redirect || (requestLike ? input.redirect : undefined),
      };
      const response = await fetchWithFingerprint(
        "youtubejs_chrome",
        url,
        requestInit,
        () => undiciFetch(url, { ...requestInit, dispatcher }),
      );
      throwIfAborted(externalSignal);
      stats.statuses[response.status] = (stats.statuses[response.status] || 0) + 1;
      let responseSample = "";
      if (!response.ok || response.status === 200) {
        const sample = await response.clone().text();
        throwIfAborted(externalSignal);
        responseSample = sample.slice(0, 500);
        if (isYoutubeJsHttpBotChallenge(sample)) {
          stats.challenges += 1;
          const error = annotateYoutubeFailure(new Error(`YouTube bot challenge HTTP 200 for ${url.pathname}`), {
            status: 200,
            body: responseSample,
            source: "youtubejs_fetch",
            targetUrl: url.toString(),
            client: "WEB",
          });
          recordChannelExecutionRequest({
            engine: "youtubejs",
            client: "WEB",
            status: 200,
            durationMs: Date.now() - startedAt,
            ok: false,
            error,
            body: responseSample,
            source: "youtubejs_fetch",
            targetUrl: url.toString(),
          });
          recorded = true;
          throw error;
        }
        if (response.ok) capturePlayerTypeSurface(url, body, sample, playerTypeSurfaces);
      }
      const requestOk = response.ok && response.status !== 403 && response.status !== 429;
      recordChannelExecutionRequest({
        engine: "youtubejs",
        client: "WEB",
        status: response.status,
        durationMs: Date.now() - startedAt,
        ok: requestOk,
        body: responseSample,
        source: "youtubejs_fetch",
        targetUrl: url.toString(),
      });
      recorded = true;
      if (response.status === 403 || response.status === 429) {
        throw annotateYoutubeFailure(new Error(`YouTube.js request failed HTTP ${response.status}`), {
          status: response.status,
          body: responseSample,
          source: "youtubejs_fetch",
          targetUrl: url.toString(),
          client: "WEB",
        });
      }
      return response;
    } catch (error) {
      if (externalSignal?.aborted) throw externalSignal.reason;
      stats.failures += 1;
      const annotated = annotateYoutubeFailure(error, {
        source: error?.youtube_failure_evidence?.source || "youtubejs_fetch",
        targetUrl: url.toString(),
        client: "WEB",
      });
      if (!recorded) {
        recordChannelExecutionRequest({
          engine: "youtubejs",
          client: "WEB",
          status: annotated.youtube_failure_evidence?.status,
          durationMs: Date.now() - startedAt,
          ok: false,
          error: annotated,
          body: annotated.youtube_failure_evidence?.body,
          source: annotated.youtube_failure_evidence?.source,
          targetUrl: url.toString(),
        });
      }
      throw annotated;
    } finally {
      stats.duration_ms += Date.now() - startedAt;
    }
  };
}

async function closeRuntime(current) {
  if (!current) return;
  try {
    await current.dispatcher?.close();
  } catch {
    try {
      current.dispatcher?.destroy();
    } catch {
      // Best-effort shutdown.
    }
  }
}

function runtimeKey(proxyUrl, profile) {
  return `${proxyUrl || "direct"}|${profile?.profile_id || "default"}`;
}

async function createRuntime(proxyUrl, profile = null) {
  const startedAt = Date.now();
  const dispatcher = proxyUrl && !profile
    ? new ProxyAgent({
        uri: proxyUrl,
        connections: 2,
        pipelining: 1,
        keepAliveTimeout: 60000,
        keepAliveMaxTimeout: 300000,
      })
    : undefined;
  const stats = { requests: 0, failures: 0, challenges: 0, duration_ms: 0, statuses: {} };
  const playerTypeSurfaces = new Map();
  try {
    const fetch = await createFetch(proxyUrl, dispatcher, stats, playerTypeSurfaces);
    const client = await Innertube.create({
      lang: DEFAULT_LANGUAGE,
      location: DEFAULT_COUNTRY,
      user_agent: profile?.user_agent,
      visitor_data: profile?.visitor_data,
      timezone: profile?.timezone,
      retrieve_player: false,
      generate_session_locally: true,
      retrieve_innertube_config: false,
      enable_session_cache: false,
      fetch,
    });
    return {
      client,
      dispatcher,
      proxyUrl,
      profileId: profile?.profile_id ?? null,
      runtimeKey: runtimeKey(proxyUrl, profile),
      startedAt: Date.now(),
      startupMs: Date.now() - startedAt,
      stats,
      playerTypeSurfaces,
    };
  } catch (error) {
    await closeRuntime({ dispatcher });
    throw error;
  }
}

async function getRuntime() {
  if (extractorMode() === "disabled") return null;
  const proxyUrl = activeLease?.proxyUrl ?? currentProxyUrl();
  const profile = activeLease?.profile ?? null;
  const key = runtimeKey(proxyUrl, profile);
  if (runtime && runtime.runtimeKey === key) return runtime;
  if (runtimePromise) return runtimePromise;
  runtimePromise = (async () => {
    const stale = runtime;
    runtime = null;
    await closeRuntime(stale);
    runtime = await createRuntime(proxyUrl, profile);
    return runtime;
  })().finally(() => {
    runtimePromise = null;
  });
  return runtimePromise;
}

export async function warmYoutubeJs() {
  if (extractorMode() === "disabled") return { enabled: false, mode: "disabled" };
  if (fingerprintTransportRequired() && !activeLease) {
    return { enabled: true, mode: "deferred_until_profile", version: VERSION };
  }
  try {
    const current = await getRuntime();
    return {
      enabled: true,
      mode: extractorMode(),
      version: VERSION,
      startup_ms: current.startupMs,
      proxy_bound: Boolean(current.proxyUrl),
    };
  } catch (error) {
    return { enabled: false, mode: extractorMode(), version: VERSION, error: String(error?.message || error) };
  }
}

export async function closeYoutubeJs() {
  activeLease = null;
  const current = runtime;
  runtime = null;
  await closeRuntime(current);
}

export async function acquireYoutubeJs(channelId, { profile, proxyUrl = currentProxyUrl() } = {}) {
  if (extractorMode() === "disabled") return { enabled: false, mode: "disabled" };
  if (activeLease) throw new Error(`YouTube.js runtime is already leased by ${activeLease.channelId}`);
  if (fingerprintTransportRequired() && !profile?.profile_id) {
    throw new Error("YouTube.js fingerprint profile is required");
  }
  activeLease = {
    channelId: String(channelId || "unknown"),
    proxyUrl,
    profile: profile || null,
    startedAt: Date.now(),
  };
  try {
    const current = await getRuntime();
    return {
      enabled: true,
      mode: extractorMode(),
      version: VERSION,
      proxy_bound: Boolean(proxyUrl),
      profile_id: profile?.profile_id ?? null,
      startup_ms: current.startupMs,
    };
  } catch (error) {
    return {
      enabled: false,
      mode: extractorMode(),
      version: VERSION,
      proxy_bound: Boolean(proxyUrl),
      error: String(error?.message || error),
    };
  }
}

export async function releaseYoutubeJs() {
  const lease = activeLease;
  activeLease = null;
  if (!lease) return null;
  const proxyChanged = lease.proxyUrl !== currentProxyUrl();
  if (proxyChanged) {
    const stale = runtime;
    runtime = null;
    await closeRuntime(stale);
  }
  return {
    channel_id: lease.channelId,
    duration_ms: Date.now() - lease.startedAt,
    proxy_changed: proxyChanged,
  };
}

export function youtubeJsState() {
  return {
    enabled: extractorMode() !== "disabled",
    mode: extractorMode(),
    version: VERSION,
    ready: Boolean(runtime),
    proxy_url_matches: runtime ? runtime.proxyUrl === (activeLease?.proxyUrl ?? currentProxyUrl()) : null,
    profile_id: runtime?.profileId ?? null,
    active_channel: activeLease?.channelId ?? null,
    started_at: runtime?.startedAt ? new Date(runtime.startedAt).toISOString() : null,
    startup_ms: runtime?.startupMs ?? null,
    stats: runtime ? { ...runtime.stats, statuses: { ...runtime.stats.statuses } } : null,
  };
}

export async function openYoutubeJsChannel(channelId, { includeAbout = true } = {}) {
  if (!youtubeJsChannelEnabled()) throw new Error("YouTube.js channel extraction is disabled");
  const cleanChannelId = String(channelId ?? "").trim();
  if (!cleanChannelId) throw new Error("channel_id is required");
  const current = await getRuntime();
  const startedAt = Date.now();
  const requestStart = current.stats.requests;
  const root = await current.client.getChannel(cleanChannelId);
  const channelRequestCount = current.stats.requests - requestStart;
  let about = null;
  let aboutFailure = null;
  let aboutError = null;
  const aboutRequestStart = current.stats.requests;
  let aboutRequestCount = 0;
  if (includeAbout) {
    try {
      about = await root.getAbout();
      aboutRequestCount = current.stats.requests - aboutRequestStart;
    } catch (error) {
      throwIfYoutubeJsOperationAborted();
      aboutRequestCount = current.stats.requests - aboutRequestStart;
      aboutFailure = error;
      aboutError = String(error?.message || error);
    }
  }
  const metadata = channelMetadata(cleanChannelId, root, about, {
    strictSubscriberCount: includeAbout,
  });

  return {
    metadata,
    about_requested: includeAbout,
    about_observed: includeAbout && Boolean(about) && !aboutFailure,
    about_error: aboutFailure,
    raw: {
      engine: `youtubei.js@${VERSION}`,
      channel_id: cleanChannelId,
      elapsed_ms: Date.now() - startedAt,
      request_count: current.stats.requests - requestStart,
      request_counts: {
        get_channel: channelRequestCount,
        get_about: aboutRequestCount,
      },
      about_error: aboutError,
      available_tabs: {
        videos: Boolean(root.has_videos),
        shorts: Boolean(root.has_shorts),
        live: Boolean(root.has_live_streams),
      },
      metadata,
    },
    async fetchContents(limit = 30, { locale = DEFAULT_LANGUAGE, now = Date.now() } = {}) {
      const cleanLimit = Math.max(1, Math.min(Number(limit) || 30, 100));
      const bundleStartedAt = Date.now();
      const bundleRequestStart = current.stats.requests;
      const hasContent = Boolean(root.has_videos || root.has_shorts || root.has_live_streams);
      const {
        playlistId,
        uploads,
        entries,
        contentTypeCounts,
        activityEvidenceComplete,
      } = await collectYoutubeJsUploadBundle(
        current.client,
        cleanChannelId,
        cleanLimit,
        {
          hasContent,
          locale,
          now,
        },
      );
      return {
        channel_id: cleanChannelId,
        playlist_id: playlistId,
        playlist_url: `https://www.youtube.com/playlist?list=${encodeURIComponent(playlistId)}`,
        entries,
        tab_counts: contentTypeCounts,
        uploads_missing: Boolean(uploads.absent),
        activity_evidence_complete: activityEvidenceComplete,
        activity_parse_gap_count: uploads.parse_gap_count,
        scan: {
          pages: uploads.pages,
          inspected_count: uploads.inspected_count,
          parse_gap_count: uploads.parse_gap_count,
          selected_count: entries.length,
          stop_reason: uploads.stop_reason,
          terminal_reason: uploads.terminal_reason,
          complete: uploads.complete,
        },
        untyped_ids: entries.filter((entry) => !entry.content_type).map((entry) => entry.video_id),
        raw: {
          engine: `youtubei.js@${VERSION}`,
          channel_id: cleanChannelId,
          playlist_id: playlistId,
          elapsed_ms: Date.now() - bundleStartedAt,
          request_count: current.stats.requests - bundleRequestStart,
          upload_pages: uploads.pages,
          uploads_inspected_count: uploads.inspected_count,
          uploads_parse_gap_count: uploads.parse_gap_count,
          tab_pages: {},
          tab_counts: contentTypeCounts,
          counts_scope: "selected_uploads",
          classification_source: "uploads_order+watch_detail",
          entries,
          untyped_ids: entries.filter((entry) => !entry.content_type).map((entry) => entry.video_id),
        },
      };
    },
    async scanUploads({ anchors = [], maxPages = MAX_TAB_PAGES, catchUpMaxItems = 50 } = {}) {
      const playlistId = cleanChannelId.startsWith("UC")
        ? `UU${cleanChannelId.slice(2)}`
        : cleanChannelId;
      const scanStartedAt = Date.now();
      const scanRequestStart = current.stats.requests;
      const hasContent = Boolean(root.has_videos || root.has_shorts || root.has_live_streams);
      if (!hasContent) {
        return {
          channel_id: cleanChannelId,
          playlist_id: playlistId,
          entries: [],
          pages: 0,
          item_count: 0,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          crossed_anchor_ids: [],
          active_anchor_id: anchors[0]?.id ?? null,
          stop_reason: "list_end",
          terminal_reason: "list_end",
          complete: true,
          raw: {
            engine: `youtubei.js@${VERSION}`,
            request_count: 0,
            elapsed_ms: Date.now() - scanStartedAt,
          },
        };
      }
      const feed = await current.client.getPlaylist(playlistId);
      const scan = await scanYoutubeJsFeed(feed, {
        anchors,
        maxPages,
        catchUpMaxItems,
        now: scanStartedAt,
        locale: DEFAULT_LANGUAGE,
      });
      return {
        channel_id: cleanChannelId,
        playlist_id: playlistId,
        ...scan,
        raw: {
          engine: `youtubei.js@${VERSION}`,
          request_count: current.stats.requests - scanRequestStart,
          elapsed_ms: Date.now() - scanStartedAt,
          pages: scan.pages,
          item_count: scan.item_count,
          stop_reason: scan.stop_reason,
        },
      };
    },
  };
}

function isEmptyAgeGateCommentsResponse(info, commentsError) {
  if (!commentsError) return false;
  const status = String(info?.playability_status?.status || "").toUpperCase();
  const reason = renderedText(info?.playability_status?.reason);
  return status === "LOGIN_REQUIRED"
    && info?.basic_info?.is_family_safe === false
    && /confirm your age|age[- ]restricted|verify your age/i.test(reason || "")
    && /comments page did not have any content/i.test(youtubeErrorText(commentsError));
}

export function normalizeYoutubeJsVideoInfo(info, comments = null, {
  commentsError = null,
  contentTypeSignals = null,
} = {}) {
  const player = info?.page?.[0];
  const microformat = player?.microformat || {};
  const basic = info?.basic_info || {};
  const playabilityStatus = info?.playability_status?.status || null;
  const playabilityReason = info?.playability_status?.reason || null;
  const published = isoTimestamp(microformat.publish_date || microformat.upload_date);
  const commentHint = parseObservedYoutubeJsCount(
    info?.comments_entry_point_header?.comment_count,
    {
      field: "comment_count",
      source: "youtubejs_next",
      context: { video_id: basic.id || null },
    },
  );
  const commentsPage = commentPageHasFirstPage(comments)
    ? comments
    : isYoutubeJsCommentsResult(comments)
      ? youtubeCommentPageFromGetComments(comments, {
        locale: DEFAULT_LANGUAGE,
        totalCount: commentHint,
      })
      : comments
        ? normalizeYoutubeCommentPage(comments, {
          locale: DEFAULT_LANGUAGE,
          totalCount: commentHint,
        })
        : null;
  const classified = classifyYoutubeCommentPage(commentsPage, {
    disabled: youtubeCommentsDisabled(comments) || commentsPage?.comments_disabled === true,
    commentsError,
  });
  let commentCount = classified.comments_disabled === true
    ? 0
    : (classified.comment_count ?? commentHint);
  let commentsDisabled = classified.comments_disabled;
  let commentStatus = classified.comments_disabled === true
    ? "disabled"
    : classified.comment_count != null
      ? classified.comment_count_status
      : commentCount != null ? "exact" : "unresolved";
  let commentsSource = classified.comments_disabled === true || classified.comment_count != null
    ? classified.comment_count_source
    : commentHint != null ? "youtubejs_next" : null;
  if (commentsDisabled == null && isEmptyAgeGateCommentsResponse(info, commentsError)) {
    commentsDisabled = true;
    commentCount = 0;
    commentStatus = "disabled";
    commentsSource = "youtubejs_comments_age_gate_empty";
  } else if (commentsDisabled == null && commentCount != null) {
    commentsDisabled = false;
    if (!commentsSource) commentsSource = "youtubejs_next";
  }
  const isUpcoming = Boolean(basic.is_upcoming);
  const isLive = Boolean(basic.is_live);
  const isLiveContent = typeof contentTypeSignals?.is_live_content === "boolean"
    ? contentTypeSignals.is_live_content
    : typeof basic.is_live_content === "boolean" ? basic.is_live_content : null;
  const wasLive = !isLive && !isUpcoming && isLiveContent === true;
  const startTimestamp = isoTimestamp(basic.start_timestamp || microformat.start_timestamp).value;
  const endTimestamp = isoTimestamp(basic.end_timestamp || microformat.end_timestamp).value;
  const duration = positiveInteger(basic.duration ?? microformat.length_seconds);
  const viewCount = finiteInteger(basic.view_count ?? microformat.view_count);
  const likeCount = finiteInteger(basic.like_count);
  const isUnlisted = microformat.is_unlisted === true || basic.is_unlisted === true;
  const publicMetadataComplete = Boolean(published.value && viewCount != null);
  const playability = resolveYoutubePlayability({
    status: playabilityStatus,
    reason: playabilityReason,
  });
  const inconclusivePublicSurface = playability.kind === "inconclusive"
    && publicMetadataComplete;
  const availability = isUnlisted
    ? "unlisted"
    : playability.kind === "content"
      ? playability.availability
      : inconclusivePublicSurface || (!playabilityStatus && publicMetadataComplete)
        ? "public"
        : null;
  const accessStatus = isUnlisted
    ? "unlisted"
    : playability.kind === "content"
      ? playability.access_status
      : inconclusivePublicSurface || (!playabilityStatus && publicMetadataComplete)
        ? "public"
        : "unknown";
  if (
    playability.kind === "inconclusive"
    && playabilityStatus
    && String(playabilityStatus).toUpperCase() !== "OK"
    && !publicMetadataComplete
    && !isUpcoming
  ) {
    throw new ParserContractError({
      field: "playability_status",
      value: playabilityReason || playabilityStatus,
      locale: DEFAULT_LANGUAGE,
      source: "youtubejs_player",
      reason: "unsupported_playability_status",
      context: { status: playabilityStatus, video_id: basic.id || null },
    });
  }
  const title = basic.title || renderedText(microformat.title);
  const description = typeof basic.short_description === "string"
    ? basic.short_description
    : typeof basic.description === "string"
      ? basic.description
      : typeof microformat.description === "string"
        ? microformat.description
        : null;
  const textMetadata = normalizeVideoTextMetadata({
    title,
    description,
    description_source: description == null ? null : "youtubejs_player",
    keywords: normalizeVideoKeywords(basic.tags, basic.keywords, microformat.keywords),
    keywords_observed: true,
  });
  return {
    id: basic.id || null,
    title,
    description: textMetadata.description,
    description_status: textMetadata.description_status,
    description_source: textMetadata.description_source,
    hashtags: textMetadata.hashtags,
    hashtags_observed: textMetadata.hashtags_observed,
    keywords: textMetadata.keywords,
    keywords_observed: textMetadata.keywords_observed,
    url: basic.id ? `https://www.youtube.com/watch?v=${encodeURIComponent(basic.id)}` : null,
    thumbnail_url: bestThumbnail(basic.thumbnail, microformat.thumbnails),
    duration_seconds: duration,
    duration_source: duration == null ? null : "youtubejs_player",
    length_text: duration == null
      ? null
      : `${Math.floor(duration / 60)}:${String(duration % 60).padStart(2, "0")}`,
    view_count_text: viewCount == null ? null : String(viewCount),
    view_count_source: viewCount == null ? null : "youtubejs_player",
    like_count: likeCount,
    like_count_source: likeCount == null ? null : "youtubejs_next",
    comment_count: commentCount,
    comment_count_status: commentStatus,
    comments_disabled: commentsDisabled,
    comments_status_source: commentsSource,
    comment_count_source: commentsSource,
    comments_first_page: commentsDisabled === true
      ? emptyYoutubeCommentPage({ totalCount: 0 })
      : commentsPage,
    published_at: published.value,
    published_text: published.value?.slice(0, 10) ?? null,
    published_at_precision: published.precision,
    published_at_source: published.value ? "youtubejs_player_microformat" : null,
    availability,
    access_status: accessStatus,
    is_unlisted: isUnlisted,
    playability_status: playabilityStatus,
    playability_reason: playabilityReason,
    playability_kind: playability.kind,
    playability_reason_code: playability.reason_code,
    playability_retry_mode: playability.retry_mode,
    is_live: isLive,
    was_live: wasLive,
    is_upcoming: isUpcoming,
    live_status: isUpcoming ? "is_upcoming" : isLive ? "is_live" : wasLive ? "was_live" : "not_live",
    live_scheduled_at: isUpcoming ? startTimestamp : null,
    live_started_at: isUpcoming ? null : startTimestamp,
    live_ended_at: endTimestamp,
    channel_id: basic.channel_id || microformat.channel?.id || null,
    content_type_signals: {
      source: contentTypeSignals?.source ?? "youtubei_player",
      canonical_url: contentTypeSignals?.canonical_url ?? null,
      is_shorts_eligible: typeof contentTypeSignals?.is_shorts_eligible === "boolean"
        ? contentTypeSignals.is_shorts_eligible
        : null,
      is_live_content: isLiveContent,
      is_live: typeof contentTypeSignals?.is_live === "boolean"
        ? contentTypeSignals.is_live
        : isLive,
      is_upcoming: typeof contentTypeSignals?.is_upcoming === "boolean"
        ? contentTypeSignals.is_upcoming
        : isUpcoming,
      is_live_now: typeof contentTypeSignals?.is_live_now === "boolean"
        ? contentTypeSignals.is_live_now
        : null,
      has_live_broadcast_details: contentTypeSignals?.has_live_broadcast_details === true,
    },
    extractor_version: `youtubei.js@${VERSION}`,
    source: "youtubejs_get_info",
    youtubejs_raw_summary: {
      playability_status: playabilityStatus,
      playability_reason: playabilityReason,
      has_player: Boolean(player),
      has_microformat: Boolean(player?.microformat),
      comments_query_used: comments != null || commentsPage != null,
    },
  };
}

export async function fetchYoutubeJsBasicPlayerInfo(client, videoId) {
  if (!client || typeof client.getBasicInfo !== "function") {
    throw new TypeError("YouTube.js client with getBasicInfo() is required");
  }
  const cleanVideoId = String(videoId ?? "").trim();
  if (!cleanVideoId) throw new Error("video_id is required");
  return client.getBasicInfo(cleanVideoId, { client: "WEB" });
}

function throwYoutubeJsBotChallenge(videoId, playabilityStatus, playabilityReason) {
  const error = annotateYoutubeFailure(new Error(`YouTube bot challenge: ${playabilityReason || playabilityStatus}`), {
    status: 200,
    body: String(playabilityReason || playabilityStatus),
    source: "youtubejs_player",
    targetUrl: `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`,
    client: "WEB",
  });
  recordChannelExecutionFailure({
    error,
    engine: "youtubejs",
    client: "WEB",
    status: 200,
    body: String(playabilityReason || playabilityStatus),
    source: "youtubejs_player",
    targetUrl: `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`,
  });
  throw error;
}

export async function fetchYoutubeJsPlayerTypeDetail(videoId) {
  if (!youtubeJsDetailEnabled()) throw new Error("YouTube.js detail extraction is disabled");
  const cleanVideoId = String(videoId ?? "").trim();
  if (!cleanVideoId) throw new Error("video_id is required");
  const current = await getRuntime();
  const startedAt = Date.now();
  const requestStart = current.stats.requests;
  current.playerTypeSurfaces.delete(cleanVideoId);
  let info;
  let contentTypeSignals;
  try {
    info = await fetchYoutubeJsBasicPlayerInfo(current.client, cleanVideoId);
    contentTypeSignals = current.playerTypeSurfaces.get(cleanVideoId) ?? null;
  } finally {
    current.playerTypeSurfaces.delete(cleanVideoId);
  }
  const playabilityStatus = info?.playability_status?.status || null;
  const playabilityReason = info?.playability_status?.reason || null;
  if (isYoutubeJsBotChallenge(playabilityStatus, playabilityReason)) {
    throwYoutubeJsBotChallenge(cleanVideoId, playabilityStatus, playabilityReason);
  }
  const basic = info?.basic_info || {};
  const playability = resolveYoutubePlayability({
    status: playabilityStatus,
    reason: playabilityReason,
  });
  return assertYoutubeContentObservation({
    id: basic.id || cleanVideoId,
    content_type_signals: contentTypeSignals ?? {
      source: "youtubei_player",
      canonical_url: null,
      is_shorts_eligible: null,
      is_live_content: typeof basic.is_live_content === "boolean" ? basic.is_live_content : null,
      is_live: typeof basic.is_live === "boolean" ? basic.is_live : null,
      is_upcoming: typeof basic.is_upcoming === "boolean" ? basic.is_upcoming : null,
      is_live_now: null,
      has_live_broadcast_details: false,
    },
    playability_status: playabilityStatus,
    playability_reason: playabilityReason,
    playability_kind: playability.kind,
    playability_reason_code: playability.reason_code,
    playability_retry_mode: playability.retry_mode,
    access_status: playability.access_status,
    availability: playability.availability,
    extractor_version: `youtubei.js@${VERSION}`,
    source: "youtubejs_get_basic_info",
    youtubejs_duration_ms: Date.now() - startedAt,
    youtubejs_request_count: current.stats.requests - requestStart,
  }, {
    videoId: cleanVideoId,
    source: "youtubejs_player",
  });
}

export async function fetchYoutubeJsVideoDetail(videoId, { signal = null } = {}) {
  if (!youtubeJsDetailEnabled()) throw new Error("YouTube.js detail extraction is disabled");
  const cleanVideoId = String(videoId ?? "").trim();
  if (!cleanVideoId) throw new Error("video_id is required");
  throwIfAborted(signal);
  const current = await getRuntime();
  return operationSignalStorage.run(signal, async () => {
    throwIfYoutubeJsOperationAborted();
    const startedAt = Date.now();
    const requestStart = current.stats.requests;
    current.playerTypeSurfaces.delete(cleanVideoId);
    let info;
    let contentTypeSignals;
    try {
      info = await current.client.getInfo(cleanVideoId, { client: "WEB" });
      throwIfYoutubeJsOperationAborted();
      contentTypeSignals = current.playerTypeSurfaces.get(cleanVideoId) ?? null;
    } finally {
      current.playerTypeSurfaces.delete(cleanVideoId);
    }
    const playabilityStatus = info?.playability_status?.status || null;
    const playabilityReason = info?.playability_status?.reason || null;
    if (isYoutubeJsBotChallenge(playabilityStatus, playabilityReason)) {
      throwYoutubeJsBotChallenge(cleanVideoId, playabilityStatus, playabilityReason);
    }
    let comments = null;
    let commentsError = null;
    const hint = parseObservedYoutubeJsCount(
      info?.comments_entry_point_header?.comment_count,
      {
        field: "comment_count",
        source: "youtubejs_next",
        context: { video_id: cleanVideoId },
      },
    );
    try {
      const raw = await fetchYoutubeJsCommentsSection(current.client, cleanVideoId);
      throwIfYoutubeJsOperationAborted();
      if (youtubeCommentsDisabled(raw)) {
        comments = emptyYoutubeCommentPage({ totalCount: 0 });
        comments.comments_disabled = true;
      } else {
        comments = normalizeYoutubeCommentPage(raw, {
          locale: DEFAULT_LANGUAGE,
          totalCount: hint,
        });
      }
    } catch (error) {
      throwIfYoutubeJsOperationAborted();
      commentsError = String(error?.message || error);
    }
    throwIfYoutubeJsOperationAborted();
    return {
      ...normalizeYoutubeJsVideoInfo(info, comments, { commentsError, contentTypeSignals }),
      youtubejs_duration_ms: Date.now() - startedAt,
      youtubejs_request_count: current.stats.requests - requestStart,
      youtubejs_comments_error: commentsError,
    };
  });
}

export function isYoutubeJsBotChallenge(status, reason) {
  return String(status || "").toUpperCase() === "LOGIN_REQUIRED"
    && youtubeChallengeTextSignal(reason, { allowBareCaptcha: true });
}
