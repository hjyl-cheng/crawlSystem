import {
  extractChannelCandidates,
  extractVideoOwnerCandidates,
  fetchSearchInitial,
  fetchVideoOwnerSearchInitial,
  findAll,
  textValue,
} from "./youtube.js";
import { isParserContractError } from "./localizedParsing.js";
import { parseRequiredLocalizedCount } from "./localizedCount.js";
import { parseRequiredLocalizedAgeDays } from "./localizedTime.js";

const DEFAULT_LANGUAGE = process.env.YOUTUBE_LANGUAGE || "pt-BR";
const DEFAULT_COUNTRY = process.env.YOUTUBE_COUNTRY || "BR";
const DEFAULT_MIN_SUBSCRIBER_COUNT = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000);
const DEFAULT_TOP_VIDEOS = Number(process.env.QUERY_QUALITY_TOP_VIDEOS || 20);
const DEFAULT_CONCURRENCY = Number(process.env.QUERY_QUALITY_CONCURRENCY || 3);

function isRateLimitError(error) {
  return /429|captcha|rate|too many|not a bot/i.test(String(error?.message ?? error ?? ""));
}

function clamp(value, low = 0, high = 1) {
  return Math.max(low, Math.min(high, value));
}

function stripAccents(value) {
  return String(value ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").normalize("NFC");
}

function canonicalLocale(value) {
  try {
    return Intl.getCanonicalLocales(String(value || DEFAULT_LANGUAGE).replaceAll("_", "-"))[0] || DEFAULT_LANGUAGE;
  } catch {
    return DEFAULT_LANGUAGE;
  }
}

function foldText(value, locale = DEFAULT_LANGUAGE) {
  return stripAccents(String(value ?? "").trim().toLocaleLowerCase(canonicalLocale(locale))).replace(/\s+/g, " ");
}

export function queryTokens(value, locale = DEFAULT_LANGUAGE) {
  const folded = foldText(value, locale);
  try {
    const segmenter = new Intl.Segmenter(canonicalLocale(locale), { granularity: "word" });
    const tokens = [...segmenter.segment(folded)]
      .filter((item) => item.isWordLike)
      .map((item) => item.segment.trim())
      .filter(Boolean);
    if (tokens.length > 0) return tokens;
  } catch {
    // Unicode tokenization below remains deterministic on minimal ICU builds.
  }
  return folded.match(/[\p{L}\p{N}]+/gu) ?? [];
}

function tokenCoverage(text, tokens, locale = DEFAULT_LANGUAGE) {
  if (!tokens.length) return 0;
  const folded = foldText(text, locale);
  const matched = tokens.filter((token) => folded.includes(token)).length;
  return matched / tokens.length;
}

function average(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  return filtered.length ? filtered.reduce((sum, value) => sum + value, 0) / filtered.length : 0;
}

function logRatio(value, maxValue) {
  if (!value || value <= 0 || !maxValue || maxValue <= 0) return 0;
  return clamp(Math.log1p(value) / Math.log1p(maxValue));
}

function mergeCandidates(candidates) {
  const byId = new Map();
  for (const candidate of candidates) {
    if (!candidate?.channel_id) continue;
    const existing = byId.get(candidate.channel_id);
    if (!existing) {
      byId.set(candidate.channel_id, candidate);
      continue;
    }
    byId.set(candidate.channel_id, {
      ...existing,
      ...candidate,
      subscriber_count: existing.subscriber_count ?? candidate.subscriber_count ?? null,
      subscriber_count_text: existing.subscriber_count_text ?? candidate.subscriber_count_text ?? null,
      description: existing.description ?? candidate.description ?? null,
      source_video: existing.source_video ?? candidate.source_video ?? null,
      discovery_strategy: [existing.discovery_strategy, candidate.discovery_strategy].filter(Boolean).join("+"),
    });
  }
  return [...byId.values()];
}

function parseVideoStats(root, query, topVideos = DEFAULT_TOP_VIDEOS, locale = DEFAULT_LANGUAGE) {
  const tokens = queryTokens(query, locale);
  const videos = findAll(root, "videoRenderer").slice(0, topVideos);
  const rows = [];
  for (const renderer of videos) {
    const title = textValue(renderer.title) ?? "";
    const publishedText = textValue(renderer.publishedTimeText);
    const viewCountText = textValue(renderer.viewCountText) ?? textValue(renderer.shortViewCountText);
    const viewCount = viewCountText == null
      ? 0
      : parseRequiredLocalizedCount(viewCountText, {
          locale,
          field: "view_count",
          source: "youtube_query_quality_video_renderer",
          context: { query, title },
        });
    const ageDays = publishedText == null
      ? null
      : parseRequiredLocalizedAgeDays(publishedText, {
          locale,
          field: "published_age",
          source: "youtube_query_quality_video_renderer",
          context: { query, title },
        });
    rows.push({
      title,
      view_count: viewCount,
      published_text: publishedText,
      age_days: ageDays,
      relevance: tokenCoverage(title, tokens, locale),
    });
  }
  const viewCounts = rows.map((row) => row.view_count);
  const knownAges = rows.map((row) => row.age_days).filter((value) => value != null);
  return {
    count: rows.length,
    avg_views: average(viewCounts),
    max_views: Math.max(0, ...viewCounts),
    relevance_rate: average(rows.map((row) => row.relevance)),
    recent_rate: rows.length ? rows.filter((row) => row.age_days != null && row.age_days <= 30).length / rows.length : 0,
    avg_age_days: average(knownAges),
    samples: rows.slice(0, 5),
  };
}

function scoreSpecificity(tokens) {
  if (tokens.length <= 0) return 0;
  if (tokens.length === 1) return 0.35;
  if (tokens.length === 2) return 0.75;
  if (tokens.length <= 5) return 1;
  if (tokens.length <= 7) return 0.8;
  return 0.55;
}

function localFallbackScore(query, tokens, locale = DEFAULT_LANGUAGE) {
  const folded = foldText(query, locale);
  const tokenCount = tokens.length;
  const uniqueRatio = tokenCount > 0 ? new Set(tokens).size / tokenCount : 0;
  const codePointLength = [...folded.replace(/\s+/g, "")].length;
  const tooBroad = tokenCount <= 1;
  const tooLong = tokenCount >= 8;
  const specificity = scoreSpecificity(tokens);

  let score = 30 + (specificity * 42) + (uniqueRatio * 8);
  if (codePointLength >= 6 && codePointLength <= 80) score += 4;
  if (tooBroad) score -= 12;
  if (tooLong) score -= 8;
  return Math.round(clamp(score / 100) * 10000) / 100;
}

function fallbackQueryQuality(query, options = {}, reason = "youtube_unavailable", errors = []) {
  const locale = options.language || DEFAULT_LANGUAGE;
  const tokens = queryTokens(query, locale);
  const minSubscriberCount = Number(options.minSubscriberCount || DEFAULT_MIN_SUBSCRIBER_COUNT);
  const score = localFallbackScore(query, tokens, locale);
  const rateLimited = errors.some(isRateLimitError) || /429|captcha|rate|too many|not a bot/i.test(reason);
  return {
    query,
    quality_score: score,
    quality_status: "scored_fallback",
    checked_at: new Date().toISOString(),
    elapsed_ms: 0,
    signals: {
      min_subscriber_count: minSubscriberCount,
      candidate_count: 0,
      known_subscriber_count: 0,
      qualified_candidate_count: 0,
      qualified_rate: 0,
      known_subscriber_rate: 0,
      candidate_relevance_rate: 0,
      video_count: 0,
      video_relevance_rate: 0,
      avg_video_views: 0,
      max_video_views: 0,
      recent_video_rate: 0,
      avg_video_age_days: 0,
      specificity_tokens: tokens.length,
      institutional_penalty: 0,
      include_video_search: options.includeVideoSearch === true,
      rate_limited: rateLimited,
      fallback: true,
      fallback_reason: reason,
    },
    component_scores: {
      channel_yield: 0,
      relevance: Math.round(score * 100) / 100,
      demand: 0,
      freshness: 0,
      specificity: Math.round(scoreSpecificity(tokens) * 10000) / 100,
    },
    evidence: {
      top_channels: [],
      top_videos: [],
      source_urls: [],
    },
    errors,
  };
}

export function queryScoringErrorResult(query, options, error) {
  if (isParserContractError(error)) throw error;
  return fallbackQueryQuality(
    query,
    options,
    isRateLimitError(error) ? "youtube_rate_limited" : "score_query_error",
    [{ phase: "score_query", message: error?.message || String(error) }],
  );
}

export function institutionalPenalty() {
  // No language-neutral structured signal currently identifies institutions.
  // Applying a partial vocabulary would systematically bias scores by market.
  return 0;
}

export async function scoreQueryQuality(query, options = {}) {
  const startedAt = Date.now();
  const language = options.language || DEFAULT_LANGUAGE;
  const country = options.country || DEFAULT_COUNTRY;
  const minSubscriberCount = Number(options.minSubscriberCount || DEFAULT_MIN_SUBSCRIBER_COUNT);
  const topVideos = Number(options.topVideos || DEFAULT_TOP_VIDEOS);
  const includeVideoSearch = options.includeVideoSearch === true;
  const tokens = queryTokens(query, language);
  const errors = [];

  if (options.fallbackOnly === true) {
    return fallbackQueryQuality(query, options, "fallback_only");
  }

  let channelFetched = null;
  let ownerFetched = null;
  try {
    channelFetched = await fetchSearchInitial(query, { language, country });
  } catch (error) {
    errors.push({ phase: "channel_search", message: error?.message || String(error) });
  }
  if (includeVideoSearch) {
    try {
      ownerFetched = await fetchVideoOwnerSearchInitial(query, { language, country });
    } catch (error) {
      errors.push({ phase: "video_owner_search", message: error?.message || String(error) });
    }
  }

  const channelCandidates = channelFetched
    ? extractChannelCandidates(channelFetched.initialData, query, null, language).map((candidate) => ({ ...candidate, discovery_strategy: "channel_filter" }))
    : [];
  const ownerCandidates = ownerFetched
    ? extractVideoOwnerCandidates(ownerFetched.initialData, query, null, language)
    : [];
  const candidates = mergeCandidates([...channelCandidates, ...ownerCandidates]);
  const candidatesWithKnownSubs = candidates.filter((candidate) => candidate.subscriber_count != null);
  const qualifiedCandidates = candidatesWithKnownSubs.filter((candidate) => Number(candidate.subscriber_count) >= minSubscriberCount);
  const candidateTextRelevance = average(candidates.map((candidate) => tokenCoverage(
    `${candidate.title ?? ""} ${candidate.handle ?? ""} ${candidate.description ?? ""} ${candidate.source_video?.title ?? ""}`,
    tokens,
    language,
  )));
  const videos = parseVideoStats(ownerFetched?.initialData, query, topVideos, language);
  const rateLimited = errors.some(isRateLimitError);

  if (errors.length > 0 && candidates.length === 0 && videos.count === 0) {
    return fallbackQueryQuality(
      query,
      options,
      rateLimited ? "youtube_rate_limited" : "youtube_unavailable",
      errors,
    );
  }

  const knownRate = candidates.length ? candidatesWithKnownSubs.length / candidates.length : 0;
  const qualifiedRate = candidatesWithKnownSubs.length ? qualifiedCandidates.length / candidatesWithKnownSubs.length : 0;
  const qualifiedDensity = clamp(qualifiedCandidates.length / 8);
  const channelYieldScore = 100 * (
    0.55 * qualifiedDensity
    + 0.30 * qualifiedRate
    + 0.15 * knownRate
  );
  const relevanceScore = 100 * (
    0.60 * candidateTextRelevance
    + 0.40 * videos.relevance_rate
  );
  const demandScore = 100 * (
    0.65 * logRatio(videos.avg_views, 500_000)
    + 0.35 * logRatio(videos.max_views, 5_000_000)
  );
  const freshnessScore = 100 * (
    0.75 * videos.recent_rate
    + 0.25 * (videos.avg_age_days ? Math.exp(-videos.avg_age_days / 180) : 0)
  );
  const specificityScore = 100 * scoreSpecificity(tokens);
  const institutionPenalty = institutionalPenalty(candidates, videos, language);
  const errorPenalty = Math.min(errors.length * 12, 30);
  const noSignalPenalty = candidates.length === 0 && videos.count === 0 ? 35 : 0;

  const rawScore = (
    0.40 * channelYieldScore
    + 0.25 * relevanceScore
    + 0.15 * demandScore
    + 0.10 * freshnessScore
    + 0.10 * specificityScore
  ) - (institutionPenalty * 18) - errorPenalty - noSignalPenalty;
  const qualityScore = Math.round(clamp(rawScore / 100) * 10000) / 100;

  return {
    query,
    quality_score: qualityScore,
    quality_status: errors.length > 0 ? "scored_partial" : "scored",
    checked_at: new Date().toISOString(),
    elapsed_ms: Date.now() - startedAt,
    signals: {
      min_subscriber_count: minSubscriberCount,
      candidate_count: candidates.length,
      known_subscriber_count: candidatesWithKnownSubs.length,
      qualified_candidate_count: qualifiedCandidates.length,
      qualified_rate: Math.round(qualifiedRate * 10000) / 10000,
      known_subscriber_rate: Math.round(knownRate * 10000) / 10000,
      candidate_relevance_rate: Math.round(candidateTextRelevance * 10000) / 10000,
      video_count: videos.count,
      video_relevance_rate: Math.round(videos.relevance_rate * 10000) / 10000,
      avg_video_views: Math.round(videos.avg_views),
      max_video_views: videos.max_views,
      recent_video_rate: Math.round(videos.recent_rate * 10000) / 10000,
      avg_video_age_days: Math.round(videos.avg_age_days * 100) / 100,
      specificity_tokens: tokens.length,
      institutional_penalty: Math.round(institutionPenalty * 10000) / 10000,
      include_video_search: includeVideoSearch,
      rate_limited: rateLimited,
      fallback: false,
    },
    component_scores: {
      channel_yield: Math.round(channelYieldScore * 100) / 100,
      relevance: Math.round(relevanceScore * 100) / 100,
      demand: Math.round(demandScore * 100) / 100,
      freshness: Math.round(freshnessScore * 100) / 100,
      specificity: Math.round(specificityScore * 100) / 100,
    },
    evidence: {
      top_channels: candidates.slice(0, 8).map((candidate) => ({
        channel_id: candidate.channel_id,
        title: candidate.title,
        handle: candidate.handle,
        subscriber_count: candidate.subscriber_count,
        source_video_title: candidate.source_video?.title ?? null,
      })),
      top_videos: videos.samples,
      source_urls: [
        channelFetched?.url,
        ownerFetched?.url,
      ].filter(Boolean),
    },
    errors,
  };
}

export async function scoreQueryBatch(queries, options = {}) {
  const concurrency = Math.max(1, Math.min(Number(options.concurrency || DEFAULT_CONCURRENCY), 8));
  const uniqueQueries = Array.from(new Set((queries || []).map((query) => String(query ?? "").trim()).filter(Boolean)));
  const results = new Array(uniqueQueries.length);
  let nextIndex = 0;
  let fallbackOnly = options.fallbackOnly === true;

  async function worker() {
    while (nextIndex < uniqueQueries.length) {
      const index = nextIndex;
      nextIndex += 1;
      const query = uniqueQueries[index];
      try {
        results[index] = await scoreQueryQuality(query, { ...options, fallbackOnly });
        if (options.fallbackOnRateLimit !== false && results[index]?.signals?.rate_limited) {
          fallbackOnly = true;
        }
      } catch (error) {
        results[index] = queryScoringErrorResult(query, options, error);
        if (options.fallbackOnRateLimit !== false && results[index]?.signals?.rate_limited) {
          fallbackOnly = true;
        }
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, uniqueQueries.length) }, () => worker()));
  return results;
}
