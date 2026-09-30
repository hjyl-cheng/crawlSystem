import { normalizeDetailConcurrency } from "./detailConcurrency.js";

function intValue(value, fallback, min, max) {
  const number = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(min, Math.min(max, number));
}

function numberValue(value, fallback, min, max) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(min, Math.min(max, number));
}

// Reads the shared 'crawl' settings row with the caller's database query, so a
// process with its own pool (the remote center) applies the same rules.
export function createCrawlSettingsLoader({ query, env = process.env }) {
  const defaultMinSubscribers = Number(env.MIN_SUBSCRIBER_COUNT || 1000);
  const defaultContentLimit = Number(env.YOUTUBE_CHANNEL_CONTENT_LIMIT || 30);
  const defaultContentMaxAgeDays = Number(env.YOUTUBE_CONTENT_MAX_AGE_DAYS || 90);
  const defaultDetailConcurrency = normalizeDetailConcurrency(env.YOUTUBE_DETAIL_CONCURRENCY, 2);
  let crawlSettingsCache = { expiresAt: 0, value: null };

  return async function getCrawlSettings() {
    const now = Date.now();
    if (crawlSettingsCache.value && crawlSettingsCache.expiresAt > now) return crawlSettingsCache.value;
    const fallback = {
      minSubscriberCount: intValue(defaultMinSubscribers, 1000, 0, 1_000_000_000),
      discoverStopMinQualifiedRatio: 1 / 3,
      channelContentLimit: intValue(defaultContentLimit, 30, 1, 100),
      contentMaxAgeDays: intValue(defaultContentMaxAgeDays, 90, 0, 3650),
      detailMaxAttempts: intValue(env.YOUTUBE_DETAIL_MAX_ATTEMPTS, 3, 1, 10),
      detailConcurrency: defaultDetailConcurrency,
      publishedAtRequiredPrecision: "date_only",
    };
    try {
      const rows = await query("SELECT value_json FROM crawler.settings WHERE setting_key = 'crawl' LIMIT 1");
      const value = rows.rows[0]?.value_json ?? {};
      const settings = {
        minSubscriberCount: intValue(value.min_subscriber_count, fallback.minSubscriberCount, 0, 1_000_000_000),
        discoverStopMinQualifiedRatio: numberValue(value.discover_stop_min_qualified_ratio, fallback.discoverStopMinQualifiedRatio, 0, 1),
        channelContentLimit: intValue(value.channel_content_limit, fallback.channelContentLimit, 1, 100),
        contentMaxAgeDays: intValue(value.content_max_age_days, fallback.contentMaxAgeDays, 0, 3650),
        detailMaxAttempts: intValue(value.detail_max_attempts, fallback.detailMaxAttempts, 1, 10),
        detailConcurrency: normalizeDetailConcurrency(value.detail_concurrency, fallback.detailConcurrency),
        publishedAtRequiredPrecision: "date_only",
      };
      crawlSettingsCache = { expiresAt: now + 30000, value: settings };
      return settings;
    } catch {
      crawlSettingsCache = { expiresAt: now + 30000, value: fallback };
      return fallback;
    }
  };
}
