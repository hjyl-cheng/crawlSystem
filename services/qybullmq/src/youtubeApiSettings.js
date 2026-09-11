// Pure settings loader shared by local and remote centers. Importing it opens
// no queues or database connections. Defaults and 30-second cache are unchanged.
function intValue(value, fallback, min, max) {
  const number = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(min, Math.min(max, number));
}

function parseKeys(value) {
  const values = Array.isArray(value) ? value : String(value ?? "").split(/[\s,;]+/);
  return [...new Set(values.map((item) => String(item ?? "").trim()).filter(Boolean))];
}

export function createYoutubeApiSettingsLoader({query,environment=process.env,nowImpl=()=>Date.now()}) {
  let youtubeApiSettingsCache={expiresAt:0,value:null};
  return async function getYoutubeApiSettings() {
    const now = nowImpl();
    if (youtubeApiSettingsCache.value && youtubeApiSettingsCache.expiresAt > now) return youtubeApiSettingsCache.value;
    const fallback = {
      apiKeys: parseKeys(environment.YOUTUBE_DATA_API_KEYS || environment.YOUTUBE_DATA_API_KEY || ""),
      timeoutMs: 12000,
      batchSize: 50,
      dailyRequestLimit: intValue(environment.YOUTUBE_DATA_API_DAILY_REQUEST_LIMIT, 500, 0, 10000),
      fallbackMode: environment.YOUTUBE_DATA_API_FALLBACK_MODE === "disabled" ? "disabled" : "emergency",
    };
    try {
      const rows = await query("SELECT value_json FROM crawler.settings WHERE setting_key = 'youtube_api' LIMIT 1");
      const value = rows.rows[0]?.value_json ?? {};
      const settings = {
        apiKeys: parseKeys(value.api_keys?.length ? value.api_keys : (value.api_key || fallback.apiKeys)),
        timeoutMs: intValue(value.timeout_ms, fallback.timeoutMs, 1000, 60000),
        batchSize: intValue(value.batch_size, fallback.batchSize, 1, 50),
        dailyRequestLimit: intValue(value.daily_request_limit, fallback.dailyRequestLimit, 0, 10000),
        fallbackMode: value.fallback_mode === "disabled" ? "disabled" : "emergency",
      };
      youtubeApiSettingsCache = { expiresAt: now + 30000, value: settings };
      return settings;
    } catch {
      youtubeApiSettingsCache = { expiresAt: now + 30000, value: fallback };
      return fallback;
    }
  };
}
