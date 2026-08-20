const CONTENT_TYPES = new Set(["video", "short", "live"]);

function cleanText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function firstBoolean(...values) {
  for (const value of values) {
    if (typeof value === "boolean") return value;
  }
  return null;
}

function canonicalKind(value, videoId) {
  const raw = cleanText(value);
  if (!raw) return null;
  try {
    const url = new URL(raw, "https://www.youtube.com");
    const shortMatch = url.pathname.match(/^\/shorts\/([^/?#]+)/i);
    if (shortMatch && decodeURIComponent(shortMatch[1]) === videoId) return "short";
    if (url.pathname === "/watch" && url.searchParams.get("v") === videoId) return "video";
  } catch {
    return null;
  }
  return null;
}

function contentUrl(videoId, contentType) {
  return contentType === "short"
    ? `https://www.youtube.com/shorts/${encodeURIComponent(videoId)}`
    : `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`;
}

function classification(videoId, contentType, source, canonicalUrl, authoritative) {
  return {
    content_type: contentType,
    source,
    canonical_url: cleanText(canonicalUrl) ?? contentUrl(videoId, contentType),
    authoritative,
  };
}

export function extractYoutubePlayerContentTypeSignals(player, {
  source = "youtubei_player",
  canonicalUrl = null,
} = {}) {
  const response = player && typeof player === "object" ? player : {};
  const details = response.videoDetails && typeof response.videoDetails === "object"
    ? response.videoDetails
    : {};
  const microformat = response.microformat?.playerMicroformatRenderer ?? {};
  const live = microformat.liveBroadcastDetails;
  return {
    source,
    canonical_url: cleanText(
      canonicalUrl
      ?? microformat.canonicalUrl
      ?? response.canonicalUrl,
    ),
    is_shorts_eligible: firstBoolean(
      microformat.isShortsEligible,
      details.isShortsEligible,
    ),
    is_live_content: firstBoolean(details.isLiveContent),
    is_live: firstBoolean(details.isLive),
    is_upcoming: firstBoolean(details.isUpcoming),
    is_live_now: firstBoolean(live?.isLiveNow),
    has_live_broadcast_details: Boolean(live && typeof live === "object"),
  };
}

function detailSignals(detail) {
  const structured = detail?.content_type_signals;
  if (structured && typeof structured === "object") {
    return {
      structured: true,
      source: cleanText(structured.source) ?? "youtube_watch_detail",
      canonical_url: cleanText(structured.canonical_url ?? detail?.canonical_url),
      is_shorts_eligible: firstBoolean(structured.is_shorts_eligible),
      is_live_content: firstBoolean(structured.is_live_content),
      is_live: firstBoolean(structured.is_live),
      is_upcoming: firstBoolean(structured.is_upcoming),
      is_live_now: firstBoolean(structured.is_live_now),
    };
  }
  return {
    structured: false,
    source: cleanText(detail?.source),
    canonical_url: cleanText(detail?.canonical_url),
    is_shorts_eligible: firstBoolean(detail?.is_shorts_eligible),
    is_live_content: firstBoolean(detail?.is_live_content),
    is_live: firstBoolean(detail?.is_live),
    is_upcoming: firstBoolean(detail?.is_upcoming),
    is_live_now: firstBoolean(detail?.is_live_now),
  };
}

function resolveFromDetail(videoId, detail) {
  if (!detail || typeof detail !== "object") return null;
  const signals = detailSignals(detail);
  const canonicalType = canonicalKind(signals.canonical_url, videoId);
  const watchSignals = /youtubei|youtubejs|youtube_watch|yt_dlp/i.test(signals.source ?? "");

  if (signals.structured) {
    if (signals.is_live_content === true) {
      return classification(
        videoId,
        "live",
        watchSignals ? "youtube_watch_live_content" : "youtube_detail_live_content",
        signals.canonical_url,
        watchSignals,
      );
    }
    if (signals.is_live === true || signals.is_upcoming === true || signals.is_live_now === true) {
      return classification(
        videoId,
        "live",
        watchSignals ? "youtube_watch_live_flag" : "youtube_detail_live_flag",
        signals.canonical_url,
        watchSignals,
      );
    }
    if (watchSignals && signals.is_shorts_eligible === true) {
      return classification(videoId, "short", "youtube_watch_shorts_eligible", signals.canonical_url, true);
    }
    if (watchSignals && canonicalType === "short") {
      return classification(videoId, "short", "youtube_watch_shorts_canonical", signals.canonical_url, true);
    }
    if (watchSignals && canonicalType === "video") {
      return classification(videoId, "video", "youtube_watch_canonical", signals.canonical_url, true);
    }
    const explicitlyNotLive = signals.is_live_content === false
      || (signals.is_live === false && signals.is_upcoming === false && signals.is_live_now === false);
    if (watchSignals && signals.is_shorts_eligible === false && explicitlyNotLive) {
      return classification(videoId, "video", "youtube_watch_type_flags", signals.canonical_url, true);
    }
    return null;
  }

  if (signals.is_live_content === true
      || signals.is_live === true
      || signals.is_upcoming === true
      || signals.is_live_now === true) {
    return classification(
      videoId,
      "live",
      watchSignals ? "youtube_watch_live_flag" : "youtube_detail_live_flag",
      signals.canonical_url,
      watchSignals,
    );
  }
  if (signals.is_shorts_eligible === true || canonicalType === "short") {
    return classification(
      videoId,
      "short",
      watchSignals ? "youtube_watch_shorts_signal" : "youtube_detail_shorts_signal",
      signals.canonical_url,
      watchSignals,
    );
  }
  if (canonicalType === "video") {
    return classification(
      videoId,
      "video",
      watchSignals ? "youtube_watch_canonical" : "youtube_detail_canonical",
      signals.canonical_url,
      watchSignals,
    );
  }
  return null;
}

function resolveFromUpload(videoId, upload) {
  if (!upload || typeof upload !== "object") return null;
  const explicitLive = upload.is_live === true || upload.is_upcoming === true;
  const contentType = explicitLive ? "live" : cleanText(upload.content_type);
  const source = explicitLive
    ? cleanText(upload.type_source) ?? "youtube_uploads_live_flag"
    : cleanText(upload.type_source);
  if (!source || !["short", "live"].includes(contentType) || !CONTENT_TYPES.has(contentType)) return null;
  return classification(videoId, contentType, source, upload.canonical_url, false);
}

export function resolveYoutubeContentType({ videoId, upload = null, detail = null } = {}) {
  const cleanVideoId = cleanText(videoId ?? upload?.id ?? upload?.video_id ?? detail?.id);
  if (!cleanVideoId) return null;
  return resolveFromDetail(cleanVideoId, detail) ?? resolveFromUpload(cleanVideoId, upload);
}
