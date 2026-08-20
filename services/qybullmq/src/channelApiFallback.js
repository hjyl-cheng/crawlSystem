function cleanText(value) {
  return String(value ?? "").trim();
}

function observedSubscriberCount(metadata) {
  if (metadata?.subscriber_count === null || metadata?.subscriber_count === undefined || metadata?.subscriber_count === "") {
    return false;
  }
  return Boolean(cleanText(metadata?.subscriber_count_source));
}

export function channelApiFallbackMissingFields(metadata, { enforceMinSubscribers = false } = {}) {
  const missing = [];
  if (!cleanText(metadata?.title)) missing.push("title");
  if (enforceMinSubscribers && !observedSubscriberCount(metadata)) missing.push("subscriber_count");
  return missing;
}

export async function resolveChannelApiFallback({
  channelId,
  metadata,
  enforceMinSubscribers = false,
  fallbackMode = "disabled",
  apiKeys = [],
  timeoutMs = 12000,
  dailyRequestLimit = 0,
  reserveRequest,
  fetchDetails,
}) {
  const cleanChannelId = cleanText(channelId);
  if (!cleanChannelId) throw new Error("channel data api fallback requires channel_id");
  const missingFields = channelApiFallbackMissingFields(metadata, { enforceMinSubscribers });
  if (missingFields.length === 0) {
    return { attempted: false, reason: "not_needed", missingFields, detail: null };
  }
  if (fallbackMode !== "emergency") {
    return { attempted: false, reason: "disabled", missingFields, detail: null };
  }
  const keys = [...new Set((apiKeys ?? []).map(cleanText).filter(Boolean))];
  if (keys.length === 0) throw new Error("channel data api fallback required but no API key is configured");
  if (typeof reserveRequest !== "function") throw new Error("channel data api fallback requires a quota reservation function");
  if (typeof fetchDetails !== "function") throw new Error("channel data api fallback requires a fetch function");

  let lastError = null;
  for (let keyIndex = 0; keyIndex < keys.length; keyIndex += 1) {
    const usage = await reserveRequest(dailyRequestLimit, 1);
    if (!usage) throw new Error("channel data api daily request limit reached");
    try {
      const result = await fetchDetails([cleanChannelId], keys[keyIndex], { timeoutMs });
      const detail = result?.detailsById?.get?.(cleanChannelId) ?? null;
      if (!detail) {
        return {
          attempted: true,
          reason: "not_found",
          missingFields,
          detail: null,
          raw: result?.raw ?? null,
          returnedCount: Number(result?.returnedCount ?? 0),
          keyIndex,
        };
      }
      return {
        attempted: true,
        reason: "resolved",
        missingFields,
        detail,
        raw: result.raw ?? null,
        returnedCount: Number(result.returnedCount ?? 0),
        keyIndex,
      };
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError ?? new Error(`channel data api fallback failed for ${cleanChannelId}`);
}

export function resolveChannelQualificationAfterApi(qualification, fallback) {
  if (fallback?.attempted && fallback?.reason === "not_found") {
    return { ...qualification, qualified: false, status: "failed", reason: "channel_unavailable" };
  }
  if (qualification?.qualified) return qualification;
  if (qualification?.reason !== "subscriber_count_unknown") return qualification;
  if (!fallback?.attempted) return qualification;
  return {
    ...qualification,
    status: "failed",
    reason: fallback.detail?.hidden_subscriber_count === true
      ? "subscriber_count_hidden"
      : "subscriber_count_unavailable",
  };
}
