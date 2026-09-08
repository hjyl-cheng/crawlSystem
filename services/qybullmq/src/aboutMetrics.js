import { createHash } from "node:crypto";
import { parseLocalizedCountDetails } from "./localizedCount.js";

const ABOUT_SOURCE = "youtube_about";
const RESOLVED_STATUSES = new Set(["exact", "estimated"]);

const METRICS = [
  {
    name: "subscriber_count",
    valueKey: "subscriber_count",
    textKey: "subscriber_count_text",
    sourceKey: "subscriber_count_source",
  },
  {
    name: "total_view_count",
    valueKey: "total_view_count",
    textKey: "view_count_text",
    sourceKey: "view_count_source",
  },
  {
    name: "total_video_count",
    valueKey: "total_video_count",
    textKey: "video_count_text",
    sourceKey: "video_count_source",
  },
];

function text(value) {
  if (value === null || value === undefined) return null;
  const normalized = String(value).trim();
  return normalized || null;
}

function safeCount(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}

function normalizeMetric(metadata, definition, { aboutObserved, locale }) {
  const source = text(metadata?.[definition.sourceKey]);
  const rawText = text(metadata?.[definition.textKey]);
  if (aboutObserved && definition.name === "total_view_count"
      && rawText === null && text(metadata?.[definition.valueKey]) === null
      && (source === null || source === ABOUT_SOURCE)) {
    // Product policy: an observed About page may omit lifetime views.
    // Preserve the absent display text and distinguish this default from a
    // YouTube-provided zero. Failed requests and malformed values stay unresolved.
    return { value: 0, text: null, status: "exact", source: "youtube_about_missing_view_count" };
  }
  if (!aboutObserved || source !== ABOUT_SOURCE) {
    return { value: null, text: rawText, status: "unavailable", source };
  }

  if (rawText !== null) {
    const parsed = parseLocalizedCountDetails(rawText, { locale });
    if (!parsed) return { value: null, text: rawText, status: "unresolved", source };
    return {
      value: parsed.value,
      text: rawText,
      status: parsed.multiplier > 1 ? "estimated" : "exact",
      source,
    };
  }

  const numericValue = safeCount(metadata?.[definition.valueKey]);
  return numericValue === null
    ? { value: null, text: null, status: "unavailable", source }
    : { value: numericValue, text: String(numericValue), status: "exact", source };
}

function hashFacts(facts) {
  return `sha256:${createHash("sha256").update(JSON.stringify(facts)).digest("hex")}`;
}

export function normalizeAboutMetrics({
  metadata = {},
  aboutObserved = false,
  locale = process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en",
} = {}) {
  const metrics = Object.fromEntries(METRICS.map((definition) => [
    definition.name,
    normalizeMetric(metadata, definition, { aboutObserved: aboutObserved === true, locale }),
  ]));
  const resolvedCount = Object.values(metrics)
    .filter((metric) => RESOLVED_STATUSES.has(metric.status)).length;
  const outcome = aboutObserved !== true
    ? "failed"
    : resolvedCount === METRICS.length
      ? "complete"
      : "partial";
  const outcomeReasonCode = outcome === "failed"
    ? "get_about_failed"
    : outcome === "complete"
      ? "about_metrics_complete"
      : resolvedCount > 0
        ? "about_metrics_partial"
        : "about_metrics_unavailable";
  const facts = {
    subscriber_count: metrics.subscriber_count.value,
    subscriber_count_status: metrics.subscriber_count.status,
    total_view_count: metrics.total_view_count.value,
    total_view_count_status: metrics.total_view_count.status,
    total_video_count: metrics.total_video_count.value,
    total_video_count_status: metrics.total_video_count.status,
  };

  return {
    outcome,
    outcome_reason_code: outcomeReasonCode,
    snapshot_eligible: resolvedCount > 0,
    resolved_metric_count: resolvedCount,
    subscriber_count: metrics.subscriber_count.value,
    subscriber_count_text: metrics.subscriber_count.text,
    subscriber_count_status: metrics.subscriber_count.status,
    subscriber_count_source: metrics.subscriber_count.source,
    total_view_count: metrics.total_view_count.value,
    total_view_count_text: metrics.total_view_count.text,
    total_view_count_status: metrics.total_view_count.status,
    total_view_count_source: metrics.total_view_count.source,
    total_video_count: metrics.total_video_count.value,
    total_video_count_text: metrics.total_video_count.text,
    total_video_count_status: metrics.total_video_count.status,
    total_video_count_source: metrics.total_video_count.source,
    facts_hash: hashFacts(facts),
  };
}

export function combinedAboutObservationMetrics(metrics) {
  if (metrics?.outcome !== "failed") return metrics;
  return {
    ...metrics,
    outcome: "partial",
    outcome_reason_code: "get_about_failed_identity_current",
  };
}
