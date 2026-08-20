import { createHash } from "node:crypto";

export const DORMANT_REASON = "no_published_content_within_90_days";
export const DORMANT_WINDOW_DAYS = 90;
export const DORMANT_RECHECK_MIN_DAYS = 30;
export const DORMANT_RECHECK_MAX_DAYS = 90;

function nonNegativeInteger(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : fallback;
}

export function utcCalendarDay(value) {
  const parsed = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError("value must be a timestamp");
  return parsed.toISOString().slice(0, 10);
}

export function addUtcCalendarDays(day, days) {
  const normalizedDay = String(day ?? "").trim();
  const parsed = new Date(`${normalizedDay}T00:00:00.000Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalizedDay)
      || Number.isNaN(parsed.getTime())
      || parsed.toISOString().slice(0, 10) !== normalizedDay) {
    throw new TypeError("day must be an ISO UTC calendar date");
  }
  parsed.setUTCDate(parsed.getUTCDate() + Number(days));
  return parsed.toISOString().slice(0, 10);
}

export function dormantRecheckDelayDays(channelId, cycle) {
  const id = String(channelId ?? "").trim();
  const normalizedCycle = nonNegativeInteger(cycle);
  if (!id) throw new TypeError("channelId is required");
  if (normalizedCycle <= 0) throw new TypeError("cycle must be positive");
  const digest = createHash("sha256")
    .update(`dormant-video-probe-v1\u0000${id}\u0000${normalizedCycle}`)
    .digest();
  const width = DORMANT_RECHECK_MAX_DAYS - DORMANT_RECHECK_MIN_DAYS + 1;
  return DORMANT_RECHECK_MIN_DAYS + (digest.readUInt32BE(0) % width);
}

export function buildDormantLifecycle({
  channelId,
  observedAt,
  dormantSince = null,
  dormantCycle = 0,
} = {}) {
  const observed = new Date(observedAt);
  if (Number.isNaN(observed.getTime())) throw new TypeError("observedAt must be a timestamp");
  const cycle = nonNegativeInteger(dormantCycle) + 1;
  const since = dormantSince == null ? observed : new Date(dormantSince);
  if (Number.isNaN(since.getTime())) throw new TypeError("dormantSince must be a timestamp");
  const delayDays = dormantRecheckDelayDays(channelId, cycle);
  return Object.freeze({
    lifecycle_status: "dormant",
    dormant_reason: DORMANT_REASON,
    dormant_since: since.toISOString(),
    dormant_recheck_day: addUtcCalendarDays(utcCalendarDay(observed), delayDays),
    dormant_last_probe_at: observed.toISOString(),
    dormant_cycle: cycle,
    dormant_recheck_delay_days: delayDays,
  });
}

export function activeVideoActivity(recentPublishedContentCount) {
  const count = nonNegativeInteger(recentPublishedContentCount);
  return Object.freeze({
    window_days: DORMANT_WINDOW_DAYS,
    recent_published_content_count: count,
    lifecycle_status: "active",
    dormant_reason: null,
    dormant_since: null,
    dormant_recheck_day: null,
    dormant_cycle: 0,
  });
}

export function dormantVideoActivity(state) {
  if (state?.lifecycle_status !== "dormant") {
    throw new TypeError("a dormant lifecycle state is required");
  }
  return Object.freeze({
    window_days: DORMANT_WINDOW_DAYS,
    recent_published_content_count: 0,
    lifecycle_status: "dormant",
    dormant_reason: state.dormant_reason,
    dormant_since: state.dormant_since,
    dormant_recheck_day: state.dormant_recheck_day,
    dormant_cycle: state.dormant_cycle,
  });
}

export function evaluateVideoActivity({
  recentPublishedContentCount = 0,
  uncertainContentCount = 0,
  discoveryComplete = false,
} = {}) {
  const recent = nonNegativeInteger(recentPublishedContentCount);
  const uncertain = nonNegativeInteger(uncertainContentCount);
  if (recent > 0) {
    return Object.freeze({ decision: "active", recent, uncertain, conclusive: true });
  }
  if (!discoveryComplete || uncertain > 0) {
    return Object.freeze({ decision: "inconclusive", recent, uncertain, conclusive: false });
  }
  return Object.freeze({ decision: "dormant", recent: 0, uncertain: 0, conclusive: true });
}

