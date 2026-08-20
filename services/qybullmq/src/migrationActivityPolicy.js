import { localizedPublishedUtcDay } from "./localizedTime.js";

function count(value) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
}

function utcDayNumber(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return Math.floor(Date.UTC(
    parsed.getUTCFullYear(),
    parsed.getUTCMonth(),
    parsed.getUTCDate(),
  ) / 86400000);
}

function isUnfinishedLive(entry) {
  const status = String(entry?.live_status ?? "").trim().toLowerCase();
  return entry?.is_live === true || ["is_live", "live"].includes(status);
}

function isUpcoming(entry) {
  const status = String(entry?.live_status ?? "").trim().toLowerCase();
  return entry?.is_upcoming === true || ["is_upcoming", "upcoming"].includes(status);
}

export function evaluateMigrationUploadsActivity({
  required = false,
  entries = [],
  evidenceComplete = false,
  maxAgeDays = 90,
  observedAt = new Date(),
  locale = "en",
} = {}) {
  const windowDays = Math.max(1, count(maxAgeDays) || 90);
  const observed = new Date(observedAt);
  const referenceDayNumber = utcDayNumber(observed);
  const referenceDay = Number.isNaN(observed.getTime())
    ? null
    : observed.toISOString().slice(0, 10);
  const sourceEntries = Array.isArray(entries) ? entries : [];
  let recent = 0;
  let uncertain = 0;
  let excludedUpcoming = 0;
  let newestPublishedDay = null;

  if (required && evidenceComplete && referenceDayNumber != null) {
    for (const entry of sourceEntries) {
      if (isUpcoming(entry)) {
        excludedUpcoming += 1;
        continue;
      }
      if (isUnfinishedLive(entry)) {
        uncertain += 1;
        continue;
      }
      const publishedDay = localizedPublishedUtcDay(
        entry?.published_at ?? entry?.published_day ?? entry?.published_text,
        { locale, now: observed.getTime() },
      );
      if (!publishedDay) {
        uncertain += 1;
        continue;
      }
      if (newestPublishedDay == null || publishedDay > newestPublishedDay) {
        newestPublishedDay = publishedDay;
      }
      const ageDays = referenceDayNumber - utcDayNumber(publishedDay);
      if (ageDays < windowDays) recent += 1;
    }
  }

  const common = {
    recentPublishedContentCount: recent,
    uncertainContentCount: uncertain,
    inspectedContentCount: sourceEntries.length,
    excludedUpcomingCount: excludedUpcoming,
    newestPublishedDay,
    referenceDay,
    maxAgeDays: windowDays,
    evidenceComplete: evidenceComplete === true,
  };
  if (!required) return { ...common, decision: "not_required", dormant: false, reason: null };
  if (!evidenceComplete || referenceDayNumber == null) {
    return { ...common, decision: "pending", dormant: false, reason: null };
  }
  if (recent > 0) return { ...common, decision: "continue", dormant: false, reason: null };
  if (uncertain > 0) return { ...common, decision: "inconclusive", dormant: false, reason: null };
  return {
    ...common,
    decision: "dormant",
    dormant: true,
    reason: "no_published_content_within_" + windowDays + "_days",
  };
}

export function evaluateMigrationActivity({
  required = false,
  detailStatus = null,
  recentPublishedContentCount = 0,
  uncertainContentCount = 0,
  maxAgeDays = 90,
} = {}) {
  const recent = count(recentPublishedContentCount);
  const uncertain = count(uncertainContentCount);
  const windowDays = Math.max(1, count(maxAgeDays) || 90);

  if (!required) {
    return {
      decision: "not_required",
      activate: false,
      dormant: false,
      reject: false,
      reason: null,
      recentPublishedContentCount: recent,
      uncertainContentCount: uncertain,
      maxAgeDays: windowDays,
    };
  }
  if (detailStatus !== "done") {
    return {
      decision: "pending",
      activate: false,
      dormant: false,
      reject: false,
      reason: null,
      recentPublishedContentCount: recent,
      uncertainContentCount: uncertain,
      maxAgeDays: windowDays,
    };
  }
  if (recent > 0) {
    return {
      decision: "passed",
      activate: true,
      dormant: false,
      reject: false,
      reason: null,
      recentPublishedContentCount: recent,
      uncertainContentCount: uncertain,
      maxAgeDays: windowDays,
    };
  }
  if (uncertain > 0) {
    return {
      decision: "inconclusive",
      activate: true,
      dormant: false,
      reject: false,
      reason: null,
      recentPublishedContentCount: recent,
      uncertainContentCount: uncertain,
      maxAgeDays: windowDays,
    };
  }
  return {
    decision: "dormant",
    activate: false,
    dormant: true,
    reject: false,
    reason: "no_published_content_within_" + windowDays + "_days",
    recentPublishedContentCount: recent,
    uncertainContentCount: uncertain,
    maxAgeDays: windowDays,
  };
}

export function migrationActivityCanFinalize(activity) {
  return activity?.decision !== "rejected";
}
