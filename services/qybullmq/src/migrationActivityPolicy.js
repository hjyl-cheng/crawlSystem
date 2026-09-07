import { createVideoActivityAccumulator } from "./videoActivityEvidence.js";
import {
  normalizePublicationEvidence,
  PUBLICATION_TIME_CLASSIFIER_VERSION,
} from "./publicationTimeEvidence.js";

export const MIGRATION_ACTIVITY_POLICY_VERSION = "migration-activity-v2";

function count(value) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
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
} = {}) {
  const windowDays = Math.max(1, count(maxAgeDays) || 90);
  const observed = new Date(observedAt);
  const referenceDay = Number.isNaN(observed.getTime())
    ? null
    : observed.toISOString().slice(0, 10);
  const sourceEntries = Array.isArray(entries) ? entries : [];
  let excludedUpcoming = 0;
  let newestPublishedDay = null;
  const accumulator = createVideoActivityAccumulator({
    observedAt: observed, maxAgeDays: windowDays,
  });

  if (required && referenceDay != null) {
    for (const entry of sourceEntries) {
      if (isUpcoming(entry)) {
        excludedUpcoming += 1;
        continue;
      }
      if (isUnfinishedLive(entry)) {
        accumulator.addUnresolved();
        continue;
      }
      const publication = normalizePublicationEvidence({
        published_at: entry?.published_at ?? entry?.published_day,
        published_at_status: entry?.published_at_status,
        published_at_precision: entry?.published_at_precision,
        published_at_source: entry?.published_at_source,
      });
      const publishedDay = publication.published_at?.slice(0, 10) ?? null;
      if (newestPublishedDay == null || publishedDay > newestPublishedDay) {
        newestPublishedDay = publishedDay;
      }
      accumulator.add(publication);
    }
  }

  const common = {
    ...accumulator.evidence,
    inspectedContentCount: sourceEntries.length,
    excludedUpcomingCount: excludedUpcoming,
    newestPublishedDay,
    referenceDay,
    referenceAt: referenceDay == null ? null : observed.toISOString(),
    maxAgeDays: windowDays,
    evidenceComplete: evidenceComplete === true,
    classifierVersion: PUBLICATION_TIME_CLASSIFIER_VERSION,
    policyVersion: MIGRATION_ACTIVITY_POLICY_VERSION,
  };
  if (!required) return { ...common, decision: "not_required", dormant: false, reason: null };
  if (!evidenceComplete || referenceDay == null) {
    return { ...common, decision: "pending", dormant: false, reason: null };
  }
  if (common.recentPublishedContentCount > 0) return { ...common, decision: "continue", dormant: false, reason: null };
  if (common.uncertainContentCount > 0) return { ...common, decision: "inconclusive", dormant: false, reason: null };
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
  evidenceComplete = true,
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
  if (evidenceComplete !== true) {
    return {
      decision: "inconclusive",
      activate: true,
      dormant: false,
      reject: false,
      reason: "activity_evidence_incomplete",
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
