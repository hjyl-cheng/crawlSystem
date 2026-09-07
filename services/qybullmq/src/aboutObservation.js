import { combinedAboutObservationMetrics, normalizeAboutMetrics } from "./aboutMetrics.js";
import { normalizeAboutObservationCurrent } from "./aboutCurrent.js";
import { aboutObservationIdempotencyKey } from "./aboutObservationStore.js";

export function buildAboutObservation(snapshot, {
  locale,
  executionAttemptId,
  channelId,
  runId,
  observedAt,
  startedAt,
  crawlerVersion,
  planId = null,
  planDay = null,
  triggerReason,
  scheduledAt = null,
}) {
  const aboutObserved = snapshot.about_observed === true;
  return {
    idempotencyKey: aboutObservationIdempotencyKey({ runId, executionAttemptId }),
    channelId, runId, observedAt, planId, planDay, triggerReason, scheduledAt,
    startedAt, finishedAt: observedAt, crawlerVersion,
    extractorVersions: { youtubejs: snapshot.raw?.engine ?? null },
    errorClass: snapshot.about_error?.name ?? null,
    errorMessage: snapshot.about_error
      ? String(snapshot.about_error?.message || snapshot.about_error) : null,
    about: combinedAboutObservationMetrics(normalizeAboutMetrics({
      metadata: snapshot.metadata, aboutObserved, locale,
    })),
    current: normalizeAboutObservationCurrent(snapshot.metadata, { aboutObserved, locale }),
  };
}
