import {
  combinedAboutObservationMetrics,
  normalizeAboutMetrics,
} from "./aboutMetrics.js";
import {
  aboutObservationIdempotencyKey,
  recordAboutObservation,
} from "./aboutObservationStore.js";
import { normalizeAboutObservationCurrent } from "./aboutCurrent.js";
import { currentChannelExecution } from "./channelExecutionContext.js";

export async function executeIncrementalAbout({
  plan,
  runId,
  getChannelSnapshot,
  withTransaction,
  startedAt,
  crawlerVersion = String(process.env.CRAWLER_VERSION || "qy-v16"),
  recordAbout = recordAboutObservation,
}) {
  const snapshot = await getChannelSnapshot();
  if (snapshot.about_requested !== true) {
    throw new Error("incremental About requires a Channel snapshot with getAbout enabled");
  }
  const observedAt = new Date().toISOString();
  const about = combinedAboutObservationMetrics(normalizeAboutMetrics({
    metadata: snapshot.metadata,
    aboutObserved: snapshot.about_observed === true,
  }));
  const current = normalizeAboutObservationCurrent(snapshot.metadata, {
    aboutObserved: snapshot.about_observed === true,
  });
  const executionAttemptId = currentChannelExecution()?.attempt_id
    ?? `job-attempt:${plan.job_id}`;
  return withTransaction((client) => recordAbout(client, {
    idempotencyKey: aboutObservationIdempotencyKey({ runId, executionAttemptId }),
    channelId: plan.channel_id,
    runId,
    observedAt,
    planId: plan.plan_id,
    planDay: plan.plan_day,
    triggerReason: "clock_due",
    scheduledAt: plan.scheduled_at,
    startedAt,
    finishedAt: observedAt,
    crawlerVersion,
    extractorVersions: { youtubejs: snapshot.raw?.engine ?? null },
    errorClass: snapshot.about_error?.name ?? null,
    errorMessage: snapshot.about_error
      ? String(snapshot.about_error?.message || snapshot.about_error)
      : null,
    about,
    current,
  }));
}
