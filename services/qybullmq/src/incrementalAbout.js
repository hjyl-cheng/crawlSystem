import { recordAboutObservation } from "./aboutObservationStore.js";
import { buildAboutObservation } from "./aboutObservation.js";
import { currentChannelExecution } from "./channelExecutionContext.js";

export async function executeIncrementalAbout({
  plan,
  runId,
  getChannelSnapshot,
  withTransaction,
  startedAt,
  crawlerVersion = String(process.env.CRAWLER_VERSION || "qy-v16"),
  recordAbout = recordAboutObservation,
  locale = process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en",
}) {
  const snapshot = await getChannelSnapshot();
  if (snapshot.about_requested !== true) {
    throw new Error("incremental About requires a Channel snapshot with getAbout enabled");
  }
  const observedAt = new Date().toISOString();
  const executionAttemptId = currentChannelExecution()?.attempt_id
    ?? `job-attempt:${plan.job_id}`;
  const observation = buildAboutObservation(snapshot, {
    locale,
    executionAttemptId,
    channelId: plan.channel_id,
    runId,
    observedAt,
    planId: plan.plan_id,
    planDay: plan.plan_day,
    triggerReason: "clock_due",
    scheduledAt: plan.scheduled_at,
    startedAt,
    crawlerVersion,
  });
  return withTransaction((client) => recordAbout(client, observation));
}
