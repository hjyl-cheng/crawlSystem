import { randomUUID as nodeRandomUUID } from "node:crypto";
import { queuesByRole } from "./queues.js";

const JOB_PREFIXES = Object.freeze({
  youtubejs_canary: "youtubejs-canary",
  comment_backfill: "comment-backfill",
  comment_probe: "comment-probe",
  incremental_video_probe: "incremental-video-probe",
});

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

export function buildManagedDiagnosticJob({
  kind,
  channelId,
  runId = null,
  randomUUID = nodeRandomUUID,
} = {}) {
  const normalizedKind = String(kind ?? "").trim();
  const prefix = JOB_PREFIXES[normalizedKind];
  if (!prefix) {
    throw new TypeError(`unsupported managed diagnostic Job kind: ${normalizedKind || "missing"}`);
  }
  if (typeof randomUUID !== "function") throw new TypeError("randomUUID must be a function");
  const nonce = requiredText(randomUUID(), "diagnostic Job UUID");
  const data = Object.freeze({
    channel_id: requiredText(channelId, "channelId"),
    run_id: runId == null ? null : requiredText(runId, "runId"),
    dispatch_generation: 1,
  });
  return Object.freeze({
    id: `${prefix}:${nonce}`,
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data,
  });
}
