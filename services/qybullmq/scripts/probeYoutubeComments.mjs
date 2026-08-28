#!/usr/bin/env node

import { ChannelExecutionRuntimeAdapter } from "../src/channelExecutionRuntimeAdapter.js";
import { closeDb } from "../src/db.js";
import { dynamicRotaProxyConfig } from "../src/fixedProxyConfig.js";
import { resolveWorkerIdentityPolicy } from "../src/identityPolicyCatalog.js";
import { buildManagedDiagnosticJob } from "../src/managedDiagnosticJob.js";
import { closeProxyControlClient, proxyControlClient } from "../src/proxyControlClient.js";
import { RotaSlotAdapter } from "../src/rotaSlotAdapter.js";
import {
  closeYoutubeJs,
  fetchYoutubeJsVideoDetail,
  inspectYoutubeJsCommentsSection,
} from "../src/youtubeJs.js";
import {
  classifyYoutubeCommentPage,
  normalizeYoutubeCommentPage,
  youtubeCommentsDisabled,
} from "../src/youtubeCommentPage.js";

const VIDEO_IDS = process.argv.slice(2);
const targets = VIDEO_IDS.length > 0
  ? VIDEO_IDS
  : ["pbChxZStqo8", "oW983TlO5mI", "H2VpsabLFv0", "DBXNpwTteg4"];

function allKeys(value, targetKey, output = 0) {
  if (!value || typeof value !== "object") return output;
  if (Array.isArray(value)) {
    return value.reduce((count, item) => allKeys(item, targetKey, count), output);
  }
  for (const [key, child] of Object.entries(value)) {
    if (key === targetKey) output += 1;
    output = allKeys(child, targetKey, output);
  }
  return output;
}

function firstNode(value, targetKey) {
  if (!value || typeof value !== "object") return null;
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = firstNode(item, targetKey);
      if (found) return found;
    }
    return null;
  }
  if (Object.prototype.hasOwnProperty.call(value, targetKey)) return value[targetKey];
  for (const child of Object.values(value)) {
    const found = firstNode(child, targetKey);
    if (found) return found;
  }
  return null;
}

function summarizeRaw(raw) {
  const entity = firstNode(raw, "commentEntityPayload");
  const header = firstNode(raw, "commentsHeaderRenderer") ?? firstNode(raw, "commentsHeaderViewModel");
  const message = firstNode(raw, "messageRenderer");
  return {
    top_keys: raw && typeof raw === "object" ? Object.keys(raw).slice(0, 30) : [],
    counts: {
      onResponseReceivedEndpoints: allKeys(raw, "onResponseReceivedEndpoints"),
      onResponseReceivedActions: allKeys(raw, "onResponseReceivedActions"),
      commentsHeaderRenderer: allKeys(raw, "commentsHeaderRenderer"),
      commentsHeaderViewModel: allKeys(raw, "commentsHeaderViewModel"),
      commentThreadRenderer: allKeys(raw, "commentThreadRenderer"),
      commentViewModel: allKeys(raw, "commentViewModel"),
      commentRenderer: allKeys(raw, "commentRenderer"),
      commentEntityPayload: allKeys(raw, "commentEntityPayload"),
      messageRenderer: allKeys(raw, "messageRenderer"),
      continuationItemRenderer: allKeys(raw, "continuationItemRenderer"),
      itemSectionRenderer: allKeys(raw, "itemSectionRenderer"),
    },
    header_keys: header && typeof header === "object" ? Object.keys(header) : [],
    entity_keys: entity && typeof entity === "object" ? Object.keys(entity) : [],
    entity_property_keys: entity?.properties && typeof entity.properties === "object"
      ? Object.keys(entity.properties)
      : [],
    entity_content_type: entity?.properties?.content == null
      ? null
      : typeof entity.properties.content,
    entity_content_keys: entity?.properties?.content && typeof entity.properties.content === "object"
      ? Object.keys(entity.properties.content)
      : [],
    message_text: message?.text?.simpleText
      ?? message?.text?.runs?.map((run) => run?.text || "").join("")
      ?? null,
    disabled_signal: youtubeCommentsDisabled(raw),
  };
}

function createRotaSlot() {
  const workerId = String(process.env.COMMENT_BACKFILL_WORKER_ID || "qy-comment-backfill").trim();
  const rotaProxyPassword = String(process.env.ROTA_BULLMQ_PROXY_PASSWORD || "");
  const dynamicProxy = dynamicRotaProxyConfig({
    slotRole: "channel",
    controlUrl: process.env.ROTA_PROXY_CONTROL_URL,
    controlToken: process.env.ROTA_PROXY_CONTROL_TOKEN,
    proxyPassword: rotaProxyPassword,
  });
  if (!dynamicProxy) throw new Error("comment probe requires a Rota channel slot");
  const resolvedPolicy = resolveWorkerIdentityPolicy({
    role: "channel",
    policyId: process.env.ROTA_IDENTITY_POLICY_ID,
    expectedWorkloadScope: process.env.ROTA_WORKLOAD_SCOPE_EXPECTED,
  });
  return new RotaSlotAdapter({
    client: proxyControlClient(),
    role: "channel",
    workerId,
    resolvedPolicy,
    proxyBaseUrl: String(process.env.ROTA_PROXY_BASE_URL || "http://youtube-rota-qy-core:8000"),
    proxyPassword: rotaProxyPassword,
    identityRuntime: new ChannelExecutionRuntimeAdapter({ workerId }),
  });
}

const rotaSlot = createRotaSlot();
const policy = rotaSlot.policy;
const reports = [];
try {
  await rotaSlot.start();
  const runId = `comment-probe:${Date.now()}`;
  await rotaSlot.executeJob(buildManagedDiagnosticJob({
    kind: "comment_probe",
    channelId: "comment-probe",
    runId,
  }), {
    prepare: async () => ({
      kind: "ready",
      businessRunId: `comment-probe:${Date.now()}`,
      workloadKind: "channel_full",
      identityPolicyId: policy.id,
      identityPolicyVersion: policy.version,
      identityPolicyHash: policy.hash,
      initialResumeMode: "initial",
    }),
    executeAttempt: async () => {
      for (const videoId of targets) {
        const report = { video_id: videoId };
        try {
          const raw = await inspectYoutubeJsCommentsSection(videoId);
          const page = normalizeYoutubeCommentPage(raw);
          const classified = classifyYoutubeCommentPage(page, {
            disabled: youtubeCommentsDisabled(raw) || page?.comments_disabled === true,
          });
          report.raw = summarizeRaw(raw);
          report.page = {
            total_count: page.total_count,
            returned_count: page.returned_count,
            comments_disabled: page.comments_disabled,
            surface: page.surface,
            first_text: page.comments[0]?.text ?? null,
            first_id: page.comments[0]?.comment_id ?? null,
          };
          report.classified = classified;
          try {
            const detail = await fetchYoutubeJsVideoDetail(videoId);
            report.detail = {
              comment_count: detail.comment_count,
              comment_count_status: detail.comment_count_status,
              comments_disabled: detail.comments_disabled,
              comment_count_source: detail.comment_count_source,
              returned_count: detail.comments_first_page?.returned_count ?? null,
              first_text: detail.comments_first_page?.comments?.[0]?.text ?? null,
              youtubejs_comments_error: detail.youtubejs_comments_error ?? null,
            };
          } catch (error) {
            report.detail_error = String(error?.message || error);
          }
        } catch (error) {
          report.error = String(error?.message || error);
        }
        reports.push(report);
        console.log(JSON.stringify({ event: "comment_probe_video", ...report }));
      }
      return {
        kind: "managed_work_complete",
        businessState: "terminal",
        result: { probed: reports.length },
      };
    },
  });
} finally {
  await rotaSlot.close().catch(() => null);
  await closeYoutubeJs().catch(() => null);
  await closeProxyControlClient().catch(() => null);
  await closeDb();
}

console.log(JSON.stringify({ event: "comment_probe_done", reports }, null, 2));
