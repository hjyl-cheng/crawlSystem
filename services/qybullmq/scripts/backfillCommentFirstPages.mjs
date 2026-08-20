#!/usr/bin/env node

import { parseArgs } from "node:util";
import { ChannelExecutionRuntimeAdapter } from "../src/channelExecutionRuntimeAdapter.js";
import {
  DEFAULT_COMMENT_KEEP_BATCH_ID,
  backfillOneCommentFirstPage,
  loadCommentBackfillTargets,
  persistCommentFirstPage,
} from "../src/commentFirstPageBackfill.js";
import { closeDb, query, withTransaction } from "../src/db.js";
import { dynamicRotaProxyConfig } from "../src/fixedProxyConfig.js";
import { resolveWorkerIdentityPolicy } from "../src/identityPolicyCatalog.js";
import { closeProxyControlClient, proxyControlClient } from "../src/proxyControlClient.js";
import { RotaSlotAdapter } from "../src/rotaSlotAdapter.js";
import { closeYoutubeJs } from "../src/youtubeJs.js";
import { closePersistentYtDlp } from "../src/ytdlpSession.js";

const DEFAULTS = {
  batchId: process.env.COMMENT_BACKFILL_BATCH_ID || DEFAULT_COMMENT_KEEP_BATCH_ID,
  planDay: process.env.COMMENT_BACKFILL_PLAN_DAY || null,
  limit: Number(process.env.COMMENT_BACKFILL_LIMIT || 500),
  shardCount: Number(process.env.COMMENT_BACKFILL_SHARD_COUNT || 1),
  shardIndex: Number(process.env.COMMENT_BACKFILL_SHARD_INDEX || 0),
};

function positiveInteger(value, name, { min = 1, max = 10_000 } = {}) {
  const number = Number(value);
  if (!Number.isInteger(number) || number < min || number > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return number;
}

function optionsFromArgs() {
  const { values } = parseArgs({
    options: {
      apply: { type: "boolean", default: false },
      "batch-id": { type: "string" },
      "plan-day": { type: "string" },
      limit: { type: "string" },
      "shard-count": { type: "string" },
      "shard-index": { type: "string" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  if (values.help) {
    console.log("Usage: node scripts/backfillCommentFirstPages.mjs [--apply] [--batch-id=... | --plan-day=YYYY-MM-DD] [--limit=N] [--shard-count=N --shard-index=I]");
    process.exit(0);
  }
  const planDay = String(values["plan-day"] || DEFAULTS.planDay || "").trim() || null;
  if (planDay && values["batch-id"]) {
    throw new Error("--batch-id and --plan-day are mutually exclusive");
  }
  const shardCount = positiveInteger(values["shard-count"] ?? DEFAULTS.shardCount, "shard-count", {
    min: 1,
    max: 100,
  });
  return {
    apply: values.apply === true,
    batchId: planDay ? null : String(values["batch-id"] || DEFAULTS.batchId).trim(),
    planDay,
    limit: positiveInteger(values.limit ?? DEFAULTS.limit, "limit"),
    shardCount,
    shardIndex: positiveInteger(values["shard-index"] ?? DEFAULTS.shardIndex, "shard-index", {
      min: 0,
      max: shardCount - 1,
    }),
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
  if (!dynamicProxy) throw new Error("comment backfill requires a Rota channel slot");
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

async function backfillTargets(targets) {
  const results = [];
  for (const target of targets) {
    const result = await backfillOneCommentFirstPage(target, {
      persist: (_client, payload) => withTransaction((tx) => persistCommentFirstPage(tx, payload)),
    });
    results.push(result);
    console.log(JSON.stringify({ event: "comment_backfill_video", ...result }));
  }
  return results;
}

const options = optionsFromArgs();
const targets = await loadCommentBackfillTargets(query, options);
const summary = {
  event: "comment_backfill_plan",
  apply: options.apply,
  batch_id: options.batchId,
  plan_day: options.planDay,
  shard_count: options.shardCount,
  shard_index: options.shardIndex,
  target_count: targets.length,
  channel_count: new Set(targets.map((row) => row.channel_id)).size,
  sample_video_ids: targets.slice(0, 10).map((row) => row.source_content_id),
};
console.log(JSON.stringify(summary));

if (!options.apply) {
  await closeDb();
  process.exit(0);
}

const rotaSlot = createRotaSlot();
const policy = rotaSlot.policy;
const targetScopeBase = options.planDay ? `plan-day:${options.planDay}` : `batch:${options.batchId}`;
const targetScope = options.shardCount > 1
  ? `${targetScopeBase}:shard-${options.shardIndex}-of-${options.shardCount}`
  : targetScopeBase;
let results = [];
try {
  await rotaSlot.start();
  const job = {
    id: `comment-backfill:${targetScope}:${Date.now()}`,
    queueName: "youtube-channel-crawl",
    attemptsMade: 0,
    data: {
      channel_id: targets[0]?.channel_id ?? "comment-backfill",
      run_id: `comment-backfill:${targetScope}`,
    },
  };
  await rotaSlot.executeJob(job, {
    prepare: async () => ({
      kind: "ready",
      businessRunId: `comment-backfill:${targetScope}:${Date.now()}`,
      workloadKind: "channel_full",
      identityPolicyId: policy.id,
      identityPolicyVersion: policy.version,
      identityPolicyHash: policy.hash,
      initialResumeMode: "initial",
    }),
    executeAttempt: async () => {
      results = targets.length === 0 ? [] : await backfillTargets(targets);
      return {
        kind: "managed_work_complete",
        businessState: "terminal",
        result: { updated: results.filter((row) => row.updated).length },
      };
    },
  });
} finally {
  await rotaSlot.close().catch(() => null);
  await closePersistentYtDlp().catch(() => null);
  await closeYoutubeJs().catch(() => null);
  await closeProxyControlClient().catch(() => null);
  await closeDb();
}

console.log(JSON.stringify({
  event: "comment_backfill_done",
  apply: true,
  batch_id: options.batchId,
  plan_day: options.planDay,
  shard_count: options.shardCount,
  shard_index: options.shardIndex,
  target_count: targets.length,
  updated: results.filter((row) => row.updated).length,
  unresolved: results.filter((row) => !row.updated).length,
  with_items: results.filter((row) => Number(row.returned_count) > 0).length,
}));
