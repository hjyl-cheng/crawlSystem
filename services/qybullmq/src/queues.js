import { Queue, QueueEvents } from "bullmq";
import IORedis from "ioredis";
import { createHash } from "node:crypto";

export const queueNames = [
  "youtube-query-quality",
  "youtube-discover-page",
  "youtube-channel-crawl",
  "youtube-channel-incremental",
  "youtube-content-enrich",
  "youtube-content-detail",
  "youtube-data-api-batch",
  "youtube-agent-batch",
  "youtube-agent-incremental",
  "youtube-finalize",
];

export const queuesByRole = {
  queryQuality: "youtube-query-quality",
  discoverPage: "youtube-discover-page",
  channelCrawl: "youtube-channel-crawl",
  channelIncremental: "youtube-channel-incremental",
  contentEnrich: "youtube-content-enrich",
  contentDetail: "youtube-content-detail",
  dataApiBatch: "youtube-data-api-batch",
  agentBatch: "youtube-agent-batch",
  agentIncremental: "youtube-agent-incremental",
  finalize: "youtube-finalize",
};

const queryPipelineQueueNames = Object.freeze([
  queuesByRole.discoverPage,
  queuesByRole.channelCrawl,
  queuesByRole.contentDetail,
  queuesByRole.dataApiBatch,
  queuesByRole.agentBatch,
  queuesByRole.finalize,
]);

export function hasQueryPipelineQueueBacklog(stats = {}) {
  return queryPipelineQueueNames.some((queueName) => {
    const row = stats[queueName] ?? {};
    return Number(row.waiting ?? 0)
      + Number(row.active ?? 0)
      + Number(row.delayed ?? 0)
      + Number(row.paused ?? 0)
      + Number(row.prioritized ?? 0)
      + Number(row["waiting-children"] ?? 0) > 0;
  });
}

export const redisOptions = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

export const defaultJobOptions = {
  attempts: 3,
  backoff: { type: "exponential", delay: 5000, jitter: 0.5 },
  removeOnComplete: { age: 86400, count: 10000 },
  removeOnFail: { age: 604800, count: 20000 },
};

export function applyFailureRetryDecision(job, decision) {
  const retryMode = String(decision?.retry_mode || "default");
  if (retryMode === "none") {
    job?.discard?.();
    return { retry: false, retry_mode: retryMode };
  }
  return {
    retry: true,
    retry_mode: retryMode,
    requires_new_identity: retryMode === "new_identity",
  };
}

export function safeJobId(...parts) {
  const id = parts
    .flat()
    .map((part) => String(part ?? "").trim())
    .filter(Boolean)
    .map((part) => part.replace(/[^a-zA-Z0-9_-]+/g, "_").replace(/^_+|_+$/g, ""))
    .filter(Boolean)
    .join("__");
  const normalized = id || "job";
  if (normalized.length <= 256) return normalized;
  const hash = createHash("sha1").update(normalized).digest("hex").slice(0, 12);
  return `${normalized.slice(0, 240)}__${hash}`;
}

export function createRedisConnection() {
  return new IORedis(redisOptions);
}

export function createQueues() {
  return Object.fromEntries(
    queueNames.map((name) => [
      name,
      new Queue(name, {
        connection: redisOptions,
        defaultJobOptions,
      }),
    ]),
  );
}

export function createQueueEvents() {
  return Object.fromEntries(
    queueNames.map((name) => [name, new QueueEvents(name, { connection: redisOptions })]),
  );
}

export async function closeQueues(queues) {
  await Promise.all(Object.values(queues).map((queue) => queue.close()));
}

export async function getQueueStats(queues) {
  const out = {};
  for (const [name, queue] of Object.entries(queues)) {
    out[name] = await queue.getJobCounts(
      "waiting",
      "active",
      "delayed",
      "failed",
      "completed",
      "paused",
      "waiting-children",
      "prioritized",
    );
  }
  return out;
}
