import { createFullCrawlHandoff } from "./fullCrawlHandoff.js";
import { Queue } from "bullmq";
import { createVideoDetailApiFallback } from "./videoDetailApiFallback.js";
import { query, withTransaction } from "./db.js";
import { createFullCrawlYoutubeJsExecutor } from "./fullCrawlYoutubeJsFactory.js";
import { FullCrawlYoutubeJsStore } from "./fullCrawlYoutubeJsStore.js";
import { createLocalFullCrawlCollector } from "./localFullCrawlCollector.js";
import {
  bullmqPrefix,
  defaultJobOptions,
  queuesByRole,
  redisOptions,
} from "./queues.js";
import {
  fetchYoutubeJsChannelUploads,
  fetchYoutubeJsVideoDetail,
  openYoutubeJsChannel,
} from "./youtubeJs.js";

const outboundQueues = new Map();

function outboundQueue(name) {
  if (!outboundQueues.has(name)) {
    outboundQueues.set(name, new Queue(name, {
      connection: redisOptions,
      defaultJobOptions,
      ...(bullmqPrefix ? { prefix: bullmqPrefix } : {}),
    }));
  }
  return outboundQueues.get(name);
}

export async function closeFullCrawlYoutubeJsQueues() {
  const results = await Promise.allSettled(
    [...outboundQueues].map(async ([name, queue]) => {
      await queue.close();
      outboundQueues.delete(name);
    }),
  );
  const errors = results.filter((result) => result.status === "rejected")
    .map((result) => result.reason);
  if (errors.length > 0) {
    throw new AggregateError(errors, "Failed to close Full Crawl YouTubeJS queues");
  }
}

const handoff = createFullCrawlHandoff({query, withTransaction,
  discoveryQueue: () => outboundQueue(queuesByRole.discoverPage),
  finalizeQueue: () => outboundQueue(queuesByRole.finalize),
});

const executor = createFullCrawlYoutubeJsExecutor({
  videoApiFallback: createVideoDetailApiFallback({ query, withTransaction,
    loadSettings: async () => (await import("./pipelineV2.js")).getYoutubeApiSettingsV2() }),
  store: new FullCrawlYoutubeJsStore({ query, withTransaction }),
  collector: createLocalFullCrawlCollector({ youtube: {
    fetchChannel: openYoutubeJsChannel,
    fetchUploads: fetchYoutubeJsChannelUploads,
    fetchDetail: fetchYoutubeJsVideoDetail,
  } }),
  handoff,
  locale: process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en",
});

export async function executeFullCrawlYoutubeJs(
  job,
  { resumeMode = "initial" } = {},
) {
  return executor(job, { resumeMode });
}
