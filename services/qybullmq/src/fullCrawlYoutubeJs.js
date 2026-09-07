import { Queue } from "bullmq";
import { createVideoDetailApiFallback } from "./videoDetailApiFallback.js";
import { lockChannelCandidateAttempt } from "./channelCandidateAttemptMutations.js";
import { query, withTransaction } from "./db.js";
import { publishReadyDiscoveryPages } from "./discoveryPageWakeup.js";
import { reconcileDispatchBatchCandidateState } from "./dispatchBatchCandidateState.js";
import { dispatchFinalizeForRun } from "./finalizeDispatch.js";
import { createFullCrawlYoutubeJsExecutor } from "./fullCrawlYoutubeJsFactory.js";
import { FullCrawlYoutubeJsStore } from "./fullCrawlYoutubeJsStore.js";
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

async function candidateSettled({ candidateId, dispatchBatchId }) {
  if (dispatchBatchId) {
    await reconcileDispatchBatchCandidateState(query, dispatchBatchId);
  }
  try {
    const pageIds = await publishReadyDiscoveryPages({
      query,
      queue: outboundQueue(queuesByRole.discoverPage),
      candidateId,
    });
    if (pageIds.length > 0) {
      console.log(JSON.stringify({
        event: "discovery_page_qualification_ready",
        candidate_id: candidateId,
        page_ids: pageIds,
      }));
    }
  } catch (error) {
    console.error(JSON.stringify({
      event: "discovery_page_wakeup_failed",
      candidate_id: candidateId,
      error: error?.message || String(error),
    }));
  }
}

async function fetchCompleted({ channelId, runId, reason, candidateAttemptFence }) {
  return withTransaction(async (client) => {
    const transactionQuery = client.query.bind(client);
    await lockChannelCandidateAttempt(transactionQuery, candidateAttemptFence);
    return dispatchFinalizeForRun({
      query: transactionQuery,
      queue: outboundQueue(queuesByRole.finalize),
      channelId,
      runId,
      reason,
    });
  });
}

const executor = createFullCrawlYoutubeJsExecutor({
  videoApiFallback: createVideoDetailApiFallback({ query, withTransaction,
    loadSettings: async () => (await import("./pipelineV2.js")).getYoutubeApiSettingsV2() }),
  store: new FullCrawlYoutubeJsStore({ query, withTransaction }),
  youtube: {
    fetchChannel: openYoutubeJsChannel,
    fetchUploads: fetchYoutubeJsChannelUploads,
    fetchDetail: fetchYoutubeJsVideoDetail,
  },
  handoff: { candidateSettled, fetchCompleted },
  locale: process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en",
});

export async function executeFullCrawlYoutubeJs(
  job,
  { resumeMode = "initial" } = {},
) {
  return executor(job, { resumeMode });
}
