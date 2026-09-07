import { Worker } from "bullmq";
import { closeDb, query } from "../../src/db.js";
import {
  closeFullCrawlYoutubeJsQueues,
  executeFullCrawlYoutubeJs,
} from "../../src/fullCrawlYoutubeJs.js";
import {
  clearChannelCandidateJobAttempt,
  markChannelCandidateJobAttemptActive,
} from "../../src/managedWorkerJob.js";
import { bullmqPrefix, queuesByRole, redisOptions } from "../../src/queues.js";
import { assertFullCrawlWorkerLane, fullCrawlWorkerPrefix } from "../../src/fullCrawlCanary.js";

function report(message) {
  return new Promise((resolve, reject) => {
    process.send(message, (error) => error ? reject(error) : resolve());
  });
}

const worker = new Worker(queuesByRole.channelCrawl, async (job) => {
  assertFullCrawlWorkerLane(job, process.env.FULL_CRAWL_CANARY_WORKER === "true");
  if (!await markChannelCandidateJobAttemptActive(query, job)) {
    throw new Error("Failed to acquire Channel Candidate attempt");
  }
  const result = await executeFullCrawlYoutubeJs(job);
  if (process.env.FULL_CRAWL_TEST_BLOCK_AFTER_EXECUTION === "true") {
    await report({ event: "before_queue_ack", jobId: job.id, result });
    await new Promise(() => {});
  }
  await clearChannelCandidateJobAttempt(query, job);
  return result;
}, {
  connection: redisOptions,
  prefix: fullCrawlWorkerPrefix({ prefix: bullmqPrefix, enabledQueues: [queuesByRole.channelCrawl], canary: process.env.FULL_CRAWL_CANARY_WORKER === "true" }),
  concurrency: 1,
  lockDuration: 1500,
  stalledInterval: 500,
  maxStalledCount: 2,
});

worker.on("error", (error) => {
  void report({ event: "worker_error", message: error.stack }).catch(() => {});
});
worker.on("failed", (job, error) => {
  void report({ event: "failed", jobId: job?.id, message: error.stack }).catch(() => {});
});
worker.on("stalled", (jobId) => {
  void report({ event: "stalled", jobId }).catch(() => {});
});
worker.on("completed", (job, result) => {
  void report({ event: "completed", jobId: job.id, attempt: job.attemptsStarted, result })
    .catch(() => {});
});

let closing = false;
process.on("message", async (message) => {
  if (message?.event !== "shutdown" || closing) return;
  closing = true;
  try {
    await worker.close();
    await closeFullCrawlYoutubeJsQueues();
    await closeFullCrawlYoutubeJsQueues();
    await closeDb();
    await report({ event: "closed" });
    process.disconnect();
  } catch (error) {
    await report({ event: "shutdown_error", message: error.stack });
    process.exitCode = 1;
    process.disconnect();
  }
});

await worker.waitUntilReady();
await report({ event: "ready" });
