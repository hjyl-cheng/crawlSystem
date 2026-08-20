import { UnrecoverableError, Worker } from "bullmq";
import {
  createFeatureRecalcProcessor,
  FEATURE_RECALC_QUEUE,
  FeatureIngestPermanentError,
} from "./featureTransport.js";
import { redisOptions } from "./queues.js";
import { environmentValue } from "./runtimeEnvironment.js";

const endpoint = process.env.FEATURE_INGEST_URL || "http://127.0.0.1:8090/v1/crawler-observations";
const processor = createFeatureRecalcProcessor({
  endpoint,
  token: environmentValue("FEATURE_INGEST_TOKEN", { required: false }),
  timeoutMs: Number(process.env.FEATURE_INGEST_TIMEOUT_MS || 15000),
});
const worker = new Worker(
  process.env.FEATURE_RECALC_QUEUE || FEATURE_RECALC_QUEUE,
  async (job) => {
    try {
      return await processor(job);
    } catch (error) {
      if (error instanceof FeatureIngestPermanentError) {
        throw new UnrecoverableError(error.message);
      }
      throw error;
    }
  },
  {
    connection: redisOptions,
    concurrency: Math.max(1, Number(process.env.FEATURE_RECALC_CONCURRENCY || 4)),
  },
);

worker.on("completed", (job, result) => {
  console.log(JSON.stringify({
    event: "feature_recalc_delivered",
    job_id: job.id,
    event_id: job.data?.event_id,
    apply_status: result?.status ?? null,
    duplicate: result?.duplicate ?? null,
  }));
});
worker.on("failed", (job, error) => {
  console.error(JSON.stringify({
    event: "feature_recalc_delivery_failed",
    job_id: job?.id,
    event_id: job?.data?.event_id,
    error: String(error?.message || error),
  }));
});

let closing = false;
async function shutdown() {
  if (closing) return;
  closing = true;
  await worker.close();
}
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());

await worker.waitUntilReady();
console.log(JSON.stringify({
  event: "feature_recalc_relay_ready",
  queue: worker.name,
  endpoint,
  concurrency: worker.opts.concurrency,
}));
