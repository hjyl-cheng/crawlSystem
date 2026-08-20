import { hostname } from "node:os";
import { Queue } from "bullmq";
import { closeDb, query, withTransaction } from "./db.js";
import { assertSharedFeatureDatabase } from "./databaseTopology.js";
import { DispatchOutboxPublisher, PostgresDispatchOutboxStore } from "./dispatchOutboxPublisher.js";
import {
  AGENT_INCREMENTAL_QUEUE,
  BullMqCapacityProbe,
  CHANNEL_CRAWL_QUEUE,
  DynamicDispatcher,
  PostgresDynamicDispatchStore,
} from "./dynamicDispatcher.js";
import { INCREMENTAL_QUEUE } from "./dispatchTransport.js";

const pollMs = Math.max(100, Number(process.env.DISPATCH_PUBLISH_POLL_MS || 1000));
const queueName = String(process.env.INCREMENTAL_QUEUE_NAME || INCREMENTAL_QUEUE).trim();
let stopping = false;

const redisOptions = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function assertFeatureDatabase() {
  const expected = String(process.env.EXPECTED_FEATURE_DATABASE || "").trim();
  const expectedUser = String(process.env.EXPECTED_FEATURE_DATABASE_USER || "").trim();
  const result = await query(
    `SELECT current_database() AS database_name,
            current_user AS database_user,
            to_regclass('feature_clock.dispatch_outbox') IS NOT NULL AS outbox_ready,
            to_regnamespace('crawler') IS NOT NULL AS crawler_ready,
            to_regclass('crawler.channels') IS NOT NULL AS channels_ready,
            CASE WHEN to_regclass('crawler.channels') IS NULL THEN NULL
                 ELSE has_table_privilege(current_user,'crawler.channels','SELECT')
            END AS crawler_channels_readable,
            current_setting('TimeZone') = 'UTC' AS timezone_utc`,
  );
  assertSharedFeatureDatabase(result.rows[0], {
    expectedDatabase: expected,
    expectedUser,
  });
}

async function main() {
  if (queueName !== INCREMENTAL_QUEUE) {
    throw new Error(`INCREMENTAL_QUEUE_NAME must be ${INCREMENTAL_QUEUE}; existing Full queues are forbidden`);
  }
  await assertFeatureDatabase();
  const queue = new Queue(queueName, { connection: redisOptions });
  const channelCrawlQueue = new Queue(CHANNEL_CRAWL_QUEUE, { connection: redisOptions });
  const agentIncrementalQueue = new Queue(AGENT_INCREMENTAL_QUEUE, { connection: redisOptions });
  const publisher = new DispatchOutboxPublisher({
    store: new PostgresDispatchOutboxStore({ query, withTransaction }),
    queue,
    leaseOwner: process.env.DISPATCH_PUBLISHER_ID || `feature-dispatch:${hostname()}:${process.pid}`,
    releaseBatchSize: process.env.DISPATCH_RELEASE_BATCH_SIZE
      || process.env.DISPATCH_PUBLISH_BATCH_SIZE,
    leaseSeconds: process.env.DISPATCH_PUBLISH_LEASE_SECONDS,
    maxAttempts: process.env.DISPATCH_PUBLISH_MAX_ATTEMPTS,
  });
  const dispatcher = new DynamicDispatcher({
    probe: new BullMqCapacityProbe({
      incrementalQueue: queue,
      channelCrawlQueue,
      agentIncrementalQueue,
      proxyCapacityUrl: process.env.DISPATCH_PROXY_CAPACITY_URL
        || "http://youtube-rota-qy-core:8001/api/v1/proxy-control",
      proxyCapacityToken: process.env.DISPATCH_PROXY_CAPACITY_TOKEN,
      fetchTimeoutMs: process.env.DISPATCH_CAPACITY_TIMEOUT_MS,
    }),
    store: new PostgresDynamicDispatchStore({ withTransaction }),
    publisher,
    budgetOptions: {
      releaseBatchSize: process.env.DISPATCH_RELEASE_BATCH_SIZE,
      bufferPerWorker: process.env.DISPATCH_BUFFER_PER_WORKER,
      maximumQueueBuffer: process.env.DISPATCH_MAX_QUEUE_BUFFER,
      minimumIncrementalShare: process.env.DISPATCH_INCREMENTAL_MIN_SHARE,
      agentShare: process.env.DISPATCH_AGENT_SHARE,
      agentBatchSize: process.env.DISPATCH_AGENT_BATCH_SIZE,
      agentBufferBatches: process.env.DISPATCH_AGENT_BUFFER_BATCHES,
    },
    executionTimeoutMinutes: process.env.DISPATCH_EXECUTION_TIMEOUT_MINUTES,
  });
  const stop = () => { stopping = true; };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    while (!stopping) {
      const summary = await dispatcher.runOnce();
      if (summary.staged.staged > 0 || summary.published.claimed > 0) {
        console.log(JSON.stringify({ event: "dynamic_dispatch_batch", ...summary }));
      }
      if (summary.published.claimed === 0) await sleep(pollMs);
    }
  } finally {
    await Promise.all([queue.close(), channelCrawlQueue.close(), agentIncrementalQueue.close()]);
    await closeDb();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ event: "dispatch_outbox_publisher_fatal", error: error?.stack || String(error) }));
  process.exitCode = 1;
});
