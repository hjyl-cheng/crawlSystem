import { hostname } from "node:os";
import { Queue } from "bullmq";
import { closeDb, query, withTransaction } from "./db.js";
import { CrawlerOutboxPublisher, PostgresCrawlerOutboxStore } from "./crawlerOutboxPublisher.js";
import { FEATURE_RECALC_QUEUE } from "./featureTransport.js";
import { redisOptions } from "./queues.js";

const pollMs = Math.max(100, Number(process.env.OUTBOX_PUBLISH_POLL_MS || 1000));
let stopping = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function assertQyCrawlerDatabase() {
  const expected = String(process.env.EXPECTED_CRAWLER_DATABASE || "bullmq_crawler_migration").trim();
  const result = await query(
    `SELECT current_database() AS database_name,
            to_regclass('crawler.crawler_outbox') IS NOT NULL AS outbox_ready`,
  );
  if (result.rows[0]?.database_name !== expected || result.rows[0]?.outbox_ready !== true) {
    throw new Error(`refusing to publish from unexpected or unmigrated database: ${result.rows[0]?.database_name}`);
  }
}

async function main() {
  await assertQyCrawlerDatabase();
  const queue = new Queue(process.env.FEATURE_RECALC_QUEUE || FEATURE_RECALC_QUEUE, {
    connection: redisOptions,
  });
  const publisher = new CrawlerOutboxPublisher({
    store: new PostgresCrawlerOutboxStore({ query, withTransaction }),
    queue,
    leaseOwner: process.env.OUTBOX_PUBLISHER_ID || `qy-outbox:${hostname()}:${process.pid}`,
    batchSize: process.env.OUTBOX_PUBLISH_BATCH_SIZE,
    leaseSeconds: process.env.OUTBOX_PUBLISH_LEASE_SECONDS,
    maxAttempts: process.env.OUTBOX_PUBLISH_MAX_ATTEMPTS,
  });
  const stop = () => { stopping = true; };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    while (!stopping) {
      const summary = await publisher.runOnce();
      if (summary.claimed > 0) console.log(JSON.stringify({ event: "crawler_outbox_batch", ...summary }));
      if (summary.claimed === 0) await sleep(pollMs);
    }
  } finally {
    await queue.close();
    await closeDb();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ event: "crawler_outbox_publisher_fatal", error: error?.stack || String(error) }));
  process.exitCode = 1;
});
