import { hostname } from "node:os";
import { closeDb, query, withTransaction } from "./db.js";
import {
  PostgresPublicationOutboxStore,
  PublicationPublisher,
} from "./publicationPublisher.js";
import { HttpPublicationIngressAdapter } from "./publicationTransport.js";
import { environmentValue } from "./runtimeEnvironment.js";

const pollMs = Math.max(100, Number(process.env.PUBLICATION_PUBLISH_POLL_MS || 1000));
let stopping = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function assertPublicationDatabase() {
  const expected = String(process.env.EXPECTED_CRAWLER_DATABASE || "bullmq_crawler_migration").trim();
  const result = await query(
    `SELECT current_database() AS database_name,
            to_regclass('publication.revision') IS NOT NULL AS revision_ready,
            to_regclass('publication.outbox') IS NOT NULL AS outbox_ready`,
  );
  const state = result.rows[0] ?? {};
  if (state.database_name !== expected || state.revision_ready !== true || state.outbox_ready !== true) {
    throw new Error(`refusing to publish from unexpected or unmigrated database: ${state.database_name}`);
  }
}

async function main() {
  await assertPublicationDatabase();
  const destination = String(process.env.PUBLICATION_DESTINATION || "business").trim();
  const ingress = new HttpPublicationIngressAdapter({
    endpoint: environmentValue("BUSINESS_PUBLICATION_INGRESS_URL"),
    token: environmentValue("BUSINESS_PUBLICATION_INGRESS_TOKEN", { required: false }),
    trustedInternalHttpHostname: environmentValue(
      "BUSINESS_PUBLICATION_INGRESS_TRUSTED_HTTP_HOSTNAME",
      { required: false },
    ),
    timeoutMs: process.env.PUBLICATION_INGRESS_TIMEOUT_MS,
  });
  const publisher = new PublicationPublisher({
    store: new PostgresPublicationOutboxStore({ query, withTransaction }),
    ingress,
    destination,
    leaseOwner: process.env.PUBLICATION_PUBLISHER_ID
      || `publication:${destination}:${hostname()}:${process.pid}`,
    batchSize: process.env.PUBLICATION_PUBLISH_BATCH_SIZE,
    leaseSeconds: process.env.PUBLICATION_PUBLISH_LEASE_SECONDS,
    maxAttempts: process.env.PUBLICATION_PUBLISH_MAX_ATTEMPTS,
    maximumShardBytes: process.env.PUBLICATION_SHARD_MAX_BYTES,
  });
  const stop = () => { stopping = true; };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    while (!stopping) {
      const summary = await publisher.runOnce();
      if (summary.claimed > 0) {
        console.log(JSON.stringify({ event: "publication_publisher_batch", destination, ...summary }));
      }
      if (summary.claimed === 0) await sleep(pollMs);
    }
  } finally {
    await closeDb();
  }
}

main().catch((error) => {
  console.error(JSON.stringify({
    event: "publication_publisher_fatal",
    error: error?.stack || String(error),
  }));
  process.exitCode = 1;
});
