import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { Queue } from "bullmq";

import {
  DEFAULT_MANUAL_MIGRATION_BATCH_ID,
  dispatchManualMigrationChannel,
} from "../src/manualMigrationDispatch.js";
import { sourceSnapshotHash } from "../src/migrationSource.js";

const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();

function sourceSnapshot() {
  const value = {
    source_id: "qy-migration-v1",
    source_database: "bullmq_crawler_migration",
    source_database_oid: "16384",
    source_candidate_id: "42",
    source_candidate_status: "discovered",
    source_dispatch_batch_id: "legacy-results-full-v1",
    channel_id: "UC1234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
    handle: "@example",
    title: "Example",
    description: null,
    avatar_url: null,
    search_subscriber_count: "1200",
    search_subscriber_count_text: "1.2K",
    is_verified: false,
    priority: 100,
    snapshot_json: {},
    source_json: { source: "legacy_results_db" },
    source_created_at: "2026-08-01T00:00:00.000Z",
    source_updated_at: "2026-08-02T00:00:00.000Z",
  };
  return { ...value, snapshot_sha256: sourceSnapshotHash(value) };
}

test("manual migration validates the Redis Job after a duplicate BullMQ add", {
  skip: redisUrl ? false : "MANAGED_JOB_TEST_REDIS_URL is not configured",
}, async () => {
  const url = new URL(redisUrl);
  const queue = new Queue(`manual-migration-${randomUUID()}`, {
    connection: {
      host: url.hostname,
      port: Number(url.port),
      password: url.password || undefined,
      maxRetriesPerRequest: null,
    },
    prefix: "rota-fix-review",
  });
  const snapshot = sourceSnapshot();
  const jobId = `channel-snapshot__${DEFAULT_MANUAL_MIGRATION_BATCH_ID}__${snapshot.channel_id}__g1`;
  const conflictingPayload = { candidate_id: 999, dispatch_generation: 1 };
  let firstLookup = true;
  const racedQueue = {
    name: queue.name,
    async getJob(candidateJobId) {
      if (firstLookup) {
        firstLookup = false;
        return null;
      }
      return queue.getJob(candidateJobId);
    },
    async add(name, data, options) {
      await queue.add("unrelated-channel-job", conflictingPayload, { jobId: options.jobId });
      return queue.add(name, data, options);
    },
  };

  try {
    await queue.waitUntilReady();
    await assert.rejects(
      dispatchManualMigrationChannel({
        channelId: snapshot.channel_id,
        candidateId: 42,
        queue: racedQueue,
        sourceLoader: async () => snapshot,
        transaction: (action) => action({}),
        targetPreparer: async () => ({
          candidate: {
            candidate_id: 91,
            channel_id: snapshot.channel_id,
            channel_url: snapshot.channel_url,
            priority: 100,
            status: "queued",
            snapshot_dispatch_generation: 1,
          },
          batchId: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
          previousStatus: "failed",
          shouldEnqueue: true,
          intentId: 7,
        }),
        dbQuery: async () => ({ rowCount: 1 }),
      }),
      (error) => error?.code === "job_identity_conflict",
    );
    const persisted = await queue.getJob(jobId);
    assert.equal(persisted?.name, "unrelated-channel-job");
    assert.deepEqual(persisted?.data, conflictingPayload);
  } finally {
    await queue.obliterate({ force: true }).catch(() => {});
    await queue.close();
  }
});
