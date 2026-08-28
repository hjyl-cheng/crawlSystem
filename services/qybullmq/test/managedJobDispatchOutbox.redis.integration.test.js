import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { Queue } from "bullmq";

import {
  InMemoryManagedJobDispatchRepository,
  ManagedJobOutboxDispatcher,
} from "../src/managedJobDispatchOutbox.js";

const redisUrl = String(process.env.MANAGED_JOB_TEST_REDIS_URL ?? "").trim();

test("Outbox validates the Redis-persisted Job after a duplicate BullMQ add", {
  skip: redisUrl ? false : "MANAGED_JOB_TEST_REDIS_URL is not configured",
}, async () => {
  const url = new URL(redisUrl);
  const queue = new Queue(`managed-outbox-${randomUUID()}`, {
    connection: {
      host: url.hostname,
      port: Number(url.port),
      password: url.password || undefined,
      maxRetriesPerRequest: null,
    },
    prefix: "rota-fix-review",
  });
  const jobId = "discover-page__page-1";
  const persistedPayload = {
    page_id: "persisted-page", intent_schema_version: 1, dispatch_generation: 1,
  };
  const requestedPayload = {
    page_id: "requested-page", intent_schema_version: 1, dispatch_generation: 1,
  };
  const repository = new InMemoryManagedJobDispatchRepository({
    rows: [{
      dispatch_id: "dispatch-1",
      aggregate_kind: "discover_page",
      aggregate_id: "requested-page",
      intent_hash: "sha256:requested",
      queue_registry_key: "youtube-discover-page",
      deterministic_job_id: jobId,
      payload_json: requestedPayload,
      status: "pending",
      attempts: 0,
    }],
  });

  try {
    await queue.waitUntilReady();
    await queue.add("discover-page", persistedPayload, { jobId });
    const result = await new ManagedJobOutboxDispatcher({
      repository,
      queues: { "youtube-discover-page": queue },
      maxAttempts: 1,
    }).dispatchAvailable({ limit: 1 });

    assert.deepEqual(result, { claimed: 1, sent: 0, failed: 0, dead: 1 });
    assert.deepEqual((await queue.getJob(jobId))?.data, persistedPayload);
    assert.match(repository.rows.get("dispatch-1").last_error, /conflicts/);
  } finally {
    await queue.obliterate({ force: true }).catch(() => {});
    await queue.close();
  }
});
