import assert from "node:assert/strict";
import test from "node:test";
import { dispatchManagedQueryQualityBatch } from "../src/managedJobApi.js";

test("typed Query Quality dispatch stages immutable Chunks before flushing Outbox", async () => {
  const calls = [];
  const response = await dispatchManagedQueryQualityBatch({
    qualityBatchId: "batch-1",
    intentStore: {
      prepareQueryQualityBatch: async (qualityBatchId) => {
        calls.push(`prepare:${qualityBatchId}`);
        return { chunks: [{ quality_chunk_id: "chunk-1" }, { quality_chunk_id: "chunk-2" }] };
      },
    },
    outboxDispatcher: {
      dispatchAvailable: async ({ limit }) => {
        calls.push(`dispatch:${limit}`);
        return { claimed: 2, sent: 2, failed: 0, dead: 0 };
      },
    },
  });

  assert.deepEqual(calls, ["prepare:batch-1", "dispatch:500"]);
  assert.equal(response.status, 201);
  assert.deepEqual(response.body.quality_chunk_ids, ["chunk-1", "chunk-2"]);
});

test("typed Query Quality dispatch is an idempotent Outbox flush when no Tasks remain", async () => {
  const response = await dispatchManagedQueryQualityBatch({
    qualityBatchId: "batch-1",
    intentStore: { prepareQueryQualityBatch: async () => ({ chunks: [], terminal: false }) },
    outboxDispatcher: {
      dispatchAvailable: async () => ({ claimed: 1, sent: 1, failed: 0, dead: 0 }),
    },
  });

  assert.equal(response.status, 200);
  assert.equal(response.body.created_chunk_count, 0);
  assert.equal(response.body.dispatch.sent, 1);
});
