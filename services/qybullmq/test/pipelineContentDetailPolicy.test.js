import assert from "node:assert/strict";
import test from "node:test";
import { contentDetailBatchStopReason } from "../src/contentDetailBatchPolicy.js";
import { processWithOrderedPrefetch } from "../src/detailConcurrency.js";

test("an outside candidate does not stop classification of later candidates", () => {
  assert.equal(contentDetailBatchStopReason({ cutoff: true, retryable: false }), null);
  assert.equal(contentDetailBatchStopReason({ cutoff: false, retryable: true }), "retryable");
});

test("ordered detail processing continues after one outside candidate", async () => {
  const handled = [];
  const execution = await processWithOrderedPrefetch({
    items: ["outside", "unresolved"],
    concurrency: 1,
    shouldPrefetch: () => false,
    process: async (item) => {
      handled.push(item);
      return item === "outside" ? { cutoff: true } : { cutoff: false };
    },
    stopAfter: contentDetailBatchStopReason,
  });

  assert.deepEqual(handled, ["outside", "unresolved"]);
  assert.equal(execution.processed, 2);
  assert.equal(execution.remaining.length, 0);
});
