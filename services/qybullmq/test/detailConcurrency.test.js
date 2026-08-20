import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeDetailConcurrency,
  processWithOrderedPrefetch,
} from "../src/detailConcurrency.js";

const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

test("normalizeDetailConcurrency defaults to two and clamps to the supported range", () => {
  assert.equal(normalizeDetailConcurrency(undefined), 2);
  assert.equal(normalizeDetailConcurrency(0), 1);
  assert.equal(normalizeDetailConcurrency(3.9), 3);
  assert.equal(normalizeDetailConcurrency(20), 4);
});

test("processWithOrderedPrefetch prefetches concurrently but processes in source order", async () => {
  let active = 0;
  let maximumActive = 0;
  const prefetched = [];
  const processed = [];
  const result = await processWithOrderedPrefetch({
    items: [1, 2, 3, 4, 5],
    concurrency: 2,
    prefetch: async (item) => {
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      prefetched.push(item);
      await sleep(5);
      active -= 1;
      return `detail-${item}`;
    },
    process: async (item, detailPromise) => {
      processed.push(item);
      return { item, detail: await detailPromise };
    },
  });

  assert.equal(maximumActive, 2);
  assert.deepEqual(prefetched, [1, 2, 3, 4, 5]);
  assert.deepEqual(processed, [1, 2, 3, 4, 5]);
  assert.deepEqual(result.results.map((item) => item.detail), [
    "detail-1", "detail-2", "detail-3", "detail-4", "detail-5",
  ]);
  assert.equal(result.processed, 5);
  assert.equal(result.stopReason, null);
});

test("processWithOrderedPrefetch stops opening windows after a retryable result", async () => {
  const prefetched = [];
  const processed = [];
  const result = await processWithOrderedPrefetch({
    items: [1, 2, 3, 4],
    concurrency: 2,
    prefetch: async (item) => {
      prefetched.push(item);
      await sleep(2);
      return item;
    },
    process: async (item, detailPromise) => {
      await detailPromise;
      processed.push(item);
      return { item, retryable: item === 1 };
    },
    stopAfter: (item) => item.retryable ? "retryable" : null,
  });

  assert.deepEqual(prefetched, [1, 2]);
  assert.deepEqual(processed, [1]);
  assert.equal(result.stopReason, "retryable");
  assert.deepEqual(result.remaining, [2, 3, 4]);
});

test("processWithOrderedPrefetch returns all rows after an ordered cutoff", async () => {
  const result = await processWithOrderedPrefetch({
    items: [1, 2, 3, 4],
    concurrency: 2,
    prefetch: async (item) => item,
    process: async (item, detailPromise) => {
      await detailPromise;
      return { item, cutoff: item === 2 };
    },
    stopAfter: (item) => item.cutoff ? "cutoff" : null,
  });

  assert.deepEqual(result.results.map((item) => item.item), [1, 2]);
  assert.equal(result.stopReason, "cutoff");
  assert.equal(result.stopIndex, 1);
  assert.deepEqual(result.remaining, [3, 4]);
});

test("processWithOrderedPrefetch drains the current prefetch window after processing throws", async () => {
  let secondPrefetchFinished = false;
  await assert.rejects(
    processWithOrderedPrefetch({
      items: [1, 2, 3],
      concurrency: 2,
      prefetch: async (item) => {
        await sleep(item === 2 ? 10 : 1);
        if (item === 2) secondPrefetchFinished = true;
        return item;
      },
      process: async () => {
        throw new Error("database write failed");
      },
    }),
    /database write failed/,
  );
  assert.equal(secondPrefetchFinished, true);
});
