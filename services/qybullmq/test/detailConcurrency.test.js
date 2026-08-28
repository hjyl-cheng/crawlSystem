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

test("processWithOrderedPrefetch rejects an already-cancelled empty run", async () => {
  const controller = new AbortController();
  const reason = new Error("content detail cancelled");
  controller.abort(reason);

  await assert.rejects(
    processWithOrderedPrefetch({
      items: [],
      signal: controller.signal,
      prefetch: async () => assert.fail("empty run must not prefetch"),
      process: async () => assert.fail("empty run must not process"),
    }),
    (error) => error === reason,
  );
});

test("processWithOrderedPrefetch discards a result when cancellation arrives during processing", async () => {
  const controller = new AbortController();
  const reason = new Error("lease lost while processing");
  let releaseProcess;
  let releaseSibling;
  let notifyProcessStarted;
  let siblingFinished = false;
  let stopAfterCalls = 0;
  const processStarted = new Promise((resolve) => { notifyProcessStarted = resolve; });
  const processGate = new Promise((resolve) => { releaseProcess = resolve; });
  const siblingGate = new Promise((resolve) => { releaseSibling = resolve; });

  const execution = processWithOrderedPrefetch({
    items: [1, 2],
    concurrency: 2,
    signal: controller.signal,
    prefetch: async (item) => {
      if (item === 2) {
        await siblingGate;
        siblingFinished = true;
      }
      return item;
    },
    process: async (item, detailPromise) => {
      await detailPromise;
      if (item === 1) {
        notifyProcessStarted();
        await processGate;
      }
      return { item };
    },
    stopAfter: () => {
      stopAfterCalls += 1;
      return null;
    },
  });

  await processStarted;
  controller.abort(reason);
  releaseProcess();
  releaseSibling();

  await assert.rejects(execution, (error) => error === reason);
  assert.equal(siblingFinished, true);
  assert.equal(stopAfterCalls, 0);
});

test("processWithOrderedPrefetch gives cancellation priority during stop-path draining", async () => {
  const controller = new AbortController();
  const reason = new Error("cancelled while draining a stopped window");
  let releaseSibling;
  let notifyStop;
  const siblingGate = new Promise((resolve) => { releaseSibling = resolve; });
  const stopReached = new Promise((resolve) => { notifyStop = resolve; });

  const execution = processWithOrderedPrefetch({
    items: [1, 2],
    concurrency: 2,
    signal: controller.signal,
    prefetch: async (item) => {
      if (item === 2) await siblingGate;
      return item;
    },
    process: async (item, detailPromise) => ({ item, detail: await detailPromise }),
    stopAfter: ({ item }) => {
      if (item !== 1) return null;
      notifyStop();
      return "retryable";
    },
  });

  await stopReached;
  controller.abort(reason);
  releaseSibling();

  await assert.rejects(execution, (error) => error === reason);
});

test("processWithOrderedPrefetch gives cancellation priority during error-path draining", async () => {
  const controller = new AbortController();
  const ordinaryError = new Error("candidate persistence failed");
  const reason = new Error("cancelled while draining a failed window");
  let releaseSibling;
  let notifyFailure;
  const siblingGate = new Promise((resolve) => { releaseSibling = resolve; });
  const failureReached = new Promise((resolve) => { notifyFailure = resolve; });

  const execution = processWithOrderedPrefetch({
    items: [1, 2],
    concurrency: 2,
    signal: controller.signal,
    prefetch: async (item) => {
      if (item === 2) await siblingGate;
      return item;
    },
    process: async () => {
      notifyFailure();
      throw ordinaryError;
    },
  });

  await failureReached;
  controller.abort(reason);
  releaseSibling();

  await assert.rejects(execution, (error) => error === reason);
});

test("processWithOrderedPrefetch handles a later sibling rejection before it is consumed", async () => {
  const primaryError = new Error("first candidate failed");
  const siblingError = new Error("later prefetch failed early");
  const unhandled = [];
  const onUnhandled = (error) => unhandled.push(error);
  process.on("unhandledRejection", onUnhandled);

  try {
    await assert.rejects(
      processWithOrderedPrefetch({
        items: [1, 2],
        concurrency: 2,
        prefetch: async (item) => {
          if (item === 1) return item;
          await new Promise((resolve) => setImmediate(resolve));
          throw siblingError;
        },
        process: async (item, detailPromise) => {
          await detailPromise;
          if (item === 1) {
            await new Promise((resolve) => setImmediate(resolve));
            await new Promise((resolve) => setImmediate(resolve));
            throw primaryError;
          }
          return { item };
        },
      }),
      (error) => error === primaryError,
    );
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(unhandled, []);
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
});

test("processWithOrderedPrefetch checks cancellation before processing the next item", async () => {
  const controller = new AbortController();
  const reason = new Error("cancelled after the first ordered result");
  const processed = [];

  await assert.rejects(
    processWithOrderedPrefetch({
      items: [1, 2],
      concurrency: 2,
      signal: controller.signal,
      prefetch: async (item) => item,
      process: async (item, detailPromise) => {
        await detailPromise;
        processed.push(item);
        return { item };
      },
      stopAfter: ({ item }) => {
        if (item === 1) controller.abort(reason);
        return null;
      },
    }),
    (error) => error === reason,
  );

  assert.deepEqual(processed, [1]);
});

test("processWithOrderedPrefetch checks cancellation before opening the next window", async () => {
  const controller = new AbortController();
  const reason = new Error("cancelled between prefetch windows");
  const prefetched = [];

  await assert.rejects(
    processWithOrderedPrefetch({
      items: [1, 2],
      concurrency: 1,
      signal: controller.signal,
      prefetch: async (item) => {
        prefetched.push(item);
        return item;
      },
      process: async (item, detailPromise) => ({ item, detail: await detailPromise }),
      stopAfter: ({ item }) => {
        if (item === 1) controller.abort(reason);
        return null;
      },
    }),
    (error) => error === reason,
  );

  assert.deepEqual(prefetched, [1]);
});

test("processWithOrderedPrefetch checks cancellation before its final return", async () => {
  const controller = new AbortController();
  const reason = new Error("cancelled by the final stop check");

  await assert.rejects(
    processWithOrderedPrefetch({
      items: [1],
      signal: controller.signal,
      prefetch: async (item) => item,
      process: async (item, detailPromise) => ({ item, detail: await detailPromise }),
      stopAfter: () => {
        controller.abort(reason);
        return null;
      },
    }),
    (error) => error === reason,
  );
});
