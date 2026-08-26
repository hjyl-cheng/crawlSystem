import { throwIfAborted } from "./abortSignal.js";

export function normalizeDetailConcurrency(value, fallback = 2) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return normalizeDetailConcurrency(fallback, 2);
  return Math.max(1, Math.min(4, Math.floor(parsed)));
}

export async function processWithOrderedPrefetch({
  items,
  concurrency = 2,
  shouldPrefetch = () => true,
  prefetch,
  process,
  stopAfter = () => null,
  signal = null,
}) {
  throwIfAborted(signal);
  const rows = Array.from(items ?? []);
  const windowSize = normalizeDetailConcurrency(concurrency);
  const results = [];
  let processed = 0;

  for (let offset = 0; offset < rows.length; offset += windowSize) {
    throwIfAborted(signal);
    const window = rows.slice(offset, offset + windowSize);
    const prefetched = window.map((item, index) => {
      const promise = shouldPrefetch(item, offset + index)
        ? Promise.resolve().then(() => prefetch(item, offset + index))
        : Promise.resolve(undefined);
      void promise.catch(() => {});
      return promise;
    });

    try {
      for (let index = 0; index < window.length; index += 1) {
        throwIfAborted(signal);
        const result = await process(window[index], prefetched[index], offset + index);
        throwIfAborted(signal);
        processed += 1;
        results.push(result);
        const stopReason = stopAfter(result, offset + index);
        if (!stopReason) continue;

        await Promise.allSettled(prefetched);
        throwIfAborted(signal);
        const stopIndex = offset + index;
        return {
          results,
          processed,
          stopReason,
          stopIndex,
          remaining: rows.slice(stopIndex + 1),
        };
      }
    } catch (error) {
      await Promise.allSettled(prefetched);
      throwIfAborted(signal);
      throw error;
    }
  }

  throwIfAborted(signal);
  return {
    results,
    processed,
    stopReason: null,
    stopIndex: null,
    remaining: [],
  };
}
