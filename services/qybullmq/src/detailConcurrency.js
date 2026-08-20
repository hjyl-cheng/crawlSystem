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
}) {
  const rows = Array.from(items ?? []);
  const windowSize = normalizeDetailConcurrency(concurrency);
  const results = [];
  let processed = 0;

  for (let offset = 0; offset < rows.length; offset += windowSize) {
    const window = rows.slice(offset, offset + windowSize);
    const prefetched = window.map((item, index) => (
      shouldPrefetch(item, offset + index)
        ? Promise.resolve().then(() => prefetch(item, offset + index))
        : Promise.resolve(undefined)
    ));

    try {
      for (let index = 0; index < window.length; index += 1) {
        const result = await process(window[index], prefetched[index], offset + index);
        processed += 1;
        results.push(result);
        const stopReason = stopAfter(result, offset + index);
        if (!stopReason) continue;

        await Promise.allSettled(prefetched);
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
      throw error;
    }
  }

  return {
    results,
    processed,
    stopReason: null,
    stopIndex: null,
    remaining: [],
  };
}
