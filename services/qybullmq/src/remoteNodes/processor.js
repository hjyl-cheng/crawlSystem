import { setTimeout as delay } from 'node:timers/promises';

// Separate from the HTTP receiver: accepting a result never waits for its business writes.
// Abort stops intake; any transaction already running is allowed to finish.
export async function runRemoteResultProcessor({ store, handlers, signal, concurrency = 1,
  pollMs = 1000, onStatus = () => {} }) {
  if (!signal || !Object.keys(handlers).length || !Number.isInteger(concurrency) || concurrency < 1 || concurrency > 4) {
    throw new TypeError('handlers, stop signal and bounded concurrency required');
  }
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (!signal.aborted) {
      let result;
      try {
        result = await store.processOne(handlers);
        if (result) onStatus(result);
      } catch {
        onStatus({ status: 'unavailable' });
      }
      if (!result && !signal.aborted) {
        await delay(pollMs, null, { signal }).catch((error) => {
          if (error.name !== 'AbortError') throw error;
        });
      }
    }
  }));
}
