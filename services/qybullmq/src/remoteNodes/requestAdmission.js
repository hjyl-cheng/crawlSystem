import { RemoteProtocolError } from './protocol.js';

// A short, bounded HTTP admission queue. Durable work stays in SQL/BullMQ;
// waiting here never acknowledges a result and never extends an execution lease.
export function createRequestAdmission({ concurrency, maxPending = 256, timeoutMs = 5000 }) {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1
      || !Number.isSafeInteger(maxPending) || maxPending < 0
      || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1) throw new TypeError('invalid request admission limits');
  let active = 0; const pending = new Set();
  const stats = { admitted: 0, rejected: 0, timedOut: 0, cancelled: 0, peakActive: 0, peakPending: 0 };
  const lease = () => {
    active++; stats.admitted++; stats.peakActive = Math.max(stats.peakActive, active);
    let released = false;
    return () => {
      if (released) return; released = true; active--;
      const next = pending.values().next().value;
      if (next) { pending.delete(next); next.cleanup(); next.resolve(lease()); }
    };
  };
  return {
    snapshot: () => ({ ...stats, active, pending: pending.size, concurrency, maxPending }),
    acquire(signal) {
      if (signal?.aborted) return Promise.reject(new RemoteProtocolError('REQUEST_ABORTED', 400));
      if (active < concurrency) return Promise.resolve(lease());
      if (pending.size >= maxPending) { stats.rejected++; return Promise.reject(new RemoteProtocolError('GATEWAY_BUSY', 503)); }
      return new Promise((resolve, reject) => {
        const remove = (code, counter, status) => {
          if (!pending.delete(ticket)) return;
          ticket.cleanup(); stats[counter]++; reject(new RemoteProtocolError(code, status));
        };
        const abort = () => remove('REQUEST_ABORTED', 'cancelled', 400);
        const timer = setTimeout(() => remove('GATEWAY_BUSY', 'timedOut', 503), timeoutMs);
        const ticket = { resolve, cleanup: () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); } };
        pending.add(ticket); stats.peakPending = Math.max(stats.peakPending, pending.size);
        signal?.addEventListener('abort', abort, { once: true });
      });
    },
  };
}
