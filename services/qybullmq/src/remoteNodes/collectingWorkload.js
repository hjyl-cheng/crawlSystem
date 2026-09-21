// Worker identity is separate from Rota's shared `channel` role. Unknown or
// mixed identities must never fall back to an incremental queue/runtime.
const incremental = Object.freeze({
  role: 'incremental', mode: 'incremental_collect',
  queue: 'youtube-channel-incremental', capability: 'youtube.incremental.plan.v1',
  revisions: Object.freeze(['youtubejs-incremental-v1', 'youtubejs-incremental-whole-v1']),
});

export const FULL_CRAWL_WORKLOAD = Object.freeze({
  role: 'fullcrawl', mode: 'full_crawl_collect',
  queue: 'youtube-channel-crawl', capability: 'youtube.full-crawl.v1',
  revisions: Object.freeze(['youtubejs-full-crawl-v1']),
});

export function collectingWorkload(mode) {
  if (mode === incremental.mode) return incremental;
  if (mode === FULL_CRAWL_WORKLOAD.mode) return FULL_CRAWL_WORKLOAD;
  return null;
}

export function collectingSlotValid(workload, slot) {
  if (typeof slot !== 'string' || !/^[a-z0-9-]{1,60}$/.test(slot)) return false;
  // Keep legacy incremental slots compatible. Full Crawl has its own namespace.
  return workload?.role === 'incremental' ? !slot.startsWith('full-crawl-')
    : workload?.role === 'fullcrawl' && /^full-crawl-[1-9][0-9]*$/.test(slot);
}
