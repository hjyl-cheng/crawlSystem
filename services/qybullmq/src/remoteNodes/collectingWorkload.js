// Worker identity is separate from Rota's shared `channel` role. Unknown or
// mixed identities must never fall back to an incremental queue/runtime.
const incremental = Object.freeze({
  role: 'incremental', mode: 'incremental_collect',
  queue: 'youtube-channel-incremental', capability: 'youtube.incremental.plan.v1',
  revisions: Object.freeze(['youtubejs-incremental-v1', 'youtubejs-incremental-whole-v1']),
  rotaRole: 'channel',
});

export const FULL_CRAWL_WORKLOAD = Object.freeze({
  role: 'fullcrawl', mode: 'full_crawl_collect',
  queue: 'youtube-channel-crawl', capability: 'youtube.full-crawl.v1',
  revisions: Object.freeze(['youtubejs-full-crawl-v1']),
  rotaRole: 'channel',
});

// Query Discover sends one YouTube search request per page. It leases Rota's
// own `discover` slots, never the channel slots shared by the other workloads.
export const DISCOVER_WORKLOAD = Object.freeze({
  role: 'discover', mode: 'discover_collect',
  queue: 'youtube-discover-page', capability: 'youtube.discover-page.v1',
  revisions: Object.freeze(['youtube-search-discover-v1']),
  rotaRole: 'discover',
});

export function collectingWorkload(mode) {
  if (mode === incremental.mode) return incremental;
  if (mode === FULL_CRAWL_WORKLOAD.mode) return FULL_CRAWL_WORKLOAD;
  if (mode === DISCOVER_WORKLOAD.mode) return DISCOVER_WORKLOAD;
  return null;
}

// Workloads other than incremental own a dedicated capability. Such a node
// declares only that capability; incremental nodes may not declare any of them.
export const DEDICATED_WORKLOADS = Object.freeze([FULL_CRAWL_WORKLOAD, DISCOVER_WORKLOAD]);

export function dedicatedWorkloadForCapability(capability) {
  return DEDICATED_WORKLOADS.find(workload => workload.capability === capability) ?? null;
}

export function collectingSlotValid(workload, slot) {
  if (typeof slot !== 'string' || !/^[a-z0-9-]{1,60}$/.test(slot)) return false;
  // Keep legacy incremental slots compatible. Other workloads own a namespace.
  if (workload?.role === 'incremental') return !slot.startsWith('full-crawl-') && !slot.startsWith('discover-');
  if (workload?.role === 'fullcrawl') return /^full-crawl-[1-9][0-9]*$/.test(slot);
  return workload?.role === 'discover' && /^discover-[1-9][0-9]*$/.test(slot);
}
