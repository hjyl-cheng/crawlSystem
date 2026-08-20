export const CHANNEL_QUEUE_PRESSURE_STATES = [
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "paused",
  "waiting-children",
];

export function channelQueuePressure(counts = {}) {
  return CHANNEL_QUEUE_PRESSURE_STATES.reduce(
    (total, state) => total + Math.max(0, Number(counts[state] ?? 0)),
    0,
  );
}

export function channelDispatchCapacity(counts = {}, {
  highWater = 6,
  refill = 2,
  paused = false,
} = {}) {
  if (paused) return 0;
  const available = Math.max(0, Math.floor(Number(highWater)) - channelQueuePressure(counts));
  return Math.min(Math.max(0, Math.floor(Number(refill))), available);
}

export function migrationBatchHasOpenWork(batch = {}) {
  return Number(batch.open_count ?? 0) > 0
    || Number(batch.open_run_count ?? 0) > 0;
}

export function channelSnapshotPayload(candidate, batchId, { minSubscriberCount = 1000 } = {}) {
  return {
    candidate_id: Number(candidate.candidate_id),
    dispatch_batch_id: batchId,
    channel_id: candidate.channel_id,
    channel_url: candidate.channel_url,
    crawl_mode: "full",
    query_id: null,
    query_text: "results.db migration",
    pipeline_cycle_id: batchId,
    enforce_min_subscribers: true,
    min_subscriber_count: Number(minSubscriberCount),
    reject_if_no_recent_content: true,
  };
}
