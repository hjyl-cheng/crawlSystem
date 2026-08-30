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

export function migrationBatchCompletion({
  total = 0,
  accepted = 0,
  rejected = 0,
  failed = 0,
  systemFailures = 0,
} = {}) {
  const counts = Object.fromEntries(Object.entries({
    total,
    accepted,
    rejected,
    failed,
    systemFailures,
  }).map(([field, value]) => {
    const normalized = Number(value);
    if (!Number.isSafeInteger(normalized) || normalized < 0) {
      throw new TypeError(`${field} must be a non-negative integer`);
    }
    return [field, normalized];
  }));
  if (counts.accepted + counts.rejected + counts.failed > counts.total) {
    throw new TypeError("terminal migration counts cannot exceed total");
  }
  if (counts.systemFailures > counts.total) {
    throw new TypeError("systemFailures cannot exceed total");
  }
  return Object.freeze({
    status: "completed",
    outcome: counts.systemFailures > 0
      ? "completed_with_system_failures"
      : "completed",
    total: counts.total,
    accepted: counts.accepted,
    rejected: counts.rejected,
    failed: counts.failed,
  });
}

export function channelSnapshotPayload(candidate, batchId, { minSubscriberCount = 1000 } = {}) {
  const dispatchGeneration = Number(candidate?.snapshot_dispatch_generation);
  if (!Number.isSafeInteger(dispatchGeneration) || dispatchGeneration <= 0) {
    throw new TypeError("candidate.snapshot_dispatch_generation must be a positive integer");
  }
  const migrationIntentId = candidate?.migration_intent_id == null
    ? null
    : Number(candidate.migration_intent_id);
  if (migrationIntentId != null
      && (!Number.isSafeInteger(migrationIntentId) || migrationIntentId <= 0)) {
    throw new TypeError("candidate.migration_intent_id must be a positive integer");
  }
  return {
    candidate_id: Number(candidate.candidate_id),
    ...(migrationIntentId == null ? {} : { migration_intent_id: migrationIntentId }),
    dispatch_generation: dispatchGeneration,
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
