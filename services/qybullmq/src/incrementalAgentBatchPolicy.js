function positiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

export function decideIncrementalAgentBatch({
  pendingChannelCount,
  unregisteredPlanCount = null,
  oldestPendingAt = null,
  newestPendingAt = null,
  now = new Date(),
  batchSize = 30,
  tailQuietMs = 15 * 60 * 1000,
} = {}) {
  const pending = Math.max(0, Number.parseInt(String(pendingChannelCount ?? 0), 10) || 0);
  const size = positiveInteger(batchSize, 30);
  if (pending === 0) {
    return { dispatch: false, limit: 0, partial: false, reason: "empty" };
  }
  if (pending >= size) {
    return { dispatch: true, limit: size, partial: false, reason: "full_batch" };
  }

  const parsedUnregistered = Number.parseInt(String(unregisteredPlanCount ?? ""), 10);
  if (Number.isSafeInteger(parsedUnregistered) && parsedUnregistered >= 0) {
    if (parsedUnregistered === 0) {
      return { dispatch: true, limit: pending, partial: true, reason: "planned_tail_ready" };
    }
    return { dispatch: false, limit: 0, partial: false, reason: "awaiting_planned_requests" };
  }

  const newest = newestPendingAt ?? oldestPendingAt;
  const newestTime = newest == null ? Number.NaN : new Date(newest).getTime();
  const nowTime = new Date(now).getTime();
  const quietWindow = Math.max(0, Number(tailQuietMs) || 0);
  if (!Number.isFinite(newestTime) || !Number.isFinite(nowTime) || nowTime - newestTime < quietWindow) {
    return { dispatch: false, limit: 0, partial: false, reason: "collecting_tail" };
  }
  return { dispatch: true, limit: pending, partial: true, reason: "tail_quiet_window_elapsed" };
}
