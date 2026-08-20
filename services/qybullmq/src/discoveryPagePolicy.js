function count(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? Math.floor(parsed) : 0;
}

function ratio(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.max(0, Math.min(1, parsed)) : fallback;
}

export function resolveDiscoveryPageQualification({
  acceptedCount,
  existingCount,
  rejectedCount,
  failedCount,
  pendingCount,
  hasContinuation,
  minQualifiedRatio,
}) {
  const accepted = count(acceptedCount);
  const existing = count(existingCount);
  const rejected = count(rejectedCount);
  const failed = count(failedCount);
  const pending = count(pendingCount);
  const qualified = accepted + existing;
  const total = qualified + rejected + failed + pending;
  const threshold = ratio(minQualifiedRatio, 1 / 3);

  if (pending > 0) {
    return {
      settled: false,
      failed: false,
      total,
      qualified,
      rejected,
      pending,
      qualifiedRatio: null,
      unqualifiedRatio: null,
      shouldContinue: null,
      stopReason: null,
    };
  }

  const qualifiedRatio = total > 0 ? qualified / total : 0;
  const unqualifiedRatio = total > 0 ? rejected / total : 1;
  if (failed > 0) {
    return {
      settled: true,
      failed: true,
      total,
      qualified,
      rejected,
      pending: 0,
      qualifiedRatio,
      unqualifiedRatio,
      shouldContinue: false,
      stopReason: "snapshot_validation_failed",
    };
  }

  const shouldContinue = Boolean(hasContinuation)
    && total > 0
    && qualifiedRatio >= threshold;
  return {
    settled: true,
    failed: false,
    total,
    qualified,
    rejected,
    pending: 0,
    qualifiedRatio,
    unqualifiedRatio,
    shouldContinue,
    stopReason: shouldContinue
      ? null
      : total === 0
        ? "no_candidates"
        : !hasContinuation
          ? "no_continuation"
          : "qualified_ratio_below_threshold",
  };
}
