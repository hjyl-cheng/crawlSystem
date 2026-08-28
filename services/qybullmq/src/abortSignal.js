export function combineAbortSignals(...signals) {
  const unique = [...new Set(signals.filter(Boolean))];
  if (unique.length === 0) return null;
  if (unique.length === 1) return unique[0];
  return AbortSignal.any(unique);
}

export function throwIfAborted(signal) {
  if (signal?.aborted) throw signal.reason;
}
