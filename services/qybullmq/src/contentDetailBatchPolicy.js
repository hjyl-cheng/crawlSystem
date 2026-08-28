export function contentDetailBatchStopReason(result) {
  return result?.retryable ? "retryable" : null;
}
