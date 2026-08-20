export function shouldReportProxyJobSuccess(jobName, result = {}) {
  if (result?.skipped) return false;
  if (jobName === "channel-detail-repair") {
    return result?.status === "done"
      && Number(result?.failed ?? 0) === 0
      && Number(result?.partial ?? 0) === 0;
  }
  return Boolean(result?.phase_timings_ms)
    && Number(result?.detail_partial ?? 0) === 0;
}
