const ROW_OUTCOMES = new Set([
  "success_applied",
  "failure_applied",
  "fence_rejected",
]);

export function classifyFullAgentBatchSettlement(rowOutcomes) {
  if (!Array.isArray(rowOutcomes) || rowOutcomes.length === 0) {
    throw new TypeError("full Agent row outcomes must be a non-empty array");
  }
  const outcomes = rowOutcomes.map(String);
  if (outcomes.some((outcome) => !ROW_OUTCOMES.has(outcome))) {
    throw new TypeError("full Agent row outcome is invalid");
  }
  const failedCount = outcomes.filter((outcome) => outcome === "failure_applied").length;
  const fenceRejectedCount = outcomes
    .filter((outcome) => outcome === "fence_rejected").length;
  const appliedCount = outcomes.length - fenceRejectedCount;
  const action = failedCount > 0
    ? "throw"
    : fenceRejectedCount === outcomes.length ? "skip" : "complete";
  return Object.freeze({
    action,
    rowCount: outcomes.length,
    appliedCount,
    failedCount,
    fenceRejectedCount,
  });
}
