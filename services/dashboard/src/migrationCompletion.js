export const MIGRATION_FINAL_STATUSES = Object.freeze(["ready_auto", "ready_partial"]);
export const MIGRATION_FINALIZED_SQL = "fp.status IN ('ready_auto','ready_partial')";
export const MIGRATION_WORK_STATUSES = Object.freeze([
  "discovered",
  "queued",
  "validating",
  "failed",
  "finishing",
]);
export const MIGRATION_WORK_STATUSES_SQL = "('discovered','queued','validating','failed','finishing')";

const finalStatusSet = new Set(MIGRATION_FINAL_STATUSES);
const workStatusSet = new Set(MIGRATION_WORK_STATUSES);

export function migrationIsFinalized(status) {
  return finalStatusSet.has(String(status ?? ""));
}

export function migrationIsWorkStatus(status) {
  return workStatusSet.has(String(status ?? ""));
}

function isPromotedCandidate(candidateId, promotionCandidateId) {
  return candidateId != null
    && String(candidateId) !== ""
    && String(candidateId) === String(promotionCandidateId ?? "");
}

export function migrationIsIncompletePromotion({
  candidateId,
  candidateStatus,
  channelStatus,
  promotionCandidateId,
  finalizedStatus,
} = {}) {
  return String(candidateStatus ?? "") === "accepted"
    && String(channelStatus ?? "") === "active"
    && isPromotedCandidate(candidateId, promotionCandidateId)
    && !migrationIsFinalized(finalizedStatus);
}

export function migrationLifecycleState({
  candidateId,
  candidateStatus,
  channelStatus,
  promotionCandidateId,
  finalizedStatus,
} = {}) {
  const normalizedCandidateStatus = String(candidateStatus ?? "").trim();
  const normalizedChannelStatus = String(channelStatus ?? "").trim();
  const migrationIncomplete = migrationIsIncompletePromotion({
    candidateId,
    candidateStatus: normalizedCandidateStatus,
    channelStatus: normalizedChannelStatus,
    promotionCandidateId,
    finalizedStatus,
  });
  const migrationDone = normalizedCandidateStatus === "accepted"
    && ["active", "dormant"].includes(normalizedChannelStatus)
    && isPromotedCandidate(candidateId, promotionCandidateId)
    && migrationIsFinalized(finalizedStatus);
  const resolvedCandidateStatus = migrationIncomplete
    ? "finishing"
    : normalizedCandidateStatus || "discovered";
  return {
    candidateStatus: resolvedCandidateStatus,
    status: migrationIncomplete
      ? "finishing"
      : normalizedChannelStatus || resolvedCandidateStatus,
    migrationIncomplete,
    migrationDone,
  };
}

export function migrationIncompleteSql(
  candidateAlias = "candidate",
  channelAlias = "channel",
  finalizedAlias = "finalized",
) {
  return `COALESCE((
      ${candidateAlias}.status='accepted'
      AND ${channelAlias}.registry_promotion_candidate_id=${candidateAlias}.candidate_id
      AND ${channelAlias}.status='active'
      AND COALESCE(${finalizedAlias}.status,'pending') NOT IN ('ready_auto','ready_partial')
    ),false)`;
}

export function migrationDoneSql(
  candidateAlias = "candidate",
  channelAlias = "channel",
  finalizedAlias = "finalized",
) {
  return `COALESCE((
      ${candidateAlias}.status='accepted'
      AND ${channelAlias}.registry_promotion_candidate_id=${candidateAlias}.candidate_id
      AND ${channelAlias}.status IN ('active','dormant')
      AND COALESCE(${finalizedAlias}.status,'pending') IN ('ready_auto','ready_partial')
    ),false)`;
}

export function migrationCandidateStatusSql(
  candidateAlias = "candidate",
  channelAlias = "channel",
  finalizedAlias = "finalized",
) {
  return `CASE
    WHEN ${migrationIncompleteSql(candidateAlias, channelAlias, finalizedAlias)}
    THEN 'finishing'
    ELSE COALESCE(${candidateAlias}.status,'discovered')
  END`;
}

export function migrationDisplayStatusSql(
  candidateAlias = "candidate",
  channelAlias = "channel",
  finalizedAlias = "finalized",
) {
  return `CASE
    WHEN ${migrationIncompleteSql(candidateAlias, channelAlias, finalizedAlias)}
    THEN 'finishing'
    ELSE COALESCE(
      ${channelAlias}.status,
      ${migrationCandidateStatusSql(candidateAlias, channelAlias, finalizedAlias)}
    )
  END`;
}
