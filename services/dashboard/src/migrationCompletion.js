export const MIGRATION_FINAL_STATUSES = Object.freeze(["ready_auto", "ready_partial"]);
export const MIGRATION_FINALIZED_SQL = "fp.status IN ('ready_auto','ready_partial')";

const finalStatusSet = new Set(MIGRATION_FINAL_STATUSES);

export function migrationIsFinalized(status) {
  return finalStatusSet.has(String(status ?? ""));
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
    && String(candidateId ?? "") !== ""
    && String(candidateId) === String(promotionCandidateId ?? "")
    && !migrationIsFinalized(finalizedStatus);
}

export function migrationIncompleteSql(
  candidateAlias = "candidate",
  channelAlias = "channel",
  finalizedAlias = "finalized",
) {
  return `(
    ${candidateAlias}.status='accepted'
    AND ${channelAlias}.registry_promotion_candidate_id=${candidateAlias}.candidate_id
    AND ${channelAlias}.status='active'
    AND COALESCE(${finalizedAlias}.status,'pending') NOT IN ('ready_auto','ready_partial')
  )`;
}

export function migrationWorkSql() {
  return `(
    candidate.status IN ('discovered','queued','validating','failed')
    OR ${migrationIncompleteSql()}
  )`;
}
