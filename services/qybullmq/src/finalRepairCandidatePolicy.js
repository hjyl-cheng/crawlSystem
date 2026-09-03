import {
  videoDispositionEligibleForImmediateRepair,
  videoDispositionImmediateRepairSql,
} from "./videoDisposition.js";

export const FINAL_REPAIR_TERMINAL_ACCESS_STATUSES = Object.freeze([
  "members_only",
  "private",
  "unlisted",
  "unavailable",
]);

function normalizedAlias(value) {
  const alias = String(value ?? "").trim();
  if (!alias) return "";
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) {
    throw new TypeError("SQL alias must be an identifier");
  }
  return `${alias}.`;
}

function normalizedAccessStatus(candidate = {}) {
  const value = String(candidate?.result_json?.access?.access_status ?? "unknown")
    .trim()
    .toLowerCase();
  return value || "unknown";
}

export function finalRepairCandidateDecision(candidate = {}, options = {}) {
  const accessStatus = normalizedAccessStatus(candidate);
  if (FINAL_REPAIR_TERMINAL_ACCESS_STATUSES.includes(accessStatus)) {
    return { repairable: false, reason: `terminal_access:${accessStatus}` };
  }
  if (!videoDispositionEligibleForImmediateRepair(candidate, options)) {
    return { repairable: false, reason: "scheduled_disposition_not_due" };
  }

  const detailStatus = String(candidate?.detail_status ?? "").trim().toLowerCase();
  const missingFields = Array.isArray(candidate?.missing_fields)
    ? candidate.missing_fields.map((field) => String(field))
    : [];
  if (detailStatus === "failed") return { repairable: true, reason: "detail_failed" };
  if (detailStatus === "unavailable" && accessStatus === "unknown") {
    return { repairable: true, reason: "unavailable_without_access_evidence" };
  }
  if (missingFields.includes("content_type")) {
    return { repairable: true, reason: "content_type_missing" };
  }
  if (["done", "unavailable"].includes(detailStatus) && missingFields.length > 0) {
    return { repairable: true, reason: "detail_fields_missing" };
  }
  return { repairable: false, reason: "no_repairable_detail_gap" };
}

export function finalRepairCandidateSql(alias = "") {
  const prefix = normalizedAlias(alias);
  const accessStatus = `COALESCE(${prefix}result_json#>>'{access,access_status}','unknown')`;
  const terminalStatuses = FINAL_REPAIR_TERMINAL_ACCESS_STATUSES
    .map((status) => `'${status}'`)
    .join(",");
  return `(
    ${accessStatus} NOT IN (${terminalStatuses})
    AND ${videoDispositionImmediateRepairSql(alias)}
    AND (
      ${prefix}detail_status='failed'
      OR (${prefix}detail_status='unavailable' AND ${accessStatus}='unknown')
      OR ${prefix}missing_fields @> ARRAY['content_type']::text[]
      OR (
        ${prefix}detail_status IN ('done','unavailable')
        AND cardinality(${prefix}missing_fields)>0
      )
    )
  )`;
}

export function preparedFinalDetailRepairSql(candidateAlias = "", runAlias = "") {
  const candidate = normalizedAlias(candidateAlias);
  const run = normalizedAlias(runAlias);
  if (!candidate || !run) {
    throw new TypeError("candidate and Run SQL aliases are required");
  }
  return `(
    ${candidate}result_json#>>'{final_repair_dispatch,mode}'='detail'
    AND ${candidate}result_json#>>'{final_repair_dispatch,status}'='prepared'
    AND ${candidate}result_json#>>'{final_repair_dispatch,repair_round}'
          =(
            COALESCE((${run}result_json#>>'{final_repair,rounds}')::int,0)+1
          )::text
  )`;
}

export function recoverablePreparedFinalDetailRepairSql(candidateAlias = "", runAlias = "") {
  const candidate = normalizedAlias(candidateAlias);
  const run = normalizedAlias(runAlias);
  if (!candidate || !run) {
    throw new TypeError("candidate and Run SQL aliases are required");
  }
  return `(
    ${candidate}result_json#>>'{final_repair_dispatch,mode}'='detail'
    AND ${candidate}result_json#>>'{final_repair_dispatch,status}'='prepared'
    AND ${candidate}result_json#>>'{final_repair_dispatch,repair_round}'
          =COALESCE((${run}result_json#>>'{final_repair,rounds}')::int,0)::text
    AND (
      ${run}detail_status<>'done'
      OR ${candidate}detail_status IN ('queued','running','failed')
      OR ${candidate}api_status='failed'
    )
  )`;
}
