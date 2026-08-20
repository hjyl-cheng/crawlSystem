export function repairDispatchCapacity({ ready, inFlight = 0, maximum = 50 }) {
  const maxBatch = Math.max(0, Math.floor(Number(maximum) || 0));
  const active = Math.max(0, Math.floor(Number(inFlight) || 0));
  const hasReady = ready !== null && ready !== undefined && Number.isFinite(Number(ready));
  const healthy = hasReady
    ? Math.max(0, Math.floor(Number(ready)))
    : maxBatch;
  return Math.max(0, Math.min(maxBatch, healthy) - active);
}

function nonNegativeInteger(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : 0;
}

function sqlAlias(value) {
  const alias = String(value ?? "").trim();
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) {
    throw new TypeError("Run SQL alias must be an identifier");
  }
  return `${alias}.`;
}

function sqlParameter(value, field) {
  const parameter = String(value ?? "").trim();
  if (!/^\$[1-9][0-9]*$/.test(parameter)) {
    throw new TypeError(`${field} must be a positional SQL parameter`);
  }
  return parameter;
}

export function finalRepairRoundDecision({
  finalRepairRounds = 0,
  finalRepairMaxRounds = 0,
  proxyControlStatus = null,
  checkpointRepairRounds = 0,
  checkpointRepairMaxRounds = 0,
} = {}) {
  const finalRounds = nonNegativeInteger(finalRepairRounds);
  const finalMaximum = nonNegativeInteger(finalRepairMaxRounds);
  const checkpointRounds = nonNegativeInteger(checkpointRepairRounds);
  const checkpointMaximum = nonNegativeInteger(checkpointRepairMaxRounds);
  const businessRunBudgetExhausted = String(proxyControlStatus ?? "").trim()
    === "business_run_budget_exhausted";

  if (businessRunBudgetExhausted) {
    if (checkpointRounds >= checkpointMaximum) {
      return {
        eligible: false,
        businessRunBudgetExhausted: true,
        finalRepairRound: null,
        checkpointRepairRound: null,
      };
    }
    return {
      eligible: true,
      businessRunBudgetExhausted: true,
      finalRepairRound: finalRounds + 1,
      checkpointRepairRound: checkpointRounds + 1,
    };
  }

  if (finalRounds >= finalMaximum) {
    return {
      eligible: false,
      businessRunBudgetExhausted: false,
      finalRepairRound: null,
      checkpointRepairRound: null,
    };
  }
  return {
    eligible: true,
    businessRunBudgetExhausted: false,
    finalRepairRound: finalRounds + 1,
    checkpointRepairRound: null,
  };
}

export function finalRepairRoundEligibilitySql(alias, {
  finalRepairMaxRoundsParameter = "$1",
  checkpointRepairMaxRoundsParameter = "$5",
} = {}) {
  const run = sqlAlias(alias);
  const finalMaximum = sqlParameter(
    finalRepairMaxRoundsParameter,
    "finalRepairMaxRoundsParameter",
  );
  const checkpointMaximum = sqlParameter(
    checkpointRepairMaxRoundsParameter,
    "checkpointRepairMaxRoundsParameter",
  );
  const proxyStatus = `COALESCE(${run}result_json#>>'{proxy_control,status}','')`;
  const finalRounds = `COALESCE((${run}result_json#>>'{final_repair,rounds}')::int,0)`;
  const checkpointRounds =
    `COALESCE((${run}result_json#>>'{checkpoint_repair,rounds}')::int,0)`;
  return `(
    (
      ${proxyStatus}<>'business_run_budget_exhausted'
      AND ${finalRounds} < ${finalMaximum}
    )
    OR (
      ${proxyStatus}='business_run_budget_exhausted'
      AND ${checkpointRounds} < ${checkpointMaximum}
    )
  )`;
}

export function finalRepairDispatchDecision({
  publicationGap = false,
  businessRunBudgetExhausted = false,
  failedCandidates = 0,
  preparedDetailCandidates = 0,
  repairableCandidates = 0,
  typeMissingCandidates = 0,
} = {}) {
  if (businessRunBudgetExhausted) {
    return {
      detailOnly: false,
      name: "channel-checkpoint-repair",
      strategy: "checkpoint",
    };
  }
  const detailOnly = !publicationGap
    && (
      Number(preparedDetailCandidates) > 0
      || (
        Number(typeMissingCandidates) === 0
        && (Number(repairableCandidates) > 0 || Number(failedCandidates) > 0)
      )
    );
  return {
    detailOnly,
    name: detailOnly ? "channel-detail-repair" : "channel-crawl-repair",
    strategy: detailOnly ? "detail" : "channel",
  };
}

export function automaticFinalRepairReference({
  runId,
  candidateId = null,
  registryPromotionRunId = null,
  detailOnly = false,
  forceChildRun = false,
} = {}) {
  const normalizedRunId = String(runId ?? "").trim();
  if (!normalizedRunId) throw new TypeError("runId is required");
  if (detailOnly) return { run_id: normalizedRunId };
  if (forceChildRun) return { repair_parent_run_id: normalizedRunId };

  const promotionRunId = String(registryPromotionRunId ?? "").trim();
  if (promotionRunId === normalizedRunId) {
    const normalizedCandidateId = Number(candidateId);
    if (!Number.isSafeInteger(normalizedCandidateId) || normalizedCandidateId <= 0) {
      throw new TypeError("a Promotion Run repair requires candidateId");
    }
    return {
      candidate_id: normalizedCandidateId,
      repair_parent_run_id: normalizedRunId,
    };
  }

  return { repair_parent_run_id: normalizedRunId };
}
