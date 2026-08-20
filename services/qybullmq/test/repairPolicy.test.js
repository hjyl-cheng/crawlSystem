import assert from "node:assert/strict";
import test from "node:test";
import {
  automaticFinalRepairReference,
  finalRepairDispatchDecision,
  finalRepairRoundDecision,
  finalRepairRoundEligibilitySql,
  repairDispatchCapacity,
} from "../src/repairPolicy.js";

test("an exhausted Rota Business Run gets a checkpoint child instead of reusing its dead budget", () => {
  assert.deepEqual(finalRepairDispatchDecision({
    businessRunBudgetExhausted: true,
    failedCandidates: 3,
    repairableCandidates: 30,
  }), {
    detailOnly: false,
    name: "channel-checkpoint-repair",
    strategy: "checkpoint",
  });
  assert.deepEqual(automaticFinalRepairReference({
    runId: "run:exhausted",
    candidateId: 42,
    registryPromotionRunId: "run:exhausted",
    forceChildRun: true,
  }), {
    repair_parent_run_id: "run:exhausted",
  });
});

test("budget exhaustion gets one checkpoint round beyond the ordinary repair limit", () => {
  assert.deepEqual(finalRepairRoundDecision({
    finalRepairRounds: 3,
    finalRepairMaxRounds: 3,
    proxyControlStatus: "business_run_budget_exhausted",
    checkpointRepairRounds: 0,
    checkpointRepairMaxRounds: 1,
  }), {
    eligible: true,
    businessRunBudgetExhausted: true,
    finalRepairRound: 4,
    checkpointRepairRound: 1,
  });

  assert.equal(finalRepairRoundDecision({
    finalRepairRounds: 4,
    finalRepairMaxRounds: 3,
    proxyControlStatus: "business_run_budget_exhausted",
    checkpointRepairRounds: 1,
    checkpointRepairMaxRounds: 1,
  }).eligible, false);
});

test("ordinary repairs cannot bypass their round limit", () => {
  assert.equal(finalRepairRoundDecision({
    finalRepairRounds: 3,
    finalRepairMaxRounds: 3,
    proxyControlStatus: null,
    checkpointRepairRounds: 0,
    checkpointRepairMaxRounds: 1,
  }).eligible, false);

  assert.deepEqual(finalRepairRoundDecision({
    finalRepairRounds: 2,
    finalRepairMaxRounds: 3,
    proxyControlStatus: null,
    checkpointRepairRounds: 0,
    checkpointRepairMaxRounds: 1,
  }), {
    eligible: true,
    businessRunBudgetExhausted: false,
    finalRepairRound: 3,
    checkpointRepairRound: null,
  });
});

test("the SQL eligibility policy mirrors the bounded checkpoint exception", () => {
  const sql = finalRepairRoundEligibilitySql("r", {
    finalRepairMaxRoundsParameter: "$1",
    checkpointRepairMaxRoundsParameter: "$5",
  });

  assert.match(sql, /final_repair,rounds/);
  assert.match(sql, /proxy_control,status/);
  assert.match(sql, /business_run_budget_exhausted/);
  assert.match(sql, /checkpoint_repair,rounds/);
  assert.match(sql, /< \$1/);
  assert.match(sql, /< \$5/);
});

test("repair dispatch follows healthy channel capacity", () => {
  assert.equal(repairDispatchCapacity({ ready: 22, inFlight: 0, maximum: 50 }), 22);
  assert.equal(repairDispatchCapacity({ ready: 22, inFlight: 17, maximum: 50 }), 5);
  assert.equal(repairDispatchCapacity({ ready: 22, inFlight: 22, maximum: 50 }), 0);
});

test("repair dispatch honors its safety maximum and missing capacity fallback", () => {
  assert.equal(repairDispatchCapacity({ ready: 100, inFlight: 0, maximum: 50 }), 50);
  assert.equal(repairDispatchCapacity({ ready: null, inFlight: 4, maximum: 20 }), 16);
});

test("a failed Promotion Run is repaired as the same Candidate Business Run", () => {
  assert.deepEqual(automaticFinalRepairReference({
    runId: "run:promotion",
    candidateId: 42,
    registryPromotionRunId: "run:promotion",
    detailOnly: false,
  }), {
    candidate_id: 42,
    repair_parent_run_id: "run:promotion",
  });
});

test("ordinary Full Repair creates a later Business Run", () => {
  assert.deepEqual(automaticFinalRepairReference({
    runId: "run:current",
    candidateId: 42,
    registryPromotionRunId: "run:promotion",
    detailOnly: false,
  }), {
    repair_parent_run_id: "run:current",
  });
});

test("Detail Repair always resumes its existing Business Run", () => {
  assert.deepEqual(automaticFinalRepairReference({
    runId: "run:current",
    candidateId: 42,
    registryPromotionRunId: "run:promotion",
    detailOnly: true,
  }), {
    run_id: "run:current",
  });
});

test("a prepared Detail Repair keeps its immutable dispatch mode after candidate reset", () => {
  assert.deepEqual(finalRepairDispatchDecision({
    publicationGap: false,
    failedCandidates: 0,
    repairableCandidates: 0,
    typeMissingCandidates: 0,
    preparedDetailCandidates: 1,
  }), {
    detailOnly: true,
    name: "channel-detail-repair",
    strategy: "detail",
  });

  assert.deepEqual(finalRepairDispatchDecision({
    publicationGap: false,
    failedCandidates: 0,
    repairableCandidates: 0,
    typeMissingCandidates: 0,
    preparedDetailCandidates: 0,
  }), {
    detailOnly: false,
    name: "channel-crawl-repair",
    strategy: "channel",
  });

  assert.equal(finalRepairDispatchDecision({
    publicationGap: true,
    preparedDetailCandidates: 1,
  }).detailOnly, false);
  assert.equal(finalRepairDispatchDecision({
    preparedDetailCandidates: 1,
    typeMissingCandidates: 1,
  }).detailOnly, true);
});
