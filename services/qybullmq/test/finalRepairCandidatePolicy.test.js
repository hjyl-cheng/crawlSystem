import assert from "node:assert/strict";
import test from "node:test";
import {
  finalRepairCandidateDecision,
  finalRepairCandidateSql,
  preparedFinalDetailRepairSql,
} from "../src/finalRepairCandidatePolicy.js";

function candidate({
  detailStatus = "done",
  accessStatus = "public",
  missingFields = [],
  disposition = null,
  nextAttemptAt = null,
} = {}) {
  return {
    detail_status: detailStatus,
    missing_fields: missingFields,
    disposition,
    next_attempt_at: nextAttemptAt,
    result_json: { access: { access_status: accessStatus } },
  };
}

test("unknown unavailable detail is repairable because no terminal access fact exists", () => {
  assert.deepEqual(finalRepairCandidateDecision(candidate({
    detailStatus: "unavailable",
    accessStatus: "unknown",
  })), {
    repairable: true,
    reason: "unavailable_without_access_evidence",
  });
});

test("terminal access evidence cannot be bypassed by a missing content type", () => {
  for (const accessStatus of ["members_only", "private", "unlisted", "unavailable"]) {
    assert.deepEqual(finalRepairCandidateDecision(candidate({
      detailStatus: "unavailable",
      accessStatus,
      missingFields: ["content_type", "duration_seconds"],
    })), {
      repairable: false,
      reason: `terminal_access:${accessStatus}`,
    });
  }
});

test("a public missing type and a non-terminal failed detail remain repairable", () => {
  assert.equal(finalRepairCandidateDecision(candidate({
    missingFields: ["content_type"],
  })).repairable, true);
  assert.equal(finalRepairCandidateDecision(candidate({
    detailStatus: "failed",
    accessStatus: "unknown",
  })).repairable, true);
});

test("a deferred detail becomes repairable only when its scheduled retry is due", () => {
  const now = new Date("2026-08-22T13:45:00.000Z");
  assert.deepEqual(finalRepairCandidateDecision(candidate({
    detailStatus: "failed",
    accessStatus: "unknown",
    missingFields: ["content_type"],
    disposition: "deferred",
    nextAttemptAt: "2026-08-22T19:45:00.000Z",
  }), { now }), {
    repairable: false,
    reason: "scheduled_disposition_not_due",
  });
  assert.equal(finalRepairCandidateDecision(candidate({
    detailStatus: "failed",
    accessStatus: "unknown",
    missingFields: ["content_type"],
    disposition: "deferred",
    nextAttemptAt: "2026-08-22T12:45:00.000Z",
  }), { now }).repairable, true);
});

test("a scheduled disposition without a valid retry time never repairs immediately", () => {
  const now = new Date("2026-08-22T13:45:00.000Z");
  for (const nextAttemptAt of [null, "", "not-a-timestamp"]) {
    assert.deepEqual(finalRepairCandidateDecision(candidate({
      detailStatus: "failed",
      accessStatus: "unknown",
      missingFields: ["content_type"],
      disposition: "deferred",
      nextAttemptAt,
    }), { now }), {
      repairable: false,
      reason: "scheduled_disposition_not_due",
    });
  }
});

test("the SQL policy applies terminal access exclusion outside every repair branch", () => {
  const sql = finalRepairCandidateSql("cc");
  assert.match(sql, /^\(\s*COALESCE\(cc\.result_json/s);
  assert.match(sql, /NOT IN \('members_only','private','unlisted','unavailable'\)\s+AND \(/s);
  assert.match(sql, /cc\.disposition NOT IN \('deferred','terminal_excluded'\)/);
  assert.match(sql, /cc\.next_attempt_at<=now\(\)/);
  assert.match(sql, /cc\.missing_fields @> ARRAY\['content_type'\]::text\[\]/);
  assert.throws(() => finalRepairCandidateSql("cc; DELETE"), /SQL alias/);
});

test("prepared Detail Repair SQL is scoped to the same pending repair round", () => {
  const sql = preparedFinalDetailRepairSql("cc", "r");
  assert.match(sql, /final_repair_dispatch,mode.*='detail'/s);
  assert.match(sql, /final_repair_dispatch,status.*='prepared'/s);
  assert.match(sql, /final_repair_dispatch,repair_round/s);
  assert.match(sql, /r\.result_json#>>'\{final_repair,rounds\}'/);
  assert.throws(
    () => preparedFinalDetailRepairSql("cc", "r; DELETE"),
    /SQL alias/,
  );
});
