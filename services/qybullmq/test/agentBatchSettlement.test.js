import assert from "node:assert/strict";
import test from "node:test";

import { classifyFullAgentBatchSettlement } from "../src/agentBatchSettlement.js";

test("one stale row cannot hide another row's applied Agent failure", () => {
  assert.deepEqual(classifyFullAgentBatchSettlement([
    "fence_rejected",
    "failure_applied",
  ]), {
    action: "throw",
    rowCount: 2,
    appliedCount: 1,
    failedCount: 1,
    fenceRejectedCount: 1,
  });
});

test("a full Agent batch is skipped only when every row is stale", () => {
  assert.deepEqual(classifyFullAgentBatchSettlement([
    "fence_rejected",
    "fence_rejected",
  ]), {
    action: "skip",
    rowCount: 2,
    appliedCount: 0,
    failedCount: 0,
    fenceRejectedCount: 2,
  });
  assert.deepEqual(classifyFullAgentBatchSettlement([
    "success_applied",
    "fence_rejected",
  ]), {
    action: "complete",
    rowCount: 2,
    appliedCount: 1,
    failedCount: 0,
    fenceRejectedCount: 1,
  });
});
