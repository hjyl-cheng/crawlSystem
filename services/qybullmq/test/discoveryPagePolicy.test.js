import assert from "node:assert/strict";
import test from "node:test";

import { resolveDiscoveryPageQualification } from "../src/discoveryPagePolicy.js";

test("discovery page waits for every snapshot before deciding continuation", () => {
  const result = resolveDiscoveryPageQualification({
    acceptedCount: 4,
    existingCount: 1,
    rejectedCount: 2,
    failedCount: 0,
    pendingCount: 3,
    hasContinuation: true,
    minQualifiedRatio: 1 / 3,
  });

  assert.equal(result.settled, false);
  assert.equal(result.shouldContinue, null);
  assert.equal(result.qualifiedRatio, null);
});

test("accepted and existing channels both count as qualified", () => {
  const result = resolveDiscoveryPageQualification({
    acceptedCount: 4,
    existingCount: 2,
    rejectedCount: 4,
    failedCount: 0,
    pendingCount: 0,
    hasContinuation: true,
    minQualifiedRatio: 0.5,
  });

  assert.equal(result.settled, true);
  assert.equal(result.qualified, 6);
  assert.equal(result.qualifiedRatio, 0.6);
  assert.equal(result.shouldContinue, true);
});

test("a page stops when its settled qualified ratio is below the threshold", () => {
  const result = resolveDiscoveryPageQualification({
    acceptedCount: 2,
    existingCount: 1,
    rejectedCount: 7,
    failedCount: 0,
    pendingCount: 0,
    hasContinuation: true,
    minQualifiedRatio: 1 / 3,
  });

  assert.equal(result.qualifiedRatio, 0.3);
  assert.equal(result.shouldContinue, false);
  assert.equal(result.stopReason, "qualified_ratio_below_threshold");
});

test("terminal snapshot failures fail the page instead of skewing qualification", () => {
  const result = resolveDiscoveryPageQualification({
    acceptedCount: 8,
    existingCount: 0,
    rejectedCount: 1,
    failedCount: 1,
    pendingCount: 0,
    hasContinuation: true,
    minQualifiedRatio: 1 / 3,
  });

  assert.equal(result.failed, true);
  assert.equal(result.shouldContinue, false);
  assert.equal(result.stopReason, "snapshot_validation_failed");
});
