import assert from "node:assert/strict";
import test from "node:test";
import {
  disabledCommentCountRepairConfirmation,
  disabledCommentCountRepairEvidenceHash,
} from "../src/disabledCommentCountRepair.js";
import { disabledCommentCountRepairCommand } from
  "../scripts/repairDisabledCommentCounts.mjs";

function environment(overrides = {}) {
  return {
    CRAWLER_DATABASE_URL: "postgres://crawler.test/disabled_comments_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "61",
    EXPECTED_CRAWLER_CONTENT_COUNT: "500",
    DISABLED_COMMENT_COUNT_REPAIR_OPERATOR: "integration-test",
    DISABLED_COMMENT_COUNT_REPAIR_REASON: "normalize disabled comments",
    ...overrides,
  };
}

test("disabled Comment repair evidence hash is canonical and confirmation binds the target", () => {
  const evidence = {
    database_name: "disabled_comments_test",
    target_count: 2,
    targets: [{ channel_id: "UC2" }, { channel_id: "UC1" }],
  };
  const reordered = {
    targets: evidence.targets,
    target_count: evidence.target_count,
    database_name: evidence.database_name,
  };
  const evidenceHash = disabledCommentCountRepairEvidenceHash(evidence);
  assert.equal(disabledCommentCountRepairEvidenceHash(reordered), evidenceHash);
  assert.equal(
    disabledCommentCountRepairConfirmation({ ...evidence, evidence_hash: evidenceHash }),
    `repair-disabled-comment-counts:disabled_comments_test:2:${evidenceHash}`,
  );
});

test("disabled Comment repair defaults to read-only plan mode", () => {
  assert.deepEqual(disabledCommentCountRepairCommand([], environment()), {
    apply: false,
    databaseUrl: "postgres://crawler.test/disabled_comments_test",
    expectedChannelCount: 61,
    expectedContentCount: 500,
    operator: "integration-test",
    reason: "normalize disabled comments",
    expectedTargetCount: null,
    expectedEvidenceHash: null,
    confirmation: null,
  });
});

test("disabled Comment repair apply requires exact count, hash, and confirmation", () => {
  assert.throws(
    () => disabledCommentCountRepairCommand(["--apply"], environment()),
    /EXPECTED_DISABLED_COMMENT_COUNT_REPAIR_COUNT/,
  );
  const command = disabledCommentCountRepairCommand(["--apply"], environment({
    EXPECTED_DISABLED_COMMENT_COUNT_REPAIR_COUNT: "49",
    EXPECTED_DISABLED_COMMENT_COUNT_REPAIR_EVIDENCE_HASH: `sha256:${"a".repeat(64)}`,
    CONFIRM_DISABLED_COMMENT_COUNT_REPAIR: "explicit-confirmation",
  }));
  assert.equal(command.expectedTargetCount, 49);
  assert.equal(command.expectedEvidenceHash, `sha256:${"a".repeat(64)}`);
  assert.equal(command.confirmation, "explicit-confirmation");
});
