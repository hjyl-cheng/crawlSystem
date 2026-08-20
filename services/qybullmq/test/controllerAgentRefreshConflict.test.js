import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { finalRepairCandidateDecision } from "../src/finalRepairCandidatePolicy.js";

test("Full Crawl Agent dispatch ignores terminal incremental refresh failures", async () => {
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");
  const retryableFailureGuards = source.match(
    /refresh\.status='failed' AND isfinite\(refresh\.next_retry_at\)/g,
  ) ?? [];

  assert.equal(retryableFailureGuards.length, 3);
  assert.doesNotMatch(
    source,
    /refresh\.status IN \('pending','queued','running','failed'\)/,
  );
});

test("Full Crawl repair never retries explicitly unlisted Content", () => {
  assert.deepEqual(finalRepairCandidateDecision({
    detail_status: "failed",
    result_json: { access: { access_status: "unlisted" } },
  }), {
    repairable: false,
    reason: "terminal_access:unlisted",
  });
});
