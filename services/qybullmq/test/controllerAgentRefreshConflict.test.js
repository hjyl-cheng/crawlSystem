import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { finalRepairCandidateDecision } from "../src/finalRepairCandidatePolicy.js";

test("Full Crawl Agent dispatch ignores terminal incremental refresh failures", async () => {
  const source = (await Promise.all(["controller.js", "agentBatchDispatch.js"].map(name => readFile(new URL(`../src/${name}`, import.meta.url), "utf8")))).join("\n");
  const retryableFailureGuards = source.match(
    /refresh\.status='failed' AND isfinite\(refresh\.next_retry_at\)/g,
  ) ?? [];

  // One Controller query and three Agent dispatch queries share this guard.
  assert.equal(retryableFailureGuards.length, 4);
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
