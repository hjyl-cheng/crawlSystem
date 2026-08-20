import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";


test("a successful Content detail retry clears the stale Channel Run error", async () => {
  const source = await readFile(new URL("../src/pipelineV2.js", import.meta.url), "utf8");
  const update = source.match(
    /async function updateRunDetailStatus[\s\S]*?UPDATE crawler\.channel_runs[\s\S]*?WHERE run_id=\$1/,
  )?.[0];

  assert.ok(update, "updateRunDetailStatus SQL was not found");
  assert.match(
    update,
    /error_message=CASE WHEN \$2='failed' THEN error_message ELSE NULL END/,
  );
});

test("Content detail aggregate failures carry the original retryable network error", async () => {
  const source = await readFile(new URL("../src/pipelineV2.js", import.meta.url), "utf8");
  const aggregateThrows = source.match(/throw contentDetailFailureError\([\s\S]*?\n\s*\);/g) ?? [];

  assert.match(source, /retryable_failure_error:/);
  assert.ok(aggregateThrows.some((block) => block.includes("inline channel crawl")
    && block.includes("retryable_failure_error")));
  assert.ok(aggregateThrows.some((block) => block.includes("resumed inline channel crawl")
    && block.includes("retryable_failure_error")));
});

test("classified-only candidates use access-status API fallback instead of blind retries", async () => {
  const source = await readFile(new URL("../src/pipelineV2.js", import.meta.url), "utf8");
  const branch = source.match(
    /if \(storageAction\.kind === "classified_only"\) \{[\s\S]*?\n  \}/,
  )?.[0];

  assert.ok(branch, "classified_only branch was not found");
  assert.match(branch, /classifiedOnlyResolutionAction/);
  assert.match(branch, /detail_status='api_pending'/);
  assert.match(branch, /enqueueYoutubeApiFallback/);
  assert.match(branch, /access_status/);
});
