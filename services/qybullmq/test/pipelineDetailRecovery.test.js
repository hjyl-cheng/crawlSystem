import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { reconcileRunDetailStatus } from "../src/runDetailStatus.js";

test("a successful Content detail retry clears stale Run error and persists request metrics", async () => {
  let runUpdate = null;
  const client = {
    async query(sql, params) {
      if (sql.includes("count(*)::int AS total")) {
        return {
          rows: [{
            total: 3,
            terminal: 3,
            api_open: 0,
            failed: 0,
            undisposed: 0,
            partial: 0,
            excluded: 1,
            age_excluded: 0,
            upcoming_excluded: 1,
            live_in_progress_excluded: 0,
            details_requested_due_to_unresolved_count: 2,
          }],
        };
      }
      if (sql.includes("UPDATE crawler.channel_runs")) {
        runUpdate = { sql, params };
        return { rows: [], rowCount: 1 };
      }
      if (sql.includes("FROM crawler.channel_runs run") && sql.includes("FOR UPDATE")) {
        return {
          rows: [{
            run_id: "run:detail-retry",
            channel_id: "UCdetailRetry",
            candidate_id: null,
            channel_status: "active",
            result_json: {},
          }],
        };
      }
      throw new Error(`unexpected SQL: ${sql}`);
    },
  };

  const result = await reconcileRunDetailStatus(client, "run:detail-retry");

  assert.equal(result.status, "done");
  assert.ok(runUpdate, "Run summary was not persisted");
  assert.equal(runUpdate.params[1], "done");
  assert.equal(runUpdate.params[8], null);
  assert.equal(runUpdate.params[9], 2);
  assert.match(runUpdate.sql, /WHEN \$2='failed' THEN error_message[\s\S]*?ELSE NULL/);
  assert.match(runUpdate.sql, /details_requested_due_to_unresolved_count/);
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
