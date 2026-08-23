import assert from "node:assert/strict";
import test from "node:test";
import { queueRefreshTask } from "../src/incrementalVideo.js";

test("open Enrich Task re-entry preserves attempts, retry time, and a live lease", async () => {
  let statement = null;
  const client = {
    async query(sql, params) {
      statement = { sql, params };
      return { rowCount: 1, rows: [] };
    },
  };

  await queueRefreshTask(client, {
    contentKey: "UC-requeue:video:one",
    channelId: "UC-requeue",
    runId: "incremental:requeue",
    observationId: "11111111-1111-4111-8111-111111111111",
    jobType: "player-refresh",
    error: new Error("temporary detail failure"),
  });

  assert.match(statement.sql, /attempts=CASE[\s\S]*status IN \('done','terminal','skipped'\)[\s\S]*THEN 0[\s\S]*ELSE crawler\.content_enrich_tasks\.attempts END/);
  assert.match(statement.sql, /status IN \('leased','running'\)[\s\S]*lease_expires_at>now\(\)[\s\S]*THEN crawler\.content_enrich_tasks\.lease_owner/);
  assert.match(statement.sql, /next_retry_at=CASE[\s\S]*status IN \('done','terminal','skipped'\)[\s\S]*THEN now\(\)[\s\S]*ELSE crawler\.content_enrich_tasks\.next_retry_at END/);
  assert.doesNotMatch(statement.sql, /SET[\s\S]*attempts=0\s*,/);
});
