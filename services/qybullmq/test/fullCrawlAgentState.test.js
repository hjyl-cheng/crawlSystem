import assert from "node:assert/strict";
import test from "node:test";

import { AGENT_TAXONOMY_VERSION } from "../src/publicationContract.js";
import { reconcileFullCrawlAgentState } from "../src/fullCrawlAgentState.js";

test("Full Crawl preserves done only for current publication Agent metadata", async () => {
  let statement = "";
  let parameters = null;
  const state = await reconcileFullCrawlAgentState(async (sql, params) => {
    statement = sql;
    parameters = params;
    return {
      rows: [{
        channel_id: "UCpublication",
        ready_for_agent: true,
        agent_status: "pending",
      }],
    };
  }, {
    channelId: "UCpublication",
    eligible: true,
  });

  assert.deepEqual(parameters, ["UCpublication", true, AGENT_TAXONOMY_VERSION]);
  assert.deepEqual(state, {
    channel_id: "UCpublication",
    ready_for_agent: true,
    agent_status: "pending",
  });
  assert.match(statement, /channel\.agent_status='done' AND EXISTS/);
  assert.match(statement, /profile\.input_content_hash ~ '\^sha256:/);
  assert.match(statement, /profile\.taxonomy_version=\$3/);
  assert.match(statement, /profile\.agent_version_hash ~ '\^sha256:/);
  assert.match(statement, /config\.provider='local-offline'/);
  assert.match(statement, /profile\.prompt_variant='local_offline'/);
  assert.match(statement, /profile\.prompt_template_id IS NULL/);
  assert.match(statement, /WHEN \$2 THEN 'pending'/);
  assert.match(statement, /ELSE 'skipped'/);
});

test("Full Crawl Agent reconciliation rejects missing Channels", async () => {
  await assert.rejects(
    reconcileFullCrawlAgentState(async () => ({ rows: [] }), {
      channelId: "UCmissing",
      eligible: false,
    }),
    /active Full Crawl Channel not found: UCmissing/,
  );
});

test("Full Crawl Agent reconciliation validates its boundary", async () => {
  await assert.rejects(
    reconcileFullCrawlAgentState(async () => ({ rows: [] }), {
      channelId: "UCpublication",
      eligible: "yes",
    }),
    /eligible must be boolean/,
  );
});
