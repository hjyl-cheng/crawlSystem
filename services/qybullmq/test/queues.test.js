import assert from "node:assert/strict";
import test from "node:test";
import {
  applyFailureRetryDecision,
  hasQueryPipelineQueueBacklog,
  queueNames,
  queuesByRole,
} from "../src/queues.js";

test("incremental Channel Plans have a queue isolated from Full crawling", () => {
  assert.equal(queuesByRole.channelIncremental, "youtube-channel-incremental");
  assert.equal(queueNames.includes(queuesByRole.channelIncremental), true);
  assert.notEqual(queuesByRole.channelIncremental, queuesByRole.channelCrawl);
});

test("terminal failure decisions discard BullMQ retries", () => {
  let discarded = false;
  const result = applyFailureRetryDecision({ discard() { discarded = true; } }, {
    retry_mode: "none",
  });
  assert.equal(discarded, true);
  assert.deepEqual(result, { retry: false, retry_mode: "none" });
  assert.deepEqual(applyFailureRetryDecision({}, { retry_mode: "new_identity" }), {
    retry: true,
    retry_mode: "new_identity",
    requires_new_identity: true,
  });
});

test("Query pipeline completion ignores independent Incremental queue backlog", () => {
  const stats = {
    [queuesByRole.channelIncremental]: { paused: 30 },
    [queuesByRole.agentIncremental]: { waiting: 2 },
  };

  assert.equal(hasQueryPipelineQueueBacklog(stats), false);

  stats[queuesByRole.channelCrawl] = { waiting: 1 };
  assert.equal(hasQueryPipelineQueueBacklog(stats), true);
});
