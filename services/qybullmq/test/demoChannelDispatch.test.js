import assert from "node:assert/strict";
import test from "node:test";

import { buildDemoChannelCrawlJob } from "../src/demoChannelDispatch.js";

test("each Demo Page gets a distinct Channel Job execution identity", () => {
  const first = buildDemoChannelCrawlJob({
    pageId: "demo:run-one:page:1",
    channelId: "UC1234567890123456789012",
    pipelineCycleId: "pipeline:demo:run-one",
  });
  const second = buildDemoChannelCrawlJob({
    pageId: "demo:run-two:page:1",
    channelId: "UC1234567890123456789012",
    pipelineCycleId: "pipeline:demo:run-two",
  });

  assert.equal(first.data.dispatch_generation, 1);
  assert.equal(first.data.full_intent_id, "discover-demo:demo:run-one:page:1:UC1234567890123456789012");
  assert.notEqual(first.options.jobId, second.options.jobId);
});
