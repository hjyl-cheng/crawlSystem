import assert from "node:assert/strict";
import test from "node:test";
import {
  assertFullCrawlCanaryJob,
  assertFullCrawlWorkerLane,
  fullCrawlWorkerPrefix,
} from "../src/fullCrawlCanary.js";
import {
  newFullCrawlFetchContractForJob,
  YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
} from "../src/fullCrawlFetchContract.js";

const data = {
  dispatch_batch_id: "fullcrawl-youtubejs-canary-one",
  pipeline_cycle_id: "fullcrawl-youtubejs-canary-one",
  reject_if_no_recent_content: true,
  crawl_mode: "full",
  query_id: null,
};

test("only the immutable migration canary batch selects the canary executor", () => {
  assert.deepEqual(newFullCrawlFetchContractForJob({ name: "channel-snapshot", data }, {}), YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT);
  assert.throws(() => assertFullCrawlCanaryJob("channel-snapshot-recovery", data));
  assert.throws(() => assertFullCrawlCanaryJob("channel-snapshot", { ...data, query_id: 1 }));
  assert.throws(() => assertFullCrawlCanaryJob("channel-snapshot", { ...data, pipeline_cycle_id: "ordinary" }));
  assert.throws(() => assertFullCrawlCanaryJob("channel-snapshot", { ...data, reject_if_no_recent_content: false }));
});

test("canary intake is separate and rejects ordinary jobs before execution", () => {
  assert.equal(fullCrawlWorkerPrefix({ enabledQueues: ["youtube-channel-crawl"], canary: true }), "bull-fullcrawl-youtubejs-v1");
  assert.equal(fullCrawlWorkerPrefix({ prefix: "shared", enabledQueues: [], canary: false }), "shared");
  assert.throws(() => fullCrawlWorkerPrefix({ enabledQueues: ["youtube-finalize"], canary: true }));
  const job = { name: "channel-snapshot", queueName: "youtube-channel-crawl", data };
  assert.doesNotThrow(() => assertFullCrawlWorkerLane(job, true));
  assert.throws(() => assertFullCrawlWorkerLane(job, false));
  assert.throws(() => assertFullCrawlWorkerLane({ ...job, data: {} }, true));
});
