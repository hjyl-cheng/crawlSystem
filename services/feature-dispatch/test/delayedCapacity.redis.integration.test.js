import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { Queue } from "bullmq";
import { BullMqCapacityProbe, computeDispatchBudget } from "../src/dynamicDispatcher.js";

const redisUrl = process.env.FEATURE_DISPATCH_REDIS_TEST_URL;

test("real Redis delayed scores reserve due retries but release future API waits", {
  skip: !redisUrl, timeout: 10000,
}, async t => {
  const url = new URL(redisUrl);
  assert.ok(["localhost", "127.0.0.1"].includes(url.hostname));
  const options = { connection: { host: url.hostname, port: Number(url.port) }, prefix: `dispatch-delay-test-${randomUUID()}` };
  const queues = ["youtube-channel-incremental", "youtube-channel-crawl", "youtube-agent-incremental"]
    .map(name => new Queue(name, options));
  t.after(async () => {
    for (const queue of queues) { await queue.obliterate({ force: true }); await queue.close(); }
  });
  const [incrementalQueue, channelCrawlQueue, agentIncrementalQueue] = queues;
  await incrementalQueue.addBulk(Array.from({ length: 26 }, (_, i) => ({
    name: "api-wait", data: { video_api_continuation: { request_id: `request-${i}` } },
    opts: { delay: 86400000 },
  })));
  await incrementalQueue.addBulk(Array.from({ length: 3 }, () => ({ name: "retry", data: {}, opts: { delay: 1 } })));
  await delay(20);
  await channelCrawlQueue.add("migration", {});
  const probe = new BullMqCapacityProbe({ incrementalQueue, channelCrawlQueue, agentIncrementalQueue });
  const sample = await probe.sample();
  assert.equal(sample.incremental.counts.delayed, 29);
  assert.equal(sample.incremental.counts.delayed_ready, 3);
  sample.incremental.workers = 25;
  sample.channel_crawl.counts.waiting = 1000;
  const budget = computeDispatchBudget(sample, { minimumIncrementalShare: 0.7 });
  assert.equal(budget.incremental_pressure, 3);
  assert.equal(budget.total_limit, 32);
});
