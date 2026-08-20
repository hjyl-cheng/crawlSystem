import assert from "node:assert/strict";
import test from "node:test";
import {
  ChannelSlotExecutor,
  ChannelSlotExecutorClosedError,
} from "../src/channelSlotExecutor.js";

const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

test("one slot executes channel jobs one at a time", async () => {
  const executor = new ChannelSlotExecutor();
  const events = [];
  let active = 0;
  let maximumActive = 0;
  const execute = (id, delay) => executor.run({ id, queueName: `queue-${id}` }, async () => {
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    events.push(`start:${id}`);
    await sleep(delay);
    events.push(`finish:${id}`);
    active -= 1;
    return id;
  });

  const first = execute("full", 10);
  const second = execute("incremental", 1);
  assert.equal(executor.state().waiting, 1);

  assert.deepEqual(await Promise.all([first, second]), ["full", "incremental"]);
  assert.equal(maximumActive, 1);
  assert.deepEqual(events, [
    "start:full",
    "finish:full",
    "start:incremental",
    "finish:incremental",
  ]);
  assert.deepEqual(executor.state(), {
    active_job_id: null,
    active_queue: null,
    waiting: 0,
    paused: false,
    pause_reason: null,
    closed: false,
    oldest_full_wait_ms: 0,
  });
});

test("slot serialization does not limit concurrency inside one channel job", async () => {
  const executor = new ChannelSlotExecutor();
  let activeVideos = 0;
  let maximumActiveVideos = 0;
  await executor.run({ id: "channel-a" }, async () => {
    await Promise.all([1, 2].map(async () => {
      activeVideos += 1;
      maximumActiveVideos = Math.max(maximumActiveVideos, activeVideos);
      await sleep(5);
      activeVideos -= 1;
    }));
  });
  assert.equal(maximumActiveVideos, 2);
});

test("a failed channel job releases the slot for the next job", async () => {
  const executor = new ChannelSlotExecutor();
  const failed = executor.run({ id: "failed" }, async () => {
    throw new Error("expected failure");
  });
  const next = executor.run({ id: "next" }, async () => "completed");

  await assert.rejects(failed, /expected failure/);
  assert.equal(await next, "completed");
});

test("incremental jobs go first while an aged full job cannot starve", async () => {
  let now = 0;
  const executor = new ChannelSlotExecutor({ fullMaxWaitMs: 100, now: () => now });
  let releaseActive;
  const active = executor.execute({ id: "active", queueName: "youtube-channel-crawl" }, () => (
    new Promise((resolve) => { releaseActive = resolve; })
  ));
  await Promise.resolve();
  const order = [];
  const firstFull = executor.execute({ id: "full-1", queueName: "youtube-channel-crawl" }, () => order.push("full-1"));
  now = 50;
  const incremental = executor.execute({ id: "incremental", queueName: "youtube-channel-incremental" }, () => order.push("incremental"));
  const secondFull = executor.execute({ id: "full-2", queueName: "youtube-channel-crawl" }, () => order.push("full-2"));
  releaseActive();
  await Promise.all([active, incremental, firstFull, secondFull]);
  assert.deepEqual(order, ["incremental", "full-1", "full-2"]);

  let releaseSecondActive;
  const secondActive = executor.execute({ id: "active-2", queueName: "youtube-channel-crawl" }, () => (
    new Promise((resolve) => { releaseSecondActive = resolve; })
  ));
  await Promise.resolve();
  const agedFull = executor.execute({ id: "aged-full", queueName: "youtube-channel-crawl" }, () => order.push("aged-full"));
  now = 200;
  const newerIncremental = executor.execute({ id: "incremental-2", queueName: "youtube-channel-incremental" }, () => order.push("incremental-2"));
  releaseSecondActive();
  await Promise.all([secondActive, agedFull, newerIncremental]);
  assert.deepEqual(order.slice(-2), ["aged-full", "incremental-2"]);
});

test("pause blocks the next job without interrupting the active job", async () => {
  const executor = new ChannelSlotExecutor();
  let release;
  const events = [];
  const active = executor.execute({ id: "active" }, async () => {
    events.push("active:start");
    await new Promise((resolve) => { release = resolve; });
    events.push("active:finish");
  });
  await Promise.resolve();
  const waiting = executor.execute({ id: "waiting" }, () => events.push("waiting:start"));
  executor.pause("proxy_replacement");
  release();
  await active;
  await sleep(1);
  assert.deepEqual(events, ["active:start", "active:finish"]);
  assert.equal(executor.state().pause_reason, "proxy_replacement");
  executor.resume();
  await waiting;
  assert.deepEqual(events, ["active:start", "active:finish", "waiting:start"]);
});

test("close rejects waiting and future jobs but lets the active job finish", async () => {
  const executor = new ChannelSlotExecutor();
  let release;
  const active = executor.execute({ id: "active" }, () => new Promise((resolve) => { release = resolve; }));
  await Promise.resolve();
  const waiting = executor.execute({ id: "waiting" }, () => assert.fail("waiting job must not run"));
  const closing = executor.close();
  await assert.rejects(waiting, ChannelSlotExecutorClosedError);
  await assert.rejects(executor.execute({ id: "future" }, () => {}), ChannelSlotExecutorClosedError);
  release();
  await active;
  await closing;
  assert.equal(executor.state().closed, true);
});
