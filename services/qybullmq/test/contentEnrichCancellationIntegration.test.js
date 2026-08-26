import assert from "node:assert/strict";
import test from "node:test";
import { Innertube } from "youtubei.js";
import {
  ChannelExecutionMetrics,
  runWithChannelExecution,
} from "../src/channelExecutionContext.js";
import { ContentEnrichExecutor } from "../src/contentEnrichExecution.js";
import { fetchIncrementalVideoDetail } from "../src/incrementalVideo.js";
import { closeYoutubeJs } from "../src/youtubeJs.js";

function heartbeatTimer() {
  let callback = null;
  return {
    set(next) {
      callback = next;
      return { unref() {} };
    },
    clear() {
      callback = null;
    },
    async fire() {
      assert.equal(typeof callback, "function");
      const next = callback;
      callback = null;
      next();
      for (let index = 0; index < 8; index += 1) await Promise.resolve();
    },
  };
}

test("Enrich lease loss aborts the real YouTube.js detail transport without failure evidence", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const timer = heartbeatTimer();
  const metrics = new ChannelExecutionMetrics();
  const proxy = { proxy_id: 29, proxy_address_hash: "enrich-cancel-route" };
  let notifyTransportStarted;
  let rejectTransport;
  let transportSignal = null;
  const transportStarted = new Promise((resolve) => { notifyTransportStarted = resolve; });
  const settledCalls = [];
  const tasks = [
    { task_id: "enrich-first", dispatch_generation: 1, source_content_id: "enrich-first" },
    { task_id: "enrich-second", dispatch_generation: 2, source_content_id: "enrich-second" },
  ];

  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  delete process.env.YOUTUBE_PROXY_URL;
  Innertube.create = async ({ fetch }) => ({
    async getInfo(videoId) {
      await fetch("https://www.youtube.com/youtubei/v1/player", {
        method: "POST",
        body: JSON.stringify({ videoId }),
      });
      throw new Error("controlled transport unexpectedly resolved");
    },
  });

  const executor = new ContentEnrichExecutor({
    repository: {
      async claimBatch() { return tasks; },
      async renewBatch() { return 0; },
      async settleBatch(input) {
        settledCalls.push(input);
        return { done: 0, terminal: 0, retryable: 0, dead_letter: 0, skipped: 0 };
      },
    },
    fetchDetail: fetchIncrementalVideoDetail,
    now: () => new Date("2026-08-26T00:00:00.000Z"),
    leaseDurationMs: 60_000,
    heartbeatIntervalMs: 10_000,
    setHeartbeatTimeout: timer.set,
    clearHeartbeatTimeout: timer.clear,
  });

  const execution = runWithChannelExecution({
    proxy,
    get_proxy_snapshot: () => proxy,
    profile_group: {
      clients: { youtubejs_chrome: { profile_id: "enrich-cancel-chrome" } },
    },
    fingerprint_gateway: {
      fetch(_profile, _input, init) {
        transportSignal = init.signal;
        notifyTransportStarted();
        return new Promise((resolve, reject) => {
          rejectTransport = reject;
          const rejectFromAbort = () => reject(init.signal.reason);
          if (init.signal.aborted) rejectFromAbort();
          else init.signal.addEventListener("abort", rejectFromAbort, { once: true });
        });
      },
    },
    metrics,
    abort_signal: null,
  }, () => executor.execute({
    id: "content_enrich__UC-enrich__cancel",
    data: {
      channel_id: "UC-enrich",
      tasks: tasks.map(({ task_id, dispatch_generation }) => ({ task_id, dispatch_generation })),
    },
  }));

  try {
    await transportStarted;
    await timer.fire();
    const result = await execution;

    assert.equal(transportSignal.aborted, true);
    assert.equal(result.attempted, 1);
    assert.equal(result.skipped, 2);
    assert.equal(result.heartbeat.lease_lost, true);
    assert.equal(settledCalls[0].outcomes.length, 0);
    assert.equal(settledCalls[0].unattemptedTasks.length, 2);
    assert.equal(metrics.snapshot().failure_count, 0);
    assert.deepEqual(metrics.snapshot().failure_evidence, []);
  } finally {
    rejectTransport?.(new Error("controlled transport cleanup"));
    await execution.catch(() => {});
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});
