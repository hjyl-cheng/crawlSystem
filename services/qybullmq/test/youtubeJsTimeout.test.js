import assert from "node:assert/strict";
import test from "node:test";
import { Innertube } from "youtubei.js";
import {
  ChannelExecutionMetrics,
  runWithChannelExecution,
} from "../src/channelExecutionContext.js";

test("YouTube.js internal timeout remains a recorded upstream failure", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousTimeout = process.env.YOUTUBEJS_TIMEOUT_MS;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const proxy = { proxy_id: 20, proxy_address_hash: "same-route" };

  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  process.env.YOUTUBEJS_TIMEOUT_MS = "1000";
  delete process.env.YOUTUBE_PROXY_URL;
  Innertube.create = async ({ fetch }) => ({
    async getInfo(videoId) {
      await fetch("https://www.youtube.com/youtubei/v1/player", {
        method: "POST",
        body: JSON.stringify({ videoId }),
      });
      throw new Error("transport unexpectedly resolved");
    },
  });

  const {
    closeYoutubeJs,
    fetchYoutubeJsVideoDetail,
    youtubeJsState,
  } = await import("../src/youtubeJs.js");
  const metrics = new ChannelExecutionMetrics();
  let caught = null;

  try {
    await runWithChannelExecution({
      proxy,
      get_proxy_snapshot: () => proxy,
      profile_group: {
        clients: { youtubejs_chrome: { profile_id: "test-chrome-timeout" } },
      },
      fingerprint_gateway: {
        fetch(_profile, _input, init) {
          return new Promise((resolve, reject) => {
            const rejectFromAbort = () => reject(init.signal.reason);
            if (init.signal.aborted) rejectFromAbort();
            else init.signal.addEventListener("abort", rejectFromAbort, { once: true });
          });
        },
      },
      metrics,
      abort_signal: null,
    }, async () => {
      try {
        await fetchYoutubeJsVideoDetail("timeout-video");
      } catch (error) {
        caught = error;
        throw error;
      }
    });
    assert.fail("internal timeout must reject");
  } catch (error) {
    assert.equal(error, caught);
    assert.equal(youtubeJsState().stats.requests, 1);
    assert.equal(youtubeJsState().stats.failures, 1);
    assert.equal(metrics.snapshot().failure_count, 1);
    assert.equal(metrics.snapshot().failure_evidence.length, 1);
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousTimeout === undefined) delete process.env.YOUTUBEJS_TIMEOUT_MS;
    else process.env.YOUTUBEJS_TIMEOUT_MS = previousTimeout;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});
