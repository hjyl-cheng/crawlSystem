import assert from "node:assert/strict";
import test from "node:test";
import { Innertube } from "youtubei.js";
import {
  ChannelExecutionMetrics,
  runWithChannelExecution,
} from "../src/channelExecutionContext.js";
import {
  closeYoutubeJs,
  fetchYoutubeJsVideoDetail,
  openYoutubeJsChannel,
  scanYoutubeJsFeed,
  youtubeJsState,
} from "../src/youtubeJs.js";
import { runWithProxyIdentity } from "../src/proxyIdentity.js";

async function settleWithin(promise, milliseconds) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => {
        timer = setTimeout(() => resolve({ deadlineExceeded: true }), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function videoInfo(videoId) {
  return {
    page: [{
      microformat: {
        publish_date: "2026-08-20T00:00:00Z",
        upload_date: "2026-08-20",
        length_seconds: 60,
        view_count: 10,
        channel: { id: "UCcancel" },
      },
    }],
    basic_info: {
      id: videoId,
      channel_id: "UCcancel",
      title: "Cancellation fixture",
      duration: 60,
      view_count: 10,
      like_count: 1,
      is_live: false,
      is_live_content: false,
      is_upcoming: false,
    },
    playability_status: { status: "OK" },
    comments_entry_point_header: { comment_count: "1 comment" },
  };
}

test("fetchYoutubeJsVideoDetail rejects a pre-cancelled operation before getInfo", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const controller = new AbortController();
  const reason = new Error("detail cancelled before entry");
  let getInfoCalls = 0;

  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  delete process.env.YOUTUBE_PROXY_URL;
  controller.abort(reason);
  Innertube.create = async () => ({
    async getInfo(videoId) {
      getInfoCalls += 1;
      return videoInfo(videoId);
    },
  });

  try {
    await assert.rejects(
      fetchYoutubeJsVideoDetail("pre-cancelled-video", { signal: controller.signal }),
      (error) => error === reason,
    );
    assert.equal(getInfoCalls, 0);
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});

test("YouTube.js transport does not record a response completed with external cancellation", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const controller = new AbortController();
  const reason = new Error("detail cancelled as transport completed");
  const proxy = { proxy_id: 16, proxy_address_hash: "same-route" };

  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  delete process.env.YOUTUBE_PROXY_URL;
  Innertube.create = async ({ fetch }) => ({
    async getInfo(videoId) {
      await fetch("https://www.youtube.com/youtubei/v1/player", {
        method: "POST",
        body: JSON.stringify({ videoId }),
      });
      return videoInfo(videoId);
    },
  });

  const metrics = new ChannelExecutionMetrics();
  try {
    await assert.rejects(
      runWithChannelExecution({
        proxy,
        get_proxy_snapshot: () => proxy,
        profile_group: {
          clients: { youtubejs_chrome: { profile_id: "test-chrome-response-race" } },
        },
        fingerprint_gateway: {
          async fetch() {
            controller.abort(reason);
            return new Response(null, { status: 204 });
          },
        },
        metrics,
        abort_signal: null,
      }, () => fetchYoutubeJsVideoDetail("response-race-video", {
        signal: controller.signal,
      })),
      (error) => error === reason,
    );
    assert.equal(youtubeJsState().stats.requests, 1);
    assert.equal(youtubeJsState().stats.failures, 0);
    assert.equal(metrics.snapshot().request_count, 0);
    assert.equal(metrics.snapshot().failure_count, 0);
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});

test("fetchYoutubeJsVideoDetail forwards its explicit cancellation to the real transport", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const controller = new AbortController();
  const reason = new Error("Enrich lease lost");
  const cleanupError = new Error("test transport cleanup");
  const proxy = { proxy_id: 17, proxy_address_hash: "same-route" };
  let notifyTransportStarted;
  let rejectTransport;
  let transportSignal = null;
  const transportStarted = new Promise((resolve) => { notifyTransportStarted = resolve; });

  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
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

  const metrics = new ChannelExecutionMetrics();
  const operation = runWithChannelExecution({
    proxy,
    get_proxy_snapshot: () => proxy,
    profile_group: {
      clients: { youtubejs_chrome: { profile_id: "test-chrome" } },
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
  }, () => fetchYoutubeJsVideoDetail("cancelled-video", { signal: controller.signal }));
  const settled = operation.then(
    (value) => ({ value }),
    (error) => ({ error }),
  );

  try {
    await transportStarted;
    controller.abort(reason);
    const outcome = await settleWithin(settled, 250);
    if (outcome.deadlineExceeded) {
      rejectTransport(cleanupError);
      await settled;
    }

    assert.equal(outcome.deadlineExceeded, undefined, "explicit cancellation did not reach transport");
    assert.equal(outcome.error, reason);
    assert.equal(transportSignal.aborted, true);
    assert.equal(youtubeJsState().stats.requests, 1);
    assert.equal(youtubeJsState().stats.failures, 0);
    assert.equal(metrics.snapshot().failure_count, 0);
    assert.equal(reason.youtube_failure_evidence, undefined);
  } finally {
    rejectTransport?.(cleanupError);
    await settled;
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});

test("fetchYoutubeJsVideoDetail does not turn comment cancellation into a partial result", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const controller = new AbortController();
  const reason = new Error("lease lost during comments");
  const cleanupError = new Error("test transport cleanup");
  const proxy = { proxy_id: 18, proxy_address_hash: "same-route" };
  let notifyTransportStarted;
  let rejectTransport;
  const transportStarted = new Promise((resolve) => { notifyTransportStarted = resolve; });

  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  delete process.env.YOUTUBE_PROXY_URL;
  Innertube.create = async ({ fetch }) => ({
    async getInfo(videoId) {
      return videoInfo(videoId);
    },
    actions: {
      async execute() {
        await fetch("https://www.youtube.com/youtubei/v1/next", {
          method: "POST",
          body: "{}",
        });
        return { success: true, data: {} };
      },
    },
  });

  const metrics = new ChannelExecutionMetrics();
  const operation = runWithChannelExecution({
    proxy,
    get_proxy_snapshot: () => proxy,
    profile_group: {
      clients: { youtubejs_chrome: { profile_id: "test-chrome-comments" } },
    },
    fingerprint_gateway: {
      fetch(_profile, _input, init) {
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
  }, () => fetchYoutubeJsVideoDetail("comment-cancelled-video", { signal: controller.signal }));
  const settled = operation.then(
    (value) => ({ value }),
    (error) => ({ error }),
  );

  try {
    await transportStarted;
    controller.abort(reason);
    const outcome = await settleWithin(settled, 250);
    if (outcome.deadlineExceeded) {
      rejectTransport(cleanupError);
      await settled;
    }

    assert.equal(outcome.deadlineExceeded, undefined, "comment cancellation did not settle promptly");
    assert.equal(outcome.error, reason);
    assert.equal(metrics.snapshot().failure_count, 0);
  } finally {
    rejectTransport?.(cleanupError);
    await settled;
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});

test("scanYoutubeJsFeed does not turn pagination cancellation into a stop reason", async () => {
  const controller = new AbortController();
  const reason = new Error("channel scan cancelled");
  const feed = {
    videos: [{
      id: "first-video",
      title: "First video",
      endpoint: { metadata: { url: "/watch?v=first-video" } },
    }],
    has_continuation: true,
    async getContinuation() {
      controller.abort(reason);
      throw reason;
    },
  };

  await assert.rejects(
    runWithProxyIdentity(
      { abort_signal: controller.signal },
      () => scanYoutubeJsFeed(feed, { maxPages: 2 }),
    ),
    (error) => error === reason,
  );
});

test("openYoutubeJsChannel does not turn About cancellation into observation data", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const controller = new AbortController();
  const reason = new Error("channel About cancelled");
  const cleanupError = new Error("test transport cleanup");
  const proxy = { proxy_id: 19, proxy_address_hash: "same-route" };
  let notifyTransportStarted;
  let rejectTransport;
  const transportStarted = new Promise((resolve) => { notifyTransportStarted = resolve; });

  process.env.YOUTUBEJS_EXTRACTOR_MODE = "channel";
  delete process.env.YOUTUBE_PROXY_URL;
  Innertube.create = async ({ fetch }) => ({
    async getChannel(channelId) {
      return {
        metadata: {
          external_id: channelId,
          title: "Cancellation channel",
          vanity_channel_url: "https://www.youtube.com/@cancel",
          description: "Channel fixture",
        },
        header: {
          subscribers: "1 subscriber",
          content: {
            metadata: {
              metadata_rows: [{ metadata_parts: [
                { text: "@cancel" },
                { text: "1 subscriber" },
                { text: "1 video" },
              ] }],
            },
          },
        },
        subscribe_button: { subscribers: "1 subscriber" },
        has_videos: true,
        has_shorts: false,
        has_live_streams: false,
        async getAbout() {
          await fetch("https://www.youtube.com/youtubei/v1/browse", {
            method: "POST",
            body: "{}",
          });
          return { metadata: {} };
        },
      };
    },
  });

  const metrics = new ChannelExecutionMetrics();
  const operation = runWithProxyIdentity(
    { abort_signal: controller.signal },
    () => runWithChannelExecution({
      proxy,
      get_proxy_snapshot: () => proxy,
      profile_group: {
        clients: { youtubejs_chrome: { profile_id: "test-chrome-about" } },
      },
      fingerprint_gateway: {
        fetch(_profile, _input, init) {
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
      abort_signal: controller.signal,
    }, () => openYoutubeJsChannel("UCcancel", { includeAbout: true })),
  );
  const settled = operation.then(
    (value) => ({ value }),
    (error) => ({ error }),
  );

  try {
    await transportStarted;
    controller.abort(reason);
    const outcome = await settleWithin(settled, 250);
    if (outcome.deadlineExceeded) {
      rejectTransport(cleanupError);
      await settled;
    }

    assert.equal(outcome.deadlineExceeded, undefined, "About cancellation did not settle promptly");
    assert.equal(outcome.error, reason);
    assert.equal(metrics.snapshot().failure_count, 0);
  } finally {
    rejectTransport?.(cleanupError);
    await settled;
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});
