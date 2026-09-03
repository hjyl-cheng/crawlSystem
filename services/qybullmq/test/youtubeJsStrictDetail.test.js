import assert from "node:assert/strict";
import test from "node:test";
import { Innertube } from "youtubei.js";
import { closeYoutubeJs, fetchYoutubeJsVideoDetail } from "../src/youtubeJs.js";
import { shouldReportProxyFailure } from "../src/youtubeFailurePolicy.js";

function publicVideoInfo(videoId) {
  return {
    page: [{
      microformat: {
        publish_date: "2026-09-01T00:00:00Z",
        upload_date: "2026-09-01",
        length_seconds: 60,
        view_count: 10,
      },
    }],
    basic_info: {
      id: videoId,
      title: "Strict comments fixture",
      duration: 60,
      view_count: 10,
      is_live: false,
      is_live_content: false,
      is_upcoming: false,
    },
    playability_status: { status: "OK" },
  };
}

test("strict detail preserves the main response and original comments failure as its cause", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const previousProxy = process.env.YOUTUBE_PROXY_URL;
  const originalCreate = Innertube.create;
  const commentsFailure = new Error("429 Too Many Requests from comments surface");
  commentsFailure.status = 429;
  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  delete process.env.YOUTUBE_PROXY_URL;
  Innertube.create = async () => ({
    getInfo: async (videoId) => publicVideoInfo(videoId),
    actions: {
      execute: async () => {
        throw commentsFailure;
      },
    },
  });

  try {
    const legacy = await fetchYoutubeJsVideoDetail("strict-comments-default");
    assert.match(legacy.youtubejs_comments_error, /429 Too Many Requests/);

    await assert.rejects(
      fetchYoutubeJsVideoDetail("strict-comments-enabled", {
        strictRequiredSurfaces: true,
      }),
      (error) => {
        assert.equal(error.name, "YoutubeJsRequiredSurfaceError");
        assert.equal(error.required_surface, "comments");
        assert.equal(error.cause, commentsFailure);
        assert.equal(error.partial_detail.id, "strict-comments-enabled");
        assert.match(error.partial_detail.youtubejs_comments_error, /429 Too Many Requests/);
        assert.equal(shouldReportProxyFailure({ error }), true);
        return true;
      },
    );
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
    if (previousProxy === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousProxy;
  }
});
