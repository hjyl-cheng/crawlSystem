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

test("age-gated empty raw comments retain the disabled semantics in both detail consumers", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const originalCreate = Innertube.create;
  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  let ageRestricted = true;
  let failComments = false;
  Innertube.create = async () => ({
    getInfo: async (videoId) => {
      const info = publicVideoInfo(videoId);
      info.basic_info.is_family_safe = !ageRestricted;
      if (ageRestricted) info.playability_status = {
        status: "LOGIN_REQUIRED", reason: "Sign in to confirm your age",
      };
      return info;
    },
    actions: { execute: async () => {
      if (failComments) throw new Error("comments network failed");
      return { success: true, data: { responseContext: {}, trackingParams: "bare-response" } };
    } },
  });
  try {
    for (const optionalComments of [true, false]) {
      const detail = await fetchYoutubeJsVideoDetail("NGOT1hCseGU", {
        strictRequiredSurfaces: true, optionalComments,
      });
      assert.equal(detail.comments_disabled, true);
      assert.equal(detail.comment_count, 0);
      assert.equal(detail.comment_count_status, "disabled");
      assert.equal(detail.comment_count_source, "youtubejs_comments_age_gate_empty");
    }
    failComments = true;
    const failed = await fetchYoutubeJsVideoDetail("failed-age-gate-comments", { optionalComments: true });
    assert.equal(failed.comments_disabled, null);
    assert.equal(failed.comment_count, null);
    assert.equal(failed.comment_count_status, "unresolved");
    failComments = false;
    ageRestricted = false;
    const ordinary = await fetchYoutubeJsVideoDetail("ordinary-empty-comments", { optionalComments: true });
    assert.equal(ordinary.comments_disabled, null);
    assert.equal(ordinary.comment_count_status, "unresolved");
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
  }
});

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

    await assert.rejects(fetchYoutubeJsVideoDetail("optional-comments-enabled", {
      strictRequiredSurfaces: true,
      optionalComments: true,
    }), error => error.required_surface === "comments" && error.cause === commentsFailure);

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

test("transient comments SSL failure retries only comments and preserves video metrics", async () => {
  const originalCreate = Innertube.create;
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  let infoCalls = 0;
  let commentsCalls = 0;
  let alwaysFail = false;
  const failure = Object.assign(new Error("fingerprint gateway proxy_transport: SSLError curl_code=35"), {
    code: "FINGERPRINT_PROXY_TRANSPORT",
  });
  Innertube.create = async () => ({
    getInfo: async id => {
      infoCalls++;
      const info = publicVideoInfo(id);
      info.basic_info.like_count = 388;
      return info;
    },
    actions: { execute: async () => {
      commentsCalls++;
      if (alwaysFail || commentsCalls === 1) throw failure;
      return { success: true, data: { commentsHeaderRenderer: { countText: { simpleText: "12 Comments" } } } };
    } },
  });
  try {
    const detail = await fetchYoutubeJsVideoDetail("HHQNB1X0U70", { strictRequiredSurfaces: true, optionalComments: true });
    assert.equal(detail.comment_count, 12);
    assert.equal(detail.youtubejs_comments_error, null);
    assert.equal(detail.like_count, 388);
    assert.equal(infoCalls, 1);
    assert.equal(commentsCalls, 2);
    alwaysFail = true;
    commentsCalls = 0;
    await assert.rejects(fetchYoutubeJsVideoDetail("HHQNB1X0U70", { strictRequiredSurfaces: true, optionalComments: true }), error => {
      assert.equal(error.cause, failure);
      assert.equal(error.partial_detail.like_count, 388);
      assert.equal(error.partial_detail.comment_count, null);
      assert.equal(shouldReportProxyFailure({ error }), true);
      return true;
    });
    assert.equal(commentsCalls, 2);
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
  }
});

test("strict full detail distinguishes absent live comments from disabled or failed comments", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const originalCreate = Innertube.create;
  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  let upcoming = false;
  let failComments = false;
  Innertube.create = async () => ({
    getInfo: async (videoId) => {
      const info = publicVideoInfo(videoId);
      info.basic_info.is_upcoming = upcoming;
      info.basic_info.is_live = !upcoming;
      return info;
    },
    actions: { execute: async () => {
      if (failComments) throw new Error("comments network failed");
      return { success: true, data: { responseContext: {}, trackingParams: "bare-response" } };
    } },
  });
  try {
    for (upcoming of [false, true]) {
      const detail = await fetchYoutubeJsVideoDetail("absent-live-comments", {
        strictRequiredSurfaces: true, detailMode: "full",
      });
      assert.equal(detail.comment_count, 0);
      assert.equal(detail.comment_count_status, upcoming ? "zero_from_upcoming" : "zero_from_empty");
      assert.equal(detail.comments_disabled, null);
      assert.equal(detail.comment_count_source, upcoming
        ? "youtubejs_upcoming_comments_not_public" : "youtubejs_live_comments_not_public");
    }
    upcoming = false;
    failComments = true;
    await assert.rejects(fetchYoutubeJsVideoDetail("failed-live-comments", {
      strictRequiredSurfaces: true, detailMode: "full",
    }), (error) => {
      assert.equal(error.required_surface, "comments");
      assert.equal(error.partial_detail.comment_count, null);
      assert.equal(error.partial_detail.comment_count_status, "unresolved");
      return true;
    });
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
  }
});

test("strict full detail recognizes disabled comments from parsed Next messages", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const originalCreate = Innertube.create;
  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  Innertube.create = async () => ({
    getInfo: async (videoId) => {
      const info = publicVideoInfo(videoId);
      info.page.push({ contents_memo: { getType: () => [{ text: "Comments are turned off." }] } });
      return info;
    },
    actions: { execute: async () => ({ success: true, data: {
      responseContext: {}, trackingParams: "bare-response",
    } }) },
  });
  try {
    const detail = await fetchYoutubeJsVideoDetail("disabled-next-message", {
      strictRequiredSurfaces: true, detailMode: "full",
    });
    assert.equal(detail.comments_disabled, true);
    assert.equal(detail.comment_count, 0);
    assert.equal(detail.comment_count_status, "disabled");
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
  }
});

test("optional comments never swallow task cancellation", async () => {
  const previousMode = process.env.YOUTUBEJS_EXTRACTOR_MODE;
  const originalCreate = Innertube.create;
  const controller = new AbortController();
  const cancelled = new Error("task cancelled");
  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  Innertube.create = async () => ({
    getInfo: async id => publicVideoInfo(id),
    actions: { execute: async () => {
      controller.abort(cancelled);
      throw new Error("comments aborted");
    } },
  });
  try {
    await assert.rejects(fetchYoutubeJsVideoDetail("cancelled-comments", {
      optionalComments: true, strictRequiredSurfaces: true, signal: controller.signal,
    }), error => error === cancelled);
  } finally {
    await closeYoutubeJs();
    Innertube.create = originalCreate;
    if (previousMode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE;
    else process.env.YOUTUBEJS_EXTRACTOR_MODE = previousMode;
  }
});
