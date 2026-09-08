import assert from "node:assert/strict";
import test from "node:test";
import { validateFullCrawlYoutubeJsDetail } from "../src/fullCrawlYoutubeJsModel.js";
import { fetchIncrementalYoutubeJsVideoDetail } from "../src/incrementalYoutubeJsVideo.js";

function video(overrides = {}) {
  return {
    id: "shared-video",
    title: "Shared video",
    published_at: "2026-09-01T00:00:00.000Z",
    view_count: 0,
    view_count_text: "0",
    duration_seconds: 60,
    access_status: "public",
    playability_kind: "content",
    like_count: 0,
    like_count_status: "zero_from_empty",
    comment_count: 0,
    comment_count_status: "zero_from_surface",
    comments_disabled: false,
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: "https://www.youtube.com/watch?v=shared-video",
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
    ...overrides,
  };
}

function incremental(detail, options = {}) {
  return fetchIncrementalYoutubeJsVideoDetail(detail.id, {
    fetchYoutubeJs: async () => detail,
    ...options,
  });
}

function surfaceFailure(detail, surface) {
  return (error) => {
    assert.equal(error.name, "YoutubeJsRequiredSurfaceError");
    assert.equal(error.required_surface, surface);
    assert.equal(error.partial_detail, detail);
    return true;
  };
}

test("both pipelines preserve zero evidence and the original detail object", async () => {
  const detail = Object.freeze(video());
  assert.equal(validateFullCrawlYoutubeJsDetail(detail.id, detail).detail, detail);
  assert.equal(await incremental(detail), detail);
  assert.equal(detail.like_count_status, "zero_from_empty");
  assert.equal(detail.comment_count_status, "zero_from_surface");
});

test("both pipelines reject incomplete public metadata with recoverable partial detail", async () => {
  const detail = video({ title: null });
  assert.throws(() => validateFullCrawlYoutubeJsDetail(detail.id, detail), surfaceFailure(detail, "player"));
  await assert.rejects(incremental(detail), surfaceFailure(detail, "player"));
});

test("optional comment rows never waive a failed comments request", async () => {
  const detail = video({
    comment_count: null,
    comment_count_status: "unresolved",
    youtubejs_comments_error: "comments request failed",
  });
  assert.throws(() => validateFullCrawlYoutubeJsDetail(detail.id, detail, {
    optionalComments: true,
  }), surfaceFailure(detail, "comments"));
  assert.throws(() => validateFullCrawlYoutubeJsDetail(detail.id, detail), surfaceFailure(detail, "comments"));
  await assert.rejects(incremental(detail), surfaceFailure(detail, "comments"));
  assert.equal(detail.comment_count, null);
  const observedCount = { ...detail, comment_count: 12, comment_count_status: "exact" };
  assert.throws(() => validateFullCrawlYoutubeJsDetail(detail.id, observedCount, {
    optionalComments: true,
  }), surfaceFailure(observedCount, "comments"));
  const successfulEmpty = { ...observedCount, youtubejs_comments_error: null };
  assert.equal(validateFullCrawlYoutubeJsDetail(detail.id, successfulEmpty, {
    optionalComments: true,
  }).detail, successfulEmpty);
});

test("optional comments never relax the required player fields", () => {
  const detail = video({ title: null });
  assert.throws(() => validateFullCrawlYoutubeJsDetail(detail.id, detail, {
    optionalComments: true,
  }), surfaceFailure(detail, "player"));
});

test("recent metrics accept zero views but require full detail for pending enrichment", async () => {
  const detail = video({ title: null, published_at: null, duration_seconds: null });
  assert.equal(await incremental(detail, { phase: "recent" }), detail);
  await assert.rejects(incremental(detail), surfaceFailure(detail, "player"));
  await assert.rejects(incremental(detail, {
    phase: "recent", target: { enrich_pending: true },
  }), surfaceFailure(detail, "player"));
  for (const value of [null, "", -1]) {
    const missing = { ...detail, view_count: value, view_count_text: value };
    await assert.rejects(incremental(missing, { phase: "recent" }), surfaceFailure(missing, "player"));
  }
});

test("ongoing live exclusion remains specific to Full Crawl", async () => {
  const detail = video({ is_live: true, title: null, published_at: null });
  assert.equal(validateFullCrawlYoutubeJsDetail(detail.id, detail).detail, detail);
  await assert.rejects(incremental(detail), surfaceFailure(detail, "player"));
});

test("both pipelines preserve restricted access without demanding public fields", async () => {
  for (const accessStatus of ["private", "members_only", "unavailable"]) {
    const detail = {
      id: "shared-video", access_status: accessStatus,
      youtubejs_comments_error: "comments unavailable",
    };
    const full = validateFullCrawlYoutubeJsDetail(detail.id, detail);
    assert.equal(full.detail, detail);
    assert.equal(full.access.access_status, accessStatus);
    assert.equal(await incremental(detail), detail);
  }
});

test("Incremental cancellation still wins over detail validation", async () => {
  const controller = new AbortController();
  const cancelled = new Error("cancelled while collecting detail");
  await assert.rejects(incremental(video(), {
    signal: controller.signal,
    fetchYoutubeJs: async () => {
      controller.abort(cancelled);
      return video({ title: null });
    },
  }), (error) => error === cancelled);
});
