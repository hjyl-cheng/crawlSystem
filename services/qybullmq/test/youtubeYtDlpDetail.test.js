import assert from "node:assert/strict";
import test from "node:test";

test("yt-dlp detail conversion preserves its normalized first comment page", async () => {
  const youtube = await import("../src/youtube.js");
  assert.equal(typeof youtube.detailFromYtDlpResult, "function");
  const page = {
    version: 1,
    collected_at: "2026-08-18T12:00:00.000Z",
    sort: "TOP_COMMENTS",
    total_count: 9000,
    returned_count: 1,
    comments: [{ comment_id: "Ugw-parent", position: 1, text: "First comment" }],
  };

  const detail = youtube.detailFromYtDlpResult({
    ok: true,
    id: "video-id",
    title: "Video",
    comments_first_page: page,
  }, "https://www.youtube.com/watch?v=video-id");

  assert.deepEqual(detail.comments_first_page, page);
});

test("yt-dlp detail conversion canonicalizes disabled comments to zero", async () => {
  const youtube = await import("../src/youtube.js");
  const detail = youtube.detailFromYtDlpResult({
    ok: true,
    id: "video-id",
    title: "Video",
    comments_disabled: true,
    comment_count: null,
    comment_count_status: "disabled",
    comments_status_source: "yt_dlp_initial_data",
  }, "https://www.youtube.com/watch?v=video-id");

  assert.deepEqual({
    comments_disabled: detail.comments_disabled,
    comment_count: detail.comment_count,
    comment_count_status: detail.comment_count_status,
  }, {
    comments_disabled: true,
    comment_count: 0,
    comment_count_status: "disabled",
  });
});

test("yt-dlp detail conversion preserves unlisted privacy when the video is playable", async () => {
  const youtube = await import("../src/youtube.js");
  const detail = youtube.detailFromYtDlpResult({
    ok: true,
    id: "unlisted-video",
    title: "Unlisted Video",
    availability: "unlisted",
    playability_status: "OK",
  }, "https://www.youtube.com/watch?v=unlisted-video");

  assert.deepEqual({
    access_status: detail.access_status,
    availability: detail.availability,
  }, {
    access_status: "unlisted",
    availability: "unlisted",
  });
});
