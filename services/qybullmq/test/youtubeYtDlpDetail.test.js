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

