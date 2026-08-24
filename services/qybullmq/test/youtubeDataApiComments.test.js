import assert from "node:assert/strict";
import test from "node:test";
import {
  detailFromDataApiItem,
  fetchVideoCommentThreadsDataApi,
} from "../src/youtube.js";

test("videos.list classifies a public video without comment statistics as disabled zero", () => {
  const detail = detailFromDataApiItem({
    id: "video-id",
    snippet: { title: "Video" },
    statistics: { viewCount: "10" },
    status: { privacyStatus: "public" },
  });

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

test("commentThreads.list returns disabled comments with an authoritative zero", async () => {
  const result = await fetchVideoCommentThreadsDataApi("video-id", "test-key", {
    collectedAt: "2026-08-24T00:00:00.000Z",
    fetchImpl: async () => ({
      ok: false,
      status: 403,
      statusText: "Forbidden",
      async text() {
        return JSON.stringify({
          error: { errors: [{ reason: "commentsDisabled" }] },
        });
      },
    }),
  });

  assert.deepEqual({
    comments_disabled: result.detail.comments_disabled,
    comment_count: result.detail.comment_count,
    comment_count_status: result.detail.comment_count_status,
  }, {
    comments_disabled: true,
    comment_count: 0,
    comment_count_status: "disabled",
  });
});
