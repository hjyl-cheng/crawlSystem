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

test("videos.list only marks an explicitly active broadcast as currently live", () => {
  const current = detailFromDataApiItem({
    id: "current-live",
    snippet: { liveBroadcastContent: "live" },
    liveStreamingDetails: { actualStartTime: "2026-08-25T01:00:00Z" },
  });
  assert.deepEqual({
    is_live: current.is_live,
    was_live: current.was_live,
    live_status: current.live_status,
  }, {
    is_live: true,
    was_live: false,
    live_status: "is_live",
  });

  const replay = detailFromDataApiItem({
    id: "live-replay",
    snippet: { liveBroadcastContent: "none" },
    liveStreamingDetails: {
      actualStartTime: "2026-08-24T01:00:00Z",
      actualEndTime: "2026-08-24T02:00:00Z",
    },
  });
  assert.deepEqual({
    is_live: replay.is_live,
    was_live: replay.was_live,
    live_status: replay.live_status,
  }, {
    is_live: false,
    was_live: true,
    live_status: "was_live",
  });
});

test("videos.list does not infer a current live from a start time without an explicit live flag", () => {
  const ambiguous = detailFromDataApiItem({
    id: "ambiguous-live",
    snippet: { liveBroadcastContent: "none" },
    liveStreamingDetails: { actualStartTime: "2026-08-24T01:00:00Z" },
  });

  assert.equal(ambiguous.is_live, false);
  assert.equal(ambiguous.was_live, undefined);
  assert.equal(ambiguous.live_status, undefined);
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
