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

test("yt-dlp detail conversion emits an exact publication evidence quartet", async () => {
  const youtube = await import("../src/youtube.js");
  const detail = youtube.detailFromYtDlpResult({
    ok: true,
    id: "video-id",
    published_at: "2026-05-28T17:20:53Z",
    published_at_precision: "second",
    published_at_source: "yt_dlp_timestamp",
  });

  assert.deepEqual({
    published_at: detail.published_at,
    published_at_status: detail.published_at_status,
    published_at_precision: detail.published_at_precision,
    published_at_source: detail.published_at_source,
  }, {
    published_at: "2026-05-28T17:20:53.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "yt_dlp_timestamp",
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

test("yt-dlp detail conversion preserves an explicit age restriction over generic auth", async () => {
  const youtube = await import("../src/youtube.js");
  const detail = youtube.detailFromYtDlpResult({
    ok: true,
    id: "oy7l_ybAng8",
    title: "Age Restricted Video",
    availability: "needs_auth",
    playability_status: "LOGIN_REQUIRED",
    playability_reason: "Sign in to confirm your age",
  }, "https://www.youtube.com/watch?v=oy7l_ybAng8");

  assert.deepEqual({
    access_status: detail.access_status,
    availability: detail.availability,
    playability_reason_code: detail.playability_reason_code,
  }, {
    access_status: "public",
    availability: "age_restricted",
    playability_reason_code: "age_restricted",
  });
});

test("yt-dlp detail conversion keeps non-age access restrictions unchanged", async () => {
  const youtube = await import("../src/youtube.js");
  const fixtures = [
    { availability: "needs_auth", reason: "Sign in to continue", expected: "login_required" },
    { availability: "private", reason: "This is a private video", expected: "private" },
    { availability: "subscriber_only", reason: "Join this channel to watch", expected: "members_only" },
  ];
  for (const fixture of fixtures) {
    const detail = youtube.detailFromYtDlpResult({
      ok: true,
      id: `access-${fixture.expected}`,
      availability: fixture.availability,
      playability_status: "LOGIN_REQUIRED",
      playability_reason: fixture.reason,
    });
    assert.equal(detail.access_status, fixture.expected, fixture.availability);
  }
});

test("yt-dlp flat Uploads preserve timestamp and upload-date evidence", async () => {
  const youtube = await import("../src/youtube.js");
  const timestamp = youtube.channelUploadEntryFromYtDlpResult({
    id: "timestamp-video",
    timestamp: 1779988853,
  }, 0);
  const uploadDate = youtube.channelUploadEntryFromYtDlpResult({
    id: "date-video",
    upload_date: "20260528",
  }, 1);
  const missing = youtube.channelUploadEntryFromYtDlpResult({ id: "missing-video" }, 2);

  assert.deepEqual({
    status: timestamp.published_at_status,
    precision: timestamp.published_at_precision,
    source: timestamp.published_at_source,
  }, {
    status: "exact",
    precision: "second",
    source: "yt_dlp_flat_timestamp",
  });
  assert.deepEqual({
    published_at: uploadDate.published_at,
    status: uploadDate.published_at_status,
    precision: uploadDate.published_at_precision,
    source: uploadDate.published_at_source,
  }, {
    published_at: "2026-05-28T00:00:00.000Z",
    status: "exact",
    precision: "date_only",
    source: "yt_dlp_flat_upload_date",
  });
  assert.deepEqual({
    published_at: missing.published_at,
    status: missing.published_at_status,
    precision: missing.published_at_precision,
    source: missing.published_at_source,
  }, {
    published_at: null,
    status: "unresolved",
    precision: "unknown",
    source: null,
  });
});
