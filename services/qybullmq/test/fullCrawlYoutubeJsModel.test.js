import assert from "node:assert/strict";
import test from "node:test";
import {
  classifyFullCrawlTargetBeforeDetail,
  fullCrawlTargetHash,
  fullCrawlUploadsDocument,
  fullCrawlUploadsHash,
  normalizeFullCrawlTargets,
  validateFullCrawlYoutubeJsDetail,
} from "../src/fullCrawlYoutubeJsModel.js";

function entry(overrides = {}) {
  return {
    video_id: "video-1",
    position: 1,
    url: "https://www.youtube.com/watch?v=video-1",
    title: "Video 1",
    published_at: "2026-09-03T00:00:00.000Z",
    published_at_status: "relative",
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads_relative_time",
    ...overrides,
  };
}

test("Uploads and target hashes are stable across object key order", () => {
  const uploads = {
    playlist_id: "UUexample",
    entries: [entry()],
    activity_evidence_complete: true,
    scan: {
      complete: false,
      terminal_reason: "max_items",
      stop_reason: "max_items",
      pages: 1,
      inspected_count: 30,
      parse_gap_count: 0,
    },
  };
  const document = fullCrawlUploadsDocument(uploads);

  assert.equal(fullCrawlUploadsHash(uploads), fullCrawlUploadsHash(document));
  assert.equal(fullCrawlTargetHash(uploads.entries), fullCrawlTargetHash(document.entries));
});

test("target normalization rejects duplicate identity and position", () => {
  assert.throws(
    () => normalizeFullCrawlTargets([entry(), entry({ position: 2 })]),
    /duplicate video_id/,
  );
  assert.throws(
    () => normalizeFullCrawlTargets([entry(), entry({ video_id: "video-2" })]),
    /duplicate position/,
  );
});

test("strict detail accepts a complete public YouTubeJS observation", () => {
  const resolved = validateFullCrawlYoutubeJsDetail("video-1", {
    id: "video-1",
    title: "Video 1",
    published_at: "2026-09-03T00:00:00.000Z",
    view_count_text: "100",
    duration_seconds: 60,
    access_status: "public",
    access_status_source: "youtubejs_player",
    playability_kind: "content",
    comments_disabled: true,
    youtubejs_comments_error: "optional comments request timed out",
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: "https://www.youtube.com/watch?v=video-1",
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
  });

  assert.equal(resolved.access.access_status, "public");
  assert.equal(resolved.classification.content_type, "video");
  assert.equal(resolved.classification.authoritative, true);
  assert.equal(validateFullCrawlYoutubeJsDetail("video-1", {
    ...resolved.detail, comments_disabled: false,
  }, { optionalComments: true }).detail.youtubejs_comments_error, "optional comments request timed out");
  assert.throws(() => validateFullCrawlYoutubeJsDetail("video-1", {
    ...resolved.detail, comments_disabled: false,
  }), error => error.required_surface === "comments");
});

test("strict detail rejects a public parser gap instead of creating an empty result", () => {
  assert.throws(
    () => validateFullCrawlYoutubeJsDetail("video-1", {
      id: "video-1",
      title: "Video 1",
      access_status: "public",
      playability_kind: "content",
      comments_disabled: true,
    }),
    (error) => error.name === "YoutubeJsRequiredSurfaceError"
      && error.required_surface === "player",
  );
});

test("known old and upcoming targets are terminal before a player request", () => {
  const old = classifyFullCrawlTargetBeforeDetail(entry({
    published_at: "2026-01-01T00:00:00.000Z",
    published_at_status: "exact",
    published_at_source: "youtubejs_player_microformat",
  }), {
    contentMaxAgeDays: 90,
    observedAt: "2026-09-04T00:00:00.000Z",
  });
  const upcoming = classifyFullCrawlTargetBeforeDetail(entry({
    is_upcoming: true,
    live_status: "is_upcoming",
  }), {
    contentMaxAgeDays: 90,
    observedAt: "2026-09-04T00:00:00.000Z",
  });

  assert.equal(old.terminalReason, "outside_content_window");
  assert.equal(upcoming.terminalReason, "upcoming_live");
});
