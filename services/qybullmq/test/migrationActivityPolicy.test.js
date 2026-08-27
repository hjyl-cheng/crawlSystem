import assert from "node:assert/strict";
import test from "node:test";
import {
  evaluateMigrationActivity,
  evaluateMigrationUploadsActivity,
  migrationActivityCanFinalize,
} from "../src/migrationActivityPolicy.js";

const observedAt = "2026-07-23T12:00:00.000Z";

function exactDate(videoId, publishedAt) {
  return {
    video_id: videoId,
    published_at: publishedAt,
    published_at_status: "exact",
    published_at_precision: publishedAt.includes("T") ? "second" : "date_only",
    published_at_source: publishedAt.includes("T") ? "yt_dlp_flat_timestamp" : "yt_dlp_flat_upload_date",
  };
}

test("an exact date-only cutoff overlap cannot short-circuit migration", () => {
  const result = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [
      exactDate("cutoff", "2026-04-24"),
      exactDate("older", "2026-04-23"),
    ],
    observedAt,
  });
  assert.equal(result.decision, "inconclusive");
  assert.equal(result.dormant, false);
  assert.equal(result.recentPublishedContentCount, 0);
  assert.equal(result.uncertainContentCount, 1);
  assert.equal(result.newestPublishedDay, "2026-04-24");
  assert.equal(result.referenceDay, "2026-07-23");
  assert.equal(result.relationCounts.cutoff_overlap, 1);
  assert.equal(result.relationCounts.outside, 1);
});

test("only resolved old evidence can short-circuit migration as dormant", () => {
  const result = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [exactDate("old", "2026-04-23")],
    observedAt,
  });
  assert.equal(result.decision, "dormant");
  assert.equal(result.dormant, true);
  assert.equal(result.uncertainContentCount, 0);
});

test("an Upload published inside the 90-day UTC window keeps the detail flow", () => {
  const result = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [exactDate("recent", "2026-04-25")],
    observedAt,
  });
  assert.equal(result.decision, "continue");
  assert.equal(result.dormant, false);
  assert.equal(result.recentPublishedContentCount, 1);
});

test("Uploads gaps and unknown publication dates cannot short-circuit details", () => {
  assert.equal(evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: false,
    entries: [exactDate("old", "2025-01-01")],
    observedAt,
  }).decision, "pending");
  assert.equal(evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [{ video_id: "unknown" }],
    observedAt,
  }).decision, "inconclusive");
  const relative = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [{
      video_id: "relative",
      published_at: "2026-04-01",
      published_at_status: "relative",
      published_at_precision: "date_only",
      published_at_source: "youtube_uploads_relative_time",
    }],
    observedAt,
  });
  assert.equal(relative.unresolvedByStatusCounts.relative, 1);
});

test("upcoming Live is excluded but a running Live remains uncertain", () => {
  const dormant = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [
      { video_id: "upcoming", content_type: "live", is_upcoming: true },
      exactDate("old", "2025-01-01"),
    ],
    observedAt,
  });
  assert.equal(dormant.decision, "dormant");
  assert.equal(dormant.excludedUpcomingCount, 1);

  const uncertain = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [{ video_id: "live", content_type: "live", is_live: true }],
    observedAt,
  });
  assert.equal(uncertain.decision, "inconclusive");
});

test("migration activity waits until Video detail coverage is terminal", () => {
  assert.equal(evaluateMigrationActivity({
    required: true,
    detailStatus: "running",
  }).decision, "pending");
});

test("one published item inside the UTC window keeps the Channel", () => {
  assert.deepEqual(evaluateMigrationActivity({
    required: true,
    detailStatus: "done",
    recentPublishedContentCount: 1,
    uncertainContentCount: 2,
    maxAgeDays: 90,
  }), {
    decision: "passed",
    activate: true,
    dormant: false,
    reject: false,
    reason: null,
    recentPublishedContentCount: 1,
    uncertainContentCount: 2,
    maxAgeDays: 90,
  });
});

test("a recent detail passes even when the Uploads scan was incomplete", () => {
  const result = evaluateMigrationActivity({
    required: true,
    detailStatus: "done",
    evidenceComplete: false,
    recentPublishedContentCount: 1,
    uncertainContentCount: 0,
  });
  assert.equal(result.decision, "passed");
  assert.equal(result.activate, true);
  assert.equal(result.dormant, false);
});

test("unknown publication evidence cannot be misclassified as inactivity", () => {
  const result = evaluateMigrationActivity({
    required: true,
    detailStatus: "done",
    recentPublishedContentCount: 0,
    uncertainContentCount: 1,
  });
  assert.equal(result.decision, "inconclusive");
  assert.equal(result.activate, true);
  assert.equal(result.reject, false);
});

test("a fully resolved Run with no recent publication becomes dormant", () => {
  const result = evaluateMigrationActivity({
    required: true,
    detailStatus: "done",
    recentPublishedContentCount: 0,
    uncertainContentCount: 0,
  });
  assert.equal(result.decision, "dormant");
  assert.equal(result.dormant, true);
  assert.equal(result.reject, false);
  assert.equal(result.reason, "no_published_content_within_90_days");
});

test("dormant migration proceeds to Finalize while rejected admission does not", () => {
  assert.equal(migrationActivityCanFinalize({ decision: "dormant" }), true);
  assert.equal(migrationActivityCanFinalize({ decision: "passed" }), true);
  assert.equal(migrationActivityCanFinalize({ decision: "rejected" }), false);
});
