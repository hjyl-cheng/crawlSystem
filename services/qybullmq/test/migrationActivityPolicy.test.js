import assert from "node:assert/strict";
import test from "node:test";
import {
  evaluateMigrationActivity,
  evaluateMigrationUploadsActivity,
  migrationActivityCanFinalize,
} from "../src/migrationActivityPolicy.js";

const observedAt = "2026-07-23T12:00:00.000Z";

test("Uploads publication dates short-circuit migration at the 90-day boundary", () => {
  const result = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [
      { video_id: "old", published_at: "2026-04-24" },
      { video_id: "older", published_text: "4 months ago" },
    ],
    observedAt,
    locale: "en",
  });
  assert.equal(result.decision, "dormant");
  assert.equal(result.dormant, true);
  assert.equal(result.recentPublishedContentCount, 0);
  assert.equal(result.uncertainContentCount, 0);
  assert.equal(result.newestPublishedDay, "2026-04-24");
  assert.equal(result.referenceDay, "2026-07-23");
});

test("an Upload published inside the 90-day UTC window keeps the detail flow", () => {
  const result = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [{ video_id: "recent", published_at: "2026-04-25" }],
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
    entries: [{ video_id: "old", published_at: "2025-01-01" }],
    observedAt,
  }).decision, "pending");
  assert.equal(evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [{ video_id: "unknown" }],
    observedAt,
  }).decision, "inconclusive");
});

test("upcoming Live is excluded but a running Live remains uncertain", () => {
  const dormant = evaluateMigrationUploadsActivity({
    required: true,
    evidenceComplete: true,
    entries: [
      { video_id: "upcoming", content_type: "live", is_upcoming: true },
      { video_id: "old", published_at: "2025-01-01" },
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
