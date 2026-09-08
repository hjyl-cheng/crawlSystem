import assert from "node:assert/strict";
import test from "node:test";

test("successful About without a view count defaults only that metric to zero with explicit provenance", () => {
  const metadata = {
    subscriber_count_text: "17.5K subscribers", subscriber_count_source: "youtube_about",
    video_count_text: "38 videos", video_count_source: "youtube_about",
  };
  const result = normalizeAboutMetrics({ metadata, aboutObserved: true, locale: "en" });
  assert.equal(result.total_view_count, 0);
  assert.equal(result.total_view_count_source, "youtube_about_missing_view_count");
  assert.equal(result.subscriber_count, 17500);
  assert.equal(result.total_video_count, 38);
  assert.equal(result.outcome, "complete");
  assert.equal(normalizeAboutMetrics({ metadata, aboutObserved: false }).total_view_count, null);
  assert.equal(normalizeAboutMetrics({ metadata: {
    ...metadata, view_count_text: "unknown views", view_count_source: "youtube_about",
  }, aboutObserved: true }).total_view_count_status, "unresolved");
  assert.equal(normalizeAboutMetrics({ metadata: {
    ...metadata, total_view_count: -1, view_count_source: "youtube_about",
  }, aboutObserved: true }).total_view_count, null);
});
import { normalizeAboutMetrics } from "../src/aboutMetrics.js";
import { aboutObservationIdempotencyKey } from "../src/aboutObservationStore.js";

function metadata(values = {}) {
  return {
    subscriber_count: 1234,
    subscriber_count_text: "1,234 subscribers",
    subscriber_count_source: "youtube_about",
    view_count_text: "98,765 views",
    view_count_source: "youtube_about",
    video_count_text: "42 videos",
    video_count_source: "youtube_about",
    ...values,
  };
}

test("About normalization produces an exact complete three-metric fact", () => {
  const result = normalizeAboutMetrics({ metadata: metadata(), aboutObserved: true, locale: "en" });

  assert.equal(result.outcome, "complete");
  assert.equal(result.snapshot_eligible, true);
  assert.equal(result.subscriber_count, 1234);
  assert.equal(result.subscriber_count_status, "exact");
  assert.equal(result.total_view_count, 98765);
  assert.equal(result.total_view_count_status, "exact");
  assert.equal(result.total_video_count, 42);
  assert.equal(result.total_video_count_status, "exact");
  assert.equal(
    result.facts_hash,
    "sha256:d876fbf5f26c0280782a349db9f42ac1437a6b23188688297efa587c44221f22",
  );
});

test("compact About values remain explicitly estimated", () => {
  const result = normalizeAboutMetrics({
    metadata: metadata({
      subscriber_count: 1_200_000,
      subscriber_count_text: "1.2M subscribers",
      view_count_text: "2.5B views",
      video_count_text: "1.4K videos",
    }),
    aboutObserved: true,
    locale: "en",
  });

  assert.equal(result.subscriber_count, 1_200_000);
  assert.equal(result.subscriber_count_status, "estimated");
  assert.equal(result.total_view_count, 2_500_000_000);
  assert.equal(result.total_view_count_status, "estimated");
  assert.equal(result.total_video_count, 1400);
  assert.equal(result.total_video_count_status, "estimated");
});

test("a successful partial About defaults missing views without defaulting subscribers", () => {
  const result = normalizeAboutMetrics({
    metadata: metadata({
      subscriber_count: 9999,
      subscriber_count_text: "9.9K subscribers",
      subscriber_count_source: "youtube_channel_header",
      view_count_text: null,
      view_count_source: null,
    }),
    aboutObserved: true,
    locale: "en",
  });

  assert.equal(result.outcome, "partial");
  assert.equal(result.snapshot_eligible, true);
  assert.equal(result.subscriber_count, null);
  assert.equal(result.subscriber_count_status, "unavailable");
  assert.equal(result.total_view_count, 0);
  assert.equal(result.total_view_count_status, "exact");
  assert.equal(result.total_view_count_source, "youtube_about_missing_view_count");
  assert.equal(result.total_video_count, 42);
});

test("a failed getAbout ignores header fallback and does not qualify for a snapshot", () => {
  const result = normalizeAboutMetrics({
    metadata: metadata({ subscriber_count_source: "youtube_channel_header" }),
    aboutObserved: false,
    locale: "en",
  });

  assert.equal(result.outcome, "failed");
  assert.equal(result.outcome_reason_code, "get_about_failed");
  assert.equal(result.snapshot_eligible, false);
  assert.equal(result.resolved_metric_count, 0);
  assert.equal(result.subscriber_count, null);
  assert.equal(result.total_view_count, null);
  assert.equal(result.total_video_count, null);
});

test("a genuine About zero is distinct from a missing value", () => {
  const result = normalizeAboutMetrics({
    metadata: metadata({ subscriber_count: 0, subscriber_count_text: "0 subscribers" }),
    aboutObserved: true,
    locale: "en",
  });

  assert.equal(result.subscriber_count, 0);
  assert.equal(result.subscriber_count_status, "exact");
  assert.equal(result.snapshot_eligible, true);
});

test("an observed but unparseable About value stays unresolved", () => {
  const result = normalizeAboutMetrics({
    metadata: metadata({ view_count_text: "a new unsupported count format" }),
    aboutObserved: true,
    locale: "en",
  });

  assert.equal(result.outcome, "partial");
  assert.equal(result.total_view_count, null);
  assert.equal(result.total_view_count_status, "unresolved");
});

test("About idempotency is stable within an execution attempt and distinct across retries", () => {
  const first = aboutObservationIdempotencyKey({
    runId: "run:UCexample:1",
    executionAttemptId: "channel-attempt:UCexample:1",
  });
  const duplicate = aboutObservationIdempotencyKey({
    runId: "run:UCexample:1",
    executionAttemptId: "channel-attempt:UCexample:1",
  });
  const retry = aboutObservationIdempotencyKey({
    runId: "run:UCexample:1",
    executionAttemptId: "channel-attempt:UCexample:2",
  });

  assert.equal(first, duplicate);
  assert.notEqual(first, retry);
});
