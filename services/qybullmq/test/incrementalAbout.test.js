import assert from "node:assert/strict";
import test from "node:test";
import { executeIncrementalAbout } from "../src/incrementalAbout.js";

function context(snapshot, recordAbout) {
  return {
    plan: {
      job_id: "incremental__UCabout__20260725__clock_7__hash",
      plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
      plan_day: "2026-07-25",
      scheduled_at: "2026-07-25T00:00:00.000Z",
      channel_id: "UCabout",
    },
    runId: "incremental:4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    getChannelSnapshot: async () => snapshot,
    withTransaction: async (action) => action({ query: async () => ({ rows: [] }) }),
    startedAt: "2026-07-25T00:00:00.000Z",
    crawlerVersion: "test",
    recordAbout,
  };
}

function metadata(values = {}) {
  return {
    title: "About Channel",
    handle: "@about-channel",
    avatar_url: "https://example.test/avatar.jpg",
    keywords: ["Build", "Code", "Build"],
    available_tabs: ["videos", "shorts"],
    description: "Current channel summary",
    subscriber_count_text: "1,234 subscribers",
    subscriber_count_source: "youtube_about",
    view_count_text: "98,765 views",
    view_count_source: "youtube_about",
    video_count_text: "42 videos",
    video_count_source: "youtube_about",
    ...values,
  };
}

test("About execution carries identity fields in the same Current command", async () => {
  let command = null;
  const result = await executeIncrementalAbout(context({
    about_requested: true,
    about_observed: true,
    metadata: metadata(),
    raw: { engine: "youtubei.js@test" },
  }, async (_client, value) => {
    command = value;
    return { outcome: value.about.outcome, snapshot_written: true };
  }));

  assert.equal(result.outcome, "complete");
  assert.deepEqual(command.current.identity, {
    title: "About Channel",
    handle: "@about-channel",
    avatar_url: "https://example.test/avatar.jpg",
    keywords: ["Build", "Code"],
    available_tabs: ["shorts", "videos"],
    summary: "Current channel summary",
  });
});

test("a failed getAbout becomes one Partial About with identity Current and no metric snapshot", async () => {
  let command = null;
  const aboutError = new Error("getAbout unavailable");
  const result = await executeIncrementalAbout(context({
    about_requested: true,
    about_observed: false,
    about_error: aboutError,
    metadata: metadata({
      subscriber_count_text: "1.2K subscribers",
      subscriber_count_source: "youtube_channel_header",
      view_count_text: null,
      view_count_source: null,
      video_count_text: null,
      video_count_source: null,
    }),
    raw: {
      engine: "youtubei.js@test",
      request_counts: { get_channel: 1, get_about: 1 },
    },
  }, async (_client, value) => {
    command = value;
    const resolved = [
      value.about.subscriber_count,
      value.about.total_view_count,
      value.about.total_video_count,
    ].filter((item) => item !== null).length;
    return {
      outcome: value.about.outcome,
      snapshot_written: value.about.outcome !== "failed" && resolved > 0,
    };
  }));

  assert.equal(command.about.outcome, "partial");
  assert.equal(command.about.outcome_reason_code, "get_about_failed_identity_current");
  assert.equal(command.about.resolved_metric_count, 0);
  assert.equal(command.current.identity.title, "About Channel");
  assert.equal(command.errorClass, "Error");
  assert.equal(result.snapshot_written, false);
});
