import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import {
  agentBaseline,
  aboutBaseline,
  baselineManifest,
  discoveryBaseline,
  recentSamplingBaseline,
  serializeBaselineEvents,
} from "../src/baselineBundle.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";

test("migration About baseline never invents an unresolved count", () => {
  const about = aboutBaseline({
    subscriber_count: "123",
    subscriber_count_status: "unresolved",
    total_view_count: "456",
    total_view_count_status: "exact",
    total_video_count: null,
    total_video_count_status: "unavailable",
  });
  assert.equal(about.payload.subscriber_count, null);
  assert.equal(about.payload.total_view_count, 456);
  assert.equal(about.about.outcome, "partial");
  assert.deepEqual(about.current.identity, {
    title: null,
    handle: null,
    avatar_url: null,
    keywords: [],
    available_tabs: [],
    summary: null,
  });
});

test("Video identity and Recent summaries contain no per-Video metric history", () => {
  const discovery = discoveryBaseline({
    identityCount: 30,
    entries: [{
      video_id: "video-1",
      content_type: "video",
      first_published_at: "2026-07-20T00:00:00Z",
      first_published_at_precision: "second",
    }],
  });
  assert.equal(discovery.outcome, "partial");
  assert.equal(discovery.payload.stop_reason, "max_items");
  assert.deepEqual(Object.keys(discovery.payload.first_seen[0]).sort(), [
    "content_type", "position", "published_at", "published_at_precision", "video_id",
  ]);

  const sampling = recentSamplingBaseline({ recentCount: 10, staleCount: 4 });
  assert.equal(sampling.payload.stale_ratio, 0.4);
  assert.equal(sampling.payload.selected_count, 0);
});

test("Agent baseline is derived only from Current metrics and normalizes an invalid ratio", () => {
  const baseline = agentBaseline({
    status: "success",
    agent_model: "agent-v1",
    metrics_json: {
      audience_profile_agent: {
        channel_categories: { value: { level_1: "Tech", level_2: ["AI", "AI"] } },
        channel_tags: { value: { tags: ["software"] } },
        active_subscriber_ratio: { value: "not-a-ratio" },
      },
    },
  });
  assert.equal(baseline.outcome, "complete");
  assert.equal(baseline.payload.category_level_1, "Tech");
  assert.deepEqual(baseline.payload.category_level_2, ["AI"]);
  assert.equal(baseline.payload.active_subscriber_ratio, null);
  assert.equal(baseline.payload.fulfilled_plan_count, 1);
  assert.equal(baseline.payload.agent_version_hash, null);
  assert.equal(baseline.agent_version_hash, null);
  assert.match(baseline.current_hash, /^sha256:[0-9a-f]{64}$/);
});

test("Agent baseline preserves persisted input and execution version audit hashes", () => {
  const contentIds = ["video-01", "video-02"];
  const inputHash = observationFactsHash(contentIds);
  const versionHash = `sha256:${"d".repeat(64)}`;
  const baseline = agentBaseline({
    metrics_json: {},
    input_content_ids: ["video-02", "video-01", "video-02"],
    input_content_hash: inputHash,
    agent_version_hash: versionHash,
  });

  assert.equal(baseline.payload.input_content_count, 2);
  assert.equal(baseline.payload.input_content_hash, inputHash);
  assert.equal(baseline.payload.agent_version_hash, versionHash);
  assert.equal(baseline.input_content_hash, inputHash);
  assert.equal(baseline.agent_version_hash, versionHash);
});

function event(channelId, sequence = 1) {
  return {
    event_id: randomUUID(),
    observation_id: randomUUID(),
    channel_id: channelId,
    observation_kind: "about",
    kind_sequence: sequence,
    observed_at: "2026-07-20T00:00:00Z",
    outcome: "complete",
  };
}

test("Baseline event serialization sorts rows and enforces contiguous sequences", () => {
  const serialized = serializeBaselineEvents(
    [event("UC-b"), event("UC-a", 2), event("UC-a", 1)],
    { expectedChannelCount: 2, exportedAt: "2026-07-21T00:00:00Z" },
  );
  assert.equal(serialized.eventCount, 3);
  assert.match(serialized.eventsSha256, /^sha256:[0-9a-f]{64}$/);
  assert.equal(JSON.parse(serialized.bytes.toString().split("\n")[0]).channel_id, "UC-a");

  assert.throws(
    () => serializeBaselineEvents(
      [event("UC-a", 2)],
      { expectedChannelCount: 1, exportedAt: "2026-07-21T00:00:00Z" },
    ),
    /contiguous from 1/,
  );
  assert.throws(
    () => serializeBaselineEvents(
      [{ ...event("UC-a"), observed_at: "invalid" }],
      { expectedChannelCount: 1, exportedAt: "2026-07-21T00:00:00Z" },
    ),
    /observed_at must be a timestamp/,
  );
});

test("Baseline serialization uses cross-runtime byte order instead of locale collation", () => {
  const upper = "UC_KL7Oz1j-tf3kOtTRn69GQ";
  const lower = "UC_j5FMpJ28viGJUwyFkwyKw";
  const serialized = serializeBaselineEvents(
    [event(lower), event(upper)],
    { expectedChannelCount: 2, exportedAt: "2026-07-21T00:00:00Z" },
  );
  const channelIds = serialized.bytes.toString().trimEnd().split("\n")
    .map((line) => JSON.parse(line).channel_id);
  assert.deepEqual(channelIds, [upper, lower]);
});

test("Baseline Manifest requires immutable identity, counts, and digest", () => {
  assert.throws(
    () => baselineManifest({
      baselineVersion: "baseline-1",
      sourceDatabase: "crawler-test",
      sourceSnapshotId: "snapshot-1",
      exportedAt: "2026-07-21T00:00:00Z",
      eventsFile: "events.ndjson",
      eventCount: 0,
      channelCount: 1,
      byteCount: 1,
      eventsSha256: "sha256:" + "0".repeat(64),
    }),
    /counts and event digest/,
  );
});
