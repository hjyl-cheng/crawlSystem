import assert from "node:assert/strict";
import test from "node:test";
import {
  incrementalVideoPlannerConfig,
  orderedDiscoveryAnchors,
  planRecentVideoSampling,
} from "../src/incrementalVideoPlanner.js";

function plan(capacity = {}) {
  return {
    planner_config_version: "video-plan-1",
    capacity: { factor: 1, player_cap: 20, next_cap: 8, ...capacity },
  };
}

test("Video Discovery gives Catch-up a separate 50-ID budget", () => {
  const defaultConfig = incrementalVideoPlannerConfig(plan(), {});
  const overriddenConfig = incrementalVideoPlannerConfig(plan(), {
    INCREMENTAL_DISCOVERY_CATCH_UP_MAX_ITEMS: "37",
  });

  assert.equal(defaultConfig.discoveryCatchUpMaxItems, 50);
  assert.equal(overriddenConfig.discoveryCatchUpMaxItems, 37);
  assert.equal("discoveryMaxItems" in defaultConfig, false);
});

test("Video Discovery preserves the database order and requires a publication day", () => {
  const anchors = orderedDiscoveryAnchors([
    { video_id: "V30", published_at: "2026-07-10T22:15:00.000Z" },
    { video_id: "V30", published_at: "2026-07-10T22:15:00.000Z" },
    { video_id: "missing-date", published_at: null },
    { video_id: "V29", published_at: "2026-07-08T01:00:00.000Z" },
  ]);
  assert.deepEqual(anchors, [
    { id: "V30", published_day: "2026-07-10" },
    { id: "V29", published_day: "2026-07-08" },
  ]);
});

test("Recent Sampling excludes Videos initialized by this Discovery", () => {
  const config = incrementalVideoPlannerConfig(plan(), {});
  const rows = [
    { content_key: "c:new", source_content_id: "new", published_at: "2026-07-19T00:00:00Z" },
    { content_key: "c:old-1", source_content_id: "old-1", published_at: "2026-07-18T00:00:00Z" },
    { content_key: "c:old-2", source_content_id: "old-2", published_at: "2026-07-17T00:00:00Z" },
  ];
  const result = planRecentVideoSampling(rows, {
    plan: plan(),
    config,
    excludeVideoIds: ["new"],
    now: new Date("2026-07-20T00:00:00Z"),
  });
  assert.equal(result.recent_count, 3);
  assert.equal(result.rows.some((row) => row.source_content_id === "new"), false);
});

test("Recent Sampling can select zero fresh low-change Videos", () => {
  const config = incrementalVideoPlannerConfig(plan(), {});
  const now = new Date("2026-07-20T00:00:00Z");
  const result = planRecentVideoSampling([{
    content_key: "c:stable",
    source_content_id: "stable",
    published_at: "2026-06-20T00:00:00Z",
    player_last_observed_at: now.toISOString(),
    video_change_probability: 0,
  }], {
    plan: plan(),
    config,
    now,
  });
  assert.equal(result.candidate_count, 0);
  assert.equal(result.player_quota, 0);
  assert.deepEqual(result.rows, []);
});

test("Recent Sampling ranks equal-staleness Videos by learned change probability", () => {
  const config = { ...incrementalVideoPlannerConfig(plan(), {}), minimumRefreshScore: 0 };
  const observedAt = "2026-07-19T00:00:00Z";
  const result = planRecentVideoSampling([
    {
      content_key: "c:stable",
      source_content_id: "stable",
      published_at: "2026-07-10T00:00:00Z",
      player_last_observed_at: observedAt,
      video_change_probability: 0.1,
    },
    {
      content_key: "c:changing",
      source_content_id: "changing",
      published_at: "2026-07-10T00:00:00Z",
      player_last_observed_at: observedAt,
      video_change_probability: 0.9,
    },
  ], {
    plan: plan({ player_cap: 1 }),
    config,
    now: new Date("2026-07-20T00:00:00Z"),
  });
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].source_content_id, "changing");
});

test("Recent Sampling prioritizes queued detail recovery", () => {
  const config = { ...incrementalVideoPlannerConfig(plan(), {}), minimumRefreshScore: 0 };
  const now = new Date("2026-07-20T00:00:00Z");
  const result = planRecentVideoSampling([
    {
      content_key: "c:high-score",
      source_content_id: "high-score",
      published_at: "2026-07-19T00:00:00Z",
      player_last_observed_at: "2026-07-10T00:00:00Z",
      video_change_probability: 1,
      enrich_pending: false,
    },
    {
      content_key: "c:recovery",
      source_content_id: "recovery",
      published_at: "2026-07-19T00:00:00Z",
      player_last_observed_at: now.toISOString(),
      video_change_probability: 0,
      enrich_pending: true,
    },
  ], {
    plan: plan({ player_cap: 1 }),
    config,
    now,
  });

  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0].source_content_id, "recovery");
});

test("Recent Sampling obeys frozen capacity factor and hard caps", () => {
  const config = incrementalVideoPlannerConfig(plan(), {});
  const rows = Array.from({ length: 20 }, (_, index) => ({
    content_key: `c:${index}`,
    source_content_id: `video-${index}`,
    published_at: "2026-07-10T00:00:00Z",
    player_last_observed_at: null,
    next_last_observed_at: null,
  }));
  const result = planRecentVideoSampling(rows, {
    plan: plan({ factor: 0.5, player_cap: 6, next_cap: 2 }),
    config,
    now: new Date("2026-07-20T00:00:00Z"),
  });
  assert.equal(result.player_quota <= 3, true);
  assert.equal(result.next_quota <= 2, true);
});

test("Crawler rejects a frozen Planner version it does not support", () => {
  assert.throws(
    () => incrementalVideoPlannerConfig({
      ...plan(),
      planner_config_version: "video-plan-future",
    }, {}),
    /unsupported Video Planner version/,
  );
});
