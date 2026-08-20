import assert from "node:assert/strict";
import test from "node:test";
import { executeIncrementalAbout } from "../src/incrementalAbout.js";

const PLAN_ID = "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d";

function plan(channelId) {
  return {
    job_id: `incremental__${channelId}__20260720__clock_7__hash`,
    plan_id: PLAN_ID,
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: channelId,
  };
}

function observationFixture() {
  const events = [];
  const client = {
    async query(sql, params = []) {
      if (sql.includes("INSERT INTO crawler.crawl_observation_keys")) {
        return { rowCount: 1, rows: [{ observation_id: params[1] }] };
      }
      if (sql.includes("FROM crawler.channel_domain_cursors") && sql.includes("FOR UPDATE")) {
        return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
      }
      if (sql.includes("INSERT INTO crawler.crawler_outbox")) {
        events.push(JSON.parse(params[7]));
      }
      return { rowCount: 1, rows: [] };
    },
  };
  return {
    events,
    withTransaction: async (action) => action(client),
  };
}

test("About Observation Outbox preserves the originating plan_id", async () => {
  const fixture = observationFixture();
  const value = plan("UCaboutPlanId");

  await executeIncrementalAbout({
    plan: value,
    runId: `incremental:${PLAN_ID}`,
    startedAt: value.scheduled_at,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      about_requested: true,
      about_observed: true,
      metadata: {
        subscriber_count: 1234,
        subscriber_count_text: "1,234 subscribers",
        subscriber_count_source: "youtube_about",
        view_count_text: "98,765 views",
        view_count_source: "youtube_about",
        video_count_text: "42 videos",
        video_count_source: "youtube_about",
      },
      raw: { engine: "youtubei.js@test" },
    }),
  });

  assert.equal(fixture.events.length, 1);
  assert.equal(fixture.events[0].observation_kind, "about");
  assert.equal(fixture.events[0].plan_id, PLAN_ID);
});
