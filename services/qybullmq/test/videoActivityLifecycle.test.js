import assert from "node:assert/strict";
import test from "node:test";
import { applyVideoActivityLifecycle } from "../src/videoActivityLifecycle.js";

function lifecycleFixture(contents, channel = { status: "active", dormant_cycle: 0 }) {
  const queries = [];
  const client = {
    async query(sql, params = []) {
      queries.push({ sql, params });
      if (sql.includes("FROM crawler.channels")) {
        return { rows: [channel] };
      }
      if (sql.includes("FROM crawler.contents")) {
        return { rows: contents };
      }
      return { rows: [], rowCount: 1 };
    },
  };
  return { client, queries };
}

test("Incremental lifecycle keeps relative publication evidence inconclusive", async () => {
  const fixture = lifecycleFixture([{
    source_content_id: "relative-video",
    content_type: "video",
    published_at: "2026-04-01T00:00:00.000Z",
    published_at_status: "relative",
    published_at_precision: "date_only",
    published_at_source: "youtube_uploads_relative_time",
  }]);

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCrelative",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
  });

  assert.equal(result.conclusive, false);
  assert.equal(result.lifecycle_status, "active");
  assert.equal(result.uncertain_content_count, 1);
  assert.equal(
    fixture.queries.some(({ sql }) => sql.includes("SET status='dormant'")),
    false,
  );
});

test("Incremental lifecycle keeps exact recent evidence active", async () => {
  const fixture = lifecycleFixture([{
    source_content_id: "recent-video",
    content_type: "video",
    published_at: "2026-07-01T10:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtubejs_player_microformat",
  }]);

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCrecent",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
  });

  assert.equal(result.lifecycle_status, "active");
  assert.equal(result.recent_published_content_count, 1);
  assert.equal(result.relation_counts.inside, 1);
  assert.equal(result.conclusive, true);
});

test("Incremental lifecycle marks complete exact old evidence dormant", async () => {
  const fixture = lifecycleFixture([{
    source_content_id: "old-video",
    content_type: "video",
    published_at: "2026-04-01T10:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtubejs_player_microformat",
  }]);

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCold",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
  });

  assert.equal(result.lifecycle_status, "dormant");
  assert.equal(result.relation_counts.outside, 1);
  assert.equal(result.conclusive, true);
  assert.equal(
    fixture.queries.some(({ sql }) => sql.includes("SET status='dormant'")),
    true,
  );
});

test("Incremental lifecycle keeps a date-only cutoff overlap inconclusive", async () => {
  const fixture = lifecycleFixture([{
    source_content_id: "cutoff-video",
    content_type: "video",
    published_at: "2026-04-24T00:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "yt_dlp_flat_upload_date",
  }]);

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCcutoff",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
  });

  assert.equal(result.lifecycle_status, "active");
  assert.equal(result.uncertain_content_count, 1);
  assert.equal(result.relation_counts.cutoff_overlap, 1);
  assert.equal(result.conclusive, false);
  assert.equal(
    fixture.queries.some(({ sql }) => sql.includes("SET status='dormant'")),
    false,
  );
});
