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
        const cursor = String(params[1] ?? "");
        const limit = Number(params[2] ?? contents.length);
        return {
          rows: contents
            .filter((row) => String(row.source_content_id) > cursor)
            .sort((left, right) => String(left.source_content_id).localeCompare(String(right.source_content_id)))
            .slice(0, limit),
        };
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

test("Incremental lifecycle keeps a truncated historical evidence scan inconclusive", async () => {
  const fixture = lifecycleFixture([
    {
      content_key: 1,
      source_content_id: "old-video-1",
      content_type: "video",
      published_at: "2026-04-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
    {
      content_key: 2,
      source_content_id: "old-video-2",
      content_type: "video",
      published_at: "2026-03-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
  ]);

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCscanlimit",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
    evidenceScanRowLimit: 1,
    evidenceScanPageSize: 1,
  });

  assert.equal(result.lifecycle_status, "active");
  assert.equal(result.conclusive, false);
  assert.equal(result.evidence_complete, false);
  assert.equal(result.evidence_scan_rows, 1);
  assert.equal(result.evidence_scan_truncated_count, 1);
  assert.equal(result.evidence_scan_truncated_count_is_lower_bound, true);
  assert.equal(result.evidence_scan_stop_reason, "row_limit");
  assert.equal(Number.isFinite(result.evidence_scan_elapsed_ms), true);
  assert.equal(
    fixture.queries.some(({ sql }) => sql.includes("SET status='dormant'")),
    false,
  );
});

test("Incremental lifecycle stops historical evidence pagination at its time budget", async () => {
  const fixture = lifecycleFixture([
    {
      source_content_id: "old-video-1",
      content_type: "video",
      published_at: "2026-04-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
    {
      source_content_id: "old-video-2",
      content_type: "video",
      published_at: "2026-03-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
  ]);
  const clock = [10, 111];

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCscantime",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
    evidenceScanRowLimit: 10,
    evidenceScanPageSize: 1,
    evidenceScanTimeBudgetMs: 100,
    monotonicNow: () => clock.shift() ?? 111,
  });

  assert.equal(result.lifecycle_status, "active");
  assert.equal(result.conclusive, false);
  assert.equal(result.evidence_complete, false);
  assert.equal(result.evidence_scan_rows, 1);
  assert.equal(result.evidence_scan_elapsed_ms, 101);
  assert.equal(result.evidence_scan_truncated_count, 1);
  assert.equal(result.evidence_scan_stop_reason, "time_budget");
});

test("Incremental lifecycle can prove dormancy after a complete paginated scan", async () => {
  const fixture = lifecycleFixture([
    {
      source_content_id: "old-video-1",
      content_type: "video",
      published_at: "2026-04-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
    {
      source_content_id: "old-video-2",
      content_type: "video",
      published_at: "2026-03-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
  ]);

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCscancomplete",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
    evidenceScanRowLimit: 10,
    evidenceScanPageSize: 1,
  });

  assert.equal(result.lifecycle_status, "dormant");
  assert.equal(result.conclusive, true);
  assert.equal(result.evidence_complete, true);
  assert.equal(result.evidence_scan_rows, 2);
  assert.equal(result.evidence_scan_page_count, 2);
  assert.equal(result.evidence_scan_truncated_count, 0);
  assert.equal(result.evidence_scan_stop_reason, "complete");
});

test("current-run recent evidence reactivates a channel when history is truncated", async () => {
  const fixture = lifecycleFixture([
    {
      source_content_id: "old-video-1",
      content_type: "video",
      published_at: "2026-04-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
    {
      source_content_id: "old-video-2",
      content_type: "video",
      published_at: "2026-03-01T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    },
  ], {
    status: "dormant",
    dormant_cycle: 1,
    dormant_since: "2026-06-01T00:00:00.000Z",
  });

  const result = await applyVideoActivityLifecycle(fixture.client, {
    channelId: "UCscanrecent",
    observedAt: "2026-07-23T12:00:00.000Z",
    discoveryComplete: true,
    runActivityEvidence: [{
      source_content_id: "new-video",
      content_type: "video",
      published_at: "2026-07-22T10:00:00.000Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "youtubejs_player_microformat",
    }],
    evidenceScanRowLimit: 1,
    evidenceScanPageSize: 1,
  });

  assert.equal(result.lifecycle_status, "active");
  assert.equal(result.conclusive, true);
  assert.equal(result.evidence_complete, false);
  assert.equal(result.recent_published_content_count, 1);
  assert.equal(result.evidence_scan_stop_reason, "row_limit");
});
