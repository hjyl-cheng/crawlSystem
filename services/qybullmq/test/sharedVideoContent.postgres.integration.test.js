import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { upsertFullVideoContent } from "../src/fullVideoContentStore.js";
import { applyIncrementalVideoDetail } from "../src/incrementalYoutubeJsVideo.js";
import { upsertDiscoveredVideoContent } from "../src/videoContentStore.js";
import { projectVideoDetail } from "../src/videoDetailEvidence.js";

test("Full Crawl content survives Incremental metrics, stale replay, repair and first-seen conflict", {
  skip: !process.env.VIDEO_POSTGRES_TEST_URL,
}, async () => {
  const client = new pg.Client({ connectionString: process.env.VIDEO_POSTGRES_TEST_URL });
  await client.connect();
  try {
    assert.match((await client.query("SELECT current_database() AS name")).rows[0].name, /_test$/);
    await client.query("BEGIN");
    const channelId = `UCshared${randomUUID().replaceAll("-", "")}`;
    const videoId = "shared-video";
    await client.query("INSERT INTO crawler.channels(channel_id,channel_url,title,status) VALUES ($1,$2,'Shared','active')",
      [channelId, `https://www.youtube.com/channel/${channelId}`]);
    const access = { access_status: "public", access_status_source: "youtubejs_player" };
    const detail = {
      id: videoId, title: "Full title", description: "Full description", description_status: "exact",
      description_observed: true, hashtags: ["full"], hashtags_observed: true,
      keywords: ["stored"], keywords_observed: true, duration_seconds: 60,
      published_at: "2026-09-01T10:00:00Z", published_at_status: "exact",
      published_at_precision: "second", published_at_source: "youtubejs_player",
      view_count: 1200, view_count_status: "estimated", view_count_source: "youtubejs_player",
      like_count: 0, like_count_status: "zero_from_empty", like_count_source: "youtubejs_like_count_not_public",
      comment_count: 1, comment_count_status: "exact", comments_disabled: false,
      comments_first_page: { returned_count: 1, comments: [{ text: "Retain this comment" }] },
      content_type_signals: { canonical_url: `https://www.youtube.com/watch?v=${videoId}`, is_shorts_eligible: false },
      ...access,
    };
    const contentKey = await upsertFullVideoContent(client, {
      candidate: { channel_id: channelId, source_content_id: videoId, content_type: "video",
        type_source: "youtube_watch_canonical", type_authoritative: true, position: 1 },
      state: { detail, access }, access,
    });
    const read = async () => (await client.query("SELECT * FROM crawler.contents WHERE content_key=$1", [contentKey])).rows[0];
    const initial = await read();
    assert.equal(initial.view_count_status, "estimated");
    assert.equal(initial.like_count_status, "zero_from_empty");
    const incoming = { ...detail, title: "Incoming title", description: "", description_status: "empty",
      keywords: [], hashtags: [], view_count: 1500, duration_seconds: 99,
      comment_count: 0, comment_count_status: "zero_from_surface",
      comments_first_page: { returned_count: 0, comments: [] } };
    await applyIncrementalVideoDetail(client, {
      row: initial, detail: incoming, observedAt: "2026-09-07T10:00:00Z", allowStaticRepair: false,
    });
    const metrics = await read();
    assert.equal(metrics.title, "Full title");
    assert.equal(metrics.description, "Full description");
    assert.deepEqual(metrics.keywords, ["stored"]);
    assert.equal(metrics.duration_seconds, 60);
    assert.equal(metrics.view_count, "1500");
    assert.equal(metrics.view_count_status, "estimated");
    assert.equal(metrics.comment_count_status, "zero_from_surface");
    assert.equal(metrics.comments_first_page.returned_count, 1);
    await applyIncrementalVideoDetail(client, {
      row: metrics, detail: { ...incoming, view_count: 2, title: "Stale" },
      observedAt: "2026-09-06T10:00:00Z", allowStaticRepair: true,
    });
    assert.deepEqual(await read(), metrics);
    await applyIncrementalVideoDetail(client, {
      row: metrics, detail: incoming, observedAt: "2026-09-07T11:00:00Z", allowStaticRepair: true,
    });
    const repaired = await read();
    assert.equal(repaired.title, "Incoming title");
    assert.equal(repaired.duration_seconds, 99);
    assert.equal(repaired.description, "Full description");
    assert.deepEqual(repaired.keywords, ["stored"]);
    const facts = projectVideoDetail(incoming, { fallbackSource: "youtubejs_player" });
    const conflictedKey = await upsertDiscoveredVideoContent(client, {
      channelId, runId: null, observationId: null, observedAt: "2026-09-07T12:00:00Z",
      entry: { id: videoId, position: 1 }, detail: incoming, facts, publication: facts,
      publicationConflict: null, detailComplete: true,
      classification: { content_type: "short", authoritative: true, source: "youtube_shorts_canonical" },
    });
    assert.equal(conflictedKey, contentKey);
    const corrected = await read();
    assert.equal(corrected.content_type, "short");
    assert.equal(corrected.url, `https://www.youtube.com/shorts/${videoId}`);
    assert.equal(corrected.view_count_status, "estimated");
    assert.equal(corrected.description, "Full description");
    assert.equal(corrected.comments_first_page.returned_count, 1);
    assert.equal((await client.query("SELECT count(*) FROM crawler.contents WHERE channel_id=$1", [channelId])).rows[0].count, "1");
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    await client.end();
  }
});
