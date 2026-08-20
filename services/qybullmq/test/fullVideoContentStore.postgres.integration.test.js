import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { upsertFullVideoContent } from "../src/fullVideoContentStore.js";

const { Pool } = pg;
const integrationUrl = process.env.VIDEO_POSTGRES_TEST_URL;

test("Full Crawl persists numeric Video Current and retains trusted facts on weaker retries", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCfullvideo${suffix}`;
  const contentId = `full${suffix}`;
  const terminalContentId = `terminal${suffix}`;

  async function upsert(sourceContentId, detail, access = {
    is_members_only: false,
    access_status: "public",
    access_status_source: "youtubejs_player",
  }) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const contentKey = await upsertFullVideoContent(client, {
        candidate: {
          channel_id: channelId,
          run_id: null,
          content_type: "video",
          type_source: "youtube_uploads",
          source_content_id: sourceContentId,
          position: 1,
          title: sourceContentId === contentId ? "Full Crawl Video" : "Terminal State Video",
          thumbnail_url: null,
        },
        state: { detail, access },
        access,
        locale: "en",
      });
      await client.query("COMMIT");
      return contentKey;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  const trustedDetail = {
    title: "Full Crawl Video",
    thumbnail_url: `https://i.ytimg.com/vi/${contentId}/hqdefault.jpg`,
    published_text: "2026-07-20",
    published_at: "2026-07-20T08:00:00.000Z",
    published_at_status: "exact",
    published_at_source: "youtubejs_player",
    published_at_precision: "second",
    length_text: "2:00",
    duration_seconds: 120,
    duration_status: "exact",
    duration_source: "youtubejs_player",
    view_count_text: "1,234 views",
    view_count_status: "exact",
    view_count_source: "youtubejs_player",
    like_count: 100,
    like_count_status: "exact",
    like_count_source: "youtubejs_next",
    comment_count: 10,
    comment_count_status: "exact",
    comments_disabled: false,
    comment_count_source: "youtubejs_next",
    description: "Original description",
    description_status: "exact",
    description_source: "youtubejs_player",
    hashtags: [],
    hashtags_observed: true,
    keywords: ["full-crawl"],
    keywords_observed: true,
    extractor_version: "youtubei.js@integration",
  };

  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Full Video Integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );

    const contentKey = await upsert(contentId, trustedDetail);
    const first = (await pool.query(
      `SELECT published_at,published_at_status,published_at_source,published_at_precision,
              duration_seconds,duration_status,duration_source,
              view_count,view_count_text,view_count_status,view_count_source,
              like_count,like_count_status,like_count_source,
              comment_count,comment_count_status,comments_disabled,comment_count_source,
              description,description_status,description_source,
              is_members_only,access_status,access_status_source,
              publication_item_hash
       FROM crawler.contents WHERE content_key=$1`,
      [contentKey],
    )).rows[0];
    assert.equal(first.view_count, "1234");
    assert.equal(first.view_count_status, "exact");
    assert.equal(first.view_count_source, "youtubejs_player");
    assert.match(first.publication_item_hash, /^sha256:[0-9a-f]{64}$/);

    await upsert(contentId, {
      published_at_status: "unavailable",
      published_at_precision: "unknown",
      duration_status: "unavailable",
      view_count_status: "unavailable",
      like_count_status: "unavailable",
      comment_count_status: "unavailable",
      description_status: "unavailable",
      extractor_version: "weaker-retry",
    }, {
      is_members_only: false,
      access_status: "unknown",
      access_status_source: null,
    });
    const retained = (await pool.query(
      `SELECT published_at,published_at_status,published_at_source,published_at_precision,
              duration_seconds,duration_status,duration_source,
              view_count,view_count_text,view_count_status,view_count_source,
              like_count,like_count_status,like_count_source,
              comment_count,comment_count_status,comments_disabled,comment_count_source,
              description,description_status,description_source,
              is_members_only,access_status,access_status_source,
              publication_item_hash
       FROM crawler.contents WHERE content_key=$1`,
      [contentKey],
    )).rows[0];
    assert.deepEqual(retained, first);

    await upsert(contentId, {
      view_count_text: "2.5K views",
      view_count_status: "exact",
      view_count_source: "youtube_uploads",
      extractor_version: "compact-list-retry",
    });
    const compact = (await pool.query(
      `SELECT view_count,view_count_text,view_count_status,view_count_source,
              publication_item_hash
       FROM crawler.contents WHERE content_key=$1`,
      [contentKey],
    )).rows[0];
    assert.deepEqual({
      view_count: compact.view_count,
      view_count_text: compact.view_count_text,
      view_count_status: compact.view_count_status,
      view_count_source: compact.view_count_source,
    }, {
      view_count: "2500",
      view_count_text: "2.5K views",
      view_count_status: "estimated",
      view_count_source: "youtube_uploads",
    });
    assert.notEqual(compact.publication_item_hash, first.publication_item_hash);

    await upsert(contentId, {
      description: "",
      description_status: "empty",
      description_source: "youtubejs_player",
      hashtags: [],
      hashtags_observed: true,
    });
    const emptied = (await pool.query(
      `SELECT description,description_status,description_source,publication_item_hash
       FROM crawler.contents WHERE content_key=$1`,
      [contentKey],
    )).rows[0];
    assert.deepEqual({
      description: emptied.description,
      description_status: emptied.description_status,
      description_source: emptied.description_source,
    }, {
      description: "Original description",
      description_status: "exact",
      description_source: "youtubejs_player",
    });
    assert.equal(emptied.publication_item_hash, compact.publication_item_hash);

    await upsert(terminalContentId, {
      title: "Terminal State Video",
      published_at_status: "unresolved",
      published_at_precision: "unknown",
      duration_status: "unresolved",
      view_count_status: "unresolved",
      like_count_status: "unresolved",
      comment_count_status: "unresolved",
      description_status: "unresolved",
    }, {
      is_members_only: false,
      access_status: "unknown",
      access_status_source: null,
    });
    await upsert(terminalContentId, {
      title: "Terminal State Video",
      published_at_status: "unavailable",
      published_at_precision: "unknown",
      duration_status: "unavailable",
      view_count_status: "unavailable",
      like_count_status: "unavailable",
      comment_count_status: "unavailable",
      description_status: "unavailable",
    }, {
      is_members_only: false,
      access_status: "unavailable",
      access_status_source: "youtube_data_api",
    });
    const terminal = (await pool.query(
      `SELECT published_at_status,duration_status,view_count_status,
              like_count_status,comment_count_status,description_status,
              publication_item_hash
       FROM crawler.contents
       WHERE channel_id=$1 AND source_content_id=$2`,
      [channelId, terminalContentId],
    )).rows[0];
    assert.deepEqual(terminal, {
      published_at_status: "unavailable",
      duration_status: "unavailable",
      view_count_status: "unavailable",
      like_count_status: "unavailable",
      comment_count_status: "unavailable",
      description_status: "unavailable",
      publication_item_hash: null,
    });
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
