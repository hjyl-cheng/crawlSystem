import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { refreshVideoPublicationItemHashes } from "../src/videoPublicationItemStore.js";

const { Pool } = pg;
const integrationUrl = process.env.VIDEO_POSTGRES_TEST_URL;

test("Video Item Hash persists atomically and changes only with business state", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCvideo${suffix}`;
  const contentId = `video${suffix}`;
  const contentKey = `${channelId}:video:${contentId}`;

  async function refresh() {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await refreshVideoPublicationItemHashes(client, [contentKey]);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Video Hash Integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,source_content_id,title,url,thumbnail_url,
         published_at,published_at_status,published_at_source,published_at_precision,
         duration_seconds,duration_status,duration_source,
         view_count,view_count_status,view_count_source,
         like_count,like_count_status,like_count_source,
         comment_count,comment_count_status,comments_disabled,comment_count_source,
         description,description_status,description_source,
         access_status,access_status_source,is_members_only,last_enriched_at
       ) VALUES (
         $1,$2,'video',$3,'Integration Video',$4,$5,
         '2026-07-20T08:00:00Z','exact','youtube_player','second',
         120,'exact','youtube_player',
         1000,'exact','youtube_player',
         100,'exact','youtube_player',
         10,'exact',false,'youtube_next',
         '','empty','youtube_player',
         'public','youtube_player',false,'2026-07-26T10:00:00Z'
       )`,
      [
        contentKey,
        channelId,
        contentId,
        `https://www.youtube.com/watch?v=${contentId}`,
        `https://i.ytimg.com/vi/${contentId}/hqdefault.jpg`,
      ],
    );

    const first = await refresh();
    assert.equal(first.ready_count, 1);
    const firstHash = (await pool.query(
      "SELECT publication_item_hash FROM crawler.contents WHERE content_key=$1",
      [contentKey],
    )).rows[0].publication_item_hash;
    assert.match(firstHash, /^sha256:[0-9a-f]{64}$/);

    await pool.query(
      `UPDATE crawler.contents
       SET view_count_source='youtube_data_api',
           last_enriched_at='2026-07-26T11:00:00Z',
           extractor_version='integration-next'
       WHERE content_key=$1`,
      [contentKey],
    );
    await refresh();
    const auditOnlyHash = (await pool.query(
      "SELECT publication_item_hash FROM crawler.contents WHERE content_key=$1",
      [contentKey],
    )).rows[0].publication_item_hash;
    assert.equal(auditOnlyHash, firstHash);

    await pool.query(
      "UPDATE crawler.contents SET view_count=1001 WHERE content_key=$1",
      [contentKey],
    );
    await refresh();
    const changedHash = (await pool.query(
      "SELECT publication_item_hash FROM crawler.contents WHERE content_key=$1",
      [contentKey],
    )).rows[0].publication_item_hash;
    assert.notEqual(changedHash, firstHash);
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
