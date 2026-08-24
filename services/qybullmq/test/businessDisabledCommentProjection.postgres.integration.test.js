import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";

const { Pool } = pg;
const integrationUrl = process.env.BUSINESS_DISABLED_COMMENTS_POSTGRES_TEST_URL;

async function insertContentSnapshot(client, {
  id,
  channelSnapshotId,
  channelId,
  videoId,
  adapterVersion,
  commentCount,
  commentCountStatus,
  commentCountObservedAt,
  isCanonical = true,
}) {
  return client.query(
    `INSERT INTO public.content_snapshots (
       id,channel_snapshot_id,video_id,content_kind,title,raw_item,
       published_at_status,view_count_status,like_count_status,
       comment_count,comment_count_status,comment_count_observed_at,
       duration_status,channel_id,comments_disabled,description_status,
       source_content_key,is_canonical
     ) VALUES (
       $1,$2,$3,'videos','Disabled comments fixture',
       jsonb_build_object('adapter_version',$4::text),
       'unresolved','unavailable','unavailable',$5,$6,$7,
       'unavailable',$8,true,'unresolved',$1,$9
     )`,
    [
      id,
      channelSnapshotId,
      videoId,
      adapterVersion,
      commentCount,
      commentCountStatus,
      commentCountObservedAt,
      channelId,
      isCanonical,
    ],
  );
}

test("Business accepts legacy disabled/null snapshots but requires v4 disabled/zero", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `disabled-comments-batch-${suffix}`;
  const channelId = `UCdisabledcomments${suffix}`;
  const channelSnapshotId = `disabled-comments-channel-${suffix}`;
  const videoId = `disabled-comments-video-${suffix}`;
  let began = false;

  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    began = true;
    await client.query(
      `INSERT INTO public.import_batches (
         id,source_file,source_sha256,captured_at,raw_payload,source_kind
       ) VALUES ($1,'disabled-comments-test',$2,now(),'{}'::jsonb,'publication_projection')`,
      [batchId, suffix.padEnd(64, "0").slice(0, 64)],
    );
    await client.query(
      "INSERT INTO public.channels (channel_id) VALUES ($1)",
      [channelId],
    );
    await client.query(
      `INSERT INTO public.channel_snapshots (
         id,channel_id,import_batch_id,captured_at,title,raw_channel,channel_observed_at
       ) VALUES ($1,$2,$3,now(),'Disabled comments fixture','{}'::jsonb,now())`,
      [channelSnapshotId, channelId, batchId],
    );
    await client.query(
      "INSERT INTO public.content_items (video_id,channel_id) VALUES ($1,$2)",
      [videoId, channelId],
    );

    await insertContentSnapshot(client, {
      id: `v4-zero-${suffix}`,
      channelSnapshotId,
      channelId,
      videoId,
      adapterVersion: "business-publication-projection-v4",
      commentCount: 0,
      commentCountStatus: "exact",
      commentCountObservedAt: new Date(),
    });
    await insertContentSnapshot(client, {
      id: `legacy-null-${suffix}`,
      channelSnapshotId,
      channelId,
      videoId,
      adapterVersion: "business-publication-projection-v3",
      commentCount: null,
      commentCountStatus: "unavailable",
      commentCountObservedAt: null,
      isCanonical: false,
    });

    await client.query("SAVEPOINT invalid_v4_state");
    await assert.rejects(
      insertContentSnapshot(client, {
        id: `v4-null-${suffix}`,
        channelSnapshotId,
        channelId,
        videoId,
        adapterVersion: "business-publication-projection-v4",
        commentCount: null,
        commentCountStatus: "unavailable",
        commentCountObservedAt: null,
        isCanonical: false,
      }),
      (error) => error?.code === "23514" && error?.constraint === "content_snapshots_access_shape",
    );
    await client.query("ROLLBACK TO SAVEPOINT invalid_v4_state");

    const stored = await client.query(
      `SELECT id,comment_count,comment_count_status,comments_disabled
       FROM public.content_snapshots
       WHERE channel_snapshot_id=$1
       ORDER BY id`,
      [channelSnapshotId],
    );
    assert.deepEqual(stored.rows, [
      {
        id: `legacy-null-${suffix}`,
        comment_count: null,
        comment_count_status: "unavailable",
        comments_disabled: true,
      },
      {
        id: `v4-zero-${suffix}`,
        comment_count: "0",
        comment_count_status: "exact",
        comments_disabled: true,
      },
    ]);
  } finally {
    if (began) await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
