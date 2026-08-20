import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { projectBusinessPublicationChannels } from "../src/businessPublicationProjector.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_BUSINESS_PROJECTION_POSTGRES_TEST_URL;
const channelId = process.env.PUBLICATION_BUSINESS_PROJECTION_TEST_CHANNEL_ID
  || "UClG5ECaZCBQCzCpSqI7hWUA";

test("Projector applies the real public Snapshot and Search path atomically", {
  skip: !integrationUrl,
  timeout: 300000,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const schema = await readFile(
    new URL("../src/businessPublicationProjectionSchema.sql", import.meta.url),
    "utf8",
  );
  const incrementalSchema = await readFile(
    new URL("../src/businessCreatorSearchIncrementalSchema.sql", import.meta.url),
    "utf8",
  );
  const client = await pool.connect();
  let began = false;
  let batchId;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Projection integration URL must target *_test");
    const before = (await client.query(
      `SELECT ownership.projection_mode,
              to_regclass('publication.projection_batch') AS projection_batch,
              (SELECT watermark FROM public.creator_search_active WHERE singleton=true) AS watermark,
              (SELECT count(*)::int FROM publication.projection_outbox
               WHERE channel_id=$1 AND status='held_shadow') AS held_count
       FROM publication.channel_ownership ownership WHERE ownership.channel_id=$1`,
      [channelId],
    )).rows[0];
    assert.ok(["held_shadow", "online"].includes(before.projection_mode));

    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL statement_timeout='300s'");
    await client.query(schema);
    await client.query(incrementalSchema);
    const bumped = await client.query(
      `UPDATE publication.consumer_cursor
       SET active_sequence=active_sequence+2000000000
       WHERE channel_id=$1 AND domain='channel'
       RETURNING active_sequence`,
      [channelId],
    );
    assert.equal(bumped.rows.length, 1, "Projection fixture must have Channel Current");
    const projected = await projectBusinessPublicationChannels(client, [channelId], {
      projectionStatuses: ["held_shadow"],
      markOutbox: false,
      capturedAt: new Date().toISOString(),
    });
    batchId = projected.batch_id;
    assert.equal(projected.outcome, "published");
    assert.equal(projected.projected, 1);
    assert.equal(projected.delivered, 0);
    assert.equal(projected.upsert_channel_ids[0], channelId);
    const state = (await client.query(
      `SELECT
         (SELECT count(*)::int FROM public.creator_search_live
          WHERE channel_id=$2) AS search_count,
         (SELECT watermark=$1 FROM public.creator_search_live
          WHERE channel_id=$2) AS search_watermark_matches,
         (SELECT count(*)::int FROM public.content_snapshots
          WHERE channel_snapshot_id=$3) AS content_count,
         (SELECT count(*)::int FROM public.channel_profile_facts
          WHERE channel_snapshot_id=$3) AS fact_count,
         (SELECT count(*)::int FROM public.channel_metric_values
          WHERE channel_snapshot_id=$3) AS metric_count,
         (SELECT verified_status FROM public.creator_search_live
          WHERE channel_id=$2) AS verified_status,
         (SELECT channel_observed_at FROM public.creator_search_live
          WHERE channel_id=$2) AS search_channel_observed_at,
         (SELECT subscribers_observed_at FROM public.creator_search_live
          WHERE channel_id=$2) AS search_subscribers_observed_at,
         (SELECT youtube_business_email_available FROM public.creator_search_live
          WHERE channel_id=$2) AS search_business_email_available,
         (SELECT youtube_business_email_observed_at FROM public.creator_search_live
          WHERE channel_id=$2) AS search_business_email_observed_at,
         (SELECT snapshot.captured_at
          FROM public.channel_snapshots snapshot
          WHERE snapshot.id=$3) AS snapshot_captured_at,
         (SELECT snapshot.channel_observed_at
          FROM public.channel_snapshots snapshot
          WHERE snapshot.id=$3) AS channel_observed_at,
         (SELECT snapshot.subscriber_count_observed_at
          FROM public.channel_snapshots snapshot
          WHERE snapshot.id=$3) AS subscriber_count_observed_at,
         (SELECT snapshot.total_view_count_observed_at
          FROM public.channel_snapshots snapshot
          WHERE snapshot.id=$3) AS total_view_count_observed_at,
         (SELECT snapshot.video_count_observed_at
          FROM public.channel_snapshots snapshot
          WHERE snapshot.id=$3) AS video_count_observed_at,
         (SELECT snapshot.youtube_business_email_available
          FROM public.channel_snapshots snapshot
          WHERE snapshot.id=$3) AS snapshot_business_email_available,
         (SELECT snapshot.youtube_business_email_observed_at
          FROM public.channel_snapshots snapshot
          WHERE snapshot.id=$3) AS snapshot_business_email_observed_at,
         (SELECT (revision.source_json #>> '{complete_observation,observed_at}')::timestamptz
          FROM result.entity_current current_row
          JOIN publication.revision revision
            ON revision.revision_id=current_row.active_revision_id
          WHERE current_row.channel_id=$2) AS source_observed_at,
         (SELECT count(*)::int FROM publication.projection_outbox
          WHERE channel_id=$2 AND status='held_shadow') AS held_count`,
      [batchId, channelId, projected.projections[0].snapshot_id],
    )).rows[0];
    assert.equal(Number(state.search_count), 1);
    assert.equal(state.search_watermark_matches, true);
    assert.equal(Number(state.content_count), 30);
    assert.equal(Number(state.fact_count), 10);
    assert.equal(Number(state.metric_count), 77);
    assert.equal(state.verified_status, "not_verified");
    assert.ok(state.snapshot_captured_at >= state.source_observed_at);
    assert.equal(state.channel_observed_at.toISOString(), state.source_observed_at.toISOString());
    assert.equal(
      state.subscriber_count_observed_at?.toISOString(),
      state.source_observed_at.toISOString(),
    );
    assert.equal(
      state.total_view_count_observed_at?.toISOString(),
      state.source_observed_at.toISOString(),
    );
    assert.equal(
      state.video_count_observed_at?.toISOString(),
      state.source_observed_at.toISOString(),
    );
    assert.equal(
      state.search_channel_observed_at.toISOString(),
      state.source_observed_at.toISOString(),
    );
    assert.equal(
      state.search_subscribers_observed_at?.toISOString(),
      state.source_observed_at.toISOString(),
    );
    assert.equal(
      state.search_business_email_available,
      state.snapshot_business_email_available,
    );
    assert.equal(
      state.search_business_email_observed_at?.toISOString() ?? null,
      state.snapshot_business_email_observed_at?.toISOString() ?? null,
    );
    assert.equal(Number(state.held_count), Number(before.held_count));
    await client.query("ROLLBACK");
    began = false;

    const after = (await client.query(
      `SELECT
         to_regclass('publication.projection_batch') AS projection_batch,
         (SELECT watermark FROM public.creator_search_active WHERE singleton=true) AS watermark,
         (SELECT projection_mode FROM publication.channel_ownership WHERE channel_id=$1)
           AS projection_mode,
         (SELECT count(*)::int FROM public.import_batches WHERE id=$2) AS batch_count`,
      [channelId, batchId],
    )).rows[0];
    assert.equal(after.projection_batch, before.projection_batch);
    assert.equal(after.watermark, before.watermark);
    assert.equal(after.projection_mode, before.projection_mode);
    assert.equal(Number(after.batch_count), 0);
  } finally {
    if (began) await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("Projector skips a Channel whose current Version Vector is already live", {
  skip: !integrationUrl,
  timeout: 300000,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const [schema, incrementalSchema] = await Promise.all([
    readFile(new URL("../src/businessPublicationProjectionSchema.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/businessCreatorSearchIncrementalSchema.sql", import.meta.url), "utf8"),
  ]);
  const client = await pool.connect();
  let began = false;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Projection integration URL must target *_test");
    const companion = (await client.query(
      `SELECT ownership.channel_id
       FROM publication.channel_ownership ownership
       JOIN publication.consumer_cursor cursor ON cursor.channel_id=ownership.channel_id
       JOIN result.entity_current entity ON entity.channel_id=ownership.channel_id
       JOIN result.video_current video ON video.channel_id=ownership.channel_id
       JOIN result.agent_current agent ON agent.channel_id=ownership.channel_id
       JOIN public.creator_search_live search ON search.channel_id=ownership.channel_id
       WHERE ownership.channel_id<>$1
         AND ownership.active_publication_stream_id=(
           SELECT active_publication_stream_id
           FROM publication.channel_ownership WHERE channel_id=$1
         )
       GROUP BY ownership.channel_id
       HAVING count(DISTINCT cursor.domain)=3
       ORDER BY ownership.channel_id
       LIMIT 1`,
      [channelId],
    )).rows[0]?.channel_id;
    assert.ok(companion, "Projection fixture must provide a second current Channel");

    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL statement_timeout='300s'");
    await client.query(schema);
    await client.query(incrementalSchema);
    await client.query(
      `UPDATE publication.consumer_cursor
       SET active_sequence=active_sequence+3000000000
       WHERE channel_id=$1 AND domain='channel'`,
      [channelId],
    );
    const later = new Date(Date.now() + (2 * 24 * 60 * 60 * 1000)).toISOString();
    const alreadyLive = await projectBusinessPublicationChannels(client, [channelId], {
      projectionStatuses: ["held_shadow"],
      markOutbox: false,
      capturedAt: later,
    });
    assert.equal(alreadyLive.outcome, "published");

    await client.query(
      `UPDATE publication.consumer_cursor
       SET active_sequence=active_sequence+3000000000
       WHERE channel_id=$1 AND domain='channel'`,
      [companion],
    );
    const earlier = new Date(Date.now() + (24 * 60 * 60 * 1000)).toISOString();
    const mixed = await projectBusinessPublicationChannels(client, [channelId, companion], {
      projectionStatuses: ["held_shadow"],
      markOutbox: false,
      capturedAt: earlier,
    });

    assert.equal(mixed.outcome, "published");
    assert.equal(mixed.projected, 1);
    assert.equal(mixed.covered, 1);
    assert.deepEqual(mixed.upsert_channel_ids, [companion]);
    assert.deepEqual(mixed.projections.map((row) => row.channel_id), [companion]);
    await client.query("ROLLBACK");
    began = false;
  } finally {
    if (began) await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
