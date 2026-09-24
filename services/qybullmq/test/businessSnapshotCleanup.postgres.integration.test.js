import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import pg from 'pg';
import { buildBusinessPublicationProjection } from '../src/businessPublicationProjectionAdapter.js';
import { assertLatestProjectionMode, writeLatestProjectionRows } from '../src/businessLatestProjection.js';
import { pruneSnapshotPayloadBatch } from '../scripts/pruneBusinessSnapshotPayloads.mjs';
import { completeInput } from './support/businessProjectionFixture.js';

const url = process.env.BUSINESS_SNAPSHOT_CLEANUP_TEST_URL;
test('pruning preserves current, fallback, references, trends and receipts; skips busy ownership; is atomic and repeatable', {
  skip: !url, timeout: 120000,
}, async () => {
  const parsed = new URL(url);
  assert.equal(parsed.hostname, '127.0.0.1'); assert.match(parsed.pathname, /_test$/);
  const database = parsed.pathname.slice(1);
  const client = new pg.Client({ connectionString: url });
  const blocker = new pg.Client({ connectionString: url });
  await client.connect(); await blocker.connect();
  const tables = ['channel_snapshots', 'content_snapshots', 'channel_metric_values', 'channel_profile_facts', 'channel_links'];
  try {
    await client.query('BEGIN');
    await client.query(await readFile(new URL('../src/businessLatestStorageSchema.sql', import.meta.url), 'utf8'));
    await client.query("UPDATE publication.creator_search_storage_state SET read_mode='live',write_mode='incremental'");
    const input = completeInput();
    const channelId = input.channelId;
    const streamId = randomUUID();
    await client.query(`INSERT INTO publication.stream(publication_stream_id,source_deployment_key,source_identity_json,
      registered_by,registered_reason,status_changed_by,status_reason) VALUES($1,'cleanup-test','{}','test','test','test','test')`, [streamId]);
    await client.query(`INSERT INTO publication.channel_ownership(channel_id,active_publication_stream_id,projection_mode,
      state_changed_by,state_reason) VALUES($1,$2,'online','test','test')`, [channelId, streamId]);
    await client.query('INSERT INTO public.channels(channel_id) VALUES($1)', [channelId]);
    const legacy = [];
    const seed = async projection => {
      const s = projection.snapshot;
      await client.query(`INSERT INTO public.import_batches(id,source_file,source_sha256,captured_at,raw_payload,status)
        VALUES($1,'cleanup-test',$2,$3,'{}','published')`,
      [s.import_batch_id, createHash('sha256').update(s.id).digest('hex'), s.captured_at]);
      for (const row of projection.contentItems) await client.query(`INSERT INTO public.content_items
        (video_id,channel_id,url,first_seen_at,last_seen_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
      [row.video_id, row.channel_id, row.url, row.first_seen_at, row.last_seen_at]);
    };
    for (let i = 1; i <= 8; i++) {
      input.batchId = `cleanup-legacy-${i}`;
      input.capturedAt = `2026-08-${String(i).padStart(2, '0')}T00:00:00Z`;
      input.current.channel.source_observed_at = input.capturedAt;
      const p = buildBusinessPublicationProjection({ ...input, storageMode: 'snapshots' });
      await seed(p);
      for (const [table, rows] of [['channel_snapshots', [p.snapshot]], ['content_snapshots', p.contents],
        ['channel_metric_values', p.metrics], ['channel_profile_facts', p.facts], ['channel_links', p.links]]) {
        await client.query(`INSERT INTO public.${table} SELECT (jsonb_populate_record(NULL::public.${table},v)).*
          FROM jsonb_array_elements($1::jsonb) v`, [JSON.stringify(rows)]);
      }
      legacy.push(p);
    }
    // A retained Search release must keep its complete referenced payload.
    await client.query('SELECT public.refresh_creator_search_release_v9($1,$2,ARRAY[]::text[])',
      [input.batchId, [channelId]]);
    await client.query(`INSERT INTO public.creator_search_current SELECT * FROM public.creator_search_live`);
    await client.query(`UPDATE public.creator_search_current SET snapshot_id=$1`, [legacy[3].snapshot.id]);
    await client.query(`INSERT INTO publication.projection_snapshot_time_repair
      (snapshot_id,channel_id,snapshot_captured_at,source_observed_at,repair_version)
      VALUES($1,$2,$3,$3,'test')`, [legacy[2].snapshot.id, channelId, legacy[2].snapshot.captured_at]);
    const taxonomy = (await client.query(`INSERT INTO public.creator_taxonomy_versions
      (version_key,status,source_zh_sha256,source_en_sha256,identity_registry_sha256,node_count)
      VALUES('creator-taxonomy-v999','draft',$1,$1,$1,1) RETURNING id`, ['a'.repeat(64)])).rows[0].id;
    const category = (await client.query(`INSERT INTO public.creator_categories(category_key) VALUES('cat_999999') RETURNING id`)).rows[0].id;
    await client.query(`INSERT INTO public.creator_taxonomy_nodes(version_id,category_id,level,sort_order)
      VALUES($1,$2,1,1)`, [taxonomy, category]);
    await client.query(`INSERT INTO public.creator_category_claims(channel_id,channel_snapshot_id,taxonomy_version_id,
      category_id,source_kind,source_system,evidence) VALUES($1,$2,$3,$4,'manual','test','{}')`,
    [channelId, legacy[4].snapshot.id, taxonomy, category]);
    await client.query(`INSERT INTO public.creator_classification_runs(channel_id,channel_snapshot_id,taxonomy_version_id,
      requested_model,prompt_version,cleaner_version,input_hash,input_content_ids,lease_expires_at)
      VALUES($1,$2,$3,'test','test','test',$4,$5,now()+interval '1 hour')`,
    [channelId, legacy[5].snapshot.id, taxonomy, 'a'.repeat(64), legacy[5].contents.map(row => row.id)]);
    await client.query("UPDATE publication.business_storage_state SET mode='latest',activated_at='2026-09-01'");
    await assertLatestProjectionMode(client);
    input.batchId = 'cleanup-latest'; input.capturedAt = '2026-09-02T00:00:00Z';
    input.current.channel.source_observed_at = input.capturedAt;
    const current = buildBusinessPublicationProjection({ ...input, storageMode: 'latest' });
    await seed(current); await writeLatestProjectionRows(client, [current], input.batchId);
    await client.query('SELECT public.refresh_creator_search_release_v9($1,$2,ARRAY[]::text[])', [input.batchId, [channelId]]);
    await client.query(`UPDATE public.channel_metric_values SET baseline_snapshot_id=$1
      WHERE channel_snapshot_id=$2 AND metric_key='content_count'`, [legacy[1].snapshot.id, current.snapshot.id]);
    await client.query('COMMIT');
    const config = { database, channelIds: [channelId] };
    const fingerprint = async () => {
      const result = {};
      for (const table of tables) result[table] = (await client.query(`SELECT md5(string_agg(to_jsonb(row)::text,'' ORDER BY id)) AS hash
        FROM public.${table} row`)).rows[0].hash;
      return result;
    };
    const before = await fingerprint();
    const dry = await pruneSnapshotPayloadBatch(client, config);
    assert.deepEqual(dry.snapshots.map(row => row.id).sort(), [legacy[0], legacy[6]].map(p => p.snapshot.id).sort());
    assert.deepEqual(await fingerprint(), before);
    assert.deepEqual(dry.counts, { content_snapshots: 2, channel_metric_values: 154, channel_profile_facts: 20, channel_links: 2 });
    const rollback = await pruneSnapshotPayloadBatch(client, { ...config, apply: true, rollback: true });
    assert.equal(rollback.outcome, 'rolled_back'); assert.deepEqual(await fingerprint(), before);
    await assert.rejects(pruneSnapshotPayloadBatch(client, { ...config, database: 'wrong_test', apply: true }), /unexpected business/);
    await blocker.query('BEGIN');
    await blocker.query('SELECT channel_id FROM publication.channel_ownership WHERE channel_id=$1 FOR UPDATE', [channelId]);
    const busy = await pruneSnapshotPayloadBatch(client, { ...config, apply: true });
    assert.deepEqual(busy.skipped, [channelId]); assert.equal(busy.snapshots.length, 0);
    await blocker.query('ROLLBACK');
    // Unknown foreign-key dependencies fail closed.
    await client.query('CREATE TABLE public.extra_payload_reference(content_id text REFERENCES public.content_snapshots(id))');
    await assert.rejects(pruneSnapshotPayloadBatch(client, { ...config, apply: true }), /unreviewed snapshot payload/);
    await client.query('DROP TABLE public.extra_payload_reference');
    // A failure after the first DELETE must roll back all payload tables.
    const fail = { query: (sql, args) => {
      if (sql.startsWith('DELETE FROM public.channel_metric_values')) throw new Error('injected delete failure');
      return client.query(sql, args);
    } };
    await assert.rejects(pruneSnapshotPayloadBatch(fail, { ...config, apply: true }), /injected/);
    assert.deepEqual(await fingerprint(), before);
    const done = await pruneSnapshotPayloadBatch(client, { ...config, apply: true });
    assert.deepEqual(done.counts, dry.counts); assert.equal(done.outcome, 'committed');
    assert.equal((await fingerprint()).channel_snapshots, before.channel_snapshots);
    assert.equal((await pruneSnapshotPayloadBatch(client, { ...config, apply: true })).snapshots.length, 0);
    assert.equal(Number((await client.query('SELECT count(*) FROM public.content_snapshots')).rows[0].count), 7);
    // A non-adopted or retracted channel is never eligible.
    await client.query('DELETE FROM publication.latest_projection_state WHERE channel_id=$1', [channelId]);
    assert.equal((await pruneSnapshotPayloadBatch(client, config)).snapshots.length, 0);
  } finally {
    await blocker.query('ROLLBACK').catch(() => {}); await client.query('ROLLBACK').catch(() => {});
    await blocker.end(); await client.end();
  }
});
