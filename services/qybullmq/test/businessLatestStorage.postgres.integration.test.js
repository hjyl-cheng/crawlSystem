import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import pg from 'pg';
import { buildBusinessPublicationProjection } from '../src/businessPublicationProjectionAdapter.js';
import { assertLatestProjectionMode, assertLatestVersionOrder, writeLatestProjectionRows } from '../src/businessLatestProjection.js';
import { completeInput } from './support/businessProjectionFixture.js';
import { publicationRuntimeRoleSpecifications } from '../src/publicationRuntimeRoleAdmin.js';
import { manageLatestStorage } from '../scripts/manageBusinessLatestStorage.mjs';

const url = process.env.BUSINESS_LATEST_POSTGRES_TEST_URL;
test('latest storage keeps bounded records, correct Search, history, ordering and atomic rollback', {
  skip: !url, timeout: 120000,
}, async () => {
  const parsed = new URL(url);
  assert.match(parsed.pathname, /_test$/);
  assert.ok(['localhost','127.0.0.1'].includes(parsed.hostname));
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL statement_timeout='30s'");
    assert.equal(Number((await client.query('SELECT count(*) FROM public.channels')).rows[0].count), 0);
    await client.query('CREATE ROLE business_publication_projector NOLOGIN');
    await client.query('GRANT USAGE ON SCHEMA public,publication,result TO business_publication_projector');
    const role = publicationRuntimeRoleSpecifications().business.find(item => item.role === 'business_publication_projector');
    for (const [table, privileges] of Object.entries(role.tables)) {
      await client.query(`GRANT ${privileges.join(',')} ON ${table} TO business_publication_projector`);
    }
    for (const file of ['businessPublicationProjectionSchema.sql','businessCreatorSearchIncrementalSchema.sql',
      'businessLatestStorageSchema.sql','businessLatestStorageSchema.sql']) {
      await client.query(await readFile(new URL(`../src/${file}`,import.meta.url),'utf8'));
    }
    await client.query("UPDATE publication.creator_search_storage_state SET write_mode='incremental',read_mode='live'");
    const input = completeInput(); input.storageMode = 'latest';
    for (const [domain, row] of Object.entries({channel:input.current.channel,video:input.current.video,agent:input.current.agent})) {
      input.versionVector[domain] = {publication_stream_id:row.publication_stream_id,
        sequence:1,revision_id:row.active_revision_id,result_hash:row.result_hash};
    }
    const seed = async (projection, batchId) => {
      await client.query(`INSERT INTO public.import_batches(id,source_file,source_sha256,captured_at,raw_payload,status)
        VALUES ($1,'latest-test',$2,$3,'{}','published')`,
      [batchId,createHash('sha256').update(batchId).digest('hex'),projection.snapshot.captured_at]);
      await client.query('INSERT INTO public.channels(channel_id) VALUES ($1) ON CONFLICT DO NOTHING',[input.channelId]);
      for (const row of projection.contentItems) await client.query(`INSERT INTO public.content_items
        (video_id,channel_id,url,first_seen_at,last_seen_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
      [row.video_id,row.channel_id,row.url,row.first_seen_at,row.last_seen_at]);
    };
    // Installing the additive schema leaves the old publisher usable.
    const legacyInput = {...input,storageMode:'snapshots',batchId:'legacy-baseline'};
    const legacy = buildBusinessPublicationProjection(legacyInput);
    await seed(legacy,legacyInput.batchId);
    for (const [table,rows] of [['channel_snapshots',[legacy.snapshot]],['content_snapshots',legacy.contents],
      ['channel_links',legacy.links],['channel_profile_facts',legacy.facts],['channel_metric_values',legacy.metrics]]) {
      await client.query(`INSERT INTO public.${table} SELECT (jsonb_populate_record(NULL::public.${table},value)).*
        FROM jsonb_array_elements($1::jsonb) value`,[JSON.stringify(rows)]);
    }
    await client.query('SELECT public.refresh_creator_search_release_v9($1,$2,ARRAY[]::text[])',
      [legacyInput.batchId,[input.channelId]]);
    const legacyDocument = (await client.query('SELECT to_jsonb(s) AS doc FROM public.channel_snapshots s WHERE id=$1',[legacy.snapshot.id])).rows[0].doc;
    await client.query('DROP TABLE IF EXISTS pg_temp.creator_search_publish_before');
    await client.query("UPDATE publication.business_storage_state SET mode='latest'");
    const publish = async (number) => {
      input.batchId = `latest-test-${number}`;
      input.capturedAt = new Date(Date.parse('2026-07-31T00:00:00Z')+number*86400000).toISOString();
      input.current.channel.source_observed_at = input.capturedAt;
      input.versionVector.channel.sequence = number+1;
      input.versionVector.channel.revision_id = `20000000-0000-4000-8000-${String(number).padStart(12,'0')}`;
      await assertLatestProjectionMode(client);
      await assertLatestVersionOrder(client,{[input.channelId]:input.versionVector});
      const projection = buildBusinessPublicationProjection(input);
      await seed(projection,input.batchId);
      await client.query('SET LOCAL ROLE business_publication_projector');
      await writeLatestProjectionRows(client,[projection],input.batchId);
      await client.query('SELECT public.refresh_creator_search_release_v9($1,$2,ARRAY[]::text[])',
        [input.batchId,[input.channelId]]);
      await client.query('RESET ROLE');
      return projection;
    };
    const first = await publish(1);
    const firstFacts = (await client.query('SELECT ctid::text AS version FROM public.channel_profile_facts WHERE channel_snapshot_id=$1 ORDER BY id',[first.snapshot.id])).rows;
    for (let i=2;i<=6;i++) { input.current.channel.payload_json.subscriber_count+=10; await publish(i); }
    const counts = (await client.query(`SELECT
      (SELECT count(*)::int FROM public.channel_snapshots WHERE id=$1) AS channels,
      (SELECT count(*)::int FROM public.content_snapshots WHERE channel_snapshot_id=$1) AS videos,
      (SELECT count(*)::int FROM public.channel_profile_facts WHERE channel_snapshot_id=$1) AS facts,
      (SELECT count(*)::int FROM public.channel_metric_values WHERE channel_snapshot_id=$1) AS metrics`,[first.snapshot.id])).rows[0];
    assert.deepEqual(counts,{channels:1,videos:1,facts:10,metrics:77});
    assert.equal((await client.query('SELECT subscribers FROM public.creator_search_live WHERE channel_id=$1',[input.channelId])).rows[0].subscribers,'150');
    assert.deepEqual((await client.query('SELECT to_jsonb(s) AS doc FROM public.channel_snapshots s WHERE id=$1',[legacy.snapshot.id])).rows[0].doc,legacyDocument);
    assert.deepEqual((await client.query('SELECT ctid::text AS version FROM public.channel_profile_facts WHERE channel_snapshot_id=$1 ORDER BY id',[first.snapshot.id])).rows,firstFacts);
    assert.equal((await client.query('SELECT * FROM public.creator_channel_metric_history_v1($1,$2,100)',[input.channelId,input.capturedAt])).rows.length,7);
    await client.query(`INSERT INTO publication.channel_metric_history
      (channel_id,observed_at,subscriber_count,subscriber_count_status,total_view_count,total_view_count_status)
      SELECT $1,'2020-01-01'::timestamptz+n*interval '1 day',n,'exact',n,'exact'
      FROM generate_series(1,110) n`,[input.channelId]);
    // Window change keeps historical video identity/data but stops showing it as recent.
    input.current.contents=[];
    await publish(7);
    assert.equal((await client.query('SELECT count(*)::int AS n FROM publication.channel_metric_history WHERE channel_id=$1',[input.channelId])).rows[0].n,100);
    const exited=(await client.query('SELECT is_recent,view_count FROM public.content_snapshots WHERE channel_snapshot_id=$1',[first.snapshot.id])).rows[0];
    assert.equal(exited.is_recent,false); assert.equal(exited.view_count,'100');
    await client.query('SAVEPOINT atomic_publication');
    input.current.channel.payload_json.subscriber_count=999;
    await publish(8);
    await client.query('ROLLBACK TO SAVEPOINT atomic_publication');
    assert.equal((await client.query('SELECT subscribers FROM public.creator_search_live WHERE channel_id=$1',[input.channelId])).rows[0].subscribers,'150');
    const stale=structuredClone(input.versionVector);stale.channel.sequence=1;
    await assert.rejects(assertLatestVersionOrder(client,{[input.channelId]:stale}),/BUSINESS_LATEST_VERSION_CONFLICT/);
    await client.query('SAVEPOINT replay_guard');
    await assert.rejects(client.query("SELECT public.rollback_creator_search_release_v9($1,'test','test')",[input.batchId]),/BUSINESS_LATEST_HISTORICAL_REPLAY_DISABLED/);
    await client.query('ROLLBACK TO SAVEPOINT replay_guard');
    await client.query("SELECT set_config('publication.business_writer_mode','',true)");
    await client.query('SAVEPOINT old_writer');
    await assert.rejects(client.query('UPDATE public.channel_snapshots SET title=title WHERE false'),/BUSINESS_LATEST_WRITER_REQUIRED/);
    await client.query('ROLLBACK TO SAVEPOINT old_writer');
  } finally { await client.query('ROLLBACK').catch(()=>{});await client.end(); }
});

test('storage cutover validates database, watermark, Search mode and publisher permissions', {
  skip: !url, timeout: 120000,
}, async () => {
  const parsed = new URL(url);
  assert.match(parsed.pathname, /_test$/);
  assert.ok(['localhost', '127.0.0.1'].includes(parsed.hostname));
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    await client.query('BEGIN');
    const config = {
      action: 'inspect', database: parsed.pathname.slice(1),
      actor: 'test', reason: 'latest storage acceptance',
    };
    // Keep real administrative SQL rollback-only, including failed transactions.
    const isolated = { query: (sql, values) => client.query(
      sql === 'BEGIN' || sql === 'BEGIN READ ONLY' ? 'SAVEPOINT storage_admin'
        : sql === 'COMMIT' ? 'RELEASE SAVEPOINT storage_admin'
          : sql === 'ROLLBACK' ? 'ROLLBACK TO SAVEPOINT storage_admin' : sql,
      values,
    ) };
    assert.equal((await manageLatestStorage(isolated, config)).installed, false);
    await assert.rejects(manageLatestStorage(isolated, { ...config, database: 'wrong_test' }),
      /unexpected or uninitialized business database/);
    await client.query('CREATE ROLE business_publication_projector NOLOGIN');
    await client.query('CREATE ROLE business_latest_test_reader NOLOGIN');
    await client.query('GRANT SELECT ON public.channel_snapshots TO business_latest_test_reader');
    const install = { ...config, action: 'install' };
    assert.equal((await manageLatestStorage(isolated, install)).state.mode, 'snapshots');
    assert.equal((await manageLatestStorage(isolated, install)).state.mode, 'snapshots');
    await client.query('SET LOCAL ROLE business_latest_test_reader');
    assert.equal((await client.query(
      "SELECT * FROM public.creator_channel_metric_history_v1('absent',now(),100)",
    )).rowCount, 0);
    await client.query('RESET ROLE');
    config.watermark = (await client.query('SELECT watermark FROM public.creator_search_active')).rows[0].watermark;
    const enable = { ...config, action: 'enable' };
    await assert.rejects(manageLatestStorage(isolated, enable), /Search is not incremental\/live/);
    await client.query("UPDATE publication.creator_search_storage_state SET read_mode='live',write_mode='incremental'");
    await assert.rejects(manageLatestStorage(isolated, { ...enable, watermark: 'stale' }),
      /business publication changed/);
    // Missing any one required privilege must reject the entire cutover.
    await client.query('REVOKE UPDATE ON public.content_snapshots FROM business_publication_projector');
    await assert.rejects(manageLatestStorage(isolated, enable), /publisher permissions/);
    assert.equal((await manageLatestStorage(isolated, config)).state.mode, 'snapshots');
    await client.query('GRANT UPDATE ON public.content_snapshots TO business_publication_projector');
    const enabled = await manageLatestStorage(isolated, enable);
    assert.equal(enabled.state.mode, 'latest');
    assert.equal(enabled.state.actor, 'test');
    // Reinstallation preserves an already activated mode.
    assert.equal((await manageLatestStorage(isolated, install)).state.mode, 'latest');
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    await client.end();
  }
});

test('batched latest writes preserve channel boundaries and unchanged video rows', {
  skip: !url, timeout: 120000,
}, async () => {
  const parsed = new URL(url);
  assert.match(parsed.pathname, /_test$/);
  assert.ok(['localhost', '127.0.0.1'].includes(parsed.hostname));
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    await client.query('BEGIN');
    await client.query(await readFile(new URL('../src/businessLatestStorageSchema.sql', import.meta.url), 'utf8'));
    await client.query("UPDATE publication.business_storage_state SET mode='latest'");
    await assertLatestProjectionMode(client);
    const firstInput = { ...completeInput(), storageMode: 'latest', batchId: 'bulk-first' };
    const secondInput = JSON.parse(JSON.stringify(firstInput)
      .replaceAll(firstInput.channelId, 'UCsecondStorageChannel')
      .replaceAll('video-fixture-1', 'video-fixture-2'));
    const initial = [firstInput, secondInput].map(buildBusinessPublicationProjection);
    for (const batchId of ['bulk-first', 'bulk-next']) {
      await client.query(`INSERT INTO public.import_batches(id,source_file,source_sha256,captured_at,raw_payload,status)
        VALUES ($1,'test',$2,now(),'{}','published')`,
      [batchId, createHash('sha256').update(batchId).digest('hex')]);
    }
    for (const projection of initial) {
      await client.query('INSERT INTO public.channels(channel_id) VALUES ($1)', [projection.channelId]);
      for (const content of projection.contentItems) {
        await client.query(`INSERT INTO public.content_items(video_id,channel_id,url,first_seen_at,last_seen_at)
          VALUES ($1,$2,$3,$4,$5)`,
        [content.video_id, content.channel_id, content.url, content.first_seen_at, content.last_seen_at]);
      }
    }
    await writeLatestProjectionRows(client, initial, 'bulk-first');
    const unchangedVideo = (await client.query(
      'SELECT ctid::text AS version,to_jsonb(content) AS data FROM public.content_snapshots content WHERE id=$1',
      [initial[1].contents[0].id],
    )).rows[0];
    firstInput.batchId = secondInput.batchId = 'bulk-next';
    firstInput.current.contents = [];
    firstInput.current.channel.payload_json.links = [];
    firstInput.current.agent.is_retracted = true;
    secondInput.current.channel.payload_json.subscriber_count = 456;
    await writeLatestProjectionRows(client, [firstInput, secondInput].map(buildBusinessPublicationProjection), 'bulk-next');
    assert.deepEqual((await client.query(
      'SELECT ctid::text AS version,to_jsonb(content) AS data FROM public.content_snapshots content WHERE id=$1',
      [initial[1].contents[0].id],
    )).rows[0], unchangedVideo);
    assert.equal((await client.query('SELECT is_recent FROM public.content_snapshots WHERE id=$1',
      [initial[0].contents[0].id])).rows[0].is_recent, false);
    assert.deepEqual((await client.query(`SELECT channel_id,count(*)::int AS count FROM public.channel_links
      GROUP BY channel_id ORDER BY channel_id`)).rows, [{ channel_id: secondInput.channelId, count: 1 }]);
    assert.deepEqual((await client.query(`SELECT channel_id,count(*)::int AS count FROM public.channel_profile_facts
      GROUP BY channel_id ORDER BY channel_id`)).rows, [{ channel_id: secondInput.channelId, count: 10 }]);
    assert.equal((await client.query('SELECT subscriber_count FROM public.channel_snapshots WHERE channel_id=$1',
      [secondInput.channelId])).rows[0].subscriber_count, '456');
    assert.equal((await client.query('SELECT count(*)::int AS count FROM publication.latest_projection_state')).rows[0].count, 2);
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    await client.end();
  }
});
