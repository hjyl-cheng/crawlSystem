import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { claimChannelScan, scanTransaction, finishChannelScan, releaseChannelScan } from '../src/backgroundReconciliationScan.js';
import { loadAutomaticPublicationCandidates, reconcileAutomaticPublicationBacklog } from '../src/publicationChannelOnboarding.js';
import { reconcileTerminalFinalizedRunPage } from '../src/terminalRunReconciliation.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';

const url = process.env.BACKGROUND_RECONCILIATION_TEST_URL;
test('durable channel pages survive restart, reject stale owners, and revisit changes behind the cursor', { skip: !url }, async () => {
  assert.equal(new URL(url).pathname, '/background_reconciliation_test');
  const pool = new pg.Pool({ connectionString: url, max: 3 });
  const query = pool.query.bind(pool);
  const withTransaction = async action => {
    const c = await pool.connect();
    try { await c.query('BEGIN'); const result = await action(c); await c.query('COMMIT'); return result; }
    catch (error) { await c.query('ROLLBACK'); throw error; } finally { c.release(); }
  };
  try {
    await query('DROP SCHEMA IF EXISTS crawler CASCADE; DROP SCHEMA IF EXISTS publication CASCADE; CREATE SCHEMA crawler');
    await query('CREATE TABLE IF NOT EXISTS crawler.channels(channel_id text PRIMARY KEY)');
    await query(await readFile(new URL('../src/backgroundReconciliationSchema.sql', import.meta.url), 'utf8'));
    await query('TRUNCATE crawler.background_reconciliation_scans,crawler.channels CASCADE');
    await query("INSERT INTO crawler.channels(channel_id) SELECT 'UC'||lpad(n::text,4,'0') FROM generate_series(1,7)n");
    const options = { withTransaction, scope: 'test', pageSize: 3, roundPauseMs: 0 };
    const first = await claimChannelScan(options);
    assert.deepEqual(first.ids, ['UC0001', 'UC0002', 'UC0003']);
    assert.equal(await claimChannelScan(options), null, 'another process cannot own the same page');
    await assert.rejects(scanTransaction(options, first, async client => {
      await client.query("DELETE FROM crawler.channels WHERE channel_id='UC0001'");
      throw new Error('injected failure');
    }, 'UC0001'), /injected failure/);
    assert.equal((await query("SELECT after_channel_id FROM crawler.background_reconciliation_scans WHERE scope='test'")).rows[0].after_channel_id, '');
    assert.equal((await query("SELECT 1 FROM crawler.channels WHERE channel_id='UC0001'")).rowCount, 1);
    await scanTransaction(options, first, async () => {}, 'UC0001');
    await releaseChannelScan(options, first);
    const resumed = await claimChannelScan(options);
    assert.deepEqual(resumed.ids, ['UC0002', 'UC0003', 'UC0004']);
    await assert.rejects(scanTransaction(options, first, async () => {}, 'UC0003'), /SCAN_LEASE_LOST/);
    await finishChannelScan(options, resumed, 'UC0004');
    await query("INSERT INTO crawler.channels(channel_id) VALUES('UC0000'),('UC9999')");
    const end = await claimChannelScan(options);
    assert.deepEqual(end.ids, ['UC0005', 'UC0006', 'UC0007']);
    assert.equal((await finishChannelScan(options, end, 'UC0007')).wrapped, true);
    const nextRound = await claimChannelScan(options);
    assert.deepEqual(nextRound.ids, ['UC0000', 'UC0001', 'UC0002']);
    assert.equal(nextRound.upper_channel_id, 'UC9999');
    await releaseChannelScan(options, nextRound);
    const leased = await claimChannelScan(options);
    await query("UPDATE crawler.background_reconciliation_scans SET lease_until=now()-interval '1 second' WHERE scope='test'");
    const recovered = await claimChannelScan(options);
    assert.notEqual(recovered.lease_token, leased.lease_token);
    await assert.rejects(scanTransaction(options, leased, async () => {}, 'UC0002'), /SCAN_LEASE_LOST/);
    await releaseChannelScan(options, recovered);
  } finally { await pool.end(); }
});

test('real publication pages retain overflow and failures; terminal audits update only current eligible Runs', { skip: !url }, async t => {
  assert.equal(new URL(url).pathname, '/background_reconciliation_test');
  const pool = new pg.Pool({ connectionString: url, max: 3, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
  let fixtureClient = null;
  const query = (sql, args) => (fixtureClient ?? pool).query(sql, args);
  let failChannel = null;
  const attemptedChannels = new Set();
  const withTransaction = async action => {
    const c = await pool.connect();
    try {
      await c.query('BEGIN');
      const client = { query: (sql, args) => {
        if (sql.includes('publication-auto-onboarding:find-owner')) attemptedChannels.add(args[0]);
        if (failChannel && sql.includes('publication-auto-onboarding:find-owner') && args[0] === failChannel) throw new Error('injected publication failure');
        return c.query(sql, args);
      } };
      const result = await action(client); await c.query('COMMIT'); return result;
    } catch (e) { await c.query('ROLLBACK'); throw e; } finally { c.release(); }
  };
  try {
    await query('DROP SCHEMA IF EXISTS crawler CASCADE; DROP SCHEMA IF EXISTS publication CASCADE');
    await query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
    fixtureClient = await pool.connect();
    await query("BEGIN");
    await query("INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,pipeline_cycle_id) VALUES('audit-batch','audit-cycle')");
    await query(`INSERT INTO crawler.channel_candidates(dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,accepted_at)
      SELECT 'audit-batch','audit-cycle','UCaudit'||lpad(n::text,4,'0'),'https://youtube.com/channel/audit','accepted','2026-01-02'
      FROM generate_series(1,65)n`);
    await query(`INSERT INTO crawler.channels(channel_id,channel_url,status,agent_status,latest_run_id,registry_promotion_run_id,registry_promotion_candidate_id)
      SELECT channel_id,channel_url,'active','done','run:'||channel_id,'run:'||channel_id,candidate_id
      FROM crawler.channel_candidates WHERE dispatch_batch_id='audit-batch'`);
    await query(`INSERT INTO crawler.channel_runs(run_id,channel_id,candidate_id,crawl_mode,status,detail_status,publication_finalized_status,publication_finalized_at,result_json)
      SELECT 'run:'||channel_id,channel_id,candidate_id,'full','running','done','ready_auto',now(),'{"pipeline_cycle_id":"audit-cycle"}'::jsonb
      FROM crawler.channel_candidates WHERE dispatch_batch_id='audit-batch'`);
    await query(`INSERT INTO crawler.finalized_profiles(channel_id,run_id,status,profile_json,quality_json,finalized_at)
      SELECT channel_id,'run:'||channel_id,'ready_auto','{}','{}',now() FROM crawler.channels`);
    const stream = '11111111-1111-4111-8111-111111111111';
    await query(`INSERT INTO publication.stream(publication_stream_id,source_deployment_key,source_identity_json,minimum_writer_version,capture_enabled_at,
      created_by,created_reason,status_changed_by,status_reason)
      VALUES($1,'audit','{}',$2,'2026-01-01','test','test','test','test')`, [stream, PUBLICATION_WRITER_VERSION]);
    await query("INSERT INTO crawler.channels(channel_id,channel_url) VALUES('ZZseed','https://youtube.com/channel/seed')");
    await query(`INSERT INTO publication.channel_stream_state(publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason)
      VALUES($1,'ZZseed','bootstrap','test','test')`, [stream]);
    await query(`INSERT INTO publication.channel_delivery_state(destination,publication_stream_id,channel_id,mode,state_changed_by,state_reason,online_at)
      VALUES('business',$1,'ZZseed','online','test','test',now())`, [stream]);
    await query("COMMIT");
    fixtureClient.release(); fixtureClient = null;
    const options = { query, withTransaction, pageSize: 100, limit: 25, roundPauseMs: 0 };
    await t.test('a batch limit smaller than a page never discards its remaining candidates', async () => {
      const first = await reconcileAutomaticPublicationBacklog(options);
      assert.equal(first.scanned, 25);
      assert.equal(first.failed, 0, JSON.stringify(first.failures));
      assert.equal(first.skipped, 25, 'incomplete initial packages are not published');
      assert.equal(first.wrapped, false);
      assert.equal(first.after_channel_id, 'UCaudit0025');
      const cursor = (await query("SELECT * FROM crawler.background_reconciliation_scans WHERE scope='publication-onboarding'")).rows[0];
      assert.equal(cursor.after_channel_id, 'UCaudit0025');
      failChannel = 'UCaudit0027';
      const failed = await reconcileAutomaticPublicationBacklog(options);
      assert.equal(failed.failed, 1);
      assert.equal((await query("SELECT after_channel_id FROM crawler.background_reconciliation_scans WHERE scope='publication-onboarding'")).rows[0].after_channel_id, 'UCaudit0026');
      failChannel = null;
      const resumed = await reconcileAutomaticPublicationBacklog(options);
      assert.equal(resumed.scanned, 25);
      assert.equal(resumed.after_channel_id, 'UCaudit0051');
      const last = await reconcileAutomaticPublicationBacklog(options);
      assert.equal(last.scanned, 14);
      assert.equal(last.wrapped, true);
      assert.equal(attemptedChannels.size, 65, 'every candidate reached the authoritative writer, including overflow');
      assert.equal((await query("SELECT count(*)::int n FROM publication.channel_stream_state")).rows[0].n, 1, 'no incomplete channel acquired ownership');
    });
    await t.test('a channel that becomes eligible behind the cursor is found next round', async () => {
      await query("UPDATE crawler.channels SET agent_status='pending'");
      await query("UPDATE crawler.channels SET agent_status='done' WHERE channel_id='UCaudit0001'");
      const again = await reconcileAutomaticPublicationBacklog(options);
      assert.equal(again.scanned, 1);
      assert.equal(again.wrapped, true);
      await query("UPDATE crawler.channels SET agent_status='done'");
    });
    await t.test('terminal reconciliation is bounded, scoped, repeatable and does not finish a partial audit', async () => {
      await query("UPDATE crawler.finalized_profiles SET status='failed' WHERE channel_id='UCaudit0001'");
      await query("UPDATE crawler.channel_runs SET result_json=jsonb_set(result_json,'{pipeline_cycle_id}','\"other-cycle\"') WHERE run_id='run:UCaudit0002'");
      const runOptions = { query, withTransaction, pipelineCycleId: 'audit-cycle', pageSize: 20 };
      const first = await reconcileTerminalFinalizedRunPage(runOptions);
      assert.equal(first.examined, 20); assert.equal(first.updated, 18); assert.equal(first.wrapped, false);
      assert.equal((await reconcileTerminalFinalizedRunPage(runOptions)).updated, 20);
      assert.equal((await reconcileTerminalFinalizedRunPage(runOptions)).updated, 20);
      const end = await reconcileTerminalFinalizedRunPage(runOptions);
      assert.equal(end.updated, 5); assert.equal(end.wrapped, true);
      assert.equal((await reconcileTerminalFinalizedRunPage(runOptions)).updated, 0);
      const unchanged = (await query("SELECT count(*)::int n FROM crawler.channel_runs WHERE status='running'")).rows[0].n;
      assert.equal(unchanged, 2);
      const global = await reconcileTerminalFinalizedRunPage({ query, withTransaction, pageSize: 100 });
      assert.equal(global.updated, 1, 'global audit also covers other cycles');
      assert.equal(global.wrapped, true);
    });
    await t.test('candidate qualification reads only the supplied channel IDs', async () => {
      await query("UPDATE crawler.channel_runs SET result_json=result_json-'publication_gap_repair'");
      const rows = await loadAutomaticPublicationCandidates(query, ['UCaudit0065']);
      assert.deepEqual(rows.rows.map(r => r.channel_id), ['UCaudit0065']);
    });
  } finally { if (fixtureClient) { await fixtureClient.query("ROLLBACK"); fixtureClient.release(); } await pool.end(); }
});
