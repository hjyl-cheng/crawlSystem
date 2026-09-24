import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import test from 'node:test';
import pg from 'pg';

const url = process.env.BUSINESS_SNAPSHOT_CLEANUP_TEST_URL;
const container = process.env.BUSINESS_SNAPSHOT_REPACK_TEST_CONTAINER;
test('PostgreSQL 18 online repack keeps snapshot links and concurrent writes; conflicting reader survives', {
  skip: !url || !container, timeout: 120000,
}, async () => {
  const parsed = new URL(url);
  assert.equal(parsed.hostname, '127.0.0.1'); assert.equal(parsed.pathname, '/business_cleanup_test');
  assert.match(container, /^qy-snapshot-cleanup-test-[0-9]+$/);
  const c = new pg.Client({ connectionString: url }); await c.connect();
  const w = new pg.Client({ connectionString: url }); await w.connect();
  const repack = () => new Promise((resolve, reject) => {
    const child = spawn('docker', ['exec', container, 'pg_repack', '-U', 'postgres', '-d', 'business_cleanup_test',
      '--table=public.channel_links', '--no-order', '--jobs=1', '--wait-timeout=2', '--no-kill-backend']);
    let output = '';
    child.stdout.on('data', data => { output += data; }); child.stderr.on('data', data => { output += data; });
    child.on('error', reject); child.on('close', code => resolve({ code, output }));
  });
  const fingerprint = async () => (await c.query(`SELECT count(*)::int AS n,
    md5(string_agg(md5((to_jsonb(row)-'title')::text),'' ORDER BY id)) AS fields,
    md5(string_agg(md5(title),'' ORDER BY id) FILTER(WHERE id<>'repack-link-1')) AS stable_titles
    FROM public.channel_links row`)).rows[0];
  let stop = true, writes = 0, writeLoop;
  try {
    await c.query('CREATE EXTENSION pg_repack');
    await c.query(`INSERT INTO public.channel_links(id,channel_id,channel_snapshot_id,link_type,url,title,source,raw_link)
      SELECT 'repack-link-'||n,s.channel_id,s.id,'website','https://example.test/'||n,'0','repack-test',
        jsonb_build_object('value',repeat(md5(n::text),8))
      FROM generate_series(1,50000) n CROSS JOIN LATERAL
        (SELECT id,channel_id FROM public.channel_snapshots WHERE id LIKE 'publication_current_snapshot_%' LIMIT 1) s`);
    const before = await fingerprint(); assert.ok(before.n >= 50000);
    await c.query('BEGIN'); await c.query('LOCK TABLE public.channel_links IN ACCESS SHARE MODE');
    const blocked = await repack();
    await c.query('ROLLBACK');
    assert.match(blocked.output, /Skipping|timed out|timeout/i);
    assert.equal((await c.query('SELECT 1 AS ok')).rows[0].ok, 1);
    assert.deepEqual(await fingerprint(), before);
    stop = false;
    writeLoop = (async () => {
      while (!stop) {
        await w.query("UPDATE public.channel_links SET title=$1 WHERE id='repack-link-1'", [String(writes + 1)]);
        writes++;
        await new Promise(resolve => setTimeout(resolve, 10));
      }
    })();
    const done = await repack(); stop = true; await writeLoop;
    assert.equal(done.code, 0, done.output); assert.doesNotMatch(done.output, /WARNING|ERROR|FATAL|Skipping/);
    assert.ok(writes > 0); assert.deepEqual(await fingerprint(), before);
    assert.equal((await c.query("SELECT title FROM public.channel_links WHERE id='repack-link-1'")).rows[0].title, String(writes));
    assert.equal((await c.query(`SELECT count(*)::int AS n FROM pg_index WHERE indrelid='public.channel_links'::regclass
      AND NOT (indisvalid AND indisready)`)).rows[0].n, 0);
    await c.query('DROP EXTENSION pg_repack RESTRICT');
    console.log(JSON.stringify({ repack_test: { rows: before.n, concurrent_writes: writes, fingerprints_equal: true, lock_conflict_skipped: true } }));
  } finally {
    stop = true; await writeLoop?.catch(() => {});
    await c.query('ROLLBACK').catch(() => {}); await w.end(); await c.end();
  }
});
