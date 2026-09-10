import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { performance } from 'node:perf_hooks';
import { loadFinalizeRecoveryCandidates } from '../src/finalizeRecoveryPolicy.js';
import { PUBLICATION_WRITER_VERSION } from '../src/publicationWriterVersion.js';

const url = new URL(process.env.THROUGHPUT_BENCHMARK_DATABASE_URL);
assert.equal(url.pathname, '/throughput_benchmark_test');
assert.ok(['localhost', '127.0.0.1'].includes(url.hostname));
assert.ok(process.argv.includes('--prepare'), 'Explicit --prepare is required for the isolated synthetic fixture');
const client = new pg.Client({ connectionString: url.href, options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
await client.connect();
try {
  assert.equal((await client.query('SELECT current_database() AS name')).rows[0].name, 'throughput_benchmark_test');
  await client.query(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8'));
  const query = client.query.bind(client);
  await query("SET statement_timeout='60s'");
  for (const count of [50000, 100000, 400000]) {
    const before = performance.now();
    const existing = Number((await query("SELECT count(*) FROM crawler.channels WHERE channel_id LIKE 'bench-%'")).rows[0].count);
    for (let start = existing + 1; start <= count; start += 5000) {
      const end = Math.min(count, start + 4999);
      await query('BEGIN');
      try {
        await query(`INSERT INTO crawler.channels(channel_id,channel_url,agent_status)
          SELECT 'bench-'||lpad(n::text,8,'0'),'https://example.test/'||n,'done' FROM generate_series($1::int,$2::int)n`, [start, end]);
        await query(`INSERT INTO crawler.channel_runs(run_id,channel_id,status,detail_status,publication_finalized_status,publication_finalized_at)
          SELECT 'bench-run-'||n,'bench-'||lpad(n::text,8,'0'),'done','done','ready_auto',now() FROM generate_series($1::int,$2::int)n`, [start, end]);
        await query(`UPDATE crawler.channels c SET latest_run_id='bench-run-'||n FROM generate_series($1::int,$2::int)n
          WHERE c.channel_id='bench-'||lpad(n::text,8,'0')`, [start, end]);
        await query(`INSERT INTO crawler.agent_profiles(channel_id,input_url)
          SELECT 'bench-'||lpad(n::text,8,'0'),'https://example.test/'||n FROM generate_series($1::int,$2::int)n`, [start, end]);
        await query(`INSERT INTO crawler.content_candidates(channel_id,run_id,source_content_id,position,detail_status)
          SELECT 'bench-'||lpad(n::text,8,'0'),'bench-run-'||n,'video-'||n||'-'||v,v,'done'
          FROM generate_series($1::int,$2::int)n CROSS JOIN generate_series(1,3)v`, [start, end]);
        await query(`INSERT INTO crawler.contents(content_key,channel_id,run_id,source_content_id,position,content_type,title)
          SELECT 'content-'||n||'-'||v,'bench-'||lpad(n::text,8,'0'),'bench-run-'||n,'video-'||n||'-'||v,v,'video',repeat('synthetic ',20)
          FROM generate_series($1::int,$2::int)n CROSS JOIN generate_series(1,3)v`, [start, end]);
        await query(`INSERT INTO crawler.finalized_profiles(channel_id,run_id,status,updated_at)
          SELECT 'bench-'||lpad(n::text,8,'0'),'bench-run-'||n,'ready_auto',now()+interval '1 day' FROM generate_series($1::int,$2::int)n`, [start, end]);
        await query('COMMIT');
      } catch (error) { await query('ROLLBACK'); throw error; }
    }
    for (const table of ['channels', 'channel_runs', 'content_candidates', 'contents', 'agent_profiles', 'finalized_profiles']) await query(`ANALYZE crawler.${table}`);
    const preparedMs = performance.now() - before;
    const started = performance.now();
    let legacyError = null;
    try { assert.equal((await loadFinalizeRecoveryCandidates(query)).length, 0); }
    catch (error) { if (error.code !== '57014') throw error; legacyError = 'statement_timeout_60s'; }
    const legacyMs = performance.now() - started;
    const pages = [];
    for (let offset = 0; offset < 10; offset++) {
      const ids = Array.from({ length: 200 }, (_, i) => `bench-${String(Math.floor(count * offset / 10) + i + 1).padStart(8, '0')}`);
      const at = performance.now();
      assert.equal((await loadFinalizeRecoveryCandidates(query, { channelIds: ids, limit: 200 })).length, 0);
      pages.push(performance.now() - at);
    }
    pages.sort((a, b) => a - b);
    console.log(JSON.stringify({ channels: count, candidates: count * 3, contents: count * 3, capture_triggers: true, prepared_ms: Math.round(preparedMs), legacy_ms: Math.round(legacyMs), legacy_error: legacyError, bounded_page_p50_ms: Math.round(pages[5]), bounded_page_p95_ms: Math.round(pages[9]) }));
    assert.ok(pages[9] < 2000, 'bounded page P95 must remain below 2s');
  }
} finally { await client.end(); }
