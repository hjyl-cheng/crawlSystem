import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';

const url = process.env.FINALIZE_DEFERRAL_TEST_DATABASE_URL;
async function fixture(action) {
  assert.match(new URL(url).pathname, /test/);
  const pool = new pg.Pool({ connectionString: url, max: 4 });
  const a = await pool.connect(); const b = await pool.connect();
  try {
    await a.query(`DROP SCHEMA IF EXISTS finalize_deferral_test CASCADE;
      CREATE SCHEMA finalize_deferral_test;
      CREATE TABLE finalize_deferral_test.channels(id text PRIMARY KEY);
      CREATE TABLE finalize_deferral_test.runs(id text PRIMARY KEY);
      CREATE TABLE finalize_deferral_test.contents(id int PRIMARY KEY,run_id text REFERENCES finalize_deferral_test.runs(id));
      INSERT INTO finalize_deferral_test.channels VALUES('channel');
      INSERT INTO finalize_deferral_test.runs VALUES('full');`);
    await action(a,b,pool);
  } finally {
    await a.query('ROLLBACK'); await b.query('ROLLBACK');
    a.release(); b.release(); await pool.end();
  }
}

test('original Run → Channel versus Channel → Run FK cycle reproduces 40P01', { skip: !url }, async () => {
  await fixture(async (a,b) => {
    await a.query('BEGIN'); await b.query('BEGIN');
    await a.query("SELECT * FROM finalize_deferral_test.runs FOR UPDATE");
    await b.query("SELECT * FROM finalize_deferral_test.channels FOR NO KEY UPDATE");
    const results = await Promise.allSettled([
      a.query('SELECT * FROM finalize_deferral_test.channels FOR UPDATE').catch(async e => { await a.query('ROLLBACK'); throw e; }),
      b.query("INSERT INTO finalize_deferral_test.contents VALUES(1,'full')").catch(async e => { await b.query('ROLLBACK'); throw e; }),
    ]);
    assert.equal(results.filter(r => r.status === 'rejected' && r.reason.code === '40P01').length,1);
  });
});

async function transact(client, action) {
  await client.query('BEGIN');
  try { const value = await action(client); await client.query('COMMIT'); return value; }
  catch (e) { await client.query('ROLLBACK'); throw e; }
}

test('secondary Finalize rolls back its Run lock before a legacy incremental FK writer deadlocks', { skip: !url }, async () => {
  const { finalizeTransactions } = await import('../src/finalizeDeferral.js');
  await fixture(async (a,b) => {
    await b.query('BEGIN');
    await b.query('SELECT * FROM finalize_deferral_test.channels FOR NO KEY UPDATE');
    const finalize = finalizeTransactions(action => transact(a,action), { channelId:'channel',runId:'full',secondary:true });
    let started;
    const held = new Promise(resolve => { started = resolve; });
    const finishing = finalize(async client => {
      await client.query('SELECT * FROM finalize_deferral_test.runs FOR UPDATE');
      started();
      await client.query('SELECT * FROM finalize_deferral_test.channels FOR UPDATE');
    });
    const assertion = assert.rejects(finishing, e => e.code === 'FINALIZE_DEFERRED' && e.cause.code === '55P03');
    await held;
    await b.query("INSERT INTO finalize_deferral_test.contents VALUES(1,'full')");
    await b.query('COMMIT');
    await assertion;
    assert.equal((await a.query('SELECT count(*)::int AS n FROM finalize_deferral_test.contents')).rows[0].n,1);
  });
});

test('same-channel try lock defers immediately, other channels and later attempts continue', { skip: !url }, async () => {
  const { finalizeTransactions } = await import('../src/finalizeDeferral.js');
  const { lockPublicationChannelMutation } = await import('../src/publicationChannelMutationLock.js');
  await fixture(async (a,b) => {
    await b.query('BEGIN'); await lockPublicationChannelMutation(b,'channel');
    const run = channelId => finalizeTransactions(action => transact(a,action), { channelId,runId:'full',secondary:true });
    await assert.rejects(run('channel')(() => assert.fail('must not run')), { code:'FINALIZE_DEFERRED' });
    assert.equal(await run('other')(() => 42),42);
    await b.query('COMMIT');
    assert.equal(await run('channel')(() => 43),43);
  });
});

test('deferred FK conflicts at COMMIT roll back completely before being rescheduled', { skip: !url }, async () => {
  const { finalizeTransactions } = await import('../src/finalizeDeferral.js');
  await fixture(async (a,b) => {
    await a.query('ALTER TABLE finalize_deferral_test.contents ALTER CONSTRAINT contents_run_id_fkey DEFERRABLE INITIALLY DEFERRED');
    await b.query('BEGIN'); await b.query('SELECT * FROM finalize_deferral_test.runs FOR UPDATE');
    const finalize=finalizeTransactions(action=>transact(a,action),{channelId:'channel',runId:'full',secondary:true});
    await assert.rejects(finalize(client=>client.query("INSERT INTO finalize_deferral_test.contents VALUES(1,'full')")),
      e=>e.code==='FINALIZE_DEFERRED' && e.cause.code==='55P03');
    assert.equal((await a.query('SELECT count(*)::int AS n FROM finalize_deferral_test.contents')).rows[0].n,0);
  });
});
