import test from 'node:test';import assert from 'node:assert/strict';import pg from 'pg';
import {randomUUID} from 'node:crypto';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {PUBLICATION_WRITER_VERSION} from '../src/publicationWriterVersion.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
const url=process.env.REMOTE_NODE_TEST_TRANSACTION_DATABASE_URL;
test('pooled concurrent transactions retain writer fence and rollback with bounded PostgreSQL backends',{skip:!url,timeout:15000},async()=>{
 const pool=new pg.Pool({connectionString:url,max:20});const store=new RemoteNodeStore({pool});
 const name='pool_fixture_'+randomUUID().replaceAll('-','');
 try{
  await assertIsolatedRemoteDatabase(pool);
  await pool.query(`CREATE TABLE ${name}(id integer PRIMARY KEY,writer text DEFAULT current_setting('publication.writer_version',true) CHECK(writer='${PUBLICATION_WRITER_VERSION}'))`);
  const pids=await Promise.all(Array.from({length:40},(_,i)=>store.transaction(async c=>{
   const state=(await c.query("SELECT pg_backend_pid() pid,current_setting('publication.writer_version') writer")).rows[0];
   assert.equal(state.writer,PUBLICATION_WRITER_VERSION);await c.query(`INSERT INTO ${name}(id) VALUES($1)`,[i]);return state.pid;
  })));
  assert.ok(new Set(pids).size<=4,'fixture transaction pool must share at most four backends');
  await assert.rejects(store.transaction(async c=>{await c.query(`INSERT INTO ${name}(id) VALUES(100)`);throw Error('fixture rollback');}),/fixture rollback/);
  assert.equal((await pool.query(`SELECT count(*)::int n FROM ${name}`)).rows[0].n,40);
  assert.ok((await pool.query("SELECT current_setting('publication.writer_version',true) writer")).rows[0].writer!==PUBLICATION_WRITER_VERSION,'writer identity is local to each transaction');
 }finally{await pool.query(`DROP TABLE IF EXISTS ${name}`);await pool.end();}
});
