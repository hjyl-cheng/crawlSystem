import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import {boundedPostgresRead} from '../src/remoteNodes/boundedPostgresRead.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('bounded read cancels real PostgreSQL work and leaves the next borrower writable', {skip:!url},async t=>{
  const pool=new pg.Pool({connectionString:url,max:1,connectionTimeoutMillis:1000});
  t.after(()=>pool.end());await assertIsolatedRemoteDatabase(pool);
  const abort=new AbortController();const timer=setTimeout(()=>abort.abort(new Error('test cancelled')),50);
  try{await assert.rejects(boundedPostgresRead(pool,{text:'SELECT pg_sleep(30)',signal:abort.signal}),/test cancelled/);}
  finally{clearTimeout(timer);}
  const client=await pool.connect();
  try {
    assert.equal((await client.query('SHOW transaction_read_only')).rows[0].transaction_read_only,'off');
    await assert.rejects(boundedPostgresRead(pool,{text:'SELECT 1',acquireTimeoutMs:20}),{code:'REMOTE_DB_ACQUIRE_TIMEOUT'});
  } finally {client.release();}
  assert.equal((await boundedPostgresRead(pool,{text:'SELECT 7 AS value'})).rows[0].value,7);
  assert.equal(pool.waitingCount,0);assert.equal(pool.idleCount,1);
});
