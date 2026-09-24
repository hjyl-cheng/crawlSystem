import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {PUBLICATION_WRITER_VERSION} from '../src/publicationWriterVersion.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

for(const [transport,url] of Object.entries({direct:process.env.REMOTE_NODE_TEST_DATABASE_URL,pooled:process.env.REMOTE_NODE_TEST_TRANSACTION_DATABASE_URL}))
test(`transaction setup uses one settings roundtrip and preserves local settings after commit, rollback and retry (${transport})`,{skip:!url},async t=>{
  const pool=new pg.Pool({connectionString:url,max:1});t.after(()=>pool.end());
  await assertIsolatedRemoteDatabase(pool);
  const settings=async client=>(await client.query(`SELECT current_setting('synchronous_commit') AS sync,
    current_setting('statement_timeout') AS statement,current_setting('lock_timeout') AS lock,
    current_setting('transaction_isolation') AS isolation,current_setting('publication.writer_version',true) AS writer`)).rows[0];
  const before=await settings(pool);const calls=[];
  const connect=pool.connect.bind(pool);
  const store=new RemoteNodeStore({pool:{async connect(){
    const client=await connect();return {query(text,...args){calls.push(text);return client.query(text,...args);},release:()=>client.release()};
  }}});
  const check=async client=>{
    assert.deepEqual(await settings(client),{sync:'on',statement:'15s',lock:'3s',isolation:'read committed',writer:PUBLICATION_WRITER_VERSION});
  };
  await store.transaction(check);
  assert.equal(calls.filter(sql=>/set_config|SET LOCAL/i.test(sql)).length,1,'transaction settings must cost a single SQL roundtrip');
  const restored=await settings(pool);
  for(const key of ['sync','statement','lock','isolation'])assert.equal(restored[key],before[key]);
  assert.equal(restored.writer||null,before.writer||null);
  await assert.rejects(store.transaction(async client=>{await check(client);throw new Error('rollback fixture');}),/rollback fixture/);
  assert.deepEqual(await settings(pool),restored);
  let attempts=0;
  await store.transaction(async client=>{
    assert.equal((await settings(client)).isolation,'repeatable read');
    if(++attempts===1)await client.query("DO $$ BEGIN RAISE EXCEPTION 'retry fixture' USING ERRCODE='40001'; END $$");
  },{repeatableRead:true});
  assert.equal(attempts,2);assert.deepEqual(await settings(pool),restored);
  assert.equal(pool.waitingCount,0);
});
