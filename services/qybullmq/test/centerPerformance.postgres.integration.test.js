import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import {CenterPerformance} from '../src/remoteNodes/centerPerformance.js';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {boundedPostgresRead} from '../src/remoteNodes/boundedPostgresRead.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('pool telemetry preserves pg promise, callback, rollback, errors and late checkout semantics',{skip:!url},async t=>{
  const pool=new pg.Pool({connectionString:url,max:1,connectionTimeoutMillis:1000});
  const metrics=new CenterPerformance({enabled:true,sampleEvery:1});t.after(async()=>{await pool.end();metrics.close();});
  await assertIsolatedRemoteDatabase(pool);metrics.attachPool(pool,'gateway');
  assert.equal((await pool.query('SELECT 1 AS value')).rows[0].value,1);
  await new Promise((resolve,reject)=>pool.query('SELECT $1::int AS value',[2],(error,result)=>{
    if(error)return reject(error);try{assert.equal(result.rows[0].value,2);resolve();}catch(e){reject(e);}
  }));
  await new Promise((resolve,reject)=>pool.connect((error,client,release)=>{
    if(error)return reject(error);
    client.query('SELECT 3 AS value',(error,result)=>{release(error);if(error)return reject(error);try{assert.equal(result.rows[0].value,3);resolve();}catch(e){reject(e);}});
  }));
  const store=new RemoteNodeStore({pool});
  await assert.rejects(store.transaction(client=>client.query('SELECT 1/0'),{operation:'apply'}),{code:'22012'});
  assert.equal((await pool.query('SHOW transaction_read_only')).rows[0].transaction_read_only,'off');
  const held=await pool.connect();
  await assert.rejects(boundedPostgresRead(pool,{text:'SELECT 1',acquireTimeoutMs:15}),{code:'REMOTE_DB_ACQUIRE_TIMEOUT'});
  held.release();
  assert.equal((await boundedPostgresRead(pool,{text:'SELECT 4 AS value'})).rows[0].value,4);
  assert.equal(pool.waitingCount,0);assert.equal(pool.idleCount,1);
  metrics.sample();const report=metrics.flush();
  assert.equal(report.metrics['operation.apply.sql_count'].mean,4,'BEGIN, settings, failed query and rollback');
  assert.equal(report.metrics['gateway.sql_errors_sampled'].count,1);
  assert.ok(report.metrics['gateway.acquire.other.ms'].max>=10);
  assert.ok(report.metrics['gateway.hold.other.ms'].count>=5);
  assert.equal(JSON.stringify(report).includes('SELECT'),false);
});

test('disabled performance observation does not wrap pool APIs or run timers',()=>{
  const pool={connect(){},query(){}};const connect=pool.connect,query=pool.query;
  const metrics=new CenterPerformance();metrics.attachPool(pool,'gateway');
  assert.equal(pool.connect,connect);assert.equal(pool.query,query);assert.equal(metrics.sampleTimer,undefined);metrics.close();
});

test('transaction sampling keeps an explicit sampled count and bounded operation names',async()=>{
  const metrics=new CenterPerformance({enabled:true,sampleEvery:20});
  try{
    let calls=0;for(let i=0;i<100;i++)await metrics.measure('untrusted-channel-id',async()=>{calls++;});
    const report=metrics.flush();assert.equal(calls,100);assert.equal(report.sample_every,20);
    assert.equal(report.metrics['operation.other.ms'].count,5);
    assert.equal(JSON.stringify(report).includes('untrusted-channel-id'),false);
  }finally{metrics.close();}
});
