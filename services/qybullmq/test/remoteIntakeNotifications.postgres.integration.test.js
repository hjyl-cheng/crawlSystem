import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
import {startRemoteNatsCenter} from '../src/remoteNodes/natsCenter.js';
import {createRemoteNatsClient} from '../src/remoteNodes/natsClient.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

test('idle NATS claims use no allocation transactions and committed work wakes only its slot',{
  skip:!process.env.REMOTE_INTAKE_TEST_NATS_URL,timeout:20000
},async t=>{
  const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL,max:8});await assertIsolatedRemoteDatabase(pool);
  const guard=await pool.connect();await guard.query('SELECT pg_advisory_lock(781137981)');
  for(const f of ['schema.sql','routeSchema.sql','natsSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${f}`,import.meta.url),'utf8'));
  const nodeId=randomUUID(),token=randomBytes(32).toString('hex'),store=new RemoteNodeStore({pool});
  await store.registerNode({nodeId,token,capabilities:['fixture'],maxLeases:2});
  for(const slot of ['incremental-1','incremental-2'])await pool.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,slot,nodeId+slot]);
  const reads=new Map();let claims=0;const has=store.hasClaimWork.bind(store),claim=store.claim.bind(store);
  store.hasClaimWork=(id,c,slot)=>{reads.set(slot,(reads.get(slot)??0)+1);return has(id,c,slot);};
  store.claim=(...args)=>{claims++;return claim(...args,{retryOnBusy:true});};
  const signals=await createTransportSignals({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL});
  const url=process.env.REMOTE_INTAKE_TEST_NATS_URL;
  const center=await startRemoteNatsCenter({url,allowLoopback:true,password:'fixture',store,signals,resultMaxBytes:32*1024*1024});
  const a=await createRemoteNatsClient({url,allowLoopback:true,nodeId,token,slot:'incremental-1'});
  const b=await createRemoteNatsClient({url,allowLoopback:true,nodeId,token,slot:'incremental-2'});
  t.after(async()=>{await a.close();await b.close();await signals.close();await center.close();
    try{await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');}finally{guard.release();await pool.end();}});
  const first=a.claim(randomUUID(),'incremental-1'),second=b.claim(randomUUID(),'incremental-2');
  await delay(350);assert.equal(claims,0);assert.equal(reads.get('incremental-1'),1);assert.equal(reads.get('incremental-2'),1);
  const task=randomUUID(),tx=await pool.connect();
  const insert=()=>tx.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,target_node_id,target_worker_slot)
    VALUES($1::uuid,$1::text,'fixture','{}','{}',$2,'incremental-1')`,[task,nodeId]);
  await tx.query('BEGIN');await insert();await tx.query('ROLLBACK');await delay(150);
  assert.equal(claims,0,'rolled-back work emits no wakeup');
  const gate=await pool.connect();await gate.query('BEGIN');await gate.query('SELECT pg_advisory_xact_lock(781138017,hashtext($1))',[nodeId]);
  await tx.query('BEGIN');await insert();await tx.query('COMMIT');tx.release();
  await delay(200);assert.ok(claims>0,'allocation contention is exercised');
  const releasedAt=Date.now();await gate.query('ROLLBACK');gate.release();
  assert.equal((await first).task_id,task);assert.ok(Date.now()-releasedAt<2000,'available work must not wait for the idle fallback');
  const claimed=claims;
  assert.equal(reads.get('incremental-2'),1,'another slot must not re-query for this task');
  assert.equal(await second,null,'bounded idle recheck recovers missed notifications');
  assert.equal(claims,claimed,'idle rechecks still do not take allocation locks');
});
