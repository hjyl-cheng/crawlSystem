import test from 'node:test';import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteWorkerActivationStore,REMOTE_RUNTIME_REVISION} from '../src/remoteNodes/workerActivationStore.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('different worker heartbeats run concurrently; node disable and claim capacity remain fenced',{skip:!url,timeout:15000},async()=>{
 const pool=new pg.Pool({connectionString:url,max:24});const nodeId=randomUUID();const admin=await pool.connect();
 try{
  await assertIsolatedRemoteDatabase(pool);await admin.query('SELECT pg_advisory_lock(781137981)');
  for(const f of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${f}`,import.meta.url),'utf8'));
  const store=new RemoteNodeStore({pool});await store.registerNode({nodeId,token:randomBytes(32).toString('hex'),capabilities:['fixture.concurrent'],maxLeases:2});
  let hold=false,entered=0;let release;const barrier=new Promise(r=>{release=r});
  const activation=new RemoteWorkerActivationStore({store,verifyExecution:async()=>{if(hold){entered++;await barrier;}return true;}});
  const beats=[];
  for(let i=1;i<=20;i++){
   const slot=`incremental-${i}`,deploymentId=randomUUID(),configHash='a'.repeat(64);
   await pool.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,slot,`${nodeId}-${slot}`]);
   await activation.register({nodeId,slot,deploymentId,configHash});
   const beat={version:1,mode:'incremental_collect',node_id:nodeId,slot,deployment_id:deploymentId,config_hash:configHash,instance_id:randomUUID(),relay_boot_id:'b'.repeat(48),runtime_revision:REMOTE_RUNTIME_REVISION,accepting:true};
   await activation.heartbeat(nodeId,beat);await activation.activate(beat);beats.push(beat);
  }
  hold=true;const pending=beats.map(b=>activation.heartbeat(nodeId,b));const settled=Promise.allSettled(pending);
  try{
   for(let i=0;i<80&&entered<20;i++)await delay(10);
   assert.equal(entered,20,'all 20 slots must reach verification without serializing on the node row');
   let disabled=false;const disabling=store.setNodeState(nodeId,'disabled').then(()=>{disabled=true});
   await delay(50);assert.equal(disabled,false,'disable waits for concurrent readers to commit');
   release();await settled;await disabling;
  }finally{release();await settled;hold=false;}
  await assert.rejects(activation.heartbeat(nodeId,beats[0]),{code:'UNAUTHORIZED'});
  await store.setNodeState(nodeId,'active');
  for(let i=0;i<5;i++)await store.enqueue({workKey:`concurrent-${nodeId}-${i}`,capability:'fixture.concurrent',input:{},context:{}});
  // Managed claims deliberately return CLAIM_BUSY while another slot holds
  // the allocation lock. Retry the same claim identity as the transport does.
  const results=await Promise.allSettled(beats.map(async b=>{
   const request={claim_id:randomUUID(),slot:b.slot,connection:b};
   for(let retry=0;retry<100;retry++){
    try{return await activation.claim(nodeId,request);}
    catch(error){if(error.code!=='CLAIM_BUSY')throw error;await delay(10);}
   }
   assert.fail('slot allocation stayed busy');
  }));
  for(const result of results)if(result.status==='rejected')throw result.reason;
  const leases=results.map(result=>result.value);
  assert.equal(leases.filter(Boolean).length,2,'parallel claims still honor node capacity');
 }finally{
  await pool.query('DELETE FROM remote_ingestion.claims WHERE node_id=$1',[nodeId]);
  await pool.query("DELETE FROM remote_ingestion.tasks WHERE work_key LIKE $1",[`concurrent-${nodeId}-%`]);
  for(const table of ['worker_connections','network_slots','nodes'])await pool.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);
  admin.release();await pool.end();
 }
});
test('a slow slot claim does not block another slot heartbeat or network ownership check',{skip:!url,timeout:15000},async()=>{
 const pool=new pg.Pool({connectionString:url,max:4});const nodeId=randomUUID();let release;const barrier=new Promise(r=>{release=r});let entered;
 const started=new Promise(r=>{entered=r});let claiming;
 try{
  await assertIsolatedRemoteDatabase(pool);
  const store=new RemoteNodeStore({pool});await store.registerNode({nodeId,token:randomBytes(32).toString('hex'),capabilities:['fixture.concurrent'],maxLeases:1});
  await pool.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,'incremental-1',nodeId]);
  claiming=store.claim(nodeId,randomUUID(),'incremental-1',{authorize:async()=>{entered();await barrier;return {allowNew:true};}});
  await started;
  let read=false;const reading=store.transaction(async c=>{await c.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[nodeId]);read=true;});
  try{for(let i=0;i<50&&!read;i++)await delay(10);assert.equal(read,true,'one claim must not lock every other slot out of shared node checks');}
  finally{release();await Promise.allSettled([claiming,reading]);}
 }finally{release();await claiming?.catch(()=>{});await pool.query('DELETE FROM remote_ingestion.network_slots WHERE node_id=$1',[nodeId]);await pool.query('DELETE FROM remote_ingestion.nodes WHERE node_id=$1',[nodeId]);await pool.end();}
});
