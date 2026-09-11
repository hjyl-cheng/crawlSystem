import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteCenterExecutionSupervisor} from '../src/remoteNodes/centerExecutionSupervisor.js';
import {RemoteWorkerActivationStore,REMOTE_RUNTIME_REVISION} from '../src/remoteNodes/workerActivationStore.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
const port=Number(process.env.REMOTE_NODE_TEST_REDIS_PORT);
async function until(check){for(let i=0;i<200;i++){if(await check())return;await delay(20);}assert.fail('supervisor fixture timed out');}

test('central ownership, heartbeat expiry and operator intent gate real queue consumers',{skip:!url||!port,timeout:30000},async t=>{
  const pool=new pg.Pool({connectionString:url,max:6});const guardPool=new pg.Pool({connectionString:url,max:4});
  const guard=await pool.connect();const supervisors=[];const nodeId=randomUUID();
  t.after(async()=>{for(const s of supervisors)await s.stop();await pool.query('DELETE FROM remote_ingestion.tasks WHERE target_node_id=$1',[nodeId]);
    for(const table of ['worker_connections','network_slots','nodes'])await pool.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);
    guard.release();await guardPool.end();await pool.end();});
  await assertIsolatedRemoteDatabase(pool);await guard.query('SELECT pg_advisory_lock(781137981)');
  // The supervisor checks original execution attempts even for an empty slot.
  // Initialize its business dependency so this test also runs in a fresh DB.
  await pool.query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
  for(const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  const store=new RemoteNodeStore({pool});await store.registerNode({nodeId,token:randomBytes(32).toString('hex'),capabilities:['fixture.supervisor']});
  let active;const activation=new RemoteWorkerActivationStore({store,verifyExecution:(client,row)=>active?.verifyExecution(client,row)??false});
  const slot='incremental-1';await pool.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,slot,'test-'+nodeId]);
  const reg={nodeId,slot,deploymentId:randomUUID(),configHash:randomBytes(32).toString('hex')};await activation.register(reg);
  const heartbeat={version:1,mode:'incremental_collect',node_id:nodeId,slot,deployment_id:reg.deploymentId,config_hash:reg.configHash,
    instance_id:randomUUID(),relay_boot_id:randomBytes(24).toString('hex'),runtime_revision:REMOTE_RUNTIME_REVISION,accepting:true};
  await activation.heartbeat(nodeId,heartbeat);let allocated=0;let admission;
  const args={store,channelStore:{store},activation,guardPool,connection:{host:'127.0.0.1',port,password:'remote-center-fixture-only',maxRetriesPerRequest:null},
    prefix:'remote-supervisor-guard-test-'+randomUUID(),allowedNodeIds:[nodeId],intervalMs:50,
    createRuntime:value=>{admission=value.assertAdmission;return value;},createProcessor:()=>async()=>assert.fail('no fixture jobs exist'),
    createRota:options=>{allocated++;let started=false;let closing=false;return {workerId:options.workerId,workerInstanceId:options.workerInstanceId,
      start:async()=>{started=true;},close:async()=>{closing=true;},status:()=>({started,closing,assignment:{ready:true},active_job:false})};}};
  const make=()=>{const s=new RemoteCenterExecutionSupervisor(args);supervisors.push(s);return s;};
  active=make();await active.tick();await until(async()=>(await activation.heartbeat(nodeId,heartbeat)).ready_for_tasks);
  assert.equal(allocated,1);const entry=[...active.entries.values()][0];
  assert.equal(await store.transaction(client=>admission(client)),true);
  await pool.query('UPDATE remote_ingestion.worker_connections SET config_hash=$2 WHERE node_id=$1',[nodeId,'f'.repeat(64)]);
  assert.equal(await store.transaction(client=>admission(client)),false,'admission rereads current deployment instead of trusting cached identity');
  await pool.query('UPDATE remote_ingestion.worker_connections SET config_hash=$2 WHERE node_id=$1',[nodeId,reg.configHash]);
  const rival=make();await rival.tick();assert.equal(rival.entries.size,0);assert.equal(allocated,1);
  await pool.query("UPDATE remote_ingestion.worker_connections SET connected_until=now()-interval '1 second' WHERE node_id=$1",[nodeId]);
  await active.tick();assert.equal(entry.worker.isPaused(),true);
  await activation.heartbeat(nodeId,heartbeat);await active.tick();assert.equal(entry.worker.isPaused(),false);
  // The actual PG backend is terminated only in the isolated DB. A persisted
  // enabled flag cannot authorize work after losing its exact session lock.
  await pool.query('SELECT pg_terminate_backend($1)',[entry.backendPid]);
  await until(()=>!entry.owned);
  assert.equal((await activation.heartbeat(nodeId,heartbeat)).ready_for_tasks,false);
  await until(()=>active.entries.size===0);await active.stop();
  // A replacement center can reconnect an idle slot using a new Rota owner.
  active=make();await active.tick();await until(async()=>(await activation.heartbeat(nodeId,heartbeat)).ready_for_tasks);
  assert.equal(allocated,2);
  await activation.drain(nodeId,slot);await active.stop();
  active=make();await active.tick();assert.equal(active.entries.size,0,'restart must preserve explicit drain');assert.equal(allocated,2);
  await active.stop();
  // Unsettled work from an abruptly terminated center is never overwritten or
  // silently treated as success. Only this slot waits for original recovery.
  await pool.query('UPDATE remote_ingestion.worker_connections SET activation_requested=true WHERE node_id=$1',[nodeId]);
  const task=await store.enqueue({workKey:'supervisor-unsettled-'+nodeId,capability:'fixture.supervisor',input:{},context:{}});
  await pool.query('UPDATE remote_ingestion.tasks SET target_node_id=$2,target_worker_slot=$3 WHERE task_id=$1',[task,nodeId,slot]);
  active=make();await active.tick();assert.equal([...active.entries.values()][0].blocked,true);assert.equal(allocated,2);
  assert.equal((await activation.heartbeat(nodeId,heartbeat)).ready_for_tasks,false);
  assert.equal((await pool.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1',[task])).rows[0].state,'pending');
  await pool.query("UPDATE remote_ingestion.tasks SET state='failed' WHERE task_id=$1",[task]);
  await active.tick();await active.tick();await until(async()=>(await activation.heartbeat(nodeId,heartbeat)).ready_for_tasks);
  assert.equal(allocated,3);
});
