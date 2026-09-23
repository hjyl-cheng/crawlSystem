import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import pg from 'pg';
import {Queue,Worker} from 'bullmq';
import {INCREMENTAL_QUEUE} from '../src/incrementalPlan.js';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {RemoteChannelRouteStore} from '../src/remoteNodes/channelRouteStore.js';
import {RemoteWorkerActivationStore,REMOTE_RUNTIME_REVISION} from '../src/remoteNodes/workerActivationStore.js';
import {RemoteCenterExecutionSupervisor} from '../src/remoteNodes/centerExecutionSupervisor.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
import {incrementalWorkerCapacity} from '../../feature-dispatch/src/dynamicDispatcher.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL,port=Number(process.env.REMOTE_NODE_TEST_REDIS_PORT);
async function until(check){for(let i=0;i<250;i++){if(await check())return;await delay(20);}assert.fail('execution-control fixture timed out');}

test('page-authorized nodes share the original queue; pause drains active work and persists across restart',{skip:!url||!port,timeout:40000},async t=>{
  const pool=new pg.Pool({connectionString:url,max:8}),guards=new pg.Pool({connectionString:url,max:4});
  const schemaGuard=await pool.connect();await assertIsolatedRemoteDatabase(pool);await schemaGuard.query('SELECT pg_advisory_lock(781137981)');
  await pool.query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
  for(const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  const store=new RemoteNodeStore({pool}),channelStore=new RemoteChannelPlanStore({store});
  const routes=new RemoteChannelRouteStore({channelStore,privateKey:generateKeyPairSync('ed25519').privateKey,secretKey:randomBytes(32),readRotaRoute:()=>{},assertBusinessFence:()=>{}});
  const nodeId=randomUUID(),deploymentId=randomUUID(),slot='incremental-1';
  const connection={host:'127.0.0.1',port,password:'remote-center-fixture-only',maxRetriesPerRequest:null},prefix='control-fixture-'+randomUUID();
  let supervisor,local,releaseActive,releaseRoute,holdRoute=false,admission;let capacityReads=0;const supervisors=[],calls=[];
  const queue=new Queue(INCREMENTAL_QUEUE,{connection,prefix});
  const activation=new RemoteWorkerActivationStore({store,verifyExecution:(client,row)=>supervisor?.verifyExecution(client,row)??false});
  const args={store,channelStore,routes,activation,guardPool:guards,connection,prefix,dashboardManaged:true,intervalMs:50,
    rotaClient:{capacity:async()=>{capacityReads++;return {roles:{channel:{provisioned:61,claimed:61,ready:61}}};}},
    createRuntime:opts=>{admission=opts.assertAdmission;return opts;},
    createProcessor:()=>async job=>{calls.push({owner:'remote',id:job.id});if(job.id==='first')await new Promise(r=>{releaseActive=r;});return 'done';},
    createRota:opts=>{let started=false,closing=false;return {workerId:opts.workerId,workerInstanceId:opts.workerInstanceId,
      start:async()=>{if(holdRoute)await new Promise(r=>{releaseRoute=r;});started=true;},
      close:async()=>{closing=true;releaseRoute?.();},status:()=>({started,closing,assignment:{ready:true}})};}};
  const make=()=>{supervisor=new RemoteCenterExecutionSupervisor(args);supervisors.push(supervisor);return supervisor;};
  make();
  const admin=createRemoteDeploymentAdmin({store,routes,activation,execution:{allowsNode:id=>supervisor.allowsNode(id),isProcessing:row=>supervisor.isProcessing(row),preparationState:row=>supervisor.preparationState(row),networkCapacity:()=>supervisor.networkCapacity()},token:randomBytes(32).toString('hex'),image:'registry.example/collect@sha256:'+'a'.repeat(64),gatewayUrl:'https://center.example/remote'});
  t.after(async()=>{releaseActive?.();releaseRoute?.();await local?.close();for(const s of supervisors)await s.stop();await queue.obliterate({force:true});await queue.close();
    for(const table of ['node_deployments','worker_connections','network_slots','nodes'])await pool.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);schemaGuard.release();await guards.end();await pool.end();});
  const files={[slot+'.json']:JSON.stringify({version:1,mode:'incremental_collect',role:'incremental',node_id:nodeId,deployment_id:deploymentId,slot,gateway_url:'https://center.example/remote'})+'\n'};
  await admin.prepare({nodeId,deploymentId,image:'registry.example/collect@sha256:'+'a'.repeat(64),files});
  const row=(await pool.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1',[nodeId])).rows[0];
  assert.equal(row.activation_requested,false,'newly deployed workers require a page action');
  const heartbeat={version:1,mode:'incremental_collect',node_id:nodeId,slot,deployment_id:deploymentId,config_hash:row.config_hash,instance_id:randomUUID(),relay_boot_id:randomBytes(24).toString('hex'),runtime_revision:REMOTE_RUNTIME_REVISION,accepting:true};
  await activation.heartbeat(nodeId,heartbeat);
  const control=(enabled,expectedRequested)=>admin.setExecution({nodeId,deploymentId,enabled,expectedRequested,workerCount:1});
  await queue.add('fixture',{}, {jobId:'first'});await supervisor.tick();assert.equal(supervisor.entries.size,0);
  await control(true,false);await supervisor.tick();await until(()=>!!releaseActive);
  await assert.rejects(admin.retire({nodeId,deploymentId,slot,operationId:randomUUID(),phase:'reserve'}),{code:'WORKER_NOT_IDLE'});
  assert.equal((await admin.status({nodeId,deploymentId})).allowedCount,1,'busy deletion leaves intake unchanged');
  local=new Worker(INCREMENTAL_QUEUE,async job=>{calls.push({owner:'local',id:job.id});return 'done';},{connection,prefix});await local.waitUntilReady();
  await queue.add('fixture',{}, {jobId:'second'});await until(async()=>(await queue.getJob('second')).getState().then(s=>s==='completed'));
  assert.deepEqual(calls.filter(r=>r.id==='first'),[{owner:'remote',id:'first'}],'local consumer cannot also run the claimed Plan');
  const paused=await control(false,true);assert.equal(paused.draining,true);await supervisor.tick();
  assert.equal(await store.transaction(client=>admission(client)),true,'pause retains the active processor execution fence');
  const current=(await pool.query('SELECT enabled,activation_requested FROM remote_ingestion.worker_connections WHERE node_id=$1',[nodeId])).rows[0];
  assert.deepEqual(current,{enabled:true,activation_requested:false});
  await queue.add('fixture',{}, {jobId:'third'});await until(async()=>(await queue.getJob('third')).getState().then(s=>s==='completed'));
  assert.equal(calls.find(r=>r.id==='third').owner,'local','paused remote cannot take another job');
  releaseActive();await until(()=>supervisor.entries.size===0);assert.equal((await admin.status({nodeId,deploymentId})).draining,false);
  await supervisor.stop();make();await supervisor.tick();assert.equal(supervisor.entries.size,0,'restart retains pause');
  await pool.query("UPDATE remote_ingestion.worker_connections SET connected_until=now()-interval '1 minute' WHERE node_id=$1",[nodeId]);
  await assert.rejects(()=>control(true,false),{code:'WORKER_NOT_READY'});await activation.heartbeat(nodeId,heartbeat);
  await assert.rejects(()=>admin.setExecution({nodeId,deploymentId,enabled:true,expectedRequested:false,workerCount:2}),{code:'WORKER_DEPLOYMENT_MISMATCH'});
  await local.close();local=null;
  await control(true,false);await supervisor.tick();await until(async()=>(await admin.status({nodeId,deploymentId})).counts.ready===1);
  await queue.add('fixture',{}, {jobId:'fourth'});await until(async()=>(await queue.getJob('fourth')).getState().then(s=>s==='completed'));
  assert.equal(calls.find(r=>r.id==='fourth').owner,'remote');
  assert.equal(new Set(calls.map(r=>r.id)).size,calls.length,'each queued task executes once during ordinary shared consumption');
  await control(false,true);await supervisor.tick();await until(()=>supervisor.entries.size===0);
  // Pause during pending network allocation must not be undone by activation.
  holdRoute=true;await control(true,false);await supervisor.tick();await until(()=>!!releaseRoute);
  const waiting=await admin.status({nodeId,deploymentId});
  assert.equal(waiting.workers[0].preparation,'waiting_network');
  assert.deepEqual(waiting.networkCapacity,{provisioned:61,claimed:61,available:0});
  assert.equal(waiting.counts.ready,0);
  const readsBefore=capacityReads;await admin.status({nodeId,deploymentId});
  assert.equal(capacityReads,readsBefore,'status polling shares cached network capacity');
  await control(false,true);releaseRoute();await supervisor.tick();await until(()=>supervisor.entries.size===0);
  assert.equal((await admin.status({nodeId,deploymentId})).requested,false);
  assert.equal((await pool.query('SELECT enabled FROM remote_ingestion.worker_connections WHERE node_id=$1',[nodeId])).rows[0].enabled,false);
  // Scaling deployment does not implicitly raise its admitted count.
  holdRoute=false;
  const expanded={...files};
  for(let i=2;i<=3;i++)expanded[`incremental-${i}.json`]=JSON.stringify({...JSON.parse(files[slot+'.json']),slot:`incremental-${i}`})+'\n';
  await admin.prepare({nodeId,deploymentId,image:'registry.example/collect@sha256:'+'a'.repeat(64),files:expanded});
  for(const r of (await pool.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1',[nodeId])).rows){
    await activation.heartbeat(nodeId,{...heartbeat,slot:r.slot,config_hash:r.config_hash});
  }
  const count=async(installed,expectedRequested)=>admin.setExecution({nodeId,deploymentId,workerCount:installed||3,enabled:installed>0,expectedRequested});
  assert.equal((await admin.status({nodeId,deploymentId})).allowedCount,0);
  // A failed expansion can register extra slots before they are installed.
  // The verified original fleet must remain controllable in that state.
  const resizedPaused=await admin.setExecution({nodeId,deploymentId,workerCount:1,enabled:false,expectedRequested:false});
  assert.equal(resizedPaused.allowedCount,0,'verified installed prefix remains paused');
  assert.equal(resizedPaused.configuredCount,3,'center reports registered count; Dashboard overlays verified installed count');
  await control(true,false);
  assert.deepEqual((await admin.status({nodeId,deploymentId})).workers.filter(w=>w.requested).map(w=>w.slot),[slot]);
  await admin.setExecution({nodeId,deploymentId,workerCount:1,enabled:false,expectedRequested:true});
  await count(2,false);await supervisor.tick();await until(async()=>(await admin.status({nodeId,deploymentId})).counts.ready===2);
  await until(async()=>await incrementalWorkerCapacity(queue)===2);
  const selected=(await admin.status({nodeId,deploymentId})).workers.filter(w=>w.requested).map(w=>w.slot);
  await count(3,true);await supervisor.tick();await until(async()=>(await admin.status({nodeId,deploymentId})).counts.ready===3);
  await count(1,true);await supervisor.tick();await until(async()=>(await admin.status({nodeId,deploymentId})).counts.draining===0);
  assert.equal((await admin.status({nodeId,deploymentId})).workers.find(w=>w.requested).slot,selected[0]);
  await until(async()=>await incrementalWorkerCapacity(queue)===1);
  await assert.rejects(count(4,true),{code:'WORKER_DEPLOYMENT_MISMATCH'});
  await assert.rejects(count(0,false),{code:'EXECUTION_CONTROL_CHANGED'});
  await count(0,true);await supervisor.tick();await until(()=>supervisor.entries.size===0);
  await supervisor.stop();make();await supervisor.tick();assert.equal(supervisor.entries.size,0);
});
