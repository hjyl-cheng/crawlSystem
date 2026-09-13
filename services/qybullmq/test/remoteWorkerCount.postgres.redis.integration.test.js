import test from 'node:test';import assert from 'node:assert/strict';import{randomUUID,randomBytes}from'node:crypto';import{readFile}from'node:fs/promises';import{setTimeout as delay}from'node:timers/promises';import pg from'pg';import{Queue}from'bullmq';
import{RemoteNodeStore}from'../src/remoteNodes/store.js';import{RemoteWorkerActivationStore,REMOTE_RUNTIME_REVISION}from'../src/remoteNodes/workerActivationStore.js';import{RemoteCenterExecutionSupervisor}from'../src/remoteNodes/centerExecutionSupervisor.js';import{assertIsolatedRemoteDatabase}from'../src/remoteNodes/isolation.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL,port=Number(process.env.REMOTE_NODE_TEST_REDIS_PORT);
async function until(check){for(let i=0;i<200;i++){if(await check())return;await delay(25)}assert.fail('worker capacity did not become ready')}
test('40 registered slots acquire distinct execution locks and process concurrently; pause retains queued work',{skip:!url||!port,timeout:60000},async t=>{
 const pool=new pg.Pool({connectionString:url,max:8}),guards=new pg.Pool({connectionString:url,max:1,connectionTimeoutMillis:2000});const lock=await pool.connect();let supervisor;let release;const barrier=new Promise(r=>release=r);const nodeId=randomUUID(),deploymentId=randomUUID();
 const connection={host:'127.0.0.1',port,password:process.env.REMOTE_NODE_TEST_REDIS_PASSWORD||'remote-center-fixture-only',maxRetriesPerRequest:null};const prefix='remote-worker-count-'+randomUUID(),queue=new Queue('youtube-channel-incremental',{connection,prefix});
 t.after(async()=>{release();await supervisor?.stop();await queue.obliterate({force:true});await queue.close();for(const table of ['worker_connections','network_slots','nodes'])await pool.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);lock.release();await guards.end();await pool.end()});
 await assertIsolatedRemoteDatabase(pool);await lock.query('SELECT pg_advisory_lock(781137981)');await pool.query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
 for(const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql','workerCountSchema.sql'])await pool.query(await readFile(new URL('../src/remoteNodes/'+file,import.meta.url),'utf8'));
 const store=new RemoteNodeStore({pool});await store.registerNode({nodeId,token:randomBytes(32).toString('hex'),capabilities:['fixture.worker-count'],maxLeases:40});
 const activation=new RemoteWorkerActivationStore({store,verifyExecution:(client,row)=>supervisor.verifyExecution(client,row)});
 for(let i=1;i<=40;i++){
  const slot='incremental-'+i;
  await pool.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,slot,'fixture-'+nodeId+'-'+i]);
  await activation.register({nodeId,slot,deploymentId,configHash:'a'.repeat(64)});
  await pool.query("UPDATE remote_ingestion.worker_connections SET instance_id=$3,relay_boot_id=$4,runtime_revision=$5,connected_until=clock_timestamp()+interval '2 minutes',accepting=true WHERE node_id=$1 AND slot=$2",[nodeId,slot,randomUUID(),'b'.repeat(48),REMOTE_RUNTIME_REVISION]);
 }
 const seen=new Set();supervisor=new RemoteCenterExecutionSupervisor({store,channelStore:{store},activation,guardPool:guards,connection,prefix,allowedNodeIds:[nodeId],
  createRuntime:x=>x,createProcessor:()=>async job=>{assert.equal(seen.has(job.id),false);seen.add(job.id);await barrier;return {ok:true}},
  createRota:args=>{let started=false;return{workerId:args.workerId,workerInstanceId:args.workerInstanceId,start:async()=>{started=true},close:async()=>{started=false},status:()=>({started,closing:false,assignment:{ready:true},active_job:false})}}
 });
 await supervisor.tick();await until(()=>supervisor.entries.size===40&&[...supervisor.entries.values()].every(e=>e.queueReady));
 await supervisor.tick();assert.equal(guards.options.max,4);assert.equal(new Set([...supervisor.entries.values()].map(e=>e.backendPid)).size,4);
 await queue.addBulk(Array.from({length:50},(_,i)=>({name:'fixture',data:{},opts:{jobId:'job-'+i}})));
 await until(async()=>{await supervisor.tick();return seen.size===40;});assert.equal(await queue.getActiveCount(),40);assert.equal(await queue.getWaitingCount(),10);
 await pool.query('UPDATE remote_ingestion.worker_connections SET activation_requested=false WHERE node_id=$1',[nodeId]);await supervisor.tick();
 release();await until(()=>supervisor.entries.size===0);assert.equal(seen.size,40);assert.equal(await queue.getWaitingCount(),10);
});
