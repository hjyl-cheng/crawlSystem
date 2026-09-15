import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {RemoteChannelRouteStore} from '../src/remoteNodes/channelRouteStore.js';
import {RemoteWorkerActivationStore,REMOTE_RUNTIME_REVISION} from '../src/remoteNodes/workerActivationStore.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {supervisionLockKey} from '../src/remoteNodes/centerExecutionRecovery.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('idle retirement fences dequeue, retries, excludes late work and never reuses removed slots',{skip:!url,timeout:40000},async t=>{
  const pool=new pg.Pool({connectionString:url,max:5});await assertIsolatedRemoteDatabase(pool);
  const schemaGuard=await pool.connect();await schemaGuard.query('SELECT pg_advisory_lock(781137981)');
  t.after(async()=>{await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');schemaGuard.release();await pool.end();});
  await pool.query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
  for(const f of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${f}`,import.meta.url),'utf8'));
  const store=new RemoteNodeStore({pool}),nodeId=randomUUID(),deploymentId=randomUUID();
  const routes=new RemoteChannelRouteStore({channelStore:new RemoteChannelPlanStore({store}),privateKey:generateKeyPairSync('ed25519').privateKey,
    secretKey:randomBytes(32),readRotaRoute:()=>{},assertBusinessFence:()=>{}});
  let processing=false;
  const image='registry.example/worker@sha256:'+'a'.repeat(64),gatewayUrl='https://center.example';
  const admin=createRemoteDeploymentAdmin({store,routes,image,gatewayUrl,token:randomBytes(32).toString('hex'),execution:{allowsNode:()=>true,isProcessing:()=>processing}});
  const plan=slots=>({nodeId,deploymentId,image,files:Object.fromEntries(slots.map(i=>[`incremental-${i}.json`,JSON.stringify({version:1,mode:'incremental_collect',role:'incremental',node_id:nodeId,deployment_id:deploymentId,slot:`incremental-${i}`,gateway_url:gatewayUrl})]))});
  await admin.prepare(plan([1,2,3]));
  await pool.query("UPDATE remote_ingestion.worker_connections SET connected_until=now()+interval '5 minutes',accepting=true,activation_requested=true WHERE node_id=$1",[nodeId]);
  const input={nodeId,deploymentId,slot:'incremental-2',operationId:randomUUID()};
  processing=true;await assert.rejects(admin.retire({...input,phase:'reserve'}),{code:'WORKER_NOT_IDLE'});processing=false;
  assert.equal((await admin.status({nodeId,deploymentId})).allowedCount,3);
  const taskId=randomUUID();
  await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state,target_node_id,target_worker_slot)
    VALUES($1::uuid,$1::text,'youtube.incremental.plan.v1','{}','{}','received',$2,$3)`,[taskId,nodeId,input.slot]);
  await assert.rejects(admin.retire({...input,phase:'reserve'}),{code:'WORKER_NOT_IDLE'});
  await pool.query("UPDATE remote_ingestion.tasks SET state='applied' WHERE task_id=$1",[taskId]);
  const guard=await pool.connect();const key=supervisionLockKey({node_id:nodeId,slot:input.slot});
  await guard.query('SELECT pg_advisory_lock(781138012,hashtext($1))',[key]);
  try{
    await admin.retire({...input,phase:'reserve'});
    assert.equal((await admin.status({nodeId,deploymentId})).allowedCount,2);
    await assert.rejects(admin.retire({...input,phase:'ready'}),{code:'WORKER_RETIREMENT_WAIT'});
    await assert.rejects(admin.retire({...input,operationId:randomUUID(),phase:'reserve'}),{code:'WORKER_RETIREMENT_CONFLICT'});
    const activation=new RemoteWorkerActivationStore({store});
    const row=(await pool.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2',[nodeId,input.slot])).rows[0];
    await assert.rejects(activation.heartbeat(nodeId,{version:1,mode:'incremental_collect',node_id:nodeId,slot:input.slot,deployment_id:deploymentId,
      config_hash:row.config_hash,instance_id:randomUUID(),relay_boot_id:randomBytes(24).toString('hex'),runtime_revision:REMOTE_RUNTIME_REVISION,accepting:true}),{code:'WORKER_DEPLOYMENT_MISMATCH'});
  }finally{await guard.query('SELECT pg_advisory_unlock(781138012,hashtext($1))',[key]);guard.release();}
  await pool.query("UPDATE remote_ingestion.tasks SET state='pending' WHERE task_id=$1",[taskId]);
  await assert.rejects(admin.retire({...input,phase:'ready'}),{code:'WORKER_NOT_IDLE'});
  await assert.rejects(admin.retire({...input,phase:'finish'}),{code:'WORKER_NOT_IDLE'});
  await pool.query("UPDATE remote_ingestion.tasks SET state='applied' WHERE task_id=$1",[taskId]);
  await pool.query('UPDATE remote_ingestion.worker_connections SET enabled=true WHERE node_id=$1 AND slot=$2',[nodeId,input.slot]);
  assert.equal((await admin.retire({...input,phase:'ready'})).ready,true,'an interrupted center does not strand retirement behind a stale enabled flag');
  assert.equal((await pool.query('SELECT enabled FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2',[nodeId,input.slot])).rows[0].enabled,false);
  assert.equal((await admin.retire({...input,phase:'finish'})).removed,true);
  assert.equal((await admin.retire({...input,phase:'finish'})).removed,true,'retry cannot decrement twice');
  assert.equal((await admin.status({nodeId,deploymentId})).workers.length,2);
  await assert.rejects(admin.prepare(plan([1,2,3])),{code:'WORKER_RETIREMENT_CONFLICT'});
  await admin.prepare(plan([1,3,4]));
  await pool.query("UPDATE remote_ingestion.worker_connections SET connected_until=now()+interval '5 minutes',accepting=true WHERE node_id=$1 AND retired_at IS NULL",[nodeId]);
  assert.equal((await admin.setExecution({nodeId,deploymentId,workerCount:3,allowedCount:3,expectedAllowedCount:2})).allowedCount,3);
  await admin.setExecution({nodeId,deploymentId,workerCount:3,allowedCount:0,expectedAllowedCount:3});
  for(const i of [1,3,4]){
    const v={nodeId,deploymentId,slot:`incremental-${i}`,operationId:randomUUID()};
    await admin.retire({...v,phase:'reserve'});await admin.retire({...v,phase:'ready'});await admin.retire({...v,phase:'finish'});
  }
  assert.equal((await admin.status({nodeId,deploymentId})).workers.length,0);
  await admin.prepare(plan([5]));
  assert.equal((await admin.status({nodeId,deploymentId})).workers[0].slot,'incremental-5');
  assert.equal((await pool.query('SELECT count(*)::int n FROM remote_ingestion.tasks WHERE task_id=$1',[taskId])).rows[0].n,1,'collection history survives');
});
