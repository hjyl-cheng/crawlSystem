import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {RemoteChannelRouteStore} from '../src/remoteNodes/channelRouteStore.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {DISCOVER_PAGE_CAPABILITY} from '../src/remoteNodes/discoverPageContract.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('Discover nodes deploy with their own image, slots and capability, and retire through the Discover execution',{skip:!url,timeout:40000},async t=>{
  const pool=new pg.Pool({connectionString:url,max:5});await assertIsolatedRemoteDatabase(pool);
  const schemaGuard=await pool.connect();await schemaGuard.query('SELECT pg_advisory_lock(781137981)');
  t.after(async()=>{await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');schemaGuard.release();await pool.end();});
  await pool.query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
  for(const file of ['schema.sql','routeSchema.sql','natsSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql',
    'wholeChannelSchema.sql','fullCrawlSchema.sql','discoverSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  const store=new RemoteNodeStore({pool}),nodeId=randomUUID(),deploymentId=randomUUID();
  const routes=new RemoteChannelRouteStore({channelStore:new RemoteChannelPlanStore({store}),privateKey:generateKeyPairSync('ed25519').privateKey,
    secretKey:randomBytes(32),readRotaRoute:()=>assert.fail('deployment must not allocate routes'),assertBusinessFence:()=>assert.fail('deployment must not execute pages')});
  const image='registry.example/incremental@sha256:'+'a'.repeat(64),discoverImage='registry.example/discover@sha256:'+'d'.repeat(64);
  const gatewayUrl='https://center.example',token=randomBytes(32).toString('hex');
  assert.throws(()=>createRemoteDeploymentAdmin({store,routes,image,gatewayUrl,token,discover:{image}}),/discover image/);
  assert.throws(()=>createRemoteDeploymentAdmin({store,routes,image,gatewayUrl,token,discover:{image:'registry.example/discover:latest'}}),/discover image/);
  let processing=false;const incrementalCalls=[];
  const discoverExecution={allowsNode:()=>true,isProcessing:()=>processing,isWorkerPaused:()=>false};
  const admin=createRemoteDeploymentAdmin({store,routes,image,gatewayUrl,token,
    execution:{allowsNode:()=>assert.fail('Discover rows never use the incremental supervisor'),isProcessing:row=>{incrementalCalls.push(row.slot);return false;}},
    discover:{image:discoverImage,execution:discoverExecution,activation:null}});
  const config=(slot,mode='discover_collect',role='discover')=>JSON.stringify({version:1,mode,role,node_id:nodeId,deployment_id:deploymentId,slot,gateway_url:gatewayUrl})+'\n';
  const plan=(slots,value=discoverImage)=>({nodeId,deploymentId,image:value,files:Object.fromEntries(slots.map(slot=>[`${slot}.json`,config(slot)]))});

  await assert.rejects(admin.prepare({...plan(['incremental-1']),files:{'incremental-1.json':config('incremental-1')}}),{code:'INVALID_DEPLOYMENT'});
  await assert.rejects(admin.prepare(plan(['discover-1'],'registry.example/other@sha256:'+'b'.repeat(64))),{code:'INVALID_DEPLOYMENT'});
  const first=await admin.prepare(plan(['discover-1','discover-2']));
  assert.equal(first.readyForTasks,false);assert.deepEqual(Object.keys(first.relayTokens).sort(),['discover-1','discover-2']);
  const node=(await pool.query('SELECT capabilities,max_leases FROM remote_ingestion.nodes WHERE node_id=$1',[nodeId])).rows[0];
  assert.deepEqual(node.capabilities,[DISCOVER_PAGE_CAPABILITY]);assert.equal(node.max_leases,2);
  const workers=(await pool.query('SELECT slot,role,mode FROM remote_ingestion.worker_connections WHERE node_id=$1 ORDER BY slot',[nodeId])).rows;
  assert.deepEqual(workers,[{slot:'discover-1',role:'discover',mode:'discover_collect'},{slot:'discover-2',role:'discover',mode:'discover_collect'}]);
  // An incremental deployment cannot take over a Discover node.
  await assert.rejects(admin.prepare({nodeId,deploymentId,image,files:{'incremental-1.json':config('incremental-1','incremental_collect','incremental')}}),
    {code:'REMOTE_DEPLOYMENT_NODE_CONFLICT'});
  assert.deepEqual(await admin.prepare(plan(['discover-1','discover-2'])),first,'a retried prepare returns the same credentials');

  const status=await admin.status({nodeId,deploymentId});
  assert.deepEqual(status.workers.map(w=>w.slot),['discover-1','discover-2']);assert.equal(status.executionAvailable,true);
  await pool.query("UPDATE remote_ingestion.worker_connections SET connected_until=now()+interval '5 minutes',accepting=true WHERE node_id=$1",[nodeId]);
  await pool.query("UPDATE remote_ingestion.nodes SET state='active' WHERE node_id=$1",[nodeId]);
  assert.equal((await admin.setExecution({nodeId,deploymentId,workerCount:2,enabled:true,expectedRequested:false})).intakeEnabled,true);
  const deploymentOnly=createRemoteDeploymentAdmin({store,routes,image,gatewayUrl,token,
    discover:{image:discoverImage,execution:{allowsNode:()=>false,isProcessing:()=>false,isWorkerPaused:()=>false},activation:null}});
  assert.equal((await deploymentOnly.status({nodeId,deploymentId})).executionAvailable,false,'deployment without Discover execution never takes pages');
  assert.equal((await admin.setExecution({nodeId,deploymentId,workerCount:2,enabled:false,expectedRequested:true})).intakeEnabled,false);

  const input={nodeId,deploymentId,slot:'discover-2',operationId:randomUUID()};
  processing=true;await assert.rejects(admin.retire({...input,phase:'reserve'}),{code:'WORKER_NOT_IDLE'});processing=false;
  const taskId=randomUUID();
  await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state,target_node_id,target_worker_slot)
    VALUES($1::uuid,$1::text,$4,'{}','{}','leased',$2,$3)`,[taskId,nodeId,input.slot,DISCOVER_PAGE_CAPABILITY]);
  await assert.rejects(admin.retire({...input,phase:'reserve'}),{code:'WORKER_NOT_IDLE'},'an open Discover delivery blocks retirement');
  await pool.query("UPDATE remote_ingestion.tasks SET state='failed' WHERE task_id=$1",[taskId]);
  await admin.retire({...input,phase:'reserve'});
  assert.equal((await admin.retire({...input,phase:'ready'})).ready,true);
  assert.equal((await admin.retire({...input,phase:'finish'})).removed,true);
  assert.equal((await pool.query('SELECT worker_count FROM remote_ingestion.node_deployments WHERE node_id=$1',[nodeId])).rows[0].worker_count,1);
  assert.deepEqual((await admin.status({nodeId,deploymentId})).workers.map(w=>w.slot),['discover-1']);
  assert.deepEqual(incrementalCalls,[],'no Discover slot is inspected through the incremental supervisor');
});
