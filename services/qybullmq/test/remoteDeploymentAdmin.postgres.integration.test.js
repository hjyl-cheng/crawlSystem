import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import {once} from 'node:events';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {RemoteChannelRouteStore} from '../src/remoteNodes/channelRouteStore.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {createRemoteNodeGateway} from '../src/remoteNodes/gateway.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
test('Dashboard registration is atomic, encrypted, repeatable and supports additive deployment only',{skip:!url},async t=>{
 const pool=new pg.Pool({connectionString:url,max:5});const guard=await pool.connect();let server;const nodeId=randomUUID();
 t.after(async()=>{if(server)await new Promise(resolve=>{server.close(resolve);server.closeAllConnections();});
   for(const table of ['node_deployments','worker_connections','network_slots','nodes'])await pool.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);
   guard.release();await pool.end();});
 await assertIsolatedRemoteDatabase(pool);await guard.query('SELECT pg_advisory_lock(781137981)');
 for(const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
 const store=new RemoteNodeStore({pool});const routes=new RemoteChannelRouteStore({channelStore:new RemoteChannelPlanStore({store}),
   readRotaRoute:()=>assert.fail('deployment must not allocate routes'),assertBusinessFence:()=>assert.fail('deployment must not execute channels'),
   secretKey:randomBytes(32),privateKey:generateKeyPairSync('ed25519').privateKey});
 const image='registry.example/collect@sha256:'+'a'.repeat(64);const token=randomBytes(32).toString('hex');const gatewayUrl='https://center.example/remote';
 const admin=createRemoteDeploymentAdmin({store,routes,image,token,gatewayUrl});
 server=createRemoteNodeGateway({store,routes,deploymentAdmin:admin});server.listen(0,'127.0.0.1');await once(server,'listening');
 const endpoint=`http://127.0.0.1:${server.address().port}`;
 const deploymentId=randomUUID();const plan=count=>({nodeId,deploymentId,image,files:Object.fromEntries(Array.from({length:count},(_,i)=>[
   `incremental-${i+1}.json`,JSON.stringify({version:1,mode:'incremental_collect',role:'incremental',node_id:nodeId,deployment_id:deploymentId,slot:`incremental-${i+1}`,gateway_url:gatewayUrl})+'\n']))});
 const post=(value,auth=token)=>fetch(endpoint+'/internal/node-deployments/prepare',{method:'POST',headers:{authorization:`Bearer ${auth}`,'content-type':'application/json'},body:JSON.stringify(value)});
 assert.equal((await post(plan(1),'bad')).status,401);
 const before=(await pool.query('SELECT task_id,state,generation FROM remote_ingestion.tasks ORDER BY task_id')).rows;
 const first=await (await post(plan(1))).json();assert.equal(first.readyForTasks,false);assert.match(first.nodeToken,/^[a-f0-9]{64}$/);
 assert.deepEqual(await (await post(plan(1))).json(),first);
 assert.equal((await post(plan(1),first.nodeToken)).status,401);
 const expanded=await (await post(plan(2))).json();assert.equal(expanded.nodeToken,first.nodeToken);
 assert.equal(expanded.relayTokens['incremental-1'],first.relayTokens['incremental-1']);assert.equal(Object.keys(expanded.relayTokens).length,2);
 assert.equal((await post(plan(1))).status,409);
 assert.equal((await post({...plan(2),image:'registry.example/other@sha256:'+'b'.repeat(64)})).status,400);
 const bad=plan(3);bad.files['incremental-1.json']=bad.files['incremental-1.json'].replace('https://center.example/remote','https://wrong.example');
 assert.notEqual((await post(bad)).status,200);
 assert.equal((await pool.query('SELECT max_leases FROM remote_ingestion.nodes WHERE node_id=$1',[nodeId])).rows[0].max_leases,2);
 const persisted=(await pool.query('SELECT * FROM remote_ingestion.node_deployments WHERE node_id=$1',[nodeId])).rows[0];
 assert.ok(Buffer.isBuffer(persisted.credentials_cipher));assert.equal(persisted.credentials_cipher.includes(Buffer.from(first.nodeToken)),false);
 const status=await admin.status({nodeId,deploymentId});assert.equal(status.workers.length,2);assert.ok(status.workers.every(w=>!w.connected&&!w.readyForTasks));
 assert.deepEqual((await pool.query('SELECT task_id,state,generation FROM remote_ingestion.tasks ORDER BY task_id')).rows,before);
});
