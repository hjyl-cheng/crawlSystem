import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {once} from 'node:events';
import express from 'express';
import pg from 'pg';
import {createServerNodeStore} from './serverNodes.js';
import {createNodeWorkerRemoval} from './serverNodeWorkerRemoval.js';
import {buildNodeCollectDeployment} from './nodeRuntime/collectDeployment.js';
import {serverNodesRoutes} from './serverNodesRoutes.js';
import {allowDashboardRequestDuringControlledMigration} from './controlledWritePolicy.js';

const url=process.env.SERVER_NODES_TEST_DATABASE_URL;
test('manager removal is durable, versioned, retries without losing slots, and blocks concurrent deployment',{skip:!url,timeout:20000},async t=>{
  assert.equal(new URL(url).pathname,'/server_nodes_dashboard_test');
  const pool=new pg.Pool({connectionString:url,max:3});let server;
  t.after(async()=>{if(server){server.closeAllConnections();await new Promise(r=>server.close(r));}await pool.end();});
  await pool.query('CREATE SCHEMA IF NOT EXISTS crawler;CREATE TABLE IF NOT EXISTS crawler.settings(setting_key TEXT PRIMARY KEY,value_json JSONB NOT NULL,updated_at TIMESTAMPTZ DEFAULT now())');
  const node={id:randomUUID(),kind:'execution',name:'fixture',host:'192.0.2.10',port:22,username:'ubuntu',
    workers:[{role:'incremental',count:3}],provisioning:{state:'ready'},runtime:{state:'ready'},deployment:{state:'connected',mode:'incremental_collect',
      image:'registry.example/worker@sha256:'+'a'.repeat(64),deploymentId:randomUUID(),appliedCount:3,desiredCount:3}};
  await pool.query("INSERT INTO crawler.settings(setting_key,value_json) VALUES('dashboard_server_nodes_v1',$1) ON CONFLICT(setting_key) DO UPDATE SET value_json=EXCLUDED.value_json",[{version:1,nodes:[node]}]);
  const store=createServerNodeStore(pool.query.bind(pool));let failReserve=true,failRemote=false,ready=false,removes=0;const operations=[];
  const center={async retire(v){operations.push(v);if(v.phase==='reserve'&&failReserve)throw Object.assign(Error('busy'),{code:'WORKER_NOT_IDLE'});if(v.phase==='ready')ready=true;return {...v,removed:v.phase==='finish'};}};
  const ssh={connect:async()=>({}),verify:async()=>{},close:()=>{},removeWorker:async(_c,_n,value)=>{assert.equal(ready,true);removes++;if(failRemote)throw Error('private diagnostic');}};
  const removal=createNodeWorkerRemoval({store,center,ssh,pollMs:1,waitMs:20});
  const app=express();app.use(express.json());app.use((req,res,next)=>allowDashboardRequestDuringControlledMigration(req.method,req.path)?next():res.sendStatus(423));
  app.use(serverNodesRoutes({store,workerRemoval:removal,layout:({body})=>body}));server=app.listen(0,'127.0.0.1');await once(server,'listening');
  const post=body=>fetch(`http://127.0.0.1:${server.address().port}/api/server-nodes/${node.id}/remove-worker`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  assert.equal((await post({version:1,slot:'../../other'})).status,400);
  assert.equal((await post({version:0,slot:'incremental-2'})).status,409);
  assert.equal((await post({version:1,slot:'incremental-2'})).status,202);await removal.waitForIdle();
  let registry=await store.load();assert.equal(registry.nodes[0].workerRemoval.state,'rejected');assert.equal(removes,0);
  assert.equal(registry.nodes[0].deployment.appliedCount,3,'busy rejection does not claim deletion');
  failReserve=false;failRemote=true;
  assert.equal((await post({version:registry.version,slot:'incremental-2'})).status,202);await removal.waitForIdle();
  registry=await store.load();const operation=registry.nodes[0].workerRemoval.operationId;
  assert.equal(registry.nodes[0].workerRemoval.state,'failed');assert.equal(registry.nodes[0].deployment.appliedCount,3);
  assert.ok(!JSON.stringify(registry).includes('private diagnostic'));
  const args={node:{...registry.nodes[0],workers:[{role:'incremental',count:4}]},deploymentId:node.deployment.deploymentId,image:node.deployment.image,gatewayUrl:'https://center.example',natsUrl:'tls://messages.example:4222'};
  await assert.rejects(store.beginWorkerDeployment({id:node.id,version:registry.version,operationId:randomUUID(),plan:buildNodeCollectDeployment(args),count:4}),{statusCode:409});
  // A new coordinator after a dashboard restart resumes the same operation.
  failRemote=false;const resumed=createNodeWorkerRemoval({store:createServerNodeStore(pool.query.bind(pool)),center,ssh});
  await resumed.start({id:node.id,version:registry.version,slot:'incremental-2'});await resumed.waitForIdle();
  registry=await store.load();const updated=registry.nodes[0];
  assert.equal(updated.workerRemoval.operationId,operation);assert.equal(updated.workerRemoval.state,'completed');
  assert.equal(updated.deployment.appliedCount,2);assert.deepEqual(updated.deployment.slots,['incremental-1','incremental-3']);
  const next=buildNodeCollectDeployment({...args,node:{...updated,workers:[{role:'incremental',count:3}]}});
  assert.deepEqual(next.slots,['incremental-1','incremental-3','incremental-4']);assert.ok(!next.files['incremental-2.json']);
  assert.equal(next.files['incremental-1.json'],buildNodeCollectDeployment(args).files['incremental-1.json']);
  assert.equal(operations.at(-1).phase,'finish');
});
