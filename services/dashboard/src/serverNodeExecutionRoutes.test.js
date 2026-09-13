import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {serverNodesRoutes} from './serverNodesRoutes.js';
import {allowDashboardRequestDuringControlledMigration} from './controlledWritePolicy.js';
test('node intake actions are explicit, versioned and scoped to its saved deployment',async t=>{
  const node={id:'54a7cdd3-eb9c-4713-8d2f-21f4a5279de0',kind:'execution',deployment:{state:'connected',deploymentId:'41041afe-bc67-418b-90bc-8a1a81499d65',desiredCount:3}};
  const registry={version:52,nodes:[node]},calls=[];
  const app=express();app.use(express.json());app.use((req,res,next)=>allowDashboardRequestDuringControlledMigration(req.method,req.path)?next():res.sendStatus(423));
  app.use(serverNodesRoutes({store:{load:async()=>registry},layout:()=>'',executionControl:{status:async()=>({requested:false,workers:[]}),setExecution:async input=>{calls.push(input);return {requested:input.enabled};}}}));
  const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>new Promise(r=>server.close(r)));
  const path=`http://127.0.0.1:${server.address().port}/api/server-nodes/${node.id}/execution`;
  const send=input=>fetch(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(input)});
  assert.equal((await fetch(path)).status,200);assert.equal(calls.length,0);
  assert.equal((await send({version:51,enabled:true,expectedRequested:false})).status,409);
  assert.equal((await send({version:52,enabled:true,expectedRequested:false,workerCount:100})).status,400);
  node.kind='center';assert.equal((await send({version:52,enabled:true,expectedRequested:false})).status,409);node.kind='execution';
  node.deployment.state='running';assert.equal((await send({version:52,enabled:true,expectedRequested:false})).status,409);node.deployment.state='connected';
  assert.equal(calls.length,0);
  assert.equal((await send({version:52,enabled:true,expectedRequested:false})).status,200);
  assert.deepEqual(calls[0],{nodeId:node.id,deploymentId:node.deployment.deploymentId,workerCount:3,enabled:true,expectedRequested:false});
  assert.equal((await send({version:52,enabled:false,expectedRequested:true})).status,200);
  assert.equal(calls[1].enabled,false);
  assert.equal((await send({version:52,allowedCount:2,expectedAllowedCount:3})).status,200);
  assert.deepEqual(calls[2],{nodeId:node.id,deploymentId:node.deployment.deploymentId,workerCount:3,allowedCount:2,expectedAllowedCount:3});
  assert.equal((await send({version:52,allowedCount:-1,expectedAllowedCount:3})).status,400);
  assert.equal((await send({version:52,allowedCount:1.2,expectedAllowedCount:3})).status,400);
  node.deployment={...node.deployment,state:'failed',desiredCount:20,appliedCount:3};
  assert.equal((await send({version:52,allowedCount:1,expectedAllowedCount:3})).status,200);
  assert.equal(calls.at(-1).workerCount,3,'failed registration growth must use the installed count');
  assert.equal((await send({version:52,allowedCount:20,expectedAllowedCount:3})).status,400);
  assert.equal((await send({version:52,allowedCount:0,expectedAllowedCount:1})).status,200);
  assert.equal((await send({version:52,allowedCount:3,expectedAllowedCount:0})).status,200,'failed expansion cannot prevent restarting the installed fleet');
  assert.equal(calls.at(-1).workerCount,3);
  assert.equal(allowDashboardRequestDuringControlledMigration('POST','/queues/pause'),false);
});

test('the center exposes count control without remote deployment or editable registration',async t=>{
  const calls=[];const app=express();app.use(express.json());
  app.use((req,res,next)=>allowDashboardRequestDuringControlledMigration(req.method,req.path)?next():res.sendStatus(423));
  app.use(serverNodesRoutes({store:{load:async()=>({version:1,nodes:[]})},layout:()=>'',deploymentEnvironment:{SERVER_NODE_LOCAL_INTAKE_CONTROL:'true'},
    executionControl:{status:async()=>({counts:{deployed:20}}),setExecution:async value=>{calls.push(value);return {allowedCount:value.allowedCount};}}}));
  const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>new Promise(r=>server.close(r)));
  const root=`http://127.0.0.1:${server.address().port}`;
  const list=await(await fetch(root+'/api/server-nodes')).json();assert.equal(list.nodes[0].id,'local-center');
  const response=await fetch(root+'/api/server-nodes/local-center/execution',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({version:1,allowedCount:5,expectedAllowedCount:20})});
  assert.equal(response.status,200);assert.deepEqual(calls,[{nodeId:'local-center',workerCount:20,allowedCount:5,expectedAllowedCount:20}]);
});
