import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createNodeWorkerDeployment } from './serverNodeWorkerDeployment.js';

for (const initial of [null, 0, 2]) test(`deployment preserves intake ${initial ?? 'new node'} even when an old browser submits auto-sync`, async () => {
  const node={id:randomUUID(),name:'Fixture',host:'192.0.2.8',port:22,username:'ubuntu',kind:'execution',
    workers:[{role:'incremental',count:3}],provisioning:{state:'ready'},runtime:{state:'ready'},
    deployment:{deploymentId:randomUUID(),appliedCount:3,desiredCount:3,state:'connected'}};
  if(initial===null)delete node.deployment;
  const installed=initial===null?0:3;
  let allowed=initial??0;let saved;
  const store={load:async()=>({version:1,nodes:[node]}),
    beginWorkerDeployment:async input=>{saved=input;return {version:2,nodes:[node]};},
    advanceWorkerDeployment:async(_id,_op,patch)=>Object.assign(node.deployment??={},patch)};
  const center={prepare:async plan=>({nodeId:node.id,deploymentId:plan.deploymentId,readyForTasks:false,
    nodeToken:'a'.repeat(64),publicKey:'fixture',relayTokens:Object.fromEntries(plan.registrations.map(r=>[r.slot,'b'.repeat(64)]))}),
    status:async plan=>({nodeId:node.id,deploymentId:plan.deploymentId,allowedCount:allowed,workers:plan.registrations.map(r=>({slot:r.slot,connected:true}))}),
    setExecution:async value=>{allowed=value.allowedCount;}};
  const deployment=createNodeWorkerDeployment({store,center,image:'registry.example/worker@sha256:'+'a'.repeat(64),
    gatewayUrl:'https://center.example',natsUrl:'tls://messages.example:4222',
    ssh:{connect:async()=>({}),verify:async()=>{},deployWorkers:async()=>{},close:()=>{}}});
  await deployment.start({id:node.id,version:1,additionalCount:1,role:'incremental',expectedInstalledCount:installed,syncIntake:true,expectedAllowedCount:initial??0});
  await deployment.waitForIdle();
  assert.equal(node.deployment.state,'connected');assert.equal(node.deployment.appliedCount,installed+1);
  assert.equal(allowed,initial??0,'deployment cannot authorize more intake');
  assert.equal(saved.syncIntake,false,'persist no deferred auto-start operation');
});
