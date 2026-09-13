import test from 'node:test';
import assert from 'node:assert/strict';
import {collectDeploymentPreview} from './nodeRuntime/collectDeployment.js';
import {workerDeploymentFromEnv} from './serverNodeWorkerDeployment.js';
const node={id:'b18d8455-7881-43be-ae52-18cfcf160514',kind:'execution',provisioning:{state:'ready'},runtime:{state:'ready'},workers:[{role:'incremental',count:40}]};
const env={SERVER_NODE_COLLECT_IMAGE:'registry.example/node@sha256:'+'a'.repeat(64),SERVER_NODE_GATEWAY_URL:'https://center.example',SERVER_NODE_WORKER_CONTROL_URL:'https://center.example',SERVER_NODE_WORKER_CONTROL_TOKEN_FILE:'/unused/token',SERVER_NODE_STATE_DIR:'/tmp/node-fixture'};
test('page-managed collector deployment refuses missing NATS configuration instead of silently deploying HTTP workers',()=>{
 const preview=collectDeploymentPreview(node,env);
 assert.equal(preview.available,false);
 assert.match(preview.reason,/NATS/);
 assert.equal(workerDeploymentFromEnv({},env),null);
});
test('page-managed deployment carries the NATS address to all 40 workers',()=>{
 const configured={...env,SERVER_NODE_NATS_URL:'wss://center.example/node-messages'};
 const preview=collectDeploymentPreview(node,configured);
 assert.equal(preview.available,true);assert.equal(preview.count,40);
 assert.equal(preview.wholeChannel,true);
 for(const service of Object.values(preview.compose.services))assert.equal(service.environment.REMOTE_NODE_NATS_URL,configured.SERVER_NODE_NATS_URL);
 for(const service of Object.values(preview.compose.services))assert.equal(service.environment.REMOTE_NODE_WHOLE_CHANNEL,'true');
 assert.ok(workerDeploymentFromEnv({},configured));
});

test('new nodes and expansion share the same released image and NATS endpoint',()=>{
 const configured={...env,SERVER_NODE_NATS_URL:'wss://center.example/node-messages'};
 const expanded=collectDeploymentPreview({...node,deployment:{deploymentId:'c18d8455-7881-43be-ae52-18cfcf160514'}},configured);
 const fresh=collectDeploymentPreview({...node,id:'a18d8455-7881-43be-ae52-18cfcf160514',workers:[{role:'incremental',count:3}]},configured);
 assert.equal(expanded.available,true);assert.equal(fresh.available,true);
 assert.notEqual(expanded.deploymentId,fresh.deploymentId);
 for(const plan of [expanded,fresh])for(const service of Object.values(plan.compose.services)){
   assert.equal(service.image,configured.SERVER_NODE_COLLECT_IMAGE);
   assert.equal(service.environment.REMOTE_NODE_NATS_URL,configured.SERVER_NODE_NATS_URL);
   assert.equal(service.environment.REMOTE_NODE_WHOLE_CHANNEL,'true');
 }
});
