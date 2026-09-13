import test from 'node:test';import assert from 'node:assert/strict';
import{buildNodeCollectDeployment}from'./nodeRuntime/collectDeployment.js';import{normalizeServerNode}from'./serverNodes.js';
const node={id:'b18d8455-7881-43be-ae52-18cfcf160514',name:'node',host:'192.0.2.1',port:22,username:'ubuntu',kind:'execution',provisioning:{state:'ready'},runtime:{state:'ready'},workers:[{role:'incremental',count:150}]};
test('node configuration and collecting recipe support counts above 32 without changing old slots',()=>{
 const {id,provisioning,runtime,...input}=node;assert.equal(normalizeServerNode(input).workers[0].count,150);
 const args={node,image:'registry.example/node@sha256:'+'a'.repeat(64),gatewayUrl:'https://center.example',deploymentId:'c18d8455-7881-43be-ae52-18cfcf160514'};
 const small=buildNodeCollectDeployment({...args,node:{...node,workers:[{role:'incremental',count:30}]}}),large=buildNodeCollectDeployment(args);
 assert.equal(large.count,150);assert.equal(large.registrations.length,150);
 for(const slot of Object.keys(small.compose.services)){assert.deepEqual(large.compose.services[slot],small.compose.services[slot]);assert.equal(large.files[slot+'.json'],small.files[slot+'.json']);}
 assert.deepEqual(large.compose.services['incremental-150'].healthcheck,{disable:true});
 for(const count of [0,-1,1.5,Infinity,Number.MAX_SAFE_INTEGER+1])assert.throws(()=>buildNodeCollectDeployment({...args,node:{...node,workers:[{role:'incremental',count}]}}));
});
