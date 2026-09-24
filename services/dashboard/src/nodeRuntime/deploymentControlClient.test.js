import test from 'node:test';
import assert from 'node:assert/strict';
import {createDeploymentControlClient} from './deploymentControlClient.js';
const clientFor=body=>createDeploymentControlClient({url:'https://center.example',token:'a'.repeat(32),fetchImpl:async()=>new Response(JSON.stringify(body),{status:503})});
test('execution reports a deferred start without deployment-registration wording',async()=>{
 await assert.rejects(clientFor({error:'REMOTE_NETWORK_CAPACITY_UNAVAILABLE',reason:'RESOURCE_SYNC_DEFERRED',attempts:3}).setExecution({enabled:true}),e=>e.statusCode===503&&e.reason==='RESOURCE_SYNC_DEFERRED'&&e.message.includes('本次启动未成功')&&!e.message.includes('登记'));
});
test('unknown upstream reasons never become user-controlled text',async()=>{
 await assert.rejects(clientFor({error:'REMOTE_NETWORK_CAPACITY_UNAVAILABLE',reason:'secret-token'}).setExecution({enabled:true}),e=>!e.message.includes('secret')&&e.reason===undefined&&e.message.includes('启动'));
});
test('transport failure of execution has an uncertain result',async()=>{
 const client=createDeploymentControlClient({url:'https://center.example',token:'a'.repeat(32),fetchImpl:async()=>{throw new Error('private address')}});
 await assert.rejects(client.setExecution({enabled:true}),{code:'EXECUTION_RESULT_UNKNOWN',statusCode:503});
});
test('a deployment retains the existing registration semantics',async()=>{
 await assert.rejects(clientFor({error:'REMOTE_NETWORK_CAPACITY_UNAVAILABLE'}).prepare({}),e=>e.message.includes('节点登记已保留'));
});
