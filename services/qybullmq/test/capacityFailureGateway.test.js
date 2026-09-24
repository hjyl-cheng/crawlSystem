import test from 'node:test';
import assert from 'node:assert/strict';
import {once} from 'node:events';
import {createRemoteNodeGateway} from '../src/remoteNodes/gateway.js';
import {RemoteProtocolError} from '../src/remoteNodes/protocol.js';
test('capacity reason survives the authenticated gateway without arbitrary upstream data',async t=>{
 let reason='RESOURCE_SYNC_DEFERRED';
 const server=createRemoteNodeGateway({store:{},deploymentAdmin:{authenticate:()=>{},setExecution:async()=>{throw Object.assign(new RemoteProtocolError('REMOTE_NETWORK_CAPACITY_UNAVAILABLE',503),{reason,attempts:3,payload:'secret fixture'});}}});
 server.listen(0,'127.0.0.1');await once(server,'listening');t.after(()=>new Promise(r=>server.close(r)));
 const post=()=>fetch(`http://127.0.0.1:${server.address().port}/internal/node-deployments/execution`,{method:'POST',headers:{authorization:'Bearer fixture'},body:'{}'});
 let r=await post();assert.equal(r.status,503);assert.deepEqual(await r.json(),{error:'REMOTE_NETWORK_CAPACITY_UNAVAILABLE',reason,attempts:3});
 reason='secret fixture';r=await post();assert.deepEqual(await r.json(),{error:'REMOTE_NETWORK_CAPACITY_UNAVAILABLE'});
});
