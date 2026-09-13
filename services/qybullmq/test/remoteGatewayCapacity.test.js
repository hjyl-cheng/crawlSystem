import assert from 'node:assert/strict';
import test from 'node:test';
import {once} from 'node:events';
import {createRemoteNodeGateway} from '../src/remoteNodes/gateway.js';

test('saturated collector requests do not reject authenticated management status', async t => {
  let release, entered;
  const held = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { entered = resolve; });
  const server = createRemoteNodeGateway({maxConcurrentRequests:1, maxControlRequests:1, maxPendingRequests:0,
    store:{authenticate:async () => { entered(); await held; return 'node'; },claim:async()=>null},
    deploymentAdmin:{authenticate:token=>assert.equal(token,'admin'),status:async()=>({ok:true})}});
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  t.after(async()=>{release();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));});
  const url=`http://127.0.0.1:${server.address().port}`;
  const post=path=>fetch(url+path,{method:'POST',headers:{authorization:'Bearer admin','content-type':'application/json'},
    body:JSON.stringify({claim_id:'12345678-1234-4234-8234-123456789012'})});
  const normal=post('/v1/work/claim');
  normal.catch(()=>{});
  await started;
  assert.equal((await post('/v1/work/claim')).status,503);
  assert.equal((await post('/internal/node-deployments/status')).status,200);
  release();assert.equal((await normal).status,200);
});
