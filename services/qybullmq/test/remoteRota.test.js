import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, verify } from 'node:crypto';
import { createRemoteRouteIssuer } from '../src/remoteNodes/routeGrant.js';
import { createLocalRotaClient } from '../src/remoteNodes/localRotaClient.js';

const pair = generateKeyPairSync('ed25519');
const current = Date.now();
const request = { node_id: 'node-a', slot: 'worker-1', task_id: 'task-a', generation: 4, boot_id: 'b'.repeat(48) };
const route = { ...request, epoch: 2, route_id: 'route-us', identity_id: 'identity-us', egress_country: 'US',
  proxy_token: 'x'.repeat(32), task_lease_until_ms: current + 50000, route_lease_until_ms: current + 30000,
  upstream: { protocol: 'http', address: '127.0.0.1:12345', username: 'user', password: 'secret' } };

test('central signature binds selected upstream and ends before both leases', async () => {
  let calls = 0;
  const issue = createRemoteRouteIssuer({ privateKey: pair.privateKey, now: () => current,
    authorize: async input => { calls++; assert.deepEqual(input, { ...request, action: 'activate' }); return route; } });
  const signed = await issue({ ...request, upstream: { address: 'attacker.test:80' }, egress_country: 'BR' });
  const payload = Buffer.from(signed.payload, 'base64');
  assert.ok(verify(null, payload, pair.publicKey, Buffer.from(signed.signature, 'base64')));
  const grant = JSON.parse(payload);
  assert.equal(grant.expires_at_ms, current + 28000);
  assert.equal(grant.egress_country, 'US');
  assert.deepEqual(grant.upstream, route.upstream);
  assert.equal(grant.task_lease_until_ms, undefined);
  await issue(request);
  assert.equal(calls, 2); // every authorization/renewal rechecks center state
});

test('central issuer rejects expired ownership and cross-channel or cross-node routes', async () => {
  for (const changed of [{ node_id: 'node-b' }, { task_id: 'task-b' }, { generation: 5 }, { slot: 'worker-2' },
    { route_lease_until_ms: current }, { task_lease_until_ms: current }, { egress_country: 'brazil' },
    { upstream: { protocol: 'direct', address: 'youtube.com:443' } }]) {
    const issue = createRemoteRouteIssuer({ privateKey: pair.privateKey, now: () => current, authorize: async () => ({ ...route, ...changed }) });
    await assert.rejects(issue(request), /ROUTE_|UPSTREAM/);
  }
  assert.throws(() => createRemoteRouteIssuer({ privateKey: pair.privateKey }), /configuration/);
});

test('local client refuses nonlocal endpoints and checks lease before touching relay', async () => {
  const config = { controlUrl: 'http://127.0.0.1:8001', proxyUrl: 'http://127.0.0.1:8000', token: 'x'.repeat(32), nodeId: 'node-a' };
  for (const controlUrl of ['http://center:8001', 'http://127.0.0.1:8001/extra', 'http://user:pw@127.0.0.1:8001']) {
    assert.throws(() => createLocalRotaClient({ ...config, controlUrl }), /loopback/);
  }
  let calls = 0;
  const client = createLocalRotaClient({ ...config, fetchImpl: async () => { calls++; throw new Error('must not request'); } });
  const issue = createRemoteRouteIssuer({ privateKey: pair.privateKey, now: () => current, authorize: async () => route });
  const signed = await issue(request);
  await assert.rejects(client.apply(signed, { lease: { task_id: 'other', generation: 4 }, slot: request.slot, bootId: request.boot_id }), /LEASE_MISMATCH/);
  assert.equal(calls, 0);
});
