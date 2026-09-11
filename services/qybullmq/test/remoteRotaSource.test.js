import test from 'node:test';
import assert from 'node:assert/strict';
import { createRotaRemoteRouteReader } from '../src/remoteNodes/rotaRouteSource.js';

const fence = { slot_name: 'channel-slot-1', worker_id: 'remote-worker', worker_instance_id: 'instance-1', lease_id: 'lease-1',
  route_generation: 2, task_id: 'rota-task-1', business_run_id: 'incremental:plan-1', job_execution_id: 'clock:plan-1:1' };
const current = Date.now();
const reply = { ...fence, ok: true, workload_scope: 'test-only', credential_generation: 3, network_identity_key: 'net-1', profile_epoch: 2,
  identity_policy_id: 'channel-policy', identity_policy_version: 1, identity_policy_hash: 'sha256:policy', egress_country: 'BR',
  server_time: new Date(current + 10000).toISOString(), lease_until: new Date(current + 40000).toISOString(),
  upstream: { protocol: 'http', address: 'assigned.example:8080', username: 'user', password: 'private-test-secret' } };
const options = { url: 'https://rota.internal/internal/v1/remote-route', token: 'center-reader-'.repeat(3) };

test('Rota source uses only the frozen task fence and charges round-trip time to lease', async () => {
  let clock = current;
  const read = createRotaRemoteRouteReader({ ...options, now: () => clock, fetchImpl: async (_url, config) => {
    assert.deepEqual(JSON.parse(config.body), fence);
    assert.equal(config.redirect, 'error');
    assert.equal(config.headers.authorization, `Bearer ${options.token}`);
    clock += 2000;
    return Response.json(reply);
  } });
  const result = await read({ ...fence, proxy_id: 9999, address: 'attacker.example' });
  assert.equal(result.route_lease_until_ms, current + 30000);
  assert.equal(result.route_lease_until_ms - clock, 28000);
  assert.equal(result.egress_country, 'BR');
  assert.deepEqual(result.upstream, reply.upstream);
  assert.equal(result.token, undefined);
});

test('Rota source rejects stale routes and identity omissions before signing', async () => {
  for (const change of [{ task_id: 'different' }, { lease_id: 'different' }, { route_generation: 3 }, { worker_instance_id: 'old' },
    { identity_policy_hash: '' }, { egress_country: 'Brazil' }, { lease_until: reply.server_time }, { server_time: 'invalid' }]) {
    const read = createRotaRemoteRouteReader({ ...options, now: () => current, fetchImpl: async () => Response.json({ ...reply, ...change }) });
    await assert.rejects(read(fence), /ROTA_ROUTE_(RESPONSE_MISMATCH|LEASE_EXPIRING)/);
  }
});

test('Rota source never exposes raw credential-bearing failures and requires protected transport', async () => {
  for (const fetchImpl of [async () => new Response('private-test-secret', { status: 503 }),
    async () => new Response('private-test-secret', { status: 200 }),
    async () => { throw new Error('network failure private-test-secret'); }]) {
    const read = createRotaRemoteRouteReader({ ...options, fetchImpl });
    await assert.rejects(read(fence), error => !JSON.stringify(error).includes('private-test-secret')
      && !error.message.includes('private-test-secret') && error.cause === undefined);
  }
  assert.throws(() => createRotaRemoteRouteReader({ ...options, url: 'http://rota.internal/internal/v1/remote-route' }), /protected/);
  assert.throws(() => createRotaRemoteRouteReader({ ...options, url: 'https://rota.internal/api/v1/proxy-control/claim' }), /protected/);
});
