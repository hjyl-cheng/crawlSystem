import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import { serverNodesRoutes } from './serverNodesRoutes.js';
import { allowDashboardRequestDuringControlledMigration } from './controlledWritePolicy.js';

test('environment action accepts only version and ephemeral password; deployment remains blocked', async t => {
  const calls = [];
  const registry = { version: 7, nodes: [{ id: '65e95c15-0311-4079-a90c-bdf887db6604', kind: 'execution',
    provisioning: { state: 'ready' }, runtime: { state: 'ready' }, workers: [{ role: 'incremental', count: 3 }] }] };
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => allowDashboardRequestDuringControlledMigration(req.method, req.path) ? next() : res.sendStatus(423));
  app.use(serverNodesRoutes({ store: { load: async () => registry }, layout: ({ body }) => body,
    deploymentEnvironment: { SERVER_NODE_GATEWAY_URL: 'https://center.example/remote-node', SERVER_NODE_CONNECTION_IMAGE: 'registry.example/node@sha256:' + 'a'.repeat(64) },
    runtime: { async start(input) { calls.push(input); return registry; } } }));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const path = '/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604';
  const send = (suffix, body, type = 'application/json') => fetch(base + path + suffix, { method: 'POST', headers: { 'Content-Type': type }, body: JSON.stringify(body) });
  assert.equal((await (await fetch(base + '/api/server-nodes')).json()).capabilities.runtime, true);
  assert.equal((await send('/prepare-runtime', {}, 'text/plain')).status, 415);
  for (const extra of [{ script: 'unsafe command' }, { image: 'custom' }, { workers: 4 }]) {
    assert.equal((await send('/prepare-runtime', { version: 7, ...extra })).status, 400);
  }
  assert.equal(calls.length, 0);
  const response = await send('/prepare-runtime', { version: 7, password: 'test-secret' });
  assert.equal(response.status, 202);
  assert.deepEqual(await response.json(), registry);
  assert.deepEqual(calls, [{ id: path.split('/').at(-1), version: 7, password: 'test-secret' }]);
  assert.equal((await send('/deploy', {})).status, 423);
  const preview = await (await fetch(base + path + '/worker-deployment')).json();
  assert.equal(preview.available, true);
  assert.equal(preview.count, 3);
  assert.equal(preview.readyForTasks, false);
  assert.equal(preview.version, 7);
  assert.equal(registry.nodes[0].deployment, undefined);
  assert.equal(calls.length, 1, 'reading a deployment recipe must not start remote operations');
});
