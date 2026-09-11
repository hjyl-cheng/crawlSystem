import test from 'node:test';
import assert from 'node:assert/strict';
import { buildNodeConnectionDeployment, connectionDeploymentPreview } from './nodeRuntime/connectionDeployment.js';

const node = { id: 'b18d8455-7881-43be-ae52-18cfcf160514', kind: 'execution', provisioning: { state: 'ready' }, runtime: { state: 'ready' }, workers: [{ role: 'incremental', count: 3 }] };
const image = 'registry.example/qy-node@sha256:' + 'a'.repeat(64);
test('saved worker count becomes isolated connection containers with no embedded credentials or exposed ports', () => {
  const input = structuredClone(node);
  const plan = buildNodeConnectionDeployment({ node: input, gatewayUrl: 'https://gateway.example/remote-node', image });
  assert.deepEqual(input, node);
  assert.equal(plan.count, 3);
  assert.equal(plan.readyForTasks, false);
  assert.equal(plan.memoryLimitMiB, 768);
  assert.deepEqual(Object.keys(plan.compose.services), ['incremental-1', 'incremental-2', 'incremental-3']);
  for (const [slot, service] of Object.entries(plan.compose.services)) {
    assert.equal(service.image, image);
    assert.equal(service.ports, undefined);
    assert.equal(service.network_mode, undefined);
    assert.equal(service.environment, undefined);
    assert.equal(service.user, '1000:1000');
    assert.equal(service.read_only, true);
    assert.ok(service.volumes.every(volume => volume.read_only && volume.bind.create_host_path === false));
    const config = JSON.parse(plan.files[`${slot}.json`]);
    assert.equal(config.slot, slot); assert.equal(config.mode, 'connect_only');
    assert.ok(plan.registrations.some(item => item.slot === slot && /^[a-f0-9]{64}$/.test(item.configHash)));
  }
  assert.ok(!JSON.stringify(plan).includes('docker.sock'));
});

test('unsupported roles, missing setup, mutable images and unsafe endpoints block the recipe', () => {
  assert.equal(connectionDeploymentPreview(node, {}).available, false);
  for (const patch of [{ node: { ...node, runtime: null } }, { node: { ...node, workers: [{ role: 'discover', count: 1 }] } },
    { image: 'registry.example/node:latest' }, { gatewayUrl: 'http://gateway.example' }, { gatewayUrl: 'https://user:secret@gateway.example' }]) {
    assert.throws(() => buildNodeConnectionDeployment({ node, image, gatewayUrl: 'https://gateway.example', ...patch }));
  }
});
