import test from 'node:test';
import assert from 'node:assert/strict';
import { createNodeRuntime } from './serverNodeRuntime.js';

function harness({ failAt, refuseAdmission = false } = {}) {
  let registry = { version: 1, nodes: [{ id: 'test-node', workers: [{ role: 'incremental', count: 3 }] }] };
  const calls = [];
  const store = {
    async beginRuntime({ operationId }) {
      calls.push('admit');
      if (refuseAdmission) throw Object.assign(new Error('Version changed'), { statusCode: 409 });
      registry.nodes[0].runtime = { state: 'running', operationId, steps: {} };
      return structuredClone(registry);
    },
    async advanceRuntime(_id, operationId, patch) {
      assert.equal(registry.nodes[0].runtime.operationId, operationId);
      calls.push(patch.state || Object.entries(patch.steps)[0].join(':'));
      const runtime = registry.nodes[0].runtime;
      registry.nodes[0].runtime = { ...runtime, ...patch, steps: { ...runtime.steps, ...patch.steps } };
    },
  };
  const ssh = {
    async connect(_node, options) {
      assert.equal(registry.nodes[0].runtime.state, 'running');
      assert.deepEqual(options, { keyOnly: true });
      calls.push('connect');
      return {};
    },
    async verify() { calls.push('sudo'); },
    async prepareRuntime(_connection, _node, _password, before, after) {
      for (const step of ['check', 'docker', 'layout', 'verify']) {
        await before(step);
        if (step === failAt) throw new Error('Install failed test-password');
        await after(step);
      }
      return { revision: 1, dockerVersion: '28.3.2', composeVersion: '2.39.2' };
    },
    close() { calls.push('close'); },
  };
  return { runtime: createNodeRuntime({ store, ssh }), calls, registry: () => registry, repair() { failAt = null; } };
}

test('runtime admission is durable before SSH; ordered preparation does not deploy a worker', async () => {
  const h = harness();
  await h.runtime.start({ id: 'test-node', version: 1, password: 'test-password' });
  await h.runtime.waitForIdle();
  assert.deepEqual(h.calls, ['admit', 'ssh:running', 'connect', 'sudo', 'ssh:completed', 'check:running', 'check:completed', 'docker:running', 'docker:completed', 'layout:running', 'layout:completed', 'verify:running', 'verify:completed', 'ready', 'close']);
  assert.equal(h.registry().nodes[0].runtime.state, 'ready');
  assert.deepEqual(h.registry().nodes[0].workers, [{ role: 'incremental', count: 3 }]);
  assert.equal(h.registry().nodes[0].deployment, undefined);
  assert.ok(!JSON.stringify(h.registry()).includes('test-password'));
});

test('failed installation records the failed step and redacts password; retry completes', async () => {
  const h = harness({ failAt: 'docker' });
  await h.runtime.start({ id: 'test-node', version: 1, password: 'test-password' });
  await h.runtime.waitForIdle();
  assert.equal(h.registry().nodes[0].runtime.state, 'failed');
  assert.equal(h.registry().nodes[0].runtime.steps.docker, 'failed');
  assert.ok(!h.calls.includes('layout:running'));
  assert.ok(!JSON.stringify(h.registry()).includes('test-password'));
  h.repair();
  await h.runtime.start({ id: 'test-node', version: 1 });
  await h.runtime.waitForIdle();
  assert.equal(h.registry().nodes[0].runtime.state, 'ready');
});

test('a rejected admission makes no SSH connection', async () => {
  const h = harness({ refuseAdmission: true });
  await assert.rejects(() => h.runtime.start({ id: 'test-node', version: 0 }), { statusCode: 409 });
  assert.deepEqual(h.calls, ['admit']);
});
