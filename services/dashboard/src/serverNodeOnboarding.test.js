import test from 'node:test';
import assert from 'node:assert/strict';
import { createNodeOnboarding } from './serverNodeOnboarding.js';
import { validateBootstrapPassword } from './serverNodeSsh.js';

function harness({ failAt, online = true } = {}) {
  const calls = [];
  let current = { version: 1, nodes: [{ id: 'node-test', provisioning: { state: 'not_started' } }] };
  const store = {
    async beginInitialization({ operationId }) { current.nodes[0].provisioning = { state: 'running', operationId, startedAt: new Date().toISOString(), steps: {} }; return structuredClone(current); },
    async advanceInitialization(_id, op, patch) {
      assert.equal(op, current.nodes[0].provisioning.operationId);
      current.nodes[0].provisioning = { ...current.nodes[0].provisioning, ...patch, steps: { ...current.nodes[0].provisioning.steps, ...patch.steps } };
      return structuredClone(current);
    },
  };
  const record = name => { calls.push(name); if (name === failAt) throw new Error('Test failure (test-secret)'); };
  const ssh = {
    async connect(_node, options) { record(options.keyOnly ? 'key-login' : 'connect'); return { fingerprint: 'test-fingerprint' }; },
    async verify() { record('verify'); return 'x86_64'; },
    async installKey() { record('install-key'); }, async installMonitoring() { record('install-monitoring'); }, close() {},
  };
  const monitoring = { async available() {}, async prepare() { record('prepare'); return { systemId: 'test-system', token: 'agent-token-secret' }; },
    async observe() { record('observe'); return { online, sampleAt: new Date().toISOString() }; } };
  const onboarding = createNodeOnboarding({ store, ssh, monitoring, pollInterval: 1, sampleTimeout: 10 });
  return { onboarding, calls, registry: () => current };
}

test('initialization verifies key-only login and new monitoring sample before marking ready', async () => {
  const h = harness();
  await h.onboarding.start({ id: 'node-test', version: 1, password: 'test-secret' });
  await h.onboarding.waitForIdle();
  assert.deepEqual(h.calls, ['connect', 'verify', 'install-key', 'key-login', 'prepare', 'install-monitoring', 'observe']);
  assert.equal(h.registry().nodes[0].provisioning.state, 'ready');
  const saved = JSON.stringify(h.registry());
  assert.ok(!saved.includes('test-secret') && !saved.includes('agent-token-secret'));
});

test('failed authentication and key validation never install monitoring; retries can complete', async () => {
  for (const failAt of ['connect', 'key-login']) {
    const h = harness({ failAt });
    await h.onboarding.start({ id: 'node-test', version: 1, password: 'test-secret' });
    await h.onboarding.waitForIdle();
    assert.equal(h.registry().nodes[0].provisioning.state, 'failed');
    assert.ok(!h.calls.includes('install-monitoring'));
    assert.ok(!JSON.stringify(h.registry()).includes('test-secret'));
  }
});

test('monitoring installed without a fresh sample does not unlock workers', async () => {
  const h = harness({ online: false });
  await h.onboarding.start({ id: 'node-test', version: 1, password: '' });
  await h.onboarding.waitForIdle();
  assert.equal(h.registry().nodes[0].provisioning.state, 'failed');
  assert.equal(h.registry().nodes[0].provisioning.steps.metrics, 'failed');
});

test('bootstrap passwords are bounded and support punctuation without shell interpretation', () => {
  assert.equal(validateBootstrapPassword("a'\"$(`x`)!"), "a'\"$(`x`)!");
  for (const password of [null, {}, 'x\ny', 'x\0y', 'x'.repeat(1025)]) assert.throws(() => validateBootstrapPassword(password), { statusCode: 400 });
});
