import test from 'node:test';
import assert from 'node:assert/strict';
import { createServerNodeStore, initializationSteps } from './serverNodes.js';
import { createNodeDeletion, createUnusedNodeCheck } from './serverNodeDeletion.js';

const config = { name: '增量节点01', host: '192.0.2.12', username: 'ubuntu', port: 22, kind: 'execution', workers: [{ role: 'incremental', count: 20 }] };
export const readyNode = { ...config, id: '23b2d2ee-fa3f-4b1e-8715-6f0521043ccf', provisioning: { state: 'ready', systemId: 'system', steps: Object.fromEntries(initializationSteps.map(step => [step, 'completed'])) } };
function fixture() {
  let registry = { version: 11, nodes: [structuredClone(readyNode)] };
  const store = createServerNodeStore(async (sql, params) => {
    if (sql.startsWith('SELECT')) return { rows: [{ value_json: structuredClone(registry) }] };
    if (JSON.stringify(registry) !== params[2]) return { rowCount: 0 };
    registry = JSON.parse(params[1]); return { rowCount: 1 };
  });
  const calls = [];
  const adapters = {
    store,
    checkCenter: async () => { calls.push('center'); },
    ssh: { connect: async () => { calls.push('ssh'); return {}; }, verify: async () => {}, close: () => {},
      removeUnusedNode: async (_connection, _node, _password, mode) => { calls.push(mode); } },
    monitoring: { remove: async () => { calls.push('monitoring'); } },
  };
  const deletion = createNodeDeletion(adapters);
  return { store, adapters, calls, deletion, node: () => registry.nodes[0], registry: () => registry };
}

test('initialized node with a saved 20-worker plan can be removed only after live checks', async () => {
  const f = fixture();
  assert.equal((await f.store.deletionCheck(readyNode.id)).allowed, false, 'direct metadata path stays protected');
  const check = await f.deletion.check(readyNode.id);
  assert.equal(check.allowed, true);
  assert.equal(check.requiresRemoteCheck, true);
  assert.deepEqual(f.calls, ['center'], 'GET never changes remote state');
  await assert.rejects(() => f.deletion.remove({ id: readyNode.id, version: 10 }), { statusCode: 409 });
  const result = await f.deletion.remove({ id: readyNode.id, version: check.version, password: 'transient-secret' });
  assert.deepEqual(result.nodes, []);
  assert.deepEqual(f.calls, ['center','center','ssh','check','center','cleanup','monitoring']);
  assert.ok(!JSON.stringify(result).includes('transient-secret'));
});

test('workers, deployments, running operations and unknown metadata prevent deletion', async () => {
  for (const patch of [{ deployment: { state: 'failed' } }, { runtime: { state: 'running' } }, { extraState: {} }, { kind: 'center' }]) {
    const f = fixture(); Object.assign(f.node(), patch);
    assert.equal((await f.deletion.check(readyNode.id)).allowed, false);
    await assert.rejects(() => f.deletion.remove({ id: readyNode.id, version: 11 }), { statusCode: 409 });
    assert.deepEqual(f.calls, []);
  }
});

test('partial failures retain registration, redact errors and can retry cleanup', async () => {
  for (const failure of ['center','ssh','check','cleanup','monitoring']) {
    const f = fixture();
    if (failure === 'center') f.adapters.checkCenter = async () => { throw new Error('secret'); };
    if (failure === 'ssh') f.adapters.ssh.connect = async () => { throw new Error('secret'); };
    if (['check','cleanup'].includes(failure)) f.adapters.ssh.removeUnusedNode = async (_c,_n,_p,mode) => { if (mode === failure) throw new Error('secret'); };
    if (failure === 'monitoring') f.adapters.monitoring.remove = async () => { throw new Error('secret'); };
    const deletion = createNodeDeletion(f.adapters);
    await assert.rejects(() => deletion.remove({ id: readyNode.id, version: 11 }), error => !error.message.includes('secret'));
    assert.equal(f.node().deletion.state, 'failed');
    assert.ok(!JSON.stringify(f.registry()).includes('secret'));
    await assert.rejects(() => f.store.save({ id: readyNode.id, version: f.registry().version, node: config }), { statusCode: 409 });
    f.adapters.checkCenter = async () => {};
    f.adapters.ssh.connect = async () => ({});
    f.adapters.ssh.removeUnusedNode = async () => {};
    f.adapters.monitoring.remove = async () => {};
    assert.equal((await createNodeDeletion(f.adapters).remove({ id: readyNode.id, version: f.registry().version })).nodes.length, 0);
  }
});

test('deletion reservation fences all competing operations and duplicate deletes', async () => {
  const f = fixture();
  await f.store.beginDeletion({ id: readyNode.id, version: 11, operationId: 'one' });
  const args = { id: readyNode.id, version: f.registry().version, operationId: 'two', node: config, plan: {} };
  for (const method of ['save','beginInitialization','beginRuntime','beginWorkerDeployment','beginDeletion','remove']) {
    await assert.rejects(() => f.store[method](args), { statusCode: 409 }, method);
  }
  await assert.rejects(() => f.store.finishDeletion(readyNode.id, 'stale'), { statusCode: 409 });
  f.node().deletion.deadline = new Date(0).toISOString();
  await f.store.beginDeletion(args);
  await assert.rejects(() => f.store.finishDeletion(readyNode.id, 'one'), { statusCode: 409 });
  assert.equal((await f.store.finishDeletion(readyNode.id, 'two')).nodes.length, 0);
});

test('center checks fail closed for every kind of registration and database failure', async () => {
  const empty = { registered: false, deployed: false, workers: false, tasks: false };
  await createUnusedNodeCheck(async () => ({ rows: [empty] }))(readyNode);
  for (const key of Object.keys(empty)) {
    await assert.rejects(() => createUnusedNodeCheck(async () => ({ rows: [{ ...empty, [key]: true }] }))(readyNode), { statusCode: 409 });
  }
  await assert.rejects(() => createUnusedNodeCheck(async () => { throw new Error('connection secret'); })(readyNode), { statusCode: 503 });
});
