import test from 'node:test';
import assert from 'node:assert/strict';
import { createServerNodeStore, normalizeServerNode, initializationSteps, unusedNodeDeletionEligibility } from './serverNodes.js';
import { nodeWorkerRole } from './nodeWorkerTypes.js';
import { createNodeWorkerDeployment } from './serverNodeWorkerDeployment.js';
import { buildNodeCollectDeployment } from './nodeRuntime/collectDeployment.js';

const input = { name: '采集节点', host: '192.0.2.50', port: 22, username: 'ubuntu', kind: 'execution', workers: [] };
const ready = { provisioning: { state: 'ready', systemId: 'fixture',
  steps: Object.fromEntries(initializationSteps.map(step => [step, 'completed'])) }, runtime: { state: 'ready' } };

function registryFixture(nodes = []) {
  let registry = { version: 0, nodes: structuredClone(nodes) };
  const query = async (sql, params) => {
    if (sql.startsWith('SELECT')) return { rows: [{ value_json: structuredClone(registry) }] };
    const expected = sql.startsWith('INSERT') ? params[3] : params[2];
    if (expected !== JSON.stringify(registry)) return { rowCount: 0 };
    registry = JSON.parse(params[1]);
    return { rowCount: 1 };
  };
  return { store: createServerNodeStore(query), query };
}

test('both functional types persist before initialization without creating a worker plan', async () => {
  for (const workerRole of ['incremental', 'fullcrawl']) {
    const { store, query } = registryFixture();
    const saved = await store.save({ version: 0, node: { ...input, workerRole } });
    const node = saved.nodes[0];
    const reloaded = await createServerNodeStore(query).load();
    assert.equal(reloaded.nodes[0].workerRole, workerRole);
    assert.deepEqual(node.workers, []);
    assert.deepEqual(node.provisioning, { state: 'not_started' });
    assert.equal(node.deployment, undefined);
    assert.equal((await store.deletionCheck(node.id)).allowed, true);
    // An older browser editing metadata must not reset full crawl to incremental.
    const edited = await store.save({ id: node.id, version: saved.version, node: { ...input, notes: 'renamed' } });
    assert.equal(edited.nodes[0].workerRole, workerRole);
    assert.deepEqual((await store.remove({ id: node.id, version: edited.version })).nodes, []);
  }
});

test('legacy roles are inferred while invalid functional types are rejected', () => {
  assert.equal(normalizeServerNode(input).workerRole, 'incremental');
  assert.equal(normalizeServerNode({ ...input, workers: [{ role: 'fullcrawl', count: 2 }] }).workerRole, 'fullcrawl');
  assert.equal(nodeWorkerRole({ ...input, deployment: { mode: 'incremental_collect' } }), 'incremental');
  for (const workerRole of ['toString', '__proto__', 'youtube-channel-crawl', '', null, []]) {
    assert.throws(() => normalizeServerNode({ ...input, workerRole }), { statusCode: 400 });
  }
});

test('an undeployed node can change type, but a deployed node cannot be relabeled', async () => {
  const node = { ...input, ...ready, id: 'node-1', workerRole: 'incremental' };
  const { store } = registryFixture([node]);
  const saved = await store.save({ id: node.id, version: 0, node: { ...input, workerRole: 'fullcrawl' } });
  assert.equal(saved.nodes[0].workerRole, 'fullcrawl');
  assert.equal(unusedNodeDeletionEligibility(saved.nodes[0]).allowed, true);

  const deployed = registryFixture([{ ...node, deployment: { state: 'connected', mode: 'incremental_collect' } }]).store;
  await assert.rejects(deployed.save({ id: node.id, version: 0, node: { ...input, workerRole: 'fullcrawl' } }), { statusCode: 409 });
  assert.equal((await deployed.load()).nodes[0].workerRole, 'incremental');
});

test('full crawl registration cannot deploy incremental containers through new or old requests', async () => {
  const node = { ...input, ...ready, id: 'node-1', workerRole: 'fullcrawl' };
  const { store } = registryFixture([node]);
  const deployment = createNodeWorkerDeployment({ store,
    ssh: { connect: () => assert.fail('must not connect to SSH') },
    center: { prepare: () => assert.fail('must not register an incremental deployment') } });
  for (const args of [{ count: 1 }, { additionalCount: 1, role: 'incremental', expectedInstalledCount: 0 },
    { additionalCount: 1, role: 'fullcrawl', expectedInstalledCount: 0 }]) {
    await assert.rejects(deployment.start({ id: node.id, version: 0, ...args }),
      error => error.statusCode === 409 && error.message.includes('全量节点部署尚未开放'));
  }
  assert.equal((await store.load()).version, 0);
  assert.throws(() => buildNodeCollectDeployment({ node: { ...node, workers: [{ role: 'incremental', count: 1 }] } }));
  await assert.rejects(store.beginWorkerDeployment({ id: node.id, version: 0, operationId: 'attempt', plan: {}, count: 1 }), { statusCode: 409 });
});

test('an incremental registration rejects a different or missing requested worker type before deployment', async () => {
  const node = { ...input, ...ready, id: 'node-1', workerRole: 'incremental' };
  const { store } = registryFixture([node]);
  const deployment = createNodeWorkerDeployment({ store });
  await assert.rejects(deployment.start({ id: node.id, version: 0, additionalCount: 1,
    role: 'fullcrawl', expectedInstalledCount: 0 }), { statusCode: 409 });
  await assert.rejects(deployment.start({ id: node.id, version: 0, additionalCount: 1,
    expectedInstalledCount: 0 }), { statusCode: 400 });
  assert.equal((await store.load()).version, 0);
});
