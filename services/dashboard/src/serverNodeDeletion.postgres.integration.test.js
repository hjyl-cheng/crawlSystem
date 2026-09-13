import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { createServerNodeStore, initializationSteps } from './serverNodes.js';
import { createNodeDeletion, createUnusedNodeCheck } from './serverNodeDeletion.js';
const url = process.env.NODE_DELETION_TEST_DATABASE_URL;
test('durable unused-node deletion fences deployment, survives retry and allows same host to be re-added', { skip: !url }, async t => {
  const parsed = new URL(url);
  assert.equal(parsed.hostname, '127.0.0.1'); assert.equal(parsed.pathname, '/server_nodes_deletion_test');
  const pool = new pg.Pool({ connectionString: url }); t.after(() => pool.end());
  await pool.query(`DROP SCHEMA IF EXISTS crawler CASCADE; DROP SCHEMA IF EXISTS remote_ingestion CASCADE; CREATE SCHEMA crawler; CREATE TABLE crawler.settings(setting_key TEXT PRIMARY KEY,value_json JSONB NOT NULL,updated_at TIMESTAMPTZ);
    CREATE SCHEMA remote_ingestion;
    CREATE TABLE remote_ingestion.nodes(node_id UUID PRIMARY KEY);
    CREATE TABLE remote_ingestion.node_deployments(node_id UUID);
    CREATE TABLE remote_ingestion.worker_connections(node_id UUID);
    CREATE TABLE remote_ingestion.tasks(node_id UUID,target_node_id UUID);`);
  const store = createServerNodeStore(pool.query.bind(pool));
  const config = { name: 'Test', host: '192.0.2.11', port: 22, username: 'ubuntu', kind: 'execution', workers: [] };
  let registry = await store.save({ version: 0, node: config });
  const id = registry.nodes[0].id;
  registry = await store.beginInitialization({ id, version: registry.version, operationId: 'init' });
  registry = await store.advanceInitialization(id, 'init', { state: 'ready', systemId: 'test', steps: Object.fromEntries(initializationSteps.map(step => [step, 'completed'])) });
  const center = createUnusedNodeCheck(pool.query.bind(pool));
  let fail = true;
  const deletion = createNodeDeletion({ store, checkCenter: center, ssh: { connect: async () => ({}), verify: async () => {}, close() {}, removeUnusedNode: async () => {} },
    monitoring: { remove: async () => { if (fail) throw new Error('secret'); } } });
  await pool.query('INSERT INTO remote_ingestion.tasks VALUES(NULL,$1)', [id]);
  assert.equal((await deletion.check(id)).allowed, false);
  await pool.query('DELETE FROM remote_ingestion.tasks');
  assert.equal((await deletion.check(id)).allowed, true);
  await assert.rejects(() => deletion.remove({ id, version: registry.version }), /中心监控/);
  registry = await store.load(); assert.equal(registry.nodes[0].deletion.state, 'failed');
  await assert.rejects(() => store.beginRuntime({ id, version: registry.version, operationId: 'runtime' }), { statusCode: 409 });
  fail = false;
  const admissions = await Promise.allSettled([
    deletion.remove({ id, version: registry.version }),
    store.beginWorkerDeployment({ id, version: registry.version, operationId: 'deploy', plan: {} }),
  ]);
  assert.equal(admissions[0].status, 'fulfilled'); assert.equal(admissions[1].status, 'rejected');
  registry = await store.load(); assert.deepEqual(registry.nodes, []);
  registry = await store.save({ version: registry.version, node: config });
  assert.notEqual(registry.nodes[0].id, id); assert.equal(registry.nodes[0].host, config.host);
});
