import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createNodeMonitoring, monitoringSystemId } from './serverNodeMonitoring.js';
test('monitoring deletion verifies ownership, revokes tokens and tolerates missing records', async t => {
  const dir = await mkdtemp(join(tmpdir(), 'node-monitoring-delete-')); t.after(() => rm(dir, { recursive: true, force: true }));
  const file = join(dir, 'credentials.json'); await writeFile(file, JSON.stringify({ email: 'fixture', password: 'fixture-secret' }));
  const node = { id: '23b2d2ee-fa3f-4b1e-8715-6f0521043ccf' }; const id = monitoringSystemId(node.id); node.provisioning = { systemId: id };
  let system = { id, host: `qy-node-${node.id}` }; let fingerprints = [{ id: 'z880rv9ve', system: id }]; const deleted = [];
  const monitor = createNodeMonitoring({ url: 'https://fixture.invalid', credentialsFile: file, fetchImpl: async (url, options) => {
    const path = new URL(url).pathname;
    const response = (data, status = 200) => new Response(status === 204 ? null : JSON.stringify(data), { status });
    if (path.endsWith('auth-with-password')) return response({ token: 'fixture-token' });
    if (options.method === 'DELETE') {
      deleted.push(path);
      if (path.includes('/fingerprints/')) fingerprints = []; else system = null;
      return response(null, 204);
    }
    if (path.includes('/fingerprints/')) return response({ items: fingerprints, totalPages: 1 });
    return response(system ?? {}, system ? 200 : 404);
  } });
  system.host = 'some-other-node';
  await assert.rejects(() => monitor.remove(node), /归属/); assert.deepEqual(deleted, []);
  system.host = `qy-node-${node.id}`;
  for (const row of [{id:'../other',system:id},{id:'',system:id},{id:'z880rv9ve',system:'other-system'}]) {
    fingerprints=[row];
    await assert.rejects(()=>monitor.remove(node),/归属/); assert.deepEqual(deleted,[]);
  }
  fingerprints=[{id:'z880rv9ve',system:id}];
  await monitor.remove(node); await monitor.remove(node);
  assert.equal(deleted.length, 2);
  assert.match(deleted[0], /fingerprints/); assert.match(deleted[1], /systems/);
  await assert.rejects(() => monitor.remove({ ...node, provisioning: { systemId: 'wrong' } }), /归属/);
});
