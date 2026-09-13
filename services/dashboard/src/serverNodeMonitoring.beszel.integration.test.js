import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createNodeMonitoring } from './serverNodeMonitoring.js';

const url = process.env.NODE_MONITORING_TEST_URL;
// Only a disposable Beszel 0.19.0 instance; never use production credentials.
test('real Beszel short fingerprint IDs: remove, retry, re-register and preserve other systems', { skip: !url }, async t => {
  const endpoint = new URL(url);
  assert.equal(endpoint.hostname, '127.0.0.1'); assert.equal(endpoint.port, '58125');
  const login = async (collection, identity, password) => {
    const res = await fetch(`${url}/api/collections/${collection}/auth-with-password`, { method: 'POST', headers: { 'Content-Type':'application/json' }, body: JSON.stringify({ identity,password }) });
    assert.equal(res.status, 200); return res.json();
  };
  const admin = await login('_superusers', 'fixture-admin@example.com', 'fixture-test-password-123');
  const email = `fixture-${randomUUID()}@example.com`, password = 'fixture-user-password-123';
  const created = await fetch(`${url}/api/collections/users/records`, { method: 'POST', headers: { 'Content-Type':'application/json',Authorization:admin.token }, body: JSON.stringify({ email,password,passwordConfirm:password,role:'admin',verified:true }) });
  assert.equal(created.status, 200);
  const user = await login('users', email, password);
  const read = path => fetch(url+path, { headers: { Authorization:user.token } });
  const dir = await mkdtemp(join(tmpdir(),'beszel-delete-test-')); t.after(()=>rm(dir,{recursive:true,force:true}));
  const file = join(dir,'credentials.json'); await writeFile(file,JSON.stringify({email,password}),{mode:0o600});
  const monitor = createNodeMonitoring({ url, credentialsFile:file, publicUrl:url });
  const node = { id:randomUUID(),name:'Deletion fixture' }, other = { id:randomUUID(),name:'Retained fixture' };
  for (const target of [node,other]) target.provisioning={systemId:(await monitor.prepare(target)).systemId};
  const path = `/api/collections/fingerprints/records?filter=${encodeURIComponent(`system="${node.provisioning.systemId}"`)}`;
  const before = await (await read(path)).json();
  assert.equal(before.items.length,1); assert.equal(before.items[0].id.length,9,'pinned Beszel version generates short fingerprint IDs');
  await monitor.remove(node);
  assert.equal((await read(`/api/collections/systems/records/${node.provisioning.systemId}`)).status,404);
  assert.equal((await (await read(path)).json()).items.length,0);
  assert.equal((await read(`/api/collections/systems/records/${other.provisioning.systemId}`)).status,200);
  await monitor.remove(node);
  assert.equal((await monitor.prepare(node)).systemId,node.provisioning.systemId);
  await monitor.remove(node); await monitor.remove(other);
});
