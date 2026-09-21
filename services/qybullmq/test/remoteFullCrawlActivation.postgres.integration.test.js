import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteWorkerActivationStore } from '../src/remoteNodes/workerActivationStore.js';
import { RemoteWorkerConnectionStore } from '../src/remoteNodes/workerConnectionStore.js';
import { FULL_CRAWL_WORKLOAD } from '../src/remoteNodes/collectingWorkload.js';
import { RemoteProtocolError, encodeResult } from '../src/remoteNodes/protocol.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { createRemoteNodeClient } from '../src/remoteNodes/client.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
const options = { skip: !url, timeout: 30000 };

async function fixture(t, { configured = true } = {}) {
  const pool = new pg.Pool({ connectionString: url, max: 5 });
  const guard = await pool.connect();
  const tasks = []; let server;
  const nodeId = randomUUID(); const token = randomBytes(32).toString('hex');
  t.after(async () => {
    try {
      if (server) await new Promise(resolve => { server.close(resolve); server.closeAllConnections(); });
      await pool.query('DELETE FROM remote_ingestion.claims WHERE node_id=$1', [nodeId]);
      await pool.query('DELETE FROM remote_ingestion.tasks WHERE task_id=ANY($1::uuid[])', [tasks]);
      await pool.query('DELETE FROM remote_ingestion.worker_connections WHERE node_id=$1', [nodeId]);
      await pool.query('DELETE FROM remote_ingestion.network_slots WHERE node_id=$1', [nodeId]);
      await pool.query('DELETE FROM remote_ingestion.nodes WHERE node_id=$1', [nodeId]);
    } finally { guard.release(); await pool.end(); }
  });
  await assertIsolatedRemoteDatabase(pool);
  await guard.query('SELECT pg_advisory_lock(781137981)');
  for (const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql','fullCrawlSchema.sql']) {
    await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`, import.meta.url), 'utf8'));
  }
  const store = new RemoteNodeStore({ pool });
  await store.registerNode({ nodeId, token, capabilities: [FULL_CRAWL_WORKLOAD.capability] });
  const slot = 'full-crawl-1';
  await pool.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)', [nodeId,slot,`test-${nodeId}`]);
  const registration = { nodeId,slot,deploymentId: randomUUID(),configHash: 'a'.repeat(64),role: 'fullcrawl',mode: FULL_CRAWL_WORKLOAD.mode };
  const value = { version: 1,mode: registration.mode,node_id: nodeId,slot,deployment_id: registration.deploymentId,
    config_hash: registration.configHash,instance_id: randomUUID(),relay_boot_id: 'b'.repeat(48),
    runtime_revision: FULL_CRAWL_WORKLOAD.revisions[0],accepting: true };
  // Only the admission seam is under test here. W04/W06 must replace these
  // fixture callbacks with the real supervisor and locked business records.
  const state = { ready: false, ownsTask: true, checks: 0, incrementalChecks: 0 };
  const fullCrawlExecution = {
    async verifyExecution(client,row) {
      assert.equal(row.mode,FULL_CRAWL_WORKLOAD.mode);
      assert.equal(row.node_id,nodeId);
      return state.ready && row.instance_id===value.instance_id;
    },
    async assertTask(client,task,row) {
      state.checks++;
      assert.equal(task.capability,FULL_CRAWL_WORKLOAD.capability);
      assert.equal(row.role,'fullcrawl');
      assert.equal((await client.query('SELECT task_id FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[task.task_id])).rowCount,1);
      if (!state.ownsTask) throw new RemoteProtocolError('FULL_CRAWL_BUSINESS_FENCE_STALE');
    },
  };
  const activation = new RemoteWorkerActivationStore({ store,
    verifyExecution: async () => { state.incrementalChecks++; return true; },
    ...(configured ? { fullCrawlExecution } : {}) });
  server = createRemoteNodeGateway({ store,workerConnections: activation });
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  const client = createRemoteNodeClient({ url: `http://127.0.0.1:${server.address().port}`,token,allowLoopbackHttp: true });
  return { pool,store,activation,registration,value,nodeId,client,state,
    async row() { return (await pool.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2',[nodeId,slot])).rows[0]; },
    async enqueue({ capability = FULL_CRAWL_WORKLOAD.capability,target = true } = {}) {
      const id = await store.enqueue({ workKey: `full-activation:${randomUUID()}`,capability,input: {},context: {} });
      tasks.push(id);
      if (target) await pool.query('UPDATE remote_ingestion.tasks SET target_node_id=$2,target_worker_slot=$3 WHERE task_id=$1',[id,nodeId,slot]);
      return id;
    },
    async capabilities(capabilities) { await pool.query('UPDATE remote_ingestion.nodes SET capabilities=$2 WHERE node_id=$1',[nodeId,capabilities]); },
  };
}

test('full registration is idempotent, defaults to paused and requires a dedicated node capability', options, async t => {
  const f = await fixture(t);
  for (const capabilities of [[],['youtube.incremental.plan.v1'],['unknown'],[FULL_CRAWL_WORKLOAD.capability,'unknown']]) {
    await f.capabilities(capabilities);
    await assert.rejects(f.activation.register(f.registration),{ code: 'WORKER_NODE_CAPABILITY_MISMATCH' });
    assert.equal(await f.row(),undefined);
  }
  await f.capabilities([FULL_CRAWL_WORKLOAD.capability]);
  await f.activation.register(f.registration); await f.activation.register(f.registration);
  assert.equal((await f.row()).enabled,false); assert.equal((await f.row()).activation_requested,false);
  assert.equal((await f.pool.query('SELECT slot_claims_required FROM remote_ingestion.nodes WHERE node_id=$1',[f.nodeId])).rows[0].slot_claims_required,true);
  for (const patch of [{ deploymentId: randomUUID() },{ configHash: 'c'.repeat(64) }]) {
    await assert.rejects(f.activation.register({ ...f.registration,...patch }),{ code: 'WORKER_DEPLOYMENT_CONFLICT' });
  }
  for (const patch of [{ role: 'incremental' },{ mode: 'incremental_collect' },{ slot: 'incremental-1' }]) {
    await assert.rejects(f.activation.register({ ...f.registration,...patch }),TypeError);
  }
  await f.capabilities(['youtube.incremental.plan.v1']);
  await assert.rejects(f.activation.register({ ...f.registration,slot:'incremental-1',role:'incremental',mode:'incremental_collect' }),{ code:'WORKER_NODE_WORKLOAD_CONFLICT' });
});

test('full heartbeats never inherit incremental readiness and reject wrong identities over HTTP', options, async t => {
  const f = await fixture(t,{ configured:false });
  assert.throws(() => new RemoteWorkerActivationStore({ store:f.store,fullCrawlExecution:{ verifyExecution:async()=>true } }),/task fence required/);
  await f.activation.register(f.registration);
  assert.equal((await f.client.workerHeartbeat(f.value)).ready_for_tasks,false);
  await assert.rejects(f.activation.activate(f.value),{ code:'CENTRAL_EXECUTION_NOT_CONFIGURED' });
  assert.equal(await f.client.claim(randomUUID(),f.value.slot,f.value),null);
  assert.equal(f.state.incrementalChecks,0);
  const before = await f.row();
  for (const patch of [{ node_id:randomUUID() },{ mode:'incremental_collect' },{ runtime_revision:'youtubejs-incremental-v1' },
    { runtime_revision:'future-version' },{ slot:'incremental-1' },{ capability:'unknown' }]) {
    await assert.rejects(f.client.workerHeartbeat({ ...f.value,...patch }),{ code:'INVALID_WORKER_CONNECTION' });
  }
  for (const patch of [{ deployment_id:randomUUID() },{ config_hash:'c'.repeat(64) }]) {
    await assert.rejects(f.client.workerHeartbeat({ ...f.value,...patch }),{ code:'WORKER_DEPLOYMENT_MISMATCH' });
  }
  await assert.rejects(f.client.workerHeartbeat({ ...f.value,instance_id:randomUUID() }),{ code:'WORKER_INSTANCE_BUSY' });
  assert.deepEqual(await f.row(),before);
  const {runtime_revision,accepting,...connectionOnly} = f.value;
  const oldConnections = new RemoteWorkerConnectionStore({store:f.store});
  await assert.rejects(oldConnections.heartbeat(f.nodeId,{...connectionOnly,mode:'connect_only'}),{code:'WORKER_DEPLOYMENT_MISMATCH'});
  assert.deepEqual(await f.row(),before,'connect-only protocol cannot refresh or replace a full owner');
  await f.capabilities(['unknown']);
  await assert.rejects(f.client.workerHeartbeat(f.value),{code:'WORKER_NODE_CAPABILITY_MISMATCH'});
});

test('full readiness and claim require separate fences; drain preserves only a still-owned claim replay', options, async t => {
  const f=await fixture(t); await f.activation.register(f.registration); await f.client.workerHeartbeat(f.value);
  await assert.rejects(f.activation.activate(f.value),{code:'CENTRAL_EXECUTION_NOT_READY'});
  f.state.ready=true;
  await assert.rejects(f.activation.activate(f.value,{requireRequested:true}),{code:'WORKER_NOT_READY'});
  await f.activation.activate(f.value);
  assert.equal((await f.client.workerHeartbeat(f.value)).ready_for_tasks,true);
  const other=await f.enqueue({capability:'youtube.incremental.plan.v1'});
  const task=await f.enqueue();
  assert.equal(await f.store.claim(f.nodeId,randomUUID(),f.value.slot),null,'generic claim cannot lease full work');
  f.state.ownsTask=false;
  await assert.rejects(f.client.claim(randomUUID(),f.value.slot,f.value),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
  assert.equal((await f.pool.query('SELECT generation FROM remote_ingestion.tasks WHERE task_id=$1',[task])).rows[0].generation,0);
  f.state.ownsTask=true;
  await assert.rejects(f.client.claim(randomUUID(),f.value.slot,{...f.value,instance_id:randomUUID()}),{code:'WORKER_CONNECTION_STALE'});
  const claim=randomUUID(); const lease=await f.client.claim(claim,f.value.slot,f.value);
  assert.equal(lease.task_id,task);
  assert.equal((await f.pool.query('SELECT generation FROM remote_ingestion.tasks WHERE task_id=$1',[other])).rows[0].generation,0);
  await assert.rejects(f.store.claim(f.nodeId,claim,f.value.slot),{code:'WORKER_TASK_CAPABILITY_MISMATCH'});
  const checks=f.state.checks;
  await f.activation.drain(f.nodeId,f.value.slot);
  assert.equal((await f.client.workerHeartbeat(f.value)).ready_for_tasks,false);
  assert.equal(await f.client.claim(randomUUID(),f.value.slot,f.value),null);
  assert.equal((await f.client.claim(claim,f.value.slot,f.value)).task_id,task);
  assert.equal(f.state.checks,checks+1,'existing claim replay rechecks the business fence');
  f.state.ownsTask=false;
  await assert.rejects(f.client.claim(claim,f.value.slot,f.value),{code:'FULL_CRAWL_BUSINESS_FENCE_STALE'});
  await assert.rejects(f.client.heartbeat(lease),{code:'STALE_LEASE'},'unfenced generic work heartbeat stays closed');
  await assert.rejects(f.client.upload(task,await encodeResult({version:1,batch_id:randomUUID(),generation:lease.generation,outcome:'success',data:{}})),{code:'FULL_CRAWL_REQUIRES_CENTRAL_COMPLETION'});
  assert.equal((await f.pool.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1',[task])).rows[0].state,'leased');
  assert.equal(f.state.incrementalChecks,0);
});

test('a full task must target its exact slot and stopped/revoked nodes cannot admit new work', options, async t => {
  const f=await fixture(t); await f.activation.register(f.registration); await f.client.workerHeartbeat(f.value);
  f.state.ready=true; await f.activation.activate(f.value);
  const task=await f.enqueue({target:false});
  await assert.rejects(f.client.claim(randomUUID(),f.value.slot,f.value),{code:'WORKER_TASK_TARGET_MISMATCH'});
  assert.equal((await f.pool.query('SELECT generation FROM remote_ingestion.tasks WHERE task_id=$1',[task])).rows[0].generation,0);
  await f.pool.query('UPDATE remote_ingestion.tasks SET target_node_id=$2,target_worker_slot=$3 WHERE task_id=$1',[task,f.nodeId,f.value.slot]);
  f.state.ready=false;
  assert.equal((await f.client.workerHeartbeat(f.value)).ready_for_tasks,false);
  assert.equal(await f.client.claim(randomUUID(),f.value.slot,f.value),null);
  f.state.ready=true;
  assert.equal((await f.client.workerHeartbeat({...f.value,accepting:false})).state,'draining');
  await assert.rejects(f.activation.activate(f.value),{code:'WORKER_NOT_READY'});
  await f.store.setNodeState(f.nodeId,'draining');
  assert.equal((await f.client.workerHeartbeat(f.value)).ready_for_tasks,false);
  assert.equal(await f.client.claim(randomUUID(),f.value.slot,f.value),null);
  await f.store.setNodeState(f.nodeId,'disabled');
  await assert.rejects(f.client.workerHeartbeat(f.value),{code:'UNAUTHORIZED'});
  await assert.rejects(f.client.claim(randomUUID(),f.value.slot,f.value),{code:'UNAUTHORIZED'});
});

test('expired full workers cannot replace unfinished evidence, and retired registrations stay retired', options, async t => {
  const f=await fixture(t); await f.activation.register(f.registration); await f.client.workerHeartbeat(f.value);
  const replacement={...f.value,instance_id:randomUUID(),relay_boot_id:'c'.repeat(48)};
  const task=await f.enqueue();
  await f.pool.query("UPDATE remote_ingestion.worker_connections SET connected_until=now()-interval '1 second' WHERE node_id=$1",[f.nodeId]);
  for (const state of ['pending','received']) {
    await f.pool.query('UPDATE remote_ingestion.tasks SET state=$2 WHERE task_id=$1',[task,state]);
    await assert.rejects(f.client.workerHeartbeat(replacement),{code:'WORKER_PREVIOUS_EXECUTION_UNSETTLED'});
  }
  await f.pool.query("UPDATE remote_ingestion.tasks SET state='failed' WHERE task_id=$1",[task]);
  const rivals=[replacement,{...replacement,instance_id:randomUUID(),relay_boot_id:'d'.repeat(48)}];
  const results=await Promise.allSettled(rivals.map(value=>f.client.workerHeartbeat(value)));
  assert.equal(results.filter(result=>result.status==='fulfilled').length,1,'concurrent replacements cannot both acquire the same slot');
  assert.equal(results.find(result=>result.status==='rejected').reason.code,'WORKER_INSTANCE_BUSY');
  const winner=results.find(result=>result.status==='fulfilled').value;
  const owner=rivals.find(value=>value.instance_id===winner.instance_id);
  assert.equal(winner.ready_for_tasks,false);
  assert.equal((await f.row()).enabled,false);
  await assert.rejects(f.activation.activate(f.value),{code:'WORKER_NOT_READY'});
  await assert.rejects(f.client.workerHeartbeat(f.value),{code:'WORKER_INSTANCE_BUSY'});
  await f.pool.query('UPDATE remote_ingestion.worker_connections SET retirement_id=$2,retired_at=now() WHERE node_id=$1',[f.nodeId,randomUUID()]);
  await assert.rejects(f.client.workerHeartbeat(owner),{code:'WORKER_DEPLOYMENT_MISMATCH'});
  await assert.rejects(f.activation.register(f.registration),{code:'WORKER_DEPLOYMENT_CONFLICT'});
});
