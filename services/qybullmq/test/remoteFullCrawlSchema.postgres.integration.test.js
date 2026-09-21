import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import pg from 'pg';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;

test('P2 schema is repeatable, preserves incremental registrations and constrains full-crawl evidence', { skip: !url }, async t => {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  t.after(async () => { await client.query('ROLLBACK'); await client.end(); });
  await assertIsolatedRemoteDatabase(client);
  await client.query('BEGIN');
  await client.query('SELECT pg_advisory_xact_lock(781137981)');
  for (const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql']) {
    await client.query(await readFile(new URL(`../src/remoteNodes/${file}`, import.meta.url), 'utf8'));
  }
  const node = randomUUID(); const deployment = randomUUID(); const task = randomUUID();
  await client.query("INSERT INTO remote_ingestion.nodes(node_id,token_hash,capabilities,max_leases) VALUES($1,$2,ARRAY['youtube.full-crawl.v1'],2)", [node, node.replaceAll('-','').repeat(2)]);
  for (const slot of ['incremental-1','full-crawl-1']) {
    await client.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)', [node,slot,`${node}-${slot}`]);
  }
  await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode,enabled,instance_id)
    VALUES($1,'incremental-1',$2,$3,'incremental','incremental_collect',true,$4)`, [node,deployment,'a'.repeat(64),randomUUID()]);
  const before = (await client.query("SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1", [node])).rows;
  const schema = await readFile(new URL('../src/remoteNodes/fullCrawlSchema.sql', import.meta.url), 'utf8');
  await client.query(schema);
  await client.query(schema);
  // Re-applying the old opt-in migration must not remove the new constraints.
  await client.query(await readFile(new URL('../src/remoteNodes/workerActivationSchema.sql', import.meta.url), 'utf8'));
  assert.deepEqual((await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1', [node])).rows, before);

  async function rejected(sql, values, code = '23514') {
    await client.query('SAVEPOINT expected_rejection');
    await assert.rejects(client.query(sql, values), { code });
    await client.query('ROLLBACK TO SAVEPOINT expected_rejection');
  }
  await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode,activation_requested)
    VALUES($1,'full-crawl-1',$2,$3,'fullcrawl','full_crawl_collect',false)`, [node,deployment,'b'.repeat(64)]);
  const full = (await client.query("SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot='full-crawl-1'", [node])).rows[0];
  assert.equal(full.enabled, false); assert.equal(full.activation_requested, false);
  for (const assignment of ["role='incremental'", "mode='incremental_collect'", "runtime_revision='youtubejs-incremental-v1'"]) {
    await rejected(`UPDATE remote_ingestion.worker_connections SET ${assignment} WHERE node_id=$1 AND slot='full-crawl-1'`, [node]);
  }
  await client.query("INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context) VALUES($1,$2,'youtube.full-crawl.v1','{}','{}')", [task,`full-schema-${task}`]);
  await client.query(`INSERT INTO remote_ingestion.full_crawl_executions(task_id,generation,node_id,worker_slot,instance_id,protocol_version,execution_input,execution_hash)
    VALUES($1,1,$2,'full-crawl-1',$3,1,'{}',$4)`, [task,node,randomUUID(),'c'.repeat(64)]);
  const admission = randomUUID(); const detail = randomUUID();
  const stageSql = `INSERT INTO remote_ingestion.full_crawl_stages(stage_id,task_id,generation,stage,sequence,input,input_hash,target_hash)
    VALUES($1,$2,$3,$4,$5,'{}',$6,$7)`;
  await client.query(stageSql, [admission,task,1,'admission',1,'d'.repeat(64),null]);
  await client.query(stageSql, [detail,task,1,'details',2,'d'.repeat(64),'e'.repeat(64)]);
  await rejected(stageSql, [randomUUID(),task,2,'uploads',3,'d'.repeat(64),null], '23503');
  await rejected(stageSql, [randomUUID(),task,1,'details',3,'d'.repeat(64),null]);
  await rejected(stageSql, [randomUUID(),task,1,'uploads',2,'d'.repeat(64),null], '23505');
  const reservation = randomUUID();
  const reserveSql = `INSERT INTO remote_ingestion.full_crawl_detail_reservations(reservation_id,stage_id,ordinal,video_id,target)
    VALUES($1,$2,1,'fixture1234','{}')`;
  await rejected(reserveSql, [randomUUID(),admission], '23503');
  await client.query(reserveSql, [reservation,detail]);
  assert.equal((await client.query('SELECT started_at FROM remote_ingestion.full_crawl_detail_reservations WHERE reservation_id=$1', [reservation])).rows[0].started_at, null);
  await rejected("UPDATE remote_ingestion.full_crawl_detail_reservations SET state='applied',applied_at=clock_timestamp() WHERE reservation_id=$1", [reservation]);
  await client.query("UPDATE remote_ingestion.full_crawl_detail_reservations SET state='started',start_id=$2,started_at=clock_timestamp() WHERE reservation_id=$1", [reservation,randomUUID()]);
  await client.query("UPDATE remote_ingestion.full_crawl_detail_reservations SET state='applied',applied_at=clock_timestamp() WHERE reservation_id=$1", [reservation]);

  const batch = randomUUID();
  await client.query(`INSERT INTO remote_ingestion.full_crawl_result_batches(batch_id,stage_id,sequence,payload_hash,payload_bytes,part_count)
    VALUES($1,$2,1,$3,524289,2)`, [batch,detail,'f'.repeat(64)]);
  const partSql = `INSERT INTO remote_ingestion.full_crawl_result_parts(batch_id,part_number,part_count,payload_bytes,part_hash,payload)
    VALUES($1,$2,$3,$4,$5,$6)`;
  await rejected(partSql,[batch,0,2,524289,'a'.repeat(64),Buffer.alloc(524289)]);
  await rejected(partSql,[batch,2,2,524289,'a'.repeat(64),Buffer.alloc(1)]);
  await rejected(partSql,[batch,0,1,1,'a'.repeat(64),Buffer.alloc(1)],'23503');
  await client.query(partSql,[batch,0,2,524289,'a'.repeat(64),Buffer.alloc(524288)]);
  await client.query(partSql,[batch,1,2,524289,'b'.repeat(64),Buffer.alloc(1)]);
  await rejected(partSql,[batch,1,2,524289,'b'.repeat(64),Buffer.alloc(1)],'23505');
  await rejected("UPDATE remote_ingestion.full_crawl_result_batches SET state='applied' WHERE batch_id=$1", [batch]);
  assert.equal((await client.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1', [task])).rows[0].state, 'pending');
  assert.deepEqual((await client.query("SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot='incremental-1'", [node])).rows, before);
});
