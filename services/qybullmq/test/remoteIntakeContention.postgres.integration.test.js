import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import pg from 'pg';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {RemoteChannelRouteStore} from '../src/remoteNodes/channelRouteStore.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
async function fixture(t){
  const pool=new pg.Pool({connectionString:url,max:8});await assertIsolatedRemoteDatabase(pool);
  const guard=await pool.connect();await guard.query('SELECT pg_advisory_lock(781137981)');
  for(const f of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'])
    await pool.query(await readFile(new URL(`../src/remoteNodes/${f}`,import.meta.url),'utf8'));
  const nodeId=randomUUID(),deploymentId=randomUUID();
  t.after(async()=>{try{await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');}
    finally{guard.release();await pool.end();}});
  const store=new RemoteNodeStore({pool});
  const routes=new RemoteChannelRouteStore({channelStore:new RemoteChannelPlanStore({store}),readRotaRoute:()=>{},assertBusinessFence:()=>{},
    secretKey:randomBytes(32),privateKey:generateKeyPairSync('ed25519').privateKey});
  const image='registry.example/collect@sha256:'+'a'.repeat(64),gatewayUrl='https://center.example/remote';
  const admin=createRemoteDeploymentAdmin({store,routes,image,gatewayUrl,token:randomBytes(32).toString('hex'),execution:{allowsNode:()=>true,isProcessing:()=>false}});
  const plan=count=>({nodeId,deploymentId,image,files:Object.fromEntries(Array.from({length:count},(_,i)=>[
    `incremental-${i+1}.json`,JSON.stringify({version:1,mode:'incremental_collect',role:'incremental',node_id:nodeId,deployment_id:deploymentId,slot:`incremental-${i+1}`,gateway_url:gatewayUrl})]))});
  await admin.prepare(plan(45));
  await pool.query("UPDATE remote_ingestion.worker_connections SET connected_until=now()+interval '5 minutes',accepting=true,activation_requested=true,enabled=true WHERE node_id=$1",[nodeId]);
  return {pool,store,admin,nodeId,deploymentId,plan};
}

test('saving desired intake does not wait for a busy Worker, survives restart and reconciles later',{skip:!url,timeout:20000},async t=>{
  const {pool,store,admin,nodeId,deploymentId}=await fixture(t);
  const busy=await pool.connect();await busy.query('BEGIN');
  await busy.query('SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[nodeId]);
  await busy.query("SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot='incremental-45' FOR UPDATE",[nodeId]);
  let timer;const action=admin.setExecution({nodeId,deploymentId,workerCount:45,allowedCount:10,expectedAllowedCount:45});
  try{
    const saved=await Promise.race([action,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('intake save blocked behind collection')),1500);})]);
    assert.equal(saved.allowedCount,10);assert.equal(saved.adjusting,true);
    assert.equal((await admin.status({nodeId,deploymentId})).allowedCount,10);
    assert.equal((await pool.query("SELECT enabled FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot='incremental-45'",[nodeId])).rows[0].enabled,true,'saving does not terminate current work');
  }finally{clearTimeout(timer);await busy.query('ROLLBACK');busy.release();await action.catch(()=>{});}
  const {reconcileIntakeRequests}=await import('../src/remoteNodes/intakeRequests.js');
  await reconcileIntakeRequests(new RemoteNodeStore({pool}));
  const status=await admin.status({nodeId,deploymentId});assert.equal(status.allowedCount,10);assert.equal(status.adjusting,false);
  assert.equal((await pool.query('SELECT count(*)::int n FROM remote_ingestion.worker_connections WHERE node_id=$1 AND activation_requested',[nodeId])).rows[0].n,10);
  await assert.rejects(admin.setExecution({nodeId,deploymentId,workerCount:45,allowedCount:0,expectedAllowedCount:45}),{code:'EXECUTION_CONTROL_CHANGED'});
  for(const [count,expected] of [[0,10],[45,0],[45,45]])assert.equal((await admin.setExecution({nodeId,deploymentId,workerCount:45,allowedCount:count,expectedAllowedCount:expected})).allowedCount,count);
  const observe=admin.status;admin.status=async()=>{throw Error('observation temporarily unavailable');};
  const saved=await admin.setExecution({nodeId,deploymentId,workerCount:45,allowedCount:10,expectedAllowedCount:45});
  assert.equal(saved.saved,true);assert.equal(saved.observationPending,true);assert.equal(saved.allowedCount,10);
  admin.status=observe;assert.equal((await admin.status({nodeId,deploymentId})).allowedCount,10);
});

test('claim with 120000 completed tasks reads the pending slot, preserves replay and exclusion',{skip:!url,timeout:30000},async t=>{
  const {pool,store,nodeId}=await fixture(t);
  await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,state,target_node_id,target_worker_slot)
    SELECT gen_random_uuid(),$1||':'||i,'youtube.incremental.plan.v1','{}','{}','applied',$1::uuid,
      'incremental-'||(1+(i%45)) FROM generate_series(1,120000) i`,[nodeId]);
  const pending=randomUUID();
  await pool.query(`INSERT INTO remote_ingestion.tasks(task_id,work_key,capability,input,context,target_node_id,target_worker_slot,scope_key)
    VALUES($1::uuid,$1::text,'youtube.incremental.plan.v1','{}','{}',$2,'incremental-1',$1::text)`,[pending,nodeId]);
  await pool.query('ANALYZE remote_ingestion.tasks');
  const original=pool.connect.bind(pool);let examined=0,claimSql;
  pool.connect=async()=>{const c=await original(),query=c.query.bind(c),release=c.release.bind(c);
    c.query=async(sql,args)=>{
      if(typeof sql==='string' && sql.startsWith('SELECT * FROM remote_ingestion.tasks candidate')){
        claimSql=sql;const explain=await query('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+sql,args);
        const visit=p=>{if(p['Index Name'])examined+=p['Actual Rows']*p['Actual Loops'];for(const child of p.Plans??[])visit(child);};
        visit(explain.rows[0]['QUERY PLAN'][0].Plan);
      }
      return query(sql,args);
    };
    c.release=()=>{c.query=query;c.release=release;release();};return c;
  };
  const claimId=randomUUID();const lease=await store.claim(nodeId,claimId,'incremental-1');pool.connect=original;
  assert.equal(lease.task_id,pending);assert.ok(claimSql);
  assert.ok(examined<100,`claim examined ${examined} index entries despite only one pending task`);
  assert.deepEqual(await store.claim(nodeId,claimId,'incremental-1'),lease,'lost response replays same generation');
  assert.equal(await store.claim(nodeId,randomUUID(),'incremental-1'),null,'occupied slot cannot take another channel');
  assert.equal(await store.hasClaimWork(nodeId,randomUUID(),'incremental-2'),false,'idle slot needs no claim transaction');
  assert.equal(await store.hasClaimWork(nodeId,claimId,'incremental-1'),true,'hint preserves response replay');
  await pool.query('DELETE FROM remote_ingestion.claims WHERE node_id=$1',[nodeId]);
});

test('idle claims do not take node/Worker locks or block additive deployment',{skip:!url,timeout:20000},async t=>{
  const {pool,store,admin,nodeId,plan}=await fixture(t);
  const gate=await pool.connect();await gate.query('BEGIN');await gate.query('SELECT pg_advisory_xact_lock(781138017,hashtext($1))',[nodeId]);
  const claim=store.claim(nodeId,randomUUID(),'incremental-1');let timer;
  try{
    const result=await Promise.race([claim,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('idle claim waited for node capacity lock')),1000);})]);
    assert.equal(result,null);
    await admin.prepare(plan(46));
    assert.equal((await pool.query('SELECT worker_count FROM remote_ingestion.node_deployments WHERE node_id=$1',[nodeId])).rows[0].worker_count,46);
  }finally{clearTimeout(timer);await gate.query('ROLLBACK');gate.release();await claim.catch(()=>{});}
});

test('paused node saves a count without enabling any slot; start and pause preserve the saved count',{skip:!url,timeout:20000},async t=>{
  const {pool,admin,nodeId,deploymentId}=await fixture(t);
  await admin.setExecution({nodeId,deploymentId,workerCount:45,enabled:false,expectedRequested:true});
  const saved=await admin.setExecution({nodeId,deploymentId,workerCount:45,allowedCount:20,expectedAllowedCount:45});
  assert.equal(saved.allowedCount,0,'saving a limit must not resume a paused node');
  assert.equal(saved.configuredCount,20);assert.equal(saved.intakeEnabled,false);
  assert.equal((await pool.query('SELECT count(*)::int n FROM remote_ingestion.worker_connections WHERE node_id=$1 AND activation_requested',[nodeId])).rows[0].n,0);
  const started=await admin.setExecution({nodeId,deploymentId,workerCount:45,enabled:true,expectedRequested:false});
  assert.equal(started.allowedCount,20);assert.equal(started.intakeEnabled,true);
  const resized=await admin.setExecution({nodeId,deploymentId,workerCount:45,allowedCount:10,expectedAllowedCount:20});
  assert.equal(resized.allowedCount,10);
  const paused=await admin.setExecution({nodeId,deploymentId,workerCount:45,enabled:false,expectedRequested:true});
  assert.equal(paused.allowedCount,0);assert.equal(paused.configuredCount,10);
  assert.equal((await admin.status({nodeId,deploymentId})).configuredCount,10);
});

test('concurrent pause and count save preserve both operator choices',{skip:!url,timeout:20000},async t=>{
  const {admin,nodeId,deploymentId}=await fixture(t);
  await Promise.all([
    admin.setExecution({nodeId,deploymentId,workerCount:45,allowedCount:20,expectedAllowedCount:45}),
    admin.setExecution({nodeId,deploymentId,workerCount:45,enabled:false,expectedRequested:true}),
  ]);
  const state=await admin.status({nodeId,deploymentId});
  assert.equal(state.configuredCount,20);assert.equal(state.intakeEnabled,false);assert.equal(state.allowedCount,0);
});
