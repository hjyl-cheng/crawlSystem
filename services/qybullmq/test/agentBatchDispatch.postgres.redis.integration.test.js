import assert from 'node:assert/strict';
import test from 'node:test';
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {Queue, Worker} from 'bullmq';
import {createAgentBatchDispatch, agentTailReady} from '../src/agentBatchDispatch.js';
import {createControllerWorkLoops} from '../src/controllerWorkLoops.js';
import {PUBLICATION_WRITER_VERSION} from '../src/publicationWriterVersion.js';
const url=process.env.AGENT_DISPATCH_TEST_DATABASE_URL,port=Number(process.env.THROUGHPUT_TEST_REDIS_PORT);
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check){const deadline=Date.now()+20000;while(!await check()){assert.ok(Date.now()<deadline,'Agent dispatch stopped making progress');await delay(50);}}
for (const recoveryBacklog of [0,10]) test(`real Agent dispatch fills three consumers during blocked maintenance, preserves tail eligibility and never repeats channels (recovery=${recoveryBacklog})`, {skip:!url||!port}, async t=>{
  assert.equal(new URL(url).pathname,'/agent_dispatch_test');
  const pool=new pg.Pool({connectionString:url,max:6,options:`-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
  const query=pool.query.bind(pool),connection={host:'127.0.0.1',port,password:process.env.THROUGHPUT_TEST_REDIS_PASSWORD,maxRetriesPerRequest:null};
  const queue=new Queue('youtube-agent-batch',{connection,prefix:`agent-dispatch-${Date.now()}`});
  const workers=[],seen=new Set(),sizes=[],errors=[];
  let loops,release,unblock,releaseRecoveries;const recoveryGate=new Promise(r=>{releaseRecoveries=r});const gate=new Promise(r=>{release=r}),maintenanceGate=new Promise(r=>{unblock=r});
  t.after(async()=>{release();releaseRecoveries();unblock();await loops?.shutdown();await Promise.all(workers.map(w=>w.close()));await queue.obliterate({force:true});await queue.close();await pool.end();});
  await query('DROP SCHEMA IF EXISTS publication CASCADE; DROP SCHEMA IF EXISTS crawler CASCADE');
  await query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
  const batch='agent-test-batch';
  await query(`INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at) VALUES($1,$1,'discovery_closed',now())`,[batch]);
  await query(`INSERT INTO crawler.channel_candidates(dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status)
    SELECT $1,$1,'UCagent'||n,'https://youtube.com/channel/UCagent'||n,'discovered' FROM generate_series(1,65)n`,[batch]);
  const seed=await pool.connect();
  try {
    await seed.query('BEGIN');
    await seed.query(`INSERT INTO crawler.channels(channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id)
      SELECT 'UCagent'||n,'https://youtube.com/channel/UCagent'||n,'Agent test','active',2000,true,'pending','agent-run-'||n,
        CASE WHEN n<=61 THEN c.candidate_id END,
        CASE WHEN n<=60 THEN 'agent-run-'||n WHEN n=61 THEN 'agent-old-61' END
      FROM generate_series(1,65)n JOIN crawler.channel_candidates c ON c.channel_id='UCagent'||n`);
    await seed.query(`INSERT INTO crawler.channel_runs(run_id,channel_id,candidate_id,crawl_mode,result_json)
      SELECT 'agent-run-'||n,'UCagent'||n,CASE WHEN n<=61 THEN c.candidate_id END,'full',jsonb_build_object('dispatch_batch_id',$1::text)
      FROM generate_series(1,65)n JOIN crawler.channel_candidates c ON c.channel_id='UCagent'||n`,[batch]);
    await seed.query(`INSERT INTO crawler.channel_runs(run_id,channel_id,candidate_id,crawl_mode,result_json)
      SELECT 'agent-old-61',channel_id,candidate_id,'full','{}' FROM crawler.channel_candidates WHERE channel_id='UCagent61'`);
    await seed.query('COMMIT');
  } finally {seed.release();}
  // Picking one batch must not inspect every remaining large Run payload.
  // Execute the actual dispatch SQL and roll it back before starting consumers.
  let selection;
  const capture=createAgentBatchDispatch({query:async(sql,args)=>{
    if(sql.startsWith('SELECT discovery_closed_at'))return {rows:[{ready:false}]};
    selection??={sql,args};return {rows:[]};
  },agentQueue:{getJobs:async()=>[]},agentBatchSize:20,agentMaxBatchesPerTick:1});
  await capture.dispatch([],[{config_id:1,batch_size:20,max_workers:3}],{concurrency:3},{pipeline_cycle_id:batch});
  const probe=await pool.connect();
  try {
    await probe.query('BEGIN');
    const explanation=await probe.query('EXPLAIN (ANALYZE,FORMAT JSON) '+selection.sql,selection.args);
    let payloadChecks=0,identityReads=0;
    const visit=node=>{if(node['Relation Name']==='channel_runs')identityReads+=node['Actual Loops'];if(node['Relation Name']==='channel_runs' && String(node.Filter||'').includes('result_json'))payloadChecks+=node['Actual Loops'];for(const child of node.Plans||[])visit(child);};
    visit(explanation.rows[0]['QUERY PLAN'][0].Plan);
    assert.ok(identityReads<=25,`one 20-channel batch revisited ${identityReads} Run identities despite existing foreign-key-backed ownership`);
    assert.ok(payloadChecks>0 && payloadChecks<=20,`one 20-channel batch inspected ${payloadChecks} Run payloads`);
  } finally {await probe.query('ROLLBACK');probe.release();}
  // Walk past another batch, both kinds of execution row lock, and a future
  // retry without starving lower-priority eligible channels or changing batch size.
  await query("UPDATE crawler.channels SET priority=1000-substring(channel_id from 8)::int");
  await query("UPDATE crawler.channel_runs SET result_json=jsonb_build_object('dispatch_batch_id','another-batch') WHERE run_id IN ('agent-run-1','agent-run-2')");
  await query("UPDATE crawler.channels SET agent_next_retry_at=now()+interval '1 day' WHERE channel_id='UCagent5'");
  // Preserve legacy Run ownership via pipeline_cycle_id as well.
  await query("UPDATE crawler.channel_runs SET result_json=jsonb_build_object('pipeline_cycle_id',$1::text) WHERE run_id='agent-run-6'",[batch]);
  const locks=await pool.connect(),picker=await pool.connect();
  try {
    await locks.query('BEGIN');
    await locks.query("SELECT channel_id FROM crawler.channels WHERE channel_id='UCagent3' FOR UPDATE");
    await locks.query("SELECT run_id FROM crawler.channel_runs WHERE run_id='agent-run-4' FOR UPDATE");
    await picker.query('BEGIN');
    await picker.query("SET LOCAL statement_timeout='2s'");
    const picked=await picker.query(selection.sql,selection.args);
    assert.deepEqual(picked.rows.map(r=>Number(r.channel_id.slice(7))).sort((a,b)=>a-b),Array.from({length:20},(_,i)=>i+6));
  } finally {
    await picker.query('ROLLBACK');picker.release();
    await locks.query('ROLLBACK');locks.release();
  }
  await query("UPDATE crawler.channel_runs SET result_json=jsonb_build_object('dispatch_batch_id',$1::text) WHERE run_id IN ('agent-run-1','agent-run-2')",[batch]);
  await query("UPDATE crawler.channels SET agent_next_retry_at=NULL WHERE channel_id='UCagent5'");
  // Active recovery belongs to its existing owner; resolved/cancelled history
  // must not keep a normal channel out of future batches.
  const recoveryCandidate=(await query("SELECT candidate_id FROM crawler.channel_candidates WHERE channel_id='UCagent1'")).rows[0].candidate_id;
  await query("UPDATE crawler.channel_runs SET candidate_id=$1 WHERE run_id='agent-run-1'",[recoveryCandidate]);
  const intent=(await query(`INSERT INTO crawler.migration_channel_intents(source_id,source_database,source_database_oid,source_candidate_id,channel_id,source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id)
    VALUES('agent-owner-test',current_database(),(SELECT oid FROM pg_database WHERE datname=current_database()),$1,'UCagent1','{}',repeat('a',64),$1,$2) RETURNING migration_intent_id`,[recoveryCandidate,batch])).rows[0].migration_intent_id;
  const retry=(await query(`INSERT INTO crawler.migration_system_retry_items(migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation,failed_job_id,failed_job_attempt,failure_code,failure_category,status)
    VALUES($1,$2,$3,1,'agent-owner-test',1,'SYSTEM_ROUTE','system','pending') RETURNING system_retry_id`,[intent,recoveryCandidate,batch])).rows[0].system_retry_id;
  for(const status of ['pending','retrying','dispatched','resolved','cancelled']) {
    await query('UPDATE crawler.migration_system_retry_items SET status=$2 WHERE system_retry_id=$1',[retry,status]);
    const client=await pool.connect();
    try {
      await client.query('BEGIN');
      const selected=await client.query(selection.sql,selection.args);
      const first=['resolved','cancelled'].includes(status)?1:2;
      assert.deepEqual(selected.rows.map(r=>Number(r.channel_id.slice(7))).sort((a,b)=>a-b),Array.from({length:20},(_,i)=>i+first));
    } finally {await client.query('ROLLBACK');client.release();}
  }
  // A later Run can have another Candidate. An old Registry owner's recovery
  // must not exclude this Run or replace its authoritative batch metadata.
  const oldOwner=(await query("SELECT registry_promotion_candidate_id FROM crawler.channels WHERE channel_id='UCagent61'")).rows[0].registry_promotion_candidate_id;
  await query("INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,pipeline_cycle_id,status) VALUES('agent-other-batch','agent-other-batch','running')");
  const laterOwner=(await query("INSERT INTO crawler.channel_candidates(dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status) VALUES('agent-other-batch','agent-other-batch','UCagent61','https://youtube.com/channel/UCagent61','discovered') RETURNING candidate_id")).rows[0].candidate_id;
  await query("UPDATE crawler.channel_runs SET candidate_id=$1 WHERE run_id='agent-run-61'",[laterOwner]);
  const oldIntent=(await query(`INSERT INTO crawler.migration_channel_intents(source_id,source_database,source_database_oid,source_candidate_id,channel_id,source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id)
    VALUES('agent-owner-test',current_database(),(SELECT oid FROM pg_database WHERE datname=current_database()),$1,'UCagent61','{}',repeat('a',64),$1,$2) RETURNING migration_intent_id`,[oldOwner,batch])).rows[0].migration_intent_id;
  await query(`INSERT INTO crawler.migration_system_retry_items(migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation,failed_job_id,failed_job_attempt,failure_code,failure_category,status)
    VALUES($1,$2,$3,1,'agent-old-owner',1,'SYSTEM_ROUTE','system','pending')`,[oldIntent,oldOwner,batch]);
  for(let i=0;i<recoveryBacklog;i++)await queue.add('agent-profile-batch',{migration_system_retry_id:i+1,channel_ids:[`recovery-${i}`]});
  for(let i=0;i<3;i++){
    const w=new Worker(queue.name,async j=>{if(j.data.migration_system_retry_id){await recoveryGate;return;}sizes.push(j.data.channel_ids.length);for(const id of j.data.channel_ids){assert.ok(!seen.has(id),`duplicate Agent channel ${id}`);seen.add(id);}await gate;await query("UPDATE crawler.channels SET agent_status='done',ready_for_agent=false WHERE channel_id=ANY($1::text[])",[j.data.channel_ids]);},{connection,prefix:queue.opts.prefix});
    w.on('error',e=>errors.push(e.message));w.on('failed',(_j,e)=>errors.push(e.message));workers.push(w);
  }
  await Promise.all(workers.map(w=>w.waitUntilReady()));await until(async()=>await queue.getWorkersCount()===3);
  const dispatch=createAgentBatchDispatch({query,agentQueue:queue,agentBatchSize:20,agentMaxBatchesPerTick:100});
  const configs=[{config_id:1,enabled:true,batch_size:20,max_workers:3}];
  let maintenanceDone=false;
  loops=createControllerWorkLoops({tasks:{
    publication:{intervalMs:300000,run:async()=>{await maintenanceGate;maintenanceDone=true;}},
    agent:{intervalMs:100,run:async()=>{const actions=[];const capacity=await dispatch.syncCapacity(actions,configs);await dispatch.dispatch(actions,configs,capacity,{pipeline_cycle_id:batch,status:'finishing'});}},
  },onError:({error})=>errors.push(error.message)});
  loops.start();
  if(recoveryBacklog){
    await until(async()=>{const waiting=await queue.getJobs(['waiting']);return waiting.filter(j=>!j.data.migration_system_retry_id).length===3;});
    assert.equal(await queue.getActiveCount(),3,'queued normal batches must not exceed execution concurrency');
  }
  releaseRecoveries();await until(()=>seen.size===60);
  assert.equal(maintenanceDone,false);assert.deepEqual(sizes,[20,20,20]);
  await delay(200);assert.equal(await queue.getActiveCount(),3);assert.equal(seen.size,60);
  release();await until(async()=>await queue.getActiveCount()===0);
  assert.equal(await agentTailReady(query,batch),false);await delay(200);assert.equal(seen.size,60);
  await query("UPDATE crawler.channel_candidates SET status='accepted' WHERE dispatch_batch_id=$1",[batch]);
  await until(()=>seen.size===65);assert.deepEqual(sizes,[20,20,20,5]);assert.equal(maintenanceDone,false);
  unblock();await loops.shutdown();assert.deepEqual(errors,[]);
});
