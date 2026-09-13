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
test('real Agent dispatch fills three consumers during blocked maintenance, preserves tail eligibility and never repeats channels', {skip:!url||!port}, async t=>{
  assert.equal(new URL(url).pathname,'/agent_dispatch_test');
  const pool=new pg.Pool({connectionString:url,max:6,options:`-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
  const query=pool.query.bind(pool),connection={host:'127.0.0.1',port,password:process.env.THROUGHPUT_TEST_REDIS_PASSWORD,maxRetriesPerRequest:null};
  const queue=new Queue('youtube-agent-batch',{connection,prefix:`agent-dispatch-${Date.now()}`});
  const workers=[],seen=new Set(),sizes=[],errors=[];
  let loops,release,unblock;const gate=new Promise(r=>{release=r}),maintenanceGate=new Promise(r=>{unblock=r});
  t.after(async()=>{release();unblock();await loops?.shutdown();await Promise.all(workers.map(w=>w.close()));await queue.obliterate({force:true});await queue.close();await pool.end();});
  await query('DROP SCHEMA IF EXISTS publication CASCADE; DROP SCHEMA IF EXISTS crawler CASCADE');
  await query(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8'));
  const batch='agent-test-batch';
  await query(`INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at) VALUES($1,$1,'discovery_closed',now())`,[batch]);
  await query(`INSERT INTO crawler.channel_candidates(dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status)
    SELECT $1,$1,'UCagent'||n,'https://youtube.com/channel/UCagent'||n,'discovered' FROM generate_series(1,65)n`,[batch]);
  await query(`INSERT INTO crawler.channels(channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status)
    SELECT 'UCagent'||n,'https://youtube.com/channel/UCagent'||n,'Agent test','active',2000,true,'pending' FROM generate_series(1,65)n`);
  await query(`INSERT INTO crawler.channel_runs(run_id,channel_id,crawl_mode,result_json)
    SELECT 'agent-run-'||n,'UCagent'||n,'full',jsonb_build_object('dispatch_batch_id',$1::text) FROM generate_series(1,65)n`,[batch]);
  await query(`UPDATE crawler.channels SET latest_run_id='agent-run-'||substring(channel_id from 8)`);
  for(let i=0;i<3;i++){
    const w=new Worker(queue.name,async j=>{sizes.push(j.data.channel_ids.length);for(const id of j.data.channel_ids){assert.ok(!seen.has(id),`duplicate Agent channel ${id}`);seen.add(id);}await gate;await query("UPDATE crawler.channels SET agent_status='done',ready_for_agent=false WHERE channel_id=ANY($1::text[])",[j.data.channel_ids]);},{connection,prefix:queue.opts.prefix});
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
  loops.start();await until(()=>seen.size===60);
  assert.equal(maintenanceDone,false);assert.deepEqual(sizes,[20,20,20]);
  await delay(200);assert.equal(await queue.getActiveCount(),3);assert.equal(seen.size,60);
  release();await until(async()=>await queue.getActiveCount()===0);
  assert.equal(await agentTailReady(query,batch),false);await delay(200);assert.equal(seen.size,60);
  await query("UPDATE crawler.channel_candidates SET status='accepted' WHERE dispatch_batch_id=$1",[batch]);
  await until(()=>seen.size===65);assert.deepEqual(sizes,[20,20,20,5]);assert.equal(maintenanceDone,false);
  unblock();await loops.shutdown();assert.deepEqual(errors,[]);
});
