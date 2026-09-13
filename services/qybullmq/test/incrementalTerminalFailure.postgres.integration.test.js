import assert from 'node:assert/strict';
import test from 'node:test';
import pg from 'pg';
import {readFile} from 'node:fs/promises';
import {crawlerRuntimeSchema} from '../src/publicationCurrentSchema.js';
import {recordIncrementalTerminalFailure} from '../src/incrementalTerminalFailure.js';
const databaseUrl=process.env.CLOCK_FAILURE_TEST_DATABASE_URL;
test('restored Clock can record a new failure and replay that failure without duplicating observations', {skip:!databaseUrl}, async()=>{
 const url=new URL(databaseUrl);assert.equal(url.pathname,'/clock_failure_test');assert.ok(['127.0.0.1','localhost'].includes(url.hostname));
 const pool=new pg.Pool({connectionString:databaseUrl});const client=await pool.connect();
 try{await client.query('BEGIN');await client.query(crawlerRuntimeSchema(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8')));
 const plan={schema_version:5,dispatch_generation:1,job_id:'incremental__clock_failure_test',plan_id:'4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d',plan_mode:'standard',plan_day:'2026-09-11',channel_id:'clock-failure-test',task_mask:{about:false,video:true,agent:false},capacity:{factor:1,player_cap:20,next_cap:8,version:'capacity-v1'},scheduled_at:'2026-09-11T01:00:00.000Z',clock_version:6,policy_version:'v16-rule-6',planner_config_version:'video-plan-1'};
 const runId='incremental:'+plan.plan_id;
 await client.query("INSERT INTO crawler.channels(channel_id,channel_url,title) VALUES($1,'https://youtube.com/channel/test','test')",[plan.channel_id]);
 await client.query("INSERT INTO crawler.channel_runs(run_id,channel_id,plan_id,status,crawl_mode,started_at,result_json) VALUES($1,$2,$3,'running','incremental',now(),$4)",[runId,plan.channel_id,plan.plan_id,{domains:{video:{status:'running'}}}]);
 const invoke=(attempts,error='STALE_LEASE')=>recordIncrementalTerminalFailure({job:{id:plan.job_id,name:'channel.incremental.plan',queueName:'youtube-channel-incremental',data:plan},attempts,maxAttempts:5,error:new Error(error),withTransaction:action=>action(client)});
 await invoke(5);
 const later=await invoke(6,'BUSINESS_RUN_BUDGET_EXHAUSTED');
 assert.equal(later.recorded,true);
 await new Promise(resolve=>setTimeout(resolve,20));
 const replay=await invoke(6,'BUSINESS_RUN_BUDGET_EXHAUSTED');
 assert.equal(replay.observation.duplicate,true);
 assert.equal((await client.query('SELECT count(*)::int n FROM crawler.crawl_observations WHERE run_id=$1',[runId])).rows[0].n,2);
 await client.query("UPDATE crawler.channel_runs SET result_json=$2 WHERE run_id=$1",[runId,{domains:{video:{status:'complete'}}}]);
 assert.equal((await invoke(7)).reason,'no_failed_domain');
 }finally{await client.query('ROLLBACK');client.release();await pool.end()}
});
