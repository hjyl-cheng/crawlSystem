import test from 'node:test';
import assert from 'node:assert/strict';
import {AsyncLocalStorage} from 'node:async_hooks';
import {randomUUID} from 'node:crypto';
import {runRemoteIncrementalPlan} from '../src/remoteNodes/incrementalCoordinator.js';
import {incrementalPlanHash} from '../src/incrementalPlan.js';

for(const failing of [false,true])test(`finished remote Plans release async transaction contexts (${failing?'failure':'success'})`,async()=>{
 const scopes=new Set(),disabled=new Set();const run=AsyncLocalStorage.prototype.run,disable=AsyncLocalStorage.prototype.disable;
 AsyncLocalStorage.prototype.run=function(store,...args){if(store?.fixtureClient)scopes.add(this);return run.call(this,store,...args)};
 AsyncLocalStorage.prototype.disable=function(){disabled.add(this);return disable.call(this)};
 try{
  await Promise.all(Array.from({length:40},async(_,i)=>{
   const plan={schema_version:5,dispatch_generation:1,job_id:`scope-${i}`,plan_id:randomUUID(),plan_mode:'standard',plan_day:'2026-09-12',scheduled_at:'2026-09-12T00:30:00.000Z',channel_id:`UCscope${i}`,
    task_mask:{about:true,video:false,agent:false},capacity:{factor:1,player_cap:20,next_cap:8,version:'capacity-1'},clock_version:1,policy_version:'v16-rule-1',planner_config_version:'video-plan-1'};
   const task={task_id:randomUUID(),generation:1,capability:'youtube.incremental.plan.v1',input:{plan},context:{plan_hash:incrementalPlanHash(plan),execution_attempt_id:`attempt-${i}`}};
   const client={fixtureClient:true,query:async sql=>{
    await Promise.resolve();
    if(failing)throw Object.assign(Error('fixture failure'),{code:'FIXTURE_FAILURE'});
    if(sql.includes('INSERT INTO crawler.channel_runs'))return {rows:[],rowCount:0};
    return {rows:[{run_id:`incremental:${plan.plan_id}`,channel_id:plan.channel_id,status:'done',result_json:{plan_payload_hash:incrementalPlanHash(plan)}}],rowCount:1};
   }};
   const channelStore={coordinate:async()=>({task,coordinatorId:'coordinator'}),transaction:async(_l,_c,_f,action)=>action(client),
    complete:async c=>assert.equal(c,client,'concurrent Plans must not share transaction clients'),lock:async()=>task,store:{transaction:async action=>action(client)}};
   const result=runRemoteIncrementalPlan({channelStore,lease:{task_id:task.task_id},assertBusinessFence:async()=>{},createApiFallback:()=>null});
   if(failing)await assert.rejects(result,{code:'FIXTURE_FAILURE'});else assert.equal((await result).terminal,true);
  }));
  assert.equal(scopes.size,40);assert.equal([...scopes].filter(s=>disabled.has(s)).length,40,'finished per-Plan storage must be disabled or Node retains it in async hook propagation');
 }finally{AsyncLocalStorage.prototype.run=run;AsyncLocalStorage.prototype.disable=disable;for(const scope of scopes)disable.call(scope)}
});
