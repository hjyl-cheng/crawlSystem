import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import pg from 'pg';
import {Queue,Worker} from 'bullmq';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
import {LocalIncrementalIntake,createLocalIntakeAdmin} from '../src/localIncrementalIntake.js';
import {intakeWorkerName} from '../src/workerIntakeTelemetry.js';
import {incrementalWorkerCapacity} from '../../feature-dispatch/src/dynamicDispatcher.js';
const databaseUrl=process.env.LOCAL_INTAKE_TEST_DATABASE_URL,redisUrl=process.env.LOCAL_INTAKE_TEST_REDIS_URL;
test('local count control drains only surplus Workers, persists zero through restart and reports actual capacity',{
 skip:!databaseUrl||!redisUrl,timeout:30000,
},async t=>{
 const db=new URL(databaseUrl),redis=new URL(redisUrl);assert.match(db.pathname,/_test$/);
 for(const u of [db,redis])assert.ok(['localhost','127.0.0.1'].includes(u.hostname));
 const pool=new pg.Pool({connectionString:databaseUrl});const query=pool.query.bind(pool);
 const transaction=async action=>{const c=await pool.connect();try{await c.query('BEGIN');const r=await action(c);await c.query('COMMIT');return r;}catch(e){await c.query('ROLLBACK');throw e;}finally{c.release();}};
 await query('CREATE SCHEMA IF NOT EXISTS remote_ingestion');await query(await readFile(new URL('../src/remoteNodes/localIntakeSchema.sql',import.meta.url),'utf8'));
 for(const file of ['schema.sql','routeSchema.sql','natsSchema.sql'])await query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
 const signals=await createTransportSignals({connectionString:databaseUrl});
 const prefix='local-intake-test-'+randomUUID();const connection={host:redis.hostname,port:Number(redis.port),password:decodeURIComponent(redis.password)||undefined};
 const queue=new Queue('youtube-channel-incremental',{connection,prefix});const controls=[],workers=[],calls=[],release=new Map();
 const admin=createLocalIntakeAdmin({query,transaction});
 const until=async condition=>{const end=Date.now()+8000;while(!await condition()){assert.ok(Date.now()<end,'condition timeout');await delay(20);}};
 const start=async id=>{
  let control;const worker=new Worker(queue.name,(job,token)=>control.process(job,token,async()=>{
    calls.push({worker:id,id:job.id});if(job.name==='hold')await new Promise(r=>release.set(id,r));return job.id;
  }),{connection,prefix,autorun:false,concurrency:1,name:intakeWorkerName('local',id)});
  worker.on('error',()=>{});workers.push(worker);
  control=new LocalIncrementalIntake({worker,query,workerId:id,intervalMs:50,signals});controls.push(control);await control.start();return control;
 };
 t.after(async()=>{for(const r of release.values())r();for(const c of controls)await c.stop();for(const w of workers)await w.close();await queue.obliterate({force:true});await queue.close();await signals.close();await pool.end();});
 for(let i=1;i<=3;i++)await start(`test-worker-${i}`);
 const set=async(allowedCount,expectedAllowedCount)=>{const s=await admin.setExecution({workerCount:3,allowedCount,expectedAllowedCount});await until(async()=>(await admin.status()).workers.filter(w=>w.readyForTasks).length===allowedCount);return admin.status();};
 assert.equal((await admin.status()).allowedCount,0,'new Workers start in standby');
 await set(2,0);await queue.addBulk([1,2].map(i=>({name:'hold',data:{},opts:{jobId:`held-${i}`}})));await until(()=>release.size===2);
 await set(1,2);let state=await admin.status();assert.equal(state.counts.running,1);assert.equal(state.counts.draining,1);
 await until(async()=>await incrementalWorkerCapacity(queue)===1);
 await queue.add('ordinary',{}, {jobId:'next'});release.get('test-worker-2')();await until(async()=>(await admin.status()).counts.draining===0);
 await delay(100);assert.equal(calls.length,2,'retiring Worker cannot take next channel');
 release.get('test-worker-1')();await until(async()=>(await queue.getJob('next')).getState().then(x=>x==='completed'));
 assert.equal(calls.find(x=>x.id==='next').worker,'test-worker-1');
 await set(0,1);await until(async()=>await incrementalWorkerCapacity(queue)===0);
 await queue.add('ordinary',{}, {jobId:'after-restart'});await controls[0].stop();await start('test-worker-1');
 await delay(150);assert.equal((await admin.status()).allowedCount,0);assert.equal(calls.length,3);
 await set(3,0);await until(async()=>(await queue.getJob('after-restart')).getState().then(x=>x==='completed'));
 await until(async()=>await incrementalWorkerCapacity(queue)===3);
 assert.equal(new Set(calls.map(x=>x.id)).size,calls.length);
 await assert.rejects(set(4,3),{code:'INVALID_EXECUTION_COUNT'});
 await assert.rejects(set(1,0),{code:'EXECUTION_CONTROL_CHANGED'});
});
