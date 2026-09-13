#!/usr/bin/env node
// Launch under `flock -n <state-file>.lock node ...` so one process owns the
// durable cursor. Database ownership is checked separately for every dispatch.
import {parseArgs} from 'node:util';
import {readFile,writeFile,rename,appendFile} from 'node:fs/promises';
import {setTimeout as sleep} from 'node:timers/promises';
import {Queue} from 'bullmq';
import {query,withTransaction as dbTransaction,closeDb} from '../src/db.js';
import {verifyCrawlerWriterDatabase} from '../src/databaseIdentity.js';
import {redisOptions,bullmqPrefix} from '../src/queues.js';
import {retryMigrationSystemFailure} from '../src/migrationSystemRetry.js';
import {deliverExistingChannelSnapshotOutbox} from '../src/manualMigrationDispatch.js';

const {values:v}=parseArgs({options:{'batch-id':{type:'string'},manifest:{type:'string'},state:{type:'string'},
  snapshot:{type:'boolean'},execute:{type:'boolean'},limit:{type:'string',default:'20'},'max-open':{type:'string',default:'100'}}});
if(!v['batch-id']||!v.manifest)throw Error('--batch-id and --manifest are required');
const batchId=v['batch-id'];
const withTransaction=action=>dbTransaction(async client=>{
  await client.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s'");
  return action(client);
});
const failure='BeginTask Lease conflict occurred after a Task became active';
let stopping=false;process.once('SIGTERM',()=>{stopping=true;});process.once('SIGINT',()=>{stopping=true;});
const log=data=>console.log(JSON.stringify({at:new Date().toISOString(),...data}));
let queue;
try{
  await verifyCrawlerWriterDatabase(query);
  if(v.snapshot){
    const rows=(await query(`SELECT r.system_retry_id,r.candidate_id,c.status AS candidate_status
      FROM crawler.migration_system_retry_items r
      JOIN crawler.migration_control_items i ON i.batch_id=r.failed_dispatch_batch_id AND i.candidate_id=r.candidate_id
      JOIN crawler.channel_candidates c ON c.candidate_id=r.candidate_id
      WHERE r.failed_dispatch_batch_id=$1 AND r.status='pending' AND i.outcome='failed'
        AND r.failure_code='SYSTEM_ROUTE' AND r.failure_evidence#>>'{system_failure,message}'=$2
      ORDER BY r.system_retry_id`,[batchId,failure])).rows;
    // Include partially collected channels in the first verification group.
    const partial=rows.filter(r=>r.candidate_status==='accepted').slice(0,5);
    const chosen=new Set(partial.map(r=>r.system_retry_id));
    const manifest={batchId,failure,createdAt:new Date().toISOString(),items:[...partial,...rows.filter(r=>!chosen.has(r.system_retry_id))]};
    await writeFile(v.manifest,JSON.stringify(manifest),{flag:'wx',mode:0o600});
    log({event:'snapshot',count:rows.length,partialSamples:partial.length});
  }else{
    const manifest=JSON.parse(await readFile(v.manifest,'utf8'));
    if(manifest.batchId!==batchId||manifest.failure!==failure)throw Error('Manifest scope mismatch');
    const limit=Number(v.limit),maxOpen=Number(v['max-open']);
    if(!Number.isSafeInteger(limit)||limit<1||!Number.isSafeInteger(maxOpen)||maxOpen<1||maxOpen>200)throw Error('Invalid limits');
    if(!v.execute){log({event:'plan',count:manifest.items.length,limit,maxOpen});}
    else{
      if(!v.state)throw Error('--state is required for execution');
      let state={batchId,cursor:0,submitted:0,skipped:0,open:[]};
      try{state=JSON.parse(await readFile(v.state,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
      if(state.batchId!==batchId)throw Error('State scope mismatch');
      const persist=async()=>{await writeFile(v.state+'.tmp',JSON.stringify(state),{mode:0o600});await rename(v.state+'.tmp',v.state);};
      queue=new Queue('youtube-channel-crawl',{connection:redisOptions,prefix:bullmqPrefix});
      // The launcher holds an OS flock for the durable progress file.
      let lastCircuitCheck=0;
      try{
        while(!stopping&&state.cursor<Math.min(limit,manifest.items.length)){
          if(Date.now()-lastCircuitCheck>10000){
            const recurrence=(await query(`SELECT EXISTS(SELECT 1 FROM crawler.migration_system_retry_items
              WHERE status IN ('pending','retrying','dispatched') AND requested_at>$1
                AND failed_dispatch_batch_id=$2 AND failure_code='SYSTEM_ROUTE'
                AND failure_evidence#>>'{system_failure,message}'=$3) AS found`,
              [manifest.createdAt,batchId,failure])).rows[0].found;
            if(recurrence)throw Error('Rota lease conflict recurred; recovery dispatch stopped');
            lastCircuitCheck=Date.now();
          }
          if(state.open.length){
            const rows=(await query('SELECT system_retry_id,status FROM crawler.migration_system_retry_items WHERE system_retry_id=ANY($1::bigint[])',[state.open])).rows;
            state.open=rows.filter(r=>['retrying','dispatched'].includes(r.status)).map(r=>String(r.system_retry_id));
          }
          const counts=await queue.getJobCounts('active','waiting','prioritized','delayed');
          const backlog=Object.values(counts).reduce((n,v)=>n+Number(v),0);
          if(await queue.isPaused()||state.open.length>=maxOpen||backlog>=maxOpen){
            await persist();log({event:'waiting_capacity',cursor:state.cursor,open:state.open.length,backlog});await sleep(10000);continue;
          }
          const item=manifest.items[state.cursor];
          const current=(await query(`SELECT status,failure_code,failure_evidence#>>'{system_failure,message}' AS message
            FROM crawler.migration_system_retry_items WHERE system_retry_id=$1 AND candidate_id=$2 AND failed_dispatch_batch_id=$3`,
            [item.system_retry_id,item.candidate_id,batchId])).rows[0];
          if(!current||current.failure_code!=='SYSTEM_ROUTE'||current.message!==failure)throw Error('Cohort member changed');
          if(!['pending','dispatched'].includes(current.status)){
            await appendFile(v.state+'.events',JSON.stringify({id:item.system_retry_id,skipped:current.status})+'\n',{mode:0o600});state.skipped++;state.cursor++;await persist();continue;
          }
          try{
            const allocation=await retryMigrationSystemFailure({systemRetryId:item.system_retry_id,controlledBatchId:batchId,withTransaction,
              minSubscriberCount:Number(process.env.MIN_SUBSCRIBER_COUNT||1000)});
            await deliverExistingChannelSnapshotOutbox(queue,allocation.outbox,{dbQuery:query});
            state.open.push(String(item.system_retry_id));state.cursor++;state.submitted++;
            await appendFile(v.state+'.events',JSON.stringify({id:item.system_retry_id,candidateId:item.candidate_id,generation:allocation.dispatch_generation,at:new Date().toISOString()})+'\n',{mode:0o600});
            await persist();
            if(state.cursor%10===0||state.cursor===limit)log({event:'submitted',cursor:state.cursor,submitted:state.submitted,open:state.open.length});
          }catch(e){
            if(['migration_controlled_retry_blocked','55P03','57014','40P01'].includes(e.code)){
              log({event:'waiting_admission',code:e.code,cursor:state.cursor});await sleep(10000);continue;
            }
            throw e;
          }
        }
        await persist();log({event:stopping?'stopped':'dispatch_window_complete',...state,total:manifest.items.length});
      }finally{await persist();}
    }
  }
}finally{if(queue)await queue.close();await closeDb();}
