#!/usr/bin/env node
import {parseArgs} from 'node:util';
import {readFile} from 'node:fs/promises';
import {setTimeout as sleep} from 'node:timers/promises';
import {query,withTransaction as transaction,closeDb} from '../src/db.js';
import {verifyCrawlerWriterDatabase} from '../src/databaseIdentity.js';
import {restoreControlledMigrationFailures} from '../src/controlledMigrationRetry.js';

const {values}=parseArgs({options:{manifest:{type:'string'},state:{type:'string'},execute:{type:'boolean'},limit:{type:'string'}}});
if(!values.manifest||!values.state)throw Error('--manifest and stopped recovery --state are required');
const manifest=JSON.parse(await readFile(values.manifest,'utf8'));
const state=JSON.parse(await readFile(values.state,'utf8'));
if(manifest.batchId!==state.batchId||!Number.isSafeInteger(state.cursor)||state.cursor<0||state.cursor>manifest.items.length)throw Error('Recovery scope mismatch');
const remaining=manifest.items.slice(state.cursor);
const limit=values.limit==null?remaining.length:Number(values.limit);
if(!Number.isSafeInteger(limit)||limit<0)throw Error('Invalid limit');
const items=remaining.slice(0,limit);
let stopping=false;process.once('SIGTERM',()=>{stopping=true;});process.once('SIGINT',()=>{stopping=true;});
const log=value=>console.log(JSON.stringify({at:new Date().toISOString(),...value}));
const withTransaction=fn=>transaction(async client=>{
  await client.query("SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='30s'");
  return fn(client);
});
try{
  await verifyCrawlerWriterDatabase(query);
  log({event:'restore_scope',batchId:manifest.batchId,alreadyDispatched:state.cursor,remaining:items.length,execute:!!values.execute});
  if(values.execute){
    let restored=0;
    for(let offset=0;offset<items.length&&!stopping;){
      const chunk=items.slice(offset,offset+500);
      try{
        const rows=await restoreControlledMigrationFailures({withTransaction,batchId:manifest.batchId,
          systemRetryIds:chunk.map(item=>item.system_retry_id),failureCode:'SYSTEM_ROUTE',failureMessage:manifest.failure});
        restored+=rows.length;offset+=chunk.length;
        // Restore the planner's pending-state estimates before the controller
        // starts refilling a table which previously had no pending items.
        if(offset===chunk.length||offset%10000===0||offset===items.length)await query('ANALYZE crawler.migration_control_items');
        log({event:'restored',checked:offset,restored,unchanged:offset-restored,total:items.length});
      }catch(error){
        if(!['55P03','57014','40P01'].includes(error.code))throw error;
        log({event:'waiting_batch_lock',code:error.code,checked:offset});await sleep(1000);
      }
    }
    log({event:stopping?'stopped':'restoration_complete',restored,total:items.length});
  }
}finally{await closeDb();}
