#!/usr/bin/env node
import {parseArgs} from 'node:util';
import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import {query,withTransaction as transaction,closeDb} from '../src/db.js';
import {verifyCrawlerWriterDatabase} from '../src/databaseIdentity.js';
import {markPendingMigrationFailuresForNormalExecution} from '../src/controlledMigrationRetry.js';

const {values}=parseArgs({options:{manifest:{type:'string'},execute:{type:'boolean'},limit:{type:'string'}}});
if(!values.manifest) throw Error('--manifest is required');
const manifest=JSON.parse(await readFile(values.manifest,'utf8'));
if(!manifest.batchId || !manifest.failure || !Array.isArray(manifest.items)
    || manifest.items.some(item=>!Number.isSafeInteger(Number(item.system_retry_id)) || Number(item.system_retry_id)<1)) {
  throw Error('An explicit frozen failure manifest is required');
}
const limit=values.limit==null?manifest.items.length:Number(values.limit);
if(!Number.isSafeInteger(limit) || limit<1) throw Error('Invalid limit');
const items=manifest.items.slice(0,limit);
const log=value=>console.log(JSON.stringify({at:new Date().toISOString(),...value}));
const withTransaction=fn=>transaction(async client=>{
  await client.query("SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='15s'");
  return fn(client);
});
let stopping=false;
process.once('SIGTERM',()=>{stopping=true;});
process.once('SIGINT',()=>{stopping=true;});
try {
  await verifyCrawlerWriterDatabase(query);
  log({event:'scope',batch_id:manifest.batchId,manifest_items:items.length,execute:!!values.execute,
    eligibility:'Only still-pending original failed items; running and terminal items remain unchanged'});
  if(values.execute){
    let changed=0,checked=0,retries=0;
    while(checked<items.length && !stopping){
      const chunk=items.slice(checked,checked+250);
      try {
        const rows=await markPendingMigrationFailuresForNormalExecution({withTransaction,
          batchId:manifest.batchId,systemRetryIds:chunk.map(item=>item.system_retry_id),
          failureCode:'SYSTEM_ROUTE',failureMessage:manifest.failure});
        checked+=chunk.length;changed+=rows.length;retries=0;
        log({event:'marked',checked,changed,unchanged:checked-changed,total:items.length});
      }catch(error){
        if(!['55P03','57014','40P01'].includes(error.code) || ++retries>5)throw error;
        log({event:'retrying_chunk',checked,code:error.code,retries});
        await delay(1000);
      }
    }
    log({event:stopping?'stopped':'complete',checked,changed,total:items.length});
  }
}finally {await closeDb();}
