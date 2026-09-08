import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {withMigrationSourceReadTransaction,closeMigrationSourcePool,sourceSnapshotHash} from '../src/migrationSource.js';
import {pool,closeDb} from '../src/db.js';
import {verifyCrawlerWriterDatabase} from '../src/databaseIdentity.js';
import {retainRestoredMigrationInventory} from '../src/restoredMigrationSources.js';
const apply=process.argv.includes('--apply');
const restorationId='legacy-active-20260908-23507';
try {
 const source=await withMigrationSourceReadTransaction(async(c,identity,config)=>{
  const rows=(await c.query(`SELECT ch.channel_id,ch.title,ch.handle,ch.avatar_url,ch.subscriber_count,
    ch.agent_status,cc.candidate_id,cc.status AS previous_status,cc.source_json->>'source' AS previous_source,
    cc.created_at,cc.updated_at
   FROM crawler.channels ch JOIN LATERAL (
    SELECT candidate_id,status,source_json,created_at,updated_at FROM crawler.channel_candidates c
    WHERE c.channel_id=ch.channel_id ORDER BY priority DESC,candidate_id DESC LIMIT 1
   ) cc ON true WHERE ch.status='active' ORDER BY ch.channel_id`)).rows;
  return {rows,identity,config};
 },{statementTimeoutMs:120000});
 assert.equal(source.rows.length,23507,'Active source scope changed; re-audit');
 assert.equal(source.rows.filter(r=>r.agent_status==='done').length,23433);
 const snapshots=source.rows.map(r=>{
  assert.match(r.channel_id,/^UC[A-Za-z0-9_-]{22}$/);
  const snapshot={source_id:source.config.sourceId,source_database:source.identity.database,
   source_database_oid:String(source.identity.databaseOid),source_candidate_id:String(r.candidate_id),
   source_candidate_status:'discovered',source_dispatch_batch_id:null,channel_id:r.channel_id,
   channel_url:`https://www.youtube.com/channel/${r.channel_id}`,handle:r.handle,title:r.title,
   description:null,avatar_url:r.avatar_url,search_subscriber_count:r.subscriber_count,
   search_subscriber_count_text:null,is_verified:null,priority:100,snapshot_json:{},
   source_json:{source:'restored_legacy_active',restoration:{id:restorationId,
    previous_status:r.previous_status,previous_source:r.previous_source,previous_channel_status:'active',
    previous_agent_status:r.agent_status}},source_created_at:r.created_at,source_updated_at:r.updated_at};
  return {...snapshot,snapshot_sha256:sourceSnapshotHash(snapshot)};
 });
 const client=await pool.connect();
 try {
  await client.query('BEGIN');
  await client.query("SET LOCAL lock_timeout='10s'");
  await client.query("SET LOCAL statement_timeout='120s'");
  await verifyCrawlerWriterDatabase(client.query.bind(client),process.env);
  await client.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',[`migration-inventory-sync:${source.config.sourceId}`]);
  const ids=source.rows.map(r=>r.channel_id);
  const overlaps=(await client.query(`SELECT channel_id FROM crawler.channels WHERE channel_id=ANY($1::text[])
    UNION SELECT channel_id FROM crawler.channel_candidates WHERE channel_id=ANY($1::text[])
    UNION SELECT channel_id FROM crawler.migration_channel_intents WHERE channel_id=ANY($1::text[])`,[ids])).rows;
  assert.equal(overlaps.length,0,'Some channels already entered target; re-audit');
  await client.query(await readFile(new URL('../src/restoredMigrationSourcesSchema.sql',import.meta.url),'utf8'));
  const sync=(await client.query('SELECT * FROM crawler.migration_channel_inventory_syncs WHERE source_id=$1 FOR UPDATE',[source.config.sourceId])).rows[0];
  assert.equal(sync.status,'ready');
  assert.equal(sync.source_database,source.identity.database);
  assert.equal(String(sync.source_database_oid),String(source.identity.databaseOid));
  const previousInventory=(await client.query(
    'SELECT * FROM crawler.migration_channel_inventory WHERE source_id=$1 AND channel_id=ANY($2::text[])',
    [source.config.sourceId,ids],
  )).rows;
  assert.equal(previousInventory.length,6,'Existing inventory overlap changed; re-audit');
  const previousById=new Map(previousInventory.map(row=>[row.channel_id,row]));
  for(const snapshot of snapshots){
    const previous=previousById.get(snapshot.channel_id);
    if(previous){snapshot.source_json.restoration.previous_inventory=previous;
      snapshot.snapshot_sha256=sourceSnapshotHash(snapshot);}
  }
  const before=Number((await client.query('SELECT count(*) AS count FROM crawler.migration_channel_inventory WHERE source_id=$1',[source.config.sourceId])).rows[0].count);
  const existing=Number((await client.query('SELECT count(*) AS count FROM crawler.restored_migration_sources WHERE source_id=$1',[source.config.sourceId])).rows[0].count);
  assert.equal(existing,0,'Restoration already exists; do not repeat');
  for(let i=0;i<snapshots.length;i+=1000){
   await client.query(`INSERT INTO crawler.restored_migration_sources(source_id,channel_id,source_candidate_id,priority,restoration_id,snapshot_json)
    SELECT $1,s->>'channel_id',(s->>'source_candidate_id')::bigint,100,$2,s FROM jsonb_array_elements($3::jsonb) s`,[source.config.sourceId,restorationId,JSON.stringify(snapshots.slice(i,i+1000))]);
  }
  await retainRestoredMigrationInventory(client,{sourceId:source.config.sourceId,syncToken:sync.sync_token});
  const after=Number((await client.query('SELECT count(*) AS count FROM crawler.migration_channel_inventory WHERE source_id=$1',[source.config.sourceId])).rows[0].count);
  assert.equal(after-before,23507-previousInventory.length);
  await client.query('UPDATE crawler.migration_channel_inventory_syncs SET eligible_count=$2,updated_at=now() WHERE source_id=$1',[source.config.sourceId,after]);
  await client.query(apply?'COMMIT':'ROLLBACK');
  console.log(JSON.stringify({apply,restoration_id:restorationId,restored:snapshots.length,already_listed:previousInventory.length,newly_listed:after-before,before,after,source_unchanged:true,tasks_created:0}));
 }catch(e){await client.query('ROLLBACK');throw e;}finally{client.release();}
}finally{await closeMigrationSourcePool();await closeDb();}
