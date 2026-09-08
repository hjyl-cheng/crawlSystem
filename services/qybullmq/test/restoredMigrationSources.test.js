import test from 'node:test';
import assert from 'node:assert/strict';
import {loadMigrationSourceChannel,loadMigrationSourceBatch,sourceSnapshotHash} from '../src/migrationSource.js';
import {retainRestoredMigrationInventory} from '../src/restoredMigrationSources.js';
const environment={MIGRATION_RESTORED_SOURCES_ENABLED:'true',MIGRATION_DATABASE_URL:'postgresql://unused',MIGRATION_SOURCE_ID:'test',EXPECTED_MIGRATION_DATABASE:'legacy',EXPECTED_MIGRATION_DATABASE_OID:'42',EXPECTED_MIGRATION_DATABASE_USER:'reader',EXPECTED_CRAWLER_DATABASE:'target'};
const base={source_id:'test',source_database:'legacy',source_database_oid:'42',source_candidate_id:'7',channel_id:'UC-restored',source_candidate_status:'discovered',source_json:{restoration:{previous_status:'accepted',previous_source:'youtube_search_discovery'}}};
const snapshot={...base,snapshot_sha256:sourceSnapshotHash(base)};
test('restored accepted/search channel can be selected without changing legacy source',async()=>{
 const selected=await loadMigrationSourceChannel({channelId:base.channel_id,candidateId:'7',environment,restoredQuery:async(sql,args)=>{assert.deepEqual(args.slice(0,3),['test','UC-restored','7']);return {rows:[{snapshot_json:snapshot}]};},pool:{connect(){throw Error('must not access legacy');}}});
 assert.deepEqual(selected,snapshot);
});
test('batch restored path retains exclusions and bounded limit',async()=>{
 const result=await loadMigrationSourceBatch({limit:1,excludeChannelIds:['already-done'],excludeSourceCandidateIds:['9'],environment,restoredQuery:async(sql,args)=>{assert.deepEqual(args.slice(3),[['9'],['already-done'],1]);return {rows:[{snapshot_json:snapshot}]};}});
 assert.deepEqual(result,[snapshot]);
});
test('corrupt or cross-source restored evidence is rejected',async()=>{
 await assert.rejects(loadMigrationSourceChannel({channelId:base.channel_id,environment,restoredQuery:async()=>({rows:[{snapshot_json:{...snapshot,source_database:'wrong'}}]})}),/identity or hash mismatch/);
});
test('inventory resync retains restored IDs under the current sync token',async()=>{
 const calls=[];await retainRestoredMigrationInventory({query:async(sql,args)=>{calls.push({sql,args});return {rows:[]};}},{sourceId:'test',syncToken:'token'});
 assert.match(calls[0].sql,/FROM crawler.restored_migration_sources/);
 assert.match(calls[0].sql,/sync_token=EXCLUDED.sync_token/);
 assert.deepEqual(calls[0].args,['test','token']);
});
