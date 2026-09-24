import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import pg from 'pg';
import { PostgresBusinessPublicationStore } from '../src/businessPublicationIngress.js';
import { PostgresBusinessPublicationActivator } from '../src/businessPublicationActivator.js';
import { projectBusinessPublicationChannels } from '../src/businessPublicationProjector.js';
import { observationFactsHash } from '../src/crawlObservationStore.js';
import { publicationResultHash } from '../src/publicationResultHash.js';
import { buildPublicationShard, publicationEnvelopeFromRow } from '../src/publicationTransport.js';
import { publicationPayloadFixture } from './support/publicationPayloadFixtures.js';
import { completeInput } from './support/businessProjectionFixture.js';

const url=process.env.BUSINESS_LATEST_POSTGRES_TEST_URL;
test('real ingress, activation and projector publish latest data and replay safely', {skip:!url,timeout:120000}, async()=>{
  const parsed=new URL(url);assert.match(parsed.pathname,/_test$/);assert.equal(parsed.hostname,'127.0.0.1');
  const client=new pg.Client({connectionString:url});await client.connect();
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL statement_timeout='30s'");
    assert.equal(Number((await client.query('SELECT count(*) FROM public.channels')).rows[0].count),0);
    for(const file of ['businessPublicationProjectionSchema.sql','businessCreatorSearchIncrementalSchema.sql','businessLatestStorageSchema.sql'])
      await client.query(await readFile(new URL(`../src/${file}`,import.meta.url),'utf8'));
    await client.query("UPDATE publication.creator_search_storage_state SET write_mode='incremental',read_mode='live'");
    await client.query("UPDATE publication.business_storage_state SET mode='latest'");
    // All real SQL and constraints run on PostgreSQL. SAVEPOINTs isolate the
    // production classes' transactions inside this test's rollback-only fixture.
    const pool={query:client.query.bind(client),connect:async()=>({release(){},query:(sql,args)=>client.query(
      sql==='BEGIN'?'SAVEPOINT service_transaction':sql==='COMMIT'?'RELEASE SAVEPOINT service_transaction':
        sql==='ROLLBACK'?'ROLLBACK TO SAVEPOINT service_transaction':sql,args)})};
    const store=new PostgresBusinessPublicationStore(pool),activator=new PostgresBusinessPublicationActivator(pool);
    const input=completeInput(),channelId=input.channelId,streamId=randomUUID();
    await client.query(`INSERT INTO publication.stream(publication_stream_id,source_deployment_key,source_identity_json,
      registered_by,registered_reason,status_changed_by,status_reason) VALUES($1,'latest-e2e','{}','test','test','test','test')`,[streamId]);
    await client.query(`INSERT INTO publication.channel_ownership(channel_id,active_publication_stream_id,projection_mode,
      ownership_reference,state_changed_by,state_reason) VALUES($1,$2,'online','{"onboarding_mode":"automatic_bootstrap"}','test','test')`,[channelId,streamId]);
    const hashes={};
    const envelope=(domain,sequence,payload,resultHash=publicationResultHash(domain,payload))=>{
      const row=publicationEnvelopeFromRow({revision_id:randomUUID(),publication_stream_id:streamId,
        revision_type:sequence===1?'bootstrap':'incremental',channel_id:channelId,domain,data_sequence:sequence,
        previous_data_sequence:sequence===1?null:sequence-1,operation:domain==='video'?(sequence===1?'replace_window':'apply_window_delta'):'replace',
        contract_version:1,policy_version:domain==='video'?'video-window-v1':'publication-policy-v1',
        occurred_at:`2026-09-${String(sequence).padStart(2,'0')}T12:00:00Z`,
        source_refs:{complete_observation:{observed_at:`2026-09-${String(sequence).padStart(2,'0')}T12:00:00Z`}},
        previous_result_hash:hashes[domain]??null,result_hash:resultHash,payload_hash:observationFactsHash(payload),payload_json:payload});
      hashes[domain]=resultHash;return row;
    };
    const channel=input.current.channel.payload_json;
    const agent={...publicationPayloadFixture('agent',channelId),...input.current.agent.payload_json};
    const video=publicationPayloadFixture('video',channelId);
    const itemHash=value=>observationFactsHash(Object.fromEntries(Object.entries(value).filter(([key])=>
      !['position','item_hash','extractor_version'].includes(key)&&!key.endsWith('_observed_at')&&!key.endsWith('_source'))));
    const item={...input.current.contents[0].payload_json,like_count_status:'exact',position:1,item_hash:input.current.contents[0].item_hash};
    item.item_hash=itemHash(item);
    const untouched={...item,content_id:'video-fixture-2',content_key:`${channelId}:video:video-fixture-2`,
      position:2,url:'https://www.youtube.com/watch?v=video-fixture-2'};
    untouched.item_hash=itemHash(untouched);
    video.items=[item,untouched];video.window_proof={...video.window_proof,catalog_candidate_count:2,qualified_count:2,selected_count:2,latest_scan_items:2};
    video.result_hash=publicationResultHash('video',video);
    const acceptAndActivate=async(revisions)=>{
      const receipt=await store.acceptShard(buildPublicationShard(revisions));
      assert.ok(receipt.receipts.every(row=>['accepted','duplicate'].includes(row.status)),JSON.stringify(receipt));
      const activated=await activator.activateReady(channelId);
      assert.equal(activated.status,'activated',JSON.stringify(activated));
    };
    const deliver=async(revisions)=>{
      await acceptAndActivate(revisions);
      return projectBusinessPublicationChannels(client,[channelId],{storageMode:'latest'});
    };
    const first=await deliver([envelope('channel',1,channel),envelope('video',1,video),envelope('agent',1,agent)]);
    assert.equal(first.outcome,'published');
    const snapshotId=first.projections[0].snapshot_id;
    const about2=envelope('channel',2,{...channel,title:'Updated About',subscriber_count:1234});
    const second=await deliver([about2]);
    assert.equal(second.projections[0].snapshot_id,snapshotId);
    assert.equal((await client.query('SELECT count(*)::int AS n FROM public.content_snapshots')).rows[0].n,2);
    assert.equal((await client.query('SELECT count(*)::int AS n FROM public.channel_profile_facts')).rows[0].n,10);
    assert.equal((await client.query('SELECT subscribers FROM public.creator_search_live')).rows[0].subscribers,'1234');
    const fingerprint=()=>client.query(`SELECT md5(string_agg(to_jsonb(s)::text,'' ORDER BY id)) AS hash FROM public.content_snapshots s`);
    const before=(await fingerprint()).rows[0].hash;
    await store.acceptShard(buildPublicationShard([about2]));
    assert.equal((await activator.activateReady(channelId)).status,'idle');
    assert.equal((await projectBusinessPublicationChannels(client,[channelId],{storageMode:'latest'})).outcome,'no_work');
    assert.equal((await fingerprint()).rows[0].hash,before);
    const changed={...item,view_count:200,item_hash:itemHash({...item,view_count:200})};
    const targetHash=publicationResultHash('video',{...video,items:[changed,untouched]});
    await deliver([envelope('video',2,{channel_id:channelId,window_policy:video.window_policy,
      window_proof:video.window_proof,upserts:[changed],window_exits:[],retractions:[],result_hash:targetHash},targetHash)]);
    assert.deepEqual((await client.query('SELECT view_count FROM public.content_snapshots ORDER BY video_id')).rows.map(row=>row.view_count),['200','100']);
    assert.equal((await client.query('SELECT count(*)::int AS n FROM public.channel_snapshots')).rows[0].n,1);
    assert.equal((await client.query('SELECT count(*)::int AS n FROM public.channel_metric_values')).rows[0].n,77);
    // A backlog publishes the newest accepted data once, acknowledging all
    // covered pending versions rather than rebuilding every historical version.
    await acceptAndActivate([envelope('channel',3,{...channel,title:'Queued About 3'})]);
    await acceptAndActivate([envelope('channel',4,{...channel,title:'Queued About 4'})]);
    const coalesced=await projectBusinessPublicationChannels(client,[channelId],{storageMode:'latest'});
    assert.equal(coalesced.projected,1);assert.equal(coalesced.delivered,2);
    assert.equal((await client.query('SELECT name FROM public.creator_search_live')).rows[0].name,'Queued About 4');
    assert.equal((await client.query("SELECT count(*)::int AS n FROM publication.projection_outbox WHERE status<>'delivered'")).rows[0].n,0);
    assert.equal((await client.query('SELECT count(*)::int AS n FROM public.channel_snapshots')).rows[0].n,1);
  } finally {await client.query('ROLLBACK').catch(()=>{});await client.end();}
});
