import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync,randomBytes} from 'node:crypto';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {setTimeout as delay} from 'node:timers/promises';
import {fullCrawlFixture,channelSnapshot,publicDetail} from './helpers/remoteFullCrawlFixture.js';
import {fullCrawlRelayFixture} from './helpers/fullCrawlRelayFixture.js';
import {createFullCrawlTransport} from '../src/remoteNodes/fullCrawlTransport.js';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
import {startRemoteNatsCenter} from '../src/remoteNodes/natsCenter.js';
import {connectNats} from '../src/remoteNodes/natsConnection.js';
import {createRemoteNatsClient} from '../src/remoteNodes/natsClient.js';
import {createRemoteFullCrawlWorker} from '../src/remoteNodes/fullCrawlWorker.js';
import {RemoteResultSpool} from '../src/remoteNodes/spool.js';
import {RemoteManagedFullCrawlRuntime} from '../src/remoteNodes/managedFullCrawlRuntime.js';
import {YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT,YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT} from '../src/fullCrawlFetchContract.js';
import {createVideoDetailApiFallback} from '../src/videoDetailApiFallback.js';
import {emptyUploadsDecision} from '../src/youtubeUploadsCountry.js';
import {pendingFullCrawlCountryRecheck} from '../src/remoteNodes/fullCrawlCountryReplay.js';
import {fullCrawlSlotUnsettled} from '../src/remoteNodes/fullCrawlCenterRecovery.js';
import {createFullCrawlYoutubeJsExecutor} from '../src/fullCrawlYoutubeJsFactory.js';
import {FullCrawlYoutubeJsStore} from '../src/fullCrawlYoutubeJsStore.js';
import {currentChannelExecution} from '../src/channelExecutionContext.js';

const remoteBusiness=[];
async function businessSnapshot(f){
  const candidate=(await f.query('SELECT status,snapshot_attempts FROM crawler.channel_candidates WHERE candidate_id=$1',[f.candidateId])).rows[0];
  const run=(await f.query('SELECT status,detail_status,result_json FROM crawler.channel_runs WHERE run_id=$1',[f.runId])).rows[0];
  const channel=(await f.query('SELECT status,country_code,subscriber_count FROM crawler.channels WHERE channel_id=$1',[f.channelId])).rows[0];
  const details=(await f.query('SELECT detail_status,attempts,disposition FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position',[f.runId])).rows;
  const contents=(await f.query('SELECT * FROM crawler.contents WHERE channel_id=$1 ORDER BY source_content_id',[f.channelId])).rows.map(row=>({id:row.source_content_id,type:row.content_type,title:row.title,views:row.view_count}));
  return {candidate,channel,details,contents,run:{status:run.status,detail_status:run.detail_status,
    selected:run.result_json.full_crawl.fetch.selected_count,stored:run.result_json.full_crawl.fetch.stored_count,excluded:run.result_json.full_crawl.fetch.excluded_count}};
}

for(const {url,scenario} of [{url:process.env.FULL_CRAWL_NATS_TEST_URL,scenario:'complete'}, {url:process.env.FULL_CRAWL_WSS_TEST_URL,scenario:'complete'},
  {url:process.env.FULL_CRAWL_NATS_TEST_URL,scenario:'country'}, {url:process.env.FULL_CRAWL_NATS_TEST_URL,scenario:'api'}].filter(value=>value.url)){
 test(`full ${scenario} over ${url.startsWith('wss:')?'WSS':'TLS NATS'} and real signed local relay`,{timeout:90000},async t=>{
  const nodeId='a88a2231-3604-4f55-a815-08662ae85fd6',token='full-p3-node-test-only-token-00000000000000';
  const pair=generateKeyPairSync('ed25519');let channelId;
  const uploads={playlist_id:'UUfixture',entries:[{video_id:'dQw4w9WgXcQ',position:1,title:'normal'}],activity_evidence_complete:true,
    scan:{complete:true,stop_reason:'limit',terminal_reason:'limit',pages:1,inspected_count:1,parse_gap_count:0}};
  const relay=await fullCrawlRelayFixture(t,{nodeId,publicKey:pair.publicKey,observe:request=>request.stage==='admission'?{...channelSnapshot(channelId),raw:{padding:'p'.repeat(600000)}}:request.stage==='uploads'?uploads:publicDetail(request.id)});
  const f=await fullCrawlFixture(t,{createAttempt:false,contract:scenario==='api'?YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT:YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,nodeIdentity:{nodeId,token,relayBootId:(await relay.localRota.boot()).boot_id}});channelId=f.channelId;
  f.connection.relay_boot_id=(await relay.localRota.boot()).boot_id;
  await f.full.activation.heartbeat(nodeId,f.connection);await f.full.activation.activate(f.connection);
  for(const file of ['youtubeSessionSchema.sql','natsSchema.sql','fullCrawlTransportSchema.sql'])await f.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  let center,client,signals,worker,work,runtime,handle;const directory=await mkdtemp(join(tmpdir(),'full-nats-'));
  const abort=new AbortController();
  const source={...f.proxy,attempt_number:1,lease_id:'fixture-lease',task_id:f.attemptArgs.task.task_id,business_run_id:f.runId,job_execution_id:'fixture-execution',
    credential_generation:1,egress_country:'BR',upstream:relay.upstream,route_lease_until_ms:Date.now()+120000};
  const transport=createFullCrawlTransport({executions:f.full,readRotaRoute:async()=>source,privateKey:pair.privateKey,secretKey:randomBytes(32),isReady:()=>Boolean(center),stopTimeoutMs:5000});
  const central=transport.forConnection(f.connection);
  try{
    const tls={caFile:process.env.FULL_CRAWL_NATS_TEST_CA};
    signals=await createTransportSignals({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL});
    const centerOptions={url,password:'full-p3-center-test-only-token-00000000000000',tls,store:f.store,signals,
      fullCrawls:transport.service,resultMaxBytes:32*1024*1024};
    center=await startRemoteNatsCenter(centerOptions);
    client=await createRemoteNatsClient({url,token,nodeId,slot:f.slot,tls});

    const observation=async input=>{const context=currentChannelExecution();return context.fingerprint_gateway.fetch(context.profile_group.clients.youtubejs_chrome,input);};
    const youtube={acquire:async()=>({enabled:true}),release:async()=>{},close:async()=>{},
      openChannel:()=>observation({stage:'admission'}),fetchUploads:async()=>{const value=await observation({stage:'uploads'});if(scenario==='country')emptyUploadsDecision('US');return value;},
      fetchDetail:async id=>{const value=await observation({stage:'detail',id});if(scenario==='api')throw Object.assign(Error('fixture missing required field'),{name:'YoutubeJsRequiredSurfaceError',required_surface:'player',partial_detail:value});return value;}};
    let ackLost=false;
    const workerClient={...client,uploadFullCrawl:async(...args)=>{const receipt=await client.uploadFullCrawl(...args);if(!ackLost){ackLost=true;throw Object.assign(Error('SIMULATED_SQL_ACK_LOSS'),{status:503});}return receipt;},claim:async(id,slot)=>{const lease=await client.claim(id,slot,f.connection);return lease?{...lease,connection:f.connection}:null;}};
    worker=createRemoteFullCrawlWorker({client:workerClient,localRota:relay.localRota,slot:f.slot,spool:new RemoteResultSpool({directory}),gateway:relay.gateway,youtube,pollMs:20,timeoutMs:30000});
    work=(async()=>{for(;;){const status=await worker.runOnce();if(status!=='idle')return status;await delay(20);}})();work.catch(()=>{});
    runtime=new RemoteManagedFullCrawlRuntime({executionStore:f.full,workerConnection:f.connection,profileSecret:f.profileSecret,
      createApiFallback:args=>createVideoDetailApiFallback({...args,loadSettings:async()=>({apiKeys:['fixture'],dailyRequestLimit:100,fallbackMode:'emergency'})}),
      transport:central,handoff:{candidateSettled:async()=>{},fetchCompleted:async()=>{}}});
    handle=await runtime.acquire({assignment:source,task:source,prepared:{businessRunId:f.runId,businessRunKey:f.execution.business_run_key},policy:f.policy,abortSignal:abort.signal});
    let result,caught;
    try{result=await handle.execute({job:f.job,attempt:{resumeMode:'initial'}},()=>runtime.executeFullCrawl());}catch(error){caught=error;}
    if(scenario==='complete'){if(caught)throw caught;assert.equal(result.ok,true);}
    else assert.equal(caught?.code,scenario==='api'?'VIDEO_API_PENDING':'UPLOADS_COUNTRY_RECHECK');
    const quiet=await runtime.quiesce(handle);assert.equal(quiet.active_managed_requests,0);
    assert.equal(await work,'closed');work=null;
    const request=handle.request;
    if(scenario!=='complete'){
      const task=(await f.query('SELECT state,applied_result FROM remote_ingestion.tasks WHERE task_id=$1',[request.task_id])).rows[0];
      assert.equal(task.state,'received');assert.equal(task.applied_result.code,caught.code);
      if(scenario==='api'){assert.equal(task.applied_result.request_id,JSON.stringify(['full',f.runId,'dQw4w9WgXcQ']));assert.equal(relay.calls.length,5);}
      else{
        assert.equal(task.applied_result.country,'US');assert.equal(relay.calls.length,2);
        const original=(await f.query('SELECT input FROM remote_ingestion.tasks WHERE task_id=$1',[request.task_id])).rows[0].input;
        const newOwner={input:original,task:{task_id:'00000000-0000-4000-8000-000000000002'}};
        assert.equal(await pendingFullCrawlCountryRecheck(f.pool,newOwner,{egressCountry:'BR',countryRecheck:null}),'US');
        assert.equal(await pendingFullCrawlCountryRecheck(f.pool,newOwner,{egressCountry:'US',countryRecheck:null}),null);
        assert.equal(await pendingFullCrawlCountryRecheck(f.pool,newOwner,{egressCountry:'BR',countryRecheck:{country:'US',status:'unavailable'}}),null);
      }
      assert.equal(await fullCrawlSlotUnsettled(f.pool,f.connection),false,'API/country wait releases the collection slot');
      return;
    }
    assert.equal(relay.calls.length,3);assert.ok(relay.calls.every(value=>value.includes(Buffer.from('p3-fixture:test-only').toString('base64'))));
    assert.deepEqual((await f.query('SELECT detail_status,attempts FROM crawler.content_candidates WHERE run_id=$1',[f.runId])).rows,[{detail_status:'done',attempts:1}]);
    const binding=(await f.query('SELECT state,release_receipt FROM remote_ingestion.network_bindings WHERE task_id=$1',[request.task_id])).rows[0];
    assert.equal(binding.state,'retired');assert.equal(binding.release_receipt.in_flight,0);
    remoteBusiness.push(await businessSnapshot(f));
    assert.equal(quiet.checkpoint.status,'success');
    const hostile=await connectNats({servers:url,user:nodeId,pass:token,tls,inboxPrefix:`_INBOX.${nodeId}.acl`});
    try{
      await assert.rejects(hostile.request(`qy.remote.results.${nodeId}`,Buffer.from('{}'),{timeout:500}),/permission|timeout/i);
      await assert.rejects(hostile.request('qy.remote.rpc.00000000-0000-4000-8000-000000000000.full_commands',Buffer.from('{}'),{timeout:500}),/permission|timeout/i);
    }finally{await hostile.close();}
    const partial=(await f.query(`SELECT b.*,s.input_hash,s.target_hash FROM remote_ingestion.full_crawl_result_batches b
      JOIN remote_ingestion.full_crawl_stages s USING(stage_id) WHERE s.task_id=$1 AND s.stage='admission'`,[request.task_id])).rows[0];
    assert.ok(partial.part_count>1);assert.equal(ackLost,true);
    await center.close();center=await startRemoteNatsCenter(centerOptions);
    const parts=(await f.query('SELECT * FROM remote_ingestion.full_crawl_result_parts WHERE batch_id=$1 ORDER BY part_number',[partial.batch_id])).rows;
    const manifest={version:1,task_id:request.task_id,generation:request.generation,stage_id:partial.stage_id,batch_id:partial.batch_id,sequence:partial.sequence,
      input_hash:partial.input_hash,target_hash:partial.target_hash,payload_hash:partial.payload_hash,payload_bytes:partial.payload_bytes,part_count:partial.part_count};
    for(const part of parts)assert.ok(['receiving','durable_received','applied'].includes((await client.uploadFullCrawl(request,{manifest,part_number:part.part_number,part_hash:part.part_hash,payload:part.payload})).state));
    await assert.rejects(client.uploadFullCrawl(request,{manifest,part_number:parts[0].part_number,part_hash:'0'.repeat(64),payload:parts[0].payload}),{code:'FULL_CRAWL_PART_HASH_CONFLICT'});
    assert.ok((await f.query('SELECT s.profile_applied_at FROM remote_ingestion.youtube_sessions s JOIN remote_ingestion.network_bindings b USING(binding_id) WHERE b.task_id=$1',[request.task_id])).rows[0].profile_applied_at);
    assert.equal((await f.query('SELECT status FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[handle.attemptId])).rows[0].status,'success');
  }catch(error){console.log('P3 central error',error.code??error.message,error.stack);throw error;}finally{
    abort.abort();worker?.stop();
    if(handle)await runtime.quiesce(handle).catch(()=>{});
    await work?.catch(()=>{});await client?.close();await signals?.close();await center?.close();await rm(directory,{recursive:true,force:true});
  }
 });
}

test('complete remote business output equals the original local full-crawl collector on identical observations',{
  skip:!process.env.FULL_CRAWL_NATS_TEST_URL,timeout:30000},async t=>{
  const f=await fullCrawlFixture(t);
  const youtube={fetchChannel:async()=>channelSnapshot(f.channelId),fetchUploads:async()=>({playlist_id:'UUfixture',entries:[{video_id:'dQw4w9WgXcQ',position:1,title:'normal'}],
    activity_evidence_complete:true,scan:{complete:true,stop_reason:'limit',terminal_reason:'limit',pages:1,inspected_count:1,parse_gap_count:0}}),fetchDetail:async id=>publicDetail(id)};
  const executor=createFullCrawlYoutubeJsExecutor({store:new FullCrawlYoutubeJsStore({query:f.query,withTransaction:action=>f.store.transaction(action)}),youtube,
    handoff:{candidateSettled:async()=>{},fetchCompleted:async()=>{}}});
  assert.equal((await executor(f.job)).ok,true);
  assert.ok(remoteBusiness.length>0);
  const local=await businessSnapshot(f);for(const remote of remoteBusiness)assert.deepEqual(remote,local);
  if(process.env.FULL_CRAWL_COMPARISON_REPORT)await writeFile(process.env.FULL_CRAWL_COMPARISON_REPORT,
    JSON.stringify({format:1,matched:true,fields:Object.keys(local),local,remote:remoteBusiness},null,2)+'\n');
});
