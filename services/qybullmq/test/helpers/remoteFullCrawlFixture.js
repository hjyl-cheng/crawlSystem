import {randomBytes,randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {BusinessRunBindingStore} from '../../src/businessRunBindingStore.js';
import {BrowserProfileStore} from '../../src/browserProfileStore.js';
import {YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT} from '../../src/fullCrawlFetchContract.js';
import {RemoteNodeStore} from '../../src/remoteNodes/store.js';
import {RemoteFullCrawlExecutionStore} from '../../src/remoteNodes/fullCrawlExecutionStore.js';
import {FULL_CRAWL_WORKLOAD} from '../../src/remoteNodes/collectingWorkload.js';
import {assertIsolatedRemoteDatabase} from '../../src/remoteNodes/isolation.js';

export async function fullCrawlFixture(t,{createAttempt=true,activate=true,verifyExecution=async()=>true,contract=YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,identityPolicy=null,nodeIdentity=null}={}){
  const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;
  const pool=new pg.Pool({connectionString:url,max:6});
  const guard=await pool.connect();
  await assertIsolatedRemoteDatabase(pool);await guard.query('SELECT pg_advisory_lock(781137981)');
  await pool.query(await readFile(new URL('../../src/schema.sql',import.meta.url),'utf8'));
  for(const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql','fullCrawlSchema.sql','fullCrawlBusinessSchema.sql']){
    await pool.query(await readFile(new URL(`../../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  }
  const store=new RemoteNodeStore({pool});const query=pool.query.bind(pool);
  const full=new RemoteFullCrawlExecutionStore({store,verifyExecution});
  const suffix=randomUUID(),nodeId=nodeIdentity?.nodeId??randomUUID(),channelId=`UC${suffix.replaceAll('-','').slice(0,22)}`;
  const runId=`full-fence:${suffix}`,batchId=`fence-batch:${suffix}`;
  const jobId=`snapshot-${suffix}`,workerId=`rota-full-${suffix}`,slot='full-crawl-1';
  const policy=identityPolicy??{youtube_language:'en',youtube_country:'BR',browser_profile_timezone:'America/Sao_Paulo',id:'full-fence-policy',version:1,hash:'sha256:fixture-policy'};
  let profileGroup,taskId,candidateId;
  t.after(async()=>{
    try {
    const taskIds=(await query('SELECT task_id FROM remote_ingestion.tasks WHERE target_node_id=$1',[nodeId])).rows.map(row=>row.task_id);
    for(const ownTask of taskIds){
      if((await query("SELECT to_regclass('remote_ingestion.transport_receipts') AS name")).rows[0].name)await query('DELETE FROM remote_ingestion.transport_receipts WHERE task_id=$1',[ownTask]);
      if((await query("SELECT to_regclass('remote_ingestion.youtube_sessions') AS name")).rows[0].name)await query('DELETE FROM remote_ingestion.youtube_sessions WHERE binding_id IN (SELECT binding_id FROM remote_ingestion.network_bindings WHERE task_id=$1)',[ownTask]);
      await query('UPDATE remote_ingestion.network_slots SET binding_id=NULL WHERE binding_id IN (SELECT binding_id FROM remote_ingestion.network_bindings WHERE task_id=$1)',[ownTask]);
      await query('DELETE FROM remote_ingestion.network_bindings WHERE task_id=$1',[ownTask]);
      await query(`DELETE FROM remote_ingestion.full_crawl_result_parts WHERE batch_id IN
        (SELECT b.batch_id FROM remote_ingestion.full_crawl_result_batches b JOIN remote_ingestion.full_crawl_stages s USING(stage_id) WHERE s.task_id=$1)`,[ownTask]);
      await query(`DELETE FROM remote_ingestion.full_crawl_result_batches WHERE stage_id IN (SELECT stage_id FROM remote_ingestion.full_crawl_stages WHERE task_id=$1)`,[ownTask]);
      await query(`DELETE FROM remote_ingestion.full_crawl_detail_reservations WHERE stage_id IN (SELECT stage_id FROM remote_ingestion.full_crawl_stages WHERE task_id=$1)`,[ownTask]);
      for(const table of ['full_crawl_stages','full_crawl_executions','claims'])await query(`DELETE FROM remote_ingestion.${table} WHERE task_id=$1`,[ownTask]);
      await query('DELETE FROM remote_ingestion.tasks WHERE task_id=$1',[ownTask]);
    }
    for(const table of ['worker_connections','network_slots','nodes'])await query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);
    await query('DELETE FROM crawler.channel_execution_attempts WHERE business_run_id=$1',[runId]);
    // Full business fixtures remain in the isolated DB as in the existing full
    // store integration suite; every identity is unique across repeated runs.
    } finally {guard.release();await pool.end();}
  });
  await store.registerNode({nodeId,token:nodeIdentity?.token??randomBytes(32).toString('hex'),capabilities:[FULL_CRAWL_WORKLOAD.capability]});
  await query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,slot,workerId]);
  const registration={nodeId,slot,deploymentId:randomUUID(),configHash:'a'.repeat(64),role:'fullcrawl',mode:FULL_CRAWL_WORKLOAD.mode};
  await full.activation.register(registration);
  const connection={version:1,mode:registration.mode,node_id:nodeId,slot,deployment_id:registration.deploymentId,
    config_hash:registration.configHash,instance_id:randomUUID(),relay_boot_id:nodeIdentity?.relayBootId??'b'.repeat(48),
    runtime_revision:FULL_CRAWL_WORKLOAD.revisions[0],accepting:true};
  await full.activation.heartbeat(nodeId,connection);
  if(activate)await full.activation.activate(connection);
  const transaction=action=>store.transaction(action);
  await query("INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,pipeline_cycle_id,status) VALUES($1,$1,'validation_closed')",[batchId]);
  candidateId=Number((await query(`INSERT INTO crawler.channel_candidates
    (dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt)
    VALUES($1,$1,$2,$3,'queued',1,$4,1) RETURNING candidate_id`,[batchId,channelId,`https://www.youtube.com/channel/${channelId}`,jobId])).rows[0].candidate_id);
  const businessRunKey=`full-candidate:${candidateId}`;
  const {binding}=await new BusinessRunBindingStore({withTransaction:transaction}).resolve({businessRunKey,explicitBusinessRunId:runId,
    requestedStatus:'reserved',runKind:'full',channelId,candidateId,policy,
    intent:{job_name:'channel-snapshot',crawl_mode:'full',repair_batch_id:null,repair_parent_run_id:null,repair_round:0,repair_version:null,publication_gap_domains:null,publication_gap_root_run_id:null,publication_gap_scope:null,fetch_contract:contract}});
  const profileSecret=randomBytes(32).toString('hex');
  const profiles=new BrowserProfileStore({queryFn:query,transactionFn:transaction,secret:profileSecret});
  const proxy={workload_scope:'full-fence-test',worker_id:workerId,worker_instance_id:`center-${suffix}`,slot_name:`rota-slot-${suffix}`,
    route_generation:1,network_identity_key:`network-${suffix}`,identity_policy_id:policy.id,identity_policy_version:policy.version,identity_policy_hash:policy.hash,profile_epoch:0};
  profileGroup=await profiles.loadOrCreate({identityPolicyId:policy.id,identityPolicyVersion:policy.version,networkIdentityKey:proxy.network_identity_key,
    profileEpoch:0,language:'pt',country:'BR',timezone:'America/Sao_Paulo'});
  const attemptArgs={channelId,runId,queueName:FULL_CRAWL_WORKLOAD.queue,jobId,jobAttempt:0,dispatchGeneration:1,
    workerId,proxy,profileGroup,prepared:{businessRunId:runId},task:{task_id:randomUUID(),business_run_id:runId,attempt_number:1}};
  const attemptId=createAttempt?await profiles.beginAttempt(attemptArgs):null;
  const execution={version:1,queue_name:FULL_CRAWL_WORKLOAD.queue,job_name:'channel-snapshot',job_id:jobId,job_attempt:1,
    candidate_id:candidateId,channel_id:channelId,run_id:runId,business_run_id:runId,business_run_key:businessRunKey,
    intent_hash:binding.intent_hash,dispatch_generation:1,execution_attempt_id:attemptId,fetch_contract:contract};

  const job={id:jobId,name:'channel-snapshot',queueName:FULL_CRAWL_WORKLOAD.queue,attemptsStarted:1,attemptsMade:0,opts:{attempts:2},
    data:{candidate_id:candidateId,channel_id:channelId,run_id:runId,business_run_key:businessRunKey,dispatch_batch_id:batchId,dispatch_generation:1,fetch_contract:contract},
    async updateData(data){this.data=data;},async updateProgress(){},discard(){}};
  return {profileSecret,profileGroup,pool,query,store,full,nodeId,channelId,runId,jobId,workerId,slot,policy,connection,binding,proxy,attemptArgs,execution,job,candidateId,
    async claim(){const prepared=await full.prepare({execution,connection});taskId=prepared.taskId;
      const lease=await full.activation.claim(nodeId,{claim_id:randomUUID(),slot,connection});
      return {task_id:taskId,generation:lease.generation,connection};}};
}

export function channelSnapshot(channelId){return {about_requested:true,about_observed:true,
  metadata:{channel_id:channelId,channel_url:`https://www.youtube.com/channel/${channelId}`,title:'P2 remote full fixture',country:'Brazil',
    subscriber_count:10000,subscriber_count_text:'10,000',subscriber_count_source:'youtube_about'},
  raw:{engine:'youtubejs',request_counts:{get_channel:1,get_about:1}}};}
export function publicDetail(videoId){return {id:videoId,title:`P2 Detail ${videoId}`,published_at:new Date().toISOString(),view_count_text:'100',
  duration_seconds:60,access_status:'public',access_status_source:'youtubejs_player',playability_kind:'content',comments_disabled:true,
  content_type_signals:{source:'youtubei_player',canonical_url:`https://www.youtube.com/watch?v=${videoId}`,is_shorts_eligible:false,
    is_live_content:false,is_live:false,is_upcoming:false,is_live_now:false}};}
