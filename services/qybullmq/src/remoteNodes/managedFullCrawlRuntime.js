import {setTimeout as delay} from 'node:timers/promises';
import {lockPublicationChannelMutation} from '../publicationChannelMutationLock.js';
import {BrowserProfileStore} from '../browserProfileStore.js';
import {ChannelExecutionMetrics} from '../channelExecutionContext.js';
import {failureDecisions} from '../channelExecutionRuntime.js';
import {runRemoteFullCrawl} from './fullCrawlCoordinator.js';
import {RemoteProtocolError} from './protocol.js';

const fail=code=>{throw new RemoteProtocolError(code);};

// The transport is explicit: P2 supplies an isolated node, P3 supplies the
// admitted remote route/session adapter. No default can advertise readiness.
export class RemoteManagedFullCrawlRuntime {
  constructor({executionStore,workerConnection,profileSecret,transport,handoff,
    createApiFallback=null,assertAdmission=null,claimTimeoutMs=30000,stageTimeoutMs=120000}){
    for(const method of ['ready','notifyTask','notifyStage','open','stop'])if(typeof transport?.[method]!=='function')throw new TypeError(`full-crawl transport.${method} required`);
    for(const name of ['candidateSettled','fetchCompleted'])if(typeof handoff?.[name]!=='function')throw new TypeError(`handoff.${name} required`);
    if(typeof profileSecret!=='string'||profileSecret.length<12)throw new TypeError('central profile secret required');
    Object.assign(this,{executions:executionStore,store:executionStore.store,connection:workerConnection,profileSecret,
      transport,handoff,createApiFallback,assertAdmission,claimTimeoutMs,stageTimeoutMs});
    this.nodeId=workerConnection.node_id;this.slot=workerConnection.slot;this.active=null;
  }
  readyForTasks(){return this.transport.ready(this.connection)===true;}
  profiles(client){return new BrowserProfileStore({queryFn:client.query.bind(client),transactionFn:action=>action(client),secret:this.profileSecret});}
  async prepare(handle,job){
    const {assignment,task,prepared,policy}=handle.args;
    if(!Number.isSafeInteger(job.attemptsStarted)||job.attemptsStarted<1)throw new TypeError('original BullMQ attempt required');
    if(assignment.identity_policy_id!==policy.id||Number(assignment.identity_policy_version)!==Number(policy.version)
      ||assignment.identity_policy_hash!==policy.hash||task.business_run_id!==prepared.businessRunId)fail('REMOTE_ROTA_IDENTITY_MISMATCH');
    const execution=await this.store.transaction(async client=>{
      await this.executions.lockConnection(client,this.nodeId,this.connection,{admission:true});
      if(this.assertAdmission&&await this.assertAdmission(client)!==true)fail('REMOTE_SUPERVISOR_NOT_READY');
      const binding=(await client.query('SELECT * FROM crawler.business_run_bindings WHERE business_run_key=$1 FOR UPDATE',[prepared.businessRunKey])).rows[0];
      if(!binding||binding.business_run_id!==prepared.businessRunId||binding.channel_id!==job.data.channel_id
        ||Number(binding.candidate_id)!==Number(job.data.candidate_id))fail('FULL_CRAWL_BUSINESS_FENCE_STALE');
      const profiles=this.profiles(client);
      handle.profileGroup=await profiles.loadOrCreate({identityPolicyId:policy.id,identityPolicyVersion:policy.version,
        networkIdentityKey:assignment.network_identity_key,profileEpoch:assignment.profile_epoch,
        language:policy.youtube_language,country:policy.youtube_country,timezone:policy.browser_profile_timezone});
      const attemptId=await profiles.beginAttempt({channelId:job.data.channel_id,runId:prepared.businessRunId,
        queueName:job.queueName,jobId:job.id,jobAttempt:job.attemptsStarted-1,dispatchGeneration:job.data.dispatch_generation,
        workerId:assignment.worker_id,proxy:assignment,profileGroup:handle.profileGroup,task,prepared});
      return {version:1,queue_name:job.queueName,job_name:job.name,job_id:String(job.id),job_attempt:job.attemptsStarted,
        candidate_id:Number(job.data.candidate_id),channel_id:job.data.channel_id,run_id:prepared.businessRunId,
        business_run_id:prepared.businessRunId,business_run_key:prepared.businessRunKey,intent_hash:binding.intent_hash,
        dispatch_generation:Number(job.data.dispatch_generation),execution_attempt_id:attemptId,fetch_contract:job.data.fetch_contract};
    });
    handle.attemptId=execution.execution_attempt_id;
    handle.admission=await this.executions.prepare({execution,connection:this.connection});
    handle.request={task_id:handle.admission.taskId,generation:handle.admission.generation,connection:this.connection};
    await this.transport.notifyTask({connection:this.connection,execution,request:handle.request});
    const signal=AbortSignal.any([handle.args.abortSignal,AbortSignal.timeout(this.claimTimeoutMs)]);
    for(;;){
      signal.throwIfAborted();
      const task=(await this.store.pool.query('SELECT *,lease_until>clock_timestamp() AS alive FROM remote_ingestion.tasks WHERE task_id=$1',[handle.request.task_id])).rows[0];
      if(task?.state==='leased'&&task.alive){await this.executions.withLease(this.nodeId,handle.request,async()=>{});break;}
      if(task?.state!=='pending')fail('REMOTE_EXECUTION_NOT_AVAILABLE');
      await delay(50,null,{signal});
    }
    // This adapter must bind the original Rota route and profile, and return a
    // handle whose stop produces zero-in-flight evidence for that exact owner.
    handle.opening=true;
    handle.network=await this.transport.open({...handle.args,request:handle.request,profileGroup:handle.profileGroup,attemptId:handle.attemptId});
  }
  async acquire(args){
    const handle={args,error:null,quiescence:null,finished:false};
    handle.execute=async({job,attempt},invoke)=>{
      if(this.active)fail('REMOTE_CENTRAL_RUNTIME_BUSY');
      this.active=handle;handle.job=job;handle.resumeMode=attempt.resumeMode;
      try{args.abortSignal.throwIfAborted();await this.prepare(handle,job);return await invoke();}
      catch(error){handle.error??=error;throw error;}
      finally{this.active=null;}
    };
    return handle;
  }
  async executeFullCrawl({resumeMode}={}){
    const handle=this.active;if(!handle?.request)fail('REMOTE_CENTRAL_EXECUTION_REQUIRED');
    try{
      handle.result=await runRemoteFullCrawl({executionStore:this.executions,nodeId:this.nodeId,request:handle.request,
        job:handle.job,handoff:this.handoff,signal:handle.args.abortSignal,resumeMode:resumeMode??handle.resumeMode,
        egressCountry:handle.args.assignment.egress_country??null,countryRecheck:handle.job.data.uploads_country_recheck??null,
        stageTimeoutMs:this.stageTimeoutMs,onCommand:(command,options)=>this.transport.notifyStage({request:handle.request,command,signal:options.signal}),
        videoApiFallback:this.createApiFallback?.({query:this.store.pool.query.bind(this.store.pool),withTransaction:action=>this.store.transaction(action)})??null});
      return handle.result;
    }catch(error){
      handle.error=error;await this.quiesce(handle);
      error.channel_execution_attempt={attempt_id:handle.attemptId,youtube_requests:handle.metrics,failure_decisions:handle.decisions};
      throw error;
    }
  }
  quiesce(handle){
    if(!handle)return Promise.resolve({active_managed_requests:0});
    if(handle.quiescence)return handle.quiescence;
    handle.quiescence=(async()=>{
      // Even an open whose acknowledgement was lost must be stopped by its
      // request identity. Never infer quiescence from a task lease timeout.
      const quiet=handle.opening?await this.transport.stop({network:handle.network,request:handle.request,
        attemptId:handle.attemptId,error:handle.error}):{active_managed_requests:0};
      if(quiet?.active_managed_requests!==0)fail('FULL_CRAWL_NETWORK_NOT_QUIESCED');
      handle.metrics=quiet.youtube_requests??new ChannelExecutionMetrics().snapshot();
      const handoff=['UPLOADS_COUNTRY_RECHECK','VIDEO_API_PENDING'].includes(handle.error?.code);
      handle.decisions=failureDecisions(handle.metrics,handoff?null:handle.error);
      if(handle.attemptId)await this.store.transaction(async client=>{
        if(handle.request){
          await client.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[this.nodeId]);
          await client.query('SELECT node_id FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[this.nodeId,this.slot]);
          const task=(await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[handle.request.task_id])).rows[0];
          if(task?.context.execution_attempt_id!==handle.attemptId||task.generation!==handle.request.generation&&task.state!=='pending')fail('REMOTE_EXECUTION_REPLACED');
          const routes=await client.query("SELECT 1 FROM remote_ingestion.network_bindings WHERE task_id=$1 AND state<>'retired'",[task.task_id]);
          if(routes.rowCount)fail('FULL_CRAWL_NETWORK_NOT_QUIESCED');
          await lockPublicationChannelMutation(client,task.input.channel_id);
          await client.query('SELECT candidate_id FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE',[task.input.candidate_id]);
          await client.query('SELECT business_run_key FROM crawler.business_run_bindings WHERE business_run_key=$1 FOR UPDATE',[task.input.business_run_key]);
        }
        const attempt=(await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE',[handle.attemptId])).rows[0];
        if(!attempt||attempt.finished_at){if(attempt?.finished_at)return;fail('REMOTE_ATTEMPT_MISSING');}
        const newer=await client.query('SELECT 1 FROM crawler.channel_execution_attempts WHERE business_run_id=$1 AND workload_scope=$2 AND attempt_number>$3',[attempt.business_run_id,attempt.workload_scope,attempt.attempt_number]);
        if(newer.rowCount)fail('REMOTE_EXECUTION_REPLACED');
        if(!handle.error&&handle.request)await this.transport.applyCheckpoint?.(client,{request:handle.request,attemptId:handle.attemptId,profileSecret:this.profileSecret});
        await this.profiles(client).finishAttempt(handle.attemptId,{status:handle.args.abortSignal.aborted?'aborted':handle.error&&!handoff?'failed':'success',
          error:handoff?null:handle.error,result:{youtube_requests:handle.metrics,failure_decisions:handle.decisions}});
        if(handle.request)await client.query(`UPDATE remote_ingestion.tasks SET state=$2,last_error=$3,applied_result=$4,
          applied_at=CASE WHEN $2='applied' THEN clock_timestamp() ELSE NULL END,coordinator_until=NULL,lease_until=NULL
          WHERE task_id=$1`,[handle.request.task_id,handoff?'received':handle.error?'failed':'applied',handle.error?.code??null,handle.result??(handoff?{version:1,run_id:attempt.business_run_id,code:handle.error.code,request_id:handle.error.requestId??null,country:handle.error.country??null}:null)]);
      });
      handle.finished=true;return quiet;
    })().catch(error=>{handle.quiescence=null;throw error;});
    return handle.quiescence;
  }
  async persistRetryableCheckpoint({error}){
    const attemptId=error?.channel_execution_attempt?.attempt_id;
    if(!attemptId)return false;
    const rows=await this.store.pool.query(`SELECT 1 FROM remote_ingestion.tasks t JOIN crawler.channel_execution_attempts a
      ON a.attempt_id=t.context->>'execution_attempt_id' WHERE a.attempt_id=$1 AND a.finished_at IS NOT NULL AND t.state='failed'
      AND NOT EXISTS(SELECT 1 FROM remote_ingestion.network_bindings b WHERE b.task_id=t.task_id
        AND (b.state<>'retired' OR (b.release_receipt->>'in_flight')::int IS DISTINCT FROM 0))`,[attemptId]);
    return rows.rowCount===1;
  }
  checkpoint(handle){return this.quiesce(handle);}
  retire(handle){return this.quiesce(handle);}
}
