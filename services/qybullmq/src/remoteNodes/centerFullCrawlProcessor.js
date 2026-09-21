import {restoreFullCrawlHandoff,runFullCrawlApiReplay} from './fullCrawlApiReplay.js';
import {DelayedError} from 'bullmq';
import {MIGRATION_START_JOB,startControlledMigrationChannel,prepareControlledMigrationSnapshot,migrationBatchControlEnabled} from '../migrationBatchControl.js';
import {assertFullCrawlSnapshotRecoveryOwner} from '../finalRepairJobRecovery.js';
import {enterMigrationRetryIntentWorkerJob,finishMigrationRetryIntent} from '../migrationRetryIntent.js';
import {isYoutubeJsFullCrawlFetchContract,readFullCrawlFetchContractFromIntent,newFullCrawlFetchContractForJob} from '../fullCrawlFetchContract.js';
import {ProxyBusinessRunPreparer} from '../proxyBusinessRun.js';
import {processManagedWorkerJob,markChannelCandidateJobAttemptActive,isStaleExecutionFailure} from '../managedWorkerJob.js';
import {executeManagedWorkerAttempt} from '../managedWorkerExecution.js';
import {runChannelCandidateWorkerJobWithDurableSettlement,failChannelCandidateWorkerJob,describeChannelCandidateWorkerFailure} from '../channelCandidateWorkerLifecycle.js';
import {gateVideoApiJob,runVideoApiResumable,isVideoApiHandoff} from '../videoApiContinuation.js';
import {runVideoExecutionResumable} from '../videoExecutionDeferral.js';
import {deferJobForSlotPause} from '../channelJobDeferral.js';
import {terminateExhaustedBusinessRun} from '../businessRunBudgetRecovery.js';
import {applyFailureRetryDecision} from '../queues.js';
import {FULL_CRAWL_WORKLOAD} from './collectingWorkload.js';

export function fullCrawlJobRoute(job){
  if(job.queueName!==FULL_CRAWL_WORKLOAD.queue)throw new TypeError('full-crawl processor requires original channel crawl queue');
  if(job.name===MIGRATION_START_JOB)return 'control';
  return job.name==='channel-snapshot'&&Number(job.data?.candidate_id)>0
    &&!job.data?.publication_gap_scope&&isYoutubeJsFullCrawlFetchContract(job.data?.fetch_contract)?'remote':'compatibility';
}

// Compatibility is a concrete, bounded local processor supplied at assembly.
// It owns the entire legacy/repair job (including its local Rota), so it never
// creates a second consumer or sends a repair through a remote snapshot fence.
export function createCenterFullCrawlProcessor({store,runtime,rota,resolvedPolicy,ready,compatibility,handoff,
  createApiFallback=null,report=()=>{},apiDelayMs=15000,controlEnabled=migrationBatchControlEnabled}){
  for(const name of ['execute','replay'])if(typeof compatibility?.[name]!=='function')throw new TypeError(`full-crawl compatibility.${name} required`);
  if(typeof ready!=='function')throw new TypeError('supervisor readiness required');
  const query=store.pool.query.bind(store.pool),withTransaction=action=>store.transaction(action);
  const preparer=new ProxyBusinessRunPreparer({queryFn:query,withTransaction,resolvedPolicy});
  const event=(job,status,result=null,error=null)=>query(`INSERT INTO crawler.task_events
    (queue_name,job_id,job_name,entity_key,status,payload_json,error_message) VALUES($1,$2,$3,$4,$5,$6,$7)`,
    [job.queueName,job.id,job.name,job.data.channel_id,status,result,error]);
  return async(job,token)=>{
    let routeJob=job;
    if(job.queueName===FULL_CRAWL_WORKLOAD.queue&&job.name==='channel-snapshot'&&Number(job.data?.candidate_id)>0
      &&!Object.hasOwn(job.data,'fetch_contract')){
      const binding=(await query('SELECT intent_json FROM crawler.business_run_bindings WHERE business_run_key=$1',
        [job.data.business_run_key??`full-candidate:${job.data.candidate_id}`])).rows[0];
      const contract=binding?readFullCrawlFetchContractFromIntent(binding.intent_json).contract:newFullCrawlFetchContractForJob(job);
      routeJob={...job,queueName:job.queueName,name:job.name,data:{...job.data,fetch_contract:contract}};
    }
    const route=fullCrawlJobRoute(routeJob);
    if(!await ready())return deferJobForSlotPause(job,token);
    if(route==='compatibility')return compatibility.execute(job,token);
    if(route==='remote')await restoreFullCrawlHandoff({query,job});
    await gateVideoApiJob({query,job,token,delayMs:apiDelayMs});
    if(route==='control'){
      if(!controlEnabled())throw new Error('Migration batch control is not enabled on this worker');
      return startControlledMigrationChannel({query,withTransaction,batchId:job.data.batch_id,channelId:job.data.channel_id});
    }
    if(job.data.migration_control_start){
      if(!controlEnabled())throw new Error('Migration batch control is not enabled on this worker');
      if(!await prepareControlledMigrationSnapshot({query,withTransaction,job}))return {not_started:true};
    }
    try{
      await assertFullCrawlSnapshotRecoveryOwner(query,job);
      if(job.data.retry_intent_id){
        const entry=await enterMigrationRetryIntentWorkerJob(query,job);
        if(entry.action==='finished_replay')return {recovered_post_commit:true,retry_intent_id:String(job.data.retry_intent_id),terminal_job_attempt:entry.terminalJobAttempt};
        if(entry.action!=='execute'){const error=new Error('Recovery Intent fence rejected Job');error.code='MIGRATION_RETRY_INTENT_FENCE_STALE';throw error;}
      }
      if(!await markChannelCandidateJobAttemptActive(query,job)){const error=new Error('Candidate attempt fence rejected Job');error.code='CANDIDATE_ATTEMPT_FENCE_STALE';throw error;}
      await event(job,'started',{remote_node_id:runtime.nodeId,remote_slot:runtime.slot});
      const managed=()=>processManagedWorkerJob({job,token,
        execute:()=>rota.executeJob(job,{prepare:()=>preparer.prepareChannel(job),
          executeAttempt:(prepared,attempt)=>executeManagedWorkerAttempt({job,prepared,attempt,
            execute:options=>runtime.executeFullCrawl(options),
            // Only committed remote receipts are checkpoints. An interrupted
            // stage is retained for recovery, not silently treated as saved.
            persistRetryableCheckpoint:args=>runtime.persistRetryableCheckpoint(args)})}),
        terminateBusinessRun:(current,error)=>terminateExhaustedBusinessRun(withTransaction,current,error),deferForSlotPause:deferJobForSlotPause});
      const result=await runChannelCandidateWorkerJobWithDurableSettlement({query,withTransaction,finishMigrationRetryIntent,job,
        execute:()=>runVideoExecutionResumable({job,token,execute:()=>runVideoApiResumable({job,token,
          execute:managed,executeReplay:()=>createApiFallback?runFullCrawlApiReplay({store,job,handoff,createApiFallback,replayLocal:current=>compatibility.replay(current)}):compatibility.replay(job),delayMs:apiDelayMs})})});
      await event(job,'completed',result);return result;
    }catch(error){
      if(error instanceof DelayedError||isVideoApiHandoff(error))throw error;
      if(['REMOTE_SUPERVISOR_NOT_READY','WORKER_NOT_READY'].includes(error?.code))return deferJobForSlotPause(job,token);
      const failure=describeChannelCandidateWorkerFailure(error);
      error.youtube_failure_decision=failure.failureDecision;applyFailureRetryDecision(job,failure.failureDecision);
      if(failure.terminalChannel||isStaleExecutionFailure(error))job.discard();
      await failChannelCandidateWorkerJob({query,withTransaction,job:{id:job.id,queueName:job.queueName,data:job.data,
        opts:job.opts,attemptsMade:Number(job.attemptsMade??0)+1,attemptsStarted:job.attemptsStarted},error,failure,finishMigrationRetryIntent,
        refreshDispatchCandidateCounts:batchId=>handoff.candidateSettled({dispatchBatchId:batchId,candidateId:job.data.candidate_id}),
        signalReadyDiscoveryPageQualifications:args=>handoff.candidateSettled(args)});
      await event(job,'failed',{youtube_failure_decision:failure.failureDecision},failure.message);
      report({event:'remote_full_crawl_failed',node_id:runtime.nodeId,slot:runtime.slot,job_id:job.id,code:error.code??error.name});throw error;
    }
  };
}
