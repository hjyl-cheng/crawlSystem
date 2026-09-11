import { DelayedError } from 'bullmq';
import { remoteChannelPlan } from './channelPlanContract.js';
import { remotePlanJob } from './executionContext.js';
import { runRemoteIncrementalApiReplay } from './incrementalCoordinator.js';
import { IncrementalChannelRunner } from '../incrementalChannelRunner.js';
import { IncrementalRunStore } from '../incrementalRunStore.js';
import { IncrementalAgentBacklog } from '../incrementalAgentBacklog.js';
import { ProxyBusinessRunPreparer } from '../proxyBusinessRun.js';
import { executeManagedWorkerAttempt } from '../managedWorkerExecution.js';
import { processManagedWorkerJob, isStaleExecutionFailure } from '../managedWorkerJob.js';
import { deferJobForSlotPause } from '../channelJobDeferral.js';
import { terminateExhaustedBusinessRun } from '../businessRunBudgetRecovery.js';
import { gateVideoApiJob, runVideoApiResumable, isVideoApiHandoff, assertVideoApiNetworkAllowed } from '../videoApiContinuation.js';
import { executeIncrementalYoutubeJsVideo, fetchIncrementalYoutubeJsVideoDetail } from '../incrementalYoutubeJsVideo.js';
import { describeChannelCandidateWorkerFailure } from '../channelCandidateWorkerLifecycle.js';
import { applyFailureRetryDecision } from '../queues.js';
import { recordIncrementalTerminalFailure } from '../incrementalTerminalFailure.js';

// Same Plan runner, attempt policy and durable API continuation as worker.js.
// This entry consumes only the original incremental queue, never migration.
export function createCenterIncrementalProcessor({ channelStore, runtime, rota, resolvedPolicy,
  createApiFallback, ready, report = () => {}, apiDelayMs = 15000 }) {
  const query = channelStore.store.pool.query.bind(channelStore.store.pool);
  const withTransaction = action => channelStore.store.transaction(action);
  const runStore = new IncrementalRunStore({ withTransaction });
  const preparer = new ProxyBusinessRunPreparer({ queryFn: query, withTransaction, incrementalRunStore: runStore, resolvedPolicy });
  const forbidNetwork = () => { assertVideoApiNetworkAllowed();throw new Error('CENTRAL_ONLY_PLAN_NETWORK_FORBIDDEN'); };
  const fallback=createApiFallback?.({query,withTransaction})??null;
  const centralOnly = new IncrementalChannelRunner({ query, withTransaction, runStore,
    agentBacklog: new IncrementalAgentBacklog({ withTransaction }),
    openChannel: forbidNetwork,
    video:context=>executeIncrementalYoutubeJsVideo({...context,fetchDetail:(id,options)=>fetchIncrementalYoutubeJsVideoDetail(id,
      {...options,videoApiFallback:fallback,fetchYoutubeJs:forbidNetwork})}) });
  const event = (job, status, payload = {}, error = null) => query(`INSERT INTO crawler.task_events
    (queue_name,job_id,job_name,entity_key,status,payload_json,error_message) VALUES($1,$2,$3,$4,$5,$6,$7)`,
  [job.queueName,job.id,job.name,job.data.channel_id,status,payload,error]);
  async function replay(job) {
    const contract = remoteChannelPlan(remotePlanJob(job));
    const task = (await query('SELECT task_id FROM remote_ingestion.tasks WHERE work_key=$1', [contract.workKey])).rows[0];
    if (!task) {
      // A local Worker may have created this continuation. Re-enter the same
      // runner in its existing no-network replay scope; remaining work goes Rota.
      return centralOnly.execute(remotePlanJob(job));
    }
    return runRemoteIncrementalApiReplay({channelStore,taskId:task.task_id,
      requestId:job.data.video_api_continuation.request_id,createApiFallback,
      assertCoordinator:runtime.assertAdmission?async client=>{
        if(await runtime.assertAdmission(client,runtime.nodeId,runtime.slot)!==true){
          const error=new Error('REMOTE_SUPERVISOR_NOT_READY');error.code='REMOTE_SUPERVISOR_NOT_READY';throw error;
        }
      }:null});
  }
  return async (job, token) => {
    const contract = remoteChannelPlan(remotePlanJob(job));
    // A center can die after the SQL API handoff committed but before BullMQ
    // updateData/moveToDelayed. Reconstruct only from that exact frozen Plan's
    // durable request, then let the unchanged API gate/replay own the rest.
    if(!job.data.video_api_continuation && contract.route==='remote'){
      const task=(await query(`SELECT applied_result FROM remote_ingestion.tasks
        WHERE work_key=$1 AND state='received' AND last_error='VIDEO_API_PENDING'`,[contract.workKey])).rows[0];
      if(task?.applied_result?.request_id){
        const data={...job.data,video_api_continuation:{request_id:task.applied_result.request_id}};
        await job.updateData(data);job.data=data;
      }
    }
    await gateVideoApiJob({query,job,token,delayMs:apiDelayMs});
    if (!await ready()) return deferJobForSlotPause(job, token);
    const started = Date.now();
    await event(job,'started',{remote_node_id:runtime.nodeId,remote_slot:runtime.slot});
    try {
      const execute = () => contract.route === 'central'
        ? centralOnly.execute(remotePlanJob(job))
        : processManagedWorkerJob({job,token,
          execute:()=>rota.executeJob(job,{prepare:()=>preparer.prepareChannel(remotePlanJob(job)),
            executeAttempt:(prepared,attempt)=>executeManagedWorkerAttempt({job,prepared,attempt,
              execute:()=>runtime.executePlan(),persistRetryableCheckpoint:async()=>true})}),
          terminateBusinessRun:(current,error)=>terminateExhaustedBusinessRun(withTransaction,current,error),
          deferForSlotPause:deferJobForSlotPause});
      const result = await runVideoApiResumable({job,token,execute,executeReplay:()=>replay(job),delayMs:apiDelayMs});
      await event(job,'completed',{...result,remote_node_id:runtime.nodeId,remote_slot:runtime.slot,duration_ms:Date.now()-started});
      return result;
    } catch (error) {
      if (error instanceof DelayedError || isVideoApiHandoff(error)) throw error;
      if (error?.code === 'REMOTE_SUPERVISOR_NOT_READY') return deferJobForSlotPause(job,token);
      const failure=describeChannelCandidateWorkerFailure(error);
      error.youtube_failure_decision=failure.failureDecision;
      applyFailureRetryDecision(job,failure.failureDecision);
      if (failure.terminalChannel || isStaleExecutionFailure(error)) job.discard();
      // Persist terminal evidence before BullMQ releases the job lock, using
      // the original idempotent observation writer and failure classification.
      await recordIncrementalTerminalFailure({job:remotePlanJob(job),error,
        attempts:Number(job.attemptsMade??0)+1,maxAttempts:Number(job.opts?.attempts??1),
        permanent:failure.permanentFailure || !!failure.terminalChannel,withTransaction});
      await event(job,'failed',{youtube_failure_decision:failure.failureDecision,
        remote_node_id:runtime.nodeId,remote_slot:runtime.slot},failure.message);
      report({event:'remote_incremental_failed',node_id:runtime.nodeId,slot:runtime.slot,job_id:job.id,code:error.code??error.name});
      throw error;
    }
  };
}
