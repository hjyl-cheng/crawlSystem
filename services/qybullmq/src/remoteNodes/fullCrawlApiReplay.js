import {lockPublicationChannelMutation} from '../publicationChannelMutationLock.js';
import {FullCrawlYoutubeJsStore} from '../fullCrawlYoutubeJsStore.js';
import {createFullCrawlYoutubeJsExecutor} from '../fullCrawlYoutubeJsFactory.js';
import {withVideoApiReplay,assertVideoApiNetworkAllowed} from '../videoApiContinuation.js';
import {RemoteProtocolError} from './protocol.js';

export async function restoreFullCrawlHandoff({query,job}){
  if(job.data.video_api_continuation)return;
  const row=(await query(`SELECT t.applied_result,e.execution_input FROM remote_ingestion.tasks t
    JOIN remote_ingestion.full_crawl_executions e USING(task_id)
    WHERE t.capability='youtube.full-crawl.v1' AND e.execution_input->>'job_id'=$1
      AND (e.execution_input->>'candidate_id')::bigint=$2 AND (e.execution_input->>'dispatch_generation')::bigint=$3
    ORDER BY t.created_at DESC LIMIT 1`,[String(job.id),job.data.candidate_id,job.data.dispatch_generation])).rows[0];
  const receipt=row?.applied_result;
  if(receipt?.code!=='VIDEO_API_PENDING'||!receipt.request_id||receipt.continuation_consumed)return;
  if(job.data.run_id&&job.data.run_id!==receipt.run_id)throw new RemoteProtocolError('FULL_CRAWL_BUSINESS_FENCE_STALE');
  const exists=(await query('SELECT run_id FROM crawler.youtube_api_detail_requests WHERE request_id=$1',[receipt.request_id])).rows[0];
  if(exists?.run_id!==receipt.run_id)throw new RemoteProtocolError('FULL_CRAWL_API_HANDOFF');
  const data={...job.data,run_id:receipt.run_id,video_api_continuation:{request_id:receipt.request_id}};
  await job.updateData(data);job.data=data;
}

export async function runFullCrawlApiReplay({store,job,handoff,createApiFallback,replayLocal=null}){
  if(typeof createApiFallback!=='function')throw TypeError('full-crawl API fallback factory required');
  const query=store.pool.query.bind(store.pool),withTransaction=action=>store.transaction(action);
  const task=(await query(`SELECT t.* FROM remote_ingestion.tasks t JOIN remote_ingestion.full_crawl_executions e USING(task_id)
    WHERE e.execution_input->>'job_id'=$1 AND e.execution_input->>'run_id'=$2
    ORDER BY t.created_at DESC LIMIT 1`,[String(job.id),job.data.run_id])).rows[0];
  if(!task&&typeof replayLocal==='function')return withVideoApiReplay(()=>replayLocal(job));
  if(!task||task.state!=='received'||task.last_error!=='VIDEO_API_PENDING'
    ||task.applied_result?.request_id!==job.data.video_api_continuation?.request_id)throw new RemoteProtocolError('FULL_CRAWL_API_HANDOFF');
  const attempt=(await query('SELECT finished_at FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[task.context.execution_attempt_id])).rows[0];
  if(!attempt?.finished_at||(await query("SELECT 1 FROM remote_ingestion.network_bindings WHERE task_id=$1 AND (state<>'retired' OR (release_receipt->>'in_flight')::int IS DISTINCT FROM 0)",[task.task_id])).rowCount)throw new RemoteProtocolError('FULL_CRAWL_NETWORK_NOT_QUIESCED');
  const fenced=action=>withTransaction(async client=>{
    await lockPublicationChannelMutation(client,task.input.channel_id);
    const candidate=(await client.query('SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE',[task.input.candidate_id])).rows[0];
    if(candidate?.snapshot_active_job_id!==String(job.id)||Number(candidate.snapshot_active_job_attempt)!==job.attemptsStarted
      ||Number(candidate.snapshot_dispatch_generation)!==task.input.dispatch_generation)throw new RemoteProtocolError('FULL_CRAWL_BUSINESS_FENCE_STALE');
    const original=(await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE',[task.context.execution_attempt_id])).rows[0];
    if(!original?.finished_at||(await client.query('SELECT 1 FROM crawler.channel_execution_attempts WHERE business_run_id=$1 AND workload_scope=$2 AND attempt_number>$3',
      [original.business_run_id,original.workload_scope,original.attempt_number])).rowCount)throw new RemoteProtocolError('FULL_CRAWL_BUSINESS_FENCE_STALE');
    return action(client);
  });
  const local=new FullCrawlYoutubeJsStore({query,withTransaction:fenced});
  const claim=local.claimNextDetail.bind(local);local.claimNextDetail=fence=>claim(fence,{reuseApiAttempt:true});
  const forbidden=async()=>{assertVideoApiNetworkAllowed();throw Error('FULL_CRAWL_REPLAY_NETWORK_FORBIDDEN');};
  const execute=createFullCrawlYoutubeJsExecutor({store:local,collector:{collectAdmission:forbidden,collectUploads:forbidden,collectDetail:forbidden},
    handoff,videoApiFallback:createApiFallback({query,withTransaction})});
  try{
    const result=await withVideoApiReplay(()=>execute(job,{resumeMode:'api_replay'}));
    await query("UPDATE remote_ingestion.tasks SET applied_result=applied_result || '{\"continuation_consumed\":true}'::jsonb WHERE task_id=$1",[task.task_id]);
    return result;
  }catch(error){
    if(error.code==='VIDEO_API_NETWORK_REQUIRED')await query("UPDATE remote_ingestion.tasks SET applied_result=applied_result || '{\"continuation_consumed\":true}'::jsonb WHERE task_id=$1",[task.task_id]);
    throw error;
  }
}
