import {BrowserProfileStore} from '../browserProfileStore.js';
import {lockPublicationChannelMutation} from '../publicationChannelMutationLock.js';
import {fullCrawlInputHash} from './fullCrawlProtocol.js';
import {RemoteProtocolError} from './protocol.js';
const fail=()=>{throw new RemoteProtocolError('YOUTUBE_SESSION_PROFILE_MISMATCH');};

// Terminal business status does not reopen collection. This narrowly scoped
// finalizer checks the original immutable owner and writes only its cookies.
export async function applyFullCrawlProfileCheckpoint(client,{routes,request,attemptId,profileSecret}){
  const task=(await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE',[request.task_id])).rows[0];
  if(!task||task.generation!==request.generation||task.context.execution_attempt_id!==attemptId)fail();
  const input=task.input;
  await lockPublicationChannelMutation(client,input.channel_id);
  const candidate=(await client.query('SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE',[input.candidate_id])).rows[0];
  if(!candidate||candidate.channel_id!==input.channel_id||Number(candidate.snapshot_dispatch_generation)!==input.dispatch_generation
    ||candidate.snapshot_active_job_id!==input.job_id||Number(candidate.snapshot_active_job_attempt)!==input.job_attempt)fail();
  const binding=(await client.query('SELECT * FROM crawler.business_run_bindings WHERE business_run_key=$1 FOR UPDATE',[input.business_run_key])).rows[0];
  if(!binding||binding.business_run_id!==input.run_id||binding.intent_hash!==input.intent_hash)fail();
  const run=(await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE',[input.run_id])).rows[0];
  if(run&&fullCrawlInputHash(run.result_json?.fetch_contract)!==fullCrawlInputHash(input.fetch_contract))fail();
  const attempt=(await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE',[attemptId])).rows[0];
  if(!attempt||attempt.finished_at||attempt.business_run_id!==input.run_id||attempt.identity_changed)fail();
  if((await client.query('SELECT 1 FROM crawler.channel_execution_attempts WHERE business_run_id=$1 AND workload_scope=$2 AND attempt_number>$3',
    [input.run_id,attempt.workload_scope,attempt.attempt_number])).rowCount)fail();
  const route=(await client.query('SELECT * FROM remote_ingestion.network_bindings WHERE task_id=$1 AND generation=$2 FOR UPDATE',[request.task_id,request.generation])).rows[0];
  if(!route)return {applied:false};
  if(route.state!=='retired'||route.release_receipt?.in_flight!==0)fail();
  const session=(await client.query('SELECT * FROM remote_ingestion.youtube_sessions WHERE binding_id=$1 FOR UPDATE',[route.binding_id])).rows[0];
  if(session?.profile_applied_at)return {applied:true,replay:true};
  if(!session?.checkpoint_cipher)return {applied:false};
  const checkpoint=routes.decrypt(session.checkpoint_cipher,`youtube-checkpoint:${route.binding_id}`);
  if(checkpoint.status!=='success'||!checkpoint.cookies)return {applied:false};
  const bundle=routes.decrypt(session.session_cipher,`youtube-session:${route.binding_id}`),profile=bundle.profile_group;
  if(bundle.attempt_id!==attemptId||attempt.profile_group_id!==profile.profile_group_id
    ||Number(attempt.profile_revision)!==profile.profile_revision||attempt.youtubejs_profile_id!==profile.clients.youtubejs_chrome.profile_id)fail();
  const current=(await client.query('SELECT status,profile_revision FROM crawler.browser_profile_groups WHERE profile_group_id=$1 FOR UPDATE',[profile.profile_group_id])).rows[0];
  if(current?.status!=='active'||Number(current.profile_revision)!==profile.profile_revision)fail();
  const profiles=new BrowserProfileStore({queryFn:client.query.bind(client),transactionFn:fn=>fn(client),secret:profileSecret});
  await profiles.checkpointCookies(profile.profile_group_id,{youtubejs_chrome:checkpoint.cookies});
  await client.query('UPDATE remote_ingestion.youtube_sessions SET profile_applied_at=clock_timestamp() WHERE binding_id=$1',[route.binding_id]);
  return {applied:true,replay:false};
}
