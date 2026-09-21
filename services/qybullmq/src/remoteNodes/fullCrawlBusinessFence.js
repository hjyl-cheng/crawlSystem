import { businessRunIntentHash } from '../businessRunBindingStore.js';
import { readFullCrawlFetchContractFromIntent } from '../fullCrawlFetchContract.js';
import { lockPublicationChannelMutation } from '../publicationChannelMutationLock.js';
import { FULL_CRAWL_WORKLOAD } from './collectingWorkload.js';
import { fullCrawlInputHash, validateFullCrawlExecution } from './fullCrawlProtocol.js';
import { RemoteProtocolError } from './protocol.js';

const reject = () => { throw new RemoteProtocolError('FULL_CRAWL_BUSINESS_FENCE_STALE'); };
const positive = value => Number.isSafeInteger(Number(value)) && Number(value) > 0;
const same = (a,b) => fullCrawlInputHash(a) === fullCrawlInputHash(b);

export function fullCrawlConnectionIdentity(row) {
  return Object.fromEntries(['node_id','slot','deployment_id','config_hash','instance_id',
    'relay_boot_id','runtime_revision'].map(key => [key,row[key]]));
}

// Center only, in the transaction that performs the protected write. The
// caller locks node -> connection -> task before entering this module.
export async function assertRemoteFullCrawlBusinessFence(client, input) {
  validateFullCrawlExecution(input);
  if (input.run_id !== input.business_run_id) reject();
  const installed=await client.query(`SELECT 1 FROM pg_trigger WHERE
    tgrelid='crawler.channel_execution_attempts'::regclass AND tgname='full_crawl_attempt_insert_lock'
    AND tgenabled IN ('O','A') AND NOT tgisinternal`);
  if (!installed.rowCount) throw new RemoteProtocolError('FULL_CRAWL_BUSINESS_SCHEMA_REQUIRED');
  await lockPublicationChannelMutation(client,input.channel_id);
  const candidate=(await client.query('SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE',
    [input.candidate_id])).rows[0];
  if (!candidate || candidate.channel_id!==input.channel_id
    || Number(candidate.snapshot_dispatch_generation)!==input.dispatch_generation
    || candidate.snapshot_active_job_id!==input.job_id
    || Number(candidate.snapshot_active_job_attempt)!==input.job_attempt
    || !['queued','validating','accepted'].includes(candidate.status)) reject();
  const binding=(await client.query('SELECT * FROM crawler.business_run_bindings WHERE business_run_key=$1 FOR UPDATE',
    [input.business_run_key])).rows[0];
  if (!binding || !['reserved','materialized'].includes(binding.status) || binding.run_kind!=='full'
    || binding.business_run_id!==input.run_id || binding.channel_id!==input.channel_id
    || Number(binding.candidate_id)!==input.candidate_id || binding.intent_schema_version!==1 || binding.intent_hash!==input.intent_hash
    || businessRunIntentHash(binding.intent_json)!==input.intent_hash) reject();
  const intent=binding.intent_json;
  if (intent.schema_version!==1 || intent.intent?.job_name!==input.job_name || intent.intent?.crawl_mode!=='full'
    || intent.business_run_key!==binding.business_run_key || intent.channel_id!==binding.channel_id
    || intent.candidate_id!==input.candidate_id || intent.run_kind!=='full'
    || intent.identity_policy_id!==binding.identity_policy_id
    || Number(intent.identity_policy_version)!==Number(binding.identity_policy_version)
    || intent.identity_policy_hash!==binding.identity_policy_hash) reject();
  const contract=readFullCrawlFetchContractFromIntent(intent);
  if (!contract.explicit || !same(contract.contract,input.fetch_contract)) reject();
  const run=(await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE',[input.run_id])).rows[0];
  const channel=(await client.query('SELECT * FROM crawler.channels WHERE channel_id=$1 FOR UPDATE',[input.channel_id])).rows[0];
  if (binding.status==='reserved') {
    if (run || candidate.status==='accepted' || channel?.status==='removed') reject();
  } else {
    if (!run || run.channel_id!==input.channel_id || Number(run.candidate_id)!==input.candidate_id
      || run.crawl_mode!=='full' || !['running','waiting_detail'].includes(run.status)
      || run.publication_finalized_at || candidate.status!=='accepted'
      || !run.result_json?.fetch_contract || !same(run.result_json.fetch_contract,input.fetch_contract)
      || channel?.status!=='active' || channel.latest_run_id!==input.run_id
      || channel.registry_promotion_run_id!==input.run_id
      || Number(channel.registry_promotion_candidate_id)!==input.candidate_id) reject();
  }
  const attempt=(await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE',
    [input.execution_attempt_id])).rows[0];
  // beginAttempt precedes admission, so run_id may remain NULL after promotion;
  // business_run_id is mandatory throughout. Its job_attempt is zero based.
  if (!attempt || attempt.status!=='running' || attempt.finished_at || attempt.identity_changed
    || attempt.channel_id!==input.channel_id || attempt.business_run_id!==input.run_id
    || (attempt.run_id!==null && attempt.run_id!==input.run_id)
    || attempt.queue_name!==input.queue_name || attempt.job_id!==input.job_id
    || Number(attempt.job_attempt)!==input.job_attempt-1
    || Number(attempt.dispatch_generation)!==input.dispatch_generation
    || attempt.identity_policy_id!==binding.identity_policy_id
    || Number(attempt.identity_policy_version)!==Number(binding.identity_policy_version)
    || !attempt.workload_scope || !attempt.worker_id || !attempt.worker_instance_id || !attempt.slot_name
    || !attempt.task_id || !attempt.network_identity_key || !positive(attempt.attempt_number)
    || !positive(attempt.route_generation)) reject();
  // The optional business schema serializes INSERTs even before a run exists.
  const newer=await client.query(`SELECT 1 FROM crawler.channel_execution_attempts
    WHERE business_run_id=$1 AND workload_scope=$2 AND attempt_number>$3 LIMIT 1`,
  [input.run_id,attempt.workload_scope,attempt.attempt_number]);
  if (newer.rowCount) reject();
  return { input, candidate, binding, run, attempt, rotaFence: {
    workload_scope:attempt.workload_scope,worker_id:attempt.worker_id,
    worker_instance_id:attempt.worker_instance_id,slot_name:attempt.slot_name,
    task_id:attempt.task_id,business_run_id:attempt.business_run_id,
    route_generation:Number(attempt.route_generation),network_identity_key:attempt.network_identity_key,
  } };
}

export async function assertRemoteFullCrawlTaskFence(client,task,connection) {
  if (task.capability!==FULL_CRAWL_WORKLOAD.capability || !['pending','leased'].includes(task.state)
    || task.target_node_id!==connection.node_id || task.target_worker_slot!==connection.slot) reject();
  const generation=task.state==='pending' ? task.generation+1 : task.generation;
  if (task.state==='leased' && (task.node_id!==connection.node_id || task.worker_slot!==connection.slot)) reject();
  const evidence=(await client.query(`SELECT * FROM remote_ingestion.full_crawl_executions
    WHERE task_id=$1 AND generation=$2 FOR UPDATE`,[task.task_id,generation])).rows[0];
  if (!evidence || evidence.node_id!==connection.node_id || evidence.worker_slot!==connection.slot
    || evidence.instance_id!==connection.instance_id || evidence.protocol_version!==1
    || !evidence.connection_identity || !same(evidence.connection_identity,fullCrawlConnectionIdentity(connection))
    || evidence.execution_hash!==fullCrawlInputHash(task.input)
    || !same(evidence.execution_input,task.input) || task.context?.execution_hash!==evidence.execution_hash
    || task.context?.execution_attempt_id!==task.input.execution_attempt_id) reject();
  const ownership=await assertRemoteFullCrawlBusinessFence(client,evidence.execution_input);
  const slot=(await client.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR SHARE',
    [connection.node_id,connection.slot])).rows[0];
  if (slot?.rota_worker_id!==ownership.attempt.worker_id || !evidence.rota_fence
    || !same(evidence.rota_fence,ownership.rotaFence)) reject();
  const route=(await client.query(`SELECT * FROM remote_ingestion.network_bindings
    WHERE task_id=$1 AND generation=$2 FOR SHARE`,[task.task_id,generation])).rows[0];
  if (route && (route.node_id!==connection.node_id || route.slot!==connection.slot
    || Object.entries(ownership.rotaFence).some(([key,value])=>(['workload_scope','network_identity_key'].includes(key)?route.identity?.[key]:route.rota_fence?.[key])!==value))) reject();
  return {...ownership,evidence};
}
