import { businessRunIntentHash } from '../businessRunBindingStore.js';
import { incrementalPlanHash, incrementalRunId, INCREMENTAL_QUEUE, validateIncrementalPlan } from '../incrementalPlan.js';
import { planFromTask, remoteChannelPlan, CHANNEL_PLAN_CAPABILITY } from './channelPlanContract.js';
import { RemoteProtocolError } from './protocol.js';

const reject = () => { throw new RemoteProtocolError('INCREMENTAL_BUSINESS_FENCE_STALE'); };

function permitsCompletion(clock, run, plan) {
  const domains = run.result_json?.domains ?? {};
  if (['dispatched', 'running'].includes(clock.status)) return true;
  // Feature ingestion can consume our committed observations before the runner
  // finishes its last transaction. Do not fence our own successful completion.
  if (['succeeded', 'partial'].includes(clock.status)) {
    return Object.entries(plan.task_mask).every(([kind, due]) => !due
      || ['complete', 'partial'].includes(domains[kind]?.status));
  }
  return clock.status === 'cancelled' && clock.error_code === 'channel_dormant'
    && plan.task_mask.video && domains.video?.status === 'complete'
    && domains.video?.lifecycle_status === 'dormant';
}

// Center only; call inside the same transaction as the protected operation.
// It reads the actual frozen Clock/dispatch/run/binding/attempt records. It does
// not claim a BullMQ lock, register a new attempt or advance a Clock.
export async function assertRemoteIncrementalBusinessFence(client, task, { admission = false, allowCompletedRun = false, apiReplayRequestId = null } = {}) {
  const plan = planFromTask(task); const planHash = incrementalPlanHash(plan);
  const executionId = task.context?.execution_attempt_id;
  if (typeof executionId !== 'string' || !executionId) reject();
  let apiReplay = false;
  if (apiReplayRequestId !== null) {
    if (admission || task.state !== 'received' || task.last_error !== 'VIDEO_API_PENDING'
      || task.applied_result?.request_id !== apiReplayRequestId || !task.target_node_id) reject();
    const api = (await client.query('SELECT status,run_id FROM crawler.youtube_api_detail_requests WHERE request_id=$1', [apiReplayRequestId])).rows[0];
    if (!api || api.run_id !== incrementalRunId(plan.plan_id)) reject();
    if (api.status === 'pending') throw new RemoteProtocolError('REMOTE_API_NOT_READY');
    const route = (await client.query('SELECT state FROM remote_ingestion.network_bindings WHERE task_id=$1 AND generation=$2', [task.task_id, task.generation])).rows[0];
    if (route?.state !== 'retired') reject();
    apiReplay = true;
  }
  const clock = (await client.query(`SELECT *,plan_day::text AS utc_plan_day FROM feature_clock.daily_channel_plans
    WHERE plan_id=$1 FOR UPDATE`, [plan.plan_id])).rows[0];
  if (!clock || clock.channel_id !== plan.channel_id || clock.utc_plan_day !== plan.plan_day
    || clock.plan_mode !== plan.plan_mode || clock.scheduled_at?.toISOString() !== plan.scheduled_at
    || Number(clock.source_clock_version) !== plan.clock_version || clock.policy_version !== plan.policy_version
    || clock.planner_config_version !== plan.planner_config_version || clock.capacity_version !== plan.capacity.version
    || Number(clock.capacity_factor) !== plan.capacity.factor || Number(clock.player_cap) !== plan.capacity.player_cap
    || Number(clock.next_cap) !== plan.capacity.next_cap
    || ['about', 'video', 'agent'].some(kind => clock[`run_${kind}`] !== plan.task_mask[kind])) reject();
  if (admission && !['dispatched', 'running'].includes(clock.status)) reject();
  const dispatch = (await client.query(`SELECT * FROM feature_clock.dispatch_outbox WHERE plan_id=$1 FOR UPDATE`, [plan.plan_id])).rows[0];
  if (!dispatch || dispatch.job_id !== plan.job_id || dispatch.queue_name !== INCREMENTAL_QUEUE
    || !['publishing', 'published'].includes(dispatch.status)) reject();
  // publishing is allowed: BullMQ can start the job before publisher records
  // its acknowledgement. The original execution record is still mandatory.
  if (incrementalPlanHash(validateIncrementalPlan(dispatch.payload_json)) !== planHash) reject();
  const binding = (await client.query(`SELECT * FROM crawler.business_run_bindings
    WHERE business_run_key=$1 FOR UPDATE`, [`incremental-plan:${plan.plan_id}`])).rows[0];
  const runId = incrementalRunId(plan.plan_id);
  if (!binding || binding.status !== 'materialized' || binding.business_run_id !== runId
    || binding.run_kind !== 'incremental' || binding.channel_id !== plan.channel_id || binding.plan_id !== plan.plan_id
    || binding.intent_hash !== businessRunIntentHash(binding.intent_json)
    || binding.intent_json?.intent?.plan_payload_hash !== planHash) reject();
  const run = (await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE', [runId])).rows[0];
  // Only the final browser checkpoint may follow our own committed run finish.
  // Admission and collector writes continue to require a running business run.
  const ownCompletedRun = allowCompletedRun && !admission && task.state === 'applied'
    && task.applied_result?.run_id === runId && ['done', 'waiting_agent'].includes(run?.status)
    && task.applied_result?.status === run.status
    && Object.entries(plan.task_mask).every(([kind, due]) => !due
      || ['complete', 'partial', 'queued'].includes(run.result_json?.domains?.[kind]?.status));
  if (!run || run.channel_id !== plan.channel_id || run.plan_id !== plan.plan_id || run.crawl_mode !== 'incremental'
    || (run.status !== 'running' && !ownCompletedRun) || run.result_json?.plan_payload_hash !== planHash
    || run.identity_policy_id !== binding.identity_policy_id
    || Number(run.identity_policy_version) !== Number(binding.identity_policy_version)
    || run.identity_policy_hash !== binding.identity_policy_hash || !permitsCompletion(clock, run, plan)) reject();

  const attempt = (await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE', [executionId])).rows[0];
  const expectedAttemptState = apiReplay ? attempt?.status === 'failed' && Boolean(attempt.finished_at)
    : attempt?.status === 'running' && !attempt.finished_at;
  if (!attempt || !expectedAttemptState || attempt.identity_changed
    || attempt.channel_id !== plan.channel_id || attempt.run_id !== runId || attempt.business_run_id !== runId
    || attempt.queue_name !== INCREMENTAL_QUEUE || attempt.job_id !== plan.job_id
    || Number(attempt.dispatch_generation) !== plan.dispatch_generation
    || attempt.identity_policy_id !== binding.identity_policy_id
    || Number(attempt.identity_policy_version) !== Number(binding.identity_policy_version)
    || !attempt.workload_scope || !attempt.worker_id || !attempt.worker_instance_id || !attempt.slot_name
    || !attempt.task_id || !attempt.network_identity_key
    || !Number.isSafeInteger(Number(attempt.attempt_number)) || Number(attempt.attempt_number) < 1
    || !Number.isSafeInteger(Number(attempt.route_generation)) || Number(attempt.route_generation) < 1) reject();
  // The original attempt insert has a run FK. The FOR UPDATE run lock above
  // serializes that insert with this write; recheck newer attempts afterwards.
  const newer = await client.query(`SELECT 1 FROM crawler.channel_execution_attempts
    WHERE workload_scope=$1 AND business_run_id=$2 AND attempt_number>$3 LIMIT 1`,
  [attempt.workload_scope, runId, attempt.attempt_number]);
  if (newer.rowCount) reject();
  return { executionAttemptId: executionId, runId, planHash,
    rotaTask: { worker_id: attempt.worker_id, worker_instance_id: attempt.worker_instance_id,
      slot_name: attempt.slot_name, task_id: attempt.task_id, business_run_id: runId,
      route_generation: Number(attempt.route_generation), network_identity_key: attempt.network_identity_key } };
}

// Validate within an admission transaction holding the remote task lock first.
// No HTTP registration is exposed; the enqueue wrapper preserves lock order.
export async function validateRemoteIncrementalAdmission(client, job, { executionAttemptId }) {
  const contract = remoteChannelPlan(job);
  if (contract.route === 'central') return contract;
  const task = { capability: CHANNEL_PLAN_CAPABILITY, input: { plan: contract.plan },
    context: { plan_hash: contract.planHash, execution_attempt_id: executionAttemptId } };
  const ownership = await assertRemoteIncrementalBusinessFence(client, task, { admission: true });
  return { ...contract, ownership, task };
}

export async function enqueueRemoteIncrementalJob(channelStore, job, { executionAttemptId }) {
  const contract = remoteChannelPlan(job);
  if (contract.route === 'central') return contract;
  return channelStore.store.transaction(async client => {
    // Uncommitted task is invisible to nodes. Lock/create it before business
    // rows, matching receive/coordinate; a failed validation rolls it all back.
    const queued = await channelStore.enqueue(job, { executionAttemptId, client });
    await validateRemoteIncrementalAdmission(client, job, { executionAttemptId });
    return queued;
  });
}
