import { incrementalPlanHash, incrementalRunId, validateIncrementalJob, validateIncrementalPlan } from './incrementalPlan.js';
import { businessRunIntentHash } from './businessRunBindingStore.js';
import { lockPublicationChannelMutation } from './publicationChannelMutationLock.js';
import { recordIncrementalTerminalFailure } from './incrementalTerminalFailure.js';
import { BusinessRunBudgetRecoveryError, exhaustedBusinessRunError, isBusinessRunBudgetExhausted } from './businessRunBudgetRecovery.js';
import { remotePlanJob } from './remoteNodes/executionContext.js';

const reason = 'proxy_control_business_run_budget_exhausted';
const stale = () => { throw Object.assign(new Error('Incremental budget settlement lost execution ownership'),
  { code: 'INCREMENTAL_BUSINESS_FENCE_STALE' }); };
const unsettled = message => { throw new BusinessRunBudgetRecoveryError(message); };

// Failure-only path. Lock order matches remote admission; no network I/O is
// allowed inside this transaction. Rota has already quiesced its last attempt.
export async function recordIncrementalBudgetExhaustion(client, job, error) {
  const cleanJob = remotePlanJob(job);
  const plan = validateIncrementalJob(cleanJob);
  const runId = incrementalRunId(plan.plan_id);
  const planHash = incrementalPlanHash(plan);
  const activation = Number(job.attemptsStarted);
  if (!Number.isSafeInteger(activation) || activation < 1) unsettled('Original BullMQ activation is required');
  const remoteSchema = (await client.query("SELECT to_regclass('remote_ingestion.tasks') AS name")).rows[0]?.name;
  const task = remoteSchema ? (await client.query(`SELECT * FROM remote_ingestion.tasks
    WHERE work_key=$1 FOR UPDATE`, [`incremental-plan:${plan.plan_id}:${plan.dispatch_generation}`])).rows[0] : null;
  await lockPublicationChannelMutation(client, plan.channel_id);
  const clock = (await client.query('SELECT * FROM feature_clock.daily_channel_plans WHERE plan_id=$1 FOR UPDATE', [plan.plan_id])).rows[0];
  const dispatch = (await client.query('SELECT * FROM feature_clock.dispatch_outbox WHERE plan_id=$1 FOR UPDATE', [plan.plan_id])).rows[0];
  if (!clock || clock.channel_id !== plan.channel_id || !['dispatched','running','failed','partial'].includes(clock.status)
      || clock.plan_mode !== plan.plan_mode || clock.scheduled_at?.toISOString() !== plan.scheduled_at
      || Number(clock.source_clock_version) !== plan.clock_version || clock.policy_version !== plan.policy_version
      || clock.planner_config_version !== plan.planner_config_version || clock.capacity_version !== plan.capacity.version
      || Number(clock.capacity_factor) !== plan.capacity.factor || Number(clock.player_cap) !== plan.capacity.player_cap
      || Number(clock.next_cap) !== plan.capacity.next_cap
      || ['about','video','agent'].some(domain => clock[`run_${domain}`] !== plan.task_mask[domain])
      || !dispatch || dispatch.job_id !== job.id || dispatch.queue_name !== job.queueName
      || !['publishing','published'].includes(dispatch.status)
      || incrementalPlanHash(validateIncrementalPlan(dispatch.payload_json)) !== planHash) stale();
  const binding = (await client.query('SELECT * FROM crawler.business_run_bindings WHERE business_run_key=$1 FOR UPDATE',
    [`incremental-plan:${plan.plan_id}`])).rows[0];
  const run = (await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE', [runId])).rows[0];
  if (!binding || !run || binding.business_run_id !== runId || binding.run_kind !== 'incremental'
      || binding.plan_id !== plan.plan_id || binding.channel_id !== plan.channel_id
      || binding.intent_hash !== businessRunIntentHash(binding.intent_json)
      || binding.intent_json?.intent?.plan_payload_hash !== planHash
      || run.plan_id !== plan.plan_id || run.channel_id !== plan.channel_id || run.crawl_mode !== 'incremental'
      || run.result_json?.plan_payload_hash !== planHash) stale();
  const latest = (await client.query(`SELECT attempt_id,job_id,job_attempt,dispatch_generation,status,finished_at
    FROM crawler.channel_execution_attempts WHERE business_run_id=$1 AND channel_id=$2
    ORDER BY attempt_number DESC LIMIT 1 FOR UPDATE`, [runId, plan.channel_id])).rows[0];
  if (latest && (latest.job_id !== job.id || Number(latest.dispatch_generation) !== plan.dispatch_generation
      || Number(latest.job_attempt) >= activation)) stale();
  if (task && latest && task.context?.execution_attempt_id !== latest.attempt_id) stale();
  if (latest && (!latest.finished_at || latest.status === 'running')) unsettled('Previous network attempt is not finished');
  if (task) {
    if (['pending','leased'].includes(task.state)) unsettled('Remote execution has not stopped');
    const unsafe = await client.query(`SELECT 1 FROM remote_ingestion.network_bindings
      WHERE task_id=$1 AND (state<>'retired' OR release_receipt->>'in_flight' IS DISTINCT FROM '0') LIMIT 1`, [task.task_id]);
    if (unsafe.rowCount) unsettled('Remote network has not quiesced');
    if (task.last_error === 'VIDEO_API_PENDING') unsettled('API continuation must settle before budget termination');
  }
  if (binding.status === 'terminal' && binding.terminal_reason === reason
      && run.result_json?.proxy_control?.status === 'business_run_budget_exhausted') {
    return { recorded: true, replay: true, run_id: runId };
  }
  if (!(binding.status === 'materialized' || (binding.status === 'terminal' && binding.terminal_reason === reason))
      || ['done','waiting_agent','skipped'].includes(run.status)) stale();
  const pendingApi = await client.query(`SELECT 1 FROM crawler.youtube_api_detail_requests
    WHERE run_id=$1 AND status='pending' LIMIT 1`, [runId]);
  if (pendingApi.rowCount) unsettled('API continuation must settle before budget termination');
  const evidence = { status: 'business_run_budget_exhausted', job_id: job.id,
    job_attempt: activation, dispatch_generation: plan.dispatch_generation,
    source: 'rota_begin_task', observed_at: new Date().toISOString() };
  await client.query(`UPDATE crawler.channel_runs SET status='failed',detail_status='failed',
    error_message='Rota Business Run budget exhausted',finished_at=COALESCE(finished_at,now()),updated_at=now(),
    result_json=jsonb_set(result_json,'{proxy_control}',COALESCE(result_json->'proxy_control','{}'::jsonb)||$2::jsonb)
    WHERE run_id=$1`, [runId, evidence]);
  const domains = run.result_json?.domains ?? {};
  for (const domain of ['about','video','agent']) {
    if (!plan.task_mask[domain] || ['complete','partial','queued'].includes(domains[domain]?.status)) continue;
    await recordIncrementalTerminalFailure({ job: cleanJob, error, attempts: activation, permanent: true,
      terminalKey: 'business-run-budget-exhausted', domainOverride: domain,
      failureKindOverride: 'business_run_budget_exhausted', withTransaction: action => action(client) });
    await client.query(`UPDATE crawler.channel_runs SET result_json=jsonb_set(result_json,ARRAY['domains',$2::text],
      COALESCE(result_json->'domains'->$2,'{}'::jsonb)||$3::jsonb) WHERE run_id=$1`,
    [runId, domain, {status:'failed',failure_kind:'business_run_budget_exhausted'}]);
  }
  await client.query(`UPDATE crawler.business_run_bindings SET status='terminal',terminal_reason=$2,updated_at=now()
    WHERE business_run_key=$1`, [binding.business_run_key, reason]);
  return { recorded: true, replay: false, run_id: runId };
}

export async function terminateExhaustedIncrementalRun(withTransaction, job, error) {
  if (!isBusinessRunBudgetExhausted(error)) return false;
  let recorded;
  try {
    recorded = await withTransaction(client => recordIncrementalBudgetExhaustion(client, job, error));
  } catch (failure) {
    if (failure.code === 'INCREMENTAL_BUSINESS_FENCE_STALE' || failure.code === 'BUSINESS_RUN_BUDGET_RECOVERY_FAILED') throw failure;
    // Do not retain a budget cause: the unsettled write must remain retryable.
    const recovery = new BusinessRunBudgetRecoveryError('Incremental budget terminal settlement failed');
    recovery.persistence_code = String(failure.code ?? failure.name);
    throw recovery;
  }
  throw exhaustedBusinessRunError(recorded);
}
