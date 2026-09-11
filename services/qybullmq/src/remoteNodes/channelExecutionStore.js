import { setTimeout as delay } from 'node:timers/promises';
import { BrowserProfileStore } from '../browserProfileStore.js';
import { IncrementalRunStore } from '../incrementalRunStore.js';
import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { remoteChannelPlan, CHANNEL_PLAN_CAPABILITY } from './channelPlanContract.js';
import { assertRemoteIncrementalBusinessFence } from './incrementalBusinessFence.js';
import { remoteExecutionOptions, remotePlanJob } from './executionContext.js';
import { RemoteProtocolError, uuid } from './protocol.js';

const fail = code => { throw new RemoteProtocolError(code); };

// Center only. Original Rota owns retry/country/API decisions and budgets. This
// module binds each of its attempts to one durable remote delivery generation.
export class RemoteChannelExecutionStore {
  constructor({ channelStore, profileSecret, assertAdmission = null }) {
    if (typeof profileSecret !== 'string' || !profileSecret) throw new TypeError('central profile encryption secret required');
    Object.assign(this, { channelStore, store: channelStore.store, profileSecret, assertAdmission });
  }

  profiles(client) { return new BrowserProfileStore({ queryFn: client.query.bind(client), transactionFn: action => action(client), secret: this.profileSecret }); }

  async pendingCountryHandoff(job) {
    const contract=remoteChannelPlan(remotePlanJob(job));
    const row=(await this.store.pool.query(`SELECT t.applied_result,t.context FROM remote_ingestion.tasks t
      JOIN crawler.channel_execution_attempts a ON a.attempt_id=t.context->>'execution_attempt_id'
      WHERE t.work_key=$1 AND t.state='received' AND t.last_error='UPLOADS_COUNTRY_RECHECK'
        AND a.finished_at IS NOT NULL AND a.status='success'
        AND NOT EXISTS(SELECT 1 FROM remote_ingestion.network_bindings b WHERE b.task_id=t.task_id AND b.state<>'retired')`,[contract.workKey])).rows[0];
    if(!row)return null;
    const country=row.applied_result?.country;
    if(row.context.plan_hash!==contract.planHash || !/^[A-Z]{2}$/.test(country))fail('REMOTE_COUNTRY_HANDOFF_MISSING');
    const saved=job.data.uploads_country_recheck;
    if(saved?.country===country && ['checked','unavailable'].includes(saved.status))return null;
    // Missing/requested is not proof that Rota selected a country. Re-enter
    // its existing country command using this newly charged original attempt.
    return country;
  }

  async prepare({ job, assignment, task: rotaTask, prepared, policy, nodeId, slot, resumeMode }) {
    uuid(nodeId); uuid(rotaTask.task_id);
    const cleanJob = remotePlanJob(job); const contract = remoteChannelPlan(cleanJob);
    if (contract.route !== 'remote') fail('CENTRAL_ONLY_PLAN');
    if (prepared.businessRunId !== `incremental:${contract.plan.plan_id}`
      || assignment.identity_policy_id !== policy.id || assignment.identity_policy_hash !== policy.hash
      || Number(assignment.identity_policy_version) !== Number(policy.version)) fail('REMOTE_ROTA_IDENTITY_MISMATCH');
    if (!Number.isSafeInteger(job.attemptsStarted) || job.attemptsStarted < 1) throw new TypeError('original BullMQ attempt required');
    const attemptId = `channel-attempt:${rotaTask.task_id}`;
    const executionOptions = remoteExecutionOptions({ egress_country: assignment.egress_country || null,
      uploads_country_recheck: job.data.uploads_country_recheck ?? null, resume_mode: resumeMode ?? 'initial' });
    const context = { plan_hash: contract.planHash, execution_attempt_id: attemptId, execution_options: executionOptions };
    return this.store.transaction(async client => {
      const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [nodeId])).rows[0];
      if (node?.state !== 'active') fail('REMOTE_NODE_NOT_ACTIVE');
      if (this.assertAdmission && await this.assertAdmission(client,nodeId,slot)!==true) fail('REMOTE_SUPERVISOR_NOT_READY');
      let transport = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE work_key=$1 FOR UPDATE', [contract.workKey])).rows[0];
      if (transport && (transport.capability !== CHANNEL_PLAN_CAPABILITY
        || canonicalIncrementalJson(transport.input) !== canonicalIncrementalJson({ plan: contract.plan }))) fail('WORK_KEY_CONFLICT');
      const sameAttempt = transport?.context.execution_attempt_id === attemptId;
      if (transport && sameAttempt) {
        if (!['pending','leased'].includes(transport.state) || transport.target_node_id !== nodeId || transport.target_worker_slot !== slot
          || canonicalIncrementalJson(transport.context) !== canonicalIncrementalJson(context)) fail('REMOTE_EXECUTION_REPLAY_CONFLICT');
      } else if (transport) {
        if (!['failed','received'].includes(transport.state)) fail('REMOTE_EXECUTION_NOT_SETTLED');
        const previous = (await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1', [transport.context.execution_attempt_id])).rows[0];
        if (!previous?.finished_at || previous.status === 'running'
          || Number(rotaTask.attempt_number) <= Number(previous.attempt_number)) fail('REMOTE_PREVIOUS_ATTEMPT_NOT_FINISHED');
        const unfinished = (await client.query("SELECT 1 FROM remote_ingestion.network_bindings WHERE task_id=$1 AND state<>'retired' LIMIT 1", [transport.task_id])).rows[0];
        if (unfinished) fail('REMOTE_PREVIOUS_NETWORK_NOT_RETIRED');
        if (transport.last_error === 'UPLOADS_COUNTRY_RECHECK'
          && (executionOptions.uploads_country_recheck?.country !== transport.applied_result?.country
            || !['checked','unavailable'].includes(executionOptions.uploads_country_recheck?.status))) fail('REMOTE_COUNTRY_HANDOFF_MISSING');
        if (transport.last_error === 'VIDEO_API_PENDING') {
          const api = (await client.query('SELECT status,run_id FROM crawler.youtube_api_detail_requests WHERE request_id=$1', [transport.applied_result?.request_id])).rows[0];
          if (!api || api.run_id !== prepared.businessRunId) fail('API_REQUEST_IDENTITY_CONFLICT');
          if (api.status === 'pending') fail('REMOTE_API_NOT_READY');
        }
      }
      if (!transport) {
        const taskId = await this.store.enqueue({ workKey: contract.workKey, capability: CHANNEL_PLAN_CAPABILITY,
          scopeKey: `channel:${contract.plan.channel_id}`, input: { plan: contract.plan }, context }, { client });
        transport = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [taskId])).rows[0];
      }
      // These are the original run/profile/attempt methods. Admission and their
      // writes roll back together if the frozen Clock or business fence changed.
      const runs = new IncrementalRunStore({ withTransaction: action => action(client) });
      const run = await runs.claim(cleanJob.data);
      if (run.terminal) fail('REMOTE_PLAN_ALREADY_COMPLETED');
      const profiles = this.profiles(client);
      const profileGroup = await profiles.loadOrCreate({ identityPolicyId: assignment.identity_policy_id,
        identityPolicyVersion: assignment.identity_policy_version, networkIdentityKey: assignment.network_identity_key,
        profileEpoch: assignment.profile_epoch, language: policy.youtube_language, country: policy.youtube_country,
        timezone: policy.browser_profile_timezone });
      await profiles.beginAttempt({ channelId: contract.plan.channel_id, runId: prepared.businessRunId,
        queueName: job.queueName, jobId: job.id, jobAttempt: job.attemptsStarted - 1,
        dispatchGeneration: contract.plan.dispatch_generation, workerId: assignment.worker_id,
        proxy: assignment, profileGroup, task: rotaTask, prepared });
      if (transport.context.execution_attempt_id !== attemptId) {
        await client.query(`INSERT INTO remote_ingestion.execution_handoffs
          (task_id,from_generation,from_attempt_id,to_attempt_id,reason,previous_context,previous_result)
          VALUES($1,$2,$3,$4,$5,$6,$7)`, [transport.task_id, transport.generation, transport.context.execution_attempt_id,
          attemptId, transport.last_error || 'CENTRAL_RECOVERY', transport.context, transport.applied_result]);
      }
      if (!sameAttempt) await client.query(`UPDATE remote_ingestion.tasks SET state='pending',context=$2,
        target_node_id=$3,target_worker_slot=$4,node_id=NULL,worker_slot=NULL,lease_until=NULL,
        coordinator_id=NULL,coordinator_until=NULL,last_error=NULL,applied_result=NULL,received_at=NULL,applied_at=NULL
        WHERE task_id=$1`, [transport.task_id, context, nodeId, slot]);
      const current = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1', [transport.task_id])).rows[0];
      await assertRemoteIncrementalBusinessFence(client, current, { admission: true });
      const registered = (await client.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2', [nodeId, slot])).rows[0];
      if (registered?.rota_worker_id !== assignment.worker_id) fail('NETWORK_SLOT_CONFLICT');
      return { taskId: transport.task_id, attemptId, profileGroup, executionOptions, plan: contract.plan };
    });
  }

  async waitClaim(admission, { nodeId, slot, signal, pollMs = 100 }) {
    for (;;) {
      signal.throwIfAborted();
      const task = (await this.store.pool.query('SELECT *,lease_until>clock_timestamp() AS alive FROM remote_ingestion.tasks WHERE task_id=$1', [admission.taskId])).rows[0];
      if (task?.context.execution_attempt_id !== admission.attemptId) fail('REMOTE_EXECUTION_REPLACED');
      if (task.state === 'leased' && task.alive && task.node_id === nodeId && task.worker_slot === slot) return this.store.lease(task);
      if (task.state !== 'pending') fail('REMOTE_EXECUTION_NOT_AVAILABLE');
      await delay(pollMs, null, { signal });
    }
  }

  async find(job, rotaTask) {
    const contract = remoteChannelPlan(remotePlanJob(job));
    const attemptId = `channel-attempt:${rotaTask.task_id}`;
    const row = (await this.store.pool.query("SELECT task_id FROM remote_ingestion.tasks WHERE work_key=$1 AND context->>'execution_attempt_id'=$2", [contract.workKey, attemptId])).rows[0];
    return row ? { taskId: row.task_id, attemptId } : null;
  }

  // Transport termination is safe after a business fence fails: it grants no
  // writes to crawler data, and only closes the exact original attempt's task.
  async stop(admission, error) {
    await this.store.pool.query(`UPDATE remote_ingestion.tasks SET state='failed',last_error=$3,
      coordinator_until=NULL WHERE task_id=$1 AND context->>'execution_attempt_id'=$2 AND state IN ('pending','leased')`,
    [admission.taskId, admission.attemptId, String(error?.code || 'REMOTE_EXECUTION_STOPPED').slice(0,300)]);
  }

  async finish(admission, { status, error = null, result = {} }) {
    return this.store.transaction(async client => {
      const transport = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [admission.taskId])).rows[0];
      if (transport?.context.execution_attempt_id !== admission.attemptId) fail('REMOTE_EXECUTION_REPLACED');
      const attempt = (await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE', [admission.attemptId])).rows[0];
      if (!attempt) fail('REMOTE_ATTEMPT_MISSING');
      if (attempt.finished_at) return;
      const newer = await client.query('SELECT 1 FROM crawler.channel_execution_attempts WHERE business_run_id=$1 AND workload_scope=$2 AND attempt_number>$3 LIMIT 1', [attempt.business_run_id, attempt.workload_scope, attempt.attempt_number]);
      if (newer.rowCount) fail('REMOTE_EXECUTION_REPLACED');
      await this.profiles(client).finishAttempt(admission.attemptId, { status, error, result });
    });
  }
}
