import {
  finalizeDispatchRevision,
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
} from "./finalizePolicy.js";
import {
  claimContentDetailExecution,
  contentDetailExecutionFence,
  lockContentDetailExecution,
  prepareContentDetailExecutionRequeue,
} from "./contentDetailExecutionFence.js";
import { queuesByRole, safeJobId } from "./queues.js";

const ACTIVE_RETRY_STATUSES = Object.freeze(["retrying", "dispatched"]);
const REPRESENTED_JOB_STATES = new Set([
  "active",
  "delayed",
  "paused",
  "prioritized",
  "waiting",
  "waiting-children",
]);
const REPRESENTED_JOB_STATE_LIST = Object.freeze([...REPRESENTED_JOB_STATES]);
const RECOVERY_SCAN_PATTERN = Object.freeze(["active", "active", "active", "active", "legacy"]);
export const CONTENT_DETAIL_RECOVERY_MAX_JOB_EPOCH = 1;
const QUEUE_ORDER = Object.freeze([
  queuesByRole.channelCrawl,
  queuesByRole.contentDetail,
  queuesByRole.dataApiBatch,
  queuesByRole.agentBatch,
  queuesByRole.finalize,
]);

function boundedLimit(value) {
  return Math.max(1, Math.min(500, Number(value) || 100));
}

function text(value) {
  return String(value ?? "").trim() || null;
}

function positiveInteger(value) {
  const normalized = Number(value);
  return Number.isSafeInteger(normalized) && normalized > 0 ? normalized : null;
}

function nonNegativeInteger(value) {
  const normalized = Number(value);
  return Number.isSafeInteger(normalized) && normalized >= 0 ? normalized : null;
}

function recoveryAgentJobEpoch(value) {
  return value == null ? 0 : nonNegativeInteger(value);
}

function contentDetailJobEpoch(value) {
  return value == null ? 0 : nonNegativeInteger(value);
}

function retryableSystemFailureDecision(decision) {
  return decision?.kind === "retryable_system_failure"
    && decision?.evidence?.failure_type === "retryable_system_failure"
    && decision?.evidence?.retryable === true;
}

function contentDetailRecoveryEvidence(row, expected) {
  const jobEpoch = contentDetailJobEpoch(expected?.data?.content_detail_job_epoch);
  if (jobEpoch == null || jobEpoch >= CONTENT_DETAIL_RECOVERY_MAX_JOB_EPOCH) return null;
  const evidence = row?.failure_evidence?.content_detail_recovery?.[String(jobEpoch)] ?? null;
  if (!evidence || typeof evidence !== "object") return null;
  return evidence.job_id === expected.id
    && Number(evidence.job_epoch) === jobEpoch
    && evidence.retryable_system_failure === true
    && evidence.requeue_allowed === true
    ? evidence
    : null;
}

export async function settleContentDetailRecoveryTerminalFailure(client, job, {
  failureDecision,
  errorMessage = null,
  parserContractError = null,
  maxJobEpoch = CONTENT_DETAIL_RECOVERY_MAX_JOB_EPOCH,
} = {}) {
  requiredPostgresClient(client);
  const normalizedMaxEpoch = nonNegativeInteger(maxJobEpoch);
  if (normalizedMaxEpoch == null) {
    throw new TypeError("Content Detail recovery max Job epoch must be a non-negative integer");
  }
  const fence = contentDetailExecutionFence(job);
  if (!fence.recovery) return Object.freeze({ action: "not_recovery" });
  if (!(await lockContentDetailExecution(client, fence))) {
    return Object.freeze({ action: "stale" });
  }

  const retryableSystemFailure = retryableSystemFailureDecision(failureDecision);
  const budgetExhausted = retryableSystemFailure && fence.jobEpoch >= normalizedMaxEpoch;
  const resolution = retryableSystemFailure
    ? budgetExhausted ? "recovery_content_detail_retry_budget_exhausted" : null
    : "recovery_content_detail_terminal_failure";
  const evidence = {
    job_id: fence.jobId,
    job_attempt: fence.jobAttempt,
    job_epoch: fence.jobEpoch,
    failure_kind: text(failureDecision?.kind) ?? "unknown",
    retry_mode: text(failureDecision?.retry_mode) ?? "unknown",
    retryable_system_failure: retryableSystemFailure,
    requeue_allowed: retryableSystemFailure && !budgetExhausted,
    budget_exhausted: budgetExhausted,
    error_message: text(errorMessage)?.slice(0, 2000) ?? null,
    ...(parserContractError ? { parser_contract_error: parserContractError } : {}),
  };
  const settled = await client.query(
     `WITH released AS (
       UPDATE crawler.channel_runs
       SET status=CASE WHEN $11::text IS NULL THEN 'waiting_detail' ELSE 'failed' END,
           detail_status=CASE WHEN $11::text IS NULL THEN 'queued' ELSE 'failed' END,
           error_message=CASE WHEN $11::text IS NULL THEN error_message ELSE $12::text END,
           result_json=jsonb_set(
             result_json,
             '{content_detail_recovery_terminal}',
             $10::jsonb,
             true
           ),
           finished_at=CASE WHEN $11::text IS NULL THEN NULL ELSE now() END,
           detail_active_job_id=NULL,detail_active_job_attempt=NULL,
           detail_active_scope_key=NULL,detail_active_job_epoch=NULL,updated_at=now()
       WHERE run_id=$5
         AND detail_job_epoch=$6
         AND detail_active_job_id=$7
         AND detail_active_job_attempt=$8
         AND detail_active_scope_key=$9
         AND detail_active_job_epoch=$6
       RETURNING run_id
     )
     UPDATE crawler.migration_system_retry_items retry
     SET failure_evidence=jsonb_set(
           retry.failure_evidence,
           '{content_detail_recovery}',
           COALESCE(retry.failure_evidence->'content_detail_recovery','{}'::jsonb)
             || jsonb_build_object($6::text,$10::jsonb),
           true
         ),
         status=CASE WHEN $11::text IS NULL THEN retry.status ELSE 'resolved' END,
         resolution=COALESCE($11::text,retry.resolution),
         resolved_at=CASE
           WHEN $11::text IS NULL THEN retry.resolved_at
           ELSE COALESCE(retry.resolved_at,now())
         END,
         updated_at=now()
     FROM released
     WHERE retry.system_retry_id=$1
       AND retry.candidate_id=$2
       AND retry.retry_dispatch_generation=$3
       AND retry.failed_dispatch_batch_id=$4
       AND retry.recovery_run_id=released.run_id
       AND retry.status='dispatched'
     RETURNING retry.system_retry_id,retry.status,retry.resolution`,
    [
      fence.migrationSystemRetryId,
      fence.candidateId,
      fence.dispatchGeneration,
      fence.pipelineCycleId,
      fence.runId,
      fence.jobEpoch,
      fence.jobId,
      fence.jobAttempt,
      fence.scopeKey,
      JSON.stringify(evidence),
      resolution,
      evidence.error_message,
    ],
  );
  if (settled.rowCount !== 1) return Object.freeze({ action: "stale" });
  return Object.freeze({
    action: resolution == null ? "requeue_allowed" : "resolved",
    resolution,
    evidence: Object.freeze(evidence),
  });
}

function expectedGeneration(row) {
  return positiveInteger(row.retry_dispatch_generation)
    ?? positiveInteger(row.failed_dispatch_generation);
}

function finalizeDispatchState(row) {
  return {
    channel_id: row.candidate_channel_id ?? row.channel_id,
    latest_run_id: row.channel_latest_run_id ?? row.latest_run_id,
    channel_status: row.channel_status,
    agent_status: row.agent_status,
    channel_updated_at: row.channel_updated_at,
    detail_status: row.run_detail_status ?? row.detail_status,
    expected_content_count: Number(row.expected_content_count ?? 0),
    pipeline_cycle_id: row.run_pipeline_cycle_id ?? row.pipeline_cycle_id,
    run_final_repair: row.run_final_repair ?? null,
    candidate_count: Number(row.candidate_count ?? 0),
    candidate_updated_at: row.candidate_updated_at,
    content_count: Number(row.content_count ?? 0),
    content_updated_at: row.content_updated_at,
    agent_updated_at: row.agent_updated_at,
  };
}

function exactCandidateFence(row) {
  const generation = expectedGeneration(row);
  return generation != null
    && Number(row.snapshot_dispatch_generation) === generation
    && text(row.candidate_dispatch_batch_id) === text(row.failed_dispatch_batch_id)
    && (
      row.status !== "dispatched"
      || Number(row.retry_dispatch_generation) === generation
    );
}

function terminalBusinessOutcome(row) {
  return exactCandidateFence(row)
    && ["rejected", "existing"].includes(String(row.candidate_status ?? ""))
    && row.snapshot_active_job_id == null
    && row.snapshot_active_job_attempt == null;
}

function exactRecoveryRunFence(row) {
  const runId = text(row.run_id);
  return runId != null
    && text(row.recovery_run_id) === runId
    && Number(row.run_candidate_id) === Number(row.candidate_id)
    && text(row.run_channel_id) === text(row.candidate_channel_id)
    && text(row.channel_latest_run_id) === runId
    && text(row.run_dispatch_batch_id) === text(row.failed_dispatch_batch_id);
}

function finalizedAfterRequiredSources(row) {
  if (row.channel_status === "dormant") {
    return row.publication_finalized_status === "ready_partial";
  }
  if (row.channel_status !== "active"
      || row.agent_status !== "done"
      || row.agent_profile_status !== "success"
      || row.agent_updated_at == null
      || row.finalized_updated_at == null) {
    return false;
  }
  const finalizedAt = new Date(row.finalized_updated_at).getTime();
  const requiredAt = [
    row.channel_updated_at,
    row.agent_updated_at,
    row.candidate_updated_at,
    row.content_updated_at,
  ].filter((value) => value != null)
    .map((value) => new Date(value).getTime());
  return Number.isFinite(finalizedAt)
    && requiredAt.every(Number.isFinite)
    && finalizedAt >= Math.max(...requiredAt);
}

function sameText(left, right) {
  return text(left) === text(right);
}

function sameInteger(left, right) {
  return Number(left) === Number(right);
}

export function representedMigrationSystemRetryAgentJob(job, expected) {
  const channelIds = Array.isArray(job?.data?.channel_ids) ? job.data.channel_ids : [];
  const jobEpoch = recoveryAgentJobEpoch(job?.data?.recovery_agent_job_epoch);
  const expectedEpoch = recoveryAgentJobEpoch(expected?.data?.recovery_agent_job_epoch);
  return job?.name === expected.name
    && sameInteger(job.data?.migration_system_retry_id, expected.data.migration_system_retry_id)
    && jobEpoch != null
    && expectedEpoch != null
    && jobEpoch === expectedEpoch
    && sameInteger(job.data?.candidate_id, expected.data.candidate_id)
    && sameInteger(job.data?.dispatch_generation, expected.data.dispatch_generation)
    && sameText(job.data?.run_id, expected.data.run_id)
    && sameText(job.data?.dispatch_batch_id, expected.data.dispatch_batch_id)
    && channelIds.length === 1
    && sameText(channelIds[0], expected.data.channel_ids[0]);
}

function representedContentDetailJob(job, expected) {
  return job?.name === expected.name
    && sameText(job.data?.channel_id, expected.data.channel_id)
    && sameText(job.data?.run_id, expected.data.run_id)
    && sameInteger(
      job.data?.migration_system_retry_id,
      expected.data.migration_system_retry_id,
    )
    && sameInteger(job.data?.candidate_id, expected.data.candidate_id)
    && sameInteger(job.data?.dispatch_generation, expected.data.dispatch_generation)
    && sameText(job.data?.dispatch_batch_id, expected.data.dispatch_batch_id)
    && sameText(job.data?.pipeline_cycle_id, expected.data.pipeline_cycle_id)
    && sameInteger(job.data?.content_max_age_days, expected.data.content_max_age_days)
    && contentDetailJobEpoch(job.data?.content_detail_job_epoch)
      === contentDetailJobEpoch(expected.data.content_detail_job_epoch);
}

function representedFinalizeJob(job, expected) {
  return job?.name === expected.name
    && sameText(job.data?.channel_id, expected.data.channel_id)
    && sameText(job.data?.run_id, expected.data.run_id)
    && sameInteger(
      job.data.migration_system_retry_id,
      expected.data.migration_system_retry_id,
    )
    && sameInteger(job.data?.candidate_id, expected.data.candidate_id)
    && sameInteger(job.data?.dispatch_generation, expected.data.dispatch_generation)
    && sameText(job.data?.dispatch_batch_id, expected.data.dispatch_batch_id)
    && sameText(job.data?.pipeline_cycle_id, expected.data.pipeline_cycle_id)
    && sameText(job.data?.source_revision, expected.data.source_revision);
}

export function migrationSystemRetryAgentJobFence(job) {
  if (job?.data?.migration_system_retry_id == null) return null;
  const channelIds = Array.isArray(job?.data?.channel_ids)
    ? job.data.channel_ids.map(text).filter(Boolean)
    : [];
  const fence = {
    systemRetryId: positiveInteger(job.data.migration_system_retry_id),
    candidateId: positiveInteger(job.data.candidate_id),
    dispatchGeneration: positiveInteger(job.data.dispatch_generation),
    dispatchBatchId: text(job.data.dispatch_batch_id),
    runId: text(job.data.run_id),
    channelId: channelIds.length === 1 ? channelIds[0] : null,
    jobId: text(job.id),
    jobAttempt: positiveInteger(job.attemptsStarted),
    jobEpoch: recoveryAgentJobEpoch(job.data.recovery_agent_job_epoch),
  };
  if (
    job?.name !== "agent-profile-batch"
    || Object.values(fence).some((value) => value == null)
  ) {
    throw new TypeError("Migration system retry Agent Job identity is incomplete");
  }
  return Object.freeze(fence);
}

function requiredPostgresClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

function recoveryAgentFenceParams(fence) {
  return [
    fence.systemRetryId,
    fence.candidateId,
    fence.dispatchGeneration,
    fence.dispatchBatchId,
    fence.runId,
    fence.channelId,
    fence.jobId,
    fence.jobAttempt,
    fence.jobEpoch,
  ];
}

async function lockMigrationSystemRetryTuple(client, fence, {
  requireAgentEpoch = false,
  requireAgentOwnership = false,
  channelStatuses = ["active"],
} = {}) {
  const candidate = await client.query(
    `/* migration-system-retry-lock:candidate */
     SELECT candidate.candidate_id,candidate.channel_id
     FROM crawler.channel_candidates candidate
     WHERE candidate.candidate_id=$1
       AND candidate.channel_id=$2
       AND candidate.dispatch_batch_id=$3
       AND candidate.snapshot_dispatch_generation=$4
       AND candidate.status='accepted'
       AND candidate.snapshot_active_job_id IS NULL
       AND candidate.snapshot_active_job_attempt IS NULL
     ORDER BY candidate.candidate_id
     FOR UPDATE OF candidate`,
    [fence.candidateId, fence.channelId, fence.dispatchBatchId, fence.dispatchGeneration],
  );
  if (candidate.rowCount !== 1) return null;

  const retryConditions = [];
  const retryParams = [
    fence.systemRetryId,
    fence.candidateId,
    fence.dispatchGeneration,
    fence.dispatchBatchId,
    fence.runId,
  ];
  if (requireAgentEpoch) {
    retryParams.push(fence.jobEpoch);
    retryConditions.push(`retry.recovery_agent_job_epoch=$${retryParams.length}`);
  }
  if (requireAgentOwnership) {
    retryParams.push(fence.jobId);
    retryConditions.push(`retry.recovery_agent_active_job_id=$${retryParams.length}`);
    retryParams.push(fence.jobAttempt);
    retryConditions.push(`retry.recovery_agent_active_job_attempt=$${retryParams.length}`);
  }
  const retry = await client.query(
    `/* migration-system-retry-lock:retry */
     SELECT retry.system_retry_id,retry.recovery_agent_job_epoch,
            retry.recovery_agent_active_job_id,retry.recovery_agent_active_job_attempt
     FROM crawler.migration_system_retry_items retry
     WHERE retry.system_retry_id=$1
       AND retry.status='dispatched'
       AND retry.candidate_id=$2
       AND retry.retry_dispatch_generation=$3
       AND retry.failed_dispatch_batch_id=$4
       AND retry.recovery_run_id=$5
       ${retryConditions.map((condition) => `AND ${condition}`).join("\n       ")}
     ORDER BY retry.system_retry_id
     FOR UPDATE OF retry`,
    retryParams,
  );
  if (retry.rowCount !== 1) return null;

  const run = await client.query(
    `/* migration-system-retry-lock:run */
     SELECT run.run_id,run.channel_id,run.candidate_id
     FROM crawler.channel_runs run
     WHERE run.run_id=$1
       AND run.channel_id=$2
       AND run.candidate_id=$3
       AND COALESCE(run.result_json->>'dispatch_batch_id',
                    run.result_json->>'pipeline_cycle_id')=$4
     ORDER BY run.run_id
     FOR UPDATE OF run`,
    [fence.runId, fence.channelId, fence.candidateId, fence.dispatchBatchId],
  );
  if (run.rowCount !== 1) return null;

  const channel = await client.query(
    `/* migration-system-retry-lock:channel */
     SELECT channel.channel_id,channel.latest_run_id,channel.status
     FROM crawler.channels channel
     WHERE channel.channel_id=$1
       AND channel.latest_run_id=$2
       AND channel.status=ANY($3::text[])
     ORDER BY channel.channel_id
     FOR UPDATE OF channel`,
    [fence.channelId, fence.runId, channelStatuses],
  );
  if (channel.rowCount !== 1) return null;
  return Object.freeze({
    candidate: candidate.rows[0],
    retry: retry.rows[0],
    run: run.rows[0],
    channel: channel.rows[0],
  });
}

export async function claimMigrationSystemRetryAgentJobFence(clientValue, fence) {
  const client = requiredPostgresClient(clientValue);
  if (!fence) return true;
  const claimed = await client.query(
    `UPDATE crawler.migration_system_retry_items retry
     SET recovery_agent_active_job_id=$7,recovery_agent_active_job_attempt=$8,
         updated_at=now()
     FROM crawler.channel_candidates candidate,
          crawler.channels channel,
          crawler.channel_runs run
     WHERE retry.system_retry_id=$1
       AND retry.status='dispatched'
       AND retry.candidate_id=$2
       AND retry.retry_dispatch_generation=$3
       AND retry.failed_dispatch_batch_id=$4
       AND retry.recovery_run_id=$5
       AND retry.recovery_agent_job_epoch=$9
       AND candidate.candidate_id=retry.candidate_id
       AND candidate.dispatch_batch_id=retry.failed_dispatch_batch_id
       AND candidate.snapshot_dispatch_generation=retry.retry_dispatch_generation
       AND candidate.status='accepted'
       AND candidate.snapshot_active_job_id IS NULL
       AND candidate.snapshot_active_job_attempt IS NULL
       AND candidate.channel_id=$6
       AND channel.channel_id=candidate.channel_id
       AND channel.status='active'
       AND channel.latest_run_id=retry.recovery_run_id
       AND run.run_id=retry.recovery_run_id
       AND run.channel_id=channel.channel_id
       AND run.candidate_id=candidate.candidate_id
       AND COALESCE(run.result_json->>'dispatch_batch_id',
                    run.result_json->>'pipeline_cycle_id')=retry.failed_dispatch_batch_id
       AND (
         (
           retry.recovery_agent_active_job_id IS NULL
           AND retry.recovery_agent_active_job_attempt IS NULL
         )
         OR (
           retry.recovery_agent_active_job_id=$7
           AND retry.recovery_agent_active_job_attempt<=$8
         )
       )
     RETURNING retry.system_retry_id`,
    recoveryAgentFenceParams(fence),
  );
  return claimed.rowCount === 1;
}

export async function lockMigrationSystemRetryAgentJobFence(client, fence) {
  requiredPostgresClient(client);
  if (!fence) return true;
  return (await lockMigrationSystemRetryTuple(client, fence, {
    requireAgentEpoch: true,
    requireAgentOwnership: true,
  })) != null;
}

export async function prepareMigrationSystemRetryAgentJobRequeue(clientValue, fence, {
  findExistingJob,
} = {}) {
  const client = requiredPostgresClient(clientValue);
  if (!fence) throw new TypeError("Migration recovery Agent requeue Fence is required");
  if (typeof findExistingJob !== "function") {
    throw new TypeError("findExistingJob is required");
  }
  const locked = await lockMigrationSystemRetryTuple(client, fence, {
    requireAgentEpoch: true,
  });
  const ownership = locked?.retry ?? null;
  if (!ownership) {
    return Object.freeze({
      ready: false,
      cleared: false,
      existingJob: null,
      jobEpoch: null,
    });
  }
  const existingJob = await findExistingJob();
  if (existingJob) {
    return Object.freeze({
      ready: false,
      cleared: false,
      existingJob,
      jobEpoch: fence.jobEpoch,
    });
  }
  const activeJobId = text(ownership.recovery_agent_active_job_id);
  const activeJobAttempt = positiveInteger(ownership.recovery_agent_active_job_attempt);
  const unclaimedIncarnation = activeJobId == null && activeJobAttempt == null;
  const ownedIncarnation = activeJobId === fence.jobId && activeJobAttempt != null;
  if (!unclaimedIncarnation && !ownedIncarnation) {
    return Object.freeze({
      ready: false,
      cleared: false,
      existingJob: null,
      jobEpoch: fence.jobEpoch,
    });
  }
  const cleared = await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET recovery_agent_job_epoch=recovery_agent_job_epoch+1,
         recovery_agent_active_job_id=NULL,recovery_agent_active_job_attempt=NULL,
         updated_at=now()
     WHERE system_retry_id=$1
       AND recovery_agent_job_epoch=$2
       AND recovery_agent_active_job_id IS NOT DISTINCT FROM $3
       AND recovery_agent_active_job_attempt IS NOT DISTINCT FROM $4
     RETURNING system_retry_id,recovery_agent_job_epoch`,
    [fence.systemRetryId, fence.jobEpoch, activeJobId, activeJobAttempt],
  );
  return Object.freeze({
    ready: cleared.rowCount === 1,
    cleared: cleared.rowCount === 1,
    existingJob: null,
    jobEpoch: cleared.rowCount === 1
      ? Number(cleared.rows[0].recovery_agent_job_epoch)
      : null,
  });
}

export function migrationSystemRetryFinalizeJobFence(job) {
  if (job?.data?.migration_system_retry_id == null) return null;
  const fence = {
    systemRetryId: positiveInteger(job.data.migration_system_retry_id),
    candidateId: positiveInteger(job.data.candidate_id),
    dispatchGeneration: positiveInteger(job.data.dispatch_generation),
    dispatchBatchId: text(job.data.dispatch_batch_id),
    runId: text(job.data.run_id),
    channelId: text(job.data.channel_id),
    sourceRevision: text(job.data.source_revision),
  };
  if (
    job?.name !== "finalize-channel"
    || Object.values(fence).some((value) => value == null)
  ) {
    throw new TypeError("Migration system retry Finalize Job identity is incomplete");
  }
  return Object.freeze(fence);
}

export async function lockMigrationSystemRetryFinalizeJobFence(client, fence) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  if (!fence) return true;
  const tuple = await lockMigrationSystemRetryTuple(client, fence, {
    channelStatuses: ["active", "dormant"],
  });
  if (!tuple) return false;
  const locked = await client.query(
    `SELECT retry.system_retry_id,retry.candidate_id,
            retry.retry_dispatch_generation,retry.failed_dispatch_batch_id,
            retry.recovery_run_id,
            channel.channel_id,channel.latest_run_id,
            channel.status AS channel_status,channel.agent_status,
            channel.updated_at AS channel_updated_at,
            run.detail_status,run.expected_content_count,
            run.result_json->'final_repair' AS run_final_repair,
            run.result_json->>'pipeline_cycle_id' AS pipeline_cycle_id,
            (SELECT count(*)::int
             FROM crawler.content_candidates source_candidate
             WHERE source_candidate.run_id=run.run_id) AS candidate_count,
            (SELECT max(source_candidate.updated_at)
             FROM crawler.content_candidates source_candidate
             WHERE source_candidate.run_id=run.run_id) AS candidate_updated_at,
            (SELECT count(*)::int
             FROM crawler.contents content
             WHERE content.channel_id=channel.channel_id AND content.run_id=run.run_id)
              AS content_count,
            (SELECT max(COALESCE(content.last_enriched_at,content.last_seen_at))
             FROM crawler.contents content
             WHERE content.channel_id=channel.channel_id AND content.run_id=run.run_id)
              AS content_updated_at,
            (SELECT agent.updated_at
             FROM crawler.agent_profiles agent
             WHERE agent.channel_id=channel.channel_id
               AND agent.agent_mode='basic' AND agent.status='success'
             LIMIT 1) AS agent_updated_at
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_candidates candidate
       ON candidate.candidate_id=retry.candidate_id
     JOIN crawler.channels channel ON channel.channel_id=candidate.channel_id
     JOIN crawler.channel_runs run ON run.run_id=retry.recovery_run_id
     WHERE retry.system_retry_id=$1 AND retry.status='dispatched'
       AND retry.candidate_id=$2 AND retry.retry_dispatch_generation=$3
       AND retry.failed_dispatch_batch_id=$4 AND retry.recovery_run_id=$5
       AND candidate.dispatch_batch_id=retry.failed_dispatch_batch_id
       AND candidate.snapshot_dispatch_generation=retry.retry_dispatch_generation
       AND candidate.status='accepted'
       AND candidate.snapshot_active_job_id IS NULL
       AND candidate.snapshot_active_job_attempt IS NULL
       AND channel.channel_id=$6 AND channel.latest_run_id=retry.recovery_run_id
       AND channel.status IN ('active','dormant')
       AND run.channel_id=channel.channel_id AND run.candidate_id=candidate.candidate_id
       AND run.detail_status='done'
       AND COALESCE(run.result_json->>'dispatch_batch_id',
                    run.result_json->>'pipeline_cycle_id')=retry.failed_dispatch_batch_id
       AND (
         channel.status='dormant'
         OR (
           channel.agent_status='done'
           AND EXISTS (
             SELECT 1 FROM crawler.agent_profiles current_agent
             WHERE current_agent.channel_id=channel.channel_id
               AND current_agent.agent_mode='basic' AND current_agent.status='success'
           )
         )
       )`,
    [
      fence.systemRetryId,
      fence.candidateId,
      fence.dispatchGeneration,
      fence.dispatchBatchId,
      fence.runId,
      fence.channelId,
    ],
  );
  if (locked.rowCount !== 1) return false;
  return finalizeDispatchRevision(finalizeDispatchState(locked.rows[0]))
    === fence.sourceRevision;
}

export async function lockGenericFinalizeAgainstMigrationSystemRetry(client, {
  channelId,
  runId,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const normalizedChannelId = text(channelId);
  const normalizedRunId = text(runId);
  if (!normalizedChannelId || !normalizedRunId) {
    throw new TypeError("generic Finalize channelId and runId are required");
  }
  const candidateRows = await client.query(
    `SELECT candidate.candidate_id,candidate.dispatch_batch_id,
            candidate.snapshot_dispatch_generation
     FROM crawler.channel_runs run
     JOIN crawler.channel_candidates candidate
       ON candidate.candidate_id=run.candidate_id
     WHERE run.run_id=$2 AND run.channel_id=$1
     FOR UPDATE OF candidate`,
    [normalizedChannelId, normalizedRunId],
  );
  const candidate = candidateRows.rows[0] ?? null;
  if (!candidate) return true;
  const generation = positiveInteger(candidate.snapshot_dispatch_generation);
  const dispatchBatchId = text(candidate.dispatch_batch_id);
  if (generation == null || dispatchBatchId == null) return true;
  const blockingRetry = await client.query(
    `SELECT retry.system_retry_id
     FROM crawler.migration_system_retry_items retry
     WHERE retry.candidate_id=$1
       AND (retry.failed_dispatch_batch_id=$2 OR retry.failed_dispatch_batch_id IS NULL)
       AND retry.status IN ('retrying','pending','dispatched')
       AND (
         (retry.status IN ('retrying','pending')
          AND retry.failed_dispatch_generation=$3)
         OR (retry.status='dispatched' AND retry.retry_dispatch_generation=$3)
       )
     ORDER BY retry.system_retry_id
     LIMIT 1
     FOR UPDATE OF retry`,
    [Number(candidate.candidate_id), dispatchBatchId, generation],
  );
  return blockingRetry.rowCount === 0;
}

function terminalFinalizedOutcome(row) {
  return exactCandidateFence(row)
    && row.candidate_status === "accepted"
    && row.snapshot_active_job_id == null
    && row.snapshot_active_job_attempt == null
    && exactRecoveryRunFence(row)
    && row.run_status === "done"
    && row.run_detail_status === "done"
    && SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES.includes(row.publication_finalized_status)
    && row.publication_finalized_at != null
    && text(row.finalized_run_id) === text(row.run_id)
    && row.finalized_status === row.publication_finalized_status
    && finalizedAfterRequiredSources(row);
}

export function automaticCompletedMigrationRecoveryEnabled(scheduler) {
  return String(scheduler?.status ?? "") === "stopped"
    && String(scheduler?.stop_reason ?? "") === "pipeline_complete";
}

export class MigrationSystemRetryRecoveryReconciler {
  constructor({
    query,
    withTransaction,
    queues,
    jobIdFactory = safeJobId,
  } = {}) {
    if (typeof query !== "function") throw new TypeError("query is required");
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    if (!queues || typeof queues !== "object") throw new TypeError("queues are required");
    if (typeof jobIdFactory !== "function") throw new TypeError("jobIdFactory is required");
    this.query = query;
    this.withTransaction = withTransaction;
    this.queues = queues;
    this.jobIdFactory = jobIdFactory;
    this.activeScanCursor = "0";
    this.legacyScanCursor = "0";
    this.scanPatternOffset = 0;
  }

  async loadRecoveries(limit) {
    const loadClass = (active, cursor) => this.query(
      `SELECT retry.system_retry_id,retry.migration_intent_id,retry.candidate_id,
              retry.failed_dispatch_batch_id,retry.failed_dispatch_generation,
              retry.failed_job_id,retry.failed_job_attempt,retry.status,
              retry.retry_dispatch_generation,retry.resolution,retry.recovery_run_id,
              retry.failure_evidence,
              retry.recovery_agent_job_epoch,
              candidate.dispatch_batch_id AS candidate_dispatch_batch_id,
              candidate.pipeline_cycle_id AS candidate_pipeline_cycle_id,
              candidate.channel_id AS candidate_channel_id,
              candidate.status AS candidate_status,candidate.snapshot_dispatch_generation,
              candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
              channel.status AS channel_status,channel.ready_for_agent,
              channel.agent_status,channel.agent_next_retry_at,
              agent.status AS agent_profile_status,
              agent.updated_at AS agent_updated_at,
              channel.latest_run_id AS channel_latest_run_id,
              channel.updated_at AS channel_updated_at,
              run.run_id,run.channel_id AS run_channel_id,run.candidate_id AS run_candidate_id,
              run.status AS run_status,run.detail_status AS run_detail_status,
              run.expected_content_count,run.detail_job_epoch AS run_content_detail_job_epoch,
              run.result_json->'final_repair' AS run_final_repair,
              run.result_json->>'pipeline_cycle_id' AS run_pipeline_cycle_id,
              run.result_json->>'content_max_age_days' AS run_content_max_age_days,
              COALESCE(run.result_json->>'dispatch_batch_id',
                       run.result_json->>'pipeline_cycle_id') AS run_dispatch_batch_id,
              (SELECT count(*)::int FROM crawler.content_candidates content_candidate
               WHERE content_candidate.run_id=run.run_id) AS candidate_count,
              (SELECT max(content_candidate.updated_at)
               FROM crawler.content_candidates content_candidate
               WHERE content_candidate.run_id=run.run_id) AS candidate_updated_at,
              (SELECT count(*)::int
               FROM crawler.content_candidates content_candidate
               WHERE content_candidate.run_id=run.run_id
                 AND content_candidate.detail_status='api_pending'
                 AND content_candidate.api_status IN ('pending','queued','running','failed'))
                AS api_open_count,
              (SELECT count(*)::int FROM crawler.contents content
               WHERE content.channel_id=channel.channel_id AND content.run_id=run.run_id)
                AS content_count,
              (SELECT max(COALESCE(content.last_enriched_at,content.last_seen_at))
               FROM crawler.contents content
               WHERE content.channel_id=channel.channel_id AND content.run_id=run.run_id)
                AS content_updated_at,
              run.publication_finalized_status,run.publication_finalized_at,
              finalized.run_id AS finalized_run_id,finalized.status AS finalized_status,
              finalized.updated_at AS finalized_updated_at
       FROM crawler.migration_system_retry_items retry
       JOIN crawler.channel_candidates candidate
         ON candidate.candidate_id=retry.candidate_id
       LEFT JOIN crawler.channels channel
         ON channel.channel_id=candidate.channel_id
       LEFT JOIN crawler.channel_runs run
         ON run.run_id=COALESCE(retry.recovery_run_id,channel.latest_run_id)
       LEFT JOIN crawler.finalized_profiles finalized
         ON finalized.channel_id=channel.channel_id
       LEFT JOIN crawler.agent_profiles agent
         ON agent.channel_id=channel.channel_id AND agent.agent_mode='basic'
       WHERE (
           $4::boolean
           AND retry.status=ANY($1::text[])
         ) OR (
           NOT $4::boolean
           AND retry.status='resolved' AND retry.resolution='job_completed'
         )
       ORDER BY CASE WHEN retry.system_retry_id>$3::bigint THEN 0 ELSE 1 END,
                retry.system_retry_id
       LIMIT $2`,
      [ACTIVE_RETRY_STATUSES, limit, cursor, active],
    );
    const [activeRows, legacyRows] = await Promise.all([
      loadClass(true, this.activeScanCursor),
      loadClass(false, this.legacyScanCursor),
    ]);
    const available = {
      active: activeRows.rows,
      legacy: legacyRows.rows,
    };
    const indexes = { active: 0, legacy: 0 };
    const selected = [];
    while (selected.length < limit) {
      const preferred = RECOVERY_SCAN_PATTERN[
        this.scanPatternOffset % RECOVERY_SCAN_PATTERN.length
      ];
      this.scanPatternOffset = (this.scanPatternOffset + 1) % RECOVERY_SCAN_PATTERN.length;
      const alternate = preferred === "active" ? "legacy" : "active";
      const kind = indexes[preferred] < available[preferred].length
        ? preferred
        : indexes[alternate] < available[alternate].length ? alternate : null;
      if (kind == null) break;
      selected.push(available[kind][indexes[kind]]);
      indexes[kind] += 1;
    }
    if (indexes.active > 0) {
      this.activeScanCursor = String(available.active[indexes.active - 1].system_retry_id);
    }
    if (indexes.legacy > 0) {
      this.legacyScanCursor = String(available.legacy[indexes.legacy - 1].system_retry_id);
    }
    return selected;
  }

  async reopenLegacyJobCompletion(row) {
    if (row.status !== "resolved" || row.resolution !== "job_completed") return false;
    if (terminalBusinessOutcome(row) || terminalFinalizedOutcome(row)) return false;
    const generation = expectedGeneration(row);
    if (generation == null || !exactCandidateFence(row)) return false;
    const reopened = await this.withTransaction((client) => client.query(
      `UPDATE crawler.migration_system_retry_items retry
       SET status=CASE
             WHEN retry.retry_dispatch_generation IS NULL THEN 'retrying'
             ELSE 'dispatched'
           END,
           resolution=NULL,resolved_at=NULL,updated_at=now()
       FROM crawler.channel_candidates candidate
       WHERE retry.system_retry_id=$1
         AND retry.status='resolved' AND retry.resolution='job_completed'
         AND candidate.candidate_id=retry.candidate_id
         AND candidate.candidate_id=$2
         AND candidate.dispatch_batch_id=retry.failed_dispatch_batch_id
         AND candidate.snapshot_dispatch_generation=$3
         AND NOT EXISTS (
           SELECT 1
           FROM crawler.migration_system_retry_items active
           WHERE active.candidate_id=retry.candidate_id
             AND active.system_retry_id<>retry.system_retry_id
             AND active.status IN ('retrying','pending','dispatched')
         )
       RETURNING retry.status`,
      [Number(row.system_retry_id), Number(row.candidate_id), generation],
    ));
    if (reopened.rowCount !== 1) return false;
    row.status = reopened.rows[0].status;
    row.resolution = null;
    return true;
  }

  async normalizeLegacyStaleCompletion(row) {
    if (row.status !== "resolved" || row.resolution !== "job_completed") return false;
    const generation = expectedGeneration(row);
    const normalized = await this.withTransaction((client) => client.query(
      `UPDATE crawler.migration_system_retry_items retry
       SET resolution=CASE
             WHEN candidate.dispatch_batch_id IS DISTINCT FROM retry.failed_dispatch_batch_id
               OR ($3::bigint IS NOT NULL AND candidate.snapshot_dispatch_generation>$3)
               THEN 'recovery_fence_superseded'
             WHEN EXISTS (
               SELECT 1
               FROM crawler.migration_system_retry_items active
               WHERE active.candidate_id=retry.candidate_id
                 AND active.system_retry_id<>retry.system_retry_id
                 AND active.status IN ('retrying','pending','dispatched')
             ) THEN 'recovery_superseded_by_active_retry'
             ELSE 'recovery_stale_manual_review'
           END,
           resolved_at=COALESCE(retry.resolved_at,now()),updated_at=now()
       FROM crawler.channel_candidates candidate
       WHERE retry.system_retry_id=$1
         AND retry.status='resolved' AND retry.resolution='job_completed'
         AND retry.candidate_id=$2 AND candidate.candidate_id=retry.candidate_id
         AND (
           candidate.dispatch_batch_id IS DISTINCT FROM retry.failed_dispatch_batch_id
           OR candidate.snapshot_dispatch_generation IS DISTINCT FROM $3::bigint
           OR EXISTS (
             SELECT 1
             FROM crawler.migration_system_retry_items active
             WHERE active.candidate_id=retry.candidate_id
               AND active.system_retry_id<>retry.system_retry_id
               AND active.status IN ('retrying','pending','dispatched')
           )
         )
       RETURNING retry.system_retry_id`,
      [Number(row.system_retry_id), Number(row.candidate_id), generation],
    ));
    return normalized.rowCount === 1;
  }

  async resolveSupersededActiveFence(row) {
    const generation = expectedGeneration(row);
    const resolved = await this.withTransaction((client) => client.query(
      `UPDATE crawler.migration_system_retry_items retry
       SET status='resolved',resolution='recovery_fence_superseded',
           resolved_at=COALESCE(retry.resolved_at,now()),updated_at=now()
       FROM crawler.channel_candidates candidate
       WHERE retry.system_retry_id=$1 AND retry.status=ANY($4::text[])
         AND retry.candidate_id=$2 AND candidate.candidate_id=retry.candidate_id
         AND (
           candidate.dispatch_batch_id IS DISTINCT FROM retry.failed_dispatch_batch_id
           OR ($3::bigint IS NOT NULL AND candidate.snapshot_dispatch_generation>$3)
         )
       RETURNING retry.system_retry_id`,
      [Number(row.system_retry_id), Number(row.candidate_id), generation, ACTIVE_RETRY_STATUSES],
    ));
    return resolved.rowCount === 1;
  }

  async pinRecoveryRun(row) {
    const runId = text(row.run_id);
    const generation = expectedGeneration(row);
    if (!runId || generation == null) return false;
    const pinned = await this.withTransaction((client) => client.query(
      `UPDATE crawler.migration_system_retry_items retry
       SET recovery_run_id=COALESCE(retry.recovery_run_id,$4),updated_at=now()
       FROM crawler.channel_candidates candidate
       JOIN crawler.channels channel ON channel.channel_id=candidate.channel_id
       JOIN crawler.channel_runs run ON run.run_id=$4
       WHERE retry.system_retry_id=$1 AND retry.status=ANY($5::text[])
         AND retry.candidate_id=$2 AND candidate.candidate_id=retry.candidate_id
         AND candidate.dispatch_batch_id=retry.failed_dispatch_batch_id
         AND candidate.snapshot_dispatch_generation=$3
         AND candidate.status='accepted'
         AND candidate.snapshot_active_job_id IS NULL
         AND candidate.snapshot_active_job_attempt IS NULL
         AND channel.latest_run_id=run.run_id
         AND run.channel_id=candidate.channel_id AND run.candidate_id=candidate.candidate_id
         AND COALESCE(run.result_json->>'dispatch_batch_id',
                      run.result_json->>'pipeline_cycle_id')=retry.failed_dispatch_batch_id
         AND (retry.recovery_run_id IS NULL OR retry.recovery_run_id=run.run_id)
       RETURNING retry.recovery_run_id`,
      [Number(row.system_retry_id), Number(row.candidate_id), generation, runId, ACTIVE_RETRY_STATUSES],
    ));
    if (pinned.rowCount !== 1) return false;
    row.recovery_run_id = pinned.rows[0].recovery_run_id;
    return true;
  }

  async resolveBusinessOutcome(row, { legacy = false } = {}) {
    const generation = expectedGeneration(row);
    const resolved = await this.withTransaction((client) => client.query(
      `UPDATE crawler.migration_system_retry_items retry
       SET status='resolved',resolution='recovery_terminal_business_outcome',
           resolved_at=COALESCE(resolved_at,now()),updated_at=now()
       FROM crawler.channel_candidates candidate
       WHERE retry.system_retry_id=$1
         AND (
           retry.status=ANY($4::text[])
           OR ($5::boolean AND retry.status='resolved' AND retry.resolution='job_completed')
         )
         AND retry.candidate_id=$2 AND candidate.candidate_id=retry.candidate_id
         AND candidate.dispatch_batch_id=retry.failed_dispatch_batch_id
         AND candidate.snapshot_dispatch_generation=$3
         AND candidate.snapshot_active_job_id IS NULL
         AND candidate.snapshot_active_job_attempt IS NULL
         AND candidate.status IN ('rejected','existing')
       RETURNING retry.system_retry_id`,
      [
        Number(row.system_retry_id),
        Number(row.candidate_id),
        generation,
        ACTIVE_RETRY_STATUSES,
        legacy === true,
      ],
    ));
    return resolved.rowCount === 1;
  }

  async resolveFinalizedOutcome(row, { legacy = false } = {}) {
    const generation = expectedGeneration(row);
    const resolved = await this.withTransaction((client) => client.query(
      `UPDATE crawler.migration_system_retry_items retry
       SET status='resolved',resolution='recovery_finalized',
           resolved_at=COALESCE(resolved_at,now()),updated_at=now()
       FROM crawler.channel_candidates candidate,
            crawler.channels channel,
            crawler.channel_runs run,
            crawler.finalized_profiles finalized
       WHERE retry.system_retry_id=$1
         AND (
           retry.status=ANY($5::text[])
           OR ($6::boolean AND retry.status='resolved' AND retry.resolution='job_completed')
         )
         AND retry.candidate_id=$2 AND candidate.candidate_id=retry.candidate_id
         AND candidate.dispatch_batch_id=retry.failed_dispatch_batch_id
         AND candidate.snapshot_dispatch_generation=$3
         AND candidate.status='accepted'
         AND candidate.snapshot_active_job_id IS NULL
         AND candidate.snapshot_active_job_attempt IS NULL
         AND retry.recovery_run_id=$4
         AND channel.channel_id=candidate.channel_id
         AND channel.latest_run_id=retry.recovery_run_id
         AND run.run_id=retry.recovery_run_id
         AND run.channel_id=channel.channel_id AND run.candidate_id=candidate.candidate_id
         AND COALESCE(run.result_json->>'dispatch_batch_id',
                      run.result_json->>'pipeline_cycle_id')=retry.failed_dispatch_batch_id
         AND run.status='done' AND run.detail_status='done'
         AND run.publication_finalized_status=ANY($7::text[])
         AND run.publication_finalized_at IS NOT NULL
         AND finalized.channel_id=channel.channel_id
         AND finalized.run_id=run.run_id
         AND finalized.status=run.publication_finalized_status
         AND (
           (
             channel.status='dormant'
             AND run.publication_finalized_status='ready_partial'
           )
           OR (
             channel.status='active'
             AND channel.agent_status='done'
             AND EXISTS (
               SELECT 1
               FROM crawler.agent_profiles agent
               WHERE agent.channel_id=channel.channel_id
                 AND agent.agent_mode='basic' AND agent.status='success'
                 AND finalized.updated_at>=greatest(
                   channel.updated_at,
                   agent.updated_at,
                   COALESCE(
                     (
                       SELECT max(source_candidate.updated_at)
                       FROM crawler.content_candidates source_candidate
                       WHERE source_candidate.run_id=run.run_id
                     ),
                     'epoch'::timestamptz
                   ),
                   COALESCE(
                     (
                       SELECT max(COALESCE(content.last_enriched_at,content.last_seen_at))
                       FROM crawler.contents content
                       WHERE content.channel_id=channel.channel_id
                         AND content.run_id=run.run_id
                     ),
                     'epoch'::timestamptz
                   )
                 )
             )
           )
         )
       RETURNING retry.system_retry_id`,
      [
        Number(row.system_retry_id),
        Number(row.candidate_id),
        generation,
        text(row.recovery_run_id),
        ACTIVE_RETRY_STATUSES,
        legacy === true,
        SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
      ],
    ));
    return resolved.rowCount === 1;
  }

  async ensureQueueJob(queueName, expected, matches, {
    adoptEquivalent = false,
    createJob = null,
    allowTerminalRequeue = null,
  } = {}) {
    const queue = this.queues[queueName];
    if (!queue || typeof queue.getJob !== "function" || typeof queue.add !== "function") {
      throw new TypeError(`Migration system recovery queue is required: ${queueName}`);
    }
    if (createJob != null && typeof createJob !== "function") {
      throw new TypeError("createJob must be a function");
    }
    if (allowTerminalRequeue != null && typeof allowTerminalRequeue !== "function") {
      throw new TypeError("allowTerminalRequeue must be a function");
    }
    if (
      queueName === queuesByRole.agentBatch
      && positiveInteger(expected?.data?.migration_system_retry_id) != null
      && createJob == null
    ) {
      throw new TypeError("Migration recovery Agent Jobs require an incarnation allocator");
    }
    let existing = await queue.getJob(expected.id);
    let terminalRequeued = false;
    if (!existing && adoptEquivalent && typeof queue.getJobs === "function") {
      const equivalent = await queue.getJobs(REPRESENTED_JOB_STATE_LIST, 0, 9999, true);
      existing = equivalent.find((job) => matches(job, expected)) ?? null;
    }
    for (let inspection = 0; existing && inspection < 3; inspection += 1) {
      const represented = matches(existing, expected);
      const state = typeof existing.getState === "function"
        ? await existing.getState()
        : "unknown";
      if (!represented && !["completed", "failed"].includes(state)) {
        return { created: false, represented: false, conflict: true, state, job: existing };
      }
      if (represented && REPRESENTED_JOB_STATES.has(state)) {
        return { created: false, represented: true, conflict: false, state, job: existing };
      }
      if (["completed", "failed"].includes(state)) {
        const terminalDisposition = allowTerminalRequeue
          ? await allowTerminalRequeue({
              expected,
              job: existing,
              represented,
              state,
            })
          : true;
        const terminalAllowed = terminalDisposition === true
          || terminalDisposition?.allow === true;
        if (!terminalAllowed) {
          return {
            created: false,
            represented,
            conflict: false,
            state,
            terminalBlocked: true,
            terminalSettled: terminalDisposition?.settled === true,
            job: existing,
          };
        }
        try {
          await existing.remove();
          existing = null;
          terminalRequeued = true;
        } catch {
          existing = await queue.getJob(expected.id);
          continue;
        }
      } else if (state !== "unknown") {
        return { created: false, represented: false, conflict: true, state, job: existing };
      } else {
        existing = await queue.getJob(expected.id);
      }
    }
    if (existing) {
      return { created: false, represented: false, conflict: true, job: existing };
    }
    const created = createJob
      ? await createJob({ queue, expected, terminalRequeued })
      : await queue.add(expected.name, expected.data, { jobId: expected.id });
    const job = created?.createdJob ?? created;
    const createdExpected = created?.createdExpected ?? expected;
    if (!job || !matches(job, createdExpected)) {
      return { created: false, represented: false, conflict: true, job };
    }
    return {
      created: true,
      represented: true,
      conflict: false,
      terminalRequeued,
      job,
    };
  }

  async createContentDetailRecoveryJob(expected, { advanceEpoch = false } = {}) {
    const queue = this.queues[queuesByRole.contentDetail];
    const fence = contentDetailExecutionFence({
      id: expected.id,
      name: expected.name,
      attemptsStarted: 1,
      data: expected.data,
    });
    const prepared = await this.withTransaction(async (client) => {
      const prepared = await prepareContentDetailExecutionRequeue(client, fence, {
        findExistingJob: () => queue.getJob(expected.id),
        advanceEpoch,
      });
      return prepared;
    });
    if (prepared?.jobEpoch == null) return null;
    const createdExpected = this.contentDetailJobFromData(expected.data, prepared.jobEpoch);
    if (prepared.existingJob) {
      return { createdJob: prepared.existingJob, createdExpected };
    }
    if (!prepared.ready) return null;
    const createdJob = await queue.add(
      createdExpected.name,
      createdExpected.data,
      { jobId: createdExpected.id },
    );
    return { createdJob, createdExpected };
  }

  async settleUnclassifiedContentDetailTerminalJob(job) {
    return this.withTransaction(async (client) => {
      const options = {
        failureDecision: {
          kind: "retryable_system_failure",
          retry_mode: "system_retry",
          terminal: true,
          evidence: {
            failure_type: "retryable_system_failure",
            retryable: true,
            category: "worker",
            code: "CONTENT_DETAIL_TERMINAL_EVIDENCE_MISSING",
          },
        },
        errorMessage: text(job?.failedReason) ?? "terminal Content Detail evidence is missing",
      };
      let settled = await settleContentDetailRecoveryTerminalFailure(client, job, options);
      if (settled.action !== "stale") return settled.action === "resolved";
      const fence = contentDetailExecutionFence(job);
      if (!(await claimContentDetailExecution(client, fence))) return false;
      settled = await settleContentDetailRecoveryTerminalFailure(client, job, options);
      return settled.action === "resolved";
    });
  }

  async createAgentRecoveryJob(expected) {
    const queue = this.queues[queuesByRole.agentBatch];
    const fence = migrationSystemRetryAgentJobFence({
      id: expected.id,
      name: expected.name,
      attemptsStarted: 1,
      data: expected.data,
    });
    return this.withTransaction(async (client) => {
      const prepared = await prepareMigrationSystemRetryAgentJobRequeue(client, fence, {
        findExistingJob: () => queue.getJob(expected.id),
      });
      if (prepared.existingJob) {
        return { createdJob: prepared.existingJob, createdExpected: expected };
      }
      if (!prepared.ready) return null;
      const createdExpected = prepared.jobEpoch === fence.jobEpoch
        ? expected
        : this.agentJobFromData(expected.data, prepared.jobEpoch);
      const createdJob = await queue.add(
        createdExpected.name,
        createdExpected.data,
        { jobId: createdExpected.id },
      );
      return { createdJob, createdExpected };
    });
  }

  agentJobFromData(dataValue, jobEpochValue) {
    const data = { ...dataValue };
    const jobEpoch = nonNegativeInteger(jobEpochValue);
    const retryId = positiveInteger(data.migration_system_retry_id);
    const generation = positiveInteger(data.dispatch_generation);
    const runId = text(data.run_id);
    if (jobEpoch == null || retryId == null || generation == null || runId == null) {
      throw new TypeError("Migration recovery Agent Job epoch identity is incomplete");
    }
    data.recovery_agent_job_epoch = jobEpoch;
    return {
      id: this.jobIdFactory(
        "migration-system-retry-agent",
        retryId,
        `g${generation}`,
        runId,
        ...(jobEpoch > 0 ? [`e${jobEpoch}`] : []),
      ),
      name: "agent-profile-batch",
      data,
    };
  }

  agentJob(row) {
    const retryId = Number(row.system_retry_id);
    const generation = expectedGeneration(row);
    const runId = text(row.recovery_run_id);
    const batchId = text(row.failed_dispatch_batch_id);
    return this.agentJobFromData({
        batch_id: `migration-system-retry:${retryId}:g${generation}:${runId}`,
        channel_ids: [text(row.candidate_channel_id)],
        agent_mode: "basic",
        migration_system_retry_id: retryId,
        candidate_id: Number(row.candidate_id),
        dispatch_generation: generation,
        pipeline_cycle_id: batchId,
        dispatch_batch_id: batchId,
        run_id: runId,
      }, row.recovery_agent_job_epoch);
  }

  contentDetailJobFromData(dataValue, jobEpochValue) {
    const data = { ...dataValue };
    const runId = text(data.run_id);
    const jobEpoch = contentDetailJobEpoch(jobEpochValue);
    if (runId == null || jobEpoch == null) {
      throw new TypeError("Migration recovery Content Detail Job epoch identity is incomplete");
    }
    data.content_detail_job_epoch = jobEpoch;
    return {
      id: this.jobIdFactory("content-detail", runId),
      name: "content-detail-batch",
      data,
    };
  }

  contentDetailJob(row) {
    const retryId = Number(row.system_retry_id);
    const generation = expectedGeneration(row);
    const runId = text(row.recovery_run_id);
    const batchId = text(row.failed_dispatch_batch_id);
    const contentMaxAgeDays = Number(row.run_content_max_age_days);
    return this.contentDetailJobFromData(
      {
        channel_id: text(row.candidate_channel_id),
        run_id: runId,
        migration_system_retry_id: retryId,
        candidate_id: Number(row.candidate_id),
        dispatch_generation: generation,
        dispatch_batch_id: batchId,
        pipeline_cycle_id: text(row.run_pipeline_cycle_id)
          ?? batchId,
        content_max_age_days: Number.isSafeInteger(contentMaxAgeDays)
          && contentMaxAgeDays >= 0
          ? contentMaxAgeDays
          : null,
      },
      row.run_content_detail_job_epoch,
    );
  }

  finalizeJob(row) {
    const retryId = Number(row.system_retry_id);
    const generation = expectedGeneration(row);
    const runId = text(row.recovery_run_id);
    const batchId = text(row.failed_dispatch_batch_id);
    const sourceRevision = finalizeDispatchRevision(finalizeDispatchState(row));
    return {
      id: this.jobIdFactory(
        "migration-system-retry-finalize",
        retryId,
        `g${generation}`,
        runId,
        sourceRevision,
      ),
      name: "finalize-channel",
      data: {
        channel_id: text(row.candidate_channel_id),
        run_id: runId,
        reason: "migration-system-retry-recovery",
        source_revision: sourceRevision,
        migration_system_retry_id: retryId,
        candidate_id: Number(row.candidate_id),
        dispatch_generation: generation,
        pipeline_cycle_id: row.run_pipeline_cycle_id ?? batchId,
        dispatch_batch_id: batchId,
      },
    };
  }

  async reconcileAvailable({ limit = 100 } = {}) {
    const normalizedLimit = boundedLimit(limit);
    const requiredQueues = new Set();
    const dataApiSystemRetryIds = new Set();
    const dataApiRecoveryIdsByBatch = new Map();
    let scanned = 0;
    let detailEnqueued = 0;
    let agentEnqueued = 0;
    let finalizeEnqueued = 0;
    let resolved = 0;
    let stale = 0;
    let legacyReopened = 0;
    let legacyNormalized = 0;
    let queueConflicts = 0;
    let terminalJobsRequeued = 0;
    const rows = await this.loadRecoveries(normalizedLimit);
    for (const row of rows) {
      if (row.status === "resolved") {
        if (terminalBusinessOutcome(row)) {
          if (await this.resolveBusinessOutcome(row, { legacy: true })) {
            resolved += 1;
            legacyNormalized += 1;
          }
          continue;
        }
        if (terminalFinalizedOutcome(row)) {
          if (await this.resolveFinalizedOutcome(row, { legacy: true })) {
            resolved += 1;
            legacyNormalized += 1;
          }
          continue;
        }
        if (!(await this.reopenLegacyJobCompletion(row))) {
          if (await this.normalizeLegacyStaleCompletion(row)) legacyNormalized += 1;
          else stale += 1;
          continue;
        }
        legacyReopened += 1;
      }
      scanned += 1;
      if (!exactCandidateFence(row)) {
        if (await this.resolveSupersededActiveFence(row)) {
          resolved += 1;
          continue;
        }
        stale += 1;
        continue;
      }
      if (terminalBusinessOutcome(row)) {
        if (await this.resolveBusinessOutcome(row)) resolved += 1;
        continue;
      }
      if (
        ["discovered", "queued", "validating"].includes(row.candidate_status)
        || row.snapshot_active_job_id != null
        || row.snapshot_active_job_attempt != null
      ) {
        requiredQueues.add(queuesByRole.channelCrawl);
        continue;
      }
      if (row.candidate_status !== "accepted") {
        stale += 1;
        continue;
      }
      if (!(await this.pinRecoveryRun(row)) || !exactRecoveryRunFence(row)) {
        stale += 1;
        continue;
      }
      if (terminalFinalizedOutcome(row)) {
        if (await this.resolveFinalizedOutcome(row)) resolved += 1;
        continue;
      }
      if (row.run_detail_status !== "done") {
        if (Number(row.api_open_count ?? 0) > 0) {
          const dispatchBatchId = text(row.failed_dispatch_batch_id);
          if (!dispatchBatchId) {
            stale += 1;
            continue;
          }
          requiredQueues.add(queuesByRole.dataApiBatch);
          const systemRetryId = Number(row.system_retry_id);
          dataApiSystemRetryIds.add(systemRetryId);
          const scopedRetryIds = dataApiRecoveryIdsByBatch.get(dispatchBatchId) ?? new Set();
          scopedRetryIds.add(systemRetryId);
          dataApiRecoveryIdsByBatch.set(dispatchBatchId, scopedRetryIds);
        } else {
          requiredQueues.add(queuesByRole.contentDetail);
          const job = this.contentDetailJob(row);
          const ensured = await this.ensureQueueJob(
            queuesByRole.contentDetail,
            job,
            representedContentDetailJob,
            {
              createJob: ({ terminalRequeued }) => this.createContentDetailRecoveryJob(job, {
                advanceEpoch: terminalRequeued,
              }),
              allowTerminalRequeue: async ({ job: terminalJob, represented, state }) => {
                if (!represented) return true;
                if (
                  state === "failed"
                  && contentDetailRecoveryEvidence(row, job) != null
                ) return true;
                const settled = await this.settleUnclassifiedContentDetailTerminalJob(
                  terminalJob,
                );
                return { allow: false, settled };
              },
            },
          );
          if (ensured.created) detailEnqueued += 1;
          if (ensured.terminalSettled) resolved += 1;
          if (ensured.terminalRequeued) terminalJobsRequeued += 1;
          if (ensured.conflict) {
            queueConflicts += 1;
            stale += 1;
          }
        }
        continue;
      }
      if (row.channel_status === "active" && row.agent_status !== "done") {
        if (row.ready_for_agent !== true) {
          stale += 1;
          continue;
        }
        requiredQueues.add(queuesByRole.agentBatch);
        const job = this.agentJob(row);
        const ensured = await this.ensureQueueJob(
          queuesByRole.agentBatch,
          job,
            representedMigrationSystemRetryAgentJob,
          {
            createJob: () => this.createAgentRecoveryJob(job),
          },
        );
        if (ensured.created) agentEnqueued += 1;
        if (ensured.terminalRequeued) terminalJobsRequeued += 1;
        if (ensured.conflict) {
          queueConflicts += 1;
          stale += 1;
        }
        continue;
      }
      if (row.channel_status !== "active" && row.channel_status !== "dormant") {
        stale += 1;
        continue;
      }
      requiredQueues.add(queuesByRole.finalize);
      const job = this.finalizeJob(row);
      const ensured = await this.ensureQueueJob(
        queuesByRole.finalize,
        job,
        representedFinalizeJob,
      );
      if (ensured.created) finalizeEnqueued += 1;
      if (ensured.terminalRequeued) terminalJobsRequeued += 1;
      if (ensured.conflict) {
        queueConflicts += 1;
        stale += 1;
      }
    }
    const orderedQueues = QUEUE_ORDER.filter((queueName) => requiredQueues.has(queueName));
    const dataApiRecoveryScopes = [...dataApiRecoveryIdsByBatch.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([pipelineCycleId, systemRetryIds]) => Object.freeze({
        pipelineCycleId,
        migrationSystemRetryIds: Object.freeze([...systemRetryIds].sort((a, b) => a - b)),
      }));
    return Object.freeze({
      scanned,
      detailEnqueued,
      agentEnqueued,
      finalizeEnqueued,
      resolved,
      stale,
      legacyReopened,
      legacyNormalized,
      queueConflicts,
      terminalJobsRequeued,
      requiredQueues: Object.freeze(orderedQueues),
      dataApiSystemRetryIds: Object.freeze([...dataApiSystemRetryIds].sort((a, b) => a - b)),
      dataApiRecoveryScopes: Object.freeze(dataApiRecoveryScopes),
      limit: normalizedLimit,
    });
  }
}
