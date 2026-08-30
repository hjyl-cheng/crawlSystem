import { nanoid } from "nanoid";
import { createHash } from "node:crypto";
import { ensureDefaultAgentConfig, listEnabledAgentConfigs } from "./agentConfig.js";
import { automaticLocalAgentConfigs } from "./agentExecutionPolicy.js";
import { agentConcurrencyLimit, buildAgentDispatchPlan } from "./agentDispatchPolicy.js";
import {
  discoveryPressureReason,
  discoveryPressureRecoveredForReason,
  proxyUnavailableRatio,
} from "./backpressurePolicy.js";
import {
  closeDb,
  ensureSchema,
  query,
  withTransaction,
} from "./db.js";
import { createControllerLifecycle } from "./controllerLifecycle.js";
import { dataApiCircuitState as loadDataApiCircuitState } from "./dataApiCircuit.js";
import {
  reconcileChannelCandidateQueue as reconcileChannelCandidateQueueWithDependencies,
} from "./channelSnapshotReconciliation.js";
import { resolveDiscoveryPageQualification } from "./discoveryPagePolicy.js";
import {
  createCoalescedWakeup,
  DISCOVERY_PAGE_READY_CHANNEL,
} from "./discoveryPageWakeup.js";
import {
  CONTENT_COMPLETENESS_REPAIR_VERSION,
  enqueueContentRepairTargets,
  hasPendingContentRepairs,
  loadContentRepairTargets,
  prepareContentRepairTargets,
  reconcileLiveDurationNotApplicable,
} from "./contentRepair.js";
import {
  ContentEnrichDispatcher,
  PostgresContentEnrichDispatchRepository,
} from "./contentEnrichDispatch.js";
import { dispatchContentEnrichForController } from "./controllerContentEnrichDispatch.js";
import {
  ContentEnrichMonitor,
  PostgresContentEnrichObservabilityRepository,
} from "./contentEnrichObservability.js";
import {
  IncrementalAgentBacklog,
  IncrementalAgentBatcher,
} from "./incrementalAgentBacklog.js";
import { loadFullRepairCompletionState } from "./fullRepairDispatch.js";
import {
  finalRepairCandidateSql,
  preparedFinalDetailRepairSql,
} from "./finalRepairCandidatePolicy.js";
import { ensureFinalRepairJob } from "./finalRepairJobRecovery.js";
import { maybeStartMetadataDiscoveryCycle } from "./metadataDiscoveryLoop.js";
import {
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "./managedJobDispatchOutbox.js";
import {
  ManagedJobIntentStore,
  PostgresManagedJobIntentRepository,
} from "./managedJobIntentStore.js";
import {
  MigrationRetryIntentJobReconciler,
  PostgresMigrationRetryIntentRepository,
} from "./migrationRetryIntent.js";
import { ManagedPolicyUnavailableError } from "./managedJobIntents.js";
import { settleCompletedMigrationBatch } from "./migrationBatchCompletion.js";
import { loadIdentityPolicyCatalog } from "./identityPolicyCatalog.js";
import { closeProxyControlClient, proxyControlClient } from "./proxyControlClient.js";
import { normalizeRotaCapacity } from "./rotaCapacity.js";
import {
  activeFinalRepairExclusions,
  hasOpenPipelineCrawlerWork,
  loadFinalizeRecoveryCandidates,
  loadPublicationGapRepairCandidates,
  loadPipelineFinalizeBlockers,
  mergeFinalRepairCandidates,
  publicationGapRepairTarget,
} from "./finalizeRecoveryPolicy.js";
import { reconcileAutomaticPublicationBacklog } from "./publicationChannelOnboarding.js";
import {
  FINALIZABLE_CHANNEL_STATUSES,
  representedFinalizeRunIds,
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
} from "./finalizePolicy.js";
import {
  getQueryScheduler,
  normalizeQueryScheduler,
  QUERY_SCHEDULER_KEY,
  querySchedulerAllowsDiscovery,
  reconcileAutomaticDiscoveryClosure,
} from "./queryScheduler.js";
import {
  automaticFinalRepairReference,
  finalRepairDispatchDecision,
  finalRepairRoundDecision,
  finalRepairRoundEligibilitySql,
  repairDispatchCapacity,
} from "./repairPolicy.js";
import {
  closeQueues,
  createQueues,
  createRedisConnection,
  getQueueStats,
  hasQueryPipelineQueueBacklog,
  queuesByRole,
  safeJobId,
} from "./queues.js";

const queues = createQueues();
const managedIdentityPolicyCatalog = loadIdentityPolicyCatalog();
const expectedWorkloadScope = String(process.env.ROTA_WORKLOAD_SCOPE_EXPECTED ?? "").trim();
if (!expectedWorkloadScope) throw new Error("ROTA_WORKLOAD_SCOPE_EXPECTED is required");
if (managedIdentityPolicyCatalog.workload_scope !== expectedWorkloadScope) {
  throw new Error(
    `Identity Policy workload scope mismatch: ${managedIdentityPolicyCatalog.workload_scope} != ${expectedWorkloadScope}`,
  );
}
const managedIdentityPolicies = [...managedIdentityPolicyCatalog.policies.values()];
const managedJobIntentStore = new ManagedJobIntentStore({
  repository: new PostgresManagedJobIntentRepository({ withTransaction }),
  policies: managedIdentityPolicies,
  qualityChunkSize: 3,
});
const managedJobOutboxDispatcher = new ManagedJobOutboxDispatcher({
  repository: new PostgresManagedJobDispatchRepository({ withTransaction }),
  queues,
});
const migrationRetryIntentJobReconciler = new MigrationRetryIntentJobReconciler({
  repository: new PostgresMigrationRetryIntentRepository({ withTransaction }),
  queue: queues[queuesByRole.channelCrawl],
});

function intEnv(name, fallback, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(Math.floor(value), max));
}

function numberEnv(name, fallback, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(value, max));
}

function booleanEnv(name, fallback) {
  const value = String(process.env[name] ?? fallback).trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(value);
}

const intervalMs = intEnv("CONTROLLER_INTERVAL_MS", 15000, 1000, 300000);
const wakeupDelayMs = intEnv("CONTROLLER_WAKEUP_DELAY_MS", 25, 0, 1000);
const tickSampleMs = intEnv("CONTROLLER_TICK_SAMPLE_MS", 60000, 15000, 3600000);
const controllerTickRetentionDays = intEnv("CONTROLLER_TICK_RETENTION_DAYS", 7, 1, 3650);
const taskEventRetentionDays = intEnv("TASK_EVENT_RETENTION_DAYS", 30, 1, 3650);
const telemetryCleanupIntervalMs = intEnv("CONTROLLER_TELEMETRY_CLEANUP_INTERVAL_MS", 3600000, 60000, 86400000);
const discoverPauseChannelBacklog = intEnv("DISCOVER_PAUSE_CHANNEL_BACKLOG", 100);
const discoverResumeChannelBacklog = intEnv("DISCOVER_RESUME_CHANNEL_BACKLOG", 40);
const discoverPauseDetailBacklog = intEnv("DISCOVER_PAUSE_DETAIL_BACKLOG", 80);
const discoverResumeDetailBacklog = intEnv("DISCOVER_RESUME_DETAIL_BACKLOG", 30);
const channelPauseDetailBacklog = intEnv("CHANNEL_PAUSE_DETAIL_BACKLOG", 100);
const channelResumeDetailBacklog = intEnv("CHANNEL_RESUME_DETAIL_BACKLOG", 40);
const dataApiPauseDiscoverBacklog = intEnv("DATA_API_PAUSE_DISCOVER_BACKLOG", 20);
const dataApiResumeDiscoverBacklog = intEnv("DATA_API_RESUME_DISCOVER_BACKLOG", 8);
const agentPauseDiscoverBacklog = intEnv("AGENT_PAUSE_DISCOVER_BACKLOG", 10);
const agentResumeDiscoverBacklog = intEnv("AGENT_RESUME_DISCOVER_BACKLOG", 4);
const discoverPauseChannelEtaSeconds = intEnv("DISCOVER_PAUSE_CHANNEL_ETA_SECONDS", 300, 1, 86400);
const discoverResumeChannelEtaSeconds = intEnv("DISCOVER_RESUME_CHANNEL_ETA_SECONDS", 120, 0, 86400);
const discoverPauseChannelFailureRate = numberEnv("DISCOVER_PAUSE_CHANNEL_FAILURE_RATE", 0.2, 0, 1);
const discoverResumeChannelFailureRate = numberEnv("DISCOVER_RESUME_CHANNEL_FAILURE_RATE", 0.1, 0, 1);
const discoverPauseProxyCooldownRatio = numberEnv("DISCOVER_PAUSE_PROXY_COOLDOWN_RATIO", 0.35, 0, 1);
const discoverResumeProxyCooldownRatio = numberEnv("DISCOVER_RESUME_PROXY_COOLDOWN_RATIO", 0.15, 0, 1);
const discoverFailedRetrySeconds = intEnv("DISCOVER_FAILED_RETRY_SECONDS", 600, 30, 86400);
const channelPressureMinimumSamples = intEnv("CHANNEL_PRESSURE_MINIMUM_SAMPLES", 10, 1, 1000);
const channelPressureSampleSize = intEnv("CHANNEL_PRESSURE_SAMPLE_SIZE", 200, 10, 5000);
const channelPressureWindowSeconds = intEnv("CHANNEL_PRESSURE_WINDOW_SECONDS", 900, 60, 86400);
const agentBatchSize = intEnv("AGENT_BATCH_SIZE", 30, 1, 50);
const incrementalAgentBatchSize = intEnv("INCREMENTAL_AGENT_BATCH_SIZE", 30, 1, 50);
const incrementalAgentConfigId = intEnv("INCREMENTAL_AGENT_CONFIG_ID", 0, 0);
const incrementalAgentTailQuietMs = intEnv(
  "INCREMENTAL_AGENT_TAIL_QUIET_MS",
  15 * 60 * 1000,
  0,
  24 * 60 * 60 * 1000,
);
const incrementalAgentQueuedStaleSeconds = intEnv(
  "INCREMENTAL_AGENT_QUEUED_STALE_SECONDS",
  300,
  60,
  3600,
);
const incrementalAgentRunningStaleSeconds = intEnv(
  "INCREMENTAL_AGENT_RUNNING_STALE_SECONDS",
  3900,
  600,
  14400,
);
const queryQualityTaskChunkSize = 3;
const queryQualityReconcileLimit = intEnv("QUERY_QUALITY_RECONCILE_LIMIT", 300, 3, 3000);
const queryQualityStaleSeconds = intEnv("QUERY_QUALITY_STALE_SECONDS", 1800, 300, 14400);
const channelSnapshotReconcileLimit = intEnv("CHANNEL_SNAPSHOT_RECONCILE_LIMIT", 200, 1, 2000);
const channelSnapshotMaxAttempts = intEnv("CHANNEL_SNAPSHOT_MAX_ATTEMPTS", 6, 3, 30);
const channelSnapshotRetrySeconds = intEnv("CHANNEL_SNAPSHOT_RETRY_SECONDS", 60, 10, 3600);
const channelSnapshotStaleSeconds = intEnv("CHANNEL_SNAPSHOT_STALE_SECONDS", 900, 60, 7200);
const channelSnapshotMinSubscriberCount = intEnv("MIN_SUBSCRIBER_COUNT", 1000, 1, 100_000_000);
const channelCandidateDispatchEnabled = booleanEnv("CHANNEL_CANDIDATE_DISPATCH_ENABLED", true);
const agentMaxBatchesPerTick = intEnv("AGENT_MAX_BATCHES_PER_TICK", 5, 1, 100);
const agentQueuedStaleSeconds = intEnv("AGENT_QUEUED_STALE_SECONDS", 180, 60, 3600);
const agentRunningStaleSeconds = intEnv("AGENT_RUNNING_STALE_SECONDS", 3900, 600, 14400);
const finalizeReconcileLimit = intEnv("FINALIZE_RECONCILE_LIMIT", 200, 1, 2000);
const finalRepairIntervalMs = intEnv("FINAL_REPAIR_INTERVAL_MS", 30000, 15000, 3600000);
const finalRepairMaxRounds = intEnv("FINAL_REPAIR_MAX_ROUNDS", 3, 1, 10);
const finalCheckpointRepairMaxRounds = intEnv(
  "FINAL_CHECKPOINT_REPAIR_MAX_ROUNDS",
  1,
  1,
  3,
);
const finalRepairBatchSize = intEnv("FINAL_REPAIR_BATCH_SIZE", 5, 1, 50);
const contentRepairBatchSize = intEnv("CONTENT_REPAIR_BATCH_SIZE", 20, 1, 50);
const contentRepairIntervalMs = intEnv("CONTENT_REPAIR_INTERVAL_MS", 30000, 15000, 3600000);
const recoveredFailureCleanupIntervalMs = intEnv("RECOVERED_FAILURE_CLEANUP_INTERVAL_MS", 60000, 15000, 3600000);
const publicationOnboardingIntervalMs = intEnv(
  "PUBLICATION_ONBOARDING_RECONCILE_INTERVAL_MS",
  60000,
  15000,
  3600000,
);
const publicationOnboardingBatchSize = intEnv("PUBLICATION_ONBOARDING_RECONCILE_BATCH_SIZE", 25, 1, 1000);
const contentEnrichDispatchEnabled = booleanEnv("CONTENT_ENRICH_DISPATCH_ENABLED", false);
const contentEnrichQueueHighWater = intEnv("CONTENT_ENRICH_QUEUE_HIGH_WATER", 50, 1, 10_000);
const contentEnrichQueueRefill = intEnv("CONTENT_ENRICH_QUEUE_REFILL", 20, 1, 10_000);
const contentEnrichBatchSize = intEnv("CONTENT_ENRICH_BATCH_SIZE", 5, 1, 100);
const contentEnrichLeaseMs = intEnv(
  "CONTENT_ENRICH_DISPATCH_LEASE_MS",
  15 * 60_000,
  30_000,
  24 * 60 * 60_000,
);
const contentEnrichMetricsWindowMs = intEnv(
  "CONTENT_ENRICH_METRICS_WINDOW_SECONDS",
  300,
  60,
  86_400,
) * 1_000;
const contentEnrichMetricsSampleMs = intEnv(
  "CONTENT_ENRICH_METRICS_SAMPLE_SECONDS",
  60,
  15,
  86_400,
) * 1_000;
const contentEnrichMetricsQueryTimeoutMs = intEnv(
  "CONTENT_ENRICH_METRICS_QUERY_TIMEOUT_SECONDS",
  5,
  1,
  60,
) * 1_000;
const contentEnrichBacklogAlertThreshold = intEnv(
  "CONTENT_ENRICH_BACKLOG_ALERT_THRESHOLD",
  10_000,
  0,
  10_000_000,
);
const contentEnrichQueuedAgeAlertSeconds = intEnv(
  "CONTENT_ENRICH_QUEUED_AGE_ALERT_SECONDS",
  86_400,
  0,
  365 * 86_400,
);
const contentEnrichAlertRepeatMs = intEnv(
  "CONTENT_ENRICH_ALERT_REPEAT_SECONDS",
  900,
  60,
  86_400,
) * 1_000;
const channelInlineDetails = String(process.env.YOUTUBE_CHANNEL_INLINE_DETAILS || "true").trim().toLowerCase() !== "false";
let crawlSettingsCache = { expiresAt: 0, value: null };
let lastStoredTickAt = 0;
let lastStoredTickSignature = null;
let lastTelemetryCleanupAt = 0;
let lastFinalRepairAt = 0;
let lastContentRepairAt = 0;
let lastRecoveredFailureCleanupAt = 0;
let lastPublicationOnboardingAt = 0;
let activeDiscoverPressureReason = null;
const incrementalAgentBacklogStore = new IncrementalAgentBacklog({ withTransaction });
const incrementalAgentBatcher = new IncrementalAgentBatcher({
  backlog: incrementalAgentBacklogStore,
  queue: queues[queuesByRole.agentIncremental],
  batchSize: incrementalAgentBatchSize,
  tailQuietMs: incrementalAgentTailQuietMs,
  agentConfigId: incrementalAgentConfigId || null,
});
const contentEnrichDispatcher = new ContentEnrichDispatcher({
  repository: new PostgresContentEnrichDispatchRepository({
    queryFn: query,
    withTransaction,
  }),
  queue: queues[queuesByRole.contentEnrich],
  enabled: contentEnrichDispatchEnabled,
  highWater: contentEnrichQueueHighWater,
  refill: contentEnrichQueueRefill,
  batchSize: contentEnrichBatchSize,
  leaseDurationMs: contentEnrichLeaseMs,
});
const contentEnrichMonitor = new ContentEnrichMonitor({
  repository: new PostgresContentEnrichObservabilityRepository({
    queryFn: query,
    withTransaction,
    queryTimeoutMs: contentEnrichMetricsQueryTimeoutMs,
  }),
  windowMs: contentEnrichMetricsWindowMs,
  sampleIntervalMs: contentEnrichMetricsSampleMs,
  queryTimeoutMs: contentEnrichMetricsQueryTimeoutMs,
  backlogAlertThreshold: contentEnrichBacklogAlertThreshold,
  queuedAgeAlertSeconds: contentEnrichQueuedAgeAlertSeconds,
  alertRepeatMs: contentEnrichAlertRepeatMs,
});

function tickSignature(stats, actions, queryScheduler) {
  return createHash("sha1").update(JSON.stringify({
    queues: stats,
    actions,
    query_scheduler: {
      status: queryScheduler.status,
      query_set_id: queryScheduler.query_set_id,
      query_quality_min_score: queryScheduler.query_quality_min_score,
      max_discover_backlog: queryScheduler.max_discover_backlog,
      stop_reason: queryScheduler.stop_reason,
      pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
    },
  })).digest("hex");
}

function automaticFinalizationActive(scheduler) {
  return ["finishing", "repairing"].includes(String(scheduler?.status || ""));
}

function pipelineProducerActive(scheduler) {
  return ["running", "finishing", "repairing"].includes(String(scheduler?.status || ""));
}

async function reconcileQueryQualityQueue(actions) {
  const recovered = await query(
    `WITH stale_task AS (
       UPDATE crawler.query_quality_tasks task
       SET status='queued',error_message='recovered stale quality task',updated_at=now()
       FROM crawler.query_quality_batches batch
       WHERE task.quality_batch_id=batch.quality_batch_id
         AND task.status='running'
         AND batch.status IN ('queued','running')
         AND NOT (task.result_json ? 'parser_contract_error')
         AND task.updated_at<=now()-($1::int * interval '1 second')
       RETURNING task.quality_batch_id,task.quality_task_id
     )
     UPDATE crawler.query_quality_chunks chunk
     SET status='queued',dispatch_reason='recovered_stale_quality_chunk',
         finished_at=NULL,updated_at=now()
     FROM crawler.query_quality_chunk_members member
     JOIN stale_task stale
       ON stale.quality_batch_id=member.quality_batch_id
      AND stale.quality_task_id=member.quality_task_id
     WHERE chunk.quality_chunk_id=member.quality_chunk_id
       AND chunk.status NOT IN ('done','failed','cancelled')
     RETURNING chunk.quality_chunk_id,chunk.dispatched_job_id`,
    [queryQualityStaleSeconds],
  );
  const recoveredChunkIds = [...new Set(
    recovered.rows.map((row) => String(row.quality_chunk_id || "").trim()).filter(Boolean),
  )];
  for (const qualityChunkId of recoveredChunkIds) {
    const row = recovered.rows.find((candidate) => candidate.quality_chunk_id === qualityChunkId);
    const jobId = String(row?.dispatched_job_id || safeJobId("query-quality", qualityChunkId));
    const job = await queues[queuesByRole.queryQuality].getJob(jobId);
    const state = job ? await job.getState() : "missing";
    if (state === "failed") {
      await job.retry("failed");
      continue;
    }
    if (["waiting", "active", "delayed", "prioritized", "waiting-children"].includes(state)) {
      continue;
    }
    if (state === "completed") await job.remove();
    await withTransaction(async (client) => {
      await client.query(
        `UPDATE crawler.query_quality_chunks
         SET dispatch_status='pending',dispatched_job_id=NULL,
             dispatch_reason='recovered_stale_quality_chunk',updated_at=now()
         WHERE quality_chunk_id=$1 AND dispatch_status<>'terminal'`,
        [qualityChunkId],
      );
      await client.query(
        `UPDATE crawler.proxy_job_dispatch_outbox
         SET status='pending',attempts=0,next_attempt_at=NULL,last_error=NULL,sent_at=NULL,updated_at=now()
         WHERE aggregate_kind='query_quality_chunk'
           AND aggregate_id=$1
           AND status<>'dead'`,
        [qualityChunkId],
      );
    });
  }
  if (recoveredChunkIds.length > 0) {
    actions.push({
      action: "recover-stale-query-quality-chunks",
      chunks: recoveredChunkIds.length,
    });
  }
  const rows = await query(
    `SELECT DISTINCT batch.quality_batch_id,batch.created_at
     FROM crawler.query_quality_batches batch
     JOIN crawler.query_quality_tasks task USING (quality_batch_id)
     LEFT JOIN crawler.query_quality_chunk_members member
       ON member.quality_batch_id=task.quality_batch_id
      AND member.quality_task_id=task.quality_task_id
     WHERE batch.status IN ('queued','running')
       AND task.status='queued'
       AND member.quality_task_id IS NULL
       AND NOT (task.result_json ? 'parser_contract_error')
     ORDER BY batch.created_at,batch.quality_batch_id
     LIMIT $1`,
    [Math.ceil(queryQualityReconcileLimit / queryQualityTaskChunkSize)],
  );
  const batchIds = new Set();
  let chunkCount = 0;
  let taskCount = 0;
  for (const row of rows.rows) {
    const qualityBatchId = String(row.quality_batch_id);
    try {
      const prepared = await managedJobIntentStore.prepareQueryQualityBatch(qualityBatchId);
      if (prepared.chunks.length === 0) continue;
      batchIds.add(qualityBatchId);
      chunkCount += prepared.chunks.length;
      taskCount += prepared.chunks.reduce(
        (sum, chunk) => sum + (chunk.quality_task_ids?.length ?? 0),
        0,
      );
    } catch (error) {
      if (!(error instanceof ManagedPolicyUnavailableError)) throw error;
      await query(
        `UPDATE crawler.query_quality_batches
         SET error_message=$2,updated_at=now()
         WHERE quality_batch_id=$1 AND status IN ('queued','running')`,
        [qualityBatchId, error.message],
      );
      actions.push({
        action: "defer-query-quality-policy",
        quality_batch_id: qualityBatchId,
        language: error.language,
        country: error.country,
      });
    }
  }
  for (const qualityBatchId of batchIds) {
    await query(
      `UPDATE crawler.query_quality_batches
       SET error_message=NULL,updated_at=now()
       WHERE quality_batch_id=$1 AND status IN ('queued','running')`,
      [qualityBatchId],
    );
  }
  const dispatched = await managedJobOutboxDispatcher.dispatchAvailable({
    limit: queryQualityReconcileLimit,
  });
  if (chunkCount > 0 || dispatched.claimed > 0) {
    actions.push({
      action: "reconcile-query-quality",
      batches: batchIds.size,
      tasks: taskCount,
      chunks: chunkCount,
      dispatch: dispatched,
    });
  }
}

async function reconcileChannelCandidateQueue(actions, dispatchBatchId) {
  return reconcileChannelCandidateQueueWithDependencies({
    actions,
    dispatchBatchId,
    query,
    queue: queues[queuesByRole.channelCrawl],
    withTransaction,
    safeJobId,
    minSubscriberCount: channelSnapshotMinSubscriberCount,
    staleSeconds: channelSnapshotStaleSeconds,
    maxAttempts: channelSnapshotMaxAttempts,
    retrySeconds: channelSnapshotRetrySeconds,
    limit: channelSnapshotReconcileLimit,
  });
}

async function updateSchedulerRuntime(status, values = {}) {
  const now = new Date().toISOString();
  await query(
    `UPDATE crawler.settings
     SET value_json=value_json || $2::jsonb,updated_at=now()
     WHERE setting_key=$1`,
    [QUERY_SCHEDULER_KEY, JSON.stringify({ status, updated_at: now, updated_by: "controller", ...values })],
  );
}

async function ensureDispatchBatch(scheduler) {
  const dispatchBatchId = String(scheduler?.pipeline_cycle_id || "").trim();
  if (!dispatchBatchId) return null;
  await query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,query_set_id,status,query_quality_min_score,result_json,updated_at
     ) VALUES ($1,$1,$2,'running',$3,$4::jsonb,now())
     ON CONFLICT (dispatch_batch_id) DO UPDATE
     SET query_set_id=COALESCE(EXCLUDED.query_set_id,crawler.query_dispatch_batches.query_set_id),
         query_quality_min_score=EXCLUDED.query_quality_min_score,
         status=CASE
           WHEN crawler.query_dispatch_batches.status='stopped' THEN 'running'
           ELSE crawler.query_dispatch_batches.status
         END,
         discovery_closed_at=CASE
           WHEN crawler.query_dispatch_batches.status='stopped' THEN NULL
           ELSE crawler.query_dispatch_batches.discovery_closed_at
         END,
         validation_closed_at=CASE
           WHEN crawler.query_dispatch_batches.status='stopped' THEN NULL
           ELSE crawler.query_dispatch_batches.validation_closed_at
         END,
         agent_tail_flushed_at=CASE
           WHEN crawler.query_dispatch_batches.status='stopped' THEN NULL
           ELSE crawler.query_dispatch_batches.agent_tail_flushed_at
         END,
         finished_at=CASE
           WHEN crawler.query_dispatch_batches.status='stopped' THEN NULL
           ELSE crawler.query_dispatch_batches.finished_at
         END,
         result_json=crawler.query_dispatch_batches.result_json || EXCLUDED.result_json,
         updated_at=now()`,
    [
      dispatchBatchId,
      scheduler.query_set_id,
      scheduler.query_quality_min_score,
      JSON.stringify({
        chunk_size: scheduler.chunk_size,
        max_discover_backlog: scheduler.max_discover_backlog,
      }),
    ],
  );
  return dispatchBatchId;
}

async function addDispatchQuery(dispatchBatchId, queryId) {
  if (!dispatchBatchId || !Number.isFinite(Number(queryId))) return;
  await query(
    `UPDATE crawler.query_dispatch_batches
     SET selected_query_ids=(
           SELECT COALESCE(array_agg(DISTINCT value ORDER BY value),'{}'::bigint[])
           FROM unnest(selected_query_ids || ARRAY[$2::bigint]) AS value
         ),
         selected_query_count=(
           SELECT count(DISTINCT value)::int
           FROM unnest(selected_query_ids || ARRAY[$2::bigint]) AS value
         ),
         updated_at=now()
     WHERE dispatch_batch_id=$1`,
    [dispatchBatchId, Number(queryId)],
  );
}

async function closeDispatchDiscovery(dispatchBatchId) {
  if (!dispatchBatchId) return;
  await reconcileAutomaticDiscoveryClosure(query, {
    status: "finishing",
    pipeline_cycle_id: dispatchBatchId,
  });
}

async function resumeLegacyAutomaticFinalization(scheduler, actions) {
  if (scheduler.status !== "stopped" || scheduler.stop_reason !== "no_schedulable_query") return scheduler;
  await updateSchedulerRuntime("finishing", {
    stopped_at: null,
    stop_reason: "upstream_drained",
  });
  actions.push({ action: "resume-automatic-finalization", reason: "legacy_upstream_drained_state" });
  return { ...scheduler, status: "finishing", stopped_at: null, stop_reason: "upstream_drained" };
}

async function cleanupTelemetry(now = Date.now()) {
  if (now - lastTelemetryCleanupAt < telemetryCleanupIntervalMs) return;
  await query(
    `DELETE FROM crawler.controller_ticks
     WHERE created_at < now() - ($1::int * interval '1 day')`,
    [controllerTickRetentionDays],
  );
  await query(
    `DELETE FROM crawler.task_events
     WHERE created_at < now() - ($1::int * interval '1 day')`,
    [taskEventRetentionDays],
  );
  lastTelemetryCleanupAt = now;
}

async function persistControllerTick(stats, actions, queryScheduler) {
  const now = Date.now();
  const signature = tickSignature(stats, actions, queryScheduler);
  const changed = signature !== lastStoredTickSignature;
  const sampleDue = now - lastStoredTickAt >= tickSampleMs;
  if (!changed && !sampleDue && actions.length === 0) return false;
  await query(
    `INSERT INTO crawler.controller_ticks (status, queues_json, actions_json)
     VALUES ('ok', $1::jsonb, $2::jsonb)`,
    [JSON.stringify(stats), JSON.stringify(actions)],
  );
  lastStoredTickAt = now;
  lastStoredTickSignature = signature;
  return true;
}

async function getCrawlSettings() {
  const now = Date.now();
  if (crawlSettingsCache.value && crawlSettingsCache.expiresAt > now) return crawlSettingsCache.value;
  const fallback = {
    youtubeApiBatchSize: 50,
    youtubeApiDailyRequestLimit: intEnv("YOUTUBE_DATA_API_DAILY_REQUEST_LIMIT", 500, 0, 10000),
    youtubeApiFallbackMode: process.env.YOUTUBE_DATA_API_FALLBACK_MODE === "disabled" ? "disabled" : "emergency",
  };
  try {
    const rows = await query("SELECT value_json FROM crawler.settings WHERE setting_key = 'youtube_api' LIMIT 1");
    const value = rows.rows[0]?.value_json || {};
    const settings = {
      youtubeApiBatchSize: intEnv("YOUTUBE_DATA_API_BATCH_SIZE", Number(value.batch_size ?? fallback.youtubeApiBatchSize), 1, 50),
      youtubeApiDailyRequestLimit: intEnv(
        "YOUTUBE_DATA_API_DAILY_REQUEST_LIMIT",
        Number(value.daily_request_limit ?? fallback.youtubeApiDailyRequestLimit),
        0,
        10000,
      ),
      youtubeApiFallbackMode: value.fallback_mode === "disabled" ? "disabled" : "emergency",
    };
    crawlSettingsCache = { expiresAt: now + 30000, value: settings };
    return settings;
  } catch {
    crawlSettingsCache = { expiresAt: now + 30000, value: fallback };
    return fallback;
  }
}

function backlog(stats, queueName) {
  const row = stats[queueName] ?? {};
  return Number(row.waiting ?? 0)
    + Number(row.active ?? 0)
    + Number(row.delayed ?? 0)
    + Number(row.paused ?? 0)
    + Number(row.prioritized ?? 0)
    + Number(row["waiting-children"] ?? 0);
}

async function setPaused(queueName, paused, reason, actions) {
  const queue = queues[queueName];
  if (!queue) return;
  const isPaused = await queue.isPaused();
  if (paused && !isPaused) {
    await queue.pause();
    actions.push({ action: "pause", queue: queueName, reason });
  } else if (!paused && isPaused) {
    await queue.resume();
    actions.push({ action: "resume", queue: queueName, reason });
  }
}

function hasQueueBacklog(stats, queueName) {
  return backlog(stats, queueName) > 0;
}

async function reconcileTerminalFinalizedRunStates(actions, pipelineCycleId) {
  const rows = await query(
    `UPDATE crawler.channel_runs run
     SET status='done',detail_status='done',finished_at=COALESCE(run.finished_at,now()),updated_at=now()
     FROM crawler.channels channel,crawler.finalized_profiles finalized
     WHERE channel.latest_run_id=run.run_id
       AND channel.status=ANY($2::text[])
       AND finalized.channel_id=channel.channel_id
       AND finalized.run_id=run.run_id
       AND finalized.status=ANY($3::text[])
       AND ($1::text IS NULL OR run.result_json->>'pipeline_cycle_id'=$1::text)
       AND (run.status<>'done' OR run.detail_status<>'done' OR run.finished_at IS NULL)
     RETURNING run.run_id,run.channel_id`,
    [
      pipelineCycleId,
      FINALIZABLE_CHANNEL_STATUSES,
      SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
    ],
  );
  if (rows.rows.length > 0) {
    actions.push({
      action: "reconcile-terminal-finalized-runs",
      count: rows.rows.length,
      run_ids: rows.rows.slice(0, 20).map((row) => row.run_id),
    });
  }
  return rows.rows.length;
}

async function refreshDispatchValidationState(dispatchBatchId) {
  if (!dispatchBatchId) return false;
  const rows = await query(
    `WITH candidate_stats AS (
       SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status='accepted')::int AS accepted,
              count(*) FILTER (WHERE status='rejected')::int AS rejected,
              count(*) FILTER (WHERE status IN ('discovered','queued','validating'))::int AS open
       FROM crawler.channel_candidates
       WHERE dispatch_batch_id=$1
     ), updated AS (
       UPDATE crawler.query_dispatch_batches batch
       SET discovered_candidate_count=stats.total,
           accepted_channel_count=stats.accepted,
           rejected_channel_count=stats.rejected,
           status=CASE
             WHEN batch.discovery_closed_at IS NOT NULL AND stats.open=0 THEN 'validation_closed'
             ELSE batch.status
           END,
           validation_closed_at=CASE
             WHEN batch.discovery_closed_at IS NOT NULL AND stats.open=0
               THEN COALESCE(batch.validation_closed_at,now())
             ELSE batch.validation_closed_at
           END,
           updated_at=now()
       FROM candidate_stats stats
       WHERE batch.dispatch_batch_id=$1
       RETURNING batch.validation_closed_at,(SELECT open FROM candidate_stats) AS open
     )
     SELECT validation_closed_at,open FROM updated`,
    [dispatchBatchId],
  );
  return Boolean(rows.rows[0]?.validation_closed_at) && Number(rows.rows[0]?.open ?? 0) === 0;
}

async function syncAgentGlobalConcurrency(actions, agentConfigs) {
  const agentQueue = queues[queuesByRole.agentBatch];
  const registeredWorkers = await agentQueue.getWorkersCount();
  const concurrency = agentConcurrencyLimit(agentConfigs, registeredWorkers);
  if (concurrency > 0) {
    const previous = await agentQueue.getGlobalConcurrency();
    if (previous !== concurrency) {
      await agentQueue.setGlobalConcurrency(concurrency);
      actions.push({
        action: "set-global-concurrency",
        queue: queuesByRole.agentBatch,
        concurrency,
        previous,
        registered_workers: registeredWorkers,
        configured_capacity: agentConfigs.reduce((total, config) => total + Number(config.max_workers ?? 1), 0),
      });
    }
  }
  return { registeredWorkers, concurrency };
}

async function maybeCreateAgentBatch(actions, agentConfigs, agentCapacity, queryScheduler) {
  if (!queryScheduler.pipeline_cycle_id) return;
  const dispatchBatchId = queryScheduler.pipeline_cycle_id;
  const allowPartialFlush = await refreshDispatchValidationState(dispatchBatchId);
  const agentQueue = queues[queuesByRole.agentBatch];
  const jobs = await agentQueue.getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"],
    0,
    9999,
    true,
  );
  const outstandingByConfig = new Map();
  for (const job of jobs) {
    const configId = Number(job.data?.agent_config_id);
    if (!Number.isFinite(configId) || configId <= 0) continue;
    outstandingByConfig.set(configId, (outstandingByConfig.get(configId) ?? 0) + 1);
  }
  const dispatchPlan = buildAgentDispatchPlan({
    configs: agentConfigs,
    outstandingByConfig,
    outstandingTotal: jobs.length,
    workerCapacity: agentCapacity.concurrency,
    maxBatches: agentMaxBatchesPerTick,
  });
  for (const agentConfig of dispatchPlan) {
    const batchSize = Math.max(1, Math.min(50, Number(agentConfig?.batch_size ?? agentBatchSize)));
    const rows = await query(
      `WITH candidates AS MATERIALIZED (
         SELECT c.channel_id,c.channel_url
         FROM crawler.channels c
         JOIN crawler.channel_runs current_run ON current_run.run_id=c.latest_run_id
         WHERE c.ready_for_agent=true
           AND c.agent_status IN ('pending','failed')
           AND NOT EXISTS (
             SELECT 1 FROM crawler.agent_refresh_requests refresh
             WHERE refresh.channel_id=c.channel_id
               AND (
                 refresh.status IN ('pending','queued','running')
                 OR (refresh.status='failed' AND isfinite(refresh.next_retry_at))
               )
           )
           AND (c.agent_next_retry_at IS NULL OR c.agent_next_retry_at<=now())
           AND c.status='active'
           AND c.subscriber_count IS NOT NULL
           AND NULLIF(btrim(c.title),'') IS NOT NULL
           AND COALESCE(current_run.result_json->>'dispatch_batch_id',current_run.result_json->>'pipeline_cycle_id')=$3
         ORDER BY c.priority DESC,c.created_at ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       ), candidate_count AS (
         SELECT count(*)::int AS count FROM candidates
       ), picked AS (
         SELECT channel_id,channel_url FROM candidates
         WHERE (SELECT count FROM candidate_count)>=$1 OR $2::boolean=true
       ), updated AS (
         UPDATE crawler.channels channel
         SET agent_status='queued',updated_at=now()
         FROM picked
         WHERE channel.channel_id=picked.channel_id
         RETURNING channel.channel_id,channel.channel_url
       )
       SELECT updated.*,(SELECT count FROM candidate_count) AS eligible_count FROM updated`,
      [batchSize, allowPartialFlush, dispatchBatchId],
    );
    if (rows.rows.length === 0) continue;
    const channelIds = rows.rows.map((row) => row.channel_id);
    const batchId = `agent-batch:${dispatchBatchId}:${Date.now()}:${nanoid(8)}`;
    try {
      await queues[queuesByRole.agentBatch].add(
        "agent-profile-batch",
        {
          batch_id: batchId,
          channel_ids: channelIds,
          agent_mode: "basic",
          agent_config_id: agentConfig.config_id,
          pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
          dispatch_batch_id: dispatchBatchId,
        },
        { jobId: safeJobId("agent-batch", batchId) },
      );
    } catch (error) {
      await query(
        `UPDATE crawler.channels
         SET agent_status='pending',agent_error_message=$2,updated_at=now()
         WHERE channel_id=ANY($1::text[]) AND agent_status='queued'`,
        [channelIds, error?.message || String(error)],
      );
      throw error;
    }
    actions.push({
      action: "enqueue-agent-batch",
      queue: queuesByRole.agentBatch,
      count: channelIds.length,
      batch_size: batchSize,
      agent_config_id: agentConfig.config_id,
      agent_config_name: agentConfig.name,
      config_max_workers: Number(agentConfig.max_workers ?? 1),
      registered_agent_workers: agentCapacity.registeredWorkers,
      agent_global_concurrency: agentCapacity.concurrency,
      partial_flush: channelIds.length < batchSize,
      batch_id: batchId,
      dispatch_batch_id: dispatchBatchId,
    });
    if (rows.rows.length < batchSize) break;
  }
  if (allowPartialFlush) {
    const remaining = await query(
      `SELECT count(*)::int AS count
       FROM crawler.channels channel
       JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
       WHERE channel.ready_for_agent=true
         AND channel.agent_status IN ('pending','failed')
         AND NOT EXISTS (
           SELECT 1 FROM crawler.agent_refresh_requests refresh
           WHERE refresh.channel_id=channel.channel_id
             AND (
               refresh.status IN ('pending','queued','running')
               OR (refresh.status='failed' AND isfinite(refresh.next_retry_at))
             )
         )
         AND channel.subscriber_count IS NOT NULL
         AND NULLIF(btrim(channel.title),'') IS NOT NULL
         AND COALESCE(run.result_json->>'dispatch_batch_id',run.result_json->>'pipeline_cycle_id')=$1`,
      [dispatchBatchId],
    );
    if (Number(remaining.rows[0]?.count ?? 0) === 0) {
      await query(
        `UPDATE crawler.query_dispatch_batches
         SET agent_tail_flushed_at=COALESCE(agent_tail_flushed_at,now()),updated_at=now()
         WHERE dispatch_batch_id=$1`,
        [dispatchBatchId],
      );
    }
  }
}

async function maybeCreateIncrementalAgentBatch(actions) {
  const claimed = await incrementalAgentBatcher.runOnce();
  if (claimed.requests.length === 0) return claimed;
  actions.push({
    action: "enqueue-incremental-agent-batch",
    queue: queuesByRole.agentIncremental,
    count: claimed.requests.length,
    batch_size: incrementalAgentBatchSize,
    partial_flush: claimed.decision.partial,
    reason: claimed.decision.reason,
  });
  return claimed;
}

async function reconcileIncrementalAgentQueue(actions) {
  const jobs = await queues[queuesByRole.agentIncremental].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"],
    0,
    9999,
    true,
  );
  const activeBatchIds = [...new Set(jobs
    .map((job) => String(job.data?.batch_id ?? "").trim())
    .filter(Boolean))];
  const recovered = await incrementalAgentBacklogStore.recoverOrphans({
    activeBatchIds,
    queuedStaleSeconds: incrementalAgentQueuedStaleSeconds,
    runningStaleSeconds: incrementalAgentRunningStaleSeconds,
  });
  if (recovered.length > 0) {
    actions.push({
      action: "reconcile-orphaned-incremental-agent-requests",
      count: recovered.length,
      channel_ids: recovered.slice(0, 20),
    });
  }
  return recovered.length;
}

async function reconcileAgentQueue(actions, pipelineCycleId) {
  if (!pipelineCycleId) return 0;
  const jobs = await queues[queuesByRole.agentBatch].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"],
    0,
    9999,
    true,
  );
  const representedChannelIds = [...new Set(jobs
    .filter((job) => (
      job.data?.pipeline_cycle_id === pipelineCycleId
      || job.data?.dispatch_batch_id === pipelineCycleId
    ))
    .flatMap((job) => job.data?.channel_ids ?? [])
    .map(String)
    .filter(Boolean))];
  const rows = await query(
    `UPDATE crawler.channels channel
     SET agent_status=CASE WHEN channel.agent_status='running' THEN 'failed' ELSE 'pending' END,
         agent_next_retry_at=now(),
         agent_error_message=CASE
           WHEN channel.agent_status='running' THEN 'recovered orphaned running Agent claim'
           ELSE 'recovered orphaned queued Agent claim'
         END,
         updated_at=now()
     FROM crawler.channel_runs run
     WHERE run.run_id=channel.latest_run_id
       AND COALESCE(run.result_json->>'dispatch_batch_id',run.result_json->>'pipeline_cycle_id')=$1
       AND NOT (channel.channel_id=ANY($2::text[]))
       AND NOT EXISTS (
         SELECT 1 FROM crawler.agent_refresh_requests refresh
         WHERE refresh.channel_id=channel.channel_id
           AND (
             refresh.status IN ('pending','queued','running')
             OR (refresh.status='failed' AND isfinite(refresh.next_retry_at))
           )
       )
       AND (
         (channel.agent_status='queued' AND channel.updated_at<=now()-($3::int * interval '1 second'))
         OR
         (channel.agent_status='running' AND channel.updated_at<=now()-($4::int * interval '1 second'))
       )
     RETURNING channel.channel_id,channel.agent_status`,
    [pipelineCycleId, representedChannelIds, agentQueuedStaleSeconds, agentRunningStaleSeconds],
  );
  if (rows.rows.length > 0) {
    actions.push({
      action: "reconcile-orphaned-agent-claims",
      count: rows.rows.length,
      channel_ids: rows.rows.slice(0, 20).map((row) => row.channel_id),
    });
  }
  return rows.rows.length;
}

async function reconcileFinalizeQueue(actions, pipelineCycleId) {
  const inFlightJobs = await queues[queuesByRole.finalize].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"],
    0,
    9999,
    true,
  );
  const representedRunIds = new Set(representedFinalizeRunIds(inFlightJobs));
  const rows = await loadFinalizeRecoveryCandidates(query, {
    pipelineCycleId,
    limit: finalizeReconcileLimit,
  });
  let enqueued = 0;
  for (const row of rows) {
    if (representedRunIds.has(String(row.run_id))) continue;
    const sourceUpdatedAt = new Date(row.source_updated_at).toISOString();
    const jobId = safeJobId("finalize-reconcile", row.run_id, sourceUpdatedAt);
    const existing = await queues[queuesByRole.finalize].getJob(jobId);
    if (existing) {
      const state = await existing.getState();
      if (["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"].includes(state)) continue;
      try {
        await existing.remove();
      } catch {
        continue;
      }
    }
    await queues[queuesByRole.finalize].add(
      "finalize-channel",
      {
        channel_id: row.channel_id,
        run_id: row.run_id,
        reason: "controller-finalize-reconcile",
        pipeline_cycle_id: pipelineCycleId,
      },
      { jobId },
    );
    enqueued += 1;
  }
  if (enqueued > 0) {
    actions.push({
      action: "reconcile-finalize",
      count: enqueued,
      pipeline_cycle_id: pipelineCycleId,
    });
  }
  return enqueued;
}

async function maybeCreateDataApiBatches(
  actions,
  batchSize,
  stats,
  queryScheduler,
  dailyRequestLimit,
  dailyRequestCount,
) {
  if (!queryScheduler.pipeline_cycle_id) return;
  let upstreamDrained = automaticFinalizationActive(queryScheduler)
    && !hasQueueBacklog(stats, queuesByRole.discoverPage)
    && !hasQueueBacklog(stats, queuesByRole.channelCrawl)
    && !hasQueueBacklog(stats, queuesByRole.contentDetail);
  if (upstreamDrained) {
    upstreamDrained = !(await hasRepairableFinalRuns(queryScheduler.pipeline_cycle_id));
  }
  const queuedOrActiveBatches = backlog(stats, queuesByRole.dataApiBatch);
  const availableRequests = Math.max(0, dailyRequestLimit - dailyRequestCount - queuedOrActiveBatches);
  const maxBatches = Math.min(10, availableRequests);
  for (let index = 0; index < maxBatches; index += 1) {
    const rows = await query(
      `WITH candidates AS MATERIALIZED (
         SELECT task_id, source_content_id, created_at
         FROM crawler.youtube_api_tasks task
         WHERE task.status IN ('pending','failed')
           AND NOT (task.result_json ? 'parser_contract_error')
           AND (task.next_retry_at IS NULL OR task.next_retry_at <= now())
           AND EXISTS (
             SELECT 1
             FROM crawler.content_candidates cc
             JOIN crawler.channel_runs run ON run.run_id=cc.run_id
             WHERE cc.candidate_id=ANY(task.candidate_ids)
               AND run.result_json->>'pipeline_cycle_id'=$3::text
               AND NOT (run.result_json ? 'parser_contract_error')
               AND NOT (cc.result_json ? 'parser_contract_error')
           )
           AND NOT EXISTS (
             SELECT 1
             FROM crawler.content_candidates blocked
             WHERE blocked.candidate_id=ANY(task.candidate_ids)
               AND blocked.result_json ? 'parser_contract_error'
           )
         ORDER BY task.created_at ASC, task.task_id ASC
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       ),
       stats AS (
         SELECT count(*)::int AS count FROM candidates
       ),
       picked AS (
         SELECT task_id, source_content_id FROM candidates
         WHERE (SELECT count FROM stats) >= $1
            OR $2::boolean = true
       ),
       updated AS (
         UPDATE crawler.youtube_api_tasks t
         SET status='queued',updated_at=now()
         FROM picked
         WHERE t.task_id=picked.task_id
         RETURNING t.task_id,t.source_content_id
       )
       SELECT * FROM updated`,
      [batchSize, upstreamDrained, queryScheduler.pipeline_cycle_id],
    );
    if (rows.rows.length === 0) break;
    const batchId = `youtube-api:${Date.now()}:${nanoid(8)}`;
    const taskIds = rows.rows.map((row) => Number(row.task_id));
    const videoIds = rows.rows.map((row) => row.source_content_id);
    await query(
      `INSERT INTO crawler.youtube_api_batches (batch_id,status,task_ids,video_ids,updated_at)
       VALUES ($1,'queued',$2::bigint[],$3::text[],now())`,
      [batchId, taskIds, videoIds],
    );
    await query(
      `UPDATE crawler.content_candidates SET api_status='queued',updated_at=now()
       WHERE candidate_id IN (
         SELECT unnest(candidate_ids) FROM crawler.youtube_api_tasks WHERE task_id=ANY($1::bigint[])
       )
         AND NOT (result_json ? 'parser_contract_error')`,
      [taskIds],
    );
    await queues[queuesByRole.dataApiBatch].add(
      "youtube-data-api-batch",
      {
        batch_id: batchId,
        task_ids: taskIds,
        video_ids: videoIds,
        pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
      },
      { jobId: safeJobId("youtube-data-api", batchId) },
    );
    actions.push({ action: "enqueue-youtube-api-batch", batch_id: batchId, count: taskIds.length, batch_size: batchSize });
    if (rows.rows.length < batchSize) break;
  }
}

async function hasRepairableFinalRuns(pipelineCycleId = null) {
  const rows = await query(
    `WITH crawl_settings AS (
       SELECT GREATEST(
                0,
                LEAST(
                  3650,
                  COALESCE(
                    (
                      SELECT (value_json->>'content_max_age_days')::int
                      FROM crawler.settings
                      WHERE setting_key='crawl'
                      LIMIT 1
                    ),
                    90
                  )
                )
              )::int AS content_max_age_days
     )
     SELECT EXISTS (
       SELECT 1
       FROM crawler.channel_runs r
       JOIN crawler.channels c ON c.channel_id=r.channel_id AND c.latest_run_id=r.run_id
       JOIN crawler.content_candidates cc ON cc.run_id=r.run_id
       LEFT JOIN crawler.contents repair_content
         ON repair_content.content_key=cc.content_key
        AND repair_content.run_id=cc.run_id
       CROSS JOIN crawl_settings settings
       WHERE c.status='active'
         AND ($2::text IS NULL OR r.result_json->>'pipeline_cycle_id'=$2::text)
         AND NOT (r.result_json ? 'parser_contract_error')
         AND COALESCE((r.result_json #>> '{final_repair,rounds}')::int,0) < $1
         AND COALESCE(cc.result_json->'scope'->>'status','')<>'excluded'
         AND NOT (cc.result_json ? 'parser_contract_error')
         AND (
           settings.content_max_age_days=0
           OR repair_content.published_at IS NULL
           OR repair_content.published_at>=now()-(settings.content_max_age_days * interval '1 day')
         )
         AND ${finalRepairCandidateSql("cc")}
       LIMIT 1
     ) AS repairable`,
    [finalRepairMaxRounds, pipelineCycleId],
  );
  return Boolean(rows.rows[0]?.repairable);
}

async function getProxyCapacity() {
  try {
    const payload = await proxyControlClient().capacity();
    return normalizeRotaCapacity(payload, { catalog: managedIdentityPolicyCatalog });
  } catch (error) {
    return { active: null, cooldown: null, total: null, error: error?.message || String(error) };
  }
}

async function getChannelPressure(channelBacklog, proxyCapacity) {
  const rows = await query(
    `WITH recent AS (
       SELECT status,payload_json
       FROM crawler.task_events
       WHERE queue_name=$1
         AND status IN ('completed','failed')
         AND created_at >= now() - ($3::int * interval '1 second')
       ORDER BY created_at DESC
       LIMIT $2
     )
     SELECT
       count(*)::int AS terminal_samples,
       count(*) FILTER (WHERE status='completed')::int AS completed_samples,
       count(*) FILTER (WHERE status='failed')::int AS failed_samples,
       avg((payload_json->>'duration_ms')::numeric)
         FILTER (WHERE status='completed' AND payload_json ? 'duration_ms') AS average_duration_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY (payload_json->>'duration_ms')::numeric)
         FILTER (WHERE status='completed' AND payload_json ? 'duration_ms') AS p95_duration_ms
     FROM recent`,
    [queuesByRole.channelCrawl, channelPressureSampleSize, channelPressureWindowSeconds],
  );
  const sample = rows.rows[0] ?? {};
  const terminalSamples = Number(sample.terminal_samples ?? 0);
  const failedSamples = Number(sample.failed_samples ?? 0);
  const averageDurationMs = Number(sample.average_duration_ms);
  const ready = roleReady(proxyCapacity, "channel");
  const channelEtaSeconds = Number.isFinite(averageDurationMs) && Number.isFinite(ready) && ready > 0
    ? Math.round((Math.max(0, channelBacklog) * averageDurationMs) / ready / 1000)
    : null;
  return {
    terminal_samples: terminalSamples,
    completed_samples: Number(sample.completed_samples ?? 0),
    failed_samples: failedSamples,
    failure_rate: terminalSamples > 0 ? failedSamples / terminalSamples : 0,
    average_duration_ms: Number.isFinite(averageDurationMs) ? Math.round(averageDurationMs) : null,
    p95_duration_ms: Number.isFinite(Number(sample.p95_duration_ms)) ? Math.round(Number(sample.p95_duration_ms)) : null,
    eta_seconds: channelEtaSeconds,
    proxy_cooldown_ratio: proxyUnavailableRatio(proxyCapacity),
    sample_window_seconds: channelPressureWindowSeconds,
  };
}

function roleReady(capacity, role) {
  const value = capacity?.roles?.[role]?.ready;
  return Number.isFinite(value) ? Math.max(0, Number(value)) : null;
}

async function applyProxyConcurrency(capacity, stats, actions) {
  if (!Number.isFinite(capacity.active)) return null;
  const slotReady = {
    discover: roleReady(capacity, "discover"),
    channel: roleReady(capacity, "channel"),
    queryQuality: roleReady(capacity, "query_quality"),
    detail: roleReady(capacity, "detail"),
  };
  const hasSlotCapacity = Object.values(slotReady).every(Number.isFinite);
  const demand = {
    discover: hasQueueBacklog(stats, queuesByRole.discoverPage),
    channel: hasQueueBacklog(stats, queuesByRole.channelCrawl),
    queryQuality: hasQueueBacklog(stats, queuesByRole.queryQuality),
    detail: hasQueueBacklog(stats, queuesByRole.contentDetail),
  };
  if (channelInlineDetails && Number.isFinite(slotReady.discover) && Number.isFinite(slotReady.channel)) {
    const assignments = {
      [queuesByRole.discoverPage]: Math.max(1, demand.discover ? slotReady.discover : 1),
      [queuesByRole.channelCrawl]: Math.max(1, demand.channel ? slotReady.channel : 1),
      [queuesByRole.queryQuality]: Math.max(1, demand.queryQuality ? slotReady.queryQuality : 1),
      [queuesByRole.contentDetail]: 1,
    };
    for (const [queueName, concurrency] of Object.entries(assignments)) {
      const previous = await queues[queueName].getGlobalConcurrency();
      if (previous !== concurrency) {
        await queues[queueName].setGlobalConcurrency(concurrency);
        actions.push({ action: "set-global-concurrency", queue: queueName, concurrency, previous });
      }
    }
    return {
      mode: "channel-inline-worker-slots",
      active: capacity.active,
      ready: slotReady,
      demand,
      assignments,
    };
  }
  if (hasSlotCapacity) {
    const assignments = {
      [queuesByRole.discoverPage]: Math.max(1, demand.discover ? slotReady.discover : 1),
      [queuesByRole.channelCrawl]: Math.max(1, demand.channel ? slotReady.channel : 1),
      [queuesByRole.queryQuality]: Math.max(1, demand.queryQuality ? slotReady.queryQuality : 1),
      [queuesByRole.contentDetail]: Math.max(1, demand.detail ? slotReady.detail : 1),
    };
    for (const [queueName, concurrency] of Object.entries(assignments)) {
      const previous = await queues[queueName].getGlobalConcurrency();
      if (previous !== concurrency) {
        await queues[queueName].setGlobalConcurrency(concurrency);
        actions.push({ action: "set-global-concurrency", queue: queueName, concurrency, previous });
      }
    }
    return { mode: "worker-slots", active: capacity.active, ready: slotReady, demand, assignments };
  }
  const reserve = Math.min(3, Math.floor(capacity.active / 4));
  const budget = Math.max(1, capacity.active - reserve);
  const discoverAllocation = demand.discover ? 1 : 0;
  const channelAllocation = demand.channel
    ? Math.max(1, Math.min(2, budget - discoverAllocation))
    : 0;
  const detailAllocation = demand.detail
    ? Math.max(1, budget - discoverAllocation - channelAllocation)
    : 0;
  const assignments = {
    [queuesByRole.discoverPage]: Math.max(1, discoverAllocation),
    [queuesByRole.channelCrawl]: Math.max(1, channelAllocation),
    [queuesByRole.queryQuality]: 1,
    [queuesByRole.contentDetail]: Math.max(1, detailAllocation),
  };
  for (const [queueName, concurrency] of Object.entries(assignments)) {
    const previous = await queues[queueName].getGlobalConcurrency();
    if (previous !== concurrency) {
      await queues[queueName].setGlobalConcurrency(concurrency);
      actions.push({ action: "set-global-concurrency", queue: queueName, concurrency, previous });
    }
  }
  return { active: capacity.active, reserve, budget, demand, assignments };
}

function discoveryRunIdFromPageId(pageId, pageNo) {
  const suffix = `:page:${pageNo}`;
  const text = String(pageId || "");
  return text.endsWith(suffix) ? text.slice(0, -suffix.length) : text;
}

function discoveryPageId(discoveryRunId, pageNo) {
  return `${discoveryRunId}:page:${pageNo}`;
}

async function enqueueDiscoverPage({
  queryId,
  queryText,
  language,
  country,
  category,
  pageNo,
  pageId,
  discoveryRunId,
  pipelineCycleId,
  dispatchBatchId,
  continuationToken = null,
  ytConfig = null,
  priority = 100,
}) {
  const persisted = await managedJobIntentStore.prepareDiscoverPage({
    queryId,
    queryText,
    language,
    country,
    category,
    pageNo,
    pageId,
    discoveryRunId,
    pipelineCycleId,
    dispatchBatchId,
    continuationToken,
    continuationParentPageId: continuationToken && pageNo > 1
      ? discoveryPageId(discoveryRunId, pageNo - 1)
      : null,
    ytConfig,
    priority,
    searchFilter: "video",
    sort: "popularity",
    timeWindow: "this_year",
  });
  const dispatch = await managedJobOutboxDispatcher.dispatchAvailable({ limit: 100 });
  const page = await query(
    "SELECT dispatch_status,dispatched_job_id FROM crawler.query_pages WHERE page_id=$1",
    [pageId],
  );
  return {
    created: persisted.created,
    staged: true,
    enqueued: page.rows[0]?.dispatch_status === "enqueued",
    job_id: page.rows[0]?.dispatched_job_id ?? null,
    dispatch,
  };
}

async function enqueueResumableDiscoveryPages(scheduler, limit, actions) {
  if (limit <= 0) return 0;
  const rows = await query(
    `SELECT
       qp.page_id,
       qp.query_id,
       qp.query_text,
       qp.page_no,
       qp.priority AS page_priority,
       qp.result_json,
       qt.language,
       qt.country,
       qt.category,
       qt.priority AS query_priority
     FROM crawler.query_pages qp
     JOIN crawler.query_terms qt ON qt.query_id = qp.query_id
     WHERE qp.status = 'done'
       AND qp.managed_fetch_status = 'done'
       AND qp.qualification_status = 'done'
       AND qp.should_continue = true
       AND qp.result_json ? 'next_continuation_token'
       AND COALESCE(qp.result_json #>> '{yt_config,apiKey}', '') <> ''
       AND qp.result_json->>'pipeline_cycle_id' = $4::text
       AND ($1::bigint IS NULL OR qt.query_set_id = $1::bigint)
       AND qt.quality_score IS NOT NULL
       AND qt.quality_status NOT IN ('unscored', 'failed')
       AND COALESCE(qt.quality_score, 0) >= $3::numeric
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.query_pages next_page
         WHERE next_page.page_id = regexp_replace(
           qp.page_id,
           ':page:' || qp.page_no::text || '$',
           ':page:' || (qp.page_no + 1)::text
         )
       )
     ORDER BY qp.finished_at ASC NULLS LAST, qp.created_at ASC
     LIMIT $2`,
    [
      scheduler.query_set_id,
      Math.max(limit * 8, limit),
      scheduler.query_quality_min_score,
      scheduler.pipeline_cycle_id,
    ],
  );

  let created = 0;
  for (const row of rows.rows) {
    if (created >= limit) break;
    const nextPageNo = Number(row.page_no) + 1;
    const discoveryRunId = discoveryRunIdFromPageId(row.page_id, row.page_no);
    const nextPageId = discoveryPageId(discoveryRunId, nextPageNo);
    const existingPage = await query("SELECT 1 FROM crawler.query_pages WHERE page_id = $1 LIMIT 1", [nextPageId]);
    if (existingPage.rows.length > 0) continue;
    const continuationToken = row.result_json?.next_continuation_token;
    const ytConfig = row.result_json?.yt_config;
    if (!continuationToken || !ytConfig?.apiKey) continue;

    const result = await enqueueDiscoverPage({
      queryId: row.query_id,
      queryText: row.query_text,
      language: row.language,
      country: row.country,
      category: row.category,
      pageNo: nextPageNo,
      pageId: nextPageId,
      discoveryRunId,
      pipelineCycleId: scheduler.pipeline_cycle_id,
      dispatchBatchId: scheduler.pipeline_cycle_id,
      continuationToken,
      ytConfig,
      priority: Number(row.query_priority || row.page_priority || 100),
    });
    if (result.created) {
      await addDispatchQuery(scheduler.pipeline_cycle_id, row.query_id);
      created += 1;
      actions.push({
        action: "enqueue-discovery-continuation",
        query_id: row.query_id,
        page_no: nextPageNo,
        page_id: nextPageId,
      });
    }
  }
  return created;
}

async function resolvePendingDiscoveryPageQualifications(pipelineCycleId, actions) {
  if (!pipelineCycleId) return 0;
  const rows = await query(
    `SELECT
       page.page_id,
       page.query_id,
       page.query_text,
       page.candidate_count,
       page.result_json,
       count(DISTINCT candidate.candidate_id)
         FILTER (WHERE candidate.status='accepted')::int AS accepted_count,
       count(DISTINCT candidate.candidate_id)
         FILTER (WHERE candidate.status='existing')::int AS existing_count,
       count(DISTINCT candidate.candidate_id)
         FILTER (WHERE candidate.status='rejected')::int AS rejected_count,
       count(DISTINCT candidate.candidate_id)
         FILTER (WHERE candidate.status='failed')::int AS failed_count,
       count(DISTINCT candidate.candidate_id)
         FILTER (WHERE candidate.status IN ('discovered','queued','validating'))::int AS pending_count,
       count(DISTINCT candidate.candidate_id)
         FILTER (WHERE candidate.snapshot_json ? 'parser_contract_error')::int AS parser_failure_count,
       count(DISTINCT source.candidate_id)::int AS sourced_count
     FROM crawler.query_pages page
     JOIN crawler.channel_candidate_sources source ON source.page_id=page.page_id
     JOIN crawler.channel_candidates candidate ON candidate.candidate_id=source.candidate_id
     WHERE page.managed_fetch_status='done'
       AND page.qualification_status='pending'
       AND page.result_json->>'pipeline_cycle_id'=$1
     GROUP BY page.page_id
     HAVING count(DISTINCT candidate.candidate_id)
              FILTER (WHERE candidate.status IN ('discovered','queued','validating'))=0
        AND count(DISTINCT source.candidate_id)>=page.candidate_count
     ORDER BY page.created_at ASC
     LIMIT 500`,
    [pipelineCycleId],
  );

  for (const row of rows.rows) {
    const minQualifiedRatio = Number(
      row.result_json?.discover_stop_min_qualified_ratio ?? (1 / 3),
    );
    const hasContinuation = Boolean(
      row.result_json?.next_continuation_token
      && row.result_json?.yt_config?.apiKey,
    );
    const resolution = resolveDiscoveryPageQualification({
      acceptedCount: row.accepted_count,
      existingCount: row.existing_count,
      rejectedCount: row.rejected_count,
      failedCount: row.failed_count,
      pendingCount: row.pending_count,
      hasContinuation,
      minQualifiedRatio,
    });
    if (!resolution.settled) continue;

    const parserFailureCount = Number(row.parser_failure_count ?? 0);
    const resultPatch = {
      qualification_phase: resolution.failed ? "failed" : "complete",
      pending_qualification_channel_ids: [],
      qualified_for_stop: resolution.qualified,
      unqualified_for_stop: resolution.rejected,
      failed_for_stop: Number(row.failed_count ?? 0),
      qualified_ratio: resolution.qualifiedRatio,
      qualification_resolved_at: new Date().toISOString(),
      next_page_enqueued: false,
      ...(parserFailureCount > 0
        ? {
            parser_contract_error: {
              field: "subscriber_count",
              source: "channel_snapshot",
              reason: "discovery_page_snapshot_parser_failure",
              count: parserFailureCount,
            },
          }
        : {}),
    };
    await query(
      `UPDATE crawler.query_pages
       SET status=$2,
           qualification_status=CASE WHEN $2='failed' THEN 'failed' ELSE 'done' END,
           qualification_finished_at=now(),
           qualification_error_code=CASE WHEN $2='failed' THEN 'snapshot_validation_failed' ELSE NULL END,
           accepted_count=$3,
           unqualified_ratio=$4,
           should_continue=$5,
           stop_reason=$6,
           error_message=$7,
           result_json=result_json || $8::jsonb,
           finished_at=now(),
           updated_at=now()
       WHERE page_id=$1
         AND managed_fetch_status='done'
         AND qualification_status='pending'`,
      [
        row.page_id,
        resolution.failed ? "failed" : "done",
        resolution.qualified,
        resolution.unqualifiedRatio,
        resolution.shouldContinue,
        resolution.stopReason,
        resolution.failed
          ? `${Number(row.failed_count ?? 0)} channel snapshot validation(s) failed`
          : null,
        JSON.stringify(resultPatch),
      ],
    );
    if (!resolution.shouldContinue && !resolution.failed && row.query_id) {
      await query(
        `UPDATE crawler.query_terms
         SET next_crawl_at=now()+make_interval(secs => crawl_interval_sec),updated_at=now()
         WHERE query_id=$1`,
        [row.query_id],
      );
    }
    actions.push({
      action: resolution.failed
        ? "fail-discovery-page-qualification"
        : "resolve-discovery-page-qualification",
      page_id: row.page_id,
      query_id: row.query_id,
      candidate_count: Number(row.sourced_count ?? row.candidate_count ?? 0),
      qualified_count: resolution.qualified,
      rejected_count: resolution.rejected,
      failed_count: Number(row.failed_count ?? 0),
      qualified_ratio: resolution.qualifiedRatio,
      should_continue: resolution.shouldContinue,
      stop_reason: resolution.stopReason,
    });
  }
  return rows.rows.length;
}

async function pendingDiscoveryQualificationCount(scheduler) {
  if (!scheduler.pipeline_cycle_id) return 0;
  const rows = await query(
    `SELECT count(*)::int AS count
     FROM crawler.query_pages
     WHERE managed_fetch_status='done'
       AND qualification_status='pending'
       AND result_json->>'pipeline_cycle_id'=$1`,
    [scheduler.pipeline_cycle_id],
  );
  return Number(rows.rows[0]?.count ?? 0);
}

async function pendingDiscoveryContinuationCount(scheduler) {
  if (!scheduler.pipeline_cycle_id) return 0;
  const rows = await query(
    `SELECT count(*)::int AS count
     FROM crawler.query_pages page
     WHERE page.managed_fetch_status='done'
       AND page.qualification_status='done'
       AND page.status='done'
       AND page.should_continue=true
       AND page.result_json ? 'next_continuation_token'
       AND COALESCE(page.result_json #>> '{yt_config,apiKey}','')<>''
       AND page.result_json->>'pipeline_cycle_id'=$1
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.query_pages next_page
         WHERE next_page.page_id=regexp_replace(
           page.page_id,
           ':page:' || page.page_no::text || '$',
           ':page:' || (page.page_no + 1)::text
         )
       )`,
    [scheduler.pipeline_cycle_id],
  );
  return Number(rows.rows[0]?.count ?? 0);
}

async function enqueueDueQueryStarts(scheduler, limit, actions) {
  if (limit <= 0) return 0;
  const rows = await query(
    `SELECT qt.*
     FROM crawler.query_terms qt
     WHERE qt.next_crawl_at <= now()
       AND ($1::bigint IS NULL OR qt.query_set_id = $1::bigint)
       AND qt.quality_score IS NOT NULL
       AND qt.quality_status NOT IN ('unscored', 'failed')
       AND COALESCE(qt.quality_score, 0) >= $3::numeric
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.query_pages qp
         WHERE qp.query_id = qt.query_id
           AND qp.status IN ('queued', 'running')
       )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.query_pages qp
         WHERE qp.query_id = qt.query_id
           AND qp.status = 'done'
           AND qp.should_continue = true
           AND qp.result_json ? 'next_continuation_token'
           AND COALESCE(qp.result_json #>> '{yt_config,apiKey}', '') <> ''
           AND right(qp.page_id, length(':page:' || qp.page_no::text)) = ':page:' || qp.page_no::text
           AND NOT EXISTS (
             SELECT 1
             FROM crawler.query_pages qp2
             WHERE qp2.page_id = regexp_replace(
               qp.page_id,
               ':page:' || qp.page_no::text || '$',
               ':page:' || (qp.page_no + 1)::text
             )
           )
       )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.query_pages qp
         WHERE qp.query_id = qt.query_id
           AND qp.status = 'failed'
           AND qp.result_json ? 'parser_contract_error'
       )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.query_pages qp
         WHERE qp.query_id = qt.query_id
           AND qp.status = 'failed'
           AND qp.updated_at > now() - ($4::int * interval '1 second')
       )
     ORDER BY qt.priority DESC, qt.next_crawl_at ASC, qt.query_id ASC
     LIMIT $2`,
    [scheduler.query_set_id, limit, scheduler.query_quality_min_score, discoverFailedRetrySeconds],
  );

  let created = 0;
  for (const row of rows.rows) {
    const pageNo = 1;
    const discoveryRunId = `query:${row.query_id}:run:${Date.now()}:${nanoid(8)}`;
    const pageId = discoveryPageId(discoveryRunId, pageNo);
    const result = await enqueueDiscoverPage({
      queryId: row.query_id,
      queryText: row.query_text,
      language: row.language,
      country: row.country,
      category: row.category,
      pageNo,
      pageId,
      discoveryRunId,
      pipelineCycleId: scheduler.pipeline_cycle_id,
      dispatchBatchId: scheduler.pipeline_cycle_id,
      priority: row.priority,
    });
    if (result.created) {
      await addDispatchQuery(scheduler.pipeline_cycle_id, row.query_id);
      created += 1;
      actions.push({
        action: "enqueue-discovery-query",
        query_id: row.query_id,
        page_no: pageNo,
        page_id: pageId,
      });
    }
  }
  return created;
}

async function deferredQueryRetryAt(scheduler) {
  const rows = await query(
    `SELECT min(qp.updated_at + ($2::int * interval '1 second')) AS retry_at
     FROM crawler.query_pages qp
     JOIN crawler.query_terms qt ON qt.query_id=qp.query_id
     WHERE qp.status='failed'
       AND NOT (qp.result_json ? 'parser_contract_error')
       AND qp.updated_at > now() - ($2::int * interval '1 second')
       AND qt.next_crawl_at <= now()
       AND ($1::bigint IS NULL OR qt.query_set_id=$1::bigint)
       AND qt.quality_score IS NOT NULL
       AND qt.quality_status NOT IN ('unscored','failed')
       AND COALESCE(qt.quality_score,0) >= $3::numeric
       AND NOT EXISTS (
         SELECT 1 FROM crawler.query_pages open_page
         WHERE open_page.query_id=qt.query_id
           AND open_page.status IN ('queued','running')
       )`,
    [scheduler.query_set_id, discoverFailedRetrySeconds, scheduler.query_quality_min_score],
  );
  return rows.rows[0]?.retry_at ?? null;
}

async function pendingQueryQualityCount(scheduler) {
  const rows = await query(
    `SELECT count(*)::int AS count
     FROM crawler.query_quality_tasks task
     JOIN crawler.query_quality_batches batch USING (quality_batch_id)
     JOIN crawler.query_terms term ON term.query_id=task.query_id
     WHERE task.status IN ('queued','running')
       AND batch.status IN ('queued','running')
       AND ($1::bigint IS NULL OR term.query_set_id=$1::bigint)`,
    [scheduler.query_set_id],
  );
  return Number(rows.rows[0]?.count ?? 0);
}

async function parserContractFailureCounts(scheduler) {
  const rows = await query(
    `SELECT
       (
         SELECT count(*)::int
         FROM crawler.query_terms term
         WHERE ($1::bigint IS NULL OR term.query_set_id=$1::bigint)
           AND term.quality_json ? 'parser_contract_error'
       ) AS query_quality,
       (
         SELECT count(*)::int
         FROM crawler.query_pages page
         WHERE page.result_json->>'pipeline_cycle_id'=$2::text
           AND page.result_json ? 'parser_contract_error'
       ) AS discover,
       (
         SELECT count(*)::int
         FROM crawler.channel_candidates candidate
         WHERE candidate.dispatch_batch_id=$2::text
           AND candidate.snapshot_json ? 'parser_contract_error'
       ) AS channel_snapshot,
       (
         SELECT count(*)::int
         FROM crawler.channel_runs run
         WHERE run.result_json->>'pipeline_cycle_id'=$2::text
           AND run.result_json ? 'parser_contract_error'
       ) AS channel_run,
       (
         SELECT count(*)::int
         FROM crawler.content_candidates candidate
         JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
         WHERE run.result_json->>'pipeline_cycle_id'=$2::text
           AND candidate.result_json ? 'parser_contract_error'
       ) AS content_detail`,
    [scheduler.query_set_id, scheduler.pipeline_cycle_id],
  );
  const counts = Object.fromEntries(Object.entries(rows.rows[0] ?? {})
    .map(([key, value]) => [key, Number(value ?? 0)]));
  return {
    ...counts,
    total: Object.values(counts).reduce((sum, value) => sum + value, 0),
  };
}

async function maybeScheduleQuerySlice(actions, rawScheduler, stats) {
  const scheduler = normalizeQueryScheduler(rawScheduler);
  if (!querySchedulerAllowsDiscovery(scheduler)) return;
  await ensureDispatchBatch(scheduler);

  const queueName = queuesByRole.discoverPage;
  // Pressure-paused queues still accept bounded jobs. More importantly, the
  // drained check below must run so a paused Discover queue cannot block finalization.
  const discoverBacklog = backlog(stats, queueName);
  const slots = Math.min(
    scheduler.chunk_size,
    Math.max(0, scheduler.max_discover_backlog - discoverBacklog),
  );
  if (slots <= 0) {
    actions.push({
      action: "skip-query-slice",
      reason: "discover_backlog_limit",
      discover_backlog: discoverBacklog,
      max_discover_backlog: scheduler.max_discover_backlog,
    });
    return;
  }

  const resumed = await enqueueResumableDiscoveryPages(scheduler, slots, actions);
  const remaining = slots - resumed;
  const started = remaining > 0 ? await enqueueDueQueryStarts(scheduler, remaining, actions) : 0;
  if (resumed + started === 0 && discoverBacklog === 0) {
    const pendingQuality = await pendingQueryQualityCount(scheduler);
    if (pendingQuality > 0) {
      actions.push({ action: "wait-query-quality", pending_tasks: pendingQuality });
      return;
    }
    const pendingDiscoveryQualification = await pendingDiscoveryQualificationCount(scheduler);
    if (pendingDiscoveryQualification > 0) {
      actions.push({
        action: "wait-discovery-page-qualification",
        pending_pages: pendingDiscoveryQualification,
      });
      return;
    }
    const pendingDiscoveryContinuation = await pendingDiscoveryContinuationCount(scheduler);
    if (pendingDiscoveryContinuation > 0) {
      actions.push({
        action: "wait-discovery-continuation",
        pending_pages: pendingDiscoveryContinuation,
      });
      return;
    }
    const retryAt = await deferredQueryRetryAt(scheduler);
    if (retryAt) {
      actions.push({ action: "wait-discovery-retry", retry_at: retryAt });
      return;
    }
    await query(
      `UPDATE crawler.settings
       SET value_json = value_json
         || jsonb_build_object(
              'status', 'finishing',
              'stopped_at', NULL,
              'updated_at', $2::text,
              'updated_by', 'controller',
              'stop_reason', 'upstream_drained'
            ),
           updated_at = now()
       WHERE setting_key = $1`,
      [QUERY_SCHEDULER_KEY, new Date().toISOString()],
    );
    await closeDispatchDiscovery(scheduler.pipeline_cycle_id);
    actions.push({ action: "finish-query-discovery", reason: "no_schedulable_query" });
  }
}

async function channelRepairJobs() {
  return queues[queuesByRole.channelCrawl].getJobs(
    ["waiting", "active", "delayed", "prioritized"],
    0,
    999,
    true,
  );
}

async function maybeDispatchContentCompletenessRepairs(
  actions,
  stats,
  queryScheduler,
  proxyCapacity,
  now = Date.now(),
) {
  if (!automaticFinalizationActive(queryScheduler)) return 0;
  if (!queryScheduler.pipeline_cycle_id) return 0;
  if (now - lastContentRepairAt < contentRepairIntervalMs) return 0;
  if (
    hasQueueBacklog(stats, queuesByRole.discoverPage)
    || hasQueueBacklog(stats, queuesByRole.contentDetail)
    || hasQueueBacklog(stats, queuesByRole.dataApiBatch)
    || hasQueueBacklog(stats, queuesByRole.finalize)
  ) return 0;
  const ready = roleReady(proxyCapacity, "channel");
  if (Number.isFinite(ready) && ready <= 0) return 0;

  const jobs = await channelRepairJobs();
  const repairNames = new Set([
    "channel-detail-repair",
    "channel-crawl-repair",
    "channel-checkpoint-repair",
  ]);
  if (jobs.some((job) => !repairNames.has(job.name))) return 0;
  const dispatchLimit = repairDispatchCapacity({
    ready,
    inFlight: jobs.length,
    maximum: contentRepairBatchSize,
  });
  if (dispatchLimit <= 0) return 0;
  lastContentRepairAt = now;

  const liveRuns = await withTransaction((client) => reconcileLiveDurationNotApplicable(
    client.query.bind(client),
    queryScheduler.pipeline_cycle_id,
  ));
  if (liveRuns.length > 0) {
    for (const row of liveRuns) {
      await queues[queuesByRole.finalize].add(
        "finalize-live-duration-not-applicable",
        {
          channel_id: row.channel_id,
          run_id: row.run_id,
          reason: "live-duration-not-applicable",
          pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
        },
        { jobId: safeJobId("finalize-live-duration-na", CONTENT_COMPLETENESS_REPAIR_VERSION, row.run_id) },
      );
    }
    actions.push({
      action: "reconcile-live-duration-not-applicable",
      count: liveRuns.length,
    });
    return liveRuns.length;
  }

  const batchId = `auto-repair:${Date.now()}:${nanoid(8)}`;
  const targets = await loadContentRepairTargets(query, {
    repairVersion: CONTENT_COMPLETENESS_REPAIR_VERSION,
    limitRuns: dispatchLimit,
    pipelineCycleId: queryScheduler.pipeline_cycle_id,
  });
  const targetCount = targets.detailRuns.length + targets.channelRuns.length + targets.staleRuns.length;
  if (targetCount === 0) return 0;
  await prepareContentRepairTargets(query, targets, {
    repairVersion: CONTENT_COMPLETENESS_REPAIR_VERSION,
    batchId,
    pipelineCycleId: queryScheduler.pipeline_cycle_id,
  });
  const enqueued = await enqueueContentRepairTargets(query, queues, targets, {
    repairVersion: CONTENT_COMPLETENESS_REPAIR_VERSION,
    batchId,
    pipelineCycleId: queryScheduler.pipeline_cycle_id,
  });
  if (queryScheduler.status !== "repairing") {
    await updateSchedulerRuntime("repairing", {
      stop_reason: "automatic_content_repair",
      repair_started_at: new Date().toISOString(),
    });
  }
  actions.push({
    action: "enqueue-content-completeness-repair",
    batch_id: batchId,
    repair_version: CONTENT_COMPLETENESS_REPAIR_VERSION,
    candidates: targets.detailRows.length,
    runs: targets.detailRuns.length,
    enqueued,
    repair_capacity: {
      ready: Number.isFinite(ready) ? ready : null,
      in_flight: jobs.length,
      dispatched_limit: dispatchLimit,
    },
  });
  return targetCount;
}

async function maybeCompleteAutomaticPipeline(actions, queryScheduler) {
  if (!automaticFinalizationActive(queryScheduler)) return false;
  if (!queryScheduler.pipeline_cycle_id) return false;
  const freshStats = await getQueueStats(queues);
  if (hasQueryPipelineQueueBacklog(freshStats)) return false;
  await reconcileTerminalFinalizedRunStates(actions, queryScheduler.pipeline_cycle_id);
  const fullRepair = await loadFullRepairCompletionState(query, queryScheduler.pipeline_cycle_id);
  if (fullRepair && !fullRepair.complete) {
    actions.push({ action: "wait-full-repair-targets", ...fullRepair });
    return false;
  }
  if (await hasOpenPipelineCrawlerWork(query, queryScheduler.pipeline_cycle_id)) return false;
  if (await hasPendingContentRepairs(
    query,
    CONTENT_COMPLETENESS_REPAIR_VERSION,
    queryScheduler.pipeline_cycle_id,
  )) return false;
  if (await hasRepairableFinalRuns(queryScheduler.pipeline_cycle_id)) return false;
  const parserFailures = await parserContractFailureCounts(queryScheduler);
  if (parserFailures.total > 0) {
    if (queryScheduler.stop_reason !== "parser_contract_error") {
      await updateSchedulerRuntime("finishing", {
        stop_reason: "parser_contract_error",
        parser_failure_counts: parserFailures,
      });
    }
    actions.push({ action: "wait-parser-contract-fix", ...parserFailures });
    return false;
  }
  const blockers = await loadPipelineFinalizeBlockers(query, queryScheduler.pipeline_cycle_id);
  if (blockers.agentOpen > 0 || blockers.finalOpen > 0 || blockers.publicationOpen > 0) return false;
  const completedAt = new Date().toISOString();
  const completion = await settleCompletedMigrationBatch({
    withTransaction,
    batchId: queryScheduler.pipeline_cycle_id,
    completedAt,
    schedulerMetadata: { parser_failure_counts: parserFailures },
    maxSnapshotAttempts: channelSnapshotMaxAttempts,
  });
  if (!completion) return false;
  actions.push({
    action: "complete-automatic-pipeline",
    completed_at: completedAt,
    outcome: completion.outcome,
    statistics: {
      total: completion.total,
      accepted: completion.accepted,
      rejected: completion.rejected,
      failed: completion.failed,
    },
  });
  return true;
}

async function maybeRepairFailedChannelRuns(actions, stats, queryScheduler, proxyCapacity, now = Date.now()) {
  if (now - lastFinalRepairAt < finalRepairIntervalMs) return 0;
  if (!automaticFinalizationActive(queryScheduler)) return 0;
  if (!queryScheduler.pipeline_cycle_id) return 0;
  if (
    hasQueueBacklog(stats, queuesByRole.contentDetail)
    || hasQueueBacklog(stats, queuesByRole.dataApiBatch)
    || hasQueueBacklog(stats, queuesByRole.finalize)
  ) return 0;
  const ready = roleReady(proxyCapacity, "channel");
  if (Number.isFinite(ready) && ready <= 0) return 0;
  const channelJobs = await channelRepairJobs();
  const repairNames = new Set([
    "channel-detail-repair",
    "channel-crawl-repair",
    "channel-checkpoint-repair",
  ]);
  if (channelJobs.some((job) => !repairNames.has(job.name))) return 0;
  const {
    runIds: activeRepairRunIds,
    publicationGapRootRunIds: activePublicationGapRootRunIds,
  } = activeFinalRepairExclusions(channelJobs);
  const dispatchLimit = repairDispatchCapacity({
    ready,
    inFlight: channelJobs.length,
    maximum: finalRepairBatchSize,
  });
  if (dispatchLimit <= 0) return 0;
  lastFinalRepairAt = now;

  const rows = await query(
    `WITH crawl_settings AS (
       SELECT GREATEST(
                0,
                LEAST(
                  3650,
                  COALESCE(
                    (
                      SELECT (value_json->>'content_max_age_days')::int
                      FROM crawler.settings
                      WHERE setting_key='crawl'
                      LIMIT 1
                    ),
                    90
                  )
                )
              )::int AS content_max_age_days
     ), eligible_candidates AS (
       SELECT cc.*
       FROM crawler.content_candidates cc
       LEFT JOIN crawler.contents repair_content
         ON repair_content.content_key=cc.content_key
        AND repair_content.run_id=cc.run_id
       CROSS JOIN crawl_settings settings
       WHERE COALESCE(cc.result_json->'scope'->>'status','')<>'excluded'
         AND NOT (cc.result_json ? 'parser_contract_error')
         AND (
           settings.content_max_age_days=0
           OR repair_content.published_at IS NULL
           OR repair_content.published_at>=now()-(settings.content_max_age_days * interval '1 day')
         )
     )
     SELECT r.run_id,r.channel_id,r.candidate_id,r.crawl_mode,r.status,r.detail_status,r.result_json,
            c.channel_url,c.subscriber_count,c.registry_promotion_run_id,
            count(cc.*) FILTER (
              WHERE cc.detail_status='failed'
                AND ${finalRepairCandidateSql("cc")}
            )::int AS failed_candidates,
            count(cc.*) FILTER (
              WHERE COALESCE(cc.result_json->'scope'->>'status','')<>'excluded'
                AND ${finalRepairCandidateSql("cc")}
            )::int AS repairable_candidates,
            count(cc.*) FILTER (
              WHERE cc.missing_fields @> ARRAY['content_type']::text[]
                AND ${finalRepairCandidateSql("cc")}
            )::int AS type_missing_candidates,
            count(cc.*) FILTER (
              WHERE ${preparedFinalDetailRepairSql("cc", "r")}
            )::int AS prepared_detail_candidates,
            count(cc.*) FILTER (
              WHERE cc.result_json ? 'repair_once_version'
            )::int AS strict_repair_candidates,
            max(cc.result_json->>'repair_once_version') FILTER (
              WHERE cc.result_json ? 'repair_once_version'
            ) AS strict_repair_version,
            COALESCE((r.result_json #>> '{final_repair,rounds}')::int,0) AS repair_rounds,
            COALESCE((r.result_json #>> '{checkpoint_repair,rounds}')::int,0)
              AS checkpoint_repair_rounds
     FROM crawler.channel_runs r
     JOIN crawler.channels c ON c.channel_id=r.channel_id AND c.latest_run_id=r.run_id
     LEFT JOIN eligible_candidates cc ON cc.run_id=r.run_id
     WHERE c.status='active'
       AND r.result_json->>'pipeline_cycle_id'=$4::text
       AND NOT (r.result_json ? 'parser_contract_error')
       AND r.updated_at <= now()-interval '30 seconds'
       AND ${finalRepairRoundEligibilitySql("r", {
         finalRepairMaxRoundsParameter: "$1",
         checkpointRepairMaxRoundsParameter: "$5",
       })}
       AND NOT (r.run_id = ANY($3::text[]))
     GROUP BY r.run_id,c.channel_url,c.subscriber_count,c.registry_promotion_run_id
     HAVING r.status='failed'
         OR r.detail_status='failed'
         OR count(cc.*) FILTER (
              WHERE COALESCE(cc.result_json->'scope'->>'status','')<>'excluded'
                AND ${finalRepairCandidateSql("cc")}
            ) > 0
         OR count(cc.*) FILTER (
              WHERE ${preparedFinalDetailRepairSql("cc", "r")}
            ) > 0
     ORDER BY r.updated_at ASC
     LIMIT $2`,
    [
      finalRepairMaxRounds,
      dispatchLimit,
      activeRepairRunIds,
      queryScheduler.pipeline_cycle_id,
      finalCheckpointRepairMaxRounds,
    ],
  );

  const publicationGapRows = await loadPublicationGapRepairCandidates(query, {
    pipelineCycleId: queryScheduler.pipeline_cycle_id,
    limit: dispatchLimit,
    excludedRunIds: activeRepairRunIds,
    excludedPublicationGapRootRunIds: activePublicationGapRootRunIds,
    maxRounds: finalRepairMaxRounds,
  });
  const repairRows = mergeFinalRepairCandidates({
    standardRows: rows.rows,
    publicationGapRows,
    limit: dispatchLimit,
  });

  for (const row of repairRows) {
    const repairableCandidates = Number(row.repairable_candidates ?? 0);
    const typeMissingCandidates = Number(row.type_missing_candidates ?? 0);
    const preparedDetailCandidates = Number(row.prepared_detail_candidates ?? 0);
    const strictRepair = Number(row.strict_repair_candidates ?? 0) > 0;
    const strictRepairVersion = strictRepair
      ? String(row.strict_repair_version || CONTENT_COMPLETENESS_REPAIR_VERSION)
      : null;
    const publicationGap = row.publication_gap === true;
    const roundDecision = finalRepairRoundDecision({
      finalRepairRounds: row.repair_rounds,
      finalRepairMaxRounds,
      proxyControlStatus: publicationGap
        ? null
        : row.result_json?.proxy_control?.status,
      checkpointRepairRounds: row.checkpoint_repair_rounds,
      checkpointRepairMaxRounds: finalCheckpointRepairMaxRounds,
    });
    if (!roundDecision.eligible) continue;
    const {
      businessRunBudgetExhausted,
      finalRepairRound: round,
      checkpointRepairRound,
    } = roundDecision;
    const dispatch = finalRepairDispatchDecision({
      publicationGap,
      businessRunBudgetExhausted,
      failedCandidates: row.failed_candidates,
      preparedDetailCandidates,
      repairableCandidates,
      typeMissingCandidates,
    });
    const { detailOnly, name } = dispatch;
    let repairStrategy = dispatch.strategy;
    let repairReference;
    if (publicationGap) {
      const target = publicationGapRepairTarget(row);
      const { strategy, ...targetReference } = target;
      repairStrategy = strategy === "resume_run"
        ? (target.publication_gap_scope === "about_only" ? "publication_gap_about_only" : "publication_gap_resume")
        : "publication_gap_child";
      repairReference = targetReference;
    } else {
      repairReference = automaticFinalRepairReference({
        runId: row.run_id,
        candidateId: row.candidate_id,
        registryPromotionRunId: row.registry_promotion_run_id,
        detailOnly,
        forceChildRun: businessRunBudgetExhausted,
      });
    }
    const jobId = safeJobId("final-repair", row.run_id, strictRepairVersion, round);
    const prepareDetailRepair = detailOnly
      ? () => query(
        `WITH crawl_settings AS (
           SELECT GREATEST(
                    0,
                    LEAST(
                      3650,
                      COALESCE(
                        (
                          SELECT (value_json->>'content_max_age_days')::int
                          FROM crawler.settings
                          WHERE setting_key='crawl'
                          LIMIT 1
                        ),
                        90
                      )
                    )
                  )::int AS content_max_age_days
         )
         UPDATE crawler.content_candidates
         SET detail_status='queued',api_status='not_needed',attempts=0,
             missing_fields='{}'::text[],error_message=NULL,finished_at=NULL,
             result_json=jsonb_set(
               COALESCE(result_json,'{}'::jsonb),
               '{final_repair_dispatch}',
               jsonb_build_object(
                 'status','prepared',
                 'mode','detail',
                 'repair_round',$2::int,
                 'job_id',$3::text,
                 'prepared_at',now()
               ),
               true
             ),
             updated_at=now()
         FROM crawl_settings settings
         WHERE run_id=$1
           AND NOT (result_json ? 'parser_contract_error')
           AND COALESCE(result_json->'scope'->>'status','')<>'excluded'
           AND (
             settings.content_max_age_days=0
             OR NOT EXISTS (
               SELECT 1
               FROM crawler.contents repair_content
               WHERE repair_content.content_key=crawler.content_candidates.content_key
                 AND repair_content.run_id=crawler.content_candidates.run_id
                 AND repair_content.published_at IS NOT NULL
                 AND repair_content.published_at<now()-(settings.content_max_age_days * interval '1 day')
             )
           )
           AND ${finalRepairCandidateSql()}`,
        [row.run_id, round, jobId],
      )
      : null;
    const ensuredJob = await ensureFinalRepairJob(
      queues[queuesByRole.channelCrawl],
      {
        name,
        data: {
          dispatch_generation: round,
          channel_id: row.channel_id,
          channel_url: detailOnly ? row.channel_url : `https://www.youtube.com/channel/${row.channel_id}`,
          ...repairReference,
          crawl_mode: row.crawl_mode,
          enforce_min_subscribers: false,
          repair_reason: "automatic_final_reconciliation",
          repair_round: round,
          pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
          ...(businessRunBudgetExhausted ? {
            checkpoint_target_run_id: row.run_id,
            api_fallback_mode: "emergency",
          } : {}),
          ...(strictRepair && !businessRunBudgetExhausted ? {
            published_at_required_precision: "date_only",
            api_fallback_mode: "disabled",
          } : {}),
        },
        options: {
          jobId,
          attempts: 3,
          backoff: { type: "exponential", delay: 5000, jitter: 0.5 },
        },
        beforeDispatch: prepareDetailRepair,
      },
    );
    await query(
      `UPDATE crawler.channel_runs
       SET result_json=jsonb_set(
             CASE WHEN $5::boolean THEN
               jsonb_set(
                 COALESCE(result_json,'{}'::jsonb),
                 '{checkpoint_repair}',
                 COALESCE(result_json->'checkpoint_repair','{}'::jsonb)
                   || jsonb_build_object(
                        'rounds',$6::int,
                        'queued_at',now(),
                        'job_id',$3::text
                      ),
                 true
               )
             ELSE COALESCE(result_json,'{}'::jsonb) END,
             '{final_repair}',
             COALESCE(result_json->'final_repair','{}'::jsonb)
               || jsonb_build_object('rounds',$2::int,'queued_at',now(),'job_id',$3::text,'mode',$4::text),
             true
           ),
           updated_at=now()
       WHERE run_id=$1`,
      [
        row.run_id,
        round,
        jobId,
        repairStrategy,
        businessRunBudgetExhausted,
        checkpointRepairRound,
      ],
    );
    actions.push({
      action: "enqueue-final-repair",
      channel_id: row.channel_id,
      run_id: row.run_id,
      repair_round: round,
      checkpoint_repair_round: checkpointRepairRound,
      mode: repairStrategy,
      publication_gap: publicationGap,
      job_action: ensuredJob.action,
      attempts_made: ensuredJob.attempts_made,
      failed_candidates: Number(row.failed_candidates ?? 0),
      repairable_candidates: repairableCandidates,
      type_missing_candidates: typeMissingCandidates,
      prepared_detail_candidates: preparedDetailCandidates,
      strict_content_completeness: strictRepair,
      repair_capacity: {
        ready: Number.isFinite(ready) ? ready : null,
        in_flight: channelJobs.length,
        dispatched_limit: dispatchLimit,
      },
    });
  }
  return repairRows.length;
}

async function cleanupRecoveredFailedJobs(actions, now = Date.now()) {
  if (now - lastRecoveredFailureCleanupAt < recoveredFailureCleanupIntervalMs) return 0;
  lastRecoveredFailureCleanupAt = now;
  const removed = { quality: 0, agent: 0, channel: 0, discover: 0 };

  const qualityJobs = await queues[queuesByRole.queryQuality].getJobs(["failed"], 0, 99, true);
  for (const job of qualityJobs) {
    const qualityBatchId = String(job.data?.quality_batch_id ?? "");
    if (!qualityBatchId) continue;
    const batch = await query(
      "SELECT status FROM crawler.query_quality_batches WHERE quality_batch_id=$1 LIMIT 1",
      [qualityBatchId],
    );
    if (batch.rows[0]?.status === "cancelled") {
      await job.remove();
      removed.quality += 1;
    }
  }

  const agentJobs = await queues[queuesByRole.agentBatch].getJobs(["failed"], 0, 99, true);
  for (const job of agentJobs) {
    const channelIds = [...new Set((job.data?.channel_ids ?? []).map(String).filter(Boolean))];
    if (channelIds.length === 0) continue;
    const statuses = await query(
      `SELECT channel_id,agent_status FROM crawler.channels WHERE channel_id=ANY($1::text[])`,
      [channelIds],
    );
    if (statuses.rows.length === channelIds.length && statuses.rows.every((row) => ["done", "skipped"].includes(row.agent_status))) {
      await job.remove();
      removed.agent += 1;
    }
  }

  const channelJobs = await queues[queuesByRole.channelCrawl].getJobs(["failed"], 0, 99, true);
  for (const job of channelJobs) {
    const channelId = String(job.data?.channel_id ?? "");
    if (!channelId) continue;
    const state = await query(
      `SELECT r.status,
              count(cc.*) FILTER (WHERE cc.detail_status='failed')::int AS failed_candidates
       FROM crawler.channels c
       JOIN crawler.channel_runs r ON r.run_id=c.latest_run_id
       LEFT JOIN crawler.content_candidates cc ON cc.run_id=r.run_id
       WHERE c.channel_id=$1
       GROUP BY r.run_id`,
      [channelId],
    );
    const current = state.rows[0];
    if (current && ["done", "skipped"].includes(current.status) && Number(current.failed_candidates ?? 0) === 0) {
      await job.remove();
      removed.channel += 1;
    }
  }

  const discoverJobs = await queues[queuesByRole.discoverPage].getJobs(["failed"], 0, 99, true);
  for (const job of discoverJobs) {
    const queryId = Number(job.data?.query_id);
    if (!Number.isFinite(queryId)) continue;
    const recovered = await query(
      `SELECT EXISTS (
         SELECT 1 FROM crawler.query_pages
         WHERE query_id=$1 AND status='done' AND finished_at>to_timestamp($2::double precision/1000.0)
       ) AS recovered`,
      [queryId, Number(job.finishedOn ?? 0)],
    );
    if (recovered.rows[0]?.recovered) {
      await job.remove();
      removed.discover += 1;
    }
  }

  const total = removed.quality + removed.agent + removed.channel + removed.discover;
  if (total > 0) actions.push({ action: "cleanup-recovered-failed-jobs", ...removed, total });
  return total;
}

async function maybeReconcileAutomaticPublicationOnboarding(actions, now = Date.now()) {
  if (now - lastPublicationOnboardingAt < publicationOnboardingIntervalMs) return;
  lastPublicationOnboardingAt = now;
  const summary = await reconcileAutomaticPublicationBacklog({
    query,
    withTransaction,
    limit: publicationOnboardingBatchSize,
  });
  if (summary.scanned > 0 || summary.failed > 0) {
    actions.push({ action: "reconcile-publication-channel-onboarding", ...summary });
  }
}

async function tick() {
  const stats = await getQueueStats(queues);
  const actions = [];
  const contentEnrichController = await dispatchContentEnrichForController({
    dispatcher: contentEnrichDispatcher,
    monitor: contentEnrichMonitor,
    queueCounts: stats[queuesByRole.contentEnrich],
    actions,
  });
  if (contentEnrichController.operational) {
    stats[queuesByRole.contentEnrich].content_enrich_operational = contentEnrichController.operational;
  }
  await reconcileQueryQualityQueue(actions);
  let queryScheduler = await getQueryScheduler();
  queryScheduler = await resumeLegacyAutomaticFinalization(queryScheduler, actions);
  if (await reconcileAutomaticDiscoveryClosure(query, queryScheduler)) {
    actions.push({
      action: "reconcile-query-discovery-closure",
      pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
    });
  }
  const metadataCycle = await maybeStartMetadataDiscoveryCycle(withTransaction);
  if (metadataCycle.scheduler) queryScheduler = metadataCycle.scheduler;
  if (metadataCycle.started) {
    actions.push({
      action: "start-metadata-discovery-cycle",
      pipeline_cycle_id: metadataCycle.pipeline_cycle_id,
      query_id: metadataCycle.query_id,
    });
  }
  if (pipelineProducerActive(queryScheduler)) await ensureDispatchBatch(queryScheduler);
  const crawlSettings = await getCrawlSettings();
  const channelBacklog = backlog(stats, queuesByRole.channelCrawl);
  const detailBacklog = backlog(stats, queuesByRole.contentDetail);
  const dataApiQueueBacklog = backlog(stats, queuesByRole.dataApiBatch);
  const agentBacklog = backlog(stats, queuesByRole.agentBatch);
  const apiPendingRows = await query(
    `SELECT count(*)::int AS count FROM crawler.youtube_api_tasks
     WHERE status IN ('pending','failed','queued','running')`,
  );
  const apiPendingCount = Number(apiPendingRows.rows[0]?.count ?? 0);
  const apiUsageRows = await query(
    `SELECT request_count,requested_video_count
     FROM crawler.youtube_api_daily_usage
     WHERE usage_date=CURRENT_DATE`,
  );
  const apiDailyRequestCount = Number(apiUsageRows.rows[0]?.request_count ?? 0);
  const apiDailyVideoCount = Number(apiUsageRows.rows[0]?.requested_video_count ?? 0);
  const dataApiBacklog = Math.max(dataApiQueueBacklog, Math.ceil(apiPendingCount / crawlSettings.youtubeApiBatchSize));
  const proxyCapacity = await getProxyCapacity();
  const proxyAllocation = await applyProxyConcurrency(proxyCapacity, stats, actions);
  const channelPressure = await getChannelPressure(channelBacklog, proxyCapacity);
  const dataApiCircuit = await loadDataApiCircuitState({
    query,
    proxyCapacity,
    detailExecutionQueue: channelInlineDetails
      ? queuesByRole.channelCrawl
      : queuesByRole.contentDetail,
    detailExecutionRole: channelInlineDetails ? "channel" : "detail",
  });

  const agentConfigs = automaticLocalAgentConfigs(await listEnabledAgentConfigs());
  const agentCapacity = await syncAgentGlobalConcurrency(actions, agentConfigs);
  await reconcileIncrementalAgentQueue(actions);
  await maybeCreateIncrementalAgentBatch(actions);
  await maybeReconcileAutomaticPublicationOnboarding(actions);
  if (pipelineProducerActive(queryScheduler) && channelCandidateDispatchEnabled) {
    await reconcileChannelCandidateQueue(actions, queryScheduler.pipeline_cycle_id);
  }
  if (pipelineProducerActive(queryScheduler)) {
    await reconcileAgentQueue(actions, queryScheduler.pipeline_cycle_id);
  }
  await resolvePendingDiscoveryPageQualifications(queryScheduler.pipeline_cycle_id, actions);
  if (
    pipelineProducerActive(queryScheduler)
    && crawlSettings.youtubeApiFallbackMode === "emergency"
    && !dataApiCircuit.open
  ) {
    await maybeCreateDataApiBatches(
      actions,
      crawlSettings.youtubeApiBatchSize,
      stats,
      queryScheduler,
      crawlSettings.youtubeApiDailyRequestLimit,
      apiDailyRequestCount,
    );
  } else if (pipelineProducerActive(queryScheduler) && apiPendingCount > 0) {
    actions.push({
      action: "hold-youtube-api-fallback",
      reason: crawlSettings.youtubeApiFallbackMode === "disabled" ? "fallback_disabled" : dataApiCircuit.reason,
      pending: apiPendingCount,
    });
  }
  if (pipelineProducerActive(queryScheduler)) {
    await maybeCreateAgentBatch(actions, agentConfigs, agentCapacity, queryScheduler);
  }
  const discoverProxyReady = roleReady(proxyCapacity, "discover");
  const channelProxyReady = roleReady(proxyCapacity, "channel");
  const queryQualityProxyReady = roleReady(proxyCapacity, "query_quality");
  const detailProxyReady = roleReady(proxyCapacity, "detail");
  const effectiveDetailBacklog = channelInlineDetails ? 0 : detailBacklog;
  const pressureMetrics = {
    channelBacklog,
    channelEtaSeconds: channelPressure.eta_seconds,
    channelTerminalSamples: channelPressure.terminal_samples,
    channelFailureRate: channelPressure.failure_rate,
    proxyCooldownRatio: channelPressure.proxy_cooldown_ratio,
    detailBacklog: effectiveDetailBacklog,
    dataApiBacklog,
    agentBacklog,
  };
  const pressureLimits = {
    pauseChannelBacklog: discoverPauseChannelBacklog,
    resumeChannelBacklog: discoverResumeChannelBacklog,
    pauseChannelEtaSeconds: discoverPauseChannelEtaSeconds,
    resumeChannelEtaSeconds: discoverResumeChannelEtaSeconds,
    pauseChannelFailureRate: discoverPauseChannelFailureRate,
    resumeChannelFailureRate: discoverResumeChannelFailureRate,
    pauseProxyCooldownRatio: discoverPauseProxyCooldownRatio,
    resumeProxyCooldownRatio: discoverResumeProxyCooldownRatio,
    minimumFailureSamples: channelPressureMinimumSamples,
    pauseDetailBacklog: discoverPauseDetailBacklog,
    resumeDetailBacklog: discoverResumeDetailBacklog,
    pauseDataApiBacklog: dataApiPauseDiscoverBacklog,
    resumeDataApiBacklog: dataApiResumeDiscoverBacklog,
    pauseAgentBacklog: agentPauseDiscoverBacklog,
    resumeAgentBacklog: agentResumeDiscoverBacklog,
  };
  const pressureReason = discoveryPressureReason(pressureMetrics, pressureLimits);

  if (!querySchedulerAllowsDiscovery(queryScheduler)) {
    await setPaused(queuesByRole.discoverPage, true, `query_scheduler_${queryScheduler.status}`, actions);
  } else if (
    Number.isFinite(discoverProxyReady)
      ? discoverProxyReady === 0
      : Number.isFinite(proxyCapacity.active) && proxyCapacity.active < 3
  ) {
    await setPaused(queuesByRole.discoverPage, true, "proxy_capacity_low", actions);
  } else if (pressureReason) {
    activeDiscoverPressureReason = pressureReason;
    await setPaused(
      queuesByRole.discoverPage,
      true,
      pressureReason,
      actions,
    );
  } else if (
    !activeDiscoverPressureReason
    || discoveryPressureRecoveredForReason(activeDiscoverPressureReason, pressureMetrics, pressureLimits)
  ) {
    const recoveredReason = activeDiscoverPressureReason;
    activeDiscoverPressureReason = null;
    await setPaused(
      queuesByRole.discoverPage,
      false,
      recoveredReason ? `${recoveredReason}_recovered` : "backlog_recovered",
      actions,
    );
  }

  const pipelineHalted = ["paused", "stopped"].includes(queryScheduler.status);
  const noChannelProxy = Number.isFinite(channelProxyReady)
    ? channelProxyReady === 0
    : Number.isFinite(proxyCapacity.active) && proxyCapacity.active < 2;
  if (pipelineHalted) {
    await setPaused(queuesByRole.channelCrawl, true, `query_scheduler_${queryScheduler.status}`, actions);
  } else if (noChannelProxy || effectiveDetailBacklog >= channelPauseDetailBacklog) {
    await setPaused(queuesByRole.channelCrawl, true, effectiveDetailBacklog >= channelPauseDetailBacklog ? "content_detail_backlog_high" : "proxy_capacity_low", actions);
  } else if (effectiveDetailBacklog <= channelResumeDetailBacklog) {
    await setPaused(queuesByRole.channelCrawl, false, "content_detail_backlog_recovered", actions);
  }
  if (Number.isFinite(queryQualityProxyReady)) {
    await setPaused(
      queuesByRole.queryQuality,
      queryQualityProxyReady === 0,
      queryQualityProxyReady === 0 ? "proxy_capacity_low" : "proxy_available",
      actions,
    );
  }
  const noDetailProxy = Number.isFinite(detailProxyReady)
    ? detailProxyReady === 0
    : Number.isFinite(proxyCapacity.active) && proxyCapacity.active === 0;
  if (pipelineHalted) {
    await setPaused(queuesByRole.contentDetail, true, `query_scheduler_${queryScheduler.status}`, actions);
  } else if (channelInlineDetails) {
    await setPaused(queuesByRole.contentDetail, true, "details_run_inside_channel_queue", actions);
  } else if (noDetailProxy) {
    await setPaused(queuesByRole.contentDetail, true, "no_active_proxy", actions);
  } else {
    await setPaused(queuesByRole.contentDetail, false, "proxy_available", actions);
  }
  for (const queueName of [queuesByRole.dataApiBatch, queuesByRole.agentBatch]) {
    if (pipelineHalted) {
      await setPaused(queueName, true, `query_scheduler_${queryScheduler.status}`, actions);
    } else {
      await setPaused(queueName, false, "query_scheduler_active", actions);
    }
  }
  await setPaused(queuesByRole.finalize, false, "database_finalize_recovery", actions);
  await setPaused(
    queuesByRole.agentIncremental,
    false,
    "incremental_clock_independent",
    actions,
  );
  await maybeScheduleQuerySlice(actions, queryScheduler, stats);
  await maybeDispatchContentCompletenessRepairs(actions, stats, queryScheduler, proxyCapacity);
  await maybeRepairFailedChannelRuns(actions, stats, queryScheduler, proxyCapacity);
  await cleanupRecoveredFailedJobs(actions);
  await reconcileFinalizeQueue(actions, queryScheduler.pipeline_cycle_id);
  await maybeCompleteAutomaticPipeline(actions, queryScheduler);

  const finalManagedDispatch = await managedJobOutboxDispatcher.dispatchAvailable({
    limit: queryQualityReconcileLimit,
  });
  if (finalManagedDispatch.claimed > 0) {
    actions.push({ action: "flush-managed-job-outbox", dispatch: finalManagedDispatch });
  }
  const recoveryIntentReconcile = await migrationRetryIntentJobReconciler.reconcileAvailable({
    limit: 100,
  });
  if (recoveryIntentReconcile.scanned > 0) {
    actions.push({
      action: "reconcile-migration-retry-intents",
      ...recoveryIntentReconcile,
    });
  }

  const tickStored = await persistControllerTick(stats, actions, queryScheduler);
  await cleanupTelemetry();
  if (tickStored || actions.length > 0) console.log(JSON.stringify({
    event: "controller_tick",
    actions,
    query_scheduler: {
      status: queryScheduler.status,
      query_set_id: queryScheduler.query_set_id,
      query_quality_min_score: queryScheduler.query_quality_min_score,
      chunk_size: queryScheduler.chunk_size,
      max_discover_backlog: queryScheduler.max_discover_backlog,
      pipeline_cycle_id: queryScheduler.pipeline_cycle_id,
    },
    youtube_api_batch_size: crawlSettings.youtubeApiBatchSize,
    youtube_api_fallback_mode: crawlSettings.youtubeApiFallbackMode,
    youtube_api_circuit: dataApiCircuit,
    youtube_api_daily_usage: {
      request_limit: crawlSettings.youtubeApiDailyRequestLimit,
      request_count: apiDailyRequestCount,
      requested_video_count: apiDailyVideoCount,
      remaining_requests: Math.max(0, crawlSettings.youtubeApiDailyRequestLimit - apiDailyRequestCount),
    },
    proxy_capacity: proxyCapacity,
    proxy_allocation: proxyAllocation,
    channel_pressure: channelPressure,
    backlogs: { channelBacklog, detailBacklog, dataApiBacklog, apiPendingCount, agentBacklog },
  }));
}

let timer = null;
let wakeSubscriber = null;
let controllerWakeup = null;
let immediateWakeRequested = false;
let shutdownPromise = null;

async function runControllerTick() {
  try {
    await tick();
  } catch (error) {
    console.error(JSON.stringify({ event: "controller_tick_failed", error: error?.message || String(error) }));
    try {
      await query(
        `INSERT INTO crawler.controller_ticks (status, error_message)
         VALUES ('failed', $1)`,
        [error?.message || String(error)],
      );
    } catch {
      // Ignore logging failures during controller error handling.
    }
  } finally {
    if (!controllerLifecycle.isShuttingDown() && immediateWakeRequested) {
      controllerWakeup?.request();
    }
  }
}

async function closeControllerResources() {
  if (wakeSubscriber) {
    wakeSubscriber.removeAllListeners();
    await wakeSubscriber.quit();
    wakeSubscriber = null;
  }
  await closeQueues(queues);
  await closeProxyControlClient();
  await closeDb();
}

const controllerLifecycle = createControllerLifecycle({
  tick: runControllerTick,
  closeResources: closeControllerResources,
});

function requestImmediateControllerTick(payload) {
  if (controllerLifecycle.isShuttingDown()) return false;
  immediateWakeRequested = true;
  if (!controllerLifecycle.isRunning()) controllerWakeup?.request();
  console.log(JSON.stringify({
    event: "controller_wakeup_requested",
    reason: payload?.type ?? "unknown",
    page_count: Array.isArray(payload?.page_ids) ? payload.page_ids.length : 0,
  }));
  return true;
}

function loop() {
  return controllerLifecycle.run();
}

await ensureSchema();
await ensureDefaultAgentConfig();
controllerWakeup = createCoalescedWakeup(async () => {
  if (controllerLifecycle.isRunning() || controllerLifecycle.isShuttingDown()) return;
  immediateWakeRequested = false;
  await loop();
}, {
  delayMs: wakeupDelayMs,
  onError: (error) => {
    console.error(JSON.stringify({
      event: "controller_wakeup_failed",
      error: error?.message || String(error),
    }));
  },
});
wakeSubscriber = createRedisConnection();
wakeSubscriber.on("error", (error) => {
  console.error(JSON.stringify({
    event: "controller_wakeup_subscriber_error",
    error: error?.message || String(error),
  }));
});
wakeSubscriber.on("message", (channel, message) => {
  if (channel !== DISCOVERY_PAGE_READY_CHANNEL) return;
  try {
    const payload = JSON.parse(message);
    if (
      payload?.type === "discovery_page_qualification_ready"
      && Array.isArray(payload.page_ids)
      && payload.page_ids.length > 0
    ) {
      requestImmediateControllerTick(payload);
    }
  } catch (error) {
    console.error(JSON.stringify({
      event: "controller_wakeup_message_invalid",
      error: error?.message || String(error),
    }));
  }
});
await wakeSubscriber.subscribe(DISCOVERY_PAGE_READY_CHANNEL);
await loop();
timer = setInterval(loop, intervalMs);
console.log(`controller started interval_ms=${intervalMs} tick_sample_ms=${tickSampleMs} wakeup_delay_ms=${wakeupDelayMs}`);

function shutdown(signal) {
  if (shutdownPromise) return shutdownPromise;
  console.log(`received ${signal}, shutting down controller`);
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  controllerWakeup?.close();
  shutdownPromise = controllerLifecycle.shutdown()
    .then(() => process.exit(0))
    .catch((error) => {
      console.error(JSON.stringify({
        event: "controller_shutdown_failed",
        error: error?.message || String(error),
      }));
      process.exit(1);
    });
  return shutdownPromise;
}

process.on("SIGTERM", () => { void shutdown("SIGTERM"); });
process.on("SIGINT", () => { void shutdown("SIGINT"); });
