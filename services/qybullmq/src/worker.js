import { delayFinalizeJob } from "./finalizeDeferral.js";
import {MIGRATION_START_JOB,startControlledMigrationChannel,prepareControlledMigrationSnapshot,migrationBatchControlEnabled} from "./migrationBatchControl.js";
import { Worker, DelayedError } from "bullmq";
import { pathToFileURL } from "node:url";
import { gateVideoApiJob, isVideoApiHandoff, runVideoApiResumable, withVideoApiReplay } from "./videoApiContinuation.js";
import { runVideoExecutionResumable } from "./videoExecutionDeferral.js";
import { isVideoExecutionRecoveryPending } from "./videoExecutionRecovery.js";
import { localIntakeSignalsFromEnv } from './remoteNodes/localIntakeSignals.js';
import { createTransportSignals } from './remoteNodes/transportSignals.js';
import { LocalIncrementalIntake } from './localIncrementalIntake.js';
import { intakeWorkerName } from './workerIntakeTelemetry.js';
import { nanoid } from "nanoid";
import { ensureDefaultAgentConfig } from "./agentConfig.js";
import { ChannelExecutionRuntimeAdapter } from "./channelExecutionRuntimeAdapter.js";
import { deferJobForSlotPause } from "./channelJobDeferral.js";
import { DiscoverExecutionRuntimeAdapter } from "./discoverExecutionRuntimeAdapter.js";
import {
  createDiscoverPageProcessor,
  persistDiscoverParserContractFailure,
  persistDiscoverRetryCheckpoint,
  recordDiscoverPageTerminalFailure,
} from "./discoverPageProcessor.js";
import { reconcileDispatchBatchCandidateState } from "./dispatchBatchCandidateState.js";
import {
  classifyTerminalChannelError,
  markChannelRemoved,
} from "./channelLifecycle.js";
import {
  describeChannelCandidateWorkerFailure,
  failChannelCandidateWorkerJob,
  runChannelCandidateWorkerJobWithDurableSettlement,
  terminalChannelRunFailureEvidence,
} from "./channelCandidateWorkerLifecycle.js";
import { activeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";
import { settleTerminalDataApiBatchJob } from "./dataApiBatchJobRecovery.js";
import { ensureSchema, closeDb, logTaskEvent, query, warmDb, withTransaction } from "./db.js";
import { closePersistentHttpClient } from "./httpClient.js";
import { youtubeErrorText } from "./detailPolicy.js";
import {
  ContentEnrichExecutor,
  PostgresContentEnrichExecutionRepository,
  contentEnrichResultFromError,
} from "./contentEnrichExecution.js";
import {
  applyIncrementalVideoDetail,
  executeIncrementalVideo,
  fetchIncrementalVideoDetail,
} from "./incrementalVideo.js";
import { executeIncrementalYoutubeJsVideo } from "./incrementalYoutubeJsVideo.js";
import { dynamicRotaProxyConfig, fixedRotaProxyConfig } from "./fixedProxyConfig.js";
import { resolveWorkerIdentityPolicy } from "./identityPolicyCatalog.js";
import { IncrementalAgentBacklog } from "./incrementalAgentBacklog.js";
import { IncrementalChannelRunner } from "./incrementalChannelRunner.js";
import { IncrementalRunStore } from "./incrementalRunStore.js";
import { recordIncrementalTerminalFailure } from "./incrementalTerminalFailure.js";
import { terminateExhaustedIncrementalRun } from './incrementalBudgetRecovery.js';
import {
  getCrawlSettingsV2,
  processAgentBatchV2,
  processChannelCrawlV2,
  processCheckpointRepairV2,
  processContentDetailBatchV2,
  processDataApiBatchV2,
  processFinalizeV2,
  signalReadyDiscoveryPageQualifications,
  closePipelineV2Queues,
} from "./pipelineV2.js";
import { closeFullCrawlYoutubeJsQueues, executeFullCrawlYoutubeJs } from "./fullCrawlYoutubeJs.js";
import { assertFullCrawlSnapshotRecoveryOwner } from "./finalRepairJobRecovery.js";
import { defaultFullCrawlFetchContract, isYoutubeJsFullCrawlFetchContract } from "./fullCrawlFetchContract.js";
import { channelExtractorCapabilities, incrementalVideoExecutorMode as resolveIncrementalVideoExecutorMode } from "./channelExtractorCapabilities.js";
import { assertFullCrawlWorkerLane, fullCrawlWorkerPrefix } from "./fullCrawlCanary.js";
import { getQueryScheduler } from "./queryScheduler.js";
import { scoreQueryBatch } from "./queryQuality.js";
import { reconcilePublication } from "./publicationReconciler.js";
import { isParserContractError, parserContractDetails } from "./localizedParsing.js";
import { runWithProxyIdentity } from "./proxyIdentity.js";
import { closeProxyControlClient, proxyControlClient } from "./proxyControlClient.js";
import { ProxyBusinessRunPreparer } from "./proxyBusinessRun.js";
import {
  isBusinessRunBudgetExhausted,
  terminateExhaustedBusinessRun,
} from "./businessRunBudgetRecovery.js";
import { QueryQualityExecutionRuntimeAdapter } from "./queryQualityExecutionRuntimeAdapter.js";
import {
  executeManagedWorkerAttempt,
  validateWorkerQueueConfiguration,
} from "./managedWorkerExecution.js";
import {
  markChannelCandidateJobAttemptActive,
  processManagedWorkerJob,
  retryableSystemFailureDecision,
  isStaleExecutionFailure,
} from "./managedWorkerJob.js";
import {
  enterMigrationRetryIntentWorkerJob,
  finishMigrationRetryIntent,
} from "./migrationRetryIntent.js";
import {
  applyFailureRetryDecision,
  bullmqPrefix,
  closeQueues,
  createQueues,
  queueNames,
  queuesByRole,
  redisOptions,
  safeJobId,
} from "./queues.js";
import { settleContentDetailRecoveryTerminalFailure } from "./migrationSystemRetryRecovery.js";
import { closeStorage, putRawObject } from "./storage.js";
import { RotaSlotAdapter } from "./rotaSlotAdapter.js";
import { warmPersistentYtDlp } from "./ytdlpSession.js";
import { closeYoutubeJs, warmYoutubeJs, youtubeJsDetailEnabled } from "./youtubeJs.js";
import { annotateYoutubeFailure, decideYoutubeFailure } from "./youtubeFailurePolicy.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";
import { resolveYoutubeLocale } from "./youtubeLocale.js";

// Embedded use shares the original processor and lifecycle without creating consumers.
export function createWorkerRuntime({ embedded = false } = {}) {
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const incrementalVideoExecutorMode = resolveIncrementalVideoExecutorMode();
const incrementalVideoExecutor = incrementalVideoExecutorMode === "youtubejs_checkpoint_v1"
  ? executeIncrementalYoutubeJsVideo
  : executeIncrementalVideo;
const incrementalRunStore = new IncrementalRunStore({ withTransaction });
const incrementalAgentBacklog = new IncrementalAgentBacklog({ withTransaction });
const incrementalChannelRunner = new IncrementalChannelRunner({
  runStore: incrementalRunStore,
  agentBacklog: incrementalAgentBacklog,
  query,
  withTransaction,
  video: incrementalVideoExecutor,
});
const contentEnrichExecutor = new ContentEnrichExecutor({
  repository: new PostgresContentEnrichExecutionRepository({
    withTransaction,
    applyDetail: applyIncrementalVideoDetail,
    refreshHashes: refreshVideoPublicationItemHashes,
    reconcilePublication,
  }),
  fetchDetail: fetchIncrementalVideoDetail,
  leaseDurationMs: Number(process.env.CONTENT_ENRICH_WORKER_LEASE_MS || 15 * 60_000),
  heartbeatIntervalMs: Number(process.env.CONTENT_ENRICH_HEARTBEAT_MS || 60_000),
  retryBaseMs: Number(process.env.CONTENT_ENRICH_RETRY_BASE_MS || 30_000),
  retryMaxMs: Number(process.env.CONTENT_ENRICH_RETRY_MAX_MS || 6 * 60 * 60_000),
  maxAttempts: Number(process.env.CONTENT_ENRICH_MAX_ATTEMPTS || 8),
});
const defaultMinSubscriberCount = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000);
const rotaProxyBaseUrl = String(process.env.ROTA_PROXY_BASE_URL || "http://youtube-rota-qy-core:8000");
const rotaProxyPassword = String(process.env.ROTA_BULLMQ_PROXY_PASSWORD || "");
const dynamicProxy = dynamicRotaProxyConfig({
  slotRole: process.env.PROXY_SLOT_ROLE,
  controlUrl: process.env.ROTA_PROXY_CONTROL_URL,
  controlToken: process.env.ROTA_PROXY_CONTROL_TOKEN,
  proxyPassword: rotaProxyPassword,
});
const proxySlotRole = dynamicProxy?.role ?? "";
const resolvedIdentityPolicy = dynamicProxy
  ? resolveWorkerIdentityPolicy({
      role: proxySlotRole,
      policyId: process.env.ROTA_IDENTITY_POLICY_ID,
      expectedWorkloadScope: process.env.ROTA_WORKLOAD_SCOPE_EXPECTED,
    })
  : null;
const language = resolvedIdentityPolicy?.environment.YOUTUBE_LANGUAGE
  || process.env.YOUTUBE_LANGUAGE
  || "pt-BR";
const country = resolvedIdentityPolicy?.environment.YOUTUBE_COUNTRY
  || process.env.YOUTUBE_COUNTRY
  || "BR";
const fixedProxy = fixedRotaProxyConfig({
  proxyUser: process.env.ROTA_FIXED_PROXY_USER,
  proxyPassword: rotaProxyPassword,
  baseUrl: rotaProxyBaseUrl,
  slotRole: proxySlotRole,
});
if (fixedProxy) process.env.YOUTUBE_PROXY_URL = fixedProxy.proxyUrl;
const proxyWorkerId = String(process.env.PROXY_WORKER_ID || process.env.HOSTNAME || `${proxySlotRole || "worker"}-${process.pid}`);
const proxySlotPollMs = Math.max(1000, Number(process.env.PROXY_SLOT_POLL_MS || 5000));
let shuttingDown = false;

function identityRuntimeForRole() {
  if (proxySlotRole === "channel") {
    return new ChannelExecutionRuntimeAdapter({
      workerId: proxyWorkerId,
      incrementalExecutor: incrementalVideoExecutorMode,
    });
  }
  if (proxySlotRole === "discover") return new DiscoverExecutionRuntimeAdapter();
  if (proxySlotRole === "query_quality") return new QueryQualityExecutionRuntimeAdapter();
  return null;
}

const rotaSlot = dynamicProxy
  ? new RotaSlotAdapter({
      client: proxyControlClient(),
      role: proxySlotRole,
      workerId: proxyWorkerId,
      resolvedPolicy: resolvedIdentityPolicy,
      proxyBaseUrl: rotaProxyBaseUrl,
      proxyPassword: rotaProxyPassword,
      identityRuntime: identityRuntimeForRole(),
      renewIntervalMs: Number(process.env.ROTA_SLOT_RENEW_INTERVAL_MS || 5000),
      leaseSafetyMarginMs: Number(process.env.ROTA_LEASE_SAFETY_MARGIN_MS || 10000),
      routeReadyWaitMs: Number(process.env.ROTA_ROUTE_READY_WAIT_MS || 10000),
      maxRouteSwitchesPerExecution: Number(process.env.ROTA_MAX_ROUTE_SWITCHES_PER_EXECUTION || 2),
    })
  : null;

const businessRunPreparer = dynamicProxy
  ? new ProxyBusinessRunPreparer({
      queryFn: query,
      withTransaction,
      incrementalRunStore,
      resolvedPolicy: resolvedIdentityPolicy,
    })
  : null;

function proxyExecutionSnapshot() {
  return rotaSlot?.status().assignment ?? fixedProxy?.identity ?? null;
}

function configuredForProxySlot() {
  return rotaSlot !== null;
}

async function waitForProxySlot() {
  if (!configuredForProxySlot()) return true;
  try {
    const status = await rotaSlot.start();
    console.log(JSON.stringify({
      event: "rota_slot_ready",
      role: proxySlotRole,
      worker_id: proxyWorkerId,
      slot: status.assignment?.slot_name ?? null,
      route_generation: status.assignment?.route_generation ?? null,
    }));
    return true;
  } catch (error) {
    if (shuttingDown) return false;
    throw error;
  }
}

function concurrencyFor(queueName) {
  const envName = `${queueName.replaceAll("-", "_").toUpperCase()}_CONCURRENCY`;
  const value = Number(process.env[envName] || 1);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : 1;
}

function optionalPositiveWorkerOption(environmentName) {
  const raw = String(process.env[environmentName] ?? "").trim();
  if (!raw) return null;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${environmentName} must be a positive integer`);
  }
  return value;
}

function bullmqWorkerTimingOptions() {
  const lockDuration = optionalPositiveWorkerOption("BULLMQ_LOCK_DURATION_MS");
  const stalledInterval = optionalPositiveWorkerOption("BULLMQ_STALLED_INTERVAL_MS");
  const skipLockRenewal = String(
    process.env.BULLMQ_SKIP_LOCK_RENEWAL ?? "",
  ).trim().toLowerCase();
  if (skipLockRenewal && !["true", "false"].includes(skipLockRenewal)) {
    throw new Error("BULLMQ_SKIP_LOCK_RENEWAL must be true or false");
  }
  return {
    ...(lockDuration == null ? {} : { lockDuration }),
    ...(stalledInterval == null ? {} : { stalledInterval }),
    ...(skipLockRenewal === "true" ? { skipLockRenewal: true } : {}),
  };
}

function errorMessage(error) {
  return String(error?.message ?? error ?? "unknown error");
}

function usesChannelExecution(job) {
  if (job.queueName === queuesByRole.channelCrawl) return true;
  if (job.queueName === queuesByRole.contentEnrich) return true;
  if (job.queueName !== queuesByRole.channelIncremental) return false;
  return ["about", "video"]
    .some((domain) => job.data?.task_mask?.[domain] === true);
}

function channelExecutionEnabled() {
  return enabledQueues.includes(queuesByRole.channelCrawl)
    || enabledQueues.includes(queuesByRole.channelIncremental)
    || enabledQueues.includes(queuesByRole.contentEnrich);
}

async function refreshDispatchCandidateCounts(dispatchBatchId) {
  return reconcileDispatchBatchCandidateState(query, dispatchBatchId);
}

async function refreshQueryQualityBatch(qualityBatchId) {
  const rows = await query(
    `WITH stats AS (
       SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status IN ('scored','fallback','failed','cancelled'))::int AS processed,
              count(*) FILTER (WHERE status='scored')::int AS scored,
              count(*) FILTER (WHERE status='fallback')::int AS fallback,
              count(*) FILTER (WHERE status='failed')::int AS failed,
              count(*) FILTER (WHERE status='cancelled')::int AS cancelled,
              count(*) FILTER (WHERE status IN ('queued','running'))::int AS open
       FROM crawler.query_quality_tasks
       WHERE quality_batch_id=$1
     )
     UPDATE crawler.query_quality_batches batch
     SET total_count=stats.total,processed_count=stats.processed,scored_count=stats.scored,
         fallback_count=stats.fallback,failed_count=stats.failed,cancelled_count=stats.cancelled,
         status=CASE
           WHEN stats.open=0 AND batch.status IN ('cancel_requested','cancelled') THEN 'cancelled'
           WHEN stats.open=0 AND stats.failed>0 AND stats.scored+stats.fallback=0 THEN 'failed'
           WHEN stats.open=0 THEN 'done'
           WHEN batch.status='queued' THEN 'running'
           ELSE batch.status
         END,
         started_at=COALESCE(batch.started_at,now()),
         finished_at=CASE WHEN stats.open=0 THEN COALESCE(batch.finished_at,now()) ELSE batch.finished_at END,
         updated_at=now()
     FROM stats
     WHERE batch.quality_batch_id=$1
     RETURNING batch.*`,
    [qualityBatchId],
  );
  return rows.rows[0] ?? null;
}

async function loadQueryQualityChunkContext(qualityChunkId, queryFn = query) {
  const chunkId = String(qualityChunkId ?? "").trim();
  if (!chunkId) return null;
  const rows = await queryFn(
    `SELECT chunk.quality_chunk_id,chunk.quality_batch_id,chunk.status,
            COALESCE(array_agg(member.quality_task_id ORDER BY member.member_ordinal)
              FILTER (WHERE member.quality_task_id IS NOT NULL),'{}'::bigint[]) AS quality_task_ids
     FROM crawler.query_quality_chunks chunk
     LEFT JOIN crawler.query_quality_chunk_members member
       ON member.quality_chunk_id=chunk.quality_chunk_id
     WHERE chunk.quality_chunk_id=$1
     GROUP BY chunk.quality_chunk_id`,
    [chunkId],
  );
  const row = rows.rows[0];
  if (!row) return null;
  return {
    ...row,
    quality_task_ids: (row.quality_task_ids ?? []).map(Number).filter(Number.isSafeInteger),
  };
}

async function processQueryQualityBatch(job, preparedChunk) {
  if (!preparedChunk?.quality_chunk_id || !preparedChunk?.chunk_intent_hash) {
    throw new Error("Query Quality execution requires a persisted Chunk Intent");
  }
  const qualityChunkId = String(preparedChunk.quality_chunk_id);
  const payloadChunkId = String(job.data?.quality_chunk_id ?? "").trim();
  if (payloadChunkId !== qualityChunkId) {
    throw new Error("BullMQ quality_chunk_id conflicts with Chunk Intent");
  }
  const qualityBatchId = String(preparedChunk.quality_batch_id || "").trim();
  const taskIds = [...new Set(
    (preparedChunk.quality_task_ids || []).map(Number).filter(Number.isSafeInteger),
  )];
  if (!qualityBatchId || taskIds.length === 0) {
    throw new Error("Query Quality Chunk has no frozen members");
  }
  const batchRows = await query(
    "SELECT * FROM crawler.query_quality_batches WHERE quality_batch_id=$1 LIMIT 1",
    [qualityBatchId],
  );
  const batch = batchRows.rows[0];
  if (!batch) throw new Error(`query quality batch not found: ${qualityBatchId}`);
  if (["cancel_requested", "cancelled"].includes(batch.status)) {
    await query(
      `UPDATE crawler.query_quality_tasks
       SET status='cancelled',finished_at=now(),updated_at=now()
       WHERE quality_batch_id=$1 AND quality_task_id=ANY($2::bigint[]) AND status IN ('queued','running')`,
      [qualityBatchId, taskIds],
    );
    await query(
      `UPDATE crawler.query_quality_chunks
       SET status='cancelled',dispatch_status='terminal',dispatch_reason='batch_cancelled',
           finished_at=now(),updated_at=now()
       WHERE quality_chunk_id=$1`,
      [qualityChunkId],
    );
    const summary = await refreshQueryQualityBatch(qualityBatchId);
    return {
      ok: true,
      skipped: true,
      reason: "batch_cancelled",
      quality_batch_id: qualityBatchId,
      quality_chunk_id: qualityChunkId,
      summary,
    };
  }
  const taskRows = await query(
    `SELECT task.quality_task_id,task.query_id,term.query_text,
            member.member_ordinal
     FROM crawler.query_quality_chunk_members member
     JOIN crawler.query_quality_tasks task
       ON task.quality_batch_id=member.quality_batch_id
      AND task.quality_task_id=member.quality_task_id
     JOIN crawler.query_terms term ON term.query_id=task.query_id
     WHERE member.quality_chunk_id=$3
       AND task.quality_batch_id=$1
       AND task.quality_task_id=ANY($2::bigint[])
       AND task.status='queued'
     ORDER BY member.member_ordinal`,
    [qualityBatchId, taskIds, qualityChunkId],
  );
  await query(
    `UPDATE crawler.query_quality_chunks
     SET status='running',started_at=COALESCE(started_at,now()),dispatch_reason=NULL,updated_at=now()
     WHERE quality_chunk_id=$1 AND status NOT IN ('done','failed','cancelled')`,
    [qualityChunkId],
  );
  if (taskRows.rows.length === 0) {
    await query(
      `UPDATE crawler.query_quality_chunks chunk
       SET status=CASE WHEN EXISTS (
             SELECT 1 FROM crawler.query_quality_chunk_members member
             JOIN crawler.query_quality_tasks task
               ON task.quality_batch_id=member.quality_batch_id
              AND task.quality_task_id=member.quality_task_id
             WHERE member.quality_chunk_id=chunk.quality_chunk_id
               AND task.status IN ('queued','running')
           ) THEN 'queued' ELSE 'done' END,
           dispatch_status=CASE WHEN EXISTS (
             SELECT 1 FROM crawler.query_quality_chunk_members member
             JOIN crawler.query_quality_tasks task
               ON task.quality_batch_id=member.quality_batch_id
              AND task.quality_task_id=member.quality_task_id
             WHERE member.quality_chunk_id=chunk.quality_chunk_id
               AND task.status IN ('queued','running')
           ) THEN dispatch_status ELSE 'terminal' END,
           finished_at=CASE WHEN NOT EXISTS (
             SELECT 1 FROM crawler.query_quality_chunk_members member
             JOIN crawler.query_quality_tasks task
               ON task.quality_batch_id=member.quality_batch_id
              AND task.quality_task_id=member.quality_task_id
             WHERE member.quality_chunk_id=chunk.quality_chunk_id
               AND task.status IN ('queued','running')
           ) THEN now() ELSE NULL END,
           updated_at=now()
       WHERE chunk.quality_chunk_id=$1`,
      [qualityChunkId],
    );
    const summary = await refreshQueryQualityBatch(qualityBatchId);
    return {
      ok: true,
      skipped: true,
      reason: "tasks_already_terminal",
      quality_batch_id: qualityBatchId,
      quality_chunk_id: qualityChunkId,
      summary,
    };
  }
  await refreshQueryQualityBatch(qualityBatchId);
  const options = preparedChunk.scoring_options || {};
  const requestLocale = resolveYoutubeLocale({
    language: preparedChunk.effective_language,
    country: preparedChunk.effective_country,
  }, { language, country });
  let processed = 0;
  for (const task of taskRows.rows) {
    const currentBatchRows = await query(
      "SELECT status FROM crawler.query_quality_batches WHERE quality_batch_id=$1 LIMIT 1",
      [qualityBatchId],
    );
    if (["cancel_requested", "cancelled"].includes(currentBatchRows.rows[0]?.status)) {
      await query(
        `UPDATE crawler.query_quality_tasks
         SET status='cancelled',finished_at=now(),updated_at=now()
         WHERE quality_batch_id=$1
           AND quality_task_id=ANY($2::bigint[])
           AND status='running'`,
        [qualityBatchId, taskIds],
      );
      await query(
        `UPDATE crawler.query_quality_chunks
         SET status='cancelled',dispatch_status='terminal',dispatch_reason='batch_cancelled',
             finished_at=now(),updated_at=now()
         WHERE quality_chunk_id=$1`,
        [qualityChunkId],
      );
      const summary = await refreshQueryQualityBatch(qualityBatchId);
      return {
        ok: true,
        skipped: true,
        reason: "batch_cancelled_during_scoring",
        quality_batch_id: qualityBatchId,
        quality_chunk_id: qualityChunkId,
        summary,
      };
    }
    const claimed = await query(
      `UPDATE crawler.query_quality_tasks
       SET status='running',attempts=attempts+1,started_at=COALESCE(started_at,now()),
           finished_at=NULL,error_message=NULL,updated_at=now()
       WHERE quality_batch_id=$1 AND quality_task_id=$2 AND status='queued'
       RETURNING quality_task_id`,
      [qualityBatchId, task.quality_task_id],
    );
    if (claimed.rowCount !== 1) continue;
    const [result] = await scoreQueryBatch([task.query_text], {
      language: requestLocale.language,
      country: requestLocale.country,
      minSubscriberCount: Number(options.min_subscriber_count || defaultMinSubscriberCount),
      topVideos: Number(options.top_videos || process.env.QUERY_QUALITY_TOP_VIDEOS || 20),
      concurrency: 1,
      includeVideoSearch: options.include_video_search !== false,
      fallbackOnRateLimit: false,
    });
    if (result?.signals?.rate_limited === true) {
      const evidence = (result.errors ?? [])
        .map((item) => item?.message || String(item))
        .filter(Boolean)
        .join("; ");
      throw annotateYoutubeFailure(
        new Error(evidence || `YouTube rate limited Query Quality task ${task.quality_task_id}`),
        { source: "query_quality_search", client: "WEB" },
      );
    }
    if (!result || !Number.isFinite(Number(result.quality_score))) {
      await query(
        `UPDATE crawler.query_quality_tasks
         SET status='failed',error_message='quality scorer returned no numeric score',finished_at=now(),updated_at=now()
         WHERE quality_task_id=$1`,
        [task.quality_task_id],
      );
      continue;
    }
    const fallback = result.quality_status === "scored_fallback" || result?.signals?.fallback === true;
    const persisted = {
      ...result,
      quality_batch_id: qualityBatchId,
      quality_status_original: result.quality_status,
      score_source: fallback ? "fallback" : result.quality_status === "scored_partial" ? "partial" : "youtube",
    };
    await withTransaction(async (client) => {
      await client.query(
        `UPDATE crawler.query_terms
         SET quality_score=$2::numeric,quality_status='scored',quality_json=$3::jsonb,
             quality_checked_at=$4::timestamptz,updated_at=now()
         WHERE query_id=$1 AND quality_json->>'quality_batch_id'=$5`,
        [
          task.query_id,
          Number(result.quality_score),
          JSON.stringify(persisted),
          result.checked_at || new Date().toISOString(),
          qualityBatchId,
        ],
      );
      await client.query(
        `UPDATE crawler.query_quality_tasks
         SET status=$2,result_json=$3::jsonb,error_message=NULL,finished_at=now(),updated_at=now()
         WHERE quality_task_id=$1`,
        [task.quality_task_id, fallback ? "fallback" : "scored", JSON.stringify(persisted)],
      );
    });
    processed += 1;
    await job.updateProgress({ stage: "scoring", processed, total: taskRows.rows.length });
  }
  await query(
    `UPDATE crawler.query_quality_chunks chunk
     SET status=CASE WHEN EXISTS (
           SELECT 1 FROM crawler.query_quality_chunk_members member
           JOIN crawler.query_quality_tasks task
             ON task.quality_batch_id=member.quality_batch_id
            AND task.quality_task_id=member.quality_task_id
           WHERE member.quality_chunk_id=chunk.quality_chunk_id
             AND task.status IN ('queued','running')
         ) THEN 'queued' ELSE 'done' END,
         dispatch_status=CASE WHEN EXISTS (
           SELECT 1 FROM crawler.query_quality_chunk_members member
           JOIN crawler.query_quality_tasks task
             ON task.quality_batch_id=member.quality_batch_id
            AND task.quality_task_id=member.quality_task_id
           WHERE member.quality_chunk_id=chunk.quality_chunk_id
             AND task.status IN ('queued','running')
         ) THEN dispatch_status ELSE 'terminal' END,
         finished_at=CASE WHEN NOT EXISTS (
           SELECT 1 FROM crawler.query_quality_chunk_members member
           JOIN crawler.query_quality_tasks task
             ON task.quality_batch_id=member.quality_batch_id
            AND task.quality_task_id=member.quality_task_id
           WHERE member.quality_chunk_id=chunk.quality_chunk_id
             AND task.status IN ('queued','running')
         ) THEN now() ELSE NULL END,
         updated_at=now()
     WHERE chunk.quality_chunk_id=$1`,
    [qualityChunkId],
  );
  const summary = await refreshQueryQualityBatch(qualityBatchId);
  return {
    ok: true,
    quality_batch_id: qualityBatchId,
    quality_chunk_id: qualityChunkId,
    processed,
    summary,
  };
}

async function completeQualityTasksWithFallback(qualityBatchId, taskIds, reason) {
  const batchRows = await query(
    "SELECT status FROM crawler.query_quality_batches WHERE quality_batch_id=$1 LIMIT 1",
    [qualityBatchId],
  );
  if (["cancel_requested", "cancelled"].includes(batchRows.rows[0]?.status)) {
    await query(
      `UPDATE crawler.query_quality_tasks
       SET status='cancelled',finished_at=now(),updated_at=now()
       WHERE quality_batch_id=$1
         AND quality_task_id=ANY($2::bigint[])
         AND status IN ('queued','running','failed')`,
      [qualityBatchId, taskIds],
    );
    return refreshQueryQualityBatch(qualityBatchId);
  }
  const rows = await query(
    `SELECT task.quality_task_id,task.query_id,term.query_text
     FROM crawler.query_quality_tasks task
     JOIN crawler.query_terms term ON term.query_id=task.query_id
     WHERE task.quality_batch_id=$1
       AND task.quality_task_id=ANY($2::bigint[])
       AND task.status IN ('queued','running','failed')`,
    [qualityBatchId, taskIds],
  );
  const results = await scoreQueryBatch(rows.rows.map((row) => row.query_text), { fallbackOnly: true });
  const byQuery = new Map(results.map((result) => [String(result.query || "").toLowerCase(), result]));
  for (const row of rows.rows) {
    const result = byQuery.get(String(row.query_text).toLowerCase());
    if (!result) continue;
    const persisted = {
      ...result,
      quality_batch_id: qualityBatchId,
      quality_status_original: result.quality_status,
      score_source: "fallback",
      fallback_trigger: reason,
    };
    await withTransaction(async (client) => {
      await client.query(
        `UPDATE crawler.query_terms
         SET quality_score=$2::numeric,quality_status='scored',quality_json=$3::jsonb,
             quality_checked_at=$4::timestamptz,updated_at=now()
         WHERE query_id=$1 AND quality_json->>'quality_batch_id'=$5`,
        [row.query_id, Number(result.quality_score), JSON.stringify(persisted), result.checked_at, qualityBatchId],
      );
      await client.query(
        `UPDATE crawler.query_quality_tasks
         SET status='fallback',result_json=$2::jsonb,error_message=$3,finished_at=now(),updated_at=now()
         WHERE quality_task_id=$1`,
        [row.quality_task_id, JSON.stringify(persisted), reason],
      );
    });
  }
  return refreshQueryQualityBatch(qualityBatchId);
}

async function persistParserContractFailure(queueName, job, error) {
  if (!isParserContractError(error)) return false;
  const details = parserContractDetails(error);
  const message = youtubeErrorText(error);

  if (queueName === queuesByRole.queryQuality && job?.data?.quality_chunk_id) {
    const context = await loadQueryQualityChunkContext(job.data.quality_chunk_id);
    const qualityBatchId = String(context?.quality_batch_id ?? "");
    const taskIds = context?.quality_task_ids ?? [];
    if (taskIds.length > 0) {
      await withTransaction(async (client) => {
        await client.query(
          `UPDATE crawler.query_terms term
           SET quality_score=NULL,quality_status='failed',
               quality_json=jsonb_build_object(
                 'quality_batch_id',$1::text,
                 'status','parser_failed',
                 'parser_contract_error',$3::jsonb
               ),
               quality_checked_at=NULL,updated_at=now()
           FROM crawler.query_quality_tasks task
           WHERE task.query_id=term.query_id
             AND task.quality_batch_id=$1
             AND task.quality_task_id=ANY($2::bigint[])`,
          [qualityBatchId, taskIds, JSON.stringify(details)],
        );
        await client.query(
          `UPDATE crawler.query_quality_tasks
           SET status='failed',result_json=jsonb_build_object(
                 'parser_contract_error',$3::jsonb
               ),error_message=$4,finished_at=now(),updated_at=now()
           WHERE quality_batch_id=$1
             AND quality_task_id=ANY($2::bigint[])`,
          [qualityBatchId, taskIds, JSON.stringify(details), message],
        );
        await client.query(
          `UPDATE crawler.query_quality_chunks
           SET status='failed',dispatch_status='terminal',dispatch_reason='parser_contract_error',
               finished_at=now(),updated_at=now()
           WHERE quality_chunk_id=$1`,
          [context.quality_chunk_id],
        );
      });
      await refreshQueryQualityBatch(qualityBatchId);
    }
  }

  if (
    queueName === queuesByRole.channelCrawl
    && job?.data?.run_id
    && !job?.data?.candidate_id
  ) {
    await query(
      `UPDATE crawler.channel_runs
       SET status='failed',detail_status='failed',error_message=$2,
           result_json=COALESCE(result_json,'{}'::jsonb)
             || jsonb_build_object('parser_contract_error',$3::jsonb),
           finished_at=now(),updated_at=now()
       WHERE run_id=$1`,
      [String(job.data.run_id), message, JSON.stringify(details)],
    );
  }

  if (queueName === queuesByRole.discoverPage) {
    await persistDiscoverParserContractFailure(query, job, { message, details });
  }
  return true;
}

async function executeJob(job, { resumeMode = "initial", prepared = null } = {}) {
  switch (job.queueName) {
    case queuesByRole.queryQuality:
      return processQueryQualityBatch(job, prepared?.chunk ?? null);
    case queuesByRole.discoverPage:
      return processDiscoverPage(job, prepared?.page ?? null);
    case queuesByRole.channelCrawl:
      if (job.name === "channel-detail-repair") return processContentDetailBatchV2(job);
      if (job.name === "channel-checkpoint-repair") return processCheckpointRepairV2(job);
      return isYoutubeJsFullCrawlFetchContract(
        prepared?.fetchContract ?? job.data?.fetch_contract,
      )
        ? executeFullCrawlYoutubeJs(job, { resumeMode })
        : processChannelCrawlV2(job, { resumeMode });
    case queuesByRole.channelIncremental:
      return incrementalChannelRunner.execute(job);
    case queuesByRole.contentEnrich:
      return contentEnrichExecutor.execute(job);
    case queuesByRole.contentDetail:
      return processContentDetailBatchV2(job);
    case queuesByRole.dataApiBatch:
      return processDataApiBatchV2(job);
    case queuesByRole.agentBatch:
    case queuesByRole.agentIncremental:
      return processAgentBatchV2(job);
    case queuesByRole.finalize:
      return processFinalizeV2(job);
    default:
      await sleep(100);
      return { ok: true, placeholder: true };
  }
}

async function processJobInner(job, { resumeMode = "initial", prepared = null } = {}) {
  const startedAt = Date.now();
  const proxyStart = proxyExecutionSnapshot();
  await job.updateProgress({
    stage: "started",
    started_at: new Date(startedAt).toISOString(),
    proxy: proxyStart,
  });
  await logTaskEvent({
    queueName: job.queueName,
    jobId: job.id,
    jobName: job.name,
    entityKey: job.data?.channel_id ?? job.data?.query_id ?? job.data?.content_key ?? null,
    status: "started",
    payload: { ...(job.data ?? {}), execution_proxy: proxyStart },
  });

  let result;
  try {
    const execute = () => executeJob(job, { resumeMode, prepared });
    result = configuredForProxySlot()
      ? await execute()
      : await runWithProxyIdentity(proxyStart, execute);
  } catch (error) {
    if (error?.code === "FINALIZE_DEFERRED") throw error;
    // This is a quiesced route-selection handoff, not a failed crawl.
    if (error?.code === "UPLOADS_COUNTRY_RECHECK" || isVideoApiHandoff(error) || isVideoExecutionRecoveryPending(error)) throw error;
    const failureDecision = retryableSystemFailureDecision(error)
      ?? decideYoutubeFailure({ error });
    error.youtube_failure_decision = failureDecision;
    if (!configuredForProxySlot()
        && failureDecision.client_action === "refresh_or_fallback"
        && usesChannelExecution(job)) {
      try {
        await closeYoutubeJs();
      } catch (clientError) {
        console.error(JSON.stringify({
          event: "youtube_client_refresh_failed",
          queue: job.queueName,
          job_id: job.id,
          error: clientError?.message || String(clientError),
        }));
      }
    }
    const terminalChannel = classifyTerminalChannelError(error);
    if (
      terminalChannel
      && [queuesByRole.channelCrawl, queuesByRole.channelIncremental].includes(job.queueName)
    ) {
      try {
        if (job.queueName === queuesByRole.channelIncremental && job?.data?.plan_id) {
          await recordIncrementalTerminalFailure({
            job,
            error,
            attempts: Number(job.attemptsMade ?? 0) + 1,
            maxAttempts: Number(job?.opts?.attempts ?? 1),
            permanent: true,
            withTransaction,
          });
        } else {
          await withTransaction((client) => markChannelRemoved(client, {
            channelId: job.data?.channel_id,
            candidateId: job.data?.candidate_id ?? null,
            candidateAttemptFence: job.queueName === queuesByRole.channelCrawl
                && job.data?.candidate_id
              ? activeChannelCandidateAttemptFence(job)
              : null,
            runId: job.data?.run_id ?? null,
            terminal: terminalChannel,
          }));
        }
        job.discard();
      } catch (persistenceError) {
        console.error(JSON.stringify({
          event: "terminal_channel_persistence_failed",
          queue: job.queueName,
          job_id: job.id,
          error: persistenceError?.message || String(persistenceError),
        }));
      }
    }
    try {
      await persistParserContractFailure(job.queueName, job, error);
    } catch (persistenceError) {
      console.error(JSON.stringify({
        event: "parser_contract_persistence_failed",
        queue: job.queueName,
        job_id: job.id,
        error: persistenceError?.message || String(persistenceError),
      }));
    }
    const maxAttempts = Math.max(1, Number(job?.opts?.attempts ?? 1));
    const terminalAttempt = failureDecision.retry_mode === "none"
      || Number(job?.attemptsMade ?? 0) + 1 >= maxAttempts;
    if (
      terminalAttempt
      && job?.queueName === queuesByRole.contentDetail
      && job?.data?.migration_system_retry_id != null
    ) {
      try {
        await withTransaction((client) => settleContentDetailRecoveryTerminalFailure(client, job, {
          failureDecision,
          errorMessage: error?.message ?? String(error),
          parserContractError: isParserContractError(error)
            ? parserContractDetails(error)
            : null,
        }));
      } catch (settlementError) {
        const durabilityError = new AggregateError(
          [error, settlementError],
          `Content Detail recovery failure was not durably settled: ${settlementError.message}`,
        );
        durabilityError.code = "TASK_COMPLETION_CONFLICT";
        durabilityError.cause = settlementError;
        throw durabilityError;
      }
    }
    applyFailureRetryDecision(job, failureDecision);
    throw error;
  }

  const finishedAt = Date.now();
  const proxyEnd = proxyExecutionSnapshot();
  await logTaskEvent({
    queueName: job.queueName,
    jobId: job.id,
    jobName: job.name,
    entityKey: job.data?.channel_id ?? job.data?.query_id ?? job.data?.content_key ?? null,
    status: "completed",
    payload: {
      ...result,
      duration_ms: finishedAt - startedAt,
      execution_proxy_start: proxyStart,
      execution_proxy_end: proxyEnd,
      network_attempt_resume_mode: resumeMode,
    },
  });
  return {
    ...result,
    queue: job.queueName,
    job_id: job.id,
    name: job.name,
    duration_ms: finishedAt - startedAt,
    execution_proxy_start: proxyStart,
    execution_proxy_end: proxyEnd,
    network_attempt_resume_mode: resumeMode,
  };
}

function prepareManagedBusinessRun(job) {
  if (proxySlotRole === "channel") return businessRunPreparer.prepareChannel(job);
  if (proxySlotRole === "discover") return businessRunPreparer.prepareDiscover(job);
  if (proxySlotRole === "query_quality") return businessRunPreparer.prepareQueryQuality(job);
  throw new Error(`no managed Business Run preparer for role ${proxySlotRole || "missing"}`);
}

async function persistManagedRetryCheckpoint({ job, prepared, error, failure }) {
  const message = errorMessage(error).slice(0, 2000);
  if ([queuesByRole.channelCrawl, queuesByRole.contentDetail].includes(job.queueName)) {
    const runId = String(prepared?.businessRunId ?? job.data?.run_id ?? "").trim();
    if (runId) {
      await query(
        `UPDATE crawler.content_candidates
         SET detail_status='failed',error_message=COALESCE(error_message,$2),updated_at=now()
         WHERE run_id=$1 AND detail_status='running'`,
        [runId, message],
      );
    }
    return true;
  }
  if (job.queueName === queuesByRole.channelIncremental) {
    // IncrementalChannelRunner persists the active domain and Run failure before rethrowing.
    return true;
  }
  if (job.queueName === queuesByRole.contentEnrich) {
    return error?.content_enrich_checkpoint_persisted === true;
  }
  if (job.queueName === queuesByRole.discoverPage) {
    return persistDiscoverRetryCheckpoint(query, { job, prepared, failure, message });
  }
  if (job.queueName === queuesByRole.queryQuality) {
    const chunkId = String(prepared?.chunk?.quality_chunk_id ?? job.data?.quality_chunk_id ?? "").trim();
    const taskIds = (prepared?.chunk?.quality_task_ids ?? [])
      .map(Number)
      .filter(Number.isSafeInteger);
    await withTransaction(async (client) => {
      if (taskIds.length > 0) {
        await client.query(
          `UPDATE crawler.query_quality_tasks
           SET status='queued',error_message=$2,finished_at=NULL,updated_at=now()
           WHERE quality_task_id=ANY($1::bigint[]) AND status='running'`,
          [taskIds, message],
        );
      }
      await client.query(
        `UPDATE crawler.query_quality_chunks
         SET status='queued',dispatch_reason=$2,updated_at=now()
         WHERE quality_chunk_id=$1 AND status NOT IN ('done','failed','cancelled')`,
        [chunkId, failure.observation],
      );
    });
    return true;
  }
  return false;
}

async function processJob(job, token) {
  await gateVideoApiJob({ query, job, token });
  if(job.queueName===queuesByRole.channelCrawl&&job.name==='channel-snapshot'&&job.data?.migration_control_start){
    if(!migrationBatchControlEnabled())throw new Error('Migration batch control is not enabled on this worker');
    if(!await prepareControlledMigrationSnapshot({query,withTransaction,job}))return {not_started:true};
  }
  if(job.queueName===queuesByRole.channelCrawl&&job.name===MIGRATION_START_JOB){
    if(!migrationBatchControlEnabled())throw new Error("Migration batch control is not enabled on this worker");
    return startControlledMigrationChannel({query,withTransaction,batchId:job.data.batch_id,channelId:job.data.channel_id});
  }
  try {
    const result = await processJobWithOwnership(job, token);
    if (job.queueName === queuesByRole.finalize) {
      await query(`UPDATE crawler.finalize_recovery_requests SET defer_until=NULL,defer_job_id=NULL,
        defer_count=0,first_deferred_at=NULL,defer_reason=NULL
        WHERE channel_id=$1 AND defer_job_id=$2`, [job.data.channel_id,String(job.id)]);
    }
    return result;
  } catch (error) {
    if (error?.code === "FINALIZE_DEFERRED") {
      return delayFinalizeJob({ query, job, token, error });
    }
    // Entry/settlement fences can fail outside processJobInner as well.
    if (isStaleExecutionFailure(error)) job.discard();
    throw error;
  }
}

async function processJobWithOwnership(job, token) {
  assertFullCrawlWorkerLane(job, process.env.FULL_CRAWL_CANARY_WORKER === "true");
  await assertFullCrawlSnapshotRecoveryOwner(query, job);
  if (job?.data?.retry_intent_id) {
    const entry = await enterMigrationRetryIntentWorkerJob(query, job);
    if (entry.action === "finished_replay") {
      console.log(JSON.stringify({
        event: "migration_retry_intent_post_commit_replayed",
        job_id: job.id,
        job_attempt: Number(job.attemptsStarted),
        terminal_job_attempt: entry.terminalJobAttempt,
        retry_intent_id: String(job.data.retry_intent_id),
        dispatch_generation: Number(job.data.dispatch_generation),
      }));
      return {
        recovered_post_commit: true,
        retry_intent_id: String(job.data.retry_intent_id),
        terminal_job_attempt: entry.terminalJobAttempt,
      };
    }
    if (entry.action !== "execute") {
      const error = new Error(`Recovery Intent fence rejected Job: ${job.id}`);
      error.code = "MIGRATION_RETRY_INTENT_FENCE_STALE";
      throw error;
    }
  }
  if (job?.queueName === queuesByRole.channelCrawl && job?.data?.candidate_id) {
    const marked = await markChannelCandidateJobAttemptActive(query, job);
    if (!marked) {
      const error = new Error(`Candidate attempt fence rejected Job: ${job.id}`);
      error.code = "CANDIDATE_ATTEMPT_FENCE_STALE";
      throw error;
    }
  }
  const executeManaged = () => {
    if (!configuredForProxySlot()) return processJobInner(job);
    return processManagedWorkerJob({
      job,
      token,
      execute: () => rotaSlot.executeJob(job, {
        prepare: () => prepareManagedBusinessRun(job),
        executeAttempt: (prepared, attempt) => executeManagedWorkerAttempt({
          job,
          prepared,
          attempt,
          execute: ({ resumeMode }) => processJobInner(job, { resumeMode, prepared }),
          persistRetryableCheckpoint: persistManagedRetryCheckpoint,
        }),
      }),
      terminateBusinessRun: (currentJob, error) => (
        currentJob.queueName === queuesByRole.channelIncremental
          ? terminateExhaustedIncrementalRun(withTransaction, currentJob, error)
          : terminateExhaustedBusinessRun(withTransaction, currentJob, error)
      ),
      deferForSlotPause: deferJobForSlotPause,
      defaultDelayMs: proxySlotPollMs,
      onDeferred: (event) => console.log(JSON.stringify({ event: "rota_job_deferred", ...event })),
    });
  };
  const execute = () => runVideoExecutionResumable({ job, token, execute: () => runVideoApiResumable({ job, token, execute: executeManaged,
    executeReplay: () => processJobInner(job, { resumeMode: "api_continuation" }) }) });
  if (job?.queueName === queuesByRole.channelCrawl && job?.data?.candidate_id) {
    return runChannelCandidateWorkerJobWithDurableSettlement({
      query,
      withTransaction,
      finishMigrationRetryIntent,
      job,
      execute,
    });
  }
  return execute();
}

const enabledQueues = String(process.env.WORKER_QUEUES || queueNames.join(","))
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);
const channelWorkloads = [
  ...(enabledQueues.includes(queuesByRole.channelCrawl)
    ? [{ workloadKind: "channel_full", fetchContract: defaultFullCrawlFetchContract() }] : []),
  ...(enabledQueues.includes(queuesByRole.channelIncremental)
    ? [{ workloadKind: "channel_incremental" }] : []),
  ...(enabledQueues.includes(queuesByRole.contentEnrich)
    ? [{ workloadKind: "content_enrich" }] : []),
];
const channelCapabilities = channelWorkloads.map(
  (workload) => channelExtractorCapabilities(workload, incrementalVideoExecutorMode),
);
if (channelCapabilities.some((capabilities) => !capabilities.ytdlp) && !youtubeJsDetailEnabled()) {
  throw new Error("YouTubeJS Full Crawl and Incremental require YOUTUBEJS_EXTRACTOR_MODE=full");
}
const intakePrefix = fullCrawlWorkerPrefix({
  prefix: bullmqPrefix,
  enabledQueues,
  canary: process.env.FULL_CRAWL_CANARY_WORKER === "true",
});
const workerQueueConfiguration = validateWorkerQueueConfiguration({
  role: proxySlotRole,
  enabledQueues,
  fixedProxy: fixedProxy !== null,
});
const queues = createQueues();
const processDiscoverPage = createDiscoverPageProcessor({
  query,
  queues,
  putRawObject,
  getCrawlSettingsV2,
  getQueryScheduler,
  signalReadyDiscoveryPageQualifications,
  language,
  country,
  demoChannelId: () => nanoid(8),
});

let workers = [];
let localIntake = null;
let localIntakeSignals = null;
let shutdownPromise = null;

async function shutdownStep(stage, action) {
  const startedAt = Date.now();
  console.log(JSON.stringify({ event: "shutdown_step", stage, status: "started" }));
  await action();
  console.log(JSON.stringify({
    event: "shutdown_step",
    stage,
    status: "completed",
    duration_ms: Date.now() - startedAt,
  }));
}

function shutdown(signal) {
  if (shutdownPromise) return shutdownPromise;
  shuttingDown = true;
  shutdownPromise = (async () => {
    console.log(`received ${signal}, shutting down workers`);
    await shutdownStep("bullmq_intake", () => Promise.all(workers.map((worker) => worker.pause(true))));
    if(localIntake)await shutdownStep('local_intake',()=>localIntake.stop());
    if(localIntakeSignals)await shutdownStep('local_intake_notifications',()=>localIntakeSignals.close());
    if (rotaSlot) await shutdownStep("rota_slot", () => rotaSlot.close());
    await shutdownStep("bullmq_workers", () => Promise.all(workers.map((worker) => worker.close())));
    await shutdownStep("full_crawl_youtubejs_queues", closeFullCrawlYoutubeJsQueues);
    await shutdownStep("pipeline_queues", closePipelineV2Queues);
    await shutdownStep("proxy_control", closeProxyControlClient);
    await shutdownStep("http", closePersistentHttpClient);
    await shutdownStep("storage", async () => closeStorage());
    await shutdownStep("queues", () => closeQueues(queues));
    await shutdownStep("database", closeDb);
    console.log(JSON.stringify({ event: "shutdown_complete", signal }));
    if (!embedded) process.exit(0);
  })().catch((error) => {
    console.error(JSON.stringify({ event: "shutdown_failed", signal, error: error?.stack || String(error) }));
    if (!embedded) process.exit(1);
    throw error;
  });
  return shutdownPromise;
}



async function recordFailedJob(queueName, job, error) {
      const failure = describeChannelCandidateWorkerFailure(error);
      const {
        message,
        parserFailure,
        terminalChannel,
        businessRunBudgetTerminal,
        systemFailure,
        parserDetails,
        failureDecision,
        permanentFailure,
      } = failure;
      console.error(JSON.stringify({
        event: "failed",
        queue: queueName,
        job_id: job?.id,
        name: job?.name,
        error: message,
        failure_kind: failureDecision.kind,
        retry_mode: failureDecision.retry_mode,
      }));
      try {
        const contentEnrichResult = contentEnrichResultFromError(error);
        await logTaskEvent({
          queueName,
          jobId: job?.id,
          jobName: job?.name,
          entityKey: job?.data?.channel_id ?? job?.data?.query_id ?? job?.data?.content_key ?? null,
          status: "failed",
          payload: {
            ...(job?.data ?? {}),
            ...(error?.channel_execution_attempt
              ? { channel_execution_attempt: error.channel_execution_attempt }
              : {}),
            ...(parserDetails ? { parser_contract_error: parserDetails } : {}),
            ...(contentEnrichResult
              ? { content_enrich_result: contentEnrichResult }
              : {}),
            youtube_failure_decision: failureDecision,
          },
          errorMessage: message,
        });
        const maxAttempts = Math.max(1, Number(job?.opts?.attempts ?? 1));
        const attemptsMade = Number(job?.attemptsMade ?? 0);
        if (queueName === queuesByRole.dataApiBatch && job?.name === "youtube-data-api-batch") {
          const state = await job.getState();
          if (state === "failed") {
            const recovery = await settleTerminalDataApiBatchJob({
              job,
              withTransaction,
              observationKind: "failed",
              error,
            });
            if (recovery.action === "settled") {
              console.log(JSON.stringify({
                event: "data_api_batch_execution_orphan_recovered",
                job_id: job.id,
                job_attempt: Number(job.attemptsStarted),
                observation_kind: "failed",
              }));
            }
          }
        }
        if (queueName === queuesByRole.channelIncremental && job?.data?.plan_id) {
          const terminalFailure = await recordIncrementalTerminalFailure({
            job,
            error,
            attempts: attemptsMade,
            maxAttempts,
            permanent: permanentFailure || terminalChannel !== null,
            withTransaction,
          });
          if (terminalFailure.recorded) {
            console.log(JSON.stringify({
              event: "incremental_terminal_failure_recorded",
              job_id: job.id,
              plan_id: job.data.plan_id,
              domain: terminalFailure.domain,
              failure_kind: terminalFailure.failure_kind,
            }));
          }
        }
        if (queueName === queuesByRole.queryQuality && job?.data?.quality_chunk_id) {
          const context = await loadQueryQualityChunkContext(job.data.quality_chunk_id);
          const qualityBatchId = String(context?.quality_batch_id ?? "");
          const taskIds = context?.quality_task_ids ?? [];
          const terminal = permanentFailure || terminalChannel !== null || attemptsMade >= maxAttempts;
          if (taskIds.length > 0 && !parserFailure && terminal) {
            await completeQualityTasksWithFallback(qualityBatchId, taskIds, message);
          } else if (taskIds.length > 0 && !parserFailure) {
            await query(
              `UPDATE crawler.query_quality_tasks
               SET status='queued',error_message=$3,updated_at=now()
               WHERE quality_batch_id=$1
                 AND quality_task_id=ANY($2::bigint[])
                 AND status='running'`,
              [qualityBatchId, taskIds, message],
            );
          }
          if (context) {
            if (parserFailure) {
              await query(
                `UPDATE crawler.query_quality_chunks
                 SET status='failed',dispatch_status='terminal',dispatch_reason='parser_contract_error',
                     finished_at=now(),updated_at=now()
                 WHERE quality_chunk_id=$1`,
                [context.quality_chunk_id],
              );
            } else if (terminal) {
              await query(
                `UPDATE crawler.query_quality_chunks chunk
                 SET status=CASE WHEN EXISTS (
                       SELECT 1
                       FROM crawler.query_quality_chunk_members member
                       JOIN crawler.query_quality_tasks task
                         ON task.quality_batch_id=member.quality_batch_id
                        AND task.quality_task_id=member.quality_task_id
                       WHERE member.quality_chunk_id=chunk.quality_chunk_id
                         AND task.status IN ('queued','running')
                     ) THEN 'failed' ELSE 'done' END,
                     dispatch_status='terminal',dispatch_reason=$2,
                     finished_at=now(),updated_at=now()
                 WHERE chunk.quality_chunk_id=$1`,
                [context.quality_chunk_id, message],
              );
            } else {
              await query(
                `UPDATE crawler.query_quality_chunks
                 SET status='queued',dispatch_reason=$2,finished_at=NULL,updated_at=now()
                 WHERE quality_chunk_id=$1 AND status NOT IN ('done','failed','cancelled')`,
                [context.quality_chunk_id, failureDecision.kind],
              );
            }
          }
          if (qualityBatchId) {
            await refreshQueryQualityBatch(qualityBatchId);
          }
        }
        if (queueName === queuesByRole.channelCrawl && job?.data?.candidate_id) {
          await failChannelCandidateWorkerJob({
            query,
            withTransaction,
            job,
            error,
            failure,
            refreshDispatchCandidateCounts,
            signalReadyDiscoveryPageQualifications,
            finishMigrationRetryIntent,
          });
        }
        if (
          queueName === queuesByRole.channelCrawl
          && job?.data?.run_id
          && !job?.data?.candidate_id
          && !terminalChannel
          && !businessRunBudgetTerminal
          && !systemFailure
          && (permanentFailure || attemptsMade >= maxAttempts)
        ) {
          await query(
            `UPDATE crawler.channel_runs
             SET status='failed',detail_status='failed',error_message=$2,
                 result_json=result_json || $3::jsonb,
                 finished_at=now(),updated_at=now()
             WHERE run_id=$1`,
            [
              String(job.data.run_id),
              message,
              JSON.stringify(terminalChannelRunFailureEvidence({
                message,
                parserDetails,
                failureDecision,
                permanentFailure,
              }, { attemptsMade, maxAttempts })),
            ],
          );
        }
        if (queueName === queuesByRole.discoverPage) {
          await recordDiscoverPageTerminalFailure(query, job, {
            message, parserFailure, parserDetails, failureDecision, permanentFailure, attemptsMade, maxAttempts,
          });
        }
      } catch (eventError) {
        console.error(JSON.stringify({ event: "task_event_failed", error: eventError?.message || String(eventError) }));
      }
}

async function startWorkerRuntime() {
  console.log(JSON.stringify({
    event: "worker_queue_configuration",
    managed: workerQueueConfiguration.managed,
    role: workerQueueConfiguration.role,
    queues: workerQueueConfiguration.queues,
  }));
  await ensureSchema();
  if (shuttingDown) return;
  if (
    enabledQueues.includes(queuesByRole.agentBatch)
    || enabledQueues.includes(queuesByRole.agentIncremental)
  ) await ensureDefaultAgentConfig();
  if (shuttingDown) return;
  const proxyReady = await waitForProxySlot();
  if (!proxyReady || shuttingDown) return;
  console.log(JSON.stringify({ event: "db_pool_warm", ...(await warmDb()) }));
  if (shuttingDown) return;

  if (channelExecutionEnabled() && !configuredForProxySlot()) {
    if (channelCapabilities.some((capabilities) => capabilities.ytdlp)) {
      const warmResult = await warmPersistentYtDlp();
      console.log(JSON.stringify({ event: "ytdlp_pool_warm", ...warmResult }));
    }
    const youtubeJsWarmResult = await warmYoutubeJs();
    console.log(JSON.stringify({ event: "youtubejs_pool_warm", ...youtubeJsWarmResult }));
  }
  if (shuttingDown) return;

  const localControl=process.env.LOCAL_INCREMENTAL_INTAKE_CONTROL==='true';
  if(localControl && (enabledQueues.length!==1 || enabledQueues[0]!==queuesByRole.channelIncremental || concurrencyFor(enabledQueues[0])!==1)) {
    throw new Error('Local intake control requires one incremental Worker with concurrency 1');
  }
  workers = enabledQueues.map((queueName) => {
    const worker = new Worker(queueName, (job,token)=>localControl?localIntake.process(job,token,()=>processJob(job,token)):processJob(job,token), {
      connection: redisOptions,
      concurrency: process.env.FULL_CRAWL_CANARY_WORKER === "true" ? 1 : concurrencyFor(queueName),
      ...bullmqWorkerTimingOptions(),
      ...(intakePrefix ? { prefix: intakePrefix } : {}),
      ...(localControl?{autorun:false,name:intakeWorkerName('local',proxyWorkerId)}:{}),
    });

    worker.on("completed", async (job) => {
      console.log(JSON.stringify({ event: "completed", queue: queueName, job_id: job.id, name: job.name }));
      if (queueName === queuesByRole.dataApiBatch && job?.name === "youtube-data-api-batch") {
        try {
          const recovery = await settleTerminalDataApiBatchJob({
            job,
            withTransaction,
            observationKind: "completed",
          });
          if (recovery.action === "settled") {
            console.log(JSON.stringify({
              event: "data_api_batch_execution_orphan_recovered",
              job_id: job.id,
              job_attempt: Number(job.attemptsStarted),
              observation_kind: "completed",
            }));
          }
        } catch (eventError) {
          console.error(JSON.stringify({
            event: "data_api_batch_execution_orphan_recovery_failed",
            job_id: job.id,
            error: eventError?.message || String(eventError),
          }));
        }
      }
    });

    worker.on("failed", (job, error) => recordFailedJob(queueName, job, error));

    console.log(`worker started queue=${queueName} concurrency=${worker.opts.concurrency}`);
    return worker;
  });
  if(localControl){
    if(process.env.LOCAL_INCREMENTAL_INTAKE_NOTIFICATIONS==='true'){
      if(process.env.LOCAL_INCREMENTAL_NATS_URL){
        localIntakeSignals=await localIntakeSignalsFromEnv(proxyWorkerId);
      }else{
        if(!process.env.LOCAL_INCREMENTAL_NOTIFY_DATABASE_URL)throw new Error('DIRECT_NOTIFICATION_DATABASE_REQUIRED');
        localIntakeSignals=await createTransportSignals({connectionString:process.env.LOCAL_INCREMENTAL_NOTIFY_DATABASE_URL});
      }
    }
    localIntake=new LocalIncrementalIntake({worker:workers[0],query,signals:localIntakeSignals,workerId:proxyWorkerId,report:value=>console.log(JSON.stringify(value))});
    await localIntake.start();
  }
}

let compatibilityBusy = false;
return {
  async start() {
    if (embedded) throw new Error('EMBEDDED_WORKER_CANNOT_START_CONSUMERS');
    try { await startWorkerRuntime(); } catch (error) { if (!shuttingDown) throw error; }
  },
  shutdown,
  async startCompatibility() {
    if (!embedded || process.env.SKIP_SCHEMA_MIGRATION !== "true"
      || enabledQueues.length !== 1 || enabledQueues[0] !== queuesByRole.channelCrawl
      || proxySlotRole !== "channel" || !rotaSlot) {
      throw new Error("FULL_CRAWL_COMPATIBILITY_CONFIG_INVALID");
    }
    await warmDb();
    if (!await waitForProxySlot() || shuttingDown) throw new Error('FULL_CRAWL_COMPATIBILITY_STOPPED');
  },
  async execute(job, token) {
    if (!embedded || job.queueName !== queuesByRole.channelCrawl) throw new Error("FULL_CRAWL_COMPATIBILITY_QUEUE_INVALID");
    if (shuttingDown || compatibilityBusy) return deferJobForSlotPause(job, token);
    compatibilityBusy = true;
    try {
      return await processJob(job, token);
    } catch (error) {
      if (!(error instanceof DelayedError) && !isVideoApiHandoff(error)) {
        // The parent BullMQ consumer increments attemptsMade after this rejection.
        const failedJob = Object.create(job);
        failedJob.attemptsMade = Number(job.attemptsMade ?? 0) + 1;
        await recordFailedJob(job.queueName, failedJob, error);
      }
      throw error;
    } finally {
      compatibilityBusy = false;
    }
  },
  replay: job => withVideoApiReplay(() => processJobInner(job, { resumeMode: "api_continuation" })),
};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const runtime = createWorkerRuntime();
  process.once("SIGTERM", () => void runtime.shutdown("SIGTERM"));
  process.once("SIGINT", () => void runtime.shutdown("SIGINT"));
  await runtime.start();
}
