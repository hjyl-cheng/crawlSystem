import { Worker } from "bullmq";
import { nanoid } from "nanoid";
import { ensureDefaultAgentConfig } from "./agentConfig.js";
import { ChannelExecutionRuntimeAdapter } from "./channelExecutionRuntimeAdapter.js";
import {
  allocateDiscoveredChannelSnapshotDispatches,
  buildDiscoveredChannelSnapshotJob,
} from "./channelSnapshotDispatch.js";
import { deferJobForSlotPause } from "./channelJobDeferral.js";
import { DiscoverExecutionRuntimeAdapter } from "./discoverExecutionRuntimeAdapter.js";
import { buildDemoChannelCrawlJob } from "./demoChannelDispatch.js";
import { reconcileDispatchBatchCandidateState } from "./dispatchBatchCandidateState.js";
import { evaluateDiscoveryChannelQualification } from "./channelQualification.js";
import {
  classifyTerminalChannelError,
  markChannelRemoved,
} from "./channelLifecycle.js";
import {
  completeChannelCandidateWorkerJob,
  describeChannelCandidateWorkerFailure,
  failChannelCandidateWorkerJob,
  runChannelCandidateWorkerJobWithDurableSettlement,
} from "./channelCandidateWorkerLifecycle.js";
import { activeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";
import { settleTerminalDataApiBatchJob } from "./dataApiBatchJobRecovery.js";
import {
  persistChannelCandidateParserContractFailure,
  StaleChannelCandidateAttemptError,
} from "./channelCandidateAttemptMutations.js";
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
  fetchIncrementalVideoDetail,
} from "./incrementalVideo.js";
import { dynamicRotaProxyConfig, fixedRotaProxyConfig } from "./fixedProxyConfig.js";
import { resolveWorkerIdentityPolicy } from "./identityPolicyCatalog.js";
import { IncrementalAgentBacklog } from "./incrementalAgentBacklog.js";
import { IncrementalChannelRunner } from "./incrementalChannelRunner.js";
import { IncrementalRunStore } from "./incrementalRunStore.js";
import { recordIncrementalTerminalFailure } from "./incrementalTerminalFailure.js";
import {
  getCrawlSettingsV2,
  processAgentBatchV2,
  processChannelCrawlV2,
  processCheckpointRepairV2,
  processContentDetailBatchV2,
  processDataApiBatchV2,
  processFinalizeV2,
  signalReadyDiscoveryPageQualifications,
} from "./pipelineV2.js";
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
} from "./managedWorkerJob.js";
import {
  finishMigrationRetryIntent,
  markMigrationRetryIntentRunning,
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
import { closeStorage, putRawObject } from "./storage.js";
import { RotaSlotAdapter } from "./rotaSlotAdapter.js";
import { warmPersistentYtDlp } from "./ytdlpSession.js";
import { closeYoutubeJs, warmYoutubeJs } from "./youtubeJs.js";
import { annotateYoutubeFailure, decideYoutubeFailure } from "./youtubeFailurePolicy.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";
import {
  classifyYoutubeError,
  extractVideoOwnerCandidates,
  fetchPopularThisYearVideoSearchInitial,
  fetchSearchContinuation,
  findContinuationToken,
} from "./youtube.js";
import { resolveYoutubeLocale } from "./youtubeLocale.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const queues = createQueues();
const incrementalRunStore = new IncrementalRunStore({ withTransaction });
const incrementalAgentBacklog = new IncrementalAgentBacklog({ withTransaction });
const incrementalChannelRunner = new IncrementalChannelRunner({
  runStore: incrementalRunStore,
  agentBacklog: incrementalAgentBacklog,
  query,
  withTransaction,
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
    return new ChannelExecutionRuntimeAdapter({ workerId: proxyWorkerId });
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

async function saveFetchedRaw({ fetched, objectType, entityType, entityId, source, metadata = {} }) {
  if (!fetched?.rawText) return null;
  return putRawObject({
    objectType,
    entityType,
    entityId,
    source,
    payload: fetched.rawText,
    contentType: fetched.rawContentType || "application/json; charset=utf-8",
    metadata: {
      url: fetched.url ?? null,
      ...metadata,
    },
  });
}

async function updateNetworkEvent({ phase, ok, targetUrl = null, httpStatus = null, errorType = null, error = null, payload = {} }) {
  await query(
    `INSERT INTO crawler.task_events (
       queue_name, job_id, job_name, entity_key, status, payload_json, error_message
     )
     VALUES ('network', NULL, $1, $2, $3, $4::jsonb, $5)`,
    [
      phase,
      targetUrl,
      ok ? "completed" : "failed",
      JSON.stringify({
        phase,
        ok,
        http_status: httpStatus,
        error_type: errorType,
        ...payload,
      }),
      error ? errorMessage(error) : null,
    ],
  );
}

function mergeDiscoveryCandidates(candidates) {
  const videoViews = (video, fallback = null) => {
    const value = Number(video?.view_count);
    return Number.isFinite(value) && value >= 0 ? value : fallback;
  };
  const grouped = new Map();
  for (const candidate of candidates.filter(Boolean)) {
    if (!candidate.channel_id) continue;
    const list = grouped.get(candidate.channel_id) ?? [];
    list.push(candidate);
    grouped.set(candidate.channel_id, list);
  }
  return [...grouped.values()]
    .map((items) => {
      const videos = items
        .map((item) => item.source_video)
        .filter((video) => video?.video_id);
      const uniqueVideos = [...new Map(videos.map((video) => [video.video_id, video])).values()]
        .sort((a, b) => videoViews(b, 0) - videoViews(a, 0));
      const best = [...items].sort((a, b) => {
        const viewsA = videoViews(a.source_video, -1);
        const viewsB = videoViews(b.source_video, -1);
        if (viewsA !== viewsB) return viewsB - viewsA;
        return Number(a.rank_position ?? 9999) - Number(b.rank_position ?? 9999);
      })[0];
      const sourceStrategies = [...new Set(items.map((item) => item.discovery_strategy ?? "video_popularity_this_year"))];
      const matchedVideoCount = uniqueVideos.length;
      const bestVideoViews = videoViews(uniqueVideos[0]);
      const totalTopVideoViews = uniqueVideos.reduce((sum, video) => sum + videoViews(video, 0), 0);
      return {
        ...best,
        title: best.title ?? items.find((item) => item.title)?.title ?? null,
        handle: best.handle ?? items.find((item) => item.handle)?.handle ?? null,
        description: best.description ?? items.find((item) => item.description)?.description ?? null,
        avatar_url: best.avatar_url ?? items.find((item) => item.avatar_url)?.avatar_url ?? null,
        subscriber_count_text: best.subscriber_count_text ?? items.find((item) => item.subscriber_count_text)?.subscriber_count_text ?? null,
        subscriber_count: best.subscriber_count ?? items.find((item) => item.subscriber_count != null)?.subscriber_count ?? null,
        discovery_strategy: best.discovery_strategy ?? sourceStrategies[0] ?? "video_popularity_this_year",
        aggregate: {
          source_strategies: sourceStrategies,
          matched_video_count: matchedVideoCount,
          best_video_views: bestVideoViews,
          total_top_video_views: totalTopVideoViews,
          top_videos: uniqueVideos.slice(0, 5),
        },
        raw: {
          selected: best.raw,
          sources: items.map((item) => ({
            discovery_strategy: item.discovery_strategy ?? "video_popularity_this_year",
            rank_position: item.rank_position,
            source_video: item.source_video ?? null,
          })),
        },
      };
    })
    .sort((a, b) => {
      const viewsA = a.aggregate?.best_video_views ?? -1;
      const viewsB = b.aggregate?.best_video_views ?? -1;
      if (viewsA !== viewsB) return viewsB - viewsA;
      return Number(a.rank_position ?? 9999) - Number(b.rank_position ?? 9999);
    })
    .map((candidate, index) => ({ ...candidate, rank_position: index + 1 }));
}

async function ensureDiscoveryDispatchBatch(dispatchBatchId, pipelineCycleId) {
  await query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,result_json,updated_at
     ) VALUES ($1,$2,'running',$3::jsonb,now())
     ON CONFLICT (dispatch_batch_id) DO UPDATE
     SET result_json=crawler.query_dispatch_batches.result_json || EXCLUDED.result_json,
         updated_at=now()`,
    [
      dispatchBatchId,
      pipelineCycleId || dispatchBatchId,
      JSON.stringify({ source: "discover_worker" }),
    ],
  );
}

async function refreshDispatchCandidateCounts(dispatchBatchId) {
  return reconcileDispatchBatchCandidateState(query, dispatchBatchId);
}

async function processDiscoverPage(job, preparedPage) {
  if (!preparedPage?.page_id || !preparedPage?.page_intent_hash) {
    throw new Error("Discover execution requires a persisted Page Intent");
  }
  const payloadPageId = String(job.data?.page_id ?? "").trim();
  const pageId = String(preparedPage.page_id);
  if (payloadPageId !== pageId) throw new Error("BullMQ Discover page_id conflicts with Page Intent");
  const managedIntent = preparedPage.result_json?.managed_intent ?? {};
  const queryText = String(preparedPage.query_text);
  const queryId = preparedPage.query_id ?? null;
  const pageNo = Number(preparedPage.page_no);
  const discoveryRunId = String(managedIntent.discovery_run_id ?? "").trim() || null;
  const pipelineCycleId = String(managedIntent.pipeline_cycle_id ?? "").trim() || null;
  const dispatchBatchId = String(preparedPage.dispatch_batch_id ?? "").trim();
  if (!dispatchBatchId) throw new Error("Discover Page Intent requires dispatch_batch_id");
  const requestLocale = resolveYoutubeLocale({
    language: preparedPage.request_language,
    country: preparedPage.request_country,
  }, { language, country });
  const requestLanguage = requestLocale.language;
  const requestCountry = requestLocale.country;
  const continuation = String(managedIntent.continuation_token ?? "").trim() || null;
  const ytConfig = managedIntent.yt_config ?? null;
  const priority = Number(preparedPage.priority ?? 100);
  const demo = managedIntent.demo === true;
  const { minSubscriberCount, discoverStopMinQualifiedRatio } = await getCrawlSettingsV2();
  await ensureDiscoveryDispatchBatch(dispatchBatchId, pipelineCycleId);
  const claimed = await query(
    `UPDATE crawler.query_pages
     SET status='running',managed_fetch_status='running',
         managed_fetch_started_at=COALESCE(managed_fetch_started_at,now()),
         managed_fetch_finished_at=NULL,managed_fetch_error_code=NULL,
         error_message=NULL,started_at=COALESCE(started_at,now()),updated_at=now()
     WHERE page_id=$1 AND managed_fetch_status<>'done'
     RETURNING page_id`,
    [pageId],
  );
  if (claimed.rowCount !== 1) {
    const current = await query(
      "SELECT managed_fetch_status FROM crawler.query_pages WHERE page_id=$1 LIMIT 1",
      [pageId],
    );
    if (current.rows[0]?.managed_fetch_status === "done") {
      return { ok: true, page_id: pageId, managed_fetch_complete: true };
    }
    throw new Error(`Discover Page Intent is not executable: ${pageId}`);
  }

  if (demo) {
    const channelId = managedIntent.channel_id || `UCdemo${nanoid(8)}`;
    const channelUrl = `https://www.youtube.com/channel/${channelId}`;
    const demoJob = buildDemoChannelCrawlJob({ pageId, channelId, pipelineCycleId });
    await query(
      `INSERT INTO crawler.channels (
         channel_id, channel_url, handle, title, subscriber_count, subscriber_count_text,
         status, ready_for_agent, source_json, updated_at
       )
         VALUES ($1, $2, $3, $4, 50000, '50000 subscribers', 'active', false, $5::jsonb, now())
       ON CONFLICT (channel_id)
       DO UPDATE SET ready_for_agent = true,
                     source_json = crawler.channels.source_json || EXCLUDED.source_json,
                     updated_at = now()`,
      [channelId, channelUrl, "@demo", "Demo Channel", JSON.stringify({ source: "demo", query_text: queryText })],
    );
    await queues[queuesByRole.channelCrawl].add(
      demoJob.name,
      demoJob.data,
      demoJob.options,
    );
  } else {
    let fetched;
    try {
      fetched = continuation
        ? await fetchSearchContinuation(ytConfig, continuation, { language: requestLanguage })
        : await fetchPopularThisYearVideoSearchInitial(queryText, {
            language: requestLanguage,
            country: requestCountry,
          });
      await saveFetchedRaw({
        fetched,
        objectType: continuation ? "youtube_search_continuation_json" : "youtube_search_html",
        entityType: "query_page",
        entityId: pageId,
        source: continuation ? "youtube_search_continuation" : "youtube_search",
        metadata: {
          query_id: queryId,
          query_text: queryText,
          page_no: pageNo,
          continuation: Boolean(continuation),
          search_type: "video",
          sort: "popularity",
          upload_date: "this_year",
        },
      });
      await updateNetworkEvent({
        phase: continuation ? "discover_continuation" : "discover_initial",
        ok: true,
        targetUrl: fetched.url ?? "https://www.youtube.com/results",
        payload: { query_id: queryId, page_no: pageNo },
      });
    } catch (error) {
      const errorType = classifyYoutubeError(error);
      await query(
        `UPDATE crawler.query_pages
         SET status = 'failed',
             managed_fetch_status='failed',
             managed_fetch_finished_at=now(),
             managed_fetch_error_code=$3,
             error_message = $2,
             finished_at = now(),
             updated_at = now()
         WHERE page_id = $1`,
        [pageId, errorMessage(error), errorType],
      );
      await updateNetworkEvent({
        phase: "discover_fetch",
        ok: false,
        targetUrl: queryText,
        errorType,
        error,
        payload: { query_id: queryId, page_no: pageNo },
      });
      throw error;
    }

    const rawCandidates = extractVideoOwnerCandidates(
      fetched.initialData,
      queryText,
      queryId,
      requestLanguage,
    ).map((candidate) => ({
      ...candidate,
      discovery_strategy: "video_popularity_this_year",
    }));
    const deduped = mergeDiscoveryCandidates(rawCandidates);
    const existingRows = deduped.length > 0
      ? await query("SELECT channel_id FROM crawler.channels WHERE channel_id = ANY($1::text[])", [deduped.map((candidate) => candidate.channel_id)])
      : { rows: [] };
    const existingSet = new Set(existingRows.rows.map((row) => row.channel_id));
    const candidateSpecs = [];
    let rejectedBelow = 0;
    let pendingUnknown = 0;
    let existingCount = 0;
    for (const candidate of deduped) {
      const qualification = evaluateDiscoveryChannelQualification({
        subscriberCount: candidate.subscriber_count,
        minSubscriberCount,
      });
      let candidateStatus = "discovered";
      let rejectReason = null;
      if (existingSet.has(candidate.channel_id)) {
        candidateStatus = "existing";
        existingCount += 1;
      } else if (!qualification.qualified) {
        candidateStatus = "rejected";
        rejectReason = qualification.reason;
        rejectedBelow += 1;
      } else if (qualification.subscriberCountMissing) {
        pendingUnknown += 1;
      }
      candidateSpecs.push({
        channel_id: candidate.channel_id,
        channel_url: candidate.channel_url ?? `https://www.youtube.com/channel/${candidate.channel_id}`,
        handle: candidate.handle ?? null,
        title: candidate.title ?? null,
        description: candidate.description ?? null,
        avatar_url: candidate.avatar_url ?? null,
        subscriber_count: qualification.subscriberCount,
        subscriber_count_text: candidate.subscriber_count_text ?? null,
        is_verified: candidate.is_verified ?? null,
        priority,
        candidate_status: candidateStatus,
        reject_reason: rejectReason,
        source_json: {
          source: "youtube_search_discovery",
          query_id: queryId,
          query_text: queryText,
          page_id: pageId,
          rank_position: candidate.rank_position,
          discovery_strategy: candidate.discovery_strategy ?? "video_popularity_this_year",
          discovery_aggregate: candidate.aggregate ?? null,
          search_qualification: qualification,
        },
      });
    }

    let candidateRows = { rows: [] };
    if (candidateSpecs.length > 0) {
      candidateRows = await query(
        `WITH input AS (
           SELECT *
           FROM jsonb_to_recordset($1::jsonb) AS item(
             channel_id text, channel_url text, handle text, title text,
             description text, avatar_url text, subscriber_count bigint,
             subscriber_count_text text, is_verified boolean, priority integer,
             candidate_status text, reject_reason text, source_json jsonb
           )
         )
         INSERT INTO crawler.channel_candidates (
           dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,handle,title,
           description,avatar_url,search_subscriber_count,search_subscriber_count_text,
           is_verified,priority,status,reject_reason,source_json,
           validation_finished_at,updated_at
         )
         SELECT $2,$3,channel_id,channel_url,handle,title,description,avatar_url,
                subscriber_count,subscriber_count_text,is_verified,priority,
                candidate_status,reject_reason,source_json,
                CASE WHEN candidate_status IN ('rejected','existing') THEN now() ELSE NULL END,
                now()
         FROM input
         ON CONFLICT (dispatch_batch_id,channel_id) DO UPDATE
         SET channel_url=EXCLUDED.channel_url,
             handle=COALESCE(EXCLUDED.handle,crawler.channel_candidates.handle),
             title=COALESCE(EXCLUDED.title,crawler.channel_candidates.title),
             description=COALESCE(EXCLUDED.description,crawler.channel_candidates.description),
             avatar_url=COALESCE(EXCLUDED.avatar_url,crawler.channel_candidates.avatar_url),
             search_subscriber_count=COALESCE(EXCLUDED.search_subscriber_count,crawler.channel_candidates.search_subscriber_count),
             search_subscriber_count_text=COALESCE(EXCLUDED.search_subscriber_count_text,crawler.channel_candidates.search_subscriber_count_text),
             is_verified=COALESCE(EXCLUDED.is_verified,crawler.channel_candidates.is_verified),
             priority=GREATEST(EXCLUDED.priority,crawler.channel_candidates.priority),
             status=CASE
               WHEN crawler.channel_candidates.status IN ('accepted','rejected','existing','validating','queued')
                 THEN crawler.channel_candidates.status
               ELSE EXCLUDED.status
             END,
             reject_reason=COALESCE(crawler.channel_candidates.reject_reason,EXCLUDED.reject_reason),
             source_json=crawler.channel_candidates.source_json || EXCLUDED.source_json,
             updated_at=now()
         RETURNING candidate_id,channel_id,channel_url,status,priority`,
        [JSON.stringify(candidateSpecs), dispatchBatchId, pipelineCycleId || dispatchBatchId],
      );
    }

    const candidateByChannel = new Map(candidateRows.rows.map((row) => [row.channel_id, row]));
    if (candidateRows.rows.length > 0) {
      await query(
        `INSERT INTO crawler.channel_candidate_sources (
           candidate_id,query_id,page_id,query_text,rank_position,discovery_strategy,source_json
         )
         SELECT candidate.candidate_id,$2,$3,$4,input.rank_position,input.discovery_strategy,input.source_json
         FROM jsonb_to_recordset($1::jsonb) AS input(
           channel_id text,rank_position integer,discovery_strategy text,source_json jsonb
         )
         JOIN crawler.channel_candidates candidate
           ON candidate.dispatch_batch_id=$5 AND candidate.channel_id=input.channel_id
         ON CONFLICT (candidate_id,page_id,discovery_strategy) DO UPDATE
         SET rank_position=CASE
               WHEN crawler.channel_candidate_sources.rank_position IS NULL THEN EXCLUDED.rank_position
               WHEN EXCLUDED.rank_position IS NULL THEN crawler.channel_candidate_sources.rank_position
               ELSE LEAST(crawler.channel_candidate_sources.rank_position,EXCLUDED.rank_position)
             END,
             source_json=crawler.channel_candidate_sources.source_json || EXCLUDED.source_json`,
        [
          JSON.stringify(deduped.map((candidate) => ({
            channel_id: candidate.channel_id,
            rank_position: candidate.rank_position ?? null,
            discovery_strategy: candidate.discovery_strategy ?? "video_popularity_this_year",
            source_json: { aggregate: candidate.aggregate ?? null },
          }))),
          queryId,
          pageId,
          queryText,
          dispatchBatchId,
        ],
      );
    }

    const snapshotCandidates = candidateSpecs
      .map((candidate) => ({ spec: candidate, row: candidateByChannel.get(candidate.channel_id) }))
      .filter(({ row }) => row && row.status === "discovered");
    const snapshotEligible = candidateSpecs
      .map((candidate) => ({ spec: candidate, row: candidateByChannel.get(candidate.channel_id) }))
      .filter(({ row }) => row && ["discovered", "queued", "validating", "accepted"].includes(row.status));
    if (snapshotCandidates.length > 0) {
      const allocations = await allocateDiscoveredChannelSnapshotDispatches(
        query,
        snapshotCandidates.map(({ row }) => Number(row.candidate_id)),
      );
      const generationByCandidate = new Map(
        allocations.map((allocation) => [allocation.candidate_id, allocation.snapshot_dispatch_generation]),
      );
      const dispatches = snapshotCandidates.filter(({ row }) => (
        generationByCandidate.has(Number(row.candidate_id))
      ));
      await queues[queuesByRole.channelCrawl].addBulk(dispatches.map(({ spec, row }) => (
        buildDiscoveredChannelSnapshotJob({
          candidate: {
            ...row,
            snapshot_dispatch_generation: generationByCandidate.get(Number(row.candidate_id)),
          },
          channel: spec,
          dispatchBatchId,
          pipelineCycleId,
          queryId,
          queryText,
          minSubscriberCount,
          jobId: safeJobId(
            "channel-snapshot",
            dispatchBatchId,
            spec.channel_id,
            `g${generationByCandidate.get(Number(row.candidate_id))}`,
          ),
        })
      )));
    }

    await refreshDispatchCandidateCounts(dispatchBatchId);
    const candidateCount = deduped.length;
    const snapshotQueued = snapshotEligible.length;
    const nextToken = findContinuationToken(fetched.initialData);
    const qualityRows = queryId
      ? await query("SELECT quality_score FROM crawler.query_terms WHERE query_id=$1 LIMIT 1", [queryId])
      : { rows: [] };
    const discoverQualityScore = qualityRows.rows[0]?.quality_score ?? null;
    const scheduler = await getQueryScheduler();
    const awaitingSnapshotValidation = candidateCount > 0;
    await query(
       `UPDATE crawler.query_pages
       SET status = $2,
           managed_fetch_status='done',
           managed_fetch_finished_at=now(),
           managed_fetch_error_code=NULL,
           dispatch_status='terminal',
           dispatch_reason='managed_fetch_complete',
           qualification_status=CASE WHEN $9::boolean THEN 'pending' ELSE 'done' END,
           qualification_started_at=CASE WHEN $9::boolean THEN now() ELSE qualification_started_at END,
           qualification_finished_at=CASE WHEN $9::boolean THEN NULL ELSE now() END,
           candidate_count = $3,
           accepted_count = $4,
           unqualified_ratio = $5,
           should_continue = $6,
           stop_reason = $7,
           result_json = result_json || $8::jsonb,
           error_message=NULL,
           finished_at = CASE WHEN $2='done' THEN now() ELSE NULL END,
           updated_at = now()
       WHERE page_id = $1`,
      [
        pageId,
        awaitingSnapshotValidation ? "running" : "done",
        candidateCount,
        existingCount,
        awaitingSnapshotValidation ? null : 1,
        awaitingSnapshotValidation ? null : false,
        awaitingSnapshotValidation ? null : "no_candidates",
        JSON.stringify({
          accepted_channel_ids: [],
          pending_qualification_channel_ids: snapshotEligible
            .filter(({ row }) => row.status !== "accepted")
            .map(({ spec }) => spec.channel_id),
          rejected_below: rejectedBelow,
          rejected_unknown: 0,
          pending_unknown: pendingUnknown,
          rejected_existing: existingCount,
          qualification_phase: awaitingSnapshotValidation
            ? "awaiting_snapshot_validation"
            : "complete",
          min_subscriber_count: minSubscriberCount,
          discover_stop_min_qualified_ratio: discoverStopMinQualifiedRatio,
          discovery_strategy: "video_popularity_this_year",
          search_type: "video",
          sort: "popularity",
          upload_date: "this_year",
          discover_quality_score: discoverQualityScore,
          language: requestLanguage,
          country: requestCountry,
          pipeline_cycle_id: pipelineCycleId,
          dispatch_batch_id: dispatchBatchId,
          next_continuation_token: nextToken,
          yt_config: fetched.ytConfig,
          next_page_enqueued: false,
          query_scheduler_status: scheduler.status,
        }),
        awaitingSnapshotValidation,
      ],
    );
    await signalReadyDiscoveryPageQualifications({ pageId });
    if (!awaitingSnapshotValidation && queryId) {
      await query(
        `UPDATE crawler.query_terms
         SET next_crawl_at = now() + make_interval(secs => crawl_interval_sec),
             updated_at = now()
         WHERE query_id = $1`,
        [queryId],
      );
    }
    return {
      ok: true,
      page_id: pageId,
      query_id: queryId,
      candidate_count: candidateCount,
      accepted_count: existingCount,
      should_continue: null,
      rejected_below: rejectedBelow,
      rejected_unknown: 0,
      pending_unknown: pendingUnknown,
      rejected_existing: existingCount,
      dispatch_batch_id: dispatchBatchId,
      qualification_pending: awaitingSnapshotValidation,
      snapshot_queued: snapshotQueued,
      next_page_enqueued: false,
      query_scheduler_status: scheduler.status,
    };
  }

  await query(
    `UPDATE crawler.query_pages
     SET status = 'done',
         managed_fetch_status='done',
         managed_fetch_finished_at=now(),
         managed_fetch_error_code=NULL,
         dispatch_status='terminal',
         dispatch_reason='managed_fetch_complete',
         qualification_status='done',
         qualification_finished_at=now(),
         candidate_count = $2,
         accepted_count = $3,
         unqualified_ratio = $4,
         should_continue = false,
         stop_reason = 'demo_or_placeholder',
         error_message=NULL,finished_at = now(),
         updated_at = now()
     WHERE page_id = $1`,
    [pageId, demo ? 1 : 0, demo ? 1 : 0, 0],
  );

  return { ok: true, page_id: pageId, demo };
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

  if (queueName === queuesByRole.channelCrawl && job?.data?.candidate_id) {
    let candidateRecorded = false;
    try {
      await persistChannelCandidateParserContractFailure(
        query,
        activeChannelCandidateAttemptFence(job),
        { message, details },
      );
      candidateRecorded = true;
    } catch (candidateError) {
      if (!(candidateError instanceof StaleChannelCandidateAttemptError)) throw candidateError;
    }
    if (candidateRecorded && job.data?.dispatch_batch_id) {
      await refreshDispatchCandidateCounts(String(job.data.dispatch_batch_id));
    }
  }

  if (queueName === queuesByRole.channelCrawl && job?.data?.run_id) {
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

  if (queueName === queuesByRole.discoverPage && job?.data?.page_id) {
    await query(
      `UPDATE crawler.query_pages
       SET status=CASE WHEN managed_fetch_status='done' THEN status ELSE 'failed' END,
           managed_fetch_status=CASE WHEN managed_fetch_status='done' THEN managed_fetch_status ELSE 'failed' END,
           managed_fetch_finished_at=CASE WHEN managed_fetch_status='done' THEN managed_fetch_finished_at ELSE now() END,
           managed_fetch_error_code=CASE WHEN managed_fetch_status='done' THEN managed_fetch_error_code ELSE 'parser_contract_error' END,
           dispatch_status='terminal',dispatch_reason='parser_contract_error',error_message=$2,
           result_json=COALESCE(result_json,'{}'::jsonb)
             || jsonb_build_object('parser_contract_error',$3::jsonb),
           finished_at=now(),updated_at=now()
       WHERE page_id=$1`,
      [String(job.data.page_id), message, JSON.stringify(details)],
    );
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
      return processChannelCrawlV2(job, { resumeMode });
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
    const failureDecision = retryableSystemFailureDecision(error)
      ?? decideYoutubeFailure({ error });
    error.youtube_failure_decision = failureDecision;
    applyFailureRetryDecision(job, failureDecision);
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
    const pageId = String(prepared?.page?.page_id ?? job.data?.page_id ?? "").trim();
    const updated = await query(
      `UPDATE crawler.query_pages
       SET managed_fetch_status='failed',managed_fetch_error_code=$2,
           status=CASE WHEN status='done' THEN status ELSE 'failed' END,
           error_message=$3,updated_at=now()
       WHERE page_id=$1 AND managed_fetch_status<>'done'
       RETURNING page_id`,
      [pageId, failure.observation, message],
    );
    if (updated.rowCount === 1) return true;
    const existing = await query(
      "SELECT managed_fetch_status FROM crawler.query_pages WHERE page_id=$1 LIMIT 1",
      [pageId],
    );
    return existing.rows[0]?.managed_fetch_status === "done";
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
  if (job?.data?.retry_intent_id) {
    const marked = await markMigrationRetryIntentRunning(query, job);
    if (!marked) {
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
  const execute = () => {
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
        terminateExhaustedBusinessRun(withTransaction, currentJob, error)
      ),
      deferForSlotPause: deferJobForSlotPause,
      defaultDelayMs: proxySlotPollMs,
      onDeferred: (event) => console.log(JSON.stringify({ event: "rota_job_deferred", ...event })),
    });
  };
  if (job?.queueName === queuesByRole.channelCrawl && job?.data?.candidate_id) {
    return runChannelCandidateWorkerJobWithDurableSettlement({ query, job, execute });
  }
  return execute();
}

const enabledQueues = String(process.env.WORKER_QUEUES || queueNames.join(","))
  .split(",")
  .map((item) => item.trim())
  .filter(Boolean);
const workerQueueConfiguration = validateWorkerQueueConfiguration({
  role: proxySlotRole,
  enabledQueues,
  fixedProxy: fixedProxy !== null,
});

let workers = [];
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
    if (rotaSlot) await shutdownStep("rota_slot", () => rotaSlot.close());
    await shutdownStep("bullmq_workers", () => Promise.all(workers.map((worker) => worker.close())));
    await shutdownStep("proxy_control", closeProxyControlClient);
    await shutdownStep("http", closePersistentHttpClient);
    await shutdownStep("storage", async () => closeStorage());
    await shutdownStep("queues", () => closeQueues(queues));
    await shutdownStep("database", closeDb);
    console.log(JSON.stringify({ event: "shutdown_complete", signal }));
    process.exit(0);
  })().catch((error) => {
    console.error(JSON.stringify({ event: "shutdown_failed", signal, error: error?.stack || String(error) }));
    process.exit(1);
  });
  return shutdownPromise;
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

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
    const warmResult = await warmPersistentYtDlp();
    console.log(JSON.stringify({ event: "ytdlp_pool_warm", ...warmResult }));
    const youtubeJsWarmResult = await warmYoutubeJs();
    console.log(JSON.stringify({ event: "youtubejs_pool_warm", ...youtubeJsWarmResult }));
  }
  if (shuttingDown) return;

  workers = enabledQueues.map((queueName) => {
    const worker = new Worker(queueName, processJob, {
      connection: redisOptions,
      concurrency: concurrencyFor(queueName),
      ...bullmqWorkerTimingOptions(),
      ...(bullmqPrefix ? { prefix: bullmqPrefix } : {}),
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
      if (queueName === queuesByRole.channelCrawl && job?.data?.candidate_id) {
        try {
          await completeChannelCandidateWorkerJob(query, job);
        } catch (eventError) {
          console.error(JSON.stringify({
            event: "candidate_attempt_fence_release_failed",
            job_id: job.id,
            error: eventError?.message || String(eventError),
          }));
        }
      }
      if (job?.data?.retry_intent_id) {
        try {
          await finishMigrationRetryIntent(query, job, { outcome: "finished" });
        } catch (eventError) {
          console.error(JSON.stringify({
            event: "migration_retry_intent_finish_failed",
            job_id: job.id,
            error: eventError?.message || String(eventError),
          }));
        }
      }
    });

    worker.on("failed", async (job, error) => {
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
              JSON.stringify(parserDetails ? { parser_contract_error: parserDetails } : {}),
            ],
          );
        }
        if (queueName === queuesByRole.discoverPage && job?.data?.page_id) {
          const terminal = parserFailure || permanentFailure || attemptsMade >= maxAttempts;
          if (terminal) {
            await query(
              `UPDATE crawler.query_pages
               SET status=CASE WHEN managed_fetch_status='done' THEN status ELSE 'failed' END,
                   managed_fetch_status=CASE WHEN managed_fetch_status='done' THEN managed_fetch_status ELSE 'failed' END,
                   managed_fetch_finished_at=CASE WHEN managed_fetch_status='done' THEN managed_fetch_finished_at ELSE now() END,
                   managed_fetch_error_code=CASE WHEN managed_fetch_status='done' THEN managed_fetch_error_code ELSE $4 END,
                   dispatch_status='terminal',dispatch_reason=$4,
                   error_message=$2,result_json=result_json || $3::jsonb,
                   finished_at=now(),updated_at=now()
               WHERE page_id=$1`,
              [
                String(job.data.page_id),
                message,
                JSON.stringify(parserFailure ? { parser_contract_error: parserDetails } : {}),
                parserFailure ? "parser_contract_error" : failureDecision.kind,
              ],
            );
          }
        }
      } catch (eventError) {
        console.error(JSON.stringify({ event: "task_event_failed", error: eventError?.message || String(eventError) }));
      }
    });

    console.log(`worker started queue=${queueName} concurrency=${worker.opts.concurrency}`);
    return worker;
  });
}

try {
  await startWorkerRuntime();
} catch (error) {
  if (!shuttingDown) throw error;
}
