import {
  allocateDiscoveredChannelSnapshotDispatches,
  buildDiscoveredChannelSnapshotJob,
} from "./channelSnapshotDispatch.js";
import { evaluateDiscoveryChannelQualification } from "./channelQualification.js";
import { buildDemoChannelCrawlJob } from "./demoChannelDispatch.js";
import { reconcileDispatchBatchCandidateState } from "./dispatchBatchCandidateState.js";
import { queuesByRole, safeJobId } from "./queues.js";
import {
  classifyYoutubeError,
  extractVideoOwnerCandidates,
  findContinuationToken,
  parseDiscoverSearchPage,
  requestDiscoverSearchPage,
} from "./youtube.js";
import { resolveYoutubeLocale } from "./youtubeLocale.js";

function errorMessage(error) {
  return String(error?.message ?? error ?? "unknown error");
}

export function mergeDiscoveryCandidates(candidates) {
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

// One Discover Page Intent is claimed, fetched with a single YouTube request,
// then applied. Only `requestPage` touches the network, so the same claim and
// apply steps serve local execution and a remote node that returns the raw page.
export function createDiscoverPageProcessor({
  query,
  queues,
  putRawObject,
  getCrawlSettingsV2,
  getQueryScheduler,
  signalReadyDiscoveryPageQualifications,
  requestPage = requestDiscoverSearchPage,
  language,
  country,
  demoChannelId,
}) {
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

  async function claimDiscoverPage(job, preparedPage) {
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
        return { result: { ok: true, page_id: pageId, managed_fetch_complete: true } };
      }
      throw new Error(`Discover Page Intent is not executable: ${pageId}`);
    }
    return {
      pageId, managedIntent, queryText, queryId, pageNo, discoveryRunId, pipelineCycleId,
      dispatchBatchId, requestLanguage, requestCountry, continuation, ytConfig, priority, demo,
      minSubscriberCount, discoverStopMinQualifiedRatio,
    };
  }

  async function fetchDiscoverPage({
    pageId, queryText, queryId, pageNo, requestLanguage, requestCountry, continuation, ytConfig,
  }) {
    try {
      const page = await requestPage({
        kind: continuation ? "continuation" : "initial",
        queryText,
        language: requestLanguage,
        country: requestCountry,
        ytConfig,
        continuation,
      });
      const fetched = parseDiscoverSearchPage(page, { ytConfig });
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
      return fetched;
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
  }

  async function applyDiscoverPage({
    pageId, queryText, queryId, pipelineCycleId, dispatchBatchId, requestLanguage, requestCountry,
    priority, minSubscriberCount, discoverStopMinQualifiedRatio,
  }, fetched) {
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

    await reconcileDispatchBatchCandidateState(query, dispatchBatchId);
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

  async function completeDemoPage({ pageId, managedIntent, queryText, pipelineCycleId }) {
    const channelId = managedIntent.channel_id || `UCdemo${demoChannelId()}`;
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
      [pageId, 1, 1, 0],
    );
    return { ok: true, page_id: pageId, demo: true };
  }

  return async function processDiscoverPage(job, preparedPage) {
    const claim = await claimDiscoverPage(job, preparedPage);
    if (claim.result) return claim.result;
    if (claim.demo) return completeDemoPage(claim);
    return applyDiscoverPage(claim, await fetchDiscoverPage(claim));
  };
}

// Failure bookkeeping shared by the local worker and the remote center.

// Before Rota switches routes, the page leaves `running` so the next attempt
// claims it again. A page already done needs no checkpoint.
export async function persistDiscoverRetryCheckpoint(query, { job, prepared, failure, message }) {
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

export async function persistDiscoverParserContractFailure(query, job, { message, details }) {
  if (!job?.data?.page_id) return;
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

// A failed BullMQ job becomes terminal for its page only when it will not be
// retried: parser drift, a permanent failure or the last attempt.
export async function recordDiscoverPageTerminalFailure(query, job, {
  message, parserFailure, parserDetails, failureDecision, permanentFailure, attemptsMade, maxAttempts,
}) {
  if (!job?.data?.page_id) return;
  const terminal = parserFailure || permanentFailure || attemptsMade >= maxAttempts;
  if (!terminal) return;
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
