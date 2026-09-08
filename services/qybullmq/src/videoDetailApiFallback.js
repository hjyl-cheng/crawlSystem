import { AsyncLocalStorage } from "node:async_hooks";
import { isStaleExecutionFailure, classifyRetryableSystemFailure } from "./managedWorkerJob.js";
import { selectYoutubeFailure } from "./youtubeFailurePolicy.js";
import { requestVideoApiDetail, waitForVideoApiDetail, videoApiResultError } from "./videoApiBatchRequests.js";

const execution = new AsyncLocalStorage();
const NETWORK_KINDS = new Set(["proxy_transport", "youtube_rate_limited", "youtube_challenge", "upstream_transient", "token_or_client"]);

export function withVideoFallbackExecution(context, action) {
  return execution.run(context, action);
}

export function mergeVideoApiEvidence(videoId, partial, api) {
  const detail = { ...partial };
  for (const [key, value] of Object.entries(api)) {
    if (value != null) detail[key] = value;
  }
  // videos.list supplies metadata, never an authoritative Shorts classification.
  if (partial?.content_type_signals) detail.content_type_signals = partial.content_type_signals;
  if (partial?.canonical_url) detail.canonical_url = partial.canonical_url;
  detail.id = videoId;
  if (["public", "unlisted", "private"].includes(api.privacy_status)) {
    detail.access_status = api.privacy_status;
    detail.access_status_source = "youtube_data_api_status";
    delete detail.playability_kind;
    delete detail.playability_retry_mode;
    delete detail.playability_reason_code;
  }
  if (api.view_count_text != null) {
    detail.view_count = Number(api.view_count_text);
    detail.view_count_status = "exact";
  }
  if (api.like_count != null) detail.like_count_status = "exact";
  if (api.comments_first_page || api.comments_disabled === true) detail.youtubejs_comments_error = null;
  detail.video_detail_fallback = { source: "youtube_data_api_batch", youtubejs_exhausted: true };
  return detail;
}

export function createVideoDetailApiFallback({ query, withTransaction, loadSettings,
  request = requestVideoApiDetail, wait = waitForVideoApiDetail } = {}) {
  return async function fetchWithFallback({ videoId, runId, requestId, consumer,
    attempt = 1, signal, optionalComments = false, detailMode = "full", fetch, validate }) {
    signal?.throwIfAborted();
    const existing = (await query(`SELECT * FROM crawler.youtube_api_detail_requests
      WHERE request_id=$1`, [requestId])).rows[0];
    let partial = existing?.partial_detail ?? {};
    if (existing && (existing.run_id !== runId || existing.source_content_id !== videoId
        || existing.consumer !== consumer)) throw new Error("Video API consumer identity conflicts");
    if (!existing) {
      for (let detailAttempt = Math.max(1, Number(attempt) || 1); ; detailAttempt += 1) {
        try {
          const observed = await fetch();
          signal?.throwIfAborted();
          return validate(observed);
        } catch (error) {
          signal?.throwIfAborted();
          if (isStaleExecutionFailure(error) || classifyRetryableSystemFailure(error)) throw error;
          const failure = selectYoutubeFailure({ error }).decision;
          const parserFailure = error?.name === "YoutubeJsRequiredSurfaceError"
            || error?.name === "ParserContractError";
          partial = { ...partial, ...Object.fromEntries(Object.entries(error.partial_detail ?? {})
            .filter(([, value]) => value != null)) };
          if (parserFailure && !NETWORK_KINDS.has(failure.kind) && detailAttempt < 3) continue;
          const context = execution.getStore();
          let routeExhausted = detailAttempt >= 3;
          if (NETWORK_KINDS.has(failure.kind) && context?.getBudget) {
            const budget = await context.getBudget();
            const value = budget?.budget ?? budget;
            routeExhausted = Number(value?.business_tasks_limit) > 0
              && Number(value.business_tasks_used) >= Number(value.business_tasks_limit);
          }
          if (!(parserFailure && !NETWORK_KINDS.has(failure.kind))
              && !(NETWORK_KINDS.has(failure.kind) && routeExhausted)) throw error;
          const settings = await loadSettings();
          if (settings.fallbackMode === "disabled") throw error;
          if (!settings.apiKeys?.length || settings.dailyRequestLimit <= 0) {
            throw videoApiResultError("Video API fallback unavailable: key or daily allowance missing");
          }
          await request(withTransaction, { requestId, runId, videoId, consumer, partialDetail: partial,
            requireComments: partial.comments_disabled !== true
              && (Boolean(partial.youtubejs_comments_error)
                || (detailMode === "full" && !partial.comments_first_page)) });
          console.log(JSON.stringify({ event: "video_api_fallback_requested", run_id: runId,
            video_id: videoId, consumer, detail_attempt: detailAttempt, failure_kind: failure.kind }));
          break;
        }
      }
    }
    const api = await wait(query, requestId, { signal });
    signal?.throwIfAborted();
    try {
      return validate(mergeVideoApiEvidence(videoId, partial, api));
    } catch (cause) {
      const error = videoApiResultError(`Video API evidence is incomplete for ${videoId}: ${cause.message}`);
      error.partial_detail = cause.partial_detail ?? mergeVideoApiEvidence(videoId, partial, api);
      throw error;
    }
  };
}
