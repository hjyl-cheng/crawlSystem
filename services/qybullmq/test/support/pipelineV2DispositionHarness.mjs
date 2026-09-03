import { writeFile } from "node:fs/promises";

const outputPath = process.argv[2];
const scenario = process.argv[3] ?? "deferred_type";
const transportCancellation = scenario === "detail_transport_cancelled";
let originalInnertubeCreate = null;
if (transportCancellation) {
  process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";
  const { Innertube } = await import("youtubei.js");
  originalInnertubeCreate = Innertube.create;
  Innertube.create = async ({ fetch }) => ({
    async getInfo(videoId) {
      await fetch("https://www.youtube.com/youtubei/v1/player", {
        method: "POST",
        body: JSON.stringify({ videoId }),
      });
      throw new Error("controlled transport unexpectedly resolved");
    },
  });
}
const existingContent = scenario === "existing_private";
const dataApiReplay = scenario === "data_api_replay";
const dataApiCommentMerge = scenario === "data_api_comment_count_preserves_page";
const dataApiLive = ["data_api_live", "data_api_stale_before_request"].includes(scenario);
const dataApiScenario = dataApiReplay || dataApiCommentMerge || dataApiLive;
const dispositionWriteRetry = scenario === "disposition_write_retry";
const flatLiveInProgress = scenario === "live_in_progress_flat";
const candidateRetryPublicationConflict = scenario === "candidate_retry_publication_conflict";
const deferredDisposition = {
  version: "video-disposition-v1",
  kind: "deferred",
  reason_code: "access_unknown",
  retry_class: "access_recheck",
  retryable: true,
  observed_at: "2026-07-20T00:00:00.000Z",
  next_attempt_at: "2026-07-20T06:00:00.000Z",
};
globalThis.__pipelineV2DispositionState = {
  scenario,
  candidate: {
    candidate_id: 501,
    run_id: "run:shared-video-disposition",
    channel_id: "UCsharedDisposition",
    source_content_id: "public-without-type",
    position: 1,
    title: "Uploads title",
    source_url: "https://www.youtube.com/watch?v=public-without-type",
    thumbnail_url: null,
    content_type: flatLiveInProgress || dataApiLive
      ? "live"
      : existingContent || dataApiReplay || dataApiCommentMerge ? "video" : null,
    type_status: existingContent || dataApiScenario || flatLiveInProgress ? "resolved" : "unresolved",
    type_source: flatLiveInProgress || dataApiLive
      ? "youtube_uploads_live_flag"
      : existingContent || dataApiReplay || dataApiCommentMerge
        ? "youtube_watch_canonical"
        : null,
    detail_status: scenario === "undisposed_terminal"
      ? "done"
      : candidateRetryPublicationConflict ? "failed"
      : dataApiScenario ? "api_pending" : "queued",
    api_status: dataApiScenario ? "queued" : "not_needed",
    missing_fields: dataApiLive || dataApiCommentMerge
      ? ["comment_count"]
      : dataApiReplay ? ["access_status"] : [],
    attempts: candidateRetryPublicationConflict ? 1 : 0,
    content_key: dataApiLive
      ? "UCsharedDisposition:live:public-without-type"
      : existingContent || dataApiCommentMerge
        ? "UCsharedDisposition:video:public-without-type"
        : null,
    disposition: dataApiLive || dataApiCommentMerge
      ? "stored"
      : dataApiReplay ? "deferred" : null,
    next_attempt_at: dataApiReplay ? deferredDisposition.next_attempt_at : null,
    result_json: {
      flat: {
        id: "public-without-type",
        title: "Uploads title",
        position: 1,
        ...(scenario === "upcoming_live"
          ? {
              content_type: "live",
              type_source: "youtube_uploads_live_flag",
              is_upcoming: true,
              live_scheduled_at: "2026-07-21T12:00:00.000Z",
            }
          : {}),
        ...(flatLiveInProgress
          ? {
              content_type: "live",
              type_source: "youtube_uploads_live_flag",
              live_status: "is_live",
            }
          : {}),
        ...(candidateRetryPublicationConflict
          ? {
              published_text: "Old flat text",
              published_at: "2026-01-01T00:00:00.000Z",
              published_at_status: "exact",
              published_at_precision: "second",
              published_at_source: "yt_dlp_flat_timestamp",
            }
          : {}),
      },
      ...(scenario === "age_excluded"
        ? {
            detail: {
              id: "public-without-type",
              published_at: "2025-01-01T00:00:00.000Z",
              published_at_status: "exact",
              published_at_precision: "second",
              published_at_source: "yt_dlp_timestamp",
              access_status: "public",
            },
          }
        : {}),
      ...(candidateRetryPublicationConflict
        ? {
            detail: {
              id: "public-without-type",
              title: "Existing retry Detail",
              description: "Existing complete Detail",
              description_status: "exact",
              description_source: "youtubejs_player",
              published_text: "Existing recent Detail text",
              published_at: "2026-07-19T00:00:00.000Z",
              published_at_status: "exact",
              published_at_precision: "second",
              published_at_source: "youtubejs_player_microformat",
              duration_seconds: 90,
              view_count: 100,
              view_count_text: "100",
              like_count: 3,
              comment_count: 0,
              comments_disabled: false,
              access_status: "public",
              content_type_signals: {
                source: "youtubei_player",
                canonical_url: "https://www.youtube.com/watch?v=public-without-type",
                is_shorts_eligible: false,
                is_live_content: false,
              },
            },
          }
        : {}),
      ...(dataApiScenario
        ? {
            detail: {
              id: "public-without-type",
              title: "Recovered through stored API evidence",
              description: "Complete detail",
              description_status: "exact",
              published_at: "2026-07-19T00:00:00.000Z",
              published_at_precision: "second",
              duration_seconds: dataApiLive ? null : 90,
              view_count: 100,
              view_count_text: "100",
              like_count: 3,
              comment_count: dataApiLive || dataApiCommentMerge ? null : 0,
              comments_disabled: dataApiLive ? null : false,
              ...(dataApiCommentMerge
                ? {
                    comments_first_page: {
                      version: 1,
                      collected_at: "2026-08-31T09:27:20.437Z",
                      sort: "TOP_COMMENTS",
                      total_count: null,
                      returned_count: 1,
                      comments: [{
                        comment_id: "existing-comment-page",
                        text: "Already collected",
                      }],
                    },
                    comments_first_page_status: "collected",
                    comments_first_page_source: "yt_dlp_top_comments",
                  }
                : {}),
              is_live: dataApiLive,
              live_status: dataApiLive ? "is_live" : "not_live",
              access_status: dataApiCommentMerge ? "public" : "unknown",
              content_type_signals: {
                source: "yt_dlp_player",
                canonical_url: "https://www.youtube.com/watch?v=public-without-type",
                is_shorts_eligible: false,
                is_live_content: dataApiLive,
                is_live: dataApiLive,
                is_live_now: dataApiLive,
              },
            },
            classification: {
              content_type: dataApiLive ? "live" : "video",
              source: dataApiLive ? "youtube_watch_live_content" : "youtube_watch_canonical",
              canonical_url: "https://www.youtube.com/watch?v=public-without-type",
              authoritative: true,
            },
            access: dataApiCommentMerge
              ? { access_status: "public", access_status_source: "yt_dlp_availability" }
              : { access_status: "unknown", access_status_source: null },
            disposition: dataApiLive || dataApiCommentMerge
              ? {
                  ...deferredDisposition,
                  kind: "stored",
                  reason_code: "content_stored",
                  retry_class: null,
                  retryable: false,
                  next_attempt_at: null,
                }
              : deferredDisposition,
          }
        : {}),
    },
    error_message: dataApiReplay
      ? "content access unknown before API fallback"
      : null,
    crawl_started_at: "2026-07-20T00:00:00.000Z",
    known_content_key: dataApiLive
      ? "UCsharedDisposition:live:public-without-type"
      : existingContent || dataApiCommentMerge
        ? "UCsharedDisposition:video:public-without-type"
        : null,
    known_content_type: dataApiLive
      ? "live"
      : existingContent || dataApiCommentMerge ? "video" : null,
    known_content_type_source: dataApiLive
      ? "youtube_watch_live_content"
      : existingContent || dataApiCommentMerge ? "youtube_watch_canonical" : null,
  },
  tasks: dataApiScenario
    ? [{
        task_id: 91,
        source_content_id: "public-without-type",
        candidate_ids: [501],
        missing_fields: dataApiLive || dataApiCommentMerge
          ? ["comment_count"]
          : ["access_status"],
        result_json: {
          privacy_status: "public",
          ...(dataApiLive
            ? {
                is_live: true,
                live_status: "is_live",
                content_type_signals: {
                  source: "youtube_data_api_videos_list",
                  is_live_content: true,
                  is_live: true,
                  is_live_now: true,
                },
              }
            : {}),
          source: "youtube_data_api_videos_list",
          api_verification: { videos_list: { returned: true } },
          stored_data_api_evidence_recovery: {
            operation_id: "stored-data-api-public-access-replay-v1",
            run_id: "run:shared-video-disposition",
            candidate_ids: [501],
          },
        },
      }]
    : [],
  queries: [],
  rawObjects: [],
  youtubeRequestAttempts: 0,
  youtubeJsDetailAttempts: 0,
  ytDlpDetailAttempts: 0,
  dispositionWriteAttempts: 0,
  dataApiFenceLocks: 0,
};

const cancellationController = ["detail_cancelled", "detail_transport_cancelled"].includes(scenario)
  ? new AbortController()
  : null;
const cancellationReason = cancellationController
  ? new Error("injected channel detail cancellation")
  : null;
if (cancellationController) {
  Object.assign(globalThis.__pipelineV2DispositionState, {
    cancellationSignal: cancellationController.signal,
    cancelDetail: () => cancellationController.abort(cancellationReason),
    forwardedDetailSignal: false,
    transportAborted: false,
  });
}

const { processContentDetailBatchV2, processDataApiBatchV2 } = await import("../../src/pipelineV2.js");
const {
  ChannelExecutionMetrics,
  runWithChannelExecution,
} = await import("../../src/channelExecutionContext.js");
let value = null;
let error = null;
let firstError = null;
let retryError = null;
let requestsAfterFirst = null;
let cancellationReasonPreserved = null;
let cancellationElapsedMs = null;
let deadlineExceeded = false;
if (dispositionWriteRetry) {
  const job = {
    id: "content-detail:run:shared-video-disposition",
    name: "content-detail-batch",
    attemptsStarted: 1,
    data: {
      run_id: "run:shared-video-disposition",
      channel_id: "UCsharedDisposition",
      api_fallback_mode: "disabled",
      content_max_age_days: 0,
    },
  };
  try {
    await processContentDetailBatchV2(job);
  } catch (caught) {
    firstError = { message: caught?.message ?? String(caught), stack: caught?.stack ?? null };
  }
  requestsAfterFirst = globalThis.__pipelineV2DispositionState.youtubeRequestAttempts;
  try {
    value = await processContentDetailBatchV2(job);
  } catch (caught) {
    retryError = { message: caught?.message ?? String(caught), stack: caught?.stack ?? null };
  }
} else {
  try {
    const operation = () => (dataApiScenario
      ? processDataApiBatchV2({
        id: dataApiReplay
          ? "batch:stored-evidence"
          : dataApiCommentMerge ? "batch:comment-count" : "batch:live-api",
        name: "youtube-data-api-batch",
        attemptsStarted: 1,
        data: {
          batch_id: dataApiReplay
            ? "batch:stored-evidence"
            : dataApiCommentMerge ? "batch:comment-count" : "batch:live-api",
          task_ids: [91],
          video_ids: ["public-without-type"],
          ...(dataApiReplay
            ? {
                stored_evidence_replay: {
                  operation_id: "stored-data-api-public-access-replay-v1",
                  run_id: "run:shared-video-disposition",
                  expected_candidate_count: 1,
                  task_ids: [91],
                },
              }
            : {}),
        },
        })
      : processContentDetailBatchV2({
        id: "content-detail:run:shared-video-disposition",
        name: "content-detail-batch",
        attemptsStarted: 1,
        data: {
          run_id: "run:shared-video-disposition",
          channel_id: "UCsharedDisposition",
          api_fallback_mode: "disabled",
          content_max_age_days: [
            "age_excluded",
            "stored_public",
            "candidate_retry_publication_conflict",
          ].includes(scenario) ? 90 : 0,
        },
        }));
    if (transportCancellation) {
      let notifyTransportStarted;
      let rejectTransport;
      const transportStarted = new Promise((resolve) => { notifyTransportStarted = resolve; });
      const metrics = new ChannelExecutionMetrics();
      const proxy = { proxy_id: 17, proxy_address_hash: "transport-cancel-route" };
      const running = runWithChannelExecution({
        abort_signal: cancellationController.signal,
        proxy,
        get_proxy_snapshot: () => proxy,
        profile_group: {
          clients: { youtubejs_chrome: { profile_id: "transport-cancel-chrome" } },
        },
        fingerprint_gateway: {
          fetch(_profile, _input, init) {
            notifyTransportStarted();
            return new Promise((resolve, reject) => {
              rejectTransport = reject;
              const rejectFromAbort = () => {
                globalThis.__pipelineV2DispositionState.transportAborted = true;
                reject(init.signal.reason);
              };
              if (init.signal.aborted) rejectFromAbort();
              else init.signal.addEventListener("abort", rejectFromAbort, { once: true });
            });
          },
        },
        metrics,
      }, operation);
      const settled = running.then(
        (resolvedValue) => ({ value: resolvedValue }),
        (caught) => ({ error: caught }),
      );
      await transportStarted;
      const abortedAt = Date.now();
      cancellationController.abort(cancellationReason);
      let deadlineTimer;
      const outcome = await Promise.race([
        settled,
        new Promise((resolve) => {
          deadlineTimer = setTimeout(() => resolve({ deadlineExceeded: true }), 250);
        }),
      ]);
      clearTimeout(deadlineTimer);
      cancellationElapsedMs = Date.now() - abortedAt;
      deadlineExceeded = outcome.deadlineExceeded === true;
      if (deadlineExceeded) {
        rejectTransport?.(new Error("controlled transport deadline cleanup"));
        await settled;
      }
      if (outcome.error) throw outcome.error;
      value = outcome.value;
    } else {
      value = cancellationController
        ? await runWithChannelExecution(
          { abort_signal: cancellationController.signal },
          operation,
        )
        : await operation();
    }
  } catch (caught) {
    cancellationReasonPreserved = caught === cancellationReason;
    error = { message: caught?.message ?? String(caught), stack: caught?.stack ?? null };
  }
}

await writeFile(outputPath, JSON.stringify({
  value,
  error,
  first_error: firstError,
  retry_error: retryError,
  requests_after_first: requestsAfterFirst,
  requests_after_retry: globalThis.__pipelineV2DispositionState.youtubeRequestAttempts,
  youtubejs_detail_attempts: globalThis.__pipelineV2DispositionState.youtubeJsDetailAttempts,
  ytdlp_detail_attempts: globalThis.__pipelineV2DispositionState.ytDlpDetailAttempts,
  disposition_write_attempts: globalThis.__pipelineV2DispositionState.dispositionWriteAttempts,
  cancellation_reason_preserved: cancellationReasonPreserved,
  cancellation_elapsed_ms: cancellationElapsedMs,
  cancellation_deadline_exceeded: deadlineExceeded,
  transport_aborted: globalThis.__pipelineV2DispositionState.transportAborted ?? null,
  forwarded_detail_signal: globalThis.__pipelineV2DispositionState.forwardedDetailSignal ?? null,
  candidate: globalThis.__pipelineV2DispositionState.candidate,
  queries: globalThis.__pipelineV2DispositionState.queries.map(({ sql }) => sql),
}), "utf8");
if (originalInnertubeCreate) {
  const { Innertube } = await import("youtubei.js");
  Innertube.create = originalInnertubeCreate;
}
if (error || retryError) process.exitCode = 1;
