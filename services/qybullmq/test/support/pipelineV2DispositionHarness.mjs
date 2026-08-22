import { writeFile } from "node:fs/promises";

const outputPath = process.argv[2];
const scenario = process.argv[3] ?? "deferred_type";
const existingContent = scenario === "existing_private";
const dataApiReplay = scenario === "data_api_replay";
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
    content_type: existingContent || dataApiReplay ? "video" : null,
    type_status: existingContent || dataApiReplay ? "resolved" : "unresolved",
    type_source: existingContent || dataApiReplay ? "youtube_watch_canonical" : null,
    detail_status: scenario === "undisposed_terminal"
      ? "done"
      : dataApiReplay ? "api_pending" : "queued",
    api_status: dataApiReplay ? "queued" : "not_needed",
    missing_fields: dataApiReplay ? ["access_status"] : [],
    attempts: 0,
    content_key: existingContent ? "UCsharedDisposition:video:public-without-type" : null,
    disposition: dataApiReplay ? "deferred" : null,
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
      },
      ...(scenario === "age_excluded"
        ? {
            detail: {
              id: "public-without-type",
              published_at: "2025-01-01T00:00:00.000Z",
              published_at_precision: "second",
              access_status: "public",
            },
          }
        : {}),
      ...(dataApiReplay
        ? {
            detail: {
              id: "public-without-type",
              title: "Recovered through stored API evidence",
              description: "Complete detail",
              description_status: "exact",
              published_at: "2026-07-19T00:00:00.000Z",
              published_at_precision: "second",
              duration_seconds: 90,
              view_count: 100,
              view_count_text: "100",
              like_count: 3,
              comment_count: 0,
              comments_disabled: false,
              access_status: "unknown",
              content_type_signals: {
                source: "yt_dlp_player",
                canonical_url: "https://www.youtube.com/watch?v=public-without-type",
                is_shorts_eligible: false,
                is_live_content: false,
              },
            },
            classification: {
              content_type: "video",
              source: "youtube_watch_canonical",
              canonical_url: "https://www.youtube.com/watch?v=public-without-type",
              authoritative: true,
            },
            access: { access_status: "unknown", access_status_source: null },
            disposition: deferredDisposition,
          }
        : {}),
    },
    error_message: dataApiReplay ? "content access unknown before API fallback" : null,
    crawl_started_at: "2026-07-20T00:00:00.000Z",
    known_content_key: existingContent ? "UCsharedDisposition:video:public-without-type" : null,
    known_content_type: existingContent ? "video" : null,
    known_content_type_source: existingContent ? "youtube_watch_canonical" : null,
  },
  tasks: dataApiReplay
    ? [{
        task_id: 91,
        source_content_id: "public-without-type",
        candidate_ids: [501],
        missing_fields: ["access_status"],
        result_json: {
          privacy_status: "public",
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
};

const { processContentDetailBatchV2, processDataApiBatchV2 } = await import("../../src/pipelineV2.js");
let value = null;
let error = null;
try {
  value = dataApiReplay
    ? await processDataApiBatchV2({
        id: "batch:stored-evidence",
        data: {
          batch_id: "batch:stored-evidence",
          task_ids: [91],
          stored_evidence_replay: {
            operation_id: "stored-data-api-public-access-replay-v1",
            run_id: "run:shared-video-disposition",
            expected_candidate_count: 1,
            task_ids: [91],
          },
        },
      })
    : await processContentDetailBatchV2({
        data: {
          run_id: "run:shared-video-disposition",
          channel_id: "UCsharedDisposition",
          api_fallback_mode: "disabled",
          content_max_age_days: scenario === "age_excluded" ? 90 : 0,
        },
      });
} catch (caught) {
  error = { message: caught?.message ?? String(caught), stack: caught?.stack ?? null };
}

await writeFile(outputPath, JSON.stringify({
  value,
  error,
  candidate: globalThis.__pipelineV2DispositionState.candidate,
  queries: globalThis.__pipelineV2DispositionState.queries.map(({ sql }) => sql),
}), "utf8");
if (error) process.exitCode = 1;
