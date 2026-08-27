function state() {
  return globalThis.__pipelineV2DispositionState;
}

const YOUTUBEJS_COMMENT_SCENARIOS = new Set([
  "youtubejs_disabled_ytdlp_visible",
  "youtubejs_visible_ytdlp_disabled",
]);
const YOUTUBEJS_DETAIL_SCENARIOS = new Set([
  ...YOUTUBEJS_COMMENT_SCENARIOS,
  "youtubejs_unlisted_ytdlp_public",
  "detail_cancelled",
]);

function result(rows = [], rowCount = rows.length) {
  return { rows, rowCount };
}

function candidateSummary() {
  const candidate = state().candidate;
  const terminal = ["done", "unavailable"].includes(candidate.detail_status) ? 1 : 0;
  const scopeReason = candidate.result_json?.scope?.reason;
  return {
    total: 1,
    terminal,
    api_open: ["pending", "queued", "running", "failed"].includes(candidate.api_status) ? 1 : 0,
    failed: candidate.detail_status === "failed" ? 1 : 0,
    undisposed: terminal === 1 && candidate.disposition == null ? 1 : 0,
    partial: candidate.missing_fields.length > 0 && terminal === 1 ? 1 : 0,
    excluded: candidate.result_json?.scope?.status === "excluded" ? 1 : 0,
    age_excluded: ["older_than_max_age", "after_chronological_age_cutoff"].includes(scopeReason) ? 1 : 0,
    upcoming_excluded: scopeReason === "upcoming_live" ? 1 : 0,
    live_in_progress_excluded: scopeReason === "live_in_progress" ? 1 : 0,
    details_requested_due_to_unresolved_count:
      candidate.result_json?.detail_request?.reason_code === "initial_publication_unresolved" ? 1 : 0,
  };
}

export async function query(sqlValue, params = []) {
  const sql = String(sqlValue);
  state().queries.push({ sql, params });
  if (sql.includes("setting_key = 'crawl'")) {
    return result([{ value_json: {
      channel_content_limit: 30,
      content_max_age_days: 0,
      detail_max_attempts: 1,
      detail_concurrency: 1,
    } }]);
  }
  if (sql.includes("setting_key = 'youtube_api'")) {
    return result([{ value_json: {
      fallback_mode: "disabled",
      api_keys: state().scenario === "data_api_live" ? ["test-key"] : [],
    } }]);
  }
  if (sql.includes("INSERT INTO crawler.youtube_api_daily_usage")) {
    return result([{ usage_date: "2026-08-25", request_count: 1, requested_video_count: 1 }]);
  }
  if (sql.includes("SELECT * FROM crawler.youtube_api_tasks")) {
    return result(state().tasks.map((task) => ({ ...task })));
  }
  if (sql.includes("FROM crawler.content_candidates candidate") && sql.includes("crawl_started_at")) {
    const terminal = ["done", "unavailable"].includes(state().candidate.detail_status);
    const selectsUndisposedTerminal = sql.includes("candidate.disposition IS NULL");
    return sql.includes("candidate.detail_status NOT IN")
      && ["done", "unavailable", "api_pending"].includes(state().candidate.detail_status)
      && !(terminal && state().candidate.disposition == null && selectsUndisposedTerminal)
      ? result([])
      : result([{ ...state().candidate }]);
  }
  if (sql.includes("UPDATE crawler.content_candidates") && sql.includes("detail_status='running'")) {
    state().candidate.detail_status = "running";
    state().candidate.attempts += 1;
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates")
      && sql.includes("content_type=COALESCE($2,content_type)")
      && sql.includes("detail_status='failed'")) {
    state().candidate.content_type = params[1] ?? state().candidate.content_type;
    state().candidate.type_status = params[1] == null ? state().candidate.type_status : "resolved";
    state().candidate.type_source = params[2] ?? state().candidate.type_source;
    state().candidate.detail_status = "failed";
    state().candidate.api_status = "not_needed";
    state().candidate.missing_fields = params[3];
    state().candidate.result_json = JSON.parse(params[4]);
    state().candidate.error_message = params[5];
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates") && sql.includes("content_type=NULL,type_status=$2")) {
    state().candidate.content_type = null;
    state().candidate.type_status = params[1];
    state().candidate.detail_status = params[2];
    state().candidate.api_status = params[3];
    state().candidate.missing_fields = params[4];
    state().candidate.result_json = JSON.parse(params[5]);
    state().candidate.error_message = params[6];
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates") && sql.includes("content_type='live'")) {
    state().candidate.content_type = "live";
    state().candidate.type_status = "resolved";
    state().candidate.type_source = params[1];
    state().candidate.content_key = null;
    state().candidate.detail_status = "done";
    state().candidate.api_status = "not_needed";
    state().candidate.missing_fields = [];
    state().candidate.result_json = JSON.parse(params[2]);
    state().candidate.error_message = null;
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates")
      && sql.includes("SET content_key=NULL,detail_status='done'")
      && sql.includes("result_json=$2::jsonb")) {
    state().candidate.content_key = null;
    state().candidate.detail_status = "done";
    state().candidate.api_status = "not_needed";
    state().candidate.missing_fields = [];
    state().candidate.result_json = JSON.parse(params[1]);
    state().candidate.error_message = null;
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates")
      && sql.includes("api_status=$5")
      && sql.includes("detail_status='done'")) {
    state().candidate.content_type = params[1];
    state().candidate.type_status = "resolved";
    state().candidate.type_source = params[2];
    state().candidate.content_key = params[3];
    state().candidate.detail_status = "done";
    state().candidate.api_status = params[4];
    state().candidate.missing_fields = params[5];
    state().candidate.result_json = JSON.parse(params[6]);
    state().candidate.error_message = null;
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates")
      && sql.includes("content_type=$2,type_status='resolved'")
      && sql.includes("detail_status=$4")) {
    state().candidate.content_type = params[1];
    state().candidate.type_status = "resolved";
    state().candidate.type_source = params[2];
    state().candidate.content_key = null;
    state().candidate.detail_status = params[3];
    state().candidate.api_status = params[4];
    state().candidate.missing_fields = params[5];
    state().candidate.result_json = JSON.parse(params[6]);
    state().candidate.error_message = params[7];
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates")
      && sql.includes("content_type=$2,type_status='resolved'")
      && sql.includes("missing_fields=$5::text[]")
      && sql.includes("detail_status='done'")) {
    state().candidate.content_type = params[1];
    state().candidate.type_status = "resolved";
    state().candidate.type_source = params[2];
    state().candidate.content_key = params[3];
    state().candidate.detail_status = "done";
    state().candidate.api_status = "not_needed";
    state().candidate.missing_fields = params[4];
    state().candidate.result_json = JSON.parse(params[5]);
    state().candidate.error_message = null;
    return result([], 1);
  }
  if (sql.includes("UPDATE crawler.content_candidates")
      && sql.includes("content_type=$2,type_status='resolved'")
      && sql.includes("detail_status='done'")) {
    state().candidate.content_type = params[1];
    state().candidate.type_status = "resolved";
    state().candidate.type_source = params[2];
    state().candidate.content_key = params[3];
    state().candidate.detail_status = "done";
    state().candidate.api_status = "not_needed";
    state().candidate.missing_fields = [];
    state().candidate.result_json = JSON.parse(params[4]);
    state().candidate.error_message = null;
    return result([], 1);
  }
  if (sql.includes("jsonb_build_object('disposition'")) {
    state().dispositionWriteAttempts += 1;
    if (state().scenario === "disposition_write_retry"
        && state().dispositionWriteAttempts === 1) {
      throw new Error("injected disposition persistence failure");
    }
    state().candidate.disposition = params[1];
    state().candidate.next_attempt_at = params[2];
    state().candidate.result_json.disposition = JSON.parse(params[3]);
    if (params[4] != null) state().candidate.result_json.recovery = JSON.parse(params[4]);
    return result([], 1);
  }
  if (sql.includes("count(*)::int AS total") && sql.includes("FROM crawler.content_candidates")) {
    return result([candidateSummary()]);
  }
  if (sql.includes("sum(COALESCE((result_json#>>'{detail,youtubejs_request_count}')")) {
    return result([{ request_count: 0 }]);
  }
  if (sql.includes("UPDATE crawler.youtube_api_tasks")) return result([], 0);
  if (sql.includes("INSERT INTO crawler.contents") && sql.includes("RETURNING content_key")) {
    return result([{ content_key: params[0] }]);
  }
  if (sql.includes("UPDATE crawler.contents") && sql.includes("RETURNING content_key,content_type")) {
    return result([{
      content_key: state().candidate.known_content_key,
      content_type: state().candidate.known_content_type,
      content_type_source: state().candidate.known_content_type_source,
    }]);
  }
  return result([], 1);
}

export async function withTransaction(callback) {
  return callback({ query });
}

export async function applyMigrationActivityGate() {
  return { decision: "not_required", reject: false };
}

export const queuesByRole = Object.freeze({
  discoverPage: "youtube-discover-page",
  contentDetail: "youtube-content-detail",
  dataApiBatch: "youtube-data-api-batch",
  finalize: "youtube-finalize",
});

export function createQueues() {
  return new Proxy({}, {
    get: () => ({
      add: async () => null,
      client: Promise.resolve({ publish: async () => 0 }),
    }),
  });
}

export function safeJobId(...parts) {
  return parts.flat().join("__");
}

export async function putRawObject(input) {
  state().rawObjects.push(input);
  return { object_key: "test/video-disposition" };
}

export async function fetchVideoYtDlpDetail(videoId) {
  state().youtubeRequestAttempts += 1;
  state().ytDlpDetailAttempts += 1;
  if (state().scenario === "detail_failure") {
    const error = new Error(`Player timeout for ${videoId}`);
    error.code = "ETIMEDOUT";
    throw error;
  }
  const privateAccess = ["terminal_private", "existing_private"].includes(state().scenario);
  const authoritativeType = privateAccess
    || [
      "stored_public",
      "disabled_comments",
      "disposition_write_retry",
      "candidate_retry_publication_conflict",
      ...YOUTUBEJS_DETAIL_SCENARIOS,
    ].includes(state().scenario);
  const commentsVisible = state().scenario === "youtubejs_disabled_ytdlp_visible";
  const ytDlpDisabledConflict = state().scenario === "youtubejs_visible_ytdlp_disabled";
  const commentsDisabled = state().scenario === "disabled_comments" || ytDlpDisabledConflict;
  const liveInProgress = ["live_in_progress", "live_in_progress_flat"].includes(state().scenario);
  const liveReplay = state().scenario === "live_replay";
  return {
    id: videoId,
    title: "Public detail without authoritative type",
    description: "Complete enough for storage except type",
    published_at: "2026-07-19T00:00:00.000Z",
    published_at_precision: "second",
    duration_seconds: liveInProgress ? null : liveReplay ? 3600 : 90,
    view_count: 100,
    view_count_text: "100",
    like_count: 3,
    comment_count: commentsVisible
      ? 19
      : ytDlpDisabledConflict
        ? 0
        : commentsDisabled || liveInProgress
          ? null
          : liveReplay
            ? 12
            : 0,
    comment_count_status: commentsDisabled ? "disabled" : liveInProgress ? "unresolved" : "exact",
    comment_count_source: commentsVisible || ytDlpDisabledConflict ? "yt_dlp" : null,
    comments_status_source: commentsVisible || ytDlpDisabledConflict ? "yt_dlp" : null,
    comments_disabled: commentsVisible ? false : commentsDisabled,
    comments_first_page: commentsVisible
      ? {
          version: 1,
          collected_at: "2026-08-25T00:00:01.000Z",
          sort: "TOP_COMMENTS",
          total_count: 19,
          returned_count: 15,
          comments: [{ comment_id: "yt-dlp-visible", text: "Visible yt-dlp comment" }],
        }
      : ytDlpDisabledConflict
        ? {
            version: 1,
            collected_at: "2026-08-25T00:00:01.000Z",
            sort: "TOP_COMMENTS",
            total_count: 0,
            returned_count: 0,
            comments: [],
          }
        : null,
    comments_first_page_status: commentsVisible ? "collected" : ytDlpDisabledConflict ? "disabled" : null,
    comments_first_page_source: commentsVisible || ytDlpDisabledConflict ? "yt_dlp_top_comments" : null,
    is_live: liveInProgress,
    was_live: liveReplay,
    live_status: liveInProgress ? "is_live" : liveReplay ? "was_live" : "not_live",
    access_status: privateAccess ? "private" : "public",
    availability: privateAccess ? "private" : "public",
    ytdlp_client: "web",
    extractor_version: "yt-dlp@test",
    content_type_signals: {
      source: "yt_dlp_player",
      canonical_url: authoritativeType || liveReplay ? `https://www.youtube.com/watch?v=${videoId}` : null,
      is_shorts_eligible: authoritativeType ? false : null,
      is_live_content: liveInProgress || liveReplay ? true : authoritativeType ? false : null,
      is_live: liveInProgress,
      is_live_now: liveInProgress,
    },
  };
}

export async function fetchChannelInitial() { return null; }
export async function fetchChannelYtDlpMetadata() { return null; }
export async function fetchChannelDataApiDetails() { return null; }
export async function fetchChannelUploads() { return null; }
export async function fetchVideoDataApiDetails(videoIds) {
  state().youtubeRequestAttempts += 1;
  if (state().scenario !== "data_api_live") return null;
  const detail = {
    id: videoIds[0],
    title: "Running Live",
    description: "Live now",
    description_status: "exact",
    published_at: "2026-07-19T00:00:00.000Z",
    published_at_precision: "second",
    view_count: 100,
    view_count_text: "100",
    like_count: 3,
    comment_count: null,
    comments_disabled: null,
    privacy_status: "public",
    access_status: "public",
    is_live: true,
    live_status: "is_live",
    content_type_signals: {
      source: "youtube_data_api_videos_list",
      canonical_url: `https://www.youtube.com/watch?v=${videoIds[0]}`,
      is_live_content: true,
      is_live: true,
      is_live_now: true,
    },
  };
  return {
    detailsById: new Map([[videoIds[0], detail]]),
    raw: { items: [{ id: videoIds[0] }] },
    returnedCount: 1,
  };
}
export async function fetchVideoCommentThreadsDataApi() { return null; }
export function parseChannelHeader() { return {}; }
export function youtubeJsChannelEnabled() { return false; }
export function youtubeJsDetailEnabled() { return YOUTUBEJS_DETAIL_SCENARIOS.has(state().scenario); }
export async function openYoutubeJsChannel() { return null; }
export async function fetchYoutubeJsVideoDetail(videoId, { signal = null } = {}) {
  state().youtubeJsDetailAttempts += 1;
  if (state().scenario === "detail_cancelled") {
    state().forwardedDetailSignal = signal === state().cancellationSignal;
    state().cancelDetail();
    throw state().cancellationSignal.reason;
  }
  const unlistedConflict = state().scenario === "youtubejs_unlisted_ytdlp_public";
  const visible = state().scenario === "youtubejs_visible_ytdlp_disabled" || unlistedConflict;
  return {
    id: videoId,
    title: "YouTube.js detail",
    description: visible ? null : "Complete YouTube.js description",
    description_status: visible ? "unresolved" : "exact",
    description_source: visible ? null : "youtubejs_player",
    published_at: "2026-07-19T00:00:00.000Z",
    published_at_precision: "second",
    published_at_source: "youtubejs_player_microformat",
    duration_seconds: 90,
    view_count: 100,
    view_count_text: "100",
    like_count: 3,
    comment_count: visible ? 12 : 0,
    comment_count_status: visible ? "exact" : "disabled",
    comment_count_source: "youtubejs_comments",
    comments_status_source: "youtubejs_comments",
    comments_disabled: visible ? false : true,
    comments_first_page: visible
      ? {
          version: 1,
          collected_at: "2026-08-25T00:00:00.000Z",
          sort: "TOP_COMMENTS",
          total_count: 12,
          returned_count: 1,
          comments: [{ comment_id: "youtubejs-visible", text: "Visible YouTube.js comment" }],
        }
      : {
          version: 1,
          collected_at: "2026-08-25T00:00:00.000Z",
          sort: "TOP_COMMENTS",
          total_count: 0,
          returned_count: 0,
          comments: [],
        },
    comments_first_page_status: visible ? "collected" : "disabled",
    comments_first_page_source: "youtubejs_comments",
    is_live: false,
    live_status: "not_live",
    access_status: unlistedConflict ? "unlisted" : "public",
    availability: unlistedConflict ? "unlisted" : "public",
    ...(unlistedConflict
      ? {
          is_unlisted: true,
          playability_status: "OK",
          source: "youtubejs_get_info",
        }
      : {}),
    extractor_version: "youtubei.js@test",
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_live_now: false,
    },
  };
}
export async function fetchYoutubeJsCommentFirstPage() { return {}; }
