function state() {
  return globalThis.__pipelineV2DispositionState;
}

function result(rows = [], rowCount = rows.length) {
  return { rows, rowCount };
}

function candidateSummary() {
  const candidate = state().candidate;
  const terminal = ["done", "unavailable"].includes(candidate.detail_status) ? 1 : 0;
  return {
    total: 1,
    terminal,
    api_open: ["pending", "queued", "running", "failed"].includes(candidate.api_status) ? 1 : 0,
    failed: candidate.detail_status === "failed" ? 1 : 0,
    undisposed: terminal === 1 && candidate.disposition == null ? 1 : 0,
    partial: candidate.missing_fields.length > 0 && terminal === 1 ? 1 : 0,
    excluded: 0,
    age_excluded: 0,
    upcoming_excluded: 0,
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
    return result([{ value_json: { fallback_mode: "disabled" } }]);
  }
  if (sql.includes("SELECT * FROM crawler.youtube_api_tasks")) {
    return result(state().tasks.map((task) => ({ ...task })));
  }
  if (sql.includes("FROM crawler.content_candidates candidate") && sql.includes("crawl_started_at")) {
    return sql.includes("candidate.detail_status NOT IN")
      && ["done", "unavailable", "api_pending"].includes(state().candidate.detail_status)
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
    return result([{ content_key: `UCsharedDisposition:video:${state().candidate.source_content_id}` }]);
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
  if (state().scenario === "detail_failure") {
    const error = new Error(`Player timeout for ${videoId}`);
    error.code = "ETIMEDOUT";
    throw error;
  }
  const privateAccess = ["terminal_private", "existing_private"].includes(state().scenario);
  const authoritativeType = privateAccess || state().scenario === "stored_public";
  return {
    id: videoId,
    title: "Public detail without authoritative type",
    description: "Complete enough for storage except type",
    published_at: "2026-07-19T00:00:00.000Z",
    published_at_precision: "second",
    duration_seconds: 90,
    view_count: 100,
    view_count_text: "100",
    like_count: 3,
    comment_count: 0,
    comments_disabled: false,
    access_status: privateAccess ? "private" : "public",
    availability: privateAccess ? "private" : "public",
    ytdlp_client: "web",
    extractor_version: "yt-dlp@test",
    content_type_signals: {
      source: "yt_dlp_player",
      canonical_url: authoritativeType ? `https://www.youtube.com/watch?v=${videoId}` : null,
      is_shorts_eligible: authoritativeType ? false : null,
      is_live_content: authoritativeType ? false : null,
    },
  };
}

export async function fetchChannelInitial() { return null; }
export async function fetchChannelYtDlpMetadata() { return null; }
export async function fetchChannelDataApiDetails() { return null; }
export async function fetchChannelUploads() { return null; }
export async function fetchVideoDataApiDetails() { return null; }
export async function fetchVideoCommentThreadsDataApi() { return null; }
export function parseChannelHeader() { return {}; }
export function youtubeJsChannelEnabled() { return false; }
export function youtubeJsDetailEnabled() { return false; }
export async function openYoutubeJsChannel() { return null; }
export async function fetchYoutubeJsVideoDetail() { return null; }
export async function fetchYoutubeJsCommentFirstPage() { return {}; }
