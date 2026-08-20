const channelId = "UCaboutOnlyRegression";
const runId = "run:promotion";

function state() {
  return globalThis.__pipelineV2AboutOnlyState;
}

function result(rows = [], rowCount = rows.length) {
  return { rows, rowCount };
}

export async function query(sqlValue) {
  const sql = String(sqlValue);
  state().queries.push(sql);

  if (sql.includes("publication-gap-repair:stage-about-only")) {
    return result([{ expected_content_count: 30 }]);
  }
  if (sql.includes("FROM crawler.channel_candidates WHERE candidate_id")) {
    return result([{
      candidate_id: 42,
      channel_id: channelId,
      channel_url: `https://www.youtube.com/channel/${channelId}`,
      status: "accepted",
      title: "About-only regression",
    }]);
  }
  if (sql.includes("SELECT * FROM crawler.channels WHERE channel_id")) {
    return result([{
      channel_id: channelId,
      channel_url: `https://www.youtube.com/channel/${channelId}`,
      status: "active",
      latest_run_id: runId,
      registry_promotion_candidate_id: 42,
      registry_promotion_run_id: runId,
    }]);
  }
  if (sql.includes("SELECT count(*)::int AS count") && sql.includes("crawler.content_candidates")) {
    return result([{ count: 30 }]);
  }
  if (sql.includes("FROM crawler.content_candidates candidate")) return result([]);
  if (sql.includes("count(*)::int AS total") && sql.includes("FROM crawler.content_candidates")) {
    return result([{
      total: 30,
      terminal: 30,
      api_open: 0,
      failed: 0,
      partial: 0,
      excluded: 0,
      age_excluded: 0,
      upcoming_excluded: 0,
    }]);
  }
  if (sql.includes("sum(COALESCE((result_json#>>'{detail,youtubejs_request_count}')")) {
    return result([{ request_count: 0 }]);
  }
  if (sql.includes("FROM crawler.channels c") && sql.includes("LEFT JOIN crawler.channel_runs")) {
    return result([{
      channel_id: channelId,
      latest_run_id: runId,
      channel_status: "active",
      agent_status: "done",
      detail_status: "done",
      expected_content_count: 30,
      candidate_count: 30,
    }]);
  }
  if (sql.includes("UPDATE crawler.channels channel") && sql.includes("ready_for_agent")) {
    return result([{
      channel_id: channelId,
      ready_for_agent: true,
      agent_status: "done",
    }]);
  }
  if (sql.includes("SELECT value_json FROM crawler.settings")) return result([]);
  if (sql.includes("ready-discovery-pages")) return result([]);
  if (sql.includes("FROM crawler.query_pages page")) return result([]);
  if (sql.includes("UPDATE crawler.youtube_api_tasks")) return result([], 0);
  return result([], 1);
}

export async function withTransaction(callback) {
  const client = {
    query: async (sql, params) => {
      const response = await query(sql, params);
      if (String(sql).includes("INSERT INTO crawler.channel_runs")) {
        return result([{ run_id: runId, publication_finalized_at: null }]);
      }
      if (String(sql).includes("UPDATE crawler.channels AS channel")) {
        return result([{ latest_run_id: runId }]);
      }
      if (String(sql).includes("UPDATE crawler.business_run_bindings")) {
        return result([{ status: "materialized" }]);
      }
      if (String(sql).includes("UPDATE crawler.channel_runs run")) {
        return result([{ run_id: runId }]);
      }
      return response;
    },
  };
  return callback(client);
}

export async function applyMigrationActivityGate() {
  return { decision: "not_required", reject: false };
}

export const queuesByRole = Object.freeze({
  discoverPage: "youtube-discover-page",
  finalize: "youtube-finalize",
});

export function createQueues() {
  return {
    [queuesByRole.discoverPage]: {
      client: Promise.resolve({ publish: async () => 0 }),
    },
    [queuesByRole.finalize]: {
      add: async (...args) => state().finalizeCalls.push(args),
    },
  };
}

export function safeJobId(...parts) {
  return parts.flat().join("__");
}

export async function putRawObject(input) {
  state().rawObjects.push(input);
  return { object_key: "test/about-only" };
}

function unexpectedContentCollection() {
  const error = new Error("UNEXPECTED_CONTENT_COLLECTION");
  error.code = "UNEXPECTED_CONTENT_COLLECTION";
  throw error;
}

export async function fetchChannelInitial() {
  return unexpectedContentCollection();
}

export async function fetchChannelYtDlpMetadata() {
  return unexpectedContentCollection();
}

export async function fetchChannelDataApiDetails() {
  return null;
}

export async function fetchChannelUploads() {
  return unexpectedContentCollection();
}

export async function fetchVideoDataApiDetails() {
  return null;
}

export async function fetchVideoCommentThreadsDataApi() {
  return null;
}

export async function fetchVideoYtDlpDetail() {
  return null;
}

export function parseChannelHeader() {
  return {};
}

export function youtubeJsChannelEnabled() {
  return true;
}

export function youtubeJsDetailEnabled() {
  return false;
}

export async function openYoutubeJsChannel() {
  return {
    metadata: {
      channel_id: channelId,
      channel_url: `https://www.youtube.com/channel/${channelId}`,
      title: "About-only regression",
      handle: "@aboutOnlyRegression",
      description: "Complete About metadata for the routing regression.",
      country: "Brazil",
      subscriber_count: 10000,
      subscriber_count_text: "10000",
      subscriber_count_source: "youtube_about",
      total_view_count: 1000000,
      view_count_text: "1000000",
      view_count_source: "youtube_about",
      total_video_count: 30,
      video_count_text: "30",
      video_count_source: "youtube_about",
    },
    about_observed: true,
    about_error: null,
    raw: { engine: "test-youtubejs" },
    fetchContents: async () => unexpectedContentCollection(),
  };
}

export async function fetchYoutubeJsVideoDetail() {
  return null;
}

export async function fetchYoutubeJsCommentFirstPage() {
  return null;
}
