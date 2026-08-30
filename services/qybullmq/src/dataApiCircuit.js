function roleReady(capacity, role) {
  const value = capacity?.roles?.[role]?.ready;
  return Number.isFinite(value) ? Math.max(0, Number(value)) : null;
}

function retryableSystemFailure(payload) {
  const decision = payload?.youtube_failure_decision;
  return decision?.kind === "retryable_system_failure"
    || decision?.evidence?.failure_type === "retryable_system_failure";
}

const DETAIL_FAILURE_SOURCES = new Set([
  "yt_dlp_detail",
  "youtubejs_player",
]);

const DETAIL_TARGET_PATHS = new Set([
  "/watch",
  "/youtubei/v1/player",
  "/youtubei/v1/next",
]);

function youtubeDetailTarget(value) {
  try {
    const url = new URL(String(value ?? ""));
    const hostname = url.hostname.toLowerCase();
    return (hostname === "youtube.com" || hostname.endsWith(".youtube.com"))
      && DETAIL_TARGET_PATHS.has(url.pathname.replace(/\/$/, ""));
  } catch {
    return false;
  }
}

function realDetailRequestFailure(evidence) {
  if (!evidence || typeof evidence !== "object") return false;
  const source = String(evidence.source ?? "").trim().toLowerCase();
  return DETAIL_FAILURE_SOURCES.has(source)
    || youtubeDetailTarget(evidence.target_url);
}

export function dataApiCircuitCountsTaskFailure(payload) {
  if (retryableSystemFailure(payload)) return false;
  const requests = payload?.channel_execution_attempt?.youtube_requests;
  const requestCount = requests?.request_count;
  const failureCount = requests?.failure_count;
  return typeof requestCount === "number"
    && Number.isSafeInteger(requestCount)
    && requestCount > 0
    && typeof failureCount === "number"
    && Number.isSafeInteger(failureCount)
    && failureCount > 0
    && Array.isArray(requests?.failure_evidence)
    && requests.failure_evidence.some(realDetailRequestFailure);
}

export async function dataApiCircuitState({
  query,
  proxyCapacity,
  detailExecutionQueue,
  detailExecutionRole,
  failureThreshold = 5,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const rows = await query(
    `SELECT payload_json
     FROM crawler.task_events
     WHERE queue_name=$1
       AND status='failed'
       AND COALESCE(payload_json#>>'{youtube_failure_decision,kind}','')
             <> 'retryable_system_failure'
       AND COALESCE(payload_json#>>'{youtube_failure_decision,evidence,failure_type}','')
             <> 'retryable_system_failure'
       AND payload_json @? '$.channel_execution_attempt.youtube_requests.request_count ? (@.type() == "number" && @ > 0)'
       AND payload_json @? '$.channel_execution_attempt.youtube_requests.failure_count ? (@.type() == "number" && @ > 0)'
       AND created_at >= now() - interval '5 minutes'`,
    [detailExecutionQueue],
  );
  const recentFailures = rows.rows.filter(({ payload_json: payload }) => (
    dataApiCircuitCountsTaskFailure(payload)
  )).length;
  const detailReady = roleReady(proxyCapacity, detailExecutionRole);
  const proxyCapacityLow = Number.isFinite(detailReady)
    ? detailReady === 0
    : Number.isFinite(proxyCapacity?.active) && proxyCapacity.active < 3;
  return {
    open: proxyCapacityLow || recentFailures >= failureThreshold,
    reason: proxyCapacityLow
      ? "proxy_capacity_low"
      : recentFailures >= failureThreshold ? "detail_failure_spike" : null,
    recent_detail_failures: recentFailures,
  };
}
