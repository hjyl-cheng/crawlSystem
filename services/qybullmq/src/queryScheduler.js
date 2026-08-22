import { query } from "./db.js";

export const QUERY_SCHEDULER_KEY = "query_scheduler";
export const QUERY_SCHEDULER_STATUSES = new Set(["stopped", "running", "paused", "finishing", "repairing"]);
export const DEFAULT_DISCOVER_BACKLOG_LIMIT = 3;
const AUTOMATIC_FINALIZATION_STATUSES = new Set(["finishing", "repairing"]);

export function normalizeQueryScheduler(value = {}) {
  const status = QUERY_SCHEDULER_STATUSES.has(String(value.status || "")) ? String(value.status) : "stopped";
  const querySetId = Number(value.query_set_id);
  const chunkSize = Number(value.chunk_size);
  const maxDiscoverBacklog = Number(value.max_discover_backlog);
  const queryQualityMinScore = Number(value.query_quality_min_score);
  return {
    status,
    query_set_id: Number.isFinite(querySetId) && querySetId > 0 ? Math.floor(querySetId) : null,
    query_quality_min_score: Number.isFinite(queryQualityMinScore)
      ? Math.max(0, Math.min(100, Math.floor(queryQualityMinScore)))
      : 0,
    chunk_size: Number.isFinite(chunkSize) && chunkSize > 0 ? Math.max(1, Math.min(100, Math.floor(chunkSize))) : 3,
    max_discover_backlog: Number.isFinite(maxDiscoverBacklog) && maxDiscoverBacklog >= 1
      ? Math.max(1, Math.min(20, Math.floor(maxDiscoverBacklog)))
      : DEFAULT_DISCOVER_BACKLOG_LIMIT,
    started_at: value.started_at || null,
    paused_at: value.paused_at || null,
    stopped_at: value.stopped_at || null,
    completed_at: value.completed_at || null,
    stop_reason: value.stop_reason || null,
    paused_from_status: value.paused_from_status || null,
    pipeline_cycle_id: value.pipeline_cycle_id || null,
    updated_at: value.updated_at || null,
    updated_by: value.updated_by || null,
  };
}

export async function getQueryScheduler() {
  const row = await query(
    "SELECT value_json FROM crawler.settings WHERE setting_key = $1 LIMIT 1",
    [QUERY_SCHEDULER_KEY],
  );
  return normalizeQueryScheduler(row.rows[0]?.value_json || {});
}

export function querySchedulerAllowsDiscovery(scheduler) {
  return normalizeQueryScheduler(scheduler).status === "running";
}

export async function reconcileAutomaticDiscoveryClosure(dbQuery, scheduler) {
  if (typeof dbQuery !== "function") throw new TypeError("dbQuery must be a function");
  const normalized = normalizeQueryScheduler(scheduler);
  if (
    !normalized.pipeline_cycle_id
    || !AUTOMATIC_FINALIZATION_STATUSES.has(normalized.status)
  ) return false;

  const rows = await dbQuery(
    `UPDATE crawler.query_dispatch_batches
     SET status=CASE WHEN status='running' THEN 'discovery_closed' ELSE status END,
         discovery_closed_at=COALESCE(discovery_closed_at,now()),
         updated_at=now()
     WHERE dispatch_batch_id=$1
       AND discovery_closed_at IS NULL
     RETURNING dispatch_batch_id`,
    [normalized.pipeline_cycle_id],
  );
  return rows.rows.length > 0;
}
