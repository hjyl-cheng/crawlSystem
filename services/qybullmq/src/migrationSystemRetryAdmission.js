const ACTIVE_SYSTEM_RETRY_STATUSES = Object.freeze(["retrying", "dispatched"]);

function text(value) {
  return String(value ?? "").trim() || null;
}

export function migrationSystemRetryDispatchAdmission(scheduler = {}) {
  const schedulerStatus = text(scheduler.status) ?? "stopped";
  const schedulerStopReason = text(scheduler.stop_reason);
  const allowed = schedulerStatus === "stopped"
    && schedulerStopReason === "pipeline_complete";
  return Object.freeze({
    allowed,
    code: allowed ? null : "migration_system_retry_scheduler_blocked",
    scheduler_status: schedulerStatus,
    scheduler_stop_reason: schedulerStopReason,
    scheduler_pipeline_cycle_id: text(scheduler.pipeline_cycle_id),
  });
}

// The caller must hold the query_scheduler row lock until its transaction completes.
export async function sharedCrawlerSchedulerActivationAdmission(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const active = await client.query(
    `SELECT system_retry_id,candidate_id,failed_dispatch_batch_id,status
     FROM crawler.migration_system_retry_items
     WHERE status=ANY($1::text[])
     ORDER BY requested_at,system_retry_id
     LIMIT 1`,
    [ACTIVE_SYSTEM_RETRY_STATUSES],
  );
  const row = active.rows[0] ?? null;
  return Object.freeze({
    allowed: row == null,
    code: row == null ? null : "migration_system_retry_recovery_active",
    active_system_retry: row == null
      ? null
      : Object.freeze({
        system_retry_id: Number(row.system_retry_id),
        candidate_id: Number(row.candidate_id),
        failed_dispatch_batch_id: text(row.failed_dispatch_batch_id),
        status: text(row.status),
      }),
  });
}
