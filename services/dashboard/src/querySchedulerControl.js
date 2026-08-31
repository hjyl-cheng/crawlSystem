const ACTIVE_SYSTEM_RETRY_STATUSES = Object.freeze(["retrying", "dispatched"]);
const PAUSABLE_SCHEDULER_STATUSES = new Set(["running", "finishing", "repairing"]);
const STOPPABLE_SCHEDULER_STATUSES = new Set([
  "running",
  "finishing",
  "repairing",
  "paused",
]);

function text(value) {
  return String(value ?? "").trim() || null;
}

function schedulerExecutionIdentity(scheduler = {}) {
  return Object.freeze({
    status: text(scheduler.status) ?? "stopped",
    stop_reason: text(scheduler.stop_reason),
    pipeline_cycle_id: text(scheduler.pipeline_cycle_id),
    completed_at: text(scheduler.completed_at),
    updated_at: text(scheduler.updated_at),
  });
}

function sameSchedulerExecution(left, right) {
  const current = schedulerExecutionIdentity(left);
  const expected = schedulerExecutionIdentity(right);
  return Object.keys(current).every((key) => current[key] === expected[key]);
}

function rejectedTransition(code, scheduler) {
  return Object.freeze({
    allowed: false,
    code,
    scheduler: schedulerExecutionIdentity(scheduler),
  });
}

export function querySchedulerStartTransition(scheduler, { expected = null } = {}) {
  if (expected && !sameSchedulerExecution(scheduler, expected)) {
    return rejectedTransition("query_scheduler_start_state_changed", scheduler);
  }
  const current = schedulerExecutionIdentity(scheduler);
  if (current.status !== "stopped") {
    return rejectedTransition("query_scheduler_start_not_stopped", scheduler);
  }
  const continuesCurrentCycle = current.pipeline_cycle_id != null
    && current.completed_at == null
    && current.stop_reason === "user_requested";
  return Object.freeze({
    allowed: true,
    code: null,
    startsNewCycle: !continuesCurrentCycle,
    continuesCurrentCycle,
    scheduler: current,
  });
}

export function querySchedulerResumeTransition(scheduler, { expected = null } = {}) {
  if (expected && !sameSchedulerExecution(scheduler, expected)) {
    return rejectedTransition("query_scheduler_resume_state_changed", scheduler);
  }
  const current = schedulerExecutionIdentity(scheduler);
  if (current.status !== "paused" || current.pipeline_cycle_id == null) {
    return rejectedTransition("query_scheduler_resume_not_paused", scheduler);
  }
  return Object.freeze({
    allowed: true,
    code: null,
    startsNewCycle: false,
    resumeStatus: ["finishing", "repairing"].includes(scheduler?.paused_from_status)
      ? scheduler.paused_from_status
      : "running",
    scheduler: current,
  });
}

export function querySchedulerPauseTransition(scheduler, { expected = null } = {}) {
  if (expected && !sameSchedulerExecution(scheduler, expected)) {
    return rejectedTransition("query_scheduler_pause_state_changed", scheduler);
  }
  const current = schedulerExecutionIdentity(scheduler);
  if (!PAUSABLE_SCHEDULER_STATUSES.has(current.status)) {
    return rejectedTransition("query_scheduler_pause_not_active", scheduler);
  }
  return Object.freeze({
    allowed: true,
    code: null,
    startsNewCycle: false,
    pausedFromStatus: current.status,
    scheduler: current,
  });
}

export function querySchedulerStopTransition(scheduler, { expected = null } = {}) {
  if (expected && !sameSchedulerExecution(scheduler, expected)) {
    return rejectedTransition("query_scheduler_stop_state_changed", scheduler);
  }
  const current = schedulerExecutionIdentity(scheduler);
  if (!STOPPABLE_SCHEDULER_STATUSES.has(current.status)) {
    return rejectedTransition("query_scheduler_stop_not_active", scheduler);
  }
  return Object.freeze({
    allowed: true,
    code: null,
    startsNewCycle: false,
    scheduler: current,
  });
}

export async function stopQuerySchedulerBatch(client, scheduler) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const pipelineCycleId = text(scheduler?.pipeline_cycle_id);
  if (!pipelineCycleId) return 0;
  const stopped = await client.query(
    `UPDATE crawler.query_dispatch_batches
     SET status=CASE WHEN status='completed' THEN status ELSE 'stopped' END,
         finished_at=CASE WHEN status='completed' THEN finished_at ELSE now() END,
         updated_at=now()
     WHERE dispatch_batch_id=$1
     RETURNING dispatch_batch_id`,
    [pipelineCycleId],
  );
  return stopped.rowCount;
}

async function sharedCrawlerSchedulerActivationAdmission(client) {
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

export async function updateQuerySchedulerWithMigrationFence({
  pool,
  normalize,
  mutate,
  afterUpdate = null,
  now = new Date().toISOString(),
} = {}) {
  if (!pool || typeof pool.connect !== "function") throw new TypeError("pool is required");
  if (typeof normalize !== "function") throw new TypeError("normalize is required");
  if (typeof mutate !== "function") throw new TypeError("mutate is required");
  if (afterUpdate != null && typeof afterUpdate !== "function") {
    throw new TypeError("afterUpdate must be a function");
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const schedulerRows = await client.query(
      `SELECT value_json
       FROM crawler.settings
       WHERE setting_key='query_scheduler'
       FOR UPDATE`,
    );
    if (schedulerRows.rows.length !== 1) throw new Error("query_scheduler setting is missing");
    const current = normalize(schedulerRows.rows[0].value_json ?? {});
    const proposal = await mutate(current);
    if (proposal?.rejection?.allowed === false) {
      await client.query("COMMIT");
      return Object.freeze({
        updated: false,
        scheduler: current,
        admission: null,
        rejection: proposal.rejection,
      });
    }
    if (!proposal || !proposal.settings) throw new TypeError("Scheduler mutation is required");
    let admission = null;
    if (proposal.startsNewCycle === true) {
      admission = await sharedCrawlerSchedulerActivationAdmission(client);
      if (!admission.allowed) {
        await client.query("COMMIT");
        return Object.freeze({ updated: false, scheduler: current, admission });
      }
    }
    const scheduler = normalize({
      ...proposal.settings,
      updated_at: now,
      updated_by: "dashboard",
    });
    const updated = await client.query(
      `UPDATE crawler.settings
       SET value_json=$2::jsonb,updated_at=now()
       WHERE setting_key=$1
       RETURNING value_json`,
      ["query_scheduler", JSON.stringify(scheduler)],
    );
    if (updated.rowCount !== 1) throw new Error("query_scheduler setting is missing");
    const storedScheduler = normalize(updated.rows[0].value_json);
    if (afterUpdate) {
      await afterUpdate({ client, current, scheduler: storedScheduler });
    }
    await client.query("COMMIT");
    return Object.freeze({
      updated: true,
      scheduler: storedScheduler,
      admission,
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
