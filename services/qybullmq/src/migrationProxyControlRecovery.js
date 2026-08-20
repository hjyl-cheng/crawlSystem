import { safeJobId } from "./queues.js";

export const MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID =
  "bug-043-migration-proxy-control-recovery-v1";
export const MIGRATION_PROXY_CONTROL_FAILURE =
  "proxy control request failed: PROXY_CONTROL_REQUEST_FAILED";

const JOB_FAILURE = "proxy control request failed";
const ACTIVE_SCHEDULER_STATUSES = new Set(["running", "finishing", "repairing"]);
const REPRESENTED_JOB_STATES = new Set([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
]);
export const MIGRATION_PROXY_CONTROL_PRESSURE_STATES = Object.freeze([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
  "paused",
]);

export const MIGRATION_PROXY_CONTROL_TARGET_SQL = `
SELECT candidate.candidate_id::text,
       candidate.dispatch_batch_id,
       candidate.pipeline_cycle_id,
       candidate.channel_id,
       candidate.channel_url,
       candidate.priority,
       candidate.status,
       candidate.snapshot_attempts,
       candidate.error_message,
       candidate.validation_started_at,
       candidate.validation_finished_at,
       candidate.source_json #> $3::text[] AS recovery_marker,
       (
         SELECT count(*)::int
         FROM crawler.channel_runs run
         WHERE run.candidate_id=candidate.candidate_id
       ) AS run_count
FROM crawler.channel_candidates candidate
WHERE candidate.dispatch_batch_id=$1
  AND (
    (
      candidate.status='failed'
      AND candidate.error_message=$2
      AND candidate.snapshot_attempts=0
      AND candidate.validation_started_at IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM crawler.channel_runs run
        WHERE run.candidate_id=candidate.candidate_id
      )
    )
    OR candidate.source_json #> $3::text[] IS NOT NULL
  )
ORDER BY candidate.candidate_id`;

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function nonNegativeInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return parsed;
}

function positiveInteger(value, field) {
  const parsed = nonNegativeInteger(value, field);
  if (parsed === 0) throw new TypeError(`${field} must be positive`);
  return parsed;
}

export function migrationProxyControlDispatchCapacity(counts = {}, highWater = 20) {
  const limit = positiveInteger(highWater, "highWater");
  const pressure = MIGRATION_PROXY_CONTROL_PRESSURE_STATES.reduce(
    (total, state) => total + nonNegativeInteger(counts?.[state] ?? 0, `${state} count`),
    0,
  );
  return {
    pressure,
    dispatchLimit: Math.max(0, limit - pressure),
  };
}

function object(value) {
  if (value == null) return null;
  if (typeof value === "string") {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  }
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function normalizedTarget(value) {
  const candidateId = positiveInteger(value?.candidate_id, "candidate_id");
  const dispatchBatchId = requiredText(value?.dispatch_batch_id, "dispatch_batch_id");
  const channelId = requiredText(value?.channel_id, "channel_id");
  return {
    ...value,
    candidate_id: candidateId,
    dispatch_batch_id: dispatchBatchId,
    pipeline_cycle_id: requiredText(value?.pipeline_cycle_id, "pipeline_cycle_id"),
    channel_id: channelId,
    channel_url: requiredText(value?.channel_url, "channel_url"),
    status: requiredText(value?.status, "status"),
    snapshot_attempts: nonNegativeInteger(value?.snapshot_attempts, "snapshot_attempts"),
    run_count: nonNegativeInteger(value?.run_count, "run_count"),
    recovery_marker: object(value?.recovery_marker),
    job_id: safeJobId("channel-snapshot", dispatchBatchId, channelId),
  };
}

function assertSame(actual, expected, field) {
  if (String(actual ?? "") !== String(expected ?? "")) {
    throw new Error(`BUG-043 recovery Job has conflicting ${field}`);
  }
}

function assertJobIdentity(target, job) {
  if (!job) throw new Error(`BUG-043 recovery Job is missing: ${target.job_id}`);
  assertSame(job.id, target.job_id, "job_id");
  assertSame(job.name, "channel-snapshot", "name");
  assertSame(job.data?.candidate_id, target.candidate_id, "candidate_id");
  assertSame(job.data?.dispatch_batch_id, target.dispatch_batch_id, "dispatch_batch_id");
  assertSame(job.data?.pipeline_cycle_id, target.pipeline_cycle_id, "pipeline_cycle_id");
  assertSame(job.data?.channel_id, target.channel_id, "channel_id");
}

function assertRecoveryMarker(target, marker) {
  assertSame(
    marker.operation_id,
    MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
    "operation_id",
  );
  for (const field of ["candidate_id", "dispatch_batch_id", "channel_id", "job_id"]) {
    assertSame(marker[field], target[field], `recovery ${field}`);
  }
  return nonNegativeInteger(marker.attempts_made_before, "attempts_made_before");
}

export async function classifyMigrationProxyControlRecoveryTarget(targetValue, job) {
  const target = normalizedTarget(targetValue);
  assertJobIdentity(target, job);
  const state = requiredText(await job.getState(), "BullMQ Job state");
  const attemptsMade = nonNegativeInteger(job.attemptsMade ?? 0, "attemptsMade");

  if (!target.recovery_marker) {
    if (target.status !== "failed"
        || target.error_message !== MIGRATION_PROXY_CONTROL_FAILURE
        || target.snapshot_attempts !== 0
        || target.validation_started_at != null
        || target.run_count !== 0) {
      throw new Error(`Candidate is not an original BUG-043 failure: ${target.candidate_id}`);
    }
    if (state !== "failed" || job.failedReason !== JOB_FAILURE || attemptsMade === 0) {
      throw new Error(`BUG-043 original Job evidence is invalid: ${target.job_id}`);
    }
    return {
      ...target,
      action: "prepare_and_retry",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: attemptsMade,
    };
  }

  const baseline = assertRecoveryMarker(target, target.recovery_marker);
  if (attemptsMade < baseline) {
    throw new Error(`BUG-043 Job attempt history regressed: ${target.job_id}`);
  }
  if (state === "failed" && attemptsMade === baseline) {
    if (target.status !== "queued") {
      throw new Error(`BUG-043 prepared Candidate is not queued: ${target.candidate_id}`);
    }
    return {
      ...target,
      action: "retry_prepared",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: baseline,
    };
  }
  if (REPRESENTED_JOB_STATES.has(state)) {
    return {
      ...target,
      action: "recovery_in_progress",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: baseline,
    };
  }
  if (state === "completed" && attemptsMade > baseline) {
    return {
      ...target,
      action: "recovery_completed",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: baseline,
    };
  }
  if (state === "failed" && attemptsMade > baseline) {
    return {
      ...target,
      action: "recovery_failed",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: baseline,
    };
  }
  throw new Error(`BUG-043 recovered Job has unsupported state ${state}: ${target.job_id}`);
}

function recoveryMarker(inspection, preparedAt) {
  return {
    operation_id: MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
    reason: "rota_detached_active_task_blocked_begin_task",
    prepared_at: preparedAt,
    candidate_id: inspection.candidate_id,
    dispatch_batch_id: inspection.dispatch_batch_id,
    pipeline_cycle_id: inspection.pipeline_cycle_id,
    channel_id: inspection.channel_id,
    job_id: inspection.job_id,
    attempts_made_before: inspection.attempts_made_before,
    previous_status: inspection.status,
    previous_error_message: inspection.error_message,
    previous_validation_finished_at: inspection.validation_finished_at,
  };
}

export async function assertMigrationRecoverySchedulerInactive(client, batchId) {
  const result = await client.query(
    `SELECT value_json
     FROM crawler.settings
     WHERE setting_key='query_scheduler'
     LIMIT 1
     FOR UPDATE`,
  );
  if (result.rows.length !== 1) throw new Error("query_scheduler setting is missing");
  const scheduler = result.rows[0].value_json ?? {};
  const status = String(scheduler.status ?? "stopped");
  if (ACTIVE_SCHEDULER_STATUSES.has(status)) {
    throw new Error(
      `BUG-043 recovery requires an inactive Scheduler, got ${status}: ${scheduler.pipeline_cycle_id ?? batchId}`,
    );
  }
}

export async function prepareMigrationProxyControlRecoveryTargets(client, inspections, {
  batchId,
  now = new Date(),
} = {}) {
  const normalizedBatchId = requiredText(batchId, "batchId");
  const preparedAt = new Date(now);
  if (Number.isNaN(preparedAt.getTime())) throw new TypeError("now must be a valid date");
  const timestamp = preparedAt.toISOString();
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext('crawler-bug043-migration-recovery-v1'))",
  );
  await assertMigrationRecoverySchedulerInactive(client, normalizedBatchId);

  const prepared = [];
  for (const inspection of inspections) {
    if (inspection.action !== "prepare_and_retry") continue;
    const marker = recoveryMarker(inspection, timestamp);
    const result = await client.query(
      `UPDATE crawler.channel_candidates candidate
       SET status='queued',error_message=NULL,next_retry_at=NULL,
           validation_started_at=NULL,validation_finished_at=NULL,
           source_json=jsonb_set(
             COALESCE(candidate.source_json,'{}'::jsonb),
             '{controlled_recoveries}',
             COALESCE(candidate.source_json->'controlled_recoveries','{}'::jsonb)
               || jsonb_build_object($4,$5::jsonb),
             true
           ),
           updated_at=$6::timestamptz
       WHERE candidate.candidate_id=$1
         AND candidate.dispatch_batch_id=$2
         AND candidate.status='failed'
         AND candidate.error_message=$3
         AND candidate.snapshot_attempts=0
         AND candidate.validation_started_at IS NULL
         AND NOT EXISTS (
           SELECT 1 FROM crawler.channel_runs run
           WHERE run.candidate_id=candidate.candidate_id
         )
         AND NOT (COALESCE(candidate.source_json->'controlled_recoveries','{}'::jsonb) ? $4)
       RETURNING candidate.candidate_id`,
      [
        inspection.candidate_id,
        normalizedBatchId,
        MIGRATION_PROXY_CONTROL_FAILURE,
        MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
        JSON.stringify(marker),
        timestamp,
      ],
    );
    if (result.rowCount !== 1) {
      throw new Error(`BUG-043 Candidate changed before preparation: ${inspection.candidate_id}`);
    }
    prepared.push(inspection.candidate_id);
  }
  return prepared;
}

export async function dispatchMigrationProxyControlRecoveryTarget(queue, inspection) {
  const job = await queue.getJob(inspection.job_id);
  assertJobIdentity(inspection, job);
  const state = requiredText(await job.getState(), "BullMQ Job state");
  const attemptsMade = nonNegativeInteger(job.attemptsMade ?? 0, "attemptsMade");
  const baseline = nonNegativeInteger(
    inspection.attempts_made_before,
    "attempts_made_before",
  );
  if (state === "failed" && attemptsMade === baseline) {
    if (typeof job.retry !== "function") {
      throw new TypeError(`BUG-043 failed Job cannot be retried: ${inspection.job_id}`);
    }
    await job.retry("failed");
    return { action: "retried_failed", job_id: inspection.job_id };
  }
  if (REPRESENTED_JOB_STATES.has(state)) {
    return { action: "already_represented", job_id: inspection.job_id, state };
  }
  if (state === "completed" && attemptsMade > baseline) {
    return { action: "already_completed", job_id: inspection.job_id };
  }
  if (state === "failed" && attemptsMade > baseline) {
    return { action: "recovery_failed", job_id: inspection.job_id };
  }
  throw new Error(`BUG-043 dispatch found unsupported state ${state}: ${inspection.job_id}`);
}

export async function loadMigrationProxyControlRecoveryTargets(query, { batchId } = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const normalizedBatchId = requiredText(batchId, "batchId");
  const result = await query(MIGRATION_PROXY_CONTROL_TARGET_SQL, [
    normalizedBatchId,
    MIGRATION_PROXY_CONTROL_FAILURE,
    ["controlled_recoveries", MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID],
  ]);
  const targets = (result.rows ?? []).map(normalizedTarget);
  const candidateIds = new Set(targets.map((target) => target.candidate_id));
  const jobIds = new Set(targets.map((target) => target.job_id));
  if (candidateIds.size !== targets.length || jobIds.size !== targets.length) {
    throw new Error("BUG-043 recovery selection contains duplicate Candidate or Job identities");
  }
  return targets;
}

function actionCounts(inspections) {
  return Object.fromEntries([...inspections.reduce((counts, inspection) => {
    counts.set(inspection.action, (counts.get(inspection.action) ?? 0) + 1);
    return counts;
  }, new Map()).entries()].sort(([left], [right]) => left.localeCompare(right)));
}

async function inspectTargets(queue, targets) {
  const inspections = [];
  for (const target of targets) {
    inspections.push(await classifyMigrationProxyControlRecoveryTarget(
      target,
      await queue.getJob(target.job_id),
    ));
  }
  return inspections;
}

function sameTargets(left, right) {
  return left.length === right.length
    && left.every((target, index) => target.candidate_id === right[index]?.candidate_id
      && target.job_id === right[index]?.job_id);
}

export async function recoverMigrationProxyControlFailures({
  query,
  withTransaction,
  queue,
  batchId,
  expectedCount = null,
  apply = false,
  dispatchLimit = 0,
  now = () => new Date(),
} = {}) {
  if (!queue || typeof queue.getJob !== "function") {
    throw new TypeError("the Channel Crawl BullMQ queue is required");
  }
  const targets = await loadMigrationProxyControlRecoveryTargets(query, { batchId });
  if (expectedCount !== null && targets.length !== positiveInteger(expectedCount, "expectedCount")) {
    throw new Error(
      `BUG-043 target count changed: expected ${expectedCount}, got ${targets.length}`,
    );
  }
  let inspections = await inspectTargets(queue, targets);
  const summary = {
    apply: Boolean(apply),
    operation_id: MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
    batch_id: requiredText(batchId, "batchId"),
    target_count: targets.length,
    action_counts: actionCounts(inspections),
    prepared_count: 0,
    dispatch_count: 0,
    dispatch_results: {},
  };
  if (!apply) return summary;
  if (expectedCount === null) throw new Error("expectedCount is required when apply=true");
  if (typeof withTransaction !== "function") {
    throw new TypeError("withTransaction is required when apply=true");
  }

  const preparedAt = now();
  const prepared = await withTransaction(async (client) => {
    const lockedTargets = await loadMigrationProxyControlRecoveryTargets(
      (sql, params) => client.query(sql, params),
      { batchId },
    );
    if (!sameTargets(targets, lockedTargets)) {
      throw new Error("BUG-043 recovery target set changed before preparation");
    }
    const lockedInspections = await inspectTargets(queue, lockedTargets);
    return prepareMigrationProxyControlRecoveryTargets(client, lockedInspections, {
      batchId,
      now: preparedAt,
    });
  });
  summary.prepared_count = prepared.length;

  const preparedTargets = await loadMigrationProxyControlRecoveryTargets(query, { batchId });
  if (!sameTargets(targets, preparedTargets)) {
    throw new Error("BUG-043 recovery target set changed after preparation");
  }
  inspections = await inspectTargets(queue, preparedTargets);
  summary.action_counts = actionCounts(inspections);
  const limit = nonNegativeInteger(dispatchLimit, "dispatchLimit");
  for (const inspection of inspections.filter(({ action }) => action === "retry_prepared").slice(0, limit)) {
    const result = await dispatchMigrationProxyControlRecoveryTarget(queue, inspection);
    summary.dispatch_count += 1;
    summary.dispatch_results[result.action] = (summary.dispatch_results[result.action] ?? 0) + 1;
  }
  return summary;
}
