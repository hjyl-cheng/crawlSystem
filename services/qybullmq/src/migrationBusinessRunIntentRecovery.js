import { safeJobId } from "./queues.js";
import {
  MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
  assertMigrationRecoverySchedulerInactive,
} from "./migrationProxyControlRecovery.js";

export const MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID =
  "bug-048-migration-business-run-intent-compat-recovery-v1";

const REPRESENTED_JOB_STATES = new Set([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
]);

export const MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL = `
SELECT candidate.candidate_id::text,
       candidate.dispatch_batch_id,
       candidate.pipeline_cycle_id,
       candidate.channel_id,
       candidate.channel_url,
       candidate.status,
       candidate.snapshot_attempts,
       candidate.error_message,
       candidate.validation_started_at,
       candidate.validation_finished_at,
       candidate.source_json #> $2::text[] AS original_recovery_marker,
       candidate.source_json #> $3::text[] AS compatibility_recovery_marker,
       (
         SELECT count(*)::int
         FROM crawler.channel_runs run
         WHERE run.candidate_id=candidate.candidate_id
       ) AS run_count,
       binding.business_run_key AS binding_business_run_key,
       binding.business_run_id AS binding_business_run_id,
       binding.status AS binding_status,
       binding.terminal_reason AS binding_terminal_reason,
       binding.run_kind AS binding_run_kind,
       binding.channel_id AS binding_channel_id,
       binding.candidate_id::text AS binding_candidate_id,
       binding.intent_hash AS binding_intent_hash,
       binding.intent_json AS binding_intent_json
FROM crawler.channel_candidates candidate
LEFT JOIN crawler.business_run_bindings binding
  ON binding.business_run_key='full-candidate:' || candidate.candidate_id::text
WHERE candidate.dispatch_batch_id=$1
  AND ($4::bigint IS NULL OR candidate.candidate_id=$4::bigint)
  AND (
    (
      candidate.status='failed'
      AND candidate.error_message=
        'BUSINESS_RUN_KEY_CONFLICT: full-candidate:' || candidate.candidate_id::text
          || ': BUSINESS_RUN_KEY_CONFLICT'
      AND candidate.snapshot_attempts=0
      AND candidate.validation_started_at IS NULL
      AND candidate.source_json #> $2::text[] IS NOT NULL
      AND candidate.source_json #> $3::text[] IS NULL
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

function object(value, field) {
  if (value == null) return null;
  if (typeof value === "string") {
    const parsed = JSON.parse(value);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new TypeError(`${field} must be an object`);
    }
    return parsed;
  }
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${field} must be an object`);
  }
  return value;
}

function expectedBusinessRunKey(candidateId) {
  return `full-candidate:${candidateId}`;
}

function expectedCandidateError(candidateId) {
  return `BUSINESS_RUN_KEY_CONFLICT: ${expectedBusinessRunKey(candidateId)}: BUSINESS_RUN_KEY_CONFLICT`;
}

function expectedJobFailure(candidateId) {
  return `BUSINESS_RUN_KEY_CONFLICT: ${expectedBusinessRunKey(candidateId)}`;
}

function assertSame(actual, expected, field) {
  if (String(actual ?? "") !== String(expected ?? "")) {
    throw new Error(`Business Run compatibility recovery has conflicting ${field}`);
  }
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
    original_recovery_marker: object(
      value?.original_recovery_marker,
      "original_recovery_marker",
    ),
    compatibility_recovery_marker: object(
      value?.compatibility_recovery_marker,
      "compatibility_recovery_marker",
    ),
    binding_candidate_id: positiveInteger(value?.binding_candidate_id, "binding_candidate_id"),
    binding_intent_json: object(value?.binding_intent_json, "binding_intent_json"),
    job_id: safeJobId("channel-snapshot", dispatchBatchId, channelId),
  };
}

function assertOriginalRecoveryMarker(target) {
  const marker = target.original_recovery_marker;
  if (!marker) throw new Error("original BUG-043 recovery marker is missing");
  assertSame(
    marker.operation_id,
    MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
    "original operation_id",
  );
  for (const field of ["candidate_id", "dispatch_batch_id", "channel_id", "job_id"]) {
    assertSame(marker[field], target[field], `original recovery ${field}`);
  }
  return nonNegativeInteger(marker.attempts_made_before, "original attempts_made_before");
}

function assertOldBinding(target) {
  const key = expectedBusinessRunKey(target.candidate_id);
  assertSame(target.binding_business_run_key, key, "binding business_run_key");
  const runId = requiredText(target.binding_business_run_id, "binding_business_run_id");
  assertSame(target.binding_run_kind, "full", "binding run_kind");
  assertSame(target.binding_channel_id, target.channel_id, "binding channel_id");
  assertSame(target.binding_candidate_id, target.candidate_id, "binding candidate_id");
  requiredText(target.binding_intent_hash, "binding_intent_hash");
  if (!new Set(["reserved", "materialized", "terminal"]).has(target.binding_status)) {
    throw new Error("Business Run compatibility recovery has invalid Binding status");
  }
  if (target.binding_status === "terminal") {
    requiredText(target.binding_terminal_reason, "binding_terminal_reason");
  }
  const intent = object(target.binding_intent_json?.intent ?? {}, "binding intent");
  if (Object.prototype.hasOwnProperty.call(intent, "checkpoint_target_run_id")) {
    throw new Error("Business Run compatibility recovery requires an old-format Binding");
  }
  return { key, runId };
}

function assertJobIdentity(target, job) {
  if (!job) throw new Error(`Business Run compatibility recovery Job is missing: ${target.job_id}`);
  const binding = assertOldBinding(target);
  assertSame(job.id, target.job_id, "job_id");
  assertSame(job.name, "channel-snapshot", "job name");
  assertSame(job.data?.candidate_id, target.candidate_id, "candidate_id");
  assertSame(job.data?.dispatch_batch_id, target.dispatch_batch_id, "dispatch_batch_id");
  assertSame(job.data?.pipeline_cycle_id, target.pipeline_cycle_id, "pipeline_cycle_id");
  assertSame(job.data?.channel_id, target.channel_id, "channel_id");
  assertSame(job.data?.business_run_key, binding.key, "business_run_key");
  assertSame(job.data?.run_id, binding.runId, "run_id");
}

function assertCompatibilityMarker(target) {
  const marker = target.compatibility_recovery_marker;
  if (!marker) throw new Error("compatibility recovery marker is missing");
  assertSame(
    marker.operation_id,
    MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID,
    "compatibility operation_id",
  );
  for (const field of ["candidate_id", "dispatch_batch_id", "channel_id", "job_id"]) {
    assertSame(marker[field], target[field], `compatibility recovery ${field}`);
  }
  assertSame(
    marker.binding_business_run_key,
    target.binding_business_run_key,
    "compatibility binding_business_run_key",
  );
  assertSame(
    marker.binding_business_run_id,
    target.binding_business_run_id,
    "compatibility binding_business_run_id",
  );
  assertSame(
    marker.binding_intent_hash,
    target.binding_intent_hash,
    "compatibility binding_intent_hash",
  );
  return nonNegativeInteger(marker.attempts_made_before, "compatibility attempts_made_before");
}

export async function classifyMigrationBusinessRunIntentRecoveryTarget(targetValue, job) {
  const target = normalizedTarget(targetValue);
  assertOriginalRecoveryMarker(target);
  assertJobIdentity(target, job);
  const state = requiredText(await job.getState(), "BullMQ Job state");
  const attemptsMade = nonNegativeInteger(job.attemptsMade ?? 0, "attemptsMade");

  if (!target.compatibility_recovery_marker) {
    const originalBaseline = assertOriginalRecoveryMarker(target);
    if (target.status !== "failed"
        || target.error_message !== expectedCandidateError(target.candidate_id)
        || target.snapshot_attempts !== 0
        || target.validation_started_at != null
        || target.run_count !== 0
        || target.binding_status !== "reserved"
        || state !== "failed"
        || job.failedReason !== expectedJobFailure(target.candidate_id)
        || attemptsMade !== originalBaseline + 1) {
      throw new Error(
        `Candidate is not an original compatibility failure: ${target.candidate_id}`,
      );
    }
    return {
      ...target,
      action: "prepare_and_retry",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: attemptsMade,
    };
  }

  const baseline = assertCompatibilityMarker(target);
  if (attemptsMade < baseline) {
    throw new Error(`compatibility Job attempt history regressed: ${target.job_id}`);
  }
  if (target.binding_status === "terminal") {
    const returnvalue = job.returnvalue ?? {};
    if (state === "completed"
        && attemptsMade > baseline
        && target.status === "rejected"
        && target.run_count === 0
        && returnvalue.skipped === true
        && returnvalue.skip_reason === target.binding_terminal_reason) {
      return {
        ...target,
        action: "recovery_completed",
        job_state: state,
        attempts_made: attemptsMade,
        attempts_made_before: baseline,
      };
    }
    throw new Error(
      `terminal reason does not match a completed factual skip: ${target.candidate_id}`,
    );
  }
  if (state === "failed" && attemptsMade === baseline) {
    if (target.status !== "queued") {
      throw new Error(`prepared compatibility Candidate is not queued: ${target.candidate_id}`);
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
  throw new Error(`compatibility recovery has unsupported Job state ${state}: ${target.job_id}`);
}

function recoveryMarker(inspection, preparedAt) {
  return {
    operation_id: MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID,
    reason: "worker_added_checkpoint_null_to_frozen_candidate_intent",
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
    binding_business_run_key: inspection.binding_business_run_key,
    binding_business_run_id: inspection.binding_business_run_id,
    binding_intent_hash: inspection.binding_intent_hash,
  };
}

export async function prepareMigrationBusinessRunIntentRecoveryTargets(client, inspections, {
  batchId,
  now = new Date(),
} = {}) {
  const normalizedBatchId = requiredText(batchId, "batchId");
  const preparedAt = new Date(now);
  if (Number.isNaN(preparedAt.getTime())) throw new TypeError("now must be a valid date");
  const timestamp = preparedAt.toISOString();
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext('crawler-bug048-business-run-intent-recovery-v1'))",
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
         AND candidate.source_json#>>ARRAY[
           'controlled_recoveries',$7,'operation_id'
         ]=$7
         AND NOT (COALESCE(candidate.source_json->'controlled_recoveries','{}'::jsonb) ? $4)
         AND NOT EXISTS (
           SELECT 1 FROM crawler.channel_runs run
           WHERE run.candidate_id=candidate.candidate_id
         )
         AND EXISTS (
           SELECT 1 FROM crawler.business_run_bindings binding
           WHERE binding.business_run_key=$8
             AND binding.business_run_id=$9
             AND binding.intent_hash=$10
             AND binding.status='reserved'
             AND binding.run_kind='full'
             AND binding.channel_id=candidate.channel_id
             AND binding.candidate_id=candidate.candidate_id
         )
       RETURNING candidate.candidate_id`,
      [
        inspection.candidate_id,
        normalizedBatchId,
        expectedCandidateError(inspection.candidate_id),
        MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID,
        JSON.stringify(marker),
        timestamp,
        MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID,
        inspection.binding_business_run_key,
        inspection.binding_business_run_id,
        inspection.binding_intent_hash,
      ],
    );
    if (result.rowCount !== 1) {
      throw new Error(
        `Business Run compatibility Candidate changed before preparation: ${inspection.candidate_id}`,
      );
    }
    prepared.push(inspection.candidate_id);
  }
  return prepared;
}

export async function dispatchMigrationBusinessRunIntentRecoveryTarget(queue, inspection) {
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
      throw new TypeError(`compatibility failed Job cannot be retried: ${inspection.job_id}`);
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
  throw new Error(`compatibility dispatch found unsupported state ${state}: ${inspection.job_id}`);
}

export async function loadMigrationBusinessRunIntentRecoveryTargets(query, {
  batchId,
  candidateId = null,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const normalizedBatchId = requiredText(batchId, "batchId");
  const normalizedCandidateId = candidateId == null
    ? null
    : positiveInteger(candidateId, "candidateId");
  const result = await query(MIGRATION_BUSINESS_RUN_INTENT_TARGET_SQL, [
    normalizedBatchId,
    ["controlled_recoveries", MIGRATION_PROXY_CONTROL_RECOVERY_OPERATION_ID],
    ["controlled_recoveries", MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID],
    normalizedCandidateId,
  ]);
  const targets = (result.rows ?? []).map(normalizedTarget);
  const candidateIds = new Set(targets.map((target) => target.candidate_id));
  const jobIds = new Set(targets.map((target) => target.job_id));
  if (candidateIds.size !== targets.length || jobIds.size !== targets.length) {
    throw new Error("compatibility recovery selection contains duplicate Candidate or Job identities");
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
    inspections.push(await classifyMigrationBusinessRunIntentRecoveryTarget(
      target,
      await queue.getJob(target.job_id),
    ));
  }
  return inspections;
}

function sameTargets(left, right) {
  return left.length === right.length
    && left.every((target, index) => target.candidate_id === right[index]?.candidate_id
      && target.job_id === right[index]?.job_id
      && target.binding_business_run_id === right[index]?.binding_business_run_id
      && target.binding_intent_hash === right[index]?.binding_intent_hash);
}

export async function recoverMigrationBusinessRunIntentFailures({
  query,
  withTransaction,
  queue,
  batchId,
  candidateId = null,
  expectedCount = null,
  apply = false,
  dispatchLimit = 0,
  now = () => new Date(),
} = {}) {
  if (!queue || typeof queue.getJob !== "function") {
    throw new TypeError("the Channel Crawl BullMQ queue is required");
  }
  const targetOptions = { batchId, candidateId };
  const targets = await loadMigrationBusinessRunIntentRecoveryTargets(query, targetOptions);
  if (expectedCount !== null && targets.length !== positiveInteger(expectedCount, "expectedCount")) {
    throw new Error(
      `compatibility recovery target count changed: expected ${expectedCount}, got ${targets.length}`,
    );
  }
  let inspections = await inspectTargets(queue, targets);
  const summary = {
    apply: Boolean(apply),
    operation_id: MIGRATION_BUSINESS_RUN_INTENT_RECOVERY_OPERATION_ID,
    batch_id: requiredText(batchId, "batchId"),
    candidate_id: candidateId == null ? null : positiveInteger(candidateId, "candidateId"),
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
    const lockedTargets = await loadMigrationBusinessRunIntentRecoveryTargets(
      (sql, params) => client.query(sql, params),
      targetOptions,
    );
    if (!sameTargets(targets, lockedTargets)) {
      throw new Error("compatibility recovery target set changed before preparation");
    }
    const lockedInspections = await inspectTargets(queue, lockedTargets);
    return prepareMigrationBusinessRunIntentRecoveryTargets(client, lockedInspections, {
      batchId,
      now: preparedAt,
    });
  });
  summary.prepared_count = prepared.length;

  const preparedTargets = await loadMigrationBusinessRunIntentRecoveryTargets(query, targetOptions);
  if (!sameTargets(targets, preparedTargets)) {
    throw new Error("compatibility recovery target set changed after preparation");
  }
  inspections = await inspectTargets(queue, preparedTargets);
  summary.action_counts = actionCounts(inspections);
  const limit = nonNegativeInteger(dispatchLimit, "dispatchLimit");
  for (const inspection of inspections
    .filter(({ action }) => action === "retry_prepared")
    .slice(0, limit)) {
    const result = await dispatchMigrationBusinessRunIntentRecoveryTarget(queue, inspection);
    summary.dispatch_count += 1;
    summary.dispatch_results[result.action] = (summary.dispatch_results[result.action] ?? 0) + 1;
  }
  return summary;
}
