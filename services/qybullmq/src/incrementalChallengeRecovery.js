export const INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID =
  "bug-024-incremental-challenge-recovery-v1";

export const INCREMENTAL_CHALLENGE_RECOVERY_CHANNEL_IDS = Object.freeze([
  "UC2bZgihqibFD_vhaYEXQZFg",
  "UC73p5mW8WiD-k0P9iD5WBJA",
  "UCJ8T7b2Q_jpXT6qgc8p-fFA",
  "UCaa7wRZqVT8rrv69BZB079g",
  "UCzblNrdhQdEbxO9QJ-FpvUQ",
]);

const RECOVERY_REASON = "incremental_video_challenge_was_committed_as_partial";
const REPRESENTED_JOB_STATES = new Set([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
]);

export const INCREMENTAL_CHALLENGE_TARGET_SQL = `
WITH qualifying AS (
  SELECT DISTINCT ON (run.run_id)
         run.run_id,
         run.channel_id,
         run.plan_id::text AS plan_id,
         run.plan_day::text AS plan_day,
         run.status AS run_status,
         run.detail_status,
         run.publication_finalized_at,
         run.result_json #>> '{domains,video,status}' AS video_status,
         run.result_json #> '{domains,video}' AS previous_video_domain,
         run.result_json ->> 'job_id' AS job_id,
         run.result_json #> $3::text[] AS recovery_marker,
         observation.observation_id AS source_observation_id,
         observation.kind_sequence AS source_kind_sequence,
         observation.outcome_reason_code AS source_reason_code,
         COALESCE(
           (observation.result_summary_json #>> '{discovery,detail_failure_count}')::int,
           0
         ) AS detail_failure_count,
         evidence.challenge_decision_count,
         evidence.challenge_attempt_count,
         evidence.network_identity_count,
         evidence.route_generation_count
    FROM crawler.channel_runs AS run
    JOIN crawler.crawl_observations AS observation
      ON observation.run_id=run.run_id
    JOIN LATERAL (
      SELECT count(*)::int AS challenge_decision_count,
             count(DISTINCT attempt.attempt_id)::int AS challenge_attempt_count,
             count(DISTINCT attempt.network_identity_key)::int AS network_identity_count,
             count(DISTINCT attempt.route_generation)::int AS route_generation_count
        FROM crawler.channel_execution_attempts AS attempt
       CROSS JOIN LATERAL jsonb_array_elements(
         COALESCE(attempt.result_json->'failure_decisions','[]'::jsonb)
       ) AS failure_decision
       WHERE attempt.run_id=run.run_id
         AND failure_decision->>'kind'='youtube_challenge'
    ) AS evidence
      ON evidence.challenge_decision_count > 0
   WHERE run.plan_day=$1::date
     AND run.channel_id=ANY($2::text[])
     AND observation.observation_kind='video'
     AND observation.outcome='partial'
     AND observation.outcome_reason_code LIKE 'video_cycle_partial_%'
     AND COALESCE(
           (observation.result_summary_json #>> '{discovery,detail_failure_count}')::int,
           0
         ) > 0
   ORDER BY run.run_id,observation.kind_sequence DESC
)
SELECT *
  FROM qualifying
 ORDER BY channel_id,run_id`;

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

function object(value) {
  if (value == null) return null;
  if (typeof value === "string") {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  }
  return value && typeof value === "object" && !Array.isArray(value) ? value : null;
}

function normalizedPlanDay(value) {
  const output = requiredText(value, "planDay");
  if (!/^\d{4}-\d{2}-\d{2}$/.test(output)) {
    throw new TypeError("planDay must use YYYY-MM-DD");
  }
  return output;
}

function assertExpectedChannel(value) {
  const channelId = requiredText(value, "channel_id");
  if (!INCREMENTAL_CHALLENGE_RECOVERY_CHANNEL_IDS.includes(channelId)) {
    throw new Error(`unexpected Incremental challenge recovery channel: ${channelId}`);
  }
  return channelId;
}

function normalizedTarget(value) {
  const challengeDecisionCount = positiveInteger(
    value?.challenge_decision_count,
    "challenge_decision_count",
  );
  const challengeAttemptCount = positiveInteger(
    value?.challenge_attempt_count,
    "challenge_attempt_count",
  );
  return {
    ...value,
    run_id: requiredText(value?.run_id, "run_id"),
    channel_id: assertExpectedChannel(value?.channel_id),
    plan_id: requiredText(value?.plan_id, "plan_id"),
    plan_day: normalizedPlanDay(value?.plan_day),
    run_status: requiredText(value?.run_status, "run_status"),
    detail_status: requiredText(value?.detail_status, "detail_status"),
    video_status: requiredText(value?.video_status, "video_status"),
    job_id: requiredText(value?.job_id, "job_id"),
    source_observation_id: requiredText(
      value?.source_observation_id,
      "source_observation_id",
    ),
    source_kind_sequence: positiveInteger(
      value?.source_kind_sequence,
      "source_kind_sequence",
    ),
    source_reason_code: requiredText(value?.source_reason_code, "source_reason_code"),
    detail_failure_count: positiveInteger(
      value?.detail_failure_count,
      "detail_failure_count",
    ),
    challenge_decision_count: challengeDecisionCount,
    challenge_attempt_count: challengeAttemptCount,
    network_identity_count: positiveInteger(
      value?.network_identity_count,
      "network_identity_count",
    ),
    route_generation_count: positiveInteger(
      value?.route_generation_count,
      "route_generation_count",
    ),
    previous_video_domain: object(value?.previous_video_domain)
      ?? { status: value?.video_status },
    recovery_marker: object(value?.recovery_marker),
  };
}

function assertSame(actual, expected, field) {
  if (String(actual ?? "") !== String(expected ?? "")) {
    throw new Error(`Incremental challenge recovery Job has conflicting ${field}`);
  }
}

function assertJobIdentity(target, job) {
  if (!job) throw new Error(`Incremental challenge recovery Job is missing: ${target.job_id}`);
  assertSame(job.id, target.job_id, "job_id");
  assertSame(job.name, "channel.incremental.plan", "name");
  assertSame(job.data?.job_id, target.job_id, "payload job_id");
  assertSame(job.data?.plan_id, target.plan_id, "plan_id");
  assertSame(job.data?.channel_id, target.channel_id, "channel_id");
  if (job.data?.task_mask?.video !== true) {
    throw new Error("Incremental challenge recovery Job is not a Video Plan");
  }
}

function assertRecoveryMarker(target, marker) {
  assertSame(
    marker.operation_id,
    INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
    "recovery operation_id",
  );
  for (const field of ["run_id", "channel_id", "plan_id", "job_id"]) {
    assertSame(marker[field], target[field], `recovery ${field}`);
  }
  return nonNegativeInteger(marker.attempts_made_before, "recovery attempts_made_before");
}

export async function classifyIncrementalChallengeRecoveryTarget(targetValue, job) {
  const target = normalizedTarget(targetValue);
  assertJobIdentity(target, job);
  const state = requiredText(await job.getState(), "BullMQ Job state");
  const attemptsMade = nonNegativeInteger(job.attemptsMade ?? 0, "attemptsMade");
  const marker = target.recovery_marker;

  if (!marker) {
    if (target.publication_finalized_at != null) {
      throw new Error(`Incremental challenge Run is already finalized: ${target.run_id}`);
    }
    if (target.run_status !== "done"
        || target.detail_status !== "done"
        || target.video_status !== "partial") {
      throw new Error(`Incremental challenge Run is not an original terminal Partial: ${target.run_id}`);
    }
    if (state !== "completed") {
      throw new Error(`Incremental challenge original Job must be completed, got ${state}: ${target.job_id}`);
    }
    return {
      ...target,
      action: "prepare_and_retry",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: attemptsMade,
    };
  }

  const attemptsMadeBefore = assertRecoveryMarker(target, marker);
  if (attemptsMade < attemptsMadeBefore) {
    throw new Error(`Incremental challenge attempt history regressed: ${target.job_id}`);
  }
  if (state === "completed") {
    if (attemptsMade === attemptsMadeBefore) {
      if (target.run_status !== "queued"
          || target.detail_status !== "pending"
          || target.video_status !== "pending") {
        throw new Error(`Incremental challenge prepared Run has conflicting state: ${target.run_id}`);
      }
      return {
        ...target,
        action: "retry_prepared",
        job_state: state,
        attempts_made: attemptsMade,
        attempts_made_before: attemptsMadeBefore,
      };
    }
    return {
      ...target,
      action: "recovery_completed",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: attemptsMadeBefore,
    };
  }
  if (REPRESENTED_JOB_STATES.has(state)) {
    return {
      ...target,
      action: "recovery_in_progress",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: attemptsMadeBefore,
    };
  }
  if (state === "failed" && attemptsMade > attemptsMadeBefore) {
    return {
      ...target,
      action: "recovery_failed",
      job_state: state,
      attempts_made: attemptsMade,
      attempts_made_before: attemptsMadeBefore,
    };
  }
  throw new Error(`Incremental challenge recovered Job has unsupported state ${state}: ${target.job_id}`);
}

function recoveryMarker(inspection, preparedAt) {
  return {
    operation_id: INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
    reason: RECOVERY_REASON,
    prepared_at: preparedAt,
    run_id: inspection.run_id,
    channel_id: inspection.channel_id,
    plan_id: inspection.plan_id,
    job_id: inspection.job_id,
    attempts_made_before: inspection.attempts_made_before,
    source_observation_id: inspection.source_observation_id,
    source_kind_sequence: inspection.source_kind_sequence,
    source_reason_code: inspection.source_reason_code,
    challenge_decision_count: inspection.challenge_decision_count,
    challenge_attempt_count: inspection.challenge_attempt_count,
    previous_run_status: inspection.run_status,
    previous_detail_status: inspection.detail_status,
    previous_video_domain: inspection.previous_video_domain,
  };
}

export async function prepareIncrementalChallengeRecoveryTargets(client, inspections, {
  now = new Date(),
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const preparedAt = new Date(now);
  if (Number.isNaN(preparedAt.getTime())) throw new TypeError("now must be a valid date");
  const timestamp = preparedAt.toISOString();
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext('crawler-incremental-challenge-recovery-v1'))",
  );

  const prepared = [];
  for (const inspection of inspections) {
    if (inspection.action !== "prepare_and_retry") continue;
    const pendingDomain = {
      status: "pending",
      recovery_operation_id: INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
      recovery_reason: RECOVERY_REASON,
      prepared_at: timestamp,
    };
    const marker = recoveryMarker(inspection, timestamp);
    const result = await client.query(
      `UPDATE crawler.channel_runs AS run
          SET status='queued',
              detail_status='pending',
              error_message=NULL,
              finished_at=NULL,
              result_json=jsonb_set(
                jsonb_set(
                  COALESCE(run.result_json,'{}'::jsonb),
                  '{domains,video}',
                  $7::jsonb,
                  true
                ),
                '{controlled_recoveries}',
                COALESCE(run.result_json->'controlled_recoveries','{}'::jsonb)
                  || jsonb_build_object($5,$8::jsonb),
                true
              ),
              updated_at=$6::timestamptz
        WHERE run.run_id=$1
          AND run.plan_id=$2::uuid
          AND run.channel_id=$3
          AND run.result_json->>'job_id'=$4
          AND run.status='done'
          AND run.detail_status='done'
          AND run.publication_finalized_at IS NULL
          AND run.result_json #>> '{domains,video,status}'='partial'
          AND NOT (COALESCE(run.result_json->'controlled_recoveries','{}'::jsonb) ? $5)
      RETURNING run.run_id`,
      [
        inspection.run_id,
        inspection.plan_id,
        inspection.channel_id,
        inspection.job_id,
        INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
        timestamp,
        JSON.stringify(pendingDomain),
        JSON.stringify(marker),
      ],
    );
    if (result.rowCount !== 1) {
      throw new Error(`Incremental challenge Run changed before preparation: ${inspection.run_id}`);
    }
    prepared.push(inspection.run_id);
  }
  return prepared;
}

export async function dispatchIncrementalChallengeRecoveryTarget(queue, inspection) {
  if (!queue || typeof queue.getJob !== "function") {
    throw new TypeError("the Incremental BullMQ queue is required");
  }
  const job = await queue.getJob(inspection.job_id);
  assertJobIdentity(inspection, job);
  const state = requiredText(await job.getState(), "BullMQ Job state");
  const attemptsMade = nonNegativeInteger(job.attemptsMade ?? 0, "attemptsMade");
  const baseline = nonNegativeInteger(
    inspection.attempts_made_before,
    "attempts_made_before",
  );
  if (attemptsMade < baseline) {
    throw new Error(`Incremental challenge attempt history regressed: ${inspection.job_id}`);
  }
  if (state === "completed" && attemptsMade === baseline) {
    if (typeof job.retry !== "function") {
      throw new TypeError(`completed Incremental challenge Job cannot be retried: ${inspection.job_id}`);
    }
    await job.retry("completed");
    return { action: "retried_completed", job_id: inspection.job_id, attempts_made: attemptsMade };
  }
  if (state === "completed" && attemptsMade > baseline) {
    return { action: "already_consumed", job_id: inspection.job_id, attempts_made: attemptsMade };
  }
  if (REPRESENTED_JOB_STATES.has(state)) {
    return { action: "already_represented", job_id: inspection.job_id, state, attempts_made: attemptsMade };
  }
  if (state === "failed" && attemptsMade > baseline) {
    return { action: "recovery_failed", job_id: inspection.job_id, state, attempts_made: attemptsMade };
  }
  throw new Error(`Incremental challenge dispatch found unsupported state ${state}: ${inspection.job_id}`);
}

export async function loadIncrementalChallengeRecoveryTargets(query, {
  planDay,
  operationId = INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  if (operationId !== INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID) {
    throw new Error(`unsupported Incremental challenge recovery operation: ${operationId}`);
  }
  const result = await query(INCREMENTAL_CHALLENGE_TARGET_SQL, [
    normalizedPlanDay(planDay),
    [...INCREMENTAL_CHALLENGE_RECOVERY_CHANNEL_IDS],
    ["controlled_recoveries", operationId],
  ]);
  const targets = (result.rows ?? []).map(normalizedTarget);
  const runIds = new Set(targets.map((target) => target.run_id));
  const jobIds = new Set(targets.map((target) => target.job_id));
  const channelIds = new Set(targets.map((target) => target.channel_id));
  if (runIds.size !== targets.length
      || jobIds.size !== targets.length
      || channelIds.size !== targets.length) {
    throw new Error("Incremental challenge recovery selection contains duplicate identities");
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
    const job = await queue.getJob(target.job_id);
    inspections.push(await classifyIncrementalChallengeRecoveryTarget(target, job));
  }
  return inspections;
}

function sameTargetSet(left, right) {
  return left.length === right.length
    && left.every((target, index) => target.run_id === right[index]?.run_id
      && target.job_id === right[index]?.job_id
      && target.source_observation_id === right[index]?.source_observation_id);
}

export async function recoverIncrementalChallengeRuns({
  query,
  withTransaction,
  queue,
  planDay,
  expectedCount = null,
  apply = false,
  now = () => new Date(),
} = {}) {
  if (!queue || typeof queue.getJob !== "function") {
    throw new TypeError("the Incremental BullMQ queue is required");
  }
  const targets = await loadIncrementalChallengeRecoveryTargets(query, { planDay });
  if (expectedCount !== null) {
    const expected = positiveInteger(expectedCount, "expectedCount");
    if (targets.length !== expected) {
      throw new Error(`Incremental challenge target count changed: expected ${expected}, got ${targets.length}`);
    }
  }
  const inspections = await inspectTargets(queue, targets);
  const summary = {
    apply: Boolean(apply),
    operation_id: INCREMENTAL_CHALLENGE_RECOVERY_OPERATION_ID,
    plan_day: normalizedPlanDay(planDay),
    target_count: targets.length,
    action_counts: actionCounts(inspections),
    targets: inspections.map((inspection) => ({
      channel_id: inspection.channel_id,
      run_id: inspection.run_id,
      job_id: inspection.job_id,
      action: inspection.action,
      job_state: inspection.job_state,
      attempts_made: inspection.attempts_made,
      challenge_decision_count: inspection.challenge_decision_count,
      challenge_attempt_count: inspection.challenge_attempt_count,
    })),
    prepared_count: 0,
    dispatch_counts: {},
    dispatch_errors: [],
  };
  if (!apply) return summary;
  if (expectedCount === null) {
    throw new Error("expectedCount is required when apply=true");
  }
  if (typeof withTransaction !== "function") {
    throw new TypeError("withTransaction is required when apply=true");
  }

  const preparedIds = await withTransaction(async (client) => {
    const lockedTargets = await loadIncrementalChallengeRecoveryTargets(
      client.query.bind(client),
      { planDay },
    );
    if (!sameTargetSet(targets, lockedTargets)) {
      throw new Error("Incremental challenge target set changed during recovery preparation");
    }
    return prepareIncrementalChallengeRecoveryTargets(client, inspections, { now: now() });
  });
  summary.prepared_count = preparedIds.length;

  const dispatchResults = [];
  for (const inspection of inspections) {
    try {
      dispatchResults.push(await dispatchIncrementalChallengeRecoveryTarget(queue, inspection));
    } catch (error) {
      summary.dispatch_errors.push({
        run_id: inspection.run_id,
        job_id: inspection.job_id,
        error: String(error?.message ?? error),
      });
    }
  }
  summary.dispatch_counts = Object.fromEntries([...dispatchResults.reduce((counts, result) => {
    counts.set(result.action, (counts.get(result.action) ?? 0) + 1);
    return counts;
  }, new Map()).entries()].sort(([left], [right]) => left.localeCompare(right)));
  return summary;
}
