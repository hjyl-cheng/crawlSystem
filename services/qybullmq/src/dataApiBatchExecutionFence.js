import { reconcileRunDetailStatus } from "./runDetailStatus.js";
import { safeJobId } from "./queues.js";

function text(value) {
  return String(value ?? "").trim() || null;
}

function positiveInteger(value, field) {
  const normalized = Number(value);
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return normalized;
}

function positiveIntegers(values, field) {
  if (!Array.isArray(values)) throw new TypeError(`${field} must be an array`);
  return [...new Set(values.map((value) => positiveInteger(value, field)))]
    .sort((left, right) => left - right);
}

function texts(values, field) {
  if (!Array.isArray(values)) throw new TypeError(`${field} must be an array`);
  const normalized = [...new Set(values.map(text))].filter(Boolean).sort();
  if (normalized.length !== values.length) {
    throw new TypeError(`${field} must contain unique non-empty values`);
  }
  return normalized;
}

function sameValues(left, right) {
  return left.length === right.length
    && left.every((value, index) => String(value) === String(right[index]));
}

function jsonRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function storedReplayIdentity(jobData) {
  const replay = jsonRecord(jobData?.stored_evidence_replay);
  if (Object.keys(replay).length === 0) return null;
  const runId = text(replay.run_id);
  if (!runId) throw new TypeError("Data API stored replay run_id is required");
  return Object.freeze({
    operationId: text(replay.operation_id),
    runId,
    taskIds: positiveIntegers(replay.task_ids, "stored_evidence_replay.task_ids"),
  });
}

export function dataApiBatchExecutionFence(job) {
  const batchId = text(job?.data?.batch_id) ?? text(job?.id);
  const jobId = text(job?.id);
  const pipelineCycleId = text(job?.data?.pipeline_cycle_id);
  if (!batchId || !jobId) throw new TypeError("Data API Batch Job identity is incomplete");
  if (job?.name !== "youtube-data-api-batch") {
    throw new TypeError("Data API Batch Job name is invalid");
  }
  const migrationSystemRetryIds = positiveIntegers(
    job?.data?.migration_system_retry_ids ?? [],
    "migration_system_retry_ids",
  );
  const recoveryRunIds = texts(job?.data?.recovery_run_ids ?? [], "recovery_run_ids");
  const recovery = migrationSystemRetryIds.length > 0 || recoveryRunIds.length > 0;
  if (recovery && (
    migrationSystemRetryIds.length === 0
    || recoveryRunIds.length === 0
    || !pipelineCycleId
  )) {
    throw new TypeError("Migration recovery Data API Job identity is incomplete");
  }
  const fence = {
    batchId,
    jobId,
    jobAttempt: positiveInteger(job?.attemptsStarted, "attemptsStarted"),
    taskIds: positiveIntegers(job?.data?.task_ids ?? [], "task_ids"),
    videoIds: texts(job?.data?.video_ids ?? [], "video_ids"),
    pipelineCycleId,
    migrationSystemRetryIds,
    recoveryRunIds,
    recovery,
    storedReplay: storedReplayIdentity(job?.data),
  };
  if (fence.taskIds.length === 0) throw new TypeError("Data API Batch Job task_ids are required");
  if (!fence.storedReplay && fence.videoIds.length === 0) {
    throw new TypeError("Data API Batch Job video_ids are required");
  }
  return Object.freeze(fence);
}

function batchIdentityMatches(row, fence) {
  if (!row) return false;
  const taskIds = positiveIntegers(row.task_ids ?? [], "batch.task_ids");
  const videoIds = texts(row.video_ids ?? [], "batch.video_ids");
  if (!sameValues(taskIds, fence.taskIds) || !sameValues(videoIds, fence.videoIds)) return false;
  const result = jsonRecord(row.result_json);
  if (fence.recovery) {
    const intent = jsonRecord(result.dispatch_intent);
    return text(intent.pipeline_cycle_id) === fence.pipelineCycleId
      && sameValues(
        positiveIntegers(intent.migration_system_retry_ids ?? [], "dispatch_intent retry ids"),
        fence.migrationSystemRetryIds,
      )
      && sameValues(
        texts(intent.recovery_run_ids ?? [], "dispatch_intent recovery run ids"),
        fence.recoveryRunIds,
      );
  }
  if (fence.storedReplay) {
    const marker = jsonRecord(result.stored_evidence_recovery);
    return text(marker.operation_id) === fence.storedReplay.operationId
      && text(marker.run_id) === fence.storedReplay.runId
      && text(marker.batch_id) === fence.batchId
      && sameValues(
        positiveIntegers(marker.task_ids ?? [], "stored replay batch task ids"),
        fence.taskIds,
      );
  }
  const intent = jsonRecord(result.dispatch_intent);
  const retryIds = positiveIntegers(
    intent.migration_system_retry_ids ?? [],
    "dispatch_intent retry ids",
  );
  if (retryIds.length > 0) return false;
  const intentPipelineCycleId = text(intent.pipeline_cycle_id);
  return intentPipelineCycleId == null
    ? true
    : intentPipelineCycleId === fence.pipelineCycleId;
}

async function loadLockedBatch(client, fence) {
  const rows = await client.query(
    `SELECT batch_id,status,task_ids,video_ids,result_json,
            active_job_id,active_job_attempt
     FROM crawler.youtube_api_batches
     WHERE batch_id=$1
     FOR UPDATE`,
    [fence.batchId],
  );
  return rows.rows[0] ?? null;
}

function claimableBatch(row, fence) {
  if (!batchIdentityMatches(row, fence)) return false;
  if (!["queued", "running", "failed"].includes(String(row.status ?? ""))) return false;
  if (row.active_job_attempt == null) return row.active_job_id == null;
  const activeAttempt = Number(row.active_job_attempt);
  return activeAttempt < fence.jobAttempt
    || (activeAttempt === fence.jobAttempt && text(row.active_job_id) === fence.jobId);
}

function committedBatch(row, fence) {
  return batchIdentityMatches(row, fence)
    && row.status === "running"
    && text(row.active_job_id) === fence.jobId
    && Number(row.active_job_attempt) === fence.jobAttempt;
}

function durableDispatchIntent(fence) {
  if (!fence.pipelineCycleId || (fence.storedReplay && !fence.recovery)) return null;
  return {
    pipeline_cycle_id: fence.pipelineCycleId,
    migration_system_retry_ids: fence.migrationSystemRetryIds,
    recovery_run_ids: fence.recoveryRunIds,
  };
}

async function lockRecoveryScope(client, fence, { requireActiveRetry = true } = {}) {
  if (!fence.recovery) {
    return {
      recovery: false,
      authorized_candidate_ids_by_task: null,
      recovery_run_ids: [],
      migration_system_retry_ids: [],
    };
  }
  const retryRows = await client.query(
    `SELECT retry.system_retry_id,retry.recovery_run_id
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.channel_candidates channel_candidate
       ON channel_candidate.candidate_id=retry.candidate_id
     JOIN crawler.channels channel
       ON channel.channel_id=channel_candidate.channel_id
     JOIN crawler.channel_runs run
       ON run.run_id=retry.recovery_run_id
     WHERE retry.system_retry_id=ANY($1::bigint[])
       AND (NOT $4::boolean OR retry.status IN ('retrying','dispatched'))
       AND retry.failed_dispatch_batch_id=$2
       AND retry.recovery_run_id=ANY($3::text[])
       AND channel_candidate.dispatch_batch_id=retry.failed_dispatch_batch_id
       AND channel_candidate.snapshot_dispatch_generation=COALESCE(
             retry.retry_dispatch_generation,
             retry.failed_dispatch_generation
           )
       AND channel_candidate.status='accepted'
       AND channel_candidate.snapshot_active_job_id IS NULL
       AND channel_candidate.snapshot_active_job_attempt IS NULL
       AND channel.latest_run_id=retry.recovery_run_id
       AND run.channel_id=channel.channel_id
       AND run.candidate_id=channel_candidate.candidate_id
       AND COALESCE(
             run.result_json->>'dispatch_batch_id',
             run.result_json->>'pipeline_cycle_id'
           )=retry.failed_dispatch_batch_id
     ORDER BY retry.system_retry_id
     FOR UPDATE OF retry,channel_candidate,channel,run`,
    [
      fence.migrationSystemRetryIds,
      fence.pipelineCycleId,
      fence.recoveryRunIds,
      requireActiveRetry,
    ],
  );
  const retryIds = retryRows.rows.map(({ system_retry_id: value }) => Number(value)).sort((a, b) => a - b);
  const runIds = [...new Set(retryRows.rows.map(({ recovery_run_id: value }) => String(value)))].sort();
  if (!sameValues(retryIds, fence.migrationSystemRetryIds)
      || !sameValues(runIds, fence.recoveryRunIds)) {
    return null;
  }
  const candidateRows = await client.query(
    `SELECT task.task_id,candidate.candidate_id
     FROM crawler.youtube_api_tasks task
     JOIN crawler.content_candidates candidate
       ON candidate.source_content_id=task.source_content_id
     JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
     JOIN crawler.migration_system_retry_items retry
       ON retry.recovery_run_id=run.run_id
      AND retry.system_retry_id=ANY($2::bigint[])
      AND (NOT $5::boolean OR retry.status IN ('retrying','dispatched'))
      AND retry.failed_dispatch_batch_id=$3
     WHERE task.task_id=ANY($1::bigint[])
       AND run.run_id=ANY($4::text[])
       AND run.candidate_id=retry.candidate_id
     ORDER BY task.task_id,candidate.candidate_id`,
    [
      fence.taskIds,
      fence.migrationSystemRetryIds,
      fence.pipelineCycleId,
      fence.recoveryRunIds,
      requireActiveRetry,
    ],
  );
  const authorized = Object.fromEntries(fence.taskIds.map((taskId) => [taskId, []]));
  for (const row of candidateRows.rows) {
    authorized[Number(row.task_id)].push(Number(row.candidate_id));
  }
  if (Object.values(authorized).some((candidateIds) => candidateIds.length === 0)) return null;
  return {
    recovery: true,
    authorized_candidate_ids_by_task: authorized,
    recovery_run_ids: fence.recoveryRunIds,
    migration_system_retry_ids: fence.migrationSystemRetryIds,
  };
}

async function executionHasCommittedWork(client, fence, scope) {
  const taskRows = await client.query(
    `SELECT task_id,status
     FROM crawler.youtube_api_tasks
     WHERE task_id=ANY($1::bigint[])
     ORDER BY task_id
     FOR UPDATE`,
    [fence.taskIds],
  );
  if (taskRows.rows.length !== fence.taskIds.length) return true;
  if (taskRows.rows.some((row) => ["done", "unavailable"].includes(row.status))) {
    return true;
  }
  if (!scope.recovery) return false;
  const candidateIds = positiveIntegers(
    Object.values(scope.authorized_candidate_ids_by_task ?? {}).flat(),
    "authorized candidate ids",
  );
  if (candidateIds.length === 0) return true;
  const candidates = await client.query(
    `SELECT candidate_id,detail_status,api_status
     FROM crawler.content_candidates
     WHERE candidate_id=ANY($1::bigint[])
     ORDER BY candidate_id
     FOR UPDATE`,
    [candidateIds],
  );
  return candidates.rows.length !== candidateIds.length
    || candidates.rows.some((row) => (
      row.detail_status !== "api_pending"
      || !["pending", "queued", "running", "failed"].includes(row.api_status)
    ));
}

export async function claimDataApiBatchExecution(client, fence) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const batch = await loadLockedBatch(client, fence);
  if (!claimableBatch(batch, fence)) return null;
  const scope = await lockRecoveryScope(client, fence);
  if (!scope) return null;
  const dispatchIntent = durableDispatchIntent(fence);
  const takeover = batch.active_job_attempt != null
    && Number(batch.active_job_attempt) < fence.jobAttempt;
  if (takeover && await executionHasCommittedWork(client, fence, scope)) {
    const fenced = await client.query(
      `UPDATE crawler.youtube_api_batches
       SET active_job_id=$2,active_job_attempt=$3,
           result_json=(COALESCE(result_json,'{}'::jsonb)
             - 'execution_orphan_observation')
             || CASE
                  WHEN $4::jsonb IS NULL THEN '{}'::jsonb
                  ELSE jsonb_build_object('dispatch_intent',$4::jsonb)
                END
             || jsonb_build_object(
                  'takeover_recovery_required',
                  jsonb_build_object(
                    'failure_type','retryable_system_failure',
                    'failure_code','DATA_API_PARTIAL_COMMIT_TAKEOVER',
                    'failure_category','control_plane',
                    'job_id',$2::text,
                    'job_attempt',$3::bigint,
                    'observed_at',now()
                  )
                ),
           updated_at=now()
       WHERE batch_id=$1 AND status='running'
         AND active_job_attempt<$3
       RETURNING batch_id`,
      [
        fence.batchId,
        fence.jobId,
        fence.jobAttempt,
        dispatchIntent == null ? null : JSON.stringify(dispatchIntent),
      ],
    );
    if (fenced.rowCount !== 1) return null;
    return null;
  }
  const claimed = await client.query(
    `UPDATE crawler.youtube_api_batches
     SET status='running',active_job_id=$2,active_job_attempt=$3,
         result_json=COALESCE(result_json,'{}'::jsonb)
           || CASE
                WHEN $4::jsonb IS NULL THEN '{}'::jsonb
                ELSE jsonb_build_object('dispatch_intent',$4::jsonb)
              END,
         error_message=NULL,finished_at=NULL,started_at=COALESCE(started_at,now()),updated_at=now()
     WHERE batch_id=$1 AND status IN ('queued','running','failed')
       AND (
         active_job_attempt IS NULL
         OR active_job_attempt<$3
         OR (active_job_id=$2 AND active_job_attempt=$3)
     )
     RETURNING batch_id`,
    [
      fence.batchId,
      fence.jobId,
      fence.jobAttempt,
      dispatchIntent == null ? null : JSON.stringify(dispatchIntent),
    ],
  );
  return claimed.rowCount === 1 ? scope : null;
}

export async function lockDataApiBatchExecution(client, fence) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const batch = await loadLockedBatch(client, fence);
  if (!committedBatch(batch, fence)) return null;
  return lockRecoveryScope(client, fence);
}

function normalizedOrphanEvidence(evidence, fence) {
  const input = jsonRecord(evidence);
  const observedAt = new Date(input.observed_at ?? Date.now());
  if (Number.isNaN(observedAt.getTime())) {
    throw new TypeError("orphan evidence observed_at must be a timestamp");
  }
  return {
    failure_type: "retryable_system_failure",
    failure_code: text(input.failure_code) ?? "DATA_API_BATCH_EXECUTION_ORPHANED",
    failure_category: text(input.failure_category) ?? "control_plane",
    observation_kind: text(input.observation_kind) ?? "terminal_job",
    job_id: fence.jobId,
    job_attempt: fence.jobAttempt,
    observed_at: observedAt.toISOString(),
    ...(text(input.first_observed_at)
      ? { first_observed_at: text(input.first_observed_at) }
      : {}),
    ...(Number.isSafeInteger(Number(input.observation_count))
      ? { observation_count: Number(input.observation_count) }
      : {}),
    ...(text(input.bullmq_state) ? { bullmq_state: text(input.bullmq_state) } : {}),
    ...(text(input.error_message) ? { error_message: text(input.error_message) } : {}),
  };
}

function storedReplayMarker(batch, fence) {
  if (!fence.storedReplay) return null;
  const marker = jsonRecord(jsonRecord(batch.result_json).stored_evidence_recovery);
  const candidateIds = positiveIntegers(
    marker.candidate_ids ?? [],
    "stored replay candidate ids",
  );
  if (
    text(marker.operation_id) !== fence.storedReplay.operationId
    || text(marker.run_id) !== fence.storedReplay.runId
    || text(marker.batch_id) !== fence.batchId
    || !sameValues(
      positiveIntegers(marker.task_ids ?? [], "stored replay marker task ids"),
      fence.taskIds,
    )
    || candidateIds.length === 0
  ) {
    return null;
  }
  return { ...marker, candidate_ids: candidateIds };
}

async function lockOrphanCandidateScope(client, batch, fence, scope) {
  const taskRows = await client.query(
    `SELECT task_id,source_content_id,status
     FROM crawler.youtube_api_tasks
     WHERE task_id=ANY($1::bigint[])
     ORDER BY task_id
     FOR UPDATE`,
    [fence.taskIds],
  );
  if (taskRows.rows.length !== fence.taskIds.length) return null;
  const taskVideoIds = taskRows.rows.map((row) => String(row.source_content_id)).sort();
  if (!sameValues(taskVideoIds, fence.videoIds)) return null;
  let candidates;
  if (fence.recovery) {
    candidates = await client.query(
      `SELECT task.task_id,candidate.candidate_id,candidate.run_id,
              candidate.detail_status,candidate.api_status
       FROM crawler.youtube_api_tasks task
       JOIN crawler.content_candidates candidate
         ON candidate.source_content_id=task.source_content_id
       JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
       JOIN crawler.migration_system_retry_items retry
         ON retry.recovery_run_id=run.run_id
        AND retry.system_retry_id=ANY($2::bigint[])
        AND retry.candidate_id=run.candidate_id
        AND retry.failed_dispatch_batch_id=$3
       WHERE task.task_id=ANY($1::bigint[])
         AND run.run_id=ANY($4::text[])
       ORDER BY task.task_id,candidate.candidate_id
       FOR UPDATE OF candidate,run,retry`,
      [
        fence.taskIds,
        fence.migrationSystemRetryIds,
        fence.pipelineCycleId,
        fence.recoveryRunIds,
      ],
    );
  } else if (fence.storedReplay) {
    const marker = storedReplayMarker(batch, fence);
    if (!marker) return null;
    candidates = await client.query(
      `SELECT task.task_id,candidate.candidate_id,candidate.run_id,
              candidate.detail_status,candidate.api_status
       FROM crawler.youtube_api_tasks task
       JOIN crawler.content_candidates candidate
         ON candidate.candidate_id=ANY(task.candidate_ids)
       JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
       WHERE task.task_id=ANY($1::bigint[])
         AND candidate.candidate_id=ANY($2::bigint[])
         AND candidate.run_id=$3
       ORDER BY task.task_id,candidate.candidate_id
       FOR UPDATE OF candidate,run`,
      [fence.taskIds, marker.candidate_ids, fence.storedReplay.runId],
    );
    const actualCandidateIds = positiveIntegers(
      candidates.rows.map((row) => row.candidate_id),
      "stored replay scoped candidate ids",
    );
    if (!sameValues(actualCandidateIds, marker.candidate_ids)) return null;
  } else {
    candidates = await client.query(
      `SELECT task.task_id,candidate.candidate_id,candidate.run_id,
              candidate.detail_status,candidate.api_status
       FROM crawler.youtube_api_tasks task
       JOIN crawler.content_candidates candidate
         ON candidate.candidate_id=ANY(task.candidate_ids)
       JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
       WHERE task.task_id=ANY($1::bigint[])
       ORDER BY task.task_id,candidate.candidate_id
       FOR UPDATE OF candidate,run`,
      [fence.taskIds],
    );
  }
  return { taskRows: taskRows.rows, candidateRows: candidates.rows };
}

export async function settleOrphanedDataApiBatchExecution(client, {
  fence,
  evidence,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const batch = await loadLockedBatch(client, fence);
  if (!committedBatch(batch, fence)) {
    return { settled: false, reason: "stale_execution_fence" };
  }
  const scope = await lockRecoveryScope(client, fence, { requireActiveRetry: false });
  if (!scope) return { settled: false, reason: "stale_recovery_scope" };
  const locked = await lockOrphanCandidateScope(client, batch, fence, scope);
  if (!locked) return { settled: false, reason: "stale_task_scope" };
  const normalizedEvidence = normalizedOrphanEvidence(evidence, fence);
  const message = normalizedEvidence.error_message
    ?? `Data API Batch execution ${normalizedEvidence.observation_kind} requires recovery`;
  const resetTaskIds = locked.taskRows
    .filter((row) => row.status === "running")
    .map((row) => Number(row.task_id));
  const resetCandidateIds = locked.candidateRows
    .filter((row) => row.detail_status === "api_pending" && row.api_status === "running")
    .map((row) => Number(row.candidate_id));
  const affectedRunIds = [...new Set(
    locked.candidateRows.map((row) => String(row.run_id)),
  )].sort();
  const replayMarker = fence.storedReplay && !fence.recovery
    ? storedReplayMarker(batch, fence)
    : null;
  const replayTaskIds = replayMarker
    ? [...resetTaskIds].sort((left, right) => left - right)
    : [];
  const replayTaskIdSet = new Set(replayTaskIds);
  const replayCandidateIds = replayMarker
    ? [...new Set(locked.candidateRows
      .filter((row) => replayTaskIdSet.has(Number(row.task_id)))
      .filter((row) => (
        row.detail_status === "api_pending"
        && ["pending", "queued", "running", "failed"].includes(row.api_status)
      ))
      .map((row) => Number(row.candidate_id)))]
      .sort((left, right) => left - right)
    : [];
  const replayVideoIds = replayMarker
    ? locked.taskRows
      .filter((row) => replayTaskIdSet.has(Number(row.task_id)))
      .map((row) => String(row.source_content_id))
      .sort()
    : [];
  const successorBatchId = replayTaskIds.length > 0 && replayCandidateIds.length > 0
    ? safeJobId("stored-data-api-replay-recovery", fence.batchId, `a${fence.jobAttempt}`)
    : null;
  const recoveryEvidence = {
    ...normalizedEvidence,
    requeue_required: resetTaskIds.length > 0,
    reset_task_ids: [...resetTaskIds].sort((left, right) => left - right),
    reset_candidate_ids: [...resetCandidateIds].sort((left, right) => left - right),
    affected_run_ids: affectedRunIds,
    ...(successorBatchId ? { successor_batch_id: successorBatchId } : {}),
  };
  await client.query(
    `UPDATE crawler.youtube_api_batches
     SET status='failed',
         result_json=(COALESCE(result_json,'{}'::jsonb)
           - 'execution_orphan_observation')
           || jsonb_build_object('execution_orphan_recovery',$2::jsonb),
         error_message=$3,finished_at=now(),updated_at=now()
     WHERE batch_id=$1 AND status='running'
       AND active_job_id=$4 AND active_job_attempt=$5`,
    [
      fence.batchId,
      JSON.stringify(recoveryEvidence),
      message,
      fence.jobId,
      fence.jobAttempt,
    ],
  );
  if (resetTaskIds.length > 0) {
    await client.query(
      `UPDATE crawler.youtube_api_tasks
       SET status=CASE WHEN $4::boolean THEN 'queued' ELSE 'pending' END,
           result_json=COALESCE(result_json,'{}'::jsonb)
             || jsonb_build_object('execution_orphan_recovery',$2::jsonb),
           error_message=$3,
           next_retry_at=CASE WHEN $4::boolean THEN NULL ELSE now()+interval '5 minutes' END,
           finished_at=NULL,updated_at=now()
       WHERE task_id=ANY($1::bigint[]) AND status='running'`,
      [resetTaskIds, JSON.stringify(recoveryEvidence), message, successorBatchId != null],
    );
  }
  if (resetCandidateIds.length > 0) {
    await client.query(
      `UPDATE crawler.content_candidates
       SET api_status=CASE WHEN $4::boolean THEN 'queued' ELSE 'pending' END,
           result_json=COALESCE(result_json,'{}'::jsonb)
             || jsonb_build_object('execution_orphan_recovery',$2::jsonb),
           error_message=$3,finished_at=NULL,updated_at=now()
       WHERE candidate_id=ANY($1::bigint[])
         AND detail_status='api_pending' AND api_status='running'`,
      [resetCandidateIds, JSON.stringify(recoveryEvidence), message, successorBatchId != null],
    );
  }
  if (successorBatchId) {
    const successorMarker = {
      ...replayMarker,
      batch_id: successorBatchId,
      task_ids: replayTaskIds,
      candidate_ids: replayCandidateIds,
      prepared_at: normalizedEvidence.observed_at,
    };
    await client.query(
      `INSERT INTO crawler.youtube_api_batches (
         batch_id,status,task_ids,video_ids,result_json,updated_at
       ) VALUES ($1,'queued',$2::bigint[],$3::text[],$4::jsonb,now())
       ON CONFLICT (batch_id) DO NOTHING`,
      [
        successorBatchId,
        replayTaskIds,
        replayVideoIds,
        JSON.stringify({
          stored_evidence_recovery: successorMarker,
          orphan_requeue_intent: {
            source_batch_id: fence.batchId,
            source_job_id: fence.jobId,
            source_job_attempt: fence.jobAttempt,
            created_at: normalizedEvidence.observed_at,
          },
        }),
      ],
    );
  }
  const runSummaries = [];
  for (const runId of affectedRunIds) {
    runSummaries.push(await reconcileRunDetailStatus(client, runId));
  }
  return {
    settled: true,
    reset_task_ids: resetTaskIds.sort((left, right) => left - right),
    reset_candidate_ids: resetCandidateIds.sort((left, right) => left - right),
    affected_run_ids: affectedRunIds,
    run_summaries: runSummaries,
    successor_batch_id: successorBatchId,
  };
}
