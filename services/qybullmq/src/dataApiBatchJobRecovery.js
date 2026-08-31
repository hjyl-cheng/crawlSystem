import {
  dataApiBatchExecutionFence,
  settleOrphanedDataApiBatchExecution,
} from "./dataApiBatchExecutionFence.js";
import { safeJobId } from "./queues.js";

const REPRESENTED_JOB_STATES = new Set([
  "active",
  "delayed",
  "paused",
  "prioritized",
  "waiting",
  "waiting-children",
]);
const TERMINAL_JOB_STATES = new Set(["completed", "failed"]);

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  return String(value ?? "").trim() || null;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function positiveIntegers(values, field) {
  if (!Array.isArray(values)) throw new TypeError(`${field} must be an array`);
  const normalized = [...new Set(values.map((value) => positiveInteger(value, field)))]
    .sort((left, right) => left - right);
  if (normalized.length !== values.length) {
    throw new TypeError(`${field} must contain unique positive integers`);
  }
  return normalized;
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

function boundedLimit(value) {
  return Math.max(1, Math.min(500, Number(value) || 100));
}

function validTimestamp(value) {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function dataApiBatchJobIntent(batch) {
  const batchId = text(batch?.batch_id);
  const jobId = text(batch?.active_job_id);
  const jobAttempt = positiveInteger(batch?.active_job_attempt, "active_job_attempt");
  const taskIds = positiveIntegers(batch?.task_ids ?? [], "batch.task_ids");
  const videoIds = texts(batch?.video_ids ?? [], "batch.video_ids");
  const result = record(batch?.result_json);
  if (!batchId || !jobId || taskIds.length === 0 || videoIds.length === 0) {
    throw new TypeError("running Data API Batch identity is incomplete");
  }
  const stored = record(result.stored_evidence_recovery);
  let data;
  if (Object.keys(stored).length > 0) {
    const candidateIds = positiveIntegers(
      stored.candidate_ids ?? [],
      "stored_evidence_recovery.candidate_ids",
    );
    data = {
      batch_id: batchId,
      task_ids: taskIds,
      video_ids: videoIds,
      stored_evidence_replay: {
        operation_id: text(stored.operation_id),
        run_id: text(stored.run_id),
        expected_candidate_count: candidateIds.length,
        task_ids: taskIds,
      },
    };
  } else {
    const dispatch = record(result.dispatch_intent);
    const pipelineCycleId = text(dispatch.pipeline_cycle_id);
    if (!pipelineCycleId) {
      throw new TypeError("Data API Batch dispatch intent is incomplete");
    }
    const retryIds = positiveIntegers(
      dispatch.migration_system_retry_ids ?? [],
      "dispatch_intent.migration_system_retry_ids",
    );
    const recoveryRunIds = texts(
      dispatch.recovery_run_ids ?? [],
      "dispatch_intent.recovery_run_ids",
    );
    if ((retryIds.length > 0) !== (recoveryRunIds.length > 0)) {
      throw new TypeError("Data API Batch recovery dispatch intent is incomplete");
    }
    data = {
      batch_id: batchId,
      task_ids: taskIds,
      video_ids: videoIds,
      pipeline_cycle_id: pipelineCycleId,
      ...(retryIds.length > 0
        ? {
          migration_system_retry_ids: retryIds,
          recovery_run_ids: recoveryRunIds,
        }
        : {}),
    };
  }
  const job = {
    id: jobId,
    name: "youtube-data-api-batch",
    attemptsStarted: jobAttempt,
    data,
  };
  return Object.freeze({ job, fence: dataApiBatchExecutionFence(job) });
}

function storedReplayQueuedJob(batch) {
  const batchId = text(batch?.batch_id);
  const taskIds = positiveIntegers(batch?.task_ids ?? [], "batch.task_ids");
  const videoIds = texts(batch?.video_ids ?? [], "batch.video_ids");
  const stored = record(record(batch?.result_json).stored_evidence_recovery);
  const candidateIds = positiveIntegers(
    stored.candidate_ids ?? [],
    "stored_evidence_recovery.candidate_ids",
  );
  if (
    !batchId
    || taskIds.length === 0
    || videoIds.length === 0
    || candidateIds.length === 0
    || text(stored.batch_id) !== batchId
  ) {
    throw new TypeError("queued stored replay Batch identity is incomplete");
  }
  return {
    id: safeJobId("youtube-data-api", batchId),
    name: "youtube-data-api-batch",
    data: {
      batch_id: batchId,
      task_ids: taskIds,
      video_ids: videoIds,
      stored_evidence_replay: {
        operation_id: text(stored.operation_id),
        run_id: text(stored.run_id),
        expected_candidate_count: candidateIds.length,
        task_ids: taskIds,
      },
    },
  };
}

export function representsDataApiBatchJob(job, expectedJob) {
  try {
    if (String(job?.id ?? "") !== expectedJob.id || job?.name !== expectedJob.name) return false;
    const actualData = record(job.data);
    const expectedData = expectedJob.data;
    if (
      text(actualData.batch_id) !== expectedData.batch_id
      || !sameValues(
        positiveIntegers(actualData.task_ids ?? [], "job.task_ids"),
        expectedData.task_ids,
      )
      || !sameValues(texts(actualData.video_ids ?? [], "job.video_ids"), expectedData.video_ids)
    ) {
      return false;
    }
    if (expectedData.stored_evidence_replay) {
      const actualReplay = record(actualData.stored_evidence_replay);
      return text(actualReplay.operation_id) === expectedData.stored_evidence_replay.operation_id
        && text(actualReplay.run_id) === expectedData.stored_evidence_replay.run_id
        && Number(actualReplay.expected_candidate_count)
          === expectedData.stored_evidence_replay.expected_candidate_count
        && sameValues(
          positiveIntegers(actualReplay.task_ids ?? [], "job.stored replay task_ids"),
          expectedData.stored_evidence_replay.task_ids,
        );
    }
    return text(actualData.pipeline_cycle_id) === expectedData.pipeline_cycle_id
      && sameValues(
        positiveIntegers(
          actualData.migration_system_retry_ids ?? [],
          "job.migration_system_retry_ids",
        ),
        expectedData.migration_system_retry_ids ?? [],
      )
      && sameValues(
        texts(actualData.recovery_run_ids ?? [], "job.recovery_run_ids"),
        expectedData.recovery_run_ids ?? [],
      );
  } catch {
    return false;
  }
}

function observationMarker(row) {
  return record(record(row?.result_json).execution_orphan_observation);
}

function exactMarker(marker, fence, observationKind) {
  return text(marker.batch_id) === fence.batchId
    && text(marker.job_id) === fence.jobId
    && Number(marker.job_attempt) === fence.jobAttempt
    && text(marker.observation_kind) === observationKind;
}

async function clearObservation(withTransaction, fence) {
  return withTransaction(async (client) => {
    const cleared = await client.query(
      `UPDATE crawler.youtube_api_batches
       SET result_json=COALESCE(result_json,'{}'::jsonb)
             - 'execution_orphan_observation',
           updated_at=now()
       WHERE batch_id=$1 AND status='running'
         AND active_job_id=$2 AND active_job_attempt=$3
         AND result_json ? 'execution_orphan_observation'`,
      [fence.batchId, fence.jobId, fence.jobAttempt],
    );
    return cleared.rowCount === 1;
  });
}

async function observeOrSettle(withTransaction, intent, {
  observationKind,
  bullmqState = null,
  errorMessage = null,
  now,
  graceMs,
  minimumObservations,
  terminal = false,
}) {
  return withTransaction(async (client) => {
    const locked = await client.query(
      `SELECT batch_id,status,task_ids,video_ids,result_json,
              active_job_id,active_job_attempt
       FROM crawler.youtube_api_batches
       WHERE batch_id=$1
       FOR UPDATE`,
      [intent.fence.batchId],
    );
    const row = locked.rows[0];
    if (
      !row
      || row.status !== "running"
      || text(row.active_job_id) !== intent.fence.jobId
      || Number(row.active_job_attempt) !== intent.fence.jobAttempt
    ) {
      return { action: "stale" };
    }
    const current = observationMarker(row);
    const firstObservedAt = exactMarker(current, intent.fence, observationKind)
      ? validTimestamp(current.first_observed_at) ?? now
      : now;
    const observationCount = exactMarker(current, intent.fence, observationKind)
      ? Math.max(0, Number(current.observation_count) || 0) + 1
      : 1;
    const confirmed = terminal || (
      observationCount >= minimumObservations
      && firstObservedAt.getTime() <= now.getTime() - graceMs
    );
    const marker = {
      failure_type: "retryable_system_failure",
      failure_code: "DATA_API_BATCH_EXECUTION_ORPHANED",
      failure_category: "control_plane",
      batch_id: intent.fence.batchId,
      job_id: intent.fence.jobId,
      job_attempt: intent.fence.jobAttempt,
      observation_kind: observationKind,
      observation_count: observationCount,
      first_observed_at: firstObservedAt.toISOString(),
      last_observed_at: now.toISOString(),
      ...(bullmqState ? { bullmq_state: bullmqState } : {}),
      ...(errorMessage ? { error_message: errorMessage } : {}),
    };
    if (!confirmed) {
      await client.query(
        `UPDATE crawler.youtube_api_batches
         SET result_json=COALESCE(result_json,'{}'::jsonb)
               || jsonb_build_object('execution_orphan_observation',$2::jsonb),
             updated_at=now()
         WHERE batch_id=$1 AND status='running'
           AND active_job_id=$3 AND active_job_attempt=$4`,
        [
          intent.fence.batchId,
          JSON.stringify(marker),
          intent.fence.jobId,
          intent.fence.jobAttempt,
        ],
      );
      return { action: "suspected", marker };
    }
    const settled = await settleOrphanedDataApiBatchExecution(client, {
      fence: intent.fence,
      evidence: {
        ...marker,
        observed_at: marker.last_observed_at,
      },
    });
    return settled.settled
      ? { action: "settled", marker, settlement: settled }
      : { action: "stale", settlement: settled };
  });
}

export async function settleTerminalDataApiBatchJob({
  job,
  withTransaction,
  observationKind,
  error = null,
  now = new Date(),
} = {}) {
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (job?.name !== "youtube-data-api-batch") return { action: "ignored" };
  const timestamp = validTimestamp(now);
  if (!timestamp) throw new TypeError("now must be a timestamp");
  const intent = {
    job,
    fence: dataApiBatchExecutionFence(job),
  };
  return observeOrSettle(withTransaction, intent, {
    observationKind: text(observationKind) ?? "terminal_job",
    bullmqState: text(observationKind),
    errorMessage: text(error?.message ?? error),
    now: timestamp,
    graceMs: 0,
    minimumObservations: 1,
    terminal: true,
  });
}

export class DataApiBatchJobRecovery {
  constructor({
    query,
    withTransaction,
    queue,
    graceMs = 60_000,
    minimumObservations = 2,
    now = () => new Date(),
  } = {}) {
    if (typeof query !== "function") throw new TypeError("query is required");
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    if (!queue || typeof queue.getJob !== "function") {
      throw new TypeError("a BullMQ queue is required");
    }
    if (!Number.isSafeInteger(Number(graceMs)) || Number(graceMs) < 0) {
      throw new TypeError("graceMs must be a non-negative integer");
    }
    if (!Number.isSafeInteger(Number(minimumObservations)) || Number(minimumObservations) < 2) {
      throw new TypeError("minimumObservations must be at least 2");
    }
    this.query = query;
    this.withTransaction = withTransaction;
    this.queue = queue;
    this.graceMs = Number(graceMs);
    this.minimumObservations = Number(minimumObservations);
    this.now = now;
  }

  async loadRunning(limit) {
    const rows = await this.query(
      `SELECT batch_id,status,task_ids,video_ids,result_json,
              active_job_id,active_job_attempt
       FROM crawler.youtube_api_batches
       WHERE status='running'
         AND active_job_id IS NOT NULL
         AND active_job_attempt IS NOT NULL
       ORDER BY updated_at,batch_id
       LIMIT $1`,
      [boundedLimit(limit)],
    );
    return rows.rows;
  }

  async reconcileStoredReplayRequeues(limit) {
    if (typeof this.queue.add !== "function") return { scanned: 0, created: 0 };
    const rows = await this.query(
      `SELECT batch_id,status,task_ids,video_ids,result_json
       FROM crawler.youtube_api_batches
       WHERE status='queued'
         AND result_json ? 'orphan_requeue_intent'
         AND result_json ? 'stored_evidence_recovery'
       ORDER BY updated_at,batch_id
       LIMIT $1`,
      [boundedLimit(limit)],
    );
    let created = 0;
    for (const row of rows.rows) {
      let expected;
      try {
        expected = storedReplayQueuedJob(row);
      } catch {
        continue;
      }
      let existing = await this.queue.getJob(expected.id);
      for (let inspection = 0; existing && inspection < 3; inspection += 1) {
        if (!representsDataApiBatchJob(existing, expected)) break;
        const state = await existing.getState();
        if (REPRESENTED_JOB_STATES.has(state)) break;
        if (!TERMINAL_JOB_STATES.has(state)) break;
        try {
          await existing.remove();
          existing = null;
        } catch {
          existing = await this.queue.getJob(expected.id);
        }
      }
      if (existing) continue;
      const job = await this.queue.add(expected.name, expected.data, { jobId: expected.id });
      if (!representsDataApiBatchJob(job, expected)) {
        throw new Error(`stored replay successor Job identity changed: ${expected.id}`);
      }
      created += 1;
    }
    return { scanned: rows.rows.length, created };
  }

  async reconcileAvailable({ limit = 100 } = {}) {
    const summary = {
      scanned: 0,
      represented: 0,
      suspected: 0,
      settled: 0,
      stale: 0,
      unavailable: 0,
      identityConflicts: 0,
      replayRequeuesCreated: 0,
    };
    const replayRequeues = await this.reconcileStoredReplayRequeues(limit);
    summary.replayRequeuesCreated = replayRequeues.created;
    const rows = await this.loadRunning(limit);
    for (const row of rows) {
      summary.scanned += 1;
      let intent;
      try {
        intent = dataApiBatchJobIntent(row);
      } catch {
        summary.unavailable += 1;
        continue;
      }
      let job;
      try {
        job = await this.queue.getJob(intent.job.id);
      } catch {
        summary.unavailable += 1;
        continue;
      }
      let observationKind = "missing";
      let bullmqState = null;
      let terminal = false;
      if (job) {
        try {
          if (!representsDataApiBatchJob(job, intent.job)) {
            observationKind = "identity_conflict";
            summary.identityConflicts += 1;
          } else {
            bullmqState = await job.getState();
            if (REPRESENTED_JOB_STATES.has(bullmqState)) {
              await clearObservation(this.withTransaction, intent.fence);
              summary.represented += 1;
              continue;
            }
            if (TERMINAL_JOB_STATES.has(bullmqState)) {
              observationKind = bullmqState;
              terminal = true;
            } else {
              observationKind = "unknown_state";
            }
          }
        } catch {
          summary.unavailable += 1;
          continue;
        }
      }
      const timestamp = validTimestamp(this.now());
      if (!timestamp) throw new TypeError("now() must return a timestamp");
      const result = await observeOrSettle(this.withTransaction, intent, {
        observationKind,
        bullmqState,
        now: timestamp,
        graceMs: this.graceMs,
        minimumObservations: this.minimumObservations,
        terminal,
      });
      if (result.action === "settled") summary.settled += 1;
      else if (result.action === "suspected") summary.suspected += 1;
      else summary.stale += 1;
    }
    return Object.freeze(summary);
  }
}
