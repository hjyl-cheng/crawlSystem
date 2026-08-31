import {
  allocateChannelSnapshotDispatchOutbox,
  channelSnapshotJobPayload,
} from "./channelSnapshotDispatch.js";
import { migrationSystemRetryDispatchAdmission } from "./migrationSystemRetryAdmission.js";
import { safeJobId } from "./queues.js";

export class MigrationSystemRetryError extends Error {
  constructor(message, {
    statusCode = 409,
    code = "migration_system_retry_error",
    details = null,
  } = {}) {
    super(message);
    this.name = "MigrationSystemRetryError";
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function positiveInteger(value, field) {
  const normalized = Number(value);
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new MigrationSystemRetryError(`${field} must be a positive integer`, {
      statusCode: 400,
      code: `invalid_${field}`,
    });
  }
  return normalized;
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) {
    throw new MigrationSystemRetryError(`${field} is required`, {
      code: "migration_system_retry_fence_invalid",
    });
  }
  return normalized;
}

function retryCandidate(row, generation) {
  return {
    candidate_id: positiveInteger(row.candidate_id, "candidate_id"),
    migration_intent_id: positiveInteger(row.migration_intent_id, "migration_intent_id"),
    dispatch_batch_id: requiredText(row.dispatch_batch_id, "dispatch_batch_id"),
    pipeline_cycle_id: requiredText(row.pipeline_cycle_id, "pipeline_cycle_id"),
    channel_id: requiredText(row.channel_id, "channel_id"),
    channel_url: requiredText(row.channel_url, "channel_url"),
    priority: Number(row.priority ?? 100),
    snapshot_dispatch_generation: generation,
  };
}

function assertPendingRetryFence(row, failedGeneration) {
  const candidateGeneration = Number(row.snapshot_dispatch_generation);
  const intentGeneration = Number(row.intent_dispatch_attempts);
  const failedAttempt = Number(row.failed_job_attempt);
  if (!["failed", "accepted"].includes(row.candidate_status)
      || row.snapshot_json?.failure_type !== "retryable_system_failure"
      || candidateGeneration !== failedGeneration
      || intentGeneration !== failedGeneration
      || String(row.snapshot_active_job_id ?? "") !== String(row.failed_job_id ?? "")
      || Number(row.snapshot_active_job_attempt) !== failedAttempt) {
    throw new MigrationSystemRetryError("Migration system retry lost its Candidate Fence", {
      code: "migration_system_retry_fence_stale",
      details: {
        system_retry_id: Number(row.system_retry_id),
        candidate_id: Number(row.candidate_id),
        failed_dispatch_generation: failedGeneration,
      },
    });
  }
}

function assertDispatchedRetryFence(row, nextGeneration) {
  if (Number(row.retry_dispatch_generation) !== nextGeneration
      || Number(row.snapshot_dispatch_generation) !== nextGeneration
      || Number(row.intent_dispatch_attempts) !== nextGeneration) {
    throw new MigrationSystemRetryError("Dispatched Migration system retry changed generation", {
      code: "migration_system_retry_dispatch_stale",
      details: { system_retry_id: Number(row.system_retry_id) },
    });
  }
}

async function pinFailedDispatchBatch(client, row, failedGeneration) {
  const candidateBatchId = requiredText(row.dispatch_batch_id, "dispatch_batch_id");
  const failedBatchId = String(row.failed_dispatch_batch_id ?? "").trim() || null;
  if (failedBatchId != null) {
    if (failedBatchId !== candidateBatchId) {
      throw new MigrationSystemRetryError("Migration system retry changed Dispatch Batch", {
        code: "migration_system_retry_fence_stale",
        details: {
          system_retry_id: Number(row.system_retry_id),
          candidate_id: Number(row.candidate_id),
          failed_dispatch_batch_id: failedBatchId,
          candidate_dispatch_batch_id: candidateBatchId,
        },
      });
    }
    return failedBatchId;
  }

  const pinned = await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET failed_dispatch_batch_id=$2,updated_at=now()
     WHERE system_retry_id=$1 AND failed_dispatch_batch_id IS NULL
       AND failed_dispatch_generation=$3 AND failed_job_id=$4 AND failed_job_attempt=$5
       AND status IN ('pending','dispatched')
     RETURNING failed_dispatch_batch_id`,
    [
      Number(row.system_retry_id),
      candidateBatchId,
      failedGeneration,
      requiredText(row.failed_job_id, "failed_job_id"),
      Number(row.failed_job_attempt),
    ],
  );
  if (pinned.rowCount !== 1) {
    throw new MigrationSystemRetryError("Migration system retry could not pin its Dispatch Batch", {
      code: "migration_system_retry_item_stale",
      details: { system_retry_id: Number(row.system_retry_id) },
    });
  }
  row.failed_dispatch_batch_id = pinned.rows[0].failed_dispatch_batch_id;
  return row.failed_dispatch_batch_id;
}

async function rearmDeadRetryOutbox(client, {
  outbox,
  candidateId,
  dispatchGeneration,
  jobId,
}) {
  if (outbox?.status !== "dead") return outbox;
  const rearmedOutbox = await client.query(
    `UPDATE crawler.proxy_job_dispatch_outbox
     SET status='pending',attempts=0,next_attempt_at=NULL,last_error=NULL,
         sent_at=NULL,updated_at=now()
     WHERE dispatch_id=$1 AND aggregate_kind='channel_snapshot'
       AND aggregate_id=$2
       AND (payload_json->>'dispatch_generation')::bigint=$3
       AND deterministic_job_id=$4 AND intent_hash=$5 AND status='dead'
     RETURNING *`,
    [
      requiredText(outbox.dispatch_id, "outbox.dispatch_id"),
      String(candidateId),
      dispatchGeneration,
      jobId,
      requiredText(outbox.intent_hash, "outbox.intent_hash"),
    ],
  );
  if (rearmedOutbox.rowCount !== 1) {
    throw new MigrationSystemRetryError("Migration retry Outbox lost its dead-delivery Fence", {
      code: "migration_system_retry_outbox_stale",
      details: { candidate_id: candidateId, dispatch_generation: dispatchGeneration },
    });
  }
  const rearmedCandidate = await client.query(
    `UPDATE crawler.channel_candidates
     SET status=CASE WHEN status='accepted' THEN 'accepted' ELSE 'queued' END,
         next_retry_at=NULL,
         validation_finished_at=CASE WHEN status='accepted' THEN validation_finished_at ELSE NULL END,
         error_message=NULL,snapshot_active_job_id=$3,snapshot_active_job_attempt=0,
         updated_at=now()
     WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
       AND status IN ('failed','accepted')
       AND (
         (snapshot_active_job_id IS NULL AND snapshot_active_job_attempt IS NULL)
         OR (snapshot_active_job_id=$3 AND snapshot_active_job_attempt=0)
       )
     RETURNING candidate_id`,
    [candidateId, dispatchGeneration, jobId],
  );
  if (rearmedCandidate.rowCount !== 1) {
    throw new MigrationSystemRetryError("Migration retry lost its dead-delivery Candidate Fence", {
      code: "migration_system_retry_delivery_candidate_stale",
      details: { candidate_id: candidateId, dispatch_generation: dispatchGeneration },
    });
  }
  return rearmedOutbox.rows[0];
}

async function rearmSentRetryOutbox(client, {
  outbox,
  candidateId,
  dispatchGeneration,
  jobId,
}) {
  if (outbox?.status !== "sent") return outbox;
  const rearmed = await client.query(
    `UPDATE crawler.proxy_job_dispatch_outbox outbox
     SET status='pending',next_attempt_at=NULL,last_error=NULL,
         sent_at=NULL,updated_at=now()
     WHERE outbox.dispatch_id=$1 AND outbox.aggregate_kind='channel_snapshot'
       AND outbox.aggregate_id=$2
       AND (outbox.payload_json->>'dispatch_generation')::bigint=$3
       AND outbox.deterministic_job_id=$4 AND outbox.intent_hash=$5
       AND outbox.status='sent'
       AND EXISTS (
         SELECT 1
         FROM crawler.channel_candidates candidate
         WHERE candidate.candidate_id=$2::bigint
           AND candidate.snapshot_dispatch_generation=$3
           AND candidate.snapshot_active_job_id=$4
           AND candidate.snapshot_active_job_attempt=0
       )
     RETURNING outbox.*`,
    [
      requiredText(outbox.dispatch_id, "outbox.dispatch_id"),
      String(candidateId),
      dispatchGeneration,
      jobId,
      requiredText(outbox.intent_hash, "outbox.intent_hash"),
    ],
  );
  if (rearmed.rowCount !== 1) {
    throw new MigrationSystemRetryError("Migration retry lost its sent-delivery Candidate Fence", {
      code: "migration_system_retry_delivery_candidate_stale",
      details: { candidate_id: candidateId, dispatch_generation: dispatchGeneration },
    });
  }
  return rearmed.rows[0];
}

export async function listMigrationSystemRetryItems(query, {
  statuses = ["retrying", "pending", "dispatched"],
  limit = 100,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const normalizedLimit = Math.max(1, Math.min(500, Number(limit) || 100));
  const normalizedStatuses = [...new Set(statuses.map((status) => String(status ?? "").trim()))]
    .filter((status) => ["retrying", "pending", "dispatched", "resolved", "cancelled"].includes(status));
  if (normalizedStatuses.length === 0) throw new TypeError("at least one retry status is required");
  const rows = await query(
    `SELECT retry.system_retry_id,retry.migration_intent_id,retry.candidate_id,
            retry.failure_code,retry.failure_category,retry.failure_evidence,
            retry.failed_dispatch_batch_id,retry.failed_dispatch_generation,
            retry.failed_job_id,retry.failed_job_attempt,
            retry.status,retry.retry_dispatch_generation,retry.requested_at,
            retry.dispatched_at,retry.resolved_at,retry.resolution,
            intent.channel_id,candidate.channel_url,candidate.status AS candidate_status,
            candidate.snapshot_dispatch_generation,
            candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            candidate.dispatch_batch_id,candidate.pipeline_cycle_id
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.migration_channel_intents intent
       ON intent.migration_intent_id=retry.migration_intent_id
     JOIN crawler.channel_candidates candidate
       ON candidate.candidate_id=retry.candidate_id
     WHERE retry.status=ANY($1::text[])
     ORDER BY retry.requested_at ASC,retry.system_retry_id ASC
     LIMIT $2`,
    [normalizedStatuses, normalizedLimit],
  );
  return rows.rows.map((row) => ({
    ...row,
    failure_type: "retryable_system_failure",
    system_retry_id: Number(row.system_retry_id),
    migration_intent_id: Number(row.migration_intent_id),
    candidate_id: Number(row.candidate_id),
    failed_dispatch_generation: Number(row.failed_dispatch_generation),
    failed_job_attempt: Number(row.failed_job_attempt),
    retry_dispatch_generation: row.retry_dispatch_generation == null
      ? null
      : Number(row.retry_dispatch_generation),
  }));
}

export async function retryMigrationSystemFailure({
  systemRetryId,
  withTransaction,
  minSubscriberCount = 1000,
  allocateOutbox = allocateChannelSnapshotDispatchOutbox,
  jobIdFactory = safeJobId,
} = {}) {
  const normalizedRetryId = positiveInteger(systemRetryId, "system_retry_id");
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (typeof allocateOutbox !== "function") throw new TypeError("allocateOutbox is required");

  return withTransaction(async (client) => {
    const schedulerRows = await client.query(
      `SELECT value_json
       FROM crawler.settings
       WHERE setting_key='query_scheduler'
       LIMIT 1
       FOR UPDATE`,
    );
    if (schedulerRows.rows.length !== 1) {
      throw new MigrationSystemRetryError("query_scheduler setting is missing", {
        statusCode: 503,
        code: "migration_system_retry_scheduler_missing",
      });
    }
    const schedulerAdmission = migrationSystemRetryDispatchAdmission(
      schedulerRows.rows[0].value_json,
    );
    if (!schedulerAdmission.allowed) {
      throw new MigrationSystemRetryError(
        "Migration system retry requires a completed Scheduler",
        {
          code: schedulerAdmission.code,
          details: schedulerAdmission,
        },
      );
    }
    const candidateLock = await client.query(
      `/* migration-system-retry-dispatch-lock:candidate */
       SELECT candidate.candidate_id
       FROM crawler.migration_system_retry_items retry
       JOIN crawler.channel_candidates candidate
         ON candidate.candidate_id=retry.candidate_id
       WHERE retry.system_retry_id=$1
       ORDER BY candidate.candidate_id
       FOR UPDATE OF candidate`,
      [normalizedRetryId],
    );
    const lockedCandidateId = candidateLock.rows[0]?.candidate_id;
    if (lockedCandidateId == null) {
      throw new MigrationSystemRetryError("Migration system retry item was not found", {
        statusCode: 404,
        code: "migration_system_retry_not_found",
        details: { system_retry_id: normalizedRetryId },
      });
    }
    const lockedRetry = await client.query(
      `/* migration-system-retry-dispatch-lock:retry */
       SELECT retry.system_retry_id,retry.migration_intent_id,retry.candidate_id,
              retry.failed_dispatch_batch_id,retry.failed_dispatch_generation,
              retry.failed_job_id,retry.failed_job_attempt,
              retry.failure_code,retry.failure_category,retry.failure_evidence,retry.status,
              retry.retry_dispatch_generation,
              candidate.dispatch_batch_id,candidate.pipeline_cycle_id,candidate.channel_id,
              candidate.channel_url,candidate.priority,candidate.status AS candidate_status,
              candidate.snapshot_json,candidate.snapshot_dispatch_generation,
              candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt
       FROM crawler.migration_system_retry_items retry
       JOIN crawler.channel_candidates candidate
         ON candidate.candidate_id=retry.candidate_id
       WHERE retry.system_retry_id=$1 AND retry.candidate_id=$2
       ORDER BY retry.system_retry_id
       FOR UPDATE OF retry`,
      [normalizedRetryId, lockedCandidateId],
    );
    const retryRow = lockedRetry.rows[0];
    if (!retryRow) {
      throw new MigrationSystemRetryError("Migration system retry item was not found", {
        statusCode: 404,
        code: "migration_system_retry_not_found",
        details: { system_retry_id: normalizedRetryId },
      });
    }
    const lockedIntent = await client.query(
      `/* migration-system-retry-dispatch-lock:intent */
       SELECT intent.migration_intent_id,intent.dispatch_attempts
       FROM crawler.migration_system_retry_items retry
       JOIN crawler.migration_channel_intents intent
         ON intent.migration_intent_id=retry.migration_intent_id
        AND intent.target_candidate_id=retry.candidate_id
       WHERE retry.system_retry_id=$1
         AND retry.candidate_id=$2
         AND retry.migration_intent_id=$3
       ORDER BY intent.migration_intent_id
       FOR UPDATE OF intent`,
      [normalizedRetryId, lockedCandidateId, retryRow.migration_intent_id],
    );
    const intentRow = lockedIntent.rows[0];
    if (!intentRow) {
      throw new MigrationSystemRetryError("Migration system retry item was not found", {
        statusCode: 404,
        code: "migration_system_retry_not_found",
        details: { system_retry_id: normalizedRetryId },
      });
    }
    const row = {
      ...retryRow,
      intent_dispatch_attempts: intentRow.dispatch_attempts
        ?? intentRow.intent_dispatch_attempts,
    };
    if (!["pending", "dispatched"].includes(row.status)) {
      throw new MigrationSystemRetryError(
        row.status === "retrying"
          ? "Migration system failure is still using its BullMQ retry"
          : `Migration system retry is ${row.status}`,
        {
          code: row.status === "retrying"
            ? "migration_system_retry_not_terminal"
            : "migration_system_retry_not_actionable",
          details: { system_retry_id: normalizedRetryId, status: row.status },
        },
      );
    }

    const failedGeneration = positiveInteger(
      row.failed_dispatch_generation,
      "failed_dispatch_generation",
    );
    const nextGeneration = failedGeneration + 1;
    const retryAlreadyAllocated = Number(row.retry_dispatch_generation) === nextGeneration;
    if (row.status === "pending" && !retryAlreadyAllocated) {
      assertPendingRetryFence(row, failedGeneration);
    } else {
      assertDispatchedRetryFence(row, nextGeneration);
    }
    const failedDispatchBatchId = await pinFailedDispatchBatch(
      client,
      row,
      failedGeneration,
    );

    const candidate = retryCandidate(row, nextGeneration);
    const jobId = jobIdFactory(
      "channel-snapshot",
      candidate.dispatch_batch_id,
      candidate.channel_id,
      `g${nextGeneration}`,
    );
    const payload = channelSnapshotJobPayload(
      candidate,
      candidate.dispatch_batch_id,
      { minSubscriberCount },
    );
    const allocation = await allocateOutbox(client, {
      candidate,
      expectedGeneration: failedGeneration,
      previousJobId: row.failed_job_id,
      previousJobAttempt: Number(row.failed_job_attempt),
      migrationIntentId: candidate.migration_intent_id,
      payload,
      jobId,
    });
    const sentRearmedOutbox = await rearmSentRetryOutbox(client, {
      outbox: allocation.outbox,
      candidateId: candidate.candidate_id,
      dispatchGeneration: nextGeneration,
      jobId,
    });
    const outbox = await rearmDeadRetryOutbox(client, {
      outbox: sentRearmedOutbox,
      candidateId: candidate.candidate_id,
      dispatchGeneration: nextGeneration,
      jobId,
    });

    const cleared = await client.query(
      `UPDATE crawler.channel_candidates
       SET snapshot_json=(COALESCE(snapshot_json,'{}'::jsonb)
             - 'failure_type' - 'system_failure'),
           error_message=NULL,updated_at=now()
       WHERE candidate_id=$1 AND snapshot_dispatch_generation=$2
         AND snapshot_active_job_id=$3
       RETURNING candidate_id`,
      [candidate.candidate_id, nextGeneration, jobId],
    );
    if (cleared.rowCount !== 1) {
      throw new MigrationSystemRetryError("Migration system retry lost its G+1 Candidate Fence", {
        code: "migration_system_retry_g_plus_one_stale",
        details: { system_retry_id: normalizedRetryId, candidate_id: candidate.candidate_id },
      });
    }

    if (row.status === "pending") {
      const dispatched = await client.query(
        `UPDATE crawler.migration_system_retry_items
         SET status='dispatched',retry_dispatch_generation=$2,
             dispatched_at=COALESCE(dispatched_at,now()),updated_at=now()
         WHERE system_retry_id=$1 AND status='pending'
           AND failed_dispatch_generation=$3
           AND failed_dispatch_batch_id=$4
         RETURNING system_retry_id,status,retry_dispatch_generation,dispatched_at`,
        [normalizedRetryId, nextGeneration, failedGeneration, failedDispatchBatchId],
      );
      if (dispatched.rowCount !== 1) {
        throw new MigrationSystemRetryError("Migration system retry item changed during dispatch", {
          code: "migration_system_retry_item_stale",
          details: { system_retry_id: normalizedRetryId },
        });
      }
    }

    return Object.freeze({
      ok: true,
      created: allocation.created === true,
      system_retry_id: normalizedRetryId,
      migration_intent_id: candidate.migration_intent_id,
      candidate_id: candidate.candidate_id,
      channel_id: candidate.channel_id,
      failed_dispatch_generation: failedGeneration,
      dispatch_generation: nextGeneration,
      status: "dispatched",
      outbox,
    });
  });
}
