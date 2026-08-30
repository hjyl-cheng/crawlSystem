import {
  allocateChannelSnapshotDispatchOutbox,
  buildChannelSnapshotRedispatchAllocation,
  channelSnapshotDispatchIntentHash,
  channelSnapshotJobPayload,
  channelSnapshotQueueJobIdentityMatches,
  ChannelSnapshotDispatchConflictError,
} from "./channelSnapshotDispatch.js";

const ACTIVE_JOB_STATES = new Set([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "waiting-children",
]);

export async function reconcileChannelCandidateQueue({
  actions,
  dispatchBatchId,
  query,
  queue,
  withTransaction,
  safeJobId,
  minSubscriberCount,
  staleSeconds,
  maxAttempts,
  retrySeconds,
  limit,
} = {}) {
  if (!dispatchBatchId) return 0;
  if (!Array.isArray(actions)) throw new TypeError("actions must be an array");
  if (typeof query !== "function") throw new TypeError("query is required");
  if (!queue || typeof queue.getJob !== "function") throw new TypeError("queue is required");
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (typeof safeJobId !== "function") throw new TypeError("safeJobId is required");

  const rows = await query(
    `SELECT candidate.candidate_id,candidate.channel_id,candidate.channel_url,
            candidate.pipeline_cycle_id,candidate.priority,candidate.status,
            candidate.snapshot_dispatch_generation,
            candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            candidate.source_json->>'source' AS candidate_source,
            source.query_id,source.query_text,
            migration_intent.migration_intent_id,
            snapshot_outbox.deterministic_job_id AS snapshot_outbox_job_id,
            snapshot_outbox.payload_json AS snapshot_outbox_payload,
            snapshot_outbox.intent_hash AS snapshot_outbox_intent_hash
     FROM crawler.channel_candidates candidate
     LEFT JOIN crawler.migration_channel_intents migration_intent
       ON migration_intent.target_candidate_id=candidate.candidate_id
     LEFT JOIN LATERAL (
       SELECT candidate_source.query_id,candidate_source.query_text
       FROM crawler.channel_candidate_sources candidate_source
       WHERE candidate_source.candidate_id=candidate.candidate_id
       ORDER BY candidate_source.created_at,candidate_source.candidate_source_id
       LIMIT 1
     ) source ON true
     LEFT JOIN LATERAL (
       SELECT outbox.deterministic_job_id,outbox.payload_json,outbox.intent_hash
       FROM crawler.proxy_job_dispatch_outbox outbox
       WHERE outbox.aggregate_kind='channel_snapshot'
         AND outbox.aggregate_id=candidate.candidate_id::text
         AND (outbox.payload_json->>'dispatch_generation')::bigint
               =candidate.snapshot_dispatch_generation
       LIMIT 1
     ) snapshot_outbox ON true
     WHERE candidate.dispatch_batch_id=$1
       AND NOT (candidate.snapshot_json ? 'parser_contract_error')
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.migration_system_retry_items active_system_retry
         WHERE active_system_retry.candidate_id=candidate.candidate_id
           AND active_system_retry.status IN ('retrying','pending','dispatched')
       )
       AND (
         (
           candidate.status IN ('discovered','queued')
           AND (candidate.next_retry_at IS NULL OR candidate.next_retry_at<=now())
         )
         OR (
           candidate.status='validating'
           AND candidate.updated_at<=now()-($2::int * interval '1 second')
         )
         OR (
           candidate.status='failed'
           AND candidate.snapshot_attempts<$3
           AND candidate.updated_at<=now()-($4::int * interval '1 second')
         )
       )
     ORDER BY candidate.priority DESC,candidate.created_at
     LIMIT $5`,
    [dispatchBatchId, staleSeconds, maxAttempts, retrySeconds, limit],
  );
  let enqueued = 0;
  for (const row of rows.rows) {
    const generation = Number(row.snapshot_dispatch_generation ?? 0);
    const legacyJobId = safeJobId("channel-snapshot", dispatchBatchId, row.channel_id);
    const currentJobId = generation > 0
      ? safeJobId("channel-snapshot", dispatchBatchId, row.channel_id, `g${generation}`)
      : legacyJobId;
    const currentPayload = generation > 0
      ? channelSnapshotJobPayload({
        ...row,
        snapshot_dispatch_generation: generation,
      }, dispatchBatchId, { minSubscriberCount })
      : null;
    const persistedOutboxPayload = row.snapshot_outbox_payload
      && typeof row.snapshot_outbox_payload === "object"
      && !Array.isArray(row.snapshot_outbox_payload)
      ? row.snapshot_outbox_payload
      : currentPayload;
    const persistedOutboxIntentHash = String(
      row.snapshot_outbox_intent_hash
        || (persistedOutboxPayload
          ? channelSnapshotDispatchIntentHash(persistedOutboxPayload)
          : ""),
    );
    const expectedJobId = String(row.snapshot_outbox_job_id ?? "").trim() || currentJobId;
    let represented = false;
    let terminalCurrentJob = null;
    for (const existingJobId of new Set([expectedJobId, currentJobId, legacyJobId])) {
      const existing = await queue.getJob(existingJobId);
      if (!existing) continue;
      const state = await existing.getState();
      const currentIdentityMatches = generation > 0
        ? channelSnapshotQueueJobIdentityMatches(existing, {
          jobId: expectedJobId,
          payload: persistedOutboxPayload,
          identityPayload: currentPayload,
          intentHash: persistedOutboxIntentHash,
          migrationIntentId: row.migration_intent_id,
        })
        : true;
      if (!currentIdentityMatches) {
        actions.push({
          action: "hold-channel-snapshot-job-identity-conflict",
          candidate_id: Number(row.candidate_id),
          dispatch_generation: generation,
          job_id: existingJobId,
          state,
        });
        represented = true;
        break;
      }
      if (ACTIVE_JOB_STATES.has(state)) {
        represented = true;
        break;
      }
      if (existingJobId === expectedJobId) terminalCurrentJob = existing;
    }
    if (represented) continue;
    const activeJobId = String(row.snapshot_active_job_id ?? "").trim() || null;
    if (activeJobId && (activeJobId !== expectedJobId || !terminalCurrentJob)) {
      actions.push({
        action: "hold-channel-snapshot-active-job-fence",
        candidate_id: Number(row.candidate_id),
        dispatch_generation: generation,
        job_id: activeJobId,
      });
      continue;
    }
    const nextGeneration = generation + 1;
    const jobId = safeJobId(
      "channel-snapshot",
      dispatchBatchId,
      row.channel_id,
      `g${nextGeneration}`,
    );
    const allocation = buildChannelSnapshotRedispatchAllocation(row, dispatchBatchId, {
      expectedGeneration: generation,
      previousJobId: activeJobId,
      previousJobAttempt: activeJobId == null
        ? null
        : Number(row.snapshot_active_job_attempt),
      jobId,
      minSubscriberCount,
    });
    try {
      await withTransaction((client) => allocateChannelSnapshotDispatchOutbox(client, allocation));
    } catch (error) {
      if (!(error instanceof ChannelSnapshotDispatchConflictError)) throw error;
      actions.push({
        action: "hold-channel-snapshot-dispatch-conflict",
        candidate_id: Number(row.candidate_id),
        dispatch_generation: generation,
        reason: error.message,
      });
      continue;
    }
    enqueued += 1;
  }
  if (enqueued > 0) {
    actions.push({
      action: "reconcile-channel-snapshots",
      dispatch_batch_id: dispatchBatchId,
      enqueued,
    });
  }
  return enqueued;
}
