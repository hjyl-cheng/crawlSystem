// Explicit operator recovery of a fixed cohort. Scheduler -> Batch -> Candidate
// lock order matches migration control; ordinary retry admission is unchanged.
function conflict(code) {
  return Object.assign(new Error(code), {code, statusCode: 409});
}

export async function lockControlledMigrationRetry(client, scheduler, batchId) {
  const batch = (await client.query(
    `SELECT batch_id,status,frozen_at FROM crawler.migration_control_batches
     WHERE batch_id=$1 FOR UPDATE`, [batchId],
  )).rows[0];
  const finishing = scheduler.status === 'finishing' && batch?.status === 'running';
  const completed = scheduler.status === 'stopped' && scheduler.stop_reason === 'pipeline_complete'
    && batch?.status === 'completed';
  if (scheduler.pipeline_cycle_id !== batchId || !batch?.frozen_at || (!finishing && !completed)) {
    throw conflict('migration_controlled_retry_blocked');
  }
  const pending = (await client.query(
    `SELECT EXISTS(SELECT 1 FROM crawler.migration_control_items
      WHERE batch_id=$1 AND state='pending'
        AND snapshot_json->>'migration_system_retry_id' IS NULL) AS pending`, [batchId],
  )).rows[0].pending;
  if (pending) throw conflict('migration_controlled_retry_blocked');
  return batch;
}

export async function lockControlledMigrationRetryItem(client, batch, retry) {
  if (retry.failed_dispatch_batch_id !== batch.batch_id || retry.dispatch_batch_id !== batch.batch_id) {
    throw conflict('migration_controlled_retry_item_changed');
  }
  const item = (await client.query(
    `SELECT state,outcome,snapshot_json FROM crawler.migration_control_items
     WHERE batch_id=$1 AND channel_id=$2 AND candidate_id=$3 FOR UPDATE`,
    [batch.batch_id, retry.channel_id, retry.candidate_id],
  )).rows[0];
  // A durable pending failure can precede the asynchronous batch settlement.
  // Its Candidate/Intent fence was verified before this lock; it is not live work.
  const queuedRetry = item?.state === 'pending' && item.outcome == null
    && String(item.snapshot_json?.migration_system_retry_id) === String(retry.system_retry_id)
    && retry.status === 'pending';
  if (!queuedRetry && !(item?.state === 'terminal' && item.outcome === 'failed')
      && !(item?.state === 'started' && item.outcome == null
        && ['pending', 'dispatched'].includes(retry.status))) {
    throw conflict('migration_controlled_retry_item_changed');
  }
  return item;
}

export async function reopenControlledMigrationRetryItem(client, batch, retry) {
  const changed = await client.query(
    `UPDATE crawler.migration_control_items SET state='started',outcome=NULL,
       started_at=COALESCE(started_at,now()),finished_at=NULL,error_message=NULL,
       snapshot_json=snapshot_json-'migration_system_retry_id'
     WHERE batch_id=$1 AND channel_id=$2 AND candidate_id=$3
       AND ((state='terminal' AND outcome='failed') OR state='started'
         OR (state='pending' AND outcome IS NULL AND snapshot_json->>'migration_system_retry_id'=$4)) RETURNING channel_id`,
    [batch.batch_id, retry.channel_id, retry.candidate_id, String(retry.system_retry_id)],
  );
  if (changed.rowCount !== 1) throw conflict('migration_controlled_retry_item_changed');
  if (batch.status === 'completed') {
    await client.query(`UPDATE crawler.migration_control_batches SET status='running',finished_at=NULL,
      version=version+1,updated_at=now() WHERE batch_id=$1`, [batch.batch_id]);
    await client.query(`UPDATE crawler.query_dispatch_batches SET status='running',finished_at=NULL,
      updated_at=now() WHERE dispatch_batch_id=$1`, [batch.batch_id]);
    await client.query(`UPDATE crawler.settings SET value_json=value_json||
      jsonb_build_object('status','finishing','stop_reason','controlled_migration_dispatch'),updated_at=now()
      WHERE setting_key='query_scheduler'`, []);
  }
}

// Restore only an operator-selected failed cohort. Existing successes and live
// generations never enter the initial-migration path or lose their ownership.
export async function restoreControlledMigrationFailures({
  withTransaction, batchId, systemRetryIds, failureCode, failureMessage,
}) {
  if (!Array.isArray(systemRetryIds) || systemRetryIds.length < 1 || systemRetryIds.length > 1000
      || systemRetryIds.some(id => !Number.isSafeInteger(Number(id)) || Number(id) < 1)
      || !failureCode || !failureMessage) throw new TypeError('A bounded, explicit failure cohort is required');
  return withTransaction(async client => {
    const scheduler = (await client.query(`SELECT value_json FROM crawler.settings
      WHERE setting_key='query_scheduler' FOR UPDATE`)).rows[0]?.value_json;
    const batch = await lockControlledMigrationRetry(client, scheduler ?? {}, batchId);
    if (batch.status !== 'running') throw conflict('migration_controlled_retry_blocked');
    const restored = await client.query(`UPDATE crawler.migration_control_items item
      SET state='pending',outcome=NULL,started_at=NULL,finished_at=NULL,error_message=NULL,
        snapshot_json=COALESCE(item.snapshot_json,'{}'::jsonb)||
          jsonb_build_object('migration_system_retry_id',retry.system_retry_id::text)
      FROM crawler.migration_system_retry_items retry,
        crawler.channel_candidates candidate,crawler.migration_channel_intents intent
      WHERE retry.system_retry_id=ANY($2::bigint[])
        AND retry.failed_dispatch_batch_id=$1 AND retry.status='pending'
        AND retry.failure_code=$3 AND retry.failure_evidence#>>'{system_failure,message}'=$4
        AND item.batch_id=$1 AND item.candidate_id=retry.candidate_id
        AND item.state='terminal' AND item.outcome='failed'
        AND candidate.candidate_id=retry.candidate_id AND candidate.channel_id=item.channel_id
        AND candidate.dispatch_batch_id=$1 AND candidate.status IN ('failed','accepted')
        AND candidate.snapshot_dispatch_generation=retry.failed_dispatch_generation
        AND candidate.snapshot_json->>'failure_type'='retryable_system_failure'
        AND intent.migration_intent_id=retry.migration_intent_id
        AND intent.target_candidate_id=candidate.candidate_id
        AND intent.dispatch_attempts=retry.failed_dispatch_generation
        AND ((candidate.snapshot_active_job_id=retry.failed_job_id
          AND candidate.snapshot_active_job_attempt=retry.failed_job_attempt)
          OR (candidate.status='accepted' AND candidate.snapshot_active_job_id IS NULL
            AND candidate.snapshot_active_job_attempt IS NULL
            AND candidate.snapshot_json->>'failed_dispatch_batch_id'=$1))
      RETURNING item.candidate_id,retry.system_retry_id`,
    [batchId,systemRetryIds,failureCode,failureMessage]);
    return restored.rows;
  });
}

// A mode selection, not a redispatch: only the frozen, unstarted members of an
// explicit failure cohort can be marked. Intake rechecks all execution fences
// and atomically allocates G+1 before resolving their old recovery record.
export async function markPendingMigrationFailuresForNormalExecution({
  withTransaction, batchId, systemRetryIds, failureCode, failureMessage,
}) {
  if (!Array.isArray(systemRetryIds) || !systemRetryIds.length || systemRetryIds.length > 500
      || systemRetryIds.some(id => !Number.isSafeInteger(Number(id)) || Number(id) < 1)
      || !failureCode || !failureMessage) throw new TypeError('A bounded, explicit failure cohort is required');
  return withTransaction(async client => {
    const scheduler = (await client.query(`SELECT value_json FROM crawler.settings
      WHERE setting_key='query_scheduler' FOR UPDATE`)).rows[0]?.value_json;
    const batch = await lockControlledMigrationRetry(client, scheduler ?? {}, batchId);
    if (batch.status !== 'running') throw conflict('migration_controlled_retry_blocked');
    const result = await client.query(`UPDATE crawler.migration_control_items item
      SET snapshot_json=item.snapshot_json||'{"migration_retry_mode":"normal"}'::jsonb
      FROM crawler.migration_system_retry_items retry, crawler.channel_candidates candidate
      WHERE retry.system_retry_id=ANY($2::bigint[]) AND retry.failed_dispatch_batch_id=$1
        AND retry.status='pending' AND retry.retry_dispatch_generation IS NULL
        AND retry.failure_code=$3 AND retry.failure_evidence#>>'{system_failure,message}'=$4
        AND item.batch_id=$1 AND item.candidate_id=retry.candidate_id
        AND item.state='pending' AND item.outcome IS NULL
        AND item.snapshot_json->>'migration_system_retry_id'=retry.system_retry_id::text
        AND item.snapshot_json->>'migration_retry_mode' IS DISTINCT FROM 'normal'
        AND candidate.candidate_id=retry.candidate_id AND candidate.channel_id=item.channel_id
        AND candidate.dispatch_batch_id=$1 AND candidate.status IN ('failed','accepted')
        AND candidate.snapshot_dispatch_generation=retry.failed_dispatch_generation
        AND candidate.snapshot_json->>'failure_type'='retryable_system_failure'
        AND ((candidate.snapshot_active_job_id=retry.failed_job_id
          AND candidate.snapshot_active_job_attempt=retry.failed_job_attempt)
          OR (candidate.status='accepted' AND candidate.snapshot_active_job_id IS NULL
            AND candidate.snapshot_active_job_attempt IS NULL
            AND candidate.snapshot_json->>'failed_dispatch_batch_id'=$1))
      RETURNING retry.system_retry_id,item.candidate_id,item.channel_id`,
    [batchId,systemRetryIds,failureCode,failureMessage]);
    return result.rows;
  });
}
