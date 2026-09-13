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
      WHERE batch_id=$1 AND state='pending') AS pending`, [batchId],
  )).rows[0].pending;
  if (pending) throw conflict('migration_controlled_retry_blocked');
  return batch;
}

export async function lockControlledMigrationRetryItem(client, batch, retry) {
  if (retry.failed_dispatch_batch_id !== batch.batch_id || retry.dispatch_batch_id !== batch.batch_id) {
    throw conflict('migration_controlled_retry_item_changed');
  }
  const item = (await client.query(
    `SELECT state,outcome FROM crawler.migration_control_items
     WHERE batch_id=$1 AND channel_id=$2 AND candidate_id=$3 FOR UPDATE`,
    [batch.batch_id, retry.channel_id, retry.candidate_id],
  )).rows[0];
  if (!(item?.state === 'terminal' && item.outcome === 'failed')
      && !(item?.state === 'started' && retry.status === 'dispatched')) {
    throw conflict('migration_controlled_retry_item_changed');
  }
}

export async function reopenControlledMigrationRetryItem(client, batch, retry) {
  const changed = await client.query(
    `UPDATE crawler.migration_control_items SET state='started',outcome=NULL,
       finished_at=NULL,error_message=NULL
     WHERE batch_id=$1 AND channel_id=$2 AND candidate_id=$3
       AND ((state='terminal' AND outcome='failed') OR state='started') RETURNING channel_id`,
    [batch.batch_id, retry.channel_id, retry.candidate_id],
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
