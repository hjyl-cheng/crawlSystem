import { DelayedError } from 'bullmq';
import { tryLockPublicationChannelMutation } from './publicationChannelMutationLock.js';

export class FinalizeDeferredError extends Error {
  constructor(channelId, runId, reason, cause) {
    super(`Finalize deferred: ${reason}`, { cause });
    this.code = 'FINALIZE_DEFERRED';
    Object.assign(this, { channelId, runId, reason });
  }
}

// Every Finalize transaction, including COMMIT-time FK checks, is protected.
// Short row waits also protect mixed-version writers during rollout/rollback.
export function finalizeTransactions(withTransaction, { channelId, runId, secondary }) {
  if (!secondary) return withTransaction;
  return async action => {
    try {
      return await withTransaction(async client => {
        await client.query("SET LOCAL lock_timeout='100ms'");
        if (!await tryLockPublicationChannelMutation(client, channelId)) {
          throw new FinalizeDeferredError(channelId, runId, 'channel_writer_busy');
        }
        return action(client);
      });
    } catch (error) {
      // The transaction owner has rolled back before this escapes to the worker.
      if (['55P03', '40P01'].includes(error?.code)) {
        throw new FinalizeDeferredError(channelId, runId, 'row_lock_busy', error);
      }
      throw error;
    }
  };
}

export async function persistFinalizeDeferral(query, { channelId, runId, jobId = null, reason }) {
  const result = await query(`INSERT INTO crawler.finalize_recovery_requests AS r
    (channel_id,defer_run_id,defer_job_id,defer_until,defer_count,first_deferred_at,defer_reason)
    SELECT $1,$2,$3,now()+((5+random()*10)*interval '1 second'),1,now(),$4
    FROM crawler.channels c WHERE c.channel_id=$1
      AND $2=CASE WHEN c.status='dormant' THEN c.registry_promotion_run_id ELSE c.latest_run_id END
    ON CONFLICT(channel_id) DO UPDATE SET
      requested_generation=GREATEST(r.requested_generation,r.handled_generation+1),
      defer_run_id=$2,defer_job_id=CASE WHEN r.defer_run_id=$2 THEN COALESCE($3,r.defer_job_id) ELSE $3 END,
      defer_count=CASE WHEN r.defer_run_id=$2 THEN r.defer_count+1 ELSE 1 END,
      first_deferred_at=CASE WHEN r.defer_run_id=$2 THEN COALESCE(r.first_deferred_at,now()) ELSE now() END,
      defer_until=now()+(LEAST(60,5*power(2,LEAST(r.defer_count,4))+random()*10)*interval '1 second'),
      defer_reason=$4,last_decision='deferred',updated_at=now()
    WHERE r.defer_job_id IS NULL OR r.defer_job_id=$3 OR r.defer_until<=now() OR r.defer_run_id<>$2
    RETURNING defer_until,defer_count,first_deferred_at`, [channelId,runId,jobId,reason]);
  return result.rows[0] ?? null;
}

export async function delayFinalizeJob({ query, job, token, error }) {
  const deferred = await persistFinalizeDeferral(query, {
    channelId: error.channelId, runId: error.runId, jobId: String(job.id), reason: error.reason,
  });
  const timestamp = deferred ? new Date(deferred.defer_until).getTime() : Date.now()+10000;
  // Durable demand precedes Redis. Lost acknowledgement or process death is
  // recovered through the same Job ID (stalled) or the Controller request row.
  await job.moveToDelayed(timestamp, token);
  console.log(JSON.stringify({ event: 'finalize_deferred', channel_id: error.channelId,
    run_id: error.runId, job_id: job.id, reason: error.reason,
    defer_until: new Date(timestamp).toISOString(), defer_count: deferred?.defer_count }));
  throw new DelayedError();
}

export async function secondaryFinalizeBusy(query, channelId, runId) {
  const result = await query(`SELECT 1 FROM crawler.channel_runs full_run
    WHERE full_run.run_id=$2 AND full_run.channel_id=$1
      AND full_run.publication_finalized_at IS NOT NULL
      AND full_run.publication_finalized_status IN ('ready_auto','ready_partial')
      AND EXISTS(SELECT 1 FROM crawler.channel_execution_attempts a
        JOIN crawler.channel_runs r ON r.run_id=a.run_id
        WHERE a.channel_id=$1 AND a.status='running' AND a.finished_at IS NULL
          AND r.crawl_mode='incremental' AND r.status='running'
          AND a.updated_at>now()-interval '60 seconds')`, [channelId,runId]);
  return result.rows.length > 0;
}

// Used by both the bounded audit and the legacy Controller loop.
export async function postponeSecondaryFinalize(query, channelId, runId) {
  const deferred = await query(`SELECT 1 FROM crawler.finalize_recovery_requests
    WHERE channel_id=$1 AND defer_run_id=$2 AND defer_until>now()`, [channelId,runId]);
  if (deferred.rows.length) return true;
  if (!await secondaryFinalizeBusy(query,channelId,runId)) return false;
  await persistFinalizeDeferral(query,{channelId,runId,reason:'incremental_executing'});
  return true;
}
