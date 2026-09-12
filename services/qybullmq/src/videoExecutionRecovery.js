// Shared ownership and checkpoint recovery used by Full Crawl and Incremental.
// The caller locks and validates its business scope before entering this module.
const text = value => String(value ?? '').trim() || null;

export function ownsVideoExecution(run, fence) {
  return text(run.detail_active_job_id) === fence.jobId
    && Number(run.detail_active_job_attempt) === fence.jobAttempt
    && text(run.detail_active_scope_key) === fence.scopeKey
    && Number(run.detail_active_job_epoch) === fence.jobEpoch;
}

export function videoExecutionScope(run) {
  try {
    const scope = JSON.parse(run.detail_active_scope_key ?? 'null');
    return scope && typeof scope === 'object' && !Array.isArray(scope) ? scope : null;
  } catch { return null; }
}

function canClaim(run, fence, supersedesOwner) {
  if (run.detail_active_job_id == null && run.detail_active_job_attempt == null
    && run.detail_active_scope_key == null && run.detail_active_job_epoch == null) return true;
  if (ownsVideoExecution(run,fence)) return true;
  if (text(run.detail_active_job_id) === fence.jobId && Number(run.detail_active_job_epoch) === fence.jobEpoch
    && Number(run.detail_active_job_attempt) < fence.jobAttempt) return true;
  return Number(run.detail_active_job_epoch) === fence.jobEpoch && supersedesOwner;
}

export async function claimVideoExecution(client, run, fence, { supersedesOwner = false, recovery = null } = {}) {
  if (Number(run.detail_job_epoch) !== fence.jobEpoch || !canClaim(run,fence,supersedesOwner)) return null;
  const previousOwner = ownsVideoExecution(run,fence);
  const claimed = await client.query(`UPDATE crawler.channel_runs
    SET detail_active_job_id=$2,detail_active_job_attempt=$3,
      detail_active_scope_key=$4,detail_active_job_epoch=$5,updated_at=now()
    WHERE run_id=$1 RETURNING run_id`, [fence.runId,fence.jobId,fence.jobAttempt,fence.scopeKey,fence.jobEpoch]);
  if (claimed.rowCount !== 1) return null;
  // Recovery and ownership change commit together. Completed items are never reset.
  if (recovery?.kind === 'full') {
    await client.query(`UPDATE crawler.content_candidates SET detail_status='queued',
      result_json=result_json || jsonb_build_object('full_crawl_recovered_at',now()),updated_at=now()
      WHERE run_id=$1 AND detail_status='running'`, [fence.runId]);
  } else if (recovery?.kind === 'incremental' && (!previousOwner || recovery.previousExecutionFinished)) {
    await client.query(`UPDATE crawler.incremental_youtubejs_video_items item
      SET status='pending',claim_token=NULL,claim_expires_at=NULL,updated_at=clock_timestamp()
      FROM crawler.incremental_youtubejs_video_batches batch
      WHERE item.run_id=$1 AND item.cycle_key=$2 AND item.status='claimed'
        AND batch.run_id=item.run_id AND batch.cycle_key=item.cycle_key AND batch.status='fetching'`,
    [fence.runId,recovery.cycleKey]);
  }
  return fence;
}

export class VideoExecutionRecoveryPendingError extends Error {
  constructor(runId, delayMs = 15000) {
    super(`Video execution is waiting for safe recovery: ${runId}`);
    this.code = 'VIDEO_EXECUTION_RECOVERY_PENDING';
    this.runId = runId;
    this.delayMs = Math.max(1000,Math.min(300000,Number(delayMs) || 15000));
  }
}

export const isVideoExecutionRecoveryPending = error => error?.code === 'VIDEO_EXECUTION_RECOVERY_PENDING';
