import { currentChannelExecution } from './channelExecutionContext.js';
import { isVideoApiReplay } from './videoApiContinuation.js';
import { claimVideoExecution, ownsVideoExecution, videoExecutionScope, VideoExecutionRecoveryPendingError } from './videoExecutionRecovery.js';

const sequence = attempt => Number(attempt.attempt_number ?? Number(attempt.job_attempt) + 1);
const finished = attempt => attempt.status !== 'running' && Boolean(attempt.finished_at);
function managedAdmission(attempt, runId) {
  return Boolean(attempt.task_id && attempt.workload_scope
    && attempt.business_run_id === runId && attempt.attempt_id === `channel-attempt:${attempt.task_id}`
    && Number.isSafeInteger(Number(attempt.attempt_number)) && Number(attempt.attempt_number) > 0);
}
function stale(runId) {
  const error = new Error(`Incremental video execution was superseded: ${runId}`);
  error.code = 'CONTENT_DETAIL_EXECUTION_FENCE_STALE';
  return error;
}

export async function claimIncrementalVideoExecution(client, { plan, runId, cycleKey }) {
  const attemptId = currentChannelExecution()?.attempt_id;
  const replay = isVideoApiReplay();
  // Unmanaged fixture/compatibility callers have no authority to clear an old claim.
  if (!attemptId && !replay) return null;
  const run = (await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE',[runId])).rows[0];
  if (!run || run.crawl_mode !== 'incremental' || run.channel_id !== plan.channel_id
    || String(run.plan_id) !== String(plan.plan_id)) throw stale(runId);
  const attempts = (await client.query(`SELECT * FROM crawler.channel_execution_attempts
    WHERE run_id=$1 AND channel_id=$2 ORDER BY attempt_number,job_attempt,attempt_id FOR UPDATE`,
  [runId,plan.channel_id])).rows;
  // Local API continuation runs without a network session; it may reuse only the
  // latest, already finished execution. The existing API gate validates its request.
  const current = attemptId ? attempts.find(a => a.attempt_id === attemptId)
    : attempts.reduce((latest,a) => !latest || sequence(a) > sequence(latest) ? a : latest,null);
  if (!current && !attemptId && !run.detail_active_job_id) return null;
  if (!current || current.identity_changed || current.job_id !== plan.job_id || current.queue_name !== 'youtube-channel-incremental'
    || Number(current.dispatch_generation) !== Number(plan.dispatch_generation)
    || !Number.isSafeInteger(sequence(current)) || sequence(current) < 1
    || (replay ? !finished(current) : current.status !== 'running' || current.finished_at)) throw stale(runId);
  if (attempts.some(a => a.attempt_id !== current.attempt_id && sequence(a) >= sequence(current))) throw stale(runId);
  // Rota has a unique active task per workload/business run. A later admitted
  // task therefore supersedes an older task even if a killed local process never
  // finished its PostgreSQL attempt record. Do not infer this from time alone.
  const superseded = a => managedAdmission(current, runId) && managedAdmission(a, runId)
    && a.workload_scope === current.workload_scope && sequence(a) < sequence(current);
  if (attempts.some(a => a.attempt_id !== current.attempt_id && !finished(a) && !superseded(a))) {
    throw new VideoExecutionRecoveryPendingError(runId);
  }
  const previous = videoExecutionScope(run);
  if (run.detail_active_job_id && (previous?.kind !== 'incremental_video'
    || !attempts.some(a => a.attempt_id === previous.attempt_id))) throw stale(runId);
  const fence = Object.freeze({ runId,channelId: plan.channel_id,jobId: plan.job_id,
    jobAttempt: sequence(current),jobEpoch: Number(run.detail_job_epoch),attemptId: current.attempt_id,replay,
    scopeKey: JSON.stringify({ version: 1,kind: 'incremental_video',plan_id: plan.plan_id,
      attempt_id: current.attempt_id,cycle_key: cycleKey,dispatch_generation: plan.dispatch_generation }) });
  if (!run.detail_active_job_id && !attempts.some(a => a.attempt_id !== current.attempt_id && (finished(a) || superseded(a)))) {
    const active = (await client.query(`SELECT 1 FROM crawler.incremental_youtubejs_video_items
      WHERE run_id=$1 AND cycle_key=$2 AND status='claimed' AND claim_expires_at>clock_timestamp() LIMIT 1`,[runId,cycleKey])).rowCount;
    if (active) throw new VideoExecutionRecoveryPendingError(runId);
  }
  const claimed = await claimVideoExecution(client,run,fence,{
    supersedesOwner: previous?.attempt_id === current.attempt_id && previous.cycle_key !== cycleKey,
    recovery: { kind: 'incremental',cycleKey,previousExecutionFinished: replay },
  });
  if (!claimed) throw stale(runId);
  return fence;
}

export async function lockIncrementalVideoExecution(client, fence) {
  const row = (await client.query(`SELECT run.*,attempt.status AS execution_status,attempt.finished_at AS execution_finished_at,
      attempt.identity_changed AS execution_identity_changed
    FROM crawler.channel_runs run JOIN crawler.channel_execution_attempts attempt ON attempt.attempt_id=$2 AND attempt.run_id=run.run_id
    WHERE run.run_id=$1 FOR UPDATE OF run,attempt`,[fence.runId,fence.attemptId])).rows[0];
  if (!row || row.execution_identity_changed || !ownsVideoExecution(row,fence) || (fence.replay
    ? row.execution_status === 'running' || !row.execution_finished_at
    : row.execution_status !== 'running' || row.execution_finished_at)) throw stale(fence.runId);
}
