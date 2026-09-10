import { readFinalizeSource } from "./finalizeSourceFence.js";
import { finalizedProfileIsCurrent } from "./finalizePolicy.js";
import { loadFinalizeRecoveryCandidates } from './finalizeRecoveryPolicy.js';
import { dispatchFinalizeForRun } from './finalizeDispatch.js';

export function createFinalizeChangeRecovery({ query, withTransaction, queue, limit = 40 }) {
  return async function recover() {
    const rows = (await withTransaction(client => client.query(`WITH due AS (
      SELECT channel_id FROM crawler.finalize_recovery_requests
      WHERE requested_generation>handled_generation AND next_check_at<=now()
        AND (lease_until IS NULL OR lease_until<now())
      ORDER BY next_check_at,channel_id LIMIT $1 FOR UPDATE SKIP LOCKED
    ) UPDATE crawler.finalize_recovery_requests r SET lease_token=gen_random_uuid(),
      lease_until=now()+interval '60 seconds' FROM due
      WHERE r.channel_id=due.channel_id RETURNING r.*`, [limit]))).rows;
    let dispatched = 0;
    for (const row of rows) {
      try {
        // Once this exact generation has been dispatched, follow the existing
        // Job instead of recomputing all source aggregates on every poll.
        if (row.dispatched_job_id && row.dispatched_generation === row.requested_generation) {
          const job = await queue.getJob(row.dispatched_job_id);
          const state = job ? await job.getState() : null;
          if (['waiting', 'active', 'delayed', 'prioritized', 'waiting-children', 'paused'].includes(state)) {
            await query(`UPDATE crawler.finalize_recovery_requests SET lease_token=NULL,lease_until=NULL,
              next_check_at=CASE WHEN requested_generation>$3 THEN now() ELSE now()+interval '30 seconds' END,last_decision='waiting_job'
              WHERE channel_id=$1 AND lease_token=$2`, [row.channel_id,row.lease_token,row.requested_generation]);
            continue;
          }
          if (state === 'completed') {
            const finished = (await query(`SELECT 1 FROM crawler.channel_runs r
              JOIN crawler.channels c ON c.channel_id=r.channel_id
              JOIN crawler.finalized_profiles f ON f.channel_id=c.channel_id AND f.run_id=r.run_id
              WHERE r.run_id=$2 AND c.channel_id=$1 AND r.detail_status='done'
                AND r.run_id=CASE WHEN c.status='dormant' THEN c.registry_promotion_run_id ELSE c.latest_run_id END
                AND r.publication_finalized_at IS NOT NULL
                AND r.publication_finalized_status IN ('ready_auto','ready_partial')
                AND f.status=r.publication_finalized_status
                AND f.quality_json->>'source_revision'=$3`, [row.channel_id,row.dispatched_run_id,job.returnvalue?.source_revision ?? null])).rows.length;
            if (finished) {
              await query(`UPDATE crawler.finalize_recovery_requests SET handled_generation=GREATEST(handled_generation,$3),
                lease_token=NULL,lease_until=NULL,next_check_at=now(),updated_at=now(),last_decision='completed'
                WHERE channel_id=$1 AND lease_token=$2`, [row.channel_id,row.lease_token,row.requested_generation]);
              continue;
            }
          }
        }
        const candidates = await loadFinalizeRecoveryCandidates(query, { channelIds: [row.channel_id], limit: 1, includeCurrent: true });
        let receipt = null;
        let completed = false;
        let decision = "not_ready";
        if (candidates.length) {
          // A new source generation is the reason to do this bounded read once.
          // Compare the actual source fingerprint, not just wall-clock times.
          const source = await readFinalizeSource(query, { channelId: row.channel_id, runId: candidates[0].run_id });
          const existing = (await query("SELECT run_id,status,quality_json FROM crawler.finalized_profiles WHERE channel_id=$1", [row.channel_id])).rows[0];
          const observations = (await query(`SELECT DISTINCT ON(observation_kind) observation_kind,outcome
            FROM crawler.crawl_observations WHERE channel_id=$1 AND run_id=$2
            ORDER BY observation_kind,kind_sequence DESC`, [row.channel_id,candidates[0].run_id])).rows;
          const outcomes = Object.fromEntries(observations.map(observation => [observation.observation_kind,observation.outcome]));
          completed = source && ['ready_auto','ready_partial'].includes(existing?.status)
            && source.run.publication_finalized_at != null
            && finalizedProfileIsCurrent(existing, candidates[0].run_id, source.sourceRevision, outcomes);
          if (!completed) {
            receipt = await dispatchFinalizeForRun({ query, queue, channelId: row.channel_id, runId: candidates[0].run_id, reason: 'controller-finalize-source-change' });
            const job = await queue.getJob(receipt.jobId);
            const state = job ? await job.getState() : null;
            if (['completed', 'failed'].includes(state)) await job.retry(state);
            dispatched += 1;
            decision = "dispatched";
          }
        } else {
          // This generation was inspected and is not eligible. Consume the
          // check, never finalize it. The transactionally captured next source
          // change reopens it; the bounded audit remains the missed-event net.
          completed = true;
        }
        await query(`UPDATE crawler.finalize_recovery_requests SET
          handled_generation=CASE WHEN $4 THEN GREATEST(handled_generation,$3) ELSE handled_generation END,
          next_check_at=CASE WHEN requested_generation>$3 THEN now() ELSE now()+interval '30 seconds' END,
          dispatched_run_id=COALESCE($5,dispatched_run_id),dispatched_job_id=COALESCE($6,dispatched_job_id),
          dispatched_generation=CASE WHEN $6::text IS NULL THEN dispatched_generation ELSE $3 END,
          lease_token=NULL,lease_until=NULL,updated_at=now(),
          last_decision=$7
          WHERE channel_id=$1 AND lease_token=$2`,
        [row.channel_id, row.lease_token, row.requested_generation, completed, candidates[0]?.run_id ?? null, receipt?.jobId ?? null, candidates.length && completed ? "current" : decision]);
      } catch (error) {
        await query(`UPDATE crawler.finalize_recovery_requests SET lease_token=NULL,lease_until=NULL,
          next_check_at=now()+interval '15 seconds' WHERE channel_id=$1 AND lease_token=$2`, [row.channel_id, row.lease_token]).catch(() => {});
        throw error;
      }
    }
    return { checked: rows.length, dispatched };
  };
}
