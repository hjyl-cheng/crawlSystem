import { claimChannelScan, scanTransaction, finishChannelScan, releaseChannelScan } from './backgroundReconciliationScan.js';
import { FINALIZABLE_CHANNEL_STATUSES, SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES } from './finalizePolicy.js';

export async function reconcileTerminalFinalizedRunPage({ query, withTransaction, pipelineCycleId = null, pageSize = 100, roundPauseMs = 0 }) {
  // The unique (dispatch_batch_id,channel_id) index narrows active-cycle audits.
  // Older Runs without candidate membership are covered by the global audit;
  // the existing open-work check still prevents premature pipeline completion.
  const batchId = pipelineCycleId == null ? null : (await query(`SELECT dispatch_batch_id
    FROM crawler.query_dispatch_batches WHERE pipeline_cycle_id=$1`, [pipelineCycleId])).rows[0]?.dispatch_batch_id ?? null;
  const options = { withTransaction, scope: `terminal-runs:${pipelineCycleId ?? 'global'}`, pageSize, roundPauseMs, batchId };
  const claim = await claimChannelScan(options);
  if (!claim) return { busy: true, wrapped: false, examined: 0, updated: 0, run_ids: [] };
  try {
    const rows = claim.ids.length ? await scanTransaction(options, claim, client => client.query(`/* terminal-runs:bounded-page */
      WITH page AS MATERIALIZED (
        SELECT channel_id,latest_run_id FROM crawler.channels
        WHERE channel_id=ANY($1::text[]) AND status=ANY($3::text[])
      )
      UPDATE crawler.channel_runs run SET status='done',detail_status='done',
        finished_at=COALESCE(run.finished_at,now()),updated_at=now()
      FROM page channel,crawler.finalized_profiles finalized
      WHERE channel.latest_run_id=run.run_id AND channel.channel_id=run.channel_id
        AND finalized.channel_id=channel.channel_id AND finalized.run_id=run.run_id
        AND finalized.status=ANY($4::text[])
        AND ($2::text IS NULL OR run.result_json->>'pipeline_cycle_id'=$2::text)
        AND (run.status<>'done' OR run.detail_status<>'done' OR run.finished_at IS NULL)
      RETURNING run.run_id,run.channel_id`,
    [claim.ids, pipelineCycleId, FINALIZABLE_CHANNEL_STATUSES, SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES])) : { rows: [] };
    // Lost completion responses replay an idempotent update; a failed update
    // never commits the cursor. No network calls occur in the page transaction.
    const progress = await finishChannelScan(options, claim);
    return { ...progress, examined: claim.ids.length, updated: rows.rows.length, run_ids: rows.rows.map(r => r.run_id) };
  } finally { await releaseChannelScan(options, claim); }
}
