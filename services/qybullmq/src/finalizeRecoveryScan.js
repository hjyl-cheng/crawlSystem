import { loadFinalizeRecoveryCandidates } from './finalizeRecoveryPolicy.js';
import { dispatchFinalizeForRun } from './finalizeDispatch.js';

// Globally scan current/promotion Runs, including work whose scheduler has ended.
// Cursor advances only after every page dispatch succeeds. A crash replays the
// same source-revision Job IDs. Database ownership protects duplicate processes.
export function createFinalizeRecoveryScan({ query, withTransaction, queue, pageSize = 200, roundPauseMs = 0, registerChanges = false }) {
  let currentPageSize = Math.max(1, Math.min(1000, Math.floor(pageSize) || 200));
  return async function scan() {
    const size = currentPageSize;
    const claim = await withTransaction(async client => {
      await client.query("INSERT INTO crawler.finalize_recovery_scan(scope) VALUES ('global') ON CONFLICT DO NOTHING");
      return (await client.query(`UPDATE crawler.finalize_recovery_scan
        SET lease_token=gen_random_uuid(),lease_until=now()+interval '60 seconds'
        WHERE scope='global' AND (lease_until IS NULL OR lease_until<now())
          AND (upper_channel_id IS NOT NULL OR last_completed_at IS NULL OR last_completed_at<=now()-($1*interval '1 millisecond')) RETURNING *`, [roundPauseMs])).rows[0];
    });
    if (!claim) return { busy: true };
    try {
      let upper = claim.upper_channel_id;
      if (upper == null) upper = (await query('SELECT max(channel_id) AS id FROM crawler.channels')).rows[0].id;
      const ids = upper == null ? [] : (await query(`SELECT channel_id FROM crawler.channels
        WHERE channel_id>$1 AND channel_id<=$2 ORDER BY channel_id LIMIT $3`,
      [claim.after_channel_id, upper, size])).rows.map(row => row.channel_id);
      const candidates = ids.length ? await loadFinalizeRecoveryCandidates(query, { channelIds: ids, limit: size }) : [];
      for (const row of candidates) {
        if (registerChanges) {
          // Audit/backfill persists intent, preserving any live generation or
          // lease. A previously acknowledged gap must be made pending again.
          await query(`INSERT INTO crawler.finalize_recovery_requests(channel_id) VALUES($1)
            ON CONFLICT(channel_id) DO UPDATE SET
              requested_generation=CASE WHEN crawler.finalize_recovery_requests.requested_generation<=crawler.finalize_recovery_requests.handled_generation
                THEN crawler.finalize_recovery_requests.handled_generation+1 ELSE crawler.finalize_recovery_requests.requested_generation END,
              next_check_at=CASE WHEN crawler.finalize_recovery_requests.requested_generation<=crawler.finalize_recovery_requests.handled_generation
                THEN now() ELSE crawler.finalize_recovery_requests.next_check_at END`, [row.channel_id]);
          continue;
        }
        await dispatchFinalizeForRun({ query,
          queue: { add: (name, data, options) => queue.add(name, data, { ...options, priority: 100 }) },
          channelId: row.channel_id, runId: row.run_id, reason: 'controller-bounded-finalize-recovery' });
      }
      const wrapped = ids.length < size || ids.at(-1) === upper;
      await query(`UPDATE crawler.finalize_recovery_scan SET after_channel_id=$2,upper_channel_id=$3,
        completed_rounds=completed_rounds+$4,last_completed_at=CASE WHEN $4=1 THEN now() ELSE last_completed_at END,
        lease_until=NULL,lease_token=NULL,updated_at=now()
        WHERE scope='global' AND lease_token=$1`,
      [claim.lease_token, wrapped ? '' : ids.at(-1), wrapped ? null : upper, wrapped ? 1 : 0]);
      return { examined: ids.length, eligible: candidates.length, dispatched: registerChanges ? 0 : candidates.length, registered: registerChanges ? candidates.length : 0, wrapped };
    } catch (error) {
      // Production channels can contain many more videos than the benchmark.
      // Retry the same cursor with less work after a query timeout; never skip
      // an expensive page or treat its channels as failed.
      if (error.code === '57014' && size > 1) currentPageSize = Math.max(1, Math.floor(size / 2));
      await query("UPDATE crawler.finalize_recovery_scan SET lease_until=NULL,lease_token=NULL WHERE scope='global' AND lease_token=$1", [claim.lease_token]).catch(() => {});
      throw error;
    }
  };
}
