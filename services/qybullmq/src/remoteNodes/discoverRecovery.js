import { DISCOVER_PAGE_CAPABILITY } from './discoverPageContract.js';
import { supervisionLockKey } from './centerExecutionRecovery.js';
import { RemoteProtocolError } from './protocol.js';

// A Discover slot may take new pages only when no delivery is open and its
// network is retired. The Page Intent itself is recovered by BullMQ: a stalled
// job is retried and claims its page again, because the page is never `done`.
export async function remoteDiscoverSlotUnsettled(queryable, row) {
  return (await queryable.query(`SELECT 1 WHERE EXISTS(
      SELECT 1 FROM remote_ingestion.tasks
      WHERE target_node_id=$1 AND target_worker_slot=$2 AND capability=$3 AND state IN ('pending','leased'))
    OR EXISTS(SELECT 1 FROM remote_ingestion.network_bindings WHERE node_id=$1 AND slot=$2 AND state<>'retired')`,
  [row.node_id, row.slot, DISCOVER_PAGE_CAPABILITY])).rowCount > 0;
}

// Runs on the PG session holding this slot's supervision lock, as the
// incremental recovery does: losing the session rolls both back together.
export async function recoverRemoteDiscoverSlot({ guard, row, lockKey }) {
  if (lockKey !== supervisionLockKey(row)) throw new RemoteProtocolError('REMOTE_RECOVERY_NOT_OWNER');
  await guard.query('BEGIN');
  try {
    const owns = await guard.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted
      AND classid=781138012::oid AND objid=(hashtext($1)::bigint & 4294967295)::oid AND objsubid=2`, [lockKey]);
    if (!owns.rowCount) throw new RemoteProtocolError('REMOTE_RECOVERY_NOT_OWNER');
    await guard.query("SET LOCAL lock_timeout='2s'");
    await guard.query("SET LOCAL statement_timeout='5s'");
    await guard.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE', [row.node_id]);
    const slot = (await guard.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2',
      [row.node_id, row.slot])).rows[0];
    if (slot?.rota_worker_id !== row.rota_worker_id) throw new RemoteProtocolError('NETWORK_SLOT_CONFLICT');
    const tasks = (await guard.query(`SELECT t.* FROM remote_ingestion.tasks t
      WHERE t.target_node_id=$1 AND t.target_worker_slot=$2 AND t.capability=$3
        AND (t.state IN ('pending','leased') OR EXISTS(SELECT 1 FROM remote_ingestion.network_bindings b
          WHERE b.task_id=t.task_id AND b.state<>'retired'))
      ORDER BY t.created_at LIMIT 32 FOR UPDATE OF t`, [row.node_id, row.slot, DISCOVER_PAGE_CAPABILITY])).rows;
    let closed = 0;
    for (const task of tasks) {
      await guard.query(`UPDATE remote_ingestion.tasks SET
        state=CASE WHEN state IN ('pending','leased') THEN 'failed' ELSE state END,
        last_error=CASE WHEN state IN ('pending','leased') THEN 'REMOTE_CENTER_INTERRUPTED' ELSE last_error END,
        coordinator_id=NULL,coordinator_until=NULL WHERE task_id=$1`, [task.task_id]);
      const networkSlot = (await guard.query('SELECT * FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE',
        [row.node_id, row.slot])).rows[0];
      const bindings = (await guard.query('SELECT * FROM remote_ingestion.network_bindings WHERE task_id=$1 FOR UPDATE',
        [task.task_id])).rows;
      for (const binding of bindings) {
        if (binding.state === 'retired') continue;
        // A never-issued grant cannot have sent requests; an issued one must be
        // released by the node with its zero-in-flight receipt.
        const unissued = binding.state === 'bound' && networkSlot.binding_id === binding.binding_id && !networkSlot.grant_until;
        await guard.query(`UPDATE remote_ingestion.network_bindings SET stop_requested=true,
          state=CASE WHEN $2 THEN 'retired' ELSE state END,
          release_receipt=CASE WHEN $2 THEN '{"stopped_before_grant":true,"in_flight":0}'::jsonb ELSE release_receipt END,
          retired_at=CASE WHEN $2 THEN clock_timestamp() ELSE retired_at END WHERE binding_id=$1`, [binding.binding_id, unissued]);
      }
      closed++;
    }
    const settled = !await remoteDiscoverSlotUnsettled(guard, row);
    await guard.query('COMMIT');
    return { settled, closed };
  } catch (error) { await guard.query('ROLLBACK').catch(() => {}); throw error; }
}
