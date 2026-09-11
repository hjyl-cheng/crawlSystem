import { BrowserProfileStore } from '../browserProfileStore.js';
import { CHANNEL_PLAN_CAPABILITY } from './channelPlanContract.js';
import { RemoteProtocolError } from './protocol.js';

export const supervisionLockKey = row => `remote-incremental-supervisor:${row.node_id}/${row.slot}`;

export async function remoteSlotUnsettled(queryable, row) {
  return (await queryable.query(`SELECT 1 WHERE EXISTS(
    SELECT 1 FROM remote_ingestion.tasks t LEFT JOIN crawler.channel_execution_attempts a
      ON a.attempt_id=t.context->>'execution_attempt_id'
    WHERE t.target_node_id=$1 AND t.target_worker_slot=$2
      AND (t.state IN ('pending','leased') OR (a.status='running' AND a.finished_at IS NULL)
        OR t.coordinator_until IS NOT NULL))
    OR EXISTS(SELECT 1 FROM remote_ingestion.network_bindings WHERE node_id=$1 AND slot=$2 AND state<>'retired')`,
  [row.node_id,row.slot])).rowCount>0;
}

// Called only before attaching a new queue consumer. Use the very PG session
// holding the exclusive supervisor lock for the transaction: if it dies, both
// ownership and this cleanup roll back. No lease timeout is proof of quiescence.
export async function recoverRemoteSlot({ guard, row, lockKey, profileSecret, checkpoints = null }) {
  if(lockKey!==supervisionLockKey(row))throw new RemoteProtocolError('REMOTE_RECOVERY_NOT_OWNER');
  await guard.query('BEGIN');
  try {
    const owns=await guard.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted
      AND classid=781138012::oid AND objid=(hashtext($1)::bigint & 4294967295)::oid AND objsubid=2`,[lockKey]);
    if(!owns.rowCount)throw new RemoteProtocolError('REMOTE_RECOVERY_NOT_OWNER');
    await guard.query("SET LOCAL lock_timeout='2s'");
    await guard.query("SET LOCAL statement_timeout='5s'");
    await guard.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[row.node_id]);
    const slot=(await guard.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2',[row.node_id,row.slot])).rows[0];
    if(slot?.rota_worker_id!==row.rota_worker_id)throw new RemoteProtocolError('NETWORK_SLOT_CONFLICT');
    const tasks=(await guard.query(`SELECT t.* FROM remote_ingestion.tasks t
      LEFT JOIN crawler.channel_execution_attempts a ON a.attempt_id=t.context->>'execution_attempt_id'
      WHERE t.target_node_id=$1 AND t.target_worker_slot=$2 AND t.capability=$3
        AND (t.state IN ('pending','leased') OR (a.status='running' AND a.finished_at IS NULL)
          OR t.coordinator_until IS NOT NULL OR EXISTS(SELECT 1 FROM remote_ingestion.network_bindings b
            WHERE b.task_id=t.task_id AND b.state<>'retired'))
      ORDER BY t.created_at LIMIT 32 FOR UPDATE OF t`,[row.node_id,row.slot,CHANNEL_PLAN_CAPABILITY])).rows;
    let closed=0;
    for(const task of tasks){
      const attempt=(await guard.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE',[task.context.execution_attempt_id])).rows[0];
      const plan=task.input.plan;
      // Unknown or mismatched records remain quarantined. This path grants no
      // collector writes and cannot rewrite a newer business execution.
      if(!attempt || attempt.worker_id!==row.rota_worker_id || attempt.channel_id!==plan?.channel_id
        || attempt.business_run_id!==`incremental:${plan?.plan_id}`
        || Number(attempt.dispatch_generation)!==plan.dispatch_generation)continue;
      const newer=await guard.query(`SELECT 1 FROM crawler.channel_execution_attempts
        WHERE workload_scope=$1 AND business_run_id=$2 AND attempt_number>$3 LIMIT 1`,
      [attempt.workload_scope,attempt.business_run_id,attempt.attempt_number]);
      if(newer.rowCount)continue;
      // Close the coordinator before allowing any further old collector writes;
      // keep completed results and durable API/country handoff evidence intact.
      await guard.query(`UPDATE remote_ingestion.tasks SET
        state=CASE WHEN state IN ('pending','leased') THEN 'failed' ELSE state END,
        last_error=CASE WHEN state IN ('pending','leased') THEN 'REMOTE_CENTER_INTERRUPTED' ELSE last_error END,
        coordinator_id=NULL,coordinator_until=NULL WHERE task_id=$1`,[task.task_id]);
      const networkSlot=(await guard.query('SELECT * FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE',[row.node_id,row.slot])).rows[0];
      const bindings=(await guard.query('SELECT * FROM remote_ingestion.network_bindings WHERE task_id=$1 FOR UPDATE',[task.task_id])).rows;
      let quiet=true;
      for(const binding of bindings){
        if(binding.state==='retired'){
          if(binding.release_receipt?.in_flight!==0)quiet=false;
          continue;
        }
        const unissued=binding.state==='bound' && networkSlot.binding_id===binding.binding_id && !networkSlot.grant_until;
        await guard.query(`UPDATE remote_ingestion.network_bindings SET stop_requested=true,
          state=CASE WHEN $2 THEN 'retired' ELSE state END,
          release_receipt=CASE WHEN $2 THEN '{"stopped_before_grant":true,"in_flight":0}'::jsonb ELSE release_receipt END,
          retired_at=CASE WHEN $2 THEN clock_timestamp() ELSE retired_at END WHERE binding_id=$1`,[binding.binding_id,unissued]);
        if(!unissued)quiet=false;
      }
      if(!quiet || attempt.finished_at || attempt.status!=='running')continue;
      let status='aborted';
      if(task.state==='applied'){
        const run=(await guard.query('SELECT status FROM crawler.channel_runs WHERE run_id=$1',[attempt.business_run_id])).rows[0];
        if(task.applied_result?.run_id!==attempt.business_run_id || task.applied_result?.status!==run?.status
          || !['done','waiting_agent'].includes(run?.status))continue;
        for(const binding of bindings){
          if(binding.generation===task.generation && binding.youtube_session_required)
            await checkpoints?.apply(binding.binding_id,{client:guard});
        }
        status='success';
      }else if(task.state==='received' && task.last_error==='VIDEO_API_PENDING')status='failed';
      else if(task.state==='received' && task.last_error==='UPLOADS_COUNTRY_RECHECK')status='success';
      const profiles=new BrowserProfileStore({queryFn:guard.query.bind(guard),transactionFn:action=>action(guard),secret:profileSecret});
      await profiles.finishAttempt(attempt.attempt_id,{status,
        error:status==='success'?null:new Error(task.last_error || 'REMOTE_CENTER_INTERRUPTED'),
        result:{...(await guard.query('SELECT result_json FROM crawler.channel_execution_attempts WHERE attempt_id=$1',[attempt.attempt_id])).rows[0].result_json,
          remote_recovery:{reason:'supervisor_replaced',network_quiesced:true,
          previous_transport_state:task.state}}});
      closed++;
    }
    const settled=!await remoteSlotUnsettled(guard,row);
    await guard.query('COMMIT');return {settled,closed};
  }catch(error){await guard.query('ROLLBACK').catch(()=>{});throw error;}
}
