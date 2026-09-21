import {supervisionLockKey} from './centerExecutionRecovery.js';
import {lockPublicationChannelMutation} from '../publicationChannelMutationLock.js';
import {BrowserProfileStore} from '../browserProfileStore.js';
import {FULL_CRAWL_WORKLOAD} from './collectingWorkload.js';
import {fullCrawlInputHash} from './fullCrawlProtocol.js';
import {RemoteProtocolError} from './protocol.js';

// Full executions never contain an incremental Plan. Uncertain network/start
// evidence keeps this slot drained until the P3 transport recovery settles it.
export async function fullCrawlSlotUnsettled(pool,row){
  return (await pool.query(`SELECT 1 WHERE EXISTS(SELECT 1 FROM remote_ingestion.tasks t
    LEFT JOIN crawler.channel_execution_attempts a ON a.attempt_id=t.context->>'execution_attempt_id'
    WHERE t.target_node_id=$1 AND t.target_worker_slot=$2
      AND (t.state IN ('pending','leased') OR (a.status='running' AND a.finished_at IS NULL)))
    OR EXISTS(SELECT 1 FROM remote_ingestion.network_bindings WHERE node_id=$1 AND slot=$2
      AND (state<>'retired' OR release_receipt->>'in_flight' IS DISTINCT FROM '0'))`,[row.node_id,row.slot])).rowCount>0;
}
export async function recoverFullCrawlSlot({guard,row,lockKey,profileSecret}){
  if(row.mode!=='full_crawl_collect'||lockKey!==supervisionLockKey(row))throw new RemoteProtocolError('REMOTE_RECOVERY_NOT_OWNER');
  await guard.query('BEGIN');
  try{
    const owns=await guard.query(`SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted
      AND classid=781138012::oid AND objid=(hashtext($1)::bigint & 4294967295)::oid AND objsubid=2`,[lockKey]);
    if(!owns.rowCount)throw new RemoteProtocolError('REMOTE_RECOVERY_NOT_OWNER');
    await guard.query("SET LOCAL lock_timeout='2s'");await guard.query("SET LOCAL statement_timeout='5s'");
    await guard.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[row.node_id]);
    await guard.query('SELECT node_id FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[row.node_id,row.slot]);
    const tasks=(await guard.query(`SELECT t.* FROM remote_ingestion.tasks t LEFT JOIN crawler.channel_execution_attempts a
      ON a.attempt_id=t.context->>'execution_attempt_id' WHERE t.target_node_id=$1 AND t.target_worker_slot=$2 AND t.capability=$3
      AND (t.state IN ('pending','leased') OR (a.status='running' AND a.finished_at IS NULL)) ORDER BY t.created_at LIMIT 32 FOR UPDATE OF t`,
      [row.node_id,row.slot,FULL_CRAWL_WORKLOAD.capability])).rows;
    let closed=0;
    for(const task of tasks){
      const evidence=(await guard.query('SELECT * FROM remote_ingestion.full_crawl_executions WHERE task_id=$1 ORDER BY generation DESC LIMIT 1',[task.task_id])).rows[0];
      await lockPublicationChannelMutation(guard,task.input.channel_id);
      await guard.query('SELECT candidate_id FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE',[task.input.candidate_id]);
      await guard.query('SELECT business_run_key FROM crawler.business_run_bindings WHERE business_run_key=$1 FOR UPDATE',[task.input.business_run_key]);
      const attempt=(await guard.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE',[task.context.execution_attempt_id])).rows[0];
      if(!attempt||!evidence||evidence.execution_hash!==fullCrawlInputHash(task.input)
        ||attempt.attempt_id!==task.input.execution_attempt_id||attempt.worker_id!==row.rota_worker_id
        ||attempt.business_run_id!==task.input.run_id||attempt.channel_id!==task.input.channel_id
        ||Number(attempt.dispatch_generation)!==task.input.dispatch_generation)continue;
      const newer=await guard.query('SELECT 1 FROM crawler.channel_execution_attempts WHERE business_run_id=$1 AND workload_scope=$2 AND attempt_number>$3',
        [attempt.business_run_id,attempt.workload_scope,attempt.attempt_number]);
      if(newer.rowCount)continue;
      // Exclusive supervisor ownership closes the old coordinator immediately;
      // node evidence may still be uploaded, but no more I/O can be authorized.
      await guard.query(`UPDATE remote_ingestion.tasks SET state=CASE WHEN state IN ('pending','leased') THEN 'failed' ELSE state END,
        last_error=CASE WHEN state IN ('pending','leased') THEN 'REMOTE_CENTER_INTERRUPTED' ELSE last_error END,
        coordinator_id=NULL,coordinator_until=NULL,lease_until=NULL WHERE task_id=$1`,[task.task_id]);
      const slot=(await guard.query('SELECT * FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE',[row.node_id,row.slot])).rows[0];
      if(slot?.rota_worker_id!==row.rota_worker_id)throw new RemoteProtocolError('NETWORK_SLOT_CONFLICT');
      const bindings=(await guard.query('SELECT * FROM remote_ingestion.network_bindings WHERE task_id=$1 FOR UPDATE',[task.task_id])).rows;
      let quiet=true;
      for(const binding of bindings){
        if(binding.state==='retired'){if(binding.release_receipt?.in_flight!==0)quiet=false;continue;}
        const unissued=binding.state==='bound'&&slot.binding_id===binding.binding_id&&!slot.grant_until;
        await guard.query(`UPDATE remote_ingestion.network_bindings SET stop_requested=true,
          state=CASE WHEN $2 THEN 'retired' ELSE state END,retired_at=CASE WHEN $2 THEN clock_timestamp() ELSE retired_at END,
          release_receipt=CASE WHEN $2 THEN '{"stopped_before_grant":true,"in_flight":0}'::jsonb ELSE release_receipt END WHERE binding_id=$1`,[binding.binding_id,unissued]);
        if(!unissued)quiet=false;
      }
      if(!quiet||attempt.finished_at||attempt.status!=='running')continue;
      await guard.query(`UPDATE remote_ingestion.full_crawl_detail_reservations SET state=CASE WHEN start_id IS NULL THEN 'cancelled' ELSE 'uncertain' END
        WHERE stage_id IN (SELECT stage_id FROM remote_ingestion.full_crawl_stages WHERE task_id=$1) AND state IN ('reserved','started','captured')`,[task.task_id]);
      const profiles=new BrowserProfileStore({queryFn:guard.query.bind(guard),transactionFn:action=>action(guard),secret:profileSecret});
      await profiles.finishAttempt(attempt.attempt_id,{status:'aborted',error:new Error('REMOTE_CENTER_INTERRUPTED'),
        result:{...attempt.result_json,remote_recovery:{network_quiesced:true,previous_transport_state:task.state}}});
      closed++;
    }
    const settled=!await fullCrawlSlotUnsettled(guard,row);await guard.query('COMMIT');return {closed,settled};
  }catch(error){await guard.query('ROLLBACK').catch(()=>{});throw error;}
}
