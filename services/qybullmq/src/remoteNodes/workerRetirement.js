import {fullCrawlSlotUnsettled} from './fullCrawlCenterRecovery.js';
import {RemoteProtocolError,uuid} from './protocol.js';
import {remoteSlotUnsettled,supervisionLockKey} from './centerExecutionRecovery.js';

const fail=code=>{throw new RemoteProtocolError(code,409);};

// Two durable phases fence intake before the trusted installer touches Docker.
// Historical tasks, network identities and credentials are never reused/deleted.
export function createWorkerRetirement({store,execution,fullCrawlExecution=null}) {
  return async function retire(value) {
    if(!value || Object.keys(value).some(k=>!['nodeId','deploymentId','slot','operationId','phase'].includes(k))
      || !/^(?:incremental|full-crawl)-[1-9][0-9]*$/.test(value.slot??'') || !['reserve','ready','finish'].includes(value.phase))
      throw new RemoteProtocolError('INVALID_WORKER_RETIREMENT',400);
    uuid(value.nodeId);uuid(value.deploymentId);uuid(value.operationId);
    return store.transaction(async client=>{
      await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',[`remote-deploy:${value.nodeId}`]);
      await client.query('SELECT node_id FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[value.nodeId]);
      const deployment=(await client.query('SELECT * FROM remote_ingestion.node_deployments WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
      if(deployment?.deployment_id!==value.deploymentId)fail('WORKER_DEPLOYMENT_MISMATCH');
      // Same policy→Worker order as intake reconciliation and explicit drain.
      const policy=(await client.query('SELECT * FROM remote_ingestion.node_intake_requests WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
      const row=(await client.query('SELECT *,connected_until>clock_timestamp() AS alive FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[value.nodeId,value.slot])).rows[0];
      if(!row || row.deployment_id!==value.deploymentId)fail('WORKER_DEPLOYMENT_MISMATCH');
      if(row.retirement_id && row.retirement_id!==value.operationId)fail('WORKER_RETIREMENT_CONFLICT');
      if(row.retired_at)return {nodeId:value.nodeId,deploymentId:value.deploymentId,slot:value.slot,operationId:value.operationId,removed:true};
      if(value.phase!=='reserve' && row.retirement_id!==value.operationId)fail('WORKER_RETIREMENT_CONFLICT');
      if(!row.retirement_id && (!row.alive || !row.accepting))fail('WORKER_STATE_UNKNOWN');
      const selectedExecution=row.mode==='full_crawl_collect'?fullCrawlExecution:execution;
      const unsettled=row.mode==='full_crawl_collect'?fullCrawlSlotUnsettled:remoteSlotUnsettled;
      if(typeof selectedExecution?.isProcessing!=='function')fail('WORKER_STATE_UNKNOWN');
      const busy=selectedExecution?.isProcessing(row)===true || await unsettled(client,row)
        || (row.mode!=='full_crawl_collect' && (await client.query(`SELECT 1 FROM remote_ingestion.tasks WHERE
          ((target_node_id=$1 AND target_worker_slot=$2) OR (node_id=$1 AND worker_slot=$2))
          AND state IN ('pending','leased','received') LIMIT 1`,[value.nodeId,value.slot])).rowCount>0);
      if(busy)fail('WORKER_NOT_IDLE');
      if(value.phase==='reserve'){
        if(!row.retirement_id){
          const intake=(await client.query('SELECT intake_enabled FROM remote_ingestion.intake_controls WHERE node_key=$1',[value.nodeId])).rows[0];
          const selected=intake?.intake_enabled===true
            ?(await client.query(`SELECT node_id,slot FROM remote_ingestion.worker_connections
              WHERE node_id=$1 AND deployment_id=$2 AND retired_at IS NULL`,[value.nodeId,value.deploymentId])).rows.filter(w=>!selectedExecution?.isWorkerPaused?.(w)).map(w=>w.slot)
            :policy?.selected_slots??(await client.query(`SELECT slot FROM remote_ingestion.worker_connections
            WHERE node_id=$1 AND activation_requested AND retired_at IS NULL`,[value.nodeId])).rows.map(w=>w.slot);
          await client.query(`INSERT INTO remote_ingestion.node_intake_requests(node_id,deployment_id,selected_slots)
            VALUES($1,$2,$3) ON CONFLICT(node_id) DO UPDATE SET selected_slots=EXCLUDED.selected_slots,
            revision=remote_ingestion.node_intake_requests.revision+1,updated_at=clock_timestamp()`,
          [value.nodeId,value.deploymentId,selected.filter(slot=>slot!==value.slot)]);
          await client.query(`UPDATE remote_ingestion.worker_connections SET retirement_id=$3,activation_requested=false
            WHERE node_id=$1 AND slot=$2`,[value.nodeId,value.slot,value.operationId]);
          await client.query("SELECT pg_notify('qy_remote_transport','supervisor')");
        }
      }else{
        // A closed BullMQ consumer + released supervisor guard also excludes
        // jobs between dequeue and creation of their SQL execution records.
        const guard=(await client.query('SELECT pg_try_advisory_xact_lock(781138012,hashtext($1)) AS owned',[supervisionLockKey(row)])).rows[0];
        if(!guard.owned)fail('WORKER_RETIREMENT_WAIT');
        // After a center crash, enabled can outlive its consumer. Exclusive
        // ownership plus the checks above prove that clearing it is safe.
        if(row.enabled)await client.query('UPDATE remote_ingestion.worker_connections SET enabled=false WHERE node_id=$1 AND slot=$2',[value.nodeId,value.slot]);
        if(value.phase==='finish'){
          await client.query(`UPDATE remote_ingestion.worker_connections SET retired_at=clock_timestamp(),
            connected_until=NULL,accepting=false WHERE node_id=$1 AND slot=$2`,[value.nodeId,value.slot]);
          const count=(await client.query(`SELECT count(*)::int AS n FROM remote_ingestion.worker_connections
            WHERE node_id=$1 AND deployment_id=$2 AND retired_at IS NULL`,[value.nodeId,value.deploymentId])).rows[0].n;
          await client.query('UPDATE remote_ingestion.node_deployments SET worker_count=$2,updated_at=clock_timestamp() WHERE node_id=$1',[value.nodeId,count]);
          await client.query("SELECT pg_notify('qy_remote_transport','supervisor')");
        }
      }
      return {nodeId:value.nodeId,deploymentId:value.deploymentId,slot:value.slot,operationId:value.operationId,
        ready:value.phase!=='reserve',removed:value.phase==='finish'};
    });
  };
}
