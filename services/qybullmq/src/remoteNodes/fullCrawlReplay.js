import {decodeFullCrawlResult,validateFullCrawlCommand} from './fullCrawlMessages.js';
import {fullCrawlInputHash} from './fullCrawlProtocol.js';
import {hash,RemoteProtocolError} from './protocol.js';
const fail=()=>{throw new RemoteProtocolError('FULL_CRAWL_DETAIL_UNSETTLED');};

// Called inside the new owner's business transaction, after target/fence locks.
// Retained observations can be reused only for the same frozen contract/targets
// and an original attempt whose network has actually retired.
export async function reconcileFullCrawlReservations(client,ownership,frozen){
  const old=(await client.query(`SELECT s.*,e.execution_input,e.execution_hash,t.state AS task_state,
      a.finished_at FROM remote_ingestion.full_crawl_stages s
    JOIN remote_ingestion.full_crawl_executions e USING(task_id,generation)
    JOIN remote_ingestion.tasks t ON t.task_id=s.task_id
    JOIN crawler.channel_execution_attempts a ON a.attempt_id=e.execution_input->>'execution_attempt_id'
    WHERE e.execution_input->>'run_id'=$1 AND s.task_id<>$2 AND s.stage='details'
      AND EXISTS(SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations r WHERE r.stage_id=s.stage_id AND r.state NOT IN ('applied','cancelled'))
    ORDER BY s.created_at`,[ownership.input.run_id,ownership.task.task_id])).rows;
  const recovered=new Map();
  for(const stage of old){
    if(!stage.finished_at||['pending','leased'].includes(stage.task_state)||stage.target_hash!==frozen.targetHash
      ||fullCrawlInputHash(stage.execution_input.fetch_contract)!==fullCrawlInputHash(ownership.input.fetch_contract))fail();
    if((await client.query(`SELECT 1 FROM remote_ingestion.network_bindings WHERE task_id=$1
      AND (state<>'retired' OR (release_receipt->>'in_flight')::int IS DISTINCT FROM 0)`,[stage.task_id])).rowCount)fail();
    const command=validateFullCrawlCommand({version:1,task_id:stage.task_id,generation:stage.generation,stage_id:stage.stage_id,
      stage:stage.stage,sequence:stage.sequence,execution_hash:stage.execution_hash,input_hash:stage.input_hash,target_hash:stage.target_hash,input:stage.input},stage.execution_input);
    const reservations=(await client.query('SELECT * FROM remote_ingestion.full_crawl_detail_reservations WHERE stage_id=$1 FOR UPDATE',[stage.stage_id])).rows;
    const batches=(await client.query("SELECT * FROM remote_ingestion.full_crawl_result_batches WHERE stage_id=$1 AND state IN ('received','applied') ORDER BY sequence",[stage.stage_id])).rows;
    for(const batch of batches){
      const parts=(await client.query('SELECT payload FROM remote_ingestion.full_crawl_result_parts WHERE batch_id=$1 ORDER BY part_number',[batch.batch_id])).rows;
      const bytes=Buffer.concat(parts.map(p=>p.payload));
      if(bytes.length!==batch.payload_bytes||hash(bytes)!==batch.payload_hash)fail();
      for(const record of decodeFullCrawlResult(bytes,command,stage.execution_input).records){
        const reservation=reservations.find(r=>r.reservation_id===record.reservation_id);
        if(!reservation||reservation.start_id!==record.start_id)fail();
        if(['applied','cancelled'].includes(reservation.state)||!['captured','preflight'].includes(record.outcome))continue;
        const candidate=frozen.rows.find(c=>c.source_content_id===record.video_id);
        if(!candidate||['done','unavailable'].includes(candidate.detail_status))continue;
        const bytes=Buffer.from(JSON.stringify(record));
        recovered.set(record.video_id,{source:reservation.reservation_id,record:{source_reservation_id:reservation.reservation_id,record_hash:hash(bytes),bytes:bytes.length,part_count:Math.ceil(bytes.length/524288)}});
      }
    }
    // Do not roll back attempts already charged before a crash. A missing raw
    // observation will require a genuinely new attempt; a cached one will not.
    await client.query("UPDATE remote_ingestion.full_crawl_detail_reservations SET state='cancelled' WHERE stage_id=$1 AND state NOT IN ('applied','cancelled')",[stage.stage_id]);
  }
  return recovered;
}

export async function fullCrawlRecoveredPart(client,ownership,{stageId,reservationId,partNumber}){
  const stage=(await client.query('SELECT * FROM remote_ingestion.full_crawl_stages WHERE stage_id=$1',[stageId])).rows[0];
  if(!stage||stage.task_id!==ownership.task.task_id||stage.generation!==ownership.task.generation)fail();
  const target=stage.input.targets?.find(t=>t.reservation_id===reservationId),reference=target?.recovered_record;
  if(!reference||!Number.isSafeInteger(partNumber)||partNumber<0||partNumber>=reference.part_count)fail();
  const source=(await client.query('SELECT stage_id,start_id,video_id FROM remote_ingestion.full_crawl_detail_reservations WHERE reservation_id=$1',[reference.source_reservation_id])).rows[0];
  if(!source||source.video_id!==target.video_id)fail();
  const batches=(await client.query("SELECT batch_id,payload_hash FROM remote_ingestion.full_crawl_result_batches WHERE stage_id=$1 AND state IN ('received','applied') ORDER BY sequence",[source.stage_id])).rows;
  for(const batch of batches){
    const parts=(await client.query('SELECT payload FROM remote_ingestion.full_crawl_result_parts WHERE batch_id=$1 ORDER BY part_number',[batch.batch_id])).rows;
    const bytes=Buffer.concat(parts.map(p=>p.payload));if(hash(bytes)!==batch.payload_hash)fail();
    const record=JSON.parse(bytes).records.find(r=>r.reservation_id===reference.source_reservation_id);
    if(!record)continue;
    const raw=Buffer.from(JSON.stringify(record));
    if(record.start_id!==source.start_id||hash(raw)!==reference.record_hash||raw.length!==reference.bytes)fail();
    const payload=raw.subarray(partNumber*524288,(partNumber+1)*524288);
    return {part_number:partNumber,part_count:reference.part_count,part_hash:hash(payload),payload:payload.toString('base64')};
  }
  fail();
}
