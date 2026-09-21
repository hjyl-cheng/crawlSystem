import {fullCrawlInputHash} from './fullCrawlProtocol.js';

// Invoked under the new execution's live business fence. A crash can happen
// after SQL handoff but before Rota CompleteTask / BullMQ updateData. Replay the
// original control request; never invent a checked/unavailable country receipt.
export async function pendingFullCrawlCountryRecheck(client,owner,{egressCountry,countryRecheck}){
  if(countryRecheck)return null;
  const row=(await client.query(`SELECT t.input,t.applied_result,a.finished_at FROM remote_ingestion.tasks t
    JOIN crawler.channel_execution_attempts a ON a.attempt_id=t.context->>'execution_attempt_id'
    WHERE t.capability='youtube.full-crawl.v1' AND t.state='received' AND t.last_error='UPLOADS_COUNTRY_RECHECK'
      AND t.input->>'run_id'=$1 AND t.task_id<>$2
      AND NOT EXISTS(SELECT 1 FROM remote_ingestion.network_bindings b WHERE b.task_id=t.task_id
        AND (b.state<>'retired' OR (b.release_receipt->>'in_flight')::int IS DISTINCT FROM 0))
    ORDER BY t.created_at DESC LIMIT 1`,[owner.input.run_id,owner.task.task_id])).rows[0];
  const country=row?.applied_result?.country;
  if(!row?.finished_at||!/^[A-Z]{2}$/.test(country??'')||country===egressCountry)return null;
  for(const key of ['channel_id','candidate_id','dispatch_generation','job_id','business_run_key','intent_hash'])if(row.input[key]!==owner.input[key])return null;
  if(fullCrawlInputHash(row.input.fetch_contract)!==fullCrawlInputHash(owner.input.fetch_contract))return null;
  return country;
}
