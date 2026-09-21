import {reconcileFullCrawlReservations} from './fullCrawlReplay.js';
import { randomUUID } from 'node:crypto';
import { lockContentDetailExecution } from '../contentDetailExecutionFence.js';
import { loadLoginRequiredExclusion } from '../youtubeLoginRequired.js';
import { fullCrawlTargetHash, fullCrawlUploadsHash, normalizeFullCrawlTargets } from '../fullCrawlYoutubeJsModel.js';
import { FULL_CRAWL_LIMITS, fullCrawlInputHash, validateFullCrawlStage } from './fullCrawlProtocol.js';
import { RemoteProtocolError, generation, hash, uuid } from './protocol.js';

const fail=code=>{throw new RemoteProtocolError(code);};
const same=(a,b)=>fullCrawlInputHash(a)===fullCrawlInputHash(b);

// Center-only reservation/start/application transactions. Network delivery and
// result decoding belong to the collector/coordinator, never this SQL module.
export class RemoteFullCrawlDetailStore {
  constructor({executionStore}) { this.executions=executionStore; }

  async lockDetail(client,ownership,fence) {
    const input=ownership.input;
    if (!fence || fence.executionMode!=='channel_inline' || fence.runId!==input.run_id
      || fence.channelId!==input.channel_id || fence.candidateId!==input.candidate_id
      || fence.dispatchGeneration!==input.dispatch_generation || fence.jobId!==input.job_id
      || fence.jobAttempt!==input.job_attempt || !await lockContentDetailExecution(client,fence)) {
      fail('FULL_CRAWL_DETAIL_FENCE_STALE');
    }
  }

  async targets(client,ownership) {
    const rows=(await client.query(`SELECT * FROM crawler.content_candidates
      WHERE run_id=$1 ORDER BY position,candidate_id FOR UPDATE`,[ownership.input.run_id])).rows;
    const receipt=ownership.run?.result_json?.full_crawl?.uploads;
    if (!receipt || receipt.document?.version!==1 || !Array.isArray(receipt.document.entries)) fail('FULL_CRAWL_TARGET_CONFLICT');
    const targets=rows.map(row=>{
      const original=row.result_json?.full_crawl_target;
      if (!original || row.channel_id!==ownership.input.channel_id) fail('FULL_CRAWL_TARGET_CONFLICT');
      const target=normalizeFullCrawlTargets([original])[0];
      if (target.video_id!==row.source_content_id || target.position!==row.position || target.source_url!==row.source_url) fail('FULL_CRAWL_TARGET_CONFLICT');
      return target;
    });
    const targetHash=fullCrawlTargetHash(targets);
    if (targetHash!==receipt.target_hash || fullCrawlTargetHash(receipt.document.entries)!==targetHash
      || fullCrawlUploadsHash(receipt.document)!==receipt.uploads_hash || rows.length!==Number(receipt.selected_count)) fail('FULL_CRAWL_TARGET_CONFLICT');
    return {rows,targets,targetHash:targetHash.slice('sha256:'.length)};
  }

  command(stage,ownership) {
    return validateFullCrawlStage({version:1,task_id:stage.task_id,generation:stage.generation,
      stage_id:stage.stage_id,stage:stage.stage,sequence:stage.sequence,execution_hash:ownership.evidence.execution_hash,
      input_hash:stage.input_hash,target_hash:stage.target_hash,input:stage.input});
  }

  async reserve(nodeId,request,{stageId,sequence,detailFence,collection=null}) {
    uuid(stageId);generation(sequence);detailFence=structuredClone(detailFence);collection=structuredClone(collection);
    return this.executions.withLease(nodeId,request,async(client,ownership)=>{
      await this.lockDetail(client,ownership,detailFence);
      const frozen=await this.targets(client,ownership);
      const existing=(await client.query('SELECT * FROM remote_ingestion.full_crawl_stages WHERE stage_id=$1 FOR UPDATE',[stageId])).rows[0];
      if (existing) {
        if (existing.task_id!==ownership.task.task_id || existing.generation!==ownership.task.generation
          || existing.stage!=='details' || existing.sequence!==sequence || existing.target_hash!==frozen.targetHash
          || !same(existing.input.detail_fence,detailFence)
          || !same(existing.input.collection??null,collection)) fail('FULL_CRAWL_STAGE_CONFLICT');
        return this.command(existing,ownership);
      }
      const previous=(await client.query(`SELECT * FROM remote_ingestion.full_crawl_stages
        WHERE task_id=$1 AND generation=$2 ORDER BY sequence DESC LIMIT 1 FOR UPDATE`,
      [ownership.task.task_id,ownership.task.generation])).rows[0];
      if (sequence!==(previous?.sequence??0)+1 || (previous && !previous.applied_at)) fail('FULL_CRAWL_STAGE_ORDER');
      const recovered=await reconcileFullCrawlReservations(client,ownership,frozen);
      const open=frozen.rows.filter(row=>!['done','unavailable'].includes(row.detail_status));
      if (!open.length) return null;
      if (open.some(row=>!['queued','failed'].includes(row.detail_status) || row.api_status!=='not_needed')) fail('FULL_CRAWL_DETAIL_UNSETTLED');
      const unresolved=await client.query(`SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations r
        JOIN remote_ingestion.full_crawl_stages s USING(stage_id)
        JOIN remote_ingestion.full_crawl_executions e USING(task_id,generation)
        WHERE e.execution_input->>'run_id'=$1 AND r.state NOT IN ('applied','cancelled') LIMIT 1`,[ownership.input.run_id]);
      if (unresolved.rowCount) fail('FULL_CRAWL_DETAIL_UNSETTLED');
      const selected=open.slice(0,FULL_CRAWL_LIMITS.detailTargets);
      const targets=selected.map(row=>({reservation_id:randomUUID(),video_id:row.source_content_id,ordinal:row.position,
        target:frozen.targets.find(target=>target.video_id===row.source_content_id)}));
      if(collection)for(const target of targets){
        target.excluded_detail=await loadLoginRequiredExclusion(client.query.bind(client),ownership.input.channel_id,target.video_id);
        const previous=recovered.get(target.video_id);if(previous)target.recovered_record=previous.record;
      }
      const input={...(collection?{collection}:{}),detail_fence:detailFence,targets},inputHash=fullCrawlInputHash(input);
      const stage={stage_id:stageId,task_id:ownership.task.task_id,generation:ownership.task.generation,
        stage:'details',sequence,input,input_hash:inputHash,target_hash:frozen.targetHash};
      this.command(stage,ownership);
      await client.query(`INSERT INTO remote_ingestion.full_crawl_stages
        (stage_id,task_id,generation,stage,sequence,input,input_hash,target_hash) VALUES($1,$2,$3,'details',$4,$5,$6,$7)`,
      [stageId,stage.task_id,stage.generation,sequence,input,inputHash,stage.target_hash]);
      for (const target of targets) await client.query(`INSERT INTO remote_ingestion.full_crawl_detail_reservations
        (reservation_id,stage_id,ordinal,video_id,target) VALUES($1,$2,$3,$4,$5)`,
      [target.reservation_id,stageId,target.ordinal,target.video_id,target.target]);
      return this.command(stage,ownership);
    });
  }

  async lockReservation(client,ownership,{stageId,reservationId}) {
    const stage=(await client.query('SELECT * FROM remote_ingestion.full_crawl_stages WHERE stage_id=$1 FOR UPDATE',[stageId])).rows[0];
    if (!stage || stage.stage!=='details' || stage.task_id!==ownership.task.task_id || stage.generation!==ownership.task.generation) fail('FULL_CRAWL_STAGE_CONFLICT');
    this.command(stage,ownership);
    await this.lockDetail(client,ownership,stage.input.detail_fence);
    const frozen=await this.targets(client,ownership);
    if (frozen.targetHash!==stage.target_hash) fail('FULL_CRAWL_TARGET_CONFLICT');
    const reservation=(await client.query(`SELECT * FROM remote_ingestion.full_crawl_detail_reservations
      WHERE reservation_id=$1 AND stage_id=$2 FOR UPDATE`,[reservationId,stageId])).rows[0];
    const target=stage.input.targets.find(target=>target.reservation_id===reservationId);
    const candidate=frozen.rows.find(row=>row.source_content_id===reservation?.video_id);
    if (!reservation || !target || !candidate || target.video_id!==reservation.video_id
      || target.ordinal!==reservation.ordinal || candidate.position!==reservation.ordinal
      || !same(target.target,reservation.target) || !same(reservation.target,candidate.result_json.full_crawl_target)) fail('FULL_CRAWL_TARGET_CONFLICT');
    return {stage,reservation,candidate,detailFence:stage.input.detail_fence};
  }

  async started(nodeId,request,{stageId,reservationId,startId}) {
    for (const value of [stageId,reservationId,startId]) uuid(value);
    return this.executions.withLease(nodeId,request,async(client,ownership)=>{
      const {reservation,candidate}=await this.lockReservation(client,ownership,{stageId,reservationId});
      if (reservation.start_id) {
        if (reservation.start_id!==startId || !['started','captured','applied'].includes(reservation.state)) fail('FULL_CRAWL_START_CONFLICT');
        return {start_id:startId,started_at:reservation.started_at,attempts:candidate.attempts};
      }
      if (reservation.state!=='reserved' || !['queued','failed'].includes(candidate.detail_status) || candidate.api_status!=='not_needed') fail('FULL_CRAWL_DETAIL_UNSETTLED');
      const duplicate=await client.query('SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations WHERE stage_id=$1 AND start_id=$2',
        [stageId,startId]);
      if (duplicate.rowCount) fail('FULL_CRAWL_START_CONFLICT');
      const preceding=await client.query(`SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations
        WHERE stage_id=$1 AND ordinal<$2 AND start_id IS NULL LIMIT 1`,[stageId,reservation.ordinal]);
      if (preceding.rowCount) fail('FULL_CRAWL_START_ORDER');
      const reused=(await client.query('SELECT input FROM remote_ingestion.full_crawl_stages WHERE stage_id=$1',[stageId])).rows[0].input.targets.find(t=>t.reservation_id===reservationId)?.recovered_record;
      await client.query(`UPDATE crawler.content_candidates SET detail_status='running',attempts=attempts+$2,
        error_message=NULL,updated_at=now() WHERE candidate_id=$1`,[candidate.candidate_id,reused?0:1]);
      const row=(await client.query(`UPDATE remote_ingestion.full_crawl_detail_reservations
        SET state='started',start_id=$2,started_at=clock_timestamp() WHERE reservation_id=$1 RETURNING start_id,started_at`,
      [reservationId,startId])).rows[0];
      return {...row,attempts:candidate.attempts+(reused?0:1)};
    });
  }

  async apply(nodeId,request,{stageId,reservationId,startId,payload},applyResult,afterApply=null) {
    for (const value of [stageId,reservationId,startId]) uuid(value);
    if (typeof applyResult!=='function') throw new TypeError('central result decoder/writer required');
    if (!Buffer.isBuffer(payload) || !payload.length || payload.length>FULL_CRAWL_LIMITS.batchBytes) fail('FULL_CRAWL_BATCH_SIZE');
    const bytes=Buffer.from(payload),resultHash=hash(bytes);
    return this.executions.withLease(nodeId,request,async(client,ownership)=>{
      const current=await this.lockReservation(client,ownership,{stageId,reservationId});
      const {reservation,candidate,detailFence}=current;
      if (reservation.start_id!==startId) fail('FULL_CRAWL_START_CONFLICT');
      if (reservation.state==='applied') {
        if (reservation.result_hash!==resultHash) fail('FULL_CRAWL_RESULT_CONFLICT');
        return reservation.applied_result;
      }
      if (reservation.state!=='started' || candidate.detail_status!=='running') fail('FULL_CRAWL_DETAIL_UNSETTLED');
      const preceding=await client.query(`SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations
        WHERE stage_id=$1 AND ordinal<$2 AND state<>'applied' LIMIT 1`,[stageId,reservation.ordinal]);
      if (preceding.rowCount) fail('FULL_CRAWL_APPLY_ORDER');
      // The existing local business writer receives this same client. A throw
      // rolls back content writes, candidate disposition and the applied mark.
      const result=await applyResult(client,{candidate:{...candidate,target:reservation.target},detailFence,payload:bytes});
      const after=(await client.query('SELECT detail_status,attempts FROM crawler.content_candidates WHERE candidate_id=$1',[candidate.candidate_id])).rows[0];
      if (!['done','unavailable'].includes(after?.detail_status) || after.attempts!==candidate.attempts) fail('FULL_CRAWL_RESULT_NOT_APPLIED');
      const receipt=JSON.parse(JSON.stringify(result??null));
      await client.query(`UPDATE remote_ingestion.full_crawl_detail_reservations SET state='applied',
        applied_at=clock_timestamp(),result_hash=$2,applied_result=$3 WHERE reservation_id=$1`,[reservationId,resultHash,receipt]);
      await client.query(`UPDATE remote_ingestion.full_crawl_stages SET applied_at=clock_timestamp()
        WHERE stage_id=$1 AND NOT EXISTS(SELECT 1 FROM remote_ingestion.full_crawl_detail_reservations
          WHERE stage_id=$1 AND state<>'applied')`,[stageId]);
      if(afterApply)await afterApply(client);
      return receipt;
    });
  }
}
