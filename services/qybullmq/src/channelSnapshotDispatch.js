function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function nonNegativeInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return parsed;
}

export async function allocateChannelSnapshotDispatch(query, {
  candidateId,
  expectedGeneration,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const normalizedCandidateId = positiveInteger(candidateId, "candidateId");
  const normalizedGeneration = nonNegativeInteger(expectedGeneration, "expectedGeneration");
  const result = await query(
    `UPDATE crawler.channel_candidates
     SET status='queued',next_retry_at=NULL,validation_finished_at=NULL,
         snapshot_dispatch_generation=snapshot_dispatch_generation+1,
         snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,
         updated_at=now()
     WHERE candidate_id=$1
       AND snapshot_dispatch_generation=$2
       AND NOT (snapshot_json ? 'parser_contract_error')
       AND status IN ('discovered','queued','validating','failed')
     RETURNING candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,
               priority,status,snapshot_dispatch_generation`,
    [normalizedCandidateId, normalizedGeneration],
  );
  const row = result.rows?.[0];
  if (!row) return null;
  return {
    ...row,
    candidate_id: Number(row.candidate_id),
    snapshot_dispatch_generation: positiveInteger(
      row.snapshot_dispatch_generation,
      "snapshot_dispatch_generation",
    ),
  };
}

export async function allocateDiscoveredChannelSnapshotDispatches(query, candidateIds = []) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const ids = [...new Set(candidateIds.map((value) => positiveInteger(value, "candidateId")))];
  if (ids.length === 0) return [];
  const result = await query(
    `UPDATE crawler.channel_candidates
     SET status='queued',
         snapshot_dispatch_generation=CASE
           WHEN snapshot_dispatch_generation=0 THEN 1
           ELSE snapshot_dispatch_generation
         END,
         snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,
         updated_at=now()
     WHERE candidate_id=ANY($1::bigint[]) AND status='discovered'
     RETURNING candidate_id,snapshot_dispatch_generation`,
    [ids],
  );
  return (result.rows ?? []).map((row) => ({
    candidate_id: positiveInteger(row.candidate_id, "candidate_id"),
    snapshot_dispatch_generation: positiveInteger(
      row.snapshot_dispatch_generation,
      "snapshot_dispatch_generation",
    ),
  })).sort((left, right) => left.candidate_id - right.candidate_id);
}
