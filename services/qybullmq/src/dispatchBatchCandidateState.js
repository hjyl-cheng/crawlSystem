function requiredQuery(value) {
  if (typeof value !== "function") throw new TypeError("query is required");
  return value;
}

function requiredBatchId(value) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError("dispatchBatchId is required");
  return normalized;
}

export async function reconcileDispatchBatchCandidateState(
  queryValue,
  dispatchBatchId,
  { closeValidation = false } = {},
) {
  const query = requiredQuery(queryValue);
  const normalizedBatchId = requiredBatchId(dispatchBatchId);
  if (typeof closeValidation !== "boolean") {
    throw new TypeError("closeValidation must be a boolean");
  }
  const result = await query(
    `WITH candidate_stats AS (
       SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status='accepted')::int AS accepted,
              count(*) FILTER (WHERE status='rejected')::int AS rejected,
              count(*) FILTER (WHERE status='failed')::int AS failed,
              count(*) FILTER (
                WHERE status IN ('discovered','queued','validating')
              )::int AS open
       FROM crawler.channel_candidates
       WHERE dispatch_batch_id=$1
     ), updated AS (
       UPDATE crawler.query_dispatch_batches batch
       SET discovered_candidate_count=stats.total,
           total_channel_count=stats.total,
           accepted_channel_count=stats.accepted,
           rejected_channel_count=stats.rejected,
           failed_channel_count=stats.failed,
           status=CASE
             WHEN $2::boolean
               AND batch.discovery_closed_at IS NOT NULL
               AND stats.open=0 THEN 'validation_closed'
             ELSE batch.status
           END,
           validation_closed_at=CASE
             WHEN $2::boolean
               AND batch.discovery_closed_at IS NOT NULL
               AND stats.open=0
               THEN COALESCE(batch.validation_closed_at,now())
             ELSE batch.validation_closed_at
           END,
           updated_at=now()
       FROM candidate_stats stats
       WHERE batch.dispatch_batch_id=$1
         AND batch.status<>'completed'
       RETURNING batch.dispatch_batch_id,batch.status,
                 batch.discovered_candidate_count,batch.total_channel_count,
                 batch.accepted_channel_count,batch.rejected_channel_count,
                 batch.failed_channel_count,batch.validation_closed_at,
                 (SELECT open FROM candidate_stats) AS open
     )
     SELECT * FROM updated`,
    [normalizedBatchId, closeValidation],
  );
  const row = result.rows[0];
  if (!row) return null;
  return Object.freeze({
    ...row,
    discovered_candidate_count: Number(row.discovered_candidate_count),
    total_channel_count: Number(row.total_channel_count),
    accepted_channel_count: Number(row.accepted_channel_count),
    rejected_channel_count: Number(row.rejected_channel_count),
    failed_channel_count: Number(row.failed_channel_count),
    open: Number(row.open),
  });
}
