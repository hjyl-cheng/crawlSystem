import { loadMigrationControlProgress } from './migrationBatchControl.js';

const done = counts => ['success', 'dormant', 'rejected', 'failed', 'existing'].reduce((sum, key) => sum + Number(counts?.[key] ?? 0), 0);

export function migrationRollingRates(batch, samples, now = new Date()) {
  return Object.fromEntries([15, 60].map(minutes => {
    const cutoff = new Date(now).getTime() - minutes * 60000;
    const sample = samples.filter(row => row.batch_id === batch.batch_id
      && new Date(row.sampled_at).getTime() <= cutoff
      && new Date(row.sampled_at).getTime() >= cutoff - 120000)
      .sort((a, b) => new Date(b.sampled_at) - new Date(a.sampled_at))[0];
    const completed = sample ? done(batch.counts) - done(sample.counts) : -1;
    const fetched = batch.counts?.fetch_completed != null && sample?.counts?.fetch_completed != null
      ? Number(batch.counts.fetch_completed) - Number(sample.counts.fetch_completed) : -1;
    const seconds = Number(batch.active_seconds) - Number(sample?.active_seconds);
    return [`minutes_${minutes}`, sample && completed >= 0 && seconds >= 60
      ? { completed, active_seconds: seconds, per_hour: completed * 3600 / seconds,
        fetch_completed: fetched >= 0 ? fetched : null,
        fetch_per_hour: fetched >= 0 ? fetched * 3600 / seconds : null } : null];
  }));
}

export async function sampleMigrationThroughput(query) {
  // Closed batches cannot gain newly admitted work. Keep their final sample
  // instead of repeatedly joining historical inventories and publication rows.
  const ids = (await query(`SELECT b.batch_id FROM (
    SELECT * FROM crawler.migration_control_batches ORDER BY created_at DESC LIMIT 10
  ) b LEFT JOIN LATERAL (
    SELECT sampled_at FROM crawler.migration_throughput_samples WHERE batch_id=b.batch_id
    ORDER BY sampled_at DESC LIMIT 1
  ) s ON true WHERE b.status NOT IN ('ended','completed') OR s.sampled_at IS NULL
    OR b.finished_at>s.sampled_at`)).rows.map(row => row.batch_id);
  const progress = ids.length ? await loadMigrationControlProgress(query, { batchIds: ids }) : { batches: [] };
  for (const batch of progress.batches) {
    // Count each admitted channel once, even if it has multiple completed Runs.
    // Detail completion is distinct from Agent/Finalize/publication settlement.
    batch.counts.fetch_completed = Number((await query(`SELECT count(*)::int AS count
      FROM crawler.migration_control_items i WHERE i.batch_id=$1 AND i.candidate_id IS NOT NULL
        AND EXISTS(SELECT 1 FROM crawler.channel_runs r
          WHERE r.candidate_id=i.candidate_id AND r.channel_id=i.channel_id AND r.detail_status='done')`,
    [batch.batch_id])).rows[0].count);
    await query(`INSERT INTO crawler.migration_throughput_samples(batch_id,active_seconds,counts,publishing_count)
      VALUES($1,$2,$3::jsonb,$4)`, [batch.batch_id, batch.active_seconds, JSON.stringify(batch.counts), batch.publishing_count]);
  }
  await query(`DELETE FROM crawler.migration_throughput_samples s WHERE sampled_at<now()-interval '2 hours'
    AND sampled_at<(SELECT max(keep.sampled_at) FROM crawler.migration_throughput_samples keep WHERE keep.batch_id=s.batch_id)`);
  return { batches: progress.batches.length };
}

export function createMigrationProgressReader(query) {
  let inFlight = null;
  return function read() {
    if (inFlight) return inFlight;
    inFlight = (async () => {
      const rows = (await query(`SELECT b.*,s.counts,s.publishing_count,s.sampled_at,s.active_seconds AS sample_active_seconds,
        (SELECT result_json->>'migration_control_error' FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=b.batch_id) AS control_error,
        GREATEST(0,EXTRACT(EPOCH FROM COALESCE(b.finished_at,now())-b.created_at)-b.paused_seconds-
          CASE WHEN b.paused_at IS NOT NULL THEN EXTRACT(EPOCH FROM COALESCE(b.finished_at,now())-b.paused_at) ELSE 0 END)::float AS active_seconds
        FROM crawler.migration_control_batches b LEFT JOIN LATERAL (
          SELECT * FROM crawler.migration_throughput_samples WHERE batch_id=b.batch_id ORDER BY sampled_at DESC LIMIT 1
        ) s ON true ORDER BY b.created_at DESC LIMIT 10`)).rows;
      // First bootstrap has no samples yet. Collapse concurrent cold requests;
      // subsequent page polling reads only these small indexed samples.
      if (rows.some(row => !row.counts)) return loadMigrationControlProgress(query);
      const samples = (await query(`SELECT batch_id,sampled_at,active_seconds,counts
        FROM crawler.migration_throughput_samples WHERE batch_id=ANY($1::text[])
          AND sampled_at>=now()-interval '65 minutes'`, [rows.map(row => row.batch_id)])).rows;
      for (const batch of rows) {
        batch.rolling_rates = migrationRollingRates({ ...batch, active_seconds: batch.sample_active_seconds }, samples, batch.sampled_at);
        batch.statistics_age_seconds = Math.max(0, (Date.now() - new Date(batch.sampled_at)) / 1000);
      }
      return { ok: true, batches: rows, active: rows.find(row => !['ended', 'completed'].includes(row.status)) ?? null };
    })().finally(() => { inFlight = null; });
    return inFlight;
  };
}
