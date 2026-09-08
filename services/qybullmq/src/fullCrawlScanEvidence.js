import { isYoutubeJsFullCrawlFetchContract } from "./fullCrawlFetchContract.js";
import { fullCrawlTargetHash, fullCrawlUploadsHash } from "./fullCrawlYoutubeJsModel.js";

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

export function fullCrawlUploadScan(run, candidates) {
  const result = record(run.result_json);
  if (!isYoutubeJsFullCrawlFetchContract(result.fetch_contract)) return result.upload_scan ?? null;
  const checkpoint = record(result.full_crawl);
  const uploads = record(checkpoint.uploads);
  const document = record(uploads.document);
  const fetch = record(checkpoint.fetch);
  // Both observation and publication must prove the same frozen target set.
  const invalid = () => new Error(`Full Crawl scan checkpoint is incomplete or conflicting: ${run.run_id}`);
  if (!Array.isArray(candidates) || document.version !== 1 || !Array.isArray(document.entries)
      || fetch.status !== "complete" || run.detail_status !== "done"
      || uploads.uploads_hash !== fullCrawlUploadsHash(document)
      || uploads.target_hash !== fullCrawlTargetHash(document.entries)
      || fetch.uploads_hash !== uploads.uploads_hash || fetch.target_hash !== uploads.target_hash
      || uploads.selected_count !== document.entries.length
      || fetch.selected_count !== candidates.length || candidates.length !== document.entries.length
      || candidates.some((row) => !["done", "unavailable"].includes(row.detail_status)
        || !["stored", "terminal_excluded", "deferred"].includes(row.disposition))) throw invalid();
  const targets = candidates.map((row) => {
    const target = record(row.target);
    if (target.video_id !== row.source_content_id || target.position !== row.position
        || target.source_url !== row.source_url) throw invalid();
    return target;
  });
  if (fullCrawlTargetHash(targets) !== uploads.target_hash) throw invalid();
  return {
    ...document,
    requested_limit: run.content_limit,
    content_max_age_days: result.content_max_age_days,
    selected_count: uploads.selected_count,
    detail_processing_complete: candidates.every((row) => row.disposition !== "deferred"),
  };
}

// The caller must hold its normal transaction/execution guard. Checkpoint
// repairs use the same frozen target proof as the original Full Crawl.
export async function closeRepairedFullCrawlScan(client, runId, completedAt = new Date()) {
  const run = (await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE', [runId])).rows[0];
  if (!run || !isYoutubeJsFullCrawlFetchContract(run.result_json?.fetch_contract)) return null;
  const rows = (await client.query(`SELECT source_content_id,source_url,position,detail_status,disposition,
    result_json->'full_crawl_target' AS target FROM crawler.content_candidates
    WHERE run_id=$1 ORDER BY position,candidate_id FOR UPDATE`, [runId])).rows;
  const checkpoint = record(run.result_json.full_crawl);
  if (checkpoint.fetch?.status === 'complete') {
    fullCrawlUploadScan(run, rows);
    return checkpoint.fetch;
  }
  const receipt = {
    status: 'complete', completed_at: new Date(completedAt).toISOString(),
    uploads_hash: checkpoint.uploads?.uploads_hash, target_hash: checkpoint.uploads?.target_hash,
    selected_count: rows.length,
    stored_count: rows.filter(row => row.disposition === 'stored').length,
    excluded_count: rows.filter(row => row.disposition === 'terminal_excluded').length,
    deferred_count: rows.filter(row => row.disposition === 'deferred').length,
  };
  fullCrawlUploadScan({ ...run, result_json: { ...run.result_json,
    full_crawl: { ...checkpoint, fetch: receipt } } }, rows);
  await client.query(`UPDATE crawler.channel_runs SET result_json=jsonb_set(result_json,
    '{full_crawl,fetch}',$2::jsonb,true),updated_at=now() WHERE run_id=$1`, [runId, JSON.stringify(receipt)]);
  return receipt;
}
