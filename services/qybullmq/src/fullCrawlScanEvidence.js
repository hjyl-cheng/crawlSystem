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
