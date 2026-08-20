import { randomUUID } from "node:crypto";
import {
  normalizeQueryScheduler,
  QUERY_SCHEDULER_KEY,
} from "./queryScheduler.js";

const ENABLED_VALUES = new Set(["1", "true", "yes", "on"]);

function autoCycleEnabled(environment) {
  return ENABLED_VALUES.has(
    String(environment?.QUERY_METADATA_AUTO_CYCLE_ENABLED ?? "").trim().toLowerCase(),
  );
}

function inactive(reason, scheduler = null) {
  return {
    started: false,
    reason,
    pipeline_cycle_id: null,
    query_id: null,
    scheduler,
  };
}

function restartableScheduler(scheduler) {
  if (scheduler.status !== "stopped") return false;
  if (!scheduler.pipeline_cycle_id && scheduler.stop_reason == null) return true;
  return scheduler.stop_reason === "pipeline_complete" && scheduler.completed_at != null;
}

export async function maybeStartMetadataDiscoveryCycle(
  withTransaction,
  { environment = process.env } = {},
) {
  if (!autoCycleEnabled(environment)) return inactive("disabled");
  if (typeof withTransaction !== "function") {
    throw new TypeError("withTransaction is required");
  }

  return withTransaction(async (client) => {
    if (!client || typeof client.query !== "function") {
      throw new TypeError("withTransaction must provide an active PostgreSQL client");
    }
    const settingRows = await client.query(
      `SELECT value_json
       FROM crawler.settings
       WHERE setting_key=$1
       FOR UPDATE`,
      [QUERY_SCHEDULER_KEY],
    );
    if (settingRows.rows.length === 0) return inactive("scheduler_missing");

    const scheduler = normalizeQueryScheduler(settingRows.rows[0].value_json ?? {});
    if (!restartableScheduler(scheduler)) {
      return inactive(
        scheduler.stop_reason === "user_requested"
          ? "operator_stopped"
          : `scheduler_${scheduler.status}`,
        scheduler,
      );
    }

    const workRows = await client.query(
      `SELECT term.query_id
       FROM crawler.query_terms term
       WHERE term.status='active'
         AND term.metadata_json->>'auto_collected'='true'
         AND term.metadata_json->>'collector'='metadata'
         AND ($1::bigint IS NULL OR term.query_set_id=$1::bigint)
         AND term.quality_score IS NOT NULL
         AND term.quality_status NOT IN ('unscored','failed')
         AND COALESCE(term.quality_score,0)>=$2::numeric
         AND NOT (term.quality_json ? 'parser_contract_error')
         AND NOT EXISTS (
           SELECT 1
           FROM crawler.query_pages failed_page
           WHERE failed_page.query_id=term.query_id
             AND failed_page.status='failed'
             AND failed_page.result_json ? 'parser_contract_error'
         )
         AND (
           term.next_crawl_at<=now()
           OR EXISTS (
             SELECT 1
             FROM crawler.query_pages page
             WHERE page.query_id=term.query_id
               AND page.status='done'
               AND page.should_continue=true
               AND page.result_json ? 'next_continuation_token'
               AND COALESCE(page.result_json #>> '{yt_config,apiKey}','')<>''
               AND NOT EXISTS (
                 SELECT 1
                 FROM crawler.query_pages next_page
                 WHERE next_page.page_id=regexp_replace(
                   page.page_id,
                   ':page:' || page.page_no::text || '$',
                   ':page:' || (page.page_no + 1)::text
                 )
               )
           )
         )
       ORDER BY term.next_crawl_at,term.query_id
       LIMIT 1`,
      [scheduler.query_set_id, scheduler.query_quality_min_score],
    );
    const queryId = Number(workRows.rows[0]?.query_id);
    if (!Number.isFinite(queryId)) return inactive("no_due_metadata_query", scheduler);

    const now = new Date().toISOString();
    const pipelineCycleId = `pipeline:metadata:${randomUUID()}`;
    const patch = {
      status: "running",
      started_at: now,
      paused_at: null,
      stopped_at: null,
      completed_at: null,
      stop_reason: null,
      paused_from_status: null,
      pipeline_cycle_id: pipelineCycleId,
      updated_at: now,
      updated_by: "metadata-discovery-loop",
    };
    const updated = await client.query(
      `UPDATE crawler.settings
       SET value_json=value_json || $2::jsonb,updated_at=now()
       WHERE setting_key=$1
       RETURNING value_json`,
      [QUERY_SCHEDULER_KEY, JSON.stringify(patch)],
    );
    if (updated.rows.length === 0) return inactive("scheduler_missing");

    return {
      started: true,
      reason: "metadata_query_due",
      pipeline_cycle_id: pipelineCycleId,
      query_id: queryId,
      scheduler: normalizeQueryScheduler(updated.rows[0].value_json),
    };
  });
}
