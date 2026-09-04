import {
  FINALIZABLE_CHANNEL_STATUSES,
  finalizeDispatchRevision,
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
} from "./finalizePolicy.js";
import { buildPublicationGapRepairTarget } from "./publicationGapRepairExecution.js";

function requiredQuery(query) {
  if (typeof query !== "function") throw new TypeError("query is required");
  return query;
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function boundedLimit(value) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 2000) {
    throw new TypeError("limit must be an integer between 1 and 2000");
  }
  return parsed;
}

export function publicationGapRepairTarget(row = {}) {
  return buildPublicationGapRepairTarget(row);
}

export function finalizeRecoveryDispatchRevision(row = {}) {
  return finalizeDispatchRevision({
    channel_id: row.channel_id,
    latest_run_id: row.latest_run_id,
    channel_status: row.channel_status,
    agent_status: row.agent_status,
    detail_status: row.detail_status,
    expected_content_count: row.expected_content_count,
    pipeline_cycle_id: row.pipeline_cycle_id,
    run_final_repair: row.run_final_repair,
    candidate_count: row.candidate_count,
    candidate_updated_at: row.candidate_updated_at,
    content_count: row.content_count,
    content_updated_at: row.content_updated_at,
    agent_updated_at: row.agent_updated_at,
  });
}

export function finalizeRecoveryJobData(row = {}, pipelineCycleId = null) {
  const sourceRevision = finalizeRecoveryDispatchRevision(row);
  return {
    channel_id: row.channel_id,
    run_id: row.run_id,
    reason: "controller-finalize-reconcile",
    source_revision: sourceRevision,
    pipeline_cycle_id: optionalText(pipelineCycleId),
  };
}

function asPublicationGapRow(row = {}) {
  return {
    ...row,
    failed_candidates: 0,
    repairable_candidates: 0,
    type_missing_candidates: 0,
    strict_repair_candidates: 0,
    strict_repair_version: null,
    publication_gap: true,
  };
}

export function mergeFinalRepairCandidates({
  standardRows = [],
  publicationGapRows = [],
  limit = 200,
} = {}) {
  if (!Array.isArray(standardRows)) throw new TypeError("standardRows must be an array");
  if (!Array.isArray(publicationGapRows)) {
    throw new TypeError("publicationGapRows must be an array");
  }
  const maximum = boundedLimit(limit);
  const gapsByRunId = new Map(publicationGapRows.map((row) => {
    const runId = optionalText(row?.run_id);
    if (!runId) throw new TypeError("Publication Gap run_id is required");
    return [runId, asPublicationGapRow(row)];
  }));
  const merged = [];
  for (const gap of gapsByRunId.values()) merged.push(gap);
  for (const row of standardRows) {
    const runId = optionalText(row?.run_id);
    if (runId && gapsByRunId.has(runId)) continue;
    merged.push(row);
  }
  return merged.slice(0, maximum);
}

export function activeFinalRepairExclusions(jobs = []) {
  if (!Array.isArray(jobs)) throw new TypeError("jobs must be an array");
  const runIds = new Set();
  const publicationGapRootRunIds = new Set();
  for (const job of jobs) {
    for (const field of ["run_id", "repair_parent_run_id"]) {
      const runId = optionalText(job?.data?.[field]);
      if (runId) runIds.add(runId);
    }
    const rootRunId = optionalText(job?.data?.publication_gap_root_run_id);
    if (rootRunId) publicationGapRootRunIds.add(rootRunId);
  }
  return {
    runIds: [...runIds],
    publicationGapRootRunIds: [...publicationGapRootRunIds],
  };
}

export async function loadPublicationGapRepairCandidates(queryValue, {
  pipelineCycleId = null,
  limit = 200,
  excludedRunIds = [],
  excludedPublicationGapRootRunIds = [],
  includeRunIds = [],
  maxRounds = 3,
} = {}) {
  const query = requiredQuery(queryValue);
  if (!Array.isArray(excludedRunIds)) throw new TypeError("excludedRunIds must be an array");
  if (!Array.isArray(excludedPublicationGapRootRunIds)) {
    throw new TypeError("excludedPublicationGapRootRunIds must be an array");
  }
  if (!Array.isArray(includeRunIds)) throw new TypeError("includeRunIds must be an array");
  const rows = await query(
    `/* finalize-recovery:publication-gaps */
     SELECT run.run_id,run.channel_id,run.candidate_id,run.crawl_mode,run.status,run.detail_status,
            run.publication_finalized_status,run.result_json,
            channel.channel_url,channel.subscriber_count,
            channel.registry_promotion_run_id,
            COALESCE(
              NULLIF(run.result_json#>>'{publication_gap_repair,root_run_id}',''),
              channel.registry_promotion_run_id
            ) AS publication_gap_root_run_id,
            CASE
              WHEN run.result_json#>>'{publication_gap_repair,status}'='required'
                THEN ARRAY(
                  SELECT domain
                  FROM jsonb_array_elements_text(
                    COALESCE(run.result_json#>'{publication_gap_repair,domains}','[]'::jsonb)
                  ) AS domain
                  WHERE domain IN ('channel','video')
                  ORDER BY domain
                )
              WHEN finalized.quality_json#>'{publication_initial_package}' IS NOT NULL
                THEN ARRAY(
                  SELECT domain->>'domain'
                  FROM jsonb_array_elements(
                    COALESCE(
                      finalized.quality_json#>'{publication_initial_package,domains}',
                      '[]'::jsonb
                    )
                  ) AS domain
                  WHERE domain->>'domain' IN ('channel','video')
                    AND domain->>'readiness_status'<>'ready'
                  ORDER BY domain->>'domain'
                )
              WHEN finalized.quality_json#>>'{initial_observations,outcomes,video}'='complete'
               AND finalized.quality_json#>>'{initial_observations,outcomes,agent}'='complete'
                THEN ARRAY['channel']::text[]
              ELSE ARRAY['channel','video']::text[]
            END AS repair_domains,
            COALESCE((run.result_json#>>'{final_repair,rounds}')::int,0) AS repair_rounds
     FROM crawler.channels AS channel
     JOIN crawler.channel_runs AS run ON run.run_id=channel.latest_run_id
     JOIN crawler.finalized_profiles AS finalized
       ON finalized.channel_id=channel.channel_id
      AND finalized.run_id=run.run_id
     WHERE channel.status='active'
       AND channel.agent_status='done'
       AND run.crawl_mode='full'
       AND run.status='done'
       AND run.detail_status='done'
       AND run.publication_finalized_status IN ('ready_auto','ready_partial')
       AND run.publication_finalized_at IS NOT NULL
       AND finalized.status=run.publication_finalized_status
       AND run.updated_at<=now()-interval '30 seconds'
       AND (
         (
           run.result_json#>>'{publication_gap_repair,status}'='required'
           AND (
             run.run_id=channel.registry_promotion_run_id
             OR run.result_json#>>'{publication_gap_repair,root_run_id}'
                  =channel.registry_promotion_run_id
           )
           AND EXISTS (
             SELECT 1
             FROM jsonb_array_elements_text(
               COALESCE(run.result_json#>'{publication_gap_repair,domains}','[]'::jsonb)
             ) AS domain
             WHERE domain IN ('channel','video')
           )
         )
         OR (
           run.publication_finalized_status='ready_partial'
           AND finalized.status='ready_partial'
           AND run.candidate_id IS NOT NULL
           AND channel.registry_promotion_run_id=run.run_id
           AND channel.registry_promotion_candidate_id=run.candidate_id
           AND (
             (
               finalized.quality_json#>>'{publication_initial_package,status}'='not_ready'
               AND EXISTS (
                 SELECT 1
                 FROM jsonb_array_elements(
                   COALESCE(
                     finalized.quality_json#>'{publication_initial_package,domains}',
                     '[]'::jsonb
                   )
                 ) AS domain
                 WHERE domain->>'domain' IN ('channel','video')
                   AND domain->>'readiness_status'<>'ready'
               )
             )
             OR (
               finalized.quality_json#>'{publication_initial_package}' IS NULL
               AND finalized.quality_json#>>'{initial_observations,outcomes,about}'='partial'
               AND COALESCE(
                     (finalized.quality_json->>'unavailable_candidate_count')::int,
                     0
                   )=0
               AND COALESCE((finalized.quality_json->>'detail_open_count')::int,0)=0
               AND COALESCE((finalized.quality_json->>'api_open_count')::int,0)=0
               AND COALESCE(
                     (finalized.quality_json->>'classified_content_count')::int,
                     -1
                   )=COALESCE(
                     (finalized.quality_json->>'expected_content_count')::int,
                     -2
                   )
               AND finalized.quality_json->'missing_channel_fields'='[]'::jsonb
               AND finalized.quality_json->'missing_agent_fields'='[]'::jsonb
               AND finalized.quality_json->'missing_content_fields'='{}'::jsonb
             )
           )
         )
       )
       AND COALESCE((run.result_json#>>'{final_repair,rounds}')::int,0)<$4::int
       AND NOT (run.run_id=ANY($3::text[]))
       AND NOT (
         COALESCE(
           NULLIF(run.result_json#>>'{publication_gap_repair,root_run_id}',''),
           channel.registry_promotion_run_id
         )=ANY($6::text[])
       )
       AND (
         cardinality($5::text[])=0
         OR run.run_id=ANY($5::text[])
       )
       AND (
         $1::text IS NULL
         OR COALESCE(
              run.result_json->>'dispatch_batch_id',
              run.result_json->>'pipeline_cycle_id'
            )=$1
       )
       AND NOT EXISTS (
         SELECT 1
         FROM publication.channel_stream_state AS owner
         WHERE owner.channel_id=channel.channel_id
           AND owner.status='owned'
           AND (
             owner.onboarding_mode='bootstrap'
             AND owner.seed_status='pending'
             AND owner.ownership_reference->>'onboarding_mode'='automatic_bootstrap'
             AND owner.ownership_reference->>'initial_full_run_id'
                   =channel.registry_promotion_run_id
           ) IS NOT TRUE
       )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.content_candidates AS content
         WHERE content.run_id=run.run_id
           AND (
             content.detail_status IN ('queued','running','failed')
             OR content.api_status IN ('pending','queued','running','failed')
           )
       )
     ORDER BY run.updated_at,run.run_id
     LIMIT $2`,
    [
      optionalText(pipelineCycleId),
      boundedLimit(limit),
      excludedRunIds.map((runId) => optionalText(runId)).filter(Boolean),
      boundedLimit(maxRounds),
      includeRunIds.map((runId) => optionalText(runId)).filter(Boolean),
      excludedPublicationGapRootRunIds.map((runId) => optionalText(runId)).filter(Boolean),
    ],
  );
  return rows.rows;
}

export async function loadFinalizeRecoveryCandidates(queryValue, {
  pipelineCycleId = null,
  limit = 200,
} = {}) {
  const query = requiredQuery(queryValue);
  const rows = await query(
    `/* finalize-recovery:candidates */
     SELECT channel.channel_id,channel.latest_run_id,
            channel.status AS channel_status,channel.agent_status,
            run.run_id,run.detail_status,run.expected_content_count,
            run.result_json->>'pipeline_cycle_id' AS pipeline_cycle_id,
            run.result_json->'final_repair' AS run_final_repair,
            COALESCE(candidate_revision.candidate_count,0)::int AS candidate_count,
            candidate_revision.updated_at AS candidate_updated_at,
            COALESCE(content_revision.content_count,0)::int AS content_count,
            content_revision.updated_at AS content_updated_at,
            agent.updated_at AS agent_updated_at,
            greatest(
              channel.updated_at,
              COALESCE(agent.updated_at,'epoch'::timestamptz),
              COALESCE(candidate_revision.updated_at,'epoch'::timestamptz),
              COALESCE(content_revision.updated_at,'epoch'::timestamptz)
            ) AS source_updated_at
     FROM crawler.channels channel
     JOIN crawler.channel_runs run ON run.run_id=CASE
       WHEN channel.status='dormant' THEN channel.registry_promotion_run_id
       ELSE channel.latest_run_id
     END
     LEFT JOIN crawler.agent_profiles agent
       ON agent.channel_id=channel.channel_id
      AND agent.agent_mode='basic'
      AND agent.status='success'
     LEFT JOIN LATERAL (
       SELECT count(*) AS candidate_count,max(candidate.updated_at) AS updated_at
       FROM crawler.content_candidates candidate
       WHERE candidate.run_id=run.run_id
         AND candidate.channel_id=channel.channel_id
     ) candidate_revision ON true
     LEFT JOIN LATERAL (
       SELECT count(*) AS content_count,
              max(COALESCE(content.last_enriched_at,content.last_seen_at)) AS updated_at
       FROM crawler.contents content
       WHERE content.run_id=run.run_id
         AND content.channel_id=channel.channel_id
     ) content_revision ON true
     LEFT JOIN crawler.finalized_profiles finalized ON finalized.channel_id=channel.channel_id
     WHERE channel.status=ANY($3::text[])
       AND run.detail_status='done'
       AND (
         (
           channel.status='active'
           AND channel.agent_status='done'
           AND agent.channel_id IS NOT NULL
         )
         OR (
           channel.status='dormant'
           AND run.run_id=channel.registry_promotion_run_id
           AND run.result_json#>>'{migration_activity_gate,decision}'='dormant'
         )
       )
       AND (
         $1::text IS NULL
         OR COALESCE(run.result_json->>'dispatch_batch_id',run.result_json->>'pipeline_cycle_id')=$1
       )
       AND (
         (channel.status='dormant' AND run.publication_finalized_at IS NULL)
         OR (
           channel.status='active'
           AND (
             finalized.channel_id IS NULL
             OR finalized.run_id IS DISTINCT FROM run.run_id
             OR NOT (finalized.status=ANY($4::text[]))
             OR run.publication_finalized_status IS NULL
             OR run.publication_finalized_at IS NULL
             OR finalized.updated_at<greatest(
                  channel.updated_at,
                  COALESCE(agent.updated_at,'epoch'::timestamptz),
                  COALESCE(candidate_revision.updated_at,'epoch'::timestamptz),
                  COALESCE(content_revision.updated_at,'epoch'::timestamptz)
                )
           )
         )
       )
     ORDER BY run.updated_at
     LIMIT $2`,
    [
      optionalText(pipelineCycleId),
      boundedLimit(limit),
      FINALIZABLE_CHANNEL_STATUSES,
      SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
    ],
  );
  return rows.rows;
}

export async function hasOpenPipelineCrawlerWork(
  queryValue,
  pipelineCycleId = null,
  { includeWaitingAgent = true } = {},
) {
  const query = requiredQuery(queryValue);
  const rows = await query(
    `SELECT
       EXISTS (
         SELECT 1
         FROM crawler.query_pages
         WHERE status IN ('queued', 'running')
           AND ($1::text IS NULL OR result_json->>'pipeline_cycle_id'=$1::text)
         LIMIT 1
       ) AS open_query_pages,
       EXISTS (
         SELECT 1
         FROM crawler.channel_runs run
         JOIN crawler.channels channel
           ON channel.latest_run_id=run.run_id
          AND channel.status='active'
         WHERE run.status IN ('queued', 'running', 'waiting_pages', 'waiting_detail', 'waiting_agent', 'finalizing')
           AND ($1::text IS NULL OR run.result_json->>'pipeline_cycle_id'=$1::text)
           AND ($2::boolean = true OR run.status<>'waiting_agent')
           AND NOT EXISTS (
             SELECT 1
             FROM crawler.migration_system_retry_items retry
             WHERE retry.candidate_id=run.candidate_id
               AND retry.failed_dispatch_batch_id=$1
               AND retry.status='pending'
           )
         LIMIT 1
       ) AS open_channel_runs,
       EXISTS (
         SELECT 1
         FROM crawler.channel_candidates
         WHERE status IN ('discovered','queued','validating')
           AND ($1::text IS NULL OR dispatch_batch_id=$1::text)
         LIMIT 1
       ) AS open_channel_candidates`,
    [pipelineCycleId, includeWaitingAgent],
  );
  return Boolean(
    rows.rows[0]?.open_query_pages
    || rows.rows[0]?.open_channel_runs
    || rows.rows[0]?.open_channel_candidates
  );
}

export async function loadPipelineFinalizeBlockers(queryValue, pipelineCycleId = null) {
  const query = requiredQuery(queryValue);
  const rows = await query(
    `/* finalize-recovery:blockers */
     SELECT
       count(*) FILTER (WHERE channel.status='active' AND channel.agent_status<>'done')::int
         AS agent_open,
       count(*) FILTER (
         WHERE (
           channel.status='active'
           AND NOT (
             COALESCE(current_run.publication_finalized_status,'pending')=ANY($2::text[])
           )
         ) OR (
           channel.status='dormant'
           AND channel.registry_promotion_run_id IS NOT NULL
           AND promotion_run.publication_finalized_at IS NULL
         )
       )::int AS final_open,
       count(*) FILTER (
         WHERE channel.status='active'
           AND channel.agent_status='done'
           AND channel.registry_promotion_run_id IS NOT NULL
           AND promotion_candidate.status='accepted'
           AND promotion_candidate.accepted_at IS NOT NULL
           AND NOT EXISTS (
             SELECT 1
             FROM publication.channel_stream_state AS owner
             WHERE owner.channel_id=channel.channel_id
               AND owner.status='owned'
               AND (
                 owner.onboarding_mode='bootstrap'
                 AND owner.seed_status='pending'
                 AND owner.ownership_reference->>'onboarding_mode'='automatic_bootstrap'
                 AND owner.ownership_reference->>'initial_full_run_id'
                       =channel.registry_promotion_run_id
               ) IS NOT TRUE
           )
           AND EXISTS (
             SELECT 1
             FROM publication.stream AS automatic_stream
             WHERE automatic_stream.status='active'
               AND automatic_stream.capture_enabled_at IS NOT NULL
               AND promotion_candidate.accepted_at>=automatic_stream.capture_enabled_at
               AND COALESCE(
                     automatic_stream.source_identity_json->>'stream_role',
                     ''
                   )<>'dead_letter_recovery'
               AND COALESCE((
                 SELECT bool_and(delivery.mode='online')
                 FROM publication.channel_delivery_state AS delivery
                 JOIN publication.channel_stream_state AS route_owner
                   ON route_owner.publication_stream_id=delivery.publication_stream_id
                  AND route_owner.channel_id=delivery.channel_id
                 WHERE delivery.publication_stream_id=automatic_stream.publication_stream_id
                   AND route_owner.status='owned'
               ),false)
           )
       )::int AS publication_open
     FROM crawler.channels channel
     JOIN crawler.channel_runs current_run ON current_run.run_id=channel.latest_run_id
     LEFT JOIN crawler.channel_runs promotion_run
       ON promotion_run.run_id=channel.registry_promotion_run_id
     LEFT JOIN crawler.channel_candidates promotion_candidate
       ON promotion_candidate.candidate_id=channel.registry_promotion_candidate_id
      AND promotion_candidate.channel_id=channel.channel_id
     WHERE (
       $1::text IS NULL
       OR COALESCE(
            current_run.result_json->>'dispatch_batch_id',
            current_run.result_json->>'pipeline_cycle_id'
          )=$1
     )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.migration_system_retry_items retry
         WHERE (
           retry.candidate_id=current_run.candidate_id
           OR retry.candidate_id=channel.registry_promotion_candidate_id
         )
           AND retry.failed_dispatch_batch_id=$1
           AND retry.status='pending'
       )`,
    [optionalText(pipelineCycleId), SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES],
  );
  return {
    agentOpen: Number(rows.rows[0]?.agent_open ?? 0),
    finalOpen: Number(rows.rows[0]?.final_open ?? 0),
    publicationOpen: Number(rows.rows[0]?.publication_open ?? 0),
  };
}
