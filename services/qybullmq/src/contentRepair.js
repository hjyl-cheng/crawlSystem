import { queuesByRole, safeJobId } from "./queues.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";

export const CONTENT_COMPLETENESS_REPAIR_VERSION = "content-completeness-v6";

const crawlSettingsSql = `
  SELECT GREATEST(
           0,
           LEAST(
             3650,
             COALESCE(
               (
                 SELECT (value_json->>'content_max_age_days')::int
                 FROM crawler.settings
                 WHERE setting_key='crawl'
                 LIMIT 1
               ),
               90
             )
           )
         )::int AS content_max_age_days
`;

const targetSql = `
  WITH crawl_settings AS (
    ${crawlSettingsSql}
  ), incomplete_contents AS (
    SELECT c.content_key,c.channel_id,c.run_id
    FROM crawler.contents c
    JOIN crawler.channels ch
      ON ch.channel_id=c.channel_id
     AND ch.latest_run_id=c.run_id
     AND ch.status='active'
    JOIN crawler.channel_runs current_run
      ON current_run.run_id=c.run_id
     AND ($4::text IS NULL OR current_run.result_json->>'pipeline_cycle_id'=$4::text)
    JOIN crawler.content_candidates current_candidate
      ON current_candidate.content_key=c.content_key
     AND current_candidate.run_id=c.run_id
    CROSS JOIN crawl_settings settings
    WHERE NOT (current_run.result_json ? 'parser_contract_error')
      AND NOT (current_candidate.result_json ? 'parser_contract_error')
      AND COALESCE(current_candidate.result_json->'scope'->>'status','')<>'excluded'
      AND c.access_status<>'unlisted'
      AND (
        settings.content_max_age_days=0
        OR c.published_at IS NULL
        OR c.published_at>=now()-(settings.content_max_age_days * interval '1 day')
      )
      AND (
        c.description_status NOT IN ('exact','empty')
        OR c.like_count IS NULL
        OR (c.comment_count IS NULL AND c.comments_disabled IS DISTINCT FROM true)
        OR (
          c.comments_first_page IS NULL
          AND c.comments_disabled IS DISTINCT FROM true
          AND c.comment_count > 0
        )
        OR (
          (c.duration_seconds IS NULL OR c.duration_seconds<=0 OR c.length_text IS NULL OR c.length_text IN ('0:00','00:00'))
          AND NOT (
            c.content_type='live'
            AND c.live_ended_at IS NULL
            AND COALESCE(
              current_candidate.result_json#>>'{detail,live_status}',
              c.raw_json->>'live_status',
              ''
            ) IN ('is_live','live')
          )
        )
        OR c.published_at IS NULL
      )
  ), repair_candidates AS (
    SELECT cc.candidate_id,cc.run_id,cc.channel_id,cc.updated_at
    FROM crawler.content_candidates cc
    JOIN crawler.channels ch
      ON ch.channel_id=cc.channel_id
     AND ch.latest_run_id=cc.run_id
     AND ch.status='active'
    JOIN crawler.channel_runs current_run
      ON current_run.run_id=cc.run_id
     AND ($4::text IS NULL OR current_run.result_json->>'pipeline_cycle_id'=$4::text)
    LEFT JOIN incomplete_contents incomplete ON incomplete.content_key=cc.content_key
    LEFT JOIN crawler.contents candidate_content
      ON candidate_content.content_key=cc.content_key
     AND candidate_content.run_id=cc.run_id
    CROSS JOIN crawl_settings settings
    WHERE NOT (current_run.result_json ? 'parser_contract_error')
      AND NOT (cc.result_json ? 'parser_contract_error')
      AND COALESCE(cc.result_json->'scope'->>'status','')<>'excluded'
      AND COALESCE(
            candidate_content.access_status,
            cc.result_json#>>'{access,access_status}',
            'unknown'
          )<>'unlisted'
      AND cc.detail_status<>'api_pending'
      AND cc.api_status NOT IN ('pending','queued','running','failed')
      AND NOT EXISTS (
        SELECT 1
        FROM crawler.content_candidates api_candidate
        LEFT JOIN crawler.youtube_api_tasks task
          ON task.source_content_id=api_candidate.source_content_id
        WHERE api_candidate.run_id=cc.run_id
          AND (
            api_candidate.detail_status='api_pending'
            OR api_candidate.api_status IN ('pending','queued','running','failed')
            OR task.status IN ('pending','queued','running','failed')
          )
      )
      AND (cc.detail_status='failed' OR incomplete.content_key IS NOT NULL)
      AND (
        settings.content_max_age_days=0
        OR candidate_content.published_at IS NULL
        OR candidate_content.published_at>=now()-(settings.content_max_age_days * interval '1 day')
      )
      AND (
        (NOT $2::boolean AND COALESCE(cc.result_json->'repair_dispatches'->$1->>'status','')<>'enqueued')
        OR ($2::boolean AND COALESCE(cc.result_json->'repair_dispatches'->$1->>'status','')='enqueued')
      )
  ), repair_runs AS (
    SELECT run_id,channel_id,min(updated_at) AS oldest_at,count(*)::int AS candidate_count
    FROM repair_candidates
    GROUP BY run_id,channel_id
  ), selected_runs AS (
    SELECT *
    FROM repair_runs
    ORDER BY oldest_at,run_id
    LIMIT NULLIF($3::int,0)
  )
  SELECT candidate.candidate_id,candidate.run_id,candidate.channel_id,
         selected.candidate_count
  FROM repair_candidates candidate
  JOIN selected_runs selected USING(run_id,channel_id)
  ORDER BY selected.oldest_at,candidate.run_id,candidate.candidate_id
`;

function uniqueRuns(rows) {
  return [...new Map(rows.map((row) => [String(row.run_id), {
    run_id: String(row.run_id),
    channel_id: String(row.channel_id),
    candidate_count: Number(row.candidate_count ?? 0),
  }])).values()];
}

export async function loadContentRepairTargets(dbQuery, {
  repairVersion = CONTENT_COMPLETENESS_REPAIR_VERSION,
  retryEnqueued = false,
  limitRuns = 0,
  pipelineCycleId = null,
} = {}) {
  const detailResult = await dbQuery(targetSql, [
    repairVersion,
    retryEnqueued,
    Math.max(0, Math.floor(Number(limitRuns) || 0)),
    pipelineCycleId,
  ]);
  const channelResult = await dbQuery(`
    SELECT cr.run_id,cr.channel_id,ch.channel_url
    FROM crawler.channel_runs cr
    JOIN crawler.channels ch ON ch.channel_id=cr.channel_id AND ch.latest_run_id=cr.run_id
    WHERE ch.status='active'
      AND cr.status='running'
      AND cr.detail_status='pending'
      AND cr.expected_content_count=0
      AND ($2::text IS NULL OR cr.result_json->>'pipeline_cycle_id'=$2::text)
      AND NOT (cr.result_json ? 'parser_contract_error')
      AND NOT EXISTS (
        SELECT 1 FROM crawler.content_candidates cc WHERE cc.run_id=cr.run_id
      )
    ORDER BY cr.updated_at
    LIMIT NULLIF($1::int,0)
  `, [Math.max(0, Math.floor(Number(limitRuns) || 0)), pipelineCycleId]);
  const staleResult = await dbQuery(`
    SELECT cr.run_id,cr.channel_id
    FROM crawler.channel_runs cr
    JOIN crawler.channels ch ON ch.channel_id=cr.channel_id AND ch.latest_run_id=cr.run_id
    WHERE ch.status='active'
      AND cr.detail_status='api_pending'
      AND ($2::text IS NULL OR cr.result_json->>'pipeline_cycle_id'=$2::text)
      AND NOT (cr.result_json ? 'parser_contract_error')
      AND NOT EXISTS (
        SELECT 1
        FROM crawler.content_candidates cc
        WHERE cc.run_id=cr.run_id
          AND (
            cc.result_json ? 'parser_contract_error'
            OR
            cc.detail_status='failed'
            OR cc.api_status IN ('pending','queued','running','failed')
          )
      )
    ORDER BY cr.updated_at
    LIMIT NULLIF($1::int,0)
  `, [Math.max(0, Math.floor(Number(limitRuns) || 0)), pipelineCycleId]);
  return {
    detailRows: detailResult.rows,
    detailRuns: uniqueRuns(detailResult.rows),
    channelRuns: channelResult.rows,
    staleRuns: staleResult.rows,
  };
}

export async function hasPendingContentRepairs(
  dbQuery,
  repairVersion = CONTENT_COMPLETENESS_REPAIR_VERSION,
  pipelineCycleId = null,
) {
  const result = await dbQuery(`
    WITH crawl_settings AS (
      ${crawlSettingsSql}
    ), incomplete_contents AS (
      SELECT c.content_key,c.run_id
      FROM crawler.contents c
      JOIN crawler.channels ch
        ON ch.channel_id=c.channel_id
       AND ch.latest_run_id=c.run_id
       AND ch.status='active'
      JOIN crawler.channel_runs current_run
        ON current_run.run_id=c.run_id
       AND ($2::text IS NULL OR current_run.result_json->>'pipeline_cycle_id'=$2::text)
      JOIN crawler.content_candidates cc
        ON cc.content_key=c.content_key
       AND cc.run_id=c.run_id
      CROSS JOIN crawl_settings settings
      WHERE NOT (current_run.result_json ? 'parser_contract_error')
        AND NOT (cc.result_json ? 'parser_contract_error')
        AND COALESCE(cc.result_json->'scope'->>'status','')<>'excluded'
        AND c.access_status<>'unlisted'
        AND (
          settings.content_max_age_days=0
          OR c.published_at IS NULL
          OR c.published_at>=now()-(settings.content_max_age_days * interval '1 day')
        )
        AND (
          c.description_status NOT IN ('exact','empty')
          OR c.like_count IS NULL
          OR (c.comment_count IS NULL AND c.comments_disabled IS DISTINCT FROM true)
          OR (
            c.comments_first_page IS NULL
            AND c.comments_disabled IS DISTINCT FROM true
            AND c.comment_count > 0
          )
          OR (
            (c.duration_seconds IS NULL OR c.duration_seconds<=0 OR c.length_text IS NULL OR c.length_text IN ('0:00','00:00'))
            AND NOT (
              c.content_type='live'
              AND c.live_ended_at IS NULL
              AND COALESCE(cc.result_json#>>'{detail,live_status}',c.raw_json->>'live_status','') IN ('is_live','live')
            )
          )
          OR c.published_at IS NULL
        )
    )
    SELECT EXISTS (
      SELECT 1
      FROM crawler.content_candidates cc
      JOIN crawler.channels ch
        ON ch.channel_id=cc.channel_id
       AND ch.latest_run_id=cc.run_id
       AND ch.status='active'
      JOIN crawler.channel_runs current_run
        ON current_run.run_id=cc.run_id
       AND ($2::text IS NULL OR current_run.result_json->>'pipeline_cycle_id'=$2::text)
      LEFT JOIN incomplete_contents incomplete ON incomplete.content_key=cc.content_key
      LEFT JOIN crawler.contents candidate_content
        ON candidate_content.content_key=cc.content_key
       AND candidate_content.run_id=cc.run_id
      CROSS JOIN crawl_settings settings
      WHERE NOT (current_run.result_json ? 'parser_contract_error')
        AND NOT (cc.result_json ? 'parser_contract_error')
        AND COALESCE(cc.result_json->'scope'->>'status','')<>'excluded'
        AND COALESCE(
              candidate_content.access_status,
              cc.result_json#>>'{access,access_status}',
              'unknown'
            )<>'unlisted'
        AND (cc.detail_status='failed' OR incomplete.content_key IS NOT NULL)
        AND (
          settings.content_max_age_days=0
          OR candidate_content.published_at IS NULL
          OR candidate_content.published_at>=now()-(settings.content_max_age_days * interval '1 day')
        )
        AND COALESCE(cc.result_json->'repair_dispatches'->$1->>'status','')<>'enqueued'
      LIMIT 1
    ) AS pending
  `, [repairVersion, pipelineCycleId]);
  return Boolean(result.rows[0]?.pending);
}

export async function prepareContentRepairTargets(dbQuery, targets, {
  repairVersion = CONTENT_COMPLETENESS_REPAIR_VERSION,
  batchId,
} = {}) {
  const candidateIds = targets.detailRows.map((row) => Number(row.candidate_id)).filter(Number.isFinite);
  if (candidateIds.length > 0) {
    await dbQuery(
      `UPDATE crawler.content_candidates candidate
       SET detail_status='queued',api_status='not_needed',attempts=0,
           missing_fields='{}'::text[],error_message=NULL,finished_at=NULL,
           result_json=jsonb_set(
             COALESCE(result_json,'{}'::jsonb),
             '{repair_dispatches}',
             COALESCE(result_json->'repair_dispatches','{}'::jsonb)
               || jsonb_build_object(
                    $2::text,
                    jsonb_build_object(
                      'status','prepared',
                      'batch_id',$3::text,
                      'prepared_at',now(),
                      'requirements',jsonb_build_array('description','positive_duration','published_date','engagement')
                    )
                  ),
             true
           ) || jsonb_build_object('repair_once_version',$2::text),
           updated_at=now()
       WHERE candidate.candidate_id=ANY($1::bigint[])
         AND NOT (candidate.result_json ? 'parser_contract_error')
         AND candidate.detail_status<>'api_pending'
         AND candidate.api_status NOT IN ('pending','queued','running','failed')
         AND NOT EXISTS (
           SELECT 1
           FROM crawler.content_candidates api_candidate
           LEFT JOIN crawler.youtube_api_tasks task
             ON task.source_content_id=api_candidate.source_content_id
           WHERE api_candidate.run_id=candidate.run_id
             AND (
               api_candidate.detail_status='api_pending'
               OR api_candidate.api_status IN ('pending','queued','running','failed')
               OR task.status IN ('pending','queued','running','failed')
             )
         )`,
      [candidateIds, repairVersion, batchId],
    );
  }
  const detailRunIds = targets.detailRuns.map((row) => row.run_id);
  if (detailRunIds.length > 0) {
    await dbQuery(
      `UPDATE crawler.channel_runs
       SET status='waiting_detail',detail_status='queued',error_message=NULL,
           result_json=jsonb_set(
             result_json,
             '{final_repair}',
             jsonb_build_object('rounds',0,'reset_by_version',$2::text,'reset_at',now()),
             true
           ) || jsonb_build_object(
             'publication_repair',
             jsonb_build_object('version',$2::text,'batch_id',$3::text,'prepared_at',now())
           ),
           finished_at=NULL,updated_at=now()
       WHERE run_id=ANY($1::text[])
         AND NOT (result_json ? 'parser_contract_error')`,
      [detailRunIds, repairVersion, batchId],
    );
  }
  const channelRunIds = targets.channelRuns.map((row) => row.run_id);
  if (channelRunIds.length > 0) {
    await dbQuery(
      `UPDATE crawler.channel_runs
       SET status='queued',detail_status='pending',error_message=NULL,
           result_json=COALESCE(result_json,'{}'::jsonb) || jsonb_build_object(
             'publication_repair',
             jsonb_build_object('version',$2::text,'batch_id',$3::text,'prepared_at',now())
           ),
           finished_at=NULL,updated_at=now()
       WHERE run_id=ANY($1::text[])
         AND NOT (result_json ? 'parser_contract_error')`,
      [channelRunIds, repairVersion, batchId],
    );
  }
  const staleRunIds = targets.staleRuns.map((row) => row.run_id);
  if (staleRunIds.length > 0) {
    await dbQuery(
      `UPDATE crawler.channel_runs
       SET status='waiting_agent',detail_status='done',error_message=NULL,
           result_json=COALESCE(result_json,'{}'::jsonb) || jsonb_build_object(
             'publication_repair',
             jsonb_build_object('version',$2::text,'batch_id',$3::text,'prepared_at',now())
           ),
           updated_at=now()
       WHERE run_id=ANY($1::text[])
         AND NOT (result_json ? 'parser_contract_error')`,
      [staleRunIds, repairVersion, batchId],
    );
  }
}

async function markRepairEnqueued(dbQuery, runId, repairVersion, jobId) {
  await dbQuery(
    `UPDATE crawler.content_candidates
     SET result_json=jsonb_set(
       COALESCE(result_json,'{}'::jsonb),
       '{repair_dispatches}',
       COALESCE(result_json->'repair_dispatches','{}'::jsonb)
         || jsonb_build_object(
              $2::text,
              COALESCE(result_json->'repair_dispatches'->$2,'{}'::jsonb)
                || jsonb_build_object('status','enqueued','job_id',$3::text,'enqueued_at',now())
            ),
       true
     ),
     updated_at=now()
     WHERE run_id=$1
       AND NOT (result_json ? 'parser_contract_error')
       AND result_json->'repair_dispatches'->$2->>'status'='prepared'`,
    [runId, repairVersion, jobId],
  );
}

async function reusableRepairJob(queue, jobId) {
  const existing = await queue.getJob(jobId);
  if (!existing) return null;
  if (["completed", "failed"].includes(await existing.getState())) {
    await existing.remove();
    return queue.getJob(jobId);
  }
  return existing;
}

export async function enqueueContentRepairTargets(dbQuery, queues, targets, {
  repairVersion = CONTENT_COMPLETENESS_REPAIR_VERSION,
  batchId,
  retryEnqueued = false,
  pipelineCycleId = null,
} = {}) {
  let detail = 0;
  let channel = 0;
  let finalize = 0;
  for (const row of targets.detailRuns) {
    const jobId = safeJobId("repair", repairVersion, "channel-detail", row.run_id);
    const existing = await reusableRepairJob(queues[queuesByRole.channelCrawl], jobId);
    if (!existing) {
      await queues[queuesByRole.channelCrawl].add(
        "channel-detail-repair",
        {
          run_id: row.run_id,
          channel_id: row.channel_id,
          repair_batch_id: batchId,
          repair_version: repairVersion,
          pipeline_cycle_id: pipelineCycleId,
          published_at_required_precision: "date_only",
          api_fallback_mode: "emergency",
        },
        { jobId },
      );
    }
    await markRepairEnqueued(dbQuery, row.run_id, repairVersion, jobId);
    detail += 1;
  }
  for (const row of targets.channelRuns) {
    const jobId = safeJobId("repair", repairVersion, "channel", row.run_id);
    if (!(await reusableRepairJob(queues[queuesByRole.channelCrawl], jobId))) {
      await queues[queuesByRole.channelCrawl].add(
        "channel-crawl-repair",
        {
          run_id: row.run_id,
          channel_id: row.channel_id,
          channel_url: row.channel_url,
          crawl_mode: "full",
          repair_batch_id: batchId,
          pipeline_cycle_id: pipelineCycleId,
        },
        { jobId },
      );
    }
    channel += 1;
  }
  for (const row of targets.staleRuns) {
    const jobId = safeJobId("repair", repairVersion, "finalize", row.run_id);
    if (!(await reusableRepairJob(queues[queuesByRole.finalize], jobId))) {
      await queues[queuesByRole.finalize].add(
        "finalize-reconciled-run",
        {
          run_id: row.run_id,
          channel_id: row.channel_id,
          reason: "repair-stale-api-status",
          repair_batch_id: batchId,
          pipeline_cycle_id: pipelineCycleId,
        },
        { jobId },
      );
    }
    finalize += 1;
  }
  return { detail, channel, finalize };
}

export async function reconcileLiveDurationNotApplicable(dbQuery, pipelineCycleId = null) {
  const contents = await dbQuery(`
    WITH live_rows AS (
      SELECT c.content_key,c.run_id,c.channel_id
      FROM crawler.contents c
      JOIN crawler.channels ch
        ON ch.channel_id=c.channel_id
       AND ch.latest_run_id=c.run_id
       AND ch.status='active'
      JOIN crawler.channel_runs current_run
        ON current_run.run_id=c.run_id
       AND ($1::text IS NULL OR current_run.result_json->>'pipeline_cycle_id'=$1::text)
      JOIN crawler.content_candidates cc
        ON cc.content_key=c.content_key
       AND cc.run_id=c.run_id
      WHERE c.content_type='live'
        AND NOT (current_run.result_json ? 'parser_contract_error')
        AND NOT (cc.result_json ? 'parser_contract_error')
        AND c.live_ended_at IS NULL
        AND (c.duration_seconds IS NULL OR c.duration_seconds<=0)
        AND COALESCE(cc.result_json#>>'{detail,live_status}',c.raw_json->>'live_status','') IN ('is_live','live')
    )
    UPDATE crawler.contents c
    SET duration_status='unavailable',
        duration_source='live_in_progress_not_applicable',
        raw_json=c.raw_json || jsonb_build_object('duration_not_applicable',true),
        last_enriched_at=now()
    FROM live_rows live
    WHERE c.content_key=live.content_key
      AND (c.duration_status<>'unavailable' OR c.duration_source IS DISTINCT FROM 'live_in_progress_not_applicable')
    RETURNING live.content_key,live.run_id,live.channel_id
  `, [pipelineCycleId]);
  if (contents.rows.length === 0) return [];
  const contentKeys = contents.rows.map((row) => row.content_key);
  await dbQuery(
    `UPDATE crawler.content_candidates
     SET missing_fields=array_remove(missing_fields,'duration'),
         result_json=jsonb_set(
           result_json,
           '{detail}',
           COALESCE(result_json->'detail','{}'::jsonb)
             || jsonb_build_object(
                  'duration_status','unavailable',
                  'duration_source','live_in_progress_not_applicable'
                ),
           true
         ),
         error_message=CASE
           WHEN cardinality(array_remove(missing_fields,'duration'))=0 THEN NULL
           ELSE error_message
         END,
         updated_at=now()
     WHERE content_key=ANY($1::text[])`,
    [contentKeys],
  );
  await refreshVideoPublicationItemHashes({ query: dbQuery }, contentKeys);
  return [...new Map(contents.rows.map((row) => [row.run_id, {
    run_id: row.run_id,
    channel_id: row.channel_id,
  }])).values()];
}
