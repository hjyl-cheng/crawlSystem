\set ON_ERROR_STOP on
\set QUIET 1
\pset format unaligned
\pset tuples_only on

WITH batch AS (
  SELECT *
  FROM crawler.query_dispatch_batches
  WHERE dispatch_batch_id = :'batch_id'
),
batch_query_ids AS (
  SELECT unnest(selected_query_ids) AS query_id
  FROM batch
),
batch_candidates AS (
  SELECT candidate.*
  FROM crawler.channel_candidates candidate
  JOIN batch ON batch.dispatch_batch_id = candidate.dispatch_batch_id
),
batch_channel_ids AS (
  SELECT DISTINCT channel_id
  FROM batch_candidates
  WHERE status = 'accepted'
),
batch_runs AS (
  SELECT run.*
  FROM crawler.channel_runs run
  JOIN batch_candidates candidate ON candidate.candidate_id = run.candidate_id
),
batch_content_candidates AS (
  SELECT candidate.*
  FROM crawler.content_candidates candidate
  JOIN batch_runs run ON run.run_id = candidate.run_id
),
batch_api_tasks AS (
  SELECT task.*
  FROM crawler.youtube_api_tasks task
  WHERE task.candidate_ids && COALESCE(
    (SELECT array_agg(candidate_id) FROM batch_content_candidates),
    '{}'::bigint[]
  )
),
records (section_no, row_no, payload) AS (
  SELECT
    0,
    1::bigint,
    jsonb_build_object(
      'type', 'export_manifest',
      'source_database', current_database(),
      'source_schema', 'crawler',
      'export_scope', 'pipeline_batch',
      'dispatch_batch_id', :'batch_id',
      'exported_at', now(),
      'excluded', jsonb_build_array(
        'MinIO raw bodies',
        'crawler.settings',
        'crawler.agent_configs',
        'crawler.agent_prompt_templates'
      )
    )
  FROM batch

  UNION ALL
  SELECT 10, row_number() OVER (ORDER BY query_set_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'query_sets', 'row', to_jsonb(row_data))
  FROM (
    SELECT DISTINCT query_set.*
    FROM crawler.query_sets query_set
    JOIN crawler.query_terms term USING (query_set_id)
    JOIN batch_query_ids selected USING (query_id)
  ) row_data

  UNION ALL
  SELECT 20, row_number() OVER (ORDER BY query_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'query_terms', 'row', to_jsonb(row_data))
  FROM (
    SELECT term.*
    FROM crawler.query_terms term
    JOIN batch_query_ids selected USING (query_id)
  ) row_data

  UNION ALL
  SELECT 30, row_number() OVER (ORDER BY dispatch_batch_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'query_dispatch_batches', 'row', to_jsonb(batch))
  FROM batch

  UNION ALL
  SELECT 40, row_number() OVER (ORDER BY page_no, page_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'query_pages', 'row', to_jsonb(row_data))
  FROM (
    SELECT page.*
    FROM crawler.query_pages page
    JOIN batch ON batch.dispatch_batch_id = page.dispatch_batch_id
  ) row_data

  UNION ALL
  SELECT 50, row_number() OVER (ORDER BY candidate_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'channel_candidates', 'row', to_jsonb(batch_candidates))
  FROM batch_candidates

  UNION ALL
  SELECT 60, row_number() OVER (ORDER BY candidate_source_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'channel_candidate_sources', 'row', to_jsonb(row_data))
  FROM (
    SELECT source.*
    FROM crawler.channel_candidate_sources source
    JOIN batch_candidates candidate USING (candidate_id)
  ) row_data

  UNION ALL
  SELECT 70, row_number() OVER (ORDER BY channel_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'channels', 'row', to_jsonb(row_data))
  FROM (
    SELECT channel.*
    FROM crawler.channels channel
    JOIN batch_channel_ids selected USING (channel_id)
  ) row_data

  UNION ALL
  SELECT 80, row_number() OVER (ORDER BY run_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'channel_runs', 'row', to_jsonb(batch_runs))
  FROM batch_runs

  UNION ALL
  SELECT 90, row_number() OVER (ORDER BY run_id, tab, page_no),
    jsonb_build_object('type', 'table_row', 'source_table', 'channel_tab_pages', 'row', to_jsonb(row_data))
  FROM (
    SELECT page.*
    FROM crawler.channel_tab_pages page
    JOIN batch_runs run USING (run_id)
  ) row_data

  UNION ALL
  SELECT 100, row_number() OVER (ORDER BY content_key),
    jsonb_build_object('type', 'table_row', 'source_table', 'contents', 'row', to_jsonb(row_data))
  FROM (
    SELECT content.*
    FROM crawler.contents content
    JOIN batch_runs run USING (run_id)
  ) row_data

  UNION ALL
  SELECT 110, row_number() OVER (ORDER BY candidate_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'content_candidates', 'row', to_jsonb(batch_content_candidates))
  FROM batch_content_candidates

  UNION ALL
  SELECT 120, row_number() OVER (ORDER BY task_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'youtube_api_tasks', 'row', to_jsonb(batch_api_tasks))
  FROM batch_api_tasks

  UNION ALL
  SELECT 130, row_number() OVER (ORDER BY batch_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'youtube_api_batches', 'row', to_jsonb(row_data))
  FROM (
    SELECT api_batch.*
    FROM crawler.youtube_api_batches api_batch
    WHERE api_batch.task_ids && COALESCE(
      (SELECT array_agg(task_id) FROM batch_api_tasks),
      '{}'::bigint[]
    )
  ) row_data

  UNION ALL
  SELECT 140, row_number() OVER (ORDER BY channel_id, agent_mode),
    jsonb_build_object('type', 'table_row', 'source_table', 'agent_profiles', 'row', to_jsonb(row_data))
  FROM (
    SELECT profile.*
    FROM crawler.agent_profiles profile
    JOIN batch_channel_ids selected USING (channel_id)
  ) row_data

  UNION ALL
  SELECT 150, row_number() OVER (ORDER BY channel_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'finalized_profiles', 'row', to_jsonb(row_data))
  FROM (
    SELECT profile.*
    FROM crawler.finalized_profiles profile
    JOIN batch_channel_ids selected USING (channel_id)
  ) row_data

  UNION ALL
  SELECT 160, row_number() OVER (ORDER BY event_id),
    jsonb_build_object('type', 'table_row', 'source_table', 'task_events', 'row', to_jsonb(row_data))
  FROM (
    SELECT event.*
    FROM crawler.task_events event
    CROSS JOIN batch
    WHERE event.created_at BETWEEN batch.started_at AND batch.finished_at + interval '1 second'
      AND event.queue_name IN (
        'youtube-discover-page',
        'youtube-channel-crawl',
        'youtube-data-api-batch',
        'youtube-agent-batch',
        'youtube-finalize'
      )
  ) row_data
)
SELECT payload::text
FROM records
ORDER BY section_no, row_no;
