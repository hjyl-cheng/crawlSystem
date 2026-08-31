SET TIME ZONE 'UTC';

CREATE SCHEMA IF NOT EXISTS crawler;

CREATE TABLE IF NOT EXISTS crawler.database_identity (
  singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
  database_kind TEXT NOT NULL CHECK (database_kind='crawler'),
  database_name TEXT NOT NULL,
  initialized_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO crawler.database_identity (singleton, database_kind, database_name)
VALUES (true, 'crawler', current_database())
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS crawler.query_sets (
  query_set_id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'paused', 'archived')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_query_sets_name
ON crawler.query_sets (lower(name));

CREATE TABLE IF NOT EXISTS crawler.query_terms (
  query_id BIGSERIAL PRIMARY KEY,
  query_set_id BIGINT REFERENCES crawler.query_sets(query_set_id) ON DELETE SET NULL,
  query_text TEXT NOT NULL,
  language TEXT,
  country TEXT,
  category TEXT,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'paused', 'exhausted', 'archived')),
  priority INTEGER NOT NULL DEFAULT 100,
  next_crawl_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  crawl_interval_sec INTEGER NOT NULL DEFAULT 1296000,
  quality_score NUMERIC(5, 2),
  quality_status TEXT NOT NULL DEFAULT 'unscored',
  quality_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  quality_checked_at TIMESTAMPTZ,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.query_terms
ADD COLUMN IF NOT EXISTS query_set_id BIGINT REFERENCES crawler.query_sets(query_set_id) ON DELETE SET NULL;

ALTER TABLE crawler.query_terms
ADD COLUMN IF NOT EXISTS quality_score NUMERIC(5, 2);

ALTER TABLE crawler.query_terms
ADD COLUMN IF NOT EXISTS quality_status TEXT NOT NULL DEFAULT 'unscored';

ALTER TABLE crawler.query_terms
ADD COLUMN IF NOT EXISTS quality_json JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE crawler.query_terms
ADD COLUMN IF NOT EXISTS quality_checked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_crawler_query_terms_due
ON crawler.query_terms (status, next_crawl_at, priority DESC, query_id ASC);

CREATE INDEX IF NOT EXISTS idx_crawler_query_terms_set
ON crawler.query_terms (query_set_id, status, priority DESC, query_id ASC);

CREATE INDEX IF NOT EXISTS idx_crawler_query_terms_quality
ON crawler.query_terms (quality_status, quality_score DESC NULLS LAST);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_query_terms_scope
ON crawler.query_terms (
  lower(query_text),
  COALESCE(language, ''),
  COALESCE(country, ''),
  COALESCE(category, '')
);

CREATE TABLE IF NOT EXISTS crawler.query_quality_batches (
  quality_batch_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'cancel_requested', 'cancelled', 'done', 'failed')),
  total_count INTEGER NOT NULL DEFAULT 0,
  processed_count INTEGER NOT NULL DEFAULT 0,
  scored_count INTEGER NOT NULL DEFAULT 0,
  fallback_count INTEGER NOT NULL DEFAULT 0,
  failed_count INTEGER NOT NULL DEFAULT 0,
  cancelled_count INTEGER NOT NULL DEFAULT 0,
  options_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawler_query_quality_batches_status
ON crawler.query_quality_batches (status, created_at ASC);

CREATE TABLE IF NOT EXISTS crawler.query_quality_tasks (
  quality_task_id BIGSERIAL PRIMARY KEY,
  quality_batch_id TEXT NOT NULL REFERENCES crawler.query_quality_batches(quality_batch_id) ON DELETE CASCADE,
  query_id BIGINT NOT NULL REFERENCES crawler.query_terms(query_id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'scored', 'fallback', 'failed', 'cancelled')),
  attempts INTEGER NOT NULL DEFAULT 0,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (quality_batch_id, query_id)
);

CREATE INDEX IF NOT EXISTS idx_crawler_query_quality_tasks_claim
ON crawler.query_quality_tasks (quality_batch_id, status, quality_task_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_query_quality_tasks_batch_task
ON crawler.query_quality_tasks (quality_batch_id,quality_task_id);

CREATE TABLE IF NOT EXISTS crawler.raw_objects (
  raw_object_id BIGSERIAL PRIMARY KEY,
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  object_path TEXT NOT NULL UNIQUE,
  object_type TEXT NOT NULL,
  entity_type TEXT,
  entity_id TEXT,
  source TEXT,
  content_type TEXT,
  content_hash TEXT,
  size_bytes BIGINT,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawler_raw_objects_entity
ON crawler.raw_objects (entity_type, entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_raw_objects_type
ON crawler.raw_objects (object_type, created_at DESC);

ALTER TABLE crawler.raw_objects ADD COLUMN IF NOT EXISTS content_encoding TEXT;
ALTER TABLE crawler.raw_objects ADD COLUMN IF NOT EXISTS original_size_bytes BIGINT;
ALTER TABLE crawler.raw_objects ADD COLUMN IF NOT EXISTS stored_size_bytes BIGINT;

INSERT INTO crawler.query_sets (name, description)
VALUES ('default', 'Default query set for BullMQ crawler')
ON CONFLICT (lower(name)) DO NOTHING;

UPDATE crawler.query_terms
SET query_set_id = (SELECT query_set_id FROM crawler.query_sets WHERE lower(name) = 'default' LIMIT 1)
WHERE query_set_id IS NULL;

CREATE TABLE IF NOT EXISTS crawler.query_dispatch_batches (
  dispatch_batch_id TEXT PRIMARY KEY,
  pipeline_cycle_id TEXT NOT NULL UNIQUE,
  query_set_id BIGINT REFERENCES crawler.query_sets(query_set_id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'discovery_closed', 'validation_closed', 'finishing', 'completed', 'stopped', 'failed')),
  query_quality_min_score NUMERIC(5, 2) NOT NULL DEFAULT 0,
  selected_query_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
  selected_query_count INTEGER NOT NULL DEFAULT 0,
  discovered_candidate_count INTEGER NOT NULL DEFAULT 0,
  accepted_channel_count INTEGER NOT NULL DEFAULT 0,
  rejected_channel_count INTEGER NOT NULL DEFAULT 0,
  failed_channel_count INTEGER NOT NULL DEFAULT 0,
  total_channel_count INTEGER NOT NULL DEFAULT 0,
  outcome TEXT CHECK (
    outcome IS NULL OR outcome IN ('completed','completed_with_system_failures')
  ),
  discovery_closed_at TIMESTAMPTZ,
  validation_closed_at TIMESTAMPTZ,
  agent_tail_flushed_at TIMESTAMPTZ,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawler_query_dispatch_batches_status
ON crawler.query_dispatch_batches (status, started_at DESC);

CREATE TABLE IF NOT EXISTS crawler.query_pages (
  page_id TEXT PRIMARY KEY,
  query_id BIGINT REFERENCES crawler.query_terms(query_id) ON DELETE SET NULL,
  query_text TEXT NOT NULL,
  page_no INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'done', 'failed', 'skipped')),
  priority INTEGER NOT NULL DEFAULT 100,
  accepted_count INTEGER NOT NULL DEFAULT 0,
  candidate_count INTEGER NOT NULL DEFAULT 0,
  unqualified_ratio NUMERIC(6, 4),
  should_continue BOOLEAN,
  stop_reason TEXT,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.query_pages
DROP CONSTRAINT IF EXISTS query_pages_query_id_page_no_key;

ALTER TABLE crawler.query_pages
ADD COLUMN IF NOT EXISTS dispatch_batch_id TEXT REFERENCES crawler.query_dispatch_batches(dispatch_batch_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_crawler_query_pages_dispatch_status
ON crawler.query_pages (dispatch_batch_id, status, page_no);

CREATE TABLE IF NOT EXISTS crawler.channel_candidates (
  candidate_id BIGSERIAL PRIMARY KEY,
  dispatch_batch_id TEXT NOT NULL REFERENCES crawler.query_dispatch_batches(dispatch_batch_id) ON DELETE CASCADE,
  pipeline_cycle_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  channel_url TEXT NOT NULL,
  handle TEXT,
  title TEXT,
  description TEXT,
  avatar_url TEXT,
  search_subscriber_count BIGINT,
  search_subscriber_count_text TEXT,
  is_verified BOOLEAN,
  priority INTEGER NOT NULL DEFAULT 100,
  status TEXT NOT NULL DEFAULT 'discovered'
    CHECK (status IN ('discovered', 'queued', 'validating', 'accepted', 'rejected', 'existing', 'failed')),
  snapshot_attempts INTEGER NOT NULL DEFAULT 0,
  snapshot_dispatch_generation BIGINT NOT NULL DEFAULT 0
    CHECK (snapshot_dispatch_generation >= 0),
  snapshot_active_job_id TEXT,
  snapshot_active_job_attempt INTEGER,
  snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  reject_reason TEXT,
  error_message TEXT,
  next_retry_at TIMESTAMPTZ,
  validation_started_at TIMESTAMPTZ,
  validation_finished_at TIMESTAMPTZ,
  accepted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT channel_candidates_snapshot_active_job_check CHECK (
    (snapshot_active_job_id IS NULL AND snapshot_active_job_attempt IS NULL)
    OR (
      snapshot_active_job_id IS NOT NULL
      AND snapshot_active_job_attempt IS NOT NULL
      AND snapshot_active_job_attempt >= 0
    )
  ),
  UNIQUE (dispatch_batch_id, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_crawler_channel_candidates_claim
ON crawler.channel_candidates (dispatch_batch_id, status, priority DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_crawler_channel_candidates_channel
ON crawler.channel_candidates (channel_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_channel_candidates_identity
ON crawler.channel_candidates (candidate_id, channel_id);

CREATE TABLE IF NOT EXISTS crawler.migration_channel_intents (
  migration_intent_id BIGSERIAL PRIMARY KEY,
  source_id TEXT NOT NULL,
  source_database TEXT NOT NULL,
  source_database_oid OID NOT NULL,
  source_candidate_id BIGINT NOT NULL,
  channel_id TEXT NOT NULL,
  source_snapshot JSONB NOT NULL,
  snapshot_sha256 TEXT NOT NULL CHECK (snapshot_sha256 ~ '^[a-f0-9]{64}$'),
  target_candidate_id BIGINT UNIQUE
    REFERENCES crawler.channel_candidates(candidate_id) ON DELETE RESTRICT,
  first_dispatch_batch_id TEXT NOT NULL,
  dispatch_attempts INTEGER NOT NULL DEFAULT 0 CHECK (dispatch_attempts >= 0),
  last_dispatch_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, channel_id),
  UNIQUE (source_id, source_candidate_id)
);

CREATE INDEX IF NOT EXISTS idx_crawler_migration_intents_target
ON crawler.migration_channel_intents (target_candidate_id)
WHERE target_candidate_id IS NOT NULL;

CREATE OR REPLACE FUNCTION crawler.prevent_migration_intent_source_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF ROW(
    NEW.source_id,
    NEW.source_database,
    NEW.source_database_oid,
    NEW.source_candidate_id,
    NEW.channel_id,
    NEW.source_snapshot,
    NEW.snapshot_sha256,
    NEW.first_dispatch_batch_id
  ) IS DISTINCT FROM ROW(
    OLD.source_id,
    OLD.source_database,
    OLD.source_database_oid,
    OLD.source_candidate_id,
    OLD.channel_id,
    OLD.source_snapshot,
    OLD.snapshot_sha256,
    OLD.first_dispatch_batch_id
  ) THEN
    RAISE EXCEPTION 'Migration intent source identity is immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS prevent_migration_intent_source_update
ON crawler.migration_channel_intents;
CREATE TRIGGER prevent_migration_intent_source_update
BEFORE UPDATE ON crawler.migration_channel_intents
FOR EACH ROW
EXECUTE FUNCTION crawler.prevent_migration_intent_source_update();

CREATE TABLE IF NOT EXISTS crawler.channel_candidate_sources (
  candidate_source_id BIGSERIAL PRIMARY KEY,
  candidate_id BIGINT NOT NULL REFERENCES crawler.channel_candidates(candidate_id) ON DELETE CASCADE,
  query_id BIGINT REFERENCES crawler.query_terms(query_id) ON DELETE SET NULL,
  page_id TEXT REFERENCES crawler.query_pages(page_id) ON DELETE SET NULL,
  query_text TEXT,
  rank_position INTEGER,
  discovery_strategy TEXT NOT NULL DEFAULT 'channel_filter',
  source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (candidate_id, page_id, discovery_strategy)
);

CREATE INDEX IF NOT EXISTS idx_crawler_channel_candidate_sources_query
ON crawler.channel_candidate_sources (query_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_channel_candidate_sources_page
ON crawler.channel_candidate_sources (page_id, candidate_id)
WHERE page_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS crawler.channels (
  channel_id TEXT PRIMARY KEY,
  channel_url TEXT NOT NULL,
  handle TEXT,
  title TEXT,
  country TEXT,
  country_source TEXT,
  country_code TEXT,
  country_canonical_name TEXT,
  subscriber_count BIGINT,
  subscriber_count_text TEXT,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'dormant', 'paused', 'archived', 'rejected', 'removed')),
  reject_reason TEXT,
  dormant_reason TEXT,
  dormant_since TIMESTAMPTZ,
  dormant_recheck_day DATE,
  dormant_last_probe_at TIMESTAMPTZ,
  dormant_cycle INTEGER NOT NULL DEFAULT 0,
  removed_reason TEXT,
  removed_at TIMESTAMPTZ,
  removed_source TEXT,
  removed_evidence TEXT,
  priority INTEGER NOT NULL DEFAULT 100,
  ready_for_agent BOOLEAN NOT NULL DEFAULT false,
  agent_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (agent_status IN ('pending', 'queued', 'running', 'done', 'failed', 'skipped')),
  latest_run_id TEXT,
  agent_attempts INTEGER NOT NULL DEFAULT 0,
  agent_next_retry_at TIMESTAMPTZ,
  agent_error_message TEXT,
  registry_promotion_candidate_id BIGINT,
  registry_promotion_run_id TEXT,
  source_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS registry_promotion_candidate_id BIGINT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS registry_promotion_run_id TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS removed_reason TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS removed_source TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS removed_evidence TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_reason TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_since TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_recheck_day DATE;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_last_probe_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_cycle INTEGER NOT NULL DEFAULT 0;
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_status_check
CHECK (status IN ('active', 'dormant', 'paused', 'archived', 'rejected', 'removed'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_dormant_cycle_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_dormant_cycle_check
CHECK (dormant_cycle >= 0);
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_dormant_state_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_dormant_state_check
CHECK (
  (
    status='dormant'
    AND dormant_reason='no_published_content_within_90_days'
    AND dormant_since IS NOT NULL
    AND dormant_recheck_day IS NOT NULL
    AND dormant_last_probe_at IS NOT NULL
    AND dormant_cycle > 0
    AND reject_reason IS NULL
  )
  OR (
    status<>'dormant'
    AND dormant_reason IS NULL
    AND dormant_since IS NULL
    AND dormant_recheck_day IS NULL
    AND dormant_last_probe_at IS NULL
    AND dormant_cycle=0
  )
);
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_removed_state_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_removed_state_check
CHECK (
  status<>'removed'
  OR (removed_reason IS NOT NULL AND removed_at IS NOT NULL AND removed_source IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_crawler_channels_agent_ready
ON crawler.channels (agent_status, ready_for_agent, priority DESC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_crawler_channels_dormant_recheck
ON crawler.channels (dormant_recheck_day, channel_id)
WHERE status='dormant';

CREATE TABLE IF NOT EXISTS crawler.channel_runs (
  run_id TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'waiting_pages', 'finalizing', 'done', 'failed', 'skipped')),
  crawl_mode TEXT NOT NULL DEFAULT 'full'
    CHECK (crawl_mode IN ('full', 'incremental')),
  flow_job_id TEXT,
  content_limit INTEGER NOT NULL DEFAULT 30,
  expected_content_count INTEGER NOT NULL DEFAULT 0,
  detail_status TEXT NOT NULL DEFAULT 'pending',
  profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawler_channel_runs_channel
ON crawler.channel_runs (channel_id, created_at DESC);

-- qy-rota-worker-v2-schema:start
ALTER TABLE crawler.query_dispatch_batches
ADD COLUMN IF NOT EXISTS failed_channel_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE crawler.query_dispatch_batches
ADD COLUMN IF NOT EXISTS total_channel_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE crawler.query_dispatch_batches
ADD COLUMN IF NOT EXISTS outcome TEXT;

UPDATE crawler.query_dispatch_batches
SET total_channel_count = GREATEST(
      total_channel_count,
      discovered_candidate_count,
      accepted_channel_count + rejected_channel_count + failed_channel_count
    ),
    updated_at = now()
WHERE total_channel_count < GREATEST(
  discovered_candidate_count,
  accepted_channel_count + rejected_channel_count + failed_channel_count
);

ALTER TABLE crawler.query_dispatch_batches
DROP CONSTRAINT IF EXISTS query_dispatch_batches_outcome_check;

ALTER TABLE crawler.query_dispatch_batches
ADD CONSTRAINT query_dispatch_batches_outcome_check
CHECK (outcome IS NULL OR outcome IN ('completed','completed_with_system_failures'));

ALTER TABLE crawler.query_dispatch_batches
DROP CONSTRAINT IF EXISTS query_dispatch_batches_completion_count_check;

ALTER TABLE crawler.query_dispatch_batches
ADD CONSTRAINT query_dispatch_batches_completion_count_check
CHECK (
  failed_channel_count >= 0
  AND total_channel_count >= 0
  AND accepted_channel_count >= 0
  AND rejected_channel_count >= 0
  AND accepted_channel_count + rejected_channel_count + failed_channel_count
      <= total_channel_count
);

ALTER TABLE IF EXISTS crawler.youtube_api_batches
ADD COLUMN IF NOT EXISTS active_job_id TEXT;

ALTER TABLE IF EXISTS crawler.youtube_api_batches
ADD COLUMN IF NOT EXISTS active_job_attempt BIGINT;

ALTER TABLE IF EXISTS crawler.youtube_api_batches
DROP CONSTRAINT IF EXISTS youtube_api_batches_active_job_check;

ALTER TABLE IF EXISTS crawler.youtube_api_batches
ADD CONSTRAINT youtube_api_batches_active_job_check
CHECK (
  (active_job_id IS NULL AND active_job_attempt IS NULL)
  OR (
    active_job_id IS NOT NULL
    AND active_job_attempt IS NOT NULL
    AND active_job_attempt > 0
  )
);

ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS detail_active_job_id TEXT;

ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS detail_active_job_attempt BIGINT;

ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS detail_active_scope_key TEXT;

ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS detail_job_epoch BIGINT NOT NULL DEFAULT 0;

ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS detail_active_job_epoch BIGINT;

UPDATE crawler.channel_runs
SET detail_active_job_epoch=detail_job_epoch
WHERE detail_active_job_id IS NOT NULL
  AND detail_active_job_epoch IS NULL;

ALTER TABLE crawler.channel_runs
DROP CONSTRAINT IF EXISTS channel_runs_detail_active_job_check;

ALTER TABLE crawler.channel_runs
ADD CONSTRAINT channel_runs_detail_active_job_check
CHECK (
  detail_job_epoch >= 0
  AND (
    (
      detail_active_job_id IS NULL
      AND detail_active_job_attempt IS NULL
      AND detail_active_scope_key IS NULL
      AND detail_active_job_epoch IS NULL
    )
    OR (
      detail_active_job_id IS NOT NULL
      AND detail_active_job_attempt IS NOT NULL
      AND detail_active_job_attempt > 0
      AND detail_active_scope_key IS NOT NULL
      AND detail_active_job_epoch IS NOT NULL
      AND detail_active_job_epoch = detail_job_epoch
    )
  )
);

ALTER TABLE crawler.channel_candidates
ADD COLUMN IF NOT EXISTS snapshot_dispatch_generation BIGINT NOT NULL DEFAULT 0;

ALTER TABLE crawler.channel_candidates
ADD COLUMN IF NOT EXISTS snapshot_active_job_id TEXT;

ALTER TABLE crawler.channel_candidates
ADD COLUMN IF NOT EXISTS snapshot_active_job_attempt INTEGER;

WITH candidate_generation_expectation AS (
  SELECT
    candidate.candidate_id,
    CASE
      WHEN MAX(intent.dispatch_attempts) IS NOT NULL
        THEN MAX(intent.dispatch_attempts)::BIGINT
      WHEN candidate.status <> 'discovered' THEN 1::BIGINT
      ELSE 0::BIGINT
    END AS expected_generation
  FROM crawler.channel_candidates AS candidate
  LEFT JOIN crawler.migration_channel_intents AS intent
    ON intent.target_candidate_id = candidate.candidate_id
  GROUP BY candidate.candidate_id,candidate.status
)
UPDATE crawler.channel_candidates AS candidate
SET snapshot_dispatch_generation = expectation.expected_generation,
    snapshot_active_job_id = NULL,
    snapshot_active_job_attempt = NULL
FROM candidate_generation_expectation AS expectation
WHERE expectation.candidate_id = candidate.candidate_id
  AND candidate.snapshot_dispatch_generation < expectation.expected_generation;

ALTER TABLE crawler.channel_candidates
DROP CONSTRAINT IF EXISTS channel_candidates_snapshot_dispatch_generation_check;

ALTER TABLE crawler.channel_candidates
ADD CONSTRAINT channel_candidates_snapshot_dispatch_generation_check
CHECK (snapshot_dispatch_generation >= 0);

ALTER TABLE crawler.channel_candidates
DROP CONSTRAINT IF EXISTS channel_candidates_snapshot_active_job_check;

ALTER TABLE crawler.channel_candidates
ADD CONSTRAINT channel_candidates_snapshot_active_job_check
CHECK (
  (snapshot_active_job_id IS NULL AND snapshot_active_job_attempt IS NULL)
  OR (
    snapshot_active_job_id IS NOT NULL
    AND snapshot_active_job_attempt IS NOT NULL
    AND snapshot_active_job_attempt >= 0
  )
);

CREATE TABLE IF NOT EXISTS crawler.business_run_bindings (
  business_run_key TEXT PRIMARY KEY,
  business_run_id TEXT NOT NULL UNIQUE,
  intent_schema_version INTEGER NOT NULL DEFAULT 1 CHECK (intent_schema_version > 0),
  intent_hash TEXT NOT NULL,
  intent_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  identity_policy_id TEXT NOT NULL,
  identity_policy_version INTEGER NOT NULL CHECK (identity_policy_version > 0),
  identity_policy_hash TEXT NOT NULL,
  run_kind TEXT NOT NULL CHECK (run_kind IN ('full','incremental','full_repair')),
  channel_id TEXT NOT NULL,
  candidate_id BIGINT,
  plan_id TEXT,
  full_intent_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('reserved','materialized','terminal')),
  terminal_reason TEXT,
  materialized_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    (status='materialized' AND materialized_at IS NOT NULL AND terminal_reason IS NULL)
    OR (status='reserved' AND materialized_at IS NULL AND terminal_reason IS NULL)
    OR (status='terminal' AND terminal_reason IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_crawler_business_run_bindings_status
ON crawler.business_run_bindings (status,created_at);

CREATE TABLE IF NOT EXISTS crawler.migration_retry_intents (
  retry_intent_id TEXT PRIMARY KEY,
  request_key TEXT NOT NULL UNIQUE,
  candidate_id BIGINT NOT NULL
    REFERENCES crawler.channel_candidates(candidate_id) ON DELETE RESTRICT,
  previous_business_run_id TEXT NOT NULL
    REFERENCES crawler.business_run_bindings(business_run_id) ON DELETE RESTRICT,
  new_business_run_id TEXT NOT NULL UNIQUE,
  new_business_run_key TEXT NOT NULL UNIQUE,
  new_job_id TEXT NOT NULL UNIQUE,
  dispatch_generation BIGINT NOT NULL CHECK (dispatch_generation > 0),
  reason TEXT NOT NULL,
  intent_hash TEXT NOT NULL,
  job_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'requested'
    CHECK (status IN ('requested','dispatched','running','finished','failed')),
  dispatch_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (dispatch_status IN ('pending','deferred','enqueued','terminal')),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  dispatched_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  terminal_job_attempt BIGINT CHECK (terminal_job_attempt > 0),
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (candidate_id,dispatch_generation)
);

ALTER TABLE crawler.migration_retry_intents
ADD COLUMN IF NOT EXISTS terminal_job_attempt BIGINT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='crawler.migration_retry_intents'::regclass
      AND conname='migration_retry_intents_terminal_job_attempt_check'
  ) THEN
    ALTER TABLE crawler.migration_retry_intents
    ADD CONSTRAINT migration_retry_intents_terminal_job_attempt_check
    CHECK (terminal_job_attempt IS NULL OR terminal_job_attempt > 0);
  END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_migration_retry_intents_active_candidate
ON crawler.migration_retry_intents (candidate_id)
WHERE status IN ('requested','dispatched','running');

CREATE INDEX IF NOT EXISTS idx_crawler_migration_retry_intents_status
ON crawler.migration_retry_intents (status,requested_at);

CREATE TABLE IF NOT EXISTS crawler.migration_system_retry_items (
  system_retry_id BIGSERIAL PRIMARY KEY,
  migration_intent_id BIGINT NOT NULL
    REFERENCES crawler.migration_channel_intents(migration_intent_id) ON DELETE RESTRICT,
  candidate_id BIGINT NOT NULL
    REFERENCES crawler.channel_candidates(candidate_id) ON DELETE RESTRICT,
  failed_dispatch_batch_id TEXT NOT NULL,
  failed_dispatch_generation BIGINT NOT NULL CHECK (failed_dispatch_generation > 0),
  failed_job_id TEXT NOT NULL,
  failed_job_attempt INTEGER NOT NULL CHECK (failed_job_attempt >= 0),
  failure_code TEXT NOT NULL,
  failure_category TEXT NOT NULL,
  failure_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('retrying','pending','dispatched','resolved','cancelled')),
  retry_dispatch_generation BIGINT,
  recovery_run_id TEXT,
  recovery_agent_job_epoch BIGINT NOT NULL DEFAULT 0,
  recovery_agent_active_job_id TEXT,
  recovery_agent_active_job_attempt BIGINT,
  resolution TEXT,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  dispatched_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (
    migration_intent_id,failed_dispatch_generation,failed_job_id,failed_job_attempt
  ),
  CONSTRAINT migration_system_retry_items_recovery_run_id_fkey
    FOREIGN KEY (recovery_run_id)
    REFERENCES crawler.channel_runs(run_id) ON DELETE RESTRICT,
  CONSTRAINT migration_system_retry_items_recovery_agent_active_job_check
    CHECK (
      recovery_agent_job_epoch >= 0
      AND (
        (
          recovery_agent_active_job_id IS NULL
          AND recovery_agent_active_job_attempt IS NULL
        )
        OR (
          recovery_agent_active_job_id IS NOT NULL
          AND recovery_agent_active_job_attempt IS NOT NULL
          AND recovery_agent_active_job_attempt > 0
        )
      )
    ),
  CHECK (
    retry_dispatch_generation IS NULL
    OR retry_dispatch_generation > failed_dispatch_generation
  )
);

-- Historical rows predate immutable Batch evidence and must remain unknown rather than guessed.
ALTER TABLE crawler.migration_system_retry_items
ADD COLUMN IF NOT EXISTS failed_dispatch_batch_id TEXT;

ALTER TABLE crawler.migration_system_retry_items
ADD COLUMN IF NOT EXISTS recovery_run_id TEXT;

ALTER TABLE crawler.migration_system_retry_items
ADD COLUMN IF NOT EXISTS recovery_agent_active_job_id TEXT;

ALTER TABLE crawler.migration_system_retry_items
ADD COLUMN IF NOT EXISTS recovery_agent_active_job_attempt BIGINT;

ALTER TABLE crawler.migration_system_retry_items
ADD COLUMN IF NOT EXISTS recovery_agent_job_epoch BIGINT NOT NULL DEFAULT 0;

ALTER TABLE crawler.migration_system_retry_items
DROP CONSTRAINT IF EXISTS migration_system_retry_items_recovery_run_id_fkey;

ALTER TABLE crawler.migration_system_retry_items
ADD CONSTRAINT migration_system_retry_items_recovery_run_id_fkey
FOREIGN KEY (recovery_run_id)
REFERENCES crawler.channel_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE crawler.migration_system_retry_items
DROP CONSTRAINT IF EXISTS migration_system_retry_items_recovery_agent_active_job_check;

ALTER TABLE crawler.migration_system_retry_items
ADD CONSTRAINT migration_system_retry_items_recovery_agent_active_job_check
CHECK (
  recovery_agent_job_epoch >= 0
  AND (
    (
      recovery_agent_active_job_id IS NULL
      AND recovery_agent_active_job_attempt IS NULL
    )
    OR (
      recovery_agent_active_job_id IS NOT NULL
      AND recovery_agent_active_job_attempt IS NOT NULL
      AND recovery_agent_active_job_attempt > 0
    )
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_migration_system_retry_active_candidate
ON crawler.migration_system_retry_items (candidate_id)
WHERE status IN ('retrying','pending','dispatched');

CREATE INDEX IF NOT EXISTS idx_crawler_migration_system_retry_status
ON crawler.migration_system_retry_items (status,requested_at,system_retry_id);

ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS identity_policy_id TEXT;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS identity_policy_version INTEGER;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS identity_policy_hash TEXT;

-- qy-fingerprint-schema:start
CREATE TABLE IF NOT EXISTS crawler.browser_profile_groups (
  profile_group_id TEXT PRIMARY KEY,
  proxy_id BIGINT,
  proxy_address_hash TEXT,
  identity_policy_id TEXT,
  identity_policy_version INTEGER,
  network_identity_key TEXT,
  profile_epoch INTEGER,
  profile_revision INTEGER NOT NULL DEFAULT 1 CHECK (profile_revision > 0),
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'suspect', 'retired')),
  language TEXT NOT NULL,
  country TEXT NOT NULL,
  timezone TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ,
  retired_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (proxy_id, profile_revision)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_browser_profile_groups_active_proxy
ON crawler.browser_profile_groups (proxy_id)
WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_crawler_browser_profile_groups_status
ON crawler.browser_profile_groups (status, last_used_at DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS crawler.browser_profiles (
  profile_id TEXT PRIMARY KEY,
  profile_group_id TEXT NOT NULL
    REFERENCES crawler.browser_profile_groups(profile_group_id) ON DELETE CASCADE,
  engine TEXT NOT NULL CHECK (engine IN ('youtubejs_chrome', 'ytdlp_safari')),
  impersonate_target TEXT NOT NULL,
  user_agent TEXT NOT NULL,
  visitor_data TEXT NOT NULL,
  fingerprint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  cookie_ciphertext TEXT,
  cookie_updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (profile_group_id, engine)
);

CREATE INDEX IF NOT EXISTS idx_crawler_browser_profiles_group
ON crawler.browser_profiles (profile_group_id, engine);

CREATE TABLE IF NOT EXISTS crawler.channel_execution_attempts (
  attempt_id TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL,
  run_id TEXT REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL,
  queue_name TEXT NOT NULL,
  job_id TEXT,
  job_attempt INTEGER NOT NULL DEFAULT 0,
  dispatch_generation BIGINT
    CHECK (dispatch_generation IS NULL OR dispatch_generation > 0),
  worker_id TEXT NOT NULL,
  slot_name TEXT NOT NULL,
  proxy_user TEXT NOT NULL,
  proxy_id BIGINT,
  proxy_address_hash TEXT,
  profile_group_id TEXT NOT NULL
    REFERENCES crawler.browser_profile_groups(profile_group_id) ON DELETE RESTRICT,
  profile_revision INTEGER NOT NULL,
  youtubejs_profile_id TEXT REFERENCES crawler.browser_profiles(profile_id) ON DELETE RESTRICT,
  ytdlp_profile_id TEXT REFERENCES crawler.browser_profiles(profile_id) ON DELETE RESTRICT,
  status TEXT NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'success', 'failed', 'aborted')),
  identity_changed BOOLEAN NOT NULL DEFAULT false,
  error_class TEXT,
  error_message TEXT,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.browser_profile_groups ALTER COLUMN proxy_id DROP NOT NULL;
ALTER TABLE crawler.browser_profile_groups ALTER COLUMN proxy_address_hash DROP NOT NULL;
ALTER TABLE crawler.browser_profile_groups ADD COLUMN IF NOT EXISTS identity_policy_id TEXT;
ALTER TABLE crawler.browser_profile_groups ADD COLUMN IF NOT EXISTS identity_policy_version INTEGER;
ALTER TABLE crawler.browser_profile_groups ADD COLUMN IF NOT EXISTS network_identity_key TEXT;
ALTER TABLE crawler.browser_profile_groups ADD COLUMN IF NOT EXISTS profile_epoch INTEGER;
ALTER TABLE crawler.browser_profile_groups DROP CONSTRAINT IF EXISTS browser_profile_groups_identity_shape_check;
ALTER TABLE crawler.browser_profile_groups ADD CONSTRAINT browser_profile_groups_identity_shape_check CHECK (
  (proxy_id IS NOT NULL AND proxy_address_hash IS NOT NULL)
  OR (
    identity_policy_id IS NOT NULL
    AND identity_policy_version > 0
    AND network_identity_key IS NOT NULL
    AND profile_epoch >= 0
  )
);
DROP INDEX IF EXISTS crawler.ux_crawler_browser_profile_groups_active_proxy;
CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_browser_profile_groups_active_proxy
ON crawler.browser_profile_groups (proxy_id)
WHERE status='active' AND proxy_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_browser_profile_groups_active_identity
ON crawler.browser_profile_groups (
  identity_policy_id,identity_policy_version,network_identity_key,profile_epoch
)
WHERE status='active' AND network_identity_key IS NOT NULL;

ALTER TABLE crawler.channel_execution_attempts ALTER COLUMN proxy_id DROP NOT NULL;
ALTER TABLE crawler.channel_execution_attempts ALTER COLUMN proxy_address_hash DROP NOT NULL;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS workload_scope TEXT;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS worker_instance_id TEXT;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS business_run_id TEXT;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS attempt_number INTEGER;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS task_id TEXT;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS route_generation BIGINT;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS network_identity_key TEXT;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS identity_policy_id TEXT;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS identity_policy_version INTEGER;
ALTER TABLE crawler.channel_execution_attempts ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT;
ALTER TABLE crawler.channel_execution_attempts
DROP CONSTRAINT IF EXISTS channel_execution_attempts_dispatch_generation_check;
ALTER TABLE crawler.channel_execution_attempts
ADD CONSTRAINT channel_execution_attempts_dispatch_generation_check
CHECK (dispatch_generation IS NULL OR dispatch_generation > 0);
CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_channel_execution_attempts_rota_task
ON crawler.channel_execution_attempts (task_id)
WHERE task_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_channel_execution_attempts_business_attempt
ON crawler.channel_execution_attempts (workload_scope,business_run_id,attempt_number)
WHERE workload_scope IS NOT NULL AND business_run_id IS NOT NULL AND attempt_number IS NOT NULL;

ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS page_intent_hash TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS intent_schema_version INTEGER;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS request_language TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS request_country TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS identity_policy_id TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS identity_policy_version INTEGER;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS identity_policy_hash TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS continuation_parent_page_id TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS continuation_token_hash TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS managed_fetch_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS managed_fetch_started_at TIMESTAMPTZ;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS managed_fetch_finished_at TIMESTAMPTZ;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS managed_fetch_error_code TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS qualification_status TEXT NOT NULL DEFAULT 'not_required';
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS qualification_started_at TIMESTAMPTZ;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS qualification_finished_at TIMESTAMPTZ;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS qualification_error_code TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS dispatch_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS dispatch_reason TEXT;
ALTER TABLE crawler.query_pages ADD COLUMN IF NOT EXISTS dispatched_job_id TEXT;
ALTER TABLE crawler.query_pages DROP CONSTRAINT IF EXISTS query_pages_managed_fetch_status_check;
ALTER TABLE crawler.query_pages ADD CONSTRAINT query_pages_managed_fetch_status_check
CHECK (managed_fetch_status IN ('pending','running','done','failed'));
ALTER TABLE crawler.query_pages DROP CONSTRAINT IF EXISTS query_pages_qualification_status_check;
ALTER TABLE crawler.query_pages ADD CONSTRAINT query_pages_qualification_status_check
CHECK (qualification_status IN ('not_required','pending','done','failed'));
ALTER TABLE crawler.query_pages DROP CONSTRAINT IF EXISTS query_pages_dispatch_status_check;
ALTER TABLE crawler.query_pages ADD CONSTRAINT query_pages_dispatch_status_check
CHECK (dispatch_status IN ('pending','deferred','enqueued','terminal'));

-- Legacy databases predate the composite parent key used by managed quality chunks.
CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_query_quality_tasks_batch_task
ON crawler.query_quality_tasks (quality_batch_id,quality_task_id);

CREATE TABLE IF NOT EXISTS crawler.query_quality_chunks (
  quality_chunk_id TEXT PRIMARY KEY,
  quality_batch_id TEXT NOT NULL REFERENCES crawler.query_quality_batches(quality_batch_id) ON DELETE CASCADE,
  chunk_revision INTEGER NOT NULL DEFAULT 1 CHECK (chunk_revision > 0),
  chunk_intent_hash TEXT NOT NULL,
  intent_schema_version INTEGER NOT NULL DEFAULT 1 CHECK (intent_schema_version > 0),
  effective_language TEXT NOT NULL,
  effective_country TEXT NOT NULL,
  identity_policy_id TEXT NOT NULL,
  identity_policy_version INTEGER NOT NULL CHECK (identity_policy_version > 0),
  identity_policy_hash TEXT NOT NULL,
  scoring_options JSONB NOT NULL DEFAULT '{}'::jsonb,
  scoring_options_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','queued','running','done','failed','cancelled')),
  dispatch_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (dispatch_status IN ('pending','deferred','enqueued','terminal')),
  dispatch_reason TEXT,
  dispatched_job_id TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (quality_batch_id,quality_chunk_id)
);

CREATE TABLE IF NOT EXISTS crawler.query_quality_chunk_members (
  quality_batch_id TEXT NOT NULL,
  quality_chunk_id TEXT NOT NULL,
  quality_task_id BIGINT NOT NULL,
  member_ordinal INTEGER NOT NULL CHECK (member_ordinal > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (quality_chunk_id,quality_task_id),
  UNIQUE (quality_batch_id,quality_task_id),
  UNIQUE (quality_chunk_id,member_ordinal),
  FOREIGN KEY (quality_batch_id,quality_chunk_id)
    REFERENCES crawler.query_quality_chunks(quality_batch_id,quality_chunk_id) ON DELETE CASCADE,
  FOREIGN KEY (quality_batch_id,quality_task_id)
    REFERENCES crawler.query_quality_tasks(quality_batch_id,quality_task_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS crawler.proxy_job_dispatch_outbox (
  dispatch_id TEXT PRIMARY KEY,
  aggregate_kind TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  intent_hash TEXT NOT NULL,
  queue_registry_key TEXT NOT NULL,
  deterministic_job_id TEXT NOT NULL,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','sending','sent','dead')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at TIMESTAMPTZ,
  last_error TEXT,
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (aggregate_kind,aggregate_id,intent_hash)
);

CREATE INDEX IF NOT EXISTS idx_crawler_proxy_job_dispatch_outbox_pending
ON crawler.proxy_job_dispatch_outbox (status,next_attempt_at,created_at);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_proxy_job_dispatch_outbox_channel_snapshot_generation
ON crawler.proxy_job_dispatch_outbox (
  aggregate_id,((payload_json->>'dispatch_generation')::BIGINT)
)
WHERE aggregate_kind='channel_snapshot';

CREATE OR REPLACE FUNCTION crawler.guard_managed_query_page_state()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.page_intent_hash IS NOT NULL AND (
       NEW.page_intent_hash IS DISTINCT FROM OLD.page_intent_hash
    OR NEW.intent_schema_version IS DISTINCT FROM OLD.intent_schema_version
    OR NEW.query_id IS DISTINCT FROM OLD.query_id
    OR NEW.query_text IS DISTINCT FROM OLD.query_text
    OR NEW.page_no IS DISTINCT FROM OLD.page_no
    OR NEW.request_language IS DISTINCT FROM OLD.request_language
    OR NEW.request_country IS DISTINCT FROM OLD.request_country
    OR NEW.identity_policy_id IS DISTINCT FROM OLD.identity_policy_id
    OR NEW.identity_policy_version IS DISTINCT FROM OLD.identity_policy_version
    OR NEW.identity_policy_hash IS DISTINCT FROM OLD.identity_policy_hash
    OR NEW.continuation_parent_page_id IS DISTINCT FROM OLD.continuation_parent_page_id
    OR NEW.continuation_token_hash IS DISTINCT FROM OLD.continuation_token_hash
  ) THEN
    RAISE EXCEPTION 'managed Discover Page Intent is immutable: %', OLD.page_id
      USING ERRCODE='check_violation';
  END IF;
  IF OLD.managed_fetch_status='done' AND NEW.managed_fetch_status<>'done' THEN
    RAISE EXCEPTION 'completed managed Discover fetch cannot regress: %', OLD.page_id
      USING ERRCODE='check_violation';
  END IF;
  IF OLD.dispatch_status='terminal' AND NEW.dispatch_status<>'terminal' THEN
    RAISE EXCEPTION 'terminal managed Discover dispatch cannot regress: %', OLD.page_id
      USING ERRCODE='check_violation';
  END IF;
  IF OLD.qualification_status IN ('done','failed')
     AND NEW.qualification_status IS DISTINCT FROM OLD.qualification_status THEN
    RAISE EXCEPTION 'terminal Discover qualification cannot regress: %', OLD.page_id
      USING ERRCODE='check_violation';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_managed_query_page_state ON crawler.query_pages;
CREATE TRIGGER trg_guard_managed_query_page_state
BEFORE UPDATE ON crawler.query_pages
FOR EACH ROW
EXECUTE FUNCTION crawler.guard_managed_query_page_state();

CREATE OR REPLACE FUNCTION crawler.guard_query_quality_chunk_state()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.status<>'pending' AND (
       NEW.quality_batch_id IS DISTINCT FROM OLD.quality_batch_id
    OR NEW.chunk_revision IS DISTINCT FROM OLD.chunk_revision
    OR NEW.chunk_intent_hash IS DISTINCT FROM OLD.chunk_intent_hash
    OR NEW.intent_schema_version IS DISTINCT FROM OLD.intent_schema_version
    OR NEW.effective_language IS DISTINCT FROM OLD.effective_language
    OR NEW.effective_country IS DISTINCT FROM OLD.effective_country
    OR NEW.identity_policy_id IS DISTINCT FROM OLD.identity_policy_id
    OR NEW.identity_policy_version IS DISTINCT FROM OLD.identity_policy_version
    OR NEW.identity_policy_hash IS DISTINCT FROM OLD.identity_policy_hash
    OR NEW.scoring_options IS DISTINCT FROM OLD.scoring_options
    OR NEW.scoring_options_hash IS DISTINCT FROM OLD.scoring_options_hash
  ) THEN
    RAISE EXCEPTION 'dispatched Query Quality Chunk Intent is immutable: %', OLD.quality_chunk_id
      USING ERRCODE='check_violation';
  END IF;
  IF OLD.status IN ('done','failed','cancelled') AND NEW.status IS DISTINCT FROM OLD.status THEN
    RAISE EXCEPTION 'terminal Query Quality Chunk cannot regress: %', OLD.quality_chunk_id
      USING ERRCODE='check_violation';
  END IF;
  IF OLD.dispatch_status='terminal' AND NEW.dispatch_status<>'terminal' THEN
    RAISE EXCEPTION 'terminal Query Quality dispatch cannot regress: %', OLD.quality_chunk_id
      USING ERRCODE='check_violation';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_query_quality_chunk_state ON crawler.query_quality_chunks;
CREATE TRIGGER trg_guard_query_quality_chunk_state
BEFORE UPDATE ON crawler.query_quality_chunks
FOR EACH ROW
EXECUTE FUNCTION crawler.guard_query_quality_chunk_state();

CREATE OR REPLACE FUNCTION crawler.guard_query_quality_chunk_members()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
  target_chunk_id TEXT;
  target_status TEXT;
BEGIN
  target_chunk_id := CASE WHEN TG_OP='DELETE' THEN OLD.quality_chunk_id ELSE NEW.quality_chunk_id END;
  IF TG_OP='DELETE' AND pg_trigger_depth()>1 THEN
    RETURN OLD;
  END IF;
  SELECT status INTO target_status
  FROM crawler.query_quality_chunks
  WHERE quality_chunk_id=target_chunk_id
  FOR KEY SHARE;
  IF target_status IS NULL THEN
    RAISE EXCEPTION 'Query Quality Chunk does not exist: %', target_chunk_id
      USING ERRCODE='foreign_key_violation';
  END IF;
  IF target_status<>'pending' THEN
    RAISE EXCEPTION 'Query Quality Chunk members are frozen after dispatch: %', target_chunk_id
      USING ERRCODE='check_violation';
  END IF;
  RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_query_quality_chunk_members
ON crawler.query_quality_chunk_members;
CREATE TRIGGER trg_guard_query_quality_chunk_members
BEFORE INSERT OR UPDATE OR DELETE ON crawler.query_quality_chunk_members
FOR EACH ROW
EXECUTE FUNCTION crawler.guard_query_quality_chunk_members();

CREATE INDEX IF NOT EXISTS idx_crawler_channel_execution_attempts_channel
ON crawler.channel_execution_attempts (channel_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_channel_execution_attempts_proxy
ON crawler.channel_execution_attempts (proxy_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_channel_execution_attempts_status
ON crawler.channel_execution_attempts (status, started_at DESC);
-- qy-fingerprint-schema:end
-- qy-rota-worker-v2-schema:end

ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS country TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS country_source TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS country_code TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS country_canonical_name TEXT;
UPDATE crawler.channels
SET country = NULL
WHERE country IS NOT NULL AND NULLIF(btrim(country), '') IS NULL;
UPDATE crawler.channels
SET country_source = NULL
WHERE country_source = 'youtube_about' AND country IS NULL;
ALTER TABLE crawler.channels
DROP CONSTRAINT IF EXISTS channels_youtube_about_country_check;
ALTER TABLE crawler.channels
ADD CONSTRAINT channels_youtube_about_country_check
CHECK (country_source IS DISTINCT FROM 'youtube_about' OR NULLIF(btrim(country), '') IS NOT NULL);
ALTER TABLE crawler.channels
DROP CONSTRAINT IF EXISTS channels_country_code_check;
ALTER TABLE crawler.channels
ADD CONSTRAINT channels_country_code_check
CHECK (country_code IS NULL OR country_code ~ '^[A-Z]{2}$');
UPDATE crawler.channels
SET ready_for_agent=false,
    agent_status=CASE WHEN agent_status='done' THEN 'done' ELSE 'pending' END,
    agent_error_message='base info incomplete before Agent dispatch',
    updated_at=now()
WHERE ready_for_agent=true
  AND (subscriber_count IS NULL OR NULLIF(btrim(title), '') IS NULL);
ALTER TABLE crawler.channels
DROP CONSTRAINT IF EXISTS channels_agent_base_info_check;
ALTER TABLE crawler.channels
ADD CONSTRAINT channels_agent_base_info_check
CHECK (NOT ready_for_agent OR (subscriber_count IS NOT NULL AND NULLIF(btrim(title), '') IS NOT NULL));
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS agent_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS agent_next_retry_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS agent_error_message TEXT;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS content_limit INTEGER NOT NULL DEFAULT 30;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS expected_content_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS detail_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS candidate_id BIGINT REFERENCES crawler.channel_candidates(candidate_id) ON DELETE SET NULL;

ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS publication_finalized_status TEXT;

ALTER TABLE crawler.channel_runs
ADD COLUMN IF NOT EXISTS publication_finalized_at TIMESTAMPTZ;

ALTER TABLE crawler.channel_runs
DROP CONSTRAINT IF EXISTS channel_runs_publication_finalize_check;
ALTER TABLE crawler.channel_runs
ADD CONSTRAINT channel_runs_publication_finalize_check CHECK (
  (publication_finalized_status IS NULL AND publication_finalized_at IS NULL)
  OR (
    publication_finalized_status IN ('ready_auto', 'ready_partial')
    AND publication_finalized_at IS NOT NULL
  )
);

CREATE OR REPLACE FUNCTION crawler.guard_channel_run_publication_finalize()
RETURNS trigger
LANGUAGE plpgsql
AS $crawler_finalize_guard$
BEGIN
  IF OLD.publication_finalized_at IS NOT NULL AND (
    NEW.publication_finalized_at IS DISTINCT FROM OLD.publication_finalized_at
    OR NEW.publication_finalized_status IS NULL
    OR (
      OLD.publication_finalized_status='ready_auto'
      AND NEW.publication_finalized_status<>'ready_auto'
    )
    OR (
      OLD.publication_finalized_status='ready_partial'
      AND NEW.publication_finalized_status NOT IN ('ready_partial','ready_auto')
    )
  ) THEN
    RAISE EXCEPTION 'Channel Run Publication Finalize evidence cannot regress'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$crawler_finalize_guard$;

DROP TRIGGER IF EXISTS trg_channel_run_publication_finalize ON crawler.channel_runs;
CREATE TRIGGER trg_channel_run_publication_finalize
BEFORE UPDATE ON crawler.channel_runs
FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_run_publication_finalize();

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_channel_runs_identity
ON crawler.channel_runs (run_id, channel_id, candidate_id);

ALTER TABLE crawler.channel_runs
DROP CONSTRAINT IF EXISTS channel_runs_candidate_channel_fk;
ALTER TABLE crawler.channel_runs
ADD CONSTRAINT channel_runs_candidate_channel_fk
FOREIGN KEY (candidate_id,channel_id)
REFERENCES crawler.channel_candidates(candidate_id,channel_id)
DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE crawler.channels
DROP CONSTRAINT IF EXISTS channels_registry_promotion_pair_check;
ALTER TABLE crawler.channels
ADD CONSTRAINT channels_registry_promotion_pair_check CHECK (
  (registry_promotion_candidate_id IS NULL AND registry_promotion_run_id IS NULL)
  OR (registry_promotion_candidate_id IS NOT NULL AND registry_promotion_run_id IS NOT NULL)
);

ALTER TABLE crawler.channels
DROP CONSTRAINT IF EXISTS channels_registry_promotion_candidate_fk;
ALTER TABLE crawler.channels
ADD CONSTRAINT channels_registry_promotion_candidate_fk
FOREIGN KEY (registry_promotion_candidate_id,channel_id)
REFERENCES crawler.channel_candidates(candidate_id,channel_id)
DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE crawler.channels
DROP CONSTRAINT IF EXISTS channels_registry_promotion_run_fk;
ALTER TABLE crawler.channels
ADD CONSTRAINT channels_registry_promotion_run_fk
FOREIGN KEY (registry_promotion_run_id,channel_id,registry_promotion_candidate_id)
REFERENCES crawler.channel_runs(run_id,channel_id,candidate_id)
DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_channels_registry_promotion_candidate
ON crawler.channels (registry_promotion_candidate_id)
WHERE registry_promotion_candidate_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_channels_registry_promotion_run
ON crawler.channels (registry_promotion_run_id)
WHERE registry_promotion_run_id IS NOT NULL;

CREATE OR REPLACE FUNCTION crawler.guard_channel_registry_promotion_run()
RETURNS trigger
LANGUAGE plpgsql
AS $crawler_registry_run_guard$
BEGIN
  IF TG_OP='INSERT' AND EXISTS (
    SELECT 1
    FROM crawler.channels AS channel
    WHERE channel.registry_promotion_run_id=NEW.run_id
      AND (
        channel.channel_id IS DISTINCT FROM NEW.channel_id
        OR channel.registry_promotion_candidate_id IS DISTINCT FROM NEW.candidate_id
        OR NEW.crawl_mode IS DISTINCT FROM 'full'
      )
  ) THEN
    RAISE EXCEPTION 'Channel Registry promotion Run identity is immutable and must be Full Crawl'
      USING ERRCODE = '55000';
  ELSIF TG_OP='UPDATE' AND EXISTS (
    SELECT 1
    FROM crawler.channels AS channel
    WHERE channel.registry_promotion_run_id=OLD.run_id
  ) AND (
    NEW.run_id IS DISTINCT FROM OLD.run_id
    OR NEW.channel_id IS DISTINCT FROM OLD.channel_id
    OR NEW.candidate_id IS DISTINCT FROM OLD.candidate_id
    OR NEW.crawl_mode IS DISTINCT FROM 'full'
  ) THEN
    RAISE EXCEPTION 'Channel Registry promotion Run identity is immutable and must be Full Crawl'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$crawler_registry_run_guard$;

DROP TRIGGER IF EXISTS trg_channel_registry_promotion_run ON crawler.channel_runs;
CREATE TRIGGER trg_channel_registry_promotion_run
BEFORE INSERT OR UPDATE ON crawler.channel_runs
FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_registry_promotion_run();

CREATE OR REPLACE FUNCTION crawler.guard_channel_registry_promotion_candidate()
RETURNS trigger
LANGUAGE plpgsql
AS $crawler_registry_candidate_guard$
BEGIN
  IF OLD.accepted_at IS NOT NULL
     AND EXISTS (
       SELECT 1
       FROM crawler.channels AS channel
       WHERE channel.registry_promotion_candidate_id=OLD.candidate_id
         AND channel.channel_id=OLD.channel_id
     )
     AND (
       NEW.channel_id IS DISTINCT FROM OLD.channel_id
       OR NEW.status IS DISTINCT FROM 'accepted'
       OR NEW.accepted_at IS DISTINCT FROM OLD.accepted_at
     ) THEN
    RAISE EXCEPTION 'Accepted Channel Registry promotion Candidate is immutable'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$crawler_registry_candidate_guard$;

DROP TRIGGER IF EXISTS trg_channel_registry_promotion_candidate
ON crawler.channel_candidates;
CREATE TRIGGER trg_channel_registry_promotion_candidate
BEFORE UPDATE ON crawler.channel_candidates
FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_registry_promotion_candidate();

-- This Crawler guard must also work before the optional Publication schemas are installed.
CREATE OR REPLACE FUNCTION crawler.registry_promotion_is_complete(
  target_channel_id TEXT,
  target_run_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $crawler_registry_promotion_complete$
DECLARE
  registry_complete BOOLEAN;
  publication_table_count INTEGER;
  publication_complete BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM crawler.channels AS channel
    JOIN crawler.channel_candidates AS candidate
      ON candidate.candidate_id=channel.registry_promotion_candidate_id
     AND candidate.channel_id=channel.channel_id
     AND candidate.status='accepted'
     AND candidate.accepted_at IS NOT NULL
    JOIN crawler.channel_runs AS promotion_run
      ON promotion_run.run_id=channel.registry_promotion_run_id
     AND promotion_run.channel_id=channel.channel_id
     AND promotion_run.candidate_id=candidate.candidate_id
     AND promotion_run.crawl_mode='full'
    WHERE channel.channel_id=target_channel_id
      AND promotion_run.run_id=target_run_id
      AND promotion_run.publication_finalized_status='ready_auto'
      AND promotion_run.publication_finalized_at IS NOT NULL
  ) INTO registry_complete;

  IF NOT registry_complete THEN
    RETURN FALSE;
  END IF;

  SELECT count(*)::integer
  INTO publication_table_count
  FROM unnest(ARRAY[
    'publication.stream',
    'publication.channel_stream_state',
    'publication.channel_delivery_state',
    'publication.domain_current',
    'publication.revision',
    'publication.outbox'
  ]::text[]) AS required_table(table_name)
  WHERE to_regclass(required_table.table_name) IS NOT NULL;

  IF publication_table_count = 0 THEN
    RETURN TRUE;
  END IF;
  IF publication_table_count <> 6 THEN
    RETURN FALSE;
  END IF;

  EXECUTE $publication_promotion_complete$
    SELECT
      EXISTS (
        SELECT 1
        FROM publication.channel_stream_state AS owner
        WHERE owner.channel_id=$1
          AND owner.status='owned'
          AND owner.onboarding_mode='bootstrap'
          AND owner.seed_status='complete'
          AND owner.ownership_reference->>'onboarding_mode'='automatic_bootstrap'
          AND owner.ownership_reference->>'initial_full_run_id'=$2
      )
      OR (
        NOT EXISTS (
          SELECT 1
          FROM publication.channel_stream_state AS pending_owner
          WHERE pending_owner.channel_id=$1
            AND pending_owner.status='owned'
            AND pending_owner.onboarding_mode='bootstrap'
            AND pending_owner.seed_status='pending'
            AND pending_owner.ownership_reference->>'onboarding_mode'='automatic_bootstrap'
            AND pending_owner.ownership_reference->>'initial_full_run_id'=$2
        )
        AND NOT EXISTS (
          SELECT 1
          FROM crawler.channels AS channel
          JOIN crawler.channel_candidates AS candidate
            ON candidate.candidate_id=channel.registry_promotion_candidate_id
           AND candidate.channel_id=channel.channel_id
          JOIN publication.stream AS stream
            ON stream.status='active'
           AND stream.capture_enabled_at IS NOT NULL
           AND candidate.accepted_at>=stream.capture_enabled_at
          WHERE channel.channel_id=$1
            AND channel.registry_promotion_run_id=$2
            AND EXISTS (
              SELECT 1
              FROM publication.channel_delivery_state AS delivery
              JOIN publication.channel_stream_state AS route_owner
                ON route_owner.publication_stream_id=delivery.publication_stream_id
               AND route_owner.channel_id=delivery.channel_id
              WHERE delivery.publication_stream_id=stream.publication_stream_id
                AND route_owner.status='owned'
                AND delivery.mode='online'
            )
            AND NOT EXISTS (
              SELECT 1
              FROM publication.channel_delivery_state AS delivery
              JOIN publication.channel_stream_state AS route_owner
                ON route_owner.publication_stream_id=delivery.publication_stream_id
               AND route_owner.channel_id=delivery.channel_id
              WHERE delivery.publication_stream_id=stream.publication_stream_id
                AND route_owner.status='owned'
                AND delivery.mode<>'online'
            )
        )
      )
  $publication_promotion_complete$
  INTO publication_complete
  USING target_channel_id,target_run_id;

  RETURN COALESCE(publication_complete,FALSE);
END
$crawler_registry_promotion_complete$;

CREATE OR REPLACE FUNCTION crawler.registry_publication_gap_repair_is_allowed(
  target_channel_id TEXT,
  target_parent_run_id TEXT,
  target_repair_run_id TEXT
)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $crawler_registry_publication_gap_repair$
  SELECT EXISTS (
    SELECT 1
    FROM crawler.channels AS channel
    JOIN crawler.channel_runs AS promotion_run
      ON promotion_run.run_id=channel.registry_promotion_run_id
     AND promotion_run.channel_id=channel.channel_id
     AND promotion_run.crawl_mode='full'
    JOIN crawler.channel_runs AS source_run
      ON source_run.run_id=target_parent_run_id
     AND source_run.channel_id=channel.channel_id
    JOIN crawler.channel_runs AS repair_run
      ON repair_run.run_id=target_repair_run_id
     AND repair_run.channel_id=channel.channel_id
    WHERE channel.channel_id=target_channel_id
      AND channel.latest_run_id=target_parent_run_id
      AND promotion_run.publication_finalized_status='ready_auto'
      AND promotion_run.publication_finalized_at IS NOT NULL
      AND promotion_run.result_json#>>'{publication_gap_repair,status}'='required'
      AND source_run.publication_finalized_status='ready_auto'
      AND source_run.publication_finalized_at IS NOT NULL
      AND source_run.result_json#>>'{publication_gap_repair,status}'='required'
      AND (
        source_run.run_id=promotion_run.run_id
        OR (
          source_run.result_json#>>'{final_repair,parent_run_id}'=promotion_run.run_id
          AND source_run.result_json#>>'{final_repair,mode}'='channel'
          AND source_run.result_json#>>'{publication_gap_repair,root_run_id}'
                =promotion_run.run_id
        )
      )
      AND EXISTS (
        SELECT 1
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(source_run.result_json#>'{publication_gap_repair,domains}')='array'
              THEN source_run.result_json#>'{publication_gap_repair,domains}'
            ELSE '[]'::jsonb
          END
        ) AS missing_domain(value)
        WHERE missing_domain.value IN ('channel','video')
      )
      AND repair_run.status='running'
      AND repair_run.crawl_mode='full'
      AND repair_run.publication_finalized_at IS NULL
      AND repair_run.result_json#>>'{final_repair,parent_run_id}'=promotion_run.run_id
      AND repair_run.result_json#>>'{final_repair,mode}'='channel'
      AND COALESCE(repair_run.result_json#>>'{final_repair,rounds}','') ~ '^[1-9][0-9]*$'
      AND repair_run.result_json#>>'{publication_gap_repair,status}'='required'
      AND repair_run.result_json#>>'{publication_gap_repair,root_run_id}'=promotion_run.run_id
      AND NOT EXISTS (
        SELECT source_domain.value
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(source_run.result_json#>'{publication_gap_repair,domains}')='array'
              THEN source_run.result_json#>'{publication_gap_repair,domains}'
            ELSE '[]'::jsonb
          END
        ) AS source_domain(value)
        WHERE source_domain.value IN ('channel','video')
        EXCEPT
        SELECT repair_domain.value
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(repair_run.result_json#>'{publication_gap_repair,domains}')='array'
              THEN repair_run.result_json#>'{publication_gap_repair,domains}'
            ELSE '[]'::jsonb
          END
        ) AS repair_domain(value)
        WHERE repair_domain.value IN ('channel','video')
      )
      AND NOT EXISTS (
        SELECT repair_domain.value
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(repair_run.result_json#>'{publication_gap_repair,domains}')='array'
              THEN repair_run.result_json#>'{publication_gap_repair,domains}'
            ELSE '[]'::jsonb
          END
        ) AS repair_domain(value)
        WHERE repair_domain.value IN ('channel','video')
        EXCEPT
        SELECT source_domain.value
        FROM jsonb_array_elements_text(
          CASE
            WHEN jsonb_typeof(source_run.result_json#>'{publication_gap_repair,domains}')='array'
              THEN source_run.result_json#>'{publication_gap_repair,domains}'
            ELSE '[]'::jsonb
          END
        ) AS source_domain(value)
        WHERE source_domain.value IN ('channel','video')
      )
  )
$crawler_registry_publication_gap_repair$;

CREATE OR REPLACE FUNCTION crawler.guard_channel_registry_promotion()
RETURNS trigger
LANGUAGE plpgsql
AS $crawler_registry_guard$
BEGIN
  IF TG_OP='UPDATE' AND (
    NEW.registry_promotion_candidate_id IS DISTINCT FROM OLD.registry_promotion_candidate_id
    OR NEW.registry_promotion_run_id IS DISTINCT FROM OLD.registry_promotion_run_id
  ) THEN
    RAISE EXCEPTION 'Channel Registry promotion evidence is immutable'
      USING ERRCODE = '55000';
  END IF;
  IF TG_OP='UPDATE'
     AND OLD.registry_promotion_run_id IS NOT NULL
     AND NEW.latest_run_id IS DISTINCT FROM OLD.latest_run_id
     AND NEW.latest_run_id IS DISTINCT FROM OLD.registry_promotion_run_id
     AND NOT crawler.registry_promotion_is_complete(
       OLD.channel_id,
       OLD.registry_promotion_run_id
     )
     AND NOT crawler.registry_publication_gap_repair_is_allowed(
       OLD.channel_id,
       OLD.latest_run_id,
       NEW.latest_run_id
     ) THEN
    RAISE EXCEPTION 'Channel Registry promotion Run must Finalize before a later Run'
      USING ERRCODE = '55000';
  END IF;
  IF NEW.registry_promotion_run_id IS NOT NULL AND EXISTS (
    SELECT 1
    FROM crawler.channel_runs AS run
    WHERE run.run_id=NEW.registry_promotion_run_id
      AND (
        run.channel_id IS DISTINCT FROM NEW.channel_id
        OR run.candidate_id IS DISTINCT FROM NEW.registry_promotion_candidate_id
        OR run.crawl_mode IS DISTINCT FROM 'full'
      )
  ) THEN
    RAISE EXCEPTION 'Channel Registry promotion Run must be its Full Crawl'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$crawler_registry_guard$;

DROP TRIGGER IF EXISTS trg_channel_registry_promotion_immutable ON crawler.channels;
CREATE TRIGGER trg_channel_registry_promotion_immutable
BEFORE INSERT OR UPDATE ON crawler.channels
FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_registry_promotion();

CREATE INDEX IF NOT EXISTS idx_crawler_channel_runs_candidate
ON crawler.channel_runs (candidate_id)
WHERE candidate_id IS NOT NULL;

ALTER TABLE crawler.channel_runs DROP CONSTRAINT IF EXISTS channel_runs_status_check;
ALTER TABLE crawler.channel_runs ADD CONSTRAINT channel_runs_status_check
CHECK (status IN ('queued', 'running', 'waiting_pages', 'waiting_detail', 'waiting_agent', 'finalizing', 'done', 'failed', 'skipped'));

ALTER TABLE crawler.channel_runs DROP CONSTRAINT IF EXISTS channel_runs_detail_status_check;
ALTER TABLE crawler.channel_runs ADD CONSTRAINT channel_runs_detail_status_check
CHECK (detail_status IN ('pending', 'queued', 'running', 'api_pending', 'done', 'failed'));

CREATE TABLE IF NOT EXISTS crawler.channel_tab_pages (
  run_id TEXT NOT NULL REFERENCES crawler.channel_runs(run_id) ON DELETE CASCADE,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  tab TEXT NOT NULL CHECK (tab IN ('videos', 'shorts', 'lives')),
  page_no INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'done', 'failed', 'skipped')),
  continuation_token TEXT,
  item_count INTEGER NOT NULL DEFAULT 0,
  recent_cutoff_hit BOOLEAN NOT NULL DEFAULT false,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, tab, page_no)
);

CREATE TABLE IF NOT EXISTS crawler.contents (
  content_key TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  run_id TEXT REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL,
  content_type TEXT NOT NULL CHECK (content_type IN ('video', 'short', 'live', 'post')),
  content_type_source TEXT,
  source_content_id TEXT NOT NULL,
  position INTEGER,
  title TEXT,
  url TEXT,
  thumbnail_url TEXT,
  description TEXT,
  description_status TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (description_status IN ('exact', 'empty', 'unavailable', 'unresolved')),
  description_source TEXT,
  hashtags TEXT[] NOT NULL DEFAULT '{}'::text[],
  keywords TEXT[] NOT NULL DEFAULT '{}'::text[],
  published_text_raw TEXT,
  published_at TIMESTAMPTZ,
  published_at_status TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (published_at_status IN ('exact', 'relative', 'estimated', 'unavailable', 'unresolved')),
  published_at_source TEXT,
  published_at_precision TEXT NOT NULL DEFAULT 'unknown'
    CHECK (published_at_precision IN ('second', 'date_only', 'unknown')),
  is_recent BOOLEAN NOT NULL DEFAULT true,
  length_text TEXT,
  duration_seconds INTEGER,
  duration_status TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (duration_status IN ('exact', 'unavailable', 'unresolved')),
  duration_source TEXT,
  view_count_text TEXT,
  view_count_status TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (view_count_status IN ('exact', 'estimated', 'unavailable', 'unresolved')),
  view_count_source TEXT,
  like_count BIGINT,
  like_count_status TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (like_count_status IN ('exact', 'zero_from_empty', 'unavailable', 'unresolved')),
  like_count_source TEXT,
  comment_count BIGINT,
  comment_count_status TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (comment_count_status IN ('exact', 'zero_from_empty', 'zero_from_surface', 'zero_from_upcoming', 'disabled', 'unavailable', 'unresolved')),
  comments_disabled BOOLEAN,
  comment_count_source TEXT,
  comments_first_page JSONB,
  is_members_only BOOLEAN NOT NULL DEFAULT false,
  access_status TEXT NOT NULL DEFAULT 'unknown'
    CHECK (access_status IN ('public', 'unlisted', 'members_only', 'private', 'unavailable', 'login_required', 'unknown')),
  access_status_source TEXT,
  live_scheduled_at TIMESTAMPTZ,
  live_started_at TIMESTAMPTZ,
  live_ended_at TIMESTAMPTZ,
  extractor_version TEXT,
  raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_enriched_at TIMESTAMPTZ,
  CONSTRAINT contents_comment_state_shape CHECK (
    (comments_disabled IS TRUE AND comment_count=0 AND comment_count_status='disabled')
    OR (comments_disabled IS DISTINCT FROM TRUE AND comment_count_status<>'disabled')
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_contents_channel_source
ON crawler.contents (channel_id, source_content_id);

CREATE INDEX IF NOT EXISTS idx_crawler_contents_channel
ON crawler.contents (channel_id, content_type, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_contents_published_status
ON crawler.contents (published_at_status, last_seen_at DESC);

ALTER TABLE crawler.contents
ADD COLUMN IF NOT EXISTS is_recent BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_published_at_status_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_published_at_status_check
CHECK (published_at_status IN ('exact', 'relative', 'estimated', 'unavailable', 'unresolved'));

ALTER TABLE crawler.contents
ADD COLUMN IF NOT EXISTS view_count_status TEXT NOT NULL DEFAULT 'unresolved';

ALTER TABLE crawler.contents
ADD COLUMN IF NOT EXISTS like_count_status TEXT NOT NULL DEFAULT 'unresolved';

ALTER TABLE crawler.contents
ADD COLUMN IF NOT EXISTS is_members_only BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE crawler.contents
ADD COLUMN IF NOT EXISTS access_status TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE crawler.contents
ADD COLUMN IF NOT EXISTS access_status_source TEXT;

ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS content_type_source TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS position INTEGER;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS duration_seconds INTEGER;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS duration_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS comment_count_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS comments_disabled BOOLEAN;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS published_at_precision TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS duration_source TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS view_count_source TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS like_count_source TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS comment_count_source TEXT;
-- youtube-comment-first-page-schema:start
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS comments_first_page JSONB;
ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_comments_first_page_shape_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_comments_first_page_shape_check
CHECK (
  comments_first_page IS NULL
  OR (
    jsonb_typeof(comments_first_page) = 'object'
    AND comments_first_page->>'version' = '1'
    AND comments_first_page->>'sort' = 'TOP_COMMENTS'
    AND jsonb_typeof(comments_first_page->'comments') = 'array'
    AND (comments_first_page->>'returned_count') ~ '^[0-9]+$'
    AND (comments_first_page->>'returned_count')::int = jsonb_array_length(comments_first_page->'comments')
  )
);
-- youtube-comment-first-page-schema:end
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS live_scheduled_at TIMESTAMPTZ;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS live_started_at TIMESTAMPTZ;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS live_ended_at TIMESTAMPTZ;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS extractor_version TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS description_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS description_source TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS hashtags TEXT[] NOT NULL DEFAULT '{}'::text[];
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS keywords TEXT[] NOT NULL DEFAULT '{}'::text[];

ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_description_status_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_description_status_check
CHECK (description_status IN ('exact', 'empty', 'unavailable', 'unresolved'));

ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_published_at_precision_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_published_at_precision_check
CHECK (published_at_precision IN ('second', 'date_only', 'unknown'));

ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_comment_count_status_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_comment_count_status_check
CHECK (comment_count_status IN ('exact', 'zero_from_empty', 'zero_from_surface', 'zero_from_upcoming', 'disabled', 'unavailable', 'unresolved'));

UPDATE crawler.contents
SET published_at_precision = CASE
  WHEN published_at IS NULL THEN 'unknown'
  WHEN date_trunc('day', published_at) <> published_at THEN 'second'
  ELSE 'date_only'
END
WHERE published_at_precision = 'unknown';

UPDATE crawler.contents
SET view_count_status = 'exact'
WHERE view_count_text IS NOT NULL
  AND view_count_status = 'unresolved';

UPDATE crawler.contents
SET like_count_status = 'exact'
WHERE like_count IS NOT NULL
  AND like_count_status = 'unresolved';

CREATE INDEX IF NOT EXISTS idx_crawler_contents_recent
ON crawler.contents (channel_id, is_recent, content_type, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_contents_current_run
ON crawler.contents (channel_id, run_id, position ASC);

CREATE INDEX IF NOT EXISTS idx_crawler_contents_hashtags
ON crawler.contents USING GIN (hashtags);

CREATE INDEX IF NOT EXISTS idx_crawler_contents_keywords
ON crawler.contents USING GIN (keywords);

CREATE TABLE IF NOT EXISTS crawler.content_candidates (
  candidate_id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES crawler.channel_runs(run_id) ON DELETE CASCADE,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  source_content_id TEXT NOT NULL,
  position INTEGER NOT NULL,
  title TEXT,
  source_url TEXT,
  thumbnail_url TEXT,
  content_type TEXT CHECK (content_type IN ('video', 'short', 'live')),
  type_status TEXT NOT NULL DEFAULT 'unresolved'
    CHECK (type_status IN ('unresolved', 'resolved', 'unavailable')),
  type_source TEXT,
  detail_status TEXT NOT NULL DEFAULT 'queued'
    CHECK (detail_status IN ('queued', 'running', 'api_pending', 'done', 'unavailable', 'failed')),
  api_status TEXT NOT NULL DEFAULT 'not_needed'
    CHECK (api_status IN ('not_needed', 'pending', 'queued', 'running', 'done', 'failed', 'unavailable')),
  missing_fields TEXT[] NOT NULL DEFAULT '{}'::text[],
  attempts INTEGER NOT NULL DEFAULT 0,
  content_key TEXT REFERENCES crawler.contents(content_key) ON DELETE SET NULL,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  disposition TEXT,
  next_attempt_at TIMESTAMPTZ,
  first_seen_ledger_status TEXT NOT NULL DEFAULT 'not_applicable',
  first_seen_ledger_observation_id UUID,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  CONSTRAINT content_candidates_disposition_kind_check
    CHECK (disposition IN ('stored', 'deferred', 'terminal_excluded')),
  CONSTRAINT content_candidates_disposition_schedule_check
    CHECK (
      (disposition IS NULL AND next_attempt_at IS NULL)
      OR (disposition='stored' AND next_attempt_at IS NULL)
      OR (
        disposition IN ('deferred','terminal_excluded')
        AND next_attempt_at IS NOT NULL
      )
    ),
  CONSTRAINT content_candidates_first_seen_ledger_shape_check
    CHECK (
      (first_seen_ledger_status='not_applicable' AND first_seen_ledger_observation_id IS NULL)
      OR (first_seen_ledger_status='pending' AND first_seen_ledger_observation_id IS NULL)
      OR (first_seen_ledger_status='consumed' AND first_seen_ledger_observation_id IS NOT NULL)
    ),
  UNIQUE (run_id, source_content_id),
  UNIQUE (run_id, position)
);

-- video-disposition-schema:start
ALTER TABLE crawler.content_candidates
ADD COLUMN IF NOT EXISTS disposition TEXT;

ALTER TABLE crawler.content_candidates
ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

WITH legacy_video_disposition_backfill AS (
  SELECT
    candidate.candidate_id,
    COALESCE(candidate.finished_at,candidate.updated_at,candidate.first_seen_at,now()) AS observed_at,
    CASE
      WHEN candidate.result_json->'scope'->>'reason'='upcoming_live'
        THEN 'terminal_excluded'
      WHEN candidate.result_json->'scope'->>'reason'
        IN ('older_than_max_age','after_chronological_age_cutoff')
        THEN 'terminal_excluded'
      WHEN candidate.content_key IS NOT NULL THEN 'stored'
      WHEN candidate.result_json#>>'{access,access_status}' IN ('private','unavailable')
        THEN 'terminal_excluded'
      ELSE 'deferred'
    END AS disposition,
    CASE
      WHEN candidate.result_json->'scope'->>'reason'='upcoming_live' THEN 'upcoming_live'
      WHEN candidate.result_json->'scope'->>'reason'
        IN ('older_than_max_age','after_chronological_age_cutoff')
        THEN 'outside_content_window'
      WHEN candidate.content_key IS NOT NULL THEN 'legacy_content_already_stored'
      WHEN candidate.result_json#>>'{access,access_status}'='private' THEN 'access_private'
      WHEN candidate.result_json#>>'{access,access_status}'='unavailable' THEN 'access_unavailable'
      WHEN candidate.detail_status='failed' THEN 'detail_collection_failed'
      WHEN candidate.api_status IN ('pending','queued','running','failed')
        THEN 'legacy_api_resolution_pending'
      ELSE 'legacy_terminal_unresolved'
    END AS reason_code,
    CASE
      WHEN candidate.result_json->'scope'->>'reason'
        IN ('older_than_max_age','after_chronological_age_cutoff')
        THEN 'low_frequency_policy_recheck'
      WHEN candidate.result_json->'scope'->>'reason'='upcoming_live'
        THEN 'low_frequency_access_recheck'
      WHEN candidate.content_key IS NOT NULL THEN NULL
      WHEN candidate.result_json#>>'{access,access_status}' IN ('private','unavailable')
        THEN 'low_frequency_access_recheck'
      WHEN candidate.detail_status='failed' THEN 'player_retry'
      ELSE 'legacy_recovery'
    END AS retry_class,
    CASE
      WHEN candidate.result_json->'scope'->>'reason'
        IN ('older_than_max_age','after_chronological_age_cutoff')
        THEN interval '30 days'
      WHEN candidate.result_json->'scope'->>'reason'='upcoming_live' THEN interval '1 day'
      WHEN candidate.content_key IS NOT NULL THEN NULL
      WHEN candidate.result_json#>>'{access,access_status}' IN ('private','unavailable')
        THEN interval '7 days'
      WHEN candidate.detail_status='failed' THEN interval '1 hour'
      ELSE interval '6 hours'
    END AS retry_after
  FROM crawler.content_candidates candidate
  WHERE candidate.disposition IS NULL
), resolved_video_disposition_backfill AS (
  SELECT
    backfill.*,
    CASE
      WHEN backfill.disposition='stored' THEN NULL
      ELSE backfill.observed_at+backfill.retry_after
    END AS next_attempt_at
  FROM legacy_video_disposition_backfill backfill
)
UPDATE crawler.content_candidates candidate
SET disposition=backfill.disposition,
    next_attempt_at=backfill.next_attempt_at,
    result_json=COALESCE(candidate.result_json,'{}'::jsonb)
      || jsonb_build_object(
           'disposition',jsonb_build_object(
             'version','video-disposition-v1',
             'kind',backfill.disposition,
             'reason_code',backfill.reason_code,
             'retry_class',backfill.retry_class,
             'retryable',backfill.disposition='deferred',
             'observed_at',backfill.observed_at,
             'next_attempt_at',backfill.next_attempt_at
           ),
           'legacy_video_disposition_backfill',jsonb_build_object(
             'applied_at',now(),
             'prior_detail_status',candidate.detail_status,
             'prior_api_status',candidate.api_status,
             'prior_error_message',candidate.error_message
           )
         ),
    error_message=CASE
      WHEN backfill.disposition='deferred'
        THEN COALESCE(
          NULLIF(btrim(candidate.error_message),''),
          'legacy Candidate lacked an explicit Video disposition and requires recovery'
        )
      ELSE candidate.error_message
    END,
    updated_at=now()
FROM resolved_video_disposition_backfill backfill
WHERE candidate.candidate_id=backfill.candidate_id;

ALTER TABLE crawler.content_candidates
DROP CONSTRAINT IF EXISTS content_candidates_disposition_kind_check;
ALTER TABLE crawler.content_candidates
ADD CONSTRAINT content_candidates_disposition_kind_check
CHECK (disposition IN ('stored', 'deferred', 'terminal_excluded'));

ALTER TABLE crawler.content_candidates
DROP CONSTRAINT IF EXISTS content_candidates_disposition_schedule_check;
ALTER TABLE crawler.content_candidates
ADD CONSTRAINT content_candidates_disposition_schedule_check
CHECK (
  (disposition IS NULL AND next_attempt_at IS NULL)
  OR (disposition='stored' AND next_attempt_at IS NULL)
  OR (
    disposition IN ('deferred','terminal_excluded')
    AND next_attempt_at IS NOT NULL
  )
);

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_batch
ON crawler.content_candidates (run_id, detail_status, position ASC);

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_disposition_due
ON crawler.content_candidates (disposition, next_attempt_at ASC, candidate_id ASC)
WHERE disposition IN ('deferred', 'terminal_excluded') AND next_attempt_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_channel_disposition_due
ON crawler.content_candidates (channel_id, disposition, next_attempt_at ASC, candidate_id ASC)
WHERE disposition IN ('deferred', 'terminal_excluded') AND next_attempt_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_disposition_history
ON crawler.content_candidates (channel_id, source_content_id, candidate_id DESC);
-- video-disposition-schema:end

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_repairable_run
ON crawler.content_candidates (run_id) INCLUDE (content_key)
WHERE COALESCE(result_json->'scope'->>'status','')<>'excluded'
  AND NOT (result_json ? 'parser_contract_error')
  AND (
    detail_status='failed'
    OR missing_fields @> ARRAY['content_type']::text[]
    OR (
      detail_status IN ('done','unavailable')
      AND cardinality(missing_fields)>0
      AND COALESCE(result_json#>>'{access,access_status}','unknown')
        NOT IN ('members_only','private','unlisted','unavailable')
    )
  );

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_content_key
ON crawler.content_candidates (content_key)
WHERE content_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_api
ON crawler.content_candidates (api_status, updated_at ASC)
WHERE api_status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_open_api_source
ON crawler.content_candidates (source_content_id, candidate_id)
WHERE detail_status = 'api_pending'
  AND api_status IN ('pending', 'queued', 'running');

CREATE TABLE IF NOT EXISTS crawler.youtube_api_tasks (
  task_id BIGSERIAL PRIMARY KEY,
  source_content_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'queued', 'running', 'done', 'failed', 'unavailable')),
  missing_fields TEXT[] NOT NULL DEFAULT '{}'::text[],
  candidate_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
  attempts INTEGER NOT NULL DEFAULT 0,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  next_retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_crawler_youtube_api_tasks_claim
ON crawler.youtube_api_tasks (status, next_retry_at, created_at ASC);

CREATE TABLE IF NOT EXISTS crawler.youtube_api_batches (
  batch_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'done', 'failed')),
  task_ids BIGINT[] NOT NULL,
  video_ids TEXT[] NOT NULL,
  key_index INTEGER,
  active_job_id TEXT,
  active_job_attempt BIGINT,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT youtube_api_batches_active_job_check CHECK (
    (active_job_id IS NULL AND active_job_attempt IS NULL)
    OR (
      active_job_id IS NOT NULL
      AND active_job_attempt IS NOT NULL
      AND active_job_attempt > 0
    )
  )
);

CREATE TABLE IF NOT EXISTS crawler.youtube_api_daily_usage (
  usage_date DATE PRIMARY KEY,
  request_count INTEGER NOT NULL DEFAULT 0 CHECK (request_count >= 0),
  requested_video_count INTEGER NOT NULL DEFAULT 0 CHECK (requested_video_count >= 0),
  requested_channel_count INTEGER NOT NULL DEFAULT 0 CHECK (requested_channel_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- channel-data-api-batch-schema:start
ALTER TABLE crawler.youtube_api_daily_usage
ADD COLUMN IF NOT EXISTS requested_channel_count INTEGER NOT NULL DEFAULT 0
  CHECK (requested_channel_count >= 0);

CREATE TABLE IF NOT EXISTS crawler.youtube_channel_api_tasks (
  task_id BIGSERIAL PRIMARY KEY,
  request_key TEXT NOT NULL UNIQUE,
  channel_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'running', 'done', 'failed')),
  batch_id TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_crawler_youtube_channel_api_tasks_claim
ON crawler.youtube_channel_api_tasks (status, created_at, task_id)
WHERE status IN ('pending', 'running');

CREATE TABLE IF NOT EXISTS crawler.youtube_channel_api_batches (
  batch_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'done', 'failed')),
  task_ids BIGINT[] NOT NULL,
  channel_ids TEXT[] NOT NULL,
  key_index INTEGER,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);
-- channel-data-api-batch-schema:end

INSERT INTO crawler.youtube_api_daily_usage (
  usage_date, request_count, requested_video_count, created_at, updated_at
)
SELECT
  CURRENT_DATE,
  count(*)::int,
  COALESCE(sum(cardinality(video_ids)), 0)::int,
  now(),
  now()
FROM crawler.youtube_api_batches
WHERE created_at >= CURRENT_DATE
  AND status IN ('running', 'done', 'failed')
HAVING count(*) > 0
ON CONFLICT (usage_date) DO NOTHING;

CREATE TABLE IF NOT EXISTS crawler.content_enrich_tasks (
  task_id TEXT PRIMARY KEY,
  content_key TEXT NOT NULL REFERENCES crawler.contents(content_key) ON DELETE CASCADE,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  job_type TEXT NOT NULL
    CHECK (job_type IN ('date-resolve', 'duration-resolve', 'view-resolve', 'stats-resolve')),
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'done', 'failed', 'skipped')),
  priority INTEGER NOT NULL DEFAULT 100,
  attempts INTEGER NOT NULL DEFAULT 0,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (content_key, job_type)
);

CREATE INDEX IF NOT EXISTS idx_crawler_content_enrich_tasks_claim
ON crawler.content_enrich_tasks (status, priority ASC, created_at ASC);

CREATE TABLE IF NOT EXISTS crawler.agent_prompt_templates (
  template_id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  template_text TEXT NOT NULL,
  output_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'active', 'archived')),
  is_default BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (name, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_agent_prompt_templates_default
ON crawler.agent_prompt_templates (is_default)
WHERE is_default = true;

CREATE INDEX IF NOT EXISTS idx_crawler_agent_prompt_templates_status
ON crawler.agent_prompt_templates (status, is_default, updated_at DESC);

CREATE TABLE IF NOT EXISTS crawler.agent_configs (
  config_id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  provider TEXT NOT NULL DEFAULT 'rules',
  model TEXT NOT NULL DEFAULT 'rules-agent-v1',
  endpoint TEXT,
  secret_ref TEXT,
  prompt_template_id BIGINT REFERENCES crawler.agent_prompt_templates(template_id) ON DELETE SET NULL,
  batch_size INTEGER NOT NULL DEFAULT 50,
  min_batch_size INTEGER NOT NULL DEFAULT 20,
  max_workers INTEGER NOT NULL DEFAULT 1,
  timeout_ms INTEGER NOT NULL DEFAULT 120000,
  max_retries INTEGER NOT NULL DEFAULT 2,
  tools_json JSONB NOT NULL DEFAULT '[{"type":"web_search"}]'::jsonb,
  enabled BOOLEAN NOT NULL DEFAULT true,
  is_default BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.agent_configs
ADD COLUMN IF NOT EXISTS max_workers INTEGER NOT NULL DEFAULT 1;

UPDATE crawler.agent_configs
SET is_default = false
WHERE is_default = true;

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_agent_configs_default
ON crawler.agent_configs (is_default)
WHERE is_default = true;

CREATE TABLE IF NOT EXISTS crawler.settings (
  setting_key TEXT PRIMARY KEY,
  value_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO crawler.settings (setting_key,value_json,updated_at)
VALUES (
  'query_scheduler',
  '{"status":"stopped","stop_reason":"fresh_migration_bootstrap","updated_by":"bootstrap"}'::jsonb,
  now()
)
ON CONFLICT (setting_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS crawler.agent_profiles (
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  agent_mode TEXT NOT NULL DEFAULT 'basic',
  input_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'success'
    CHECK (status IN ('success', 'failed')),
  metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  agent_model TEXT,
  agent_config_id BIGINT REFERENCES crawler.agent_configs(config_id) ON DELETE SET NULL,
  prompt_template_id BIGINT REFERENCES crawler.agent_prompt_templates(template_id) ON DELETE SET NULL,
  prompt_hash TEXT,
  prompt_variant TEXT NOT NULL DEFAULT 'with_country',
  input_content_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
  input_content_hash TEXT,
  taxonomy_version TEXT,
  agent_version_hash TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel_id, agent_mode)
);

ALTER TABLE crawler.agent_profiles
ADD COLUMN IF NOT EXISTS agent_config_id BIGINT REFERENCES crawler.agent_configs(config_id) ON DELETE SET NULL;

ALTER TABLE crawler.agent_profiles
ADD COLUMN IF NOT EXISTS prompt_template_id BIGINT REFERENCES crawler.agent_prompt_templates(template_id) ON DELETE SET NULL;

ALTER TABLE crawler.agent_profiles
ADD COLUMN IF NOT EXISTS prompt_hash TEXT;

ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS prompt_variant TEXT NOT NULL DEFAULT 'with_country';
ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS crawler.finalized_profiles (
  channel_id TEXT PRIMARY KEY REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  run_id TEXT REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'ready_auto', 'ready_partial', 'pending_detail', 'pending_api', 'pending_agent', 'failed')),
  profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  quality_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  finalized_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.finalized_profiles DROP CONSTRAINT IF EXISTS finalized_profiles_status_check;
ALTER TABLE crawler.finalized_profiles ADD CONSTRAINT finalized_profiles_status_check
CHECK (status IN ('pending', 'ready_auto', 'ready_partial', 'pending_detail', 'pending_api', 'pending_enrich', 'pending_agent', 'failed'));

UPDATE crawler.channel_runs AS run
SET publication_finalized_status=finalized.status,
    publication_finalized_at=COALESCE(finalized.finalized_at,finalized.updated_at)
FROM crawler.finalized_profiles AS finalized
WHERE finalized.run_id=run.run_id
  AND finalized.channel_id=run.channel_id
  AND finalized.status IN ('ready_auto','ready_partial')
  AND run.publication_finalized_status IS NULL
  AND run.publication_finalized_at IS NULL;

CREATE TABLE IF NOT EXISTS crawler.task_events (
  event_id BIGSERIAL PRIMARY KEY,
  queue_name TEXT NOT NULL,
  job_id TEXT,
  job_name TEXT,
  entity_key TEXT,
  status TEXT NOT NULL,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawler_task_events_entity
ON crawler.task_events (entity_key, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_task_events_created_at
ON crawler.task_events (created_at);

CREATE INDEX IF NOT EXISTS idx_crawler_task_events_queue_created
ON crawler.task_events (queue_name, created_at DESC);

CREATE TABLE IF NOT EXISTS crawler.controller_ticks (
  tick_id BIGSERIAL PRIMARY KEY,
  status TEXT NOT NULL,
  queues_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  actions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawler_controller_ticks_created_at
ON crawler.controller_ticks (created_at);

-- v16-rule-clock-schema:start
-- Dormant Channel lifecycle is deployed through this controlled migration block;
-- runtime services start with SKIP_SCHEMA_MIGRATION=true.
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_reason TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_since TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_recheck_day DATE;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_last_probe_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS dormant_cycle INTEGER NOT NULL DEFAULT 0;
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_status_check
CHECK (status IN ('active', 'dormant', 'paused', 'archived', 'rejected', 'removed'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_dormant_cycle_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_dormant_cycle_check
CHECK (dormant_cycle >= 0);
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_dormant_state_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_dormant_state_check
CHECK (
  (
    status='dormant'
    AND dormant_reason='no_published_content_within_90_days'
    AND dormant_since IS NOT NULL
    AND dormant_recheck_day IS NOT NULL
    AND dormant_last_probe_at IS NOT NULL
    AND dormant_cycle > 0
    AND reject_reason IS NULL
  )
  OR (
    status<>'dormant'
    AND dormant_reason IS NULL
    AND dormant_since IS NULL
    AND dormant_recheck_day IS NULL
    AND dormant_last_probe_at IS NULL
    AND dormant_cycle=0
  )
);
CREATE INDEX IF NOT EXISTS idx_crawler_channels_dormant_recheck
ON crawler.channels (dormant_recheck_day, channel_id)
WHERE status='dormant';

DROP INDEX IF EXISTS crawler.idx_crawler_content_candidates_channel_source;

-- Crawler PostgreSQL remains the source of truth. Only the three getAbout()
-- metrics below have a business history table; every other domain table is a
-- current projection, cursor, backlog, or transport ledger.

ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS avatar_url TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS summary TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS keywords TEXT[] NOT NULL DEFAULT '{}'::text[];
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS keywords_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS available_tabs TEXT[] NOT NULL DEFAULT '{}'::text[];
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS available_tabs_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS about_description TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS description_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS joined_date_text TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS joined_at DATE;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS joined_at_precision TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS external_links JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS external_links_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS rss_url TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS vanity_channel_url TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS is_family_safe BOOLEAN;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS is_verified BOOLEAN;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS is_verified_status TEXT NOT NULL DEFAULT 'unknown';
-- youtube-business-email-crawler-schema:start
ALTER TABLE crawler.channels
ADD COLUMN IF NOT EXISTS youtube_business_email_available BOOLEAN,
ADD COLUMN IF NOT EXISTS youtube_business_email_observed_at TIMESTAMPTZ;

ALTER TABLE crawler.channels
DROP CONSTRAINT IF EXISTS channels_youtube_business_email_shape;

ALTER TABLE crawler.channels
ADD CONSTRAINT channels_youtube_business_email_shape CHECK (
  (youtube_business_email_available IS NULL)
  = (youtube_business_email_observed_at IS NULL)
);

COMMENT ON COLUMN crawler.channels.youtube_business_email_available IS
'True/false only after a successful recognized YouTube About observation; null means unknown.';
COMMENT ON COLUMN crawler.channels.youtube_business_email_observed_at IS
'UTC time of the successful About observation that established the current availability value.';
-- youtube-business-email-crawler-schema:end
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS subscriber_count_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS subscriber_count_source TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS subscriber_count_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_view_count BIGINT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_view_count_text TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_view_count_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_view_count_source TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_view_count_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_video_count BIGINT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_video_count_text TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_video_count_status TEXT NOT NULL DEFAULT 'unresolved';
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_video_count_source TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS total_video_count_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS about_identity_last_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS about_last_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS about_identity_current_hash TEXT;
ALTER TABLE crawler.channels ADD COLUMN IF NOT EXISTS about_current_hash TEXT;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='crawler' AND table_name='channels'
      AND column_name='profile_last_observed_at'
  ) THEN
    EXECUTE $migration$
      UPDATE crawler.channels
      SET about_identity_last_observed_at=COALESCE(
        about_identity_last_observed_at,profile_last_observed_at
      )
    $migration$;
  END IF;
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='crawler' AND table_name='channels'
      AND column_name='profile_current_hash'
  ) THEN
    EXECUTE $migration$
      UPDATE crawler.channels
      SET about_identity_current_hash=COALESCE(
        about_identity_current_hash,profile_current_hash
      )
    $migration$;
  END IF;
END
$$;

ALTER TABLE crawler.channels
DROP COLUMN IF EXISTS profile_last_observed_at,
DROP COLUMN IF EXISTS profile_current_hash;

ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_joined_at_precision_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_joined_at_precision_check
CHECK (joined_at_precision IN ('date_only', 'unknown'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_keywords_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_keywords_status_check
CHECK (keywords_status IN ('observed', 'unresolved'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_available_tabs_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_available_tabs_status_check
CHECK (available_tabs_status IN ('observed', 'unresolved'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_description_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_description_status_check
CHECK (description_status IN ('exact', 'empty', 'unresolved'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_external_links_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_external_links_status_check
CHECK (external_links_status IN ('observed', 'unresolved'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_verified_current_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_verified_current_check
CHECK (
  (is_verified_status='verified' AND is_verified IS TRUE)
  OR (is_verified_status='not_verified' AND is_verified IS FALSE)
  OR (is_verified_status='unknown' AND is_verified IS NULL)
);
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_subscriber_count_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_subscriber_count_status_check
CHECK (subscriber_count_status IN ('exact', 'estimated', 'unavailable', 'unresolved'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_total_view_count_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_total_view_count_status_check
CHECK (total_view_count_status IN ('exact', 'estimated', 'unavailable', 'unresolved'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_total_video_count_status_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_total_video_count_status_check
CHECK (total_video_count_status IN ('exact', 'estimated', 'unavailable', 'unresolved'));
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_v16_metric_nonnegative_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_v16_metric_nonnegative_check
CHECK (
  (subscriber_count IS NULL OR subscriber_count >= 0)
  AND (total_view_count IS NULL OR total_view_count >= 0)
  AND (total_video_count IS NULL OR total_video_count >= 0)
);

ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS view_count BIGINT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS playlist_last_seen_at TIMESTAMPTZ;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS player_last_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS next_last_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS player_current_hash TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS next_current_hash TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS publication_item_hash TEXT;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS video_change_probability DOUBLE PRECISION;
ALTER TABLE crawler.contents ADD COLUMN IF NOT EXISTS last_observation_id UUID;
ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_view_count_nonnegative_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_view_count_nonnegative_check
CHECK (view_count IS NULL OR view_count >= 0);
ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_video_change_probability_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_video_change_probability_check
CHECK (video_change_probability IS NULL OR video_change_probability BETWEEN 0 AND 1);
ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_publication_item_hash_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_publication_item_hash_check
CHECK (publication_item_hash IS NULL OR publication_item_hash ~ '^sha256:[0-9a-f]{64}$');

CREATE INDEX IF NOT EXISTS idx_crawler_contents_channel_published
ON crawler.contents (channel_id, published_at DESC)
WHERE published_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_crawler_contents_player_refresh
ON crawler.contents (channel_id, player_last_observed_at, published_at DESC);

ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS last_observation_id UUID;
ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS current_output_hash TEXT;
ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS input_content_ids TEXT[] NOT NULL DEFAULT '{}'::text[];
ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS input_content_hash TEXT;
ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS taxonomy_version TEXT;
ALTER TABLE crawler.agent_profiles ADD COLUMN IF NOT EXISTS agent_version_hash TEXT;
ALTER TABLE crawler.agent_profiles DROP CONSTRAINT IF EXISTS agent_profiles_prompt_hash_check;
ALTER TABLE crawler.agent_profiles ADD CONSTRAINT agent_profiles_prompt_hash_check
CHECK (prompt_hash IS NULL OR prompt_hash ~ '^[0-9a-f]{64}$');
ALTER TABLE crawler.agent_profiles DROP CONSTRAINT IF EXISTS agent_profiles_current_output_hash_check;
ALTER TABLE crawler.agent_profiles ADD CONSTRAINT agent_profiles_current_output_hash_check
CHECK (current_output_hash IS NULL OR current_output_hash ~ '^sha256:[0-9a-f]{64}$');
ALTER TABLE crawler.agent_profiles DROP CONSTRAINT IF EXISTS agent_profiles_input_content_hash_check;
ALTER TABLE crawler.agent_profiles ADD CONSTRAINT agent_profiles_input_content_hash_check
CHECK (input_content_hash IS NULL OR input_content_hash ~ '^sha256:[0-9a-f]{64}$');
ALTER TABLE crawler.agent_profiles DROP CONSTRAINT IF EXISTS agent_profiles_agent_version_hash_check;
ALTER TABLE crawler.agent_profiles ADD CONSTRAINT agent_profiles_agent_version_hash_check
CHECK (agent_version_hash IS NULL OR agent_version_hash ~ '^sha256:[0-9a-f]{64}$');
ALTER TABLE crawler.finalized_profiles ADD COLUMN IF NOT EXISTS last_observation_id UUID;
ALTER TABLE crawler.finalized_profiles ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ;
ALTER TABLE crawler.finalized_profiles ADD COLUMN IF NOT EXISTS current_output_hash TEXT;

ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS plan_id UUID;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS plan_day DATE;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS trigger_reason TEXT;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS task_mask JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS clock_version BIGINT;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS planner_config_version TEXT;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS capacity_version TEXT;
ALTER TABLE crawler.channel_runs ADD COLUMN IF NOT EXISTS crawler_version TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_channel_runs_plan_id
ON crawler.channel_runs (plan_id)
WHERE plan_id IS NOT NULL;
ALTER TABLE crawler.channel_runs DROP CONSTRAINT IF EXISTS channel_runs_trigger_reason_check;
ALTER TABLE crawler.channel_runs ADD CONSTRAINT channel_runs_trigger_reason_check
CHECK (trigger_reason IS NULL OR trigger_reason IN (
  'initial_full', 'clock_due', 'retry', 'manual', 'repair', 'migration_baseline'
));

CREATE TABLE IF NOT EXISTS crawler.agent_refresh_requests (
  plan_id UUID PRIMARY KEY,
  plan_day DATE NOT NULL,
  scheduled_at TIMESTAMPTZ,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  run_id TEXT REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'queued', 'running', 'done', 'failed', 'cancelled')),
  batch_id TEXT,
  clock_version BIGINT NOT NULL CHECK (clock_version > 0),
  policy_version TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  queued_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE crawler.agent_refresh_requests
ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ;

ALTER TABLE crawler.agent_refresh_requests
DROP CONSTRAINT IF EXISTS agent_refresh_requests_status_check;
ALTER TABLE crawler.agent_refresh_requests
ADD CONSTRAINT agent_refresh_requests_status_check
CHECK (status IN ('pending', 'queued', 'running', 'done', 'failed', 'cancelled'));

CREATE INDEX IF NOT EXISTS idx_crawler_agent_refresh_requests_claim
ON crawler.agent_refresh_requests (status, next_retry_at, created_at);

CREATE INDEX IF NOT EXISTS idx_crawler_agent_refresh_requests_channel
ON crawler.agent_refresh_requests (channel_id, status, created_at);

ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS requested_by_run_id TEXT;
ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS requested_observation_id UUID;
ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ;
ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ;
ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS last_success_at TIMESTAMPTZ;
ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS lease_owner TEXT;
ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE crawler.content_enrich_tasks ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE crawler.content_enrich_tasks DROP CONSTRAINT IF EXISTS content_enrich_tasks_job_type_check;
ALTER TABLE crawler.content_enrich_tasks ADD CONSTRAINT content_enrich_tasks_job_type_check
CHECK (job_type IN (
  'date-resolve', 'duration-resolve', 'view-resolve', 'stats-resolve',
  'player-refresh', 'next-refresh'
));
ALTER TABLE crawler.content_enrich_tasks DROP CONSTRAINT IF EXISTS content_enrich_tasks_status_check;
ALTER TABLE crawler.content_enrich_tasks ADD CONSTRAINT content_enrich_tasks_status_check
CHECK (status IN (
  'queued', 'leased', 'running', 'done', 'failed', 'terminal', 'dead_letter', 'skipped'
));
ALTER TABLE crawler.content_enrich_tasks DROP CONSTRAINT IF EXISTS content_enrich_tasks_dispatch_generation_check;
ALTER TABLE crawler.content_enrich_tasks ADD CONSTRAINT content_enrich_tasks_dispatch_generation_check
CHECK (dispatch_generation >= 0);

INSERT INTO crawler.settings (setting_key,value_json)
VALUES ('content_enrich_dispatch','{"mode":"clock"}'::jsonb)
ON CONFLICT (setting_key) DO NOTHING;

INSERT INTO crawler.settings (setting_key,value_json)
VALUES ('content_enrich_dispatch_cursor','{"channel_id":""}'::jsonb)
ON CONFLICT (setting_key) DO NOTHING;

INSERT INTO crawler.settings (setting_key,value_json)
VALUES ('content_enrich_dispatch_mutex','{"owner":null,"expires_at":null}'::jsonb)
ON CONFLICT (setting_key) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_crawler_content_enrich_tasks_retry
ON crawler.content_enrich_tasks (status, next_retry_at, priority ASC, created_at ASC);

CREATE INDEX IF NOT EXISTS idx_crawler_content_enrich_tasks_dispatch
ON crawler.content_enrich_tasks (job_type,status,next_retry_at,priority,created_at,channel_id);

CREATE INDEX IF NOT EXISTS idx_crawler_content_enrich_tasks_lease_owner
ON crawler.content_enrich_tasks (lease_owner)
WHERE lease_owner IS NOT NULL;

-- video-identity-schema:start
-- A YouTube video_id identifies one piece of content. Uploads is only a generic
-- listing surface; stronger Shorts/Live evidence updates the type in place.
ALTER TABLE crawler.contents DROP CONSTRAINT IF EXISTS contents_access_status_check;
ALTER TABLE crawler.contents ADD CONSTRAINT contents_access_status_check
CHECK (access_status IN (
  'public', 'unlisted', 'members_only', 'private',
  'unavailable', 'login_required', 'unknown'
));

DROP TABLE IF EXISTS pg_temp.content_identity_merge_map;
CREATE TEMP TABLE content_identity_merge_map AS
WITH ranked AS (
  SELECT
    content_key,
    first_value(content_key) OVER (
      PARTITION BY channel_id,source_content_id
      ORDER BY
        CASE content_type
          WHEN 'live' THEN 1
          WHEN 'short' THEN 2
          WHEN 'video' THEN 3
          ELSE 4
        END,
        COALESCE(last_enriched_at,last_seen_at) DESC,
        content_key
    ) AS survivor_content_key
  FROM crawler.contents
)
SELECT
  content_key AS duplicate_content_key,
  survivor_content_key
FROM ranked
WHERE content_key<>survivor_content_key;

CREATE UNIQUE INDEX content_identity_merge_map_duplicate
ON content_identity_merge_map (duplicate_content_key);

DROP TABLE IF EXISTS pg_temp.content_identity_merged;
CREATE TEMP TABLE content_identity_merged AS
WITH identities AS (
  SELECT DISTINCT survivor_content_key
  FROM content_identity_merge_map
), members AS (
  SELECT identity.survivor_content_key,content.*
  FROM identities identity
  JOIN crawler.contents survivor
    ON survivor.content_key=identity.survivor_content_key
  JOIN crawler.contents content
    ON content.channel_id=survivor.channel_id
   AND content.source_content_id=survivor.source_content_id
)
SELECT
  survivor_content_key,
  (array_agg(content_type ORDER BY
    CASE content_type WHEN 'live' THEN 1 WHEN 'short' THEN 2 WHEN 'video' THEN 3 ELSE 4 END,
    COALESCE(last_enriched_at,last_seen_at) DESC,content_key
  ))[1] AS content_type,
  (array_agg(content_type_source ORDER BY
    CASE content_type WHEN 'live' THEN 1 WHEN 'short' THEN 2 WHEN 'video' THEN 3 ELSE 4 END,
    COALESCE(last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE content_type_source IS NOT NULL))[1] AS content_type_source,
  (array_agg(run_id ORDER BY last_seen_at DESC,content_key)
    FILTER (WHERE run_id IS NOT NULL))[1] AS run_id,
  (array_agg(position ORDER BY playlist_last_seen_at DESC NULLS LAST,last_seen_at DESC,content_key)
    FILTER (WHERE position IS NOT NULL))[1] AS position,
  (array_agg(title ORDER BY COALESCE(last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE NULLIF(btrim(title),'') IS NOT NULL))[1] AS title,
  (array_agg(thumbnail_url ORDER BY COALESCE(last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE NULLIF(btrim(thumbnail_url),'') IS NOT NULL))[1] AS thumbnail_url,
  (array_agg(published_text_raw ORDER BY
    CASE published_at_precision WHEN 'second' THEN 1 WHEN 'date_only' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE published_at IS NOT NULL))[1] AS published_text_raw,
  (array_agg(published_at ORDER BY
    CASE published_at_precision WHEN 'second' THEN 1 WHEN 'date_only' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE published_at IS NOT NULL))[1] AS published_at,
  (array_agg(published_at_status ORDER BY
    CASE published_at_precision WHEN 'second' THEN 1 WHEN 'date_only' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE published_at IS NOT NULL))[1] AS published_at_status,
  (array_agg(published_at_source ORDER BY
    CASE published_at_precision WHEN 'second' THEN 1 WHEN 'date_only' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE published_at IS NOT NULL))[1] AS published_at_source,
  (array_agg(published_at_precision ORDER BY
    CASE published_at_precision WHEN 'second' THEN 1 WHEN 'date_only' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE published_at IS NOT NULL))[1] AS published_at_precision,
  bool_or(is_recent) AS is_recent,
  (array_agg(length_text ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE duration_seconds>0))[1] AS length_text,
  (array_agg(duration_seconds ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE duration_seconds>0))[1] AS duration_seconds,
  (array_agg(duration_status ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE duration_seconds>0))[1] AS duration_status,
  (array_agg(duration_source ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE duration_seconds>0))[1] AS duration_source,
  (array_agg(view_count ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE view_count IS NOT NULL))[1] AS view_count,
  (array_agg(view_count_text ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE view_count IS NOT NULL))[1] AS view_count_text,
  (array_agg(view_count_status ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE view_count IS NOT NULL))[1] AS view_count_status,
  (array_agg(view_count_source ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE view_count IS NOT NULL))[1] AS view_count_source,
  (array_agg(like_count ORDER BY COALESCE(next_last_observed_at,player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE like_count IS NOT NULL))[1] AS like_count,
  (array_agg(like_count_status ORDER BY COALESCE(next_last_observed_at,player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE like_count IS NOT NULL))[1] AS like_count_status,
  (array_agg(like_count_source ORDER BY COALESCE(next_last_observed_at,player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE like_count IS NOT NULL))[1] AS like_count_source,
  (array_agg(comment_count ORDER BY COALESCE(next_last_observed_at,player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE comment_count IS NOT NULL OR comments_disabled=true))[1] AS comment_count,
  (array_agg(comment_count_status ORDER BY COALESCE(next_last_observed_at,player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE comment_count IS NOT NULL OR comments_disabled=true))[1] AS comment_count_status,
  (array_agg(comments_disabled ORDER BY COALESCE(next_last_observed_at,player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE comment_count IS NOT NULL OR comments_disabled=true))[1] AS comments_disabled,
  (array_agg(comment_count_source ORDER BY COALESCE(next_last_observed_at,player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE comment_count IS NOT NULL OR comments_disabled=true))[1] AS comment_count_source,
  (array_agg(is_members_only ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE access_status<>'unknown'))[1] AS is_members_only,
  (array_agg(access_status ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE access_status<>'unknown'))[1] AS access_status,
  (array_agg(access_status_source ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE access_status<>'unknown'))[1] AS access_status_source,
  max(live_scheduled_at) AS live_scheduled_at,
  max(live_started_at) AS live_started_at,
  max(live_ended_at) AS live_ended_at,
  (array_agg(extractor_version ORDER BY COALESCE(last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE extractor_version IS NOT NULL))[1] AS extractor_version,
  (array_agg(description ORDER BY
    CASE description_status WHEN 'exact' THEN 1 WHEN 'empty' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE description_status IN ('exact','empty')))[1] AS description,
  (array_agg(description_status ORDER BY
    CASE description_status WHEN 'exact' THEN 1 WHEN 'empty' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE description_status IN ('exact','empty')))[1] AS description_status,
  (array_agg(description_source ORDER BY
    CASE description_status WHEN 'exact' THEN 1 WHEN 'empty' THEN 2 ELSE 3 END,
    COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key
  ) FILTER (WHERE description_status IN ('exact','empty')))[1] AS description_source,
  ((array_agg(hashtags::text ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE cardinality(hashtags)>0))[1])::text[] AS hashtags,
  ((array_agg(keywords::text ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE cardinality(keywords)>0))[1])::text[] AS keywords,
  min(first_seen_at) AS first_seen_at,
  max(last_seen_at) AS last_seen_at,
  max(last_enriched_at) AS last_enriched_at,
  max(playlist_last_seen_at) AS playlist_last_seen_at,
  max(player_last_observed_at) AS player_last_observed_at,
  max(next_last_observed_at) AS next_last_observed_at,
  (array_agg(video_change_probability ORDER BY COALESCE(player_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE video_change_probability IS NOT NULL))[1] AS video_change_probability,
  (array_agg(last_observation_id ORDER BY COALESCE(player_last_observed_at,next_last_observed_at,last_enriched_at,last_seen_at) DESC,content_key)
    FILTER (WHERE last_observation_id IS NOT NULL))[1] AS last_observation_id,
  (
    (array_agg(raw_json ORDER BY COALESCE(last_enriched_at,last_seen_at) DESC,content_key))[1]
    || jsonb_build_object(
      'identity_merge',jsonb_build_object(
        'merged_at',transaction_timestamp(),
        'source_content_keys',array_agg(content_key ORDER BY content_key),
        'legacy_raw_by_content_key',jsonb_object_agg(content_key,raw_json)
      )
    )
  ) AS raw_json
FROM members
GROUP BY survivor_content_key;

UPDATE crawler.contents survivor
SET
  run_id=COALESCE(merged.run_id,survivor.run_id),
  content_type=merged.content_type,
  content_type_source=COALESCE(merged.content_type_source,survivor.content_type_source),
  position=COALESCE(merged.position,survivor.position),
  title=COALESCE(merged.title,survivor.title),
  url=CASE merged.content_type
    WHEN 'short' THEN 'https://www.youtube.com/shorts/' || survivor.source_content_id
    ELSE 'https://www.youtube.com/watch?v=' || survivor.source_content_id
  END,
  thumbnail_url=COALESCE(merged.thumbnail_url,survivor.thumbnail_url),
  published_text_raw=COALESCE(merged.published_text_raw,survivor.published_text_raw),
  published_at=COALESCE(merged.published_at,survivor.published_at),
  published_at_status=COALESCE(merged.published_at_status,survivor.published_at_status),
  published_at_source=COALESCE(merged.published_at_source,survivor.published_at_source),
  published_at_precision=COALESCE(merged.published_at_precision,survivor.published_at_precision),
  is_recent=merged.is_recent,
  length_text=COALESCE(merged.length_text,survivor.length_text),
  duration_seconds=COALESCE(merged.duration_seconds,survivor.duration_seconds),
  duration_status=COALESCE(merged.duration_status,survivor.duration_status),
  duration_source=COALESCE(merged.duration_source,survivor.duration_source),
  view_count=COALESCE(merged.view_count,survivor.view_count),
  view_count_text=COALESCE(merged.view_count_text,survivor.view_count_text),
  view_count_status=COALESCE(merged.view_count_status,survivor.view_count_status),
  view_count_source=COALESCE(merged.view_count_source,survivor.view_count_source),
  like_count=COALESCE(merged.like_count,survivor.like_count),
  like_count_status=COALESCE(merged.like_count_status,survivor.like_count_status),
  like_count_source=COALESCE(merged.like_count_source,survivor.like_count_source),
  comment_count=CASE
    WHEN merged.comments_disabled=true THEN 0
    ELSE COALESCE(merged.comment_count,survivor.comment_count)
  END,
  comment_count_status=COALESCE(merged.comment_count_status,survivor.comment_count_status),
  comments_disabled=COALESCE(merged.comments_disabled,survivor.comments_disabled),
  comment_count_source=COALESCE(merged.comment_count_source,survivor.comment_count_source),
  is_members_only=COALESCE(merged.is_members_only,survivor.is_members_only),
  access_status=COALESCE(merged.access_status,survivor.access_status),
  access_status_source=COALESCE(merged.access_status_source,survivor.access_status_source),
  live_scheduled_at=COALESCE(merged.live_scheduled_at,survivor.live_scheduled_at),
  live_started_at=COALESCE(merged.live_started_at,survivor.live_started_at),
  live_ended_at=COALESCE(merged.live_ended_at,survivor.live_ended_at),
  extractor_version=COALESCE(merged.extractor_version,survivor.extractor_version),
  description=COALESCE(merged.description,survivor.description),
  description_status=COALESCE(merged.description_status,survivor.description_status),
  description_source=COALESCE(merged.description_source,survivor.description_source),
  hashtags=COALESCE(merged.hashtags,survivor.hashtags),
  keywords=COALESCE(merged.keywords,survivor.keywords),
  raw_json=merged.raw_json,
  first_seen_at=merged.first_seen_at,
  last_seen_at=merged.last_seen_at,
  last_enriched_at=merged.last_enriched_at,
  playlist_last_seen_at=merged.playlist_last_seen_at,
  player_last_observed_at=merged.player_last_observed_at,
  next_last_observed_at=merged.next_last_observed_at,
  player_current_hash=NULL,
  next_current_hash=NULL,
  video_change_probability=COALESCE(merged.video_change_probability,survivor.video_change_probability),
  last_observation_id=COALESCE(merged.last_observation_id,survivor.last_observation_id),
  publication_item_hash=NULL
FROM content_identity_merged merged
WHERE survivor.content_key=merged.survivor_content_key;

UPDATE crawler.content_candidates candidate
SET content_key=identity.survivor_content_key,updated_at=now()
FROM content_identity_merge_map identity
WHERE candidate.content_key=identity.duplicate_content_key;

UPDATE crawler.content_enrich_tasks survivor_task
SET
  status=CASE
    WHEN survivor_task.status='done' OR duplicate_task.status='done' THEN 'done'
    WHEN survivor_task.status IN ('queued','leased','running')
      OR duplicate_task.status IN ('queued','leased','running') THEN 'queued'
    WHEN survivor_task.status='failed' OR duplicate_task.status='failed' THEN 'failed'
    WHEN survivor_task.status='terminal' OR duplicate_task.status='terminal' THEN 'terminal'
    WHEN survivor_task.status='dead_letter' OR duplicate_task.status='dead_letter' THEN 'dead_letter'
    ELSE 'skipped'
  END,
  priority=LEAST(survivor_task.priority,duplicate_task.priority),
  attempts=GREATEST(survivor_task.attempts,duplicate_task.attempts),
  result_json=duplicate_task.result_json || survivor_task.result_json,
  error_message=CASE
    WHEN survivor_task.status='done' OR duplicate_task.status='done' THEN NULL
    ELSE COALESCE(survivor_task.error_message,duplicate_task.error_message)
  END,
  created_at=LEAST(survivor_task.created_at,duplicate_task.created_at),
  updated_at=GREATEST(survivor_task.updated_at,duplicate_task.updated_at),
  requested_by_run_id=COALESCE(survivor_task.requested_by_run_id,duplicate_task.requested_by_run_id),
  requested_observation_id=COALESCE(survivor_task.requested_observation_id,duplicate_task.requested_observation_id),
  next_retry_at=LEAST(survivor_task.next_retry_at,duplicate_task.next_retry_at),
  last_attempt_at=GREATEST(survivor_task.last_attempt_at,duplicate_task.last_attempt_at),
  last_success_at=GREATEST(survivor_task.last_success_at,duplicate_task.last_success_at),
  dispatch_generation=GREATEST(survivor_task.dispatch_generation,duplicate_task.dispatch_generation)
    + CASE
        WHEN survivor_task.status IN ('leased','running')
          OR duplicate_task.status IN ('leased','running') THEN 1
        ELSE 0
      END,
  lease_owner=NULL,
  lease_expires_at=NULL
FROM crawler.content_enrich_tasks duplicate_task
JOIN content_identity_merge_map identity
  ON identity.duplicate_content_key=duplicate_task.content_key
WHERE survivor_task.content_key=identity.survivor_content_key
  AND survivor_task.job_type=duplicate_task.job_type;

DELETE FROM crawler.content_enrich_tasks duplicate_task
USING content_identity_merge_map identity
WHERE duplicate_task.content_key=identity.duplicate_content_key
  AND EXISTS (
    SELECT 1
    FROM crawler.content_enrich_tasks survivor_task
    WHERE survivor_task.content_key=identity.survivor_content_key
      AND survivor_task.job_type=duplicate_task.job_type
  );

UPDATE crawler.content_enrich_tasks task
SET
  content_key=identity.survivor_content_key,
  status=CASE WHEN task.status IN ('leased','running') THEN 'queued' ELSE task.status END,
  dispatch_generation=task.dispatch_generation
    + CASE WHEN task.status IN ('leased','running') THEN 1 ELSE 0 END,
  lease_owner=NULL,
  lease_expires_at=NULL,
  updated_at=now()
FROM content_identity_merge_map identity
WHERE task.content_key=identity.duplicate_content_key;

DELETE FROM crawler.contents duplicate
USING content_identity_merge_map identity
WHERE duplicate.content_key=identity.duplicate_content_key;

ALTER TABLE crawler.contents
DROP CONSTRAINT IF EXISTS contents_channel_id_content_type_source_content_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_contents_channel_source
ON crawler.contents (channel_id, source_content_id);

DROP TABLE content_identity_merged;
DROP TABLE content_identity_merge_map;
-- video-identity-schema:end

CREATE TABLE IF NOT EXISTS crawler.crawl_observation_keys (
  idempotency_key TEXT PRIMARY KEY,
  observation_id UUID NOT NULL UNIQUE,
  command_hash TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  observation_kind TEXT NOT NULL
    CHECK (observation_kind IN ('about', 'video', 'agent')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (expires_at > created_at)
);

ALTER TABLE crawler.crawl_observation_keys
DROP CONSTRAINT IF EXISTS crawl_observation_keys_observation_kind_check;
ALTER TABLE crawler.crawl_observation_keys
ADD CONSTRAINT crawl_observation_keys_observation_kind_check
CHECK (observation_kind IN ('about', 'video', 'agent')) NOT VALID;

CREATE INDEX IF NOT EXISTS idx_crawler_crawl_observation_keys_expiry
ON crawler.crawl_observation_keys (expires_at);

CREATE TABLE IF NOT EXISTS crawler.crawl_observations (
  observation_id UUID PRIMARY KEY,
  observed_at TIMESTAMPTZ NOT NULL,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  run_id TEXT REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL,
  observation_kind TEXT NOT NULL
    CHECK (observation_kind IN ('about', 'video', 'agent')),
  kind_sequence BIGINT NOT NULL CHECK (kind_sequence > 0),
  plan_id UUID,
  plan_day DATE,
  trigger_reason TEXT NOT NULL
    CHECK (trigger_reason IN ('initial_full', 'clock_due', 'retry', 'manual', 'repair', 'migration_baseline')),
  scheduled_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  outcome TEXT NOT NULL CHECK (outcome IN ('complete', 'partial', 'failed')),
  outcome_reason_code TEXT NOT NULL,
  result_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  facts_hash TEXT NOT NULL,
  crawler_version TEXT,
  extractor_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_class TEXT,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (channel_id, observation_kind, kind_sequence)
);

ALTER TABLE crawler.crawl_observations
DROP CONSTRAINT IF EXISTS crawl_observations_observation_kind_check;
ALTER TABLE crawler.crawl_observations
ADD CONSTRAINT crawl_observations_observation_kind_check
CHECK (observation_kind IN ('about', 'video', 'agent')) NOT VALID;

CREATE INDEX IF NOT EXISTS idx_crawler_crawl_observations_channel_kind
ON crawler.crawl_observations (channel_id, observation_kind, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_crawler_crawl_observations_created_at
ON crawler.crawl_observations (created_at);

ALTER TABLE crawler.content_candidates
ADD COLUMN IF NOT EXISTS first_seen_ledger_status TEXT NOT NULL DEFAULT 'not_applicable';
ALTER TABLE crawler.content_candidates
ADD COLUMN IF NOT EXISTS first_seen_ledger_observation_id UUID;

DO $first_seen_ledger_shape$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='crawler.content_candidates'::regclass
      AND conname='content_candidates_first_seen_ledger_shape_check'
  ) THEN
    ALTER TABLE crawler.content_candidates
    ADD CONSTRAINT content_candidates_first_seen_ledger_shape_check
    CHECK (
      (first_seen_ledger_status='not_applicable' AND first_seen_ledger_observation_id IS NULL)
      OR (first_seen_ledger_status='pending' AND first_seen_ledger_observation_id IS NULL)
      OR (first_seen_ledger_status='consumed' AND first_seen_ledger_observation_id IS NOT NULL)
    ) NOT VALID;
  END IF;
END
$first_seen_ledger_shape$;

DO $first_seen_ledger_observation_fkey$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='crawler.content_candidates'::regclass
      AND conname='content_candidates_first_seen_ledger_observation_id_fkey'
  ) THEN
    ALTER TABLE crawler.content_candidates
    ADD CONSTRAINT content_candidates_first_seen_ledger_observation_id_fkey
    FOREIGN KEY (first_seen_ledger_observation_id)
    REFERENCES crawler.crawl_observations(observation_id)
    ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED NOT VALID;
  END IF;
END
$first_seen_ledger_observation_fkey$;

CREATE TABLE IF NOT EXISTS crawler.channel_about_metric_snapshots (
  observation_id UUID PRIMARY KEY REFERENCES crawler.crawl_observations(observation_id) ON DELETE CASCADE,
  observed_at TIMESTAMPTZ NOT NULL,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  subscriber_count BIGINT,
  total_view_count BIGINT,
  total_video_count BIGINT,
  subscriber_count_status TEXT NOT NULL
    CHECK (subscriber_count_status IN ('exact', 'estimated', 'unavailable', 'unresolved')),
  total_view_count_status TEXT NOT NULL
    CHECK (total_view_count_status IN ('exact', 'estimated', 'unavailable', 'unresolved')),
  total_video_count_status TEXT NOT NULL
    CHECK (total_video_count_status IN ('exact', 'estimated', 'unavailable', 'unresolved')),
  facts_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (channel_id, observed_at, observation_id),
  CHECK (subscriber_count IS NULL OR subscriber_count >= 0),
  CHECK (total_view_count IS NULL OR total_view_count >= 0),
  CHECK (total_video_count IS NULL OR total_video_count >= 0),
  CHECK ((subscriber_count IS NOT NULL) = (subscriber_count_status IN ('exact', 'estimated'))),
  CHECK ((total_view_count IS NOT NULL) = (total_view_count_status IN ('exact', 'estimated'))),
  CHECK ((total_video_count IS NOT NULL) = (total_video_count_status IN ('exact', 'estimated'))),
  CHECK (subscriber_count IS NOT NULL OR total_view_count IS NOT NULL OR total_video_count IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_crawler_about_snapshots_channel_observed
ON crawler.channel_about_metric_snapshots (channel_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS crawler.channel_domain_cursors (
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  observation_kind TEXT NOT NULL
    CHECK (observation_kind IN ('about', 'video', 'agent')),
  latest_sequence BIGINT NOT NULL DEFAULT 0 CHECK (latest_sequence >= 0),
  latest_observation_id UUID,
  latest_observed_at TIMESTAMPTZ,
  latest_complete_observation_id UUID,
  latest_complete_observed_at TIMESTAMPTZ,
  consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
  anchor_video_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
  source_cursor JSONB NOT NULL DEFAULT '{}'::jsonb,
  current_facts_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel_id, observation_kind),
  CHECK (cardinality(anchor_video_ids) <= 20)
);

ALTER TABLE crawler.channel_domain_cursors
DROP CONSTRAINT IF EXISTS channel_domain_cursors_observation_kind_check;
ALTER TABLE crawler.channel_domain_cursors
ADD CONSTRAINT channel_domain_cursors_observation_kind_check
CHECK (observation_kind IN ('about', 'video', 'agent')) NOT VALID;

CREATE TABLE IF NOT EXISTS crawler.observation_raw_objects (
  observation_id UUID NOT NULL REFERENCES crawler.crawl_observations(observation_id) ON DELETE CASCADE,
  raw_object_id BIGINT NOT NULL REFERENCES crawler.raw_objects(raw_object_id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  content_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (observation_id, raw_object_id, role)
);

CREATE TABLE IF NOT EXISTS crawler.crawler_outbox (
  event_id UUID PRIMARY KEY,
  observation_id UUID NOT NULL UNIQUE REFERENCES crawler.crawl_observations(observation_id) ON DELETE CASCADE,
  observed_at TIMESTAMPTZ NOT NULL,
  event_type TEXT NOT NULL DEFAULT 'crawler.observation.recorded'
    CHECK (event_type = 'crawler.observation.recorded'),
  event_version INTEGER NOT NULL DEFAULT 1 CHECK (event_version > 0),
  aggregate_key TEXT NOT NULL,
  kind_sequence BIGINT NOT NULL CHECK (kind_sequence > 0),
  payload_json JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'publishing', 'published', 'dead_letter')),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);

ALTER TABLE crawler.crawler_outbox ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE crawler.crawler_outbox ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_crawler_outbox_publish
ON crawler.crawler_outbox (status, next_attempt_at, lease_expires_at, created_at);

CREATE INDEX IF NOT EXISTS idx_crawler_outbox_created_at
ON crawler.crawler_outbox (created_at);

CREATE TABLE IF NOT EXISTS crawler.baseline_exports (
  baseline_version TEXT PRIMARY KEY,
  export_id UUID NOT NULL UNIQUE,
  as_of_at TIMESTAMPTZ NOT NULL,
  expected_channel_count INTEGER NOT NULL CHECK (expected_channel_count > 0),
  status TEXT NOT NULL DEFAULT 'preparing'
    CHECK (status IN ('preparing', 'events_committed', 'ready', 'failed')),
  event_count BIGINT CHECK (event_count IS NULL OR event_count > 0),
  channel_count BIGINT CHECK (channel_count IS NULL OR channel_count > 0),
  byte_count BIGINT CHECK (byte_count IS NULL OR byte_count > 0),
  events_sha256 TEXT,
  manifest_sha256 TEXT,
  output_directory TEXT,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (events_sha256 IS NULL OR events_sha256 ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (manifest_sha256 IS NULL OR manifest_sha256 ~ '^sha256:[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS crawler.baseline_export_events (
  export_id UUID NOT NULL REFERENCES crawler.baseline_exports(export_id) ON DELETE CASCADE,
  event_id UUID NOT NULL REFERENCES crawler.crawler_outbox(event_id) ON DELETE CASCADE,
  channel_id TEXT NOT NULL REFERENCES crawler.channels(channel_id) ON DELETE CASCADE,
  observation_kind TEXT NOT NULL
    CHECK (observation_kind IN ('about', 'video', 'agent')),
  kind_sequence BIGINT NOT NULL CHECK (kind_sequence > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (export_id,event_id),
  UNIQUE (export_id,channel_id,observation_kind,kind_sequence)
);

ALTER TABLE crawler.baseline_export_events
DROP CONSTRAINT IF EXISTS baseline_export_events_observation_kind_check;
ALTER TABLE crawler.baseline_export_events
ADD CONSTRAINT baseline_export_events_observation_kind_check
CHECK (observation_kind IN ('about', 'video', 'agent')) NOT VALID;

ALTER TABLE crawler.baseline_export_events
DROP CONSTRAINT IF EXISTS baseline_export_events_event_id_fkey;
ALTER TABLE crawler.baseline_export_events
ADD CONSTRAINT baseline_export_events_event_id_fkey
FOREIGN KEY (event_id) REFERENCES crawler.crawler_outbox(event_id) ON DELETE CASCADE;
ALTER TABLE crawler.baseline_export_events
DROP CONSTRAINT IF EXISTS baseline_export_events_channel_id_fkey;
ALTER TABLE crawler.baseline_export_events
ADD CONSTRAINT baseline_export_events_channel_id_fkey
FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_crawler_baseline_export_events_sequence
ON crawler.baseline_export_events (export_id,channel_id,observation_kind,kind_sequence);
-- v16-rule-clock-schema:end

-- publication-current-schema:start
CREATE SCHEMA IF NOT EXISTS publication;

CREATE TABLE IF NOT EXISTS publication.stream (
  publication_stream_id UUID PRIMARY KEY,
  source_deployment_key TEXT NOT NULL,
  source_identity_json JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'sealed')),
  minimum_writer_version TEXT,
  capture_enabled_at TIMESTAMPTZ,
  automatic_onboarding_destination TEXT,
  created_by TEXT NOT NULL,
  created_reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status_changed_by TEXT NOT NULL,
  status_reason TEXT NOT NULL,
  sealed_at TIMESTAMPTZ,
  CHECK (btrim(source_deployment_key) <> ''),
  CHECK (jsonb_typeof(source_identity_json) = 'object'),
  CHECK (btrim(created_by) <> '' AND btrim(created_reason) <> ''),
  CHECK (btrim(status_changed_by) <> '' AND btrim(status_reason) <> ''),
  CHECK (minimum_writer_version IS NULL OR btrim(minimum_writer_version) <> ''),
  CHECK (capture_enabled_at IS NULL OR minimum_writer_version IS NOT NULL),
  CONSTRAINT chk_publication_stream_automatic_onboarding_destination CHECK (
    automatic_onboarding_destination IS NULL
    OR btrim(automatic_onboarding_destination) <> ''
  ),
  CHECK (
    (status = 'active' AND sealed_at IS NULL)
    OR (status = 'sealed' AND sealed_at IS NOT NULL)
  )
);

ALTER TABLE publication.stream
ADD COLUMN IF NOT EXISTS automatic_onboarding_destination TEXT;

DO $publication_schema$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='publication.stream'::regclass
      AND conname='chk_publication_stream_automatic_onboarding_destination'
  ) THEN
    ALTER TABLE publication.stream
    ADD CONSTRAINT chk_publication_stream_automatic_onboarding_destination
    CHECK (
      automatic_onboarding_destination IS NULL
      OR btrim(automatic_onboarding_destination) <> ''
    );
  END IF;
END
$publication_schema$;

DROP INDEX IF EXISTS publication.ux_publication_stream_active_deployment;
CREATE INDEX IF NOT EXISTS idx_publication_stream_deployment_status
ON publication.stream (source_deployment_key, status, created_at);

CREATE OR REPLACE FUNCTION publication.guard_stream_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $publication_guard$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Publication Stream rows cannot be deleted or reused'
      USING ERRCODE = '55000';
  END IF;
  IF NEW.publication_stream_id IS DISTINCT FROM OLD.publication_stream_id
     OR NEW.source_deployment_key IS DISTINCT FROM OLD.source_deployment_key
     OR NEW.source_identity_json IS DISTINCT FROM OLD.source_identity_json
     OR NEW.automatic_onboarding_destination
        IS DISTINCT FROM OLD.automatic_onboarding_destination
     OR NEW.created_by IS DISTINCT FROM OLD.created_by
     OR NEW.created_reason IS DISTINCT FROM OLD.created_reason
     OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'Publication Stream identity and creation fields are immutable'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.status = 'sealed' AND (
    NEW.status IS DISTINCT FROM OLD.status
    OR NEW.minimum_writer_version IS DISTINCT FROM OLD.minimum_writer_version
    OR NEW.capture_enabled_at IS DISTINCT FROM OLD.capture_enabled_at
    OR NEW.status_changed_at IS DISTINCT FROM OLD.status_changed_at
    OR NEW.status_changed_by IS DISTINCT FROM OLD.status_changed_by
    OR NEW.status_reason IS DISTINCT FROM OLD.status_reason
    OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
  ) THEN
    RAISE EXCEPTION 'a sealed Publication Stream is immutable'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$publication_guard$;

DROP TRIGGER IF EXISTS trg_publication_stream_lifecycle ON publication.stream;
CREATE TRIGGER trg_publication_stream_lifecycle
BEFORE UPDATE OR DELETE ON publication.stream
FOR EACH ROW EXECUTE FUNCTION publication.guard_stream_lifecycle();

CREATE TABLE IF NOT EXISTS publication.channel_stream_state (
  publication_stream_id UUID NOT NULL,
  channel_id TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'owned'
    CHECK (status IN ('owned', 'sealed')),
  onboarding_mode TEXT NOT NULL
    CHECK (onboarding_mode IN ('baseline', 'bootstrap', 'cutover')),
  seed_status TEXT NOT NULL DEFAULT 'pending'
    CHECK (seed_status IN ('pending', 'complete')),
  ownership_reference JSONB NOT NULL DEFAULT '{}'::jsonb,
  final_version_vector JSONB,
  owned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  seed_completed_at TIMESTAMPTZ,
  sealed_at TIMESTAMPTZ,
  state_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  state_changed_by TEXT NOT NULL,
  state_reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (publication_stream_id, channel_id),
  FOREIGN KEY (publication_stream_id)
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  FOREIGN KEY (channel_id)
    REFERENCES crawler.channels(channel_id) ON DELETE RESTRICT,
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(ownership_reference) = 'object'),
  CHECK (final_version_vector IS NULL OR jsonb_typeof(final_version_vector) = 'object'),
  CHECK (btrim(state_changed_by) <> '' AND btrim(state_reason) <> ''),
  CHECK (
    (seed_status = 'pending' AND seed_completed_at IS NULL)
    OR (seed_status = 'complete' AND seed_completed_at IS NOT NULL)
  ),
  CHECK (
    (status = 'owned' AND sealed_at IS NULL AND final_version_vector IS NULL)
    OR (status = 'sealed' AND sealed_at IS NOT NULL AND final_version_vector IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_publication_channel_stream_state_status
ON publication.channel_stream_state (publication_stream_id, status, onboarding_mode, channel_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_publication_channel_stream_owned
ON publication.channel_stream_state (channel_id)
WHERE status = 'owned';

CREATE OR REPLACE FUNCTION publication.guard_channel_stream_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $publication_guard$
BEGIN
  IF NEW.publication_stream_id IS DISTINCT FROM OLD.publication_stream_id
     OR NEW.channel_id IS DISTINCT FROM OLD.channel_id
     OR NEW.onboarding_mode IS DISTINCT FROM OLD.onboarding_mode
     OR NEW.owned_at IS DISTINCT FROM OLD.owned_at
     OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'Publication Channel ownership identity and creation fields are immutable'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.status = 'sealed' AND (
    NEW.status IS DISTINCT FROM OLD.status
    OR NEW.final_version_vector IS DISTINCT FROM OLD.final_version_vector
    OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
    OR NEW.state_changed_at IS DISTINCT FROM OLD.state_changed_at
    OR NEW.state_changed_by IS DISTINCT FROM OLD.state_changed_by
    OR NEW.state_reason IS DISTINCT FROM OLD.state_reason
  ) THEN
    RAISE EXCEPTION 'sealed Publication Channel ownership cannot be changed'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.seed_status = 'complete' AND (
    NEW.seed_status IS DISTINCT FROM OLD.seed_status
    OR NEW.seed_completed_at IS DISTINCT FROM OLD.seed_completed_at
  ) THEN
    RAISE EXCEPTION 'completed Publication Seed state cannot be changed'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$publication_guard$;

DROP TRIGGER IF EXISTS trg_publication_channel_stream_lifecycle
ON publication.channel_stream_state;
CREATE TRIGGER trg_publication_channel_stream_lifecycle
BEFORE UPDATE ON publication.channel_stream_state
FOR EACH ROW EXECUTE FUNCTION publication.guard_channel_stream_lifecycle();

CREATE TABLE IF NOT EXISTS publication.channel_delivery_state (
  destination TEXT NOT NULL,
  publication_stream_id UUID NOT NULL,
  channel_id TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'hold'
    CHECK (mode IN ('hold', 'online', 'sealed')),
  latest_successful_baseline_id UUID,
  channel_watermark_sequence BIGINT,
  video_watermark_sequence BIGINT,
  agent_watermark_sequence BIGINT,
  source_ownership_reference JSONB NOT NULL DEFAULT '{}'::jsonb,
  cutover_reference JSONB NOT NULL DEFAULT '{}'::jsonb,
  online_at TIMESTAMPTZ,
  sealed_at TIMESTAMPTZ,
  state_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  state_changed_by TEXT NOT NULL,
  state_reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (destination, publication_stream_id, channel_id),
  FOREIGN KEY (publication_stream_id, channel_id)
    REFERENCES publication.channel_stream_state(publication_stream_id, channel_id)
    ON DELETE RESTRICT,
  CHECK (btrim(destination) <> ''),
  CHECK (jsonb_typeof(source_ownership_reference) = 'object'),
  CHECK (jsonb_typeof(cutover_reference) = 'object'),
  CHECK (btrim(state_changed_by) <> '' AND btrim(state_reason) <> ''),
  CHECK (channel_watermark_sequence IS NULL OR channel_watermark_sequence >= 0),
  CHECK (video_watermark_sequence IS NULL OR video_watermark_sequence >= 0),
  CHECK (agent_watermark_sequence IS NULL OR agent_watermark_sequence >= 0),
  CHECK (
    (
      latest_successful_baseline_id IS NULL
      AND channel_watermark_sequence IS NULL
      AND video_watermark_sequence IS NULL
      AND agent_watermark_sequence IS NULL
    ) OR (
      latest_successful_baseline_id IS NOT NULL
      AND channel_watermark_sequence IS NOT NULL
      AND video_watermark_sequence IS NOT NULL
      AND agent_watermark_sequence IS NOT NULL
    )
  ),
  CHECK (
    (mode = 'hold' AND online_at IS NULL AND sealed_at IS NULL)
    OR (mode = 'online' AND online_at IS NOT NULL AND sealed_at IS NULL)
    OR (mode = 'sealed' AND sealed_at IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_publication_channel_delivery_state_mode
ON publication.channel_delivery_state (destination, mode, publication_stream_id, channel_id);

CREATE OR REPLACE FUNCTION publication.guard_channel_delivery_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $publication_guard$
BEGIN
  IF NEW.destination IS DISTINCT FROM OLD.destination
     OR NEW.publication_stream_id IS DISTINCT FROM OLD.publication_stream_id
     OR NEW.channel_id IS DISTINCT FROM OLD.channel_id
     OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'Publication Delivery identity and creation fields are immutable'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.mode = 'online' AND NEW.mode = 'hold' THEN
    RAISE EXCEPTION 'online Publication Delivery cannot return to hold'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.mode = 'sealed' AND (
    NEW.mode IS DISTINCT FROM OLD.mode
    OR NEW.latest_successful_baseline_id IS DISTINCT FROM OLD.latest_successful_baseline_id
    OR NEW.channel_watermark_sequence IS DISTINCT FROM OLD.channel_watermark_sequence
    OR NEW.video_watermark_sequence IS DISTINCT FROM OLD.video_watermark_sequence
    OR NEW.agent_watermark_sequence IS DISTINCT FROM OLD.agent_watermark_sequence
    OR NEW.source_ownership_reference IS DISTINCT FROM OLD.source_ownership_reference
    OR NEW.cutover_reference IS DISTINCT FROM OLD.cutover_reference
    OR NEW.online_at IS DISTINCT FROM OLD.online_at
    OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
    OR NEW.state_changed_at IS DISTINCT FROM OLD.state_changed_at
    OR NEW.state_changed_by IS DISTINCT FROM OLD.state_changed_by
    OR NEW.state_reason IS DISTINCT FROM OLD.state_reason
  ) THEN
    RAISE EXCEPTION 'sealed Publication Delivery state cannot be changed'
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$publication_guard$;

DROP TRIGGER IF EXISTS trg_publication_channel_delivery_lifecycle
ON publication.channel_delivery_state;
CREATE TRIGGER trg_publication_channel_delivery_lifecycle
BEFORE UPDATE ON publication.channel_delivery_state
FOR EACH ROW EXECUTE FUNCTION publication.guard_channel_delivery_lifecycle();

CREATE TABLE IF NOT EXISTS publication.domain_current (
  publication_stream_id UUID NOT NULL,
  channel_id TEXT NOT NULL,
  domain TEXT NOT NULL
    CHECK (domain IN ('channel', 'video', 'agent')),
  contract_version INTEGER NOT NULL CHECK (contract_version > 0),
  policy_version TEXT NOT NULL,
  readiness_status TEXT NOT NULL
    CHECK (readiness_status IN ('ready', 'not_ready')),
  readiness_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload_json JSONB,
  result_hash TEXT,
  source_refs JSONB NOT NULL DEFAULT '{}'::jsonb,
  complete_observed_at TIMESTAMPTZ,
  data_sequence BIGINT NOT NULL DEFAULT 0 CHECK (data_sequence >= 0),
  current_revision_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (publication_stream_id, channel_id, domain),
  FOREIGN KEY (publication_stream_id, channel_id)
    REFERENCES publication.channel_stream_state(publication_stream_id, channel_id)
    ON DELETE RESTRICT,
  CHECK (btrim(channel_id) <> '' AND btrim(policy_version) <> ''),
  CHECK (jsonb_typeof(readiness_reasons) = 'array'),
  CHECK (payload_json IS NULL OR jsonb_typeof(payload_json) = 'object'),
  CHECK (jsonb_typeof(source_refs) = 'object'),
  CHECK (result_hash IS NULL OR result_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK ((payload_json IS NULL) = (result_hash IS NULL)),
  CHECK (
    readiness_status <> 'ready'
    OR (
      payload_json IS NOT NULL
      AND result_hash IS NOT NULL
      AND complete_observed_at IS NOT NULL
      AND jsonb_array_length(readiness_reasons) = 0
    )
  ),
  CHECK (readiness_status <> 'not_ready' OR jsonb_array_length(readiness_reasons) > 0),
  CHECK (
    (data_sequence = 0 AND current_revision_id IS NULL)
    OR (data_sequence > 0 AND current_revision_id IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_publication_domain_current_readiness
ON publication.domain_current (readiness_status, domain, updated_at, publication_stream_id, channel_id);
-- publication-current-schema:end

-- publication-capture-schema:start
CREATE TABLE IF NOT EXISTS publication.revision (
  revision_id UUID PRIMARY KEY,
  publication_stream_id UUID NOT NULL,
  channel_id TEXT NOT NULL,
  domain TEXT NOT NULL
    CHECK (domain IN ('channel', 'video', 'agent')),
  data_sequence BIGINT NOT NULL CHECK (data_sequence > 0),
  previous_data_sequence BIGINT,
  revision_type TEXT NOT NULL
    CHECK (revision_type IN ('bootstrap', 'incremental', 'repair', 'retraction')),
  operation TEXT NOT NULL
    CHECK (operation IN (
      'replace', 'replace_window', 'apply_window_delta', 'retract_channel', 'retract_agent'
    )),
  contract_version INTEGER NOT NULL CHECK (contract_version > 0),
  policy_version TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  source_refs JSONB NOT NULL,
  previous_result_hash TEXT,
  result_hash TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (publication_stream_id, channel_id, domain, data_sequence),
  UNIQUE (
    publication_stream_id, channel_id, domain, data_sequence, revision_id, result_hash
  ),
  FOREIGN KEY (publication_stream_id, channel_id)
    REFERENCES publication.channel_stream_state(publication_stream_id, channel_id)
    ON DELETE RESTRICT,
  CHECK (btrim(channel_id) <> '' AND btrim(policy_version) <> ''),
  CHECK (jsonb_typeof(source_refs) = 'object'),
  CHECK (jsonb_typeof(payload_json) = 'object'),
  CHECK (previous_result_hash IS NULL OR previous_result_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (result_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (payload_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (
    (
      revision_type = 'bootstrap'
      AND data_sequence = 1
      AND previous_data_sequence IS NULL
      AND previous_result_hash IS NULL
    ) OR (
      revision_type <> 'bootstrap'
      AND previous_data_sequence = data_sequence - 1
      AND previous_result_hash IS NOT NULL
    )
  ),
  CHECK (
    (domain = 'channel' AND (
      (revision_type = 'retraction' AND operation = 'retract_channel')
      OR (revision_type <> 'retraction' AND operation = 'replace')
    ))
    OR (domain = 'video' AND (
      (revision_type = 'bootstrap' AND operation = 'replace_window')
      OR (revision_type <> 'bootstrap' AND operation = 'apply_window_delta')
    ))
    OR (domain = 'agent' AND (
      (revision_type = 'retraction' AND operation = 'retract_agent')
      OR (revision_type <> 'retraction' AND operation = 'replace')
    ))
  )
);

CREATE INDEX IF NOT EXISTS idx_publication_revision_channel_sequence
ON publication.revision (publication_stream_id, channel_id, domain, data_sequence DESC);

CREATE OR REPLACE FUNCTION publication.guard_revision_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $publication_guard$
BEGIN
  RAISE EXCEPTION 'Publication Revision rows are immutable'
    USING ERRCODE = '55000';
END
$publication_guard$;

DROP TRIGGER IF EXISTS trg_publication_revision_immutable ON publication.revision;
CREATE TRIGGER trg_publication_revision_immutable
BEFORE UPDATE OR DELETE ON publication.revision
FOR EACH ROW EXECUTE FUNCTION publication.guard_revision_immutable();

ALTER TABLE publication.domain_current
DROP CONSTRAINT IF EXISTS domain_current_online_payload_check;
ALTER TABLE publication.domain_current
ADD CONSTRAINT domain_current_online_payload_check
CHECK (
  data_sequence = 0
  OR (current_revision_id IS NOT NULL AND payload_json IS NOT NULL AND result_hash IS NOT NULL)
);

ALTER TABLE publication.domain_current
DROP CONSTRAINT IF EXISTS domain_current_current_revision_id_fkey;
ALTER TABLE publication.domain_current
ADD CONSTRAINT domain_current_current_revision_id_fkey
FOREIGN KEY (
  publication_stream_id, channel_id, domain, data_sequence, current_revision_id, result_hash
)
REFERENCES publication.revision (
  publication_stream_id, channel_id, domain, data_sequence, revision_id, result_hash
)
ON DELETE RESTRICT;

CREATE TABLE IF NOT EXISTS publication.outbox (
  destination TEXT NOT NULL,
  revision_id UUID NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN (
      'held', 'pending', 'leased', 'retry_wait', 'delivered',
      'covered_by_baseline', 'dead_letter'
    )),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  last_error TEXT,
  receipt_id TEXT,
  receipt_status TEXT
    CHECK (receipt_status IS NULL OR receipt_status IN (
      'accepted', 'duplicate', 'waiting_gap', 'rejected', 'conflict'
    )),
  receipt_received_at TIMESTAMPTZ,
  receipt_json JSONB,
  delivered_at TIMESTAMPTZ,
  covered_by_baseline_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (destination, revision_id),
  FOREIGN KEY (revision_id) REFERENCES publication.revision(revision_id) ON DELETE RESTRICT,
  CHECK (btrim(destination) <> ''),
  CHECK (lease_owner IS NULL OR btrim(lease_owner) <> ''),
  CHECK (receipt_id IS NULL OR btrim(receipt_id) <> ''),
  CHECK (receipt_json IS NULL OR jsonb_typeof(receipt_json) = 'object'),
  CHECK (
    (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
    OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK (
    (
      status = 'delivered'
      AND delivered_at IS NOT NULL
      AND receipt_id IS NOT NULL
      AND receipt_status IN ('accepted', 'duplicate', 'waiting_gap')
      AND receipt_received_at IS NOT NULL
    ) OR (status <> 'delivered' AND delivered_at IS NULL)
  ),
  CHECK (
    (status = 'covered_by_baseline' AND covered_by_baseline_id IS NOT NULL)
    OR (status <> 'covered_by_baseline' AND covered_by_baseline_id IS NULL)
  ),
  CHECK (status <> 'dead_letter' OR last_error IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_publication_outbox_claim
ON publication.outbox (status, next_attempt_at, created_at, revision_id)
WHERE status IN ('pending', 'retry_wait', 'leased');

CREATE INDEX IF NOT EXISTS idx_publication_outbox_revision
ON publication.outbox (revision_id, destination);

CREATE OR REPLACE FUNCTION publication.guard_outbox_lifecycle()
RETURNS trigger
LANGUAGE plpgsql
AS $publication_guard$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Publication Outbox rows cannot be deleted'
      USING ERRCODE = '55000';
  END IF;
  IF NEW.destination IS DISTINCT FROM OLD.destination
     OR NEW.revision_id IS DISTINCT FROM OLD.revision_id
     OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'Publication Outbox identity and creation fields are immutable'
      USING ERRCODE = '55000';
  END IF;
  IF NEW.attempts < OLD.attempts THEN
    RAISE EXCEPTION 'Publication Outbox attempts cannot decrease'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.status = 'delivered' AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'delivered Publication Outbox rows are immutable'
      USING ERRCODE = '55000';
  END IF;
  IF OLD.status = 'covered_by_baseline' AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'covered Publication Outbox rows are immutable'
      USING ERRCODE = '55000';
  END IF;
  IF NOT (
    (OLD.status = 'held' AND NEW.status IN ('held', 'pending', 'covered_by_baseline'))
    OR (OLD.status = 'pending' AND NEW.status IN ('pending', 'leased', 'dead_letter'))
    OR (OLD.status = 'leased' AND NEW.status IN ('leased', 'delivered', 'retry_wait', 'dead_letter'))
    OR (OLD.status = 'retry_wait' AND NEW.status IN ('retry_wait', 'pending', 'dead_letter'))
    OR (OLD.status = 'dead_letter' AND NEW.status IN ('dead_letter', 'pending'))
    OR (OLD.status = 'delivered' AND NEW.status = 'delivered')
    OR (OLD.status = 'covered_by_baseline' AND NEW.status = 'covered_by_baseline')
  ) THEN
    RAISE EXCEPTION 'invalid Publication Outbox status transition: % -> %', OLD.status, NEW.status
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$publication_guard$;

DROP TRIGGER IF EXISTS trg_publication_outbox_lifecycle ON publication.outbox;
CREATE TRIGGER trg_publication_outbox_lifecycle
BEFORE UPDATE OR DELETE ON publication.outbox
FOR EACH ROW EXECUTE FUNCTION publication.guard_outbox_lifecycle();

CREATE OR REPLACE FUNCTION publication.writer_version_satisfies(
  actual_version TEXT,
  minimum_version TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $publication_writer_version$
DECLARE
  actual_match TEXT[];
  minimum_match TEXT[];
BEGIN
  IF btrim(actual_version) = btrim(minimum_version) THEN
    RETURN true;
  END IF;
  actual_match := regexp_match(btrim(actual_version), '^(.*)-v([1-9][0-9]*)$');
  minimum_match := regexp_match(btrim(minimum_version), '^(.*)-v([1-9][0-9]*)$');
  RETURN actual_match IS NOT NULL
    AND minimum_match IS NOT NULL
    AND actual_match[1] = minimum_match[1]
    AND actual_match[2]::numeric >= minimum_match[2]::numeric;
END
$publication_writer_version$;

CREATE OR REPLACE FUNCTION publication.assert_source_writer_version(target_channel_id TEXT)
RETURNS VOID
LANGUAGE plpgsql
AS $publication_writer_guard$
DECLARE
  actual_version TEXT;
  required_versions TEXT[];
  required_version TEXT;
BEGIN
  SELECT array_agg(DISTINCT stream.minimum_writer_version ORDER BY stream.minimum_writer_version)
  INTO required_versions
  FROM publication.channel_stream_state AS channel_state
  JOIN publication.stream AS stream
    ON stream.publication_stream_id=channel_state.publication_stream_id
  WHERE channel_state.channel_id=target_channel_id
    AND channel_state.status='owned'
    AND stream.status='active'
    AND stream.capture_enabled_at IS NOT NULL;

  IF required_versions IS NULL THEN
    RETURN;
  END IF;
  actual_version := nullif(btrim(current_setting('publication.writer_version', true)), '');
  IF actual_version IS NULL THEN
    actual_version := nullif(btrim(current_setting('application_name', true)), '');
  END IF;
  IF actual_version IS NULL THEN
    RAISE EXCEPTION 'Publication writer version is required for captured Channel %',
      target_channel_id
      USING ERRCODE = '55000';
  END IF;
  FOREACH required_version IN ARRAY required_versions LOOP
    IF NOT publication.writer_version_satisfies(actual_version, required_version) THEN
      RAISE EXCEPTION
        'Publication writer version % does not satisfy required version % for Channel %',
        actual_version, required_version, target_channel_id
        USING ERRCODE = '55000';
    END IF;
  END LOOP;
END
$publication_writer_guard$;

CREATE OR REPLACE FUNCTION publication.guard_source_writer_version()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $publication_writer_guard$
BEGIN
  IF TG_OP IN ('UPDATE', 'DELETE') THEN
    PERFORM publication.assert_source_writer_version(OLD.channel_id);
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE')
     AND (TG_OP <> 'UPDATE' OR NEW.channel_id IS DISTINCT FROM OLD.channel_id) THEN
    PERFORM publication.assert_source_writer_version(NEW.channel_id);
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$publication_writer_guard$;

DROP TRIGGER IF EXISTS trg_publication_channels_writer_version ON crawler.channels;
CREATE TRIGGER trg_publication_channels_writer_version
BEFORE INSERT OR UPDATE OR DELETE ON crawler.channels
FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();

DROP TRIGGER IF EXISTS trg_publication_contents_writer_version ON crawler.contents;
CREATE TRIGGER trg_publication_contents_writer_version
BEFORE INSERT OR UPDATE OR DELETE ON crawler.contents
FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();

DROP TRIGGER IF EXISTS trg_publication_agent_profiles_writer_version ON crawler.agent_profiles;
CREATE TRIGGER trg_publication_agent_profiles_writer_version
BEFORE INSERT OR UPDATE OR DELETE ON crawler.agent_profiles
FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();

DROP TRIGGER IF EXISTS trg_publication_finalized_profiles_writer_version
ON crawler.finalized_profiles;
CREATE TRIGGER trg_publication_finalized_profiles_writer_version
BEFORE INSERT OR UPDATE OR DELETE ON crawler.finalized_profiles
FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();
-- publication-capture-schema:end

-- Keep this compatibility constraint last: pre-existing disabled/null rows are repaired
-- separately, and earlier schema maintenance statements may still touch those rows.
DO $contents_comment_state_shape$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='crawler.contents'::regclass
      AND conname='contents_comment_state_shape'
  ) THEN
    ALTER TABLE crawler.contents
    ADD CONSTRAINT contents_comment_state_shape CHECK (
      (comments_disabled IS TRUE AND comment_count=0 AND comment_count_status='disabled')
      OR (comments_disabled IS DISTINCT FROM TRUE AND comment_count_status<>'disabled')
    ) NOT VALID;
  END IF;
END
$contents_comment_state_shape$;
