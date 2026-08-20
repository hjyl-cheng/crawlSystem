SET TIME ZONE 'UTC';

DO $$
BEGIN
  IF to_regnamespace('feature_clock') IS NULL THEN
    EXECUTE 'CREATE SCHEMA feature_clock';
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION feature_clock.normalize_clock_due_at(input_at TIMESTAMPTZ)
RETURNS TIMESTAMPTZ
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
  SELECT CASE
    WHEN (input_at AT TIME ZONE 'UTC')::time < TIME '00:30'
      THEN input_at + INTERVAL '30 minutes'
    WHEN (input_at AT TIME ZONE 'UTC')::time >= TIME '21:30'
      THEN input_at + INTERVAL '3 hours'
    ELSE input_at
  END
$$;

CREATE OR REPLACE FUNCTION feature_clock.clock_due_at_in_safe_window(input_at TIMESTAMPTZ)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
  SELECT (input_at AT TIME ZONE 'UTC')::time >= TIME '00:30'
     AND (input_at AT TIME ZONE 'UTC')::time < TIME '21:30'
$$;

CREATE TABLE IF NOT EXISTS feature_clock.crawler_event_inbox (
  event_id UUID PRIMARY KEY,
  observation_id UUID NOT NULL UNIQUE,
  event_type TEXT NOT NULL CHECK (event_type = 'crawler.observation.recorded'),
  event_version INTEGER NOT NULL CHECK (event_version > 0),
  channel_id TEXT NOT NULL,
  observation_kind TEXT NOT NULL
    CHECK (observation_kind IN ('about', 'video', 'agent')),
  kind_sequence BIGINT NOT NULL CHECK (kind_sequence > 0),
  plan_id UUID,
  observed_at TIMESTAMPTZ NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('complete', 'partial', 'failed')),
  payload_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'received'
    CHECK (status IN ('received', 'waiting_gap', 'applied', 'rejected')),
  pending_payload_json JSONB,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  applied_at TIMESTAMPTZ,
  error_code TEXT,
  error_message TEXT,
  UNIQUE (channel_id, observation_kind, kind_sequence),
  CHECK (
    (status = 'waiting_gap' AND pending_payload_json IS NOT NULL)
    OR (status = 'received')
    OR (status IN ('applied', 'rejected') AND pending_payload_json IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_feature_clock_inbox_status_sequence
ON feature_clock.crawler_event_inbox (status, channel_id, observation_kind, kind_sequence);

CREATE INDEX IF NOT EXISTS idx_feature_clock_inbox_received_at
ON feature_clock.crawler_event_inbox (received_at);

ALTER TABLE feature_clock.crawler_event_inbox
ADD COLUMN IF NOT EXISTS plan_id UUID;

ALTER TABLE feature_clock.crawler_event_inbox
DROP CONSTRAINT IF EXISTS crawler_event_inbox_observation_kind_check;
ALTER TABLE feature_clock.crawler_event_inbox
ADD CONSTRAINT crawler_event_inbox_observation_kind_check
CHECK (observation_kind IN ('about', 'video', 'agent')) NOT VALID;

CREATE INDEX IF NOT EXISTS idx_feature_clock_inbox_plan_kind
ON feature_clock.crawler_event_inbox (plan_id, observation_kind, kind_sequence DESC)
WHERE plan_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS feature_clock.channel_observation_checkpoints (
  channel_id TEXT NOT NULL,
  observation_kind TEXT NOT NULL
    CHECK (observation_kind IN ('about', 'video', 'agent')),
  last_applied_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_applied_sequence >= 0),
  last_observation_id UUID,
  last_observed_at TIMESTAMPTZ,
  last_payload_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel_id, observation_kind)
);

ALTER TABLE feature_clock.channel_observation_checkpoints
DROP CONSTRAINT IF EXISTS channel_observation_checkpoints_observation_kind_check;
ALTER TABLE feature_clock.channel_observation_checkpoints
ADD CONSTRAINT channel_observation_checkpoints_observation_kind_check
CHECK (observation_kind IN ('about', 'video', 'agent')) NOT VALID;

CREATE TABLE IF NOT EXISTS feature_clock.rule_policy_definitions (
  policy_version TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'active', 'retired')),
  effective_from TIMESTAMPTZ NOT NULL,
  allowed_days INTEGER[] NOT NULL,
  about_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  discovery_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  recent_sampling_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  agent_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  partial_retry_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  checksum TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  activated_at TIMESTAMPTZ,
  CHECK (cardinality(allowed_days) > 0),
  CHECK (0 < ALL (allowed_days))
);

-- Upgrade four-Clock policy rows before installing the immutable-policy trigger.
-- Profile has no policy, retry cadence, or retained compatibility column.
DROP TRIGGER IF EXISTS trg_protect_active_policy_definition
ON feature_clock.rule_policy_definitions;

ALTER TABLE feature_clock.rule_policy_definitions
DROP COLUMN IF EXISTS profile_config;

UPDATE feature_clock.rule_policy_definitions
SET partial_retry_config=partial_retry_config - 'profile_days'
WHERE partial_retry_config ? 'profile_days';

CREATE UNIQUE INDEX IF NOT EXISTS ux_feature_clock_one_active_policy
ON feature_clock.rule_policy_definitions (status)
WHERE status = 'active';

CREATE OR REPLACE FUNCTION feature_clock.protect_active_policy_definition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.status = 'active' AND NEW.status NOT IN ('active', 'retired') THEN
    RAISE EXCEPTION 'an active policy can only remain active or become retired';
  END IF;
  IF OLD.status = 'active' AND (
    NEW.policy_version IS DISTINCT FROM OLD.policy_version
    OR NEW.effective_from IS DISTINCT FROM OLD.effective_from
    OR NEW.allowed_days IS DISTINCT FROM OLD.allowed_days
    OR NEW.about_config IS DISTINCT FROM OLD.about_config
    OR NEW.discovery_config IS DISTINCT FROM OLD.discovery_config
    OR NEW.recent_sampling_config IS DISTINCT FROM OLD.recent_sampling_config
    OR NEW.agent_config IS DISTINCT FROM OLD.agent_config
    OR NEW.partial_retry_config IS DISTINCT FROM OLD.partial_retry_config
    OR NEW.checksum IS DISTINCT FROM OLD.checksum
  ) THEN
    RAISE EXCEPTION 'active policy definitions are immutable; create a new version';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_protect_active_policy_definition
ON feature_clock.rule_policy_definitions;
CREATE TRIGGER trg_protect_active_policy_definition
BEFORE UPDATE ON feature_clock.rule_policy_definitions
FOR EACH ROW EXECUTE FUNCTION feature_clock.protect_active_policy_definition();

CREATE TABLE IF NOT EXISTS feature_clock.feature_reference_distributions (
  as_of_day DATE NOT NULL,
  cohort_key TEXT NOT NULL,
  feature_name TEXT NOT NULL,
  sample_count BIGINT NOT NULL CHECK (sample_count >= 0),
  quantiles JSONB NOT NULL,
  method_version TEXT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (as_of_day, cohort_key, feature_name, method_version)
);

CREATE TABLE IF NOT EXISTS feature_clock.collection_priority_signals (
  channel_id TEXT PRIMARY KEY,
  user_query_demand DOUBLE PRECISION NOT NULL DEFAULT 0.0
    CHECK (user_query_demand BETWEEN 0 AND 1),
  manual_priority DOUBLE PRECISION NOT NULL DEFAULT 0.0
    CHECK (manual_priority BETWEEN 0 AND 1),
  source_version TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  user_query_demand_expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    user_query_demand_expires_at IS NULL
    OR user_query_demand_expires_at > observed_at
  )
);

CREATE INDEX IF NOT EXISTS idx_feature_clock_priority_signals_expiry
ON feature_clock.collection_priority_signals (user_query_demand_expires_at)
WHERE user_query_demand_expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS feature_clock.channel_feature_state (
  channel_id TEXT PRIMARY KEY,

  last_subscriber_count BIGINT,
  last_subscriber_observed_at TIMESTAMPTZ,
  last_total_view_count BIGINT,
  last_total_view_observed_at TIMESTAMPTZ,
  last_total_video_count BIGINT,
  last_total_video_observed_at TIMESTAMPTZ,
  last_about_observed_at TIMESTAMPTZ,
  about_metric_confidence DOUBLE PRECISION,
  subscriber_velocity_ewma DOUBLE PRECISION,
  view_velocity_ewma DOUBLE PRECISION,
  video_count_delta BIGINT,
  subscriber_size_percentile DOUBLE PRECISION,
  subscriber_growth_percentile DOUBLE PRECISION,
  view_growth_percentile DOUBLE PRECISION,
  growth_momentum DOUBLE PRECISION,
  about_stable_since TIMESTAMPTZ,
  about_stable_runs INTEGER NOT NULL DEFAULT 0 CHECK (about_stable_runs >= 0),

  recent_publish_interval_days DOUBLE PRECISION[] NOT NULL DEFAULT '{}'::double precision[],
  publish_interval_ewma DOUBLE PRECISION,
  publish_interval_median DOUBLE PRECISION,
  publish_interval_mad DOUBLE PRECISION,
  publish_regularity DOUBLE PRECISION,
  last_publish_at TIMESTAMPTZ,
  recent30_video_count INTEGER,
  new_video_empty_runs INTEGER NOT NULL DEFAULT 0 CHECK (new_video_empty_runs >= 0),
  last_discovery_observed_at TIMESTAMPTZ,
  last_complete_discovery_at TIMESTAMPTZ,

  recent_stale_ratio DOUBLE PRECISION,
  recent_view_change_ewma DOUBLE PRECISION,
  recent_engagement_change_ewma DOUBLE PRECISION,
  recent_upload_change_ewma DOUBLE PRECISION,
  recent_change_probability DOUBLE PRECISION,
  recent_sampling_stable_runs INTEGER NOT NULL DEFAULT 0 CHECK (recent_sampling_stable_runs >= 0),
  last_recent_sampling_at TIMESTAMPTZ,
  last_recent_sample_count INTEGER,

  current_topic_vector REAL[] NOT NULL DEFAULT '{}'::real[],
  current_topic_tokens TEXT[] NOT NULL DEFAULT '{}'::text[],
  current_agent_output_hash TEXT,
  current_agent_evidence_fingerprints TEXT[] NOT NULL DEFAULT '{}'::text[],
  current_agent_version_hash TEXT,
  last_agent_evidence_count INTEGER,
  topic_drift DOUBLE PRECISION,
  evidence_replacement DOUBLE PRECISION,
  recent_content_shift DOUBLE PRECISION,
  agent_version_changed BOOLEAN NOT NULL DEFAULT false,
  agent_output_changed BOOLEAN NOT NULL DEFAULT false,
  agent_change_score DOUBLE PRECISION,
  agent_topic_vector_source TEXT,
  agent_confidence DOUBLE PRECISION,
  agent_stable_runs INTEGER NOT NULL DEFAULT 0 CHECK (agent_stable_runs >= 0),
  last_agent_observed_at TIMESTAMPTZ,

  user_query_demand DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  data_incompleteness DOUBLE PRECISION NOT NULL DEFAULT 1.0,
  manual_priority DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  collection_priority DOUBLE PRECISION NOT NULL DEFAULT 0.346875,
  channel_activity DOUBLE PRECISION,
  feature_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
  fallback_reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  reference_distribution_version TEXT,
  feature_version TEXT NOT NULL DEFAULT 'v16-feature-2',
  state_version BIGINT NOT NULL DEFAULT 0 CHECK (state_version >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (last_subscriber_count IS NULL OR last_subscriber_count >= 0),
  CHECK (last_total_view_count IS NULL OR last_total_view_count >= 0),
  CHECK (last_total_video_count IS NULL OR last_total_video_count >= 0),
  CHECK (about_metric_confidence IS NULL OR about_metric_confidence BETWEEN 0 AND 1),
  CHECK (subscriber_size_percentile IS NULL OR subscriber_size_percentile BETWEEN 0 AND 1),
  CHECK (subscriber_growth_percentile IS NULL OR subscriber_growth_percentile BETWEEN 0 AND 1),
  CHECK (view_growth_percentile IS NULL OR view_growth_percentile BETWEEN 0 AND 1),
  CHECK (growth_momentum IS NULL OR growth_momentum BETWEEN 0 AND 1),
  CHECK (user_query_demand BETWEEN 0 AND 1),
  CHECK (data_incompleteness BETWEEN 0 AND 1),
  CHECK (manual_priority BETWEEN 0 AND 1),
  CHECK (collection_priority BETWEEN 0 AND 1),
  CHECK (channel_activity IS NULL OR channel_activity BETWEEN 0 AND 1),
  CHECK (feature_confidence BETWEEN 0 AND 1),
  CHECK (publish_regularity IS NULL OR publish_regularity BETWEEN 0 AND 1),
  CHECK (recent_stale_ratio IS NULL OR recent_stale_ratio BETWEEN 0 AND 1),
  CHECK (recent_change_probability IS NULL OR recent_change_probability BETWEEN 0 AND 1),
  CHECK (topic_drift IS NULL OR topic_drift BETWEEN 0 AND 1),
  CHECK (evidence_replacement IS NULL OR evidence_replacement BETWEEN 0 AND 1),
  CHECK (recent_content_shift IS NULL OR recent_content_shift BETWEEN 0 AND 1),
  CHECK (agent_change_score IS NULL OR agent_change_score BETWEEN 0 AND 1),
  CHECK (agent_confidence IS NULL OR agent_confidence BETWEEN 0 AND 1),
  CHECK (last_agent_evidence_count IS NULL OR last_agent_evidence_count >= 0),
  CHECK (cardinality(recent_publish_interval_days) <= 32),
  CHECK (cardinality(current_topic_tokens) <= 128),
  CHECK (cardinality(current_agent_evidence_fingerprints) <= 256)
);

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS last_discovery_observed_at TIMESTAMPTZ;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS about_metric_confidence DOUBLE PRECISION;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS last_agent_evidence_count INTEGER;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS agent_stable_runs INTEGER NOT NULL DEFAULT 0;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS subscriber_size_percentile DOUBLE PRECISION;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS user_query_demand DOUBLE PRECISION NOT NULL DEFAULT 0.0;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS data_incompleteness DOUBLE PRECISION NOT NULL DEFAULT 1.0;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS manual_priority DOUBLE PRECISION NOT NULL DEFAULT 0.0;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS reference_distribution_version TEXT;

ALTER TABLE feature_clock.channel_feature_state
DROP COLUMN IF EXISTS profile_field_hashes,
DROP COLUMN IF EXISTS profile_change_score,
DROP COLUMN IF EXISTS profile_major_change,
DROP COLUMN IF EXISTS profile_change_ewma,
DROP COLUMN IF EXISTS profile_stable_runs,
DROP COLUMN IF EXISTS last_profile_observed_at;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS current_topic_tokens TEXT[] NOT NULL DEFAULT '{}'::text[];

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS current_agent_evidence_fingerprints TEXT[] NOT NULL DEFAULT '{}'::text[];

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS current_agent_version_hash TEXT;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS recent_content_shift DOUBLE PRECISION;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS agent_version_changed BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS agent_output_changed BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS agent_change_score DOUBLE PRECISION;

ALTER TABLE feature_clock.channel_feature_state
ADD COLUMN IF NOT EXISTS agent_topic_vector_source TEXT;

ALTER TABLE feature_clock.channel_feature_state
ALTER COLUMN collection_priority SET DEFAULT 0.346875;

ALTER TABLE feature_clock.channel_feature_state
ALTER COLUMN feature_version SET DEFAULT 'v16-feature-2';

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_subscriber_size_percentile_check;

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_about_metric_confidence_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_about_metric_confidence_check
CHECK (about_metric_confidence IS NULL OR about_metric_confidence BETWEEN 0 AND 1);

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_subscriber_size_percentile_check
CHECK (subscriber_size_percentile IS NULL OR subscriber_size_percentile BETWEEN 0 AND 1);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_user_query_demand_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_user_query_demand_check
CHECK (user_query_demand BETWEEN 0 AND 1);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_data_incompleteness_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_data_incompleteness_check
CHECK (data_incompleteness BETWEEN 0 AND 1);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_manual_priority_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_manual_priority_check
CHECK (manual_priority BETWEEN 0 AND 1);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_last_agent_evidence_count_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_last_agent_evidence_count_check
CHECK (last_agent_evidence_count IS NULL OR last_agent_evidence_count >= 0);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_agent_stable_runs_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_agent_stable_runs_check
CHECK (agent_stable_runs >= 0);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_current_topic_tokens_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_current_topic_tokens_check
CHECK (cardinality(current_topic_tokens) <= 128);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_current_agent_evidence_fingerprints_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_current_agent_evidence_fingerprints_check
CHECK (cardinality(current_agent_evidence_fingerprints) <= 256);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_recent_content_shift_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_recent_content_shift_check
CHECK (recent_content_shift IS NULL OR recent_content_shift BETWEEN 0 AND 1);

ALTER TABLE feature_clock.channel_feature_state
DROP CONSTRAINT IF EXISTS channel_feature_state_agent_change_score_check;

ALTER TABLE feature_clock.channel_feature_state
ADD CONSTRAINT channel_feature_state_agent_change_score_check
CHECK (agent_change_score IS NULL OR agent_change_score BETWEEN 0 AND 1);

CREATE TABLE IF NOT EXISTS feature_clock.channel_clock_state (
  channel_id TEXT PRIMARY KEY,
  lifecycle_status TEXT NOT NULL DEFAULT 'active'
    CHECK (lifecycle_status IN ('active', 'dormant', 'removed')),
  removed_reason TEXT,
  removed_at TIMESTAMPTZ,
  removed_source_event_id UUID,
  dormant_reason TEXT,
  dormant_since TIMESTAMPTZ,
  dormant_recheck_day DATE,
  dormant_cycle INTEGER NOT NULL DEFAULT 0 CHECK (dormant_cycle >= 0),
  dormant_source_event_id UUID,
  about_due_at TIMESTAMPTZ NOT NULL,
  about_due_day DATE NOT NULL,
  about_tier INTEGER NOT NULL,
  about_last_complete_at TIMESTAMPTZ,
  video_due_at TIMESTAMPTZ NOT NULL,
  video_due_day DATE NOT NULL,
  video_tier INTEGER NOT NULL,
  video_last_complete_at TIMESTAMPTZ,
  video_last_outcome TEXT
    CHECK (video_last_outcome IS NULL OR video_last_outcome IN ('complete', 'partial')),
  agent_due_at TIMESTAMPTZ NOT NULL,
  agent_due_day DATE NOT NULL,
  agent_tier INTEGER NOT NULL,
  agent_mode TEXT NOT NULL DEFAULT 'basic',
  agent_last_complete_at TIMESTAMPTZ,
  channel_next_run_at TIMESTAMPTZ NOT NULL,
  channel_next_run_day DATE NOT NULL,
  dispatch_slot INTEGER NOT NULL CHECK (dispatch_slot >= 0),
  estimated_request_cost INTEGER NOT NULL DEFAULT 0 CHECK (estimated_request_cost >= 0),
  policy_version TEXT NOT NULL REFERENCES feature_clock.rule_policy_definitions(policy_version),
  feature_state_version BIGINT NOT NULL CHECK (feature_state_version >= 0),
  clock_version BIGINT NOT NULL DEFAULT 1 CHECK (clock_version > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (about_tier IN (1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365)),
  CHECK (video_tier IN (1, 3, 7, 14, 30, 60, 90, 180, 365)),
  CHECK (
    agent_tier IN (1, 3, 7, 14, 30)
    OR agent_tier BETWEEN 60 AND 365
  ),
  CONSTRAINT channel_clock_state_next_run_day_check
    CHECK (channel_next_run_day = LEAST(about_due_day, video_due_day, agent_due_day)),
  CONSTRAINT channel_clock_state_next_run_at_check
    CHECK (channel_next_run_at = LEAST(about_due_at, video_due_at, agent_due_at))
);

ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_about_tier_check;

ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_about_tier_check
CHECK (about_tier IN (1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365));

ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_agent_tier_check;

ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_agent_tier_check
CHECK (
  agent_tier IN (1, 3, 7, 14, 30)
  OR agent_tier BETWEEN 60 AND 365
);

ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS about_due_at TIMESTAMPTZ;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS video_due_at TIMESTAMPTZ;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS agent_due_at TIMESTAMPTZ;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS channel_next_run_at TIMESTAMPTZ;

ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS lifecycle_status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS removed_reason TEXT;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS removed_at TIMESTAMPTZ;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS removed_source_event_id UUID;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS dormant_reason TEXT;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS dormant_since TIMESTAMPTZ;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS dormant_recheck_day DATE;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS dormant_cycle INTEGER NOT NULL DEFAULT 0;
ALTER TABLE feature_clock.channel_clock_state
ADD COLUMN IF NOT EXISTS dormant_source_event_id UUID;
ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_lifecycle_status_check;
ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_lifecycle_status_check
CHECK (lifecycle_status IN ('active', 'dormant', 'removed'));
ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_removed_check;
ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_removed_check
CHECK (
  lifecycle_status<>'removed'
  OR (removed_reason IS NOT NULL AND removed_at IS NOT NULL AND removed_source_event_id IS NOT NULL)
);
ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_dormant_cycle_check;
ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_dormant_cycle_check
CHECK (dormant_cycle >= 0);
ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_dormant_check;
ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_dormant_check
CHECK (
  lifecycle_status<>'dormant'
  OR (
    dormant_reason='no_published_content_within_90_days'
    AND dormant_since IS NOT NULL
    AND dormant_recheck_day IS NOT NULL
    AND dormant_cycle > 0
    AND dormant_source_event_id IS NOT NULL
  )
);

UPDATE feature_clock.channel_clock_state
SET about_due_at=COALESCE(
      about_due_at,
      about_due_day::timestamp AT TIME ZONE 'UTC'
    ),
    video_due_at=COALESCE(
      video_due_at,
      video_due_day::timestamp AT TIME ZONE 'UTC'
    ),
    agent_due_at=COALESCE(
      agent_due_at,
      agent_due_day::timestamp AT TIME ZONE 'UTC'
    );

-- Remove aggregate constraints from older four-Clock schemas before recomputing
-- the authoritative three-Clock minimum and dropping their Profile columns.
DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  FOR constraint_name IN
    SELECT item.conname
    FROM pg_constraint AS item
    WHERE item.conrelid='feature_clock.channel_clock_state'::regclass
      AND item.contype='c'
      AND (
        pg_get_constraintdef(item.oid) LIKE '%channel_next_run_day%'
        OR pg_get_constraintdef(item.oid) LIKE '%channel_next_run_at%'
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE feature_clock.channel_clock_state DROP CONSTRAINT %I',
      constraint_name
    );
  END LOOP;
END
$$;

UPDATE feature_clock.channel_clock_state
SET channel_next_run_at=LEAST(about_due_at,video_due_at,agent_due_at),
    channel_next_run_day=LEAST(about_due_day,video_due_day,agent_due_day)
WHERE channel_next_run_at IS DISTINCT FROM LEAST(
        about_due_at,video_due_at,agent_due_at
      )
   OR channel_next_run_day IS DISTINCT FROM LEAST(
        about_due_day,video_due_day,agent_due_day
      );

ALTER TABLE feature_clock.channel_clock_state
ALTER COLUMN about_due_at SET NOT NULL;
ALTER TABLE feature_clock.channel_clock_state
ALTER COLUMN video_due_at SET NOT NULL;
ALTER TABLE feature_clock.channel_clock_state
ALTER COLUMN agent_due_at SET NOT NULL;
ALTER TABLE feature_clock.channel_clock_state
ALTER COLUMN channel_next_run_at SET NOT NULL;

ALTER TABLE feature_clock.channel_clock_state
DROP COLUMN IF EXISTS profile_due_at,
DROP COLUMN IF EXISTS profile_due_day,
DROP COLUMN IF EXISTS profile_tier,
DROP COLUMN IF EXISTS profile_last_complete_at;

ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_next_run_day_check;
ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_next_run_day_check
CHECK (channel_next_run_day = LEAST(about_due_day,video_due_day,agent_due_day));
ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_next_run_at_check;
ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_next_run_at_check
CHECK (channel_next_run_at = LEAST(about_due_at,video_due_at,agent_due_at));

ALTER TABLE feature_clock.channel_clock_state
DROP CONSTRAINT IF EXISTS channel_clock_state_safe_window_check;

DROP INDEX IF EXISTS feature_clock.idx_feature_clock_channel_next_run;
CREATE INDEX idx_feature_clock_channel_next_run
ON feature_clock.channel_clock_state (
  channel_next_run_day, dispatch_slot, channel_id
)
WHERE lifecycle_status='active';

DROP INDEX IF EXISTS feature_clock.idx_feature_clock_channel_next_run_at;
CREATE INDEX idx_feature_clock_channel_next_run_at
ON feature_clock.channel_clock_state (
  channel_next_run_at, dispatch_slot, channel_id
)
WHERE lifecycle_status='active';

CREATE INDEX IF NOT EXISTS idx_feature_clock_dormant_recheck
ON feature_clock.channel_clock_state (dormant_recheck_day, dispatch_slot, channel_id)
WHERE lifecycle_status='dormant';

CREATE TABLE IF NOT EXISTS feature_clock.clock_decision_log (
  decision_id UUID PRIMARY KEY,
  channel_id TEXT NOT NULL,
  clock_kind TEXT NOT NULL
    CHECK (clock_kind IN ('about', 'video', 'agent')),
  trigger_event_id UUID,
  trigger_observation_id UUID,
  decision_mode TEXT NOT NULL
    CHECK (decision_mode IN ('post_run', 'bootstrap', 'policy_rebuild', 'repair')),
  previous_due_at TIMESTAMPTZ,
  previous_due_day DATE,
  decided_due_at TIMESTAMPTZ NOT NULL,
  decided_due_day DATE NOT NULL,
  tier INTEGER NOT NULL,
  reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
  feature_state_version BIGINT NOT NULL CHECK (feature_state_version >= 0),
  feature_summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  policy_version TEXT NOT NULL REFERENCES feature_clock.rule_policy_definitions(policy_version),
  reference_distribution_version TEXT,
  clock_version_before BIGINT NOT NULL CHECK (clock_version_before >= 0),
  clock_version_after BIGINT NOT NULL CHECK (clock_version_after > 0),
  decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (clock_version_after > clock_version_before)
);

ALTER TABLE feature_clock.clock_decision_log
DROP CONSTRAINT IF EXISTS clock_decision_log_tier_check;

ALTER TABLE feature_clock.clock_decision_log
ADD CONSTRAINT clock_decision_log_tier_check
CHECK (
  (clock_kind='about' AND tier IN (1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365))
  OR (clock_kind='video' AND tier IN (1, 3, 7, 14, 30, 60, 90, 180, 365))
  OR (
    clock_kind='agent'
    AND (tier IN (1, 3, 7, 14, 30) OR tier BETWEEN 60 AND 365)
  )
) NOT VALID;

ALTER TABLE feature_clock.clock_decision_log
DROP CONSTRAINT IF EXISTS clock_decision_log_clock_kind_check;
ALTER TABLE feature_clock.clock_decision_log
ADD CONSTRAINT clock_decision_log_clock_kind_check
CHECK (clock_kind IN ('about', 'video', 'agent')) NOT VALID;

ALTER TABLE feature_clock.clock_decision_log
ADD COLUMN IF NOT EXISTS previous_due_at TIMESTAMPTZ;
ALTER TABLE feature_clock.clock_decision_log
ADD COLUMN IF NOT EXISTS decided_due_at TIMESTAMPTZ;

UPDATE feature_clock.clock_decision_log
SET previous_due_at=CASE
      WHEN previous_due_day IS NULL THEN NULL
      ELSE previous_due_day::timestamp AT TIME ZONE 'UTC'
    END,
    decided_due_at=COALESCE(
      decided_due_at,
      decided_due_day::timestamp AT TIME ZONE 'UTC'
    )
WHERE decided_due_at IS NULL
   OR (previous_due_day IS NOT NULL AND previous_due_at IS NULL);

ALTER TABLE feature_clock.clock_decision_log
ALTER COLUMN decided_due_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_feature_clock_decisions_channel_kind
ON feature_clock.clock_decision_log (channel_id, clock_kind, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_feature_clock_decisions_decided_at
ON feature_clock.clock_decision_log (decided_at);

CREATE TABLE IF NOT EXISTS feature_clock.daily_channel_plans (
  plan_id UUID PRIMARY KEY,
  plan_mode TEXT NOT NULL DEFAULT 'standard'
    CHECK (plan_mode IN ('standard', 'dormant_probe')),
  plan_day DATE NOT NULL,
  channel_id TEXT NOT NULL,
  due_day DATE NOT NULL,
  due_at TIMESTAMPTZ NOT NULL,
  eligible_at TIMESTAMPTZ NOT NULL,
  scheduled_at TIMESTAMPTZ,
  execution_deadline_at TIMESTAMPTZ,
  dispatched_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  run_about BOOLEAN NOT NULL,
  run_video BOOLEAN NOT NULL,
  run_agent BOOLEAN NOT NULL,
  dispatch_slot INTEGER NOT NULL CHECK (dispatch_slot >= 0),
  capacity_factor DOUBLE PRECISION NOT NULL DEFAULT 1.0
    CHECK (capacity_factor BETWEEN 0 AND 1),
  player_cap INTEGER NOT NULL DEFAULT 0 CHECK (player_cap >= 0),
  next_cap INTEGER NOT NULL DEFAULT 0 CHECK (next_cap >= 0),
  estimated_request_cost INTEGER NOT NULL DEFAULT 0 CHECK (estimated_request_cost >= 0),
  source_clock_version BIGINT NOT NULL CHECK (source_clock_version > 0),
  policy_version TEXT NOT NULL REFERENCES feature_clock.rule_policy_definitions(policy_version),
  planner_config_version TEXT NOT NULL,
  capacity_version TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'planned'
    CHECK (status IN (
      'planned', 'dispatching', 'dispatched', 'running', 'succeeded',
      'partial', 'failed', 'cancelled'
    )),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  error_code TEXT,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (plan_day, channel_id),
  CHECK (run_about OR run_video OR run_agent),
  CHECK (due_day <= plan_day),
  CHECK (
    execution_deadline_at IS NULL
    OR (scheduled_at IS NOT NULL AND execution_deadline_at > scheduled_at)
  )
);

ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS due_day DATE;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS due_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS eligible_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS execution_deadline_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;
ALTER TABLE feature_clock.daily_channel_plans
ADD COLUMN IF NOT EXISTS plan_mode TEXT NOT NULL DEFAULT 'standard';

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='feature_clock'
      AND table_name='daily_channel_plans'
      AND column_name='run_profile'
  ) THEN
    FOR constraint_name IN
      SELECT item.conname
      FROM pg_constraint AS item
      WHERE item.conrelid='feature_clock.daily_channel_plans'::regclass
        AND item.contype='c'
        AND pg_get_constraintdef(item.oid) LIKE '%run_profile%'
    LOOP
      EXECUTE format(
        'ALTER TABLE feature_clock.daily_channel_plans DROP CONSTRAINT %I',
        constraint_name
      );
    END LOOP;
    EXECUTE $migration$
      UPDATE feature_clock.daily_channel_plans
      SET run_about=run_about OR run_profile
      WHERE run_profile
    $migration$;
    EXECUTE 'ALTER TABLE feature_clock.daily_channel_plans DROP COLUMN run_profile';
  END IF;
END
$$;

ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_plan_mode_check;
ALTER TABLE feature_clock.daily_channel_plans
ADD CONSTRAINT daily_channel_plans_plan_mode_check
CHECK (plan_mode IN ('standard', 'dormant_probe'));

ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_mode_mask_check;
ALTER TABLE feature_clock.daily_channel_plans
ADD CONSTRAINT daily_channel_plans_mode_mask_check
CHECK (
  plan_mode='standard'
  OR (NOT run_about AND run_video AND NOT run_agent)
);

ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_run_mask_check;
ALTER TABLE feature_clock.daily_channel_plans
ADD CONSTRAINT daily_channel_plans_run_mask_check
CHECK (run_about OR run_video OR run_agent);

-- A previous schema revision installed this constraint as NOT VALID.  It still
-- checks every UPDATE, so remove it before backfilling legacy terminal plans
-- whose historical dispatch time may predate the current safe window.
ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_safe_window_check;

UPDATE feature_clock.daily_channel_plans
SET due_day=COALESCE(due_day,(due_at AT TIME ZONE 'UTC')::date,plan_day),
    due_at=COALESCE(due_at,plan_day::timestamp AT TIME ZONE 'UTC'),
    eligible_at=COALESCE(
      eligible_at,
      plan_day::timestamp AT TIME ZONE 'UTC' + interval '30 minutes'
    );

UPDATE feature_clock.daily_channel_plans
SET execution_deadline_at=COALESCE(execution_deadline_at,scheduled_at + interval '1 day')
WHERE execution_deadline_at IS NULL AND scheduled_at IS NOT NULL;

ALTER TABLE feature_clock.daily_channel_plans
ALTER COLUMN due_day SET NOT NULL;
ALTER TABLE feature_clock.daily_channel_plans
ALTER COLUMN due_at SET NOT NULL;
ALTER TABLE feature_clock.daily_channel_plans
ALTER COLUMN eligible_at SET NOT NULL;
ALTER TABLE feature_clock.daily_channel_plans
ALTER COLUMN scheduled_at DROP NOT NULL;
ALTER TABLE feature_clock.daily_channel_plans
ALTER COLUMN execution_deadline_at DROP NOT NULL;

-- A Clock recalculation can make another domain due after the Channel's first
-- Plan has already completed. Preserve every Plan as immutable history while
-- allowing a supplemental Plan for the uncovered domain on the same UTC day.
ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_plan_day_channel_id_key;

CREATE INDEX IF NOT EXISTS idx_feature_clock_daily_plans_channel_day
ON feature_clock.daily_channel_plans (channel_id, plan_day, created_at, plan_id);

DO $$
DECLARE
  constraint_row RECORD;
BEGIN
  FOR constraint_row IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid='feature_clock.daily_channel_plans'::regclass
      AND contype='c'
      AND (
        pg_get_constraintdef(oid) ILIKE '%scheduled_at >= due_at%'
        OR pg_get_constraintdef(oid) ILIKE '%execution_deadline_at > scheduled_at%'
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE feature_clock.daily_channel_plans DROP CONSTRAINT %I',
      constraint_row.conname
    );
  END LOOP;
END
$$;

ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_schedule_check;
ALTER TABLE feature_clock.daily_channel_plans
ADD CONSTRAINT daily_channel_plans_schedule_check
CHECK (
  execution_deadline_at IS NULL
  OR (scheduled_at IS NOT NULL AND execution_deadline_at > scheduled_at)
);

ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_due_day_check;
ALTER TABLE feature_clock.daily_channel_plans
ADD CONSTRAINT daily_channel_plans_due_day_check
CHECK (due_day <= plan_day);

ALTER TABLE feature_clock.daily_channel_plans
ADD CONSTRAINT daily_channel_plans_safe_window_check
CHECK (
  status IN ('succeeded','partial','failed','cancelled')
  OR scheduled_at IS NULL
  OR (
    (scheduled_at AT TIME ZONE 'UTC')::time >= TIME '00:30'
    AND (scheduled_at AT TIME ZONE 'UTC')::time < TIME '21:30'
  )
) NOT VALID;

CREATE INDEX IF NOT EXISTS idx_feature_clock_daily_plans_day_status
ON feature_clock.daily_channel_plans (plan_day, status, dispatch_slot, channel_id);

DROP INDEX IF EXISTS feature_clock.idx_feature_clock_daily_plans_release;
CREATE INDEX idx_feature_clock_daily_plans_release
ON feature_clock.daily_channel_plans (
  status, plan_day, due_day, dispatch_slot, channel_id
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_feature_clock_one_active_plan_per_channel
ON feature_clock.daily_channel_plans (channel_id)
WHERE status IN ('planned','dispatching','dispatched','running');

CREATE TABLE IF NOT EXISTS feature_clock.daily_plan_status_repair_audit (
  audit_id UUID PRIMARY KEY,
  repair_batch_id UUID NOT NULL,
  plan_id UUID NOT NULL
    REFERENCES feature_clock.daily_channel_plans(plan_id),
  plan_day DATE NOT NULL,
  channel_id TEXT NOT NULL,
  previous_status TEXT NOT NULL CHECK (previous_status IN ('failed','partial')),
  previous_error_code TEXT,
  repaired_status TEXT NOT NULL CHECK (repaired_status='succeeded'),
  latest_outcomes_json JSONB NOT NULL,
  confirmation TEXT NOT NULL
    CHECK (confirmation ~ '^sha256:[0-9a-f]{64}$'),
  operator TEXT NOT NULL CHECK (length(btrim(operator))>0),
  reason TEXT NOT NULL CHECK (length(btrim(reason))>0),
  repaired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (repair_batch_id,plan_id)
);

CREATE INDEX IF NOT EXISTS idx_feature_clock_plan_status_repair_plan
ON feature_clock.daily_plan_status_repair_audit (plan_id,repaired_at DESC);

ALTER TABLE feature_clock.daily_channel_plans
DROP CONSTRAINT IF EXISTS daily_channel_plans_capacity_factor_check;

ALTER TABLE feature_clock.daily_channel_plans
ADD CONSTRAINT daily_channel_plans_capacity_factor_check
CHECK (capacity_factor BETWEEN 0 AND 1);

CREATE TABLE IF NOT EXISTS feature_clock.dispatch_outbox (
  dispatch_event_id UUID PRIMARY KEY,
  plan_id UUID NOT NULL UNIQUE
    REFERENCES feature_clock.daily_channel_plans(plan_id) ON DELETE CASCADE,
  job_id TEXT NOT NULL UNIQUE,
  queue_name TEXT NOT NULL,
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
  published_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (queue_name = 'youtube-channel-incremental')
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname='crawler_event_inbox_plan_id_fkey'
      AND conrelid='feature_clock.crawler_event_inbox'::regclass
  ) THEN
    ALTER TABLE feature_clock.crawler_event_inbox
    ADD CONSTRAINT crawler_event_inbox_plan_id_fkey
    FOREIGN KEY (plan_id)
    REFERENCES feature_clock.daily_channel_plans(plan_id)
    ON DELETE SET NULL;
  END IF;
END
$$;

ALTER TABLE feature_clock.dispatch_outbox
ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE feature_clock.dispatch_outbox
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE feature_clock.dispatch_outbox
DROP CONSTRAINT IF EXISTS dispatch_outbox_queue_name_check;

ALTER TABLE feature_clock.dispatch_outbox
ADD CONSTRAINT dispatch_outbox_queue_name_check
CHECK (queue_name = 'youtube-channel-incremental');

CREATE INDEX IF NOT EXISTS idx_feature_clock_dispatch_outbox_publish
ON feature_clock.dispatch_outbox (status, next_attempt_at, lease_expires_at, created_at);

CREATE TABLE IF NOT EXISTS feature_clock.baseline_bundle_manifests (
  baseline_version TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  bundle_format TEXT NOT NULL CHECK (bundle_format = 'crawler-observation-ndjson-v1'),
  source_database TEXT NOT NULL,
  source_schema TEXT NOT NULL CHECK (source_schema = 'crawler'),
  source_snapshot_id TEXT NOT NULL,
  exported_at TIMESTAMPTZ NOT NULL,
  events_file TEXT NOT NULL,
  event_count BIGINT NOT NULL CHECK (event_count > 0),
  channel_count BIGINT NOT NULL CHECK (channel_count > 0),
  byte_count BIGINT NOT NULL CHECK (byte_count > 0),
  events_sha256 TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,
  imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (events_sha256 ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (manifest_sha256 ~ '^sha256:[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS feature_clock.recalculation_runs (
  recalculation_id UUID PRIMARY KEY,
  mode TEXT NOT NULL CHECK (mode IN ('initial_bootstrap', 'policy_rebuild', 'repair')),
  policy_version TEXT NOT NULL REFERENCES feature_clock.rule_policy_definitions(policy_version),
  source_baseline_version TEXT,
  shard_count INTEGER NOT NULL CHECK (shard_count > 0),
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'running', 'succeeded', 'partial', 'failed', 'cancelled')),
  processed_channels BIGINT NOT NULL DEFAULT 0 CHECK (processed_channels >= 0),
  failed_channels BIGINT NOT NULL DEFAULT 0 CHECK (failed_channels >= 0),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  checksum TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_feature_clock_one_unresolved_recalculation
ON feature_clock.recalculation_runs ((1))
WHERE status IN ('pending', 'running', 'partial', 'failed');

CREATE UNIQUE INDEX IF NOT EXISTS ux_feature_clock_initial_bootstrap_baseline
ON feature_clock.recalculation_runs (source_baseline_version)
WHERE mode = 'initial_bootstrap';

ALTER TABLE feature_clock.recalculation_runs
DROP CONSTRAINT IF EXISTS recalculation_runs_bootstrap_source_check;

ALTER TABLE feature_clock.recalculation_runs
ADD CONSTRAINT recalculation_runs_bootstrap_source_check
CHECK (mode <> 'initial_bootstrap' OR source_baseline_version IS NOT NULL);

CREATE TABLE IF NOT EXISTS feature_clock.recalculation_shards (
  recalculation_id UUID NOT NULL
    REFERENCES feature_clock.recalculation_runs(recalculation_id) ON DELETE CASCADE,
  shard_id INTEGER NOT NULL CHECK (shard_id >= 0),
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
  cursor TEXT,
  processed_rows BIGINT NOT NULL DEFAULT 0 CHECK (processed_rows >= 0),
  failed_rows BIGINT NOT NULL DEFAULT 0 CHECK (failed_rows >= 0),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  last_error TEXT,
  checksum TEXT,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (recalculation_id, shard_id)
);

ALTER TABLE feature_clock.recalculation_shards
ADD COLUMN IF NOT EXISTS last_error TEXT;

CREATE TABLE IF NOT EXISTS feature_clock.bootstrap_channel_receipts (
  baseline_version TEXT NOT NULL
    REFERENCES feature_clock.baseline_bundle_manifests(baseline_version),
  channel_id TEXT NOT NULL,
  recalculation_id UUID NOT NULL
    REFERENCES feature_clock.recalculation_runs(recalculation_id),
  channel_clock_version BIGINT NOT NULL CHECK (channel_clock_version > 0),
  checksum TEXT NOT NULL CHECK (checksum ~ '^sha256:[0-9a-f]{64}$'),
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (baseline_version, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_feature_clock_bootstrap_receipts_run
ON feature_clock.bootstrap_channel_receipts (recalculation_id, channel_id);

INSERT INTO feature_clock.rule_policy_definitions (
  policy_version,status,effective_from,allowed_days,
  about_config,discovery_config,recent_sampling_config,
  agent_config,partial_retry_config,checksum,activated_at
) VALUES (
  'v16-rule-1',
  'active',
  '2026-07-20T00:00:00Z',
  ARRAY[1,3,7,14,30,60,90,180,365],
  '{"velocity_ewma_alpha":0.30,"baseline_interval_days":7,"neutral_growth_percentile":0.50,"stable_min_days_for_long_interval":90,"video_delta_full_scale":3}'::jsonb,
  '{"fallback_interval_days":7,"interval_ewma_alpha":0.35,"regularity_threshold":0.65,"silence_decay":0.60}'::jsonb,
  '{"fallback_interval_days":14,"change_ewma_alpha":0.40}'::jsonb,
  '{"baseline_interval_days":90,"bootstrap_min_days":30,"bootstrap_max_days":90,"high_priority_cap_days":90,"version_change_interval_days":14}'::jsonb,
  '{"about_days":3,"discovery_days":3,"recent_sampling_days":7,"agent_days":14}'::jsonb,
  'md5:' || md5('v16-rule-1:2026-07-20'),
  now()
)
ON CONFLICT (policy_version) DO NOTHING;

BEGIN;

INSERT INTO feature_clock.rule_policy_definitions (
  policy_version,status,effective_from,allowed_days,
  about_config,discovery_config,recent_sampling_config,
  agent_config,partial_retry_config,checksum,activated_at
) VALUES (
  'v16-rule-2',
  'draft',
  '2026-07-22T00:00:00Z',
  ARRAY[1,3,7,14,30,60,90,180,365],
  '{"velocity_ewma_alpha":0.30,"baseline_interval_days":7,"neutral_growth_percentile":0.50,"stable_min_days_for_long_interval":90,"video_delta_full_scale":3,"cold_start_priority_floor":0.75,"cold_start_min_recent_video_count":20,"cold_start_max_publish_interval_days":1.5,"cold_start_max_publish_age_days":3.0,"cold_start_min_reliable_intervals":3,"cold_start_min_subscriber_count":5000,"cold_start_min_subscriber_percentile":0.50,"cold_start_min_feature_confidence":0.75}'::jsonb,
  '{"fallback_interval_days":7,"interval_ewma_alpha":0.35,"regularity_threshold":0.65,"silence_decay":0.60,"automatic_min_interval_days":3}'::jsonb,
  '{"fallback_interval_days":14,"change_ewma_alpha":0.40}'::jsonb,
  '{"baseline_interval_days":90,"bootstrap_min_days":30,"bootstrap_max_days":90,"high_priority_cap_days":90,"version_change_interval_days":14}'::jsonb,
  '{"about_days":3,"discovery_days":3,"recent_sampling_days":7,"agent_days":14}'::jsonb,
  'md5:' || md5('v16-rule-2:2026-07-22'),
  NULL
)
ON CONFLICT (policy_version) DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM feature_clock.rule_policy_definitions
    WHERE policy_version='v16-rule-2' AND status='draft'
  ) THEN
    UPDATE feature_clock.rule_policy_definitions
    SET status='retired'
    WHERE status='active' AND policy_version='v16-rule-1';

    UPDATE feature_clock.rule_policy_definitions
    SET status='active',activated_at=now()
    WHERE policy_version='v16-rule-2' AND status='draft';
  END IF;
END
$$;

COMMIT;

BEGIN;

INSERT INTO feature_clock.rule_policy_definitions (
  policy_version,status,effective_from,allowed_days,
  about_config,discovery_config,recent_sampling_config,
  agent_config,partial_retry_config,checksum,activated_at
) VALUES (
  'v16-rule-3',
  'draft',
  '2026-07-22T00:00:00Z',
  ARRAY[1,3,7,14,30,60,90,180,365],
  '{"velocity_ewma_alpha":0.30,"baseline_interval_days":7,"neutral_growth_percentile":0.50,"stable_min_days_for_long_interval":90,"video_delta_full_scale":3,"cold_start_priority_floor":0.75,"cold_start_min_recent_video_count":20,"cold_start_max_publish_interval_days":1.5,"cold_start_max_publish_age_days":3.0,"cold_start_min_reliable_intervals":3,"cold_start_min_subscriber_count":5000,"cold_start_min_subscriber_percentile":0.50,"cold_start_min_feature_confidence":0.75,"dynamic_baseline_enabled":true}'::jsonb,
  '{"fallback_interval_days":7,"interval_ewma_alpha":0.35,"regularity_threshold":0.65,"silence_decay":0.60,"automatic_min_interval_days":3}'::jsonb,
  '{"fallback_interval_days":14,"change_ewma_alpha":0.40}'::jsonb,
  '{"baseline_interval_days":90,"bootstrap_min_days":60,"bootstrap_max_days":90,"high_priority_cap_days":90,"version_change_interval_days":14,"dynamic_baseline_enabled":true}'::jsonb,
  '{"about_days":3,"discovery_days":3,"recent_sampling_days":7,"agent_days":14}'::jsonb,
  'md5:' || md5('v16-rule-3:2026-07-22'),
  NULL
)
ON CONFLICT (policy_version) DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM feature_clock.rule_policy_definitions
    WHERE policy_version='v16-rule-3' AND status='draft'
  ) THEN
    UPDATE feature_clock.rule_policy_definitions
    SET status='retired'
    WHERE status='active' AND policy_version<>'v16-rule-3';

    UPDATE feature_clock.rule_policy_definitions
    SET status='active',activated_at=now()
    WHERE policy_version='v16-rule-3' AND status='draft';
  END IF;
END
$$;

COMMIT;

BEGIN;

INSERT INTO feature_clock.rule_policy_definitions (
  policy_version,status,effective_from,allowed_days,
  about_config,discovery_config,recent_sampling_config,
  agent_config,partial_retry_config,checksum,activated_at
) VALUES (
  'v16-rule-4',
  'draft',
  '2026-07-22T00:00:00Z',
  ARRAY[1,2,3,5,7,14,30,60,90,180,365],
  '{"velocity_ewma_alpha":0.30,"baseline_interval_days":7,"neutral_growth_percentile":0.50,"stable_min_days_for_long_interval":90,"video_delta_full_scale":3,"cold_start_priority_floor":0.75,"cold_start_min_recent_video_count":20,"cold_start_max_publish_interval_days":1.5,"cold_start_max_publish_age_days":7.0,"cold_start_min_reliable_intervals":3,"cold_start_min_subscriber_count":5000,"cold_start_min_subscriber_percentile":0.50,"cold_start_min_feature_confidence":0.75,"dynamic_baseline_enabled":true,"cadence_baseline_enabled":true}'::jsonb,
  '{"fallback_interval_days":7,"interval_ewma_alpha":0.35,"regularity_threshold":0.65,"silence_decay":0.60,"automatic_min_interval_days":3}'::jsonb,
  '{"fallback_interval_days":14,"change_ewma_alpha":0.40}'::jsonb,
  '{"baseline_interval_days":90,"bootstrap_min_days":60,"bootstrap_max_days":90,"high_priority_cap_days":90,"version_change_interval_days":14,"dynamic_baseline_enabled":true}'::jsonb,
  '{"about_days":3,"discovery_days":3,"recent_sampling_days":7,"agent_days":14}'::jsonb,
  'md5:' || md5('v16-rule-4:2026-07-22'),
  NULL
)
ON CONFLICT (policy_version) DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM feature_clock.rule_policy_definitions
    WHERE policy_version='v16-rule-4' AND status='draft'
  ) THEN
    UPDATE feature_clock.rule_policy_definitions
    SET status='retired'
    WHERE status='active' AND policy_version<>'v16-rule-4';

    UPDATE feature_clock.rule_policy_definitions
    SET status='active',activated_at=now()
    WHERE policy_version='v16-rule-4' AND status='draft';
  END IF;
END
$$;

COMMIT;

BEGIN;

INSERT INTO feature_clock.rule_policy_definitions (
  policy_version,status,effective_from,allowed_days,
  about_config,discovery_config,recent_sampling_config,
  agent_config,partial_retry_config,checksum,activated_at
) VALUES (
  'v16-rule-5',
  'draft',
  '2026-07-22T00:00:00Z',
  ARRAY[1,2,3,5,7,14,30,60,90,180,365],
  '{"velocity_ewma_alpha":0.30,"baseline_interval_days":7,"neutral_growth_percentile":0.50,"stable_min_days_for_long_interval":90,"video_delta_full_scale":3,"cold_start_priority_floor":0.75,"cold_start_min_recent_video_count":20,"cold_start_max_publish_interval_days":1.5,"cold_start_max_publish_age_days":7.0,"cold_start_tier_one_max_publish_interval_days":1.0,"cold_start_min_reliable_intervals":3,"cold_start_min_subscriber_count":5000,"cold_start_min_subscriber_percentile":0.50,"cold_start_min_feature_confidence":0.75,"dynamic_baseline_enabled":true,"cadence_baseline_enabled":true}'::jsonb,
  '{"fallback_interval_days":7,"interval_ewma_alpha":0.35,"regularity_threshold":0.65,"silence_decay":0.60,"automatic_min_interval_days":3}'::jsonb,
  '{"fallback_interval_days":14,"change_ewma_alpha":0.40}'::jsonb,
  '{"baseline_interval_days":90,"bootstrap_min_days":60,"bootstrap_max_days":90,"high_priority_cap_days":90,"version_change_interval_days":14,"dynamic_baseline_enabled":true}'::jsonb,
  '{"about_days":3,"discovery_days":3,"recent_sampling_days":7,"agent_days":14}'::jsonb,
  'md5:' || md5('v16-rule-5:2026-07-22'),
  NULL
)
ON CONFLICT (policy_version) DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM feature_clock.rule_policy_definitions
    WHERE policy_version='v16-rule-5' AND status='draft'
  ) THEN
    UPDATE feature_clock.rule_policy_definitions
    SET status='retired'
    WHERE status='active' AND policy_version<>'v16-rule-5';

    UPDATE feature_clock.rule_policy_definitions
    SET status='active',activated_at=now()
    WHERE policy_version='v16-rule-5' AND status='draft';
  END IF;
END
$$;

COMMIT;

BEGIN;

INSERT INTO feature_clock.rule_policy_definitions (
  policy_version,status,effective_from,allowed_days,
  about_config,discovery_config,recent_sampling_config,
  agent_config,partial_retry_config,checksum,activated_at
) VALUES (
  'v16-rule-6',
  'draft',
  '2026-07-22T00:00:00Z',
  ARRAY[1,2,3,5,7,14,30,60,90,180,365],
  '{"velocity_ewma_alpha":0.30,"baseline_interval_days":7,"neutral_growth_percentile":0.50,"stable_min_days_for_long_interval":90,"video_delta_full_scale":3,"cold_start_priority_floor":0.75,"cold_start_min_recent_video_count":20,"cold_start_max_publish_interval_days":1.5,"cold_start_max_publish_age_days":7.0,"cold_start_tier_one_max_publish_interval_days":1.0,"cold_start_min_reliable_intervals":3,"cold_start_min_subscriber_count":5000,"cold_start_min_subscriber_percentile":0.50,"cold_start_min_feature_confidence":0.75,"dynamic_baseline_enabled":true,"cadence_baseline_enabled":true}'::jsonb,
  '{"fallback_interval_days":7,"interval_ewma_alpha":0.35,"regularity_threshold":0.65,"silence_decay":0.60,"automatic_min_interval_days":3}'::jsonb,
  '{"fallback_interval_days":14,"change_ewma_alpha":0.40}'::jsonb,
  '{"baseline_interval_days":90,"bootstrap_min_days":60,"bootstrap_max_days":90,"high_priority_cap_days":90,"version_change_interval_days":14,"dynamic_baseline_enabled":true}'::jsonb,
  '{"about_days":3,"discovery_days":3,"recent_sampling_days":7,"agent_days":14}'::jsonb,
  'md5:' || md5('v16-rule-6:2026-07-22'),
  NULL
)
ON CONFLICT (policy_version) DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM feature_clock.rule_policy_definitions
    WHERE policy_version='v16-rule-6' AND status='draft'
  ) THEN
    UPDATE feature_clock.rule_policy_definitions
    SET status='retired'
    WHERE status='active' AND policy_version<>'v16-rule-6';

    UPDATE feature_clock.rule_policy_definitions
    SET status='active',activated_at=now()
    WHERE policy_version='v16-rule-6' AND status='draft';
  END IF;
END
$$;

COMMIT;

BEGIN;

INSERT INTO feature_clock.rule_policy_definitions (
  policy_version,status,effective_from,allowed_days,
  about_config,discovery_config,recent_sampling_config,
  agent_config,partial_retry_config,checksum,activated_at
) VALUES (
  'v16-rule-7',
  'draft',
  '2026-08-10T00:00:00Z',
  ARRAY[1,2,3,5,7,14,30,60,90,180,365],
  '{"velocity_ewma_alpha":0.30,"baseline_interval_days":7,"neutral_growth_percentile":0.50,"stable_min_days_for_long_interval":90,"video_delta_full_scale":3,"cold_start_priority_floor":0.75,"cold_start_min_recent_video_count":20,"cold_start_max_publish_interval_days":1.5,"cold_start_max_publish_age_days":7.0,"cold_start_tier_one_max_publish_interval_days":1.0,"cold_start_min_reliable_intervals":3,"cold_start_min_subscriber_count":5000,"cold_start_min_subscriber_percentile":0.50,"cold_start_min_feature_confidence":0.75,"dynamic_baseline_enabled":true,"cadence_baseline_enabled":true}'::jsonb,
  '{"fallback_interval_days":7,"interval_ewma_alpha":0.35,"regularity_threshold":0.65,"silence_decay":0.60,"automatic_min_interval_days":3}'::jsonb,
  '{"fallback_interval_days":14,"change_ewma_alpha":0.40}'::jsonb,
  '{"baseline_interval_days":180,"bootstrap_min_days":60,"bootstrap_max_days":90,"high_priority_cap_days":90,"version_change_interval_days":14,"dynamic_baseline_enabled":true}'::jsonb,
  '{"about_days":3,"discovery_days":3,"recent_sampling_days":7,"agent_days":14}'::jsonb,
  'md5:' || md5('v16-rule-7:2026-08-10'),
  NULL
)
ON CONFLICT (policy_version) DO NOTHING;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM feature_clock.rule_policy_definitions
    WHERE policy_version='v16-rule-7' AND status='draft'
  ) THEN
    UPDATE feature_clock.rule_policy_definitions
    SET status='retired'
    WHERE status='active' AND policy_version<>'v16-rule-7';

    UPDATE feature_clock.rule_policy_definitions
    SET status='active',activated_at=now()
    WHERE policy_version='v16-rule-7' AND status='draft';
  END IF;
END
$$;

COMMIT;
