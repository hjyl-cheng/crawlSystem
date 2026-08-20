--
-- PostgreSQL database dump
--

\restrict O9XdYjR0MYsmpruEeWrhL2VM2gFAmDJn81uhjxTtfGcA0gsksGw98LR0uTxbAb8

-- Dumped from database version 16.14
-- Dumped by pg_dump version 16.14

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: crawler; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA crawler;


--
-- Name: feature_clock; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA feature_clock;


--
-- Name: clock_due_at_in_safe_window(timestamp with time zone); Type: FUNCTION; Schema: feature_clock; Owner: -
--

CREATE FUNCTION feature_clock.clock_due_at_in_safe_window(input_at timestamp with time zone) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT (input_at AT TIME ZONE 'UTC')::time >= TIME '00:30'
     AND (input_at AT TIME ZONE 'UTC')::time < TIME '21:30'
$$;


--
-- Name: normalize_clock_due_at(timestamp with time zone); Type: FUNCTION; Schema: feature_clock; Owner: -
--

CREATE FUNCTION feature_clock.normalize_clock_due_at(input_at timestamp with time zone) RETURNS timestamp with time zone
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    AS $$
  SELECT CASE
    WHEN (input_at AT TIME ZONE 'UTC')::time < TIME '00:30'
      THEN input_at + INTERVAL '30 minutes'
    WHEN (input_at AT TIME ZONE 'UTC')::time >= TIME '21:30'
      THEN input_at + INTERVAL '3 hours'
    ELSE input_at
  END
$$;


--
-- Name: protect_active_policy_definition(); Type: FUNCTION; Schema: feature_clock; Owner: -
--

CREATE FUNCTION feature_clock.protect_active_policy_definition() RETURNS trigger
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
    OR NEW.profile_config IS DISTINCT FROM OLD.profile_config
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


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: channels; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channels (
    channel_id text NOT NULL,
    channel_url text,
    title text
);


--
-- Name: crawl_observations; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.crawl_observations (
    observation_id uuid NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    channel_id text NOT NULL,
    observation_kind text NOT NULL,
    kind_sequence bigint NOT NULL,
    plan_id uuid,
    plan_day date,
    trigger_reason text,
    scheduled_at timestamp with time zone,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    outcome text,
    outcome_reason_code text,
    facts_hash text
);


--
-- Name: baseline_bundle_manifests; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.baseline_bundle_manifests (
    baseline_version text NOT NULL,
    schema_version integer NOT NULL,
    bundle_format text NOT NULL,
    source_database text NOT NULL,
    source_schema text NOT NULL,
    source_snapshot_id text NOT NULL,
    exported_at timestamp with time zone NOT NULL,
    events_file text NOT NULL,
    event_count bigint NOT NULL,
    channel_count bigint NOT NULL,
    byte_count bigint NOT NULL,
    events_sha256 text NOT NULL,
    manifest_sha256 text NOT NULL,
    imported_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT baseline_bundle_manifests_bundle_format_check CHECK ((bundle_format = 'crawler-observation-ndjson-v1'::text)),
    CONSTRAINT baseline_bundle_manifests_byte_count_check CHECK ((byte_count > 0)),
    CONSTRAINT baseline_bundle_manifests_channel_count_check CHECK ((channel_count > 0)),
    CONSTRAINT baseline_bundle_manifests_event_count_check CHECK ((event_count > 0)),
    CONSTRAINT baseline_bundle_manifests_events_sha256_check CHECK ((events_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT baseline_bundle_manifests_manifest_sha256_check CHECK ((manifest_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT baseline_bundle_manifests_schema_version_check CHECK ((schema_version = 1)),
    CONSTRAINT baseline_bundle_manifests_source_schema_check CHECK ((source_schema = 'crawler'::text))
);


--
-- Name: bootstrap_channel_receipts; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.bootstrap_channel_receipts (
    baseline_version text NOT NULL,
    channel_id text NOT NULL,
    recalculation_id uuid NOT NULL,
    channel_clock_version bigint NOT NULL,
    checksum text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT bootstrap_channel_receipts_channel_clock_version_check CHECK ((channel_clock_version > 0)),
    CONSTRAINT bootstrap_channel_receipts_checksum_check CHECK ((checksum ~ '^sha256:[0-9a-f]{64}$'::text))
);


--
-- Name: channel_clock_state; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.channel_clock_state (
    channel_id text NOT NULL,
    lifecycle_status text DEFAULT 'active'::text NOT NULL,
    removed_reason text,
    removed_at timestamp with time zone,
    removed_source_event_id uuid,
    dormant_reason text,
    dormant_since timestamp with time zone,
    dormant_recheck_day date,
    dormant_cycle integer DEFAULT 0 NOT NULL,
    dormant_source_event_id uuid,
    profile_due_at timestamp with time zone NOT NULL,
    profile_due_day date NOT NULL,
    profile_tier integer NOT NULL,
    profile_last_complete_at timestamp with time zone,
    about_due_at timestamp with time zone NOT NULL,
    about_due_day date NOT NULL,
    about_tier integer NOT NULL,
    about_last_complete_at timestamp with time zone,
    video_due_at timestamp with time zone NOT NULL,
    video_due_day date NOT NULL,
    video_tier integer NOT NULL,
    video_last_complete_at timestamp with time zone,
    video_last_outcome text,
    agent_due_at timestamp with time zone NOT NULL,
    agent_due_day date NOT NULL,
    agent_tier integer NOT NULL,
    agent_mode text DEFAULT 'basic'::text NOT NULL,
    agent_last_complete_at timestamp with time zone,
    channel_next_run_at timestamp with time zone NOT NULL,
    channel_next_run_day date NOT NULL,
    dispatch_slot integer NOT NULL,
    estimated_request_cost integer DEFAULT 0 NOT NULL,
    policy_version text NOT NULL,
    feature_state_version bigint NOT NULL,
    clock_version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_clock_state_about_tier_check CHECK ((about_tier = ANY (ARRAY[1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]))),
    CONSTRAINT channel_clock_state_agent_tier_check CHECK ((agent_tier = ANY (ARRAY[1, 3, 7, 14, 30, 60, 90, 180, 365]))),
    CONSTRAINT channel_clock_state_clock_version_check CHECK ((clock_version > 0)),
    CONSTRAINT channel_clock_state_dispatch_slot_check CHECK ((dispatch_slot >= 0)),
    CONSTRAINT channel_clock_state_dormant_check CHECK (((lifecycle_status <> 'dormant'::text) OR ((dormant_reason = 'no_published_content_within_90_days'::text) AND (dormant_since IS NOT NULL) AND (dormant_recheck_day IS NOT NULL) AND (dormant_cycle > 0) AND (dormant_source_event_id IS NOT NULL)))),
    CONSTRAINT channel_clock_state_dormant_cycle_check CHECK ((dormant_cycle >= 0)),
    CONSTRAINT channel_clock_state_estimated_request_cost_check CHECK ((estimated_request_cost >= 0)),
    CONSTRAINT channel_clock_state_feature_state_version_check CHECK ((feature_state_version >= 0)),
    CONSTRAINT channel_clock_state_lifecycle_status_check CHECK ((lifecycle_status = ANY (ARRAY['active'::text, 'dormant'::text, 'removed'::text]))),
    CONSTRAINT channel_clock_state_next_run_at_check CHECK ((channel_next_run_at = LEAST(about_due_at, video_due_at, agent_due_at))),
    CONSTRAINT channel_clock_state_next_run_day_check CHECK ((channel_next_run_day = LEAST(about_due_day, video_due_day, agent_due_day))),
    CONSTRAINT channel_clock_state_profile_tier_check CHECK ((profile_tier = ANY (ARRAY[1, 3, 7, 14, 30, 60, 90, 180, 365]))),
    CONSTRAINT channel_clock_state_removed_check CHECK (((lifecycle_status <> 'removed'::text) OR ((removed_reason IS NOT NULL) AND (removed_at IS NOT NULL) AND (removed_source_event_id IS NOT NULL)))),
    CONSTRAINT channel_clock_state_video_last_outcome_check CHECK (((video_last_outcome IS NULL) OR (video_last_outcome = ANY (ARRAY['complete'::text, 'partial'::text])))),
    CONSTRAINT channel_clock_state_video_tier_check CHECK ((video_tier = ANY (ARRAY[1, 3, 7, 14, 30, 60, 90, 180, 365])))
);


--
-- Name: channel_feature_state; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.channel_feature_state (
    channel_id text NOT NULL,
    last_subscriber_count bigint,
    last_subscriber_observed_at timestamp with time zone,
    last_total_view_count bigint,
    last_total_view_observed_at timestamp with time zone,
    last_total_video_count bigint,
    last_total_video_observed_at timestamp with time zone,
    last_about_observed_at timestamp with time zone,
    about_metric_confidence double precision,
    subscriber_velocity_ewma double precision,
    view_velocity_ewma double precision,
    video_count_delta bigint,
    subscriber_size_percentile double precision,
    subscriber_growth_percentile double precision,
    view_growth_percentile double precision,
    growth_momentum double precision,
    about_stable_since timestamp with time zone,
    about_stable_runs integer DEFAULT 0 NOT NULL,
    profile_field_hashes jsonb DEFAULT '{}'::jsonb NOT NULL,
    profile_change_score double precision,
    profile_major_change boolean DEFAULT false NOT NULL,
    profile_change_ewma double precision,
    profile_stable_runs integer DEFAULT 0 NOT NULL,
    last_profile_observed_at timestamp with time zone,
    recent_publish_interval_days double precision[] DEFAULT '{}'::double precision[] NOT NULL,
    publish_interval_ewma double precision,
    publish_interval_median double precision,
    publish_interval_mad double precision,
    publish_regularity double precision,
    last_publish_at timestamp with time zone,
    recent30_video_count integer,
    new_video_empty_runs integer DEFAULT 0 NOT NULL,
    last_discovery_observed_at timestamp with time zone,
    last_complete_discovery_at timestamp with time zone,
    recent_stale_ratio double precision,
    recent_view_change_ewma double precision,
    recent_engagement_change_ewma double precision,
    recent_upload_change_ewma double precision,
    recent_change_probability double precision,
    recent_sampling_stable_runs integer DEFAULT 0 NOT NULL,
    last_recent_sampling_at timestamp with time zone,
    last_recent_sample_count integer,
    current_topic_vector real[] DEFAULT '{}'::real[] NOT NULL,
    current_topic_tokens text[] DEFAULT '{}'::text[] NOT NULL,
    current_agent_output_hash text,
    current_agent_evidence_fingerprints text[] DEFAULT '{}'::text[] NOT NULL,
    current_agent_version_hash text,
    last_agent_evidence_count integer,
    topic_drift double precision,
    evidence_replacement double precision,
    recent_content_shift double precision,
    agent_version_changed boolean DEFAULT false NOT NULL,
    agent_output_changed boolean DEFAULT false NOT NULL,
    agent_change_score double precision,
    agent_topic_vector_source text,
    agent_confidence double precision,
    agent_stable_runs integer DEFAULT 0 NOT NULL,
    last_agent_observed_at timestamp with time zone,
    user_query_demand double precision DEFAULT 0.0 NOT NULL,
    data_incompleteness double precision DEFAULT 1.0 NOT NULL,
    manual_priority double precision DEFAULT 0.0 NOT NULL,
    collection_priority double precision DEFAULT 0.346875 NOT NULL,
    channel_activity double precision,
    feature_confidence double precision DEFAULT 0.0 NOT NULL,
    fallback_reason_codes text[] DEFAULT '{}'::text[] NOT NULL,
    reference_distribution_version text,
    feature_version text DEFAULT 'v16-feature-2'::text NOT NULL,
    state_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_feature_state_about_metric_confidence_check CHECK (((about_metric_confidence IS NULL) OR ((about_metric_confidence >= (0)::double precision) AND (about_metric_confidence <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_about_stable_runs_check CHECK ((about_stable_runs >= 0)),
    CONSTRAINT channel_feature_state_agent_change_score_check CHECK (((agent_change_score IS NULL) OR ((agent_change_score >= (0)::double precision) AND (agent_change_score <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_agent_confidence_check CHECK (((agent_confidence IS NULL) OR ((agent_confidence >= (0)::double precision) AND (agent_confidence <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_agent_stable_runs_check CHECK ((agent_stable_runs >= 0)),
    CONSTRAINT channel_feature_state_channel_activity_check CHECK (((channel_activity IS NULL) OR ((channel_activity >= (0)::double precision) AND (channel_activity <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_collection_priority_check CHECK (((collection_priority >= (0)::double precision) AND (collection_priority <= (1)::double precision))),
    CONSTRAINT channel_feature_state_current_agent_evidence_fingerprints_check CHECK ((cardinality(current_agent_evidence_fingerprints) <= 256)),
    CONSTRAINT channel_feature_state_current_topic_tokens_check CHECK ((cardinality(current_topic_tokens) <= 128)),
    CONSTRAINT channel_feature_state_data_incompleteness_check CHECK (((data_incompleteness >= (0)::double precision) AND (data_incompleteness <= (1)::double precision))),
    CONSTRAINT channel_feature_state_evidence_replacement_check CHECK (((evidence_replacement IS NULL) OR ((evidence_replacement >= (0)::double precision) AND (evidence_replacement <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_feature_confidence_check CHECK (((feature_confidence >= (0)::double precision) AND (feature_confidence <= (1)::double precision))),
    CONSTRAINT channel_feature_state_growth_momentum_check CHECK (((growth_momentum IS NULL) OR ((growth_momentum >= (0)::double precision) AND (growth_momentum <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_last_agent_evidence_count_check CHECK (((last_agent_evidence_count IS NULL) OR (last_agent_evidence_count >= 0))),
    CONSTRAINT channel_feature_state_last_subscriber_count_check CHECK (((last_subscriber_count IS NULL) OR (last_subscriber_count >= 0))),
    CONSTRAINT channel_feature_state_last_total_video_count_check CHECK (((last_total_video_count IS NULL) OR (last_total_video_count >= 0))),
    CONSTRAINT channel_feature_state_last_total_view_count_check CHECK (((last_total_view_count IS NULL) OR (last_total_view_count >= 0))),
    CONSTRAINT channel_feature_state_manual_priority_check CHECK (((manual_priority >= (0)::double precision) AND (manual_priority <= (1)::double precision))),
    CONSTRAINT channel_feature_state_new_video_empty_runs_check CHECK ((new_video_empty_runs >= 0)),
    CONSTRAINT channel_feature_state_profile_stable_runs_check CHECK ((profile_stable_runs >= 0)),
    CONSTRAINT channel_feature_state_publish_regularity_check CHECK (((publish_regularity IS NULL) OR ((publish_regularity >= (0)::double precision) AND (publish_regularity <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_recent_change_probability_check CHECK (((recent_change_probability IS NULL) OR ((recent_change_probability >= (0)::double precision) AND (recent_change_probability <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_recent_content_shift_check CHECK (((recent_content_shift IS NULL) OR ((recent_content_shift >= (0)::double precision) AND (recent_content_shift <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_recent_publish_interval_days_check CHECK ((cardinality(recent_publish_interval_days) <= 32)),
    CONSTRAINT channel_feature_state_recent_sampling_stable_runs_check CHECK ((recent_sampling_stable_runs >= 0)),
    CONSTRAINT channel_feature_state_recent_stale_ratio_check CHECK (((recent_stale_ratio IS NULL) OR ((recent_stale_ratio >= (0)::double precision) AND (recent_stale_ratio <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_state_version_check CHECK ((state_version >= 0)),
    CONSTRAINT channel_feature_state_subscriber_growth_percentile_check CHECK (((subscriber_growth_percentile IS NULL) OR ((subscriber_growth_percentile >= (0)::double precision) AND (subscriber_growth_percentile <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_subscriber_size_percentile_check CHECK (((subscriber_size_percentile IS NULL) OR ((subscriber_size_percentile >= (0)::double precision) AND (subscriber_size_percentile <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_topic_drift_check CHECK (((topic_drift IS NULL) OR ((topic_drift >= (0)::double precision) AND (topic_drift <= (1)::double precision)))),
    CONSTRAINT channel_feature_state_user_query_demand_check CHECK (((user_query_demand >= (0)::double precision) AND (user_query_demand <= (1)::double precision))),
    CONSTRAINT channel_feature_state_view_growth_percentile_check CHECK (((view_growth_percentile IS NULL) OR ((view_growth_percentile >= (0)::double precision) AND (view_growth_percentile <= (1)::double precision))))
);


--
-- Name: channel_observation_checkpoints; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.channel_observation_checkpoints (
    channel_id text NOT NULL,
    observation_kind text NOT NULL,
    last_applied_sequence bigint DEFAULT 0 NOT NULL,
    last_observation_id uuid,
    last_observed_at timestamp with time zone,
    last_payload_hash text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_observation_checkpoints_last_applied_sequence_check CHECK ((last_applied_sequence >= 0)),
    CONSTRAINT channel_observation_checkpoints_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['profile'::text, 'about'::text, 'video'::text, 'agent'::text])))
);


--
-- Name: clock_decision_log; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.clock_decision_log (
    decision_id uuid NOT NULL,
    channel_id text NOT NULL,
    clock_kind text NOT NULL,
    trigger_event_id uuid,
    trigger_observation_id uuid,
    decision_mode text NOT NULL,
    previous_due_at timestamp with time zone,
    previous_due_day date,
    decided_due_at timestamp with time zone NOT NULL,
    decided_due_day date NOT NULL,
    tier integer NOT NULL,
    reason_codes text[] DEFAULT '{}'::text[] NOT NULL,
    feature_state_version bigint NOT NULL,
    feature_summary_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    policy_version text NOT NULL,
    reference_distribution_version text,
    clock_version_before bigint NOT NULL,
    clock_version_after bigint NOT NULL,
    decided_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT clock_decision_log_check CHECK ((clock_version_after > clock_version_before)),
    CONSTRAINT clock_decision_log_clock_kind_check CHECK ((clock_kind = ANY (ARRAY['profile'::text, 'about'::text, 'video'::text, 'agent'::text]))),
    CONSTRAINT clock_decision_log_clock_version_after_check CHECK ((clock_version_after > 0)),
    CONSTRAINT clock_decision_log_clock_version_before_check CHECK ((clock_version_before >= 0)),
    CONSTRAINT clock_decision_log_decision_mode_check CHECK ((decision_mode = ANY (ARRAY['post_run'::text, 'bootstrap'::text, 'policy_rebuild'::text, 'repair'::text]))),
    CONSTRAINT clock_decision_log_feature_state_version_check CHECK ((feature_state_version >= 0)),
    CONSTRAINT clock_decision_log_tier_check CHECK ((tier = ANY (ARRAY[1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365])))
);


--
-- Name: collection_priority_signals; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.collection_priority_signals (
    channel_id text NOT NULL,
    user_query_demand double precision DEFAULT 0.0 NOT NULL,
    manual_priority double precision DEFAULT 0.0 NOT NULL,
    source_version text NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    user_query_demand_expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT collection_priority_signals_check CHECK (((user_query_demand_expires_at IS NULL) OR (user_query_demand_expires_at > observed_at))),
    CONSTRAINT collection_priority_signals_manual_priority_check CHECK (((manual_priority >= (0)::double precision) AND (manual_priority <= (1)::double precision))),
    CONSTRAINT collection_priority_signals_user_query_demand_check CHECK (((user_query_demand >= (0)::double precision) AND (user_query_demand <= (1)::double precision)))
);


--
-- Name: crawler_event_inbox; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.crawler_event_inbox (
    event_id uuid NOT NULL,
    observation_id uuid NOT NULL,
    event_type text NOT NULL,
    event_version integer NOT NULL,
    channel_id text NOT NULL,
    observation_kind text NOT NULL,
    kind_sequence bigint NOT NULL,
    plan_id uuid,
    observed_at timestamp with time zone NOT NULL,
    outcome text NOT NULL,
    payload_hash text NOT NULL,
    status text DEFAULT 'received'::text NOT NULL,
    pending_payload_json jsonb,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    applied_at timestamp with time zone,
    error_code text,
    error_message text,
    CONSTRAINT crawler_event_inbox_check CHECK ((((status = 'waiting_gap'::text) AND (pending_payload_json IS NOT NULL)) OR (status = 'received'::text) OR ((status = ANY (ARRAY['applied'::text, 'rejected'::text])) AND (pending_payload_json IS NULL)))),
    CONSTRAINT crawler_event_inbox_event_type_check CHECK ((event_type = 'crawler.observation.recorded'::text)),
    CONSTRAINT crawler_event_inbox_event_version_check CHECK ((event_version > 0)),
    CONSTRAINT crawler_event_inbox_kind_sequence_check CHECK ((kind_sequence > 0)),
    CONSTRAINT crawler_event_inbox_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['profile'::text, 'about'::text, 'video'::text, 'agent'::text]))),
    CONSTRAINT crawler_event_inbox_outcome_check CHECK ((outcome = ANY (ARRAY['complete'::text, 'partial'::text, 'failed'::text]))),
    CONSTRAINT crawler_event_inbox_status_check CHECK ((status = ANY (ARRAY['received'::text, 'waiting_gap'::text, 'applied'::text, 'rejected'::text])))
);


--
-- Name: daily_channel_plans; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.daily_channel_plans (
    plan_id uuid NOT NULL,
    plan_mode text DEFAULT 'standard'::text NOT NULL,
    plan_day date NOT NULL,
    channel_id text NOT NULL,
    due_day date NOT NULL,
    due_at timestamp with time zone NOT NULL,
    eligible_at timestamp with time zone NOT NULL,
    scheduled_at timestamp with time zone,
    execution_deadline_at timestamp with time zone,
    dispatched_at timestamp with time zone,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    run_profile boolean DEFAULT false NOT NULL,
    run_about boolean NOT NULL,
    run_video boolean NOT NULL,
    run_agent boolean NOT NULL,
    dispatch_slot integer NOT NULL,
    capacity_factor double precision DEFAULT 1.0 NOT NULL,
    player_cap integer DEFAULT 0 NOT NULL,
    next_cap integer DEFAULT 0 NOT NULL,
    estimated_request_cost integer DEFAULT 0 NOT NULL,
    source_clock_version bigint NOT NULL,
    policy_version text NOT NULL,
    planner_config_version text NOT NULL,
    capacity_version text NOT NULL,
    status text DEFAULT 'planned'::text NOT NULL,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    attempts integer DEFAULT 0 NOT NULL,
    error_code text,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT daily_channel_plans_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT daily_channel_plans_capacity_factor_check CHECK (((capacity_factor >= (0)::double precision) AND (capacity_factor <= (1)::double precision))),
    CONSTRAINT daily_channel_plans_check CHECK ((run_profile OR run_about OR run_video OR run_agent)),
    CONSTRAINT daily_channel_plans_check1 CHECK ((due_day <= plan_day)),
    CONSTRAINT daily_channel_plans_dispatch_slot_check CHECK ((dispatch_slot >= 0)),
    CONSTRAINT daily_channel_plans_due_day_check CHECK ((due_day <= plan_day)),
    CONSTRAINT daily_channel_plans_estimated_request_cost_check CHECK ((estimated_request_cost >= 0)),
    CONSTRAINT daily_channel_plans_mode_mask_check CHECK (((plan_mode = 'standard'::text) OR ((NOT run_about) AND run_video AND (NOT run_agent)))),
    CONSTRAINT daily_channel_plans_next_cap_check CHECK ((next_cap >= 0)),
    CONSTRAINT daily_channel_plans_plan_mode_check CHECK ((plan_mode = ANY (ARRAY['standard'::text, 'dormant_probe'::text]))),
    CONSTRAINT daily_channel_plans_player_cap_check CHECK ((player_cap >= 0)),
    CONSTRAINT daily_channel_plans_schedule_check CHECK (((execution_deadline_at IS NULL) OR ((scheduled_at IS NOT NULL) AND (execution_deadline_at > scheduled_at)))),
    CONSTRAINT daily_channel_plans_source_clock_version_check CHECK ((source_clock_version > 0)),
    CONSTRAINT daily_channel_plans_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'dispatching'::text, 'dispatched'::text, 'running'::text, 'succeeded'::text, 'partial'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: dispatch_outbox; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.dispatch_outbox (
    dispatch_event_id uuid NOT NULL,
    plan_id uuid NOT NULL,
    job_id text NOT NULL,
    queue_name text NOT NULL,
    payload_json jsonb NOT NULL,
    payload_hash text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT dispatch_outbox_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT dispatch_outbox_queue_name_check CHECK ((queue_name = 'youtube-channel-incremental'::text)),
    CONSTRAINT dispatch_outbox_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'publishing'::text, 'published'::text, 'dead_letter'::text])))
);


--
-- Name: feature_reference_distributions; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.feature_reference_distributions (
    as_of_day date NOT NULL,
    cohort_key text NOT NULL,
    feature_name text NOT NULL,
    sample_count bigint NOT NULL,
    quantiles jsonb NOT NULL,
    method_version text NOT NULL,
    checksum text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT feature_reference_distributions_sample_count_check CHECK ((sample_count >= 0))
);


--
-- Name: recalculation_runs; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.recalculation_runs (
    recalculation_id uuid NOT NULL,
    mode text NOT NULL,
    policy_version text NOT NULL,
    source_baseline_version text,
    shard_count integer NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    processed_channels bigint DEFAULT 0 NOT NULL,
    failed_channels bigint DEFAULT 0 NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    checksum text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recalculation_runs_bootstrap_source_check CHECK (((mode <> 'initial_bootstrap'::text) OR (source_baseline_version IS NOT NULL))),
    CONSTRAINT recalculation_runs_failed_channels_check CHECK ((failed_channels >= 0)),
    CONSTRAINT recalculation_runs_mode_check CHECK ((mode = ANY (ARRAY['initial_bootstrap'::text, 'policy_rebuild'::text, 'repair'::text]))),
    CONSTRAINT recalculation_runs_processed_channels_check CHECK ((processed_channels >= 0)),
    CONSTRAINT recalculation_runs_shard_count_check CHECK ((shard_count > 0)),
    CONSTRAINT recalculation_runs_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'succeeded'::text, 'partial'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: recalculation_shards; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.recalculation_shards (
    recalculation_id uuid NOT NULL,
    shard_id integer NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    cursor text,
    processed_rows bigint DEFAULT 0 NOT NULL,
    failed_rows bigint DEFAULT 0 NOT NULL,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    last_error text,
    checksum text,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT recalculation_shards_failed_rows_check CHECK ((failed_rows >= 0)),
    CONSTRAINT recalculation_shards_processed_rows_check CHECK ((processed_rows >= 0)),
    CONSTRAINT recalculation_shards_shard_id_check CHECK ((shard_id >= 0)),
    CONSTRAINT recalculation_shards_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: rule_policy_definitions; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.rule_policy_definitions (
    policy_version text NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    allowed_days integer[] NOT NULL,
    profile_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    about_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    discovery_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    recent_sampling_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    agent_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    partial_retry_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    checksum text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    activated_at timestamp with time zone,
    CONSTRAINT rule_policy_definitions_allowed_days_check CHECK ((cardinality(allowed_days) > 0)),
    CONSTRAINT rule_policy_definitions_allowed_days_check1 CHECK ((0 < ALL (allowed_days))),
    CONSTRAINT rule_policy_definitions_status_check CHECK ((status = ANY (ARRAY['draft'::text, 'active'::text, 'retired'::text])))
);


--
-- Name: channels channels_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (channel_id);


--
-- Name: crawl_observations crawl_observations_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observations
    ADD CONSTRAINT crawl_observations_pkey PRIMARY KEY (observation_id);


--
-- Name: baseline_bundle_manifests baseline_bundle_manifests_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.baseline_bundle_manifests
    ADD CONSTRAINT baseline_bundle_manifests_pkey PRIMARY KEY (baseline_version);


--
-- Name: bootstrap_channel_receipts bootstrap_channel_receipts_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.bootstrap_channel_receipts
    ADD CONSTRAINT bootstrap_channel_receipts_pkey PRIMARY KEY (baseline_version, channel_id);


--
-- Name: channel_clock_state channel_clock_state_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.channel_clock_state
    ADD CONSTRAINT channel_clock_state_pkey PRIMARY KEY (channel_id);


--
-- Name: channel_feature_state channel_feature_state_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.channel_feature_state
    ADD CONSTRAINT channel_feature_state_pkey PRIMARY KEY (channel_id);


--
-- Name: channel_observation_checkpoints channel_observation_checkpoints_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.channel_observation_checkpoints
    ADD CONSTRAINT channel_observation_checkpoints_pkey PRIMARY KEY (channel_id, observation_kind);


--
-- Name: clock_decision_log clock_decision_log_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.clock_decision_log
    ADD CONSTRAINT clock_decision_log_pkey PRIMARY KEY (decision_id);


--
-- Name: collection_priority_signals collection_priority_signals_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.collection_priority_signals
    ADD CONSTRAINT collection_priority_signals_pkey PRIMARY KEY (channel_id);


--
-- Name: crawler_event_inbox crawler_event_inbox_channel_id_observation_kind_kind_sequen_key; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.crawler_event_inbox
    ADD CONSTRAINT crawler_event_inbox_channel_id_observation_kind_kind_sequen_key UNIQUE (channel_id, observation_kind, kind_sequence);


--
-- Name: crawler_event_inbox crawler_event_inbox_observation_id_key; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.crawler_event_inbox
    ADD CONSTRAINT crawler_event_inbox_observation_id_key UNIQUE (observation_id);


--
-- Name: crawler_event_inbox crawler_event_inbox_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.crawler_event_inbox
    ADD CONSTRAINT crawler_event_inbox_pkey PRIMARY KEY (event_id);


--
-- Name: daily_channel_plans daily_channel_plans_no_profile_mask; Type: CHECK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE feature_clock.daily_channel_plans
    ADD CONSTRAINT daily_channel_plans_no_profile_mask CHECK ((NOT run_profile)) NOT VALID;


--
-- Name: daily_channel_plans daily_channel_plans_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.daily_channel_plans
    ADD CONSTRAINT daily_channel_plans_pkey PRIMARY KEY (plan_id);


--
-- Name: daily_channel_plans daily_channel_plans_safe_window_check; Type: CHECK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE feature_clock.daily_channel_plans
    ADD CONSTRAINT daily_channel_plans_safe_window_check CHECK (((status = ANY (ARRAY['succeeded'::text, 'partial'::text, 'failed'::text, 'cancelled'::text])) OR (scheduled_at IS NULL) OR ((((scheduled_at AT TIME ZONE 'UTC'::text))::time without time zone >= '00:30:00'::time without time zone) AND (((scheduled_at AT TIME ZONE 'UTC'::text))::time without time zone < '21:30:00'::time without time zone)))) NOT VALID;


--
-- Name: dispatch_outbox dispatch_outbox_job_id_key; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.dispatch_outbox
    ADD CONSTRAINT dispatch_outbox_job_id_key UNIQUE (job_id);


--
-- Name: dispatch_outbox dispatch_outbox_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.dispatch_outbox
    ADD CONSTRAINT dispatch_outbox_pkey PRIMARY KEY (dispatch_event_id);


--
-- Name: dispatch_outbox dispatch_outbox_plan_id_key; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.dispatch_outbox
    ADD CONSTRAINT dispatch_outbox_plan_id_key UNIQUE (plan_id);


--
-- Name: feature_reference_distributions feature_reference_distributions_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.feature_reference_distributions
    ADD CONSTRAINT feature_reference_distributions_pkey PRIMARY KEY (as_of_day, cohort_key, feature_name, method_version);


--
-- Name: recalculation_runs recalculation_runs_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.recalculation_runs
    ADD CONSTRAINT recalculation_runs_pkey PRIMARY KEY (recalculation_id);


--
-- Name: recalculation_shards recalculation_shards_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.recalculation_shards
    ADD CONSTRAINT recalculation_shards_pkey PRIMARY KEY (recalculation_id, shard_id);


--
-- Name: rule_policy_definitions rule_policy_definitions_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.rule_policy_definitions
    ADD CONSTRAINT rule_policy_definitions_pkey PRIMARY KEY (policy_version);


--
-- Name: idx_feature_clock_bootstrap_receipts_run; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_bootstrap_receipts_run ON feature_clock.bootstrap_channel_receipts USING btree (recalculation_id, channel_id);


--
-- Name: idx_feature_clock_channel_next_run; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_channel_next_run ON feature_clock.channel_clock_state USING btree (channel_next_run_day, dispatch_slot, channel_id) WHERE (lifecycle_status = 'active'::text);


--
-- Name: idx_feature_clock_channel_next_run_at; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_channel_next_run_at ON feature_clock.channel_clock_state USING btree (channel_next_run_at, dispatch_slot, channel_id) WHERE (lifecycle_status = 'active'::text);


--
-- Name: idx_feature_clock_daily_plans_channel_day; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_daily_plans_channel_day ON feature_clock.daily_channel_plans USING btree (channel_id, plan_day, created_at, plan_id);


--
-- Name: idx_feature_clock_daily_plans_day_status; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_daily_plans_day_status ON feature_clock.daily_channel_plans USING btree (plan_day, status, dispatch_slot, channel_id);


--
-- Name: idx_feature_clock_daily_plans_release; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_daily_plans_release ON feature_clock.daily_channel_plans USING btree (status, plan_day, due_day, dispatch_slot, channel_id);


--
-- Name: idx_feature_clock_decisions_channel_kind; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_decisions_channel_kind ON feature_clock.clock_decision_log USING btree (channel_id, clock_kind, decided_at DESC);


--
-- Name: idx_feature_clock_decisions_decided_at; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_decisions_decided_at ON feature_clock.clock_decision_log USING btree (decided_at);


--
-- Name: idx_feature_clock_dispatch_outbox_publish; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_dispatch_outbox_publish ON feature_clock.dispatch_outbox USING btree (status, next_attempt_at, lease_expires_at, created_at);


--
-- Name: idx_feature_clock_dormant_recheck; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_dormant_recheck ON feature_clock.channel_clock_state USING btree (dormant_recheck_day, dispatch_slot, channel_id) WHERE (lifecycle_status = 'dormant'::text);


--
-- Name: idx_feature_clock_inbox_plan_kind; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_inbox_plan_kind ON feature_clock.crawler_event_inbox USING btree (plan_id, observation_kind, kind_sequence DESC) WHERE (plan_id IS NOT NULL);


--
-- Name: idx_feature_clock_inbox_received_at; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_inbox_received_at ON feature_clock.crawler_event_inbox USING btree (received_at);


--
-- Name: idx_feature_clock_inbox_status_sequence; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_inbox_status_sequence ON feature_clock.crawler_event_inbox USING btree (status, channel_id, observation_kind, kind_sequence);


--
-- Name: idx_feature_clock_priority_signals_expiry; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_priority_signals_expiry ON feature_clock.collection_priority_signals USING btree (user_query_demand_expires_at) WHERE (user_query_demand_expires_at IS NOT NULL);


--
-- Name: ux_feature_clock_initial_bootstrap_baseline; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE UNIQUE INDEX ux_feature_clock_initial_bootstrap_baseline ON feature_clock.recalculation_runs USING btree (source_baseline_version) WHERE (mode = 'initial_bootstrap'::text);


--
-- Name: ux_feature_clock_one_active_plan_per_channel; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE UNIQUE INDEX ux_feature_clock_one_active_plan_per_channel ON feature_clock.daily_channel_plans USING btree (channel_id) WHERE (status = ANY (ARRAY['planned'::text, 'dispatching'::text, 'dispatched'::text, 'running'::text]));


--
-- Name: ux_feature_clock_one_active_policy; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE UNIQUE INDEX ux_feature_clock_one_active_policy ON feature_clock.rule_policy_definitions USING btree (status) WHERE (status = 'active'::text);


--
-- Name: ux_feature_clock_one_unresolved_recalculation; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE UNIQUE INDEX ux_feature_clock_one_unresolved_recalculation ON feature_clock.recalculation_runs USING btree ((1)) WHERE (status = ANY (ARRAY['pending'::text, 'running'::text, 'partial'::text, 'failed'::text]));


--
-- Name: rule_policy_definitions trg_protect_active_policy_definition; Type: TRIGGER; Schema: feature_clock; Owner: -
--

CREATE TRIGGER trg_protect_active_policy_definition BEFORE UPDATE ON feature_clock.rule_policy_definitions FOR EACH ROW EXECUTE FUNCTION feature_clock.protect_active_policy_definition();


--
-- Name: bootstrap_channel_receipts bootstrap_channel_receipts_baseline_version_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.bootstrap_channel_receipts
    ADD CONSTRAINT bootstrap_channel_receipts_baseline_version_fkey FOREIGN KEY (baseline_version) REFERENCES feature_clock.baseline_bundle_manifests(baseline_version);


--
-- Name: bootstrap_channel_receipts bootstrap_channel_receipts_recalculation_id_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.bootstrap_channel_receipts
    ADD CONSTRAINT bootstrap_channel_receipts_recalculation_id_fkey FOREIGN KEY (recalculation_id) REFERENCES feature_clock.recalculation_runs(recalculation_id);


--
-- Name: channel_clock_state channel_clock_state_policy_version_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.channel_clock_state
    ADD CONSTRAINT channel_clock_state_policy_version_fkey FOREIGN KEY (policy_version) REFERENCES feature_clock.rule_policy_definitions(policy_version);


--
-- Name: clock_decision_log clock_decision_log_policy_version_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.clock_decision_log
    ADD CONSTRAINT clock_decision_log_policy_version_fkey FOREIGN KEY (policy_version) REFERENCES feature_clock.rule_policy_definitions(policy_version);


--
-- Name: crawler_event_inbox crawler_event_inbox_plan_id_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.crawler_event_inbox
    ADD CONSTRAINT crawler_event_inbox_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES feature_clock.daily_channel_plans(plan_id) ON DELETE SET NULL;


--
-- Name: daily_channel_plans daily_channel_plans_policy_version_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.daily_channel_plans
    ADD CONSTRAINT daily_channel_plans_policy_version_fkey FOREIGN KEY (policy_version) REFERENCES feature_clock.rule_policy_definitions(policy_version);


--
-- Name: dispatch_outbox dispatch_outbox_plan_id_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.dispatch_outbox
    ADD CONSTRAINT dispatch_outbox_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES feature_clock.daily_channel_plans(plan_id) ON DELETE CASCADE;


--
-- Name: recalculation_runs recalculation_runs_policy_version_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.recalculation_runs
    ADD CONSTRAINT recalculation_runs_policy_version_fkey FOREIGN KEY (policy_version) REFERENCES feature_clock.rule_policy_definitions(policy_version);


--
-- Name: recalculation_shards recalculation_shards_recalculation_id_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.recalculation_shards
    ADD CONSTRAINT recalculation_shards_recalculation_id_fkey FOREIGN KEY (recalculation_id) REFERENCES feature_clock.recalculation_runs(recalculation_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict O9XdYjR0MYsmpruEeWrhL2VM2gFAmDJn81uhjxTtfGcA0gsksGw98LR0uTxbAb8

