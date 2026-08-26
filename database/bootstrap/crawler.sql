--
-- PostgreSQL database dump
--

\restrict lJLMm7kerNHrnNnKPrSGNhBVfEqa6y3pntHpQL3Og2IziHLUyFiTXlh9XI747T8

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
-- Name: publication; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA publication;


--
-- Name: guard_channel_registry_promotion(); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.guard_channel_registry_promotion() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_channel_registry_promotion_candidate(); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.guard_channel_registry_promotion_candidate() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_channel_registry_promotion_run(); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.guard_channel_registry_promotion_run() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_channel_run_publication_finalize(); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.guard_channel_run_publication_finalize() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_managed_query_page_state(); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.guard_managed_query_page_state() RETURNS trigger
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


--
-- Name: guard_query_quality_chunk_members(); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.guard_query_quality_chunk_members() RETURNS trigger
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


--
-- Name: guard_query_quality_chunk_state(); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.guard_query_quality_chunk_state() RETURNS trigger
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


--
-- Name: registry_promotion_is_complete(text, text); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.registry_promotion_is_complete(target_channel_id text, target_run_id text) RETURNS boolean
    LANGUAGE plpgsql STABLE
    AS $_$
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
$_$;


--
-- Name: registry_publication_gap_repair_is_allowed(text, text, text); Type: FUNCTION; Schema: crawler; Owner: -
--

CREATE FUNCTION crawler.registry_publication_gap_repair_is_allowed(target_channel_id text, target_parent_run_id text, target_repair_run_id text) RETURNS boolean
    LANGUAGE sql STABLE
    AS $_$
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
$_$;


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


--
-- Name: assert_source_writer_version(text); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.assert_source_writer_version(target_channel_id text) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_channel_delivery_lifecycle(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_channel_delivery_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_channel_stream_lifecycle(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_channel_stream_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_outbox_lifecycle(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_outbox_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_revision_immutable(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_revision_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  RAISE EXCEPTION 'Publication Revision rows are immutable'
    USING ERRCODE = '55000';
END
$$;


--
-- Name: guard_source_writer_version(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_source_writer_version() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: guard_stream_lifecycle(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_stream_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: writer_version_satisfies(text, text); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.writer_version_satisfies(actual_version text, minimum_version text) RETURNS boolean
    LANGUAGE plpgsql IMMUTABLE STRICT
    AS $_$
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
$_$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: agent_configs; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.agent_configs (
    config_id bigint NOT NULL,
    name text NOT NULL,
    provider text DEFAULT 'rules'::text NOT NULL,
    model text DEFAULT 'rules-agent-v1'::text NOT NULL,
    endpoint text,
    secret_ref text,
    prompt_template_id bigint,
    batch_size integer DEFAULT 50 NOT NULL,
    min_batch_size integer DEFAULT 20 NOT NULL,
    timeout_ms integer DEFAULT 120000 NOT NULL,
    max_retries integer DEFAULT 2 NOT NULL,
    tools_json jsonb DEFAULT '[{"type": "web_search"}]'::jsonb NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    is_default boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    max_workers integer DEFAULT 1 NOT NULL
);


--
-- Name: agent_configs_config_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.agent_configs_config_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: agent_configs_config_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.agent_configs_config_id_seq OWNED BY crawler.agent_configs.config_id;


--
-- Name: agent_profiles; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.agent_profiles (
    channel_id text NOT NULL,
    agent_mode text DEFAULT 'basic'::text NOT NULL,
    input_url text NOT NULL,
    status text DEFAULT 'success'::text NOT NULL,
    metrics_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    agent_model text,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    agent_config_id bigint,
    prompt_template_id bigint,
    prompt_hash text,
    prompt_variant text DEFAULT 'with_country'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_observation_id uuid,
    last_observed_at timestamp with time zone,
    current_output_hash text,
    input_content_ids text[] DEFAULT '{}'::text[] NOT NULL,
    input_content_hash text,
    taxonomy_version text,
    agent_version_hash text,
    CONSTRAINT agent_profiles_agent_version_hash_check CHECK (((agent_version_hash IS NULL) OR (agent_version_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT agent_profiles_current_output_hash_check CHECK (((current_output_hash IS NULL) OR (current_output_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT agent_profiles_input_content_hash_check CHECK (((input_content_hash IS NULL) OR (input_content_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT agent_profiles_prompt_hash_check CHECK (((prompt_hash IS NULL) OR (prompt_hash ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT agent_profiles_status_check CHECK ((status = ANY (ARRAY['success'::text, 'failed'::text])))
);


--
-- Name: agent_prompt_templates; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.agent_prompt_templates (
    template_id bigint NOT NULL,
    name text NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    template_text text NOT NULL,
    output_schema_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    is_default boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_prompt_templates_status_check CHECK ((status = ANY (ARRAY['draft'::text, 'active'::text, 'archived'::text])))
);


--
-- Name: agent_prompt_templates_template_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.agent_prompt_templates_template_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: agent_prompt_templates_template_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.agent_prompt_templates_template_id_seq OWNED BY crawler.agent_prompt_templates.template_id;


--
-- Name: agent_refresh_requests; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.agent_refresh_requests (
    plan_id uuid NOT NULL,
    plan_day date NOT NULL,
    channel_id text NOT NULL,
    run_id text,
    status text DEFAULT 'pending'::text NOT NULL,
    batch_id text,
    clock_version bigint NOT NULL,
    policy_version text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_retry_at timestamp with time zone DEFAULT now() NOT NULL,
    queued_at timestamp with time zone,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    scheduled_at timestamp with time zone,
    CONSTRAINT agent_refresh_requests_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT agent_refresh_requests_clock_version_check CHECK ((clock_version > 0)),
    CONSTRAINT agent_refresh_requests_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: baseline_export_events; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.baseline_export_events (
    export_id uuid NOT NULL,
    event_id uuid NOT NULL,
    channel_id text NOT NULL,
    observation_kind text NOT NULL,
    kind_sequence bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT baseline_export_events_kind_sequence_check CHECK ((kind_sequence > 0))
);


--
-- Name: baseline_exports; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.baseline_exports (
    baseline_version text NOT NULL,
    export_id uuid NOT NULL,
    as_of_at timestamp with time zone NOT NULL,
    expected_channel_count integer NOT NULL,
    status text DEFAULT 'preparing'::text NOT NULL,
    event_count bigint,
    channel_count bigint,
    byte_count bigint,
    events_sha256 text,
    manifest_sha256 text,
    output_directory text,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT baseline_exports_byte_count_check CHECK (((byte_count IS NULL) OR (byte_count > 0))),
    CONSTRAINT baseline_exports_channel_count_check CHECK (((channel_count IS NULL) OR (channel_count > 0))),
    CONSTRAINT baseline_exports_event_count_check CHECK (((event_count IS NULL) OR (event_count > 0))),
    CONSTRAINT baseline_exports_events_sha256_check CHECK (((events_sha256 IS NULL) OR (events_sha256 ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT baseline_exports_expected_channel_count_check CHECK ((expected_channel_count > 0)),
    CONSTRAINT baseline_exports_manifest_sha256_check CHECK (((manifest_sha256 IS NULL) OR (manifest_sha256 ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT baseline_exports_status_check CHECK ((status = ANY (ARRAY['preparing'::text, 'events_committed'::text, 'ready'::text, 'failed'::text])))
);


--
-- Name: browser_profile_groups; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.browser_profile_groups (
    profile_group_id text NOT NULL,
    proxy_id bigint,
    proxy_address_hash text,
    profile_revision integer DEFAULT 1 NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    language text NOT NULL,
    country text NOT NULL,
    timezone text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_used_at timestamp with time zone,
    retired_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    identity_policy_id text,
    identity_policy_version integer,
    network_identity_key text,
    profile_epoch integer,
    CONSTRAINT browser_profile_groups_identity_shape_check CHECK ((((proxy_id IS NOT NULL) AND (proxy_address_hash IS NOT NULL)) OR ((identity_policy_id IS NOT NULL) AND (identity_policy_version > 0) AND (network_identity_key IS NOT NULL) AND (profile_epoch >= 0)))),
    CONSTRAINT browser_profile_groups_profile_revision_check CHECK ((profile_revision > 0)),
    CONSTRAINT browser_profile_groups_status_check CHECK ((status = ANY (ARRAY['active'::text, 'suspect'::text, 'retired'::text])))
);


--
-- Name: browser_profiles; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.browser_profiles (
    profile_id text NOT NULL,
    profile_group_id text NOT NULL,
    engine text NOT NULL,
    impersonate_target text NOT NULL,
    user_agent text NOT NULL,
    visitor_data text NOT NULL,
    fingerprint_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    cookie_ciphertext text,
    cookie_updated_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT browser_profiles_engine_check CHECK ((engine = ANY (ARRAY['youtubejs_chrome'::text, 'ytdlp_safari'::text])))
);


--
-- Name: business_run_bindings; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.business_run_bindings (
    business_run_key text NOT NULL,
    business_run_id text NOT NULL,
    intent_schema_version integer DEFAULT 1 NOT NULL,
    intent_hash text NOT NULL,
    intent_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    identity_policy_id text NOT NULL,
    identity_policy_version integer NOT NULL,
    identity_policy_hash text NOT NULL,
    run_kind text NOT NULL,
    channel_id text NOT NULL,
    candidate_id bigint,
    plan_id text,
    full_intent_id text,
    status text NOT NULL,
    terminal_reason text,
    materialized_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT business_run_bindings_check CHECK ((((status = 'materialized'::text) AND (materialized_at IS NOT NULL) AND (terminal_reason IS NULL)) OR ((status = 'reserved'::text) AND (materialized_at IS NULL) AND (terminal_reason IS NULL)) OR ((status = 'terminal'::text) AND (terminal_reason IS NOT NULL)))),
    CONSTRAINT business_run_bindings_identity_policy_version_check CHECK ((identity_policy_version > 0)),
    CONSTRAINT business_run_bindings_intent_schema_version_check CHECK ((intent_schema_version > 0)),
    CONSTRAINT business_run_bindings_run_kind_check CHECK ((run_kind = ANY (ARRAY['full'::text, 'incremental'::text, 'full_repair'::text]))),
    CONSTRAINT business_run_bindings_status_check CHECK ((status = ANY (ARRAY['reserved'::text, 'materialized'::text, 'terminal'::text])))
);


--
-- Name: channel_about_metric_snapshots; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channel_about_metric_snapshots (
    observation_id uuid NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    channel_id text NOT NULL,
    subscriber_count bigint,
    total_view_count bigint,
    total_video_count bigint,
    subscriber_count_status text NOT NULL,
    total_view_count_status text NOT NULL,
    total_video_count_status text NOT NULL,
    facts_hash text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    snapshot_origin text DEFAULT 'observation'::text NOT NULL,
    evidence_source text,
    evidence_ref text,
    recovered_at timestamp with time zone,
    CONSTRAINT channel_about_metric_snapshots_check CHECK (((subscriber_count IS NOT NULL) = (subscriber_count_status = ANY (ARRAY['exact'::text, 'estimated'::text])))),
    CONSTRAINT channel_about_metric_snapshots_check1 CHECK (((total_view_count IS NOT NULL) = (total_view_count_status = ANY (ARRAY['exact'::text, 'estimated'::text])))),
    CONSTRAINT channel_about_metric_snapshots_check2 CHECK (((total_video_count IS NOT NULL) = (total_video_count_status = ANY (ARRAY['exact'::text, 'estimated'::text])))),
    CONSTRAINT channel_about_metric_snapshots_check3 CHECK (((subscriber_count IS NOT NULL) OR (total_view_count IS NOT NULL) OR (total_video_count IS NOT NULL))),
    CONSTRAINT channel_about_metric_snapshots_origin_check CHECK ((((snapshot_origin = 'observation'::text) AND (evidence_source IS NULL) AND (evidence_ref IS NULL) AND (recovered_at IS NULL)) OR ((snapshot_origin = 'historical_recovery'::text) AND (NULLIF(btrim(evidence_source), ''::text) IS NOT NULL) AND (NULLIF(btrim(evidence_ref), ''::text) IS NOT NULL) AND (recovered_at IS NOT NULL)))),
    CONSTRAINT channel_about_metric_snapshots_subscriber_count_check CHECK (((subscriber_count IS NULL) OR (subscriber_count >= 0))),
    CONSTRAINT channel_about_metric_snapshots_subscriber_count_status_check CHECK ((subscriber_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT channel_about_metric_snapshots_total_video_count_check CHECK (((total_video_count IS NULL) OR (total_video_count >= 0))),
    CONSTRAINT channel_about_metric_snapshots_total_video_count_status_check CHECK ((total_video_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT channel_about_metric_snapshots_total_view_count_check CHECK (((total_view_count IS NULL) OR (total_view_count >= 0))),
    CONSTRAINT channel_about_metric_snapshots_total_view_count_status_check CHECK ((total_view_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text])))
);


--
-- Name: channel_candidate_sources; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channel_candidate_sources (
    candidate_source_id bigint NOT NULL,
    candidate_id bigint NOT NULL,
    query_id bigint,
    page_id text,
    query_text text,
    rank_position integer,
    discovery_strategy text DEFAULT 'channel_filter'::text NOT NULL,
    source_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: channel_candidate_sources_candidate_source_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.channel_candidate_sources_candidate_source_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_candidate_sources_candidate_source_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.channel_candidate_sources_candidate_source_id_seq OWNED BY crawler.channel_candidate_sources.candidate_source_id;


--
-- Name: channel_candidates; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channel_candidates (
    candidate_id bigint NOT NULL,
    dispatch_batch_id text NOT NULL,
    pipeline_cycle_id text NOT NULL,
    channel_id text NOT NULL,
    channel_url text NOT NULL,
    handle text,
    title text,
    description text,
    avatar_url text,
    search_subscriber_count bigint,
    search_subscriber_count_text text,
    is_verified boolean,
    priority integer DEFAULT 100 NOT NULL,
    status text DEFAULT 'discovered'::text NOT NULL,
    snapshot_attempts integer DEFAULT 0 NOT NULL,
    snapshot_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    source_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    reject_reason text,
    error_message text,
    next_retry_at timestamp with time zone,
    validation_started_at timestamp with time zone,
    validation_finished_at timestamp with time zone,
    accepted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_candidates_status_check CHECK ((status = ANY (ARRAY['discovered'::text, 'queued'::text, 'validating'::text, 'accepted'::text, 'rejected'::text, 'existing'::text, 'failed'::text])))
);


--
-- Name: channel_candidates_candidate_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.channel_candidates_candidate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_candidates_candidate_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.channel_candidates_candidate_id_seq OWNED BY crawler.channel_candidates.candidate_id;


--
-- Name: channel_domain_cursors; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channel_domain_cursors (
    channel_id text NOT NULL,
    observation_kind text NOT NULL,
    latest_sequence bigint DEFAULT 0 NOT NULL,
    latest_observation_id uuid,
    latest_observed_at timestamp with time zone,
    latest_complete_observation_id uuid,
    latest_complete_observed_at timestamp with time zone,
    consecutive_failures integer DEFAULT 0 NOT NULL,
    anchor_video_ids text[] DEFAULT '{}'::text[] NOT NULL,
    source_cursor jsonb DEFAULT '{}'::jsonb NOT NULL,
    current_facts_hash text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_domain_cursors_anchor_video_ids_check CHECK ((cardinality(anchor_video_ids) <= 20)),
    CONSTRAINT channel_domain_cursors_consecutive_failures_check CHECK ((consecutive_failures >= 0)),
    CONSTRAINT channel_domain_cursors_latest_sequence_check CHECK ((latest_sequence >= 0))
);


--
-- Name: channel_execution_attempts; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channel_execution_attempts (
    attempt_id text NOT NULL,
    channel_id text NOT NULL,
    run_id text,
    queue_name text NOT NULL,
    job_id text,
    job_attempt integer DEFAULT 0 NOT NULL,
    worker_id text NOT NULL,
    slot_name text NOT NULL,
    proxy_user text NOT NULL,
    proxy_id bigint,
    proxy_address_hash text,
    profile_group_id text NOT NULL,
    profile_revision integer NOT NULL,
    youtubejs_profile_id text,
    ytdlp_profile_id text,
    status text DEFAULT 'running'::text NOT NULL,
    identity_changed boolean DEFAULT false NOT NULL,
    error_class text,
    error_message text,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    workload_scope text,
    worker_instance_id text,
    business_run_id text,
    attempt_number integer,
    task_id text,
    route_generation bigint,
    network_identity_key text,
    identity_policy_id text,
    identity_policy_version integer,
    CONSTRAINT channel_execution_attempts_status_check CHECK ((status = ANY (ARRAY['running'::text, 'success'::text, 'failed'::text, 'aborted'::text])))
);


--
-- Name: channel_runs; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channel_runs (
    run_id text NOT NULL,
    channel_id text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    crawl_mode text DEFAULT 'full'::text NOT NULL,
    flow_job_id text,
    profile_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    content_limit integer DEFAULT 30 NOT NULL,
    expected_content_count integer DEFAULT 0 NOT NULL,
    detail_status text DEFAULT 'pending'::text NOT NULL,
    candidate_id bigint,
    plan_id uuid,
    plan_day date,
    trigger_reason text,
    task_mask jsonb DEFAULT '{}'::jsonb NOT NULL,
    scheduled_at timestamp with time zone,
    clock_version bigint,
    policy_version text,
    planner_config_version text,
    capacity_version text,
    crawler_version text,
    publication_finalized_status text,
    publication_finalized_at timestamp with time zone,
    identity_policy_id text,
    identity_policy_version integer,
    identity_policy_hash text,
    CONSTRAINT channel_runs_crawl_mode_check CHECK ((crawl_mode = ANY (ARRAY['full'::text, 'incremental'::text]))),
    CONSTRAINT channel_runs_detail_status_check CHECK ((detail_status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'api_pending'::text, 'done'::text, 'failed'::text]))),
    CONSTRAINT channel_runs_publication_finalize_check CHECK ((((publication_finalized_status IS NULL) AND (publication_finalized_at IS NULL)) OR ((publication_finalized_status = ANY (ARRAY['ready_auto'::text, 'ready_partial'::text])) AND (publication_finalized_at IS NOT NULL)))),
    CONSTRAINT channel_runs_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'waiting_pages'::text, 'waiting_detail'::text, 'waiting_agent'::text, 'finalizing'::text, 'done'::text, 'failed'::text, 'skipped'::text]))),
    CONSTRAINT channel_runs_trigger_reason_check CHECK (((trigger_reason IS NULL) OR (trigger_reason = ANY (ARRAY['initial_full'::text, 'clock_due'::text, 'retry'::text, 'manual'::text, 'repair'::text, 'migration_baseline'::text]))))
);


--
-- Name: channel_tab_pages; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channel_tab_pages (
    run_id text NOT NULL,
    channel_id text NOT NULL,
    tab text NOT NULL,
    page_no integer NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    continuation_token text,
    item_count integer DEFAULT 0 NOT NULL,
    recent_cutoff_hit boolean DEFAULT false NOT NULL,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_tab_pages_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'skipped'::text]))),
    CONSTRAINT channel_tab_pages_tab_check CHECK ((tab = ANY (ARRAY['videos'::text, 'shorts'::text, 'lives'::text])))
);


--
-- Name: channels; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.channels (
    channel_id text NOT NULL,
    channel_url text NOT NULL,
    handle text,
    title text,
    subscriber_count bigint,
    subscriber_count_text text,
    status text DEFAULT 'active'::text NOT NULL,
    reject_reason text,
    priority integer DEFAULT 100 NOT NULL,
    ready_for_agent boolean DEFAULT false NOT NULL,
    agent_status text DEFAULT 'pending'::text NOT NULL,
    latest_run_id text,
    source_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    country text,
    country_source text,
    agent_attempts integer DEFAULT 0 NOT NULL,
    agent_next_retry_at timestamp with time zone,
    agent_error_message text,
    country_code text,
    country_canonical_name text,
    avatar_url text,
    summary text,
    keywords text[] DEFAULT '{}'::text[] NOT NULL,
    available_tabs text[] DEFAULT '{}'::text[] NOT NULL,
    about_description text,
    joined_date_text text,
    joined_at date,
    joined_at_precision text DEFAULT 'unknown'::text NOT NULL,
    external_links jsonb DEFAULT '[]'::jsonb NOT NULL,
    subscriber_count_status text DEFAULT 'unresolved'::text NOT NULL,
    subscriber_count_source text,
    subscriber_count_observed_at timestamp with time zone,
    total_view_count bigint,
    total_view_count_text text,
    total_view_count_status text DEFAULT 'unresolved'::text NOT NULL,
    total_view_count_source text,
    total_view_count_observed_at timestamp with time zone,
    total_video_count bigint,
    total_video_count_text text,
    total_video_count_status text DEFAULT 'unresolved'::text NOT NULL,
    total_video_count_source text,
    total_video_count_observed_at timestamp with time zone,
    about_last_observed_at timestamp with time zone,
    about_current_hash text,
    removed_reason text,
    removed_at timestamp with time zone,
    removed_source text,
    removed_evidence text,
    dormant_reason text,
    dormant_since timestamp with time zone,
    dormant_recheck_day date,
    dormant_last_probe_at timestamp with time zone,
    dormant_cycle integer DEFAULT 0 NOT NULL,
    keywords_status text DEFAULT 'unresolved'::text NOT NULL,
    available_tabs_status text DEFAULT 'unresolved'::text NOT NULL,
    description_status text DEFAULT 'unresolved'::text NOT NULL,
    external_links_status text DEFAULT 'unresolved'::text NOT NULL,
    rss_url text,
    vanity_channel_url text,
    is_family_safe boolean,
    is_verified boolean,
    is_verified_status text DEFAULT 'unknown'::text NOT NULL,
    about_identity_last_observed_at timestamp with time zone,
    about_identity_current_hash text,
    registry_promotion_candidate_id bigint,
    registry_promotion_run_id text,
    youtube_business_email_available boolean,
    youtube_business_email_observed_at timestamp with time zone,
    CONSTRAINT channels_agent_base_info_check CHECK (((NOT ready_for_agent) OR ((subscriber_count IS NOT NULL) AND (NULLIF(btrim(title), ''::text) IS NOT NULL)))),
    CONSTRAINT channels_agent_status_check CHECK ((agent_status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'skipped'::text]))),
    CONSTRAINT channels_available_tabs_status_check CHECK ((available_tabs_status = ANY (ARRAY['observed'::text, 'unresolved'::text]))),
    CONSTRAINT channels_country_code_check CHECK (((country_code IS NULL) OR (country_code ~ '^[A-Z]{2}$'::text))),
    CONSTRAINT channels_description_status_check CHECK ((description_status = ANY (ARRAY['exact'::text, 'empty'::text, 'unresolved'::text]))),
    CONSTRAINT channels_dormant_cycle_check CHECK ((dormant_cycle >= 0)),
    CONSTRAINT channels_dormant_state_check CHECK ((((status = 'dormant'::text) AND (dormant_reason = 'no_published_content_within_90_days'::text) AND (dormant_since IS NOT NULL) AND (dormant_recheck_day IS NOT NULL) AND (dormant_last_probe_at IS NOT NULL) AND (dormant_cycle > 0) AND (reject_reason IS NULL)) OR ((status <> 'dormant'::text) AND (dormant_reason IS NULL) AND (dormant_since IS NULL) AND (dormant_recheck_day IS NULL) AND (dormant_last_probe_at IS NULL) AND (dormant_cycle = 0)))),
    CONSTRAINT channels_external_links_status_check CHECK ((external_links_status = ANY (ARRAY['observed'::text, 'unresolved'::text]))),
    CONSTRAINT channels_joined_at_precision_check CHECK ((joined_at_precision = ANY (ARRAY['date_only'::text, 'unknown'::text]))),
    CONSTRAINT channels_keywords_status_check CHECK ((keywords_status = ANY (ARRAY['observed'::text, 'unresolved'::text]))),
    CONSTRAINT channels_registry_promotion_pair_check CHECK ((((registry_promotion_candidate_id IS NULL) AND (registry_promotion_run_id IS NULL)) OR ((registry_promotion_candidate_id IS NOT NULL) AND (registry_promotion_run_id IS NOT NULL)))),
    CONSTRAINT channels_removed_state_check CHECK (((status <> 'removed'::text) OR ((removed_reason IS NOT NULL) AND (removed_at IS NOT NULL) AND (removed_source IS NOT NULL)))),
    CONSTRAINT channels_status_check CHECK ((status = ANY (ARRAY['active'::text, 'dormant'::text, 'paused'::text, 'archived'::text, 'rejected'::text, 'removed'::text]))),
    CONSTRAINT channels_subscriber_count_status_check CHECK ((subscriber_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT channels_total_video_count_status_check CHECK ((total_video_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT channels_total_view_count_status_check CHECK ((total_view_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT channels_v16_metric_nonnegative_check CHECK ((((subscriber_count IS NULL) OR (subscriber_count >= 0)) AND ((total_view_count IS NULL) OR (total_view_count >= 0)) AND ((total_video_count IS NULL) OR (total_video_count >= 0)))),
    CONSTRAINT channels_verified_current_check CHECK ((((is_verified_status = 'verified'::text) AND (is_verified IS TRUE)) OR ((is_verified_status = 'not_verified'::text) AND (is_verified IS FALSE)) OR ((is_verified_status = 'unknown'::text) AND (is_verified IS NULL)))),
    CONSTRAINT channels_youtube_about_country_check CHECK (((country_source IS DISTINCT FROM 'youtube_about'::text) OR (NULLIF(btrim(country), ''::text) IS NOT NULL))),
    CONSTRAINT channels_youtube_business_email_shape CHECK (((youtube_business_email_available IS NULL) = (youtube_business_email_observed_at IS NULL)))
);


--
-- Name: COLUMN channels.youtube_business_email_available; Type: COMMENT; Schema: crawler; Owner: -
--

COMMENT ON COLUMN crawler.channels.youtube_business_email_available IS 'True/false only after a successful recognized YouTube About observation; null means unknown.';


--
-- Name: COLUMN channels.youtube_business_email_observed_at; Type: COMMENT; Schema: crawler; Owner: -
--

COMMENT ON COLUMN crawler.channels.youtube_business_email_observed_at IS 'UTC time of the successful About observation that established the current availability value.';


--
-- Name: content_candidates; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.content_candidates (
    candidate_id bigint NOT NULL,
    run_id text NOT NULL,
    channel_id text NOT NULL,
    source_content_id text NOT NULL,
    "position" integer NOT NULL,
    title text,
    source_url text,
    thumbnail_url text,
    content_type text,
    type_status text DEFAULT 'unresolved'::text NOT NULL,
    type_source text,
    detail_status text DEFAULT 'queued'::text NOT NULL,
    api_status text DEFAULT 'not_needed'::text NOT NULL,
    missing_fields text[] DEFAULT '{}'::text[] NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    content_key text,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    disposition text,
    next_attempt_at timestamp with time zone,
    first_seen_ledger_status text DEFAULT 'not_applicable'::text NOT NULL,
    first_seen_ledger_observation_id uuid,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT content_candidates_api_status_check CHECK ((api_status = ANY (ARRAY['not_needed'::text, 'pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'unavailable'::text]))),
    CONSTRAINT content_candidates_content_type_check CHECK ((content_type = ANY (ARRAY['video'::text, 'short'::text, 'live'::text]))),
    CONSTRAINT content_candidates_detail_status_check CHECK ((detail_status = ANY (ARRAY['queued'::text, 'running'::text, 'api_pending'::text, 'done'::text, 'unavailable'::text, 'failed'::text]))),
    CONSTRAINT content_candidates_disposition_kind_check CHECK ((disposition = ANY (ARRAY['stored'::text, 'deferred'::text, 'terminal_excluded'::text]))),
    CONSTRAINT content_candidates_disposition_schedule_check CHECK ((((disposition IS NULL) AND (next_attempt_at IS NULL)) OR ((disposition = 'stored'::text) AND (next_attempt_at IS NULL)) OR ((disposition = ANY (ARRAY['deferred'::text, 'terminal_excluded'::text])) AND (next_attempt_at IS NOT NULL)))),
    CONSTRAINT content_candidates_first_seen_ledger_shape_check CHECK ((((first_seen_ledger_status = 'not_applicable'::text) AND (first_seen_ledger_observation_id IS NULL)) OR ((first_seen_ledger_status = 'pending'::text) AND (first_seen_ledger_observation_id IS NULL)) OR ((first_seen_ledger_status = 'consumed'::text) AND (first_seen_ledger_observation_id IS NOT NULL)))),
    CONSTRAINT content_candidates_type_status_check CHECK ((type_status = ANY (ARRAY['unresolved'::text, 'resolved'::text, 'unavailable'::text])))
);


--
-- Name: content_candidates_candidate_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.content_candidates_candidate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: content_candidates_candidate_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.content_candidates_candidate_id_seq OWNED BY crawler.content_candidates.candidate_id;


--
-- Name: content_enrich_tasks; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.content_enrich_tasks (
    task_id text NOT NULL,
    content_key text NOT NULL,
    channel_id text NOT NULL,
    job_type text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    requested_by_run_id text,
    requested_observation_id uuid,
    next_retry_at timestamp with time zone,
    last_attempt_at timestamp with time zone,
    last_success_at timestamp with time zone,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    dispatch_generation bigint DEFAULT 0 NOT NULL,
    CONSTRAINT content_enrich_tasks_dispatch_generation_check CHECK ((dispatch_generation >= 0)),
    CONSTRAINT content_enrich_tasks_job_type_check CHECK ((job_type = ANY (ARRAY['date-resolve'::text, 'duration-resolve'::text, 'view-resolve'::text, 'stats-resolve'::text, 'player-refresh'::text, 'next-refresh'::text]))),
    CONSTRAINT content_enrich_tasks_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'leased'::text, 'running'::text, 'done'::text, 'failed'::text, 'terminal'::text, 'dead_letter'::text, 'skipped'::text])))
);


--
-- Name: contents; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.contents (
    content_key text NOT NULL,
    channel_id text NOT NULL,
    run_id text,
    content_type text NOT NULL,
    source_content_id text NOT NULL,
    title text,
    url text,
    thumbnail_url text,
    published_text_raw text,
    published_at timestamp with time zone,
    published_at_status text DEFAULT 'unresolved'::text NOT NULL,
    published_at_source text,
    length_text text,
    view_count_text text,
    like_count bigint,
    comment_count bigint,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_enriched_at timestamp with time zone,
    is_recent boolean DEFAULT true NOT NULL,
    view_count_status text DEFAULT 'unresolved'::text NOT NULL,
    like_count_status text DEFAULT 'unresolved'::text NOT NULL,
    is_members_only boolean DEFAULT false NOT NULL,
    access_status text DEFAULT 'unknown'::text NOT NULL,
    access_status_source text,
    content_type_source text,
    "position" integer,
    duration_seconds integer,
    duration_status text DEFAULT 'unresolved'::text NOT NULL,
    comment_count_status text DEFAULT 'unresolved'::text NOT NULL,
    comments_disabled boolean,
    published_at_precision text DEFAULT 'unknown'::text NOT NULL,
    duration_source text,
    view_count_source text,
    like_count_source text,
    comment_count_source text,
    live_scheduled_at timestamp with time zone,
    live_started_at timestamp with time zone,
    live_ended_at timestamp with time zone,
    extractor_version text,
    description text,
    description_status text DEFAULT 'unresolved'::text NOT NULL,
    description_source text,
    hashtags text[] DEFAULT '{}'::text[] NOT NULL,
    keywords text[] DEFAULT '{}'::text[] NOT NULL,
    view_count bigint,
    playlist_last_seen_at timestamp with time zone,
    player_last_observed_at timestamp with time zone,
    next_last_observed_at timestamp with time zone,
    player_current_hash text,
    next_current_hash text,
    video_change_probability double precision,
    last_observation_id uuid,
    publication_item_hash text,
    comments_first_page jsonb,
    CONSTRAINT contents_access_status_check CHECK ((access_status = ANY (ARRAY['public'::text, 'unlisted'::text, 'members_only'::text, 'private'::text, 'unavailable'::text, 'login_required'::text, 'unknown'::text]))),
    CONSTRAINT contents_comment_count_status_check CHECK ((comment_count_status = ANY (ARRAY['exact'::text, 'zero_from_empty'::text, 'zero_from_surface'::text, 'zero_from_upcoming'::text, 'disabled'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT contents_comment_state_shape CHECK ((((comments_disabled IS TRUE) AND (comment_count = 0) AND (comment_count_status = 'disabled'::text)) OR ((comments_disabled IS DISTINCT FROM TRUE) AND (comment_count_status <> 'disabled'::text)))),
    CONSTRAINT contents_comments_first_page_shape_check CHECK (((comments_first_page IS NULL) OR ((jsonb_typeof(comments_first_page) = 'object'::text) AND ((comments_first_page ->> 'version'::text) = '1'::text) AND ((comments_first_page ->> 'sort'::text) = 'TOP_COMMENTS'::text) AND (jsonb_typeof((comments_first_page -> 'comments'::text)) = 'array'::text) AND ((comments_first_page ->> 'returned_count'::text) ~ '^[0-9]+$'::text) AND (((comments_first_page ->> 'returned_count'::text))::integer = jsonb_array_length((comments_first_page -> 'comments'::text)))))),
    CONSTRAINT contents_content_type_check CHECK ((content_type = ANY (ARRAY['video'::text, 'short'::text, 'live'::text, 'post'::text]))),
    CONSTRAINT contents_description_status_check CHECK ((description_status = ANY (ARRAY['exact'::text, 'empty'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT contents_publication_item_hash_check CHECK (((publication_item_hash IS NULL) OR (publication_item_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT contents_published_at_precision_check CHECK ((published_at_precision = ANY (ARRAY['second'::text, 'date_only'::text, 'unknown'::text]))),
    CONSTRAINT contents_published_at_status_check CHECK ((published_at_status = ANY (ARRAY['exact'::text, 'relative'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT contents_video_change_probability_check CHECK (((video_change_probability IS NULL) OR ((video_change_probability >= (0)::double precision) AND (video_change_probability <= (1)::double precision)))),
    CONSTRAINT contents_view_count_nonnegative_check CHECK (((view_count IS NULL) OR (view_count >= 0)))
);


--
-- Name: controller_ticks; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.controller_ticks (
    tick_id bigint NOT NULL,
    status text NOT NULL,
    queues_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    actions_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: controller_ticks_tick_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.controller_ticks_tick_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: controller_ticks_tick_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.controller_ticks_tick_id_seq OWNED BY crawler.controller_ticks.tick_id;


--
-- Name: crawl_observation_keys; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.crawl_observation_keys (
    idempotency_key text NOT NULL,
    observation_id uuid NOT NULL,
    command_hash text NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    channel_id text NOT NULL,
    observation_kind text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT crawl_observation_keys_check CHECK ((expires_at > created_at))
);


--
-- Name: crawl_observations; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.crawl_observations (
    observation_id uuid NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    channel_id text NOT NULL,
    run_id text,
    observation_kind text NOT NULL,
    kind_sequence bigint NOT NULL,
    plan_id uuid,
    plan_day date,
    trigger_reason text NOT NULL,
    scheduled_at timestamp with time zone,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    outcome text NOT NULL,
    outcome_reason_code text NOT NULL,
    result_summary_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    facts_hash text NOT NULL,
    crawler_version text,
    extractor_versions jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_class text,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT crawl_observations_kind_sequence_check CHECK ((kind_sequence > 0)),
    CONSTRAINT crawl_observations_outcome_check CHECK ((outcome = ANY (ARRAY['complete'::text, 'partial'::text, 'failed'::text]))),
    CONSTRAINT crawl_observations_trigger_reason_check CHECK ((trigger_reason = ANY (ARRAY['initial_full'::text, 'clock_due'::text, 'retry'::text, 'manual'::text, 'repair'::text, 'migration_baseline'::text])))
);


--
-- Name: crawler_outbox; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.crawler_outbox (
    event_id uuid NOT NULL,
    observation_id uuid NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    event_type text DEFAULT 'crawler.observation.recorded'::text NOT NULL,
    event_version integer DEFAULT 1 NOT NULL,
    aggregate_key text NOT NULL,
    kind_sequence bigint NOT NULL,
    payload_json jsonb NOT NULL,
    payload_hash text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    CONSTRAINT crawler_outbox_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT crawler_outbox_event_type_check CHECK ((event_type = 'crawler.observation.recorded'::text)),
    CONSTRAINT crawler_outbox_event_version_check CHECK ((event_version > 0)),
    CONSTRAINT crawler_outbox_kind_sequence_check CHECK ((kind_sequence > 0)),
    CONSTRAINT crawler_outbox_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'publishing'::text, 'published'::text, 'dead_letter'::text])))
);


--
-- Name: finalized_profiles; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.finalized_profiles (
    channel_id text NOT NULL,
    run_id text,
    status text DEFAULT 'pending'::text NOT NULL,
    profile_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    quality_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    finalized_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_observation_id uuid,
    last_observed_at timestamp with time zone,
    current_output_hash text,
    CONSTRAINT finalized_profiles_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'ready_auto'::text, 'ready_partial'::text, 'pending_detail'::text, 'pending_api'::text, 'pending_enrich'::text, 'pending_agent'::text, 'failed'::text])))
);


--
-- Name: observation_raw_objects; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.observation_raw_objects (
    observation_id uuid NOT NULL,
    raw_object_id bigint NOT NULL,
    role text NOT NULL,
    content_hash text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: proxy_job_dispatch_outbox; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.proxy_job_dispatch_outbox (
    dispatch_id text NOT NULL,
    aggregate_kind text NOT NULL,
    aggregate_id text NOT NULL,
    intent_hash text NOT NULL,
    queue_registry_key text NOT NULL,
    deterministic_job_id text NOT NULL,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone,
    last_error text,
    sent_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT proxy_job_dispatch_outbox_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT proxy_job_dispatch_outbox_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'sending'::text, 'sent'::text, 'dead'::text])))
);


--
-- Name: query_dispatch_batches; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_dispatch_batches (
    dispatch_batch_id text NOT NULL,
    pipeline_cycle_id text NOT NULL,
    query_set_id bigint,
    status text DEFAULT 'running'::text NOT NULL,
    query_quality_min_score numeric(5,2) DEFAULT 0 NOT NULL,
    selected_query_ids bigint[] DEFAULT '{}'::bigint[] NOT NULL,
    selected_query_count integer DEFAULT 0 NOT NULL,
    discovered_candidate_count integer DEFAULT 0 NOT NULL,
    accepted_channel_count integer DEFAULT 0 NOT NULL,
    rejected_channel_count integer DEFAULT 0 NOT NULL,
    discovery_closed_at timestamp with time zone,
    validation_closed_at timestamp with time zone,
    agent_tail_flushed_at timestamp with time zone,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT query_dispatch_batches_status_check CHECK ((status = ANY (ARRAY['running'::text, 'discovery_closed'::text, 'validation_closed'::text, 'finishing'::text, 'completed'::text, 'stopped'::text, 'failed'::text])))
);


--
-- Name: query_pages; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_pages (
    page_id text NOT NULL,
    query_id bigint,
    query_text text NOT NULL,
    page_no integer NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    accepted_count integer DEFAULT 0 NOT NULL,
    candidate_count integer DEFAULT 0 NOT NULL,
    unqualified_ratio numeric(6,4),
    should_continue boolean,
    stop_reason text,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    dispatch_batch_id text,
    page_intent_hash text,
    intent_schema_version integer,
    request_language text,
    request_country text,
    identity_policy_id text,
    identity_policy_version integer,
    identity_policy_hash text,
    continuation_parent_page_id text,
    continuation_token_hash text,
    managed_fetch_status text DEFAULT 'pending'::text NOT NULL,
    managed_fetch_started_at timestamp with time zone,
    managed_fetch_finished_at timestamp with time zone,
    managed_fetch_error_code text,
    qualification_status text DEFAULT 'not_required'::text NOT NULL,
    qualification_started_at timestamp with time zone,
    qualification_finished_at timestamp with time zone,
    qualification_error_code text,
    dispatch_status text DEFAULT 'pending'::text NOT NULL,
    dispatch_reason text,
    dispatched_job_id text,
    CONSTRAINT query_pages_dispatch_status_check CHECK ((dispatch_status = ANY (ARRAY['pending'::text, 'deferred'::text, 'enqueued'::text, 'terminal'::text]))),
    CONSTRAINT query_pages_managed_fetch_status_check CHECK ((managed_fetch_status = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text, 'failed'::text]))),
    CONSTRAINT query_pages_qualification_status_check CHECK ((qualification_status = ANY (ARRAY['not_required'::text, 'pending'::text, 'done'::text, 'failed'::text]))),
    CONSTRAINT query_pages_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'skipped'::text])))
);


--
-- Name: query_quality_batches; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_quality_batches (
    quality_batch_id text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    total_count integer DEFAULT 0 NOT NULL,
    processed_count integer DEFAULT 0 NOT NULL,
    scored_count integer DEFAULT 0 NOT NULL,
    fallback_count integer DEFAULT 0 NOT NULL,
    failed_count integer DEFAULT 0 NOT NULL,
    cancelled_count integer DEFAULT 0 NOT NULL,
    options_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT query_quality_batches_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'cancel_requested'::text, 'cancelled'::text, 'done'::text, 'failed'::text])))
);


--
-- Name: query_quality_chunk_members; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_quality_chunk_members (
    quality_batch_id text NOT NULL,
    quality_chunk_id text NOT NULL,
    quality_task_id bigint NOT NULL,
    member_ordinal integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT query_quality_chunk_members_member_ordinal_check CHECK ((member_ordinal > 0))
);


--
-- Name: query_quality_chunks; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_quality_chunks (
    quality_chunk_id text NOT NULL,
    quality_batch_id text NOT NULL,
    chunk_revision integer DEFAULT 1 NOT NULL,
    chunk_intent_hash text NOT NULL,
    intent_schema_version integer DEFAULT 1 NOT NULL,
    effective_language text NOT NULL,
    effective_country text NOT NULL,
    identity_policy_id text NOT NULL,
    identity_policy_version integer NOT NULL,
    identity_policy_hash text NOT NULL,
    scoring_options jsonb DEFAULT '{}'::jsonb NOT NULL,
    scoring_options_hash text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    dispatch_status text DEFAULT 'pending'::text NOT NULL,
    dispatch_reason text,
    dispatched_job_id text,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT query_quality_chunks_chunk_revision_check CHECK ((chunk_revision > 0)),
    CONSTRAINT query_quality_chunks_dispatch_status_check CHECK ((dispatch_status = ANY (ARRAY['pending'::text, 'deferred'::text, 'enqueued'::text, 'terminal'::text]))),
    CONSTRAINT query_quality_chunks_identity_policy_version_check CHECK ((identity_policy_version > 0)),
    CONSTRAINT query_quality_chunks_intent_schema_version_check CHECK ((intent_schema_version > 0)),
    CONSTRAINT query_quality_chunks_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: query_quality_tasks; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_quality_tasks (
    quality_task_id bigint NOT NULL,
    quality_batch_id text NOT NULL,
    query_id bigint NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT query_quality_tasks_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'scored'::text, 'fallback'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: query_quality_tasks_quality_task_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.query_quality_tasks_quality_task_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: query_quality_tasks_quality_task_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.query_quality_tasks_quality_task_id_seq OWNED BY crawler.query_quality_tasks.quality_task_id;


--
-- Name: query_sets; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_sets (
    query_set_id bigint NOT NULL,
    name text NOT NULL,
    description text,
    status text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT query_sets_status_check CHECK ((status = ANY (ARRAY['active'::text, 'paused'::text, 'archived'::text])))
);


--
-- Name: query_sets_query_set_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.query_sets_query_set_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: query_sets_query_set_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.query_sets_query_set_id_seq OWNED BY crawler.query_sets.query_set_id;


--
-- Name: query_terms; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.query_terms (
    query_id bigint NOT NULL,
    query_text text NOT NULL,
    language text,
    country text,
    category text,
    status text DEFAULT 'active'::text NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    next_crawl_at timestamp with time zone DEFAULT now() NOT NULL,
    crawl_interval_sec integer DEFAULT 1296000 NOT NULL,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    query_set_id bigint,
    quality_score numeric(5,2),
    quality_status text DEFAULT 'unscored'::text NOT NULL,
    quality_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    quality_checked_at timestamp with time zone,
    CONSTRAINT query_terms_status_check CHECK ((status = ANY (ARRAY['active'::text, 'paused'::text, 'exhausted'::text, 'archived'::text])))
);


--
-- Name: query_terms_query_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.query_terms_query_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: query_terms_query_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.query_terms_query_id_seq OWNED BY crawler.query_terms.query_id;


--
-- Name: raw_objects; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.raw_objects (
    raw_object_id bigint NOT NULL,
    bucket text NOT NULL,
    object_key text NOT NULL,
    object_path text NOT NULL,
    object_type text NOT NULL,
    entity_type text,
    entity_id text,
    source text,
    content_type text,
    content_hash text,
    size_bytes bigint,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    content_encoding text,
    original_size_bytes bigint,
    stored_size_bytes bigint
);


--
-- Name: raw_objects_raw_object_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.raw_objects_raw_object_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_objects_raw_object_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.raw_objects_raw_object_id_seq OWNED BY crawler.raw_objects.raw_object_id;


--
-- Name: settings; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.settings (
    setting_key text NOT NULL,
    value_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

INSERT INTO crawler.settings (setting_key,value_json) VALUES
    ('content_enrich_dispatch', '{"mode":"clock"}'::jsonb),
    ('content_enrich_dispatch_cursor', '{"channel_id":""}'::jsonb),
    ('content_enrich_dispatch_mutex', '{"owner":null,"expires_at":null}'::jsonb);


--
-- Name: task_events; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.task_events (
    event_id bigint NOT NULL,
    queue_name text NOT NULL,
    job_id text,
    job_name text,
    entity_key text,
    status text NOT NULL,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: task_events_event_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.task_events_event_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_events_event_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.task_events_event_id_seq OWNED BY crawler.task_events.event_id;


--
-- Name: youtube_api_batches; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.youtube_api_batches (
    batch_id text NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    task_ids bigint[] NOT NULL,
    video_ids text[] NOT NULL,
    key_index integer,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT youtube_api_batches_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'done'::text, 'failed'::text])))
);


--
-- Name: youtube_api_daily_usage; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.youtube_api_daily_usage (
    usage_date date NOT NULL,
    request_count integer DEFAULT 0 NOT NULL,
    requested_video_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    requested_channel_count integer DEFAULT 0 NOT NULL,
    CONSTRAINT youtube_api_daily_usage_request_count_check CHECK ((request_count >= 0)),
    CONSTRAINT youtube_api_daily_usage_requested_channel_count_check CHECK ((requested_channel_count >= 0)),
    CONSTRAINT youtube_api_daily_usage_requested_video_count_check CHECK ((requested_video_count >= 0))
);


--
-- Name: youtube_api_tasks; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.youtube_api_tasks (
    task_id bigint NOT NULL,
    source_content_id text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    missing_fields text[] DEFAULT '{}'::text[] NOT NULL,
    candidate_ids bigint[] DEFAULT '{}'::bigint[] NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    next_retry_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT youtube_api_tasks_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'unavailable'::text])))
);


--
-- Name: youtube_api_tasks_task_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.youtube_api_tasks_task_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: youtube_api_tasks_task_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.youtube_api_tasks_task_id_seq OWNED BY crawler.youtube_api_tasks.task_id;


--
-- Name: youtube_channel_api_batches; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.youtube_channel_api_batches (
    batch_id text NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    task_ids bigint[] NOT NULL,
    channel_ids text[] NOT NULL,
    key_index integer,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT youtube_channel_api_batches_status_check CHECK ((status = ANY (ARRAY['running'::text, 'done'::text, 'failed'::text])))
);


--
-- Name: youtube_channel_api_tasks; Type: TABLE; Schema: crawler; Owner: -
--

CREATE TABLE crawler.youtube_channel_api_tasks (
    task_id bigint NOT NULL,
    request_key text NOT NULL,
    channel_id text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    batch_id text,
    attempts integer DEFAULT 0 NOT NULL,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT youtube_channel_api_tasks_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text, 'failed'::text])))
);


--
-- Name: youtube_channel_api_tasks_task_id_seq; Type: SEQUENCE; Schema: crawler; Owner: -
--

CREATE SEQUENCE crawler.youtube_channel_api_tasks_task_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: youtube_channel_api_tasks_task_id_seq; Type: SEQUENCE OWNED BY; Schema: crawler; Owner: -
--

ALTER SEQUENCE crawler.youtube_channel_api_tasks_task_id_seq OWNED BY crawler.youtube_channel_api_tasks.task_id;


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
    about_due_day date NOT NULL,
    about_tier integer NOT NULL,
    about_last_complete_at timestamp with time zone,
    video_due_day date NOT NULL,
    video_tier integer NOT NULL,
    video_last_complete_at timestamp with time zone,
    video_last_outcome text,
    agent_due_day date NOT NULL,
    agent_tier integer NOT NULL,
    agent_mode text DEFAULT 'basic'::text NOT NULL,
    agent_last_complete_at timestamp with time zone,
    channel_next_run_day date NOT NULL,
    dispatch_slot integer NOT NULL,
    estimated_request_cost integer DEFAULT 0 NOT NULL,
    policy_version text NOT NULL,
    feature_state_version bigint NOT NULL,
    clock_version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    about_due_at timestamp with time zone NOT NULL,
    video_due_at timestamp with time zone NOT NULL,
    agent_due_at timestamp with time zone NOT NULL,
    channel_next_run_at timestamp with time zone NOT NULL,
    lifecycle_status text DEFAULT 'active'::text NOT NULL,
    removed_reason text,
    removed_at timestamp with time zone,
    removed_source_event_id uuid,
    dormant_reason text,
    dormant_since timestamp with time zone,
    dormant_recheck_day date,
    dormant_cycle integer DEFAULT 0 NOT NULL,
    dormant_source_event_id uuid,
    CONSTRAINT channel_clock_state_about_tier_check CHECK ((about_tier = ANY (ARRAY[1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]))),
    CONSTRAINT channel_clock_state_agent_tier_check CHECK (((agent_tier = ANY (ARRAY[1, 3, 7, 14, 30])) OR ((agent_tier >= 60) AND (agent_tier <= 365)))),
    CONSTRAINT channel_clock_state_clock_version_check CHECK ((clock_version > 0)),
    CONSTRAINT channel_clock_state_dispatch_slot_check CHECK ((dispatch_slot >= 0)),
    CONSTRAINT channel_clock_state_dormant_check CHECK (((lifecycle_status <> 'dormant'::text) OR ((dormant_reason = 'no_published_content_within_90_days'::text) AND (dormant_since IS NOT NULL) AND (dormant_recheck_day IS NOT NULL) AND (dormant_cycle > 0) AND (dormant_source_event_id IS NOT NULL)))),
    CONSTRAINT channel_clock_state_dormant_cycle_check CHECK ((dormant_cycle >= 0)),
    CONSTRAINT channel_clock_state_estimated_request_cost_check CHECK ((estimated_request_cost >= 0)),
    CONSTRAINT channel_clock_state_feature_state_version_check CHECK ((feature_state_version >= 0)),
    CONSTRAINT channel_clock_state_lifecycle_status_check CHECK ((lifecycle_status = ANY (ARRAY['active'::text, 'dormant'::text, 'removed'::text]))),
    CONSTRAINT channel_clock_state_next_run_at_check CHECK ((channel_next_run_at = LEAST(about_due_at, video_due_at, agent_due_at))),
    CONSTRAINT channel_clock_state_next_run_day_check CHECK ((channel_next_run_day = LEAST(about_due_day, video_due_day, agent_due_day))),
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
    subscriber_velocity_ewma double precision,
    view_velocity_ewma double precision,
    video_count_delta bigint,
    subscriber_size_percentile double precision,
    subscriber_growth_percentile double precision,
    view_growth_percentile double precision,
    growth_momentum double precision,
    about_stable_since timestamp with time zone,
    about_stable_runs integer DEFAULT 0 NOT NULL,
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
    about_metric_confidence double precision,
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
    CONSTRAINT channel_observation_checkpoints_last_applied_sequence_check CHECK ((last_applied_sequence >= 0))
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
    previous_due_day date,
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
    previous_due_at timestamp with time zone,
    decided_due_at timestamp with time zone NOT NULL,
    CONSTRAINT clock_decision_log_check CHECK ((clock_version_after > clock_version_before)),
    CONSTRAINT clock_decision_log_clock_version_after_check CHECK ((clock_version_after > 0)),
    CONSTRAINT clock_decision_log_clock_version_before_check CHECK ((clock_version_before >= 0)),
    CONSTRAINT clock_decision_log_decision_mode_check CHECK ((decision_mode = ANY (ARRAY['post_run'::text, 'bootstrap'::text, 'policy_rebuild'::text, 'repair'::text]))),
    CONSTRAINT clock_decision_log_feature_state_version_check CHECK ((feature_state_version >= 0))
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
    observed_at timestamp with time zone NOT NULL,
    outcome text NOT NULL,
    payload_hash text NOT NULL,
    status text DEFAULT 'received'::text NOT NULL,
    pending_payload_json jsonb,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    applied_at timestamp with time zone,
    error_code text,
    error_message text,
    plan_id uuid,
    CONSTRAINT crawler_event_inbox_check CHECK ((((status = 'waiting_gap'::text) AND (pending_payload_json IS NOT NULL)) OR (status = 'received'::text) OR ((status = ANY (ARRAY['applied'::text, 'rejected'::text])) AND (pending_payload_json IS NULL)))),
    CONSTRAINT crawler_event_inbox_event_type_check CHECK ((event_type = 'crawler.observation.recorded'::text)),
    CONSTRAINT crawler_event_inbox_event_version_check CHECK ((event_version > 0)),
    CONSTRAINT crawler_event_inbox_kind_sequence_check CHECK ((kind_sequence > 0)),
    CONSTRAINT crawler_event_inbox_outcome_check CHECK ((outcome = ANY (ARRAY['complete'::text, 'partial'::text, 'failed'::text]))),
    CONSTRAINT crawler_event_inbox_status_check CHECK ((status = ANY (ARRAY['received'::text, 'waiting_gap'::text, 'applied'::text, 'rejected'::text])))
);


--
-- Name: daily_channel_plans; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.daily_channel_plans (
    plan_id uuid NOT NULL,
    plan_day date NOT NULL,
    channel_id text NOT NULL,
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
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    due_at timestamp with time zone NOT NULL,
    scheduled_at timestamp with time zone,
    execution_deadline_at timestamp with time zone,
    completed_at timestamp with time zone,
    due_day date NOT NULL,
    eligible_at timestamp with time zone NOT NULL,
    dispatched_at timestamp with time zone,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    plan_mode text DEFAULT 'standard'::text NOT NULL,
    CONSTRAINT daily_channel_plans_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT daily_channel_plans_capacity_factor_check CHECK (((capacity_factor >= (0)::double precision) AND (capacity_factor <= (1)::double precision))),
    CONSTRAINT daily_channel_plans_dispatch_slot_check CHECK ((dispatch_slot >= 0)),
    CONSTRAINT daily_channel_plans_due_day_check CHECK ((due_day <= plan_day)),
    CONSTRAINT daily_channel_plans_estimated_request_cost_check CHECK ((estimated_request_cost >= 0)),
    CONSTRAINT daily_channel_plans_mode_mask_check CHECK (((plan_mode = 'standard'::text) OR ((NOT run_about) AND run_video AND (NOT run_agent)))),
    CONSTRAINT daily_channel_plans_next_cap_check CHECK ((next_cap >= 0)),
    CONSTRAINT daily_channel_plans_plan_mode_check CHECK ((plan_mode = ANY (ARRAY['standard'::text, 'dormant_probe'::text]))),
    CONSTRAINT daily_channel_plans_player_cap_check CHECK ((player_cap >= 0)),
    CONSTRAINT daily_channel_plans_run_mask_check CHECK ((run_about OR run_video OR run_agent)),
    CONSTRAINT daily_channel_plans_schedule_check CHECK (((execution_deadline_at IS NULL) OR ((scheduled_at IS NOT NULL) AND (execution_deadline_at > scheduled_at)))),
    CONSTRAINT daily_channel_plans_source_clock_version_check CHECK ((source_clock_version > 0)),
    CONSTRAINT daily_channel_plans_status_check CHECK ((status = ANY (ARRAY['planned'::text, 'dispatching'::text, 'dispatched'::text, 'running'::text, 'succeeded'::text, 'partial'::text, 'failed'::text, 'cancelled'::text])))
);


--
-- Name: daily_plan_status_repair_audit; Type: TABLE; Schema: feature_clock; Owner: -
--

CREATE TABLE feature_clock.daily_plan_status_repair_audit (
    audit_id uuid NOT NULL,
    repair_batch_id uuid NOT NULL,
    plan_id uuid NOT NULL,
    plan_day date NOT NULL,
    channel_id text NOT NULL,
    previous_status text NOT NULL,
    previous_error_code text,
    repaired_status text NOT NULL,
    latest_outcomes_json jsonb NOT NULL,
    confirmation text NOT NULL,
    operator text NOT NULL,
    reason text NOT NULL,
    repaired_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT daily_plan_status_repair_audit_confirmation_check CHECK ((confirmation ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT daily_plan_status_repair_audit_operator_check CHECK ((length(btrim(operator)) > 0)),
    CONSTRAINT daily_plan_status_repair_audit_previous_status_check CHECK ((previous_status = ANY (ARRAY['failed'::text, 'partial'::text]))),
    CONSTRAINT daily_plan_status_repair_audit_reason_check CHECK ((length(btrim(reason)) > 0)),
    CONSTRAINT daily_plan_status_repair_audit_repaired_status_check CHECK ((repaired_status = 'succeeded'::text))
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
-- Name: channel_delivery_state; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.channel_delivery_state (
    destination text NOT NULL,
    publication_stream_id uuid NOT NULL,
    channel_id text NOT NULL,
    mode text DEFAULT 'hold'::text NOT NULL,
    latest_successful_baseline_id uuid,
    channel_watermark_sequence bigint,
    video_watermark_sequence bigint,
    agent_watermark_sequence bigint,
    source_ownership_reference jsonb DEFAULT '{}'::jsonb NOT NULL,
    cutover_reference jsonb DEFAULT '{}'::jsonb NOT NULL,
    online_at timestamp with time zone,
    sealed_at timestamp with time zone,
    state_changed_at timestamp with time zone DEFAULT now() NOT NULL,
    state_changed_by text NOT NULL,
    state_reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_delivery_state_agent_watermark_sequence_check CHECK (((agent_watermark_sequence IS NULL) OR (agent_watermark_sequence >= 0))),
    CONSTRAINT channel_delivery_state_channel_watermark_sequence_check CHECK (((channel_watermark_sequence IS NULL) OR (channel_watermark_sequence >= 0))),
    CONSTRAINT channel_delivery_state_check CHECK (((btrim(state_changed_by) <> ''::text) AND (btrim(state_reason) <> ''::text))),
    CONSTRAINT channel_delivery_state_check1 CHECK ((((latest_successful_baseline_id IS NULL) AND (channel_watermark_sequence IS NULL) AND (video_watermark_sequence IS NULL) AND (agent_watermark_sequence IS NULL)) OR ((latest_successful_baseline_id IS NOT NULL) AND (channel_watermark_sequence IS NOT NULL) AND (video_watermark_sequence IS NOT NULL) AND (agent_watermark_sequence IS NOT NULL)))),
    CONSTRAINT channel_delivery_state_check2 CHECK ((((mode = 'hold'::text) AND (online_at IS NULL) AND (sealed_at IS NULL)) OR ((mode = 'online'::text) AND (online_at IS NOT NULL) AND (sealed_at IS NULL)) OR ((mode = 'sealed'::text) AND (sealed_at IS NOT NULL)))),
    CONSTRAINT channel_delivery_state_cutover_reference_check CHECK ((jsonb_typeof(cutover_reference) = 'object'::text)),
    CONSTRAINT channel_delivery_state_destination_check CHECK ((btrim(destination) <> ''::text)),
    CONSTRAINT channel_delivery_state_mode_check CHECK ((mode = ANY (ARRAY['hold'::text, 'online'::text, 'sealed'::text]))),
    CONSTRAINT channel_delivery_state_source_ownership_reference_check CHECK ((jsonb_typeof(source_ownership_reference) = 'object'::text)),
    CONSTRAINT channel_delivery_state_video_watermark_sequence_check CHECK (((video_watermark_sequence IS NULL) OR (video_watermark_sequence >= 0)))
);


--
-- Name: channel_stream_state; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.channel_stream_state (
    publication_stream_id uuid NOT NULL,
    channel_id text NOT NULL,
    status text DEFAULT 'owned'::text NOT NULL,
    onboarding_mode text NOT NULL,
    seed_status text DEFAULT 'pending'::text NOT NULL,
    ownership_reference jsonb DEFAULT '{}'::jsonb NOT NULL,
    final_version_vector jsonb,
    owned_at timestamp with time zone DEFAULT now() NOT NULL,
    seed_completed_at timestamp with time zone,
    sealed_at timestamp with time zone,
    state_changed_at timestamp with time zone DEFAULT now() NOT NULL,
    state_changed_by text NOT NULL,
    state_reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_stream_state_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT channel_stream_state_check CHECK (((btrim(state_changed_by) <> ''::text) AND (btrim(state_reason) <> ''::text))),
    CONSTRAINT channel_stream_state_check1 CHECK ((((seed_status = 'pending'::text) AND (seed_completed_at IS NULL)) OR ((seed_status = 'complete'::text) AND (seed_completed_at IS NOT NULL)))),
    CONSTRAINT channel_stream_state_check2 CHECK ((((status = 'owned'::text) AND (sealed_at IS NULL) AND (final_version_vector IS NULL)) OR ((status = 'sealed'::text) AND (sealed_at IS NOT NULL) AND (final_version_vector IS NOT NULL)))),
    CONSTRAINT channel_stream_state_final_version_vector_check CHECK (((final_version_vector IS NULL) OR (jsonb_typeof(final_version_vector) = 'object'::text))),
    CONSTRAINT channel_stream_state_onboarding_mode_check CHECK ((onboarding_mode = ANY (ARRAY['baseline'::text, 'bootstrap'::text, 'cutover'::text]))),
    CONSTRAINT channel_stream_state_ownership_reference_check CHECK ((jsonb_typeof(ownership_reference) = 'object'::text)),
    CONSTRAINT channel_stream_state_seed_status_check CHECK ((seed_status = ANY (ARRAY['pending'::text, 'complete'::text]))),
    CONSTRAINT channel_stream_state_status_check CHECK ((status = ANY (ARRAY['owned'::text, 'sealed'::text])))
);


--
-- Name: domain_current; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.domain_current (
    publication_stream_id uuid NOT NULL,
    channel_id text NOT NULL,
    domain text NOT NULL,
    contract_version integer NOT NULL,
    policy_version text NOT NULL,
    readiness_status text NOT NULL,
    readiness_reasons jsonb DEFAULT '[]'::jsonb NOT NULL,
    payload_json jsonb,
    result_hash text,
    source_refs jsonb DEFAULT '{}'::jsonb NOT NULL,
    complete_observed_at timestamp with time zone,
    data_sequence bigint DEFAULT 0 NOT NULL,
    current_revision_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT domain_current_check CHECK (((btrim(channel_id) <> ''::text) AND (btrim(policy_version) <> ''::text))),
    CONSTRAINT domain_current_check1 CHECK (((payload_json IS NULL) = (result_hash IS NULL))),
    CONSTRAINT domain_current_check2 CHECK (((readiness_status <> 'ready'::text) OR ((payload_json IS NOT NULL) AND (result_hash IS NOT NULL) AND (complete_observed_at IS NOT NULL) AND (jsonb_array_length(readiness_reasons) = 0)))),
    CONSTRAINT domain_current_check3 CHECK (((readiness_status <> 'not_ready'::text) OR (jsonb_array_length(readiness_reasons) > 0))),
    CONSTRAINT domain_current_check4 CHECK ((((data_sequence = 0) AND (current_revision_id IS NULL)) OR ((data_sequence > 0) AND (current_revision_id IS NOT NULL)))),
    CONSTRAINT domain_current_contract_version_check CHECK ((contract_version > 0)),
    CONSTRAINT domain_current_data_sequence_check CHECK ((data_sequence >= 0)),
    CONSTRAINT domain_current_domain_check CHECK ((domain = ANY (ARRAY['channel'::text, 'video'::text, 'agent'::text]))),
    CONSTRAINT domain_current_online_payload_check CHECK (((data_sequence = 0) OR ((current_revision_id IS NOT NULL) AND (payload_json IS NOT NULL) AND (result_hash IS NOT NULL)))),
    CONSTRAINT domain_current_payload_json_check CHECK (((payload_json IS NULL) OR (jsonb_typeof(payload_json) = 'object'::text))),
    CONSTRAINT domain_current_readiness_reasons_check CHECK ((jsonb_typeof(readiness_reasons) = 'array'::text)),
    CONSTRAINT domain_current_readiness_status_check CHECK ((readiness_status = ANY (ARRAY['ready'::text, 'not_ready'::text]))),
    CONSTRAINT domain_current_result_hash_check CHECK (((result_hash IS NULL) OR (result_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT domain_current_source_refs_check CHECK ((jsonb_typeof(source_refs) = 'object'::text))
);


--
-- Name: outbox; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.outbox (
    destination text NOT NULL,
    revision_id uuid NOT NULL,
    status text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    last_error text,
    receipt_id text,
    receipt_status text,
    receipt_received_at timestamp with time zone,
    receipt_json jsonb,
    delivered_at timestamp with time zone,
    covered_by_baseline_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT outbox_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT outbox_check CHECK ((((status = 'leased'::text) AND (lease_owner IS NOT NULL) AND (lease_expires_at IS NOT NULL)) OR ((status <> 'leased'::text) AND (lease_owner IS NULL) AND (lease_expires_at IS NULL)))),
    CONSTRAINT outbox_check1 CHECK ((((status = 'delivered'::text) AND (delivered_at IS NOT NULL) AND (receipt_id IS NOT NULL) AND (receipt_status = ANY (ARRAY['accepted'::text, 'duplicate'::text, 'waiting_gap'::text])) AND (receipt_received_at IS NOT NULL)) OR ((status <> 'delivered'::text) AND (delivered_at IS NULL)))),
    CONSTRAINT outbox_check2 CHECK ((((status = 'covered_by_baseline'::text) AND (covered_by_baseline_id IS NOT NULL)) OR ((status <> 'covered_by_baseline'::text) AND (covered_by_baseline_id IS NULL)))),
    CONSTRAINT outbox_check3 CHECK (((status <> 'dead_letter'::text) OR (last_error IS NOT NULL))),
    CONSTRAINT outbox_destination_check CHECK ((btrim(destination) <> ''::text)),
    CONSTRAINT outbox_lease_owner_check CHECK (((lease_owner IS NULL) OR (btrim(lease_owner) <> ''::text))),
    CONSTRAINT outbox_receipt_id_check CHECK (((receipt_id IS NULL) OR (btrim(receipt_id) <> ''::text))),
    CONSTRAINT outbox_receipt_json_check CHECK (((receipt_json IS NULL) OR (jsonb_typeof(receipt_json) = 'object'::text))),
    CONSTRAINT outbox_receipt_status_check CHECK (((receipt_status IS NULL) OR (receipt_status = ANY (ARRAY['accepted'::text, 'duplicate'::text, 'waiting_gap'::text, 'rejected'::text, 'conflict'::text])))),
    CONSTRAINT outbox_status_check CHECK ((status = ANY (ARRAY['held'::text, 'pending'::text, 'leased'::text, 'retry_wait'::text, 'delivered'::text, 'covered_by_baseline'::text, 'dead_letter'::text])))
);


--
-- Name: revision; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.revision (
    revision_id uuid NOT NULL,
    publication_stream_id uuid NOT NULL,
    channel_id text NOT NULL,
    domain text NOT NULL,
    data_sequence bigint NOT NULL,
    previous_data_sequence bigint,
    revision_type text NOT NULL,
    operation text NOT NULL,
    contract_version integer NOT NULL,
    policy_version text NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    source_refs jsonb NOT NULL,
    previous_result_hash text,
    result_hash text NOT NULL,
    payload_hash text NOT NULL,
    payload_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT revision_check CHECK (((btrim(channel_id) <> ''::text) AND (btrim(policy_version) <> ''::text))),
    CONSTRAINT revision_check1 CHECK ((((revision_type = 'bootstrap'::text) AND (data_sequence = 1) AND (previous_data_sequence IS NULL) AND (previous_result_hash IS NULL)) OR ((revision_type <> 'bootstrap'::text) AND (previous_data_sequence = (data_sequence - 1)) AND (previous_result_hash IS NOT NULL)))),
    CONSTRAINT revision_check2 CHECK ((((domain = 'channel'::text) AND (((revision_type = 'retraction'::text) AND (operation = 'retract_channel'::text)) OR ((revision_type <> 'retraction'::text) AND (operation = 'replace'::text)))) OR ((domain = 'video'::text) AND (((revision_type = 'bootstrap'::text) AND (operation = 'replace_window'::text)) OR ((revision_type <> 'bootstrap'::text) AND (operation = 'apply_window_delta'::text)))) OR ((domain = 'agent'::text) AND (((revision_type = 'retraction'::text) AND (operation = 'retract_agent'::text)) OR ((revision_type <> 'retraction'::text) AND (operation = 'replace'::text)))))),
    CONSTRAINT revision_contract_version_check CHECK ((contract_version > 0)),
    CONSTRAINT revision_data_sequence_check CHECK ((data_sequence > 0)),
    CONSTRAINT revision_domain_check CHECK ((domain = ANY (ARRAY['channel'::text, 'video'::text, 'agent'::text]))),
    CONSTRAINT revision_operation_check CHECK ((operation = ANY (ARRAY['replace'::text, 'replace_window'::text, 'apply_window_delta'::text, 'retract_channel'::text, 'retract_agent'::text]))),
    CONSTRAINT revision_payload_hash_check CHECK ((payload_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT revision_payload_json_check CHECK ((jsonb_typeof(payload_json) = 'object'::text)),
    CONSTRAINT revision_previous_result_hash_check CHECK (((previous_result_hash IS NULL) OR (previous_result_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT revision_result_hash_check CHECK ((result_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT revision_revision_type_check CHECK ((revision_type = ANY (ARRAY['bootstrap'::text, 'incremental'::text, 'repair'::text, 'retraction'::text]))),
    CONSTRAINT revision_source_refs_check CHECK ((jsonb_typeof(source_refs) = 'object'::text))
);


--
-- Name: stream; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.stream (
    publication_stream_id uuid NOT NULL,
    source_deployment_key text NOT NULL,
    source_identity_json jsonb NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    minimum_writer_version text,
    capture_enabled_at timestamp with time zone,
    automatic_onboarding_destination text,
    created_by text NOT NULL,
    created_reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    status_changed_at timestamp with time zone DEFAULT now() NOT NULL,
    status_changed_by text NOT NULL,
    status_reason text NOT NULL,
    sealed_at timestamp with time zone,
    CONSTRAINT stream_check CHECK (((btrim(created_by) <> ''::text) AND (btrim(created_reason) <> ''::text))),
    CONSTRAINT stream_check1 CHECK (((btrim(status_changed_by) <> ''::text) AND (btrim(status_reason) <> ''::text))),
    CONSTRAINT stream_check2 CHECK (((capture_enabled_at IS NULL) OR (minimum_writer_version IS NOT NULL))),
    CONSTRAINT stream_check3 CHECK ((((status = 'active'::text) AND (sealed_at IS NULL)) OR ((status = 'sealed'::text) AND (sealed_at IS NOT NULL)))),
    CONSTRAINT chk_publication_stream_automatic_onboarding_destination CHECK (((automatic_onboarding_destination IS NULL) OR (btrim(automatic_onboarding_destination) <> ''::text))),
    CONSTRAINT stream_minimum_writer_version_check CHECK (((minimum_writer_version IS NULL) OR (btrim(minimum_writer_version) <> ''::text))),
    CONSTRAINT stream_source_deployment_key_check CHECK ((btrim(source_deployment_key) <> ''::text)),
    CONSTRAINT stream_source_identity_json_check CHECK ((jsonb_typeof(source_identity_json) = 'object'::text)),
    CONSTRAINT stream_status_check CHECK ((status = ANY (ARRAY['active'::text, 'sealed'::text])))
);


--
-- Name: agent_configs config_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_configs ALTER COLUMN config_id SET DEFAULT nextval('crawler.agent_configs_config_id_seq'::regclass);


--
-- Name: agent_prompt_templates template_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_prompt_templates ALTER COLUMN template_id SET DEFAULT nextval('crawler.agent_prompt_templates_template_id_seq'::regclass);


--
-- Name: channel_candidate_sources candidate_source_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidate_sources ALTER COLUMN candidate_source_id SET DEFAULT nextval('crawler.channel_candidate_sources_candidate_source_id_seq'::regclass);


--
-- Name: channel_candidates candidate_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidates ALTER COLUMN candidate_id SET DEFAULT nextval('crawler.channel_candidates_candidate_id_seq'::regclass);


--
-- Name: content_candidates candidate_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates ALTER COLUMN candidate_id SET DEFAULT nextval('crawler.content_candidates_candidate_id_seq'::regclass);


--
-- Name: controller_ticks tick_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.controller_ticks ALTER COLUMN tick_id SET DEFAULT nextval('crawler.controller_ticks_tick_id_seq'::regclass);


--
-- Name: query_quality_tasks quality_task_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_tasks ALTER COLUMN quality_task_id SET DEFAULT nextval('crawler.query_quality_tasks_quality_task_id_seq'::regclass);


--
-- Name: query_sets query_set_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_sets ALTER COLUMN query_set_id SET DEFAULT nextval('crawler.query_sets_query_set_id_seq'::regclass);


--
-- Name: query_terms query_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_terms ALTER COLUMN query_id SET DEFAULT nextval('crawler.query_terms_query_id_seq'::regclass);


--
-- Name: raw_objects raw_object_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.raw_objects ALTER COLUMN raw_object_id SET DEFAULT nextval('crawler.raw_objects_raw_object_id_seq'::regclass);


--
-- Name: task_events event_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.task_events ALTER COLUMN event_id SET DEFAULT nextval('crawler.task_events_event_id_seq'::regclass);


--
-- Name: youtube_api_tasks task_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_api_tasks ALTER COLUMN task_id SET DEFAULT nextval('crawler.youtube_api_tasks_task_id_seq'::regclass);


--
-- Name: youtube_channel_api_tasks task_id; Type: DEFAULT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_channel_api_tasks ALTER COLUMN task_id SET DEFAULT nextval('crawler.youtube_channel_api_tasks_task_id_seq'::regclass);


--
-- Name: agent_configs agent_configs_name_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_configs
    ADD CONSTRAINT agent_configs_name_key UNIQUE (name);


--
-- Name: agent_configs agent_configs_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_configs
    ADD CONSTRAINT agent_configs_pkey PRIMARY KEY (config_id);


--
-- Name: agent_profiles agent_profiles_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_profiles
    ADD CONSTRAINT agent_profiles_pkey PRIMARY KEY (channel_id, agent_mode);


--
-- Name: agent_prompt_templates agent_prompt_templates_name_version_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_prompt_templates
    ADD CONSTRAINT agent_prompt_templates_name_version_key UNIQUE (name, version);


--
-- Name: agent_prompt_templates agent_prompt_templates_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_prompt_templates
    ADD CONSTRAINT agent_prompt_templates_pkey PRIMARY KEY (template_id);


--
-- Name: agent_refresh_requests agent_refresh_requests_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_refresh_requests
    ADD CONSTRAINT agent_refresh_requests_pkey PRIMARY KEY (plan_id);


--
-- Name: baseline_export_events baseline_export_events_export_id_channel_id_observation_kin_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.baseline_export_events
    ADD CONSTRAINT baseline_export_events_export_id_channel_id_observation_kin_key UNIQUE (export_id, channel_id, observation_kind, kind_sequence);


--
-- Name: baseline_export_events baseline_export_events_observation_kind_check; Type: CHECK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE crawler.baseline_export_events
    ADD CONSTRAINT baseline_export_events_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['about'::text, 'video'::text, 'agent'::text]))) NOT VALID;


--
-- Name: baseline_export_events baseline_export_events_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.baseline_export_events
    ADD CONSTRAINT baseline_export_events_pkey PRIMARY KEY (export_id, event_id);


--
-- Name: baseline_exports baseline_exports_export_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.baseline_exports
    ADD CONSTRAINT baseline_exports_export_id_key UNIQUE (export_id);


--
-- Name: baseline_exports baseline_exports_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.baseline_exports
    ADD CONSTRAINT baseline_exports_pkey PRIMARY KEY (baseline_version);


--
-- Name: browser_profile_groups browser_profile_groups_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.browser_profile_groups
    ADD CONSTRAINT browser_profile_groups_pkey PRIMARY KEY (profile_group_id);


--
-- Name: browser_profile_groups browser_profile_groups_proxy_id_profile_revision_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.browser_profile_groups
    ADD CONSTRAINT browser_profile_groups_proxy_id_profile_revision_key UNIQUE (proxy_id, profile_revision);


--
-- Name: browser_profiles browser_profiles_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.browser_profiles
    ADD CONSTRAINT browser_profiles_pkey PRIMARY KEY (profile_id);


--
-- Name: browser_profiles browser_profiles_profile_group_id_engine_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.browser_profiles
    ADD CONSTRAINT browser_profiles_profile_group_id_engine_key UNIQUE (profile_group_id, engine);


--
-- Name: business_run_bindings business_run_bindings_business_run_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.business_run_bindings
    ADD CONSTRAINT business_run_bindings_business_run_id_key UNIQUE (business_run_id);


--
-- Name: business_run_bindings business_run_bindings_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.business_run_bindings
    ADD CONSTRAINT business_run_bindings_pkey PRIMARY KEY (business_run_key);


--
-- Name: channel_about_metric_snapshots channel_about_metric_snapshot_channel_id_observed_at_observ_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_about_metric_snapshots
    ADD CONSTRAINT channel_about_metric_snapshot_channel_id_observed_at_observ_key UNIQUE (channel_id, observed_at, observation_id);


--
-- Name: channel_about_metric_snapshots channel_about_metric_snapshots_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_about_metric_snapshots
    ADD CONSTRAINT channel_about_metric_snapshots_pkey PRIMARY KEY (observation_id);


--
-- Name: channel_candidate_sources channel_candidate_sources_candidate_id_page_id_discovery_st_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidate_sources
    ADD CONSTRAINT channel_candidate_sources_candidate_id_page_id_discovery_st_key UNIQUE (candidate_id, page_id, discovery_strategy);


--
-- Name: channel_candidate_sources channel_candidate_sources_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidate_sources
    ADD CONSTRAINT channel_candidate_sources_pkey PRIMARY KEY (candidate_source_id);


--
-- Name: channel_candidates channel_candidates_dispatch_batch_id_channel_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidates
    ADD CONSTRAINT channel_candidates_dispatch_batch_id_channel_id_key UNIQUE (dispatch_batch_id, channel_id);


--
-- Name: channel_candidates channel_candidates_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidates
    ADD CONSTRAINT channel_candidates_pkey PRIMARY KEY (candidate_id);


--
-- Name: channel_domain_cursors channel_domain_cursors_observation_kind_check; Type: CHECK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE crawler.channel_domain_cursors
    ADD CONSTRAINT channel_domain_cursors_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['about'::text, 'video'::text, 'agent'::text]))) NOT VALID;


--
-- Name: channel_domain_cursors channel_domain_cursors_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_domain_cursors
    ADD CONSTRAINT channel_domain_cursors_pkey PRIMARY KEY (channel_id, observation_kind);


--
-- Name: channel_execution_attempts channel_execution_attempts_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_execution_attempts
    ADD CONSTRAINT channel_execution_attempts_pkey PRIMARY KEY (attempt_id);


--
-- Name: channel_runs channel_runs_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_runs
    ADD CONSTRAINT channel_runs_pkey PRIMARY KEY (run_id);


--
-- Name: channel_tab_pages channel_tab_pages_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_tab_pages
    ADD CONSTRAINT channel_tab_pages_pkey PRIMARY KEY (run_id, tab, page_no);


--
-- Name: channels channels_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (channel_id);


--
-- Name: content_candidates content_candidates_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates
    ADD CONSTRAINT content_candidates_pkey PRIMARY KEY (candidate_id);


--
-- Name: content_candidates content_candidates_run_id_position_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates
    ADD CONSTRAINT content_candidates_run_id_position_key UNIQUE (run_id, "position");


--
-- Name: content_candidates content_candidates_run_id_source_content_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates
    ADD CONSTRAINT content_candidates_run_id_source_content_id_key UNIQUE (run_id, source_content_id);


--
-- Name: content_enrich_tasks content_enrich_tasks_content_key_job_type_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_enrich_tasks
    ADD CONSTRAINT content_enrich_tasks_content_key_job_type_key UNIQUE (content_key, job_type);


--
-- Name: content_enrich_tasks content_enrich_tasks_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_enrich_tasks
    ADD CONSTRAINT content_enrich_tasks_pkey PRIMARY KEY (task_id);


--
-- Name: contents contents_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.contents
    ADD CONSTRAINT contents_pkey PRIMARY KEY (content_key);


--
-- Name: controller_ticks controller_ticks_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.controller_ticks
    ADD CONSTRAINT controller_ticks_pkey PRIMARY KEY (tick_id);


--
-- Name: crawl_observation_keys crawl_observation_keys_observation_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observation_keys
    ADD CONSTRAINT crawl_observation_keys_observation_id_key UNIQUE (observation_id);


--
-- Name: crawl_observation_keys crawl_observation_keys_observation_kind_check; Type: CHECK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE crawler.crawl_observation_keys
    ADD CONSTRAINT crawl_observation_keys_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['about'::text, 'video'::text, 'agent'::text]))) NOT VALID;


--
-- Name: crawl_observation_keys crawl_observation_keys_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observation_keys
    ADD CONSTRAINT crawl_observation_keys_pkey PRIMARY KEY (idempotency_key);


--
-- Name: crawl_observations crawl_observations_channel_id_observation_kind_kind_sequenc_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observations
    ADD CONSTRAINT crawl_observations_channel_id_observation_kind_kind_sequenc_key UNIQUE (channel_id, observation_kind, kind_sequence);


--
-- Name: crawl_observations crawl_observations_observation_kind_check; Type: CHECK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE crawler.crawl_observations
    ADD CONSTRAINT crawl_observations_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['about'::text, 'video'::text, 'agent'::text]))) NOT VALID;


--
-- Name: crawl_observations crawl_observations_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observations
    ADD CONSTRAINT crawl_observations_pkey PRIMARY KEY (observation_id);


--
-- Name: crawler_outbox crawler_outbox_observation_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawler_outbox
    ADD CONSTRAINT crawler_outbox_observation_id_key UNIQUE (observation_id);


--
-- Name: crawler_outbox crawler_outbox_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawler_outbox
    ADD CONSTRAINT crawler_outbox_pkey PRIMARY KEY (event_id);


--
-- Name: finalized_profiles finalized_profiles_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.finalized_profiles
    ADD CONSTRAINT finalized_profiles_pkey PRIMARY KEY (channel_id);


--
-- Name: observation_raw_objects observation_raw_objects_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.observation_raw_objects
    ADD CONSTRAINT observation_raw_objects_pkey PRIMARY KEY (observation_id, raw_object_id, role);


--
-- Name: proxy_job_dispatch_outbox proxy_job_dispatch_outbox_aggregate_kind_aggregate_id_inten_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.proxy_job_dispatch_outbox
    ADD CONSTRAINT proxy_job_dispatch_outbox_aggregate_kind_aggregate_id_inten_key UNIQUE (aggregate_kind, aggregate_id, intent_hash);


--
-- Name: proxy_job_dispatch_outbox proxy_job_dispatch_outbox_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.proxy_job_dispatch_outbox
    ADD CONSTRAINT proxy_job_dispatch_outbox_pkey PRIMARY KEY (dispatch_id);


--
-- Name: query_dispatch_batches query_dispatch_batches_pipeline_cycle_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_dispatch_batches
    ADD CONSTRAINT query_dispatch_batches_pipeline_cycle_id_key UNIQUE (pipeline_cycle_id);


--
-- Name: query_dispatch_batches query_dispatch_batches_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_dispatch_batches
    ADD CONSTRAINT query_dispatch_batches_pkey PRIMARY KEY (dispatch_batch_id);


--
-- Name: query_pages query_pages_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_pages
    ADD CONSTRAINT query_pages_pkey PRIMARY KEY (page_id);


--
-- Name: query_quality_batches query_quality_batches_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_batches
    ADD CONSTRAINT query_quality_batches_pkey PRIMARY KEY (quality_batch_id);


--
-- Name: query_quality_chunk_members query_quality_chunk_members_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunk_members
    ADD CONSTRAINT query_quality_chunk_members_pkey PRIMARY KEY (quality_chunk_id, quality_task_id);


--
-- Name: query_quality_chunk_members query_quality_chunk_members_quality_batch_id_quality_task_i_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunk_members
    ADD CONSTRAINT query_quality_chunk_members_quality_batch_id_quality_task_i_key UNIQUE (quality_batch_id, quality_task_id);


--
-- Name: query_quality_chunk_members query_quality_chunk_members_quality_chunk_id_member_ordinal_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunk_members
    ADD CONSTRAINT query_quality_chunk_members_quality_chunk_id_member_ordinal_key UNIQUE (quality_chunk_id, member_ordinal);


--
-- Name: query_quality_chunks query_quality_chunks_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunks
    ADD CONSTRAINT query_quality_chunks_pkey PRIMARY KEY (quality_chunk_id);


--
-- Name: query_quality_chunks query_quality_chunks_quality_batch_id_quality_chunk_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunks
    ADD CONSTRAINT query_quality_chunks_quality_batch_id_quality_chunk_id_key UNIQUE (quality_batch_id, quality_chunk_id);


--
-- Name: query_quality_tasks query_quality_tasks_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_tasks
    ADD CONSTRAINT query_quality_tasks_pkey PRIMARY KEY (quality_task_id);


--
-- Name: query_quality_tasks query_quality_tasks_quality_batch_id_query_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_tasks
    ADD CONSTRAINT query_quality_tasks_quality_batch_id_query_id_key UNIQUE (quality_batch_id, query_id);


--
-- Name: query_sets query_sets_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_sets
    ADD CONSTRAINT query_sets_pkey PRIMARY KEY (query_set_id);


--
-- Name: query_terms query_terms_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_terms
    ADD CONSTRAINT query_terms_pkey PRIMARY KEY (query_id);


--
-- Name: raw_objects raw_objects_object_path_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.raw_objects
    ADD CONSTRAINT raw_objects_object_path_key UNIQUE (object_path);


--
-- Name: raw_objects raw_objects_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.raw_objects
    ADD CONSTRAINT raw_objects_pkey PRIMARY KEY (raw_object_id);


--
-- Name: settings settings_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.settings
    ADD CONSTRAINT settings_pkey PRIMARY KEY (setting_key);


--
-- Name: task_events task_events_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.task_events
    ADD CONSTRAINT task_events_pkey PRIMARY KEY (event_id);


--
-- Name: youtube_api_batches youtube_api_batches_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_api_batches
    ADD CONSTRAINT youtube_api_batches_pkey PRIMARY KEY (batch_id);


--
-- Name: youtube_api_daily_usage youtube_api_daily_usage_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_api_daily_usage
    ADD CONSTRAINT youtube_api_daily_usage_pkey PRIMARY KEY (usage_date);


--
-- Name: youtube_api_tasks youtube_api_tasks_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_api_tasks
    ADD CONSTRAINT youtube_api_tasks_pkey PRIMARY KEY (task_id);


--
-- Name: youtube_api_tasks youtube_api_tasks_source_content_id_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_api_tasks
    ADD CONSTRAINT youtube_api_tasks_source_content_id_key UNIQUE (source_content_id);


--
-- Name: youtube_channel_api_batches youtube_channel_api_batches_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_channel_api_batches
    ADD CONSTRAINT youtube_channel_api_batches_pkey PRIMARY KEY (batch_id);


--
-- Name: youtube_channel_api_tasks youtube_channel_api_tasks_pkey; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_channel_api_tasks
    ADD CONSTRAINT youtube_channel_api_tasks_pkey PRIMARY KEY (task_id);


--
-- Name: youtube_channel_api_tasks youtube_channel_api_tasks_request_key_key; Type: CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.youtube_channel_api_tasks
    ADD CONSTRAINT youtube_channel_api_tasks_request_key_key UNIQUE (request_key);


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
-- Name: channel_observation_checkpoints channel_observation_checkpoints_observation_kind_check; Type: CHECK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE feature_clock.channel_observation_checkpoints
    ADD CONSTRAINT channel_observation_checkpoints_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['about'::text, 'video'::text, 'agent'::text]))) NOT VALID;


--
-- Name: channel_observation_checkpoints channel_observation_checkpoints_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.channel_observation_checkpoints
    ADD CONSTRAINT channel_observation_checkpoints_pkey PRIMARY KEY (channel_id, observation_kind);


--
-- Name: clock_decision_log clock_decision_log_clock_kind_check; Type: CHECK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE feature_clock.clock_decision_log
    ADD CONSTRAINT clock_decision_log_clock_kind_check CHECK ((clock_kind = ANY (ARRAY['about'::text, 'video'::text, 'agent'::text]))) NOT VALID;


--
-- Name: clock_decision_log clock_decision_log_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.clock_decision_log
    ADD CONSTRAINT clock_decision_log_pkey PRIMARY KEY (decision_id);


--
-- Name: clock_decision_log clock_decision_log_tier_check; Type: CHECK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE feature_clock.clock_decision_log
    ADD CONSTRAINT clock_decision_log_tier_check CHECK ((((clock_kind = 'about'::text) AND (tier = ANY (ARRAY[1, 2, 3, 5, 7, 14, 30, 60, 90, 180, 365]))) OR ((clock_kind = 'video'::text) AND (tier = ANY (ARRAY[1, 3, 7, 14, 30, 60, 90, 180, 365]))) OR ((clock_kind = 'agent'::text) AND ((tier = ANY (ARRAY[1, 3, 7, 14, 30])) OR ((tier >= 60) AND (tier <= 365)))))) NOT VALID;


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
-- Name: crawler_event_inbox crawler_event_inbox_observation_kind_check; Type: CHECK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE feature_clock.crawler_event_inbox
    ADD CONSTRAINT crawler_event_inbox_observation_kind_check CHECK ((observation_kind = ANY (ARRAY['about'::text, 'video'::text, 'agent'::text]))) NOT VALID;


--
-- Name: crawler_event_inbox crawler_event_inbox_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.crawler_event_inbox
    ADD CONSTRAINT crawler_event_inbox_pkey PRIMARY KEY (event_id);


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
-- Name: daily_plan_status_repair_audit daily_plan_status_repair_audit_pkey; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.daily_plan_status_repair_audit
    ADD CONSTRAINT daily_plan_status_repair_audit_pkey PRIMARY KEY (audit_id);


--
-- Name: daily_plan_status_repair_audit daily_plan_status_repair_audit_repair_batch_id_plan_id_key; Type: CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.daily_plan_status_repair_audit
    ADD CONSTRAINT daily_plan_status_repair_audit_repair_batch_id_plan_id_key UNIQUE (repair_batch_id, plan_id);


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
-- Name: channel_delivery_state channel_delivery_state_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_delivery_state
    ADD CONSTRAINT channel_delivery_state_pkey PRIMARY KEY (destination, publication_stream_id, channel_id);


--
-- Name: channel_stream_state channel_stream_state_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_stream_state
    ADD CONSTRAINT channel_stream_state_pkey PRIMARY KEY (publication_stream_id, channel_id);


--
-- Name: domain_current domain_current_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.domain_current
    ADD CONSTRAINT domain_current_pkey PRIMARY KEY (publication_stream_id, channel_id, domain);


--
-- Name: outbox outbox_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.outbox
    ADD CONSTRAINT outbox_pkey PRIMARY KEY (destination, revision_id);


--
-- Name: revision revision_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.revision
    ADD CONSTRAINT revision_pkey PRIMARY KEY (revision_id);


--
-- Name: revision revision_publication_stream_id_channel_id_domain_data_sequ_key1; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.revision
    ADD CONSTRAINT revision_publication_stream_id_channel_id_domain_data_sequ_key1 UNIQUE (publication_stream_id, channel_id, domain, data_sequence, revision_id, result_hash);


--
-- Name: revision revision_publication_stream_id_channel_id_domain_data_seque_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.revision
    ADD CONSTRAINT revision_publication_stream_id_channel_id_domain_data_seque_key UNIQUE (publication_stream_id, channel_id, domain, data_sequence);


--
-- Name: stream stream_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.stream
    ADD CONSTRAINT stream_pkey PRIMARY KEY (publication_stream_id);


--
-- Name: idx_crawler_about_snapshots_channel_observed; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_about_snapshots_channel_observed ON crawler.channel_about_metric_snapshots USING btree (channel_id, observed_at DESC);


--
-- Name: idx_crawler_about_snapshots_origin; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_about_snapshots_origin ON crawler.channel_about_metric_snapshots USING btree (snapshot_origin, channel_id);


--
-- Name: idx_crawler_agent_prompt_templates_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_agent_prompt_templates_status ON crawler.agent_prompt_templates USING btree (status, is_default, updated_at DESC);


--
-- Name: idx_crawler_agent_refresh_requests_channel; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_agent_refresh_requests_channel ON crawler.agent_refresh_requests USING btree (channel_id, status, created_at);


--
-- Name: idx_crawler_agent_refresh_requests_claim; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_agent_refresh_requests_claim ON crawler.agent_refresh_requests USING btree (status, next_retry_at, created_at);


--
-- Name: idx_crawler_baseline_export_events_sequence; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_baseline_export_events_sequence ON crawler.baseline_export_events USING btree (export_id, channel_id, observation_kind, kind_sequence);


--
-- Name: idx_crawler_browser_profile_groups_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_browser_profile_groups_status ON crawler.browser_profile_groups USING btree (status, last_used_at DESC NULLS LAST);


--
-- Name: idx_crawler_browser_profiles_group; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_browser_profiles_group ON crawler.browser_profiles USING btree (profile_group_id, engine);


--
-- Name: idx_crawler_business_run_bindings_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_business_run_bindings_status ON crawler.business_run_bindings USING btree (status, created_at);


--
-- Name: idx_crawler_channel_candidate_sources_page; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_candidate_sources_page ON crawler.channel_candidate_sources USING btree (page_id, candidate_id) WHERE (page_id IS NOT NULL);


--
-- Name: idx_crawler_channel_candidate_sources_query; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_candidate_sources_query ON crawler.channel_candidate_sources USING btree (query_id, created_at DESC);


--
-- Name: idx_crawler_channel_candidates_channel; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_candidates_channel ON crawler.channel_candidates USING btree (channel_id, created_at DESC);


--
-- Name: idx_crawler_channel_candidates_claim; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_candidates_claim ON crawler.channel_candidates USING btree (dispatch_batch_id, status, priority DESC, created_at);


--
-- Name: idx_crawler_channel_execution_attempts_channel; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_execution_attempts_channel ON crawler.channel_execution_attempts USING btree (channel_id, started_at DESC);


--
-- Name: idx_crawler_channel_execution_attempts_proxy; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_execution_attempts_proxy ON crawler.channel_execution_attempts USING btree (proxy_id, started_at DESC);


--
-- Name: idx_crawler_channel_execution_attempts_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_execution_attempts_status ON crawler.channel_execution_attempts USING btree (status, started_at DESC);


--
-- Name: idx_crawler_channel_runs_candidate; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_runs_candidate ON crawler.channel_runs USING btree (candidate_id) WHERE (candidate_id IS NOT NULL);


--
-- Name: idx_crawler_channel_runs_channel; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channel_runs_channel ON crawler.channel_runs USING btree (channel_id, created_at DESC);


--
-- Name: idx_crawler_channels_agent_ready; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channels_agent_ready ON crawler.channels USING btree (agent_status, ready_for_agent, priority DESC, created_at);


--
-- Name: idx_crawler_channels_dormant_recheck; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_channels_dormant_recheck ON crawler.channels USING btree (dormant_recheck_day, channel_id) WHERE (status = 'dormant'::text);


--
-- Name: idx_crawler_content_candidates_api; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_api ON crawler.content_candidates USING btree (api_status, updated_at) WHERE (api_status = ANY (ARRAY['pending'::text, 'failed'::text]));


--
-- Name: idx_crawler_content_candidates_batch; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_batch ON crawler.content_candidates USING btree (run_id, detail_status, "position");


--
-- Name: idx_crawler_content_candidates_channel_disposition_due; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_channel_disposition_due ON crawler.content_candidates USING btree (channel_id, disposition, next_attempt_at, candidate_id) WHERE ((disposition = ANY (ARRAY['deferred'::text, 'terminal_excluded'::text])) AND (next_attempt_at IS NOT NULL));


--
-- Name: idx_crawler_content_candidates_content_key; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_content_key ON crawler.content_candidates USING btree (content_key) WHERE (content_key IS NOT NULL);


--
-- Name: idx_crawler_content_candidates_disposition_due; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_disposition_due ON crawler.content_candidates USING btree (disposition, next_attempt_at, candidate_id) WHERE ((disposition = ANY (ARRAY['deferred'::text, 'terminal_excluded'::text])) AND (next_attempt_at IS NOT NULL));


--
-- Name: idx_crawler_content_candidates_disposition_history; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_disposition_history ON crawler.content_candidates USING btree (channel_id, source_content_id, candidate_id DESC);


--
-- Name: idx_crawler_content_candidates_first_seen_ledger_pending; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_first_seen_ledger_pending ON crawler.content_candidates USING btree (channel_id, candidate_id) WHERE (first_seen_ledger_status = 'pending'::text);


--
-- Name: idx_crawler_content_candidates_open_api_source; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_open_api_source ON crawler.content_candidates USING btree (source_content_id, candidate_id) WHERE ((detail_status = 'api_pending'::text) AND (api_status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text])));


--
-- Name: idx_crawler_content_candidates_repairable_run; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_candidates_repairable_run ON crawler.content_candidates USING btree (run_id) INCLUDE (content_key) WHERE ((COALESCE(((result_json -> 'scope'::text) ->> 'status'::text), ''::text) <> 'excluded'::text) AND (NOT (result_json ? 'parser_contract_error'::text)) AND ((detail_status = 'failed'::text) OR (missing_fields @> ARRAY['content_type'::text]) OR ((detail_status = ANY (ARRAY['done'::text, 'unavailable'::text])) AND (cardinality(missing_fields) > 0) AND (COALESCE((result_json #>> '{access,access_status}'::text[]), 'unknown'::text) <> ALL (ARRAY['members_only'::text, 'private'::text, 'unlisted'::text, 'unavailable'::text])))));


--
-- Name: idx_crawler_content_enrich_tasks_claim; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_enrich_tasks_claim ON crawler.content_enrich_tasks USING btree (status, priority, created_at);


--
-- Name: idx_crawler_content_enrich_tasks_retry; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_enrich_tasks_retry ON crawler.content_enrich_tasks USING btree (status, next_retry_at, priority, created_at);


--
-- Name: idx_crawler_content_enrich_tasks_dispatch; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_enrich_tasks_dispatch ON crawler.content_enrich_tasks USING btree (job_type, status, next_retry_at, priority, created_at, channel_id);


--
-- Name: idx_crawler_content_enrich_tasks_lease_owner; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_content_enrich_tasks_lease_owner ON crawler.content_enrich_tasks USING btree (lease_owner) WHERE (lease_owner IS NOT NULL);


--
-- Name: idx_crawler_contents_channel; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_channel ON crawler.contents USING btree (channel_id, content_type, last_seen_at DESC);


--
-- Name: idx_crawler_contents_channel_published; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_channel_published ON crawler.contents USING btree (channel_id, published_at DESC) WHERE (published_at IS NOT NULL);


--
-- Name: idx_crawler_contents_current_run; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_current_run ON crawler.contents USING btree (channel_id, run_id, "position");


--
-- Name: idx_crawler_contents_hashtags; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_hashtags ON crawler.contents USING gin (hashtags);


--
-- Name: idx_crawler_contents_keywords; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_keywords ON crawler.contents USING gin (keywords);


--
-- Name: idx_crawler_contents_player_refresh; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_player_refresh ON crawler.contents USING btree (channel_id, player_last_observed_at, published_at DESC);


--
-- Name: idx_crawler_contents_published_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_published_status ON crawler.contents USING btree (published_at_status, last_seen_at DESC);


--
-- Name: idx_crawler_contents_recent; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_contents_recent ON crawler.contents USING btree (channel_id, is_recent, content_type, last_seen_at DESC);


--
-- Name: idx_crawler_controller_ticks_created_at; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_controller_ticks_created_at ON crawler.controller_ticks USING btree (created_at);


--
-- Name: idx_crawler_crawl_observation_keys_expiry; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_crawl_observation_keys_expiry ON crawler.crawl_observation_keys USING btree (expires_at);


--
-- Name: idx_crawler_crawl_observations_channel_kind; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_crawl_observations_channel_kind ON crawler.crawl_observations USING btree (channel_id, observation_kind, observed_at DESC);


--
-- Name: idx_crawler_crawl_observations_created_at; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_crawl_observations_created_at ON crawler.crawl_observations USING btree (created_at);


--
-- Name: idx_crawler_outbox_created_at; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_outbox_created_at ON crawler.crawler_outbox USING btree (created_at);


--
-- Name: idx_crawler_outbox_publish; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_outbox_publish ON crawler.crawler_outbox USING btree (status, next_attempt_at, lease_expires_at, created_at);


--
-- Name: idx_crawler_proxy_job_dispatch_outbox_pending; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_proxy_job_dispatch_outbox_pending ON crawler.proxy_job_dispatch_outbox USING btree (status, next_attempt_at, created_at);


--
-- Name: idx_crawler_query_dispatch_batches_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_query_dispatch_batches_status ON crawler.query_dispatch_batches USING btree (status, started_at DESC);


--
-- Name: idx_crawler_query_pages_dispatch_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_query_pages_dispatch_status ON crawler.query_pages USING btree (dispatch_batch_id, status, page_no);


--
-- Name: idx_crawler_query_quality_batches_status; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_query_quality_batches_status ON crawler.query_quality_batches USING btree (status, created_at);


--
-- Name: idx_crawler_query_quality_tasks_claim; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_query_quality_tasks_claim ON crawler.query_quality_tasks USING btree (quality_batch_id, status, quality_task_id);


--
-- Name: idx_crawler_query_terms_due; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_query_terms_due ON crawler.query_terms USING btree (status, next_crawl_at, priority DESC, query_id);


--
-- Name: idx_crawler_query_terms_quality; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_query_terms_quality ON crawler.query_terms USING btree (quality_status, quality_score DESC NULLS LAST);


--
-- Name: idx_crawler_query_terms_set; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_query_terms_set ON crawler.query_terms USING btree (query_set_id, status, priority DESC, query_id);


--
-- Name: idx_crawler_raw_objects_entity; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_raw_objects_entity ON crawler.raw_objects USING btree (entity_type, entity_id, created_at DESC);


--
-- Name: idx_crawler_raw_objects_type; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_raw_objects_type ON crawler.raw_objects USING btree (object_type, created_at DESC);


--
-- Name: idx_crawler_task_events_created_at; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_task_events_created_at ON crawler.task_events USING btree (created_at);


--
-- Name: idx_crawler_task_events_entity; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_task_events_entity ON crawler.task_events USING btree (entity_key, created_at DESC);


--
-- Name: idx_crawler_task_events_queue_created; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_task_events_queue_created ON crawler.task_events USING btree (queue_name, created_at DESC);


--
-- Name: idx_crawler_youtube_api_tasks_claim; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_youtube_api_tasks_claim ON crawler.youtube_api_tasks USING btree (status, next_retry_at, created_at);


--
-- Name: idx_crawler_youtube_channel_api_tasks_claim; Type: INDEX; Schema: crawler; Owner: -
--

CREATE INDEX idx_crawler_youtube_channel_api_tasks_claim ON crawler.youtube_channel_api_tasks USING btree (status, created_at, task_id) WHERE (status = ANY (ARRAY['pending'::text, 'running'::text]));


--
-- Name: ux_crawler_agent_configs_default; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_agent_configs_default ON crawler.agent_configs USING btree (is_default) WHERE (is_default = true);


--
-- Name: ux_crawler_agent_prompt_templates_default; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_agent_prompt_templates_default ON crawler.agent_prompt_templates USING btree (is_default) WHERE (is_default = true);


--
-- Name: ux_crawler_browser_profile_groups_active_identity; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_browser_profile_groups_active_identity ON crawler.browser_profile_groups USING btree (identity_policy_id, identity_policy_version, network_identity_key, profile_epoch) WHERE ((status = 'active'::text) AND (network_identity_key IS NOT NULL));


--
-- Name: ux_crawler_browser_profile_groups_active_proxy; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_browser_profile_groups_active_proxy ON crawler.browser_profile_groups USING btree (proxy_id) WHERE ((status = 'active'::text) AND (proxy_id IS NOT NULL));


--
-- Name: ux_crawler_channel_candidates_identity; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_channel_candidates_identity ON crawler.channel_candidates USING btree (candidate_id, channel_id);


--
-- Name: ux_crawler_channel_execution_attempts_business_attempt; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_channel_execution_attempts_business_attempt ON crawler.channel_execution_attempts USING btree (workload_scope, business_run_id, attempt_number) WHERE ((workload_scope IS NOT NULL) AND (business_run_id IS NOT NULL) AND (attempt_number IS NOT NULL));


--
-- Name: ux_crawler_channel_execution_attempts_rota_task; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_channel_execution_attempts_rota_task ON crawler.channel_execution_attempts USING btree (task_id) WHERE (task_id IS NOT NULL);


--
-- Name: ux_crawler_channel_runs_identity; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_channel_runs_identity ON crawler.channel_runs USING btree (run_id, channel_id, candidate_id);


--
-- Name: ux_crawler_channel_runs_plan_id; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_channel_runs_plan_id ON crawler.channel_runs USING btree (plan_id) WHERE (plan_id IS NOT NULL);


--
-- Name: ux_crawler_channels_registry_promotion_candidate; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_channels_registry_promotion_candidate ON crawler.channels USING btree (registry_promotion_candidate_id) WHERE (registry_promotion_candidate_id IS NOT NULL);


--
-- Name: ux_crawler_channels_registry_promotion_run; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_channels_registry_promotion_run ON crawler.channels USING btree (registry_promotion_run_id) WHERE (registry_promotion_run_id IS NOT NULL);


--
-- Name: ux_crawler_contents_channel_source; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_contents_channel_source ON crawler.contents USING btree (channel_id, source_content_id);


--
-- Name: ux_crawler_query_quality_tasks_batch_task; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_query_quality_tasks_batch_task ON crawler.query_quality_tasks USING btree (quality_batch_id, quality_task_id);


--
-- Name: ux_crawler_query_sets_name; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_query_sets_name ON crawler.query_sets USING btree (lower(name));


--
-- Name: ux_crawler_query_terms_scope; Type: INDEX; Schema: crawler; Owner: -
--

CREATE UNIQUE INDEX ux_crawler_query_terms_scope ON crawler.query_terms USING btree (lower(query_text), COALESCE(language, ''::text), COALESCE(country, ''::text), COALESCE(category, ''::text));


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
-- Name: idx_feature_clock_plan_status_repair_plan; Type: INDEX; Schema: feature_clock; Owner: -
--

CREATE INDEX idx_feature_clock_plan_status_repair_plan ON feature_clock.daily_plan_status_repair_audit USING btree (plan_id, repaired_at DESC);


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
-- Name: idx_publication_channel_delivery_state_mode; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_publication_channel_delivery_state_mode ON publication.channel_delivery_state USING btree (destination, mode, publication_stream_id, channel_id);


--
-- Name: idx_publication_channel_stream_state_status; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_publication_channel_stream_state_status ON publication.channel_stream_state USING btree (publication_stream_id, status, onboarding_mode, channel_id);


--
-- Name: idx_publication_domain_current_readiness; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_publication_domain_current_readiness ON publication.domain_current USING btree (readiness_status, domain, updated_at, publication_stream_id, channel_id);


--
-- Name: idx_publication_outbox_claim; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_publication_outbox_claim ON publication.outbox USING btree (status, next_attempt_at, created_at, revision_id) WHERE (status = ANY (ARRAY['pending'::text, 'retry_wait'::text, 'leased'::text]));


--
-- Name: idx_publication_outbox_revision; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_publication_outbox_revision ON publication.outbox USING btree (revision_id, destination);


--
-- Name: idx_publication_revision_channel_sequence; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_publication_revision_channel_sequence ON publication.revision USING btree (publication_stream_id, channel_id, domain, data_sequence DESC);


--
-- Name: idx_publication_stream_deployment_status; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_publication_stream_deployment_status ON publication.stream USING btree (source_deployment_key, status, created_at);


--
-- Name: ux_publication_channel_stream_owned; Type: INDEX; Schema: publication; Owner: -
--

CREATE UNIQUE INDEX ux_publication_channel_stream_owned ON publication.channel_stream_state USING btree (channel_id) WHERE (status = 'owned'::text);


--
-- Name: channel_candidates trg_channel_registry_promotion_candidate; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_channel_registry_promotion_candidate BEFORE UPDATE ON crawler.channel_candidates FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_registry_promotion_candidate();


--
-- Name: channels trg_channel_registry_promotion_immutable; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_channel_registry_promotion_immutable BEFORE INSERT OR UPDATE ON crawler.channels FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_registry_promotion();


--
-- Name: channel_runs trg_channel_registry_promotion_run; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_channel_registry_promotion_run BEFORE INSERT OR UPDATE ON crawler.channel_runs FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_registry_promotion_run();


--
-- Name: channel_runs trg_channel_run_publication_finalize; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_channel_run_publication_finalize BEFORE UPDATE ON crawler.channel_runs FOR EACH ROW EXECUTE FUNCTION crawler.guard_channel_run_publication_finalize();


--
-- Name: query_pages trg_guard_managed_query_page_state; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_guard_managed_query_page_state BEFORE UPDATE ON crawler.query_pages FOR EACH ROW EXECUTE FUNCTION crawler.guard_managed_query_page_state();


--
-- Name: query_quality_chunk_members trg_guard_query_quality_chunk_members; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_guard_query_quality_chunk_members BEFORE INSERT OR DELETE OR UPDATE ON crawler.query_quality_chunk_members FOR EACH ROW EXECUTE FUNCTION crawler.guard_query_quality_chunk_members();


--
-- Name: query_quality_chunks trg_guard_query_quality_chunk_state; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_guard_query_quality_chunk_state BEFORE UPDATE ON crawler.query_quality_chunks FOR EACH ROW EXECUTE FUNCTION crawler.guard_query_quality_chunk_state();


--
-- Name: agent_profiles trg_publication_agent_profiles_writer_version; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_publication_agent_profiles_writer_version BEFORE INSERT OR DELETE OR UPDATE ON crawler.agent_profiles FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();


--
-- Name: channels trg_publication_channels_writer_version; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_publication_channels_writer_version BEFORE INSERT OR DELETE OR UPDATE ON crawler.channels FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();


--
-- Name: contents trg_publication_contents_writer_version; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_publication_contents_writer_version BEFORE INSERT OR DELETE OR UPDATE ON crawler.contents FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();


--
-- Name: finalized_profiles trg_publication_finalized_profiles_writer_version; Type: TRIGGER; Schema: crawler; Owner: -
--

CREATE TRIGGER trg_publication_finalized_profiles_writer_version BEFORE INSERT OR DELETE OR UPDATE ON crawler.finalized_profiles FOR EACH ROW EXECUTE FUNCTION publication.guard_source_writer_version();


--
-- Name: rule_policy_definitions trg_protect_active_policy_definition; Type: TRIGGER; Schema: feature_clock; Owner: -
--

CREATE TRIGGER trg_protect_active_policy_definition BEFORE UPDATE ON feature_clock.rule_policy_definitions FOR EACH ROW EXECUTE FUNCTION feature_clock.protect_active_policy_definition();


--
-- Name: channel_delivery_state trg_publication_channel_delivery_lifecycle; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_publication_channel_delivery_lifecycle BEFORE UPDATE ON publication.channel_delivery_state FOR EACH ROW EXECUTE FUNCTION publication.guard_channel_delivery_lifecycle();


--
-- Name: channel_stream_state trg_publication_channel_stream_lifecycle; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_publication_channel_stream_lifecycle BEFORE UPDATE ON publication.channel_stream_state FOR EACH ROW EXECUTE FUNCTION publication.guard_channel_stream_lifecycle();


--
-- Name: outbox trg_publication_outbox_lifecycle; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_publication_outbox_lifecycle BEFORE DELETE OR UPDATE ON publication.outbox FOR EACH ROW EXECUTE FUNCTION publication.guard_outbox_lifecycle();


--
-- Name: revision trg_publication_revision_immutable; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_publication_revision_immutable BEFORE DELETE OR UPDATE ON publication.revision FOR EACH ROW EXECUTE FUNCTION publication.guard_revision_immutable();


--
-- Name: stream trg_publication_stream_lifecycle; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_publication_stream_lifecycle BEFORE DELETE OR UPDATE ON publication.stream FOR EACH ROW EXECUTE FUNCTION publication.guard_stream_lifecycle();


--
-- Name: agent_configs agent_configs_prompt_template_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_configs
    ADD CONSTRAINT agent_configs_prompt_template_id_fkey FOREIGN KEY (prompt_template_id) REFERENCES crawler.agent_prompt_templates(template_id) ON DELETE SET NULL;


--
-- Name: agent_profiles agent_profiles_agent_config_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_profiles
    ADD CONSTRAINT agent_profiles_agent_config_id_fkey FOREIGN KEY (agent_config_id) REFERENCES crawler.agent_configs(config_id) ON DELETE SET NULL;


--
-- Name: agent_profiles agent_profiles_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_profiles
    ADD CONSTRAINT agent_profiles_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: agent_profiles agent_profiles_prompt_template_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_profiles
    ADD CONSTRAINT agent_profiles_prompt_template_id_fkey FOREIGN KEY (prompt_template_id) REFERENCES crawler.agent_prompt_templates(template_id) ON DELETE SET NULL;


--
-- Name: agent_refresh_requests agent_refresh_requests_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_refresh_requests
    ADD CONSTRAINT agent_refresh_requests_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: agent_refresh_requests agent_refresh_requests_run_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.agent_refresh_requests
    ADD CONSTRAINT agent_refresh_requests_run_id_fkey FOREIGN KEY (run_id) REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL;


--
-- Name: baseline_export_events baseline_export_events_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.baseline_export_events
    ADD CONSTRAINT baseline_export_events_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: baseline_export_events baseline_export_events_event_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.baseline_export_events
    ADD CONSTRAINT baseline_export_events_event_id_fkey FOREIGN KEY (event_id) REFERENCES crawler.crawler_outbox(event_id) ON DELETE CASCADE;


--
-- Name: baseline_export_events baseline_export_events_export_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.baseline_export_events
    ADD CONSTRAINT baseline_export_events_export_id_fkey FOREIGN KEY (export_id) REFERENCES crawler.baseline_exports(export_id) ON DELETE CASCADE;


--
-- Name: browser_profiles browser_profiles_profile_group_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.browser_profiles
    ADD CONSTRAINT browser_profiles_profile_group_id_fkey FOREIGN KEY (profile_group_id) REFERENCES crawler.browser_profile_groups(profile_group_id) ON DELETE CASCADE;


--
-- Name: channel_about_metric_snapshots channel_about_metric_snapshots_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_about_metric_snapshots
    ADD CONSTRAINT channel_about_metric_snapshots_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_about_metric_snapshots channel_about_metric_snapshots_observation_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_about_metric_snapshots
    ADD CONSTRAINT channel_about_metric_snapshots_observation_id_fkey FOREIGN KEY (observation_id) REFERENCES crawler.crawl_observations(observation_id) ON DELETE CASCADE;


--
-- Name: channel_candidate_sources channel_candidate_sources_candidate_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidate_sources
    ADD CONSTRAINT channel_candidate_sources_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES crawler.channel_candidates(candidate_id) ON DELETE CASCADE;


--
-- Name: channel_candidate_sources channel_candidate_sources_page_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidate_sources
    ADD CONSTRAINT channel_candidate_sources_page_id_fkey FOREIGN KEY (page_id) REFERENCES crawler.query_pages(page_id) ON DELETE SET NULL;


--
-- Name: channel_candidate_sources channel_candidate_sources_query_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidate_sources
    ADD CONSTRAINT channel_candidate_sources_query_id_fkey FOREIGN KEY (query_id) REFERENCES crawler.query_terms(query_id) ON DELETE SET NULL;


--
-- Name: channel_candidates channel_candidates_dispatch_batch_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_candidates
    ADD CONSTRAINT channel_candidates_dispatch_batch_id_fkey FOREIGN KEY (dispatch_batch_id) REFERENCES crawler.query_dispatch_batches(dispatch_batch_id) ON DELETE CASCADE;


--
-- Name: channel_domain_cursors channel_domain_cursors_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_domain_cursors
    ADD CONSTRAINT channel_domain_cursors_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_execution_attempts channel_execution_attempts_profile_group_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_execution_attempts
    ADD CONSTRAINT channel_execution_attempts_profile_group_id_fkey FOREIGN KEY (profile_group_id) REFERENCES crawler.browser_profile_groups(profile_group_id) ON DELETE RESTRICT;


--
-- Name: channel_execution_attempts channel_execution_attempts_run_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_execution_attempts
    ADD CONSTRAINT channel_execution_attempts_run_id_fkey FOREIGN KEY (run_id) REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL;


--
-- Name: channel_execution_attempts channel_execution_attempts_youtubejs_profile_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_execution_attempts
    ADD CONSTRAINT channel_execution_attempts_youtubejs_profile_id_fkey FOREIGN KEY (youtubejs_profile_id) REFERENCES crawler.browser_profiles(profile_id) ON DELETE RESTRICT;


--
-- Name: channel_execution_attempts channel_execution_attempts_ytdlp_profile_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_execution_attempts
    ADD CONSTRAINT channel_execution_attempts_ytdlp_profile_id_fkey FOREIGN KEY (ytdlp_profile_id) REFERENCES crawler.browser_profiles(profile_id) ON DELETE RESTRICT;


--
-- Name: channel_runs channel_runs_candidate_channel_fk; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_runs
    ADD CONSTRAINT channel_runs_candidate_channel_fk FOREIGN KEY (candidate_id, channel_id) REFERENCES crawler.channel_candidates(candidate_id, channel_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: channel_runs channel_runs_candidate_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_runs
    ADD CONSTRAINT channel_runs_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES crawler.channel_candidates(candidate_id) ON DELETE SET NULL;


--
-- Name: channel_runs channel_runs_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_runs
    ADD CONSTRAINT channel_runs_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_tab_pages channel_tab_pages_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_tab_pages
    ADD CONSTRAINT channel_tab_pages_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_tab_pages channel_tab_pages_run_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channel_tab_pages
    ADD CONSTRAINT channel_tab_pages_run_id_fkey FOREIGN KEY (run_id) REFERENCES crawler.channel_runs(run_id) ON DELETE CASCADE;


--
-- Name: channels channels_registry_promotion_candidate_fk; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channels
    ADD CONSTRAINT channels_registry_promotion_candidate_fk FOREIGN KEY (registry_promotion_candidate_id, channel_id) REFERENCES crawler.channel_candidates(candidate_id, channel_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: channels channels_registry_promotion_run_fk; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.channels
    ADD CONSTRAINT channels_registry_promotion_run_fk FOREIGN KEY (registry_promotion_run_id, channel_id, registry_promotion_candidate_id) REFERENCES crawler.channel_runs(run_id, channel_id, candidate_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: content_candidates content_candidates_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates
    ADD CONSTRAINT content_candidates_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: content_candidates content_candidates_content_key_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates
    ADD CONSTRAINT content_candidates_content_key_fkey FOREIGN KEY (content_key) REFERENCES crawler.contents(content_key) ON DELETE SET NULL;


--
-- Name: content_candidates content_candidates_first_seen_ledger_observation_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates
    ADD CONSTRAINT content_candidates_first_seen_ledger_observation_id_fkey FOREIGN KEY (first_seen_ledger_observation_id) REFERENCES crawler.crawl_observations(observation_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: content_candidates content_candidates_run_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_candidates
    ADD CONSTRAINT content_candidates_run_id_fkey FOREIGN KEY (run_id) REFERENCES crawler.channel_runs(run_id) ON DELETE CASCADE;


--
-- Name: content_enrich_tasks content_enrich_tasks_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_enrich_tasks
    ADD CONSTRAINT content_enrich_tasks_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: content_enrich_tasks content_enrich_tasks_content_key_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.content_enrich_tasks
    ADD CONSTRAINT content_enrich_tasks_content_key_fkey FOREIGN KEY (content_key) REFERENCES crawler.contents(content_key) ON DELETE CASCADE;


--
-- Name: contents contents_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.contents
    ADD CONSTRAINT contents_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: contents contents_run_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.contents
    ADD CONSTRAINT contents_run_id_fkey FOREIGN KEY (run_id) REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL;


--
-- Name: crawl_observation_keys crawl_observation_keys_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observation_keys
    ADD CONSTRAINT crawl_observation_keys_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: crawl_observations crawl_observations_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observations
    ADD CONSTRAINT crawl_observations_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: crawl_observations crawl_observations_run_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawl_observations
    ADD CONSTRAINT crawl_observations_run_id_fkey FOREIGN KEY (run_id) REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL;


--
-- Name: crawler_outbox crawler_outbox_observation_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.crawler_outbox
    ADD CONSTRAINT crawler_outbox_observation_id_fkey FOREIGN KEY (observation_id) REFERENCES crawler.crawl_observations(observation_id) ON DELETE CASCADE;


--
-- Name: finalized_profiles finalized_profiles_channel_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.finalized_profiles
    ADD CONSTRAINT finalized_profiles_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE CASCADE;


--
-- Name: finalized_profiles finalized_profiles_run_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.finalized_profiles
    ADD CONSTRAINT finalized_profiles_run_id_fkey FOREIGN KEY (run_id) REFERENCES crawler.channel_runs(run_id) ON DELETE SET NULL;


--
-- Name: observation_raw_objects observation_raw_objects_observation_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.observation_raw_objects
    ADD CONSTRAINT observation_raw_objects_observation_id_fkey FOREIGN KEY (observation_id) REFERENCES crawler.crawl_observations(observation_id) ON DELETE CASCADE;


--
-- Name: observation_raw_objects observation_raw_objects_raw_object_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.observation_raw_objects
    ADD CONSTRAINT observation_raw_objects_raw_object_id_fkey FOREIGN KEY (raw_object_id) REFERENCES crawler.raw_objects(raw_object_id) ON DELETE CASCADE;


--
-- Name: query_dispatch_batches query_dispatch_batches_query_set_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_dispatch_batches
    ADD CONSTRAINT query_dispatch_batches_query_set_id_fkey FOREIGN KEY (query_set_id) REFERENCES crawler.query_sets(query_set_id) ON DELETE SET NULL;


--
-- Name: query_pages query_pages_dispatch_batch_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_pages
    ADD CONSTRAINT query_pages_dispatch_batch_id_fkey FOREIGN KEY (dispatch_batch_id) REFERENCES crawler.query_dispatch_batches(dispatch_batch_id) ON DELETE SET NULL;


--
-- Name: query_pages query_pages_query_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_pages
    ADD CONSTRAINT query_pages_query_id_fkey FOREIGN KEY (query_id) REFERENCES crawler.query_terms(query_id) ON DELETE SET NULL;


--
-- Name: query_quality_chunk_members query_quality_chunk_members_quality_batch_id_quality_chunk_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunk_members
    ADD CONSTRAINT query_quality_chunk_members_quality_batch_id_quality_chunk_fkey FOREIGN KEY (quality_batch_id, quality_chunk_id) REFERENCES crawler.query_quality_chunks(quality_batch_id, quality_chunk_id) ON DELETE CASCADE;


--
-- Name: query_quality_chunk_members query_quality_chunk_members_quality_batch_id_quality_task__fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunk_members
    ADD CONSTRAINT query_quality_chunk_members_quality_batch_id_quality_task__fkey FOREIGN KEY (quality_batch_id, quality_task_id) REFERENCES crawler.query_quality_tasks(quality_batch_id, quality_task_id) ON DELETE CASCADE;


--
-- Name: query_quality_chunks query_quality_chunks_quality_batch_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_chunks
    ADD CONSTRAINT query_quality_chunks_quality_batch_id_fkey FOREIGN KEY (quality_batch_id) REFERENCES crawler.query_quality_batches(quality_batch_id) ON DELETE CASCADE;


--
-- Name: query_quality_tasks query_quality_tasks_quality_batch_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_tasks
    ADD CONSTRAINT query_quality_tasks_quality_batch_id_fkey FOREIGN KEY (quality_batch_id) REFERENCES crawler.query_quality_batches(quality_batch_id) ON DELETE CASCADE;


--
-- Name: query_quality_tasks query_quality_tasks_query_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_quality_tasks
    ADD CONSTRAINT query_quality_tasks_query_id_fkey FOREIGN KEY (query_id) REFERENCES crawler.query_terms(query_id) ON DELETE CASCADE;


--
-- Name: query_terms query_terms_query_set_id_fkey; Type: FK CONSTRAINT; Schema: crawler; Owner: -
--

ALTER TABLE ONLY crawler.query_terms
    ADD CONSTRAINT query_terms_query_set_id_fkey FOREIGN KEY (query_set_id) REFERENCES crawler.query_sets(query_set_id) ON DELETE SET NULL;


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
-- Name: daily_plan_status_repair_audit daily_plan_status_repair_audit_plan_id_fkey; Type: FK CONSTRAINT; Schema: feature_clock; Owner: -
--

ALTER TABLE ONLY feature_clock.daily_plan_status_repair_audit
    ADD CONSTRAINT daily_plan_status_repair_audit_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES feature_clock.daily_channel_plans(plan_id);


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
-- Name: channel_delivery_state channel_delivery_state_publication_stream_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_delivery_state
    ADD CONSTRAINT channel_delivery_state_publication_stream_id_channel_id_fkey FOREIGN KEY (publication_stream_id, channel_id) REFERENCES publication.channel_stream_state(publication_stream_id, channel_id) ON DELETE RESTRICT;


--
-- Name: channel_stream_state channel_stream_state_channel_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_stream_state
    ADD CONSTRAINT channel_stream_state_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES crawler.channels(channel_id) ON DELETE RESTRICT;


--
-- Name: channel_stream_state channel_stream_state_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_stream_state
    ADD CONSTRAINT channel_stream_state_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: domain_current domain_current_current_revision_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.domain_current
    ADD CONSTRAINT domain_current_current_revision_id_fkey FOREIGN KEY (publication_stream_id, channel_id, domain, data_sequence, current_revision_id, result_hash) REFERENCES publication.revision(publication_stream_id, channel_id, domain, data_sequence, revision_id, result_hash) ON DELETE RESTRICT;


--
-- Name: domain_current domain_current_publication_stream_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.domain_current
    ADD CONSTRAINT domain_current_publication_stream_id_channel_id_fkey FOREIGN KEY (publication_stream_id, channel_id) REFERENCES publication.channel_stream_state(publication_stream_id, channel_id) ON DELETE RESTRICT;


--
-- Name: outbox outbox_revision_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.outbox
    ADD CONSTRAINT outbox_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES publication.revision(revision_id) ON DELETE RESTRICT;


--
-- Name: revision revision_publication_stream_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.revision
    ADD CONSTRAINT revision_publication_stream_id_channel_id_fkey FOREIGN KEY (publication_stream_id, channel_id) REFERENCES publication.channel_stream_state(publication_stream_id, channel_id) ON DELETE RESTRICT;


CREATE TABLE crawler.database_identity (
    singleton boolean DEFAULT true NOT NULL,
    database_kind text NOT NULL,
    database_name text NOT NULL,
    initialized_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT database_identity_kind_check CHECK ((database_kind = 'crawler'::text)),
    CONSTRAINT database_identity_singleton_check CHECK (singleton),
    CONSTRAINT database_identity_pkey PRIMARY KEY (singleton)
);

INSERT INTO crawler.database_identity (singleton, database_kind, database_name)
VALUES (true, 'crawler', current_database());

INSERT INTO crawler.settings (setting_key,value_json,updated_at)
VALUES (
  'query_scheduler',
  '{"status":"stopped","stop_reason":"fresh_migration_bootstrap","updated_by":"bootstrap"}'::jsonb,
  now()
);

CREATE TABLE crawler.migration_channel_intents (
    migration_intent_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    source_id text NOT NULL,
    source_database text NOT NULL,
    source_database_oid oid NOT NULL,
    source_candidate_id bigint NOT NULL,
    channel_id text NOT NULL,
    source_snapshot jsonb NOT NULL,
    snapshot_sha256 text NOT NULL CHECK (snapshot_sha256 ~ '^[a-f0-9]{64}$'),
    target_candidate_id bigint UNIQUE REFERENCES crawler.channel_candidates(candidate_id) ON DELETE RESTRICT,
    first_dispatch_batch_id text NOT NULL,
    dispatch_attempts integer DEFAULT 0 NOT NULL CHECK (dispatch_attempts >= 0),
    last_dispatch_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    UNIQUE (source_id, channel_id),
    UNIQUE (source_id, source_candidate_id)
);

CREATE INDEX idx_crawler_migration_intents_target
ON crawler.migration_channel_intents (target_candidate_id)
WHERE target_candidate_id IS NOT NULL;

-- migration-channel-inventory-schema:start
CREATE TABLE crawler.migration_channel_inventory_syncs (
    source_id text PRIMARY KEY,
    source_database text NOT NULL,
    source_database_oid oid NOT NULL,
    status text DEFAULT 'syncing'::text NOT NULL,
    sync_token uuid NOT NULL,
    eligible_count bigint DEFAULT 0 NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    last_error text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT migration_channel_inventory_syncs_eligible_count_check CHECK ((eligible_count >= 0)),
    CONSTRAINT migration_channel_inventory_syncs_status_check CHECK ((status = ANY (ARRAY['syncing'::text, 'ready'::text, 'failed'::text])))
);

CREATE TABLE crawler.migration_channel_inventory (
    source_id text NOT NULL REFERENCES crawler.migration_channel_inventory_syncs(source_id) ON DELETE CASCADE,
    source_candidate_id bigint NOT NULL,
    channel_id text NOT NULL,
    channel_url text NOT NULL,
    handle text,
    title text,
    avatar_url text,
    search_subscriber_count bigint,
    priority integer DEFAULT 100 NOT NULL,
    source_candidate_status text NOT NULL,
    source_updated_at timestamp with time zone,
    sync_token uuid NOT NULL,
    synced_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT migration_channel_inventory_pkey PRIMARY KEY (source_id, channel_id),
    CONSTRAINT migration_channel_inventory_source_candidate_key UNIQUE (source_id, source_candidate_id),
    CONSTRAINT migration_channel_inventory_source_status_check CHECK ((source_candidate_status = ANY (ARRAY['discovered'::text, 'queued'::text, 'validating'::text, 'failed'::text])))
);

CREATE INDEX idx_crawler_migration_inventory_page
ON crawler.migration_channel_inventory (source_id, priority DESC, source_candidate_id ASC);
-- migration-channel-inventory-schema:end

CREATE FUNCTION crawler.prevent_migration_intent_source_update()
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

CREATE TRIGGER prevent_migration_intent_source_update
BEFORE UPDATE ON crawler.migration_channel_intents
FOR EACH ROW
EXECUTE FUNCTION crawler.prevent_migration_intent_source_update();

--
-- PostgreSQL database dump complete
--

\unrestrict lJLMm7kerNHrnNnKPrSGNhBVfEqa6y3pntHpQL3Og2IziHLUyFiTXlh9XI747T8
