--
-- PostgreSQL database dump
--

\restrict Sf4gQ0ucPgbD9EiBQYZfjZedS6nldo10cznALxvzrPwEuo798jq2FGMuR3WhMrH

-- Dumped from database version 18.4
-- Dumped by pg_dump version 18.4

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: publication; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA publication;


--
-- Name: raw_crawler; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw_crawler;


--
-- Name: result; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA result;


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: activate_creator_search_incremental_v1(text, integer, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.activate_creator_search_incremental_v1(p_expected_watermark text, p_expected_live_count integer, p_actor text, p_reason text) RETURNS text
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  active_watermark TEXT;
  live_count INTEGER;
  legacy_count INTEGER;
  current_write_mode TEXT;
  current_read_mode TEXT;
BEGIN
  IF NULLIF(btrim(p_expected_watermark),'') IS NULL
     OR p_expected_live_count IS NULL OR p_expected_live_count<0
     OR NULLIF(btrim(p_actor),'') IS NULL
     OR NULLIF(btrim(p_reason),'') IS NULL THEN
    RAISE EXCEPTION 'expected watermark, non-negative count, actor, and reason are required';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  SELECT watermark INTO active_watermark
  FROM public.creator_search_active
  WHERE singleton=true
  FOR UPDATE;
  IF active_watermark IS DISTINCT FROM p_expected_watermark THEN
    RAISE EXCEPTION 'Creator Search active watermark changed before storage cutover';
  END IF;

  SELECT write_mode,read_mode INTO current_write_mode,current_read_mode
  FROM publication.creator_search_storage_state
  WHERE singleton=true
  FOR UPDATE;
  IF current_write_mode<>'shadow' OR current_read_mode<>'legacy' THEN
    RAISE EXCEPTION 'Creator Search storage is not in shadow/legacy mode';
  END IF;

  SELECT count(*)::int INTO live_count FROM public.creator_search_live;
  SELECT count(*)::int INTO legacy_count
  FROM public.creator_search_current
  WHERE watermark=active_watermark;
  IF live_count<>p_expected_live_count OR legacy_count<>p_expected_live_count THEN
    RAISE EXCEPTION 'Creator Search cutover row count differs from expectation';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM (
      SELECT legacy.channel_id AS legacy_channel_id,
             live.channel_id AS live_channel_id,
             CASE WHEN legacy.channel_id IS NULL THEN NULL ELSE
               to_jsonb(legacy)-'watermark' END
               AS legacy_document,
             CASE WHEN live.channel_id IS NULL THEN NULL ELSE
               to_jsonb(live)-'watermark' END
               AS live_document
      FROM (
        SELECT search.*
        FROM public.creator_search_current search
        WHERE search.watermark=active_watermark
      ) legacy
      FULL JOIN public.creator_search_live live USING(channel_id)
    ) parity
    WHERE parity.legacy_channel_id IS NULL
       OR parity.live_channel_id IS NULL
       OR parity.legacy_document IS DISTINCT FROM parity.live_document
  ) THEN
    RAISE EXCEPTION 'Creator Search Live differs from the active Legacy release';
  END IF;

  UPDATE publication.creator_search_storage_state
  SET read_mode='live',write_mode='incremental',updated_at=clock_timestamp(),
      cutover_actor=btrim(p_actor),cutover_reason=btrim(p_reason),
      cutover_at=clock_timestamp()
  WHERE singleton=true AND write_mode='shadow' AND read_mode='legacy';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search storage state changed during cutover';
  END IF;

  RETURN 'incremental';
END
$$;


--
-- Name: channel_link_type_v1(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.channel_link_type_v1(value text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog'
    AS $_$
  select case
    when lower(value) like 'mailto:%' then 'email'
    when lower(value) ~ '^https?://([^/]+\.)?instagram\.com(/|$)' then 'instagram'
    when lower(value) ~ '^https?://([^/]+\.)?(facebook\.com|fb\.com)(/|$)' then 'facebook'
    when lower(value) ~ '^https?://([^/]+\.)?(x\.com|twitter\.com)(/|$)' then 'x_twitter'
    when lower(value) ~ '^https?://([^/]+\.)?tiktok\.com(/|$)' then 'tiktok'
    when lower(value) ~ '^https?://([^/]+\.)?spotify\.com(/|$)' then 'spotify'
    when lower(value) ~ '^https?://([^/]+\.)?(youtube\.com|youtu\.be)(/|$)' then 'youtube'
    when lower(value) ~ '^https?://([^/]+\.)?(whatsapp\.com|wa\.me)(/|$)' then 'whatsapp'
    when lower(value) ~ '^https?://([^/]+\.)?(t\.me|telegram\.me)(/|$)' then 'telegram'
    else 'website'
  end
$_$;


--
-- Name: content_window_proof_v1(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.content_window_proof_v1(p_import_batch_id text, p_channel_id text, p_run_id text) RETURNS jsonb
    LANGUAGE sql STABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog', 'public', 'raw_crawler'
    AS $_$
  with source_run as (
    select
      run.run_id,
      run.expected_content_count,
      snapshot.captured_at,
      finalized.quality_json,
      case
        when finalized.quality_json #>> '{candidate_count}' ~ '^[0-9]{1,9}$'
          then (finalized.quality_json #>> '{candidate_count}')::integer
      end candidate_count,
      case
        when finalized.quality_json #>> '{classified_content_count}' ~ '^[0-9]{1,9}$'
          then (finalized.quality_json #>> '{classified_content_count}')::integer
      end classified_content_count,
      case
        when finalized.quality_json #>> '{expected_content_count}' ~ '^[0-9]{1,9}$'
          then (finalized.quality_json #>> '{expected_content_count}')::integer
      end quality_expected_content_count
    from raw_crawler.channel_runs run
    join raw_crawler.channels source_channel
      on source_channel.import_batch_id = run.import_batch_id
     and source_channel.channel_id = run.channel_id
     and source_channel.latest_run_id = run.run_id
    join public.channel_snapshots snapshot
      on snapshot.import_batch_id = run.import_batch_id
     and snapshot.channel_id = run.channel_id
     and snapshot.source_latest_run_id = run.run_id
    join raw_crawler.finalized_profiles finalized
      on finalized.import_batch_id = run.import_batch_id
     and finalized.channel_id = run.channel_id
     and finalized.run_id = run.run_id
     and finalized.status = 'ready_auto'
     and finalized.quality_json #>> '{data_complete}' = 'true'
    where run.import_batch_id = p_import_batch_id
      and run.channel_id = p_channel_id
      and run.run_id = p_run_id
      and run.status = 'done'
      and run.detail_status = 'done'
  ), candidates as (
    select candidate.*
    from raw_crawler.content_candidates candidate
    join source_run on source_run.run_id = candidate.run_id
    where candidate.import_batch_id = p_import_batch_id
      and candidate.channel_id = p_channel_id
      and candidate.position is not null
  ), candidate_shape as (
    select
      count(*)::integer candidate_count,
      count(distinct position)::integer distinct_positions,
      min(position) min_position,
      max(position) max_position
    from candidates
  ), content_shape as (
    select
      count(*)::integer content_count,
      count(*) filter (
        where content.published_at is null
           or content.published_at_status is distinct from 'exact'
           or content.published_at_precision is null
           or content.published_at_precision not in ('second', 'date_only')
      )::integer invalid_publication_count
    from raw_crawler.contents content
    where content.import_batch_id = p_import_batch_id
      and content.channel_id = p_channel_id
      and content.run_id = p_run_id
  ), linked_shape as (
    select
      count(*)::integer linked_content_count,
      count(distinct candidate.source_content_id)::integer linked_candidate_count
    from candidates candidate
    join raw_crawler.contents content
      on content.import_batch_id = candidate.import_batch_id
     and content.channel_id = candidate.channel_id
     and content.run_id = candidate.run_id
     and content.source_content_id = candidate.source_content_id
  ), valid_cutoffs as (
    select candidate.*
    from candidates candidate
    where candidate.result_json #>> '{scope,status}' = 'excluded'
      and candidate.result_json #>> '{scope,reason}' = 'older_than_max_age'
      and candidate.result_json #>> '{scope,source}' in ('youtube_detail', 'uploads_playlist')
      and candidate.result_json #>> '{scope,max_age_days}' = '90'
      and case
        when candidate.result_json #>> '{scope,age_days}' ~ '^[0-9]{1,9}$'
          then (candidate.result_json #>> '{scope,age_days}')::integer
      end > 90
  ), valid_upcoming as (
    select candidate.*
    from candidates candidate
    where candidate.result_json #>> '{scope,status}' = 'excluded'
      and candidate.result_json #>> '{scope,reason}' = 'upcoming_live'
      and candidate.result_json #>> '{scope,source}' in ('youtube_detail', 'historical_cleanup')
      and candidate.result_json #>> '{scope,live_status}' in ('upcoming', 'is_upcoming')
  ), first_cutoff as (
    select cutoff.*
    from valid_cutoffs cutoff
    order by cutoff.position
    limit 1
  ), post_cutoff_shape as (
    select
      count(*) filter (where content.source_content_id is not null)::integer materialized_count,
      count(*) filter (
        where content.source_content_id is null
          and valid_cutoff.candidate_id is not null
      )::integer older_count,
      count(*) filter (
        where content.source_content_id is null
          and valid_upcoming_candidate.candidate_id is not null
      )::integer upcoming_count,
      count(*) filter (
        where content.source_content_id is null
          and candidate.result_json #>> '{scope,reason}' = 'after_chronological_age_cutoff'
      )::integer chained_count
    from first_cutoff
    join candidates candidate on candidate.position >= first_cutoff.position
    left join raw_crawler.contents content
      on content.import_batch_id = candidate.import_batch_id
     and content.channel_id = candidate.channel_id
     and content.run_id = candidate.run_id
     and content.source_content_id = candidate.source_content_id
    left join valid_cutoffs valid_cutoff on valid_cutoff.candidate_id = candidate.candidate_id
    left join valid_upcoming valid_upcoming_candidate
      on valid_upcoming_candidate.candidate_id = candidate.candidate_id
  )
  select jsonb_build_object(
    'status', 'complete',
    'windowDays', 90,
    'proofVersion', 'chronological-age-cutoff-v3',
    'source', 'raw_crawler.content_candidates',
    'runId', p_run_id,
    'candidateCount', shape.candidate_count,
    'expectedContentCount', source_run.expected_content_count,
    'classifiedContentCount', source_run.classified_content_count,
    'materializedContentCount', content_shape.content_count,
    'finalizedStatus', 'ready_auto',
    'dataComplete', true,
    'cutoffVideoId', first_cutoff.source_content_id,
    'cutoffPosition', first_cutoff.position,
    'cutoffAgeDays', case
      when first_cutoff.result_json #>> '{scope,age_days}' ~ '^[0-9]{1,9}$'
        then (first_cutoff.result_json #>> '{scope,age_days}')::integer
    end,
    'postCutoffMaterializedCount', post_cutoff_shape.materialized_count,
    'postCutoffOlderCount', post_cutoff_shape.older_count,
    'postCutoffUpcomingCount', post_cutoff_shape.upcoming_count,
    'postCutoffChainedCount', post_cutoff_shape.chained_count
  )
  from first_cutoff
  cross join source_run
  cross join candidate_shape shape
  cross join content_shape
  cross join linked_shape
  cross join post_cutoff_shape
  where shape.candidate_count > 0
    and shape.candidate_count = shape.distinct_positions
    and shape.min_position = 1
    and shape.max_position = shape.candidate_count
    and shape.candidate_count = source_run.candidate_count
    and source_run.expected_content_count = source_run.quality_expected_content_count
    and source_run.expected_content_count = source_run.classified_content_count
    and source_run.expected_content_count = content_shape.content_count
    and source_run.expected_content_count = linked_shape.linked_content_count
    and source_run.expected_content_count = linked_shape.linked_candidate_count
    and content_shape.invalid_publication_count = 0
    and not exists (
      select 1
      from raw_crawler.content_candidates candidate
      where candidate.import_batch_id = p_import_batch_id
        and candidate.channel_id = p_channel_id
        and candidate.run_id = p_run_id
        and candidate.position is null
    )
    and not exists (
      select 1
      from candidates before_cutoff
      where before_cutoff.position < first_cutoff.position
        and not exists (
          select 1
          from valid_upcoming upcoming
          where upcoming.candidate_id = before_cutoff.candidate_id
        )
        and not exists (
          select 1
          from raw_crawler.contents content
          where content.import_batch_id = before_cutoff.import_batch_id
            and content.channel_id = before_cutoff.channel_id
            and content.run_id = before_cutoff.run_id
            and content.source_content_id = before_cutoff.source_content_id
        )
    )
    and not exists (
      select 1
      from candidates after_cutoff
      join raw_crawler.contents content
        on content.import_batch_id = after_cutoff.import_batch_id
       and content.channel_id = after_cutoff.channel_id
       and content.run_id = after_cutoff.run_id
       and content.source_content_id = after_cutoff.source_content_id
      where after_cutoff.position >= first_cutoff.position
        and (
          content.published_at is null
          or content.published_at_status is distinct from 'exact'
          or content.published_at_precision is null
          or content.published_at_precision not in ('second', 'date_only')
          or case content.published_at_precision
            when 'second' then content.published_at > source_run.captured_at - interval '90 days'
            when 'date_only' then (content.published_at at time zone 'UTC')::date
              > (source_run.captured_at at time zone 'UTC')::date - 90
            else true
          end
        )
    )
    and not exists (
      select 1
      from candidates after_cutoff
      where after_cutoff.position >= first_cutoff.position
        and not exists (
          select 1
          from raw_crawler.contents content
          where content.import_batch_id = after_cutoff.import_batch_id
            and content.channel_id = after_cutoff.channel_id
            and content.run_id = after_cutoff.run_id
            and content.source_content_id = after_cutoff.source_content_id
        )
        and not (
          exists (
            select 1 from valid_cutoffs cutoff
            where cutoff.candidate_id = after_cutoff.candidate_id
          )
          or exists (
            select 1 from valid_upcoming upcoming
            where upcoming.candidate_id = after_cutoff.candidate_id
          )
          or (
            after_cutoff.result_json #>> '{scope,status}' = 'excluded'
            and after_cutoff.result_json #>> '{scope,reason}' = 'after_chronological_age_cutoff'
            and after_cutoff.result_json #>> '{scope,source}' = 'uploads_playlist_order'
            and after_cutoff.result_json #>> '{scope,max_age_days}' = '90'
            and case
              when after_cutoff.result_json #>> '{scope,cutoff_age_days}' ~ '^[0-9]{1,9}$'
                then (after_cutoff.result_json #>> '{scope,cutoff_age_days}')::integer
            end > 90
            and exists (
              select 1
              from valid_cutoffs referenced_cutoff
              where referenced_cutoff.position < after_cutoff.position
                and referenced_cutoff.source_content_id = after_cutoff.result_json #>> '{scope,cutoff_video_id}'
                and case
                  when referenced_cutoff.result_json #>> '{scope,age_days}' ~ '^[0-9]{1,9}$'
                    then (referenced_cutoff.result_json #>> '{scope,age_days}')::integer
                end = case
                  when after_cutoff.result_json #>> '{scope,cutoff_age_days}' ~ '^[0-9]{1,9}$'
                    then (after_cutoff.result_json #>> '{scope,cutoff_age_days}')::integer
                end
            )
          )
        )
    )
$_$;


--
-- Name: FUNCTION content_window_proof_v1(p_import_batch_id text, p_channel_id text, p_run_id text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.content_window_proof_v1(p_import_batch_id text, p_channel_id text, p_run_id text) IS 'Returns v3 90-day completeness evidence only when all raw contents have usable exact or date-only publication dates and every pre/post-cutoff candidate is materialized or has a fully validated exclusion chain.';


--
-- Name: enforce_confirmed_creator_category_assignment(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_confirmed_creator_category_assignment() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
  claim_status text;
  category_is_assignable boolean;
  taxonomy_version_status text;
begin
  select claim.status, node.is_assignable, version_row.status
  into claim_status, category_is_assignable, taxonomy_version_status
  from creator_category_claims claim
  join creator_taxonomy_nodes node
    on node.version_id = claim.taxonomy_version_id
   and node.category_id = claim.category_id
  join creator_taxonomy_versions version_row on version_row.id = node.version_id
  where claim.id = new.claim_id
  for share of version_row;

  if claim_status is distinct from 'accepted' then
    raise exception 'confirmed category assignment requires an accepted claim; claim % has status %',
      new.claim_id, coalesce(claim_status, '<missing>');
  end if;
  if category_is_assignable is distinct from true then
    raise exception 'confirmed category assignment requires an assignable taxonomy node';
  end if;
  if taxonomy_version_status is distinct from 'active' then
    raise exception 'confirmed category assignment requires the active taxonomy version';
  end if;
  return new;
end;
$$;


--
-- Name: enforce_creator_category_retirement(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_creator_category_retirement() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if old.retired_at is not null and new.retired_at is distinct from old.retired_at then
    raise exception 'creator category retirement is irreversible';
  end if;

  if old.retired_at is null and new.retired_at is not null then
    perform version_row.id
    from creator_taxonomy_nodes node
    join creator_taxonomy_versions version_row on version_row.id = node.version_id
    where node.category_id = old.id
    order by version_row.id
    for share of version_row;

    if exists (
      select 1
      from creator_taxonomy_nodes node
      join creator_taxonomy_versions version_row on version_row.id = node.version_id
      where node.category_id = old.id and version_row.status = 'active'
    ) then
      raise exception 'cannot retire category % while an active taxonomy references it', old.category_key;
    end if;
  end if;

  return new;
end;
$$;


--
-- Name: enforce_creator_classification_run_input_scope(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_creator_classification_run_input_scope() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if cardinality(new.input_content_ids) <> (
    select count(distinct input_id)
    from unnest(new.input_content_ids) as input_id
  ) then
    raise exception 'classification run input_content_ids cannot contain duplicates';
  end if;

  if exists (
    select 1
    from unnest(new.input_content_ids) as input_id
    left join content_snapshots content
      on content.id = input_id
     and content.channel_snapshot_id = new.channel_snapshot_id
     and content.channel_id = new.channel_id
     and content.is_recent
     and content.is_canonical
     and content.content_kind in ('videos', 'shorts')
     and btrim(content.title) <> ''
    where content.id is null
  ) then
    raise exception 'classification run input content must be recent canonical long/short content from the same snapshot';
  end if;

  return new;
end;
$$;


--
-- Name: enforce_creator_content_tag_evidence_input(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_creator_content_tag_evidence_input() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if not exists (
    select 1
    from creator_classification_runs run
    where run.id = new.classification_run_id
      and new.content_snapshot_id = any(run.input_content_ids)
  ) then
    raise exception 'content tag evidence must belong to the classification run input';
  end if;
  return new;
end;
$$;


--
-- Name: enforce_creator_taxonomy_node_parent(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_creator_taxonomy_node_parent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
  parent_level smallint;
begin
  if new.level = 1 then
    return new;
  end if;

  select level into parent_level
  from creator_taxonomy_nodes
  where version_id = new.version_id and id = new.parent_node_id;

  if parent_level is null or parent_level <> new.level - 1 then
    raise exception 'taxonomy node % at level % requires a same-version parent at level %',
      new.id, new.level, new.level - 1;
  end if;
  return new;
end;
$$;


--
-- Name: enforce_creator_taxonomy_version_transition(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_creator_taxonomy_version_transition() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if old.status = new.status then
    return new;
  end if;

  if not (
    (old.status = 'draft' and new.status = 'active')
    or (old.status = 'active' and new.status = 'retired')
  ) then
    raise exception 'invalid creator taxonomy version transition: % to %', old.status, new.status;
  end if;

  if old.status = 'draft' and new.status = 'active' then
    if new.activated_at is null then
      raise exception 'activating taxonomy version % requires activated_at', new.version_key;
    end if;
    if (select count(*) from creator_taxonomy_nodes where version_id = new.id) <> new.node_count then
      raise exception 'taxonomy version % node_count does not match its catalog', new.version_key;
    end if;
    if exists (
      select 1
      from creator_taxonomy_nodes node
      join creator_categories category on category.id = node.category_id
      where node.version_id = new.id and category.retired_at is not null
    ) then
      raise exception 'taxonomy version % references a retired category identity', new.version_key;
    end if;
    if exists (
      select 1
      from creator_taxonomy_nodes node
      left join creator_taxonomy_nodes parent
        on parent.version_id = node.version_id and parent.id = node.parent_node_id
      where node.version_id = new.id
        and (
          (node.level = 1 and node.parent_node_id is not null)
          or (node.level > 1 and (parent.id is null or parent.level <> node.level - 1))
        )
    ) then
      raise exception 'taxonomy version % has an invalid parent/level relationship', new.version_key;
    end if;
    if exists (
      select 1
      from creator_taxonomy_nodes node
      left join creator_taxonomy_labels label_row on label_row.node_id = node.id
      where node.version_id = new.id
      group by node.id
      having count(label_row.id) <> 2
         or count(*) filter (where label_row.locale = 'zh-CN') <> 1
         or count(*) filter (where label_row.locale = 'en') <> 1
    ) then
      raise exception 'taxonomy version % requires exactly one Chinese and one English label per node', new.version_key;
    end if;
    if exists (
      with recursive expected_closure as (
        select node.id as ancestor_node_id,
               node.id as descendant_node_id,
               0::smallint as depth
        from creator_taxonomy_nodes node
        where node.version_id = new.id

        union all

        select current_node.parent_node_id,
               expected_closure.descendant_node_id,
               (expected_closure.depth + 1)::smallint
        from expected_closure
        join creator_taxonomy_nodes current_node
          on current_node.version_id = new.id
         and current_node.id = expected_closure.ancestor_node_id
        where current_node.parent_node_id is not null
      )
      select 1
      from (
        (
          select ancestor_node_id, descendant_node_id, depth
          from expected_closure
          except
          select ancestor_node_id, descendant_node_id, depth
          from creator_taxonomy_closure
          where version_id = new.id
        )
        union all
        (
          select ancestor_node_id, descendant_node_id, depth
          from creator_taxonomy_closure
          where version_id = new.id
          except
          select ancestor_node_id, descendant_node_id, depth
          from expected_closure
        )
      ) closure_difference
    ) then
      raise exception 'taxonomy version % closure does not match its parent hierarchy', new.version_key;
    end if;
  end if;

  if old.status = 'active' and new.status = 'retired' and exists (
    select 1
    from creator_category_assignments
    where taxonomy_version_id = old.id and valid_until is null
  ) then
    raise exception 'cannot retire taxonomy version % while confirmed assignments remain active', old.version_key;
  end if;

  return new;
end;
$$;


--
-- Name: extract_channel_description_links(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.extract_channel_description_links(value text) RETURNS TABLE(link_type text, url text, title text, raw_value text, purpose text, confidence text, source_context text)
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog', 'public'
    AS $$
  select extracted.*
  from public.extract_channel_description_links_v2_impl(
    public.strip_channel_link_format_chars_v1(value)
  ) extracted
$$;


--
-- Name: FUNCTION extract_channel_description_links(value text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.extract_channel_description_links(value text) IS 'Extracts auditable public links after deterministic Unicode-format normalization.';


--
-- Name: extract_channel_description_links_v1_impl(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.extract_channel_description_links_v1_impl(value text) RETURNS TABLE(link_type text, url text, title text, raw_value text, purpose text, confidence text, source_context text)
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
  with input as (
    select coalesce(value, '') body
  ), split_lines as (
    select line, ordinality line_number
    from input
    cross join lateral regexp_split_to_table(body, E'\n') with ordinality source(line, ordinality)
  ), lines as (
    select
      line,
      line_number,
      left(concat_ws(
        E'\n',
        lag(line) over (order by line_number),
        line,
        lead(line) over (order by line_number)
      ), 500) source_context
    from split_lines
  ), email_matches as (
    select
      'email'::text link_type,
      'mailto:' || lower(m.match[1]) url,
      lower(m.match[1]) title,
      m.match[1] raw_value,
      case
        when lower(lines.line) ~ '(^|[^[:alpha:]])(pix|paypal|vakinha|donat[[:alpha:]]*|doa[cç][[:alpha:]]*|coffee|apoie[[:alpha:]]*|d[ií]zim[[:alpha:]]*|contribui[[:alpha:]]*|credits?|affiliate|afiliad[[:alpha:]]*|support[[:space:]]+(this|the|our|my)[[:space:]]+(work|channel|canal|project|content)|support[[:space:]]+(me|us))([^[:alpha:]]|$)'
          then 'public_reference'
        when lower(lines.line) ~ '(contat|contact|business|commercial|comercial|inquir|e-?mail|email|mail[[:space:]]*:|mailto:|parce|partner|partnership|collab|colab|sponsor|publicidad|publicidade|propostas?[[:space:]]+de[[:space:]]+trabalho|trabalho|contrat|neg[oó]ci|reach|connect|fale|falar|escrev|write|questions?|d[uú]vid|need[[:space:]]+help|soporte[[:space:]]+t[eé]cnico|produ[cç][aã]o|assessoria|or[cç]ament|servi[cç]|shows?|booking|agend|story|relatos?|sugest|ouvidoria|atendimento|copyright|direitos[[:space:]]+autorais|escrit[oó]rio|say[[:space:]]+hello|📧|✉|💌|📩|📨|𝐂𝐨𝐧𝐭𝐚𝐭𝐨|𝐜𝐨𝐧𝐭𝐚𝐭𝐨|문의|메일|ご質問|お問い合わせ|ご依頼|お仕事|コラボ)'
          then 'contact'
        when lower(lines.source_context) ~ '(^|[^[:alpha:]])(pix|paypal|vakinha|donat[[:alpha:]]*|doa[cç][[:alpha:]]*|coffee|apoie[[:alpha:]]*|d[ií]zim[[:alpha:]]*|contribui[[:alpha:]]*|credits?|affiliate|afiliad[[:alpha:]]*|support[[:space:]]+(this|the|our|my)[[:space:]]+(work|channel|canal|project|content)|support[[:space:]]+(me|us))([^[:alpha:]]|$)'
          then 'public_reference'
        when lower(lines.source_context) ~ '(contat|contact|business|commercial|comercial|inquir|e-?mail|email|mail[[:space:]]*:|mailto:|parce|partner|partnership|collab|colab|sponsor|publicidad|publicidade|propostas?[[:space:]]+de[[:space:]]+trabalho|trabalho|contrat|neg[oó]ci|reach|connect|fale|falar|escrev|write|questions?|d[uú]vid|need[[:space:]]+help|soporte[[:space:]]+t[eé]cnico|produ[cç][aã]o|assessoria|or[cç]ament|servi[cç]|shows?|booking|agend|story|relatos?|sugest|ouvidoria|atendimento|copyright|direitos[[:space:]]+autorais|escrit[oó]rio|say[[:space:]]+hello|📧|✉|💌|📩|📨|𝐂𝐨𝐧𝐭𝐚𝐭𝐨|𝐜𝐨𝐧𝐭𝐚𝐭𝐨|문의|메일|ご質問|お問い合わせ|ご依頼|お仕事|コラボ)'
          then 'contact'
        else 'public_reference'
      end purpose,
      'high'::text confidence,
      lines.source_context,
      1 source_rank
    from lines
    cross join lateral regexp_matches(
      line,
      '([[:alnum:]_.%+-]+@[[:alnum:].-]+\.[[:alpha:]]{2,})',
      'gi'
    ) m(match)
  ), explicit_url_matches as (
    select
      case when lower(clean.raw_value) like 'www.%'
        then 'https://' || clean.raw_value
        else clean.raw_value
      end url,
      clean.raw_value,
      lines.source_context,
      2 source_rank
    from lines
    cross join lateral regexp_matches(
      line,
      '(https?://[[:graph:]]+|www\.[[:graph:]]+)',
      'gi'
    ) m(match)
    cross join lateral (
      select rtrim(m.match[1], $strip$.,;:!?)]}>"'$strip$) raw_value
    ) clean
    where clean.raw_value <> ''
  ), labeled_domain_matches as (
    select
      'https://' || clean.raw_value url,
      clean.raw_value,
      lines.source_context,
      3 source_rank
    from lines
    cross join lateral regexp_matches(
      line,
      '(site|website|web|go[[:space:]]+to|acesse|visite|visit|contato|contact)[[:space:]]*[:=-]?[[:space:]]+(([[:alnum:]-]+\.)+(com\.br|com|org|net|edu|gov|io|co|tv|me|info|site|online|app|ai|br|pt|jp)(/[[:graph:]]*)?)($|[^[:alnum:].-])',
      'gi'
    ) m(match)
    cross join lateral (
      select rtrim(m.match[2], $strip$.,;:!?)]}>"'$strip$) raw_value
    ) clean
    where clean.raw_value <> ''
  ), url_matches as (
    select
      channel_link_type_v1(url) link_type,
      url,
      raw_value title,
      raw_value,
      case
        when lower(source_context) ~ '(^|[^[:alpha:]])(pix|donat[[:alpha:]]*|doa[cç][[:alpha:]]*|coffee|apoie|support|credits?|design|art|arte|rig|logo|affiliate|afiliad[[:alpha:]]*|loja|shop|compr[[:alpha:]]*)([^[:alpha:]]|$)'
          then 'public_reference'
        when lower(source_context) ~ '(contat|contact|business|commercial|comercial|inquir|e-?mail|email|whats|telefone|phone)'
          then 'contact'
        when channel_link_type_v1(url) in ('instagram', 'facebook', 'x_twitter', 'tiktok', 'whatsapp', 'telegram')
          and lower(source_context) ~ '(instagram|facebook|twitter|tiktok|tik[[:space:]]+tok|whatsapp|telegram)'
          then 'contact'
        when lower(source_context) ~ '(^|[^[:alpha:]])(site|website)([^[:alpha:]]|$)'
          then 'contact'
        else 'public_reference'
      end purpose,
      'high'::text confidence,
      source_context,
      source_rank
    from (
      select * from explicit_url_matches
      union all
      select * from labeled_domain_matches
    ) matched
  ), social_handle_matches as (
    select
      case lower(regexp_replace(m.match[1], '[[:space:]]+', '', 'g'))
        when 'instagram' then 'instagram'
        when 'insta' then 'instagram'
        when 'ig' then 'instagram'
        when 'tiktok' then 'tiktok'
        when 'tik tok' then 'tiktok'
        when 'twitter' then 'x_twitter'
        when 'x/twitter' then 'x_twitter'
        when 'facebook' then 'facebook'
        when 'fb' then 'facebook'
      end link_type,
      case lower(regexp_replace(m.match[1], '[[:space:]]+', '', 'g'))
        when 'instagram' then 'https://www.instagram.com/' || clean.handle
        when 'insta' then 'https://www.instagram.com/' || clean.handle
        when 'ig' then 'https://www.instagram.com/' || clean.handle
        when 'tiktok' then 'https://www.tiktok.com/@' || clean.handle
        when 'tik tok' then 'https://www.tiktok.com/@' || clean.handle
        when 'twitter' then 'https://x.com/' || clean.handle
        when 'x/twitter' then 'https://x.com/' || clean.handle
        when 'facebook' then 'https://www.facebook.com/' || clean.handle
        when 'fb' then 'https://www.facebook.com/' || clean.handle
      end url,
      '@' || clean.handle title,
      m.match[1] || ' @' || clean.handle raw_value,
      case
        when lower(lines.source_context) ~ '(^|[^[:alpha:]])(credits?|design|art|arte|rig|logo|affiliate|afiliad[[:alpha:]]*)([^[:alpha:]]|$)'
          then 'public_reference'
        else 'contact'
      end purpose,
      'high'::text confidence,
      lines.source_context,
      4 source_rank
    from lines
    cross join lateral regexp_matches(
      line,
      '(instagram|insta|ig|tiktok|tik[[:space:]]+tok|twitter|x[[:space:]]*/[[:space:]]*twitter|facebook|fb)[[:space:]]*[:=-]?[[:space:]]+@([[:alnum:]_][[:alnum:]_.-]{0,62}[[:alnum:]_])',
      'gi'
    ) m(match)
    cross join lateral (
      select regexp_replace(m.match[2], '[.-]+$', '', 'g') handle
    ) clean
    where clean.handle <> ''
  ), labeled_phone_matches as (
    select
      case when lower(m.match[1]) in ('whatsapp', 'whats', 'zap') and normalized.has_country_code
        then 'whatsapp'
        else 'phone'
      end link_type,
      case when lower(m.match[1]) in ('whatsapp', 'whats', 'zap') and normalized.has_country_code
        then 'https://wa.me/' || normalized.digits
        else 'tel:' || normalized.phone
      end url,
      normalized.phone title,
      m.match[1] || ': ' || m.match[2] raw_value,
      'contact'::text purpose,
      case when normalized.has_country_code then 'high' else 'medium' end confidence,
      lines.source_context,
      5 source_rank
    from lines
    cross join lateral regexp_matches(
      line,
      '(whatsapp|whats|zap|telefone|phone|tel|contato|contact|📞|☎[️]?)[[:space:]]*[:=-]?[[:space:]]*(\+?[0-9(][0-9 ()-]{6,}[0-9])',
      'gi'
    ) m(match)
    cross join lateral (
      select
        regexp_replace(m.match[2], '[^0-9]', '', 'g') digits,
        case when left(btrim(m.match[2]), 1) = '+' then '+' else '' end
          || regexp_replace(m.match[2], '[^0-9]', '', 'g') phone,
        left(btrim(m.match[2]), 1) = '+' has_country_code
    ) normalized
    where length(normalized.digits) between 8 and 15
  ), candidates as (
    select * from email_matches
    union all
    select * from url_matches
    union all
    select * from social_handle_matches
    union all
    select * from labeled_phone_matches
  )
  select distinct on (lower(candidates.url))
    candidates.link_type,
    candidates.url,
    candidates.title,
    candidates.raw_value,
    candidates.purpose,
    candidates.confidence,
    candidates.source_context
  from candidates
  where candidates.link_type is not null
    and candidates.url is not null
  order by lower(candidates.url),
    case candidates.purpose when 'contact' then 0 else 1 end,
    candidates.source_rank,
    candidates.source_context,
    candidates.raw_value
$_$;


--
-- Name: FUNCTION extract_channel_description_links_v1_impl(value text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.extract_channel_description_links_v1_impl(value text) IS 'Extracts explicit public emails, URLs, domains and labeled social handles from the preserved channel description.';


--
-- Name: extract_channel_description_links_v2_impl(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.extract_channel_description_links_v2_impl(value text) RETURNS TABLE(link_type text, url text, title text, raw_value text, purpose text, confidence text, source_context text)
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
  select
    extracted.link_type,
    extracted.url,
    extracted.title,
    extracted.raw_value,
    case
      when extracted.link_type = 'email'
        and extracted.purpose = 'public_reference'
        and lower(extracted.source_context) ~
          '(consultas?|patroc[ií]ni[oa]|fala[[:space:]]+comigo)'
        and lower(extracted.source_context) !~
          '(^|[^[:alpha:]])(pix|paypal|vakinha|donat[[:alpha:]]*|doa[cç][[:alpha:]]*|coffee|apoie[[:alpha:]]*|d[ií]zim[[:alpha:]]*|contribui[[:alpha:]]*|support[[:space:]]+(me|us))([^[:alpha:]]|$)'
        then 'contact'
      else extracted.purpose
    end purpose,
    extracted.confidence,
    extracted.source_context
  from public.extract_channel_description_links_v1_impl(value) extracted
$_$;


--
-- Name: FUNCTION extract_channel_description_links_v2_impl(value text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.extract_channel_description_links_v2_impl(value text) IS 'Extracts auditable public contact identities and references from a channel description, including Portuguese business-intent phrases.';


--
-- Name: extract_channel_header_external_links(jsonb); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.extract_channel_header_external_links(value jsonb) RETURNS TABLE(link_type text, url text, title text, raw_value text, purpose text, confidence text, source_context text, source_position bigint)
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
  with items as (
    select
      item.value,
      item.ordinality source_position
    from jsonb_array_elements(
      case
        when jsonb_typeof(value) = 'array' then value
        else '[]'::jsonb
      end
    ) with ordinality item(value, ordinality)
    where jsonb_typeof(item.value) = 'object'
  ), raw_values as (
    select
      nullif(btrim(coalesce(value ->> 'url', value ->> 'display_url')), '') raw_value,
      nullif(btrim(value ->> 'title'), '') raw_title,
      source_position
    from items
  ), normalized as (
    select
      raw_value,
      raw_title,
      source_position,
      case
        when raw_value ~* '^[[:alnum:]_.%+-]+@[[:alnum:].-]+\.[[:alpha:]]{2,}$'
          then 'mailto:' || lower(raw_value)
        when raw_value ~* '^https?://'
          then raw_value
        when raw_value ~* '^www\.'
          then 'https://' || raw_value
        when raw_value ~* '^[[:alnum:]][[:alnum:].-]*\.[[:alpha:]]{2,}([/:?#]|$)'
          then 'https://' || raw_value
        else null
      end normalized_url
    from raw_values
    where raw_value is not null
  ), valid as (
    select
      public.channel_link_type_v1(normalized_url) link_type,
      normalized_url url,
      coalesce(raw_title, raw_value) title,
      raw_value,
      raw_title,
      source_position,
      left(concat_ws(' · ', raw_title, raw_value), 500) source_context
    from normalized
    where normalized_url like 'mailto:%'
       or (
         normalized_url ~*
           '^https?://[[:alnum:]][[:alnum:].-]*\.[[:alpha:]]{2,}(:[0-9]{1,5})?([/?#][^[:space:][:cntrl:]]*)?$'
         and normalized_url !~ '[[:space:][:cntrl:]]'
       )
  ), classified as (
    select
      link_type,
      url,
      title,
      raw_value,
      case
        when lower(source_context) ~
          '(^|[^[:alpha:]])(pix|paypal|vakinha|donat[[:alpha:]]*|doa[cç][[:alpha:]]*|coffee|apoie[[:alpha:]]*|contribui[[:alpha:]]*|loja|shop|compr[[:alpha:]]*)([^[:alpha:]]|$)'
          then 'public_reference'
        when link_type in (
          'email', 'instagram', 'facebook', 'x_twitter',
          'tiktok', 'whatsapp', 'telegram'
        )
          then 'contact'
        when lower(coalesce(raw_title, '')) ~
          '(contat|contact|business|commercial|comercial|inquir|e-?mail|email|parce|partner|collab|colab|sponsor|patroc|fale|suporte|support|atendimento)'
          then 'contact'
        else 'public_reference'
      end purpose,
      'high'::text confidence,
      source_context,
      source_position
    from valid
  ), ranked as (
    select
      classified.*,
      row_number() over (
        partition by link_type, lower(url)
        order by source_position, title
      ) duplicate_rank
    from classified
  )
  select
    link_type,
    url,
    title,
    raw_value,
    purpose,
    confidence,
    source_context,
    source_position
  from ranked
  where duplicate_rank = 1
  order by source_position, link_type, lower(url)
$_$;


--
-- Name: FUNCTION extract_channel_header_external_links(value jsonb); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.extract_channel_header_external_links(value jsonb) IS 'Safely normalizes structured YouTube About external links and preserves contact/public-reference intent.';


--
-- Name: normalize_channel_snapshot_verified_status(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.normalize_channel_snapshot_verified_status() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
BEGIN
  IF NEW.is_verified_status='unknown' AND NEW.is_verified IS NOT NULL THEN
    NEW.is_verified_status=CASE WHEN NEW.is_verified THEN 'verified' ELSE 'not_verified' END;
  END IF;
  RETURN NEW;
END
$$;


--
-- Name: normalize_country_code(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.normalize_country_code(value text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog'
    AS $$
  select case lower(btrim(value))
    when '' then null
    when 'unknown' then null
    when 'unavailable' then null
    when 'not available' then null
    when 'n/a' then null
    when 'us' then 'US'
    when 'usa' then 'US'
    when 'united states' then 'US'
    when 'united states of america' then 'US'
    when 'estados unidos' then 'US'
    when 'br' then 'BR'
    when 'brasil' then 'BR'
    when 'brazil' then 'BR'
    when 'in' then 'IN'
    when 'india' then 'IN'
    when 'índia' then 'IN'
    when 'es' then 'ES'
    when 'spain' then 'ES'
    when 'espanha' then 'ES'
    when 'españa' then 'ES'
    when 'gb' then 'GB'
    when 'uk' then 'GB'
    when 'united kingdom' then 'GB'
    when 'great britain' then 'GB'
    when 'reino unido' then 'GB'
    when 'inglaterra' then 'GB'
    when 'de' then 'DE'
    when 'germany' then 'DE'
    when 'alemanha' then 'DE'
    when 'deutschland' then 'DE'
    when 'pt' then 'PT'
    when 'portugal' then 'PT'
    when 'jp' then 'JP'
    when 'japan' then 'JP'
    when 'japão' then 'JP'
    when 'japao' then 'JP'
    when 'mx' then 'MX'
    when 'mexico' then 'MX'
    when 'méxico' then 'MX'
    when 'cn' then 'CN'
    when 'china' then 'CN'
    when 'kr' then 'KR'
    when 'south korea' then 'KR'
    when 'republic of korea' then 'KR'
    when 'fr' then 'FR'
    when 'france' then 'FR'
    when 'frança' then 'FR'
    when 'au' then 'AU'
    when 'australia' then 'AU'
    when 'austrália' then 'AU'
    when 'ca' then 'CA'
    when 'canada' then 'CA'
    when 'canadá' then 'CA'
    when 'jo' then 'JO'
    when 'jordan' then 'JO'
    when 'ru' then 'RU'
    when 'russia' then 'RU'
    when 'it' then 'IT'
    when 'italy' then 'IT'
    when 'ar' then 'AR'
    when 'argentina' then 'AR'
    when 'sg' then 'SG'
    when 'singapore' then 'SG'
    when 'singapura' then 'SG'
    when 'ao' then 'AO'
    when 'angola' then 'AO'
    when 'cv' then 'CV'
    when 'cabo verde' then 'CV'
    when 'cl' then 'CL'
    when 'chile' then 'CL'
    when 'gh' then 'GH'
    when 'ghana' then 'GH'
    when 'id' then 'ID'
    when 'indonesia' then 'ID'
    when 'indonésia' then 'ID'
    when 'ie' then 'IE'
    when 'ireland' then 'IE'
    when 'irlanda' then 'IE'
    when 'lb' then 'LB'
    when 'lebanon' then 'LB'
    when 'líbano' then 'LB'
    when 'nl' then 'NL'
    when 'netherlands' then 'NL'
    when 'países baixos' then 'NL'
    when 'se' then 'SE'
    when 'sweden' then 'SE'
    when 'th' then 'TH'
    when 'thailand' then 'TH'
    when 'tr' then 'TR'
    when 'turkey' then 'TR'
    when 'türkiye' then 'TR'
    when 'bh' then 'BH'
    when 'bahrain' then 'BH'
    else null
  end
$$;


--
-- Name: FUNCTION normalize_country_code(value text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.normalize_country_code(value text) IS 'Controlled creator-filter country aliases; unsupported non-empty values fail crawler publication.';


--
-- Name: normalize_creator_search_channel_observation_times(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.normalize_creator_search_channel_observation_times() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
BEGIN
  SELECT
    snapshot.channel_observed_at,
    snapshot.subscriber_count_observed_at,
    snapshot.total_view_count_observed_at,
    snapshot.video_count_observed_at,
    snapshot.youtube_business_email_available,
    snapshot.youtube_business_email_observed_at
  INTO
    NEW.channel_observed_at,
    NEW.subscribers_observed_at,
    NEW.total_views_observed_at,
    NEW.channel_video_count_observed_at,
    NEW.youtube_business_email_available,
    NEW.youtube_business_email_observed_at
  FROM public.channel_snapshots snapshot
  WHERE snapshot.id=NEW.snapshot_id
    AND snapshot.channel_id=NEW.channel_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search row requires its canonical Channel Snapshot: %/%',
      NEW.channel_id,NEW.snapshot_id;
  END IF;
  RETURN NEW;
END
$$;


--
-- Name: normalize_creator_search_verified_status(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.normalize_creator_search_verified_status() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
BEGIN
  IF NEW.verified_status='unknown' AND NEW.verified IS NOT NULL THEN
    NEW.verified_status=CASE WHEN NEW.verified THEN 'verified' ELSE 'not_verified' END;
  END IF;
  RETURN NEW;
END
$$;


--
-- Name: normalize_language_code(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.normalize_language_code(value text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog'
    AS $$
  select case lower(btrim(value))
    when '' then null
    when 'unknown' then null
    when 'unavailable' then null
    when 'not available' then null
    when 'n/a' then null
    when 'en' then 'en'
    when 'english' then 'en'
    when 'pt' then 'pt'
    when 'portuguese' then 'pt'
    when 'portugues' then 'pt'
    when 'português' then 'pt'
    when 'es' then 'es'
    when 'spanish' then 'es'
    when 'español' then 'es'
    when 'hi' then 'hi'
    when 'hindi' then 'hi'
    when 'de' then 'de'
    when 'german' then 'de'
    when 'deutsch' then 'de'
    when 'ja' then 'ja'
    when 'japanese' then 'ja'
    when '日本語' then 'ja'
    when 'zh' then 'zh'
    when 'chinese' then 'zh'
    when 'mandarin' then 'zh'
    when 'mandarin chinese' then 'zh'
    when 'ko' then 'ko'
    when 'korean' then 'ko'
    when 'fr' then 'fr'
    when 'french' then 'fr'
    when 'ar' then 'ar'
    when 'arabic' then 'ar'
    when 'ru' then 'ru'
    when 'russian' then 'ru'
    when 'it' then 'it'
    when 'italian' then 'it'
    when 'nl' then 'nl'
    when 'dutch' then 'nl'
    when 'id' then 'id'
    when 'indonesian' then 'id'
    when 'sv' then 'sv'
    when 'swedish' then 'sv'
    when 'th' then 'th'
    when 'thai' then 'th'
    when 'tr' then 'tr'
    when 'turkish' then 'tr'
    else null
  end
$$;


--
-- Name: FUNCTION normalize_language_code(value text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.normalize_language_code(value text) IS 'Maps every supported creator/audience language label to the controlled filter vocabulary; unsupported labels are null.';


--
-- Name: prevent_assigned_claim_status_reversal(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.prevent_assigned_claim_status_reversal() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if old.status = 'accepted' and new.status <> 'accepted' and exists (
    select 1 from creator_category_assignments where claim_id = new.id
  ) then
    raise exception 'accepted claim % backs a confirmed assignment and cannot change status', new.id;
  end if;
  return new;
end;
$$;


--
-- Name: protect_creator_category_assignment_history(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_creator_category_assignment_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if tg_op = 'DELETE' then
    if exists (select 1 from channels where channel_id = old.channel_id) then
      raise exception 'category assignments can only be deleted through the parent channel lifecycle';
    end if;
    return old;
  end if;
  if old.channel_id is distinct from new.channel_id
     or old.taxonomy_version_id is distinct from new.taxonomy_version_id
     or old.category_id is distinct from new.category_id
     or old.claim_id is distinct from new.claim_id
     or old.is_primary is distinct from new.is_primary
     or old.valid_from is distinct from new.valid_from
     or old.assigned_by is distinct from new.assigned_by
     or old.assignment_note is distinct from new.assignment_note
     or old.created_at is distinct from new.created_at
     or old.valid_until is not null
     or new.valid_until is null then
    raise exception 'category assignments are append-only except for one-way closure';
  end if;
  return new;
end;
$$;


--
-- Name: protect_creator_category_key(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_creator_category_key() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if old.category_key is distinct from new.category_key then
    raise exception 'creator category keys are immutable';
  end if;
  return new;
end;
$$;


--
-- Name: protect_creator_content_tag_evidence_history(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_creator_content_tag_evidence_history() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
  claim_status text;
begin
  if tg_op = 'DELETE' then
    if exists (select 1 from channels where channel_id = old.channel_id) then
      raise exception 'content tag evidence can only be deleted through the parent channel lifecycle';
    end if;
    return old;
  end if;
  if tg_op = 'UPDATE' then
    raise exception 'content tag evidence is immutable; append a new claim instead';
  end if;

  select status into claim_status
  from creator_content_tag_claims
  where id = new.tag_claim_id;
  if claim_status is distinct from 'proposed' then
    raise exception 'content tag evidence can only be added while its claim is proposed';
  end if;
  return new;
end;
$$;


--
-- Name: protect_published_creator_taxonomy_catalog(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_published_creator_taxonomy_catalog() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
  old_version_id bigint;
  new_version_id bigint;
begin
  if tg_table_name = 'creator_taxonomy_labels' then
    if tg_op <> 'INSERT' then
      select version_id into old_version_id
      from creator_taxonomy_nodes
      where id = old.node_id;
    end if;
    if tg_op <> 'DELETE' then
      select version_id into new_version_id
      from creator_taxonomy_nodes
      where id = new.node_id;
    end if;
  else
    if tg_op <> 'INSERT' then
      old_version_id := old.version_id;
    end if;
    if tg_op <> 'DELETE' then
      new_version_id := new.version_id;
    end if;
  end if;

  perform id
  from creator_taxonomy_versions
  where id in (old_version_id, new_version_id)
  order by id
  for share;

  if exists (
    select 1
    from creator_taxonomy_versions
    where id in (old_version_id, new_version_id)
      and status in ('active', 'retired')
  ) then
    raise exception 'published creator taxonomy versions are immutable; create a new draft version';
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$;


--
-- Name: protect_published_creator_taxonomy_version(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_published_creator_taxonomy_version() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if old.status in ('active', 'retired') then
    if tg_op = 'DELETE' then
      raise exception 'published creator taxonomy versions cannot be deleted';
    end if;
    if old.version_key is distinct from new.version_key
       or old.source_zh_sha256 is distinct from new.source_zh_sha256
       or old.source_en_sha256 is distinct from new.source_en_sha256
       or old.identity_registry_sha256 is distinct from new.identity_registry_sha256
       or old.node_count is distinct from new.node_count
       or old.created_at is distinct from new.created_at
       or old.activated_at is distinct from new.activated_at then
      raise exception 'published creator taxonomy version metadata is immutable';
    end if;
  end if;

  if tg_op = 'DELETE' then
    return old;
  end if;
  return new;
end;
$$;


--
-- Name: protect_reviewed_creator_category_claim(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_reviewed_creator_category_claim() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if tg_op = 'DELETE' then
    if exists (select 1 from channels where channel_id = old.channel_id) then
      raise exception 'category claims can only be deleted through the parent channel lifecycle';
    end if;
    return old;
  end if;

  if old.status <> 'proposed' then
    raise exception 'reviewed category claims are immutable; append a new claim instead';
  end if;

  if (to_jsonb(new) - array['status', 'reviewed_by', 'review_note', 'reviewed_at'])
     is distinct from
     (to_jsonb(old) - array['status', 'reviewed_by', 'review_note', 'reviewed_at']) then
    raise exception 'proposed category claim payload is immutable; append a new claim instead';
  end if;

  return new;
end;
$$;


--
-- Name: protect_reviewed_creator_content_tag_claim(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.protect_reviewed_creator_content_tag_claim() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin
  if tg_op = 'DELETE' then
    if exists (select 1 from channels where channel_id = old.channel_id) then
      raise exception 'content tag claims can only be deleted through the parent channel lifecycle';
    end if;
    return old;
  end if;

  if old.status <> 'proposed' then
    raise exception 'reviewed content tag claims are immutable; append a new claim instead';
  end if;

  if (to_jsonb(new) - array['status', 'reviewed_by', 'review_note', 'reviewed_at'])
     is distinct from
     (to_jsonb(old) - array['status', 'reviewed_by', 'review_note', 'reviewed_at']) then
    raise exception 'proposed content tag claim payload is immutable; append a new claim instead';
  end if;

  return new;
end;
$$;


--
-- Name: prune_creator_search_legacy_history_v1(text, integer, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.prune_creator_search_legacy_history_v1(p_expected_active_watermark text, p_expected_delete_row_count integer, p_actor text, p_reason text) RETURNS integer
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  current_active_watermark TEXT;
  current_write_mode TEXT;
  current_read_mode TEXT;
  initialized_watermark TEXT;
  initialized_row_count INTEGER;
  retained_watermarks TEXT[];
  delete_row_count INTEGER;
  deleted_row_count INTEGER;
BEGIN
  IF NULLIF(btrim(p_expected_active_watermark),'') IS NULL
     OR p_expected_delete_row_count IS NULL OR p_expected_delete_row_count<0
     OR NULLIF(btrim(p_actor),'') IS NULL
     OR NULLIF(btrim(p_reason),'') IS NULL THEN
    RAISE EXCEPTION 'expected active watermark, non-negative count, actor, and reason are required';
  END IF;

  PERFORM pg_advisory_xact_lock(781137233);
  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  SELECT active.watermark,state.write_mode,state.read_mode,
         state.initialized_watermark,state.initialized_row_count
  INTO current_active_watermark,current_write_mode,current_read_mode,
       initialized_watermark,initialized_row_count
  FROM public.creator_search_active active
  CROSS JOIN publication.creator_search_storage_state state
  WHERE active.singleton=true AND state.singleton=true
  FOR UPDATE OF active,state;

  IF current_active_watermark IS DISTINCT FROM p_expected_active_watermark THEN
    RAISE EXCEPTION 'Creator Search active watermark changed before Legacy prune';
  END IF;
  IF current_write_mode<>'incremental' OR current_read_mode<>'live' THEN
    RAISE EXCEPTION 'Creator Search Legacy history can only be pruned in incremental/live mode';
  END IF;
  IF (
    SELECT count(*)::int FROM public.creator_search_current
    WHERE watermark=initialized_watermark
  )<>initialized_row_count THEN
    RAISE EXCEPTION 'Creator Search initialized Legacy fallback is incomplete';
  END IF;

  SELECT array_agg(watermark ORDER BY watermark)
  INTO retained_watermarks
  FROM (
    SELECT initialized_watermark AS watermark
    UNION
    SELECT active.watermark
    FROM public.creator_search_active active
    WHERE active.singleton=true
      AND EXISTS (
        SELECT 1 FROM public.creator_search_current search
        WHERE search.watermark=active.watermark
      )
    UNION
    SELECT cutover.previous_watermark
    FROM publication.projection_cutover cutover
    WHERE cutover.status='applied'
    UNION
    SELECT latest_shadow.watermark
    FROM (
      SELECT release.watermark
      FROM public.creator_search_releases release
      WHERE release.storage_mode='shadow'
        AND EXISTS (
          SELECT 1 FROM public.creator_search_current search
          WHERE search.watermark=release.watermark
        )
      ORDER BY release.created_at DESC,release.watermark DESC
      LIMIT 1
    ) latest_shadow
  ) retained
  WHERE watermark IS NOT NULL;

  IF retained_watermarks IS NULL OR NOT (initialized_watermark=ANY(retained_watermarks)) THEN
    RAISE EXCEPTION 'Creator Search Legacy prune has no initialized fallback retention';
  END IF;
  IF EXISTS (
    SELECT 1 FROM unnest(retained_watermarks) retained(watermark)
    WHERE NOT EXISTS (
      SELECT 1 FROM public.creator_search_current search
      WHERE search.watermark=retained.watermark
    )
  ) THEN
    RAISE EXCEPTION 'Creator Search retained Legacy release has no rows';
  END IF;

  SELECT count(*)::int INTO delete_row_count
  FROM public.creator_search_current
  WHERE NOT (watermark=ANY(retained_watermarks));
  IF delete_row_count<>p_expected_delete_row_count THEN
    RAISE EXCEPTION 'Creator Search Legacy prune row count differs from expectation: %',
      delete_row_count;
  END IF;

  DELETE FROM public.creator_search_current
  WHERE NOT (watermark=ANY(retained_watermarks));
  GET DIAGNOSTICS deleted_row_count = ROW_COUNT;
  IF deleted_row_count<>delete_row_count THEN
    RAISE EXCEPTION 'Creator Search Legacy prune deleted an unexpected row count';
  END IF;

  INSERT INTO publication.creator_search_legacy_prune_audit (
    active_watermark,retained_watermarks,deleted_row_count,pruned_by,prune_reason
  ) VALUES (
    current_active_watermark,retained_watermarks,delete_row_count,btrim(p_actor),btrim(p_reason)
  );
  RETURN delete_row_count;
END
$$;


--
-- Name: publication_in_utc_window_v1(timestamp with time zone, date, text, timestamp with time zone, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.publication_in_utc_window_v1(p_published_at timestamp with time zone, p_published_date date, p_status text, p_as_of timestamp with time zone, p_window_days integer) RETURNS boolean
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    RETURN CASE WHEN ((p_as_of IS NULL) OR (p_window_days IS NULL) OR (p_window_days <= 0)) THEN false WHEN (p_status = 'exact'::text) THEN ((p_published_at > (p_as_of - make_interval(days => p_window_days))) AND (p_published_at <= p_as_of)) WHEN (p_status = 'date_exact'::text) THEN ((p_published_date > (((p_as_of AT TIME ZONE 'UTC'::text))::date - p_window_days)) AND (p_published_date <= ((p_as_of AT TIME ZONE 'UTC'::text))::date)) ELSE false END;


--
-- Name: publication_sort_at_v1(timestamp with time zone, date, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.publication_sort_at_v1(p_published_at timestamp with time zone, p_published_date date, p_status text) RETURNS timestamp with time zone
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    RETURN CASE p_status WHEN 'exact'::text THEN p_published_at WHEN 'date_exact'::text THEN ((p_published_date)::timestamp without time zone AT TIME ZONE 'UTC'::text) ELSE NULL::timestamp with time zone END;


--
-- Name: refresh_creator_search_current(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_creator_search_current(p_channel_id text) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
declare
  target_watermark text;
begin
  select import_batch_id into target_watermark
  from channel_snapshots
  where channel_id = p_channel_id
  order by captured_at desc, id desc
  limit 1;
  if target_watermark is not null then
    perform refresh_creator_search_release(target_watermark, array[p_channel_id]);
  end if;
end $$;


--
-- Name: refresh_creator_search_release(text, text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_creator_search_release(p_watermark text, p_channel_ids text[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
begin
  perform refresh_creator_search_release_v4_impl(p_watermark, p_channel_ids);

  update creator_search_current projection
  set country = coalesce(
        normalize_country_code(snapshot.country_text),
        normalize_country_code(country_fact.value_json #>> '{}')
      ),
      country_source = case
        when normalize_country_code(snapshot.country_text) is not null then 'channel'
        when normalize_country_code(country_fact.value_json #>> '{}') is not null then 'agent'
        else 'unavailable'
      end,
      language = normalize_language_code(language_fact.value_json #>> '{}'),
      channel_video_count = snapshot.video_count,
      audience_country = normalize_country_code(projection.audience_country),
      audience_language = normalize_language_code(projection.audience_language),
      contact_types = contacts.value,
      publish_weekly = publication.publish_weekly,
      publish_monthly = publication.publish_monthly,
      search_text = lower(concat_ws(
        ' ', projection.name, projection.handle,
        coalesce(
          normalize_country_code(snapshot.country_text),
          normalize_country_code(country_fact.value_json #>> '{}')
        ),
        normalize_language_code(language_fact.value_json #>> '{}'),
        array_to_string(projection.category_paths, ' '),
        array_to_string(projection.tags, ' ')
      )),
      projection_version = 'creator-search-v6'
  from channel_snapshots snapshot
  left join channel_profile_facts country_fact
    on country_fact.channel_snapshot_id = snapshot.id
   and country_fact.field_key = 'country'
  left join channel_profile_facts language_fact
    on language_fact.channel_snapshot_id = snapshot.id
   and language_fact.field_key = 'creator_language'
  cross join lateral (
    select coalesce(
      array_agg(distinct link.link_type order by link.link_type)
        filter (
          where link.link_type is not null
            and coalesce(link.raw_link ->> 'purpose', 'contact') = 'contact'
        ),
      '{}'::text[]
    ) value
    from channel_links link
    where link.channel_snapshot_id = snapshot.id
  ) contacts
  cross join lateral snapshot_publication_stats_v2(snapshot.id) publication
  where projection.watermark = p_watermark
    and snapshot.id = projection.snapshot_id;
end $$;


--
-- Name: refresh_creator_search_release_v3_impl(text, text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_creator_search_release_v3_impl(p_watermark text, p_channel_ids text[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
declare
  target_watermark text := p_watermark;
  previous_watermark text;
begin
  if target_watermark is null or coalesce(array_length(p_channel_ids, 1), 0) = 0 then return; end if;

  insert into creator_search_releases(watermark, status)
  values (target_watermark, 'building')
  on conflict (watermark) do nothing;

  select watermark into previous_watermark from creator_search_active where singleton;
  if previous_watermark is not null and previous_watermark <> target_watermark then
    insert into creator_search_current(
      channel_id, snapshot_id, captured_at, name, handle, avatar_url, verified,
      country, language, category_paths, tags, subscribers, total_views,
      channel_video_count, content_sample_count, contact_types, last_published_at,
      last_published_date, content_count_30d, content_count_90d, publish_weekly,
      publish_monthly, avg_views, view_subscribers_ratio,
      engagement_rate_by_views, avg_likes, likes_subscribers_ratio, avg_comments,
      comments_subscribers_ratio, audience_country, audience_country_share,
      audience_language, audience_language_share, audience_gender,
      audience_gender_share, audience_age, audience_age_share,
      active_subscriber_ratio, search_text, projection_version, watermark
    )
    select
      channel_id, snapshot_id, captured_at, name, handle, avatar_url, verified,
      country, language, category_paths, tags, subscribers, total_views,
      channel_video_count, content_sample_count, contact_types, last_published_at,
      last_published_date, content_count_30d, content_count_90d, publish_weekly,
      publish_monthly, avg_views, view_subscribers_ratio,
      engagement_rate_by_views, avg_likes, likes_subscribers_ratio, avg_comments,
      comments_subscribers_ratio, audience_country, audience_country_share,
      audience_language, audience_language_share, audience_gender,
      audience_gender_share, audience_age, audience_age_share,
      active_subscriber_ratio, search_text, projection_version, target_watermark
    from creator_search_current
    where watermark = previous_watermark and not (channel_id = any(p_channel_ids))
    on conflict (watermark, channel_id) do nothing;
  end if;

  delete from creator_search_current
  where watermark = target_watermark and channel_id = any(p_channel_ids);

  insert into creator_search_current(
    channel_id, snapshot_id, captured_at, name, handle, avatar_url, verified,
    country, language, category_paths, tags, subscribers, total_views,
    channel_video_count, content_sample_count, contact_types, last_published_at,
    last_published_date, content_count_30d, content_count_90d, publish_weekly,
    publish_monthly, avg_views, view_subscribers_ratio,
    engagement_rate_by_views, avg_likes, likes_subscribers_ratio, avg_comments,
    comments_subscribers_ratio, audience_country, audience_country_share,
    audience_language, audience_language_share, audience_gender,
    audience_gender_share, audience_age, audience_age_share,
    active_subscriber_ratio, search_text, projection_version, watermark
  )
  with snapshot as (
    select distinct on (channel_id) * from channel_snapshots
    where channel_id = any(p_channel_ids)
    order by channel_id, captured_at desc, id desc
  ), fact_values as (
    select s.id, coalesce(jsonb_object_agg(f.field_key, f.value_json) filter (where f.field_key is not null), '{}'::jsonb) facts
    from snapshot s left join channel_profile_facts f on f.channel_snapshot_id = s.id
    group by s.id
  ), metric_values as (
    select s.id,
      max(m.value_numeric) filter (where m.scope = 'all' and m.metric_key = 'average_views') avg_views,
      max(m.value_numeric) filter (where m.scope = 'all' and m.metric_key = 'views_subscribers_ratio') view_subscribers_ratio,
      max(m.value_numeric) filter (where m.scope = 'all' and m.metric_key = 'engagement_rate_by_views') engagement_rate_by_views,
      max(m.value_numeric) filter (where m.scope = 'all' and m.metric_key = 'average_likes') avg_likes,
      max(m.value_numeric) filter (where m.scope = 'all' and m.metric_key = 'likes_subscribers_ratio') likes_subscribers_ratio,
      max(m.value_numeric) filter (where m.scope = 'all' and m.metric_key = 'average_comments') avg_comments,
      max(m.value_numeric) filter (where m.scope = 'all' and m.metric_key = 'comments_subscribers_ratio') comments_subscribers_ratio
    from snapshot s
    left join channel_metric_values m on m.channel_snapshot_id = s.id and m.value_status <> 'placeholder'
    group by s.id
  ), content_values as (
    select s.id, stats.*
    from snapshot s
    cross join lateral snapshot_publication_stats_v2(s.id) stats
  ), contact_values as (
    select s.id, coalesce(array_agg(distinct l.link_type order by l.link_type) filter (where l.link_type is not null), '{}') contact_types
    from snapshot s left join channel_links l on l.channel_snapshot_id = s.id
    group by s.id
  ), projection as (
    select s.*, f.facts,
      m.avg_views, m.view_subscribers_ratio, m.engagement_rate_by_views,
      m.avg_likes, m.likes_subscribers_ratio, m.avg_comments, m.comments_subscribers_ratio,
      c.content_sample_count, c.last_published_at, c.last_published_date,
      c.content_count_30d, c.content_count_90d, c.publish_weekly, c.publish_monthly,
      l.contact_types,
      coalesce((
        select array_agg(coalesce(t.canonical_path, v.raw_value) order by v.ordinality)
        from jsonb_array_elements_text(coalesce(f.facts -> 'channel_categories', '[]'::jsonb)) with ordinality v(raw_value, ordinality)
        left join category_taxonomy t on t.raw_value = v.raw_value
      ), '{}') category_paths,
      coalesce((select array_agg(v) from jsonb_array_elements_text(coalesce(f.facts #> '{channel_tags,tags}', '[]'::jsonb)) v), '{}') tags,
      (select sum((v ->> 'male')::numeric) from jsonb_array_elements(coalesce(f.facts -> 'audience_age_gender', '[]'::jsonb)) v where nullif(v ->> 'male', '') is not null) audience_male,
      (select sum((v ->> 'female')::numeric) from jsonb_array_elements(coalesce(f.facts -> 'audience_age_gender', '[]'::jsonb)) v where nullif(v ->> 'female', '') is not null) audience_female,
      (select v from jsonb_array_elements(coalesce(f.facts -> 'audience_age_gender', '[]'::jsonb)) v
        where nullif(v ->> 'male', '') is not null or nullif(v ->> 'female', '') is not null
        order by coalesce((v ->> 'male')::numeric, 0) + coalesce((v ->> 'female')::numeric, 0) desc limit 1) top_age
    from snapshot s
    join fact_values f on f.id = s.id
    join metric_values m on m.id = s.id
    join content_values c on c.id = s.id
    join contact_values l on l.id = s.id
  )
  select channel_id, id, captured_at, title, handle, avatar_url, is_verified,
    case
      when lower(coalesce(country_text, facts ->> 'country', '')) in ('brasil', 'brazil', 'br') then 'BR'
      when lower(coalesce(country_text, facts ->> 'country', '')) in ('portugal', 'pt') then 'PT'
      when lower(coalesce(country_text, facts ->> 'country', '')) in ('argentina', 'ar') then 'AR'
      when lower(coalesce(country_text, facts ->> 'country', '')) in ('mexico', 'méxico', 'mx') then 'MX'
      when lower(coalesce(country_text, facts ->> 'country', '')) in ('united states', 'usa', 'us') then 'US'
      else nullif(coalesce(country_text, facts ->> 'country'), '')
    end,
    case
      when lower(coalesce(facts ->> 'creator_language', '')) in ('portuguese', 'portugues', 'português', 'pt') then 'pt'
      when lower(coalesce(facts ->> 'creator_language', '')) in ('spanish', 'español', 'es') then 'es'
      when lower(coalesce(facts ->> 'creator_language', '')) in ('english', 'en') then 'en'
      else nullif(facts ->> 'creator_language', '')
    end,
    category_paths, tags, subscriber_count, total_view_count, video_count,
    content_sample_count, contact_types, last_published_at, last_published_date,
    content_count_30d, content_count_90d, publish_weekly, publish_monthly,
    avg_views, view_subscribers_ratio, engagement_rate_by_views, avg_likes,
    likes_subscribers_ratio, avg_comments, comments_subscribers_ratio,
    case when lower(coalesce(facts #>> '{audience_region,0,region}', '')) in ('brasil', 'brazil', 'br') then 'BR' else nullif(facts #>> '{audience_region,0,region}', '') end,
    nullif(facts #>> '{audience_region,0,percentage}', '')::numeric,
    case when lower(coalesce(facts #>> '{audience_language,0,language}', '')) in ('portuguese', 'portugues', 'português', 'pt') then 'pt' else nullif(facts #>> '{audience_language,0,language}', '') end,
    nullif(facts #>> '{audience_language,0,percentage}', '')::numeric,
    case when audience_male is null and audience_female is null then null when audience_male >= audience_female then 'male' else 'female' end,
    greatest(audience_male, audience_female), top_age ->> 'age_range',
    case when top_age is null then null else coalesce((top_age ->> 'male')::numeric, 0) + coalesce((top_age ->> 'female')::numeric, 0) end,
    nullif(facts ->> 'active_subscriber_ratio', '')::numeric,
    lower(concat_ws(' ', title, handle, coalesce(country_text, facts ->> 'country'), facts ->> 'creator_language', array_to_string(category_paths, ' '), array_to_string(tags, ' '))),
    'creator-search-v3', target_watermark
  from projection;

  update creator_search_releases set status = 'retired'
  where status = 'active' and watermark <> target_watermark;
  update creator_search_releases set status = 'active', activated_at = coalesce(activated_at, clock_timestamp())
  where watermark = target_watermark;
  insert into creator_search_active(singleton, watermark) values (true, target_watermark)
  on conflict (singleton) do update set watermark = excluded.watermark;
end $$;


--
-- Name: refresh_creator_search_release_v4_impl(text, text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_creator_search_release_v4_impl(p_watermark text, p_channel_ids text[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
declare
  normalized_channel_ids text[];
  previous_generation bigint;
  previous_release_status text;
  next_generation bigint;
  projected_count bigint;
begin
  select array_agg(channel_id order by channel_id)
  into normalized_channel_ids
  from (
    select distinct btrim(channel_id) channel_id
    from unnest(coalesce(p_channel_ids, '{}'::text[])) channel_id
    where nullif(btrim(channel_id), '') is not null
  ) normalized;

  if nullif(btrim(p_watermark), '') is null
     or coalesce(cardinality(normalized_channel_ids), 0) = 0 then
    raise exception 'watermark and at least one channel id are required';
  end if;

  perform pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));

  if not exists (
    select 1 from import_batches
    where id = p_watermark and status = 'published'
  ) then
    raise exception 'creator search release requires a published import batch: %', p_watermark;
  end if;

  select generation, status
  into previous_generation, previous_release_status
  from creator_search_releases
  where watermark = p_watermark
  for update;

  if previous_release_status = 'retired' then
    raise exception 'retired releases cannot be rebuilt in place: %', p_watermark;
  end if;

  if exists (
    select 1
    from unnest(normalized_channel_ids) requested(channel_id)
    left join lateral (
      select snapshot.import_batch_id
      from channel_snapshots snapshot
      where snapshot.channel_id = requested.channel_id
      order by snapshot.captured_at desc, snapshot.id desc
      limit 1
    ) latest on true
    where latest.import_batch_id is distinct from p_watermark
  ) then
    raise exception 'requested channels must use their latest snapshot from target watermark %', p_watermark;
  end if;

  perform refresh_creator_search_release_v3_impl(p_watermark, normalized_channel_ids);

  update creator_search_current projection
  set country = case
        when lower(coalesce(snapshot.country_text, '')) in ('brasil', 'brazil', 'br') then 'BR'
        when lower(coalesce(snapshot.country_text, '')) in ('portugal', 'pt') then 'PT'
        when lower(coalesce(snapshot.country_text, '')) in ('argentina', 'ar') then 'AR'
        when lower(coalesce(snapshot.country_text, '')) in ('mexico', 'méxico', 'mx') then 'MX'
        when lower(coalesce(snapshot.country_text, '')) in ('united states', 'usa', 'us') then 'US'
        else nullif(snapshot.country_text, '')
      end,
      search_text = lower(concat_ws(
        ' ', projection.name, projection.handle, snapshot.country_text,
        projection.language, array_to_string(projection.category_paths, ' '),
        array_to_string(projection.tags, ' ')
      )),
      projection_version = 'creator-search-v4'
  from channel_snapshots snapshot
  where projection.watermark = p_watermark
    and snapshot.id = projection.snapshot_id;

  select count(*) into projected_count
  from creator_search_current
  where watermark = p_watermark
    and channel_id = any(normalized_channel_ids);
  if projected_count <> cardinality(normalized_channel_ids) then
    raise exception 'creator search release does not cover every requested channel';
  end if;

  next_generation := case
    when previous_generation is null then 1
    when previous_release_status = 'active' then previous_generation + 1
    else previous_generation
  end;
  update creator_search_releases
  set generation = next_generation,
      rebuilt_at = clock_timestamp()
  where watermark = p_watermark;
end $$;


--
-- Name: refresh_creator_search_release_v7(text, text[], text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_creator_search_release_v7(p_watermark text, p_upsert_channel_ids text[], p_removed_channel_ids text[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
DECLARE
  upsert_ids TEXT[];
  removed_ids TEXT[];
  previous_watermark TEXT;
  previous_generation BIGINT;
  previous_status TEXT;
BEGIN
  SELECT COALESCE(array_agg(channel_id ORDER BY channel_id),'{}'::text[])
  INTO upsert_ids
  FROM (
    SELECT DISTINCT btrim(channel_id) AS channel_id
    FROM unnest(COALESCE(p_upsert_channel_ids,'{}'::text[])) channel_id
    WHERE NULLIF(btrim(channel_id),'') IS NOT NULL
  ) normalized;

  SELECT COALESCE(array_agg(channel_id ORDER BY channel_id),'{}'::text[])
  INTO removed_ids
  FROM (
    SELECT DISTINCT btrim(channel_id) AS channel_id
    FROM unnest(COALESCE(p_removed_channel_ids,'{}'::text[])) channel_id
    WHERE NULLIF(btrim(channel_id),'') IS NOT NULL
  ) normalized;

  IF NULLIF(btrim(p_watermark),'') IS NULL
     OR cardinality(upsert_ids)+cardinality(removed_ids)=0 THEN
    RAISE EXCEPTION 'watermark and at least one changed channel id are required';
  END IF;
  IF upsert_ids && removed_ids THEN
    RAISE EXCEPTION 'upsert and removed channel ids must be disjoint';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  IF NOT EXISTS (
    SELECT 1 FROM public.import_batches
    WHERE id=p_watermark AND status='published'
  ) THEN
    RAISE EXCEPTION 'creator search release requires a published import batch: %', p_watermark;
  END IF;

  IF cardinality(upsert_ids)>0 THEN
    PERFORM public.refresh_creator_search_release(p_watermark,upsert_ids);
  ELSE
    SELECT watermark INTO previous_watermark
    FROM public.creator_search_active WHERE singleton=true;
    SELECT generation,status INTO previous_generation,previous_status
    FROM public.creator_search_releases
    WHERE watermark=p_watermark FOR UPDATE;
    IF previous_status='retired' THEN
      RAISE EXCEPTION 'retired releases cannot be rebuilt in place: %',p_watermark;
    END IF;
    INSERT INTO public.creator_search_releases(watermark,status)
    VALUES (p_watermark,'building')
    ON CONFLICT (watermark) DO NOTHING;
    IF previous_watermark IS NOT NULL AND previous_watermark<>p_watermark THEN
      INSERT INTO public.creator_search_current
      SELECT (jsonb_populate_record(
        NULL::public.creator_search_current,
        to_jsonb(previous_row)||jsonb_build_object('watermark',p_watermark)
      )).*
      FROM public.creator_search_current previous_row
      WHERE previous_row.watermark=previous_watermark
        AND NOT (previous_row.channel_id=ANY(removed_ids))
      ON CONFLICT (watermark,channel_id) DO NOTHING;
    END IF;
    UPDATE public.creator_search_releases
    SET status='retired'
    WHERE status='active' AND watermark<>p_watermark;
    UPDATE public.creator_search_releases
    SET status='active',
        activated_at=COALESCE(activated_at,clock_timestamp()),
        generation=CASE
          WHEN previous_generation IS NULL THEN 1
          WHEN previous_status='active' THEN previous_generation+1
          ELSE previous_generation
        END,
        rebuilt_at=clock_timestamp()
    WHERE watermark=p_watermark;
    INSERT INTO public.creator_search_active(singleton,watermark)
    VALUES (true,p_watermark)
    ON CONFLICT (singleton) DO UPDATE SET watermark=excluded.watermark;
  END IF;

  DELETE FROM public.creator_search_current
  WHERE watermark=p_watermark AND channel_id=ANY(removed_ids);

  UPDATE public.creator_search_current projection
  SET verified=snapshot.is_verified,
      verified_status=snapshot.is_verified_status,
      channel_observed_at=snapshot.channel_observed_at,
      subscribers_observed_at=snapshot.subscriber_count_observed_at,
      total_views_observed_at=snapshot.total_view_count_observed_at,
      channel_video_count_observed_at=snapshot.video_count_observed_at,
      projection_version='creator-search-v7'
  FROM public.channel_snapshots snapshot
  WHERE projection.watermark=p_watermark
    AND snapshot.id=projection.snapshot_id
    AND snapshot.channel_id=projection.channel_id;

  IF EXISTS (
    SELECT 1 FROM unnest(upsert_ids) requested(channel_id)
    LEFT JOIN public.creator_search_current projected
      ON projected.watermark=p_watermark AND projected.channel_id=requested.channel_id
    WHERE projected.channel_id IS NULL
  ) THEN
    RAISE EXCEPTION 'creator search release does not cover every upsert channel';
  END IF;
  IF EXISTS (
    SELECT 1 FROM public.creator_search_current
    WHERE watermark=p_watermark AND channel_id=ANY(removed_ids)
  ) THEN
    RAISE EXCEPTION 'creator search release retained a removed channel';
  END IF;
END
$$;


--
-- Name: refresh_creator_search_release_v8(text, text[], text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_creator_search_release_v8(p_watermark text, p_upsert_channel_ids text[], p_removed_channel_ids text[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'pg_temp'
    AS $$
DECLARE
  upsert_ids TEXT[];
  removed_ids TEXT[];
  previous_watermark TEXT;
  previous_generation BIGINT;
  previous_status TEXT;
BEGIN
  SELECT COALESCE(array_agg(channel_id ORDER BY channel_id),'{}'::text[])
  INTO upsert_ids
  FROM (
    SELECT DISTINCT btrim(channel_id) AS channel_id
    FROM unnest(COALESCE(p_upsert_channel_ids,'{}'::text[])) channel_id
    WHERE NULLIF(btrim(channel_id),'') IS NOT NULL
  ) normalized;

  SELECT COALESCE(array_agg(channel_id ORDER BY channel_id),'{}'::text[])
  INTO removed_ids
  FROM (
    SELECT DISTINCT btrim(channel_id) AS channel_id
    FROM unnest(COALESCE(p_removed_channel_ids,'{}'::text[])) channel_id
    WHERE NULLIF(btrim(channel_id),'') IS NOT NULL
  ) normalized;

  IF NULLIF(btrim(p_watermark),'') IS NULL
     OR cardinality(upsert_ids)+cardinality(removed_ids)=0 THEN
    RAISE EXCEPTION 'watermark and at least one changed channel id are required';
  END IF;
  IF upsert_ids && removed_ids THEN
    RAISE EXCEPTION 'upsert and removed channel ids must be disjoint';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  IF NOT EXISTS (
    SELECT 1 FROM public.import_batches
    WHERE id=p_watermark AND status='published'
  ) THEN
    RAISE EXCEPTION 'creator search release requires a published import batch: %', p_watermark;
  END IF;

  IF cardinality(upsert_ids)>0 THEN
    PERFORM public.refresh_creator_search_release(p_watermark,upsert_ids);
  ELSE
    SELECT watermark INTO previous_watermark
    FROM public.creator_search_active WHERE singleton=true;
    SELECT generation,status INTO previous_generation,previous_status
    FROM public.creator_search_releases
    WHERE watermark=p_watermark FOR UPDATE;
    IF previous_status='retired' THEN
      RAISE EXCEPTION 'retired releases cannot be rebuilt in place: %',p_watermark;
    END IF;
    INSERT INTO public.creator_search_releases(watermark,status)
    VALUES (p_watermark,'building')
    ON CONFLICT (watermark) DO NOTHING;
    IF previous_watermark IS NOT NULL AND previous_watermark<>p_watermark THEN
      INSERT INTO public.creator_search_current
      SELECT (jsonb_populate_record(
        NULL::public.creator_search_current,
        to_jsonb(previous_row)||jsonb_build_object('watermark',p_watermark)
      )).*
      FROM public.creator_search_current previous_row
      WHERE previous_row.watermark=previous_watermark
        AND NOT (previous_row.channel_id=ANY(removed_ids))
      ON CONFLICT (watermark,channel_id) DO NOTHING;
    END IF;
    UPDATE public.creator_search_releases
    SET status='retired'
    WHERE status='active' AND watermark<>p_watermark;
    UPDATE public.creator_search_releases
    SET status='active',
        activated_at=COALESCE(activated_at,clock_timestamp()),
        generation=CASE
          WHEN previous_generation IS NULL THEN 1
          WHEN previous_status='active' THEN previous_generation+1
          ELSE previous_generation
        END,
        rebuilt_at=clock_timestamp()
    WHERE watermark=p_watermark;
    INSERT INTO public.creator_search_active(singleton,watermark)
    VALUES (true,p_watermark)
    ON CONFLICT (singleton) DO UPDATE SET watermark=excluded.watermark;
  END IF;

  DELETE FROM public.creator_search_current
  WHERE watermark=p_watermark AND channel_id=ANY(removed_ids);

  UPDATE public.creator_search_current projection
  SET verified=snapshot.is_verified,
      verified_status=snapshot.is_verified_status,
      youtube_business_email_available=snapshot.youtube_business_email_available,
      youtube_business_email_observed_at=snapshot.youtube_business_email_observed_at,
      channel_observed_at=snapshot.channel_observed_at,
      subscribers_observed_at=snapshot.subscriber_count_observed_at,
      total_views_observed_at=snapshot.total_view_count_observed_at,
      channel_video_count_observed_at=snapshot.video_count_observed_at,
      projection_version='creator-search-v8'
  FROM public.channel_snapshots snapshot
  WHERE projection.watermark=p_watermark
    AND snapshot.id=projection.snapshot_id
    AND snapshot.channel_id=projection.channel_id;

  IF EXISTS (
    SELECT 1 FROM unnest(upsert_ids) requested(channel_id)
    LEFT JOIN public.creator_search_current projected
      ON projected.watermark=p_watermark AND projected.channel_id=requested.channel_id
    WHERE projected.channel_id IS NULL
  ) THEN
    RAISE EXCEPTION 'creator search release does not cover every upsert channel';
  END IF;
  IF EXISTS (
    SELECT 1 FROM public.creator_search_current
    WHERE watermark=p_watermark AND channel_id=ANY(removed_ids)
  ) THEN
    RAISE EXCEPTION 'creator search release retained a removed channel';
  END IF;
END
$$;


--
-- Name: refresh_creator_search_release_v9(text, text[], text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_creator_search_release_v9(p_watermark text, p_upsert_channel_ids text[], p_removed_channel_ids text[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  upsert_ids TEXT[];
  removed_ids TEXT[];
  changed_ids TEXT[];
  prior_watermark TEXT;
  publish_mode TEXT;
  current_read_mode TEXT;
BEGIN
  SELECT COALESCE(array_agg(channel_id ORDER BY channel_id),'{}'::text[])
  INTO upsert_ids
  FROM (
    SELECT DISTINCT btrim(channel_id) AS channel_id
    FROM unnest(COALESCE(p_upsert_channel_ids,'{}'::text[])) channel_id
    WHERE NULLIF(btrim(channel_id),'') IS NOT NULL
  ) normalized;

  SELECT COALESCE(array_agg(channel_id ORDER BY channel_id),'{}'::text[])
  INTO removed_ids
  FROM (
    SELECT DISTINCT btrim(channel_id) AS channel_id
    FROM unnest(COALESCE(p_removed_channel_ids,'{}'::text[])) channel_id
    WHERE NULLIF(btrim(channel_id),'') IS NOT NULL
  ) normalized;

  changed_ids := upsert_ids || removed_ids;
  IF NULLIF(btrim(p_watermark),'') IS NULL OR cardinality(changed_ids)=0 THEN
    RAISE EXCEPTION 'watermark and at least one changed channel id are required';
  END IF;
  IF upsert_ids && removed_ids THEN
    RAISE EXCEPTION 'upsert and removed channel ids must be disjoint';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  SELECT write_mode,read_mode INTO publish_mode,current_read_mode
  FROM publication.creator_search_storage_state
  WHERE singleton=true
  FOR UPDATE;
  IF publish_mode IS NULL THEN
    RAISE EXCEPTION 'Creator Search storage state is missing';
  END IF;
  IF publish_mode='incremental' AND current_read_mode<>'live' THEN
    RAISE EXCEPTION 'incremental Creator Search writes require Live reads';
  END IF;
  SELECT watermark INTO prior_watermark
  FROM public.creator_search_active WHERE singleton=true;

  DROP TABLE IF EXISTS pg_temp.creator_search_publish_before;
  CREATE TEMP TABLE creator_search_publish_before (
    channel_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    document JSONB
  ) ON COMMIT DROP;
  INSERT INTO creator_search_publish_before(channel_id,action,document)
  SELECT requested.channel_id,requested.action,
         CASE WHEN live.channel_id IS NULL THEN NULL ELSE to_jsonb(live) END
  FROM (
    SELECT channel_id,'upsert'::text AS action FROM unnest(upsert_ids) channel_id
    UNION ALL
    SELECT channel_id,'remove'::text AS action FROM unnest(removed_ids) channel_id
  ) requested
  LEFT JOIN public.creator_search_live live USING(channel_id);

  IF publish_mode='incremental' THEN
    DELETE FROM public.creator_search_active WHERE singleton=true;
  END IF;

  PERFORM public.refresh_creator_search_release_v8(
    p_watermark,upsert_ids,removed_ids
  );

  DELETE FROM public.creator_search_live
  WHERE channel_id=ANY(changed_ids);

  INSERT INTO public.creator_search_live
  SELECT search.*
  FROM public.creator_search_current search
  WHERE search.watermark=p_watermark
    AND search.channel_id=ANY(upsert_ids);

  IF (
    SELECT count(*) FROM public.creator_search_live
    WHERE channel_id=ANY(upsert_ids)
  ) <> cardinality(upsert_ids) THEN
    RAISE EXCEPTION 'Creator Search Live does not cover every upsert channel';
  END IF;

  INSERT INTO publication.creator_search_changes (
    watermark,channel_id,action,before_document,after_document
  )
  SELECT p_watermark,before.channel_id,before.action,before.document,
         CASE WHEN live.channel_id IS NULL THEN NULL ELSE to_jsonb(live) END
  FROM creator_search_publish_before before
  LEFT JOIN public.creator_search_live live USING(channel_id);

  IF publish_mode='incremental' THEN
    DELETE FROM public.creator_search_current
    WHERE watermark=p_watermark;
  END IF;

  UPDATE public.creator_search_releases
  SET previous_watermark=prior_watermark,
      storage_mode=publish_mode,
      changed_channel_count=cardinality(changed_ids)
  WHERE watermark=p_watermark;
END
$$;


--
-- Name: replay_creator_search_release_v9(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.replay_creator_search_release_v9(p_watermark text) RETURNS text
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  active_watermark TEXT;
  prior_watermark TEXT;
  release_status TEXT;
  expected_change_count INTEGER;
  actual_change_count INTEGER;
  current_write_mode TEXT;
BEGIN
  IF NULLIF(btrim(p_watermark),'') IS NULL THEN
    RAISE EXCEPTION 'watermark is required';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  SELECT write_mode INTO current_write_mode
  FROM publication.creator_search_storage_state
  WHERE singleton=true;
  IF current_write_mode IS NULL THEN
    RAISE EXCEPTION 'Creator Search storage state is missing';
  END IF;
  SELECT watermark INTO active_watermark
  FROM public.creator_search_active
  WHERE singleton=true
  FOR UPDATE;

  SELECT previous_watermark,status,changed_channel_count
  INTO prior_watermark,release_status,expected_change_count
  FROM public.creator_search_releases
  WHERE watermark=p_watermark
  FOR UPDATE;
  IF prior_watermark IS NULL THEN
    RAISE EXCEPTION 'Creator Search release has no replay predecessor: %',p_watermark;
  END IF;

  SELECT count(*)::int INTO actual_change_count
  FROM publication.creator_search_changes
  WHERE watermark=p_watermark;
  IF actual_change_count=0
     OR actual_change_count IS DISTINCT FROM expected_change_count THEN
    RAISE EXCEPTION 'Creator Search release change history is incomplete: %',p_watermark;
  END IF;

  IF active_watermark=p_watermark AND release_status='active' THEN
    IF EXISTS (
      SELECT 1
      FROM publication.creator_search_changes change
      LEFT JOIN public.creator_search_live live USING(channel_id)
      WHERE change.watermark=p_watermark
        AND (
          (change.after_document IS NULL AND live.channel_id IS NOT NULL)
          OR (
            change.after_document IS NOT NULL
            AND (
              live.channel_id IS NULL
              OR to_jsonb(live) IS DISTINCT FROM change.after_document
            )
          )
        )
    ) THEN
      RAISE EXCEPTION 'active Creator Search release differs from its replay state: %',p_watermark;
    END IF;
    RETURN p_watermark;
  END IF;

  IF active_watermark IS DISTINCT FROM prior_watermark OR release_status<>'retired' THEN
    RAISE EXCEPTION 'Creator Search release cannot replay over active predecessor: %',p_watermark;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM publication.creator_search_changes change
    LEFT JOIN public.creator_search_live live USING(channel_id)
    WHERE change.watermark=p_watermark
      AND (
        (change.before_document IS NULL AND live.channel_id IS NOT NULL)
        OR (
          change.before_document IS NOT NULL
          AND (
            live.channel_id IS NULL
            OR to_jsonb(live) IS DISTINCT FROM change.before_document
          )
        )
      )
  ) THEN
    RAISE EXCEPTION 'Creator Search Live no longer matches the replay predecessor: %',p_watermark;
  END IF;

  DELETE FROM public.creator_search_live live
  USING publication.creator_search_changes change
  WHERE change.watermark=p_watermark
    AND live.channel_id=change.channel_id;

  INSERT INTO public.creator_search_live
  SELECT (jsonb_populate_record(
    NULL::public.creator_search_live,change.after_document
  )).*
  FROM publication.creator_search_changes change
  WHERE change.watermark=p_watermark
    AND change.after_document IS NOT NULL;

  IF EXISTS (
    SELECT 1
    FROM publication.creator_search_changes change
    LEFT JOIN public.creator_search_live live USING(channel_id)
    WHERE change.watermark=p_watermark
      AND (
        (change.after_document IS NULL AND live.channel_id IS NOT NULL)
        OR (
          change.after_document IS NOT NULL
          AND (
            live.channel_id IS NULL
            OR to_jsonb(live) IS DISTINCT FROM change.after_document
          )
        )
      )
  ) THEN
    RAISE EXCEPTION 'Creator Search replay did not restore exact after state: %',p_watermark;
  END IF;

  IF current_write_mode='shadow' THEN
    DELETE FROM public.creator_search_current
    WHERE watermark=p_watermark;

    INSERT INTO public.creator_search_current
    SELECT (jsonb_populate_record(
      NULL::public.creator_search_current,
      to_jsonb(live)||jsonb_build_object('watermark',p_watermark)
    )).*
    FROM public.creator_search_live live;

    IF EXISTS (
      SELECT 1
      FROM (
        SELECT legacy.channel_id AS legacy_channel_id,
               live.channel_id AS live_channel_id,
               CASE WHEN legacy.channel_id IS NULL THEN NULL ELSE to_jsonb(legacy) END
                 AS legacy_document,
               CASE WHEN live.channel_id IS NULL THEN NULL ELSE
                 to_jsonb(live)||jsonb_build_object('watermark',p_watermark) END
                 AS live_document
        FROM (
          SELECT search.*
          FROM public.creator_search_current search
          WHERE search.watermark=p_watermark
        ) legacy
        FULL JOIN public.creator_search_live live USING(channel_id)
      ) parity
      WHERE parity.legacy_channel_id IS NULL
         OR parity.live_channel_id IS NULL
         OR parity.legacy_document IS DISTINCT FROM parity.live_document
    ) THEN
      RAISE EXCEPTION 'Creator Search shadow replay did not materialize exact Legacy state: %',
        p_watermark;
    END IF;
  END IF;

  UPDATE public.creator_search_releases
  SET status='retired'
  WHERE watermark=prior_watermark AND status='active';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search replay predecessor is not active: %',prior_watermark;
  END IF;

  UPDATE public.creator_search_releases
  SET status='active',activated_at=COALESCE(activated_at,clock_timestamp()),
      generation=generation+1,rebuilt_at=clock_timestamp()
  WHERE watermark=p_watermark AND status='retired';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search replay release is not retired: %',p_watermark;
  END IF;

  UPDATE public.creator_search_active
  SET watermark=p_watermark
  WHERE singleton=true AND watermark=prior_watermark;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search active release changed during replay';
  END IF;

  RETURN p_watermark;
END
$$;


--
-- Name: restore_creator_search_live_from_legacy_v1(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.restore_creator_search_live_from_legacy_v1(p_watermark text) RETURNS integer
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  restored_count INTEGER;
BEGIN
  IF NULLIF(btrim(p_watermark),'') IS NULL THEN
    RAISE EXCEPTION 'watermark is required';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  IF NOT EXISTS (
    SELECT 1 FROM public.creator_search_releases WHERE watermark=p_watermark
  ) THEN
    RAISE EXCEPTION 'Creator Search Legacy release is missing: %',p_watermark;
  END IF;

  SELECT count(*)::int INTO restored_count
  FROM public.creator_search_current
  WHERE watermark=p_watermark;
  IF restored_count=0 THEN
    RAISE EXCEPTION 'Creator Search Legacy release has no rows: %',p_watermark;
  END IF;

  DELETE FROM public.creator_search_live;
  INSERT INTO public.creator_search_live
  SELECT (jsonb_populate_record(
    NULL::public.creator_search_live,to_jsonb(search)
  )).*
  FROM public.creator_search_current search
  WHERE search.watermark=p_watermark;

  IF (SELECT count(*)::int FROM public.creator_search_live)<>restored_count
     OR EXISTS (
       SELECT 1
       FROM (
         SELECT legacy.channel_id AS legacy_channel_id,
                live.channel_id AS live_channel_id,
                CASE WHEN legacy.channel_id IS NULL THEN NULL ELSE to_jsonb(legacy) END
                  AS legacy_document,
                CASE WHEN live.channel_id IS NULL THEN NULL ELSE to_jsonb(live) END
                  AS live_document
         FROM (
           SELECT search.*
           FROM public.creator_search_current search
           WHERE search.watermark=p_watermark
         ) legacy
         FULL JOIN public.creator_search_live live USING(channel_id)
       ) parity
       WHERE parity.legacy_channel_id IS NULL
          OR parity.live_channel_id IS NULL
          OR parity.legacy_document IS DISTINCT FROM parity.live_document
     ) THEN
    RAISE EXCEPTION 'Creator Search Live restore differs from Legacy release: %',p_watermark;
  END IF;

  RETURN restored_count;
END
$$;


--
-- Name: rollback_creator_search_incremental_storage_v1(text, integer, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.rollback_creator_search_incremental_storage_v1(p_target_watermark text, p_expected_live_count integer, p_actor text, p_reason text) RETURNS integer
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  current_write_mode TEXT;
  current_read_mode TEXT;
  active_watermark TEXT;
  live_count INTEGER;
  legacy_count INTEGER;
  rollback_count INTEGER;
BEGIN
  IF NULLIF(btrim(p_target_watermark),'') IS NULL
     OR p_expected_live_count IS NULL OR p_expected_live_count<0
     OR NULLIF(btrim(p_actor),'') IS NULL
     OR NULLIF(btrim(p_reason),'') IS NULL THEN
    RAISE EXCEPTION 'target watermark, non-negative count, actor, and reason are required';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  SELECT write_mode,read_mode INTO current_write_mode,current_read_mode
  FROM publication.creator_search_storage_state
  WHERE singleton=true
  FOR UPDATE;
  IF current_write_mode<>'incremental' OR current_read_mode<>'live' THEN
    RAISE EXCEPTION 'Creator Search storage is not in incremental/live mode';
  END IF;

  rollback_count := public.rollback_creator_search_to_watermark_v1(
    p_target_watermark,p_actor,p_reason
  );
  SELECT watermark INTO active_watermark
  FROM public.creator_search_active
  WHERE singleton=true
  FOR UPDATE;
  IF active_watermark IS DISTINCT FROM p_target_watermark THEN
    RAISE EXCEPTION 'Creator Search storage rollback did not reach its target watermark';
  END IF;

  SELECT count(*)::int INTO live_count FROM public.creator_search_live;
  SELECT count(*)::int INTO legacy_count
  FROM public.creator_search_current
  WHERE watermark=active_watermark;
  IF live_count<>p_expected_live_count OR legacy_count<>p_expected_live_count THEN
    RAISE EXCEPTION 'Creator Search storage rollback row count differs from expectation';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM (
      SELECT legacy.channel_id AS legacy_channel_id,
             live.channel_id AS live_channel_id,
             CASE WHEN legacy.channel_id IS NULL THEN NULL ELSE
               to_jsonb(legacy)-'watermark' END
               AS legacy_document,
             CASE WHEN live.channel_id IS NULL THEN NULL ELSE
               to_jsonb(live)-'watermark' END
               AS live_document
      FROM (
        SELECT search.*
        FROM public.creator_search_current search
        WHERE search.watermark=active_watermark
      ) legacy
      FULL JOIN public.creator_search_live live USING(channel_id)
    ) parity
    WHERE parity.legacy_channel_id IS NULL
       OR parity.live_channel_id IS NULL
       OR parity.legacy_document IS DISTINCT FROM parity.live_document
  ) THEN
    RAISE EXCEPTION 'Creator Search storage rollback target differs from Legacy state';
  END IF;

  UPDATE publication.creator_search_storage_state
  SET write_mode='shadow',read_mode='legacy',updated_at=clock_timestamp(),
      storage_rollback_actor=btrim(p_actor),storage_rollback_reason=btrim(p_reason),
      storage_rollback_at=clock_timestamp()
  WHERE singleton=true AND write_mode='incremental' AND read_mode='live';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search storage state changed during rollback';
  END IF;

  RETURN rollback_count;
END
$$;


--
-- Name: rollback_creator_search_release_v9(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.rollback_creator_search_release_v9(p_watermark text, p_actor text, p_reason text) RETURNS text
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  active_watermark TEXT;
  prior_watermark TEXT;
  expected_change_count INTEGER;
  actual_change_count INTEGER;
BEGIN
  IF NULLIF(btrim(p_watermark),'') IS NULL
     OR NULLIF(btrim(p_actor),'') IS NULL
     OR NULLIF(btrim(p_reason),'') IS NULL THEN
    RAISE EXCEPTION 'watermark, rollback actor, and rollback reason are required';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  SELECT watermark INTO active_watermark
  FROM public.creator_search_active
  WHERE singleton=true
  FOR UPDATE;
  IF active_watermark IS DISTINCT FROM p_watermark THEN
    RAISE EXCEPTION 'only the active Creator Search release can be rolled back: %',p_watermark;
  END IF;

  SELECT previous_watermark,changed_channel_count
  INTO prior_watermark,expected_change_count
  FROM public.creator_search_releases
  WHERE watermark=p_watermark AND status='active'
  FOR UPDATE;
  IF prior_watermark IS NULL THEN
    RAISE EXCEPTION 'Creator Search release has no rollback predecessor: %',p_watermark;
  END IF;

  SELECT count(*)::int INTO actual_change_count
  FROM publication.creator_search_changes
  WHERE watermark=p_watermark;
  IF actual_change_count=0
     OR actual_change_count IS DISTINCT FROM expected_change_count THEN
    RAISE EXCEPTION 'Creator Search release change history is incomplete: %',p_watermark;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM publication.creator_search_changes change
    LEFT JOIN public.creator_search_live live USING(channel_id)
    WHERE change.watermark=p_watermark
      AND (
        (change.after_document IS NULL AND live.channel_id IS NOT NULL)
        OR (
          change.after_document IS NOT NULL
          AND (
            live.channel_id IS NULL
            OR to_jsonb(live) IS DISTINCT FROM change.after_document
          )
        )
      )
  ) THEN
    RAISE EXCEPTION 'Creator Search Live has advanced or drifted beyond release: %',p_watermark;
  END IF;

  DELETE FROM public.creator_search_live live
  USING publication.creator_search_changes change
  WHERE change.watermark=p_watermark
    AND live.channel_id=change.channel_id;

  INSERT INTO public.creator_search_live
  SELECT (jsonb_populate_record(
    NULL::public.creator_search_live,change.before_document
  )).*
  FROM publication.creator_search_changes change
  WHERE change.watermark=p_watermark
    AND change.before_document IS NOT NULL;

  IF EXISTS (
    SELECT 1
    FROM publication.creator_search_changes change
    LEFT JOIN public.creator_search_live live USING(channel_id)
    WHERE change.watermark=p_watermark
      AND (
        (change.before_document IS NULL AND live.channel_id IS NOT NULL)
        OR (
          change.before_document IS NOT NULL
          AND (
            live.channel_id IS NULL
            OR to_jsonb(live) IS DISTINCT FROM change.before_document
          )
        )
      )
  ) THEN
    RAISE EXCEPTION 'Creator Search rollback did not restore exact before state: %',p_watermark;
  END IF;

  UPDATE public.creator_search_releases
  SET status='retired',rolled_back_at=clock_timestamp(),
      rollback_actor=btrim(p_actor),rollback_reason=btrim(p_reason)
  WHERE watermark=p_watermark AND status='active';

  UPDATE public.creator_search_releases
  SET status='active',activated_at=COALESCE(activated_at,clock_timestamp()),
      generation=generation+1,rebuilt_at=clock_timestamp()
  WHERE watermark=prior_watermark AND status='retired';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search rollback predecessor is not retired: %',prior_watermark;
  END IF;

  UPDATE public.creator_search_active
  SET watermark=prior_watermark
  WHERE singleton=true AND watermark=p_watermark;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Creator Search active release changed during rollback';
  END IF;

  RETURN prior_watermark;
END
$$;


--
-- Name: rollback_creator_search_to_watermark_v1(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.rollback_creator_search_to_watermark_v1(p_target_watermark text, p_actor text, p_reason text) RETURNS integer
    LANGUAGE plpgsql
    SET search_path TO 'public', 'publication', 'pg_temp'
    AS $$
DECLARE
  current_watermark TEXT;
  current_previous_watermark TEXT;
  current_storage_mode TEXT;
  rollback_count INTEGER := 0;
BEGIN
  IF NULLIF(btrim(p_target_watermark),'') IS NULL
     OR NULLIF(btrim(p_actor),'') IS NULL
     OR NULLIF(btrim(p_reason),'') IS NULL THEN
    RAISE EXCEPTION 'target watermark, rollback actor, and rollback reason are required';
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'));
  LOOP
    SELECT active.watermark,release.previous_watermark,release.storage_mode
    INTO current_watermark,current_previous_watermark,current_storage_mode
    FROM public.creator_search_active active
    JOIN public.creator_search_releases release ON release.watermark=active.watermark
    WHERE active.singleton=true
    FOR UPDATE OF active,release;

    IF current_watermark=p_target_watermark THEN
      RETURN rollback_count;
    END IF;
    IF current_storage_mode NOT IN ('shadow','incremental')
       OR current_previous_watermark IS NULL THEN
      RAISE EXCEPTION 'Creator Search rollback chain cannot reach target from release: %',
        current_watermark;
    END IF;

    PERFORM public.rollback_creator_search_release_v9(
      current_watermark,p_actor,p_reason
    );
    rollback_count := rollback_count+1;
    IF rollback_count>10000 THEN
      RAISE EXCEPTION 'Creator Search rollback chain exceeds safety limit';
    END IF;
  END LOOP;
END
$$;


--
-- Name: snapshot_publication_stats_v2(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.snapshot_publication_stats_v2(p_snapshot_id text) RETURNS TABLE(content_sample_count integer, last_published_at timestamp with time zone, last_published_date date, content_count_30d integer, content_count_90d integer, publish_weekly numeric, publish_monthly numeric)
    LANGUAGE sql STABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog', 'public'
    AS $$
  select
    count(content.id)::integer,
    greatest(
      max(content.published_at) filter (
        where content.published_at_status = 'exact'
          and content.published_at <= snapshot.captured_at
      ),
      snapshot.candidate_last_published_at
    ),
    greatest(
      max((publication_sort_at_v1(
        content.published_at,
        content.published_date,
        content.published_at_status
      ) at time zone 'UTC')::date) filter (
        where publication_sort_at_v1(
          content.published_at,
          content.published_date,
          content.published_at_status
        ) <= snapshot.captured_at
      ),
      snapshot.candidate_last_published_date
    ),
    count(content.id) filter (
      where publication_in_utc_window_v1(
        content.published_at,
        content.published_date,
        content.published_at_status,
        snapshot.captured_at,
        30
      )
    )::integer,
    count(content.id) filter (
      where publication_in_utc_window_v1(
        content.published_at,
        content.published_date,
        content.published_at_status,
        snapshot.captured_at,
        90
      )
    )::integer,
    case when coalesce(
      (snapshot.parse_status ->> 'contentWindowComplete')::boolean,
      false
    ) then count(content.id) filter (
      where publication_in_utc_window_v1(
        content.published_at,
        content.published_date,
        content.published_at_status,
        snapshot.captured_at,
        90
      )
    )::numeric / 90 * 7 else null end,
    case when coalesce(
      (snapshot.parse_status ->> 'contentWindowComplete')::boolean,
      false
    ) then count(content.id) filter (
      where publication_in_utc_window_v1(
        content.published_at,
        content.published_date,
        content.published_at_status,
        snapshot.captured_at,
        90
      )
    )::numeric / 90 * 30 else null end
  from public.channel_snapshots snapshot
  left join public.content_snapshots content
    on content.channel_snapshot_id = snapshot.id
   and content.is_recent
   and content.is_canonical
  where snapshot.id = p_snapshot_id
  group by
    snapshot.id,
    snapshot.captured_at,
    snapshot.parse_status,
    snapshot.candidate_last_published_at,
    snapshot.candidate_last_published_date;
$$;


--
-- Name: FUNCTION snapshot_publication_stats_v2(p_snapshot_id text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.snapshot_publication_stats_v2(p_snapshot_id text) IS 'Publication counts and cadence across recent canonical video, short, and live content for one channel snapshot.';


--
-- Name: strip_channel_link_format_chars_v1(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.strip_channel_link_format_chars_v1(value text) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    SET search_path TO 'pg_catalog'
    AS $$
  select translate(
    coalesce(value, ''),
    (
      select string_agg(chr(codepoint), '' order by codepoint)
      from unnest(array[
        173, 1564, 6158,
        8203, 8204, 8205, 8206, 8207,
        8234, 8235, 8236, 8237, 8238,
        8288, 8289, 8290, 8291, 8292,
        8294, 8295, 8296, 8297, 8298, 8299, 8300, 8301, 8302, 8303,
        65279
      ]) codepoint
    ),
    ''
  )
$$;


--
-- Name: FUNCTION strip_channel_link_format_chars_v1(value text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.strip_channel_link_format_chars_v1(value text) IS 'Removes invisible Unicode formatting controls before public-link parsing; source descriptions remain unchanged.';


--
-- Name: guard_business_activation_audit(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_business_activation_audit() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  RAISE EXCEPTION 'Business Publication Activation audit is immutable'
    USING ERRCODE='55000';
END
$$;


--
-- Name: guard_business_consumer_cursor(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_business_consumer_cursor() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'Business Publication Consumer Cursor cannot be deleted'
      USING ERRCODE='55000';
  END IF;
  IF NEW.channel_id IS DISTINCT FROM OLD.channel_id
     OR NEW.domain IS DISTINCT FROM OLD.domain THEN
    RAISE EXCEPTION 'Business Publication Consumer Cursor identity is immutable'
      USING ERRCODE='55000';
  END IF;
  IF NEW.publication_stream_id=OLD.publication_stream_id
     AND NEW.active_sequence<=OLD.active_sequence THEN
    RAISE EXCEPTION 'Business Publication Consumer Cursor must advance monotonically'
      USING ERRCODE='55000';
  END IF;
  RETURN NEW;
END
$$;


--
-- Name: guard_business_inbox(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_business_inbox() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'Business Publication Inbox rows cannot be deleted'
      USING ERRCODE='55000';
  END IF;
  IF NEW.revision_id IS DISTINCT FROM OLD.revision_id
     OR NEW.publication_stream_id IS DISTINCT FROM OLD.publication_stream_id
     OR NEW.channel_id IS DISTINCT FROM OLD.channel_id
     OR NEW.domain IS DISTINCT FROM OLD.domain
     OR NEW.data_sequence IS DISTINCT FROM OLD.data_sequence
     OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
     OR NEW.envelope_hash IS DISTINCT FROM OLD.envelope_hash
     OR NEW.received_envelope IS DISTINCT FROM OLD.received_envelope
     OR NEW.receipt_id IS DISTINCT FROM OLD.receipt_id
     OR NEW.receive_status IS DISTINCT FROM OLD.receive_status
     OR NEW.error_code IS DISTINCT FROM OLD.error_code
     OR NEW.error_message IS DISTINCT FROM OLD.error_message
     OR NEW.first_received_at IS DISTINCT FROM OLD.first_received_at THEN
    RAISE EXCEPTION 'Business Publication Inbox identity and evidence are immutable'
      USING ERRCODE='55000';
  END IF;
  IF NEW.receive_count < OLD.receive_count OR NEW.last_received_at < OLD.last_received_at THEN
    RAISE EXCEPTION 'Business Publication Inbox receipt counters cannot decrease'
      USING ERRCODE='55000';
  END IF;
  RETURN NEW;
END
$$;


--
-- Name: guard_business_revision(); Type: FUNCTION; Schema: publication; Owner: -
--

CREATE FUNCTION publication.guard_business_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF TG_OP='DELETE' THEN
    RAISE EXCEPTION 'Business Publication Revision rows cannot be deleted'
      USING ERRCODE='55000';
  END IF;
  IF NEW.revision_id IS DISTINCT FROM OLD.revision_id
     OR NEW.publication_stream_id IS DISTINCT FROM OLD.publication_stream_id
     OR NEW.channel_id IS DISTINCT FROM OLD.channel_id
     OR NEW.domain IS DISTINCT FROM OLD.domain
     OR NEW.data_sequence IS DISTINCT FROM OLD.data_sequence
     OR NEW.previous_data_sequence IS DISTINCT FROM OLD.previous_data_sequence
     OR NEW.revision_type IS DISTINCT FROM OLD.revision_type
     OR NEW.operation IS DISTINCT FROM OLD.operation
     OR NEW.contract_version IS DISTINCT FROM OLD.contract_version
     OR NEW.policy_version IS DISTINCT FROM OLD.policy_version
     OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at
     OR NEW.source_json IS DISTINCT FROM OLD.source_json
     OR NEW.previous_result_hash IS DISTINCT FROM OLD.previous_result_hash
     OR NEW.result_hash IS DISTINCT FROM OLD.result_hash
     OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
     OR NEW.payload_json IS DISTINCT FROM OLD.payload_json
     OR NEW.envelope_hash IS DISTINCT FROM OLD.envelope_hash
     OR NEW.received_at IS DISTINCT FROM OLD.received_at THEN
    RAISE EXCEPTION 'Business Publication Revision Envelope is immutable'
      USING ERRCODE='55000';
  END IF;
  RETURN NEW;
END
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: category_taxonomy; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.category_taxonomy (
    raw_value text NOT NULL,
    canonical_path text NOT NULL,
    version text NOT NULL
);


--
-- Name: TABLE category_taxonomy; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.category_taxonomy IS 'Current raw category mapping; version labels the mapping adapter and is not a historical key.';


--
-- Name: COLUMN category_taxonomy.version; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.category_taxonomy.version IS 'Mapping adapter version attached to the current row; table does not retain multiple versions per raw_value.';


--
-- Name: channel_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_links (
    id text NOT NULL,
    channel_id text NOT NULL,
    channel_snapshot_id text NOT NULL,
    link_type text NOT NULL,
    url text NOT NULL,
    title text,
    source text NOT NULL,
    raw_link jsonb NOT NULL,
    CONSTRAINT channel_links_text_shape CHECK (((btrim(link_type) <> ''::text) AND (btrim(url) <> ''::text) AND (btrim(source) <> ''::text)))
);


--
-- Name: TABLE channel_links; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.channel_links IS 'One sourced public link appearance inside a channel snapshot; import does not imply verification.';


--
-- Name: channel_metric_values; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_metric_values (
    id text NOT NULL,
    channel_id text NOT NULL,
    channel_snapshot_id text NOT NULL,
    metric_key text NOT NULL,
    scope text DEFAULT 'all'::text NOT NULL,
    value_numeric numeric,
    value_json jsonb,
    formula_version text NOT NULL,
    status_reason text,
    computed_at timestamp with time zone DEFAULT now() NOT NULL,
    sample_size bigint DEFAULT 0 NOT NULL,
    population_size bigint DEFAULT 0 NOT NULL,
    coverage numeric(9,8) DEFAULT 0 NOT NULL,
    value_status text DEFAULT 'unavailable'::text NOT NULL,
    numerator_numeric numeric,
    denominator_numeric numeric,
    baseline_snapshot_id text,
    actual_interval_seconds bigint,
    requested_window_days integer,
    CONSTRAINT channel_metric_values_degraded_reason_shape CHECK (((value_status <> ALL (ARRAY['estimated'::text, 'recovered'::text])) OR (NULLIF(btrim(status_reason), ''::text) IS NOT NULL))),
    CONSTRAINT channel_metric_values_history_shape CHECK ((((actual_interval_seconds IS NULL) OR (actual_interval_seconds >= 0)) AND ((requested_window_days IS NULL) OR (requested_window_days > 0)))),
    CONSTRAINT channel_metric_values_operand_shape CHECK (((metric_key <> ALL (ARRAY['content_count'::text, 'observed_content_views'::text, 'average_views'::text, 'average_likes'::text, 'average_comments'::text, 'engagement_rate_by_views'::text, 'views_subscribers_ratio'::text, 'likes_subscribers_ratio'::text, 'comments_subscribers_ratio'::text, 'subscriber_delta'::text, 'subscriber_growth_rate'::text, 'content_view_delta'::text, 'content_like_delta'::text, 'content_comment_delta'::text])) OR (value_status = ANY (ARRAY['unavailable'::text, 'placeholder'::text])) OR ((numerator_numeric IS NOT NULL) AND (denominator_numeric IS NOT NULL) AND (denominator_numeric <> (0)::numeric)))),
    CONSTRAINT channel_metric_values_state_shape CHECK (((btrim(metric_key) <> ''::text) AND (btrim(scope) <> ''::text) AND (btrim(formula_version) <> ''::text) AND (sample_size >= 0) AND (population_size >= 0) AND (sample_size <= population_size) AND ((coverage >= (0)::numeric) AND (coverage <= (1)::numeric)) AND (coverage =
CASE
    WHEN (population_size = 0) THEN (0)::numeric
    ELSE round(((sample_size)::numeric / (population_size)::numeric), 8)
END) AND (value_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'recovered'::text, 'unavailable'::text, 'placeholder'::text])) AND
CASE
    WHEN (value_status = ANY (ARRAY['unavailable'::text, 'placeholder'::text])) THEN ((num_nonnulls(value_numeric, value_json) = 0) AND (NULLIF(btrim(status_reason), ''::text) IS NOT NULL))
    ELSE (num_nonnulls(value_numeric, value_json) = 1)
END))
);


--
-- Name: TABLE channel_metric_values; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.channel_metric_values IS 'Current materialized metric result per snapshot, metric key, and scope; not an immutable recomputation ledger.';


--
-- Name: COLUMN channel_metric_values.scope; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_metric_values.scope IS 'Metric population selector; current legacy values mix content windows, kinds, and comparison scopes.';


--
-- Name: COLUMN channel_metric_values.formula_version; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_metric_values.formula_version IS 'Legacy opaque formula specification plus version label; not a pure version number.';


--
-- Name: COLUMN channel_metric_values.status_reason; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_metric_values.status_reason IS 'Reason for unavailable/placeholder values and optional explanation for other degraded states.';


--
-- Name: COLUMN channel_metric_values.coverage; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_metric_values.coverage IS 'round(sample_size/population_size,8), or 0 for an empty population; independent from value_status.';


--
-- Name: COLUMN channel_metric_values.value_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_metric_values.value_status IS 'Availability/quality state; estimated can result from partial coverage, stale inputs, or an approximate denominator.';


--
-- Name: COLUMN channel_metric_values.numerator_numeric; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_metric_values.numerator_numeric IS 'Auditable formula numerator when the metric has a canonical algebraic operand pair.';


--
-- Name: COLUMN channel_metric_values.denominator_numeric; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_metric_values.denominator_numeric IS 'Auditable nonzero formula denominator when the metric has a canonical algebraic operand pair.';


--
-- Name: CONSTRAINT channel_metric_values_degraded_reason_shape ON channel_metric_values; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON CONSTRAINT channel_metric_values_degraded_reason_shape ON public.channel_metric_values IS 'Estimated and recovered metrics require a non-empty audit reason.';


--
-- Name: channel_profile_facts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_profile_facts (
    id text NOT NULL,
    channel_id text NOT NULL,
    channel_snapshot_id text NOT NULL,
    field_key text NOT NULL,
    value_json jsonb NOT NULL,
    source text NOT NULL,
    confidence text NOT NULL,
    evidence jsonb DEFAULT '[]'::jsonb NOT NULL,
    source_urls text[] DEFAULT '{}'::text[] NOT NULL,
    reason text,
    provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT channel_profile_facts_audit_shape CHECK (((btrim(field_key) <> ''::text) AND (btrim(source) <> ''::text) AND (confidence = ANY (ARRAY['low'::text, 'medium'::text, 'high'::text])) AND (jsonb_typeof(evidence) = 'array'::text) AND (jsonb_typeof(provenance) = 'object'::text) AND
CASE field_key
    WHEN 'active_subscriber_ratio'::text THEN ((jsonb_typeof(value_json) = 'number'::text) AND ((((value_json #>> '{}'::text[]))::numeric >= (0)::numeric) AND (((value_json #>> '{}'::text[]))::numeric <= (100)::numeric)))
    WHEN 'audience_age_gender'::text THEN (jsonb_typeof(value_json) = 'array'::text)
    WHEN 'audience_interests'::text THEN (jsonb_typeof(value_json) = 'array'::text)
    WHEN 'audience_language'::text THEN (jsonb_typeof(value_json) = 'array'::text)
    WHEN 'audience_region'::text THEN (jsonb_typeof(value_json) = 'array'::text)
    WHEN 'channel_categories'::text THEN (jsonb_typeof(value_json) = 'array'::text)
    WHEN 'channel_tags'::text THEN (jsonb_typeof(value_json) = 'object'::text)
    WHEN 'country'::text THEN (jsonb_typeof(value_json) = 'string'::text)
    WHEN 'creator_age_range'::text THEN (jsonb_typeof(value_json) = 'number'::text)
    WHEN 'creator_gender'::text THEN (jsonb_typeof(value_json) = 'string'::text)
    WHEN 'creator_language'::text THEN (jsonb_typeof(value_json) = 'string'::text)
    ELSE true
END))
);


--
-- Name: TABLE channel_profile_facts; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.channel_profile_facts IS 'One model/profile fact key per channel snapshot, with audit provenance.';


--
-- Name: COLUMN channel_profile_facts.field_key; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_profile_facts.field_key IS 'Source-adapter fact key; creator_age_range is a known legacy scalar-name mismatch.';


--
-- Name: COLUMN channel_profile_facts.value_json; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_profile_facts.value_json IS 'Stored model/profile value exactly as admitted by the versioned adapter.';


--
-- Name: COLUMN channel_profile_facts.provenance; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_profile_facts.provenance IS 'Fact-level source and carry-forward lineage; snapshot time does not imply fact observation time.';


--
-- Name: channel_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_snapshots (
    id text NOT NULL,
    channel_id text NOT NULL,
    import_batch_id text NOT NULL,
    captured_at timestamp with time zone NOT NULL,
    title text NOT NULL,
    handle text,
    country_text text,
    avatar_url text,
    description text,
    is_verified boolean,
    subscriber_count_text text,
    view_count_text text,
    video_count_text text,
    joined_date_text text,
    subscriber_count bigint,
    total_view_count bigint,
    video_count integer,
    joined_date date,
    raw_channel jsonb NOT NULL,
    parse_status jsonb DEFAULT '{}'::jsonb NOT NULL,
    channel_url text,
    source_status text,
    source_reject_reason text,
    source_priority integer,
    source_ready_for_agent boolean,
    source_agent_status text,
    source_latest_run_id text,
    source_created_at timestamp with time zone,
    source_updated_at timestamp with time zone,
    subscriber_count_status text DEFAULT 'unavailable'::text NOT NULL,
    total_view_count_status text DEFAULT 'unavailable'::text NOT NULL,
    video_count_status text DEFAULT 'unavailable'::text NOT NULL,
    joined_date_status text DEFAULT 'unavailable'::text NOT NULL,
    candidate_last_published_at timestamp with time zone,
    candidate_last_published_status text,
    candidate_last_published_source text,
    candidate_last_published_date date,
    is_verified_status text DEFAULT 'unknown'::text NOT NULL,
    channel_observed_at timestamp with time zone NOT NULL,
    subscriber_count_observed_at timestamp with time zone,
    total_view_count_observed_at timestamp with time zone,
    video_count_observed_at timestamp with time zone,
    youtube_business_email_available boolean,
    youtube_business_email_observed_at timestamp with time zone,
    CONSTRAINT channel_snapshot_metric_time_shape CHECK (((channel_observed_at IS NOT NULL) AND (channel_observed_at <= captured_at) AND (NOT (subscriber_count_observed_at IS DISTINCT FROM
CASE
    WHEN (subscriber_count IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)) AND (NOT (total_view_count_observed_at IS DISTINCT FROM
CASE
    WHEN (total_view_count IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)) AND (NOT (video_count_observed_at IS DISTINCT FROM
CASE
    WHEN (video_count IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)))),
    CONSTRAINT channel_snapshots_candidate_publication_precision_shape CHECK ((((candidate_last_published_at IS NULL) AND (candidate_last_published_date IS NULL) AND (candidate_last_published_status IS NULL) AND (candidate_last_published_source IS NULL)) OR ((candidate_last_published_status = 'exact'::text) AND (candidate_last_published_at IS NOT NULL) AND (candidate_last_published_date IS NOT NULL) AND (candidate_last_published_date = ((candidate_last_published_at AT TIME ZONE 'UTC'::text))::date) AND (candidate_last_published_at <= captured_at) AND (candidate_last_published_source IS NOT NULL) AND (btrim(candidate_last_published_source) <> ''::text)) OR ((candidate_last_published_status = 'date_exact'::text) AND (candidate_last_published_at IS NULL) AND (candidate_last_published_date IS NOT NULL) AND (candidate_last_published_date <= ((captured_at AT TIME ZONE 'UTC'::text))::date) AND (candidate_last_published_source IS NOT NULL) AND (btrim(candidate_last_published_source) <> ''::text)))),
    CONSTRAINT channel_snapshots_nonnegative_counts CHECK ((((subscriber_count IS NULL) OR (subscriber_count >= 0)) AND ((total_view_count IS NULL) OR (total_view_count >= 0)) AND ((video_count IS NULL) OR (video_count >= 0)))),
    CONSTRAINT channel_snapshots_observation_status_check CHECK (((subscriber_count_status = ANY (ARRAY['exact'::text, 'approximate'::text, 'unavailable'::text])) AND (total_view_count_status = ANY (ARRAY['exact'::text, 'approximate'::text, 'unavailable'::text])) AND (video_count_status = ANY (ARRAY['exact'::text, 'approximate'::text, 'unavailable'::text])) AND (joined_date_status = ANY (ARRAY['exact'::text, 'approximate'::text, 'unavailable'::text])))),
    CONSTRAINT channel_snapshots_value_status_shape CHECK ((((subscriber_count_status = 'unavailable'::text) = (subscriber_count IS NULL)) AND ((total_view_count_status = 'unavailable'::text) = (total_view_count IS NULL)) AND ((video_count_status = 'unavailable'::text) = (video_count IS NULL)) AND ((joined_date_status = 'unavailable'::text) = (joined_date IS NULL)))),
    CONSTRAINT channel_snapshots_verified_shape CHECK ((((is_verified_status = 'verified'::text) AND (is_verified IS TRUE)) OR ((is_verified_status = 'not_verified'::text) AND (is_verified IS FALSE)) OR ((is_verified_status = 'unknown'::text) AND (is_verified IS NULL)))),
    CONSTRAINT channel_snapshots_youtube_business_email_shape CHECK ((((youtube_business_email_available IS NULL) = (youtube_business_email_observed_at IS NULL)) AND ((youtube_business_email_observed_at IS NULL) OR (youtube_business_email_observed_at <= captured_at))))
);


--
-- Name: TABLE channel_snapshots; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.channel_snapshots IS 'One observed channel state per channel and import batch.';


--
-- Name: COLUMN channel_snapshots.captured_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.captured_at IS 'UTC time when the composite Channel, Video, and Agent Snapshot was built';


--
-- Name: COLUMN channel_snapshots.country_text; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.country_text IS 'Channel-level country/region observation; never an audience-country value.';


--
-- Name: COLUMN channel_snapshots.is_verified; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.is_verified IS 'Observed YouTube channel verification badge; unrelated to contact verification.';


--
-- Name: COLUMN channel_snapshots.candidate_last_published_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.candidate_last_published_at IS 'Latest trustworthy exact publication instant from the latest-run candidate ledger; NULL for date-only observations.';


--
-- Name: COLUMN channel_snapshots.candidate_last_published_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.candidate_last_published_status IS 'Candidate publication precision status: exact for an observed instant, date_exact for an observed civil date, or NULL.';


--
-- Name: COLUMN channel_snapshots.candidate_last_published_source; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.candidate_last_published_source IS 'Crawler source recorded for the candidate publication observation.';


--
-- Name: COLUMN channel_snapshots.candidate_last_published_date; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.candidate_last_published_date IS 'Latest trustworthy UTC civil publication date from the latest-run candidate ledger across exact and date-only observations.';


--
-- Name: COLUMN channel_snapshots.channel_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.channel_observed_at IS 'UTC source Observation time for Channel identity and aggregate counts';


--
-- Name: COLUMN channel_snapshots.subscriber_count_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.subscriber_count_observed_at IS 'UTC source Observation time for subscriber_count';


--
-- Name: COLUMN channel_snapshots.total_view_count_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.total_view_count_observed_at IS 'UTC source Observation time for total_view_count';


--
-- Name: COLUMN channel_snapshots.video_count_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.video_count_observed_at IS 'UTC source Observation time for video_count';


--
-- Name: COLUMN channel_snapshots.youtube_business_email_available; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.youtube_business_email_available IS 'Whether the latest successful YouTube About observation exposed a business email entry';


--
-- Name: COLUMN channel_snapshots.youtube_business_email_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channel_snapshots.youtube_business_email_observed_at IS 'UTC source About observation time for youtube_business_email_available';


--
-- Name: CONSTRAINT channel_snapshot_metric_time_shape ON channel_snapshots; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON CONSTRAINT channel_snapshot_metric_time_shape ON public.channel_snapshots IS 'Channel aggregate metric timestamps are UTC source Observation times';


--
-- Name: channels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channels (
    channel_id text NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_handle text,
    last_title text,
    CONSTRAINT channels_seen_order_check CHECK ((first_seen_at <= last_seen_at))
);


--
-- Name: TABLE channels; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.channels IS 'Stable YouTube channel identity with first/last seen cache metadata.';


--
-- Name: COLUMN channels.last_handle; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channels.last_handle IS 'Latest-known identity cache; channel_snapshots remain the observation history.';


--
-- Name: COLUMN channels.last_title; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.channels.last_title IS 'Latest-known identity cache; channel_snapshots remain the observation history.';


--
-- Name: content_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_items (
    video_id text NOT NULL,
    channel_id text NOT NULL,
    url text,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT content_items_seen_order_check CHECK ((first_seen_at <= last_seen_at))
);


--
-- Name: TABLE content_items; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_items IS 'Stable YouTube content identity owned by one channel.';


--
-- Name: COLUMN content_items.video_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_items.video_id IS 'Legacy-named YouTube content identifier; also identifies Shorts and live content.';


--
-- Name: COLUMN content_items.url; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_items.url IS 'Latest-known stable content locator; snapshot URLs preserve appearance-specific evidence.';


--
-- Name: content_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_snapshots (
    id text NOT NULL,
    channel_snapshot_id text NOT NULL,
    video_id text NOT NULL,
    content_kind text NOT NULL,
    title text NOT NULL,
    thumbnail_url text,
    published_text text,
    published_date date,
    view_count_text text,
    view_count bigint,
    like_count bigint,
    comment_count bigint,
    length_text text,
    duration_seconds integer,
    url text,
    raw_item jsonb NOT NULL,
    source_content_key text,
    source_run_id text,
    source_content_type text,
    published_at timestamp with time zone,
    published_at_status text DEFAULT 'unresolved'::text NOT NULL,
    published_at_source text,
    is_recent boolean DEFAULT true NOT NULL,
    is_canonical boolean DEFAULT true NOT NULL,
    view_count_status text DEFAULT 'unavailable'::text NOT NULL,
    source_first_seen_at timestamp with time zone,
    source_last_seen_at timestamp with time zone,
    source_last_enriched_at timestamp with time zone,
    parse_status jsonb DEFAULT '{}'::jsonb NOT NULL,
    like_count_status text DEFAULT 'unavailable'::text NOT NULL,
    comment_count_status text DEFAULT 'unavailable'::text NOT NULL,
    duration_status text DEFAULT 'unavailable'::text NOT NULL,
    view_count_observed_at timestamp with time zone,
    like_count_observed_at timestamp with time zone,
    comment_count_observed_at timestamp with time zone,
    source_url text,
    channel_id text NOT NULL,
    comments_disabled boolean,
    is_members_only boolean DEFAULT false NOT NULL,
    access_status text DEFAULT 'unknown'::text NOT NULL,
    access_status_source text,
    source_position integer,
    published_at_precision text,
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
    CONSTRAINT content_snapshots_access_shape CHECK (((access_status = ANY (ARRAY['public'::text, 'unlisted'::text, 'login_required'::text, 'members_only'::text, 'unavailable'::text, 'unknown'::text])) AND ((access_status_source IS NULL) OR (btrim(access_status_source) <> ''::text)) AND ((source_position IS NULL) OR (source_position > 0)) AND ((comments_disabled IS DISTINCT FROM true) OR ((comment_count = 0) AND (comment_count_status = 'exact'::text))))),
    CONSTRAINT content_snapshots_content_kind_check CHECK ((content_kind = ANY (ARRAY['videos'::text, 'shorts'::text, 'lives'::text]))),
    CONSTRAINT content_snapshots_description_shape CHECK (((description_status = ANY (ARRAY['exact'::text, 'empty'::text, 'unavailable'::text, 'unresolved'::text])) AND (((description_status = 'exact'::text) AND (description IS NOT NULL) AND (description <> ''::text)) OR ((description_status = 'empty'::text) AND (description = ''::text)) OR ((description_status = ANY (ARRAY['unavailable'::text, 'unresolved'::text])) AND (description IS NULL))) AND ((description_source IS NULL) OR (btrim(description_source) <> ''::text)))),
    CONSTRAINT content_snapshots_nonnegative_counts CHECK ((((view_count IS NULL) OR (view_count >= 0)) AND ((like_count IS NULL) OR (like_count >= 0)) AND ((comment_count IS NULL) OR (comment_count >= 0)) AND ((duration_seconds IS NULL) OR (duration_seconds >= 0)))),
    CONSTRAINT content_snapshots_observation_status_check CHECK (((like_count_status = ANY (ARRAY['exact'::text, 'stale'::text, 'unavailable'::text])) AND (comment_count_status = ANY (ARRAY['exact'::text, 'stale'::text, 'unavailable'::text])) AND (duration_status = ANY (ARRAY['exact'::text, 'stale'::text, 'unavailable'::text])))),
    CONSTRAINT content_snapshots_precision_shape CHECK (((published_at_precision IS NULL) OR ((published_at_precision = 'date_only'::text) AND (published_at_status = 'date_exact'::text) AND (published_at IS NULL) AND (published_date IS NOT NULL)) OR ((published_at_precision = 'second'::text) AND (published_at_status = 'exact'::text) AND (published_at IS NOT NULL)))),
    CONSTRAINT content_snapshots_publication_shape CHECK (
CASE published_at_status
    WHEN 'exact'::text THEN ((published_at IS NOT NULL) AND (published_date = ((published_at AT TIME ZONE 'UTC'::text))::date))
    WHEN 'date_exact'::text THEN ((published_at IS NULL) AND (published_date IS NOT NULL))
    WHEN 'relative'::text THEN ((published_at IS NULL) AND (published_date IS NULL))
    WHEN 'unresolved'::text THEN ((published_at IS NULL) AND (published_date IS NULL))
    WHEN 'estimated'::text THEN true
    ELSE false
END),
    CONSTRAINT content_snapshots_published_status_check CHECK (((published_at_status IS NULL) OR (published_at_status = ANY (ARRAY['exact'::text, 'date_exact'::text, 'relative'::text, 'estimated'::text, 'unresolved'::text])))),
    CONSTRAINT content_snapshots_source_key_nonblank CHECK (((source_content_key IS NULL) OR (btrim(source_content_key) <> ''::text))),
    CONSTRAINT content_snapshots_value_status_shape CHECK ((((view_count_status = 'unavailable'::text) = (view_count IS NULL)) AND ((view_count_status = 'unavailable'::text) = (view_count_observed_at IS NULL)) AND ((like_count_status = 'unavailable'::text) = (like_count IS NULL)) AND ((like_count_status = 'unavailable'::text) = (like_count_observed_at IS NULL)) AND ((comment_count_status = 'unavailable'::text) = (comment_count IS NULL)) AND ((comment_count_status = 'unavailable'::text) = (comment_count_observed_at IS NULL)) AND ((duration_status = 'unavailable'::text) = (duration_seconds IS NULL)))),
    CONSTRAINT content_snapshots_view_count_status_check CHECK ((view_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'recovered'::text, 'unavailable'::text])))
);


--
-- Name: TABLE content_snapshots; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_snapshots IS 'One source appearance/observation of content inside a channel snapshot.';


--
-- Name: COLUMN content_snapshots.published_date; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.published_date IS 'UTC civil date used by exact/date_exact publication semantics.';


--
-- Name: COLUMN content_snapshots.published_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.published_at IS 'Observed/estimated timestamp only; remains NULL for date_exact observations.';


--
-- Name: COLUMN content_snapshots.published_at_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.published_at_status IS 'Publication precision: exact, date_exact, relative, estimated, or unresolved.';


--
-- Name: COLUMN content_snapshots.is_recent; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.is_recent IS 'Importer sample-membership flag at capture time; not proof of a complete N-day window.';


--
-- Name: COLUMN content_snapshots.is_canonical; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.is_canonical IS 'Exactly one metric-eligible canonical appearance per snapshot and content identity.';


--
-- Name: COLUMN content_snapshots.comments_disabled; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.comments_disabled IS 'True when the source explicitly reports comments disabled; null means the source did not classify this state.';


--
-- Name: COLUMN content_snapshots.access_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.access_status IS 'Source access state such as public or login_required; it does not imply content deletion.';


--
-- Name: COLUMN content_snapshots.source_position; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.source_position IS 'One-based crawler candidate position inside the source run.';


--
-- Name: COLUMN content_snapshots.published_at_precision; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.published_at_precision IS 'Preserved source precision used to distinguish exact instants from exact civil dates.';


--
-- Name: COLUMN content_snapshots.comment_count_source; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.comment_count_source IS 'Business observation lineage for the accepted comment count.';


--
-- Name: COLUMN content_snapshots.description; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.description IS 'Business content observation retained for backend use; not exposed by the public content DTO.';


--
-- Name: COLUMN content_snapshots.hashtags; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.hashtags IS 'Business copy of crawler-extracted content hashtags.';


--
-- Name: COLUMN content_snapshots.keywords; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.content_snapshots.keywords IS 'Business copy of crawler-extracted content keywords retained for backend analysis.';


--
-- Name: content_type_taxonomy; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.content_type_taxonomy (
    source_content_type text NOT NULL,
    content_kind text NOT NULL,
    canonical_priority smallint NOT NULL,
    CONSTRAINT content_type_taxonomy_canonical_priority_check CHECK ((canonical_priority > 0)),
    CONSTRAINT content_type_taxonomy_content_kind_check CHECK ((content_kind = ANY (ARRAY['videos'::text, 'shorts'::text, 'lives'::text]))),
    CONSTRAINT content_type_taxonomy_source_content_type_check CHECK ((source_content_type = ANY (ARRAY['video'::text, 'short'::text, 'live'::text])))
);


--
-- Name: TABLE content_type_taxonomy; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.content_type_taxonomy IS 'Mapping from source content type to canonical product content kind.';


--
-- Name: crawler_ingest_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.crawler_ingest_batches (
    import_batch_id text NOT NULL,
    source_file text NOT NULL,
    source_sha256 text NOT NULL,
    source_bytes bigint NOT NULL,
    exported_at timestamp with time zone NOT NULL,
    source_postgres_version text NOT NULL,
    expected_row_count bigint NOT NULL,
    row_counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'loading'::text NOT NULL,
    published_at timestamp with time zone,
    error_summary text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT crawler_ingest_batches_check CHECK (((status = 'published'::text) = (published_at IS NOT NULL))),
    CONSTRAINT crawler_ingest_batches_expected_row_count_check CHECK ((expected_row_count >= 0)),
    CONSTRAINT crawler_ingest_batches_manifest_shape CHECK (((btrim(source_file) <> ''::text) AND (btrim(source_postgres_version) <> ''::text) AND (jsonb_typeof(row_counts) = 'object'::text) AND
CASE
    WHEN (status = 'failed'::text) THEN (NULLIF(btrim(error_summary), ''::text) IS NOT NULL)
    ELSE (error_summary IS NULL)
END)),
    CONSTRAINT crawler_ingest_batches_source_bytes_check CHECK ((source_bytes > 0)),
    CONSTRAINT crawler_ingest_batches_source_sha256_check CHECK ((source_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT crawler_ingest_batches_status_check CHECK ((status = ANY (ARRAY['loading'::text, 'published'::text, 'failed'::text])))
);


--
-- Name: TABLE crawler_ingest_batches; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.crawler_ingest_batches IS 'One-to-one crawler dump manifest subtype of import_batches.';


--
-- Name: creator_categories; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_categories (
    id bigint NOT NULL,
    category_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    retired_at timestamp with time zone,
    CONSTRAINT creator_categories_key_shape CHECK ((category_key ~ '^cat_[0-9]{6}$'::text)),
    CONSTRAINT creator_categories_retirement_shape CHECK (((retired_at IS NULL) OR (retired_at >= created_at)))
);


--
-- Name: TABLE creator_categories; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_categories IS 'Stable, language-neutral creator category identities. Labels and tree placement are versioned separately.';


--
-- Name: COLUMN creator_categories.category_key; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_categories.category_key IS 'Stable public category key. Database relationships use category_id, never localized labels.';


--
-- Name: creator_categories_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_categories ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_categories_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_category_assignments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_category_assignments (
    id bigint NOT NULL,
    channel_id text NOT NULL,
    taxonomy_version_id bigint NOT NULL,
    category_id bigint NOT NULL,
    claim_id bigint NOT NULL,
    is_primary boolean DEFAULT false NOT NULL,
    valid_from timestamp with time zone DEFAULT now() NOT NULL,
    valid_until timestamp with time zone,
    assigned_by text NOT NULL,
    assignment_note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT creator_category_assignments_assigned_by_shape CHECK ((btrim(assigned_by) <> ''::text)),
    CONSTRAINT creator_category_assignments_validity_shape CHECK (((valid_until IS NULL) OR (valid_until > valid_from)))
);


--
-- Name: TABLE creator_category_assignments; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_category_assignments IS 'Human-confirmed, time-bounded channel classifications backed by accepted claims.';


--
-- Name: creator_category_assignments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_category_assignments ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_category_assignments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_category_claims; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_category_claims (
    id bigint NOT NULL,
    channel_id text NOT NULL,
    channel_snapshot_id text,
    taxonomy_version_id bigint NOT NULL,
    category_id bigint NOT NULL,
    source_kind text NOT NULL,
    source_system text NOT NULL,
    source_record_key text,
    evidence jsonb NOT NULL,
    confidence numeric(5,4),
    status text DEFAULT 'proposed'::text NOT NULL,
    reviewed_by text,
    review_note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    reviewed_at timestamp with time zone,
    classification_run_id bigint,
    CONSTRAINT creator_category_claims_classification_run_shape CHECK (((classification_run_id IS NULL) OR ((source_kind = 'model'::text) AND (channel_snapshot_id IS NOT NULL)))),
    CONSTRAINT creator_category_claims_confidence_shape CHECK (((confidence IS NULL) OR ((confidence >= (0)::numeric) AND (confidence <= (1)::numeric)))),
    CONSTRAINT creator_category_claims_evidence_shape CHECK ((jsonb_typeof(evidence) = ANY (ARRAY['array'::text, 'object'::text]))),
    CONSTRAINT creator_category_claims_review_shape CHECK ((((status = 'proposed'::text) AND (reviewed_by IS NULL) AND (reviewed_at IS NULL)) OR ((status = ANY (ARRAY['accepted'::text, 'rejected'::text, 'superseded'::text])) AND (reviewed_by IS NOT NULL) AND (btrim(reviewed_by) <> ''::text) AND (reviewed_at IS NOT NULL)))),
    CONSTRAINT creator_category_claims_source_kind_shape CHECK ((source_kind = ANY (ARRAY['manual'::text, 'agent'::text, 'model'::text, 'rule'::text, 'import'::text, 'other'::text]))),
    CONSTRAINT creator_category_claims_source_record_shape CHECK (((source_record_key IS NULL) OR (btrim(source_record_key) <> ''::text))),
    CONSTRAINT creator_category_claims_source_system_shape CHECK ((btrim(source_system) <> ''::text)),
    CONSTRAINT creator_category_claims_status_shape CHECK ((status = ANY (ARRAY['proposed'::text, 'accepted'::text, 'rejected'::text, 'superseded'::text])))
);


--
-- Name: TABLE creator_category_claims; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_category_claims IS 'Unconfirmed, provenance-bearing category proposals. No legacy agent labels are imported automatically.';


--
-- Name: creator_category_claims_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_category_claims ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_category_claims_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_classification_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_classification_runs (
    id bigint NOT NULL,
    channel_id text NOT NULL,
    channel_snapshot_id text NOT NULL,
    taxonomy_version_id bigint NOT NULL,
    requested_model text NOT NULL,
    prompt_version text NOT NULL,
    cleaner_version text NOT NULL,
    input_hash text NOT NULL,
    input_content_ids text[] DEFAULT '{}'::text[] NOT NULL,
    input_summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    response_json jsonb,
    raw_response_json jsonb,
    usage_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    provider_request_id text,
    response_model text,
    system_fingerprint text,
    finish_reason text,
    error_code text,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_expires_at timestamp with time zone,
    completed_at timestamp with time zone,
    CONSTRAINT creator_classification_runs_input_hash_shape CHECK ((input_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT creator_classification_runs_input_summary_shape CHECK ((jsonb_typeof(input_summary) = 'object'::text)),
    CONSTRAINT creator_classification_runs_optional_text_shape CHECK ((((provider_request_id IS NULL) OR (btrim(provider_request_id) <> ''::text)) AND ((response_model IS NULL) OR (btrim(response_model) <> ''::text)) AND ((system_fingerprint IS NULL) OR (btrim(system_fingerprint) <> ''::text)) AND ((finish_reason IS NULL) OR (btrim(finish_reason) <> ''::text)) AND ((error_code IS NULL) OR (btrim(error_code) <> ''::text)) AND ((error_message IS NULL) OR (btrim(error_message) <> ''::text)))),
    CONSTRAINT creator_classification_runs_raw_response_shape CHECK (((raw_response_json IS NULL) OR (jsonb_typeof(raw_response_json) = 'object'::text))),
    CONSTRAINT creator_classification_runs_response_shape CHECK (((response_json IS NULL) OR (jsonb_typeof(response_json) = 'object'::text))),
    CONSTRAINT creator_classification_runs_state_shape CHECK ((((status = 'running'::text) AND (completed_at IS NULL) AND (lease_expires_at IS NOT NULL) AND (response_json IS NULL) AND (error_code IS NULL) AND (error_message IS NULL)) OR ((status = 'succeeded'::text) AND (completed_at IS NOT NULL) AND (lease_expires_at IS NULL) AND (response_json IS NOT NULL) AND (error_code IS NULL) AND (error_message IS NULL)) OR ((status = ANY (ARRAY['failed'::text, 'rejected'::text])) AND (completed_at IS NOT NULL) AND (lease_expires_at IS NULL) AND (error_code IS NOT NULL) AND (error_message IS NOT NULL)))),
    CONSTRAINT creator_classification_runs_status_shape CHECK ((status = ANY (ARRAY['running'::text, 'succeeded'::text, 'failed'::text, 'rejected'::text]))),
    CONSTRAINT creator_classification_runs_text_shape CHECK (((btrim(requested_model) <> ''::text) AND (btrim(prompt_version) <> ''::text) AND (btrim(cleaner_version) <> ''::text))),
    CONSTRAINT creator_classification_runs_time_shape CHECK (((started_at >= created_at) AND ((completed_at IS NULL) OR (completed_at >= started_at)) AND ((lease_expires_at IS NULL) OR (lease_expires_at > started_at)))),
    CONSTRAINT creator_classification_runs_usage_shape CHECK ((jsonb_typeof(usage_json) = 'object'::text))
);


--
-- Name: creator_classification_runs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_classification_runs ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_classification_runs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_content_tag_claims; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_content_tag_claims (
    id bigint NOT NULL,
    classification_run_id bigint NOT NULL,
    channel_id text NOT NULL,
    channel_snapshot_id text NOT NULL,
    tag_label text NOT NULL,
    normalized_label text NOT NULL,
    tag_type text NOT NULL,
    rank smallint NOT NULL,
    confidence numeric(5,4) NOT NULL,
    evidence jsonb NOT NULL,
    status text DEFAULT 'proposed'::text NOT NULL,
    reviewed_by text,
    review_note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    reviewed_at timestamp with time zone,
    CONSTRAINT creator_content_tag_claims_confidence_shape CHECK (((confidence >= (0)::numeric) AND (confidence <= (1)::numeric))),
    CONSTRAINT creator_content_tag_claims_evidence_shape CHECK ((jsonb_typeof(evidence) = 'object'::text)),
    CONSTRAINT creator_content_tag_claims_label_shape CHECK (((btrim(tag_label) <> ''::text) AND (btrim(normalized_label) <> ''::text))),
    CONSTRAINT creator_content_tag_claims_rank_shape CHECK (((rank >= 1) AND (rank <= 20))),
    CONSTRAINT creator_content_tag_claims_review_shape CHECK ((((status = 'proposed'::text) AND (reviewed_by IS NULL) AND (reviewed_at IS NULL)) OR ((status = ANY (ARRAY['accepted'::text, 'rejected'::text, 'superseded'::text])) AND (reviewed_by IS NOT NULL) AND (btrim(reviewed_by) <> ''::text) AND (reviewed_at IS NOT NULL)))),
    CONSTRAINT creator_content_tag_claims_status_shape CHECK ((status = ANY (ARRAY['proposed'::text, 'accepted'::text, 'rejected'::text, 'superseded'::text]))),
    CONSTRAINT creator_content_tag_claims_type_shape CHECK ((tag_type = ANY (ARRAY['entity'::text, 'topic'::text, 'intent'::text, 'format'::text])))
);


--
-- Name: creator_content_tag_claims_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_content_tag_claims ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_content_tag_claims_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_content_tag_evidence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_content_tag_evidence (
    tag_claim_id bigint NOT NULL,
    classification_run_id bigint NOT NULL,
    channel_id text NOT NULL,
    channel_snapshot_id text NOT NULL,
    content_snapshot_id text NOT NULL,
    evidence_rank smallint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT creator_content_tag_evidence_rank_shape CHECK ((evidence_rank > 0))
);


--
-- Name: creator_search_active; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_search_active (
    singleton boolean DEFAULT true NOT NULL,
    watermark text NOT NULL,
    CONSTRAINT creator_search_active_singleton_check CHECK (singleton)
);


--
-- Name: TABLE creator_search_active; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_search_active IS 'Singleton pointer to the active creator search release.';


--
-- Name: creator_search_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_search_current (
    channel_id text NOT NULL,
    snapshot_id text NOT NULL,
    captured_at timestamp with time zone NOT NULL,
    name text NOT NULL,
    handle text,
    avatar_url text,
    verified boolean,
    country text,
    language text,
    category_paths text[] DEFAULT '{}'::text[] NOT NULL,
    tags text[] DEFAULT '{}'::text[] NOT NULL,
    subscribers bigint,
    total_views bigint,
    channel_video_count integer,
    content_sample_count integer DEFAULT 0 NOT NULL,
    contact_types text[] DEFAULT '{}'::text[] NOT NULL,
    last_published_at timestamp with time zone,
    content_count_30d integer DEFAULT 0 NOT NULL,
    content_count_90d integer DEFAULT 0 NOT NULL,
    publish_weekly numeric(12,4),
    publish_monthly numeric(12,4),
    avg_views numeric,
    view_subscribers_ratio numeric,
    engagement_rate_by_views numeric,
    avg_likes numeric,
    likes_subscribers_ratio numeric,
    avg_comments numeric,
    comments_subscribers_ratio numeric,
    audience_country text,
    audience_country_share numeric,
    audience_language text,
    audience_language_share numeric,
    audience_gender text,
    audience_gender_share numeric,
    audience_age text,
    audience_age_share numeric,
    active_subscriber_ratio numeric,
    search_text text NOT NULL,
    projection_version text NOT NULL,
    watermark text NOT NULL,
    last_published_date date,
    country_source text,
    verified_status text DEFAULT 'unknown'::text NOT NULL,
    channel_observed_at timestamp with time zone NOT NULL,
    subscribers_observed_at timestamp with time zone,
    total_views_observed_at timestamp with time zone,
    channel_video_count_observed_at timestamp with time zone,
    youtube_business_email_available boolean,
    youtube_business_email_observed_at timestamp with time zone,
    CONSTRAINT creator_search_country_source_check CHECK (((country_source IS NULL) OR (country_source = ANY (ARRAY['channel'::text, 'agent'::text, 'unavailable'::text])))),
    CONSTRAINT creator_search_current_value_shape CHECK ((((subscribers IS NULL) OR (subscribers >= 0)) AND ((total_views IS NULL) OR (total_views >= 0)) AND ((channel_video_count IS NULL) OR (channel_video_count >= 0)) AND (content_sample_count >= 0) AND (content_count_30d >= 0) AND (content_count_90d >= content_count_30d) AND (content_sample_count >= content_count_90d) AND ((publish_weekly IS NULL) = (publish_monthly IS NULL)) AND ((publish_weekly IS NULL) OR (publish_weekly >= (0)::numeric)) AND ((publish_monthly IS NULL) OR (publish_monthly >= (0)::numeric)) AND ((avg_views IS NULL) OR (avg_views >= (0)::numeric)) AND ((view_subscribers_ratio IS NULL) OR (view_subscribers_ratio >= (0)::numeric)) AND ((engagement_rate_by_views IS NULL) OR (engagement_rate_by_views >= (0)::numeric)) AND ((avg_likes IS NULL) OR (avg_likes >= (0)::numeric)) AND ((likes_subscribers_ratio IS NULL) OR (likes_subscribers_ratio >= (0)::numeric)) AND ((avg_comments IS NULL) OR (avg_comments >= (0)::numeric)) AND ((comments_subscribers_ratio IS NULL) OR (comments_subscribers_ratio >= (0)::numeric)) AND ((audience_country_share IS NULL) OR ((audience_country_share >= (0)::numeric) AND (audience_country_share <= (100)::numeric))) AND ((audience_language_share IS NULL) OR ((audience_language_share >= (0)::numeric) AND (audience_language_share <= (100)::numeric))) AND ((audience_gender_share IS NULL) OR ((audience_gender_share >= (0)::numeric) AND (audience_gender_share <= (100)::numeric))) AND ((audience_age_share IS NULL) OR ((audience_age_share >= (0)::numeric) AND (audience_age_share <= (100)::numeric))) AND ((active_subscriber_ratio IS NULL) OR ((active_subscriber_ratio >= (0)::numeric) AND (active_subscriber_ratio <= (100)::numeric))) AND ((last_published_at IS NULL) OR (last_published_at <= captured_at)) AND ((last_published_date IS NULL) OR (last_published_date <= ((captured_at AT TIME ZONE 'UTC'::text))::date)) AND (btrim(projection_version) <> ''::text))),
    CONSTRAINT creator_search_current_verified_shape CHECK ((((verified_status = 'verified'::text) AND (verified IS TRUE)) OR ((verified_status = 'not_verified'::text) AND (verified IS FALSE)) OR ((verified_status = 'unknown'::text) AND (verified IS NULL)))),
    CONSTRAINT creator_search_metric_time_shape CHECK (((channel_observed_at IS NOT NULL) AND (channel_observed_at <= captured_at) AND (NOT (subscribers_observed_at IS DISTINCT FROM
CASE
    WHEN (subscribers IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)) AND (NOT (total_views_observed_at IS DISTINCT FROM
CASE
    WHEN (total_views IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)) AND (NOT (channel_video_count_observed_at IS DISTINCT FROM
CASE
    WHEN (channel_video_count IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)))),
    CONSTRAINT creator_search_youtube_business_email_shape CHECK ((((youtube_business_email_available IS NULL) = (youtube_business_email_observed_at IS NULL)) AND ((youtube_business_email_observed_at IS NULL) OR (youtube_business_email_observed_at <= captured_at))))
);


--
-- Name: TABLE creator_search_current; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_search_current IS 'One rebuildable search projection row per release watermark and channel; name retained for compatibility.';


--
-- Name: COLUMN creator_search_current.captured_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.captured_at IS 'UTC time when the composite Business Projection Snapshot was built';


--
-- Name: COLUMN creator_search_current.country; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.country IS 'Channel country/region projection only; inferred creator and audience countries are separate facts.';


--
-- Name: COLUMN creator_search_current.contact_types; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.contact_types IS 'Canonical link types materialized from channel_links for search filtering; refreshed as part of the release generation.';


--
-- Name: COLUMN creator_search_current.last_published_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.last_published_at IS 'Latest genuine exact publication timestamp only.';


--
-- Name: COLUMN creator_search_current.content_count_30d; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.content_count_30d IS 'Count of recent canonical video, short, and live content in the 30-day publication window.';


--
-- Name: COLUMN creator_search_current.content_count_90d; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.content_count_90d IS 'Count of recent canonical video, short, and live content in the 90-day publication window.';


--
-- Name: COLUMN creator_search_current.audience_country; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.audience_country IS 'Highest-ranked modeled audience-country segment, not any-segment membership.';


--
-- Name: COLUMN creator_search_current.audience_language; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.audience_language IS 'Highest-ranked modeled audience-language segment, not any-segment membership.';


--
-- Name: COLUMN creator_search_current.audience_gender; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.audience_gender IS 'Highest aggregate modeled audience-gender segment.';


--
-- Name: COLUMN creator_search_current.audience_age; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.audience_age IS 'One highest-ranked modeled audience age segment; ties are not multi-valued.';


--
-- Name: COLUMN creator_search_current.watermark; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.watermark IS 'Import-batch-backed release key; pair with creator_search_releases.generation for cache versioning.';


--
-- Name: COLUMN creator_search_current.last_published_date; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.last_published_date IS 'Latest supported UTC publication date across exact and date_exact observations.';


--
-- Name: COLUMN creator_search_current.country_source; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.country_source IS 'Whether the displayed country came from channel metadata or an agent profile fact.';


--
-- Name: COLUMN creator_search_current.channel_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.channel_observed_at IS 'UTC source Observation time for Channel identity and aggregate counts';


--
-- Name: COLUMN creator_search_current.subscribers_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.subscribers_observed_at IS 'UTC source Observation time for subscribers';


--
-- Name: COLUMN creator_search_current.total_views_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.total_views_observed_at IS 'UTC source Observation time for total_views';


--
-- Name: COLUMN creator_search_current.channel_video_count_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.channel_video_count_observed_at IS 'UTC source Observation time for channel_video_count';


--
-- Name: COLUMN creator_search_current.youtube_business_email_available; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.youtube_business_email_available IS 'Current business-query projection of the YouTube business email entry';


--
-- Name: COLUMN creator_search_current.youtube_business_email_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_current.youtube_business_email_observed_at IS 'UTC source About observation time for youtube_business_email_available';


--
-- Name: CONSTRAINT creator_search_metric_time_shape ON creator_search_current; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON CONSTRAINT creator_search_metric_time_shape ON public.creator_search_current IS 'Channel aggregate metric timestamps are UTC source Observation times';


--
-- Name: creator_search_live; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_search_live (
    channel_id text CONSTRAINT creator_search_current_channel_id_not_null NOT NULL,
    snapshot_id text CONSTRAINT creator_search_current_snapshot_id_not_null NOT NULL,
    captured_at timestamp with time zone CONSTRAINT creator_search_current_captured_at_not_null NOT NULL,
    name text CONSTRAINT creator_search_current_name_not_null NOT NULL,
    handle text,
    avatar_url text,
    verified boolean,
    country text,
    language text,
    category_paths text[] DEFAULT '{}'::text[] CONSTRAINT creator_search_current_category_paths_not_null NOT NULL,
    tags text[] DEFAULT '{}'::text[] CONSTRAINT creator_search_current_tags_not_null NOT NULL,
    subscribers bigint,
    total_views bigint,
    channel_video_count integer,
    content_sample_count integer DEFAULT 0 CONSTRAINT creator_search_current_content_sample_count_not_null NOT NULL,
    contact_types text[] DEFAULT '{}'::text[] CONSTRAINT creator_search_current_contact_types_not_null NOT NULL,
    last_published_at timestamp with time zone,
    content_count_30d integer DEFAULT 0 CONSTRAINT creator_search_current_content_count_30d_not_null NOT NULL,
    content_count_90d integer DEFAULT 0 CONSTRAINT creator_search_current_content_count_90d_not_null NOT NULL,
    publish_weekly numeric(12,4),
    publish_monthly numeric(12,4),
    avg_views numeric,
    view_subscribers_ratio numeric,
    engagement_rate_by_views numeric,
    avg_likes numeric,
    likes_subscribers_ratio numeric,
    avg_comments numeric,
    comments_subscribers_ratio numeric,
    audience_country text,
    audience_country_share numeric,
    audience_language text,
    audience_language_share numeric,
    audience_gender text,
    audience_gender_share numeric,
    audience_age text,
    audience_age_share numeric,
    active_subscriber_ratio numeric,
    search_text text CONSTRAINT creator_search_current_search_text_not_null NOT NULL,
    projection_version text CONSTRAINT creator_search_current_projection_version_not_null NOT NULL,
    watermark text CONSTRAINT creator_search_current_watermark_not_null NOT NULL,
    last_published_date date,
    country_source text,
    verified_status text DEFAULT 'unknown'::text CONSTRAINT creator_search_current_verified_status_not_null NOT NULL,
    channel_observed_at timestamp with time zone CONSTRAINT creator_search_current_channel_observed_at_not_null NOT NULL,
    subscribers_observed_at timestamp with time zone,
    total_views_observed_at timestamp with time zone,
    channel_video_count_observed_at timestamp with time zone,
    youtube_business_email_available boolean,
    youtube_business_email_observed_at timestamp with time zone,
    CONSTRAINT creator_search_country_source_check CHECK (((country_source IS NULL) OR (country_source = ANY (ARRAY['channel'::text, 'agent'::text, 'unavailable'::text])))),
    CONSTRAINT creator_search_current_value_shape CHECK ((((subscribers IS NULL) OR (subscribers >= 0)) AND ((total_views IS NULL) OR (total_views >= 0)) AND ((channel_video_count IS NULL) OR (channel_video_count >= 0)) AND (content_sample_count >= 0) AND (content_count_30d >= 0) AND (content_count_90d >= content_count_30d) AND (content_sample_count >= content_count_90d) AND ((publish_weekly IS NULL) = (publish_monthly IS NULL)) AND ((publish_weekly IS NULL) OR (publish_weekly >= (0)::numeric)) AND ((publish_monthly IS NULL) OR (publish_monthly >= (0)::numeric)) AND ((avg_views IS NULL) OR (avg_views >= (0)::numeric)) AND ((view_subscribers_ratio IS NULL) OR (view_subscribers_ratio >= (0)::numeric)) AND ((engagement_rate_by_views IS NULL) OR (engagement_rate_by_views >= (0)::numeric)) AND ((avg_likes IS NULL) OR (avg_likes >= (0)::numeric)) AND ((likes_subscribers_ratio IS NULL) OR (likes_subscribers_ratio >= (0)::numeric)) AND ((avg_comments IS NULL) OR (avg_comments >= (0)::numeric)) AND ((comments_subscribers_ratio IS NULL) OR (comments_subscribers_ratio >= (0)::numeric)) AND ((audience_country_share IS NULL) OR ((audience_country_share >= (0)::numeric) AND (audience_country_share <= (100)::numeric))) AND ((audience_language_share IS NULL) OR ((audience_language_share >= (0)::numeric) AND (audience_language_share <= (100)::numeric))) AND ((audience_gender_share IS NULL) OR ((audience_gender_share >= (0)::numeric) AND (audience_gender_share <= (100)::numeric))) AND ((audience_age_share IS NULL) OR ((audience_age_share >= (0)::numeric) AND (audience_age_share <= (100)::numeric))) AND ((active_subscriber_ratio IS NULL) OR ((active_subscriber_ratio >= (0)::numeric) AND (active_subscriber_ratio <= (100)::numeric))) AND ((last_published_at IS NULL) OR (last_published_at <= captured_at)) AND ((last_published_date IS NULL) OR (last_published_date <= ((captured_at AT TIME ZONE 'UTC'::text))::date)) AND (btrim(projection_version) <> ''::text))),
    CONSTRAINT creator_search_current_verified_shape CHECK ((((verified_status = 'verified'::text) AND (verified IS TRUE)) OR ((verified_status = 'not_verified'::text) AND (verified IS FALSE)) OR ((verified_status = 'unknown'::text) AND (verified IS NULL)))),
    CONSTRAINT creator_search_metric_time_shape CHECK (((channel_observed_at IS NOT NULL) AND (channel_observed_at <= captured_at) AND (NOT (subscribers_observed_at IS DISTINCT FROM
CASE
    WHEN (subscribers IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)) AND (NOT (total_views_observed_at IS DISTINCT FROM
CASE
    WHEN (total_views IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)) AND (NOT (channel_video_count_observed_at IS DISTINCT FROM
CASE
    WHEN (channel_video_count IS NULL) THEN NULL::timestamp with time zone
    ELSE channel_observed_at
END)))),
    CONSTRAINT creator_search_youtube_business_email_shape CHECK ((((youtube_business_email_available IS NULL) = (youtube_business_email_observed_at IS NULL)) AND ((youtube_business_email_observed_at IS NULL) OR (youtube_business_email_observed_at <= captured_at))))
);


--
-- Name: TABLE creator_search_live; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_search_live IS 'One directly queryable current Creator Search row per Channel';


--
-- Name: COLUMN creator_search_live.captured_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.captured_at IS 'UTC time when the composite Business Projection Snapshot was built';


--
-- Name: COLUMN creator_search_live.country; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.country IS 'Channel country/region projection only; inferred creator and audience countries are separate facts.';


--
-- Name: COLUMN creator_search_live.contact_types; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.contact_types IS 'Canonical link types materialized from channel_links for search filtering; refreshed as part of the release generation.';


--
-- Name: COLUMN creator_search_live.last_published_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.last_published_at IS 'Latest genuine exact publication timestamp only.';


--
-- Name: COLUMN creator_search_live.content_count_30d; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.content_count_30d IS 'Count of recent canonical video, short, and live content in the 30-day publication window.';


--
-- Name: COLUMN creator_search_live.content_count_90d; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.content_count_90d IS 'Count of recent canonical video, short, and live content in the 90-day publication window.';


--
-- Name: COLUMN creator_search_live.audience_country; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.audience_country IS 'Highest-ranked modeled audience-country segment, not any-segment membership.';


--
-- Name: COLUMN creator_search_live.audience_language; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.audience_language IS 'Highest-ranked modeled audience-language segment, not any-segment membership.';


--
-- Name: COLUMN creator_search_live.audience_gender; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.audience_gender IS 'Highest aggregate modeled audience-gender segment.';


--
-- Name: COLUMN creator_search_live.audience_age; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.audience_age IS 'One highest-ranked modeled audience age segment; ties are not multi-valued.';


--
-- Name: COLUMN creator_search_live.watermark; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.watermark IS 'Import-batch-backed release key; pair with creator_search_releases.generation for cache versioning.';


--
-- Name: COLUMN creator_search_live.last_published_date; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.last_published_date IS 'Latest supported UTC publication date across exact and date_exact observations.';


--
-- Name: COLUMN creator_search_live.country_source; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.country_source IS 'Whether the displayed country came from channel metadata or an agent profile fact.';


--
-- Name: COLUMN creator_search_live.channel_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.channel_observed_at IS 'UTC source Observation time for Channel identity and aggregate counts';


--
-- Name: COLUMN creator_search_live.subscribers_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.subscribers_observed_at IS 'UTC source Observation time for subscribers';


--
-- Name: COLUMN creator_search_live.total_views_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.total_views_observed_at IS 'UTC source Observation time for total_views';


--
-- Name: COLUMN creator_search_live.channel_video_count_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.channel_video_count_observed_at IS 'UTC source Observation time for channel_video_count';


--
-- Name: COLUMN creator_search_live.youtube_business_email_available; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.youtube_business_email_available IS 'Current business-query projection of the YouTube business email entry';


--
-- Name: COLUMN creator_search_live.youtube_business_email_observed_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_live.youtube_business_email_observed_at IS 'UTC source About observation time for youtube_business_email_available';


--
-- Name: CONSTRAINT creator_search_metric_time_shape ON creator_search_live; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON CONSTRAINT creator_search_metric_time_shape ON public.creator_search_live IS 'Channel aggregate metric timestamps are UTC source Observation times';


--
-- Name: creator_search_releases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_search_releases (
    watermark text NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    activated_at timestamp with time zone,
    generation bigint DEFAULT 1 NOT NULL,
    rebuilt_at timestamp with time zone DEFAULT now() NOT NULL,
    previous_watermark text,
    storage_mode text,
    changed_channel_count integer,
    rolled_back_at timestamp with time zone,
    rollback_actor text,
    rollback_reason text,
    CONSTRAINT creator_search_releases_lifecycle_shape CHECK (((generation > 0) AND
CASE
    WHEN (status = 'building'::text) THEN (activated_at IS NULL)
    ELSE (activated_at IS NOT NULL)
END AND (rebuilt_at >= created_at))),
    CONSTRAINT creator_search_releases_status_check CHECK ((status = ANY (ARRAY['building'::text, 'active'::text, 'retired'::text])))
);


--
-- Name: TABLE creator_search_releases; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_search_releases IS 'Lifecycle and generation metadata for rebuildable creator search cache releases.';


--
-- Name: COLUMN creator_search_releases.generation; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_releases.generation IS 'Monotonic rebuild generation within one watermark; cursor validity binds to it.';


--
-- Name: COLUMN creator_search_releases.rebuilt_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.creator_search_releases.rebuilt_at IS 'Time the current cache generation was completed.';


--
-- Name: creator_taxonomy_closure; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_taxonomy_closure (
    id bigint NOT NULL,
    version_id bigint NOT NULL,
    ancestor_node_id bigint NOT NULL,
    descendant_node_id bigint NOT NULL,
    depth smallint NOT NULL,
    CONSTRAINT creator_taxonomy_closure_depth_shape CHECK (((depth >= 0) AND (depth <= 2))),
    CONSTRAINT creator_taxonomy_closure_self_shape CHECK (((depth = 0) = (ancestor_node_id = descendant_node_id)))
);


--
-- Name: TABLE creator_taxonomy_closure; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_taxonomy_closure IS 'Ancestor/descendant paths for exact and subtree category queries.';


--
-- Name: creator_taxonomy_closure_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_taxonomy_closure ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_taxonomy_closure_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_taxonomy_labels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_taxonomy_labels (
    id bigint NOT NULL,
    node_id bigint NOT NULL,
    locale text NOT NULL,
    label text NOT NULL,
    CONSTRAINT creator_taxonomy_labels_label_shape CHECK ((btrim(label) <> ''::text)),
    CONSTRAINT creator_taxonomy_labels_locale_shape CHECK ((locale = ANY (ARRAY['zh-CN'::text, 'en'::text])))
);


--
-- Name: TABLE creator_taxonomy_labels; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_taxonomy_labels IS 'Versioned localized display labels; duplicate labels are valid and are never identifiers.';


--
-- Name: creator_taxonomy_labels_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_taxonomy_labels ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_taxonomy_labels_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_taxonomy_nodes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_taxonomy_nodes (
    id bigint NOT NULL,
    version_id bigint NOT NULL,
    category_id bigint NOT NULL,
    parent_node_id bigint,
    level smallint NOT NULL,
    sort_order integer NOT NULL,
    is_assignable boolean DEFAULT true NOT NULL,
    CONSTRAINT creator_taxonomy_nodes_level_shape CHECK (((level >= 1) AND (level <= 3))),
    CONSTRAINT creator_taxonomy_nodes_parent_shape CHECK ((((level = 1) AND (parent_node_id IS NULL)) OR ((level = ANY (ARRAY[2, 3])) AND (parent_node_id IS NOT NULL)))),
    CONSTRAINT creator_taxonomy_nodes_sort_order_shape CHECK ((sort_order > 0))
);


--
-- Name: TABLE creator_taxonomy_nodes; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.creator_taxonomy_nodes IS 'A category placement in one immutable taxonomy version.';


--
-- Name: creator_taxonomy_nodes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_taxonomy_nodes ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_taxonomy_nodes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_taxonomy_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.creator_taxonomy_versions (
    id bigint NOT NULL,
    version_key text NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    source_zh_sha256 text NOT NULL,
    source_en_sha256 text NOT NULL,
    identity_registry_sha256 text NOT NULL,
    node_count integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    activated_at timestamp with time zone,
    CONSTRAINT creator_taxonomy_versions_activation_shape CHECK ((((status = 'draft'::text) AND (activated_at IS NULL)) OR ((status = ANY (ARRAY['active'::text, 'retired'::text])) AND (activated_at IS NOT NULL)))),
    CONSTRAINT creator_taxonomy_versions_hash_shape CHECK (((source_zh_sha256 ~ '^[0-9a-f]{64}$'::text) AND (source_en_sha256 ~ '^[0-9a-f]{64}$'::text) AND (identity_registry_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT creator_taxonomy_versions_key_shape CHECK ((version_key ~ '^creator-taxonomy-v[1-9][0-9]*$'::text)),
    CONSTRAINT creator_taxonomy_versions_node_count_shape CHECK ((node_count > 0)),
    CONSTRAINT creator_taxonomy_versions_status_shape CHECK ((status = ANY (ARRAY['draft'::text, 'active'::text, 'retired'::text])))
);


--
-- Name: creator_taxonomy_versions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.creator_taxonomy_versions ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.creator_taxonomy_versions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: import_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.import_batches (
    id text NOT NULL,
    source_file text NOT NULL,
    source_sha256 text NOT NULL,
    captured_at timestamp with time zone NOT NULL,
    imported_at timestamp with time zone DEFAULT now() NOT NULL,
    schema_version integer DEFAULT 1 NOT NULL,
    raw_payload jsonb NOT NULL,
    parse_warnings jsonb DEFAULT '[]'::jsonb NOT NULL,
    source_byte_sha256 text,
    source_bytes bigint,
    source_kind text DEFAULT 'channel_json'::text NOT NULL,
    status text DEFAULT 'published'::text NOT NULL,
    row_counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    CONSTRAINT import_batches_contract_shape CHECK (((btrim(source_file) <> ''::text) AND (source_sha256 ~ '^[0-9a-f]{64}$'::text) AND ((source_byte_sha256 IS NULL) OR (source_byte_sha256 ~ '^[0-9a-f]{64}$'::text)) AND ((source_byte_sha256 IS NULL) = (source_bytes IS NULL)) AND ((source_bytes IS NULL) OR (source_bytes > 0)) AND (schema_version > 0) AND (jsonb_typeof(raw_payload) = 'object'::text) AND (jsonb_typeof(parse_warnings) = 'array'::text) AND (jsonb_typeof(row_counts) = 'object'::text) AND (source_kind = ANY (ARRAY['channel_json'::text, 'crawler_postgresql_dump'::text, 'derived_baseline'::text, 'public_browser_snapshot'::text, 'publication_projection'::text])) AND (status = ANY (ARRAY['loading'::text, 'published'::text, 'failed'::text])) AND
CASE
    WHEN (status = 'failed'::text) THEN (NULLIF(btrim(error_message), ''::text) IS NOT NULL)
    ELSE (error_message IS NULL)
END))
);


--
-- Name: TABLE import_batches; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.import_batches IS 'One row per semantic import payload and publication attempt.';


--
-- Name: COLUMN import_batches.source_sha256; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.import_batches.source_sha256 IS 'Semantic payload SHA-256 used for import idempotency; not necessarily the source file byte hash.';


--
-- Name: COLUMN import_batches.source_byte_sha256; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.import_batches.source_byte_sha256 IS 'Optional exact source-file byte SHA-256, paired with source_bytes.';


--
-- Name: COLUMN import_batches.source_kind; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.import_batches.source_kind IS 'Versioned ingest adapter family, not a generic MIME type.';


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT schema_migrations_text_shape CHECK (((btrim(version) <> ''::text) AND (checksum ~ '^[0-9a-f]{64}$'::text)))
);


--
-- Name: TABLE schema_migrations; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.schema_migrations IS 'One row per immutable applied migration checksum.';


--
-- Name: activation; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.activation (
    activation_id uuid NOT NULL,
    publication_stream_id uuid NOT NULL,
    channel_id text NOT NULL,
    ownership_reference jsonb NOT NULL,
    before_version_vector jsonb NOT NULL,
    after_version_vector jsonb NOT NULL,
    revision_count integer NOT NULL,
    projection_mode text NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    activated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT activation_after_version_vector_check CHECK ((jsonb_typeof(after_version_vector) = 'object'::text)),
    CONSTRAINT activation_before_version_vector_check CHECK ((jsonb_typeof(before_version_vector) = 'object'::text)),
    CONSTRAINT activation_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT activation_check CHECK (((btrim(actor) <> ''::text) AND (btrim(reason) <> ''::text))),
    CONSTRAINT activation_ownership_reference_check CHECK ((jsonb_typeof(ownership_reference) = 'object'::text)),
    CONSTRAINT activation_projection_mode_check CHECK ((projection_mode = ANY (ARRAY['held_shadow'::text, 'online'::text]))),
    CONSTRAINT activation_revision_count_check CHECK ((revision_count > 0))
);


--
-- Name: activation_item; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.activation_item (
    activation_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    domain text NOT NULL,
    previous_sequence bigint,
    active_sequence bigint NOT NULL,
    previous_result_hash text,
    active_result_hash text NOT NULL,
    outcome text DEFAULT 'applied'::text NOT NULL,
    activated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT activation_item_active_result_hash_check CHECK ((active_result_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT activation_item_active_sequence_check CHECK ((active_sequence > 0)),
    CONSTRAINT activation_item_domain_check CHECK ((domain = ANY (ARRAY['channel'::text, 'video'::text, 'agent'::text]))),
    CONSTRAINT activation_item_outcome_check CHECK ((outcome = 'applied'::text)),
    CONSTRAINT activation_item_previous_result_hash_check CHECK (((previous_result_hash IS NULL) OR (previous_result_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT activation_item_previous_sequence_check CHECK (((previous_sequence IS NULL) OR (previous_sequence >= 0)))
);


--
-- Name: channel_ownership; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.channel_ownership (
    channel_id text NOT NULL,
    active_publication_stream_id uuid NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    previous_publication_stream_id uuid,
    ownership_reference jsonb DEFAULT '{}'::jsonb NOT NULL,
    state_changed_by text NOT NULL,
    state_reason text NOT NULL,
    state_changed_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    projection_mode text DEFAULT 'held_shadow'::text NOT NULL,
    CONSTRAINT channel_ownership_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT channel_ownership_check CHECK (((btrim(state_changed_by) <> ''::text) AND (btrim(state_reason) <> ''::text))),
    CONSTRAINT channel_ownership_check1 CHECK ((previous_publication_stream_id IS DISTINCT FROM active_publication_stream_id)),
    CONSTRAINT channel_ownership_ownership_reference_check CHECK ((jsonb_typeof(ownership_reference) = 'object'::text)),
    CONSTRAINT channel_ownership_status_check CHECK ((status = ANY (ARRAY['active'::text, 'cutover_pending'::text]))),
    CONSTRAINT chk_business_publication_projection_mode CHECK ((projection_mode = ANY (ARRAY['held_shadow'::text, 'online'::text])))
);


--
-- Name: consumer_cursor; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.consumer_cursor (
    channel_id text NOT NULL,
    domain text NOT NULL,
    publication_stream_id uuid NOT NULL,
    active_sequence bigint NOT NULL,
    active_revision_id uuid NOT NULL,
    active_result_hash text NOT NULL,
    activated_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT consumer_cursor_active_result_hash_check CHECK ((active_result_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT consumer_cursor_active_sequence_check CHECK ((active_sequence >= 0)),
    CONSTRAINT consumer_cursor_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT consumer_cursor_domain_check CHECK ((domain = ANY (ARRAY['channel'::text, 'video'::text, 'agent'::text])))
);


--
-- Name: creator_search_changes; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.creator_search_changes (
    watermark text NOT NULL,
    channel_id text NOT NULL,
    action text NOT NULL,
    before_document jsonb,
    after_document jsonb,
    changed_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT creator_search_changes_action_check CHECK ((action = ANY (ARRAY['upsert'::text, 'remove'::text]))),
    CONSTRAINT creator_search_changes_after_document_check CHECK (((after_document IS NULL) OR (jsonb_typeof(after_document) = 'object'::text))),
    CONSTRAINT creator_search_changes_before_document_check CHECK (((before_document IS NULL) OR (jsonb_typeof(before_document) = 'object'::text))),
    CONSTRAINT creator_search_changes_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT creator_search_changes_check CHECK ((((action = 'upsert'::text) AND (after_document IS NOT NULL)) OR ((action = 'remove'::text) AND (after_document IS NULL) AND (before_document IS NOT NULL))))
);


--
-- Name: TABLE creator_search_changes; Type: COMMENT; Schema: publication; Owner: -
--

COMMENT ON TABLE publication.creator_search_changes IS 'Exact before and after Creator Search state for each changed Channel and release';


--
-- Name: creator_search_legacy_prune_audit; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.creator_search_legacy_prune_audit (
    prune_id bigint NOT NULL,
    active_watermark text NOT NULL,
    retained_watermarks text[] NOT NULL,
    deleted_row_count integer NOT NULL,
    pruned_by text NOT NULL,
    prune_reason text NOT NULL,
    pruned_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT creator_search_legacy_prune_audit_deleted_row_count_check CHECK ((deleted_row_count >= 0)),
    CONSTRAINT creator_search_legacy_prune_audit_prune_reason_check CHECK ((btrim(prune_reason) <> ''::text)),
    CONSTRAINT creator_search_legacy_prune_audit_pruned_by_check CHECK ((btrim(pruned_by) <> ''::text)),
    CONSTRAINT creator_search_legacy_prune_audit_retained_watermarks_check CHECK ((cardinality(retained_watermarks) > 0))
);


--
-- Name: creator_search_legacy_prune_audit_prune_id_seq; Type: SEQUENCE; Schema: publication; Owner: -
--

ALTER TABLE publication.creator_search_legacy_prune_audit ALTER COLUMN prune_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME publication.creator_search_legacy_prune_audit_prune_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: creator_search_storage_state; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.creator_search_storage_state (
    singleton boolean DEFAULT true NOT NULL,
    write_mode text DEFAULT 'shadow'::text NOT NULL,
    read_mode text DEFAULT 'legacy'::text NOT NULL,
    initialized_watermark text NOT NULL,
    initialized_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    cutover_actor text,
    cutover_reason text,
    cutover_at timestamp with time zone,
    storage_rollback_actor text,
    storage_rollback_reason text,
    storage_rollback_at timestamp with time zone,
    initialized_row_count integer NOT NULL,
    CONSTRAINT creator_search_storage_initialized_row_count_check CHECK ((initialized_row_count >= 0)),
    CONSTRAINT creator_search_storage_state_check CHECK (((write_mode = 'shadow'::text) OR (read_mode = 'live'::text))),
    CONSTRAINT creator_search_storage_state_read_mode_check CHECK ((read_mode = ANY (ARRAY['legacy'::text, 'live'::text]))),
    CONSTRAINT creator_search_storage_state_singleton_check CHECK (singleton),
    CONSTRAINT creator_search_storage_state_write_mode_check CHECK ((write_mode = ANY (ARRAY['shadow'::text, 'incremental'::text])))
);


--
-- Name: inbox; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.inbox (
    revision_id uuid NOT NULL,
    publication_stream_id uuid NOT NULL,
    channel_id text NOT NULL,
    domain text NOT NULL,
    data_sequence bigint NOT NULL,
    payload_hash text NOT NULL,
    envelope_hash text NOT NULL,
    received_envelope jsonb NOT NULL,
    receipt_id uuid NOT NULL,
    receive_status text NOT NULL,
    error_code text,
    error_message text,
    first_received_at timestamp with time zone DEFAULT now() NOT NULL,
    last_received_at timestamp with time zone DEFAULT now() NOT NULL,
    receive_count integer DEFAULT 1 NOT NULL,
    CONSTRAINT inbox_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT inbox_check CHECK ((last_received_at >= first_received_at)),
    CONSTRAINT inbox_check1 CHECK ((((receive_status = ANY (ARRAY['rejected'::text, 'conflict'::text])) AND (error_code IS NOT NULL)) OR (receive_status <> ALL (ARRAY['rejected'::text, 'conflict'::text])))),
    CONSTRAINT inbox_data_sequence_check CHECK ((data_sequence > 0)),
    CONSTRAINT inbox_domain_check CHECK ((domain = ANY (ARRAY['channel'::text, 'video'::text, 'agent'::text]))),
    CONSTRAINT inbox_envelope_hash_check CHECK ((envelope_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT inbox_payload_hash_check CHECK ((payload_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT inbox_receive_count_check CHECK ((receive_count > 0)),
    CONSTRAINT inbox_receive_status_check CHECK ((receive_status = ANY (ARRAY['accepted'::text, 'waiting_gap'::text, 'waiting_ownership'::text, 'rejected'::text, 'conflict'::text]))),
    CONSTRAINT inbox_received_envelope_check CHECK ((jsonb_typeof(received_envelope) = 'object'::text))
);


--
-- Name: inbox_conflict; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.inbox_conflict (
    conflict_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    conflicting_payload_hash text NOT NULL,
    conflicting_envelope_hash text NOT NULL,
    conflicting_envelope jsonb NOT NULL,
    first_received_at timestamp with time zone DEFAULT now() NOT NULL,
    last_received_at timestamp with time zone DEFAULT now() NOT NULL,
    receive_count integer DEFAULT 1 NOT NULL,
    CONSTRAINT inbox_conflict_check CHECK ((last_received_at >= first_received_at)),
    CONSTRAINT inbox_conflict_conflicting_envelope_check CHECK ((jsonb_typeof(conflicting_envelope) = 'object'::text)),
    CONSTRAINT inbox_conflict_conflicting_envelope_hash_check CHECK ((conflicting_envelope_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT inbox_conflict_conflicting_payload_hash_check CHECK ((conflicting_payload_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT inbox_conflict_receive_count_check CHECK ((receive_count > 0))
);


--
-- Name: projection_batch; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.projection_batch (
    batch_id text NOT NULL,
    source_sha256 text NOT NULL,
    publication_stream_id uuid NOT NULL,
    adapter_version text NOT NULL,
    status text NOT NULL,
    version_vectors jsonb NOT NULL,
    upsert_channel_ids text[] DEFAULT '{}'::text[] NOT NULL,
    removed_channel_ids text[] DEFAULT '{}'::text[] NOT NULL,
    projection_count integer NOT NULL,
    previous_watermark text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    published_at timestamp with time zone,
    CONSTRAINT projection_batch_check CHECK (((btrim(batch_id) <> ''::text) AND (btrim(adapter_version) <> ''::text))),
    CONSTRAINT projection_batch_check1 CHECK ((projection_count = (cardinality(upsert_channel_ids) + cardinality(removed_channel_ids)))),
    CONSTRAINT projection_batch_check2 CHECK ((NOT (upsert_channel_ids && removed_channel_ids))),
    CONSTRAINT projection_batch_check3 CHECK ((((status = 'published'::text) AND (published_at IS NOT NULL)) OR ((status = 'loading'::text) AND (published_at IS NULL)))),
    CONSTRAINT projection_batch_projection_count_check CHECK ((projection_count > 0)),
    CONSTRAINT projection_batch_source_sha256_check CHECK ((source_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT projection_batch_status_check CHECK ((status = ANY (ARRAY['loading'::text, 'published'::text]))),
    CONSTRAINT projection_batch_version_vectors_check CHECK ((jsonb_typeof(version_vectors) = 'object'::text))
);


--
-- Name: projection_batch_item; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.projection_batch_item (
    batch_id text NOT NULL,
    channel_id text NOT NULL,
    action text NOT NULL,
    snapshot_id text,
    previous_snapshot_id text,
    version_vector jsonb NOT NULL,
    projection_hash text NOT NULL,
    projected_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT projection_batch_item_action_check CHECK ((action = ANY (ARRAY['upsert'::text, 'remove'::text]))),
    CONSTRAINT projection_batch_item_check CHECK (((btrim(channel_id) <> ''::text) AND (jsonb_typeof(version_vector) = 'object'::text))),
    CONSTRAINT projection_batch_item_check1 CHECK ((((action = 'upsert'::text) AND (snapshot_id IS NOT NULL) AND (btrim(snapshot_id) <> ''::text)) OR ((action = 'remove'::text) AND (snapshot_id IS NULL)))),
    CONSTRAINT projection_batch_item_projection_hash_check CHECK ((projection_hash ~ '^sha256:[0-9a-f]{64}$'::text))
);


--
-- Name: projection_cutover; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.projection_cutover (
    cutover_id text NOT NULL,
    publication_stream_id uuid NOT NULL,
    evidence_format text NOT NULL,
    evidence_sha256 text NOT NULL,
    cohort_hash text NOT NULL,
    channel_ids text[] NOT NULL,
    owner_count integer NOT NULL,
    released_projection_count integer NOT NULL,
    previous_watermark text NOT NULL,
    first_projection_watermark text,
    last_projection_watermark text,
    status text NOT NULL,
    evidence_json jsonb NOT NULL,
    applied_by text NOT NULL,
    applied_reason text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL,
    rolled_back_by text,
    rolled_back_reason text,
    rolled_back_at timestamp with time zone,
    rollback_json jsonb,
    CONSTRAINT projection_cutover_check CHECK (((btrim(cutover_id) <> ''::text) AND (btrim(evidence_format) <> ''::text) AND (jsonb_typeof(evidence_json) = 'object'::text) AND (btrim(applied_by) <> ''::text) AND (btrim(applied_reason) <> ''::text) AND (owner_count = cardinality(channel_ids)))),
    CONSTRAINT projection_cutover_check1 CHECK ((((status = 'applied'::text) AND (rolled_back_by IS NULL) AND (rolled_back_reason IS NULL) AND (rolled_back_at IS NULL) AND (rollback_json IS NULL)) OR ((status = 'rolled_back'::text) AND (btrim(rolled_back_by) <> ''::text) AND (btrim(rolled_back_reason) <> ''::text) AND (rolled_back_at IS NOT NULL) AND (jsonb_typeof(rollback_json) = 'object'::text)))),
    CONSTRAINT projection_cutover_cohort_hash_check CHECK ((cohort_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT projection_cutover_evidence_sha256_check CHECK ((evidence_sha256 ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT projection_cutover_owner_count_check CHECK ((owner_count > 0)),
    CONSTRAINT projection_cutover_released_projection_count_check CHECK ((released_projection_count >= 0)),
    CONSTRAINT projection_cutover_status_check CHECK ((status = ANY (ARRAY['applied'::text, 'rolled_back'::text])))
);


--
-- Name: projection_outbox; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.projection_outbox (
    projection_id uuid NOT NULL,
    activation_id uuid NOT NULL,
    publication_stream_id uuid NOT NULL,
    channel_id text NOT NULL,
    version_vector jsonb NOT NULL,
    status text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    last_error text,
    delivered_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    released_by_cutover_id text,
    CONSTRAINT projection_outbox_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT projection_outbox_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT projection_outbox_check CHECK ((((status = 'leased'::text) AND (btrim(lease_owner) <> ''::text) AND (lease_expires_at IS NOT NULL)) OR ((status <> 'leased'::text) AND (lease_owner IS NULL) AND (lease_expires_at IS NULL)))),
    CONSTRAINT projection_outbox_check1 CHECK ((((status = 'delivered'::text) AND (delivered_at IS NOT NULL)) OR ((status <> 'delivered'::text) AND (delivered_at IS NULL)))),
    CONSTRAINT projection_outbox_status_check CHECK ((status = ANY (ARRAY['held_shadow'::text, 'pending'::text, 'leased'::text, 'retry_wait'::text, 'delivered'::text, 'dead_letter'::text]))),
    CONSTRAINT projection_outbox_version_vector_check CHECK ((jsonb_typeof(version_vector) = 'object'::text))
)
WITH (autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='100');


--
-- Name: projection_snapshot_time_repair; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.projection_snapshot_time_repair (
    snapshot_id text NOT NULL,
    channel_id text NOT NULL,
    active_revision_id uuid,
    snapshot_captured_at timestamp with time zone NOT NULL,
    source_observed_at timestamp with time zone NOT NULL,
    repair_version text NOT NULL,
    repaired_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT projection_snapshot_time_repair_check CHECK (((btrim(channel_id) <> ''::text) AND (btrim(repair_version) <> ''::text) AND (source_observed_at <= snapshot_captured_at)))
);


--
-- Name: quarantine; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.quarantine (
    quarantine_id uuid NOT NULL,
    revision_id uuid NOT NULL,
    issue_code text NOT NULL,
    issue_hash text NOT NULL,
    details_json jsonb NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone,
    resolved_by text,
    resolution_reason text,
    CONSTRAINT quarantine_check CHECK ((((status = 'open'::text) AND (resolved_at IS NULL) AND (resolved_by IS NULL) AND (resolution_reason IS NULL)) OR ((status = ANY (ARRAY['resolved'::text, 'ignored'::text])) AND (resolved_at IS NOT NULL) AND (btrim(resolved_by) <> ''::text) AND (btrim(resolution_reason) <> ''::text)))),
    CONSTRAINT quarantine_details_json_check CHECK ((jsonb_typeof(details_json) = 'object'::text)),
    CONSTRAINT quarantine_issue_code_check CHECK ((btrim(issue_code) <> ''::text)),
    CONSTRAINT quarantine_issue_hash_check CHECK ((issue_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT quarantine_status_check CHECK ((status = ANY (ARRAY['open'::text, 'resolved'::text, 'ignored'::text])))
);


--
-- Name: reconciliation_state; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.reconciliation_state (
    channel_id text NOT NULL,
    attempt_count bigint DEFAULT 0 NOT NULL,
    consecutive_error_count integer DEFAULT 0 NOT NULL,
    last_outcome text,
    last_error text,
    last_attempted_at timestamp with time zone,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_owner text,
    lease_expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reconciliation_state_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT reconciliation_state_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT reconciliation_state_check CHECK ((((lease_owner IS NULL) AND (lease_expires_at IS NULL)) OR ((lease_owner IS NOT NULL) AND (btrim(lease_owner) <> ''::text) AND (lease_expires_at IS NOT NULL)))),
    CONSTRAINT reconciliation_state_check1 CHECK ((((last_outcome = 'error'::text) AND (last_error IS NOT NULL) AND (btrim(last_error) <> ''::text)) OR ((last_outcome IS DISTINCT FROM 'error'::text) AND (last_error IS NULL)))),
    CONSTRAINT reconciliation_state_consecutive_error_count_check CHECK ((consecutive_error_count >= 0)),
    CONSTRAINT reconciliation_state_last_outcome_check CHECK (((last_outcome IS NULL) OR (btrim(last_outcome) <> ''::text)))
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
    source_json jsonb NOT NULL,
    previous_result_hash text,
    result_hash text NOT NULL,
    payload_hash text NOT NULL,
    payload_json jsonb NOT NULL,
    envelope_hash text NOT NULL,
    validation_status text DEFAULT 'valid'::text NOT NULL,
    ingress_status text NOT NULL,
    activation_status text DEFAULT 'staged'::text NOT NULL,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT revision_activation_status_check CHECK ((activation_status = ANY (ARRAY['staged'::text, 'waiting_gap'::text, 'waiting_ownership'::text, 'active'::text, 'quarantined'::text, 'superseded'::text]))),
    CONSTRAINT revision_check CHECK (((btrim(channel_id) <> ''::text) AND (btrim(policy_version) <> ''::text))),
    CONSTRAINT revision_check1 CHECK ((((revision_type = 'bootstrap'::text) AND (data_sequence = 1) AND (previous_data_sequence IS NULL) AND (previous_result_hash IS NULL)) OR ((revision_type <> 'bootstrap'::text) AND (previous_data_sequence = (data_sequence - 1)) AND (previous_result_hash IS NOT NULL)))),
    CONSTRAINT revision_check2 CHECK ((((domain = 'channel'::text) AND (((revision_type = 'retraction'::text) AND (operation = 'retract_channel'::text)) OR ((revision_type <> 'retraction'::text) AND (operation = 'replace'::text)))) OR ((domain = 'video'::text) AND (((revision_type = 'bootstrap'::text) AND (operation = 'replace_window'::text)) OR ((revision_type <> 'bootstrap'::text) AND (operation = 'apply_window_delta'::text)))) OR ((domain = 'agent'::text) AND (((revision_type = 'retraction'::text) AND (operation = 'retract_agent'::text)) OR ((revision_type <> 'retraction'::text) AND (operation = 'replace'::text)))))),
    CONSTRAINT revision_contract_version_check CHECK ((contract_version > 0)),
    CONSTRAINT revision_data_sequence_check CHECK ((data_sequence > 0)),
    CONSTRAINT revision_domain_check CHECK ((domain = ANY (ARRAY['channel'::text, 'video'::text, 'agent'::text]))),
    CONSTRAINT revision_envelope_hash_check CHECK ((envelope_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT revision_ingress_status_check CHECK ((ingress_status = ANY (ARRAY['accepted'::text, 'waiting_gap'::text, 'waiting_ownership'::text]))),
    CONSTRAINT revision_operation_check CHECK ((operation = ANY (ARRAY['replace'::text, 'replace_window'::text, 'apply_window_delta'::text, 'retract_channel'::text, 'retract_agent'::text]))),
    CONSTRAINT revision_payload_hash_check CHECK ((payload_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT revision_payload_json_check CHECK ((jsonb_typeof(payload_json) = 'object'::text)),
    CONSTRAINT revision_previous_result_hash_check CHECK (((previous_result_hash IS NULL) OR (previous_result_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT revision_result_hash_check CHECK ((result_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT revision_revision_type_check CHECK ((revision_type = ANY (ARRAY['bootstrap'::text, 'incremental'::text, 'repair'::text, 'retraction'::text]))),
    CONSTRAINT revision_source_json_check CHECK ((jsonb_typeof(source_json) = 'object'::text)),
    CONSTRAINT revision_validation_status_check CHECK ((validation_status = ANY (ARRAY['valid'::text, 'quarantined'::text])))
);


--
-- Name: stream; Type: TABLE; Schema: publication; Owner: -
--

CREATE TABLE publication.stream (
    publication_stream_id uuid NOT NULL,
    source_deployment_key text NOT NULL,
    source_identity_json jsonb NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    accepted_contract_versions integer[] DEFAULT ARRAY[1, 2] NOT NULL,
    automatic_onboarding_projection_mode text,
    registered_by text NOT NULL,
    registered_reason text NOT NULL,
    registered_at timestamp with time zone DEFAULT now() NOT NULL,
    status_changed_by text NOT NULL,
    status_reason text NOT NULL,
    status_changed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT stream_accepted_contract_versions_check CHECK ((cardinality(accepted_contract_versions) > 0)),
    CONSTRAINT stream_accepted_contract_versions_check1 CHECK ((array_position(accepted_contract_versions, NULL::integer) IS NULL)),
    CONSTRAINT stream_accepted_contract_versions_check2 CHECK ((0 < ALL (accepted_contract_versions))),
    CONSTRAINT stream_check CHECK (((btrim(registered_by) <> ''::text) AND (btrim(registered_reason) <> ''::text))),
    CONSTRAINT stream_check1 CHECK (((btrim(status_changed_by) <> ''::text) AND (btrim(status_reason) <> ''::text))),
    CONSTRAINT chk_business_publication_automatic_onboarding_projection_mode CHECK ((automatic_onboarding_projection_mode = ANY (ARRAY['held_shadow'::text, 'online'::text]))),
    CONSTRAINT stream_source_deployment_key_check CHECK ((btrim(source_deployment_key) <> ''::text)),
    CONSTRAINT stream_source_identity_json_check CHECK ((jsonb_typeof(source_identity_json) = 'object'::text)),
    CONSTRAINT stream_status_check CHECK ((status = ANY (ARRAY['active'::text, 'sealed'::text, 'revoked'::text])))
);


--
-- Name: agent_profiles; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.agent_profiles (
    import_batch_id text NOT NULL,
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
    CONSTRAINT agent_profiles_status_check CHECK ((status = ANY (ARRAY['success'::text, 'failed'::text]))),
    CONSTRAINT raw_agent_profiles_v2_shape CHECK (((btrim(prompt_variant) <> ''::text) AND (attempts >= 0)))
);


--
-- Name: TABLE agent_profiles; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.agent_profiles IS 'Lossless typed crawler agent-profile rows under the current one-profile-per-channel source contract.';


--
-- Name: channel_runs; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.channel_runs (
    import_batch_id text NOT NULL,
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
    CONSTRAINT channel_runs_crawl_mode_check CHECK ((crawl_mode = ANY (ARRAY['full'::text, 'incremental'::text]))),
    CONSTRAINT channel_runs_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'waiting_pages'::text, 'waiting_detail'::text, 'waiting_agent'::text, 'finalizing'::text, 'done'::text, 'failed'::text, 'skipped'::text]))),
    CONSTRAINT raw_channel_runs_candidate_id_shape CHECK (((candidate_id IS NULL) OR (candidate_id > 0))),
    CONSTRAINT raw_channel_runs_v2_shape CHECK (((content_limit > 0) AND (expected_content_count >= 0) AND (detail_status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'api_pending'::text, 'done'::text, 'failed'::text]))))
);


--
-- Name: TABLE channel_runs; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.channel_runs IS 'Lossless typed crawler run rows, scoped by import_batch_id.';


--
-- Name: COLUMN channel_runs.candidate_id; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.channel_runs.candidate_id IS 'Source acquisition candidate identifier. The pipeline export omits its parent row, so this is intentionally not a foreign key.';


--
-- Name: channel_tab_pages; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.channel_tab_pages (
    import_batch_id text NOT NULL,
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
-- Name: TABLE channel_tab_pages; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.channel_tab_pages IS 'Lossless typed crawler tab-page rows, scoped by import_batch_id.';


--
-- Name: channels; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.channels (
    import_batch_id text NOT NULL,
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
    CONSTRAINT channels_agent_status_check CHECK ((agent_status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'skipped'::text]))),
    CONSTRAINT channels_status_check CHECK ((status = ANY (ARRAY['active'::text, 'paused'::text, 'archived'::text, 'rejected'::text]))),
    CONSTRAINT raw_channels_pipeline_v3_shape CHECK ((((country_code IS NULL) OR (country_code ~ '^[A-Z]{2}$'::text)) AND ((country_canonical_name IS NULL) OR (btrim(country_canonical_name) <> ''::text)))),
    CONSTRAINT raw_channels_v2_shape CHECK (((agent_attempts >= 0) AND ((country IS NULL) OR (btrim(country) <> ''::text)) AND ((country_source IS NULL) OR (btrim(country_source) <> ''::text))))
);


--
-- Name: TABLE channels; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.channels IS 'Lossless typed crawler channel rows, scoped by import_batch_id.';


--
-- Name: COLUMN channels.country_code; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.channels.country_code IS 'Source-normalized ISO 3166-1 alpha-2 country code; retained separately from raw country text.';


--
-- Name: COLUMN channels.country_canonical_name; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.channels.country_canonical_name IS 'Source canonical country name corresponding to country_code when observed.';


--
-- Name: content_candidates; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.content_candidates (
    import_batch_id text NOT NULL,
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
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT content_candidates_api_status_check CHECK ((api_status = ANY (ARRAY['not_needed'::text, 'pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'unavailable'::text]))),
    CONSTRAINT content_candidates_check CHECK ((("position" > 0) AND (attempts >= 0))),
    CONSTRAINT content_candidates_content_type_check CHECK (((content_type IS NULL) OR (content_type = ANY (ARRAY['video'::text, 'short'::text, 'live'::text])))),
    CONSTRAINT content_candidates_detail_status_check CHECK ((detail_status = ANY (ARRAY['queued'::text, 'running'::text, 'api_pending'::text, 'done'::text, 'unavailable'::text, 'failed'::text]))),
    CONSTRAINT content_candidates_type_status_check CHECK ((type_status = ANY (ARRAY['unresolved'::text, 'resolved'::text, 'unavailable'::text])))
);


--
-- Name: TABLE content_candidates; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.content_candidates IS 'Crawler-v2 candidate discovery rows before canonical content materialization.';


--
-- Name: content_enrich_tasks; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.content_enrich_tasks (
    import_batch_id text NOT NULL,
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
    CONSTRAINT content_enrich_tasks_job_type_check CHECK ((job_type = ANY (ARRAY['date-resolve'::text, 'duration-resolve'::text, 'view-resolve'::text, 'stats-resolve'::text]))),
    CONSTRAINT content_enrich_tasks_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'skipped'::text])))
);


--
-- Name: TABLE content_enrich_tasks; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.content_enrich_tasks IS 'Lossless typed crawler enrichment-task evidence, scoped by import_batch_id.';


--
-- Name: contents; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.contents (
    import_batch_id text NOT NULL,
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
    published_at_precision text,
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
    CONSTRAINT contents_content_type_check CHECK ((content_type = ANY (ARRAY['video'::text, 'short'::text, 'live'::text]))),
    CONSTRAINT contents_published_at_status_check CHECK ((published_at_status = ANY (ARRAY['exact'::text, 'relative'::text, 'estimated'::text, 'unavailable'::text, 'unresolved'::text]))),
    CONSTRAINT raw_contents_v4_shape CHECK (((view_count_status = ANY (ARRAY['exact'::text, 'estimated'::text, 'recovered'::text, 'unavailable'::text, 'unresolved'::text])) AND (like_count_status = ANY (ARRAY['exact'::text, 'zero_from_empty'::text, 'unavailable'::text, 'unresolved'::text])) AND (comment_count_status = ANY (ARRAY['exact'::text, 'zero_from_empty'::text, 'zero_from_surface'::text, 'zero_from_upcoming'::text, 'disabled'::text, 'unavailable'::text, 'unresolved'::text])) AND (duration_status = ANY (ARRAY['exact'::text, 'unavailable'::text, 'unresolved'::text])) AND (access_status = ANY (ARRAY['public'::text, 'unlisted'::text, 'login_required'::text, 'members_only'::text, 'unavailable'::text, 'unknown'::text])) AND (description_status = ANY (ARRAY['exact'::text, 'empty'::text, 'unavailable'::text, 'unresolved'::text])) AND (((description_status = 'exact'::text) AND (description IS NOT NULL) AND (description <> ''::text)) OR ((description_status = 'empty'::text) AND (description = ''::text)) OR ((description_status = ANY (ARRAY['unavailable'::text, 'unresolved'::text])) AND (description IS NULL))) AND ((access_status_source IS NULL) OR (btrim(access_status_source) <> ''::text)) AND ((content_type_source IS NULL) OR (btrim(content_type_source) <> ''::text)) AND ((description_source IS NULL) OR (btrim(description_source) <> ''::text)) AND (("position" IS NULL) OR ("position" > 0)) AND ((duration_seconds IS NULL) OR (duration_seconds >= 0)) AND ((published_at_precision IS NULL) OR (published_at_precision = ANY (ARRAY['second'::text, 'date_only'::text, 'unknown'::text]))) AND ((comments_disabled IS TRUE) = (comment_count_status = 'disabled'::text)) AND ((comments_disabled IS DISTINCT FROM TRUE) OR (comment_count = 0))))
);


--
-- Name: TABLE contents; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.contents IS 'Lossless typed crawler content appearances, scoped by import_batch_id.';


--
-- Name: COLUMN contents.published_at_precision; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.contents.published_at_precision IS 'Source timestamp precision. date_only is an exact civil date, not an exact instant.';


--
-- Name: COLUMN contents.comment_count_source; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.contents.comment_count_source IS 'Crawler lineage for the observed comment count, including zero-from-surface evidence.';


--
-- Name: COLUMN contents.description; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.contents.description IS 'Raw crawler content description; stored even when not exposed by the public web DTO.';


--
-- Name: COLUMN contents.hashtags; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.contents.hashtags IS 'Crawler-extracted hashtags, distinct from title-only UI fallback extraction.';


--
-- Name: COLUMN contents.keywords; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON COLUMN raw_crawler.contents.keywords IS 'Crawler-extracted content keywords retained for backend analysis.';


--
-- Name: finalized_profiles; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.finalized_profiles (
    import_batch_id text NOT NULL,
    channel_id text NOT NULL,
    run_id text,
    status text DEFAULT 'pending'::text NOT NULL,
    profile_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    quality_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    finalized_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT finalized_profiles_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'ready_auto'::text, 'ready_partial'::text, 'pending_detail'::text, 'pending_api'::text, 'pending_enrich'::text, 'pending_agent'::text, 'failed'::text])))
);


--
-- Name: TABLE finalized_profiles; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.finalized_profiles IS 'Lossless typed crawler finalized-profile rows, scoped by import_batch_id.';


--
-- Name: query_pages; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.query_pages (
    import_batch_id text NOT NULL,
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
    CONSTRAINT query_pages_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'skipped'::text])))
);


--
-- Name: TABLE query_pages; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.query_pages IS 'Lossless typed crawler query-page rows, scoped by import_batch_id.';


--
-- Name: query_sets; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.query_sets (
    import_batch_id text NOT NULL,
    query_set_id bigint NOT NULL,
    name text NOT NULL,
    description text,
    status text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT query_sets_status_check CHECK ((status = ANY (ARRAY['active'::text, 'paused'::text, 'archived'::text])))
);


--
-- Name: TABLE query_sets; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.query_sets IS 'Lossless typed crawler query-set rows, scoped by import_batch_id.';


--
-- Name: query_terms; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.query_terms (
    import_batch_id text NOT NULL,
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
    CONSTRAINT query_terms_status_check CHECK ((status = ANY (ARRAY['active'::text, 'paused'::text, 'exhausted'::text, 'archived'::text]))),
    CONSTRAINT raw_query_terms_quality_shape CHECK (((btrim(quality_status) <> ''::text) AND (jsonb_typeof(quality_json) = 'object'::text) AND ((quality_score IS NULL) OR ((quality_score >= (0)::numeric) AND (quality_score <= (100)::numeric)))))
);


--
-- Name: TABLE query_terms; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.query_terms IS 'Lossless typed crawler query-term rows, scoped by import_batch_id.';


--
-- Name: raw_objects; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.raw_objects (
    import_batch_id text NOT NULL,
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
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE raw_objects; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.raw_objects IS 'Crawler object metadata and locators only; no object bodies are stored.';


--
-- Name: youtube_api_batches; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.youtube_api_batches (
    import_batch_id text NOT NULL,
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
    CONSTRAINT youtube_api_batches_key_index_check CHECK (((key_index IS NULL) OR (key_index >= 0))),
    CONSTRAINT youtube_api_batches_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'done'::text, 'failed'::text])))
);


--
-- Name: TABLE youtube_api_batches; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.youtube_api_batches IS 'Crawler-v2 grouped YouTube Data API requests.';


--
-- Name: youtube_api_daily_usage; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.youtube_api_daily_usage (
    import_batch_id text NOT NULL,
    usage_date date NOT NULL,
    request_count integer DEFAULT 0 NOT NULL,
    requested_video_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT youtube_api_daily_usage_request_count_check CHECK ((request_count >= 0)),
    CONSTRAINT youtube_api_daily_usage_requested_video_count_check CHECK ((requested_video_count >= 0))
);


--
-- Name: TABLE youtube_api_daily_usage; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.youtube_api_daily_usage IS 'Crawler-v2 API request-volume snapshot included in one import batch.';


--
-- Name: youtube_api_tasks; Type: TABLE; Schema: raw_crawler; Owner: -
--

CREATE TABLE raw_crawler.youtube_api_tasks (
    import_batch_id text NOT NULL,
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
    CONSTRAINT youtube_api_tasks_attempts_check CHECK ((attempts >= 0)),
    CONSTRAINT youtube_api_tasks_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'queued'::text, 'running'::text, 'done'::text, 'failed'::text, 'unavailable'::text])))
);


--
-- Name: TABLE youtube_api_tasks; Type: COMMENT; Schema: raw_crawler; Owner: -
--

COMMENT ON TABLE raw_crawler.youtube_api_tasks IS 'Crawler-v2 YouTube Data API enrichment tasks and their missing-field requests.';


--
-- Name: agent_current; Type: TABLE; Schema: result; Owner: -
--

CREATE TABLE result.agent_current (
    channel_id text NOT NULL,
    publication_stream_id uuid NOT NULL,
    active_sequence bigint NOT NULL,
    active_revision_id uuid NOT NULL,
    result_hash text NOT NULL,
    payload_json jsonb NOT NULL,
    is_retracted boolean DEFAULT false NOT NULL,
    activated_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT agent_current_active_sequence_check CHECK ((active_sequence >= 0)),
    CONSTRAINT agent_current_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT agent_current_payload_json_check CHECK ((jsonb_typeof(payload_json) = 'object'::text)),
    CONSTRAINT agent_current_result_hash_check CHECK ((result_hash ~ '^sha256:[0-9a-f]{64}$'::text))
);


--
-- Name: content_current; Type: TABLE; Schema: result; Owner: -
--

CREATE TABLE result.content_current (
    channel_id text NOT NULL,
    content_id text NOT NULL,
    publication_stream_id uuid NOT NULL,
    active_sequence bigint NOT NULL,
    active_revision_id uuid NOT NULL,
    item_hash text NOT NULL,
    payload_json jsonb NOT NULL,
    "position" integer,
    window_status text NOT NULL,
    state_reason text,
    activated_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT content_current_active_sequence_check CHECK ((active_sequence >= 0)),
    CONSTRAINT content_current_check CHECK (((btrim(channel_id) <> ''::text) AND (btrim(content_id) <> ''::text))),
    CONSTRAINT content_current_check1 CHECK ((((window_status = 'active'::text) AND ("position" > 0) AND (state_reason IS NULL)) OR ((window_status <> 'active'::text) AND ("position" IS NULL) AND (btrim(state_reason) <> ''::text)))),
    CONSTRAINT content_current_item_hash_check CHECK ((item_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT content_current_payload_json_check CHECK ((jsonb_typeof(payload_json) = 'object'::text)),
    CONSTRAINT content_current_window_status_check CHECK ((window_status = ANY (ARRAY['active'::text, 'window_exit'::text, 'retracted'::text])))
);


--
-- Name: entity_current; Type: TABLE; Schema: result; Owner: -
--

CREATE TABLE result.entity_current (
    channel_id text NOT NULL,
    publication_stream_id uuid NOT NULL,
    active_sequence bigint NOT NULL,
    active_revision_id uuid NOT NULL,
    result_hash text NOT NULL,
    payload_json jsonb NOT NULL,
    lifecycle_status text,
    is_retracted boolean DEFAULT false NOT NULL,
    activated_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT entity_current_active_sequence_check CHECK ((active_sequence >= 0)),
    CONSTRAINT entity_current_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT entity_current_payload_json_check CHECK ((jsonb_typeof(payload_json) = 'object'::text)),
    CONSTRAINT entity_current_result_hash_check CHECK ((result_hash ~ '^sha256:[0-9a-f]{64}$'::text))
);


--
-- Name: video_current; Type: TABLE; Schema: result; Owner: -
--

CREATE TABLE result.video_current (
    channel_id text NOT NULL,
    publication_stream_id uuid NOT NULL,
    active_sequence bigint NOT NULL,
    active_revision_id uuid NOT NULL,
    result_hash text NOT NULL,
    window_policy jsonb NOT NULL,
    window_proof jsonb NOT NULL,
    activated_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT video_current_active_sequence_check CHECK ((active_sequence >= 0)),
    CONSTRAINT video_current_channel_id_check CHECK ((btrim(channel_id) <> ''::text)),
    CONSTRAINT video_current_result_hash_check CHECK ((result_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT video_current_window_policy_check CHECK ((jsonb_typeof(window_policy) = 'object'::text)),
    CONSTRAINT video_current_window_proof_check CHECK ((jsonb_typeof(window_proof) = 'object'::text))
);


--
-- Name: category_taxonomy category_taxonomy_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.category_taxonomy
    ADD CONSTRAINT category_taxonomy_pkey PRIMARY KEY (raw_value);


--
-- Name: channel_links channel_links_channel_snapshot_id_link_type_url_source_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_links
    ADD CONSTRAINT channel_links_channel_snapshot_id_link_type_url_source_key UNIQUE (channel_snapshot_id, link_type, url, source);


--
-- Name: channel_links channel_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_links
    ADD CONSTRAINT channel_links_pkey PRIMARY KEY (id);


--
-- Name: channel_metric_values channel_metric_values_channel_snapshot_id_metric_key_scope_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_metric_values
    ADD CONSTRAINT channel_metric_values_channel_snapshot_id_metric_key_scope_key UNIQUE (channel_snapshot_id, metric_key, scope);


--
-- Name: channel_metric_values channel_metric_values_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_metric_values
    ADD CONSTRAINT channel_metric_values_pkey PRIMARY KEY (id);


--
-- Name: channel_profile_facts channel_profile_facts_channel_snapshot_id_field_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_profile_facts
    ADD CONSTRAINT channel_profile_facts_channel_snapshot_id_field_key_key UNIQUE (channel_snapshot_id, field_key);


--
-- Name: channel_profile_facts channel_profile_facts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_profile_facts
    ADD CONSTRAINT channel_profile_facts_pkey PRIMARY KEY (id);


--
-- Name: channel_snapshots channel_snapshots_channel_id_import_batch_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_snapshots
    ADD CONSTRAINT channel_snapshots_channel_id_import_batch_id_key UNIQUE (channel_id, import_batch_id);


--
-- Name: channel_snapshots channel_snapshots_id_channel_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_snapshots
    ADD CONSTRAINT channel_snapshots_id_channel_id_key UNIQUE (id, channel_id);


--
-- Name: channel_snapshots channel_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_snapshots
    ADD CONSTRAINT channel_snapshots_pkey PRIMARY KEY (id);


--
-- Name: channels channels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (channel_id);


--
-- Name: content_items content_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_items
    ADD CONSTRAINT content_items_pkey PRIMARY KEY (video_id);


--
-- Name: content_items content_items_video_id_channel_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_items
    ADD CONSTRAINT content_items_video_id_channel_id_key UNIQUE (video_id, channel_id);


--
-- Name: content_snapshots content_snapshots_id_snapshot_channel_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_snapshots
    ADD CONSTRAINT content_snapshots_id_snapshot_channel_key UNIQUE (id, channel_snapshot_id, channel_id);


--
-- Name: content_snapshots content_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_snapshots
    ADD CONSTRAINT content_snapshots_pkey PRIMARY KEY (id);


--
-- Name: content_type_taxonomy content_type_taxonomy_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_type_taxonomy
    ADD CONSTRAINT content_type_taxonomy_pkey PRIMARY KEY (source_content_type);


--
-- Name: content_type_taxonomy content_type_taxonomy_source_kind_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_type_taxonomy
    ADD CONSTRAINT content_type_taxonomy_source_kind_key UNIQUE (source_content_type, content_kind);


INSERT INTO public.content_type_taxonomy (
    source_content_type, content_kind, canonical_priority
) VALUES
    ('live', 'lives', 1),
    ('short', 'shorts', 2),
    ('video', 'videos', 3)
ON CONFLICT (source_content_type,content_kind) DO UPDATE
SET canonical_priority = EXCLUDED.canonical_priority;


--
-- Name: crawler_ingest_batches crawler_ingest_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawler_ingest_batches
    ADD CONSTRAINT crawler_ingest_batches_pkey PRIMARY KEY (import_batch_id);


--
-- Name: crawler_ingest_batches crawler_ingest_batches_source_sha256_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawler_ingest_batches
    ADD CONSTRAINT crawler_ingest_batches_source_sha256_key UNIQUE (source_sha256);


--
-- Name: creator_categories creator_categories_category_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_categories
    ADD CONSTRAINT creator_categories_category_key_key UNIQUE (category_key);


--
-- Name: creator_categories creator_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_categories
    ADD CONSTRAINT creator_categories_pkey PRIMARY KEY (id);


--
-- Name: creator_category_assignments creator_category_assignments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_assignments
    ADD CONSTRAINT creator_category_assignments_pkey PRIMARY KEY (id);


--
-- Name: creator_category_claims creator_category_claims_identity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_claims
    ADD CONSTRAINT creator_category_claims_identity_key UNIQUE (id, channel_id, taxonomy_version_id, category_id);


--
-- Name: creator_category_claims creator_category_claims_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_claims
    ADD CONSTRAINT creator_category_claims_pkey PRIMARY KEY (id);


--
-- Name: creator_classification_runs creator_classification_runs_identity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_classification_runs
    ADD CONSTRAINT creator_classification_runs_identity_key UNIQUE (id, channel_id, channel_snapshot_id, taxonomy_version_id);


--
-- Name: creator_classification_runs creator_classification_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_classification_runs
    ADD CONSTRAINT creator_classification_runs_pkey PRIMARY KEY (id);


--
-- Name: creator_classification_runs creator_classification_runs_tag_identity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_classification_runs
    ADD CONSTRAINT creator_classification_runs_tag_identity_key UNIQUE (id, channel_id, channel_snapshot_id);


--
-- Name: creator_content_tag_claims creator_content_tag_claims_identity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_claims
    ADD CONSTRAINT creator_content_tag_claims_identity_key UNIQUE (id, classification_run_id, channel_id, channel_snapshot_id);


--
-- Name: creator_content_tag_claims creator_content_tag_claims_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_claims
    ADD CONSTRAINT creator_content_tag_claims_pkey PRIMARY KEY (id);


--
-- Name: creator_content_tag_claims creator_content_tag_claims_run_label_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_claims
    ADD CONSTRAINT creator_content_tag_claims_run_label_key UNIQUE (classification_run_id, normalized_label);


--
-- Name: creator_content_tag_claims creator_content_tag_claims_run_rank_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_claims
    ADD CONSTRAINT creator_content_tag_claims_run_rank_key UNIQUE (classification_run_id, rank);


--
-- Name: creator_content_tag_evidence creator_content_tag_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_evidence
    ADD CONSTRAINT creator_content_tag_evidence_pkey PRIMARY KEY (tag_claim_id, content_snapshot_id);


--
-- Name: creator_content_tag_evidence creator_content_tag_evidence_rank_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_evidence
    ADD CONSTRAINT creator_content_tag_evidence_rank_key UNIQUE (tag_claim_id, evidence_rank);


--
-- Name: creator_search_active creator_search_active_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_active
    ADD CONSTRAINT creator_search_active_pkey PRIMARY KEY (singleton);


--
-- Name: creator_search_active creator_search_active_watermark_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_active
    ADD CONSTRAINT creator_search_active_watermark_key UNIQUE (watermark);


--
-- Name: creator_search_current creator_search_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_current
    ADD CONSTRAINT creator_search_current_pkey PRIMARY KEY (watermark, channel_id);


--
-- Name: creator_search_current creator_search_current_watermark_snapshot_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_current
    ADD CONSTRAINT creator_search_current_watermark_snapshot_id_key UNIQUE (watermark, snapshot_id);


--
-- Name: creator_search_live creator_search_live_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_live
    ADD CONSTRAINT creator_search_live_pkey PRIMARY KEY (channel_id);


--
-- Name: creator_search_live creator_search_live_snapshot_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_live
    ADD CONSTRAINT creator_search_live_snapshot_id_key UNIQUE (snapshot_id);


--
-- Name: creator_search_releases creator_search_releases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_releases
    ADD CONSTRAINT creator_search_releases_pkey PRIMARY KEY (watermark);


--
-- Name: creator_taxonomy_closure creator_taxonomy_closure_path_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_closure
    ADD CONSTRAINT creator_taxonomy_closure_path_key UNIQUE (version_id, ancestor_node_id, descendant_node_id);


--
-- Name: creator_taxonomy_closure creator_taxonomy_closure_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_closure
    ADD CONSTRAINT creator_taxonomy_closure_pkey PRIMARY KEY (id);


--
-- Name: creator_taxonomy_labels creator_taxonomy_labels_node_locale_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_labels
    ADD CONSTRAINT creator_taxonomy_labels_node_locale_key UNIQUE (node_id, locale);


--
-- Name: creator_taxonomy_labels creator_taxonomy_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_labels
    ADD CONSTRAINT creator_taxonomy_labels_pkey PRIMARY KEY (id);


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_nodes
    ADD CONSTRAINT creator_taxonomy_nodes_pkey PRIMARY KEY (id);


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_version_category_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_nodes
    ADD CONSTRAINT creator_taxonomy_nodes_version_category_key UNIQUE (version_id, category_id);


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_version_id_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_nodes
    ADD CONSTRAINT creator_taxonomy_nodes_version_id_id_key UNIQUE (version_id, id);


--
-- Name: creator_taxonomy_versions creator_taxonomy_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_versions
    ADD CONSTRAINT creator_taxonomy_versions_pkey PRIMARY KEY (id);


--
-- Name: creator_taxonomy_versions creator_taxonomy_versions_version_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_versions
    ADD CONSTRAINT creator_taxonomy_versions_version_key_key UNIQUE (version_key);


--
-- Name: import_batches import_batches_id_sha_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.import_batches
    ADD CONSTRAINT import_batches_id_sha_key UNIQUE (id, source_sha256);


--
-- Name: import_batches import_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.import_batches
    ADD CONSTRAINT import_batches_pkey PRIMARY KEY (id);


--
-- Name: import_batches import_batches_source_sha256_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.import_batches
    ADD CONSTRAINT import_batches_source_sha256_key UNIQUE (source_sha256);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: activation_item activation_item_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.activation_item
    ADD CONSTRAINT activation_item_pkey PRIMARY KEY (activation_id, revision_id);


--
-- Name: activation_item activation_item_revision_id_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.activation_item
    ADD CONSTRAINT activation_item_revision_id_key UNIQUE (revision_id);


--
-- Name: activation activation_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.activation
    ADD CONSTRAINT activation_pkey PRIMARY KEY (activation_id);


--
-- Name: channel_ownership channel_ownership_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_ownership
    ADD CONSTRAINT channel_ownership_pkey PRIMARY KEY (channel_id);


--
-- Name: consumer_cursor consumer_cursor_active_revision_id_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.consumer_cursor
    ADD CONSTRAINT consumer_cursor_active_revision_id_key UNIQUE (active_revision_id);


--
-- Name: consumer_cursor consumer_cursor_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.consumer_cursor
    ADD CONSTRAINT consumer_cursor_pkey PRIMARY KEY (channel_id, domain);


--
-- Name: creator_search_changes creator_search_changes_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.creator_search_changes
    ADD CONSTRAINT creator_search_changes_pkey PRIMARY KEY (watermark, channel_id);


--
-- Name: creator_search_legacy_prune_audit creator_search_legacy_prune_audit_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.creator_search_legacy_prune_audit
    ADD CONSTRAINT creator_search_legacy_prune_audit_pkey PRIMARY KEY (prune_id);


--
-- Name: creator_search_storage_state creator_search_storage_state_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.creator_search_storage_state
    ADD CONSTRAINT creator_search_storage_state_pkey PRIMARY KEY (singleton);


--
-- Name: inbox_conflict inbox_conflict_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.inbox_conflict
    ADD CONSTRAINT inbox_conflict_pkey PRIMARY KEY (conflict_id);


--
-- Name: inbox_conflict inbox_conflict_revision_id_conflicting_envelope_hash_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.inbox_conflict
    ADD CONSTRAINT inbox_conflict_revision_id_conflicting_envelope_hash_key UNIQUE (revision_id, conflicting_envelope_hash);


--
-- Name: inbox inbox_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.inbox
    ADD CONSTRAINT inbox_pkey PRIMARY KEY (revision_id);


--
-- Name: inbox inbox_receipt_id_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.inbox
    ADD CONSTRAINT inbox_receipt_id_key UNIQUE (receipt_id);


--
-- Name: projection_batch_item projection_batch_item_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_batch_item
    ADD CONSTRAINT projection_batch_item_pkey PRIMARY KEY (batch_id, channel_id);


--
-- Name: projection_batch projection_batch_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_batch
    ADD CONSTRAINT projection_batch_pkey PRIMARY KEY (batch_id);


--
-- Name: projection_batch projection_batch_source_sha256_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_batch
    ADD CONSTRAINT projection_batch_source_sha256_key UNIQUE (source_sha256);


--
-- Name: projection_cutover projection_cutover_evidence_sha256_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_cutover
    ADD CONSTRAINT projection_cutover_evidence_sha256_key UNIQUE (evidence_sha256);


--
-- Name: projection_cutover projection_cutover_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_cutover
    ADD CONSTRAINT projection_cutover_pkey PRIMARY KEY (cutover_id);


--
-- Name: projection_outbox projection_outbox_activation_id_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_outbox
    ADD CONSTRAINT projection_outbox_activation_id_key UNIQUE (activation_id);


--
-- Name: projection_outbox projection_outbox_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_outbox
    ADD CONSTRAINT projection_outbox_pkey PRIMARY KEY (projection_id);


--
-- Name: projection_snapshot_time_repair projection_snapshot_time_repair_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_snapshot_time_repair
    ADD CONSTRAINT projection_snapshot_time_repair_pkey PRIMARY KEY (snapshot_id);


--
-- Name: quarantine quarantine_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.quarantine
    ADD CONSTRAINT quarantine_pkey PRIMARY KEY (quarantine_id);


--
-- Name: quarantine quarantine_revision_id_issue_code_issue_hash_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.quarantine
    ADD CONSTRAINT quarantine_revision_id_issue_code_issue_hash_key UNIQUE (revision_id, issue_code, issue_hash);


--
-- Name: reconciliation_state reconciliation_state_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.reconciliation_state
    ADD CONSTRAINT reconciliation_state_pkey PRIMARY KEY (channel_id);


--
-- Name: revision revision_pkey; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.revision
    ADD CONSTRAINT revision_pkey PRIMARY KEY (revision_id);


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
-- Name: stream stream_source_deployment_key_key; Type: CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.stream
    ADD CONSTRAINT stream_source_deployment_key_key UNIQUE (source_deployment_key);


--
-- Name: agent_profiles agent_profiles_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.agent_profiles
    ADD CONSTRAINT agent_profiles_pkey PRIMARY KEY (import_batch_id, channel_id);


--
-- Name: channel_runs channel_runs_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_runs
    ADD CONSTRAINT channel_runs_pkey PRIMARY KEY (import_batch_id, run_id);


--
-- Name: channel_tab_pages channel_tab_pages_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_tab_pages
    ADD CONSTRAINT channel_tab_pages_pkey PRIMARY KEY (import_batch_id, run_id, tab, page_no);


--
-- Name: channels channels_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (import_batch_id, channel_id);


--
-- Name: content_candidates content_candidates_import_batch_id_run_id_position_key; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_candidates
    ADD CONSTRAINT content_candidates_import_batch_id_run_id_position_key UNIQUE (import_batch_id, run_id, "position");


--
-- Name: content_candidates content_candidates_import_batch_id_run_id_source_content_id_key; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_candidates
    ADD CONSTRAINT content_candidates_import_batch_id_run_id_source_content_id_key UNIQUE (import_batch_id, run_id, source_content_id);


--
-- Name: content_candidates content_candidates_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_candidates
    ADD CONSTRAINT content_candidates_pkey PRIMARY KEY (import_batch_id, candidate_id);


--
-- Name: content_enrich_tasks content_enrich_tasks_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_enrich_tasks
    ADD CONSTRAINT content_enrich_tasks_pkey PRIMARY KEY (import_batch_id, task_id);


--
-- Name: contents contents_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.contents
    ADD CONSTRAINT contents_pkey PRIMARY KEY (import_batch_id, content_key);


--
-- Name: finalized_profiles finalized_profiles_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.finalized_profiles
    ADD CONSTRAINT finalized_profiles_pkey PRIMARY KEY (import_batch_id, channel_id);


--
-- Name: query_pages query_pages_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_pages
    ADD CONSTRAINT query_pages_pkey PRIMARY KEY (import_batch_id, page_id);


--
-- Name: query_sets query_sets_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_sets
    ADD CONSTRAINT query_sets_pkey PRIMARY KEY (import_batch_id, query_set_id);


--
-- Name: query_terms query_terms_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_terms
    ADD CONSTRAINT query_terms_pkey PRIMARY KEY (import_batch_id, query_id);


--
-- Name: channel_runs raw_channel_runs_batch_run_channel_key; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_runs
    ADD CONSTRAINT raw_channel_runs_batch_run_channel_key UNIQUE (import_batch_id, run_id, channel_id);


--
-- Name: contents raw_contents_batch_key_channel_key; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.contents
    ADD CONSTRAINT raw_contents_batch_key_channel_key UNIQUE (import_batch_id, content_key, channel_id);


--
-- Name: raw_objects raw_objects_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.raw_objects
    ADD CONSTRAINT raw_objects_pkey PRIMARY KEY (import_batch_id, raw_object_id);


--
-- Name: youtube_api_batches youtube_api_batches_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.youtube_api_batches
    ADD CONSTRAINT youtube_api_batches_pkey PRIMARY KEY (import_batch_id, batch_id);


--
-- Name: youtube_api_daily_usage youtube_api_daily_usage_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.youtube_api_daily_usage
    ADD CONSTRAINT youtube_api_daily_usage_pkey PRIMARY KEY (import_batch_id, usage_date);


--
-- Name: youtube_api_tasks youtube_api_tasks_import_batch_id_source_content_id_key; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.youtube_api_tasks
    ADD CONSTRAINT youtube_api_tasks_import_batch_id_source_content_id_key UNIQUE (import_batch_id, source_content_id);


--
-- Name: youtube_api_tasks youtube_api_tasks_pkey; Type: CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.youtube_api_tasks
    ADD CONSTRAINT youtube_api_tasks_pkey PRIMARY KEY (import_batch_id, task_id);


--
-- Name: agent_current agent_current_pkey; Type: CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.agent_current
    ADD CONSTRAINT agent_current_pkey PRIMARY KEY (channel_id);


--
-- Name: content_current content_current_pkey; Type: CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.content_current
    ADD CONSTRAINT content_current_pkey PRIMARY KEY (channel_id, content_id);


--
-- Name: entity_current entity_current_pkey; Type: CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.entity_current
    ADD CONSTRAINT entity_current_pkey PRIMARY KEY (channel_id);


--
-- Name: video_current video_current_pkey; Type: CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.video_current
    ADD CONSTRAINT video_current_pkey PRIMARY KEY (channel_id);


--
-- Name: creator_category_assignments_active_taxonomy_category_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_assignments_active_taxonomy_category_idx ON public.creator_category_assignments USING btree (taxonomy_version_id, category_id, channel_id) WHERE (valid_until IS NULL);


--
-- Name: creator_category_assignments_channel_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_assignments_channel_idx ON public.creator_category_assignments USING btree (channel_id, valid_from DESC);


--
-- Name: creator_category_assignments_claim_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_assignments_claim_idx ON public.creator_category_assignments USING btree (claim_id, channel_id, taxonomy_version_id, category_id);


--
-- Name: creator_category_assignments_one_active_category_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX creator_category_assignments_one_active_category_idx ON public.creator_category_assignments USING btree (channel_id, category_id) WHERE (valid_until IS NULL);


--
-- Name: creator_category_assignments_one_active_primary_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX creator_category_assignments_one_active_primary_idx ON public.creator_category_assignments USING btree (channel_id) WHERE (is_primary AND (valid_until IS NULL));


--
-- Name: creator_category_assignments_taxonomy_category_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_assignments_taxonomy_category_idx ON public.creator_category_assignments USING btree (taxonomy_version_id, category_id, channel_id);


--
-- Name: creator_category_claims_channel_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_claims_channel_idx ON public.creator_category_claims USING btree (channel_id, status, created_at DESC);


--
-- Name: creator_category_claims_classification_run_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_claims_classification_run_idx ON public.creator_category_claims USING btree (classification_run_id, id) WHERE (classification_run_id IS NOT NULL);


--
-- Name: creator_category_claims_snapshot_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_claims_snapshot_idx ON public.creator_category_claims USING btree (channel_snapshot_id, channel_id) WHERE (channel_snapshot_id IS NOT NULL);


--
-- Name: creator_category_claims_source_record_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX creator_category_claims_source_record_idx ON public.creator_category_claims USING btree (source_system, source_record_key) WHERE (source_record_key IS NOT NULL);


--
-- Name: creator_category_claims_taxonomy_category_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_category_claims_taxonomy_category_idx ON public.creator_category_claims USING btree (taxonomy_version_id, category_id, status, channel_id);


--
-- Name: creator_classification_runs_active_identity_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX creator_classification_runs_active_identity_idx ON public.creator_classification_runs USING btree (channel_id, channel_snapshot_id, taxonomy_version_id, requested_model, prompt_version, cleaner_version, input_hash) WHERE (status = ANY (ARRAY['running'::text, 'succeeded'::text]));


--
-- Name: creator_classification_runs_channel_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_classification_runs_channel_idx ON public.creator_classification_runs USING btree (channel_id, created_at DESC, id DESC);


--
-- Name: creator_classification_runs_snapshot_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_classification_runs_snapshot_idx ON public.creator_classification_runs USING btree (channel_snapshot_id, channel_id, id);


--
-- Name: creator_classification_runs_stale_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_classification_runs_stale_idx ON public.creator_classification_runs USING btree (lease_expires_at, id) WHERE (status = 'running'::text);


--
-- Name: creator_classification_runs_taxonomy_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_classification_runs_taxonomy_idx ON public.creator_classification_runs USING btree (taxonomy_version_id, id);


--
-- Name: creator_content_tag_claims_channel_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_content_tag_claims_channel_idx ON public.creator_content_tag_claims USING btree (channel_id, status, created_at DESC, id DESC);


--
-- Name: creator_content_tag_claims_label_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_content_tag_claims_label_idx ON public.creator_content_tag_claims USING btree (normalized_label, status, channel_id);


--
-- Name: creator_content_tag_evidence_content_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_content_tag_evidence_content_idx ON public.creator_content_tag_evidence USING btree (content_snapshot_id, tag_claim_id);


--
-- Name: creator_taxonomy_closure_descendant_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_taxonomy_closure_descendant_idx ON public.creator_taxonomy_closure USING btree (version_id, descendant_node_id, depth, ancestor_node_id);


--
-- Name: creator_taxonomy_nodes_category_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX creator_taxonomy_nodes_category_id_idx ON public.creator_taxonomy_nodes USING btree (category_id);


--
-- Name: creator_taxonomy_nodes_child_sort_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX creator_taxonomy_nodes_child_sort_idx ON public.creator_taxonomy_nodes USING btree (version_id, parent_node_id, sort_order) WHERE (parent_node_id IS NOT NULL);


--
-- Name: creator_taxonomy_nodes_root_sort_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX creator_taxonomy_nodes_root_sort_idx ON public.creator_taxonomy_nodes USING btree (version_id, sort_order) WHERE (parent_node_id IS NULL);


--
-- Name: creator_taxonomy_versions_one_active_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX creator_taxonomy_versions_one_active_idx ON public.creator_taxonomy_versions USING btree ((true)) WHERE (status = 'active'::text);


--
-- Name: idx_channel_links_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_links_channel ON public.channel_links USING btree (channel_id);


--
-- Name: idx_channel_links_snapshot_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_links_snapshot_type ON public.channel_links USING btree (channel_snapshot_id, link_type);


--
-- Name: idx_channel_metric_values_baseline_snapshot; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_metric_values_baseline_snapshot ON public.channel_metric_values USING btree (baseline_snapshot_id) WHERE (baseline_snapshot_id IS NOT NULL);


--
-- Name: idx_channel_metric_values_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_metric_values_channel ON public.channel_metric_values USING btree (channel_id);


--
-- Name: idx_channel_metric_values_snapshot; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_metric_values_snapshot ON public.channel_metric_values USING btree (channel_snapshot_id, scope, metric_key);


--
-- Name: idx_channel_profile_facts_channel_field; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_profile_facts_channel_field ON public.channel_profile_facts USING btree (channel_id, field_key, channel_snapshot_id);


--
-- Name: idx_channel_snapshots_import_batch; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_snapshots_import_batch ON public.channel_snapshots USING btree (import_batch_id);


--
-- Name: idx_channel_snapshots_latest; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_channel_snapshots_latest ON public.channel_snapshots USING btree (channel_id, captured_at DESC, id DESC);


--
-- Name: idx_content_items_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_content_items_channel ON public.content_items USING btree (channel_id);


--
-- Name: idx_content_snapshots_metric_scope; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_content_snapshots_metric_scope ON public.content_snapshots USING btree (channel_snapshot_id, is_recent, is_canonical, content_kind, published_at DESC);


--
-- Name: idx_content_snapshots_one_canonical; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_content_snapshots_one_canonical ON public.content_snapshots USING btree (channel_snapshot_id, video_id) WHERE is_canonical;


--
-- Name: idx_content_snapshots_snapshot_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_content_snapshots_snapshot_date ON public.content_snapshots USING btree (channel_snapshot_id, published_date DESC, video_id);


--
-- Name: idx_content_snapshots_snapshot_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_content_snapshots_snapshot_kind ON public.content_snapshots USING btree (channel_snapshot_id, content_kind);


--
-- Name: idx_content_snapshots_source_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_content_snapshots_source_key ON public.content_snapshots USING btree (channel_snapshot_id, source_content_key) WHERE (source_content_key IS NOT NULL);


--
-- Name: idx_content_snapshots_sourceless_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_content_snapshots_sourceless_kind ON public.content_snapshots USING btree (channel_snapshot_id, video_id, content_kind) WHERE (source_content_key IS NULL);


--
-- Name: idx_content_snapshots_video; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_content_snapshots_video ON public.content_snapshots USING btree (video_id);


--
-- Name: idx_content_snapshots_video_history; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_content_snapshots_video_history ON public.content_snapshots USING btree (video_id, channel_snapshot_id);


--
-- Name: idx_crawler_ingest_batches_published; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_crawler_ingest_batches_published ON public.crawler_ingest_batches USING btree (published_at DESC) WHERE (status = 'published'::text);


--
-- Name: idx_creator_search_audience_age; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_audience_age ON public.creator_search_current USING btree (watermark, audience_age, channel_id);


--
-- Name: idx_creator_search_audience_country; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_audience_country ON public.creator_search_current USING btree (watermark, audience_country, channel_id);


--
-- Name: idx_creator_search_audience_gender; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_audience_gender ON public.creator_search_current USING btree (watermark, audience_gender, channel_id);


--
-- Name: idx_creator_search_audience_language; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_audience_language ON public.creator_search_current USING btree (watermark, audience_language, channel_id);


--
-- Name: idx_creator_search_avg_views; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_avg_views ON public.creator_search_current USING btree (watermark, avg_views DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_categories; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_categories ON public.creator_search_current USING gin (category_paths);


--
-- Name: idx_creator_search_contacts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_contacts ON public.creator_search_current USING gin (contact_types);


--
-- Name: idx_creator_search_country_language; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_country_language ON public.creator_search_current USING btree (watermark, country, language, channel_id);


--
-- Name: idx_creator_search_current_channel_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_current_channel_id ON public.creator_search_current USING btree (channel_id);


--
-- Name: idx_creator_search_current_snapshot_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_current_snapshot_id ON public.creator_search_current USING btree (snapshot_id);


--
-- Name: idx_creator_search_engagement; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_engagement ON public.creator_search_current USING btree (watermark, engagement_rate_by_views DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_last_published_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_last_published_date ON public.creator_search_current USING btree (watermark, last_published_date DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_live_audience_age; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_audience_age ON public.creator_search_live USING btree (audience_age, channel_id);


--
-- Name: idx_creator_search_live_audience_country; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_audience_country ON public.creator_search_live USING btree (audience_country, channel_id);


--
-- Name: idx_creator_search_live_audience_gender; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_audience_gender ON public.creator_search_live USING btree (audience_gender, channel_id);


--
-- Name: idx_creator_search_live_audience_language; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_audience_language ON public.creator_search_live USING btree (audience_language, channel_id);


--
-- Name: idx_creator_search_live_avg_views; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_avg_views ON public.creator_search_live USING btree (avg_views DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_live_categories; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_categories ON public.creator_search_live USING gin (category_paths);


--
-- Name: idx_creator_search_live_contacts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_contacts ON public.creator_search_live USING gin (contact_types);


--
-- Name: idx_creator_search_live_country_language; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_country_language ON public.creator_search_live USING btree (country, language, channel_id);


--
-- Name: idx_creator_search_live_engagement; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_engagement ON public.creator_search_live USING btree (engagement_rate_by_views DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_live_last_published_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_last_published_date ON public.creator_search_live USING btree (last_published_date DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_live_subscribers; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_subscribers ON public.creator_search_live USING btree (subscribers DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_live_tags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_tags ON public.creator_search_live USING gin (tags);


--
-- Name: idx_creator_search_live_text; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_text ON public.creator_search_live USING gin (search_text public.gin_trgm_ops);


--
-- Name: idx_creator_search_live_watermark; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_live_watermark ON public.creator_search_live USING btree (watermark, channel_id);


--
-- Name: idx_creator_search_one_active_release; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_creator_search_one_active_release ON public.creator_search_releases USING btree (status) WHERE (status = 'active'::text);


--
-- Name: idx_creator_search_subscribers; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_subscribers ON public.creator_search_current USING btree (watermark, subscribers DESC NULLS LAST, channel_id);


--
-- Name: idx_creator_search_tags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_tags ON public.creator_search_current USING gin (tags);


--
-- Name: idx_creator_search_text; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_creator_search_text ON public.creator_search_current USING gin (search_text public.gin_trgm_ops);


--
-- Name: idx_import_batches_source_byte_sha256; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_import_batches_source_byte_sha256 ON public.import_batches USING btree (source_byte_sha256) WHERE (source_byte_sha256 IS NOT NULL);


--
-- Name: idx_business_projection_batch_item_channel; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_projection_batch_item_channel ON publication.projection_batch_item USING btree (channel_id, projected_at, batch_id);


--
-- Name: idx_business_projection_outbox_cutover; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_projection_outbox_cutover ON publication.projection_outbox USING btree (released_by_cutover_id) WHERE (released_by_cutover_id IS NOT NULL);


--
-- Name: idx_business_publication_activation_channel; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_publication_activation_channel ON publication.activation USING btree (channel_id, activated_at, activation_id);


--
-- Name: idx_business_publication_inbox_route; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_publication_inbox_route ON publication.inbox USING btree (publication_stream_id, channel_id, domain, data_sequence);


--
-- Name: idx_business_publication_projection_claim; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_publication_projection_claim ON publication.projection_outbox USING btree (status, next_attempt_at, created_at) WHERE (status = ANY (ARRAY['pending'::text, 'leased'::text, 'retry_wait'::text]));


--
-- Name: idx_business_publication_projection_predecessor; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_publication_projection_predecessor ON publication.projection_outbox USING btree (channel_id, publication_stream_id, created_at, projection_id);


--
-- Name: idx_business_publication_reconciliation_claim; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_publication_reconciliation_claim ON publication.reconciliation_state USING btree (next_attempt_at, channel_id) WHERE (lease_owner IS NULL);


--
-- Name: idx_business_publication_reconciliation_expired_lease; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_publication_reconciliation_expired_lease ON publication.reconciliation_state USING btree (lease_expires_at, channel_id) WHERE (lease_owner IS NOT NULL);


--
-- Name: idx_business_publication_revision_activation; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_business_publication_revision_activation ON publication.revision USING btree (activation_status, publication_stream_id, channel_id, domain, data_sequence);


--
-- Name: idx_creator_search_changes_channel; Type: INDEX; Schema: publication; Owner: -
--

CREATE INDEX idx_creator_search_changes_channel ON publication.creator_search_changes USING btree (channel_id, changed_at DESC, watermark);


--
-- Name: raw_channel_runs_channel_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_channel_runs_channel_idx ON raw_crawler.channel_runs USING btree (import_batch_id, channel_id);


--
-- Name: raw_channel_tab_pages_channel_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_channel_tab_pages_channel_idx ON raw_crawler.channel_tab_pages USING btree (import_batch_id, channel_id);


--
-- Name: raw_channels_latest_run_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_channels_latest_run_idx ON raw_crawler.channels USING btree (import_batch_id, latest_run_id);


--
-- Name: raw_content_candidates_channel_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_content_candidates_channel_idx ON raw_crawler.content_candidates USING btree (import_batch_id, channel_id);


--
-- Name: raw_content_candidates_content_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_content_candidates_content_idx ON raw_crawler.content_candidates USING btree (import_batch_id, content_key);


--
-- Name: raw_content_candidates_run_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_content_candidates_run_idx ON raw_crawler.content_candidates USING btree (import_batch_id, run_id);


--
-- Name: raw_content_enrich_channel_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_content_enrich_channel_idx ON raw_crawler.content_enrich_tasks USING btree (import_batch_id, channel_id);


--
-- Name: raw_content_enrich_content_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_content_enrich_content_idx ON raw_crawler.content_enrich_tasks USING btree (import_batch_id, content_key);


--
-- Name: raw_contents_lineage_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_contents_lineage_idx ON raw_crawler.contents USING btree (import_batch_id, channel_id, source_content_id);


--
-- Name: raw_contents_run_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_contents_run_idx ON raw_crawler.contents USING btree (import_batch_id, run_id);


--
-- Name: raw_finalized_profiles_run_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_finalized_profiles_run_idx ON raw_crawler.finalized_profiles USING btree (import_batch_id, run_id);


--
-- Name: raw_query_pages_query_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_query_pages_query_idx ON raw_crawler.query_pages USING btree (import_batch_id, query_id);


--
-- Name: raw_query_terms_query_set_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_query_terms_query_set_idx ON raw_crawler.query_terms USING btree (import_batch_id, query_set_id);


--
-- Name: raw_youtube_api_tasks_source_idx; Type: INDEX; Schema: raw_crawler; Owner: -
--

CREATE INDEX raw_youtube_api_tasks_source_idx ON raw_crawler.youtube_api_tasks USING btree (import_batch_id, source_content_id);


--
-- Name: uq_business_result_content_active_position; Type: INDEX; Schema: result; Owner: -
--

CREATE UNIQUE INDEX uq_business_result_content_active_position ON result.content_current USING btree (channel_id, "position") WHERE (window_status = 'active'::text);


--
-- Name: creator_categories creator_categories_key_immutable_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_categories_key_immutable_trigger BEFORE UPDATE OF category_key ON public.creator_categories FOR EACH ROW EXECUTE FUNCTION public.protect_creator_category_key();


--
-- Name: creator_categories creator_categories_retirement_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_categories_retirement_trigger BEFORE UPDATE OF retired_at ON public.creator_categories FOR EACH ROW EXECUTE FUNCTION public.enforce_creator_category_retirement();


--
-- Name: creator_category_assignments creator_category_assignments_accepted_claim_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_category_assignments_accepted_claim_trigger AFTER INSERT OR UPDATE OF claim_id, channel_id, taxonomy_version_id, category_id, valid_until ON public.creator_category_assignments FOR EACH ROW EXECUTE FUNCTION public.enforce_confirmed_creator_category_assignment();


--
-- Name: creator_category_assignments creator_category_assignments_history_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_category_assignments_history_trigger BEFORE DELETE OR UPDATE ON public.creator_category_assignments FOR EACH ROW EXECUTE FUNCTION public.protect_creator_category_assignment_history();


--
-- Name: creator_category_claims creator_category_claims_assignment_status_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER creator_category_claims_assignment_status_trigger AFTER UPDATE OF status ON public.creator_category_claims DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.prevent_assigned_claim_status_reversal();


--
-- Name: creator_category_claims creator_category_claims_reviewed_immutable_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_category_claims_reviewed_immutable_trigger BEFORE DELETE OR UPDATE ON public.creator_category_claims FOR EACH ROW EXECUTE FUNCTION public.protect_reviewed_creator_category_claim();


--
-- Name: creator_classification_runs creator_classification_runs_input_scope_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_classification_runs_input_scope_trigger BEFORE INSERT OR UPDATE OF input_content_ids, channel_id, channel_snapshot_id ON public.creator_classification_runs FOR EACH ROW EXECUTE FUNCTION public.enforce_creator_classification_run_input_scope();


--
-- Name: creator_content_tag_claims creator_content_tag_claims_reviewed_immutable_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_content_tag_claims_reviewed_immutable_trigger BEFORE DELETE OR UPDATE ON public.creator_content_tag_claims FOR EACH ROW EXECUTE FUNCTION public.protect_reviewed_creator_content_tag_claim();


--
-- Name: creator_content_tag_evidence creator_content_tag_evidence_history_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_content_tag_evidence_history_trigger BEFORE INSERT OR DELETE OR UPDATE ON public.creator_content_tag_evidence FOR EACH ROW EXECUTE FUNCTION public.protect_creator_content_tag_evidence_history();


--
-- Name: creator_content_tag_evidence creator_content_tag_evidence_input_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_content_tag_evidence_input_trigger BEFORE INSERT OR UPDATE OF classification_run_id, content_snapshot_id ON public.creator_content_tag_evidence FOR EACH ROW EXECUTE FUNCTION public.enforce_creator_content_tag_evidence_input();


--
-- Name: creator_taxonomy_closure creator_taxonomy_closure_published_immutable_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_taxonomy_closure_published_immutable_trigger BEFORE INSERT OR DELETE OR UPDATE ON public.creator_taxonomy_closure FOR EACH ROW EXECUTE FUNCTION public.protect_published_creator_taxonomy_catalog();


--
-- Name: creator_taxonomy_labels creator_taxonomy_labels_published_immutable_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_taxonomy_labels_published_immutable_trigger BEFORE INSERT OR DELETE OR UPDATE ON public.creator_taxonomy_labels FOR EACH ROW EXECUTE FUNCTION public.protect_published_creator_taxonomy_catalog();


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_parent_level_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER creator_taxonomy_nodes_parent_level_trigger AFTER INSERT OR UPDATE OF version_id, parent_node_id, level ON public.creator_taxonomy_nodes DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_creator_taxonomy_node_parent();


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_published_immutable_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_taxonomy_nodes_published_immutable_trigger BEFORE INSERT OR DELETE OR UPDATE ON public.creator_taxonomy_nodes FOR EACH ROW EXECUTE FUNCTION public.protect_published_creator_taxonomy_catalog();


--
-- Name: creator_taxonomy_versions creator_taxonomy_versions_published_immutable_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_taxonomy_versions_published_immutable_trigger BEFORE DELETE OR UPDATE ON public.creator_taxonomy_versions FOR EACH ROW EXECUTE FUNCTION public.protect_published_creator_taxonomy_version();


--
-- Name: creator_taxonomy_versions creator_taxonomy_versions_transition_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER creator_taxonomy_versions_transition_trigger BEFORE UPDATE OF status ON public.creator_taxonomy_versions FOR EACH ROW EXECUTE FUNCTION public.enforce_creator_taxonomy_version_transition();


--
-- Name: channel_snapshots trg_channel_snapshot_verified_status; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_channel_snapshot_verified_status BEFORE INSERT OR UPDATE OF is_verified, is_verified_status ON public.channel_snapshots FOR EACH ROW EXECUTE FUNCTION public.normalize_channel_snapshot_verified_status();


--
-- Name: creator_search_current trg_creator_search_channel_observation_times; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_creator_search_channel_observation_times BEFORE INSERT OR UPDATE OF snapshot_id, channel_id, channel_observed_at, subscribers_observed_at, total_views_observed_at, channel_video_count_observed_at, youtube_business_email_available, youtube_business_email_observed_at ON public.creator_search_current FOR EACH ROW EXECUTE FUNCTION public.normalize_creator_search_channel_observation_times();


--
-- Name: creator_search_live trg_creator_search_live_channel_observation_times; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_creator_search_live_channel_observation_times BEFORE INSERT OR UPDATE OF snapshot_id, channel_id, channel_observed_at, subscribers_observed_at, total_views_observed_at, channel_video_count_observed_at, youtube_business_email_available, youtube_business_email_observed_at ON public.creator_search_live FOR EACH ROW EXECUTE FUNCTION public.normalize_creator_search_channel_observation_times();


--
-- Name: creator_search_live trg_creator_search_live_verified_status; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_creator_search_live_verified_status BEFORE INSERT OR UPDATE OF verified, verified_status ON public.creator_search_live FOR EACH ROW EXECUTE FUNCTION public.normalize_creator_search_verified_status();


--
-- Name: creator_search_current trg_creator_search_verified_status; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_creator_search_verified_status BEFORE INSERT OR UPDATE OF verified, verified_status ON public.creator_search_current FOR EACH ROW EXECUTE FUNCTION public.normalize_creator_search_verified_status();


--
-- Name: activation trg_business_publication_activation_audit; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_business_publication_activation_audit BEFORE DELETE OR UPDATE ON publication.activation FOR EACH ROW EXECUTE FUNCTION publication.guard_business_activation_audit();


--
-- Name: activation_item trg_business_publication_activation_item_audit; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_business_publication_activation_item_audit BEFORE DELETE OR UPDATE ON publication.activation_item FOR EACH ROW EXECUTE FUNCTION publication.guard_business_activation_audit();


--
-- Name: consumer_cursor trg_business_publication_consumer_cursor; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_business_publication_consumer_cursor BEFORE DELETE OR UPDATE ON publication.consumer_cursor FOR EACH ROW EXECUTE FUNCTION publication.guard_business_consumer_cursor();


--
-- Name: inbox trg_business_publication_inbox; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_business_publication_inbox BEFORE DELETE OR UPDATE ON publication.inbox FOR EACH ROW EXECUTE FUNCTION publication.guard_business_inbox();


--
-- Name: revision trg_business_publication_revision; Type: TRIGGER; Schema: publication; Owner: -
--

CREATE TRIGGER trg_business_publication_revision BEFORE DELETE OR UPDATE ON publication.revision FOR EACH ROW EXECUTE FUNCTION publication.guard_business_revision();


--
-- Name: channel_links channel_links_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_links
    ADD CONSTRAINT channel_links_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_links channel_links_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_links
    ADD CONSTRAINT channel_links_snapshot_channel_fkey FOREIGN KEY (channel_snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: channel_metric_values channel_metric_values_baseline_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_metric_values
    ADD CONSTRAINT channel_metric_values_baseline_channel_fkey FOREIGN KEY (baseline_snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE SET NULL (baseline_snapshot_id);


--
-- Name: channel_metric_values channel_metric_values_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_metric_values
    ADD CONSTRAINT channel_metric_values_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_metric_values channel_metric_values_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_metric_values
    ADD CONSTRAINT channel_metric_values_snapshot_channel_fkey FOREIGN KEY (channel_snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: channel_profile_facts channel_profile_facts_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_profile_facts
    ADD CONSTRAINT channel_profile_facts_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_profile_facts channel_profile_facts_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_profile_facts
    ADD CONSTRAINT channel_profile_facts_snapshot_channel_fkey FOREIGN KEY (channel_snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: channel_snapshots channel_snapshots_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_snapshots
    ADD CONSTRAINT channel_snapshots_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: channel_snapshots channel_snapshots_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_snapshots
    ADD CONSTRAINT channel_snapshots_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.import_batches(id) ON DELETE CASCADE;


--
-- Name: content_items content_items_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_items
    ADD CONSTRAINT content_items_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: content_snapshots content_snapshots_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_snapshots
    ADD CONSTRAINT content_snapshots_snapshot_channel_fkey FOREIGN KEY (channel_snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: content_snapshots content_snapshots_source_kind_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_snapshots
    ADD CONSTRAINT content_snapshots_source_kind_fkey FOREIGN KEY (source_content_type, content_kind) REFERENCES public.content_type_taxonomy(source_content_type, content_kind);


--
-- Name: content_snapshots content_snapshots_video_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.content_snapshots
    ADD CONSTRAINT content_snapshots_video_channel_fkey FOREIGN KEY (video_id, channel_id) REFERENCES public.content_items(video_id, channel_id) ON DELETE CASCADE;


--
-- Name: crawler_ingest_batches crawler_ingest_batches_import_sha_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.crawler_ingest_batches
    ADD CONSTRAINT crawler_ingest_batches_import_sha_fkey FOREIGN KEY (import_batch_id, source_sha256) REFERENCES public.import_batches(id, source_sha256) ON DELETE RESTRICT;


--
-- Name: creator_category_assignments creator_category_assignments_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_assignments
    ADD CONSTRAINT creator_category_assignments_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: creator_category_assignments creator_category_assignments_claim_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_assignments
    ADD CONSTRAINT creator_category_assignments_claim_fkey FOREIGN KEY (claim_id, channel_id, taxonomy_version_id, category_id) REFERENCES public.creator_category_claims(id, channel_id, taxonomy_version_id, category_id);


--
-- Name: creator_category_assignments creator_category_assignments_taxonomy_node_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_assignments
    ADD CONSTRAINT creator_category_assignments_taxonomy_node_fkey FOREIGN KEY (taxonomy_version_id, category_id) REFERENCES public.creator_taxonomy_nodes(version_id, category_id);


--
-- Name: creator_category_claims creator_category_claims_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_claims
    ADD CONSTRAINT creator_category_claims_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: creator_category_claims creator_category_claims_classification_run_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_claims
    ADD CONSTRAINT creator_category_claims_classification_run_fkey FOREIGN KEY (classification_run_id, channel_id, channel_snapshot_id, taxonomy_version_id) REFERENCES public.creator_classification_runs(id, channel_id, channel_snapshot_id, taxonomy_version_id) ON DELETE CASCADE;


--
-- Name: creator_category_claims creator_category_claims_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_claims
    ADD CONSTRAINT creator_category_claims_snapshot_channel_fkey FOREIGN KEY (channel_snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: creator_category_claims creator_category_claims_taxonomy_node_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_category_claims
    ADD CONSTRAINT creator_category_claims_taxonomy_node_fkey FOREIGN KEY (taxonomy_version_id, category_id) REFERENCES public.creator_taxonomy_nodes(version_id, category_id);


--
-- Name: creator_classification_runs creator_classification_runs_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_classification_runs
    ADD CONSTRAINT creator_classification_runs_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: creator_classification_runs creator_classification_runs_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_classification_runs
    ADD CONSTRAINT creator_classification_runs_snapshot_channel_fkey FOREIGN KEY (channel_snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: creator_classification_runs creator_classification_runs_taxonomy_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_classification_runs
    ADD CONSTRAINT creator_classification_runs_taxonomy_version_id_fkey FOREIGN KEY (taxonomy_version_id) REFERENCES public.creator_taxonomy_versions(id);


--
-- Name: creator_content_tag_claims creator_content_tag_claims_run_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_claims
    ADD CONSTRAINT creator_content_tag_claims_run_fkey FOREIGN KEY (classification_run_id, channel_id, channel_snapshot_id) REFERENCES public.creator_classification_runs(id, channel_id, channel_snapshot_id) ON DELETE CASCADE;


--
-- Name: creator_content_tag_evidence creator_content_tag_evidence_content_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_evidence
    ADD CONSTRAINT creator_content_tag_evidence_content_fkey FOREIGN KEY (content_snapshot_id, channel_snapshot_id, channel_id) REFERENCES public.content_snapshots(id, channel_snapshot_id, channel_id) ON DELETE CASCADE;


--
-- Name: creator_content_tag_evidence creator_content_tag_evidence_tag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_content_tag_evidence
    ADD CONSTRAINT creator_content_tag_evidence_tag_fkey FOREIGN KEY (tag_claim_id, classification_run_id, channel_id, channel_snapshot_id) REFERENCES public.creator_content_tag_claims(id, classification_run_id, channel_id, channel_snapshot_id) ON DELETE CASCADE;


--
-- Name: creator_search_active creator_search_active_watermark_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_active
    ADD CONSTRAINT creator_search_active_watermark_fkey FOREIGN KEY (watermark) REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT;


--
-- Name: creator_search_current creator_search_current_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_current
    ADD CONSTRAINT creator_search_current_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: creator_search_current creator_search_current_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_current
    ADD CONSTRAINT creator_search_current_snapshot_channel_fkey FOREIGN KEY (snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: creator_search_current creator_search_current_watermark_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_current
    ADD CONSTRAINT creator_search_current_watermark_fkey FOREIGN KEY (watermark) REFERENCES public.creator_search_releases(watermark) ON DELETE CASCADE;


--
-- Name: creator_search_live creator_search_live_channel_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_live
    ADD CONSTRAINT creator_search_live_channel_id_fkey FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;


--
-- Name: creator_search_live creator_search_live_snapshot_channel_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_live
    ADD CONSTRAINT creator_search_live_snapshot_channel_fkey FOREIGN KEY (snapshot_id, channel_id) REFERENCES public.channel_snapshots(id, channel_id) ON DELETE CASCADE;


--
-- Name: creator_search_live creator_search_live_watermark_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_live
    ADD CONSTRAINT creator_search_live_watermark_fkey FOREIGN KEY (watermark) REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT;


--
-- Name: creator_search_releases creator_search_releases_watermark_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_search_releases
    ADD CONSTRAINT creator_search_releases_watermark_fkey FOREIGN KEY (watermark) REFERENCES public.import_batches(id) ON DELETE CASCADE;


--
-- Name: creator_taxonomy_closure creator_taxonomy_closure_ancestor_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_closure
    ADD CONSTRAINT creator_taxonomy_closure_ancestor_fkey FOREIGN KEY (version_id, ancestor_node_id) REFERENCES public.creator_taxonomy_nodes(version_id, id);


--
-- Name: creator_taxonomy_closure creator_taxonomy_closure_descendant_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_closure
    ADD CONSTRAINT creator_taxonomy_closure_descendant_fkey FOREIGN KEY (version_id, descendant_node_id) REFERENCES public.creator_taxonomy_nodes(version_id, id);


--
-- Name: creator_taxonomy_closure creator_taxonomy_closure_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_closure
    ADD CONSTRAINT creator_taxonomy_closure_version_id_fkey FOREIGN KEY (version_id) REFERENCES public.creator_taxonomy_versions(id);


--
-- Name: creator_taxonomy_labels creator_taxonomy_labels_node_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_labels
    ADD CONSTRAINT creator_taxonomy_labels_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.creator_taxonomy_nodes(id);


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_category_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_nodes
    ADD CONSTRAINT creator_taxonomy_nodes_category_id_fkey FOREIGN KEY (category_id) REFERENCES public.creator_categories(id);


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_parent_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_nodes
    ADD CONSTRAINT creator_taxonomy_nodes_parent_fkey FOREIGN KEY (version_id, parent_node_id) REFERENCES public.creator_taxonomy_nodes(version_id, id);


--
-- Name: creator_taxonomy_nodes creator_taxonomy_nodes_version_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.creator_taxonomy_nodes
    ADD CONSTRAINT creator_taxonomy_nodes_version_id_fkey FOREIGN KEY (version_id) REFERENCES public.creator_taxonomy_versions(id);


--
-- Name: activation_item activation_item_activation_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.activation_item
    ADD CONSTRAINT activation_item_activation_id_fkey FOREIGN KEY (activation_id) REFERENCES publication.activation(activation_id) ON DELETE RESTRICT;


--
-- Name: activation_item activation_item_revision_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.activation_item
    ADD CONSTRAINT activation_item_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES publication.revision(revision_id) ON DELETE RESTRICT;


--
-- Name: activation activation_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.activation
    ADD CONSTRAINT activation_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: channel_ownership channel_ownership_active_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_ownership
    ADD CONSTRAINT channel_ownership_active_publication_stream_id_fkey FOREIGN KEY (active_publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: channel_ownership channel_ownership_previous_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.channel_ownership
    ADD CONSTRAINT channel_ownership_previous_publication_stream_id_fkey FOREIGN KEY (previous_publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: consumer_cursor consumer_cursor_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.consumer_cursor
    ADD CONSTRAINT consumer_cursor_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: creator_search_changes creator_search_changes_watermark_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.creator_search_changes
    ADD CONSTRAINT creator_search_changes_watermark_fkey FOREIGN KEY (watermark) REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT;


--
-- Name: creator_search_legacy_prune_audit creator_search_legacy_prune_audit_active_watermark_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.creator_search_legacy_prune_audit
    ADD CONSTRAINT creator_search_legacy_prune_audit_active_watermark_fkey FOREIGN KEY (active_watermark) REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT;


--
-- Name: creator_search_storage_state creator_search_storage_state_initialized_watermark_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.creator_search_storage_state
    ADD CONSTRAINT creator_search_storage_state_initialized_watermark_fkey FOREIGN KEY (initialized_watermark) REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT;


--
-- Name: inbox_conflict inbox_conflict_revision_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.inbox_conflict
    ADD CONSTRAINT inbox_conflict_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES publication.inbox(revision_id) ON DELETE RESTRICT;


--
-- Name: projection_batch_item projection_batch_item_batch_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_batch_item
    ADD CONSTRAINT projection_batch_item_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES publication.projection_batch(batch_id) ON DELETE RESTRICT;


--
-- Name: projection_batch projection_batch_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_batch
    ADD CONSTRAINT projection_batch_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: projection_cutover projection_cutover_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_cutover
    ADD CONSTRAINT projection_cutover_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: projection_outbox projection_outbox_activation_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_outbox
    ADD CONSTRAINT projection_outbox_activation_id_fkey FOREIGN KEY (activation_id) REFERENCES publication.activation(activation_id) ON DELETE RESTRICT;


--
-- Name: projection_outbox projection_outbox_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_outbox
    ADD CONSTRAINT projection_outbox_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: projection_outbox projection_outbox_released_by_cutover_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_outbox
    ADD CONSTRAINT projection_outbox_released_by_cutover_id_fkey FOREIGN KEY (released_by_cutover_id) REFERENCES publication.projection_cutover(cutover_id) ON DELETE RESTRICT;


--
-- Name: projection_snapshot_time_repair projection_snapshot_time_repair_active_revision_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_snapshot_time_repair
    ADD CONSTRAINT projection_snapshot_time_repair_active_revision_id_fkey FOREIGN KEY (active_revision_id) REFERENCES publication.revision(revision_id) ON DELETE RESTRICT;


--
-- Name: projection_snapshot_time_repair projection_snapshot_time_repair_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.projection_snapshot_time_repair
    ADD CONSTRAINT projection_snapshot_time_repair_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.channel_snapshots(id) ON DELETE RESTRICT;


--
-- Name: quarantine quarantine_revision_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.quarantine
    ADD CONSTRAINT quarantine_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES publication.inbox(revision_id) ON DELETE RESTRICT;


--
-- Name: revision revision_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.revision
    ADD CONSTRAINT revision_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: revision revision_revision_id_fkey; Type: FK CONSTRAINT; Schema: publication; Owner: -
--

ALTER TABLE ONLY publication.revision
    ADD CONSTRAINT revision_revision_id_fkey FOREIGN KEY (revision_id) REFERENCES publication.inbox(revision_id) ON DELETE RESTRICT;


--
-- Name: agent_profiles agent_profiles_import_batch_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.agent_profiles
    ADD CONSTRAINT agent_profiles_import_batch_id_channel_id_fkey FOREIGN KEY (import_batch_id, channel_id) REFERENCES raw_crawler.channels(import_batch_id, channel_id);


--
-- Name: agent_profiles agent_profiles_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.agent_profiles
    ADD CONSTRAINT agent_profiles_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: channel_runs channel_runs_import_batch_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_runs
    ADD CONSTRAINT channel_runs_import_batch_id_channel_id_fkey FOREIGN KEY (import_batch_id, channel_id) REFERENCES raw_crawler.channels(import_batch_id, channel_id);


--
-- Name: channel_runs channel_runs_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_runs
    ADD CONSTRAINT channel_runs_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: channel_tab_pages channel_tab_pages_import_batch_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_tab_pages
    ADD CONSTRAINT channel_tab_pages_import_batch_id_channel_id_fkey FOREIGN KEY (import_batch_id, channel_id) REFERENCES raw_crawler.channels(import_batch_id, channel_id);


--
-- Name: channel_tab_pages channel_tab_pages_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_tab_pages
    ADD CONSTRAINT channel_tab_pages_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: channels channels_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channels
    ADD CONSTRAINT channels_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: content_candidates content_candidates_import_batch_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_candidates
    ADD CONSTRAINT content_candidates_import_batch_id_channel_id_fkey FOREIGN KEY (import_batch_id, channel_id) REFERENCES raw_crawler.channels(import_batch_id, channel_id);


--
-- Name: content_candidates content_candidates_import_batch_id_content_key_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_candidates
    ADD CONSTRAINT content_candidates_import_batch_id_content_key_fkey FOREIGN KEY (import_batch_id, content_key) REFERENCES raw_crawler.contents(import_batch_id, content_key) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: content_candidates content_candidates_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_candidates
    ADD CONSTRAINT content_candidates_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: content_candidates content_candidates_import_batch_id_run_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_candidates
    ADD CONSTRAINT content_candidates_import_batch_id_run_id_fkey FOREIGN KEY (import_batch_id, run_id) REFERENCES raw_crawler.channel_runs(import_batch_id, run_id);


--
-- Name: content_enrich_tasks content_enrich_tasks_import_batch_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_enrich_tasks
    ADD CONSTRAINT content_enrich_tasks_import_batch_id_channel_id_fkey FOREIGN KEY (import_batch_id, channel_id) REFERENCES raw_crawler.channels(import_batch_id, channel_id);


--
-- Name: content_enrich_tasks content_enrich_tasks_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_enrich_tasks
    ADD CONSTRAINT content_enrich_tasks_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: contents contents_import_batch_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.contents
    ADD CONSTRAINT contents_import_batch_id_channel_id_fkey FOREIGN KEY (import_batch_id, channel_id) REFERENCES raw_crawler.channels(import_batch_id, channel_id);


--
-- Name: contents contents_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.contents
    ADD CONSTRAINT contents_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: finalized_profiles finalized_profiles_import_batch_id_channel_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.finalized_profiles
    ADD CONSTRAINT finalized_profiles_import_batch_id_channel_id_fkey FOREIGN KEY (import_batch_id, channel_id) REFERENCES raw_crawler.channels(import_batch_id, channel_id);


--
-- Name: finalized_profiles finalized_profiles_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.finalized_profiles
    ADD CONSTRAINT finalized_profiles_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: query_pages query_pages_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_pages
    ADD CONSTRAINT query_pages_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: query_pages query_pages_import_batch_id_query_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_pages
    ADD CONSTRAINT query_pages_import_batch_id_query_id_fkey FOREIGN KEY (import_batch_id, query_id) REFERENCES raw_crawler.query_terms(import_batch_id, query_id);


--
-- Name: query_sets query_sets_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_sets
    ADD CONSTRAINT query_sets_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: query_terms query_terms_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_terms
    ADD CONSTRAINT query_terms_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: query_terms query_terms_import_batch_id_query_set_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.query_terms
    ADD CONSTRAINT query_terms_import_batch_id_query_set_id_fkey FOREIGN KEY (import_batch_id, query_set_id) REFERENCES raw_crawler.query_sets(import_batch_id, query_set_id);


--
-- Name: channel_tab_pages raw_channel_tab_pages_run_channel_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channel_tab_pages
    ADD CONSTRAINT raw_channel_tab_pages_run_channel_fkey FOREIGN KEY (import_batch_id, run_id, channel_id) REFERENCES raw_crawler.channel_runs(import_batch_id, run_id, channel_id);


--
-- Name: channels raw_channels_latest_run_channel_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.channels
    ADD CONSTRAINT raw_channels_latest_run_channel_fkey FOREIGN KEY (import_batch_id, latest_run_id, channel_id) REFERENCES raw_crawler.channel_runs(import_batch_id, run_id, channel_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: content_enrich_tasks raw_content_enrich_tasks_content_channel_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.content_enrich_tasks
    ADD CONSTRAINT raw_content_enrich_tasks_content_channel_fkey FOREIGN KEY (import_batch_id, content_key, channel_id) REFERENCES raw_crawler.contents(import_batch_id, content_key, channel_id);


--
-- Name: contents raw_contents_run_channel_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.contents
    ADD CONSTRAINT raw_contents_run_channel_fkey FOREIGN KEY (import_batch_id, run_id, channel_id) REFERENCES raw_crawler.channel_runs(import_batch_id, run_id, channel_id);


--
-- Name: finalized_profiles raw_finalized_profiles_run_channel_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.finalized_profiles
    ADD CONSTRAINT raw_finalized_profiles_run_channel_fkey FOREIGN KEY (import_batch_id, run_id, channel_id) REFERENCES raw_crawler.channel_runs(import_batch_id, run_id, channel_id);


--
-- Name: raw_objects raw_objects_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.raw_objects
    ADD CONSTRAINT raw_objects_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: youtube_api_batches youtube_api_batches_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.youtube_api_batches
    ADD CONSTRAINT youtube_api_batches_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: youtube_api_daily_usage youtube_api_daily_usage_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.youtube_api_daily_usage
    ADD CONSTRAINT youtube_api_daily_usage_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: youtube_api_tasks youtube_api_tasks_import_batch_id_fkey; Type: FK CONSTRAINT; Schema: raw_crawler; Owner: -
--

ALTER TABLE ONLY raw_crawler.youtube_api_tasks
    ADD CONSTRAINT youtube_api_tasks_import_batch_id_fkey FOREIGN KEY (import_batch_id) REFERENCES public.crawler_ingest_batches(import_batch_id) ON DELETE CASCADE;


--
-- Name: agent_current agent_current_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.agent_current
    ADD CONSTRAINT agent_current_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: content_current content_current_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.content_current
    ADD CONSTRAINT content_current_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: entity_current entity_current_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.entity_current
    ADD CONSTRAINT entity_current_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


--
-- Name: video_current video_current_publication_stream_id_fkey; Type: FK CONSTRAINT; Schema: result; Owner: -
--

ALTER TABLE ONLY result.video_current
    ADD CONSTRAINT video_current_publication_stream_id_fkey FOREIGN KEY (publication_stream_id) REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT;


INSERT INTO public.import_batches (
    id, source_file, source_sha256, captured_at, schema_version, raw_payload,
    parse_warnings, source_kind, status, row_counts
)
SELECT 'fresh-business-empty-v1', 'bootstrap://fresh-business-empty-v1',
       '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
       '1970-01-01T00:00:00Z'::timestamptz, 1, '{}'::jsonb, '[]'::jsonb,
       'derived_baseline', 'published', '{"channels":0}'::jsonb
WHERE NOT EXISTS (SELECT 1 FROM public.creator_search_releases);

INSERT INTO public.creator_search_releases (
    watermark, status, activated_at, generation, rebuilt_at, storage_mode, changed_channel_count
)
SELECT 'fresh-business-empty-v1', 'active', clock_timestamp(), 1,
       clock_timestamp(), 'shadow', 0
WHERE NOT EXISTS (SELECT 1 FROM public.creator_search_releases);

INSERT INTO public.creator_search_active (singleton, watermark)
SELECT true, 'fresh-business-empty-v1'
WHERE NOT EXISTS (SELECT 1 FROM public.creator_search_active)
  AND EXISTS (
      SELECT 1 FROM public.creator_search_releases
      WHERE watermark = 'fresh-business-empty-v1' AND status = 'active'
  );

INSERT INTO publication.creator_search_storage_state (
    singleton, write_mode, read_mode, initialized_watermark, initialized_row_count
)
SELECT true, 'shadow', 'legacy', active.watermark, 0
FROM public.creator_search_active AS active
WHERE active.singleton = true
  AND NOT EXISTS (SELECT 1 FROM publication.creator_search_storage_state);


CREATE TABLE publication.database_identity (
    singleton boolean DEFAULT true NOT NULL,
    database_kind text NOT NULL,
    database_name text NOT NULL,
    initialized_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT database_identity_kind_check CHECK ((database_kind = 'business'::text)),
    CONSTRAINT database_identity_singleton_check CHECK (singleton),
    CONSTRAINT database_identity_pkey PRIMARY KEY (singleton)
);

INSERT INTO publication.database_identity (singleton, database_kind, database_name)
VALUES (true, 'business', current_database());

--
-- PostgreSQL database dump complete
--

\unrestrict Sf4gQ0ucPgbD9EiBQYZfjZedS6nldo10cznALxvzrPwEuo798jq2FGMuR3WhMrH
