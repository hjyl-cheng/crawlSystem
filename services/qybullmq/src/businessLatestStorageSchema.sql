-- Additive migration. Installing it does not change production write mode.
CREATE TABLE IF NOT EXISTS publication.business_storage_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
  mode TEXT NOT NULL DEFAULT 'snapshots' CHECK (mode IN ('snapshots','latest')),
  activated_at TIMESTAMPTZ,
  actor TEXT,
  reason TEXT
);
INSERT INTO publication.business_storage_state(singleton) VALUES (true) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS publication.latest_projection_state (
  channel_id TEXT PRIMARY KEY,
  snapshot_id TEXT,
  batch_id TEXT NOT NULL,
  version_vector JSONB NOT NULL CHECK (jsonb_typeof(version_vector)='object'),
  projection_hash TEXT NOT NULL,
  is_removed BOOLEAN NOT NULL DEFAULT false,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CHECK (is_removed = (snapshot_id IS NULL))
);
CREATE TABLE IF NOT EXISTS publication.channel_metric_history (
  channel_id TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  subscriber_count BIGINT,
  subscriber_count_status TEXT,
  total_view_count BIGINT,
  total_view_count_status TEXT,
  PRIMARY KEY(channel_id,observed_at)
);

-- Readers can use this before or after adoption. Old snapshots stay untouched
-- until an explicitly reviewed cleanup. No full video/profile copies for trends.
CREATE OR REPLACE FUNCTION public.creator_channel_metric_history_v1(
  p_channel_id TEXT,p_as_of TIMESTAMPTZ,p_limit INTEGER DEFAULT 100
) RETURNS TABLE(captured_at TIMESTAMPTZ,subscriber_count BIGINT,
  subscriber_count_status TEXT,total_view_count BIGINT,total_view_count_status TEXT)
LANGUAGE SQL STABLE SET search_path=public,publication,pg_temp AS $$
  SELECT * FROM (
    SELECT DISTINCT ON (point.captured_at) point.captured_at,point.subscriber_count,
      point.subscriber_count_status,point.total_view_count,point.total_view_count_status
    FROM (
      (SELECT observed_at AS captured_at,subscriber_count,subscriber_count_status,
        total_view_count,total_view_count_status,0 AS priority
       FROM publication.channel_metric_history
       WHERE channel_id=p_channel_id AND observed_at<=p_as_of
       ORDER BY observed_at DESC LIMIT LEAST(GREATEST(p_limit,1),100))
      UNION ALL
      (SELECT COALESCE(channel_observed_at,captured_at),subscriber_count,subscriber_count_status,
        total_view_count,total_view_count_status,1 AS priority
       FROM public.channel_snapshots
       WHERE channel_id=p_channel_id AND captured_at<=p_as_of
       ORDER BY captured_at DESC,id DESC LIMIT LEAST(GREATEST(p_limit,1),100))
    ) point ORDER BY point.captured_at DESC,point.priority
    LIMIT LEAST(GREATEST(p_limit,1),100)
  ) recent ORDER BY captured_at;
$$;

-- Fail closed if an older publisher runs after activation. It must not silently
-- resume writing historical copies or roll Search back to mutable records.
CREATE OR REPLACE FUNCTION publication.guard_latest_business_writer() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path=public,publication,pg_temp AS $$
BEGIN
  IF (SELECT mode FROM publication.business_storage_state WHERE singleton)='latest'
     AND current_setting('publication.business_writer_mode',true) IS DISTINCT FROM 'latest' THEN
    RAISE EXCEPTION 'BUSINESS_LATEST_WRITER_REQUIRED';
  END IF;
  RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS business_latest_snapshot_writer ON public.channel_snapshots;
CREATE TRIGGER business_latest_snapshot_writer BEFORE INSERT OR UPDATE OR DELETE
  ON public.channel_snapshots FOR EACH STATEMENT EXECUTE FUNCTION publication.guard_latest_business_writer();
DROP TRIGGER IF EXISTS business_latest_search_writer ON public.creator_search_live;
CREATE TRIGGER business_latest_search_writer BEFORE INSERT OR UPDATE OR DELETE
  ON public.creator_search_live FOR EACH STATEMENT EXECUTE FUNCTION publication.guard_latest_business_writer();

-- Reusing a stable snapshot changes the rollback contract. Explicit old-release
-- replay is rejected after activation; transactional failures still roll back.
DO $guard_old_replay$
DECLARE item RECORD; definition TEXT;
BEGIN
  FOR item IN SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='public' AND p.proname IN (
      'rollback_creator_search_release_v9','replay_creator_search_release_v9',
      'restore_creator_search_live_from_legacy_v1','rollback_creator_search_incremental_storage_v1')
  LOOP
    definition := pg_get_functiondef(item.oid);
    IF position('BUSINESS_LATEST_HISTORICAL_REPLAY_DISABLED' in definition)=0 THEN
      definition := regexp_replace(definition,'\mBEGIN\M',
        E'BEGIN\n  IF (SELECT mode FROM publication.business_storage_state WHERE singleton)=\x27latest\x27 THEN\n    RAISE EXCEPTION \x27BUSINESS_LATEST_HISTORICAL_REPLAY_DISABLED\x27;\n  END IF;', 'i');
      EXECUTE definition;
    END IF;
  END LOOP;
END;
$guard_old_replay$;

-- Grant trend access only to existing readers of the same channel data.
DO $history_readers$
DECLARE reader RECORD;
BEGIN
  FOR reader IN SELECT rolname FROM pg_roles
    WHERE NOT rolsuper AND rolname !~ '^pg_'
      AND has_table_privilege(oid,'public.channel_snapshots','SELECT')
  LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA publication TO %I',reader.rolname);
    EXECUTE format('GRANT SELECT ON publication.channel_metric_history TO %I',reader.rolname);
  END LOOP;
END;
$history_readers$;

DO $latest_writer_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='business_publication_projector') THEN
    GRANT SELECT ON publication.business_storage_state TO business_publication_projector;
    GRANT SELECT,INSERT,UPDATE ON publication.latest_projection_state TO business_publication_projector;
    GRANT SELECT,INSERT,DELETE ON publication.channel_metric_history TO business_publication_projector;
    GRANT UPDATE ON public.channel_snapshots,public.content_snapshots TO business_publication_projector;
    GRANT UPDATE,DELETE ON public.channel_links,public.channel_profile_facts,public.channel_metric_values TO business_publication_projector;
  END IF;
END;
$latest_writer_grants$;
