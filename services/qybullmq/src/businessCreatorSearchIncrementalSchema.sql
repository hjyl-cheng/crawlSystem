CREATE TABLE IF NOT EXISTS public.creator_search_live (
  LIKE public.creator_search_current
    INCLUDING DEFAULTS
    INCLUDING CONSTRAINTS
    INCLUDING STORAGE
    INCLUDING COMMENTS
);

DO $creator_search_live_constraints$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.creator_search_live'::regclass
      AND contype='p'
  ) THEN
    ALTER TABLE public.creator_search_live
    ADD CONSTRAINT creator_search_live_pkey PRIMARY KEY (channel_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.creator_search_live'::regclass
      AND conname='creator_search_live_snapshot_id_key'
  ) THEN
    ALTER TABLE public.creator_search_live
    ADD CONSTRAINT creator_search_live_snapshot_id_key UNIQUE (snapshot_id);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.creator_search_live'::regclass
      AND conname='creator_search_live_channel_id_fkey'
  ) THEN
    ALTER TABLE public.creator_search_live
    ADD CONSTRAINT creator_search_live_channel_id_fkey
      FOREIGN KEY (channel_id) REFERENCES public.channels(channel_id) ON DELETE CASCADE;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.creator_search_live'::regclass
      AND conname='creator_search_live_snapshot_channel_fkey'
  ) THEN
    ALTER TABLE public.creator_search_live
    ADD CONSTRAINT creator_search_live_snapshot_channel_fkey
      FOREIGN KEY (snapshot_id,channel_id)
      REFERENCES public.channel_snapshots(id,channel_id) ON DELETE CASCADE;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.creator_search_live'::regclass
      AND conname='creator_search_live_watermark_fkey'
  ) THEN
    ALTER TABLE public.creator_search_live
    ADD CONSTRAINT creator_search_live_watermark_fkey
      FOREIGN KEY (watermark) REFERENCES public.creator_search_releases(watermark)
      ON DELETE RESTRICT;
  END IF;
END
$creator_search_live_constraints$;

CREATE INDEX IF NOT EXISTS idx_creator_search_live_country_language
ON public.creator_search_live (country,language,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_subscribers
ON public.creator_search_live (subscribers DESC NULLS LAST,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_avg_views
ON public.creator_search_live (avg_views DESC NULLS LAST,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_engagement
ON public.creator_search_live (engagement_rate_by_views DESC NULLS LAST,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_last_published_date
ON public.creator_search_live (last_published_date DESC NULLS LAST,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_audience_country
ON public.creator_search_live (audience_country,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_audience_language
ON public.creator_search_live (audience_language,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_audience_gender
ON public.creator_search_live (audience_gender,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_audience_age
ON public.creator_search_live (audience_age,channel_id);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_categories
ON public.creator_search_live USING gin (category_paths);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_tags
ON public.creator_search_live USING gin (tags);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_contacts
ON public.creator_search_live USING gin (contact_types);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_text
ON public.creator_search_live USING gin (search_text gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_creator_search_live_watermark
ON public.creator_search_live (watermark,channel_id);

DROP TRIGGER IF EXISTS trg_creator_search_live_channel_observation_times
ON public.creator_search_live;
CREATE TRIGGER trg_creator_search_live_channel_observation_times
BEFORE INSERT OR UPDATE OF
  snapshot_id,channel_id,channel_observed_at,subscribers_observed_at,
  total_views_observed_at,channel_video_count_observed_at,
  youtube_business_email_available,youtube_business_email_observed_at
ON public.creator_search_live
FOR EACH ROW
EXECUTE FUNCTION public.normalize_creator_search_channel_observation_times();

DROP TRIGGER IF EXISTS trg_creator_search_live_verified_status
ON public.creator_search_live;
CREATE TRIGGER trg_creator_search_live_verified_status
BEFORE INSERT OR UPDATE OF verified,verified_status
ON public.creator_search_live
FOR EACH ROW
EXECUTE FUNCTION public.normalize_creator_search_verified_status();

INSERT INTO public.import_batches (
  id,source_file,source_sha256,captured_at,schema_version,raw_payload,
  parse_warnings,source_kind,status,row_counts
)
SELECT 'fresh-business-empty-v1','bootstrap://fresh-business-empty-v1',
       '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a',
       '1970-01-01T00:00:00Z'::timestamptz,1,'{}'::jsonb,'[]'::jsonb,
       'derived_baseline','published','{"channels":0}'::jsonb
WHERE NOT EXISTS (SELECT 1 FROM public.creator_search_releases)
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.creator_search_releases (
  watermark,status,activated_at,generation,rebuilt_at,storage_mode,changed_channel_count
)
SELECT 'fresh-business-empty-v1','active',clock_timestamp(),1,clock_timestamp(),'shadow',0
WHERE NOT EXISTS (SELECT 1 FROM public.creator_search_releases)
ON CONFLICT (watermark) DO NOTHING;

INSERT INTO public.creator_search_active (singleton,watermark)
SELECT true,'fresh-business-empty-v1'
WHERE NOT EXISTS (SELECT 1 FROM public.creator_search_active)
  AND EXISTS (
    SELECT 1 FROM public.creator_search_releases
    WHERE watermark='fresh-business-empty-v1' AND status='active'
  )
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS publication.creator_search_storage_state (
  singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
  write_mode TEXT NOT NULL DEFAULT 'shadow'
    CHECK (write_mode IN ('shadow','incremental')),
  read_mode TEXT NOT NULL DEFAULT 'legacy'
    CHECK (read_mode IN ('legacy','live')),
  initialized_watermark TEXT NOT NULL
    REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT,
  initialized_row_count INTEGER NOT NULL CHECK (initialized_row_count>=0),
  initialized_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  cutover_actor TEXT,
  cutover_reason TEXT,
  cutover_at TIMESTAMPTZ,
  storage_rollback_actor TEXT,
  storage_rollback_reason TEXT,
  storage_rollback_at TIMESTAMPTZ,
  CHECK (write_mode='shadow' OR read_mode='live')
);

ALTER TABLE publication.creator_search_storage_state
ADD COLUMN IF NOT EXISTS initialized_row_count INTEGER,
ADD COLUMN IF NOT EXISTS cutover_actor TEXT,
ADD COLUMN IF NOT EXISTS cutover_reason TEXT,
ADD COLUMN IF NOT EXISTS cutover_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS storage_rollback_actor TEXT,
ADD COLUMN IF NOT EXISTS storage_rollback_reason TEXT,
ADD COLUMN IF NOT EXISTS storage_rollback_at TIMESTAMPTZ;

UPDATE publication.creator_search_storage_state state
SET initialized_row_count=(
  SELECT count(*)::int
  FROM public.creator_search_current search
  WHERE search.watermark=state.initialized_watermark
)
WHERE state.initialized_row_count IS NULL;

ALTER TABLE publication.creator_search_storage_state
ALTER COLUMN initialized_row_count SET NOT NULL;

DO $creator_search_storage_state_constraints$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='publication.creator_search_storage_state'::regclass
      AND conname='creator_search_storage_initialized_row_count_check'
  ) THEN
    ALTER TABLE publication.creator_search_storage_state
    ADD CONSTRAINT creator_search_storage_initialized_row_count_check
      CHECK (initialized_row_count>=0);
  END IF;
END
$creator_search_storage_state_constraints$;

INSERT INTO publication.creator_search_storage_state (
  singleton,write_mode,read_mode,initialized_watermark,initialized_row_count
)
SELECT true,'shadow','legacy',active.watermark,
       COALESCE((SELECT count(*)::int FROM public.creator_search_current search
                 WHERE search.watermark=active.watermark),0)
FROM public.creator_search_active active
WHERE active.singleton=true
ON CONFLICT (singleton) DO NOTHING;

INSERT INTO public.creator_search_live
SELECT search.*
FROM public.creator_search_active active
JOIN public.creator_search_current search ON search.watermark=active.watermark
JOIN publication.creator_search_storage_state state ON state.singleton=true
WHERE active.singleton=true AND state.write_mode='shadow'
ON CONFLICT (channel_id) DO NOTHING;

ALTER TABLE public.creator_search_releases
ADD COLUMN IF NOT EXISTS previous_watermark TEXT,
ADD COLUMN IF NOT EXISTS storage_mode TEXT,
ADD COLUMN IF NOT EXISTS changed_channel_count INTEGER,
ADD COLUMN IF NOT EXISTS rolled_back_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS rollback_actor TEXT,
ADD COLUMN IF NOT EXISTS rollback_reason TEXT;

CREATE TABLE IF NOT EXISTS publication.creator_search_changes (
  watermark TEXT NOT NULL
    REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT,
  channel_id TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('upsert','remove')),
  before_document JSONB,
  after_document JSONB,
  changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (watermark,channel_id),
  CHECK (btrim(channel_id) <> ''),
  CHECK (before_document IS NULL OR jsonb_typeof(before_document)='object'),
  CHECK (after_document IS NULL OR jsonb_typeof(after_document)='object'),
  CHECK (
    (action='upsert' AND after_document IS NOT NULL)
    OR (action='remove' AND after_document IS NULL AND before_document IS NOT NULL)
  )
);

CREATE TABLE IF NOT EXISTS publication.creator_search_legacy_prune_audit (
  prune_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  active_watermark TEXT NOT NULL
    REFERENCES public.creator_search_releases(watermark) ON DELETE RESTRICT,
  retained_watermarks TEXT[] NOT NULL CHECK (cardinality(retained_watermarks)>0),
  deleted_row_count INTEGER NOT NULL CHECK (deleted_row_count>=0),
  pruned_by TEXT NOT NULL CHECK (btrim(pruned_by)<>''),
  prune_reason TEXT NOT NULL CHECK (btrim(prune_reason)<>''),
  pruned_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_creator_search_changes_channel
ON publication.creator_search_changes (channel_id,changed_at DESC,watermark);

CREATE OR REPLACE FUNCTION public.restore_creator_search_live_from_legacy_v1(
  p_watermark TEXT
) RETURNS INTEGER
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_restore_live$
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
$creator_search_restore_live$;

CREATE OR REPLACE FUNCTION public.prune_creator_search_legacy_history_v1(
  p_expected_active_watermark TEXT,
  p_expected_delete_row_count INTEGER,
  p_actor TEXT,
  p_reason TEXT
) RETURNS INTEGER
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_prune_legacy$
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
$creator_search_prune_legacy$;

CREATE OR REPLACE FUNCTION public.refresh_creator_search_release_v9(
  p_watermark TEXT,
  p_upsert_channel_ids TEXT[],
  p_removed_channel_ids TEXT[]
) RETURNS void
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_incremental$
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
$creator_search_incremental$;

CREATE OR REPLACE FUNCTION public.activate_creator_search_incremental_v1(
  p_expected_watermark TEXT,
  p_expected_live_count INTEGER,
  p_actor TEXT,
  p_reason TEXT
) RETURNS TEXT
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_storage_cutover$
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
$creator_search_storage_cutover$;

CREATE OR REPLACE FUNCTION public.rollback_creator_search_release_v9(
  p_watermark TEXT,
  p_actor TEXT,
  p_reason TEXT
) RETURNS TEXT
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_rollback$
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
$creator_search_rollback$;

CREATE OR REPLACE FUNCTION public.replay_creator_search_release_v9(
  p_watermark TEXT
) RETURNS TEXT
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_replay$
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
$creator_search_replay$;

CREATE OR REPLACE FUNCTION public.rollback_creator_search_to_watermark_v1(
  p_target_watermark TEXT,
  p_actor TEXT,
  p_reason TEXT
) RETURNS INTEGER
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_rollback_chain$
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
$creator_search_rollback_chain$;

CREATE OR REPLACE FUNCTION public.rollback_creator_search_incremental_storage_v1(
  p_target_watermark TEXT,
  p_expected_live_count INTEGER,
  p_actor TEXT,
  p_reason TEXT
) RETURNS INTEGER
LANGUAGE plpgsql
SET search_path TO public,publication,pg_temp
AS $creator_search_storage_rollback$
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
$creator_search_storage_rollback$;

COMMENT ON TABLE public.creator_search_live IS
  'One directly queryable current Creator Search row per Channel';
COMMENT ON TABLE publication.creator_search_changes IS
  'Exact before and after Creator Search state for each changed Channel and release';

REVOKE ALL ON FUNCTION public.refresh_creator_search_release_v9(TEXT,TEXT[],TEXT[])
FROM PUBLIC;
REVOKE ALL ON FUNCTION public.restore_creator_search_live_from_legacy_v1(TEXT)
FROM PUBLIC;
REVOKE ALL ON FUNCTION public.prune_creator_search_legacy_history_v1(TEXT,INTEGER,TEXT,TEXT)
FROM PUBLIC;
REVOKE ALL ON FUNCTION public.rollback_creator_search_release_v9(TEXT,TEXT,TEXT)
FROM PUBLIC;
REVOKE ALL ON FUNCTION public.replay_creator_search_release_v9(TEXT)
FROM PUBLIC;
REVOKE ALL ON FUNCTION public.activate_creator_search_incremental_v1(TEXT,INTEGER,TEXT,TEXT)
FROM PUBLIC;
REVOKE ALL ON FUNCTION public.rollback_creator_search_to_watermark_v1(TEXT,TEXT,TEXT)
FROM PUBLIC;
REVOKE ALL ON FUNCTION
  public.rollback_creator_search_incremental_storage_v1(TEXT,INTEGER,TEXT,TEXT)
FROM PUBLIC;

DO $creator_search_incremental_grants$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='business_publication_projector') THEN
    GRANT SELECT,INSERT,UPDATE,DELETE ON public.creator_search_active
      TO business_publication_projector;
    GRANT SELECT,INSERT,DELETE ON public.creator_search_live
      TO business_publication_projector;
    GRANT SELECT,INSERT ON publication.creator_search_changes
      TO business_publication_projector;
    GRANT SELECT,UPDATE ON publication.creator_search_storage_state
      TO business_publication_projector;
    GRANT EXECUTE ON FUNCTION
      public.refresh_creator_search_release_v9(TEXT,TEXT[],TEXT[])
      TO business_publication_projector;
    GRANT EXECUTE ON FUNCTION public.restore_creator_search_live_from_legacy_v1(TEXT)
      TO business_publication_projector;
    GRANT EXECUTE ON FUNCTION public.replay_creator_search_release_v9(TEXT)
      TO business_publication_projector;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='yewu_test_reader') THEN
    GRANT SELECT,INSERT,UPDATE,DELETE ON public.creator_search_live
      TO yewu_test_reader;
    GRANT SELECT ON publication.creator_search_changes,
      publication.creator_search_storage_state TO yewu_test_reader;
  END IF;
END
$creator_search_incremental_grants$;
