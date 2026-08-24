INSERT INTO public.content_type_taxonomy (
  source_content_type,content_kind,canonical_priority
) VALUES
  ('live','lives',1),
  ('short','shorts',2),
  ('video','videos',3)
ON CONFLICT (source_content_type,content_kind) DO UPDATE
SET canonical_priority=EXCLUDED.canonical_priority;

ALTER TABLE public.import_batches
DROP CONSTRAINT IF EXISTS import_batches_contract_shape;

ALTER TABLE public.import_batches
ADD CONSTRAINT import_batches_contract_shape CHECK (
  btrim(source_file) <> ''
  AND source_sha256 ~ '^[0-9a-f]{64}$'
  AND (source_byte_sha256 IS NULL OR source_byte_sha256 ~ '^[0-9a-f]{64}$')
  AND (source_byte_sha256 IS NULL) = (source_bytes IS NULL)
  AND (source_bytes IS NULL OR source_bytes > 0)
  AND schema_version > 0
  AND jsonb_typeof(raw_payload) = 'object'
  AND jsonb_typeof(parse_warnings) = 'array'
  AND jsonb_typeof(row_counts) = 'object'
  AND source_kind IN (
    'channel_json',
    'crawler_postgresql_dump',
    'derived_baseline',
    'public_browser_snapshot',
    'publication_projection'
  )
  AND status IN ('loading','published','failed')
  AND CASE
    WHEN status='failed' THEN NULLIF(btrim(error_message),'') IS NOT NULL
    ELSE error_message IS NULL
  END
);

ALTER TABLE publication.stream
ALTER COLUMN accepted_contract_versions SET DEFAULT ARRAY[1,2]::integer[];

UPDATE publication.stream stream
SET accepted_contract_versions=ARRAY(
  SELECT DISTINCT version
  FROM unnest(stream.accepted_contract_versions || ARRAY[1,2]::integer[]) version
  ORDER BY version
)
WHERE NOT accepted_contract_versions @> ARRAY[1,2]::integer[];

ALTER TABLE public.channel_snapshots
ADD COLUMN IF NOT EXISTS is_verified_status TEXT,
ADD COLUMN IF NOT EXISTS youtube_business_email_available BOOLEAN,
ADD COLUMN IF NOT EXISTS youtube_business_email_observed_at TIMESTAMPTZ;

ALTER TABLE public.channel_snapshots
ADD COLUMN IF NOT EXISTS channel_observed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS subscriber_count_observed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS total_view_count_observed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS video_count_observed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS publication.projection_snapshot_time_repair (
  snapshot_id TEXT PRIMARY KEY
    REFERENCES public.channel_snapshots(id) ON DELETE RESTRICT,
  channel_id TEXT NOT NULL,
  active_revision_id UUID
    REFERENCES publication.revision(revision_id) ON DELETE RESTRICT,
  snapshot_captured_at TIMESTAMPTZ NOT NULL,
  source_observed_at TIMESTAMPTZ NOT NULL,
  repair_version TEXT NOT NULL,
  repaired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    btrim(channel_id) <> ''
    AND btrim(repair_version) <> ''
    AND source_observed_at <= snapshot_captured_at
  )
);

DROP TABLE IF EXISTS pg_temp.business_projection_snapshot_times;
CREATE TEMP TABLE business_projection_snapshot_times ON COMMIT DROP AS
WITH RECURSIVE snapshot_chain AS (
  SELECT
    snapshot.id AS snapshot_id,
    snapshot.id AS source_snapshot_id,
    snapshot.channel_id,
    batch.source_kind AS snapshot_source_kind,
    batch.source_kind,
    snapshot.raw_channel,
    snapshot.captured_at AS snapshot_captured_at,
    snapshot.captured_at AS source_snapshot_captured_at,
    0 AS depth
  FROM public.channel_snapshots snapshot
  JOIN public.import_batches batch ON batch.id=snapshot.import_batch_id

  UNION ALL

  SELECT
    chain.snapshot_id,
    previous.id,
    chain.channel_id,
    chain.snapshot_source_kind,
    previous_batch.source_kind,
    previous.raw_channel,
    chain.snapshot_captured_at,
    previous.captured_at,
    chain.depth+1
  FROM snapshot_chain chain
  JOIN public.channel_snapshots previous
    ON previous.id=NULLIF(chain.raw_channel->>'carried_forward_from_snapshot_id','')
  JOIN public.import_batches previous_batch ON previous_batch.id=previous.import_batch_id
  WHERE chain.depth<100
    AND NULLIF(chain.raw_channel->>'active_revision_id','') IS NULL
    AND NOT (
      chain.raw_channel->>'adapter_version' IN (
        'business-publication-projection-v2',
        'business-publication-projection-v3'
      )
      AND NULLIF(chain.raw_channel->>'source_observed_at','') IS NOT NULL
    )
), resolved AS (
  SELECT DISTINCT ON (chain.snapshot_id)
    chain.snapshot_id,
    chain.channel_id,
    chain.snapshot_source_kind AS source_kind,
    revision.revision_id AS active_revision_id,
    chain.snapshot_captured_at,
    COALESCE(
      NULLIF(revision.source_json #>> '{complete_observation,observed_at}','')::timestamptz,
      CASE
        WHEN chain.raw_channel->>'adapter_version' IN (
          'business-publication-projection-v2',
          'business-publication-projection-v3'
        )
          THEN NULLIF(chain.raw_channel->>'source_observed_at','')::timestamptz
      END,
      CASE
        WHEN chain.source_kind<>'publication_projection'
          THEN chain.source_snapshot_captured_at
      END
    ) AS source_observed_at
  FROM snapshot_chain chain
  LEFT JOIN publication.revision revision
    ON revision.revision_id=NULLIF(chain.raw_channel->>'active_revision_id','')::uuid
   AND revision.channel_id=chain.channel_id
   AND revision.domain='channel'
  WHERE revision.revision_id IS NOT NULL
     OR (
       chain.raw_channel->>'adapter_version' IN (
         'business-publication-projection-v2',
         'business-publication-projection-v3'
       )
       AND NULLIF(chain.raw_channel->>'source_observed_at','') IS NOT NULL
     )
     OR chain.source_kind<>'publication_projection'
  ORDER BY chain.snapshot_id,chain.depth
)
SELECT * FROM resolved;

DO $snapshot_time_preflight$
BEGIN
  IF (SELECT count(*) FROM business_projection_snapshot_times)
       <> (SELECT count(*) FROM public.channel_snapshots)
     OR EXISTS (
    SELECT 1
    FROM business_projection_snapshot_times
    WHERE source_observed_at IS NULL
       OR source_observed_at > snapshot_captured_at
  ) THEN
    RAISE EXCEPTION 'Channel Snapshot observation time repair is incomplete or inverted';
  END IF;
END
$snapshot_time_preflight$;

INSERT INTO publication.projection_snapshot_time_repair (
  snapshot_id,channel_id,active_revision_id,snapshot_captured_at,
  source_observed_at,repair_version
)
SELECT
  snapshot_id,channel_id,active_revision_id,snapshot_captured_at,
  source_observed_at,'channel-observation-time-v2'
FROM business_projection_snapshot_times
WHERE source_kind='publication_projection'
ON CONFLICT (snapshot_id) DO UPDATE
SET channel_id=excluded.channel_id,
    active_revision_id=excluded.active_revision_id,
    snapshot_captured_at=excluded.snapshot_captured_at,
    source_observed_at=excluded.source_observed_at,
    repair_version=excluded.repair_version,
    repaired_at=now()
WHERE ROW(
  publication.projection_snapshot_time_repair.channel_id,
  publication.projection_snapshot_time_repair.active_revision_id,
  publication.projection_snapshot_time_repair.snapshot_captured_at,
  publication.projection_snapshot_time_repair.source_observed_at,
  publication.projection_snapshot_time_repair.repair_version
) IS DISTINCT FROM ROW(
  excluded.channel_id,
  excluded.active_revision_id,
  excluded.snapshot_captured_at,
  excluded.source_observed_at,
  excluded.repair_version
);

UPDATE public.channel_snapshots snapshot
SET channel_observed_at=source.source_observed_at,
    subscriber_count_observed_at=CASE
      WHEN snapshot.subscriber_count IS NULL THEN NULL ELSE source.source_observed_at END,
    total_view_count_observed_at=CASE
      WHEN snapshot.total_view_count IS NULL THEN NULL ELSE source.source_observed_at END,
    video_count_observed_at=CASE
      WHEN snapshot.video_count IS NULL THEN NULL ELSE source.source_observed_at END,
    raw_channel=CASE
      WHEN source.source_kind='publication_projection' THEN
        COALESCE(snapshot.raw_channel,'{}'::jsonb)||jsonb_build_object(
          'source_observed_at',source.source_observed_at,
          'projected_at',source.snapshot_captured_at,
          'timestamp_semantics','composite-capture-with-channel-observation-v2'
        )
      ELSE snapshot.raw_channel
    END
FROM business_projection_snapshot_times source
WHERE snapshot.id=source.snapshot_id
  AND (
    ROW(
      snapshot.channel_observed_at,
      snapshot.subscriber_count_observed_at,
      snapshot.total_view_count_observed_at,
      snapshot.video_count_observed_at
    ) IS DISTINCT FROM ROW(
      source.source_observed_at,
      CASE WHEN snapshot.subscriber_count IS NULL THEN NULL ELSE source.source_observed_at END,
      CASE WHEN snapshot.total_view_count IS NULL THEN NULL ELSE source.source_observed_at END,
      CASE WHEN snapshot.video_count IS NULL THEN NULL ELSE source.source_observed_at END
    )
    OR (
      source.source_kind='publication_projection'
      AND (
        NULLIF(snapshot.raw_channel->>'source_observed_at','')::timestamptz
          IS DISTINCT FROM source.source_observed_at
        OR NULLIF(snapshot.raw_channel->>'projected_at','')::timestamptz
          IS DISTINCT FROM source.snapshot_captured_at
        OR snapshot.raw_channel->>'timestamp_semantics'
          IS DISTINCT FROM 'composite-capture-with-channel-observation-v2'
      )
    )
  );

DO $channel_snapshot_metric_time_constraint$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.channel_snapshots'::regclass
      AND conname='channel_snapshot_metric_time_shape'
  ) THEN
    ALTER TABLE public.channel_snapshots
    ADD CONSTRAINT channel_snapshot_metric_time_shape CHECK (
      channel_observed_at IS NOT NULL
      AND channel_observed_at <= captured_at
      AND subscriber_count_observed_at IS NOT DISTINCT FROM
        CASE WHEN subscriber_count IS NULL THEN NULL ELSE channel_observed_at END
      AND total_view_count_observed_at IS NOT DISTINCT FROM
        CASE WHEN total_view_count IS NULL THEN NULL ELSE channel_observed_at END
      AND video_count_observed_at IS NOT DISTINCT FROM
        CASE WHEN video_count IS NULL THEN NULL ELSE channel_observed_at END
    ) NOT VALID;
  END IF;
END
$channel_snapshot_metric_time_constraint$;

ALTER TABLE public.channel_snapshots
VALIDATE CONSTRAINT channel_snapshot_metric_time_shape;

ALTER TABLE public.channel_snapshots
ALTER COLUMN channel_observed_at SET NOT NULL;

COMMENT ON CONSTRAINT channel_snapshot_metric_time_shape ON public.channel_snapshots IS
  'Channel aggregate metric timestamps are UTC source Observation times';

COMMENT ON COLUMN public.channel_snapshots.captured_at IS
  'UTC time when the composite Channel, Video, and Agent Snapshot was built';
COMMENT ON COLUMN public.channel_snapshots.channel_observed_at IS
  'UTC source Observation time for Channel identity and aggregate counts';
COMMENT ON COLUMN public.channel_snapshots.subscriber_count_observed_at IS
  'UTC source Observation time for subscriber_count';
COMMENT ON COLUMN public.channel_snapshots.total_view_count_observed_at IS
  'UTC source Observation time for total_view_count';
COMMENT ON COLUMN public.channel_snapshots.video_count_observed_at IS
  'UTC source Observation time for video_count';
COMMENT ON COLUMN public.channel_snapshots.youtube_business_email_available IS
  'Whether the latest successful YouTube About observation exposed a business email entry';
COMMENT ON COLUMN public.channel_snapshots.youtube_business_email_observed_at IS
  'UTC source About observation time for youtube_business_email_available';

ALTER TABLE public.channel_snapshots
DROP CONSTRAINT IF EXISTS channel_snapshots_youtube_business_email_shape;

ALTER TABLE public.channel_snapshots
ADD CONSTRAINT channel_snapshots_youtube_business_email_shape CHECK (
  (youtube_business_email_available IS NULL)
    = (youtube_business_email_observed_at IS NULL)
  AND (
    youtube_business_email_observed_at IS NULL
    OR youtube_business_email_observed_at <= captured_at
  )
);

UPDATE public.channel_snapshots
SET is_verified_status=CASE WHEN is_verified THEN 'verified' ELSE 'not_verified' END
WHERE is_verified_status IS NULL;

ALTER TABLE public.channel_snapshots
ALTER COLUMN is_verified DROP DEFAULT,
ALTER COLUMN is_verified DROP NOT NULL,
ALTER COLUMN is_verified_status SET DEFAULT 'unknown',
ALTER COLUMN is_verified_status SET NOT NULL;

ALTER TABLE public.channel_snapshots
DROP CONSTRAINT IF EXISTS channel_snapshots_verified_shape;

ALTER TABLE public.channel_snapshots
ADD CONSTRAINT channel_snapshots_verified_shape CHECK (
  (is_verified_status='verified' AND is_verified IS TRUE)
  OR (is_verified_status='not_verified' AND is_verified IS FALSE)
  OR (is_verified_status='unknown' AND is_verified IS NULL)
);

ALTER TABLE public.creator_search_current
ADD COLUMN IF NOT EXISTS verified_status TEXT,
ADD COLUMN IF NOT EXISTS channel_observed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS subscribers_observed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS total_views_observed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS channel_video_count_observed_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS youtube_business_email_available BOOLEAN,
ADD COLUMN IF NOT EXISTS youtube_business_email_observed_at TIMESTAMPTZ;

UPDATE public.creator_search_current search
SET channel_observed_at=snapshot.channel_observed_at,
    subscribers_observed_at=snapshot.subscriber_count_observed_at,
    total_views_observed_at=snapshot.total_view_count_observed_at,
    channel_video_count_observed_at=snapshot.video_count_observed_at,
    youtube_business_email_available=snapshot.youtube_business_email_available,
    youtube_business_email_observed_at=snapshot.youtube_business_email_observed_at
FROM public.channel_snapshots snapshot
WHERE snapshot.id=search.snapshot_id
  AND ROW(
    search.channel_observed_at,
    search.subscribers_observed_at,
    search.total_views_observed_at,
    search.channel_video_count_observed_at,
    search.youtube_business_email_available,
    search.youtube_business_email_observed_at
  ) IS DISTINCT FROM ROW(
    snapshot.channel_observed_at,
    snapshot.subscriber_count_observed_at,
    snapshot.total_view_count_observed_at,
    snapshot.video_count_observed_at,
    snapshot.youtube_business_email_available,
    snapshot.youtube_business_email_observed_at
  );

DO $creator_search_metric_time_constraint$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid='public.creator_search_current'::regclass
      AND conname='creator_search_metric_time_shape'
  ) THEN
    ALTER TABLE public.creator_search_current
    ADD CONSTRAINT creator_search_metric_time_shape CHECK (
      channel_observed_at IS NOT NULL
      AND channel_observed_at <= captured_at
      AND subscribers_observed_at IS NOT DISTINCT FROM
        CASE WHEN subscribers IS NULL THEN NULL ELSE channel_observed_at END
      AND total_views_observed_at IS NOT DISTINCT FROM
        CASE WHEN total_views IS NULL THEN NULL ELSE channel_observed_at END
      AND channel_video_count_observed_at IS NOT DISTINCT FROM
        CASE WHEN channel_video_count IS NULL THEN NULL ELSE channel_observed_at END
    ) NOT VALID;
  END IF;
END
$creator_search_metric_time_constraint$;

ALTER TABLE public.creator_search_current
VALIDATE CONSTRAINT creator_search_metric_time_shape;

ALTER TABLE public.creator_search_current
ALTER COLUMN channel_observed_at SET NOT NULL;

COMMENT ON CONSTRAINT creator_search_metric_time_shape ON public.creator_search_current IS
  'Channel aggregate metric timestamps are UTC source Observation times';

COMMENT ON COLUMN public.creator_search_current.captured_at IS
  'UTC time when the composite Business Projection Snapshot was built';
COMMENT ON COLUMN public.creator_search_current.channel_observed_at IS
  'UTC source Observation time for Channel identity and aggregate counts';
COMMENT ON COLUMN public.creator_search_current.subscribers_observed_at IS
  'UTC source Observation time for subscribers';
COMMENT ON COLUMN public.creator_search_current.total_views_observed_at IS
  'UTC source Observation time for total_views';
COMMENT ON COLUMN public.creator_search_current.channel_video_count_observed_at IS
  'UTC source Observation time for channel_video_count';
COMMENT ON COLUMN public.creator_search_current.youtube_business_email_available IS
  'Current business-query projection of the YouTube business email entry';
COMMENT ON COLUMN public.creator_search_current.youtube_business_email_observed_at IS
  'UTC source About observation time for youtube_business_email_available';

ALTER TABLE public.creator_search_current
DROP CONSTRAINT IF EXISTS creator_search_youtube_business_email_shape;

ALTER TABLE public.creator_search_current
ADD CONSTRAINT creator_search_youtube_business_email_shape CHECK (
  (youtube_business_email_available IS NULL)
    = (youtube_business_email_observed_at IS NULL)
  AND (
    youtube_business_email_observed_at IS NULL
    OR youtube_business_email_observed_at <= captured_at
  )
);

CREATE OR REPLACE FUNCTION public.normalize_creator_search_channel_observation_times()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path TO public,pg_temp
AS $search_channel_observation_times$
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
$search_channel_observation_times$;

DROP TRIGGER IF EXISTS trg_creator_search_channel_observation_times
ON public.creator_search_current;
CREATE TRIGGER trg_creator_search_channel_observation_times
BEFORE INSERT OR UPDATE OF
  snapshot_id,channel_id,channel_observed_at,subscribers_observed_at,
  total_views_observed_at,channel_video_count_observed_at,
  youtube_business_email_available,youtube_business_email_observed_at
ON public.creator_search_current
FOR EACH ROW
EXECUTE FUNCTION public.normalize_creator_search_channel_observation_times();

UPDATE public.creator_search_current
SET verified_status=CASE WHEN verified THEN 'verified' ELSE 'not_verified' END
WHERE verified_status IS NULL;

ALTER TABLE public.creator_search_current
ALTER COLUMN verified DROP DEFAULT,
ALTER COLUMN verified DROP NOT NULL,
ALTER COLUMN verified_status SET DEFAULT 'unknown',
ALTER COLUMN verified_status SET NOT NULL;

ALTER TABLE public.creator_search_current
DROP CONSTRAINT IF EXISTS creator_search_current_verified_shape;

ALTER TABLE public.creator_search_current
ADD CONSTRAINT creator_search_current_verified_shape CHECK (
  (verified_status='verified' AND verified IS TRUE)
  OR (verified_status='not_verified' AND verified IS FALSE)
  OR (verified_status='unknown' AND verified IS NULL)
);

CREATE OR REPLACE FUNCTION public.normalize_channel_snapshot_verified_status()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path TO public,pg_temp
AS $verified_snapshot$
BEGIN
  IF NEW.is_verified_status='unknown' AND NEW.is_verified IS NOT NULL THEN
    NEW.is_verified_status=CASE WHEN NEW.is_verified THEN 'verified' ELSE 'not_verified' END;
  END IF;
  RETURN NEW;
END
$verified_snapshot$;

DROP TRIGGER IF EXISTS trg_channel_snapshot_verified_status ON public.channel_snapshots;
CREATE TRIGGER trg_channel_snapshot_verified_status
BEFORE INSERT OR UPDATE OF is_verified,is_verified_status ON public.channel_snapshots
FOR EACH ROW EXECUTE FUNCTION public.normalize_channel_snapshot_verified_status();

CREATE OR REPLACE FUNCTION public.normalize_creator_search_verified_status()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path TO public,pg_temp
AS $verified_search$
BEGIN
  IF NEW.verified_status='unknown' AND NEW.verified IS NOT NULL THEN
    NEW.verified_status=CASE WHEN NEW.verified THEN 'verified' ELSE 'not_verified' END;
  END IF;
  RETURN NEW;
END
$verified_search$;

DROP TRIGGER IF EXISTS trg_creator_search_verified_status ON public.creator_search_current;
CREATE TRIGGER trg_creator_search_verified_status
BEFORE INSERT OR UPDATE OF verified,verified_status ON public.creator_search_current
FOR EACH ROW EXECUTE FUNCTION public.normalize_creator_search_verified_status();

CREATE TABLE IF NOT EXISTS publication.projection_batch (
  batch_id TEXT PRIMARY KEY,
  source_sha256 TEXT NOT NULL UNIQUE CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  adapter_version TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('loading','published')),
  version_vectors JSONB NOT NULL,
  upsert_channel_ids TEXT[] NOT NULL DEFAULT '{}',
  removed_channel_ids TEXT[] NOT NULL DEFAULT '{}',
  projection_count INTEGER NOT NULL CHECK (projection_count > 0),
  previous_watermark TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  CHECK (btrim(batch_id) <> '' AND btrim(adapter_version) <> ''),
  CHECK (jsonb_typeof(version_vectors)='object'),
  CHECK (
    projection_count=cardinality(upsert_channel_ids)+cardinality(removed_channel_ids)
  ),
  CHECK (NOT upsert_channel_ids && removed_channel_ids),
  CHECK (
    (status='published' AND published_at IS NOT NULL)
    OR (status='loading' AND published_at IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS publication.projection_batch_item (
  batch_id TEXT NOT NULL
    REFERENCES publication.projection_batch(batch_id) ON DELETE RESTRICT,
  channel_id TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('upsert','remove')),
  snapshot_id TEXT,
  previous_snapshot_id TEXT,
  version_vector JSONB NOT NULL,
  projection_hash TEXT NOT NULL CHECK (projection_hash ~ '^sha256:[0-9a-f]{64}$'),
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (batch_id,channel_id),
  CHECK (btrim(channel_id) <> '' AND jsonb_typeof(version_vector)='object'),
  CHECK (
    (action='upsert' AND snapshot_id IS NOT NULL AND btrim(snapshot_id) <> '')
    OR (action='remove' AND snapshot_id IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_business_projection_batch_item_channel
ON publication.projection_batch_item (channel_id,projected_at,batch_id);

CREATE TABLE IF NOT EXISTS publication.projection_cutover (
  cutover_id TEXT PRIMARY KEY,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  evidence_format TEXT NOT NULL,
  evidence_sha256 TEXT NOT NULL UNIQUE CHECK (evidence_sha256 ~ '^sha256:[0-9a-f]{64}$'),
  cohort_hash TEXT NOT NULL CHECK (cohort_hash ~ '^sha256:[0-9a-f]{64}$'),
  channel_ids TEXT[] NOT NULL,
  owner_count INTEGER NOT NULL CHECK (owner_count > 0),
  released_projection_count INTEGER NOT NULL CHECK (released_projection_count >= 0),
  previous_watermark TEXT NOT NULL,
  first_projection_watermark TEXT,
  last_projection_watermark TEXT,
  status TEXT NOT NULL CHECK (status IN ('applied','rolled_back')),
  evidence_json JSONB NOT NULL,
  applied_by TEXT NOT NULL,
  applied_reason TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  rolled_back_by TEXT,
  rolled_back_reason TEXT,
  rolled_back_at TIMESTAMPTZ,
  rollback_json JSONB,
  CHECK (
    btrim(cutover_id) <> ''
    AND btrim(evidence_format) <> ''
    AND jsonb_typeof(evidence_json)='object'
    AND btrim(applied_by) <> ''
    AND btrim(applied_reason) <> ''
    AND owner_count=cardinality(channel_ids)
  ),
  CHECK (
    (status='applied' AND rolled_back_by IS NULL
      AND rolled_back_reason IS NULL AND rolled_back_at IS NULL
      AND rollback_json IS NULL)
    OR (status='rolled_back' AND btrim(rolled_back_by) <> ''
      AND btrim(rolled_back_reason) <> '' AND rolled_back_at IS NOT NULL
      AND jsonb_typeof(rollback_json)='object')
  )
);

ALTER TABLE publication.projection_cutover
ADD COLUMN IF NOT EXISTS last_projection_watermark TEXT;

ALTER TABLE publication.projection_cutover
ADD COLUMN IF NOT EXISTS rollback_json JSONB;

ALTER TABLE publication.projection_outbox
ADD COLUMN IF NOT EXISTS released_by_cutover_id TEXT
  REFERENCES publication.projection_cutover(cutover_id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_business_projection_outbox_cutover
ON publication.projection_outbox (released_by_cutover_id)
WHERE released_by_cutover_id IS NOT NULL;

CREATE OR REPLACE FUNCTION public.refresh_creator_search_release_v8(
  p_watermark TEXT,
  p_upsert_channel_ids TEXT[],
  p_removed_channel_ids TEXT[]
) RETURNS void
LANGUAGE plpgsql
SET search_path TO public,pg_temp
AS $projection_search$
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
$projection_search$;
