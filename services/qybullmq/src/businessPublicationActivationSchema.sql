CREATE SCHEMA IF NOT EXISTS result;

ALTER TABLE publication.channel_ownership
ADD COLUMN IF NOT EXISTS projection_mode TEXT NOT NULL DEFAULT 'held_shadow';

DO $activation_schema$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname='chk_business_publication_projection_mode'
      AND conrelid='publication.channel_ownership'::regclass
  ) THEN
    ALTER TABLE publication.channel_ownership
    ADD CONSTRAINT chk_business_publication_projection_mode
    CHECK (projection_mode IN ('held_shadow','online'));
  END IF;
END
$activation_schema$;

CREATE TABLE IF NOT EXISTS publication.consumer_cursor (
  channel_id TEXT NOT NULL,
  domain TEXT NOT NULL CHECK (domain IN ('channel','video','agent')),
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  active_sequence BIGINT NOT NULL CHECK (active_sequence >= 0),
  active_revision_id UUID NOT NULL UNIQUE,
  active_result_hash TEXT NOT NULL
    CHECK (active_result_hash ~ '^sha256:[0-9a-f]{64}$'),
  activated_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel_id,domain),
  CHECK (btrim(channel_id) <> '')
);

CREATE TABLE IF NOT EXISTS publication.activation (
  activation_id UUID PRIMARY KEY,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  channel_id TEXT NOT NULL,
  ownership_reference JSONB NOT NULL,
  before_version_vector JSONB NOT NULL,
  after_version_vector JSONB NOT NULL,
  revision_count INTEGER NOT NULL CHECK (revision_count > 0),
  projection_mode TEXT NOT NULL CHECK (projection_mode IN ('held_shadow','online')),
  actor TEXT NOT NULL,
  reason TEXT NOT NULL,
  activated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(ownership_reference) = 'object'),
  CHECK (jsonb_typeof(before_version_vector) = 'object'),
  CHECK (jsonb_typeof(after_version_vector) = 'object'),
  CHECK (btrim(actor) <> '' AND btrim(reason) <> '')
);

CREATE INDEX IF NOT EXISTS idx_business_publication_activation_channel
ON publication.activation (channel_id,activated_at,activation_id);

CREATE TABLE IF NOT EXISTS publication.activation_item (
  activation_id UUID NOT NULL
    REFERENCES publication.activation(activation_id) ON DELETE RESTRICT,
  revision_id UUID NOT NULL UNIQUE
    REFERENCES publication.revision(revision_id) ON DELETE RESTRICT,
  domain TEXT NOT NULL CHECK (domain IN ('channel','video','agent')),
  previous_sequence BIGINT,
  active_sequence BIGINT NOT NULL CHECK (active_sequence > 0),
  previous_result_hash TEXT,
  active_result_hash TEXT NOT NULL,
  outcome TEXT NOT NULL DEFAULT 'applied' CHECK (outcome='applied'),
  activated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (activation_id,revision_id),
  CHECK (previous_sequence IS NULL OR previous_sequence >= 0),
  CHECK (previous_result_hash IS NULL OR previous_result_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (active_result_hash ~ '^sha256:[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS publication.projection_outbox (
  projection_id UUID PRIMARY KEY,
  activation_id UUID NOT NULL UNIQUE
    REFERENCES publication.activation(activation_id) ON DELETE RESTRICT,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  channel_id TEXT NOT NULL,
  version_vector JSONB NOT NULL,
  status TEXT NOT NULL
    CHECK (status IN (
      'held_shadow','pending','leased','retry_wait','delivered','dead_letter'
    )),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  last_error TEXT,
  delivered_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(version_vector) = 'object'),
  CHECK (
    (status='leased' AND btrim(lease_owner) <> '' AND lease_expires_at IS NOT NULL)
    OR (status<>'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
  ),
  CHECK (
    (status='delivered' AND delivered_at IS NOT NULL)
    OR (status<>'delivered' AND delivered_at IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_business_publication_projection_claim
ON publication.projection_outbox (status,next_attempt_at,created_at)
WHERE status IN ('pending','leased','retry_wait');

CREATE INDEX IF NOT EXISTS idx_business_publication_projection_predecessor
ON publication.projection_outbox (
  channel_id,publication_stream_id,created_at,projection_id
);

CREATE TABLE IF NOT EXISTS result.entity_current (
  channel_id TEXT PRIMARY KEY,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  active_sequence BIGINT NOT NULL CHECK (active_sequence >= 0),
  active_revision_id UUID NOT NULL,
  result_hash TEXT NOT NULL CHECK (result_hash ~ '^sha256:[0-9a-f]{64}$'),
  payload_json JSONB NOT NULL,
  lifecycle_status TEXT,
  is_retracted BOOLEAN NOT NULL DEFAULT false,
  activated_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(payload_json) = 'object')
);

CREATE TABLE IF NOT EXISTS result.video_current (
  channel_id TEXT PRIMARY KEY,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  active_sequence BIGINT NOT NULL CHECK (active_sequence >= 0),
  active_revision_id UUID NOT NULL,
  result_hash TEXT NOT NULL CHECK (result_hash ~ '^sha256:[0-9a-f]{64}$'),
  window_policy JSONB NOT NULL,
  window_proof JSONB NOT NULL,
  activated_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(window_policy) = 'object'),
  CHECK (jsonb_typeof(window_proof) = 'object')
);

CREATE TABLE IF NOT EXISTS result.content_current (
  channel_id TEXT NOT NULL,
  content_id TEXT NOT NULL,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  active_sequence BIGINT NOT NULL CHECK (active_sequence >= 0),
  active_revision_id UUID NOT NULL,
  item_hash TEXT NOT NULL CHECK (item_hash ~ '^sha256:[0-9a-f]{64}$'),
  payload_json JSONB NOT NULL,
  position INTEGER,
  window_status TEXT NOT NULL
    CHECK (window_status IN ('active','window_exit','retracted')),
  state_reason TEXT,
  activated_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel_id,content_id),
  CHECK (btrim(channel_id) <> '' AND btrim(content_id) <> ''),
  CHECK (jsonb_typeof(payload_json) = 'object'),
  CHECK (
    (window_status='active' AND position > 0 AND state_reason IS NULL)
    OR (window_status<>'active' AND position IS NULL AND btrim(state_reason) <> '')
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_business_result_content_active_position
ON result.content_current (channel_id,position)
WHERE window_status='active';

CREATE TABLE IF NOT EXISTS result.agent_current (
  channel_id TEXT PRIMARY KEY,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  active_sequence BIGINT NOT NULL CHECK (active_sequence >= 0),
  active_revision_id UUID NOT NULL,
  result_hash TEXT NOT NULL CHECK (result_hash ~ '^sha256:[0-9a-f]{64}$'),
  payload_json JSONB NOT NULL,
  is_retracted BOOLEAN NOT NULL DEFAULT false,
  activated_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(payload_json) = 'object')
);

CREATE OR REPLACE FUNCTION publication.guard_business_consumer_cursor()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $activation_guard$
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
$activation_guard$;

DROP TRIGGER IF EXISTS trg_business_publication_consumer_cursor
ON publication.consumer_cursor;
CREATE TRIGGER trg_business_publication_consumer_cursor
BEFORE UPDATE OR DELETE ON publication.consumer_cursor
FOR EACH ROW EXECUTE FUNCTION publication.guard_business_consumer_cursor();

CREATE OR REPLACE FUNCTION publication.guard_business_activation_audit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $activation_guard$
BEGIN
  RAISE EXCEPTION 'Business Publication Activation audit is immutable'
    USING ERRCODE='55000';
END
$activation_guard$;

DROP TRIGGER IF EXISTS trg_business_publication_activation_audit
ON publication.activation;
CREATE TRIGGER trg_business_publication_activation_audit
BEFORE UPDATE OR DELETE ON publication.activation
FOR EACH ROW EXECUTE FUNCTION publication.guard_business_activation_audit();

DROP TRIGGER IF EXISTS trg_business_publication_activation_item_audit
ON publication.activation_item;
CREATE TRIGGER trg_business_publication_activation_item_audit
BEFORE UPDATE OR DELETE ON publication.activation_item
FOR EACH ROW EXECUTE FUNCTION publication.guard_business_activation_audit();
