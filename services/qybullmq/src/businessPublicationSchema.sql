CREATE SCHEMA IF NOT EXISTS publication;

CREATE TABLE IF NOT EXISTS publication.database_identity (
  singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
  database_kind TEXT NOT NULL CHECK (database_kind='business'),
  database_name TEXT NOT NULL,
  initialized_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO publication.database_identity (singleton, database_kind, database_name)
VALUES (true, 'business', current_database())
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS publication.stream (
  publication_stream_id UUID PRIMARY KEY,
  source_deployment_key TEXT NOT NULL UNIQUE,
  source_identity_json JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'sealed', 'revoked')),
  accepted_contract_versions INTEGER[] NOT NULL DEFAULT ARRAY[1,2]::integer[],
  automatic_onboarding_projection_mode TEXT,
  registered_by TEXT NOT NULL,
  registered_reason TEXT NOT NULL,
  registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status_changed_by TEXT NOT NULL,
  status_reason TEXT NOT NULL,
  status_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(source_deployment_key) <> ''),
  CHECK (jsonb_typeof(source_identity_json) = 'object'),
  CHECK (cardinality(accepted_contract_versions) > 0),
  CHECK (array_position(accepted_contract_versions,NULL) IS NULL),
  CHECK (0 < ALL(accepted_contract_versions)),
  CONSTRAINT chk_business_publication_automatic_onboarding_projection_mode
    CHECK (automatic_onboarding_projection_mode IN ('held_shadow','online')),
  CHECK (btrim(registered_by) <> '' AND btrim(registered_reason) <> ''),
  CHECK (btrim(status_changed_by) <> '' AND btrim(status_reason) <> '')
);

ALTER TABLE publication.stream
ALTER COLUMN accepted_contract_versions SET DEFAULT ARRAY[1,2]::integer[];

ALTER TABLE publication.stream
ADD COLUMN IF NOT EXISTS automatic_onboarding_projection_mode TEXT;

DO $publication_schema$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='publication.stream'::regclass
      AND conname='chk_business_publication_automatic_onboarding_projection_mode'
  ) THEN
    ALTER TABLE publication.stream
    ADD CONSTRAINT chk_business_publication_automatic_onboarding_projection_mode
    CHECK (automatic_onboarding_projection_mode IN ('held_shadow','online'));
  END IF;
END
$publication_schema$;

CREATE TABLE IF NOT EXISTS publication.channel_ownership (
  channel_id TEXT PRIMARY KEY,
  active_publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'cutover_pending')),
  previous_publication_stream_id UUID
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  ownership_reference JSONB NOT NULL DEFAULT '{}'::jsonb,
  projection_mode TEXT NOT NULL DEFAULT 'held_shadow',
  state_changed_by TEXT NOT NULL,
  state_reason TEXT NOT NULL,
  state_changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(ownership_reference) = 'object'),
  CONSTRAINT chk_business_publication_projection_mode
    CHECK (projection_mode IN ('held_shadow','online')),
  CHECK (btrim(state_changed_by) <> '' AND btrim(state_reason) <> ''),
  CHECK (previous_publication_stream_id IS DISTINCT FROM active_publication_stream_id)
);

CREATE TABLE IF NOT EXISTS publication.inbox (
  revision_id UUID PRIMARY KEY,
  publication_stream_id UUID NOT NULL,
  channel_id TEXT NOT NULL,
  domain TEXT NOT NULL CHECK (domain IN ('channel', 'video', 'agent')),
  data_sequence BIGINT NOT NULL CHECK (data_sequence > 0),
  payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^sha256:[0-9a-f]{64}$'),
  envelope_hash TEXT NOT NULL CHECK (envelope_hash ~ '^sha256:[0-9a-f]{64}$'),
  received_envelope JSONB NOT NULL,
  receipt_id UUID NOT NULL UNIQUE,
  receive_status TEXT NOT NULL
    CHECK (receive_status IN (
      'accepted', 'waiting_gap', 'waiting_ownership', 'rejected', 'conflict'
    )),
  error_code TEXT,
  error_message TEXT,
  first_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  receive_count INTEGER NOT NULL DEFAULT 1 CHECK (receive_count > 0),
  CHECK (btrim(channel_id) <> ''),
  CHECK (jsonb_typeof(received_envelope) = 'object'),
  CHECK (last_received_at >= first_received_at),
  CHECK (
    (receive_status IN ('rejected', 'conflict') AND error_code IS NOT NULL)
    OR (receive_status NOT IN ('rejected', 'conflict'))
  )
);

CREATE INDEX IF NOT EXISTS idx_business_publication_inbox_route
ON publication.inbox (publication_stream_id,channel_id,domain,data_sequence);

CREATE TABLE IF NOT EXISTS publication.revision (
  revision_id UUID PRIMARY KEY
    REFERENCES publication.inbox(revision_id) ON DELETE RESTRICT,
  publication_stream_id UUID NOT NULL
    REFERENCES publication.stream(publication_stream_id) ON DELETE RESTRICT,
  channel_id TEXT NOT NULL,
  domain TEXT NOT NULL CHECK (domain IN ('channel', 'video', 'agent')),
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
  source_json JSONB NOT NULL,
  previous_result_hash TEXT,
  result_hash TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  envelope_hash TEXT NOT NULL,
  validation_status TEXT NOT NULL DEFAULT 'valid'
    CHECK (validation_status IN ('valid', 'quarantined')),
  ingress_status TEXT NOT NULL
    CHECK (ingress_status IN ('accepted', 'waiting_gap', 'waiting_ownership')),
  activation_status TEXT NOT NULL DEFAULT 'staged'
    CHECK (activation_status IN (
      'staged', 'waiting_gap', 'waiting_ownership', 'active', 'quarantined', 'superseded'
    )),
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (publication_stream_id,channel_id,domain,data_sequence),
  CHECK (btrim(channel_id) <> '' AND btrim(policy_version) <> ''),
  CHECK (jsonb_typeof(source_json) = 'object'),
  CHECK (jsonb_typeof(payload_json) = 'object'),
  CHECK (previous_result_hash IS NULL OR previous_result_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (result_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (payload_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (envelope_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (
    (
      revision_type='bootstrap'
      AND data_sequence=1
      AND previous_data_sequence IS NULL
      AND previous_result_hash IS NULL
    ) OR (
      revision_type<>'bootstrap'
      AND previous_data_sequence=data_sequence-1
      AND previous_result_hash IS NOT NULL
    )
  ),
  CHECK (
    (domain='channel' AND (
      (revision_type='retraction' AND operation='retract_channel')
      OR (revision_type<>'retraction' AND operation='replace')
    ))
    OR (domain='video' AND (
      (revision_type='bootstrap' AND operation='replace_window')
      OR (revision_type<>'bootstrap' AND operation='apply_window_delta')
    ))
    OR (domain='agent' AND (
      (revision_type='retraction' AND operation='retract_agent')
      OR (revision_type<>'retraction' AND operation='replace')
    ))
  )
);

CREATE INDEX IF NOT EXISTS idx_business_publication_revision_activation
ON publication.revision (activation_status,publication_stream_id,channel_id,domain,data_sequence);

CREATE TABLE IF NOT EXISTS publication.inbox_conflict (
  conflict_id UUID PRIMARY KEY,
  revision_id UUID NOT NULL REFERENCES publication.inbox(revision_id) ON DELETE RESTRICT,
  conflicting_payload_hash TEXT NOT NULL,
  conflicting_envelope_hash TEXT NOT NULL,
  conflicting_envelope JSONB NOT NULL,
  first_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  receive_count INTEGER NOT NULL DEFAULT 1 CHECK (receive_count > 0),
  UNIQUE (revision_id,conflicting_envelope_hash),
  CHECK (conflicting_payload_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (conflicting_envelope_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (jsonb_typeof(conflicting_envelope) = 'object'),
  CHECK (last_received_at >= first_received_at)
);

CREATE TABLE IF NOT EXISTS publication.quarantine (
  quarantine_id UUID PRIMARY KEY,
  revision_id UUID NOT NULL REFERENCES publication.inbox(revision_id) ON DELETE RESTRICT,
  issue_code TEXT NOT NULL,
  issue_hash TEXT NOT NULL,
  details_json JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'resolved', 'ignored')),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  resolved_by TEXT,
  resolution_reason TEXT,
  UNIQUE (revision_id,issue_code,issue_hash),
  CHECK (btrim(issue_code) <> ''),
  CHECK (issue_hash ~ '^sha256:[0-9a-f]{64}$'),
  CHECK (jsonb_typeof(details_json) = 'object'),
  CHECK (
    (status='open' AND resolved_at IS NULL AND resolved_by IS NULL AND resolution_reason IS NULL)
    OR (status IN ('resolved', 'ignored')
        AND resolved_at IS NOT NULL
        AND btrim(resolved_by) <> ''
        AND btrim(resolution_reason) <> '')
  )
);

CREATE OR REPLACE FUNCTION publication.guard_business_inbox()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $publication_guard$
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
$publication_guard$;

DROP TRIGGER IF EXISTS trg_business_publication_inbox ON publication.inbox;
CREATE TRIGGER trg_business_publication_inbox
BEFORE UPDATE OR DELETE ON publication.inbox
FOR EACH ROW EXECUTE FUNCTION publication.guard_business_inbox();

CREATE OR REPLACE FUNCTION publication.guard_business_revision()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $publication_guard$
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
$publication_guard$;

DROP TRIGGER IF EXISTS trg_business_publication_revision ON publication.revision;
CREATE TRIGGER trg_business_publication_revision
BEFORE UPDATE OR DELETE ON publication.revision
FOR EACH ROW EXECUTE FUNCTION publication.guard_business_revision();
