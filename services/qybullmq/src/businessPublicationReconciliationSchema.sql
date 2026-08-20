CREATE TABLE IF NOT EXISTS publication.reconciliation_state (
  channel_id TEXT PRIMARY KEY,
  attempt_count BIGINT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  consecutive_error_count INTEGER NOT NULL DEFAULT 0
    CHECK (consecutive_error_count >= 0),
  last_outcome TEXT,
  last_error TEXT,
  last_attempted_at TIMESTAMPTZ,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(channel_id) <> ''),
  CHECK (last_outcome IS NULL OR btrim(last_outcome) <> ''),
  CHECK (
    (lease_owner IS NULL AND lease_expires_at IS NULL)
    OR (
      lease_owner IS NOT NULL
      AND btrim(lease_owner) <> ''
      AND lease_expires_at IS NOT NULL
    )
  ),
  CHECK (
    (last_outcome='error' AND last_error IS NOT NULL AND btrim(last_error) <> '')
    OR (last_outcome IS DISTINCT FROM 'error' AND last_error IS NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_business_publication_reconciliation_claim
ON publication.reconciliation_state (next_attempt_at,channel_id)
WHERE lease_owner IS NULL;

CREATE INDEX IF NOT EXISTS idx_business_publication_reconciliation_expired_lease
ON publication.reconciliation_state (lease_expires_at,channel_id)
WHERE lease_owner IS NOT NULL;
