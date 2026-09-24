-- Small durable cursors only; publication ownership and business data remain
-- in their existing tables. Deploy this additive schema before the controller.
CREATE TABLE IF NOT EXISTS crawler.background_reconciliation_scans (
  scope TEXT PRIMARY KEY,
  after_channel_id TEXT NOT NULL DEFAULT '',
  upper_channel_id TEXT,
  lease_token UUID,
  lease_until TIMESTAMPTZ,
  completed_rounds BIGINT NOT NULL DEFAULT 0,
  round_started_at TIMESTAMPTZ,
  last_completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
