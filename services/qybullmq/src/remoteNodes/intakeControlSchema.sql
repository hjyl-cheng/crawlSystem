-- Operator configuration only; never changes tasks or collection results.
CREATE TABLE IF NOT EXISTS remote_ingestion.intake_controls (
  node_key TEXT PRIMARY KEY,
  configured_count INTEGER NOT NULL CHECK(configured_count >= 0),
  intake_enabled BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
