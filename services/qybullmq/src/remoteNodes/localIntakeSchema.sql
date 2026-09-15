-- Control/telemetry only. Does not alter channels, plans, or queued jobs.
CREATE TABLE IF NOT EXISTS remote_ingestion.local_incremental_workers (
  worker_id TEXT PRIMARY KEY,
  instance_id UUID NOT NULL,
  activation_requested BOOLEAN NOT NULL DEFAULT false,
  accepting BOOLEAN NOT NULL DEFAULT false,
  active BOOLEAN NOT NULL DEFAULT false,
  connected_until TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Operator configuration only; never changes tasks or collection results.
CREATE TABLE IF NOT EXISTS remote_ingestion.intake_controls (
  node_key TEXT PRIMARY KEY,
  configured_count INTEGER NOT NULL CHECK(configured_count >= 0),
  intake_enabled BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
