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
