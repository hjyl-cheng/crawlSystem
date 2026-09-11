-- Explicit opt-in, after workerConnectionSchema.sql. Never applied by startup.
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'connect_only'
  CHECK(mode IN ('connect_only','incremental_collect'));
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS activation_requested BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS accepting BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS runtime_revision TEXT;
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS activated_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS remote_ingestion.node_deployments (
  node_id UUID PRIMARY KEY REFERENCES remote_ingestion.nodes(node_id),
  deployment_id UUID NOT NULL,
  image TEXT NOT NULL,
  worker_count INTEGER NOT NULL CHECK(worker_count BETWEEN 1 AND 32),
  credentials_cipher BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
