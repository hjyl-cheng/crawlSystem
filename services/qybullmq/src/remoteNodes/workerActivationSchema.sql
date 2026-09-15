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
  worker_count INTEGER NOT NULL CHECK(worker_count >= 0),
  credentials_cipher BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Desired intake is independent of busy collection/heartbeat rows. The
-- supervisor applies it with short, skip-locked updates; a restart can resume.
CREATE TABLE IF NOT EXISTS remote_ingestion.node_intake_requests (
  node_id UUID PRIMARY KEY REFERENCES remote_ingestion.nodes(node_id) ON DELETE CASCADE,
  deployment_id UUID NOT NULL,
  selected_slots TEXT[] NOT NULL,
  revision BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Removed slots remain as tombstones so late heartbeats cannot revive them.
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS retirement_id UUID;
ALTER TABLE remote_ingestion.worker_connections ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;
ALTER TABLE remote_ingestion.node_deployments DROP CONSTRAINT IF EXISTS node_deployments_worker_count_check;
ALTER TABLE remote_ingestion.node_deployments ADD CONSTRAINT node_deployments_worker_count_check CHECK(worker_count >= 0);

-- Operator configuration only; never changes tasks or collection results.
CREATE TABLE IF NOT EXISTS remote_ingestion.intake_controls (
  node_key TEXT PRIMARY KEY,
  configured_count INTEGER NOT NULL CHECK(configured_count >= 0),
  intake_enabled BOOLEAN NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
