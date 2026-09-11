-- Opt-in connection verification. Apply after schema.sql and routeSchema.sql.
-- This records deployment connections, not channel jobs or Clock state.
CREATE TABLE IF NOT EXISTS remote_ingestion.worker_connections (
  node_id UUID NOT NULL,
  slot TEXT NOT NULL,
  deployment_id UUID NOT NULL,
  config_hash TEXT NOT NULL CHECK (config_hash ~ '^[a-f0-9]{64}$'),
  role TEXT NOT NULL CHECK (role = 'incremental'),
  instance_id UUID,
  relay_boot_id TEXT,
  connected_until TIMESTAMPTZ,
  last_seen_at TIMESTAMPTZ,
  PRIMARY KEY (node_id,slot),
  FOREIGN KEY (node_id,slot) REFERENCES remote_ingestion.network_slots(node_id,slot)
);
