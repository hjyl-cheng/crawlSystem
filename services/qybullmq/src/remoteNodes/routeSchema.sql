-- Opt-in remote routing only. Apply after remoteNodes/schema.sql.
CREATE TABLE IF NOT EXISTS remote_ingestion.network_slots (
  node_id UUID NOT NULL REFERENCES remote_ingestion.nodes(node_id),
  slot TEXT NOT NULL,
  rota_worker_id TEXT NOT NULL UNIQUE,
  epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch BETWEEN 0 AND 9007199254740991),
  binding_id UUID,
  boot_id TEXT,
  grant_request JSONB,
  grant_cipher BYTEA CHECK (octet_length(grant_cipher)<=32768),
  grant_until TIMESTAMPTZ,
  PRIMARY KEY(node_id,slot)
);
CREATE TABLE IF NOT EXISTS remote_ingestion.network_bindings (
  binding_id UUID PRIMARY KEY,
  node_id UUID NOT NULL,
  slot TEXT NOT NULL,
  task_id UUID NOT NULL REFERENCES remote_ingestion.tasks(task_id),
  generation INTEGER NOT NULL,
  rota_fence JSONB NOT NULL,
  identity JSONB NOT NULL,
  upstream_hash TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'bound' CHECK (state IN ('bound','active','retired')),
  release_receipt JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  retired_at TIMESTAMPTZ,
  UNIQUE(task_id,generation),
  FOREIGN KEY(node_id,slot) REFERENCES remote_ingestion.network_slots(node_id,slot)
);
ALTER TABLE remote_ingestion.network_bindings ADD COLUMN IF NOT EXISTS stop_requested BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE remote_ingestion.network_bindings ADD COLUMN IF NOT EXISTS youtube_session_required BOOLEAN NOT NULL DEFAULT false;
