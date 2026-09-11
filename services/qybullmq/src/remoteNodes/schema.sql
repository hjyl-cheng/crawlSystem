-- Opt-in only. Never included by ensureSchema() or the production worker.
CREATE SCHEMA IF NOT EXISTS remote_ingestion;

CREATE TABLE IF NOT EXISTS remote_ingestion.nodes (
  node_id UUID PRIMARY KEY,
  token_hash TEXT NOT NULL UNIQUE CHECK (length(token_hash) = 64),
  capabilities TEXT[] NOT NULL,
  max_leases INTEGER NOT NULL CHECK (max_leases BETWEEN 1 AND 100),
  state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active','draining','disabled')),
  last_seen_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS remote_ingestion.tasks (
  task_id UUID PRIMARY KEY,
  work_key TEXT NOT NULL UNIQUE,
  capability TEXT NOT NULL,
  input JSONB NOT NULL,
  context JSONB NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending'
    CHECK (state IN ('pending','leased','received','applied','failed','cancelled')),
  generation INTEGER NOT NULL DEFAULT 0,
  node_id UUID REFERENCES remote_ingestion.nodes(node_id),
  lease_until TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  received_at TIMESTAMPTZ,
  applied_at TIMESTAMPTZ,
  process_attempts INTEGER NOT NULL DEFAULT 0,
  next_process_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  last_error TEXT,
  applied_result JSONB
);
CREATE INDEX IF NOT EXISTS remote_tasks_claim ON remote_ingestion.tasks (capability,created_at)
  WHERE state IN ('pending','leased');
CREATE INDEX IF NOT EXISTS remote_tasks_node_leases ON remote_ingestion.tasks (node_id,lease_until)
  WHERE state='leased';
CREATE INDEX IF NOT EXISTS remote_tasks_process ON remote_ingestion.tasks (next_process_at,received_at)
  WHERE state='received';

CREATE TABLE IF NOT EXISTS remote_ingestion.claims (
  claim_id UUID PRIMARY KEY,
  node_id UUID NOT NULL REFERENCES remote_ingestion.nodes(node_id),
  task_id UUID NOT NULL REFERENCES remote_ingestion.tasks(task_id),
  generation INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS remote_ingestion.receipts (
  batch_id UUID PRIMARY KEY,
  task_id UUID NOT NULL REFERENCES remote_ingestion.tasks(task_id),
  generation INTEGER NOT NULL,
  node_id UUID NOT NULL REFERENCES remote_ingestion.nodes(node_id),
  sha256 TEXT NOT NULL CHECK (length(sha256)=64),
  payload_gzip BYTEA NOT NULL CHECK (octet_length(payload_gzip) <= 1048576),
  received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (task_id,generation)
);

ALTER TABLE remote_ingestion.nodes ADD COLUMN IF NOT EXISTS slot_claims_required BOOLEAN NOT NULL DEFAULT false;
-- One leased task is one frozen channel Plan. Commands below are private to that
-- owner; they are never independently dispatched to another node or BullMQ queue.
ALTER TABLE remote_ingestion.tasks ADD COLUMN IF NOT EXISTS coordinator_id UUID;
ALTER TABLE remote_ingestion.tasks ADD COLUMN IF NOT EXISTS coordinator_until TIMESTAMPTZ;
ALTER TABLE remote_ingestion.tasks ADD COLUMN IF NOT EXISTS lease_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE remote_ingestion.tasks ADD COLUMN IF NOT EXISTS scope_key TEXT;
ALTER TABLE remote_ingestion.tasks ADD COLUMN IF NOT EXISTS worker_slot TEXT;
ALTER TABLE remote_ingestion.tasks ADD COLUMN IF NOT EXISTS target_node_id UUID REFERENCES remote_ingestion.nodes(node_id);
ALTER TABLE remote_ingestion.tasks ADD COLUMN IF NOT EXISTS target_worker_slot TEXT;
ALTER TABLE remote_ingestion.claims ADD COLUMN IF NOT EXISTS worker_slot TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS remote_worker_slot_lease
  ON remote_ingestion.tasks(node_id,worker_slot) WHERE worker_slot IS NOT NULL AND state='leased';
CREATE UNIQUE INDEX IF NOT EXISTS remote_channel_scope_lease
  ON remote_ingestion.tasks(scope_key) WHERE scope_key IS NOT NULL AND state='leased';
CREATE TABLE IF NOT EXISTS remote_ingestion.channel_commands (
  command_id UUID PRIMARY KEY,
  task_id UUID NOT NULL REFERENCES remote_ingestion.tasks(task_id),
  generation INTEGER NOT NULL,
  command_key TEXT NOT NULL,
  operation TEXT NOT NULL CHECK (operation IN ('open_channel','scan_uploads','video_detail')),
  input JSONB NOT NULL,
  state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','received')),
  batch_id UUID UNIQUE,
  sha256 TEXT,
  payload_gzip BYTEA CHECK (octet_length(payload_gzip)<=1048576),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  received_at TIMESTAMPTZ,
  UNIQUE(task_id,generation,command_key)
);
CREATE INDEX IF NOT EXISTS remote_channel_commands_pending
  ON remote_ingestion.channel_commands(task_id,generation,created_at) WHERE state='pending';

CREATE TABLE IF NOT EXISTS remote_ingestion.execution_handoffs (
  task_id UUID NOT NULL REFERENCES remote_ingestion.tasks(task_id),
  from_generation INTEGER NOT NULL,
  from_attempt_id TEXT NOT NULL,
  to_attempt_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  previous_context JSONB NOT NULL,
  previous_result JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(task_id,to_attempt_id)
);
