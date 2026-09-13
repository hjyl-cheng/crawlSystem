-- Additive; inactive until a center explicitly supplies WholeChannelStore.
ALTER TABLE remote_ingestion.channel_commands DROP CONSTRAINT IF EXISTS channel_commands_operation_check;
ALTER TABLE remote_ingestion.channel_commands ADD CONSTRAINT channel_commands_operation_check
  CHECK (operation IN ('open_channel','scan_uploads','video_detail','collect_channel'));
CREATE TABLE IF NOT EXISTS remote_ingestion.whole_channel_inputs (
  command_id uuid PRIMARY KEY REFERENCES remote_ingestion.channel_commands(command_id) ON DELETE CASCADE,
  task_id uuid NOT NULL REFERENCES remote_ingestion.tasks(task_id) ON DELETE CASCADE,
  generation integer NOT NULL CHECK (generation > 0),
  input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[a-f0-9]{64}$'),
  input_json jsonb,
  result_manifest jsonb,
  received_at timestamptz,
  pruned_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(task_id,generation)
);
CREATE TABLE IF NOT EXISTS remote_ingestion.whole_channel_chunks (
  command_id uuid NOT NULL REFERENCES remote_ingestion.whole_channel_inputs(command_id) ON DELETE CASCADE,
  part integer NOT NULL CHECK (part >= 0 AND part < 256),
  sha256 text NOT NULL CHECK (sha256 ~ '^[a-f0-9]{64}$'),
  payload bytea CHECK (octet_length(payload) BETWEEN 1 AND 524288),
  PRIMARY KEY(command_id,part)
);
ALTER TABLE remote_ingestion.whole_channel_inputs ALTER COLUMN input_json DROP NOT NULL;
ALTER TABLE remote_ingestion.whole_channel_inputs ADD COLUMN IF NOT EXISTS pruned_at timestamptz;
ALTER TABLE remote_ingestion.whole_channel_chunks ALTER COLUMN payload DROP NOT NULL;
CREATE INDEX IF NOT EXISTS remote_whole_applied_retention
  ON remote_ingestion.tasks(applied_at) WHERE state='applied';
CREATE INDEX IF NOT EXISTS remote_whole_unpruned_task
  ON remote_ingestion.whole_channel_inputs(task_id) WHERE pruned_at IS NULL;
