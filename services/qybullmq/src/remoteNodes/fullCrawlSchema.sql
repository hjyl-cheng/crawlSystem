-- P2 isolated schema draft. Apply explicitly after the existing remote schemas.
-- No startup/production migration imports this file. Existing tasks retain the
-- only transport lease; these tables store full-crawl execution evidence.
ALTER TABLE remote_ingestion.worker_connections
  DROP CONSTRAINT IF EXISTS worker_connections_role_check,
  ADD CONSTRAINT worker_connections_role_check CHECK (role IN ('incremental','fullcrawl'));
ALTER TABLE remote_ingestion.worker_connections
  DROP CONSTRAINT IF EXISTS worker_connections_mode_check,
  ADD CONSTRAINT worker_connections_mode_check CHECK (mode IN ('connect_only','incremental_collect','full_crawl_collect'));
ALTER TABLE remote_ingestion.worker_connections
  DROP CONSTRAINT IF EXISTS worker_connections_workload_check,
  ADD CONSTRAINT worker_connections_workload_check CHECK (
    (role='incremental' AND mode IN ('connect_only','incremental_collect') AND slot NOT LIKE 'full-crawl-%')
    OR (role='fullcrawl' AND mode='full_crawl_collect' AND slot ~ '^full-crawl-[1-9][0-9]*$'
      AND (runtime_revision IS NULL OR runtime_revision='youtubejs-full-crawl-v1'))
  );

CREATE TABLE IF NOT EXISTS remote_ingestion.full_crawl_executions (
  task_id UUID NOT NULL REFERENCES remote_ingestion.tasks(task_id),
  generation INTEGER NOT NULL CHECK (generation > 0),
  node_id UUID NOT NULL,
  worker_slot TEXT NOT NULL CHECK (worker_slot ~ '^full-crawl-[1-9][0-9]*$'),
  instance_id UUID NOT NULL,
  protocol_version INTEGER NOT NULL CHECK (protocol_version=1),
  execution_input JSONB NOT NULL CHECK (jsonb_typeof(execution_input)='object'),
  execution_hash TEXT NOT NULL CHECK (execution_hash ~ '^[a-f0-9]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(task_id,generation),
  FOREIGN KEY(node_id,worker_slot) REFERENCES remote_ingestion.worker_connections(node_id,slot)
);

CREATE TABLE IF NOT EXISTS remote_ingestion.full_crawl_stages (
  stage_id UUID PRIMARY KEY,
  task_id UUID NOT NULL,
  generation INTEGER NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('admission','uploads','details','close_fetch')),
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  input JSONB NOT NULL CHECK (jsonb_typeof(input)='object'),
  input_hash TEXT NOT NULL CHECK (input_hash ~ '^[a-f0-9]{64}$'),
  target_hash TEXT CHECK (target_hash ~ '^[a-f0-9]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  applied_at TIMESTAMPTZ,
  UNIQUE(task_id,generation,sequence),
  UNIQUE(stage_id,stage),
  FOREIGN KEY(task_id,generation) REFERENCES remote_ingestion.full_crawl_executions(task_id,generation),
  CHECK ((stage IN ('details','close_fetch') AND target_hash IS NOT NULL)
    OR (stage IN ('admission','uploads') AND target_hash IS NULL))
);

-- Reserving never consumes a business attempt. W06 will apply started evidence
-- and the existing detail attempt mutation in one fenced business transaction.
CREATE TABLE IF NOT EXISTS remote_ingestion.full_crawl_detail_reservations (
  reservation_id UUID PRIMARY KEY,
  stage_id UUID NOT NULL,
  stage TEXT NOT NULL DEFAULT 'details' CHECK (stage='details'),
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  video_id TEXT NOT NULL CHECK (length(video_id) BETWEEN 1 AND 500),
  target JSONB NOT NULL CHECK (jsonb_typeof(target)='object'),
  state TEXT NOT NULL DEFAULT 'reserved'
    CHECK (state IN ('reserved','started','captured','applied','uncertain','cancelled')),
  start_id UUID,
  started_at TIMESTAMPTZ,
  applied_at TIMESTAMPTZ,
  UNIQUE(stage_id,ordinal),
  UNIQUE(stage_id,video_id),
  UNIQUE(stage_id,start_id),
  FOREIGN KEY(stage_id,stage) REFERENCES remote_ingestion.full_crawl_stages(stage_id,stage),
  CHECK ((start_id IS NULL)=(started_at IS NULL)),
  CHECK ((state='reserved' AND start_id IS NULL AND applied_at IS NULL)
    OR (state IN ('started','captured') AND start_id IS NOT NULL AND applied_at IS NULL)
    OR (state='applied' AND start_id IS NOT NULL AND applied_at IS NOT NULL)
    OR (state IN ('uncertain','cancelled') AND applied_at IS NULL))
);

CREATE TABLE IF NOT EXISTS remote_ingestion.full_crawl_result_batches (
  batch_id UUID PRIMARY KEY,
  stage_id UUID NOT NULL REFERENCES remote_ingestion.full_crawl_stages(stage_id),
  sequence INTEGER NOT NULL CHECK (sequence > 0),
  payload_hash TEXT NOT NULL CHECK (payload_hash ~ '^[a-f0-9]{64}$'),
  payload_bytes INTEGER NOT NULL CHECK (payload_bytes BETWEEN 1 AND 8388608),
  part_count INTEGER NOT NULL CHECK (part_count BETWEEN 1 AND 16),
  state TEXT NOT NULL DEFAULT 'receiving' CHECK (state IN ('receiving','received','applied','conflict')),
  received_at TIMESTAMPTZ,
  applied_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(stage_id,sequence),
  UNIQUE(batch_id,part_count,payload_bytes),
  CHECK (part_count=(payload_bytes+524287)/524288),
  CHECK ((state='receiving' AND received_at IS NULL AND applied_at IS NULL)
    OR (state='received' AND received_at IS NOT NULL AND applied_at IS NULL)
    OR (state='applied' AND received_at IS NOT NULL AND applied_at IS NOT NULL)
    OR (state='conflict' AND applied_at IS NULL))
);
CREATE INDEX IF NOT EXISTS full_crawl_batches_unapplied
  ON remote_ingestion.full_crawl_result_batches(received_at,batch_id) WHERE state='received';

CREATE TABLE IF NOT EXISTS remote_ingestion.full_crawl_result_parts (
  batch_id UUID NOT NULL,
  part_number INTEGER NOT NULL CHECK (part_number >= 0),
  part_count INTEGER NOT NULL,
  payload_bytes INTEGER NOT NULL,
  part_hash TEXT NOT NULL CHECK (part_hash ~ '^[a-f0-9]{64}$'),
  payload BYTEA NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(batch_id,part_number),
  FOREIGN KEY(batch_id,part_count,payload_bytes)
    REFERENCES remote_ingestion.full_crawl_result_batches(batch_id,part_count,payload_bytes),
  CHECK (part_number < part_count),
  CHECK (octet_length(payload)=CASE WHEN part_number=part_count-1
    THEN payload_bytes-524288*(part_count-1) ELSE 524288 END)
);
