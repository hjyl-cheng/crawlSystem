CREATE TABLE IF NOT EXISTS crawler.migration_control_batches (
 batch_id TEXT PRIMARY KEY REFERENCES crawler.query_dispatch_batches(dispatch_batch_id),
 source_id TEXT NOT NULL,
 selection TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('preparing','running','pausing','paused','stopping','ended','completed')),
 version BIGINT NOT NULL DEFAULT 1,
 total_count INTEGER NOT NULL DEFAULT 0,
 max_in_flight INTEGER NOT NULL DEFAULT 20 CHECK(max_in_flight BETWEEN 1 AND 200),
 frozen_at TIMESTAMPTZ,
 paused_at TIMESTAMPTZ,
 paused_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
 finished_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS migration_control_one_active
 ON crawler.migration_control_batches ((true)) WHERE status NOT IN ('ended','completed');
CREATE TABLE IF NOT EXISTS crawler.migration_control_items (
 batch_id TEXT NOT NULL REFERENCES crawler.migration_control_batches(batch_id),
 channel_id TEXT NOT NULL,
 source_candidate_id BIGINT NOT NULL,
 snapshot_json JSONB,
 ordinal BIGINT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','started','terminal','released')),
 candidate_id BIGINT REFERENCES crawler.channel_candidates(candidate_id),
 outcome TEXT CHECK(outcome IN ('success','dormant','rejected','failed','existing')),
 error_message TEXT,
 start_failures INTEGER NOT NULL DEFAULT 0,
 started_at TIMESTAMPTZ,
 finished_at TIMESTAMPTZ,
 PRIMARY KEY(batch_id,channel_id),
 UNIQUE(batch_id,ordinal)
);
CREATE INDEX IF NOT EXISTS migration_control_items_state ON crawler.migration_control_items(batch_id,state,ordinal);
-- Retry admission must distinguish initial inventory from restored failures
-- without reading every restored snapshot while holding the scheduler lock.
CREATE INDEX IF NOT EXISTS migration_control_items_initial_pending
  ON crawler.migration_control_items(batch_id)
  WHERE state='pending' AND snapshot_json->>'migration_system_retry_id' IS NULL;
CREATE INDEX IF NOT EXISTS migration_control_items_candidate ON crawler.migration_control_items(candidate_id) WHERE candidate_id IS NOT NULL;
-- Cover the deduplicated migration detail-completion metric without scanning
-- wide Run payloads or sorting all historical Runs every 30 seconds.
CREATE INDEX IF NOT EXISTS idx_crawler_runs_detail_done_candidate_channel
  ON crawler.channel_runs(candidate_id,channel_id)
  WHERE candidate_id IS NOT NULL AND detail_status='done';

CREATE INDEX IF NOT EXISTS migration_control_items_batch_candidate_channel
  ON crawler.migration_control_items(batch_id,candidate_id,channel_id)
  WHERE candidate_id IS NOT NULL;

-- Queue control fencing survives transaction rollback and Controller takeover.
CREATE SEQUENCE IF NOT EXISTS crawler.migration_queue_control_revision;
