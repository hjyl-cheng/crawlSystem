CREATE TABLE IF NOT EXISTS crawler.restored_migration_sources (
  source_id TEXT NOT NULL REFERENCES crawler.migration_channel_inventory_syncs(source_id),
  channel_id TEXT NOT NULL,
  source_candidate_id BIGINT NOT NULL,
  priority INTEGER NOT NULL,
  restoration_id TEXT NOT NULL,
  snapshot_json JSONB NOT NULL,
  restored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(source_id,channel_id),
  UNIQUE(source_id,source_candidate_id),
  CHECK(snapshot_json->>'channel_id'=channel_id),
  CHECK(snapshot_json->>'source_id'=source_id),
  CHECK(snapshot_json->>'source_candidate_status'='discovered')
);
