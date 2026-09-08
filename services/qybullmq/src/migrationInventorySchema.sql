-- migration-channel-inventory-schema:start
CREATE TABLE IF NOT EXISTS crawler.migration_channel_inventory_syncs (
  source_id TEXT PRIMARY KEY,
  source_database TEXT NOT NULL,
  source_database_oid OID NOT NULL,
  status TEXT NOT NULL DEFAULT 'syncing'
    CHECK (status IN ('syncing', 'ready', 'failed')),
  sync_token UUID NOT NULL,
  eligible_count BIGINT NOT NULL DEFAULT 0 CHECK (eligible_count >= 0),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crawler.migration_channel_inventory (
  source_id TEXT NOT NULL
    REFERENCES crawler.migration_channel_inventory_syncs(source_id) ON DELETE CASCADE,
  source_candidate_id BIGINT NOT NULL,
  channel_id TEXT NOT NULL,
  channel_url TEXT NOT NULL,
  handle TEXT,
  title TEXT,
  avatar_url TEXT,
  search_subscriber_count BIGINT,
  priority INTEGER NOT NULL DEFAULT 100,
  source_candidate_status TEXT NOT NULL
    CHECK (source_candidate_status IN ('discovered', 'queued', 'validating', 'failed')),
  source_updated_at TIMESTAMPTZ,
  sync_token UUID NOT NULL,
  synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, channel_id),
  UNIQUE (source_id, source_candidate_id)
);

CREATE INDEX IF NOT EXISTS idx_crawler_migration_inventory_page
ON crawler.migration_channel_inventory (
  source_id,
  priority DESC,
  source_candidate_id ASC
);
-- migration-channel-inventory-schema:end

-- Explicitly restored legacy channels; original source remains immutable.
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
