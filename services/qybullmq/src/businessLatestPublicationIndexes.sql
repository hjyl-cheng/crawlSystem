-- Run each statement separately, outside a transaction, against the verified
-- Business database. CONCURRENTLY allows publication to continue during build.
-- Keep the existing indexes: they also serve historical/audit queries.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_business_projection_open_predecessor
  ON publication.projection_outbox(channel_id,publication_stream_id,created_at,projection_id)
  WHERE status <> 'delivered';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_business_projection_ready_order
  ON publication.projection_outbox(next_attempt_at,created_at,projection_id)
  WHERE status IN ('pending','retry_wait','leased');
