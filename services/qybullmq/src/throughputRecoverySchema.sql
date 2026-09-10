CREATE TABLE IF NOT EXISTS crawler.finalize_recovery_scan (
  scope TEXT PRIMARY KEY,
  after_channel_id TEXT NOT NULL DEFAULT '',
  upper_channel_id TEXT,
  lease_token UUID,
  lease_until TIMESTAMPTZ,
  completed_rounds BIGINT NOT NULL DEFAULT 0,
  last_completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crawler.finalize_recovery_requests (
  channel_id TEXT PRIMARY KEY,
  requested_generation BIGINT NOT NULL DEFAULT 1,
  handled_generation BIGINT NOT NULL DEFAULT 0,
  next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  lease_token UUID,
  lease_until TIMESTAMPTZ,
  dispatched_run_id TEXT,
  dispatched_job_id TEXT,
  dispatched_generation BIGINT,
  last_decision TEXT,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS finalize_recovery_requests_due
  ON crawler.finalize_recovery_requests(next_check_at,channel_id)
  WHERE requested_generation>handled_generation;

CREATE TABLE IF NOT EXISTS crawler.migration_throughput_samples (
  batch_id TEXT NOT NULL,
  sampled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  active_seconds DOUBLE PRECISION NOT NULL,
  counts JSONB NOT NULL,
  publishing_count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(batch_id,sampled_at)
);

-- A channel-level durable intent is committed with each relevant source write.
-- Timestamp-only source updates matter to the existing freshness contract. Run
-- finalization bookkeeping is excluded to avoid a self-triggering recovery loop.
CREATE OR REPLACE FUNCTION crawler.capture_finalize_recovery_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE source_channel TEXT; old_value JSONB; new_value JSONB;
BEGIN
  IF TG_OP='UPDATE' AND TG_TABLE_NAME='channel_runs' THEN
    old_value=jsonb_build_array(OLD.channel_id,OLD.detail_status,OLD.expected_content_count,
      OLD.result_json);
    new_value=jsonb_build_array(NEW.channel_id,NEW.detail_status,NEW.expected_content_count,
      NEW.result_json);
    IF old_value IS NOT DISTINCT FROM new_value THEN RETURN NULL; END IF;
  ELSIF TG_OP='UPDATE' AND to_jsonb(OLD) IS NOT DISTINCT FROM to_jsonb(NEW) THEN
    RETURN NULL;
  END IF;
  FOR source_channel IN
    SELECT DISTINCT id FROM unnest(ARRAY[
      CASE WHEN TG_OP<>'INSERT' THEN OLD.channel_id END,
      CASE WHEN TG_OP<>'DELETE' THEN NEW.channel_id END
    ]) ids(id) WHERE id IS NOT NULL ORDER BY id
  LOOP
    INSERT INTO crawler.finalize_recovery_requests(channel_id) VALUES(source_channel)
    ON CONFLICT(channel_id) DO UPDATE SET
      requested_generation=crawler.finalize_recovery_requests.requested_generation+1,
      next_check_at=now(),requested_at=now(),updated_at=now();
  END LOOP;
  RETURN NULL;
END $$;

DO $$
DECLARE source_table TEXT;
BEGIN
  FOREACH source_table IN ARRAY ARRAY['channels','channel_runs','content_candidates','contents','agent_profiles','crawl_observations'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS capture_finalize_recovery ON crawler.%I',source_table);
    EXECUTE format('CREATE TRIGGER capture_finalize_recovery AFTER INSERT OR UPDATE OR DELETE ON crawler.%I
      FOR EACH ROW EXECUTE FUNCTION crawler.capture_finalize_recovery_change()',source_table);
  END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS crawler.migration_settlement_cursors (
  batch_id TEXT PRIMARY KEY,
  after_ordinal BIGINT NOT NULL DEFAULT 0
);
