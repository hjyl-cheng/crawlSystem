-- P2 opt-in only. Requires crawler/schema.sql and fullCrawlSchema.sql.
-- Old draft evidence remains readable but cannot authorize an execution until
-- the center has saved its complete connection and original Rota identity.
ALTER TABLE remote_ingestion.full_crawl_executions
  ADD COLUMN IF NOT EXISTS connection_identity JSONB,
  ADD COLUMN IF NOT EXISTS rota_fence JSONB;
ALTER TABLE remote_ingestion.full_crawl_detail_reservations
  ADD COLUMN IF NOT EXISTS result_hash TEXT CHECK (result_hash ~ '^[a-f0-9]{64}$'),
  ADD COLUMN IF NOT EXISTS applied_result JSONB;
ALTER TABLE remote_ingestion.full_crawl_result_batches ADD COLUMN IF NOT EXISTS applied_result JSONB;
ALTER TABLE remote_ingestion.full_crawl_executions
  ADD COLUMN IF NOT EXISTS settings_snapshot JSONB,
  ADD COLUMN IF NOT EXISTS reference_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS admission_started_at TIMESTAMPTZ;

-- A pre-admission attempt has no channel_runs FK yet. Locking only the run or
-- querying MAX(attempt_number) cannot prevent a concurrent newer INSERT.
-- All full attempt inserts share the candidate/binding locks used by the
-- remote business fence, including inserts from the existing local runner.
CREATE OR REPLACE FUNCTION remote_ingestion.lock_full_crawl_attempt_insert()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE owner_candidate BIGINT;
BEGIN
  IF NEW.queue_name <> 'youtube-channel-crawl' OR NEW.business_run_id IS NULL THEN
    RETURN NEW;
  END IF;
  SELECT candidate_id INTO owner_candidate FROM crawler.business_run_bindings
    WHERE business_run_id=NEW.business_run_id AND run_kind='full';
  IF owner_candidate IS NOT NULL THEN
    PERFORM 1 FROM crawler.channel_candidates WHERE candidate_id=owner_candidate FOR UPDATE;
    PERFORM 1 FROM crawler.business_run_bindings WHERE business_run_id=NEW.business_run_id FOR UPDATE;
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS full_crawl_attempt_insert_lock ON crawler.channel_execution_attempts;
CREATE TRIGGER full_crawl_attempt_insert_lock BEFORE INSERT ON crawler.channel_execution_attempts
FOR EACH ROW EXECUTE FUNCTION remote_ingestion.lock_full_crawl_attempt_insert();
