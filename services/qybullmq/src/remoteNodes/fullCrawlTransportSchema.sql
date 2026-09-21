-- Opt-in P3 transport migration. Apply after natsSchema and fullCrawlSchema.
DROP TRIGGER IF EXISTS remote_full_stage_notify ON remote_ingestion.full_crawl_stages;
CREATE TRIGGER remote_full_stage_notify AFTER INSERT OR UPDATE OF applied_at ON remote_ingestion.full_crawl_stages
  FOR EACH ROW EXECUTE FUNCTION remote_ingestion.notify_transport();
