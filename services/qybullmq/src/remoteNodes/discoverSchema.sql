-- Additive; apply explicitly after the existing remote schemas, including
-- wholeChannelSchema.sql and fullCrawlSchema.sql. Existing incremental and
-- full-crawl rules are restated unchanged; only the Discover workload is new.
ALTER TABLE remote_ingestion.worker_connections
  DROP CONSTRAINT IF EXISTS worker_connections_role_check,
  ADD CONSTRAINT worker_connections_role_check CHECK (role IN ('incremental','fullcrawl','discover'));
ALTER TABLE remote_ingestion.worker_connections
  DROP CONSTRAINT IF EXISTS worker_connections_mode_check,
  ADD CONSTRAINT worker_connections_mode_check
    CHECK (mode IN ('connect_only','incremental_collect','full_crawl_collect','discover_collect'));
ALTER TABLE remote_ingestion.worker_connections
  DROP CONSTRAINT IF EXISTS worker_connections_workload_check,
  ADD CONSTRAINT worker_connections_workload_check CHECK (
    (role='incremental' AND mode IN ('connect_only','incremental_collect')
      AND slot NOT LIKE 'full-crawl-%' AND slot NOT LIKE 'discover-%')
    OR (role='fullcrawl' AND mode='full_crawl_collect' AND slot ~ '^full-crawl-[1-9][0-9]*$'
      AND (runtime_revision IS NULL OR runtime_revision='youtubejs-full-crawl-v1'))
    OR (role='discover' AND mode='discover_collect' AND slot ~ '^discover-[1-9][0-9]*$'
      AND (runtime_revision IS NULL OR runtime_revision='youtube-search-discover-v1'))
  );

ALTER TABLE remote_ingestion.channel_commands DROP CONSTRAINT IF EXISTS channel_commands_operation_check;
ALTER TABLE remote_ingestion.channel_commands ADD CONSTRAINT channel_commands_operation_check
  CHECK (operation IN ('open_channel','scan_uploads','video_detail','collect_channel','collect_search_page'));
