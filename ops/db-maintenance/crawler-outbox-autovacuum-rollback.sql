-- Restore the verified original state: neither option was set on this table.
-- Preserve every unrelated table option. Refuse to overwrite a newer policy.
BEGIN;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '10s';
DO $$ BEGIN
  IF current_database() <> 'newcrawler_crawler' OR pg_is_in_recovery() THEN
    RAISE EXCEPTION 'unexpected maintenance target';
  END IF;
END $$;
LOCK TABLE crawler.crawler_outbox IN SHARE UPDATE EXCLUSIVE MODE;
DO $$ BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_class c, LATERAL pg_options_to_table(c.reloptions) o
    WHERE c.oid = 'crawler.crawler_outbox'::regclass
      AND ((o.option_name = 'autovacuum_vacuum_scale_factor' AND o.option_value::numeric <> 0.005)
        OR (o.option_name = 'autovacuum_vacuum_threshold' AND o.option_value::integer <> 1000))
  ) THEN
    RAISE EXCEPTION 'table vacuum policy has changed; refuse automatic rollback';
  END IF;
END $$;
ALTER TABLE crawler.crawler_outbox RESET (
  autovacuum_vacuum_scale_factor,
  autovacuum_vacuum_threshold
);
COMMIT;
