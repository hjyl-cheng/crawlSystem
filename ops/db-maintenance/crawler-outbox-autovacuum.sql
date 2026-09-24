-- Measured 2026-09-23: 367,015 dead item identifiers in the publish hot path,
-- with only 2 live pending events. No columns, tables, or indexes are added.
-- At ~1.87M rows, lower the dead-tuple trigger from ~374K to ~10.4K.
-- Keep global autovacuum worker/cost settings. This does not promise a cadence:
-- worker availability, xmin, and statistics reporting still govern execution.
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
    RAISE EXCEPTION 'existing table vacuum policy differs; review before replacing it';
  END IF;
END $$;
ALTER TABLE crawler.crawler_outbox SET (
  autovacuum_vacuum_scale_factor = 0.005,
  autovacuum_vacuum_threshold = 1000
);
COMMIT;
