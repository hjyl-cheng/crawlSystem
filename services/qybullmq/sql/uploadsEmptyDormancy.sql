-- Apply before workers emit uploads_empty dormancy. No channel data is rewritten.
BEGIN;
ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_dormant_state_check;
ALTER TABLE crawler.channels ADD CONSTRAINT channels_dormant_state_check
CHECK (
  (
    status='dormant'
    AND dormant_reason IN ('no_published_content_within_90_days','uploads_empty')
    AND dormant_since IS NOT NULL
    AND dormant_recheck_day IS NOT NULL
    AND dormant_last_probe_at IS NOT NULL
    AND dormant_cycle > 0
    AND reject_reason IS NULL
  )
  OR (
    status<>'dormant'
    AND dormant_reason IS NULL
    AND dormant_since IS NULL
    AND dormant_recheck_day IS NULL
    AND dormant_last_probe_at IS NULL
    AND dormant_cycle=0
  )
);
COMMIT;
