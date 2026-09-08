-- Apply before workers emit uploads_empty dormancy. No channel data is rewritten.
BEGIN;
ALTER TABLE feature_clock.channel_clock_state DROP CONSTRAINT IF EXISTS channel_clock_state_dormant_check;
ALTER TABLE feature_clock.channel_clock_state
ADD CONSTRAINT channel_clock_state_dormant_check
CHECK (
  lifecycle_status<>'dormant'
  OR (
    dormant_reason IN ('no_published_content_within_90_days','uploads_empty')
    AND dormant_since IS NOT NULL
    AND dormant_recheck_day IS NOT NULL
    AND dormant_cycle > 0
    AND dormant_source_event_id IS NOT NULL
  )
);
COMMIT;
