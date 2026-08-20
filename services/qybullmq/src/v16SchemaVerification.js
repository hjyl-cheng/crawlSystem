export const V16_SCHEMA_VERIFICATION_SQL = `SELECT
  to_regclass('crawler.crawl_observations') IS NOT NULL AS observations_ready,
  to_regclass('crawler.channel_about_metric_snapshots') IS NOT NULL AS snapshots_ready,
  to_regclass('crawler.agent_refresh_requests') IS NOT NULL AS agent_requests_ready,
  to_regclass('crawler.crawler_outbox') IS NOT NULL AS outbox_ready,
  to_regclass('crawler.baseline_exports') IS NOT NULL AS baseline_exports_ready,
  to_regclass('crawler.baseline_export_events') IS NOT NULL AS baseline_events_ready,
  to_regclass('crawler.ux_crawler_channel_runs_plan_id') IS NOT NULL AS plan_identity_ready,
  to_regclass('crawler.idx_crawler_content_candidates_channel_source') IS NULL
    AS candidate_identity_index_removed,
  to_regclass('crawler.ux_crawler_contents_channel_source') IS NOT NULL
    AS video_identity_index_ready,
  NOT EXISTS (
    SELECT 1
    FROM crawler.contents
    GROUP BY channel_id,source_content_id
    HAVING count(*)>1
  ) AS video_identity_deduplicated,
  EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid='crawler.contents'::regclass
      AND conname='contents_access_status_check'
      AND pg_get_constraintdef(oid) LIKE '%unlisted%'
  ) AS unlisted_access_status_ready,
  EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='crawler' AND table_name='channels'
      AND column_name='about_current_hash'
  ) AS channel_projection_ready,
  EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='crawler' AND table_name='contents'
      AND column_name='publication_item_hash'
  ) AS video_publication_hash_ready,
  (
    SELECT count(*)=4
    FROM information_schema.columns
    WHERE table_schema='crawler' AND table_name='agent_profiles'
      AND column_name IN (
        'input_content_ids','input_content_hash','taxonomy_version','agent_version_hash'
      )
  ) AS agent_publication_context_ready,
  (
    SELECT count(*)=11
    FROM information_schema.columns
    WHERE table_schema='crawler' AND table_name='channels'
      AND column_name IN (
        'keywords_status','available_tabs_status','description_status',
        'external_links_status','rss_url','vanity_channel_url',
        'is_family_safe','is_verified','is_verified_status',
        'joined_at','joined_at_precision'
      )
  ) AS channel_publication_fields_ready,
  (
    SELECT count(*)=5
    FROM information_schema.columns
    WHERE table_schema='crawler' AND table_name='channels'
      AND column_name IN (
        'dormant_reason','dormant_since','dormant_recheck_day',
        'dormant_last_probe_at','dormant_cycle'
      )
  ) AS dormant_lifecycle_ready`;
