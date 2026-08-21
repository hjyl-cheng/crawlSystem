export const CHANNEL_CURRENT_CONTENT_STATS_SQL = `
  SELECT
    content_type,
    count(*)::bigint AS total,
    count(*) FILTER (WHERE published_at_status = 'exact' AND published_at_precision = 'second')::bigint AS exact_second,
    count(*) FILTER (WHERE length_text IS NOT NULL)::bigint AS has_length,
    count(*) FILTER (WHERE description_status IN ('exact','empty'))::bigint AS description_resolved,
    count(*) FILTER (WHERE like_count IS NOT NULL OR comment_count IS NOT NULL OR comments_disabled = true)::bigint AS has_stats,
    count(*) FILTER (WHERE is_members_only = true)::bigint AS members_only
  FROM crawler.contents
  WHERE channel_id = $1
  GROUP BY content_type
  ORDER BY content_type
`;

export const CHANNEL_CURRENT_CONTENT_SQL = `
  SELECT content_type, title, source_content_id, url, published_text_raw, published_at_status,
         description, description_status, description_source, hashtags, keywords,
         published_at, published_at_precision, published_at_source, position,
         length_text, duration_seconds, duration_status, duration_source,
         view_count_text, view_count_status, view_count_source,
         like_count, like_count_status, like_count_source,
         comment_count, comment_count_status, comments_disabled, comment_count_source,
         is_members_only, access_status, access_status_source,
         live_scheduled_at, live_started_at, live_ended_at, extractor_version
  FROM crawler.contents
  WHERE channel_id = $1
  ORDER BY published_at DESC NULLS LAST, last_seen_at DESC, source_content_id
  LIMIT 100
`;

export async function loadChannelCurrentContent(queryDb, channelId) {
  if (typeof queryDb !== "function") throw new TypeError("queryDb must be a function");
  const normalizedChannelId = String(channelId ?? "").trim();
  if (!normalizedChannelId) throw new TypeError("channelId is required");
  const [contentStats, contents] = await Promise.all([
    queryDb(CHANNEL_CURRENT_CONTENT_STATS_SQL, [normalizedChannelId]),
    queryDb(CHANNEL_CURRENT_CONTENT_SQL, [normalizedChannelId]),
  ]);
  return {
    contentStats: contentStats.rows,
    contents: contents.rows,
  };
}
