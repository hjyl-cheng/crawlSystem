import { closeDb, withTransaction } from "../src/db.js";

try {
  const result = await withTransaction(async (client) => {
    const normalized = await client.query(
      `UPDATE crawler.channels
       SET country=NULL,
           country_source=CASE WHEN country_source='youtube_about' THEN NULL ELSE country_source END,
           updated_at=now()
       WHERE country IS NOT NULL AND NULLIF(btrim(country),'') IS NULL
       RETURNING channel_id`,
    );
    const repairedSources = await client.query(
      `UPDATE crawler.channels
       SET country_source=NULL,updated_at=now()
       WHERE country_source='youtube_about' AND country IS NULL
       RETURNING channel_id`,
    );
    const blockedIncompleteAgentRows = await client.query(
      `UPDATE crawler.channels
       SET ready_for_agent=false,
           agent_status=CASE WHEN agent_status='done' THEN 'done' ELSE 'pending' END,
           agent_error_message='base info incomplete before Agent dispatch',
           updated_at=now()
       WHERE ready_for_agent=true
         AND (subscriber_count IS NULL OR NULLIF(btrim(title),'') IS NULL)
       RETURNING channel_id`,
    );
    await client.query(
      "ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_youtube_about_country_check",
    );
    await client.query(
      `ALTER TABLE crawler.channels
       ADD CONSTRAINT channels_youtube_about_country_check
       CHECK (country_source IS DISTINCT FROM 'youtube_about' OR NULLIF(btrim(country),'') IS NOT NULL)`,
    );
    await client.query(
      "ALTER TABLE crawler.channels DROP CONSTRAINT IF EXISTS channels_agent_base_info_check",
    );
    await client.query(
      `ALTER TABLE crawler.channels
       ADD CONSTRAINT channels_agent_base_info_check
       CHECK (NOT ready_for_agent OR (subscriber_count IS NOT NULL AND NULLIF(btrim(title),'') IS NOT NULL))`,
    );
    const audit = await client.query(
      `SELECT count(*)::int AS invalid_count
       FROM crawler.channels
       WHERE country_source='youtube_about' AND NULLIF(btrim(country),'') IS NULL`,
    );
    return {
      normalized_blank_countries: normalized.rowCount,
      repaired_invalid_sources: repairedSources.rowCount,
      blocked_incomplete_agent_rows: blockedIncompleteAgentRows.rowCount,
      invalid_count: audit.rows[0].invalid_count,
    };
  });
  console.log(JSON.stringify({ ok: true, ...result }));
} finally {
  await closeDb();
}
