// Explicitly restored source snapshots live in the target; the legacy DB stays read-only.
export function restoredMigrationSourcesEnabled(environment = process.env) {
  return environment.MIGRATION_RESTORED_SOURCES_ENABLED === "true";
}

export async function loadRestoredMigrationSources({
  sourceId, channelId = null, candidateId = null, limit = 2000,
  excludeSourceCandidateIds = [], excludeChannelIds = [],
  query = null,
}) {
  const execute = query ?? (await import("./db.js")).query;
  const result = await execute(
    `SELECT snapshot_json FROM crawler.restored_migration_sources
     WHERE source_id=$1
       AND ($2::text IS NULL OR channel_id=$2)
       AND ($3::bigint IS NULL OR source_candidate_id=$3::bigint)
       AND NOT (source_candidate_id=ANY($4::bigint[]))
       AND NOT (channel_id=ANY($5::text[]))
     ORDER BY priority DESC,source_candidate_id
     LIMIT $6::int`,
    [sourceId, channelId, candidateId, excludeSourceCandidateIds, excludeChannelIds, limit],
  );
  return result.rows.map(row => row.snapshot_json);
}

export async function retainRestoredMigrationInventory(client, {sourceId, syncToken}) {
  await client.query(
    `INSERT INTO crawler.migration_channel_inventory (
       source_id,source_candidate_id,channel_id,channel_url,handle,title,avatar_url,
       search_subscriber_count,priority,source_candidate_status,source_updated_at,sync_token,synced_at
     ) SELECT source_id,source_candidate_id,channel_id,snapshot_json->>'channel_url',
       snapshot_json->>'handle',snapshot_json->>'title',snapshot_json->>'avatar_url',
       (snapshot_json->>'search_subscriber_count')::bigint,priority,'discovered',
       (snapshot_json->>'source_updated_at')::timestamptz,$2::uuid,now()
     FROM crawler.restored_migration_sources WHERE source_id=$1
     ON CONFLICT (source_id,channel_id) DO UPDATE
     SET source_candidate_id=EXCLUDED.source_candidate_id,
         channel_url=EXCLUDED.channel_url,handle=EXCLUDED.handle,title=EXCLUDED.title,
         avatar_url=EXCLUDED.avatar_url,search_subscriber_count=EXCLUDED.search_subscriber_count,
         priority=EXCLUDED.priority,source_candidate_status=EXCLUDED.source_candidate_status,
         source_updated_at=EXCLUDED.source_updated_at,
         sync_token=EXCLUDED.sync_token,synced_at=EXCLUDED.synced_at`,
    [sourceId,syncToken],
  );
}
