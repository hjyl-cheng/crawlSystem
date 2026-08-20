export const crawlerContentExportTables = Object.freeze([
  "query_sets",
  "query_terms",
  "query_pages",
  "channels",
  "channel_runs",
  "content_candidates",
  "contents",
  "youtube_api_tasks",
  "youtube_api_batches",
  "youtube_api_daily_usage",
  "raw_objects",
  "agent_profiles",
  "finalized_profiles",
]);

const activeChannelPredicates = Object.freeze({
  channels: "source.status='active'",
  channel_runs: `EXISTS (
    SELECT 1 FROM crawler.channels channel
    WHERE channel.channel_id=source.channel_id AND channel.status='active'
  )`,
  content_candidates: `EXISTS (
    SELECT 1 FROM crawler.channels channel
    WHERE channel.channel_id=source.channel_id AND channel.status='active'
  )`,
  contents: `EXISTS (
    SELECT 1 FROM crawler.channels channel
    WHERE channel.channel_id=source.channel_id AND channel.status='active'
  )`,
  youtube_api_tasks: `(
    NOT EXISTS (
      SELECT 1
      FROM crawler.content_candidates candidate
      WHERE candidate.candidate_id=ANY(source.candidate_ids)
    )
    OR EXISTS (
      SELECT 1
      FROM crawler.content_candidates candidate
      JOIN crawler.channels channel ON channel.channel_id=candidate.channel_id
      WHERE candidate.candidate_id=ANY(source.candidate_ids)
        AND channel.status='active'
    )
  )`,
  youtube_api_batches: `(
    NOT EXISTS (
      SELECT 1
      FROM crawler.youtube_api_tasks task
      JOIN crawler.content_candidates candidate
        ON candidate.candidate_id=ANY(task.candidate_ids)
      WHERE task.task_id=ANY(source.task_ids)
    )
    OR EXISTS (
      SELECT 1
      FROM crawler.youtube_api_tasks task
      JOIN crawler.content_candidates candidate
        ON candidate.candidate_id=ANY(task.candidate_ids)
      JOIN crawler.channels channel ON channel.channel_id=candidate.channel_id
      WHERE task.task_id=ANY(source.task_ids)
        AND channel.status='active'
    )
  )`,
  raw_objects: `NOT (
    (
      source.entity_type='channel'
      AND EXISTS (
        SELECT 1 FROM crawler.channels channel
        WHERE channel.channel_id=source.entity_id AND channel.status<>'active'
      )
    )
    OR (
      source.entity_type='channel_run'
      AND EXISTS (
        SELECT 1
        FROM crawler.channel_runs run
        JOIN crawler.channels channel ON channel.channel_id=run.channel_id
        WHERE run.run_id=source.entity_id AND channel.status<>'active'
      )
    )
    OR (
      source.entity_type='youtube_api_batch'
      AND EXISTS (
        SELECT 1
        FROM crawler.youtube_api_batches batch
        JOIN crawler.youtube_api_tasks task ON task.task_id=ANY(batch.task_ids)
        JOIN crawler.content_candidates candidate
          ON candidate.candidate_id=ANY(task.candidate_ids)
        WHERE batch.batch_id=source.entity_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM crawler.youtube_api_batches batch
        JOIN crawler.youtube_api_tasks task ON task.task_id=ANY(batch.task_ids)
        JOIN crawler.content_candidates candidate
          ON candidate.candidate_id=ANY(task.candidate_ids)
        JOIN crawler.channels channel ON channel.channel_id=candidate.channel_id
        WHERE batch.batch_id=source.entity_id AND channel.status='active'
      )
    )
  )`,
  agent_profiles: `EXISTS (
    SELECT 1 FROM crawler.channels channel
    WHERE channel.channel_id=source.channel_id AND channel.status='active'
  )`,
  finalized_profiles: `EXISTS (
    SELECT 1 FROM crawler.channels channel
    WHERE channel.channel_id=source.channel_id AND channel.status='active'
  )`,
});

function tableIdentifier(tableName) {
  const normalized = String(tableName ?? "");
  if (!crawlerContentExportTables.includes(normalized)) {
    throw new TypeError(`unsupported Crawler export table: ${normalized}`);
  }
  return `crawler."${normalized}"`;
}

export function crawlerContentExportSelect(tableName) {
  const predicate = activeChannelPredicates[tableName];
  return `SELECT source.* FROM ${tableIdentifier(tableName)} source${predicate ? ` WHERE ${predicate}` : ""}`;
}

export function crawlerContentFilteredTables() {
  return crawlerContentExportTables.filter((tableName) => activeChannelPredicates[tableName]);
}
