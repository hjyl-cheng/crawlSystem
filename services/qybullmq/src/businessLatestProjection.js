// Latest business records are published in the same transaction as Search.
// Old immutable snapshots are never overwritten during adoption.
const TABLES = new Set([
  'public.channel_snapshots', 'public.content_snapshots', 'public.channel_links',
  'public.channel_profile_facts', 'public.channel_metric_values',
]);

export async function assertLatestProjectionMode(client) {
  const result = await client.query(`SELECT mode FROM publication.business_storage_state
    WHERE singleton`);
  if (result.rows[0]?.mode !== 'latest') throw new Error('BUSINESS_LATEST_STORAGE_NOT_ENABLED');
  await client.query("SELECT set_config('publication.business_writer_mode','latest',true)");
}

export async function assertLatestVersionOrder(client, versionVectors) {
  const rows = (await client.query(`SELECT channel_id,version_vector
    FROM publication.latest_projection_state WHERE channel_id=ANY($1::text[]) FOR UPDATE`,
  [Object.keys(versionVectors)])).rows;
  for (const row of rows) {
    const next = versionVectors[row.channel_id];
    for (const domain of ['channel', 'video', 'agent']) {
      const prior = row.version_vector[domain];
      if (!prior) continue;
      const entry = next[domain];
      if (!entry || entry.publication_stream_id !== prior.publication_stream_id
          || Number(entry.sequence) < Number(prior.sequence)
          || (Number(entry.sequence) === Number(prior.sequence)
            && (entry.revision_id !== prior.revision_id || entry.result_hash !== prior.result_hash))) {
        throw new Error(`BUSINESS_LATEST_VERSION_CONFLICT: ${row.channel_id}/${domain}`);
      }
    }
  }
}

async function upsertRows(client, table, rows) {
  if (!rows.length) return;
  if (!TABLES.has(table)) throw new Error('unsupported latest business table');
  const columns = (await client.query(`SELECT attname FROM pg_attribute
    WHERE attrelid=$1::regclass AND attnum>0 AND NOT attisdropped AND attgenerated=''
    ORDER BY attnum`, [table])).rows.map(row => row.attname);
  const quote = name => '"' + name.replaceAll('"', '""') + '"';
  const update = columns.filter(name => name !== 'id')
    .map(name => `${quote(name)}=excluded.${quote(name)}`).join(',');
  await client.query(`INSERT INTO ${table} AS existing
    SELECT (jsonb_populate_record(NULL::${table},value)).*
    FROM jsonb_array_elements($1::jsonb) value
    ON CONFLICT (id) DO UPDATE SET ${update}
    WHERE to_jsonb(existing) IS DISTINCT FROM to_jsonb(excluded)`, [JSON.stringify(rows)]);
}

export async function writeLatestProjectionRows(client, projections, batchId) {
  const upserts = projections.filter(item => item.action === 'upsert');
  for (const item of upserts) {
    if (!item.snapshot.id.startsWith('publication_current_snapshot_')) {
      throw new Error('BUSINESS_LATEST_SNAPSHOT_ID_REQUIRED');
    }
  }
  await upsertRows(client, 'public.channel_snapshots', upserts.map(item => item.snapshot));
  // The input is the fully merged accepted Video state, not a partial crawl.
  // All maintenance is batched: SQL round trips must not grow per channel.
  const targets = JSON.stringify(upserts.map(item => ({
    snapshot_id: item.snapshot.id,
    content_ids: item.contents.map(row => row.id),
    link_ids: item.links.map(row => row.id),
    fact_ids: item.facts.map(row => row.id),
    metric_ids: item.metrics.map(row => row.id),
  })));
  await client.query(`UPDATE public.content_snapshots content SET is_recent=false
    FROM jsonb_to_recordset($1::jsonb) target(snapshot_id text,content_ids text[])
    WHERE content.channel_snapshot_id=target.snapshot_id AND content.is_recent
      AND NOT (content.id=ANY(target.content_ids))`, [targets]);
  for (const [table, key] of [
    ['public.channel_links', 'link_ids'], ['public.channel_profile_facts', 'fact_ids'],
    ['public.channel_metric_values', 'metric_ids'],
  ]) {
    await client.query(`DELETE FROM ${table} existing
      USING jsonb_to_recordset($1::jsonb) target(snapshot_id text,${key} text[])
      WHERE existing.channel_snapshot_id=target.snapshot_id
        AND NOT (existing.id=ANY(target.${key}))`, [targets]);
  }
  await upsertRows(client, 'public.content_snapshots', upserts.flatMap(item => item.contents));
  await upsertRows(client, 'public.channel_links', upserts.flatMap(item => item.links));
  await upsertRows(client, 'public.channel_profile_facts', upserts.flatMap(item => item.facts));
  await upsertRows(client, 'public.channel_metric_values', upserts.flatMap(item => item.metrics));
  const history = upserts.map(item => ({
    channel_id: item.channelId,
    observed_at: item.snapshot.channel_observed_at ?? item.snapshot.captured_at,
    subscriber_count: item.snapshot.subscriber_count,
    subscriber_count_status: item.snapshot.subscriber_count_status,
    total_view_count: item.snapshot.total_view_count,
    total_view_count_status: item.snapshot.total_view_count_status,
  }));
  await client.query(`INSERT INTO publication.channel_metric_history
    SELECT (jsonb_populate_record(NULL::publication.channel_metric_history,value)).*
    FROM jsonb_array_elements($1::jsonb) value
    ON CONFLICT (channel_id,observed_at) DO NOTHING`, [JSON.stringify(history)]);
  await client.query(`WITH cutoffs AS MATERIALIZED (
    SELECT target.channel_id,point.observed_at
    FROM unnest($1::text[]) target(channel_id)
    CROSS JOIN LATERAL (SELECT observed_at FROM publication.channel_metric_history
      WHERE channel_id=target.channel_id ORDER BY observed_at DESC OFFSET 99 LIMIT 1) point
    ) DELETE FROM publication.channel_metric_history history USING cutoffs
    WHERE history.channel_id=cutoffs.channel_id AND history.observed_at<cutoffs.observed_at`,
  [upserts.map(item => item.channelId)]);
  await client.query(`INSERT INTO publication.latest_projection_state
    (channel_id,snapshot_id,batch_id,version_vector,projection_hash,is_removed)
    SELECT channel_id,snapshot_id,$2,version_vector,projection_hash,is_removed
    FROM jsonb_to_recordset($1::jsonb) value(channel_id text,snapshot_id text,
      version_vector jsonb,projection_hash text,is_removed boolean)
    ON CONFLICT (channel_id) DO UPDATE SET
      snapshot_id=excluded.snapshot_id,batch_id=excluded.batch_id,
      version_vector=excluded.version_vector,projection_hash=excluded.projection_hash,
      is_removed=excluded.is_removed,updated_at=clock_timestamp()`,
  [JSON.stringify(projections.map(item => ({
    channel_id: item.channelId, snapshot_id: item.snapshot?.id ?? null,
    version_vector: item.versionVector, projection_hash: item.projectionHash,
    is_removed: item.action === 'remove',
  }))), batchId]);
}
