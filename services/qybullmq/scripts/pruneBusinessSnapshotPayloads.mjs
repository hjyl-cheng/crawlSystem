import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { environmentValue } from '../src/runtimeEnvironment.js';

const PAYLOAD_TABLES = ['content_snapshots', 'channel_metric_values', 'channel_profile_facts', 'channel_links'];

// Keep snapshot headers: historical About observations and publication receipts
// still refer to them. Only redundant, superseded payloads are eligible here.
export async function inspectSnapshotPayloads(client, channelIds) {
  return (await client.query(`WITH target AS MATERIALIZED (
      SELECT state.channel_id,state.snapshot_id,
        (SELECT id FROM public.channel_snapshots old
         WHERE old.channel_id=state.channel_id AND old.id NOT LIKE 'publication_current_snapshot_%'
         ORDER BY captured_at DESC,id DESC LIMIT 1) AS retained_legacy_id
      FROM publication.latest_projection_state state
      JOIN public.creator_search_live live ON live.channel_id=state.channel_id
        AND live.snapshot_id=state.snapshot_id AND live.watermark=state.batch_id
      JOIN public.channel_snapshots current ON current.id=state.snapshot_id
        AND current.channel_id=state.channel_id
      WHERE state.channel_id=ANY($1::text[]) AND NOT state.is_removed
        AND state.snapshot_id LIKE 'publication_current_snapshot_%'
    )
    SELECT s.id,s.channel_id,target.snapshot_id AS current_id,target.retained_legacy_id
    FROM target JOIN public.channel_snapshots s ON s.channel_id=target.channel_id
    WHERE s.id<>target.snapshot_id AND s.id<>target.retained_legacy_id
      AND s.id NOT LIKE 'publication_current_snapshot_%'
      AND s.captured_at<(SELECT activated_at FROM publication.business_storage_state WHERE singleton)
      AND NOT EXISTS (SELECT 1 FROM public.creator_search_live WHERE snapshot_id=s.id)
      AND NOT EXISTS (SELECT 1 FROM public.creator_search_current WHERE snapshot_id=s.id)
      AND NOT EXISTS (SELECT 1 FROM publication.projection_snapshot_time_repair WHERE snapshot_id=s.id)
      AND NOT EXISTS (SELECT 1 FROM public.creator_category_claims WHERE channel_snapshot_id=s.id)
      AND NOT EXISTS (SELECT 1 FROM public.creator_classification_runs WHERE channel_snapshot_id=s.id)
      AND NOT EXISTS (SELECT 1 FROM public.channel_metric_values WHERE baseline_snapshot_id=s.id)
      AND (${PAYLOAD_TABLES.map(table => `EXISTS (SELECT 1 FROM public.${table} WHERE channel_snapshot_id=s.id)`).join(' OR ')})
    ORDER BY s.channel_id,s.captured_at,s.id`, [channelIds])).rows;
}

async function assertDatabase(client, database) {
  const identity = (await client.query(`SELECT current_database() AS database,database_name,database_kind
    FROM publication.database_identity WHERE singleton`)).rows[0];
  if (identity?.database !== database || identity.database_name !== database || identity.database_kind !== 'business') {
    throw new Error('unexpected business database');
  }
  const state = (await client.query(`SELECT mode,activated_at FROM publication.business_storage_state WHERE singleton`)).rows[0];
  const search = (await client.query(`SELECT read_mode,write_mode FROM publication.creator_search_storage_state WHERE singleton`)).rows[0];
  if (state?.mode !== 'latest' || !state.activated_at || search?.read_mode !== 'live' || search.write_mode !== 'incremental') {
    throw new Error('latest business storage and live Search must be enabled');
  }
  // New dependants require review rather than silently cascading evidence away.
  const dependants = (await client.query(`SELECT conrelid::regclass::text AS source,
      confrelid::regclass::text AS target,pg_get_constraintdef(oid) AS definition
    FROM pg_constraint WHERE contype='f' AND confrelid=ANY($1::regclass[])`,
  [PAYLOAD_TABLES.map(table => `public.${table}`)])).rows;
  for (const row of dependants) {
    if (row.source !== 'creator_content_tag_evidence' || row.target !== 'content_snapshots'
        || !row.definition.startsWith('FOREIGN KEY (content_snapshot_id, channel_snapshot_id, channel_id)')) {
      throw new Error(`unreviewed snapshot payload dependency: ${row.source}`);
    }
  }
}

async function protectedSignature(client, channelIds, ids) {
  const result = {};
  for (const table of ['channel_snapshots', ...PAYLOAD_TABLES]) {
    result[table] = (await client.query(`SELECT md5(COALESCE(string_agg(to_jsonb(row)::text,E'\\n' ORDER BY id),'')) AS hash
      FROM public.${table} row WHERE ${table === 'channel_snapshots' ? 'id' : 'channel_snapshot_id'}=ANY($1::text[])`, [ids])).rows[0].hash;
  }
  result.headers = (await client.query(`SELECT md5(COALESCE(string_agg(to_jsonb(row)::text,E'\\n' ORDER BY id),'')) AS hash
    FROM public.channel_snapshots row WHERE channel_id=ANY($1::text[])`, [channelIds])).rows[0].hash;
  result.search = (await client.query(`SELECT md5(COALESCE(string_agg(to_jsonb(row)::text,E'\\n' ORDER BY channel_id),'')) AS hash
    FROM public.creator_search_live row WHERE channel_id=ANY($1::text[])`, [channelIds])).rows[0].hash;
  result.trends = (await client.query(`SELECT target.channel_id,to_jsonb(point) AS point
    FROM public.creator_search_live target
    CROSS JOIN LATERAL public.creator_channel_metric_history_v1(target.channel_id,target.captured_at,100) point
    WHERE target.channel_id=ANY($1::text[]) ORDER BY target.channel_id,point.captured_at`, [channelIds])).rows;
  return JSON.stringify(result);
}

export async function pruneSnapshotPayloadBatch(client, { database, channelIds, apply = false, rollback = false }) {
  if (!database || !Array.isArray(channelIds) || channelIds.length > 10 || !channelIds.length) {
    throw new Error('database and 1–10 channel IDs are required');
  }
  await client.query(apply ? 'BEGIN' : 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
  try {
    await client.query("SET LOCAL lock_timeout='1s'; SET LOCAL statement_timeout='20s'; SET LOCAL idle_in_transaction_session_timeout='30s'");
    await assertDatabase(client, database);
    let locked = channelIds;
    if (apply) {
      // Same first lock as publication/activation; do not wait on busy channels.
      locked = (await client.query(`SELECT channel_id FROM publication.channel_ownership
        WHERE channel_id=ANY($1::text[]) ORDER BY channel_id FOR UPDATE SKIP LOCKED`, [channelIds])).rows.map(row => row.channel_id);
    }
    let candidates = await inspectSnapshotPayloads(client, locked);
    if (apply) candidates = candidates.slice(0, 100);
    if (apply && candidates.length) {
      // Block new evidence references, then recheck references on a fresh snapshot.
      await client.query(`SELECT id FROM public.channel_snapshots WHERE id=ANY($1::text[])
        ORDER BY id FOR UPDATE`, [candidates.map(row => row.id)]);
      const fresh = new Set((await inspectSnapshotPayloads(client, locked)).map(row => row.id));
      candidates = candidates.filter(row => fresh.has(row.id));
    }
    const ids = candidates.map(row => row.id);
    const counts = {};
    for (const table of PAYLOAD_TABLES) {
      counts[table] = Number((await client.query(`SELECT count(*) AS count FROM public.${table}
        WHERE channel_snapshot_id=ANY($1::text[])`, [ids])).rows[0].count);
    }
    const retained = [...new Set(candidates.flatMap(row => [row.current_id, row.retained_legacy_id]))];
    if (apply && ids.length) {
      const before = await protectedSignature(client, locked, retained);
      // The evidence's run must protect the whole source snapshot. Check directly too.
      const evidence = await client.query(`SELECT 1 FROM public.creator_content_tag_evidence evidence
        JOIN public.content_snapshots content ON content.id=evidence.content_snapshot_id
        WHERE content.channel_snapshot_id=ANY($1::text[]) LIMIT 1`, [ids]);
      if (evidence.rowCount) throw new Error('snapshot content has classification evidence');
      for (const table of PAYLOAD_TABLES) {
        const deleted = await client.query(`DELETE FROM public.${table} WHERE channel_snapshot_id=ANY($1::text[])`, [ids]);
        if (deleted.rowCount !== counts[table]) throw new Error(`payload changed during cleanup: ${table}`);
      }
      if (before !== await protectedSignature(client, locked, retained)) {
        throw new Error('current data, retained payload or trend changed during cleanup');
      }
    }
    await client.query(rollback ? 'ROLLBACK' : 'COMMIT');
    return { outcome: !apply ? 'dry_run' : rollback ? 'rolled_back' : 'committed',
      channels: locked, skipped: channelIds.filter(id => !locked.includes(id)), snapshots: candidates, counts };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  }
}

async function main() {
  const database = process.env.EXPECTED_BUSINESS_DATABASE;
  const manifestPath = process.env.SNAPSHOT_CLEANUP_CHANNELS_FILE;
  const apply = process.argv.includes('--apply');
  if (!database || !manifestPath) throw new Error('EXPECTED_BUSINESS_DATABASE and SNAPSHOT_CLEANUP_CHANNELS_FILE are required');
  if (apply && process.env.CONFIRM_SNAPSHOT_PAYLOAD_CLEANUP !== database) {
    throw new Error('apply requires matching CONFIRM_SNAPSHOT_PAYLOAD_CLEANUP');
  }
  const channels = JSON.parse(await readFile(manifestPath, 'utf8'));
  if (!Array.isArray(channels) || channels.some(id => typeof id !== 'string' || !id.trim()) || new Set(channels).size !== channels.length) {
    throw new Error('manifest must be a unique array of channel IDs');
  }
  const client = new pg.Client({ connectionString: environmentValue('BUSINESS_DATABASE_URL'), application_name: 'business-snapshot-payload-cleanup' });
  const totals = { snapshots: 0, counts: Object.fromEntries(PAYLOAD_TABLES.map(table => [table, 0])), skipped: [] };
  try {
    await client.connect();
    for (let offset = 0; offset < channels.length; offset += 10) {
      const started = Date.now();
      const batch = await pruneSnapshotPayloadBatch(client, { database, channelIds: channels.slice(offset, offset + 10), apply,
        rollback: process.argv.includes('--rollback') });
      totals.snapshots += batch.snapshots.length;
      totals.skipped.push(...batch.skipped);
      for (const table of PAYLOAD_TABLES) totals.counts[table] += batch.counts[table];
      console.log(JSON.stringify({ at: new Date().toISOString(), offset, ms: Date.now() - started, ...batch }));
      if (apply) await new Promise(resolve => setTimeout(resolve, 250));
      // Finish this bounded channel group through separate short transactions.
      if (apply && !process.argv.includes('--rollback') && batch.snapshots.length === 100) offset -= 10;
    }
    console.log(JSON.stringify({ at: new Date().toISOString(), totals }));
  } finally { await client.end(); }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
