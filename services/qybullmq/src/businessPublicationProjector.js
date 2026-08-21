import { randomUUID } from "node:crypto";
import {
  BUSINESS_PROJECTION_ADAPTER_VERSION,
  buildBusinessPublicationProjection,
} from "./businessPublicationProjectionAdapter.js";
import {
  businessPublicationVersionVectorStreamId,
  loadBusinessPublicationVersionState,
  normalizeBusinessPublicationVersionVector,
} from "./businessPublicationVersionState.js";
import { observationFactsHash } from "./crawlObservationStore.js";

const CLAIMABLE_STATUSES = Object.freeze(["pending", "retry_wait", "leased"]);

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function integerOption(value, fallback, field, { minimum, maximum }) {
  if (value == null || String(value).trim() === "") return fallback;
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < minimum || output > maximum) {
    throw new TypeError(`${field} must be an integer between ${minimum} and ${maximum}`);
  }
  return output;
}

function uniqueSorted(values) {
  return [...new Set(values.map((value) => requiredText(value, "channelId")))].sort((left, right) => (
    Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"))
  ));
}

function rowMap(rows, key = "channel_id") {
  return new Map(rows.map((row) => [row[key], row.row ?? row]));
}

function groupRows(rows, key = "channel_id", valueKey = "row") {
  const output = new Map();
  for (const source of rows) {
    const values = output.get(source[key]) ?? [];
    values.push(source[valueKey] ?? source);
    output.set(source[key], values);
  }
  return output;
}

function maxTimestamp(values) {
  return values.filter(Boolean).map((value) => new Date(value).toISOString()).sort().at(-1);
}

function errorText(error) {
  const code = String(error?.code ?? "projection_failed").trim();
  const message = String(error?.message || error).trim();
  return `${code}: ${message}`.slice(0, 2000);
}

function semanticBatch({ streamId, versionVectors }) {
  const semantic = {
    adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
    publication_stream_id: streamId,
    channels: Object.entries(versionVectors).map(([channelId, versionVector]) => ({
      channel_id: channelId,
      version_vector: versionVector,
    })),
  };
  const sourceHash = observationFactsHash(semantic).slice("sha256:".length);
  return {
    semantic,
    sourceHash,
    batchId: `publication_projection_${sourceHash.slice(0, 40)}`,
  };
}

async function lockOwnership(client, channelIds, { lock = true } = {}) {
  const result = await client.query(
    `/* business-publication-projector:ownership */
     SELECT channel_id,active_publication_stream_id,status,projection_mode
     FROM publication.channel_ownership
     WHERE channel_id=ANY($1::text[])
     ORDER BY channel_id
     ${lock ? "FOR UPDATE" : ""}`,
    [channelIds],
  );
  if (result.rows.length !== channelIds.length) {
    const found = new Set(result.rows.map((row) => row.channel_id));
    throw new Error(`Projection ownership is missing: ${channelIds.filter((id) => !found.has(id)).join(",")}`);
  }
  for (const row of result.rows) {
    if (row.status !== "active") throw new Error(`Projection ownership is not active: ${row.channel_id}`);
  }
  return result.rows;
}

async function loadVersionVectors(client, channelIds) {
  const result = await client.query(
    `/* business-publication-projector:version-vectors */
     SELECT target.channel_id,cursor.domain,cursor.publication_stream_id,
            cursor.active_sequence,cursor.active_revision_id,cursor.active_result_hash
     FROM unnest($1::text[]) target(channel_id)
     LEFT JOIN publication.consumer_cursor cursor ON cursor.channel_id=target.channel_id
     ORDER BY target.channel_id,cursor.domain`,
    [channelIds],
  );
  const vectors = Object.fromEntries(channelIds.map((channelId) => [channelId, {
    channel: null,
    video: null,
    agent: null,
  }]));
  for (const row of result.rows) {
    if (!row.domain) continue;
    vectors[row.channel_id][row.domain] = {
      publication_stream_id: String(row.publication_stream_id),
      sequence: Number(row.active_sequence),
      revision_id: String(row.active_revision_id),
      result_hash: row.active_result_hash,
    };
  }
  return vectors;
}

async function loadPublishedVersionVectors(client, channelIds) {
  const result = await client.query(
    `/* business-publication-projector:published-version-vectors */
     SELECT target.channel_id,
            CASE WHEN batch.batch_id IS NULL THEN NULL ELSE item.version_vector END AS version_vector
     FROM unnest($1::text[]) target(channel_id)
     LEFT JOIN public.creator_search_live search
       ON search.channel_id=target.channel_id
     LEFT JOIN publication.projection_batch_item item
       ON item.channel_id=search.channel_id AND item.snapshot_id=search.snapshot_id
     LEFT JOIN publication.projection_batch batch
       ON batch.batch_id=item.batch_id AND batch.status='published'
     ORDER BY target.channel_id`,
    [channelIds],
  );
  return new Map(result.rows.map((row) => [
    row.channel_id,
    row.version_vector ?? null,
  ]));
}

async function findAlreadyAbsentRetractions(client, versionVectors) {
  const targets = Object.entries(versionVectors).map(([channelId, versionVector]) => ({
    channel_id: channelId,
    version_vector: versionVector,
  }));
  if (targets.length === 0) return new Set();
  const result = await client.query(
    `/* business-publication-projector:covered-retractions */
     WITH target AS (
       SELECT * FROM jsonb_to_recordset($1::jsonb) AS value(
         channel_id text,version_vector jsonb
       )
     )
     SELECT target.channel_id
     FROM target
     JOIN publication.revision revision
       ON revision.revision_id=(target.version_vector #>> '{channel,revision_id}')::uuid
      AND revision.publication_stream_id::text=
          target.version_vector #>> '{channel,publication_stream_id}'
      AND revision.channel_id=target.channel_id
      AND revision.domain='channel'
      AND revision.data_sequence=(target.version_vector #>> '{channel,sequence}')::bigint
      AND revision.result_hash=target.version_vector #>> '{channel,result_hash}'
      AND revision.revision_type='retraction'
      AND revision.operation='retract_channel'
     LEFT JOIN public.creator_search_live search USING(channel_id)
     WHERE search.channel_id IS NULL
     ORDER BY target.channel_id`,
    [JSON.stringify(targets)],
  );
  return new Set(result.rows.map((row) => row.channel_id));
}

function sameVersionVector(left, right) {
  return left != null
    && right != null
    && observationFactsHash(left) === observationFactsHash(right);
}

function earliestProjectionRows(projectionRows, channelIds) {
  const requested = new Set(channelIds);
  const earliest = new Map();
  for (const row of projectionRows) {
    if (requested.has(row.channel_id) && !earliest.has(row.channel_id)) {
      earliest.set(row.channel_id, row);
    }
  }
  return earliest;
}

function projectionRowsForVersionVectors(projectionRows, versionVectors) {
  return projectionRows.filter((row) => (
    versionVectors[row.channel_id]
      && sameVersionVector(row.version_vector, versionVectors[row.channel_id])
  ));
}

export async function findBusinessPublicationVersionVectorCoverage(client, versionVectors) {
  const targets = Object.entries(versionVectors).map(([channelId, versionVector]) => ({
    channel_id: channelId,
    version_vector: versionVector,
  }));
  if (targets.length === 0) return new Map();
  const result = await client.query(
    `/* business-publication-projector:projected-version-vector-coverage */
     WITH target AS (
       SELECT * FROM jsonb_to_recordset($1::jsonb) AS value(
         channel_id text,version_vector jsonb
       )
     )
     SELECT DISTINCT ON (target.channel_id)
            target.channel_id,item.batch_id,item.action,item.snapshot_id
     FROM target
     JOIN publication.projection_batch_item item
       ON item.channel_id=target.channel_id
      AND item.version_vector=target.version_vector
     JOIN publication.projection_batch batch
       ON batch.batch_id=item.batch_id AND batch.status='published'
     LEFT JOIN public.channel_snapshots snapshot
       ON snapshot.id=item.snapshot_id AND snapshot.channel_id=item.channel_id
     WHERE item.action='remove' OR snapshot.id IS NOT NULL
     ORDER BY target.channel_id,item.projected_at,item.batch_id`,
    [JSON.stringify(targets)],
  );
  return new Map(result.rows.map((row) => [row.channel_id, row]));
}

function emptyPrevious(channelIds) {
  return {
    snapshots: new Map(channelIds.map((channelId) => [channelId, null])),
    links: new Map(),
    contents: new Map(),
    facts: new Map(),
  };
}

async function loadCurrent(client, channelIds) {
  const channels = await client.query(
      `/* business-publication-projector:entity-current */
       SELECT current_row.channel_id,
              to_jsonb(current_row)||jsonb_build_object(
                'source_observed_at',CASE
                  WHEN revision.revision_type='retraction'
                    THEN revision.source_json #>> '{terminal_channel,removed_at}'
                  ELSE revision.source_json #>> '{complete_observation,observed_at}'
                END
              ) AS row
       FROM result.entity_current current_row
       LEFT JOIN publication.revision revision
         ON revision.revision_id=current_row.active_revision_id
        AND revision.channel_id=current_row.channel_id
        AND revision.domain='channel'
       WHERE current_row.channel_id=ANY($1::text[])`,
      [channelIds],
    );
  const videos = await client.query(
      `/* business-publication-projector:video-current */
       SELECT channel_id,to_jsonb(current_row) AS row
       FROM result.video_current current_row
       WHERE channel_id=ANY($1::text[])`,
      [channelIds],
    );
  const agents = await client.query(
      `/* business-publication-projector:agent-current */
       SELECT channel_id,to_jsonb(current_row) AS row
       FROM result.agent_current current_row
       WHERE channel_id=ANY($1::text[])`,
      [channelIds],
    );
  const contents = await client.query(
      `/* business-publication-projector:content-current */
       SELECT channel_id,to_jsonb(current_row) AS row
       FROM result.content_current current_row
       WHERE channel_id=ANY($1::text[]) AND window_status='active'
       ORDER BY channel_id,position,content_id`,
      [channelIds],
    );
  return {
    channels: rowMap(channels.rows),
    videos: rowMap(videos.rows),
    agents: rowMap(agents.rows),
    contents: groupRows(contents.rows),
  };
}

async function loadPrevious(client, channelIds) {
  const snapshots = await client.query(
    `/* business-publication-projector:active-snapshots */
     SELECT target.channel_id,
            CASE WHEN snapshot.id IS NULL THEN NULL ELSE to_jsonb(snapshot) END AS row
     FROM unnest($1::text[]) target(channel_id)
     LEFT JOIN public.creator_search_live search
       ON search.channel_id=target.channel_id
     LEFT JOIN public.channel_snapshots snapshot
       ON snapshot.id=search.snapshot_id AND snapshot.channel_id=search.channel_id
     ORDER BY target.channel_id`,
    [channelIds],
  );
  const snapshotMap = rowMap(snapshots.rows);
  const snapshotIds = snapshots.rows.map((row) => row.row?.id).filter(Boolean);
  if (snapshotIds.length === 0) {
    return { snapshots: snapshotMap, links: new Map(), contents: new Map(), facts: new Map() };
  }
  const links = await client.query(
      `/* business-publication-projector:active-links */
       SELECT link.channel_id,to_jsonb(link) AS row
       FROM public.channel_links link
       WHERE link.channel_snapshot_id=ANY($1::text[])
       ORDER BY link.channel_id,link.id`,
      [snapshotIds],
    );
  const contents = await client.query(
      `/* business-publication-projector:active-contents */
       SELECT content.channel_id,
              to_jsonb(content)||jsonb_build_object(
                'captured_at',snapshot.captured_at,
                'item_url',item.url,
                'item_first_seen_at',item.first_seen_at,
                'item_last_seen_at',item.last_seen_at
              ) AS row
       FROM public.content_snapshots content
       JOIN public.channel_snapshots snapshot ON snapshot.id=content.channel_snapshot_id
       JOIN public.content_items item
         ON item.video_id=content.video_id AND item.channel_id=content.channel_id
       WHERE content.channel_snapshot_id=ANY($1::text[]) AND content.is_canonical=true
       ORDER BY content.channel_id,content.source_position NULLS LAST,content.id`,
      [snapshotIds],
    );
  const facts = await client.query(
      `/* business-publication-projector:active-facts */
       SELECT fact.channel_id,to_jsonb(fact) AS row
       FROM public.channel_profile_facts fact
       WHERE fact.channel_snapshot_id=ANY($1::text[])
       ORDER BY fact.channel_id,fact.field_key`,
      [snapshotIds],
    );
  return {
    snapshots: snapshotMap,
    links: groupRows(links.rows),
    contents: groupRows(contents.rows),
    facts: groupRows(facts.rows),
  };
}

async function activeWatermark(client) {
  const result = await client.query(
    "SELECT watermark FROM public.creator_search_active WHERE singleton=true",
  );
  return result.rows[0]?.watermark ?? null;
}

function assembleProjections({ channelIds, current, previous, versionVectors, batchId, capturedAt }) {
  return channelIds.map((channelId) => buildBusinessPublicationProjection({
    channelId,
    batchId,
    versionVector: versionVectors[channelId],
    capturedAt,
    current: {
      channel: current.channels.get(channelId) ?? null,
      video: current.videos.get(channelId) ?? null,
      agent: current.agents.get(channelId) ?? null,
      contents: current.contents.get(channelId) ?? [],
    },
    previous: {
      snapshot: previous.snapshots.get(channelId) ?? null,
      links: previous.links.get(channelId) ?? [],
      contents: previous.contents.get(channelId) ?? [],
      facts: previous.facts.get(channelId) ?? [],
    },
  }));
}

async function insertCompositeRows(client, table, rows) {
  if (rows.length === 0) return;
  await client.query(
    `INSERT INTO ${table}
     SELECT (jsonb_populate_record(NULL::${table},value)).*
     FROM jsonb_array_elements($1::jsonb) value`,
    [JSON.stringify(rows)],
  );
}

async function insertIdentities(client, projections) {
  const channels = projections.filter((item) => item.action === "upsert").map((item) => ({
    channel_id: item.channelId,
    first_seen_at: item.snapshot.captured_at,
    last_seen_at: item.snapshot.captured_at,
    last_handle: item.snapshot.handle,
    last_title: item.snapshot.title,
  }));
  if (channels.length > 0) {
    await client.query(
      `/* business-publication-projector:channels */
       INSERT INTO public.channels(channel_id,first_seen_at,last_seen_at,last_handle,last_title)
       SELECT channel_id,first_seen_at,last_seen_at,last_handle,last_title
       FROM jsonb_to_recordset($1::jsonb) AS source(
         channel_id text,first_seen_at timestamptz,last_seen_at timestamptz,
         last_handle text,last_title text
       )
       ON CONFLICT (channel_id) DO UPDATE
       SET last_seen_at=GREATEST(public.channels.last_seen_at,excluded.last_seen_at),
           last_handle=COALESCE(excluded.last_handle,public.channels.last_handle),
           last_title=COALESCE(excluded.last_title,public.channels.last_title)`,
      [JSON.stringify(channels)],
    );
  }
  const contents = projections.flatMap((item) => item.contentItems ?? []);
  if (contents.length > 0) {
    const conflicts = await client.query(
      `SELECT existing.video_id,existing.channel_id AS existing_channel_id,
              incoming.channel_id AS incoming_channel_id
       FROM jsonb_to_recordset($1::jsonb) incoming(video_id text,channel_id text)
       JOIN public.content_items existing ON existing.video_id=incoming.video_id
       WHERE existing.channel_id<>incoming.channel_id`,
      [JSON.stringify(contents)],
    );
    if (conflicts.rows.length > 0) {
      throw new Error(`Content identity owner conflict: ${JSON.stringify(conflicts.rows.slice(0, 10))}`);
    }
    await client.query(
      `/* business-publication-projector:content-identities */
       INSERT INTO public.content_items(video_id,channel_id,url,first_seen_at,last_seen_at)
       SELECT video_id,channel_id,url,first_seen_at,last_seen_at
       FROM jsonb_to_recordset($1::jsonb) AS source(
         video_id text,channel_id text,url text,first_seen_at timestamptz,last_seen_at timestamptz
       )
       ON CONFLICT (video_id) DO UPDATE
       SET url=COALESCE(excluded.url,public.content_items.url),
           first_seen_at=LEAST(public.content_items.first_seen_at,excluded.first_seen_at),
           last_seen_at=GREATEST(public.content_items.last_seen_at,excluded.last_seen_at)`,
      [JSON.stringify(contents)],
    );
  }
}

async function insertProjectionRows(client, projections) {
  const upserts = projections.filter((item) => item.action === "upsert");
  await insertIdentities(client, upserts);
  await insertCompositeRows(client, "public.channel_snapshots", upserts.map((item) => item.snapshot));
  await insertCompositeRows(client, "public.channel_links", upserts.flatMap((item) => item.links));
  await insertCompositeRows(client, "public.content_snapshots", upserts.flatMap((item) => item.contents));
  await insertCompositeRows(client, "public.channel_profile_facts", upserts.flatMap((item) => item.facts));
  await insertCompositeRows(client, "public.channel_metric_values", upserts.flatMap((item) => item.metrics));
}

function projectionRowCounts(projections) {
  const upserts = projections.filter((item) => item.action === "upsert");
  return {
    projection_channels: projections.length,
    channel_snapshots: upserts.length,
    channel_links: upserts.reduce((sum, item) => sum + item.links.length, 0),
    content_items: new Set(upserts.flatMap((item) => item.contentItems.map((row) => row.video_id))).size,
    content_snapshots: upserts.reduce((sum, item) => sum + item.contents.length, 0),
    channel_profile_facts: upserts.reduce((sum, item) => sum + item.facts.length, 0),
    channel_metric_values: upserts.reduce((sum, item) => sum + item.metrics.length, 0),
    removed_channels: projections.length - upserts.length,
  };
}

async function insertBatch(client, {
  batch,
  streamId,
  versionVectors,
  projections,
  previousWatermark,
  capturedAt,
}) {
  const upsertIds = projections.filter((item) => item.action === "upsert").map((item) => item.channelId);
  const removedIds = projections.filter((item) => item.action === "remove").map((item) => item.channelId);
  const rowCounts = projectionRowCounts(projections);
  await client.query(
    `/* business-publication-projector:import-batch */
     INSERT INTO public.import_batches (
       id,source_file,source_sha256,captured_at,schema_version,raw_payload,
       parse_warnings,source_kind,status,row_counts,error_message
     ) VALUES ($1,$2,$3,$4,1,$5::jsonb,'[]'::jsonb,'publication_projection',
               'loading',$6::jsonb,NULL)`,
    [
      batch.batchId,
      `publication-projection://${batch.batchId}`,
      batch.sourceHash,
      capturedAt,
      JSON.stringify(batch.semantic),
      JSON.stringify(rowCounts),
    ],
  );
  await client.query(
    `/* business-publication-projector:projection-batch */
     INSERT INTO publication.projection_batch (
       batch_id,source_sha256,publication_stream_id,adapter_version,status,
       version_vectors,upsert_channel_ids,removed_channel_ids,projection_count,
       previous_watermark
     ) VALUES ($1,$2,$3::uuid,$4,'loading',$5::jsonb,$6::text[],$7::text[],$8,$9)`,
    [
      batch.batchId,
      batch.sourceHash,
      streamId,
      BUSINESS_PROJECTION_ADAPTER_VERSION,
      JSON.stringify(versionVectors),
      upsertIds,
      removedIds,
      projections.length,
      previousWatermark,
    ],
  );
  await insertCompositeRows(client, "publication.projection_batch_item", projections.map((item) => ({
    batch_id: batch.batchId,
    channel_id: item.channelId,
    action: item.action,
    snapshot_id: item.snapshot?.id ?? null,
    previous_snapshot_id: item.action === "upsert"
      ? item.snapshot.raw_channel?.carried_forward_from_snapshot_id ?? null
      : null,
    version_vector: item.versionVector,
    projection_hash: item.projectionHash,
    projected_at: capturedAt,
  })));
  return { upsertIds, removedIds, rowCounts };
}

async function publishBatch(client, { batchId, upsertIds, removedIds }) {
  await client.query(
    `UPDATE public.import_batches
     SET status='published',imported_at=now()
     WHERE id=$1 AND status='loading'`,
    [batchId],
  );
  await client.query(
    `UPDATE publication.projection_batch
     SET status='published',published_at=now()
     WHERE batch_id=$1 AND status='loading'`,
    [batchId],
  );
  await client.query(
    "SELECT public.refresh_creator_search_release_v9($1,$2::text[],$3::text[])",
    [batchId, upsertIds, removedIds],
  );
}

async function reusablePublishedBatch(client, { batch, streamId, versionVectors, channelIds }) {
  const existing = (await client.query(
    `/* business-publication-projector:reusable-batch */
     SELECT projection.batch_id,projection.source_sha256,
            projection.publication_stream_id,projection.adapter_version,
            projection.status,projection.version_vectors,
            projection.upsert_channel_ids,projection.removed_channel_ids,
            projection.projection_count,projection.previous_watermark,
            imported.status AS import_status,imported.row_counts,
            release.status AS release_status,
            release.storage_mode AS release_storage_mode
     FROM publication.projection_batch projection
     LEFT JOIN public.import_batches imported ON imported.id=projection.batch_id
     LEFT JOIN public.creator_search_releases release ON release.watermark=projection.batch_id
     WHERE projection.batch_id=$1`,
    [batch.batchId],
  )).rows[0];
  if (!existing) return null;
  const existingChannelIds = uniqueSorted([
    ...existing.upsert_channel_ids,
    ...existing.removed_channel_ids,
  ]);
  if (
    existing.source_sha256 !== batch.sourceHash
      || String(existing.publication_stream_id) !== streamId
      || existing.adapter_version !== BUSINESS_PROJECTION_ADAPTER_VERSION
      || existing.status !== "published"
      || existing.import_status !== "published"
      || !["active", "retired"].includes(existing.release_status)
      || Number(existing.projection_count) !== channelIds.length
      || observationFactsHash(existing.version_vectors) !== observationFactsHash(versionVectors)
      || observationFactsHash(existingChannelIds) !== observationFactsHash(channelIds)
  ) {
    throw new Error(`Existing Projection Batch does not match its semantic identity: ${batch.batchId}`);
  }
  const items = (await client.query(
    `SELECT channel_id,action,snapshot_id,projection_hash,version_vector
     FROM publication.projection_batch_item
     WHERE batch_id=$1 ORDER BY channel_id`,
    [batch.batchId],
  )).rows;
  if (items.length !== channelIds.length) {
    throw new Error(`Existing Projection Batch is incomplete: ${batch.batchId}`);
  }
  return { ...existing, items };
}

async function reactivatePublishedBatch(client, existing) {
  await client.query("SELECT pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'))");
  const currentWatermark = await activeWatermark(client);
  if (currentWatermark === existing.batch_id) return;
  if (currentWatermark !== existing.previous_watermark) {
    throw new Error(
      `Existing Projection Batch predecessor differs: ${currentWatermark} != ${existing.previous_watermark}`,
    );
  }
  if (existing.release_storage_mode) {
    await client.query(
      "SELECT public.replay_creator_search_release_v9($1)",
      [existing.batch_id],
    );
    return;
  }
  await client.query(
    "SELECT public.restore_creator_search_live_from_legacy_v1($1)",
    [existing.batch_id],
  );
  await client.query(
    `UPDATE public.creator_search_releases
     SET status='retired'
     WHERE status='active' AND watermark<>$1`,
    [existing.batch_id],
  );
  const activated = await client.query(
    `UPDATE public.creator_search_releases
     SET status='active',activated_at=COALESCE(activated_at,clock_timestamp()),
         generation=generation+1,rebuilt_at=clock_timestamp()
     WHERE watermark=$1 AND status='retired'
     RETURNING watermark`,
    [existing.batch_id],
  );
  if (activated.rows.length !== 1) {
    throw new Error(`Existing Projection Search release cannot be reactivated: ${existing.batch_id}`);
  }
  await client.query(
    `UPDATE public.creator_search_active SET watermark=$1 WHERE singleton=true`,
    [existing.batch_id],
  );
}

async function markDelivered(client, projectionRows, batchId) {
  if (projectionRows.length === 0) return 0;
  const projectionIds = projectionRows.map((row) => row.projection_id);
  const result = await client.query(
    `/* business-publication-projector:delivered */
     UPDATE publication.projection_outbox
     SET status='delivered',delivered_at=now(),lease_owner=NULL,lease_expires_at=NULL,
         last_error=NULL,updated_at=now()
     WHERE projection_id=ANY($1::uuid[])
       AND status IN ('pending','retry_wait','leased')
     RETURNING projection_id,released_by_cutover_id`,
    [projectionIds],
  );
  if (result.rows.length !== projectionIds.length) {
    throw new Error("Projection Outbox changed while its Channel ownership lock was held");
  }
  const cutoverIds = [...new Set(result.rows.map((row) => row.released_by_cutover_id).filter(Boolean))];
  if (cutoverIds.length > 0) {
    await client.query(
      `UPDATE publication.projection_cutover
       SET first_projection_watermark=COALESCE(first_projection_watermark,$1),
           last_projection_watermark=$1
       WHERE cutover_id=ANY($2::text[]) AND status='applied'`,
      [batchId, cutoverIds],
    );
  }
  return result.rows.length;
}

async function openProjectionRows(client, channelIds, statuses) {
  const result = await client.query(
    `/* business-publication-projector:open-outbox */
     SELECT projection_id,activation_id,publication_stream_id,channel_id,
            version_vector,status,attempts,created_at,released_by_cutover_id
     FROM publication.projection_outbox
     WHERE channel_id=ANY($1::text[]) AND status=ANY($2::text[])
     ORDER BY channel_id,created_at,projection_id
     FOR UPDATE`,
    [channelIds, statuses],
  );
  return result.rows;
}

export async function projectBusinessPublicationChannels(client, channelIdsValue, {
  projectionStatuses = CLAIMABLE_STATUSES,
  markOutbox = true,
  lock = true,
  capturedAt = null,
  versionVectors: explicitVersionVectorsValue = null,
  historical = false,
} = {}) {
  if (!client || typeof client.query !== "function") throw new TypeError("a PostgreSQL client is required");
  const channelIds = uniqueSorted(channelIdsValue);
  if (channelIds.length === 0) return { outcome: "no_work", projected: 0, delivered: 0 };
  if (historical && (markOutbox || explicitVersionVectorsValue == null)) {
    throw new TypeError("historical Projection requires explicit Version Vectors without Outbox mutation");
  }
  const owners = await lockOwnership(client, channelIds, { lock });
  if (markOutbox) {
    for (const owner of owners) {
      if (owner.projection_mode !== "online") {
        throw new Error(`Projection is not online: ${owner.channel_id}`);
      }
    }
  }
  const projectionRows = await openProjectionRows(client, channelIds, projectionStatuses);
  const earliestRows = earliestProjectionRows(projectionRows, channelIds);
  const requestedTargetIds = markOutbox
    ? channelIds.filter((channelId) => earliestRows.has(channelId))
    : channelIds;
  if (requestedTargetIds.length === 0) return { outcome: "no_work", projected: 0, delivered: 0 };
  let requestedVersionVectors;
  if (markOutbox) {
    requestedVersionVectors = Object.fromEntries(requestedTargetIds.map((channelId) => [
      channelId,
      normalizeBusinessPublicationVersionVector(
        earliestRows.get(channelId).version_vector,
        `Projection Outbox ${earliestRows.get(channelId).projection_id} Version Vector`,
      ),
    ]));
  } else if (explicitVersionVectorsValue != null) {
    requestedVersionVectors = Object.fromEntries(requestedTargetIds.map((channelId) => {
      const value = explicitVersionVectorsValue[channelId];
      if (value == null) throw new TypeError(`Version Vector is missing: ${channelId}`);
      return [channelId, normalizeBusinessPublicationVersionVector(
        value,
        `Explicit Version Vector ${channelId}`,
      )];
    }));
  } else {
    requestedVersionVectors = await loadVersionVectors(client, requestedTargetIds);
  }
  const deliverableRows = markOutbox
    ? projectionRowsForVersionVectors(projectionRows, requestedVersionVectors)
    : [];
  const targetOwners = owners.filter((owner) => requestedTargetIds.includes(owner.channel_id));
  const ownerStreamIds = [...new Set(
    targetOwners.map((row) => String(row.active_publication_stream_id)),
  )];
  if (!historical && ownerStreamIds.length !== 1) {
    throw new Error("A Projection Batch must use exactly one active Ownership Stream");
  }
  const vectorStreamIds = [...new Set(requestedTargetIds.map((channelId) => (
    businessPublicationVersionVectorStreamId(
      requestedVersionVectors[channelId],
      `Projection Version Vector ${channelId}`,
    )
  )))];
  if (vectorStreamIds.length !== 1) {
    throw new Error("A Projection Batch must use exactly one Publication Stream");
  }
  const streamId = historical ? vectorStreamIds[0] : ownerStreamIds[0];
  if (!historical && vectorStreamIds[0] !== streamId) {
    throw new Error("Projection Outbox must match the active Publication Stream");
  }
  const openStreamIds = [...new Set(projectionRows
    .filter((row) => requestedTargetIds.includes(row.channel_id))
    .map((row) => String(row.publication_stream_id)))];
  if (markOutbox && (
    openStreamIds.length !== 1
      || openStreamIds[0] !== streamId
  )) {
    throw new Error("Projection Outbox must match the active Publication Stream");
  }
  await client.query("SELECT pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'))");
  const exactVectorMode = markOutbox || explicitVersionVectorsValue != null;
  let coveredIds;
  if (historical) {
    const coverage = await findBusinessPublicationVersionVectorCoverage(
      client,
      requestedVersionVectors,
    );
    coveredIds = requestedTargetIds.filter((channelId) => coverage.has(channelId));
  } else {
    const publishedVersionVectors = await loadPublishedVersionVectors(
      client,
      requestedTargetIds,
    );
    const alreadyAbsentRetractions = await findAlreadyAbsentRetractions(
      client,
      requestedVersionVectors,
    );
    coveredIds = requestedTargetIds.filter((channelId) => (
      sameVersionVector(
        requestedVersionVectors[channelId],
        publishedVersionVectors.get(channelId),
      ) || alreadyAbsentRetractions.has(channelId)
    ));
  }
  const coveredSet = new Set(coveredIds);
  const targetIds = requestedTargetIds.filter((channelId) => !coveredSet.has(channelId));
  if (targetIds.length === 0) {
    const watermark = await activeWatermark(client);
    if (!watermark) throw new Error("Creator Search active watermark is missing");
    const delivered = markOutbox
      ? await markDelivered(
        client,
        deliverableRows,
        watermark,
      )
      : 0;
    return {
      outcome: "covered_by_current",
      batch_id: watermark,
      source_sha256: null,
      publication_stream_id: streamId,
      projected: 0,
      covered: coveredIds.length,
      delivered,
      deferred: projectionRows.length - deliverableRows.length,
      upsert_channel_ids: [],
      removed_channel_ids: [],
      row_counts: projectionRowCounts([]),
      projections: [],
    };
  }
  const versionVectors = Object.fromEntries(targetIds.map((channelId) => [
    channelId,
    requestedVersionVectors[channelId],
  ]));
  const batch = semanticBatch({ streamId, versionVectors });
  const reusable = await reusablePublishedBatch(client, {
    batch,
    streamId,
    versionVectors,
    channelIds: targetIds,
  });
  if (reusable) {
    await reactivatePublishedBatch(client, reusable);
    const delivered = markOutbox
      ? await markDelivered(
        client,
        deliverableRows,
        batch.batchId,
      )
      : 0;
    return {
      outcome: "replayed",
      batch_id: batch.batchId,
      source_sha256: batch.sourceHash,
      publication_stream_id: streamId,
      projected: targetIds.length,
      covered: coveredIds.length,
      delivered,
      deferred: projectionRows.length - deliverableRows.length,
      upsert_channel_ids: reusable.upsert_channel_ids,
      removed_channel_ids: reusable.removed_channel_ids,
      row_counts: reusable.row_counts,
      projections: reusable.items,
    };
  }
  const current = exactVectorMode
    ? await loadBusinessPublicationVersionState(client, versionVectors)
    : await loadCurrent(client, targetIds);
  const previous = exactVectorMode
    ? emptyPrevious(targetIds)
    : await loadPrevious(client, targetIds);
  const previousWatermark = await activeWatermark(client);
  const projectionTime = capturedAt ?? maxTimestamp([
    ...deliverableRows
      .filter((row) => targetIds.includes(row.channel_id))
      .map((row) => row.created_at),
    ...[...current.channels.values()].map((row) => row.activated_at),
    ...[...current.videos.values()].map((row) => row.activated_at),
    ...[...current.agents.values()].map((row) => row.activated_at),
  ]) ?? new Date().toISOString();
  const projections = assembleProjections({
    channelIds: targetIds,
    current,
    previous,
    versionVectors,
    batchId: batch.batchId,
    capturedAt: projectionTime,
  });
  const batchRows = await insertBatch(client, {
    batch,
    streamId,
    versionVectors,
    projections,
    previousWatermark,
    capturedAt: projectionTime,
  });
  await insertProjectionRows(client, projections);
  await publishBatch(client, {
    batchId: batch.batchId,
    upsertIds: batchRows.upsertIds,
    removedIds: batchRows.removedIds,
  });
  const delivered = markOutbox
    ? await markDelivered(
        client,
        deliverableRows,
        batch.batchId,
      )
    : 0;
  return {
    outcome: "published",
    batch_id: batch.batchId,
    source_sha256: batch.sourceHash,
    publication_stream_id: streamId,
    projected: projections.length,
    covered: coveredIds.length,
    delivered,
    deferred: projectionRows.length - deliverableRows.length,
    upsert_channel_ids: batchRows.upsertIds,
    removed_channel_ids: batchRows.removedIds,
    row_counts: batchRows.rowCounts,
    projections: projections.map((item) => ({
      channel_id: item.channelId,
      action: item.action,
      snapshot_id: item.snapshot?.id ?? null,
      projection_hash: item.projectionHash,
      version_vector: item.versionVector,
    })),
  };
}

export async function claimBusinessPublicationProjectionRows(client, {
  batchSize = 25,
  workerId,
  leaseSeconds = 300,
}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("a PostgreSQL client is required");
  }
  const normalizedBatchSize = integerOption(batchSize, 25, "batchSize", {
    minimum: 1,
    maximum: 250,
  });
  const normalizedWorkerId = requiredText(workerId, "workerId");
  const normalizedLeaseSeconds = integerOption(leaseSeconds, 300, "leaseSeconds", {
    minimum: 30,
    maximum: 3600,
  });
  const result = await client.query(
    `/* business-publication-projector:claim */
     WITH selected_stream AS MATERIALIZED (
       SELECT projection.publication_stream_id
       FROM publication.projection_outbox projection
       JOIN publication.channel_ownership ownership USING(channel_id)
       WHERE ownership.status='active' AND ownership.projection_mode='online'
         AND projection.publication_stream_id=ownership.active_publication_stream_id
         AND (
           (projection.status='pending' AND projection.next_attempt_at<=now())
           OR (projection.status='retry_wait' AND projection.next_attempt_at<=now())
           OR (projection.status='leased' AND projection.lease_expires_at<=now())
         )
         AND NOT EXISTS (
           SELECT 1
           FROM publication.projection_outbox predecessor
           WHERE predecessor.channel_id=projection.channel_id
             AND predecessor.publication_stream_id=projection.publication_stream_id
             AND predecessor.status<>'delivered'
             AND (predecessor.created_at,predecessor.projection_id)
               < (projection.created_at,projection.projection_id)
         )
       ORDER BY projection.next_attempt_at,projection.created_at,projection.projection_id
       FOR UPDATE OF projection SKIP LOCKED
       LIMIT 1
     ),
     candidates AS MATERIALIZED (
       SELECT projection.projection_id
       FROM publication.projection_outbox projection
       JOIN publication.channel_ownership ownership USING(channel_id)
       JOIN selected_stream selected
         ON selected.publication_stream_id=projection.publication_stream_id
       WHERE ownership.status='active' AND ownership.projection_mode='online'
         AND projection.publication_stream_id=ownership.active_publication_stream_id
         AND (
           (projection.status='pending' AND projection.next_attempt_at<=now())
           OR (projection.status='retry_wait' AND projection.next_attempt_at<=now())
           OR (projection.status='leased' AND projection.lease_expires_at<=now())
         )
         AND NOT EXISTS (
           SELECT 1
           FROM publication.projection_outbox predecessor
           WHERE predecessor.channel_id=projection.channel_id
             AND predecessor.publication_stream_id=projection.publication_stream_id
             AND predecessor.status<>'delivered'
             AND (predecessor.created_at,predecessor.projection_id)
               < (projection.created_at,projection.projection_id)
         )
       ORDER BY projection.next_attempt_at,projection.created_at,projection.projection_id
       FOR UPDATE OF projection SKIP LOCKED
       LIMIT $1
     )
     UPDATE publication.projection_outbox projection
     SET status='leased',lease_owner=$2,
         lease_expires_at=now()+($3::int*interval '1 second'),
         attempts=projection.attempts+1,updated_at=now()
     FROM candidates
     WHERE projection.projection_id=candidates.projection_id
     RETURNING projection.projection_id,projection.publication_stream_id,
               projection.channel_id,projection.attempts`,
    [normalizedBatchSize, normalizedWorkerId, normalizedLeaseSeconds],
  );
  const streamIds = new Set(result.rows.map((row) => String(row.publication_stream_id)));
  if (streamIds.size > 1) {
    throw new Error("Projection claim crossed Publication Streams");
  }
  return result.rows;
}

export class PostgresBusinessPublicationProjector {
  constructor(pool, {
    workerId = `business-publication-projector:${randomUUID()}`,
    batchSize = 25,
    leaseSeconds = 300,
    claimStatementTimeoutMs = 15000,
    maximumAttempts = 20,
    retrySeconds = 10,
    maximumRetrySeconds = 600,
  } = {}) {
    if (!pool || typeof pool.connect !== "function" || typeof pool.query !== "function") {
      throw new TypeError("a PostgreSQL Pool is required");
    }
    this.pool = pool;
    this.workerId = requiredText(workerId, "workerId");
    this.batchSize = integerOption(batchSize, 25, "batchSize", { minimum: 1, maximum: 250 });
    this.leaseSeconds = integerOption(leaseSeconds, 300, "leaseSeconds", { minimum: 30, maximum: 3600 });
    this.claimStatementTimeoutMs = integerOption(
      claimStatementTimeoutMs,
      15000,
      "claimStatementTimeoutMs",
      { minimum: 1000, maximum: 300000 },
    );
    this.maximumAttempts = integerOption(maximumAttempts, 20, "maximumAttempts", { minimum: 1, maximum: 100 });
    this.retrySeconds = integerOption(retrySeconds, 10, "retrySeconds", { minimum: 1, maximum: 3600 });
    this.maximumRetrySeconds = integerOption(
      maximumRetrySeconds,
      600,
      "maximumRetrySeconds",
      { minimum: this.retrySeconds, maximum: 86400 },
    );
  }

  async #claim() {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `SELECT set_config('statement_timeout',$1,true),
                set_config('lock_timeout','5s',true)`,
        [`${this.claimStatementTimeoutMs}ms`],
      );
      const rows = await claimBusinessPublicationProjectionRows(client, {
        batchSize: this.batchSize,
        workerId: this.workerId,
        leaseSeconds: this.leaseSeconds,
      });
      await client.query("COMMIT");
      return rows;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async #retry(claimed, error) {
    if (claimed.length === 0) return { retry_wait: 0, dead_letter: 0 };
    const ids = claimed.map((row) => row.projection_id);
    const result = await this.pool.query(
      `/* business-publication-projector:retry */
       UPDATE publication.projection_outbox
       SET status=CASE WHEN attempts>=$2 THEN 'dead_letter' ELSE 'retry_wait' END,
           next_attempt_at=CASE WHEN attempts>=$2 THEN next_attempt_at ELSE
             now()+(LEAST($4,$3*power(2,GREATEST(attempts-1,0)))::int*interval '1 second')
           END,
           lease_owner=NULL,lease_expires_at=NULL,last_error=$5,updated_at=now()
       WHERE projection_id=ANY($1::uuid[]) AND status='leased' AND lease_owner=$6
       RETURNING status`,
      [
        ids,
        this.maximumAttempts,
        this.retrySeconds,
        this.maximumRetrySeconds,
        errorText(error),
        this.workerId,
      ],
    );
    return {
      retry_wait: result.rows.filter((row) => row.status === "retry_wait").length,
      dead_letter: result.rows.filter((row) => row.status === "dead_letter").length,
    };
  }

  async runOnce() {
    const claimed = await this.#claim();
    if (claimed.length === 0) {
      return { claimed: 0, channels: 0, projected: 0, delivered: 0, outcome: "idle" };
    }
    const claimedStreamIds = [...new Set(
      claimed.map((row) => String(row.publication_stream_id)),
    )];
    if (claimedStreamIds.length !== 1) {
      throw new Error("Projection claim did not retain exactly one Publication Stream");
    }
    const claimedStreamId = claimedStreamIds[0];
    const channelIds = uniqueSorted(claimed.map((row) => row.channel_id));
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='300s'");
      const result = await projectBusinessPublicationChannels(client, channelIds);
      if (
        result.publication_stream_id != null
          && String(result.publication_stream_id) !== claimedStreamId
      ) {
        throw new Error("Projection result changed the claimed Publication Stream");
      }
      await client.query("COMMIT");
      return {
        claimed: claimed.length,
        channels: channelIds.length,
        ...result,
      };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      const retry = await this.#retry(claimed, error);
      return {
        claimed: claimed.length,
        channels: channelIds.length,
        projected: 0,
        delivered: 0,
        outcome: "failed",
        publication_stream_id: claimedStreamId,
        error: errorText(error),
        ...retry,
      };
    } finally {
      client.release();
    }
  }
}
