import { observationFactsHash } from "./crawlObservationStore.js";
import { normalizePublicationLinks } from "./publicationLinks.js";
import {
  normalizePublicationImageUrl,
  normalizePublicationUrl,
} from "./publicationUrl.js";

export const BUSINESS_CHANNEL_PRESERVATION_SQL = `/* publication-current-reconciliation:business-preservation */
WITH target AS (
  SELECT channel_id,ordinality::int AS target_position
  FROM unnest($1::text[]) WITH ORDINALITY AS requested(channel_id,ordinality)
), storage_state AS (
  SELECT COALESCE((
    SELECT read_mode
    FROM publication.creator_search_storage_state
    WHERE singleton=true
  ),'legacy') AS read_mode
), active_release AS (
  SELECT watermark
  FROM public.creator_search_active
  WHERE singleton=true
), active_search AS (
  SELECT target.channel_id AS target_channel_id,state.read_mode AS storage_read_mode,
         search.watermark,search.snapshot_id,search.channel_id AS snapshot_channel_id
  FROM target
  CROSS JOIN storage_state state
  LEFT JOIN public.creator_search_live search
    ON search.channel_id=target.channel_id
  WHERE state.read_mode='live'
  UNION ALL
  SELECT target.channel_id AS target_channel_id,state.read_mode AS storage_read_mode,
         release.watermark,search.snapshot_id,search.channel_id AS snapshot_channel_id
  FROM target
  CROSS JOIN storage_state state
  CROSS JOIN active_release release
  LEFT JOIN public.creator_search_current search
    ON search.watermark=release.watermark AND search.channel_id=target.channel_id
  WHERE state.read_mode='legacy'
), active_snapshot AS (
  SELECT target.channel_id AS target_channel_id,target.target_position,
         search.storage_read_mode,
         COALESCE(search.watermark,release.watermark) AS active_watermark,
         snapshot.id AS snapshot_id,snapshot.channel_id AS snapshot_channel_id,
         snapshot.captured_at AS snapshot_captured_at,
         snapshot.title,snapshot.handle,snapshot.avatar_url,snapshot.description,
         snapshot.is_verified,snapshot.subscriber_count,snapshot.subscriber_count_status,
         snapshot.total_view_count,snapshot.total_view_count_status,
         snapshot.video_count,snapshot.video_count_status,
         snapshot.joined_date,snapshot.joined_date_text,snapshot.joined_date_status,
         snapshot.country_text,snapshot.channel_url,
         EXISTS (
           SELECT 1 FROM public.channels registry
           WHERE registry.channel_id=target.channel_id
         ) AS registry_exists,
         EXISTS (
           SELECT 1 FROM public.channel_snapshots historical_snapshot
           WHERE historical_snapshot.channel_id=target.channel_id
         ) AS historical_snapshot_exists,
         snapshot.raw_channel->>'country_code' AS raw_country_code,
         snapshot.raw_channel->>'country_canonical_name' AS raw_country_canonical_name
  FROM target
  CROSS JOIN active_release release
  LEFT JOIN active_search search ON search.target_channel_id=target.channel_id
  LEFT JOIN public.channel_snapshots snapshot
    ON snapshot.id=search.snapshot_id AND snapshot.channel_id=search.snapshot_channel_id
)
SELECT active.*,
       link.id AS link_id,link.link_type,link.url AS link_url,link.title AS link_title,
       link.source AS link_source,link.raw_link,
       CASE WHEN link.id IS NULL THEN NULL ELSE
         row_number() OVER (PARTITION BY active.target_channel_id ORDER BY link.id)-1
       END AS link_position
FROM active_snapshot active
LEFT JOIN public.channel_links link ON link.channel_snapshot_id=active.snapshot_id
ORDER BY active.target_position,link.id`;

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function nonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a timestamp`);
  return parsed.toISOString();
}

function date(value) {
  if (value == null || value === "") return null;
  const output = value instanceof Date ? value.toISOString() : String(value);
  const match = output.match(/^(\d{4}-\d{2}-\d{2})/);
  return match?.[1] ?? null;
}

function metricStatus(value) {
  const status = text(value);
  if (status === "exact") return "exact";
  if (status === "approximate") return "estimated";
  return null;
}

function joinedStatus(value) {
  return metricStatus(value);
}

function storageReadMode(value) {
  const mode = text(value) ?? "legacy";
  if (mode !== "legacy" && mode !== "live") {
    throw new TypeError(`Business Creator Search storage mode is unsupported: ${mode}`);
  }
  return mode;
}

function addText(payload, key, value) {
  const output = text(value);
  if (output) payload[key] = output;
}

function addMetric(payload, valueKey, statusKey, value, status) {
  const count = nonnegativeInteger(value);
  const normalizedStatus = metricStatus(status);
  if (count === null || !normalizedStatus) return;
  payload[valueKey] = count;
  payload[statusKey] = normalizedStatus;
}

function canonicalLinks(rows, channelId) {
  const rawLinks = rows.filter((row) => text(row.link_id)).map((row, index) => ({
    title: text(row.link_title),
    display_url: text(row.raw_link?.rawValue),
    target_url: text(row.link_url),
    position: nonnegativeInteger(row.link_position) ?? index,
    link_type: text(row.link_type),
    purpose: text(row.raw_link?.purpose),
  }));
  if (rawLinks.length === 0) return null;
  const normalized = normalizePublicationLinks(rawLinks, { observed: true });
  if (!normalized.ready || normalized.valid_count !== rawLinks.length) {
    throw new TypeError(`Business Active Snapshot contains invalid Links: ${channelId}`);
  }
  return normalized.links;
}

function availableBaseline(rows, databaseName, channelId) {
  const row = rows[0];
  const readMode = storageReadMode(row.storage_read_mode);
  if (rows.some((value) => (
    text(value.snapshot_id) !== text(row.snapshot_id)
    || text(value.active_watermark) !== text(row.active_watermark)
    || storageReadMode(value.storage_read_mode) !== readMode
  ))) {
    throw new TypeError(`Business Active Snapshot is inconsistent: ${channelId}`);
  }
  if (text(row.snapshot_channel_id) !== channelId) {
    throw new TypeError(`Business Active Snapshot Channel identity differs: ${channelId}`);
  }
  const payload = { channel_id: channelId };
  addText(payload, "title", row.title);
  const canonicalUrl = normalizePublicationUrl(row.channel_url);
  if (canonicalUrl) payload.canonical_url = canonicalUrl;
  addText(payload, "handle", row.handle);
  const avatarUrl = normalizePublicationImageUrl(row.avatar_url);
  if (avatarUrl) payload.avatar = [{ url: avatarUrl, position: 0 }];
  addText(payload, "description", row.description);
  if (typeof row.is_verified === "boolean") {
    payload.is_verified = row.is_verified;
    payload.is_verified_status = row.is_verified ? "verified" : "not_verified";
  }
  addMetric(
    payload,
    "subscriber_count",
    "subscriber_count_status",
    row.subscriber_count,
    row.subscriber_count_status,
  );
  addMetric(
    payload,
    "total_video_count",
    "total_video_count_status",
    row.video_count,
    row.video_count_status,
  );
  addMetric(
    payload,
    "total_view_count",
    "total_view_count_status",
    row.total_view_count,
    row.total_view_count_status,
  );
  const joinedDate = date(row.joined_date);
  const normalizedJoinedStatus = joinedStatus(row.joined_date_status);
  if (joinedDate && normalizedJoinedStatus) {
    payload.joined_date = joinedDate;
    payload.joined_date_status = normalizedJoinedStatus;
    payload.joined_date_raw = text(row.joined_date_text);
  }
  addText(payload, "country_code", row.raw_country_code);
  addText(payload, "country_name", row.raw_country_canonical_name ?? row.country_text);
  const links = canonicalLinks(rows, channelId);
  if (links) payload.links = links;

  return {
    status: "available",
    payload,
    payload_hash: observationFactsHash(payload),
    source: {
      type: readMode === "live" ? "business_live_snapshot" : "legacy_business_active_snapshot",
      database_name: databaseName,
      active_watermark: text(row.active_watermark),
      snapshot_id: text(row.snapshot_id),
      captured_at: timestamp(row.snapshot_captured_at, "Business Snapshot captured_at"),
    },
  };
}

export function buildBusinessChannelPreservationBaselines(
  rowsValue,
  channelIdsValue,
  { databaseName } = {},
) {
  if (!Array.isArray(rowsValue)) throw new TypeError("Business baseline query must return rows");
  if (!Array.isArray(channelIdsValue) || channelIdsValue.length === 0) {
    throw new TypeError("channelIds must be a non-empty array");
  }
  const expectedDatabase = text(databaseName);
  if (!expectedDatabase) throw new TypeError("databaseName is required");
  const channelIds = [...channelIdsValue];
  const expected = new Set(channelIds);
  if (expected.size !== channelIds.length || channelIds.some((channelId) => !text(channelId))) {
    throw new TypeError("channelIds must contain unique non-empty strings");
  }
  const grouped = new Map(channelIds.map((channelId) => [channelId, []]));
  for (const row of rowsValue) {
    const channelId = text(row.target_channel_id);
    if (!expected.has(channelId)) {
      throw new TypeError(`Business baseline query returned an unexpected Channel: ${channelId}`);
    }
    grouped.get(channelId).push(row);
  }
  const missingRows = channelIds.filter((channelId) => grouped.get(channelId).length === 0);
  if (missingRows.length > 0) {
    throw new TypeError(`Business baseline query omitted Channels: ${missingRows.join(", ")}`);
  }
  return new Map(channelIds.map((channelId) => {
    const rows = grouped.get(channelId);
    const row = rows[0];
    const readMode = storageReadMode(row.storage_read_mode);
    const activeWatermark = text(row.active_watermark);
    if (!activeWatermark) throw new TypeError("Business Active Watermark is missing");
    if (!text(row.snapshot_id)) {
      if (typeof row.registry_exists !== "boolean"
          || typeof row.historical_snapshot_exists !== "boolean") {
        throw new TypeError(
          `Business absence checks are missing for Active Snapshot lookup: ${channelId}`,
        );
      }
      if (row.registry_exists || row.historical_snapshot_exists) {
        throw new TypeError(
          `Business Channel exists outside the Active release; preservation is ambiguous: ${channelId}`,
        );
      }
      return [channelId, {
        status: "not_found",
        payload: null,
        payload_hash: null,
        source: {
          type: readMode === "live"
            ? "business_live_snapshot_lookup"
            : "legacy_business_active_snapshot_lookup",
          database_name: expectedDatabase,
          active_watermark: activeWatermark,
          absence_checks: {
            channel_registry: false,
            historical_snapshot: false,
          },
        },
      }];
    }
    return [channelId, availableBaseline(rows, expectedDatabase, channelId)];
  }));
}
