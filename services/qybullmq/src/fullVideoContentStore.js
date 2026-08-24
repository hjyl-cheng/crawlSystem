import { positiveDurationSeconds } from "./detailPolicy.js";
import { parseLocalizedCountDetails } from "./localizedCount.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function nonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}

const ACCESS_ONLY_STATUSES = new Set([
  "unlisted",
  "members_only",
  "private",
  "unavailable",
]);
const NEW_CONTENT_ACCESS_STATUSES = new Set([
  "public",
  "unlisted",
  "members_only",
]);

export function fullVideoStorageAction({ candidate, classification, access } = {}) {
  const existingType = text(candidate?.known_content_type);
  const existingKey = text(candidate?.known_content_key);
  const accessStatus = text(access?.access_status);
  const classifiedType = classification?.authoritative === true
    && ["video", "short", "live"].includes(text(classification.content_type))
    ? text(classification.content_type)
    : null;
  const classifiedSource = classifiedType ? text(classification.source) : null;
  if (existingKey
      && ["video", "short", "live"].includes(existingType)
      && ACCESS_ONLY_STATUSES.has(accessStatus)) {
    return {
      kind: "update_access",
      content_key: existingKey,
      content_type: existingType,
      type_source: text(candidate?.known_content_type_source),
    };
  }
  if (classifiedType && NEW_CONTENT_ACCESS_STATUSES.has(accessStatus)) {
    return {
      kind: "upsert",
      content_type: classifiedType,
      type_source: classifiedSource,
    };
  }
  if (classifiedType) {
    return {
      kind: "classified_only",
      content_type: classifiedType,
      type_source: classifiedSource,
    };
  }
  return { kind: "unresolved" };
}

export async function updateExistingFullVideoAccess(client, {
  candidate,
  state,
  access: accessValue = null,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const access = accessValue ?? state?.access ?? {};
  const accessStatus = text(access.access_status);
  if (!ACCESS_ONLY_STATUSES.has(accessStatus)) return null;
  const channelId = text(candidate?.channel_id);
  const sourceContentId = text(candidate?.source_content_id);
  const accessSource = text(access.access_status_source);
  if (!channelId || !sourceContentId) {
    throw new TypeError("candidate channel_id and source_content_id are required");
  }
  if (!accessSource) throw new TypeError("explicit access evidence requires access_status_source");
  const detail = state?.detail ?? {};
  const stored = await client.query(
    `UPDATE crawler.contents
     SET run_id=$3,
         position=COALESCE($4::integer,position),
         is_recent=true,
         is_members_only=$5::text='members_only',
         access_status=$5,
         access_status_source=$6,
         extractor_version=COALESCE($7,extractor_version),
         raw_json=raw_json || $8::jsonb,
         last_seen_at=now(),
         last_enriched_at=now(),
         player_last_observed_at=now()
     WHERE channel_id=$1 AND source_content_id=$2
     RETURNING content_key,content_type,content_type_source`,
    [
      channelId,
      sourceContentId,
      text(candidate?.run_id),
      nonnegativeInteger(candidate?.position),
      accessStatus,
      accessSource,
      text(detail.extractor_version),
      JSON.stringify({ source: "content_access_observation_v1", ...(state ?? {}) }),
    ],
  );
  const row = stored.rows[0] ?? null;
  if (row?.content_key) await refreshVideoPublicationItemHashes(client, [row.content_key]);
  return row;
}

export function normalizeFullVideoViewCount(detailValue, { locale } = {}) {
  const detail = detailValue && typeof detailValue === "object" ? detailValue : {};
  const explicit = nonnegativeInteger(detail.view_count);
  const parsed = explicit == null
    ? parseLocalizedCountDetails(detail.view_count_text, { locale })
    : null;
  const value = explicit ?? parsed?.value ?? null;
  const compact = explicit == null && Number(parsed?.multiplier ?? 1) > 1;
  const declaredStatus = text(detail.view_count_status);
  const status = value == null
    ? ["unavailable", "unresolved"].includes(declaredStatus) ? declaredStatus : "unresolved"
    : compact
      ? "estimated"
      : ["exact", "estimated"].includes(declaredStatus) ? declaredStatus : "exact";
  return {
    value,
    text: text(detail.view_count_text) ?? (value == null ? null : String(value)),
    status,
    source: text(detail.view_count_source),
  };
}

export async function upsertFullVideoContent(client, {
  candidate,
  state,
  access: accessValue = null,
  locale,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  if (!candidate?.content_type || candidate.type_authoritative !== true) return null;
  const detail = state?.detail ?? {};
  const access = accessValue ?? state?.access ?? {};
  const accessStatus = text(access.access_status) ?? "unknown";
  const canCorrectStoredType = candidate.type_authoritative === true
    && accessStatus === "public";
  const contentKey = `${candidate.channel_id}:${candidate.content_type}:${candidate.source_content_id}`;
  const url = candidate.content_type === "short"
    ? `https://www.youtube.com/shorts/${candidate.source_content_id}`
    : `https://www.youtube.com/watch?v=${candidate.source_content_id}`;
  const views = normalizeFullVideoViewCount(detail, { locale });
  const commentsDisabled = detail.comments_disabled === true;

  const stored = await client.query(
    `INSERT INTO crawler.contents (
       content_key, channel_id, run_id, content_type, content_type_source,
       source_content_id, position, title, url, thumbnail_url,
       published_text_raw, published_at, published_at_status, published_at_source, published_at_precision,
       is_recent, length_text, duration_seconds, duration_status, duration_source,
       view_count, view_count_text, view_count_status, view_count_source,
       like_count, like_count_status, like_count_source,
       comment_count, comment_count_status, comments_disabled, comment_count_source,
       comments_first_page,
       is_members_only, access_status, access_status_source,
       live_scheduled_at, live_started_at, live_ended_at, extractor_version,
       description, description_status, description_source, hashtags, keywords,
       raw_json, first_seen_at, last_seen_at, last_enriched_at
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
       $11,$12,$13,$14,$15,true,$16,$17,$18,$19,
       $20::bigint,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
       $31::jsonb,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42::text[],$43::text[],
       $44::jsonb,now(),now(),now()
     )
     ON CONFLICT (channel_id, source_content_id)
     DO UPDATE SET run_id=EXCLUDED.run_id,
                   content_type=CASE
                     WHEN $45::boolean THEN EXCLUDED.content_type
                     ELSE crawler.contents.content_type
                   END,
                   content_type_source=CASE
                     WHEN $45::boolean THEN EXCLUDED.content_type_source
                     ELSE crawler.contents.content_type_source
                   END,
                   position=EXCLUDED.position,
                   title=COALESCE(EXCLUDED.title,crawler.contents.title),
                   url=CASE
                     WHEN $45::boolean THEN EXCLUDED.url
                     ELSE COALESCE(crawler.contents.url,EXCLUDED.url)
                   END,
                   thumbnail_url=COALESCE(EXCLUDED.thumbnail_url,crawler.contents.thumbnail_url),
                   description=CASE
                     WHEN EXCLUDED.description_status='exact' THEN EXCLUDED.description
                     WHEN EXCLUDED.description_status='empty'
                       AND NULLIF(btrim(COALESCE(crawler.contents.description,'')),'') IS NULL THEN ''
                     WHEN crawler.contents.description_status IN ('exact','empty') THEN crawler.contents.description
                     ELSE COALESCE(EXCLUDED.description,crawler.contents.description)
                   END,
                   description_status=CASE
                     WHEN EXCLUDED.description_status='exact' THEN 'exact'
                     WHEN EXCLUDED.description_status='empty'
                       AND NULLIF(btrim(COALESCE(crawler.contents.description,'')),'') IS NULL THEN 'empty'
                     WHEN crawler.contents.description_status IN ('exact','empty') THEN crawler.contents.description_status
                     ELSE EXCLUDED.description_status
                   END,
                   description_source=CASE
                     WHEN EXCLUDED.description_status='exact' THEN EXCLUDED.description_source
                     WHEN EXCLUDED.description_status='empty'
                       AND NULLIF(btrim(COALESCE(crawler.contents.description,'')),'') IS NULL
                       THEN EXCLUDED.description_source
                     WHEN crawler.contents.description_status IN ('exact','empty') THEN crawler.contents.description_source
                     ELSE COALESCE(EXCLUDED.description_source,crawler.contents.description_source)
                   END,
                   hashtags=CASE
                     WHEN COALESCE((EXCLUDED.raw_json#>>'{detail,hashtags_observed}')::boolean,false)
                       AND (
                         COALESCE(cardinality(EXCLUDED.hashtags),0)>0
                         OR COALESCE(cardinality(crawler.contents.hashtags),0)=0
                       )
                       THEN EXCLUDED.hashtags
                     ELSE crawler.contents.hashtags
                   END,
                   keywords=CASE
                     WHEN COALESCE((EXCLUDED.raw_json#>>'{detail,keywords_observed}')::boolean,false)
                       AND (
                         COALESCE(cardinality(EXCLUDED.keywords),0)>0
                         OR COALESCE(cardinality(crawler.contents.keywords),0)=0
                       )
                       THEN EXCLUDED.keywords
                     ELSE crawler.contents.keywords
                   END,
                   published_text_raw=COALESCE(EXCLUDED.published_text_raw,crawler.contents.published_text_raw),
                   published_at=CASE
                     WHEN EXCLUDED.published_at IS NOT NULL
                       AND EXCLUDED.published_at_precision='second' THEN EXCLUDED.published_at
                     WHEN crawler.contents.published_at IS NULL THEN EXCLUDED.published_at
                     ELSE crawler.contents.published_at
                   END,
                   published_at_status=CASE
                     WHEN EXCLUDED.published_at IS NOT NULL
                       AND EXCLUDED.published_at_precision='second' THEN EXCLUDED.published_at_status
                     WHEN crawler.contents.published_at IS NOT NULL THEN crawler.contents.published_at_status
                     ELSE EXCLUDED.published_at_status
                   END,
                   published_at_source=CASE
                     WHEN EXCLUDED.published_at IS NOT NULL
                       AND EXCLUDED.published_at_precision='second' THEN EXCLUDED.published_at_source
                     WHEN crawler.contents.published_at IS NOT NULL THEN crawler.contents.published_at_source
                     ELSE EXCLUDED.published_at_source
                   END,
                   published_at_precision=CASE
                     WHEN EXCLUDED.published_at IS NOT NULL
                       AND EXCLUDED.published_at_precision='second' THEN 'second'
                     WHEN crawler.contents.published_at IS NOT NULL THEN crawler.contents.published_at_precision
                     ELSE EXCLUDED.published_at_precision
                   END,
                   is_recent=true,
                   length_text=COALESCE(
                     NULLIF(NULLIF(EXCLUDED.length_text,'0:00'),'00:00'),
                     NULLIF(NULLIF(crawler.contents.length_text,'0:00'),'00:00')
                   ),
                   duration_seconds=COALESCE(
                     CASE WHEN EXCLUDED.duration_seconds>0 THEN EXCLUDED.duration_seconds END,
                     CASE WHEN crawler.contents.duration_seconds>0 THEN crawler.contents.duration_seconds END
                   ),
                   duration_status=CASE
                     WHEN EXCLUDED.duration_seconds>0 THEN EXCLUDED.duration_status
                     WHEN crawler.contents.duration_seconds>0 THEN crawler.contents.duration_status
                     ELSE EXCLUDED.duration_status
                   END,
                   duration_source=CASE
                     WHEN EXCLUDED.duration_seconds>0 THEN EXCLUDED.duration_source
                     WHEN crawler.contents.duration_seconds>0 THEN crawler.contents.duration_source
                     ELSE EXCLUDED.duration_source
                   END,
                   view_count=COALESCE(EXCLUDED.view_count,crawler.contents.view_count),
                   view_count_text=CASE
                     WHEN EXCLUDED.view_count IS NOT NULL THEN EXCLUDED.view_count_text
                     WHEN crawler.contents.view_count IS NOT NULL THEN crawler.contents.view_count_text
                     ELSE EXCLUDED.view_count_text
                   END,
                   view_count_status=CASE
                     WHEN EXCLUDED.view_count IS NOT NULL THEN EXCLUDED.view_count_status
                     WHEN crawler.contents.view_count IS NOT NULL THEN crawler.contents.view_count_status
                     ELSE EXCLUDED.view_count_status
                   END,
                   view_count_source=CASE
                     WHEN EXCLUDED.view_count IS NOT NULL THEN EXCLUDED.view_count_source
                     WHEN crawler.contents.view_count IS NOT NULL THEN crawler.contents.view_count_source
                     ELSE EXCLUDED.view_count_source
                   END,
                   like_count=COALESCE(EXCLUDED.like_count,crawler.contents.like_count),
                   like_count_status=CASE
                     WHEN EXCLUDED.like_count IS NOT NULL THEN EXCLUDED.like_count_status
                     WHEN crawler.contents.like_count IS NOT NULL THEN crawler.contents.like_count_status
                     ELSE EXCLUDED.like_count_status
                   END,
                   like_count_source=CASE
                     WHEN EXCLUDED.like_count IS NOT NULL THEN EXCLUDED.like_count_source
                     WHEN crawler.contents.like_count IS NOT NULL THEN crawler.contents.like_count_source
                     ELSE EXCLUDED.like_count_source
                   END,
                   comment_count=CASE
                     WHEN EXCLUDED.comments_disabled=true THEN 0
                     ELSE COALESCE(EXCLUDED.comment_count,crawler.contents.comment_count)
                   END,
                   comment_count_status=CASE
                     WHEN EXCLUDED.comments_disabled=true OR EXCLUDED.comment_count IS NOT NULL
                       THEN EXCLUDED.comment_count_status
                     WHEN crawler.contents.comments_disabled=true OR crawler.contents.comment_count IS NOT NULL
                       THEN crawler.contents.comment_count_status
                     ELSE EXCLUDED.comment_count_status
                   END,
                   comments_disabled=CASE
                     WHEN EXCLUDED.comments_disabled=true OR EXCLUDED.comment_count IS NOT NULL
                       THEN EXCLUDED.comments_disabled
                     WHEN crawler.contents.comments_disabled=true OR crawler.contents.comment_count IS NOT NULL
                       THEN crawler.contents.comments_disabled
                     ELSE EXCLUDED.comments_disabled
                   END,
                   comment_count_source=CASE
                     WHEN EXCLUDED.comments_disabled=true OR EXCLUDED.comment_count IS NOT NULL
                       THEN EXCLUDED.comment_count_source
                     WHEN crawler.contents.comments_disabled=true OR crawler.contents.comment_count IS NOT NULL
                       THEN crawler.contents.comment_count_source
                     ELSE EXCLUDED.comment_count_source
                   END,
                   comments_first_page=CASE
                     WHEN COALESCE((crawler.contents.comments_first_page->>'returned_count')::integer,0)>0
                       THEN crawler.contents.comments_first_page
                     WHEN COALESCE((EXCLUDED.comments_first_page->>'returned_count')::integer,0)>0
                       THEN EXCLUDED.comments_first_page
                     ELSE COALESCE(crawler.contents.comments_first_page,EXCLUDED.comments_first_page)
                   END,
                   is_members_only=CASE
                     WHEN EXCLUDED.access_status IN ('unknown','login_required')
                       THEN crawler.contents.is_members_only
                     ELSE EXCLUDED.is_members_only
                   END,
                   access_status=CASE
                     WHEN EXCLUDED.access_status IN ('unknown','login_required')
                       THEN crawler.contents.access_status
                     ELSE EXCLUDED.access_status
                   END,
                   access_status_source=CASE
                     WHEN EXCLUDED.access_status IN ('unknown','login_required')
                       THEN crawler.contents.access_status_source
                     ELSE EXCLUDED.access_status_source
                   END,
                   live_scheduled_at=COALESCE(EXCLUDED.live_scheduled_at,crawler.contents.live_scheduled_at),
                   live_started_at=COALESCE(EXCLUDED.live_started_at,crawler.contents.live_started_at),
                   live_ended_at=COALESCE(EXCLUDED.live_ended_at,crawler.contents.live_ended_at),
                   extractor_version=COALESCE(EXCLUDED.extractor_version,crawler.contents.extractor_version),
                   raw_json=crawler.contents.raw_json || EXCLUDED.raw_json,
                   last_seen_at=now(),
                   last_enriched_at=now()
     RETURNING content_key`,
    [
      contentKey,
      candidate.channel_id,
      candidate.run_id,
      candidate.content_type,
      candidate.type_source,
      candidate.source_content_id,
      candidate.position,
      detail.title ?? candidate.title,
      url,
      detail.thumbnail_url ?? candidate.thumbnail_url,
      detail.published_text ?? null,
      detail.published_at ?? null,
      detail.published_at_status ?? "unresolved",
      detail.published_at_source ?? detail.source ?? null,
      detail.published_at_precision ?? "unknown",
      detail.length_text ?? null,
      positiveDurationSeconds(detail.duration_seconds),
      detail.duration_status ?? "unresolved",
      detail.duration_source ?? null,
      views.value,
      views.text,
      views.status,
      views.source,
      nonnegativeInteger(detail.like_count),
      detail.like_count_status ?? "unresolved",
      detail.like_count_source ?? null,
      commentsDisabled ? 0 : nonnegativeInteger(detail.comment_count),
      commentsDisabled ? "disabled" : detail.comment_count_status ?? "unresolved",
      typeof detail.comments_disabled === "boolean" ? detail.comments_disabled : null,
      detail.comment_count_source ?? detail.comments_status_source ?? null,
      detail.comments_first_page ? JSON.stringify(detail.comments_first_page) : null,
      Boolean(access.is_members_only),
      accessStatus,
      access.access_status_source ?? null,
      detail.live_scheduled_at ?? null,
      detail.live_started_at ?? null,
      detail.live_ended_at ?? null,
      detail.extractor_version ?? "v4_first_success",
      detail.description ?? null,
      detail.description_status ?? "unresolved",
      detail.description_source ?? null,
      detail.hashtags ?? [],
      detail.keywords ?? [],
      JSON.stringify({ source: "content_detail_v4", ...(state ?? {}) }),
      canCorrectStoredType,
    ],
  );
  const storedContentKey = text(stored.rows[0]?.content_key) ?? contentKey;
  await refreshVideoPublicationItemHashes(client, [storedContentKey]);
  return storedContentKey;
}
