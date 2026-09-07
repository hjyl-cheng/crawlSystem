import { retainedCommentPageSql, observedListSql, descriptionMutationSql } from "./videoContentMutationPolicy.js";
import { projectVideoDetail } from "./videoDetailEvidence.js";
import { ACCESS_ONLY_STATUSES } from "./collectedVideoOutcome.js";
export { fullVideoStorageAction } from "./collectedVideoOutcome.js";
import {
  publicationEvidenceFromFields,
  normalizePublicationEvidence,
  selectPublicationEvidence,
  publicationEvidenceConflictRecord,
  publicationEvidenceCandidateWinsSql,
  publicationEvidenceConflictPatchSql,
} from "./publicationTimeEvidence.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";

const CONTENT_UPSERT_PUBLICATION_WINS = publicationEvidenceCandidateWinsSql(
  "crawler.contents",
  "EXCLUDED",
);
const CONTENT_UPSERT_PUBLICATION_CONFLICT = publicationEvidenceConflictPatchSql(
  "crawler.contents",
  "EXCLUDED",
);

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function nonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
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
  const facts = projectVideoDetail(detail, { locale });
  const publication = publicationEvidenceFromFields(detail);

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
                   ${descriptionMutationSql({ unresolved: "preserve_resolved" })},
                   hashtags=${observedListSql("crawler.contents.hashtags", "EXCLUDED.hashtags", "COALESCE((EXCLUDED.raw_json#>>'{detail,hashtags_observed}')::boolean,false)")},
                   keywords=${observedListSql("crawler.contents.keywords", "EXCLUDED.keywords", "COALESCE((EXCLUDED.raw_json#>>'{detail,keywords_observed}')::boolean,false)")},
                   published_text_raw=COALESCE(EXCLUDED.published_text_raw,crawler.contents.published_text_raw),
                   published_at=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
                     THEN EXCLUDED.published_at ELSE crawler.contents.published_at END,
                   published_at_status=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
                     THEN EXCLUDED.published_at_status ELSE crawler.contents.published_at_status END,
                   published_at_source=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
                     THEN EXCLUDED.published_at_source ELSE crawler.contents.published_at_source END,
                   published_at_precision=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
                     THEN EXCLUDED.published_at_precision ELSE crawler.contents.published_at_precision END,
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
                   comments_first_page=${retainedCommentPageSql("crawler.contents.comments_first_page", "EXCLUDED.comments_first_page")},
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
                   raw_json=crawler.contents.raw_json || EXCLUDED.raw_json
                     || ${CONTENT_UPSERT_PUBLICATION_CONFLICT},
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
      publication.published_at,
      publication.published_at_status,
      publication.published_at_source,
      publication.published_at_precision,
      detail.length_text ?? null,
      facts.duration_seconds,
      facts.duration_status,
      facts.duration_source,
      facts.view_count,
      facts.view_count_text,
      facts.view_count_status,
      facts.view_count_source,
      facts.like_count,
      facts.like_count_status,
      facts.like_count_source,
      facts.comment_count,
      facts.comment_count_status,
      facts.comments_disabled,
      facts.comment_count_source,
      detail.comments_first_page ? JSON.stringify(detail.comments_first_page) : null,
      Boolean(access.is_members_only),
      accessStatus,
      access.access_status_source ?? null,
      detail.live_scheduled_at ?? null,
      detail.live_started_at ?? null,
      detail.live_ended_at ?? null,
      detail.extractor_version ?? "v4_first_success",
      facts.description,
      facts.description_status,
      facts.description_source,
      facts.hashtags,
      facts.keywords,
      JSON.stringify({ source: "content_detail_v4", ...(state ?? {}) }),
      canCorrectStoredType,
    ],
  );
  const storedContentKey = text(stored.rows[0]?.content_key) ?? contentKey;
  await refreshVideoPublicationItemHashes(client, [storedContentKey]);
  return storedContentKey;
}

function missingStoredText(value) {
  return text(value) == null;
}

function recentStorageFacts(row, facts, { allowStaticRepair = false } = {}) {
  if (allowStaticRepair) {
    return {
      title: facts.title,
      thumbnail_url: facts.thumbnail_url,
      description: facts.description,
      description_source: facts.description_source,
      description_observed: facts.description_observed,
      hashtags: facts.hashtags,
      hashtags_observed: facts.hashtags_observed,
      keywords: facts.keywords,
      keywords_observed: facts.keywords_observed,
      duration_seconds: facts.duration_seconds,
      duration_source: facts.duration_source,
      live_scheduled_at: facts.live_scheduled_at,
      live_started_at: facts.live_started_at,
      live_ended_at: facts.live_ended_at,
    };
  }
  const descriptionResolved = ["exact", "empty"].includes(text(row.description_status));
  const descriptionMissing = missingStoredText(row.description) && !descriptionResolved;
  const durationMissing = positiveDuration(row.duration_seconds) == null;
  return {
    title: missingStoredText(row.title) ? facts.title : null,
    thumbnail_url: missingStoredText(row.thumbnail_url) ? facts.thumbnail_url : null,
    description: descriptionMissing ? facts.description : null,
    description_source: descriptionMissing ? facts.description_source : null,
    description_observed: descriptionMissing && facts.description_observed,
    hashtags: [],
    hashtags_observed: false,
    keywords: [],
    keywords_observed: false,
    duration_seconds: durationMissing ? facts.duration_seconds : null,
    duration_source: durationMissing ? facts.duration_source : null,
    live_scheduled_at: row.live_scheduled_at == null ? facts.live_scheduled_at : null,
    live_started_at: row.live_started_at == null ? facts.live_started_at : null,
    live_ended_at: row.live_ended_at == null ? facts.live_ended_at : null,
  };
}


function positiveDuration(value) {
  const number = nonnegativeInteger(value);
  return number != null && number > 0 ? number : null;
}

export async function upsertDiscoveredVideoContent(client, {
  channelId, runId, observationId, observedAt, entry, detail, facts,
  classification, publication, publicationConflict, detailComplete,
}) {
  const contentType = classification.content_type;
  const contentKey = `${channelId}:${contentType}:${entry.id}`;
  const url = contentType === "short"
    ? `https://www.youtube.com/shorts/${encodeURIComponent(entry.id)}`
    : `https://www.youtube.com/watch?v=${encodeURIComponent(entry.id)}`;
  const descriptionStatus = facts?.description_status ?? "unresolved";
  const published = publication.published_at;
  const publishedPrecision = publication.published_at_precision;
  const publishedSource = publication.published_at_source;
  const isRecent = published == null
    ? true
    : new Date(published).getTime() >= new Date(observedAt).getTime() - (30 * 86400000);
  const stored = await client.query(
    `INSERT INTO crawler.contents (
       content_key,channel_id,run_id,content_type,content_type_source,
       source_content_id,position,title,url,thumbnail_url,
       description,description_status,description_source,hashtags,keywords,
       published_at,published_at_status,published_at_source,published_at_precision,
       is_recent,duration_seconds,duration_status,duration_source,
       view_count,view_count_text,view_count_status,view_count_source,
       like_count,like_count_status,like_count_source,
       comment_count,comment_count_status,comments_disabled,comment_count_source,
       comments_first_page,
       is_members_only,access_status,access_status_source,extractor_version,
       raw_json,first_seen_at,last_seen_at,last_enriched_at,
       playlist_last_seen_at,player_last_observed_at,next_last_observed_at,last_observation_id
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
       $11,$12,$13,$14::text[],$15::text[],$16,$17,$18,$19,
       $20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$44::jsonb,
       $35,$36,$37,$38,$39::jsonb,$40::timestamptz,$40::timestamptz,
       CASE WHEN $41::boolean THEN $40::timestamptz ELSE NULL END,
       $40::timestamptz,CASE WHEN $41::boolean THEN $40::timestamptz ELSE NULL END,
       CASE WHEN $41::boolean THEN $40::timestamptz ELSE NULL END,$42
     )
     ON CONFLICT (channel_id,source_content_id) DO UPDATE
     SET run_id=EXCLUDED.run_id,position=EXCLUDED.position,
         content_type=CASE
           WHEN $43::boolean THEN EXCLUDED.content_type
           ELSE crawler.contents.content_type
         END,
         content_type_source=CASE
           WHEN $43::boolean THEN EXCLUDED.content_type_source
           ELSE crawler.contents.content_type_source
         END,
         url=CASE
           WHEN $43::boolean THEN EXCLUDED.url
           ELSE COALESCE(crawler.contents.url,EXCLUDED.url)
         END,
         title=COALESCE(EXCLUDED.title,crawler.contents.title),
         thumbnail_url=COALESCE(EXCLUDED.thumbnail_url,crawler.contents.thumbnail_url),
         ${descriptionMutationSql({ observed: "$41::boolean" })},
         hashtags=${observedListSql("crawler.contents.hashtags", "EXCLUDED.hashtags", "$41::boolean")},
         keywords=${observedListSql("crawler.contents.keywords", "EXCLUDED.keywords", "$41::boolean")},
         published_at=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at ELSE crawler.contents.published_at END,
         published_at_status=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at_status ELSE crawler.contents.published_at_status END,
         published_at_source=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at_source ELSE crawler.contents.published_at_source END,
         published_at_precision=CASE WHEN ${CONTENT_UPSERT_PUBLICATION_WINS}
           THEN EXCLUDED.published_at_precision ELSE crawler.contents.published_at_precision END,
         duration_seconds=COALESCE(EXCLUDED.duration_seconds,crawler.contents.duration_seconds),
         duration_status=CASE WHEN EXCLUDED.duration_seconds IS NOT NULL THEN 'exact' ELSE crawler.contents.duration_status END,
         duration_source=COALESCE(EXCLUDED.duration_source,crawler.contents.duration_source),
         view_count=COALESCE(EXCLUDED.view_count,crawler.contents.view_count),
         view_count_text=COALESCE(EXCLUDED.view_count_text,crawler.contents.view_count_text),
         view_count_status=CASE WHEN EXCLUDED.view_count IS NOT NULL THEN EXCLUDED.view_count_status ELSE crawler.contents.view_count_status END,
         view_count_source=COALESCE(EXCLUDED.view_count_source,crawler.contents.view_count_source),
         like_count=COALESCE(EXCLUDED.like_count,crawler.contents.like_count),
         like_count_status=CASE WHEN EXCLUDED.like_count IS NOT NULL THEN EXCLUDED.like_count_status ELSE crawler.contents.like_count_status END,
         like_count_source=COALESCE(EXCLUDED.like_count_source,crawler.contents.like_count_source),
         comment_count=CASE
           WHEN EXCLUDED.comments_disabled THEN 0
           ELSE COALESCE(EXCLUDED.comment_count,crawler.contents.comment_count) END,
         comment_count_status=CASE
           WHEN EXCLUDED.comments_disabled OR EXCLUDED.comment_count IS NOT NULL
             THEN EXCLUDED.comment_count_status
           WHEN crawler.contents.comments_disabled OR crawler.contents.comment_count IS NOT NULL
             THEN crawler.contents.comment_count_status
           ELSE EXCLUDED.comment_count_status END,
         comments_disabled=CASE
           WHEN EXCLUDED.comments_disabled OR EXCLUDED.comment_count IS NOT NULL
             THEN EXCLUDED.comments_disabled
           WHEN crawler.contents.comments_disabled OR crawler.contents.comment_count IS NOT NULL
             THEN crawler.contents.comments_disabled
           ELSE EXCLUDED.comments_disabled END,
         comment_count_source=CASE
           WHEN EXCLUDED.comments_disabled OR EXCLUDED.comment_count IS NOT NULL
             THEN EXCLUDED.comment_count_source
           WHEN crawler.contents.comments_disabled OR crawler.contents.comment_count IS NOT NULL
             THEN crawler.contents.comment_count_source
           ELSE EXCLUDED.comment_count_source END,
         comments_first_page=${retainedCommentPageSql("crawler.contents.comments_first_page", "EXCLUDED.comments_first_page")},
         is_members_only=CASE
           WHEN NOT $41::boolean THEN crawler.contents.is_members_only
           WHEN EXCLUDED.access_status='unknown'
             AND COALESCE(crawler.contents.access_status,'unknown')<>'unknown'
             THEN crawler.contents.is_members_only
           ELSE EXCLUDED.is_members_only END,
         access_status=CASE
           WHEN NOT $41::boolean THEN crawler.contents.access_status
           WHEN EXCLUDED.access_status='unknown'
             AND COALESCE(crawler.contents.access_status,'unknown')<>'unknown'
             THEN crawler.contents.access_status
           ELSE EXCLUDED.access_status END,
         access_status_source=CASE
           WHEN NOT $41::boolean THEN crawler.contents.access_status_source
           WHEN EXCLUDED.access_status='unknown'
             AND COALESCE(crawler.contents.access_status,'unknown')<>'unknown'
             THEN crawler.contents.access_status_source
           ELSE EXCLUDED.access_status_source END,
         extractor_version=COALESCE(EXCLUDED.extractor_version,crawler.contents.extractor_version),
         raw_json=crawler.contents.raw_json || EXCLUDED.raw_json
           || ${CONTENT_UPSERT_PUBLICATION_CONFLICT},
         last_seen_at=GREATEST(crawler.contents.last_seen_at,EXCLUDED.last_seen_at),
         last_enriched_at=CASE WHEN $41::boolean THEN $40::timestamptz ELSE crawler.contents.last_enriched_at END,
         playlist_last_seen_at=$40::timestamptz,
         player_last_observed_at=CASE WHEN $41::boolean THEN $40::timestamptz ELSE crawler.contents.player_last_observed_at END,
         next_last_observed_at=CASE WHEN $41::boolean THEN $40::timestamptz ELSE crawler.contents.next_last_observed_at END,
         last_observation_id=$42
     RETURNING content_key`,
    [
      contentKey,
      channelId,
      runId,
      contentType,
      classification.source,
      entry.id,
      entry.position,
      detail?.title ?? entry.title ?? null,
      url,
      detail?.thumbnail_url ?? entry.thumbnail_url ?? null,
      facts?.description ?? null,
      descriptionStatus,
      facts?.description == null ? null : facts.description_source,
      facts?.hashtags ?? [],
      facts?.keywords ?? [],
      published,
      publication.published_at_status,
      publishedSource,
      publishedPrecision,
      isRecent,
      facts?.duration_seconds ?? null,
      facts?.duration_seconds == null ? "unresolved" : "exact",
      facts?.duration_seconds == null ? null : facts.duration_source,
      facts?.view_count ?? null,
      facts?.view_count == null ? null : String(facts.view_count),
      facts?.view_count_status ?? "unresolved",
      facts?.view_count == null ? null : facts.view_count_source,
      facts?.like_count ?? null,
      facts?.like_count == null ? "unresolved" : facts.like_count_status,
      facts?.like_count == null ? null : facts.like_count_source,
      facts?.comments_disabled ? 0 : facts?.comment_count ?? null,
      facts?.comments_disabled ? "disabled" : facts?.comment_count == null ? "unresolved" : facts.comment_count_status,
      facts?.comments_disabled ?? null,
      facts?.comments_disabled || facts?.comment_count != null ? facts.comment_count_source : null,
      facts?.access_status === "members_only",
      facts?.access_status ?? "unknown",
      facts ? facts.access_status_source : null,
      facts?.extractor_version ?? null,
      JSON.stringify({
        incremental: {
          observation_id: observationId,
          playlist_position: entry.position,
          detail_collected: facts != null,
        },
        ...(publicationConflict ? { publication_evidence_conflict: publicationConflict } : {}),
      }),
      observedAt,
      detailComplete,
      observationId,
      classification.authoritative === true && facts?.access_status === "public",
      facts?.comments_first_page == null ? null : JSON.stringify(facts.comments_first_page),
    ],
  );
  const storedContentKey = text(stored.rows?.[0]?.content_key) ?? contentKey;
  return storedContentKey;
}

export async function refreshVideoContent(client, {
  row, facts, classification, storageAction, observedAt, observationId,
  collectNext, changeProbability, detailMetadataKey, intent, source,
}) {
  if (!["metrics_refresh", "detail_repair"].includes(intent)) {
    throw new TypeError("recent content mutation requires metrics_refresh or detail_repair intent");
  }
  const allowStaticRepair = intent === "detail_repair";
  const commentsObserved = facts.comments_disabled === true || facts.comment_count != null;
  const storageFacts = recentStorageFacts(row, facts, { allowStaticRepair });
  const storedPublication = normalizePublicationEvidence(row.stored_publication ?? row);
  const publicationSelection = allowStaticRepair || storedPublication.published_at == null
    ? selectPublicationEvidence(storedPublication, facts)
    : {
        evidence: storedPublication,
        selected: "current",
        reason_code: "immutable_publication_retained",
      };
  const publication = publicationSelection.evidence;
  const publicationConflict = publicationEvidenceConflictRecord(publicationSelection);
  const isRecent = publication.published_at == null
    ? null
    : new Date(publication.published_at).getTime() >= new Date(observedAt).getTime() - (30 * 86400000);
  await client.query(
    `UPDATE crawler.contents
     SET content_type=CASE WHEN $36::text IS NULL THEN content_type ELSE $36 END,
         content_type_source=CASE WHEN $36::text IS NULL THEN content_type_source ELSE $37 END,
         url=CASE
           WHEN $43::boolean THEN COALESCE($38,url)
           WHEN NULLIF(btrim(COALESCE(url,'')),'') IS NULL THEN COALESCE($38,url)
           ELSE url END,
         title=COALESCE($10,title),
         thumbnail_url=COALESCE($11,thumbnail_url),
         ${descriptionMutationSql({ current: "", observed: "$13::boolean", value: "$12",
           status: "CASE WHEN NULLIF(btrim(COALESCE($12::text,'')),'') IS NOT NULL THEN 'exact' ELSE 'empty' END",
           source: "$14" })},
         hashtags=${observedListSql("hashtags", "$15::text[]", "$16::boolean")},
         keywords=${observedListSql("keywords", "$17::text[]", "$18::boolean")},
         published_at=CASE
           WHEN $43::boolean THEN $19::timestamptz
           ELSE COALESCE(published_at,$19::timestamptz) END,
         published_at_status=CASE
           WHEN $43::boolean THEN $41::text
           WHEN published_at IS NULL AND $19::timestamptz IS NOT NULL THEN $41
           ELSE published_at_status END,
         published_at_source=CASE
           WHEN $43::boolean THEN $20::text
           WHEN published_at IS NULL AND $19::timestamptz IS NOT NULL THEN $20
           ELSE published_at_source END,
         published_at_precision=CASE
           WHEN $43::boolean THEN $21::text
           WHEN published_at IS NULL AND $19::timestamptz IS NOT NULL THEN $21
           ELSE published_at_precision END,
         is_recent=COALESCE($22::boolean,is_recent),
         duration_seconds=COALESCE($23::integer,duration_seconds),
         duration_status=CASE WHEN $23::integer IS NULL THEN duration_status ELSE 'exact' END,
         duration_source=CASE WHEN $23::integer IS NULL THEN duration_source ELSE $24 END,
         view_count=COALESCE($3,view_count),
         view_count_text=CASE WHEN $3::bigint IS NULL THEN view_count_text ELSE $3::text END,
         view_count_status=CASE WHEN $3::bigint IS NULL THEN view_count_status ELSE $46 END,
         view_count_source=CASE WHEN $3::bigint IS NULL THEN view_count_source ELSE $25 END,
         like_count=COALESCE($4,like_count),
         like_count_status=CASE WHEN $4::bigint IS NULL THEN like_count_status ELSE $44 END,
         like_count_source=CASE WHEN $4::bigint IS NULL THEN like_count_source ELSE $26 END,
         comment_count=CASE
           WHEN $35::boolean AND $6::boolean THEN 0
           WHEN $5::bigint IS NOT NULL THEN $5 ELSE comment_count END,
         comment_count_status=CASE
           WHEN $35::boolean AND $6::boolean THEN 'disabled'
           WHEN $5::bigint IS NULL THEN comment_count_status ELSE $45 END,
         comments_disabled=CASE WHEN $35::boolean THEN $6 ELSE comments_disabled END,
         comment_count_source=CASE
           WHEN $35::boolean THEN $27
           ELSE comment_count_source END,
         comments_first_page=${retainedCommentPageSql("comments_first_page", "$39::jsonb")},
         is_members_only=CASE
           WHEN $28 IN ('unknown','login_required') THEN is_members_only ELSE $28='members_only' END,
         access_status=CASE WHEN $28 IN ('unknown','login_required') THEN access_status ELSE $28 END,
         access_status_source=CASE WHEN $28 IN ('unknown','login_required') THEN access_status_source ELSE $29 END,
         live_scheduled_at=COALESCE($31::timestamptz,live_scheduled_at),
         live_started_at=COALESCE($32::timestamptz,live_started_at),
         live_ended_at=COALESCE($33::timestamptz,live_ended_at),
         extractor_version=COALESCE($30,extractor_version),
         raw_json=raw_json || jsonb_build_object(
           $40::text,jsonb_strip_nulls(jsonb_build_object(
             'observation_id',$8::uuid::text,
             'detail_collected',true,
             'source',$34::text,
             'publication_evidence_conflict',$42::jsonb
           ))
         ),
         player_last_observed_at=$2,
         next_last_observed_at=CASE WHEN $7::boolean THEN $2 ELSE next_last_observed_at END,
         last_observation_id=COALESCE($8::uuid,last_observation_id),last_enriched_at=$2,
         video_change_probability=COALESCE($9::double precision,video_change_probability)
     WHERE content_key=$1
       AND (player_last_observed_at IS NULL OR player_last_observed_at<=$2::timestamptz)`,
    [
      row.content_key,
      observedAt,
      facts.view_count,
      facts.like_count,
      facts.comment_count,
      facts.comments_disabled,
      collectNext,
      observationId,
      changeProbability,
      storageFacts.title,
      storageFacts.thumbnail_url,
      storageFacts.description,
      storageFacts.description_observed,
      storageFacts.description_source,
      storageFacts.hashtags,
      storageFacts.hashtags_observed,
      storageFacts.keywords,
      storageFacts.keywords_observed,
      publication.published_at,
      publication.published_at_source,
      publication.published_at_precision,
      isRecent,
      storageFacts.duration_seconds,
      storageFacts.duration_source,
      facts.view_count_source,
      facts.like_count_source,
      facts.comment_count_source,
      facts.access_status,
      facts.access_status_source,
      facts.extractor_version,
      storageFacts.live_scheduled_at,
      storageFacts.live_started_at,
      storageFacts.live_ended_at,
      source,
      commentsObserved,
      allowStaticRepair && storageAction.kind === "upsert" ? storageAction.content_type : null,
      allowStaticRepair && storageAction.kind === "upsert" ? storageAction.type_source : null,
      storageAction.kind === "upsert" ? classification.canonical_url : null,
      facts.comments_first_page == null ? null : JSON.stringify(facts.comments_first_page),
      detailMetadataKey,
      publication.published_at_status,
      publicationConflict == null ? null : JSON.stringify(publicationConflict),
      allowStaticRepair,
      facts.like_count_status,
      facts.comment_count_status,
      facts.view_count_status,
    ],
  );
  return publication;
}
