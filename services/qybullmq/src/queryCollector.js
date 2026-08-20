const QUERY_SOURCE_KINDS = new Set([
  "channel_keyword",
  "video_keyword",
  "video_hashtag",
]);

const ENABLED_VALUES = new Set(["1", "true", "yes", "on"]);

function emptyCollection() {
  return {
    quality_batch_id: null,
    collected_count: 0,
    inserted_count: 0,
    query_ids: [],
    query_texts: [],
  };
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function activeClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

function normalizedMetadataIdentityPolicy(value) {
  if (!value || typeof value !== "object") {
    throw new TypeError("identityPolicy is required for metadata Query collection");
  }
  const role = requiredText(value.role, "identityPolicy.role").toLowerCase();
  if (role !== "query_quality") {
    throw new TypeError(`identityPolicy must use query_quality role, received ${role}`);
  }
  const version = Number(value.version);
  if (!Number.isSafeInteger(version) || version <= 0) {
    throw new TypeError("identityPolicy.version must be a positive integer");
  }
  return Object.freeze({
    id: requiredText(value.id, "identityPolicy.id"),
    version,
    hash: requiredText(value.hash, "identityPolicy.hash"),
    role,
    language: requiredText(
      value.language ?? value.youtube_language,
      "identityPolicy.youtube_language",
    ),
    country: requiredText(
      value.country ?? value.youtube_country,
      "identityPolicy.youtube_country",
    ).toUpperCase(),
  });
}

export function resolveMetadataQueryIdentityPolicy({
  environment = process.env,
  catalog = loadIdentityPolicyCatalog(),
} = {}) {
  const policyId = requiredText(
    environment?.QUERY_METADATA_IDENTITY_POLICY_ID,
    "QUERY_METADATA_IDENTITY_POLICY_ID",
  );
  const policy = catalog?.policies?.get(policyId);
  if (!policy) throw new Error(`metadata Query Identity Policy does not exist: ${policyId}`);
  const normalized = normalizedMetadataIdentityPolicy(policy);
  const configuredLanguage = optionalText(environment?.QUERY_METADATA_LANGUAGE);
  const configuredCountry = optionalText(environment?.QUERY_METADATA_COUNTRY)?.toUpperCase() ?? null;
  if (configuredLanguage && configuredLanguage !== normalized.language) {
    throw new Error(
      `QUERY_METADATA_LANGUAGE=${configuredLanguage} conflicts with Identity Policy ${policyId}`,
    );
  }
  if (configuredCountry && configuredCountry !== normalized.country) {
    throw new Error(
      `QUERY_METADATA_COUNTRY=${configuredCountry} conflicts with Identity Policy ${policyId}`,
    );
  }
  return normalized;
}

function normalizedQueryText(value, sourceKind) {
  if (typeof value !== "string") return null;
  let output = value
    .normalize("NFKC")
    .replace(/[\u0000-\u001f\u007f]+/g, " ")
    .trim()
    .replace(/\s+/gu, " ");
  if (sourceKind === "video_hashtag") output = output.replace(/^#+/u, "").trim();
  output = output.toLowerCase();
  return output || null;
}

function normalizedSources(sources) {
  const byQuery = new Map();
  for (const source of sources ?? []) {
    const sourceKind = requiredText(source?.kind, "sources[].kind");
    if (!QUERY_SOURCE_KINDS.has(sourceKind)) {
      throw new TypeError(`unsupported Query source kind: ${sourceKind}`);
    }
    if (!Array.isArray(source?.values)) {
      throw new TypeError("sources[].values must be an array");
    }
    for (const value of source.values) {
      const queryText = normalizedQueryText(value, sourceKind);
      if (!queryText) continue;
      const existing = byQuery.get(queryText) ?? new Set();
      existing.add(sourceKind);
      byQuery.set(queryText, existing);
    }
  }
  return [...byQuery.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([queryText, sourceKinds]) => ({
      query_text: queryText,
      source_kinds: [...sourceKinds].sort(),
    }));
}

export async function collectQueryTerms(clientValue, input = {}) {
  const client = activeClient(clientValue);
  const qualityBatchId = requiredText(input.qualityBatchId, "qualityBatchId");
  const identityPolicy = normalizedMetadataIdentityPolicy(input.identityPolicy);
  const terms = normalizedSources(input.sources);
  if (terms.length === 0) return emptyCollection();

  const channelId = optionalText(input.channelId);
  const observationId = optionalText(input.observationId);
  const metadata = {
    auto_collected: true,
    collector: "metadata",
    identity_policy_id: identityPolicy.id,
    identity_policy_version: identityPolicy.version,
    identity_policy_hash: identityPolicy.hash,
    ...(channelId ? { first_channel_id: channelId } : {}),
    ...(observationId ? { first_observation_id: observationId } : {}),
  };
  const options = {
    source: "metadata_collector",
    include_video_search: true,
    language: identityPolicy.language,
    country: identityPolicy.country,
    identity_policy_id: identityPolicy.id,
    identity_policy_version: identityPolicy.version,
    identity_policy_hash: identityPolicy.hash,
    ...(channelId ? { channel_id: channelId } : {}),
    ...(observationId ? { observation_id: observationId } : {}),
  };
  const inserted = await client.query(
    `WITH input AS (
       SELECT query_text,source_kinds
       FROM jsonb_to_recordset($1::jsonb) AS item(query_text text,source_kinds jsonb)
     ), inserted AS (
       INSERT INTO crawler.query_terms (
         query_set_id,query_text,language,country,category,status,priority,
         quality_score,quality_status,quality_json,quality_checked_at,
         metadata_json,next_crawl_at,updated_at
       )
       SELECT (
                SELECT query_set_id FROM crawler.query_sets
                WHERE lower(name)='default'
                ORDER BY query_set_id
                LIMIT 1
              ),
              input.query_text,$5,$6,NULL,'active',100,
              NULL,'unscored',
              jsonb_build_object(
                'quality_batch_id',$2::text,
                'status','queued',
                'source','metadata_collector'
              ),
              NULL,
              $3::jsonb || jsonb_build_object('source_kinds',input.source_kinds),
              now(),now()
       FROM input
       WHERE NOT EXISTS (
         SELECT 1
         FROM crawler.query_terms existing
         WHERE lower(existing.query_text)=lower(input.query_text)
       )
       ORDER BY input.query_text
       ON CONFLICT (
         lower(query_text),COALESCE(language,''),COALESCE(country,''),COALESCE(category,'')
       ) DO NOTHING
       RETURNING query_id,query_text
     ), quality_batch AS (
       INSERT INTO crawler.query_quality_batches (
         quality_batch_id,status,total_count,options_json,updated_at
       )
       SELECT $2,'queued',count(*)::int,$4::jsonb,now()
       FROM inserted
       HAVING count(*)>0
       RETURNING quality_batch_id
     ), quality_tasks AS (
       INSERT INTO crawler.query_quality_tasks (
         quality_batch_id,query_id,status,updated_at
       )
       SELECT quality_batch.quality_batch_id,inserted.query_id,'queued',now()
       FROM inserted
       CROSS JOIN quality_batch
       RETURNING query_id
     )
     SELECT inserted.query_id,inserted.query_text
     FROM inserted
     JOIN quality_tasks USING (query_id)
     ORDER BY inserted.query_id`,
    [
      JSON.stringify(terms),
      qualityBatchId,
      JSON.stringify(metadata),
      JSON.stringify(options),
      identityPolicy.language,
      identityPolicy.country,
    ],
  );

  return {
    quality_batch_id: inserted.rows.length > 0 ? qualityBatchId : null,
    collected_count: terms.length,
    inserted_count: inserted.rows.length,
    query_ids: inserted.rows.map((row) => Number(row.query_id)),
    query_texts: inserted.rows.map((row) => row.query_text),
  };
}

export function queryMetadataCollectionEnabled(environment = process.env) {
  return ENABLED_VALUES.has(
    String(environment?.QUERY_METADATA_COLLECTION_ENABLED ?? "").trim().toLowerCase(),
  );
}

export async function collectObservedQueryTerms(
  client,
  input,
  { environment = process.env } = {},
) {
  if (!queryMetadataCollectionEnabled(environment)) return emptyCollection();
  const identityPolicy = input?.identityPolicy
    ?? resolveMetadataQueryIdentityPolicy({ environment });
  return collectQueryTerms(client, { ...input, identityPolicy });
}
import { loadIdentityPolicyCatalog } from "./identityPolicyCatalog.js";
