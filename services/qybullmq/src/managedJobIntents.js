import { createHash } from "node:crypto";
import { canonicalJsonValue } from "./canonicalJson.js";

export const MANAGED_JOB_INTENT_SCHEMA_VERSION = 1;
export const QUERY_QUALITY_SCORING_SCHEMA_VERSION = 1;
const INITIAL_MANAGED_DISPATCH_GENERATION = 1;

export class ManagedPolicyUnavailableError extends Error {
  constructor({ role, language, country }) {
    super(`no Identity Policy for ${role} ${language}/${country}`);
    this.name = "ManagedPolicyUnavailableError";
    this.code = "MANAGED_POLICY_UNAVAILABLE";
    this.role = role;
    this.language = language;
    this.country = country;
  }
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function positiveInteger(value, field) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return number;
}

function optionalText(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

function sha256(value) {
  return createHash("sha256").update(String(value)).digest("hex");
}

export function managedJobIntentHash(value) {
  return `sha256:${sha256(JSON.stringify(canonicalJsonValue(value)))}`;
}

function normalizedLanguage(value) {
  const language = requiredText(value, "language");
  return language.toLowerCase();
}

function normalizedCountry(value) {
  return requiredText(value, "country").toUpperCase();
}

export function resolveManagedJobPolicy({ role, language, country }, { policies = [] } = {}) {
  const normalizedRole = requiredText(role, "role").toLowerCase();
  const languageKey = normalizedLanguage(language);
  const countryKey = normalizedCountry(country);
  const matches = policies.filter((policy) => (
    String(policy?.role ?? "").trim().toLowerCase() === normalizedRole
      && String(policy?.youtube_language ?? "").trim().toLowerCase() === languageKey
      && String(policy?.youtube_country ?? "").trim().toUpperCase() === countryKey
  ));
  if (matches.length !== 1) {
    throw new ManagedPolicyUnavailableError({
      role: normalizedRole,
      language: requiredText(language, "language"),
      country: countryKey,
    });
  }
  const policy = matches[0];
  return Object.freeze({
    ...policy,
    id: requiredText(policy.id, "policy.id"),
    version: positiveInteger(policy.version, "policy.version"),
    hash: requiredText(policy.hash, "policy.hash"),
    role: normalizedRole,
  });
}

export function buildDiscoverPageIntent(input = {}, options = {}) {
  const pageId = requiredText(input.pageId, "pageId");
  const queryText = requiredText(input.queryText, "queryText");
  const queryId = positiveInteger(input.queryId, "queryId");
  const pageNo = positiveInteger(input.pageNo, "pageNo");
  const discoveryRunId = requiredText(input.discoveryRunId, "discoveryRunId");
  const language = requiredText(input.language, "language");
  const country = normalizedCountry(input.country);
  const policy = resolveManagedJobPolicy({
    role: "discover",
    language,
    country,
  }, options);
  const continuationToken = optionalText(input.continuationToken);
  const continuationTokenHash = continuationToken ? `sha256:${sha256(continuationToken)}` : null;
  const immutableIntent = canonicalJsonValue({
    schema_version: MANAGED_JOB_INTENT_SCHEMA_VERSION,
    page_id: pageId,
    query_id: queryId,
    query_text: queryText,
    page_no: pageNo,
    discovery_run_id: discoveryRunId,
    continuation_parent_page_id: optionalText(input.continuationParentPageId),
    continuation_token_hash: continuationTokenHash,
    request_language: language,
    request_country: country,
    identity_policy_id: policy.id,
    identity_policy_version: policy.version,
    identity_policy_hash: policy.hash,
    search_filter: optionalText(input.searchFilter) || "video",
    sort: optionalText(input.sort) || "popularity",
    time_window: optionalText(input.timeWindow) || "this_year",
  });
  return Object.freeze({
    pageId,
    queryId,
    queryText,
    pageNo,
    discoveryRunId,
    pipelineCycleId: optionalText(input.pipelineCycleId),
    dispatchBatchId: optionalText(input.dispatchBatchId),
    priority: Number.isSafeInteger(Number(input.priority)) ? Number(input.priority) : 100,
    category: optionalText(input.category),
    language,
    country,
    policy,
    continuationParentPageId: immutableIntent.continuation_parent_page_id,
    continuationTokenHash,
    immutableIntent,
    intentHash: managedJobIntentHash(immutableIntent),
    managedIntent: Object.freeze({
      discovery_run_id: discoveryRunId,
      pipeline_cycle_id: optionalText(input.pipelineCycleId),
      continuation_token: continuationToken,
      yt_config: input.ytConfig ?? null,
      category: optionalText(input.category),
      demo: input.demo === true,
      channel_id: optionalText(input.channelId),
      search_filter: immutableIntent.search_filter,
      sort: immutableIntent.sort,
      time_window: immutableIntent.time_window,
    }),
    jobPayload: Object.freeze({
      page_id: pageId,
      intent_schema_version: MANAGED_JOB_INTENT_SCHEMA_VERSION,
      dispatch_generation: INITIAL_MANAGED_DISPATCH_GENERATION,
    }),
  });
}

function normalizedScoringOptions(options = {}) {
  const minSubscriberCount = Number(options.min_subscriber_count ?? 1000);
  const topVideos = Number(options.top_videos ?? 20);
  return canonicalJsonValue({
    min_subscriber_count: Number.isFinite(minSubscriberCount) && minSubscriberCount >= 0
      ? Math.floor(minSubscriberCount)
      : 1000,
    top_videos: Number.isFinite(topVideos) && topVideos > 0 ? Math.floor(topVideos) : 20,
    include_video_search: options.include_video_search !== false,
    scoring_schema_version: QUERY_QUALITY_SCORING_SCHEMA_VERSION,
  });
}

function qualityTaskLocale(task, batchOptions) {
  const language = optionalText(batchOptions.language)
    || optionalText(batchOptions.effective_language)
    || optionalText(task.language);
  const country = optionalText(batchOptions.country)
    || optionalText(batchOptions.effective_country)
    || optionalText(task.country);
  if (!language || !country) {
    throw new ManagedPolicyUnavailableError({
      role: "query_quality",
      language: language || "unresolved",
      country: country || "unresolved",
    });
  }
  return { language, country: normalizedCountry(country) };
}

function chunkId(qualityBatchId, groupKey, qualityTaskIds) {
  const digest = sha256(JSON.stringify({
    quality_batch_id: qualityBatchId,
    group_key: groupKey,
    quality_task_ids: qualityTaskIds,
  })).slice(0, 32);
  return `quality-chunk:${qualityBatchId}:${digest}`;
}

export function buildQueryQualityChunkIntents(input = {}, options = {}) {
  const qualityBatchId = requiredText(input.qualityBatchId, "qualityBatchId");
  const batchOptions = input.batchOptions && typeof input.batchOptions === "object"
    ? input.batchOptions
    : {};
  const chunkSize = positiveInteger(input.chunkSize ?? 3, "chunkSize");
  const scoringOptions = normalizedScoringOptions(batchOptions);
  const scoringOptionsHash = managedJobIntentHash(scoringOptions);
  const groups = new Map();

  for (const task of input.tasks ?? []) {
    const qualityTaskId = positiveInteger(task?.quality_task_id, "quality_task_id");
    const queryId = positiveInteger(task?.query_id, "query_id");
    const locale = qualityTaskLocale(task, batchOptions);
    const policy = resolveManagedJobPolicy({
      role: "query_quality",
      language: locale.language,
      country: locale.country,
    }, options);
    const groupKey = JSON.stringify([
      qualityBatchId,
      locale.language.toLowerCase(),
      locale.country,
      policy.id,
      policy.version,
      policy.hash,
      scoringOptionsHash,
    ]);
    const group = groups.get(groupKey) ?? {
      groupKey,
      language: locale.language,
      country: locale.country,
      policy,
      tasks: [],
    };
    group.tasks.push({ qualityTaskId, queryId });
    groups.set(groupKey, group);
  }

  const output = [];
  for (const group of [...groups.values()].sort((left, right) => left.groupKey.localeCompare(right.groupKey))) {
    group.tasks.sort((left, right) => left.qualityTaskId - right.qualityTaskId);
    for (let offset = 0; offset < group.tasks.length; offset += chunkSize) {
      const members = group.tasks.slice(offset, offset + chunkSize);
      const qualityTaskIds = members.map((member) => member.qualityTaskId);
      const qualityChunkId = chunkId(qualityBatchId, group.groupKey, qualityTaskIds);
      const immutableIntent = canonicalJsonValue({
        schema_version: MANAGED_JOB_INTENT_SCHEMA_VERSION,
        quality_batch_id: qualityBatchId,
        quality_chunk_id: qualityChunkId,
        quality_task_ids: qualityTaskIds,
        effective_language: group.language,
        effective_country: group.country,
        identity_policy_id: group.policy.id,
        identity_policy_version: group.policy.version,
        identity_policy_hash: group.policy.hash,
        scoring_options: scoringOptions,
        scoring_options_hash: scoringOptionsHash,
      });
      output.push(Object.freeze({
        qualityBatchId,
        qualityChunkId,
        qualityTaskIds: Object.freeze(qualityTaskIds),
        members: Object.freeze(members),
        effectiveLanguage: group.language,
        effectiveCountry: group.country,
        policy: group.policy,
        scoringOptions: Object.freeze(scoringOptions),
        scoringOptionsHash,
        immutableIntent,
        chunkIntentHash: managedJobIntentHash(immutableIntent),
        jobPayload: Object.freeze({
          quality_chunk_id: qualityChunkId,
          intent_schema_version: MANAGED_JOB_INTENT_SCHEMA_VERSION,
          dispatch_generation: INITIAL_MANAGED_DISPATCH_GENERATION,
        }),
      }));
    }
  }
  return output;
}
