import { nanoid } from "nanoid";
import {
  combinedAboutObservationMetrics,
  normalizeAboutMetrics,
} from "./aboutMetrics.js";
import { normalizeAboutObservationCurrent } from "./aboutCurrent.js";
import {
  aboutObservationIdempotencyKey,
  recordAboutObservation,
} from "./aboutObservationStore.js";
import {
  getAgentConfigById,
  getLocalOfflineAgentConfig,
} from "./agentConfig.js";
import {
  isLocalOfflineAgentConfig,
  selectAgentConfigForJob,
} from "./agentExecutionPolicy.js";
import { buildAgentPublicationRun } from "./agentPublicationCurrent.js";
import {
  canonicalizeCrawlerCountry,
  countryRequiresAgent,
  hasResolvedCrawlerCountry,
  normalizeCrawlerCountry,
  parseYoutubeAboutCountry,
} from "./agentCountryPolicy.js";
import { evaluateChannelQualification } from "./channelQualification.js";
import {
  classifyTerminalChannelError,
  markChannelRemoved,
  terminalChannelEvidenceFromInitialData,
} from "./channelLifecycle.js";
import {
  channelApiFallbackMissingFields,
  resolveChannelApiFallback,
  resolveChannelQualificationAfterApi,
} from "./channelApiFallback.js";
import {
  bindPreparedChannelRun,
  prepareChannelRun,
  prepareChannelRunAndBindJob,
} from "./channelRunBinding.js";
import {
  materializeBusinessRunBinding,
  terminateBusinessRunBinding,
} from "./businessRunBindingStore.js";
import {
  channelCandidateCanFailAdmission,
  claimChannelRegistryPromotion,
  resolveChannelRegistryRunId,
} from "./channelRegistryPromotion.js";
import { activeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";
import {
  beginChannelCandidateValidation,
  markChannelCandidateAlreadyPromoted,
  recordAcceptedChannelCandidateSnapshot,
  rejectChannelCandidateAdmission,
} from "./channelCandidateAttemptMutations.js";
import {
  currentChannelExecution,
  currentChannelExecutionAbortSignal,
} from "./channelExecutionContext.js";
import { throwIfAborted } from "./abortSignal.js";
import {
  classifyContentWindow,
  CONTENT_WINDOW_POLICY_VERSION,
  detailAgeDays,
} from "./contentWindow.js";
import { contentDetailBatchStopReason } from "./contentDetailBatchPolicy.js";
import {
  publicationEvidenceFromFields,
  publicationEvidenceConflictRecord,
  selectPublicationEvidence,
} from "./publicationTimeEvidence.js";
import { query, withTransaction } from "./db.js";
import {
  IncrementalAgentResultStore,
  isIncrementalAgentJob,
  markAgentChannelsRunning,
  persistAgentChannelFailure,
  persistAgentChannelSuccess,
} from "./incrementalAgentResultStore.js";
import { recordInitialFullObservations } from "./initialFullObservations.js";
import {
  completeAboutOnlyPublicationGapRepair,
  deferAboutObservationUntilRepairFinalize,
  isAboutOnlyPublicationGapRepair,
  publicationGapRepairJobIntent,
} from "./publicationGapRepairExecution.js";
import { publishReadyDiscoveryPages } from "./discoveryPageWakeup.js";
import {
  isParserContractError,
  parserContractDetails,
  ParserContractError,
} from "./localizedParsing.js";
import {
  normalizeDetailConcurrency,
  processWithOrderedPrefetch,
} from "./detailConcurrency.js";
import {
  classifiedOnlyResolutionAction,
  contentDetailFailureError,
  detailResolutionAction,
  hasResolvedDuration,
  isLiveInProgress,
  isTransientYoutubeError,
  isYoutubeIpBlockedError,
  isYoutubeNetworkRetryableError,
  missingLikeIsZero,
  positiveDurationSeconds,
  unfinishedLiveReason,
  unresolvedParserContractError,
  videoAccessStatus,
  youtubeJsDetailFallbackReasons,
} from "./detailPolicy.js";
import {
  finalizedProfileIsCurrent,
  finalizeDispatchRevision,
  finalizePublicationContext,
  finalizeSourceRevision,
  finalizeStatusCanAdvance,
  isFinalizableChannelStatus,
  isSuccessfulPublicationFinalize,
  isCurrentChannelRun,
  resolveFinalizeStatus,
} from "./finalizePolicy.js";
import { reconcileFullCrawlAgentState } from "./fullCrawlAgentState.js";
import { fullRepairRunMetadata } from "./fullRepairDispatch.js";
import {
  commitFinalizedProfile,
  synchronizeFinalizedRun,
} from "./finalizedProfileStore.js";
import { runAgentBatch } from "./llmAgent.js";
import { LocalOfflineProfileExecutor } from "./localOfflineProfileExecutor.js";
import { AGENT_TAXONOMY_VERSION } from "./publicationContract.js";
import { applyMigrationActivityGate as applyMigrationActivityGateTransaction } from "./migrationActivityGate.js";
import {
  evaluateMigrationUploadsActivity,
  migrationActivityCanFinalize,
} from "./migrationActivityPolicy.js";
import {
  fullVideoStorageAction,
  updateExistingFullVideoAccess,
  upsertFullVideoContent,
} from "./fullVideoContentStore.js";
import { createQueues, queuesByRole, safeJobId } from "./queues.js";
import { putRawObject } from "./storage.js";
import {
  fetchChannelInitial,
  fetchChannelDataApiDetails,
  fetchChannelUploads,
  fetchChannelYtDlpMetadata,
  fetchVideoDataApiDetails,
  fetchVideoCommentThreadsDataApi,
  fetchVideoYtDlpDetail,
  parseChannelHeader,
} from "./youtube.js";
import { resolveYoutubeContentType } from "./youtubeContentType.js";
import { resolveVideoDisposition } from "./videoDisposition.js";
import { isYoutubeCollectionFailureError } from "./youtubePlayability.js";
import {
  fetchYoutubeJsCommentFirstPage,
  fetchYoutubeJsVideoDetail,
  openYoutubeJsChannel,
  youtubeJsChannelEnabled,
  youtubeJsDetailEnabled,
} from "./youtubeJs.js";
import { commentFirstPageNeedsResolution } from "./youtubeCommentPage.js";
import {
  missingApiFields,
  storedDataApiReplayResult,
  terminalApiMissingFields,
  youtubeApiTaskResultEvidence,
} from "./youtubeDataApiEvidence.js";
import {
  finishCheckpointRepairExecution,
  prepareCheckpointRepairCandidates,
} from "./checkpointRepair.js";
import {
  normalizeVideoTextMetadata,
} from "./videoMetadata.js";

const COMMENT_DETAIL_FIELDS = [
  "comment_count",
  "comment_count_status",
  "comment_count_source",
  "comments_disabled",
  "comments_status_source",
  "comments_first_page",
  "comments_first_page_status",
  "comments_first_page_source",
];
const ACCESS_DETAIL_FIELDS = [
  "access_status",
  "access_status_source",
  "availability",
  "privacy_status",
  "is_unlisted",
];
const queues = createQueues();
const incrementalAgentResultStore = new IncrementalAgentResultStore({
  withTransaction,
  maxAttempts: intValue(process.env.INCREMENTAL_AGENT_MAX_ATTEMPTS, 8, 1, 50),
});

function throwIfChannelExecutionAborted() {
  throwIfAborted(currentChannelExecutionAbortSignal());
}
const localOfflineProfileExecutor = new LocalOfflineProfileExecutor({
  loadLatestRunIds: async (channelIds) => {
    if (!Array.isArray(channelIds) || channelIds.length === 0) return new Map();
    const rows = await query(
      `SELECT channel_id,latest_run_id
       FROM crawler.channels
       WHERE channel_id=ANY($1::text[])`,
      [channelIds],
    );
    return new Map(rows.rows.map((row) => [row.channel_id, row.latest_run_id]));
  },
});
const language = process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en";
const country = process.env.YOUTUBE_COUNTRY || "BR";
const defaultMinSubscribers = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000);
const defaultContentLimit = Number(process.env.YOUTUBE_CHANNEL_CONTENT_LIMIT || 30);
const defaultContentMaxAgeDays = Number(process.env.YOUTUBE_CONTENT_MAX_AGE_DAYS || 90);
const defaultDetailConcurrency = normalizeDetailConcurrency(process.env.YOUTUBE_DETAIL_CONCURRENCY, 2);
const channelInlineDetails = String(process.env.YOUTUBE_CHANNEL_INLINE_DETAILS || "true").trim().toLowerCase() !== "false";
let crawlSettingsCache = { expiresAt: 0, value: null };
let youtubeApiSettingsCache = { expiresAt: 0, value: null };
let agentLlmSettingsCache = { expiresAt: 0, key: null, value: null };

function intValue(value, fallback, min, max) {
  const number = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(min, Math.min(max, number));
}

function numberValue(value, fallback, min, max) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(min, Math.min(max, number));
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function integer(value) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.round(number) : null;
}

function parseKeys(value) {
  const values = Array.isArray(value) ? value : String(value ?? "").split(/[\s,;]+/);
  return [...new Set(values.map((item) => String(item ?? "").trim()).filter(Boolean))];
}

function mergeDefined(base, patch) {
  const output = { ...(base ?? {}) };
  for (const [key, value] of Object.entries(patch ?? {})) {
    if (value !== undefined && value !== null && value !== "") output[key] = value;
  }
  return output;
}

function migrationCommentObservation(detail) {
  const page = detail?.comments_first_page;
  const countValue = integer(detail?.comment_count);
  const totalValue = integer(page?.total_count);
  const returnedValue = integer(page?.returned_count);
  const count = countValue != null && countValue >= 0 ? countValue : null;
  const totalCount = totalValue != null && totalValue >= 0 ? totalValue : null;
  const returnedCount = Math.max(
    returnedValue != null && returnedValue >= 0 ? returnedValue : 0,
    Array.isArray(page?.comments) ? page.comments.length : 0,
  );
  const hasVisibleComments = (count ?? 0) > 0
    || (totalCount ?? 0) > 0
    || returnedCount > 0;
  const disabled = detail?.comments_disabled === true
    || detail?.comment_count_status === "disabled";
  const observed = hasVisibleComments
    || disabled
    || detail?.comments_disabled === false
    || count != null
    || ["zero_from_surface", "zero_from_upcoming", "zero_from_empty"]
      .includes(detail?.comment_count_status);
  return {
    detail: detail ?? {},
    count,
    totalCount,
    returnedCount,
    hasVisibleComments,
    disabled,
    rank: hasVisibleComments ? 3 : disabled ? 2 : observed ? 1 : 0,
  };
}

function applyMigrationCommentObservation(output, observation) {
  for (const field of COMMENT_DETAIL_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(observation.detail, field)) {
      output[field] = observation.detail[field];
    } else {
      delete output[field];
    }
  }
  if (observation.hasVisibleComments) {
    output.comments_disabled = false;
    if ((observation.count ?? 0) <= 0 && (observation.totalCount ?? 0) > 0) {
      output.comment_count = observation.totalCount;
    } else if ((observation.count ?? 0) <= 0 && observation.returnedCount > 0) {
      output.comment_count = null;
    }
    if (output.comment_count_status === "disabled" || !output.comment_count_status) {
      output.comment_count_status = output.comment_count == null ? "unresolved" : "exact";
    }
  } else if (observation.disabled) {
    output.comment_count = 0;
    output.comment_count_status = "disabled";
    output.comments_disabled = true;
  }
}

function applyMigrationAccessObservation(output, detail) {
  for (const field of ACCESS_DETAIL_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(detail, field)) {
      output[field] = detail[field];
    } else {
      delete output[field];
    }
  }
  const access = accessFromDetail(detail);
  if (access.access_status_source) {
    output.access_status_source = access.access_status_source;
  } else {
    delete output.access_status_source;
  }
  if (access.access_status === "unlisted") output.is_unlisted = true;
}

function youtubeJsDisabledCommentsNeedVerification(detail) {
  return detail?.comments_disabled === true || detail?.comment_count_status === "disabled";
}

function withoutUnverifiedYoutubeJsDisabledComments(detail) {
  if (!youtubeJsDisabledCommentsNeedVerification(detail)) return detail;
  return {
    ...detail,
    comment_count: null,
    comment_count_status: "unresolved",
    comment_count_source: null,
    comments_disabled: null,
    comments_status_source: null,
    comments_first_page: null,
    comments_first_page_status: "unresolved",
    comments_first_page_source: null,
  };
}

function mergeChannelMetadata(base, patch) {
  const output = mergeDefined(base, patch);
  const baseDescription = text(base?.description);
  const patchDescription = text(patch?.description);
  if (baseDescription && (!patchDescription || baseDescription.length > patchDescription.length)) {
    output.description = baseDescription;
  }
  return output;
}

function detailPublicationEvidence(detail, { afterApi = false } = {}) {
  return publicationEvidenceFromFields(detail, {
    missingStatus: afterApi ? "unavailable" : "unresolved",
  });
}

function mergeDetail(base, patch) {
  const previous = base ?? {};
  const next = patch ?? {};
  const output = mergeDefined(previous, next);
  const previousSignals = previous.content_type_signals;
  const nextSignals = next.content_type_signals;
  if (previousSignals && typeof previousSignals === "object") {
    output.content_type_signals = nextSignals && typeof nextSignals === "object"
      ? mergeDefined(previousSignals, nextSignals)
      : previousSignals;
  }
  const previousAccess = String(previous.access_status ?? "").trim().toLowerCase();
  const nextAccess = String(next.access_status ?? "").trim().toLowerCase();
  if (previousAccess && previousAccess !== "unknown" && (!nextAccess || nextAccess === "unknown")) {
    applyMigrationAccessObservation(output, previous);
    output.playability_kind = previous.playability_kind;
    output.playability_reason_code = previous.playability_reason_code;
    output.playability_retry_mode = previous.playability_retry_mode;
  } else if (
    (previousAccess === "unlisted" && nextAccess === "public")
    || (previousAccess === "public" && nextAccess === "unlisted")
  ) {
    applyMigrationAccessObservation(
      output,
      previousAccess === "unlisted" ? previous : next,
    );
  }
  const publicationSelection = selectPublicationEvidence(
    detailPublicationEvidence(previous),
    detailPublicationEvidence(next),
  );
  Object.assign(output, publicationSelection.evidence);
  if (publicationSelection.selected === "current") output.published_text = previous.published_text;
  else if (publicationSelection.selected === "candidate") output.published_text = next.published_text;
  const publicationConflict = publicationEvidenceConflictRecord(publicationSelection);
  if (publicationConflict) output.publication_evidence_conflict = publicationConflict;

  const previousComment = migrationCommentObservation(previous);
  const nextComment = migrationCommentObservation(next);
  const selectedComment = nextComment.rank >= previousComment.rank
    ? nextComment
    : previousComment;
  if (selectedComment.rank > 0) {
    applyMigrationCommentObservation(output, selectedComment);
  }

  const descriptionRank = { unresolved: 0, unavailable: 1, empty: 2, exact: 3 };
  const previousText = normalizeVideoTextMetadata(previous);
  const nextText = normalizeVideoTextMetadata(next);
  const previousDescriptionRank = descriptionRank[previousText.description_status] ?? 0;
  const nextDescriptionRank = descriptionRank[nextText.description_status] ?? 0;
  const previousDescriptionIsBetter = previousDescriptionRank > nextDescriptionRank
    || (
      previousDescriptionRank === nextDescriptionRank
      && previousDescriptionRank === descriptionRank.exact
      && String(previousText.description ?? "").length > String(nextText.description ?? "").length
    );
  if (previousDescriptionIsBetter) {
    output.description = previousText.description;
    output.description_status = previousText.description_status;
    output.description_source = previousText.description_source;
  } else {
    output.description = nextText.description;
    output.description_status = nextText.description_status;
    output.description_source = nextText.description_source;
  }
  if (nextText.keywords_observed) {
    output.keywords = nextText.keywords;
    output.keywords_observed = true;
  } else {
    output.keywords = previousText.keywords;
    output.keywords_observed = previousText.keywords_observed;
  }
  output.hashtags_observed = typeof output.title === "string"
    && ["exact", "empty"].includes(output.description_status);
  return normalizeVideoTextMetadata(output);
}

function normalChannelUrl(channel) {
  return text(channel?.channel_url)
    ?? (text(channel?.handle) ? `https://www.youtube.com/${String(channel.handle).startsWith("@") ? channel.handle : `@${channel.handle}`}` : null)
    ?? `https://www.youtube.com/channel/${channel.channel_id}`;
}

export async function getCrawlSettingsV2() {
  const now = Date.now();
  if (crawlSettingsCache.value && crawlSettingsCache.expiresAt > now) return crawlSettingsCache.value;
  const fallback = {
    minSubscriberCount: intValue(defaultMinSubscribers, 1000, 0, 1_000_000_000),
    discoverStopMinQualifiedRatio: 1 / 3,
    channelContentLimit: intValue(defaultContentLimit, 30, 1, 100),
    contentMaxAgeDays: intValue(defaultContentMaxAgeDays, 90, 0, 3650),
    detailMaxAttempts: intValue(process.env.YOUTUBE_DETAIL_MAX_ATTEMPTS, 3, 1, 10),
    detailConcurrency: defaultDetailConcurrency,
    publishedAtRequiredPrecision: "date_only",
  };
  try {
    const rows = await query("SELECT value_json FROM crawler.settings WHERE setting_key = 'crawl' LIMIT 1");
    const value = rows.rows[0]?.value_json ?? {};
    const settings = {
      minSubscriberCount: intValue(value.min_subscriber_count, fallback.minSubscriberCount, 0, 1_000_000_000),
      discoverStopMinQualifiedRatio: numberValue(value.discover_stop_min_qualified_ratio, fallback.discoverStopMinQualifiedRatio, 0, 1),
      channelContentLimit: intValue(value.channel_content_limit, fallback.channelContentLimit, 1, 100),
      contentMaxAgeDays: intValue(value.content_max_age_days, fallback.contentMaxAgeDays, 0, 3650),
      detailMaxAttempts: intValue(value.detail_max_attempts, fallback.detailMaxAttempts, 1, 10),
      detailConcurrency: normalizeDetailConcurrency(value.detail_concurrency, fallback.detailConcurrency),
      publishedAtRequiredPrecision: "date_only",
    };
    await query(
      `INSERT INTO crawler.settings (setting_key, value_json, updated_at)
       VALUES ('crawl', $1::jsonb, now())
       ON CONFLICT (setting_key) DO UPDATE
       SET value_json = crawler.settings.value_json || EXCLUDED.value_json,
           updated_at = now()`,
      [JSON.stringify({
        min_subscriber_count: settings.minSubscriberCount,
        discover_stop_min_qualified_ratio: settings.discoverStopMinQualifiedRatio,
        channel_content_limit: settings.channelContentLimit,
        content_max_age_days: settings.contentMaxAgeDays,
        detail_max_attempts: settings.detailMaxAttempts,
        detail_concurrency: settings.detailConcurrency,
        published_at_required_precision: settings.publishedAtRequiredPrecision,
      })],
    );
    crawlSettingsCache = { expiresAt: now + 30000, value: settings };
    return settings;
  } catch {
    crawlSettingsCache = { expiresAt: now + 30000, value: fallback };
    return fallback;
  }
}

export async function getYoutubeApiSettingsV2() {
  const now = Date.now();
  if (youtubeApiSettingsCache.value && youtubeApiSettingsCache.expiresAt > now) return youtubeApiSettingsCache.value;
  const fallback = {
    apiKeys: parseKeys(process.env.YOUTUBE_DATA_API_KEYS || process.env.YOUTUBE_DATA_API_KEY || ""),
    timeoutMs: 12000,
    batchSize: 50,
    dailyRequestLimit: intValue(process.env.YOUTUBE_DATA_API_DAILY_REQUEST_LIMIT, 500, 0, 10000),
    fallbackMode: process.env.YOUTUBE_DATA_API_FALLBACK_MODE === "disabled" ? "disabled" : "emergency",
  };
  try {
    const rows = await query("SELECT value_json FROM crawler.settings WHERE setting_key = 'youtube_api' LIMIT 1");
    const value = rows.rows[0]?.value_json ?? {};
    const settings = {
      apiKeys: parseKeys(value.api_keys?.length ? value.api_keys : (value.api_key || fallback.apiKeys)),
      timeoutMs: intValue(value.timeout_ms, fallback.timeoutMs, 1000, 60000),
      batchSize: intValue(value.batch_size, fallback.batchSize, 1, 50),
      dailyRequestLimit: intValue(value.daily_request_limit, fallback.dailyRequestLimit, 0, 10000),
      fallbackMode: value.fallback_mode === "disabled" ? "disabled" : "emergency",
    };
    youtubeApiSettingsCache = { expiresAt: now + 30000, value: settings };
    return settings;
  } catch {
    youtubeApiSettingsCache = { expiresAt: now + 30000, value: fallback };
    return fallback;
  }
}

function agentSettingKey(configId) {
  const id = Number(configId);
  return Number.isFinite(id) && id > 0 ? `agent_llm:${Math.floor(id)}` : "agent_llm";
}

async function getAgentLlmSettings(config) {
  const now = Date.now();
  const key = agentSettingKey(config?.config_id);
  if (agentLlmSettingsCache.value && agentLlmSettingsCache.key === key && agentLlmSettingsCache.expiresAt > now) {
    return agentLlmSettingsCache.value;
  }
  const fallback = {
    apiKeys: parseKeys(process.env.AGENT_API_KEYS || process.env.AGENT_API_KEY || ""),
    baseUrl: text(process.env.AGENT_BASE_URL),
  };
  const rows = await query("SELECT value_json FROM crawler.settings WHERE setting_key = $1 LIMIT 1", [key]);
  const legacy = key === "agent_llm"
    ? { rows: [] }
    : await query("SELECT value_json FROM crawler.settings WHERE setting_key = 'agent_llm' LIMIT 1");
  const value = rows.rows[0]?.value_json ?? legacy.rows[0]?.value_json ?? {};
  const settings = {
    apiKeys: parseKeys(value.api_keys?.length ? value.api_keys : (value.api_key || fallback.apiKeys)),
    baseUrl: text(value.base_url) ?? fallback.baseUrl,
  };
  agentLlmSettingsCache = { expiresAt: now + 30000, key, value: settings };
  return settings;
}

async function saveJsonRaw({ objectType, entityType, entityId, source, payload, metadata = {} }) {
  return putRawObject({
    objectType,
    entityType,
    entityId,
    source,
    payload,
    contentType: "application/json; charset=utf-8",
    metadata,
  });
}

async function saveFetchedRaw({ fetched, objectType, entityType, entityId, source, metadata = {} }) {
  if (!fetched?.rawText) return null;
  return putRawObject({
    objectType,
    entityType,
    entityId,
    source,
    payload: fetched.rawText,
    contentType: fetched.rawContentType || "text/html; charset=utf-8",
    metadata: { url: fetched.url ?? null, ...metadata },
  });
}

function accessFromDetail(detail, fallback = "unknown") {
  const privacyStatus = String(detail?.privacy_status ?? "").toLowerCase();
  const explicitSource = text(detail?.access_status_source);
  const extractorSource = String(detail?.source ?? "").startsWith("youtubejs")
    ? "youtubejs_playability"
    : "yt_dlp_availability";
  const accessStatus = videoAccessStatus(detail, fallback);
  const privacySource = privacyStatus === accessStatus ? "youtube_data_api_privacy" : null;
  const unlistedSource = accessStatus === "unlisted"
    && detail?.is_unlisted === true
    && String(detail?.source ?? "").startsWith("youtubejs")
    ? "youtubejs_microformat"
    : null;
  return {
    is_members_only: accessStatus === "members_only",
    access_status: accessStatus,
    access_status_source: accessStatus === "unknown"
      ? null
      : privacySource ?? explicitSource ?? unlistedSource ?? extractorSource,
  };
}

function normalizeResolvedDetail(detail, { afterApi = false, access = null } = {}) {
  const output = normalizeVideoTextMetadata(detail, { afterApi });
  const resolvedAccess = access ?? accessFromDetail(output);
  const liveStatus = String(output.live_status ?? "").toLowerCase();
  const isUpcoming = ["is_upcoming", "upcoming"].includes(liveStatus);
  if (missingLikeIsZero(output, resolvedAccess.access_status)) {
    output.like_count = 0;
    output.like_count_status = "zero_from_empty";
    output.like_count_source = output.like_count_source ?? "public_empty_is_zero";
  } else {
    output.like_count_status = output.like_count == null
      ? (afterApi ? "unavailable" : "unresolved")
      : output.like_count_status === "zero_from_empty" ? "zero_from_empty" : "exact";
  }
  if (output.comments_disabled === true) {
    output.comment_count = 0;
    output.comment_count_status = "disabled";
  } else if (output.comment_count != null) {
    output.comment_count_status = ["zero_from_surface", "zero_from_upcoming"].includes(output.comment_count_status)
      ? output.comment_count_status
      : "exact";
  } else if (output.comments_disabled === false && output.comments_status_source) {
    output.comment_count = 0;
    output.comment_count_status = "zero_from_surface";
    output.comment_count_source = output.comment_count_source ?? output.comments_status_source;
  } else if (isUpcoming && resolvedAccess.access_status === "public") {
    output.comment_count = 0;
    output.comment_count_status = "zero_from_upcoming";
    output.comments_disabled = false;
    output.comment_count_source = "youtube_upcoming_state";
  } else {
    output.comment_count_status = afterApi ? "unavailable" : "unresolved";
  }
  Object.assign(output, detailPublicationEvidence(output, { afterApi }));
  const durationSeconds = positiveDurationSeconds(output.duration_seconds);
  if (durationSeconds != null) {
    output.duration_seconds = durationSeconds;
  } else {
    output.duration_seconds = null;
    if (!hasResolvedDuration({ length_text: output.length_text })) output.length_text = null;
  }
  if (hasResolvedDuration(output)) {
    output.duration_status = "exact";
  } else if (isLiveInProgress(output)) {
    output.duration_status = "unavailable";
    output.duration_source = "live_in_progress_not_applicable";
  } else {
    output.duration_status = afterApi ? "unavailable" : "unresolved";
  }
  output.view_count_status = output.view_count_text ? "exact" : (afterApi ? "unavailable" : "unresolved");
  if (!output.view_count_text && resolvedAccess.is_members_only && (output.like_count != null || output.comment_count != null)) {
    const estimated = Math.round((Number(output.like_count || 0) / 0.028) + (Number(output.comment_count || 0) / 0.0028));
    if (estimated > 0) {
      output.view_count_text = String(estimated);
      output.view_count_status = "estimated";
      output.view_count_source = "likes_comments_estimate";
      output.view_count_formula = "likes/0.028 + comments/0.0028";
    }
  }
  return { detail: output, access: resolvedAccess };
}

function detailFromCandidate(row) {
  const flat = row?.result_json?.flat ?? {};
  const previousDetail = row?.result_json?.detail ?? {};
  const durationSeconds = positiveDurationSeconds(flat.duration_seconds);
  const flatLiveStatus = String(flat.live_status ?? "").trim().toLowerCase();
  const isUpcoming = flat.is_upcoming === true || ["is_upcoming", "upcoming"].includes(flatLiveStatus);
  const isLive = flat.is_live === true || ["is_live", "live"].includes(flatLiveStatus);
  const flatDetail = mergeDefined({}, {
    title: flat.title ?? row.title,
    url: flat.url ?? row.source_url,
    thumbnail_url: flat.thumbnail_url ?? row.thumbnail_url,
    duration_seconds: durationSeconds,
    length_text: durationSeconds == null
      ? null
      : `${Math.floor(durationSeconds / 60)}:${String(durationSeconds % 60).padStart(2, "0")}`,
    view_count_text: flat.view_count_text,
    published_text: flat.published_text,
    published_at: flat.published_at,
    published_at_status: flat.published_at_status,
    published_at_precision: flat.published_at_precision,
    published_at_source: flat.published_at_source,
    is_upcoming: isUpcoming ? true : null,
    is_live: isLive ? true : null,
    live_status: isUpcoming ? "is_upcoming" : isLive ? "is_live" : null,
    live_scheduled_at: flat.live_scheduled_at,
    source: "uploads_playlist",
  });
  const output = mergeDefined(flatDetail, previousDetail);
  const publicationSelection = selectPublicationEvidence(
    detailPublicationEvidence(previousDetail),
    detailPublicationEvidence(flatDetail),
  );
  Object.assign(output, publicationSelection.evidence);
  if (publicationSelection.selected === "current") output.published_text = previousDetail.published_text;
  else if (publicationSelection.selected === "candidate") output.published_text = flatDetail.published_text;
  const publicationConflict = publicationEvidenceConflictRecord(publicationSelection);
  if (publicationConflict) output.publication_evidence_conflict = publicationConflict;
  return output;
}

function youtubeJsFallbackReasons(detail, requiredPrecision, videoId) {
  const normalized = normalizeResolvedDetail(detail, { access: accessFromDetail(detail) });
  const reasons = youtubeJsDetailFallbackReasons({
    missingFields: missingApiFields(normalized.detail, requiredPrecision),
    accessStatus: normalized.access.access_status,
    detail: normalized.detail,
  });
  const classification = resolveYoutubeContentType({ videoId, detail });
  if (classification?.authoritative !== true) reasons.push("content_type");
  return [...new Set(reasons)];
}

function accessIsTerminalWithoutApi(access) {
  return ["members_only", "private", "unlisted", "unavailable"].includes(access?.access_status);
}

async function enqueueYoutubeApiFallback(row, missingFields) {
  await query(
    `INSERT INTO crawler.youtube_api_tasks (
       source_content_id,status,missing_fields,candidate_ids,updated_at
     ) VALUES ($1,'pending',$2::text[],ARRAY[$3]::bigint[],now())
     ON CONFLICT (source_content_id) DO UPDATE
     SET status='pending',
         candidate_ids=COALESCE((
           SELECT array_agg(DISTINCT c.candidate_id ORDER BY c.candidate_id)
           FROM crawler.content_candidates c
           WHERE c.source_content_id=EXCLUDED.source_content_id
             AND c.detail_status='api_pending'
             AND c.api_status IN ('pending','queued','running')
         ),EXCLUDED.candidate_ids),
         missing_fields=COALESCE((
           SELECT array_agg(DISTINCT field ORDER BY field)
           FROM crawler.content_candidates c
           CROSS JOIN LATERAL unnest(c.missing_fields) AS fields(field)
           WHERE c.source_content_id=EXCLUDED.source_content_id
             AND c.detail_status='api_pending'
             AND c.api_status IN ('pending','queued','running')
         ),EXCLUDED.missing_fields),
         error_message=NULL,next_retry_at=NULL,finished_at=NULL,
         created_at=now(),updated_at=now()`,
    [row.source_content_id, missingFields, row.candidate_id],
  );
}

async function cancelResolvedYoutubeApiTasks(runId) {
  const rows = await query(
    `UPDATE crawler.youtube_api_tasks t
     SET status='done',
         result_json=COALESCE(t.result_json,'{}'::jsonb)
           || jsonb_build_object(
                'api_request_skipped',true,
                'cancel_reason','scrape_repair_completed'
              ),
         error_message=NULL,next_retry_at=NULL,finished_at=now(),updated_at=now()
     WHERE t.status IN ('pending','failed')
       AND EXISTS (
         SELECT 1 FROM crawler.content_candidates current_run
         WHERE current_run.run_id=$1
           AND current_run.source_content_id=t.source_content_id
       )
       AND NOT EXISTS (
         SELECT 1 FROM crawler.content_candidates pending
         WHERE pending.source_content_id=t.source_content_id
           AND pending.detail_status='api_pending'
           AND pending.api_status IN ('pending','queued','running','failed')
       )`,
    [runId],
  );
  return rows.rowCount;
}

async function upsertContentFromCandidate(candidate, state) {
  if (!candidate.content_type || candidate.type_authoritative !== true) return null;
  const detail = state.detail ?? {};
  const access = state.access ?? accessFromDetail(detail);
  return withTransaction((client) => upsertFullVideoContent(client, {
    candidate,
    state,
    access,
    locale: language,
  }));
}

async function updateExistingContentAccessFromCandidate(candidate, state) {
  return withTransaction((client) => updateExistingFullVideoAccess(client, {
    candidate,
    state,
    access: state?.access,
  }));
}

async function persistFullVideoDisposition(row, {
  storageAction,
  classification,
  access,
  detail,
  error = null,
  terminalReason = null,
} = {}) {
  const disposition = resolveVideoDisposition({
    storageAction,
    classification,
    access,
    detail,
    error,
    terminalReason,
    observedAt: new Date().toISOString(),
  });
  let recovery = null;
  if (row.disposition === "deferred" && disposition.kind !== "deferred") {
    const {
      disposition: fromDisposition = null,
      recovery: _previousRecovery,
      ...deferredEvidence
    } = row.result_json ?? {};
    recovery = {
      from_disposition: fromDisposition,
      deferred_evidence: deferredEvidence,
      deferred_error_message: row.error_message ?? null,
      resolved_at: disposition.observed_at,
    };
  }
  await query(
    `UPDATE crawler.content_candidates
     SET disposition=$2,next_attempt_at=$3,
         result_json=result_json
           || jsonb_build_object('disposition',$4::jsonb)
           || CASE
                WHEN $5::jsonb IS NULL THEN '{}'::jsonb
                ELSE jsonb_build_object('recovery',$5::jsonb)
              END,
         updated_at=now()
     WHERE candidate_id=$1`,
    [
      row.candidate_id,
      disposition.kind,
      disposition.next_attempt_at,
      JSON.stringify(disposition),
      recovery == null ? null : JSON.stringify(recovery),
    ],
  );
  return disposition;
}

async function updateRunDetailStatus(runId) {
  const rows = await query(
    `SELECT
       count(*)::int AS total,
       count(*) FILTER (WHERE detail_status IN ('done','unavailable'))::int AS terminal,
       count(*) FILTER (WHERE api_status IN ('pending','queued','running','failed'))::int AS api_open,
       count(*) FILTER (WHERE detail_status = 'failed')::int AS failed,
       count(*) FILTER (
         WHERE detail_status IN ('done','unavailable')
           AND disposition IS NULL
       )::int AS undisposed,
       count(*) FILTER (
         WHERE detail_status IN ('done','unavailable')
           AND cardinality(missing_fields)>0
       )::int AS partial,
       count(*) FILTER (WHERE result_json->'scope'->>'status' = 'excluded')::int AS excluded,
       count(*) FILTER (WHERE result_json->'scope'->>'reason' IN ('older_than_max_age','after_chronological_age_cutoff'))::int AS age_excluded,
       count(*) FILTER (WHERE result_json->'scope'->>'reason' = 'upcoming_live')::int AS upcoming_excluded,
       count(*) FILTER (WHERE result_json->'scope'->>'reason' = 'live_in_progress')::int AS live_in_progress_excluded,
       count(*) FILTER (
         WHERE result_json#>>'{detail_request,reason_code}'='initial_publication_unresolved'
       )::int AS details_requested_due_to_unresolved_count
     FROM crawler.content_candidates
     WHERE run_id=$1`,
    [runId],
  );
  const summary = rows.rows[0] ?? {};
  const undisposed = Number(summary.undisposed ?? 0);
  const dispositionError = undisposed > 0
    ? new Error(
        `${undisposed} terminal content candidate${undisposed === 1 ? "" : "s"} has no disposition`,
      )
    : null;
  const status = Number(summary.failed) > 0 || dispositionError
    ? "failed"
    : Number(summary.api_open) > 0
      ? "api_pending"
      : Number(summary.terminal) >= Number(summary.total)
        ? "done"
        : "running";
  await query(
    `UPDATE crawler.channel_runs
     SET detail_status=$2,
         status=CASE WHEN $2='done' THEN 'waiting_agent' WHEN $2='failed' THEN 'waiting_detail' ELSE 'waiting_detail' END,
         expected_content_count=GREATEST($3::int-$4::int,0),
         result_json=result_json || jsonb_build_object(
           'excluded_count',$4::int,
           'age_excluded_count',$5::int,
           'upcoming_live_excluded_count',$6::int,
           'live_in_progress_excluded_count',$7::int,
           'undisposed_content_count',$8::int,
           'retained_content_count',GREATEST($3::int-$4::int,0)
         ) || jsonb_build_object(
           'migration_activity_metrics',
           COALESCE(result_json->'migration_activity_metrics','{}'::jsonb)
             || jsonb_build_object(
               'details_requested_due_to_unresolved_count',$10::int
             )
         ),
         error_message=CASE
           WHEN $8::int>0 THEN $9
           WHEN $2='failed' THEN error_message
           ELSE NULL
         END,
         updated_at=now()
     WHERE run_id=$1`,
    [
      runId,
      status,
      Number(summary.total ?? 0),
      Number(summary.excluded ?? 0),
      Number(summary.age_excluded ?? 0),
      Number(summary.upcoming_excluded ?? 0),
      Number(summary.live_in_progress_excluded ?? 0),
      undisposed,
      dispositionError?.message ?? null,
      Number(summary.details_requested_due_to_unresolved_count ?? 0),
    ],
  );
  const migrationActivity = await applyMigrationActivityGate(runId, status);
  if (migrationActivity.reject) {
    await refreshDispatchCandidateCounts(migrationActivity.dispatchBatchId);
    await signalReadyDiscoveryPageQualifications({
      candidateId: migrationActivity.candidateId,
    });
  }
  if (dispositionError) throw dispositionError;
  return {
    ...summary,
    status: migrationActivity.reject ? "skipped" : status,
    migration_activity_gate: migrationActivity,
  };
}

async function applyMigrationActivityGate(runId, detailStatus, options = {}) {
  return withTransaction((client) => applyMigrationActivityGateTransaction(client, {
    runId,
    detailStatus,
    ...options,
  }));
}

async function queueFinalize(channelId, runId, reason) {
  const revisionRows = await query(
    `SELECT
       c.channel_id,c.latest_run_id,c.status AS channel_status,c.agent_status,c.updated_at AS channel_updated_at,
       r.detail_status,r.expected_content_count,r.result_json->>'pipeline_cycle_id' AS pipeline_cycle_id,
       (SELECT count(*)::int FROM crawler.content_candidates cc WHERE cc.run_id=$2) AS candidate_count,
       (SELECT max(cc.updated_at) FROM crawler.content_candidates cc WHERE cc.run_id=$2) AS candidate_updated_at,
       (SELECT count(*)::int FROM crawler.contents ct WHERE ct.channel_id=$1 AND ct.run_id=$2) AS content_count,
       (SELECT max(COALESCE(ct.last_enriched_at,ct.last_seen_at))
        FROM crawler.contents ct WHERE ct.channel_id=$1 AND ct.run_id=$2) AS content_updated_at,
       (SELECT ap.updated_at FROM crawler.agent_profiles ap
        WHERE ap.channel_id=$1 AND ap.agent_mode='basic' AND ap.status='success' LIMIT 1) AS agent_updated_at
     FROM crawler.channels c
     LEFT JOIN crawler.channel_runs r ON r.run_id=$2
     WHERE c.channel_id=$1
     LIMIT 1`,
    [channelId, runId],
  );
  const sourceRevision = finalizeDispatchRevision(
    revisionRows.rows[0] ?? { channel_id: channelId, run_id: runId },
  );
  await queues[queuesByRole.finalize].add(
    "finalize-channel",
    {
      channel_id: channelId,
      run_id: runId,
      reason,
      source_revision: sourceRevision,
      pipeline_cycle_id: revisionRows.rows[0]?.pipeline_cycle_id ?? null,
    },
    { jobId: safeJobId("finalize", runId || channelId, sourceRevision) },
  );
}

async function refreshDispatchCandidateCounts(dispatchBatchId) {
  if (!dispatchBatchId) return;
  await query(
    `UPDATE crawler.query_dispatch_batches batch
     SET discovered_candidate_count=stats.total,
         accepted_channel_count=stats.accepted,
         rejected_channel_count=stats.rejected,
         updated_at=now()
     FROM (
       SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status='accepted')::int AS accepted,
              count(*) FILTER (WHERE status='rejected')::int AS rejected
       FROM crawler.channel_candidates
       WHERE dispatch_batch_id=$1
     ) stats
     WHERE batch.dispatch_batch_id=$1`,
    [dispatchBatchId],
  );
}

export async function signalReadyDiscoveryPageQualifications({ candidateId = null, pageId = null } = {}) {
  try {
    const pageIds = await publishReadyDiscoveryPages({
      query,
      queue: queues[queuesByRole.discoverPage],
      candidateId,
      pageId,
    });
    if (pageIds.length > 0) {
      console.log(JSON.stringify({
        event: "discovery_page_qualification_ready",
        candidate_id: candidateId,
        page_ids: pageIds,
      }));
    }
    return pageIds;
  } catch (error) {
    console.error(JSON.stringify({
      event: "discovery_page_wakeup_failed",
      candidate_id: candidateId,
      page_id: pageId,
      error: error?.message || String(error),
    }));
    return [];
  }
}

export async function processChannelCrawlV2(job, { resumeMode = "initial" } = {}) {
  const pipelineStartedAt = Date.now();
  const phaseTimingsMs = {};
  const channelId = text(job.data?.channel_id);
  if (!channelId) throw new Error("channel_id is required");
  const candidateId = integer(job.data?.candidate_id);
  const candidateAttemptFence = candidateId == null
    ? null
    : activeChannelCandidateAttemptFence(job);
  const dispatchBatchId = text(job.data?.dispatch_batch_id) ?? text(job.data?.pipeline_cycle_id);
  const settings = await getCrawlSettingsV2();
  const repairRunMetadata = fullRepairRunMetadata(job.data);
  const channelContentLimit = repairRunMetadata.publication_repair?.content_limit
    ?? settings.channelContentLimit;
  const contentMaxAgeDays = repairRunMetadata.publication_repair?.content_max_age_days
    ?? settings.contentMaxAgeDays;
  let runId = text(job.data?.run_id);
  const businessRunKey = text(job.data?.business_run_key);
  if (!runId || !businessRunKey) {
    throw new Error("channel crawl requires a prepared persistent Business Run");
  }
  const publicationGapIntent = publicationGapRepairJobIntent(job.data);
  const aboutOnlyPublicationGapRepair = isAboutOnlyPublicationGapRepair(job.data);
  const inheritedPublicationGapDomains = publicationGapIntent?.domains ?? [];
  const terminateReservedBinding = async (reason) => withTransaction((client) => (
    terminateBusinessRunBinding(client, {
      businessRunKey,
      businessRunId: runId,
      reason,
    })
  ));
  const snapshotCandidateRows = candidateId == null
    ? { rows: [] }
    : await query(
      "SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 AND channel_id=$2 LIMIT 1",
      [candidateId, channelId],
    );
  const candidate = snapshotCandidateRows.rows[0] ?? null;
  if (candidateId != null && !candidate) throw new Error(`channel candidate not found: ${candidateId}`);
  const existingRows = await query("SELECT * FROM crawler.channels WHERE channel_id=$1 LIMIT 1", [channelId]);
  const existingChannel = existingRows.rows[0] ?? null;
  const registryRunId = resolveChannelRegistryRunId({
    requestedRunId: runId,
    candidate,
    channel: existingChannel,
  });
  if (registryRunId && registryRunId !== runId) {
    throw new Error(`prepared Business Run conflicts with Channel Registry Run: ${runId}`);
  }
  if (existingChannel?.status === "removed") {
    if (candidateId != null) {
      await withTransaction((client) => markChannelRemoved(client, {
        channelId,
        candidateId,
        candidateAttemptFence,
        terminal: {
          failure_kind: "channel_removed",
          removed_reason: existingChannel.removed_reason || "channel_not_found",
          removed_source: existingChannel.removed_source || "crawler_state",
          evidence: existingChannel.removed_evidence || "Channel is already marked removed",
        },
        observedAt: existingChannel.removed_at || new Date(),
      }));
      await refreshDispatchCandidateCounts(dispatchBatchId);
      await signalReadyDiscoveryPageQualifications({ candidateId });
    }
    await terminateReservedBinding("channel_removed");
    return {
      ok: true,
      skipped: true,
      skip_reason: "channel_removed",
      channel_id: channelId,
      candidate_id: candidateId,
      run_id: null,
      candidate_count: 0,
      phase_timings_ms: { total: Date.now() - pipelineStartedAt },
    };
  }
  if (candidate && ["rejected", "existing"].includes(candidate.status)) {
    await terminateReservedBinding(candidate.reject_reason || `candidate_${candidate.status}`);
    await signalReadyDiscoveryPageQualifications({ candidateId });
    return {
      ok: true,
      skipped: true,
      skip_reason: candidate.reject_reason || `candidate_${candidate.status}`,
      channel_id: channelId,
      candidate_id: candidateId,
      run_id: null,
      candidate_count: 0,
      phase_timings_ms: { total: Date.now() - pipelineStartedAt },
    };
  }
  if (candidate && existingChannel && candidate.status !== "accepted") {
    await markChannelCandidateAlreadyPromoted(query, candidateAttemptFence);
    await refreshDispatchCandidateCounts(dispatchBatchId);
    await signalReadyDiscoveryPageQualifications({ candidateId });
    await terminateReservedBinding("channel_already_promoted");
    return {
      ok: true,
      skipped: true,
      skip_reason: "channel_already_promoted",
      channel_id: channelId,
      candidate_id: candidateId,
      run_id: null,
      candidate_count: 0,
      phase_timings_ms: { total: Date.now() - pipelineStartedAt },
    };
  }
  if (candidate && candidate.status !== "accepted") {
    await beginChannelCandidateValidation(query, candidateAttemptFence);
  }
  if (candidate?.status === "accepted") {
    await signalReadyDiscoveryPageQualifications({ candidateId });
  }
  const existing = existingChannel ?? {
    channel_id: channelId,
    channel_url: candidate?.channel_url ?? job.data?.channel_url,
    handle: candidate?.handle ?? null,
    title: candidate?.title ?? null,
    subscriber_count: candidate?.search_subscriber_count ?? null,
    subscriber_count_text: candidate?.search_subscriber_count_text ?? null,
  };
  const crawlUrl = text(job.data?.channel_url) ?? normalChannelUrl(existing);
  const runResultJson = {
    job_id: job.id,
    pipeline_cycle_id: text(job.data?.pipeline_cycle_id),
    dispatch_batch_id: dispatchBatchId,
    candidate_id: candidateId,
    query_id: job.data?.query_id ?? null,
    query_text: text(job.data?.query_text),
    ...repairRunMetadata,
    ...(inheritedPublicationGapDomains.length > 0
      ? {
          publication_gap_repair: {
            status: "required",
            reason: "inherited_publication_gap_repair",
            domains: inheritedPublicationGapDomains,
            root_run_id: publicationGapIntent.rootRunId,
          },
        }
      : {}),
    ...(candidate && job.data?.reject_if_no_recent_content === true
      ? {
          migration_activity_gate: {
            required: true,
            decision: "pending",
            max_age_days: contentMaxAgeDays,
          },
        }
      : {}),
    ...(Number(job.data?.repair_round) > 0
      ? {
          final_repair: {
            rounds: Number(job.data.repair_round),
            parent_run_id: text(job.data?.repair_parent_run_id),
            mode: "channel",
          },
        }
      : {}),
  };
  let runPrepared = !candidate || candidate.status === "accepted";
  if (runPrepared) {
    await prepareChannelRunAndBindJob({
      job,
      runId,
      prepare: () => withTransaction(async (client) => {
        await prepareChannelRun(client, {
          runId,
          channelId,
          candidateId,
          crawlMode: job.data?.crawl_mode || "full",
          contentLimit: channelContentLimit,
          resultJson: runResultJson,
        });
        await materializeBusinessRunBinding(client, { businessRunKey, businessRunId: runId });
      }),
    });
  }
  phaseTimingsMs.prepare_run = Date.now() - pipelineStartedAt;

  if (runPrepared && channelInlineDetails && !aboutOnlyPublicationGapRepair
      && (Number(job.attemptsMade ?? 0) > 0 || resumeMode !== "initial")) {
    const existingCandidates = await query(
      `SELECT count(*)::int AS count
       FROM crawler.content_candidates
       WHERE run_id=$1`,
      [runId],
    );
    const candidateCount = Number(existingCandidates.rows[0]?.count ?? 0);
    if (candidateCount > 0) {
      let phaseStartedAt = Date.now();
      const detailResult = await processContentDetailRun({
        runId,
        channelId,
        signal: currentChannelExecutionAbortSignal(),
        executionMode: "channel_inline_resume",
        finalize: false,
        contentMaxAgeDays,
      });
      phaseTimingsMs.content_detail_resume = Date.now() - phaseStartedAt;
      phaseStartedAt = Date.now();
      if (migrationActivityCanFinalize(detailResult.migration_activity_gate)) {
        await queueFinalize(channelId, runId, "channel-full-resumed");
      }
      phaseTimingsMs.queue_finalize = Date.now() - phaseStartedAt;
      await query(
        `UPDATE crawler.channel_runs
         SET result_json=result_json || jsonb_build_object(
               'resumed_job_attempt', $2::int,
               'resumed_candidate_count', $3::int,
               'resumed_at', now()
             ),
             updated_at=now()
         WHERE run_id=$1`,
        [runId, Number(job.attemptsMade), candidateCount],
      );
      if (Number(detailResult?.failed ?? 0) > 0) {
        throw contentDetailFailureError(
          `${detailResult.failed} content candidates failed during resumed inline channel crawl`,
          [detailResult.retryable_failure_error],
        );
      }
      return {
        ok: true,
        resumed: true,
        channel_id: channelId,
        run_id: runId,
        content_limit: channelContentLimit,
        content_max_age_days: contentMaxAgeDays,
        candidate_count: candidateCount,
        detail_execution: "channel_inline_resume",
        detail_processed: Number(detailResult?.processed ?? 0),
        detail_status: detailResult?.status ?? "done",
        phase_timings_ms: {
          ...phaseTimingsMs,
          total: Date.now() - pipelineStartedAt,
        },
      };
    }
  }

  let fetched = null;
  let header = {};
  let primaryError = null;
  let youtubeJsChannel = null;
  let youtubeJsChannelError = null;
  let legacyHeaderError = null;
  let channelExtractor = "legacy_html";
  let phaseStartedAt = Date.now();
  const fetchLegacyHeader = async () => {
    const legacyFetched = await fetchChannelInitial(crawlUrl, { language, country });
    await saveFetchedRaw({
      fetched: legacyFetched,
      objectType: "youtube_channel_header_html",
      entityType: "channel",
      entityId: channelId,
      source: "channel_crawl_v2",
      metadata: { channel_id: channelId, run_id: runId },
    });
    const terminal = terminalChannelEvidenceFromInitialData(legacyFetched.initialData);
    if (terminal) {
      const error = new Error(terminal.evidence);
      error.name = "TerminalChannelError";
      throw error;
    }
    const legacyHeader = parseChannelHeader(legacyFetched.initialData, language);
    return { fetched: legacyFetched, header: legacyHeader };
  };
  if (youtubeJsChannelEnabled()) {
    try {
      youtubeJsChannel = await openYoutubeJsChannel(channelId);
      header = youtubeJsChannel.metadata;
      channelExtractor = "youtubejs";
      await saveJsonRaw({
        objectType: "youtube_channel_youtubejs_json",
        entityType: "channel",
        entityId: channelId,
        source: "youtubejs_channel_v2",
        payload: youtubeJsChannel.raw,
        metadata: { channel_id: channelId, run_id: runId },
      });
    } catch (error) {
      throwIfChannelExecutionAborted();
      youtubeJsChannelError = error;
    }
  }
  const youtubeJsMetadataIncomplete = youtubeJsChannel && (
    !header.title
    || !header.handle
    || header.subscriber_count == null
    || !header.description
    || /(?:\.{3}|\u2026)$/.test(String(header.description).trim())
  );
  if (!youtubeJsChannel || youtubeJsMetadataIncomplete) {
    try {
      const legacy = await fetchLegacyHeader();
      fetched = legacy.fetched;
      header = youtubeJsChannel ? mergeChannelMetadata(legacy.header, header) : legacy.header;
      channelExtractor = youtubeJsChannel ? "youtubejs+legacy_html" : "legacy_html";
    } catch (error) {
      throwIfChannelExecutionAborted();
      legacyHeaderError = error;
      if (!youtubeJsChannel) primaryError = error;
    }
  }
  if (!youtubeJsChannelEnabled() && !fetched && !primaryError) {
    try {
      const legacy = await fetchLegacyHeader();
      fetched = legacy.fetched;
      header = legacy.header;
    } catch (error) {
      primaryError = error;
    }
  }
  phaseTimingsMs.channel_initial = Date.now() - phaseStartedAt;

  const terminalChannel = [youtubeJsChannelError, primaryError, legacyHeaderError]
    .map((error) => classifyTerminalChannelError(error))
    .find(Boolean) ?? null;
  if (terminalChannel) {
    await withTransaction((client) => markChannelRemoved(client, {
      channelId,
      candidateId: candidateId ?? null,
      candidateAttemptFence,
      runId: runPrepared ? runId : null,
      terminal: terminalChannel,
    }));
    if (candidateId != null) {
      await refreshDispatchCandidateCounts(dispatchBatchId);
      await signalReadyDiscoveryPageQualifications({ candidateId });
    }
    return {
      ok: true,
      skipped: true,
      skip_reason: terminalChannel.removed_reason,
      channel_id: channelId,
      candidate_id: candidateId ?? null,
      run_id: runPrepared ? runId : null,
      candidate_count: 0,
      phase_timings_ms: { ...phaseTimingsMs, total: Date.now() - pipelineStartedAt },
    };
  }
  if (primaryError && isYoutubeIpBlockedError(primaryError)) throw primaryError;
  if (primaryError && isYoutubeNetworkRetryableError(primaryError)) {
    await new Promise((resolve) => setTimeout(resolve, Math.round(500 + (Math.random() * 1000))));
  }

  const needsMetadataFallback = primaryError
    || !header.title
    || !header.handle
    || header.subscriber_count == null
    || !header.description;
  let fallback = {};
  if (needsMetadataFallback) {
    phaseStartedAt = Date.now();
    try {
      fallback = await fetchChannelYtDlpMetadata(crawlUrl, { language, country });
      await saveJsonRaw({
        objectType: "youtube_channel_ytdlp_metadata_json",
        entityType: "channel",
        entityId: channelId,
        source: "channel_metadata_fallback_v2",
        payload: fallback.raw ?? fallback,
        metadata: { channel_id: channelId, run_id: runId },
      });
    } catch (error) {
      throwIfChannelExecutionAborted();
      if (primaryError) throw primaryError;
      fallback = { error: String(error?.message ?? error) };
    }
    phaseTimingsMs.channel_metadata_fallback = Date.now() - phaseStartedAt;
  } else {
    phaseTimingsMs.channel_metadata_fallback = 0;
  }
  let merged = mergeChannelMetadata(mergeChannelMetadata(existing, fallback), header);
  const enforceMinSubscribers = job.data?.enforce_min_subscribers === true;
  let channelApiFallback = {
    attempted: false,
    reason: "not_needed",
    missingFields: channelApiFallbackMissingFields(merged, { enforceMinSubscribers }),
    detail: null,
  };
  phaseStartedAt = Date.now();
  if (channelApiFallback.missingFields.length > 0) {
    const apiSettings = await getYoutubeApiSettingsV2();
    channelApiFallback = await resolveChannelApiFallback({
      channelId,
      metadata: merged,
      enforceMinSubscribers,
      fallbackMode: apiSettings.fallbackMode,
      apiKeys: apiSettings.apiKeys,
      timeoutMs: apiSettings.timeoutMs,
      dailyRequestLimit: apiSettings.dailyRequestLimit,
      reserveRequest: reserveYoutubeApiRequest,
      fetchDetails: fetchChannelDataApiDetails,
    });
    if (channelApiFallback.attempted) {
      await saveJsonRaw({
        objectType: "youtube_channel_data_api_json",
        entityType: "channel",
        entityId: channelId,
        source: "youtube_data_api_channels_list",
        payload: channelApiFallback.raw,
        metadata: {
          channel_id: channelId,
          run_id: runId,
          missing_fields: channelApiFallback.missingFields,
          returned_count: channelApiFallback.returnedCount,
          key_index: channelApiFallback.keyIndex,
        },
      });
    }
    if (channelApiFallback.detail) {
      merged = mergeChannelMetadata(merged, channelApiFallback.detail);
    }
  }
  phaseTimingsMs.channel_api_fallback = Date.now() - phaseStartedAt;
  const countryObservation = parseYoutubeAboutCountry(merged.country, { channel_id: channelId });
  merged.country = countryObservation.raw;
  merged.country_code = countryObservation.code;
  merged.country_canonical_name = countryObservation.name;
  const snapshotSubscriberCount = merged.subscriber_count_source
    ? merged.subscriber_count
    : null;
  let qualification = evaluateChannelQualification({
    subscriberCount: enforceMinSubscribers ? snapshotSubscriberCount : merged.subscriber_count,
    minSubscriberCount: job.data?.min_subscriber_count ?? settings.minSubscriberCount,
    required: enforceMinSubscribers,
  });
  qualification = resolveChannelQualificationAfterApi(qualification, channelApiFallback);
  if (
    !qualification.qualified
    && qualification.reason === "subscriber_count_unknown"
    && (
      (
        legacyHeaderError
        && (isYoutubeIpBlockedError(legacyHeaderError) || isYoutubeNetworkRetryableError(legacyHeaderError))
      )
      || (
        youtubeJsChannel?.about_error
        && (
          isYoutubeIpBlockedError(youtubeJsChannel.about_error)
          || isYoutubeNetworkRetryableError(youtubeJsChannel.about_error)
        )
      )
    )
  ) {
    throw legacyHeaderError || youtubeJsChannel.about_error;
  }
  if (!qualification.qualified && qualification.reason === "subscriber_count_unknown") {
    const parserError = [primaryError, youtubeJsChannelError, legacyHeaderError]
      .find((error) => isParserContractError(error));
    if (parserError) throw parserError;
    throw new ParserContractError({
      field: "subscriber_count",
      value: merged.subscriber_count_text,
      locale: language,
      source: "channel_snapshot",
      reason: "required_subscriber_count_not_observed",
      context: {
        channel_id: channelId,
        youtubejs_about_error: youtubeJsChannel?.about_error
          ? String(youtubeJsChannel.about_error?.message ?? youtubeJsChannel.about_error)
          : null,
        legacy_header_error: legacyHeaderError
          ? String(legacyHeaderError?.message ?? legacyHeaderError)
          : null,
        yt_dlp_source: fallback?.source ?? null,
      },
    });
  }
  phaseStartedAt = Date.now();
  const checkedAt = new Date().toISOString();
  const aboutMetrics = youtubeJsChannel
    ? combinedAboutObservationMetrics(normalizeAboutMetrics({
        metadata: youtubeJsChannel.metadata,
        aboutObserved: youtubeJsChannel.about_observed === true,
        locale: language,
      }))
    : null;
  const executionAttemptId = currentChannelExecution()?.attempt_id
    ?? `job-attempt:${Number(job.attemptsMade ?? 0)}`;
  const aboutObservationCommand = aboutMetrics
    ? {
        idempotencyKey: aboutObservationIdempotencyKey({ runId, executionAttemptId }),
        channelId,
        runId,
        observedAt: checkedAt,
        planId: job.data?.plan_id ?? null,
        planDay: job.data?.plan_day ?? null,
        triggerReason: text(job.data?.trigger_reason)
          ?? (Number(job.data?.repair_round) > 0
            ? "repair"
            : (job.data?.crawl_mode || "full") === "incremental" ? "clock_due" : "initial_full"),
        scheduledAt: job.data?.scheduled_at ?? null,
        startedAt: new Date(pipelineStartedAt).toISOString(),
        finishedAt: checkedAt,
        crawlerVersion: text(process.env.CRAWLER_VERSION) ?? "qy-v16",
        extractorVersions: {
          youtubejs: youtubeJsChannel.raw?.engine ?? null,
        },
        errorClass: youtubeJsChannel.about_error?.name ?? null,
        errorMessage: youtubeJsChannel.about_error
          ? String(youtubeJsChannel.about_error?.message ?? youtubeJsChannel.about_error)
          : null,
        about: aboutMetrics,
        current: normalizeAboutObservationCurrent(youtubeJsChannel.metadata, {
          aboutObserved: youtubeJsChannel.about_observed === true,
          locale: language,
        }),
      }
    : null;
  const channelSource = {
    channel_header: merged,
    country_observation: countryObservation,
    fetched_url: fetched?.url ?? null,
    channel_extractor: channelExtractor,
    youtubejs_error: youtubeJsChannelError ? String(youtubeJsChannelError?.message ?? youtubeJsChannelError) : null,
    youtubejs_about_error: youtubeJsChannel?.about_error
      ? String(youtubeJsChannel.about_error?.message ?? youtubeJsChannel.about_error)
      : null,
    legacy_header_error: legacyHeaderError ? String(legacyHeaderError?.message ?? legacyHeaderError) : null,
    channel_metadata_fallback: fallback,
    channel_api_fallback: {
      attempted: channelApiFallback.attempted,
      reason: channelApiFallback.reason,
      missing_fields: channelApiFallback.missingFields,
      returned_count: channelApiFallback.returnedCount ?? 0,
      hidden_subscriber_count: channelApiFallback.detail?.hidden_subscriber_count === true,
    },
    qualification: { ...qualification, checked_at: checkedAt },
    dispatch_batch_id: dispatchBatchId,
    candidate_id: candidateId,
  };
  const rejectIfNoRecentContent = Boolean(
    candidate && job.data?.reject_if_no_recent_content === true,
  );
  const deferAboutUntilRepairFinalize = deferAboutObservationUntilRepairFinalize(
    job.data,
    runResultJson,
  );
  const candidateRunResultJson = rejectIfNoRecentContent || deferAboutUntilRepairFinalize
    ? {
        ...runResultJson,
        pending_initial_about_observation: aboutObservationCommand,
      }
    : runResultJson;

  if (channelCandidateCanFailAdmission(candidate) && !qualification.qualified) {
    await rejectChannelCandidateAdmission(query, candidateAttemptFence, {
      reason: qualification.reason,
      sourceJson: channelSource,
    });
    await refreshDispatchCandidateCounts(dispatchBatchId);
    await signalReadyDiscoveryPageQualifications({ candidateId });
    await terminateReservedBinding(qualification.reason);
    phaseTimingsMs.channel_metadata_persist = Date.now() - phaseStartedAt;
    return {
      ok: true,
      skipped: true,
      skip_reason: qualification.reason,
      channel_id: channelId,
      candidate_id: candidateId,
      run_id: null,
      candidate_count: 0,
      phase_timings_ms: { ...phaseTimingsMs, total: Date.now() - pipelineStartedAt },
    };
  }

  if (candidate) {
    let registryPromotion = null;
    const claimCandidateAndPrepareRun = async (client) => {
      if (candidate.status === "accepted") {
        await client.query(
          `UPDATE crawler.channels
           SET channel_url=COALESCE($2,channel_url),handle=COALESCE($3,handle),title=COALESCE($4,title),
               country=COALESCE(NULLIF(btrim($5::text),''),country),
               country_source=CASE WHEN NULLIF(btrim($5::text),'') IS NOT NULL THEN 'youtube_about' ELSE country_source END,
               country_code=COALESCE($9,country_code),
               country_canonical_name=COALESCE($10,country_canonical_name),
               subscriber_count=COALESCE($6::bigint,subscriber_count),
               subscriber_count_text=COALESCE($7,subscriber_count_text),
               status='active',reject_reason=NULL,
               dormant_reason=NULL,dormant_since=NULL,dormant_recheck_day=NULL,
               dormant_last_probe_at=NULL,dormant_cycle=0,
               source_json=source_json || $8::jsonb,
               updated_at=now()
           WHERE channel_id=$1`,
          [
            channelId,
            merged.channel_url ?? crawlUrl,
            merged.handle,
            merged.title,
            merged.country,
            merged.subscriber_count,
            merged.subscriber_count_text,
            JSON.stringify(channelSource),
            merged.country_code,
            merged.country_canonical_name,
          ],
        );
        await recordAcceptedChannelCandidateSnapshot(
          client.query.bind(client),
          candidateAttemptFence,
          { sourceJson: channelSource },
        );
      } else {
        registryPromotion = await claimChannelRegistryPromotion(client, {
          candidateId,
          runId,
          channelId,
          channelUrl: merged.channel_url ?? crawlUrl,
          handle: merged.handle,
          title: merged.title,
          country: merged.country,
          countryCode: merged.country_code,
          countryCanonicalName: merged.country_canonical_name,
          subscriberCount: merged.subscriber_count,
          subscriberCountText: merged.subscriber_count_text,
          readyForAgent: !rejectIfNoRecentContent,
          sourceJson: channelSource,
          candidateAttemptFence,
        });
        if (!registryPromotion.promoted) return;
      }
      await reconcileFullCrawlAgentState(client.query.bind(client), {
        channelId,
        eligible: !rejectIfNoRecentContent,
      });
      await prepareChannelRun(client, {
        runId,
        channelId,
        candidateId,
        crawlMode: job.data?.crawl_mode || "full",
        contentLimit: channelContentLimit,
        resultJson: candidateRunResultJson,
      });
      await materializeBusinessRunBinding(client, { businessRunKey, businessRunId: runId });
      if (aboutObservationCommand && !rejectIfNoRecentContent && !deferAboutUntilRepairFinalize) {
        await recordAboutObservation(client, aboutObservationCommand);
      }
    };
    if (candidate.status === "accepted") {
      await prepareChannelRunAndBindJob({
        job,
        runId,
        prepare: () => withTransaction(claimCandidateAndPrepareRun),
      });
    } else {
      await withTransaction(claimCandidateAndPrepareRun);
      if (!registryPromotion?.promoted) {
        await refreshDispatchCandidateCounts(dispatchBatchId);
        await signalReadyDiscoveryPageQualifications({ candidateId });
        await terminateReservedBinding("channel_already_promoted");
        phaseTimingsMs.channel_metadata_persist = Date.now() - phaseStartedAt;
        return {
          ok: true,
          skipped: true,
          skip_reason: "channel_already_promoted",
          channel_id: channelId,
          candidate_id: candidateId,
          run_id: null,
          candidate_count: 0,
          phase_timings_ms: { ...phaseTimingsMs, total: Date.now() - pipelineStartedAt },
        };
      }
      await bindPreparedChannelRun({ job, runId });
    }
    runPrepared = true;
    await refreshDispatchCandidateCounts(dispatchBatchId);
    await signalReadyDiscoveryPageQualifications({ candidateId });
  } else {
    await withTransaction(async (client) => {
      await client.query(
        `UPDATE crawler.channels
         SET channel_url=COALESCE($2,channel_url),handle=COALESCE($3,handle),title=COALESCE($4,title),
             country=COALESCE(NULLIF(btrim($5::text),''),country),
             country_source=CASE WHEN NULLIF(btrim($5::text),'') IS NOT NULL THEN 'youtube_about' ELSE country_source END,
             country_code=COALESCE($9,country_code),
             country_canonical_name=COALESCE($10,country_canonical_name),
             subscriber_count=COALESCE($6::bigint,subscriber_count),
             subscriber_count_text=COALESCE($7,subscriber_count_text),
             source_json=source_json || $8::jsonb,
             updated_at=now()
         WHERE channel_id=$1`,
        [
          channelId,
          merged.channel_url ?? crawlUrl,
          merged.handle,
          merged.title,
          merged.country,
          merged.subscriber_count,
          merged.subscriber_count_text,
          JSON.stringify(channelSource),
          merged.country_code,
          merged.country_canonical_name,
        ],
      );
      await reconcileFullCrawlAgentState(client.query.bind(client), {
        channelId,
        eligible: qualification.qualified,
      });
      if (aboutObservationCommand) {
        await recordAboutObservation(client, aboutObservationCommand);
      }
      if (!qualification.qualified) {
        await client.query(
          `UPDATE crawler.channels
           SET status='rejected',reject_reason=$2,ready_for_agent=false,
               dormant_reason=NULL,dormant_since=NULL,dormant_recheck_day=NULL,
               dormant_last_probe_at=NULL,dormant_cycle=0,
               agent_status=CASE WHEN agent_status='done' THEN 'done' ELSE 'skipped' END,
               updated_at=now()
           WHERE channel_id=$1`,
          [channelId, qualification.reason],
        );
        await client.query(
          `UPDATE crawler.channel_runs
           SET status='skipped',detail_status='done',expected_content_count=0,
               result_json=result_json || $2::jsonb,finished_at=now(),updated_at=now()
           WHERE run_id=$1`,
          [runId, JSON.stringify({ qualification, skipped_before_uploads: true })],
        );
      }
    });
    if (!qualification.qualified) {
      phaseTimingsMs.channel_metadata_persist = Date.now() - phaseStartedAt;
      return {
        ok: true,
        skipped: true,
        skip_reason: qualification.reason,
        channel_id: channelId,
        run_id: runId,
        candidate_count: 0,
        phase_timings_ms: { ...phaseTimingsMs, total: Date.now() - pipelineStartedAt },
      };
    }
  }
  phaseTimingsMs.channel_metadata_persist = Date.now() - phaseStartedAt;

  if (aboutOnlyPublicationGapRepair) {
    phaseStartedAt = Date.now();
    const repair = await completeAboutOnlyPublicationGapRepair(
      (sql, params) => withTransaction((client) => client.query(sql, params)),
      {
        jobData: job.data,
        runId,
        channelId,
        aboutOutcome: aboutMetrics?.outcome ?? null,
        aboutObservationCommand,
        enqueueFinalize: ({ channelId: targetChannelId, runId: targetRunId, reason }) => (
          queueFinalize(targetChannelId, targetRunId, reason)
        ),
      },
    );
    phaseTimingsMs.publication_gap_about_repair = Date.now() - phaseStartedAt;
    return {
      ok: true,
      repaired: true,
      channel_id: channelId,
      run_id: runId,
      candidate_id: candidateId,
      ...repair,
      phase_timings_ms: {
        ...phaseTimingsMs,
        total: Date.now() - pipelineStartedAt,
      },
    };
  }

  phaseStartedAt = Date.now();
  let uploads;
  let uploadsExtractor = "yt_dlp";
  let youtubeJsUploadsError = null;
  if (youtubeJsChannel) {
    try {
      uploads = await youtubeJsChannel.fetchContents(channelContentLimit, {
        locale: language,
        now: new Date(checkedAt).getTime(),
      });
      uploadsExtractor = "youtubejs";
    } catch (error) {
      throwIfChannelExecutionAborted();
      youtubeJsUploadsError = error;
    }
  }
  if (!uploads) {
    uploads = await fetchChannelUploads(channelId, channelContentLimit, { language });
  }
  phaseTimingsMs.channel_uploads_fetch = Date.now() - phaseStartedAt;
  phaseStartedAt = Date.now();
  await saveJsonRaw({
    objectType: "youtube_channel_uploads_flat_json",
    entityType: "channel_run",
    entityId: runId,
    source: uploadsExtractor === "youtubejs" ? "youtubejs_uploads_playlist_v2" : "uploads_playlist_v2",
    payload: uploads.raw,
    metadata: {
      channel_id: channelId,
      run_id: runId,
      content_limit: channelContentLimit,
      extractor: uploadsExtractor,
      youtubejs_error: youtubeJsUploadsError ? String(youtubeJsUploadsError?.message ?? youtubeJsUploadsError) : null,
    },
  });
  const uploadsActivity = evaluateMigrationUploadsActivity({
    required: rejectIfNoRecentContent,
    entries: uploads.entries,
    evidenceComplete: uploads.activity_evidence_complete === true,
    maxAgeDays: contentMaxAgeDays,
    observedAt: checkedAt,
    locale: language,
  });
  const migrationActivityInitialEvidence = rejectIfNoRecentContent
    ? {
        evidence_complete: uploadsActivity.evidenceComplete,
        decision: uploadsActivity.decision,
        max_age_days: uploadsActivity.maxAgeDays,
        reference_day: uploadsActivity.referenceDay,
        reference_at: uploadsActivity.referenceAt,
        recent_published_content_count: uploadsActivity.recentPublishedContentCount,
        uncertain_content_count: uploadsActivity.uncertainContentCount,
        inspected_content_count: uploadsActivity.inspectedContentCount,
        excluded_upcoming_count: uploadsActivity.excludedUpcomingCount,
        newest_published_day: uploadsActivity.newestPublishedDay,
        classifier_version: uploadsActivity.classifierVersion,
        policy_version: uploadsActivity.policyVersion,
        relation_counts: uploadsActivity.relationCounts,
        unresolved_by_status_counts: uploadsActivity.unresolvedByStatusCounts,
      }
    : null;
  if (uploadsActivity.dormant) {
    const migrationActivity = await applyMigrationActivityGate(runId, "done", {
      evaluatedAt: checkedAt,
      activityEvidence: {
        complete: true,
        source: "uploads_publication_dates",
        recentPublishedContentCount: uploadsActivity.recentPublishedContentCount,
        uncertainContentCount: uploadsActivity.uncertainContentCount,
        inspectedContentCount: uploadsActivity.inspectedContentCount,
        excludedUpcomingCount: uploadsActivity.excludedUpcomingCount,
        newestPublishedDay: uploadsActivity.newestPublishedDay,
        referenceDay: uploadsActivity.referenceDay,
        referenceAt: uploadsActivity.referenceAt,
        classifierVersion: uploadsActivity.classifierVersion,
        policyVersion: uploadsActivity.policyVersion,
        relationCounts: uploadsActivity.relationCounts,
        unresolvedByStatusCounts: uploadsActivity.unresolvedByStatusCounts,
      },
    });
    phaseTimingsMs.channel_candidates_persist = 0;
    phaseTimingsMs.content_detail = 0;
    const finalizeStartedAt = Date.now();
    await queueFinalize(channelId, runId, "migration-activity-dormant");
    phaseTimingsMs.queue_finalize = Date.now() - finalizeStartedAt;
    return {
      ok: true,
      skipped: true,
      skip_reason: migrationActivity.reason,
      channel_id: channelId,
      run_id: runId,
      content_limit: channelContentLimit,
      content_max_age_days: contentMaxAgeDays,
      candidate_count: 0,
      uploads_scanned_count: uploads.entries.length,
      detail_execution: "skipped_dormant",
      detail_processed: 0,
      detail_status: "done",
      migration_activity_gate: migrationActivity,
      phase_timings_ms: {
        ...phaseTimingsMs,
        total: Date.now() - pipelineStartedAt,
      },
      channel_extractor: channelExtractor,
      uploads_extractor: uploadsExtractor,
    };
  }
  const candidateRows = uploads.entries.map((entry) => {
    const discoveryClassification = resolveYoutubeContentType({
      videoId: entry.video_id,
      upload: entry,
    });
    return {
      source_content_id: entry.video_id,
      position: entry.position,
      title: entry.title ?? null,
      source_url: entry.url,
      thumbnail_url: entry.thumbnail_url ?? null,
      content_type: null,
      type_status: "unresolved",
      type_source: null,
      result_json: { flat: entry, discovery_classification: discoveryClassification },
    };
  });
  await withTransaction(async (client) => {
    await client.query(
    `WITH stale_contents AS (
       UPDATE crawler.contents SET is_recent=false WHERE channel_id=$2 RETURNING content_key
     ), input AS (
       SELECT * FROM jsonb_to_recordset($5::jsonb) AS item(
         source_content_id text,position integer,title text,source_url text,thumbnail_url text,
         content_type text,type_status text,type_source text,result_json jsonb
       )
     ), upserted AS (
       INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,title,source_url,thumbnail_url,
         content_type,type_status,type_source,detail_status,api_status,result_json,updated_at
       )
       SELECT $1,$2,source_content_id,position,title,source_url,thumbnail_url,
              content_type,type_status,type_source,'queued','not_needed',result_json,now()
       FROM input
       ON CONFLICT (run_id,source_content_id) DO UPDATE
       SET position=EXCLUDED.position,title=COALESCE(EXCLUDED.title,crawler.content_candidates.title),
           source_url=EXCLUDED.source_url,thumbnail_url=COALESCE(EXCLUDED.thumbnail_url,crawler.content_candidates.thumbnail_url),
           content_type=EXCLUDED.content_type,
           type_status=EXCLUDED.type_status,
           type_source=EXCLUDED.type_source,
           updated_at=now()
       RETURNING candidate_id
     )
     UPDATE crawler.channel_runs
     SET status='waiting_detail',detail_status=$3,expected_content_count=$4,
         result_json=result_json || $6::jsonb,updated_at=now()
     WHERE run_id=$1`,
    [
      runId,
      channelId,
      candidateRows.length > 0 ? "queued" : "done",
      candidateRows.length,
      JSON.stringify(candidateRows),
      JSON.stringify({
        playlist_id: uploads.playlist_id,
        selected_count: candidateRows.length,
        content_limit: channelContentLimit,
        content_max_age_days: contentMaxAgeDays,
        upload_scan: {
          ...(uploads.scan ?? {}),
          scan_policy_version: repairRunMetadata.publication_repair?.scan_policy_version ?? null,
          requested_limit: channelContentLimit,
          content_max_age_days: contentMaxAgeDays,
          selected_count: candidateRows.length,
        },
        tab_counts: uploads.tab_counts,
        untyped_count: uploads.untyped_ids.length,
        channel_extractor: channelExtractor,
        uploads_extractor: uploadsExtractor,
        youtubejs_uploads_error: youtubeJsUploadsError ? String(youtubeJsUploadsError?.message ?? youtubeJsUploadsError) : null,
        ...(migrationActivityInitialEvidence == null
          ? {}
          : { migration_activity_initial_evidence: migrationActivityInitialEvidence }),
        youtube_request_counts: {
          get_channel: Number(youtubeJsChannel?.raw?.request_counts?.get_channel ?? 0),
          get_about: Number(youtubeJsChannel?.raw?.request_counts?.get_about ?? 0),
          channel_snapshot: Number(youtubeJsChannel?.raw?.request_count ?? 0),
          uploads_and_tabs: Number(uploads.raw?.request_count ?? 0),
        },
      }),
      ],
    );
  });
  phaseTimingsMs.channel_candidates_persist = Date.now() - phaseStartedAt;
  let detailResult = null;
  phaseStartedAt = Date.now();
  if (uploads.entries.length > 0 && channelInlineDetails) {
    detailResult = await processContentDetailRun({
      runId,
      channelId,
      signal: currentChannelExecutionAbortSignal(),
      executionMode: "channel_inline",
      finalize: false,
      contentMaxAgeDays,
    });
  } else if (uploads.entries.length > 0) {
    await queues[queuesByRole.contentDetail].add(
      "content-detail-batch",
      {
        channel_id: channelId,
        run_id: runId,
        pipeline_cycle_id: text(job.data?.pipeline_cycle_id),
        content_max_age_days: contentMaxAgeDays,
      },
      { jobId: safeJobId("content-detail", runId) },
    );
  } else {
    detailResult = await updateRunDetailStatus(runId);
  }
  phaseTimingsMs.content_detail = Date.now() - phaseStartedAt;
  phaseStartedAt = Date.now();
  if (migrationActivityCanFinalize(detailResult?.migration_activity_gate)) {
    await queueFinalize(
      channelId,
      runId,
      channelInlineDetails ? "channel-full-complete" : "channel-crawl-complete",
    );
  }
  phaseTimingsMs.queue_finalize = Date.now() - phaseStartedAt;
  if (Number(detailResult?.failed ?? 0) > 0) {
    throw contentDetailFailureError(
      `${detailResult.failed} content candidates failed during inline channel crawl`,
      [detailResult.retryable_failure_error],
    );
  }
  return {
    ok: true,
    channel_id: channelId,
    run_id: runId,
    content_limit: channelContentLimit,
    content_max_age_days: contentMaxAgeDays,
    candidate_count: uploads.entries.length,
    detail_execution: channelInlineDetails ? "channel_inline" : "detail_queue",
    detail_concurrency: settings.detailConcurrency,
    detail_processed: Number(detailResult?.processed ?? 0),
    detail_status: detailResult?.status ?? (uploads.entries.length > 0 ? "queued" : "done"),
    detail_partial: Number(detailResult?.partial ?? 0),
    phase_timings_ms: {
      ...phaseTimingsMs,
      total: Date.now() - pipelineStartedAt,
    },
    ytdlp_uploads_timings_ms: uploads.raw?.stage_timings_ms ?? null,
    ytdlp_uploads_engine: uploadsExtractor === "yt_dlp" ? (uploads.raw?.engine ?? "one_shot") : null,
    channel_extractor: channelExtractor,
    uploads_extractor: uploadsExtractor,
    youtubejs_channel_error: youtubeJsChannelError ? String(youtubeJsChannelError?.message ?? youtubeJsChannelError) : null,
    youtubejs_uploads_error: youtubeJsUploadsError ? String(youtubeJsUploadsError?.message ?? youtubeJsUploadsError) : null,
  };
}

async function excludeCandidateByAge(row, detail, ageDays, maxAgeDays, source, window) {
  const resultJson = {
    ...(row.result_json ?? {}),
    detail,
    scope: {
      status: "excluded",
      reason: "older_than_max_age",
      age_days: ageDays,
      max_age_days: maxAgeDays,
      source,
      classifier_version: window.classifier_version,
      policy_version: CONTENT_WINDOW_POLICY_VERSION,
      relation: window.relation,
      relation_reason_code: window.reason_code,
      basis: window.basis,
      precision: window.precision,
      evidence_source: window.source,
    },
    extractor_version: detail?.extractor_version ?? "v5_full_config_first_success",
  };
  await query(
    `UPDATE crawler.content_candidates
     SET content_key=NULL,detail_status='done',api_status='not_needed',missing_fields='{}'::text[],
         result_json=$2::jsonb,error_message=NULL,finished_at=now(),updated_at=now()
     WHERE candidate_id=$1`,
    [row.candidate_id, JSON.stringify(resultJson)],
  );
  await persistFullVideoDisposition(row, {
    storageAction: { kind: "unresolved" },
    classification: null,
    access: accessFromDetail(detail),
    detail,
    terminalReason: "outside_content_window",
  });
  if (row.content_key) {
    await query("DELETE FROM crawler.contents WHERE content_key=$1 AND run_id=$2", [row.content_key, row.run_id]);
  }
  return {
    candidate_id: row.candidate_id,
    video_id: row.source_content_id,
    content_type: row.content_type ?? null,
    api_missing: [],
    error: null,
    excluded: true,
    cutoff: true,
    age_days: ageDays,
    max_age_days: maxAgeDays,
    classifier_version: window.classifier_version,
    policy_version: CONTENT_WINDOW_POLICY_VERSION,
  };
}

async function excludeUnfinishedLiveCandidate(row, detail, source, reason = unfinishedLiveReason(detail)) {
  if (!["upcoming_live", "live_in_progress"].includes(reason)) {
    throw new Error(`unfinished Live exclusion requires a supported reason for ${row.source_content_id}`);
  }
  const liveStatus = String(
    detail?.live_status ?? (reason === "upcoming_live" ? "is_upcoming" : "is_live"),
  ).toLowerCase();
  const classification = resolveYoutubeContentType({
    videoId: row.source_content_id,
    upload: row.result_json?.flat ?? {
      video_id: row.source_content_id,
      content_type: row.content_type,
      type_source: row.type_source,
    },
    detail,
  });
  const confirmedClassification = classification?.authoritative === true ? classification : null;
  const typeSource = confirmedClassification?.source ?? `youtube_detail_${reason}`;
  const resultJson = {
    ...(row.result_json ?? {}),
    detail,
    classification: confirmedClassification ?? classification,
    scope: {
      status: "excluded",
      reason,
      source,
      live_status: liveStatus,
      scheduled_at: detail?.live_scheduled_at ?? detail?.release_at ?? null,
    },
    extractor_version: detail?.extractor_version ?? "v6_persistent_session",
  };
  await query(
    `UPDATE crawler.content_candidates
     SET content_type='live',type_status='resolved',type_source=$2,content_key=NULL,
         detail_status='done',api_status='not_needed',missing_fields='{}'::text[],
         result_json=$3::jsonb,error_message=NULL,finished_at=now(),updated_at=now()
     WHERE candidate_id=$1`,
    [row.candidate_id, typeSource, JSON.stringify(resultJson)],
  );
  await persistFullVideoDisposition(row, {
    storageAction: { kind: "unresolved" },
    classification,
    access: accessFromDetail(detail),
    detail,
    terminalReason: reason,
  });
  if (row.content_key) {
    await query("DELETE FROM crawler.contents WHERE content_key=$1", [row.content_key]);
  }
  return {
    candidate_id: row.candidate_id,
    video_id: row.source_content_id,
    content_type: "live",
    api_missing: [],
    error: null,
    excluded: true,
    upcoming: reason === "upcoming_live",
    live_in_progress: reason === "live_in_progress",
  };
}

function isUndisposedTerminalCandidate(row) {
  return ["done", "unavailable"].includes(text(row?.detail_status))
    && row?.disposition == null;
}

function terminalDispositionReasonFromCandidate(row) {
  const scopeReason = text(row?.result_json?.scope?.reason);
  if (["older_than_max_age", "after_chronological_age_cutoff"].includes(scopeReason)) {
    return "outside_content_window";
  }
  if (["upcoming_live", "live_in_progress"].includes(scopeReason)) return scopeReason;
  return null;
}

function terminalDispositionEvidence(row) {
  const resultJson = row?.result_json ?? {};
  const terminalReason = terminalDispositionReasonFromCandidate(row);
  if (terminalReason) {
    return {
      classification: resultJson.classification ?? null,
      access: resultJson.access ?? accessFromDetail(resultJson.detail),
      detail: resultJson.detail ?? null,
      error: null,
      terminalReason,
    };
  }
  const hasClassification = Object.prototype.hasOwnProperty.call(resultJson, "classification");
  const hasAccess = resultJson.access && typeof resultJson.access === "object";
  if (!hasClassification || !hasAccess) return null;
  const persistedErrors = Array.isArray(resultJson.errors) ? resultJson.errors : [];
  const persistedError = text(row?.error_message);
  return {
    classification: resultJson.classification,
    access: resultJson.access,
    detail: resultJson.detail ?? null,
    error: persistedError && persistedErrors.includes(persistedError)
      ? new Error(persistedError)
      : null,
    terminalReason: null,
  };
}

async function recoverTerminalCandidateDisposition(row) {
  const evidence = terminalDispositionEvidence(row);
  if (!evidence) {
    throw new Error(
      `terminal content candidate ${row.candidate_id} has no persisted evidence for disposition recovery`,
    );
  }
  const storageAction = fullVideoStorageAction({
    candidate: row,
    classification: evidence.classification,
    access: evidence.access,
  });
  const disposition = await persistFullVideoDisposition(row, {
    storageAction,
    classification: evidence.classification,
    access: evidence.access,
    detail: evidence.detail,
    error: evidence.error,
    terminalReason: evidence.terminalReason,
  });
  return {
    candidate_id: row.candidate_id,
    video_id: row.source_content_id,
    content_type: row.content_type ?? null,
    api_missing: [],
    missing_fields: row.missing_fields ?? [],
    error: null,
    partial: Array.isArray(row.missing_fields) && row.missing_fields.length > 0,
    excluded: disposition.kind === "terminal_excluded",
    disposition_recovered: true,
  };
}

function shouldPrefetchYoutubeJsDetail(row, settings) {
  if (isUndisposedTerminalCandidate(row)) return false;
  if (!youtubeJsDetailEnabled()) return false;
  const detail = detailFromCandidate(row);
  if (unfinishedLiveReason(detail)) return false;
  if (settings.contentMaxAgeDays <= 0) return true;
  return classifyContentWindow(
    detail,
    settings.contentMaxAgeDays,
    row.crawl_started_at ?? Date.now(),
  ).relation !== "outside";
}

async function captureYoutubeJsDetail(videoId, { signal = null } = {}) {
  try {
    return { detail: await fetchYoutubeJsVideoDetail(videoId, { signal }), error: null };
  } catch (error) {
    return { detail: null, error };
  }
}

async function processOneCandidate(row, settings, {
  youtubeJsDetail = null,
  signal = null,
} = {}) {
  throwIfAborted(signal);
  if (isUndisposedTerminalCandidate(row)) {
    return recoverTerminalCandidateDisposition(row);
  }
  const attemptNumber = Number(row.attempts ?? 0) + 1;
  const maxAttempts = settings.detailMaxAttempts;
  await query(
    `UPDATE crawler.content_candidates
     SET detail_status='running',attempts=attempts+1,error_message=NULL,updated_at=now()
     WHERE candidate_id=$1`,
    [row.candidate_id],
  );
  let detail = detailFromCandidate(row);
  let detailError = null;
  let youtubeJsDetailError = null;
  let youtubeJsCommentError = null;
  let youtubeJsFallback = [];
  let verifyYoutubeJsDisabledComments = false;
  const commentAttemptSources = [];
  let detailExtractor = "yt_dlp";
  let classification = null;
  let typeError = null;

  const channelTabLiveReason = unfinishedLiveReason(detail);
  if (channelTabLiveReason) {
    return excludeUnfinishedLiveCandidate(row, detail, "channel_tab", channelTabLiveReason);
  }

  const crawlReferenceAt = row.crawl_started_at ?? Date.now();
  const flatWindow = classifyContentWindow(detail, settings.contentMaxAgeDays, crawlReferenceAt);
  if (settings.contentMaxAgeDays > 0 && flatWindow.relation === "outside") {
    return excludeCandidateByAge(
      row,
      detail,
      detailAgeDays(detail, crawlReferenceAt),
      settings.contentMaxAgeDays,
      "uploads_playlist",
      flatWindow,
    );
  }
  if (settings.contentMaxAgeDays > 0 && flatWindow.relation === "unresolved") {
    const detailRequest = {
      reason_code: "initial_publication_unresolved",
      relation: flatWindow.relation,
      relation_reason_code: flatWindow.reason_code,
      classifier_version: flatWindow.classifier_version,
      policy_version: CONTENT_WINDOW_POLICY_VERSION,
      basis: flatWindow.basis,
      precision: flatWindow.precision,
      evidence_source: flatWindow.source,
      requested_at: new Date().toISOString(),
    };
    row.result_json = {
      ...(row.result_json ?? {}),
      detail_request: detailRequest,
    };
    await query(
      `UPDATE crawler.content_candidates
       SET result_json=result_json || jsonb_build_object('detail_request',$2::jsonb),
           updated_at=now()
       WHERE candidate_id=$1`,
      [row.candidate_id, JSON.stringify(detailRequest)],
    );
  }

  if (youtubeJsDetailEnabled()) {
    const youtubeJsResult = youtubeJsDetail === null
      ? await captureYoutubeJsDetail(row.source_content_id, { signal })
      : await youtubeJsDetail;
    throwIfAborted(signal);
    if (!youtubeJsResult?.error && youtubeJsResult?.detail) {
      const youtubeJsObservation = youtubeJsResult.detail;
      verifyYoutubeJsDisabledComments =
        youtubeJsDisabledCommentsNeedVerification(youtubeJsObservation);
      detail = mergeDetail(
        detail,
        verifyYoutubeJsDisabledComments
          ? withoutUnverifiedYoutubeJsDisabledComments(youtubeJsObservation)
          : youtubeJsObservation,
      );
      detailExtractor = "youtubejs";
    } else {
      youtubeJsDetailError = youtubeJsResult?.error ?? new Error("YouTube.js detail prefetch returned no result");
      youtubeJsFallback = ["youtubejs_error"];
    }
    if (!youtubeJsDetailError) {
      youtubeJsFallback = youtubeJsFallbackReasons(
        detail,
        settings.publishedAtRequiredPrecision,
        row.source_content_id,
      );
      if (verifyYoutubeJsDisabledComments) {
        youtubeJsFallback.push("comments_disabled_verification");
      }
      youtubeJsFallback = [...new Set(youtubeJsFallback)];
    }
  } else {
    youtubeJsFallback = ["youtubejs_disabled"];
  }

  const youtubeJsLiveReason = unfinishedLiveReason(detail);
  if (youtubeJsLiveReason) {
    return excludeUnfinishedLiveCandidate(row, detail, "youtubejs_detail", youtubeJsLiveReason);
  }

  if (youtubeJsFallback.length > 0) {
    try {
      const ytDlpDetail = await fetchVideoYtDlpDetail(
        row.source_content_id,
        row.source_url,
        { language, signal },
      );
      throwIfAborted(signal);
      detail = mergeDetail(detail, ytDlpDetail);
      commentAttemptSources.push("yt_dlp_top_comments");
      detailExtractor = youtubeJsDetailEnabled() ? "youtubejs+yt_dlp" : "yt_dlp";
      if (
        youtubeJsDetailError
        && isYoutubeIpBlockedError(youtubeJsDetailError)
        && accessFromDetail(detail).access_status === "login_required"
      ) {
        detailError = youtubeJsDetailError;
      }
    } catch (error) {
      throwIfAborted(signal);
      detailError = error;
    }
  }

  const youtubeDetailLiveReason = unfinishedLiveReason(detail);
  if (youtubeDetailLiveReason) {
    return excludeUnfinishedLiveCandidate(row, detail, "youtube_detail", youtubeDetailLiveReason);
  }

  if (commentFirstPageNeedsResolution(detail) && !youtubeJsDetailEnabled()) {
    try {
      detail = mergeDetail(detail, await fetchYoutubeJsCommentFirstPage(
        row.source_content_id,
        { totalCount: integer(detail?.comment_count), signal },
      ));
      throwIfAborted(signal);
      commentAttemptSources.push("youtubejs_comments");
      detailExtractor = `${detailExtractor}+youtubejs_comments`;
    } catch (error) {
      throwIfAborted(signal);
      youtubeJsCommentError = error;
      commentAttemptSources.push("youtubejs_comments_error");
    }
  }

  const resolvedWindow = classifyContentWindow(detail, settings.contentMaxAgeDays, crawlReferenceAt);
  if (settings.contentMaxAgeDays > 0 && resolvedWindow.relation === "outside") {
    return excludeCandidateByAge(
      row,
      detail,
      detailAgeDays(detail, crawlReferenceAt),
      settings.contentMaxAgeDays,
      "youtube_detail",
      resolvedWindow,
    );
  }

  try {
    classification = resolveYoutubeContentType({
      videoId: row.source_content_id,
      upload: row.result_json?.flat ?? {
        video_id: row.source_content_id,
        content_type: row.content_type,
        type_source: row.type_source,
      },
      detail,
    });
  } catch (error) {
    typeError = error;
  }
  const confirmedRetryClassification = classification?.authoritative === true
    ? classification
    : null;
  const retryMissing = missingApiFields(
    normalizeResolvedDetail(detail, { access: accessFromDetail(detail) }).detail,
    settings.publishedAtRequiredPrecision,
  );
  const collectionFailure = isYoutubeCollectionFailureError(detailError);
  if (detailError
      && isTransientYoutubeError(detailError)
      && (collectionFailure || retryMissing.length > 0)) {
    const resultJson = {
      ...(row.result_json ?? {}),
      detail,
      classification,
      access: accessFromDetail(detail),
      errors: [youtubeJsDetailError, youtubeJsCommentError, detailError, typeError]
        .filter(Boolean)
        .map((error) => String(error?.message ?? error)),
      extractor_version: detail?.extractor_version ?? "v4_first_success",
      detail_extractor: detailExtractor,
      youtubejs_fallback_reasons: youtubeJsFallback,
      comments_first_page_attempt_sources: [...new Set(commentAttemptSources)],
      scrape_attempt: attemptNumber,
      scrape_max_attempts: maxAttempts,
    };
    await query(
      `UPDATE crawler.content_candidates
       SET content_type=COALESCE($2,content_type),
           type_status=CASE WHEN $2::text IS NULL THEN type_status ELSE 'resolved' END,
           type_source=COALESCE($3,type_source),detail_status='failed',api_status='not_needed',
           missing_fields=$4::text[],result_json=$5::jsonb,error_message=$6,updated_at=now()
       WHERE candidate_id=$1`,
      [
        row.candidate_id,
        confirmedRetryClassification?.content_type ?? null,
        confirmedRetryClassification?.source ?? null,
        retryMissing,
        JSON.stringify(resultJson),
        String(detailError?.message ?? detailError),
      ],
    );
    const retryAccess = accessFromDetail(detail);
    await persistFullVideoDisposition(row, {
      storageAction: fullVideoStorageAction({
        candidate: row,
        classification,
        access: retryAccess,
      }),
      classification,
      access: retryAccess,
      detail,
      error: detailError,
    });
    return {
      candidate_id: row.candidate_id,
      video_id: row.source_content_id,
      content_type: confirmedRetryClassification?.content_type ?? row.content_type ?? null,
      api_missing: [],
      error: detailError,
      retryable: attemptNumber < maxAttempts,
    };
  }
  const access = accessFromDetail(detail, "unknown");
  const normalized = normalizeResolvedDetail(detail, { access });
  const apiMissing = missingApiFields(normalized.detail, settings.publishedAtRequiredPrecision);
  const storageAction = fullVideoStorageAction({
    candidate: row,
    classification,
    access: normalized.access,
  });
  const contentType = storageAction.kind === "unresolved" ? null : storageAction.content_type;
  const typeSource = storageAction.kind === "unresolved" ? null : storageAction.type_source;
  const candidateMissing = contentType ? apiMissing : ["content_type", ...apiMissing];
  const resultJson = {
    ...(row.result_json ?? {}),
    detail: normalized.detail,
    classification,
    access: normalized.access,
    errors: [youtubeJsDetailError, youtubeJsCommentError, detailError, typeError]
      .filter(Boolean)
      .map((error) => String(error?.message ?? error)),
    extractor_version: normalized.detail.extractor_version ?? "v4_first_success",
    detail_extractor: detailExtractor,
    youtubejs_fallback_reasons: youtubeJsFallback,
    comments_first_page_attempt_sources: [...new Set(commentAttemptSources)],
    scrape_attempt: attemptNumber,
    scrape_max_attempts: maxAttempts,
  };
  const parserError = unresolvedParserContractError(
    [youtubeJsDetailError, detailError, typeError],
    {
      contentType,
      missingFields: apiMissing,
      accessStatus: normalized.access.access_status,
    },
  );
  if (parserError) {
    const parserDetails = parserContractDetails(parserError);
    const parserResultJson = {
      ...resultJson,
      parser_contract_error: parserDetails,
    };
    await query(
      `UPDATE crawler.content_candidates
       SET content_type=$2,type_status=CASE WHEN $2::text IS NULL THEN 'unresolved' ELSE 'resolved' END,
           type_source=$3,detail_status='failed',api_status='not_needed',
           missing_fields=$4::text[],result_json=$5::jsonb,error_message=$6,updated_at=now()
       WHERE candidate_id=$1`,
      [
        row.candidate_id,
        contentType,
        typeSource,
        candidateMissing,
        JSON.stringify(parserResultJson),
        String(parserError?.message ?? parserError),
      ],
    );
    await persistFullVideoDisposition(row, {
      storageAction,
      classification,
      access: normalized.access,
      detail: normalized.detail,
      error: parserError,
    });
    throw parserError;
  }

  if (!contentType) {
    const unresolvedError = typeError ?? new Error(`content type unresolved for ${row.source_content_id}`);
    const terminal = attemptNumber >= maxAttempts;
    await query(
      `UPDATE crawler.content_candidates
       SET content_type=NULL,type_status=$2,type_source=NULL,detail_status=$3,
           api_status=$4,missing_fields=$5::text[],result_json=$6::jsonb,error_message=$7,
           finished_at=CASE WHEN $3='unavailable' THEN now() ELSE finished_at END,updated_at=now()
       WHERE candidate_id=$1`,
      [
        row.candidate_id,
        terminal ? "unavailable" : "unresolved",
        terminal ? "unavailable" : "failed",
        terminal ? "unavailable" : "not_needed",
        candidateMissing,
        JSON.stringify(resultJson),
        String(unresolvedError?.message ?? unresolvedError),
      ],
    );
    await persistFullVideoDisposition(row, {
      storageAction,
      classification,
      access: normalized.access,
      detail: normalized.detail,
      error: typeError,
    });
    return {
      candidate_id: row.candidate_id,
      video_id: row.source_content_id,
      content_type: null,
      api_missing: candidateMissing,
      error: unresolvedError,
      retryable: !terminal,
    };
  }

  if (storageAction.kind === "classified_only") {
    const accessStatus = normalized.access.access_status;
    const classifiedResolution = classifiedOnlyResolutionAction({
      accessStatus,
      missingFields: apiMissing,
      attemptNumber,
      maxAttempts,
      apiFallbackMode: settings.apiFallbackMode,
      apiAlreadyAttempted: Object.prototype.hasOwnProperty.call(row.result_json ?? {}, "api_detail"),
    });
    const accessError = new Error(
      `content access ${accessStatus || "unknown"} for ${row.source_content_id}`,
    );
    if (classifiedResolution.action === "api") {
      const apiResultJson = {
        ...resultJson,
        classified_only: true,
        api_trigger: {
          mode: "emergency",
          reason: "access_status_unresolved",
          attempt: attemptNumber,
          missing_fields: classifiedResolution.missingFields,
        },
      };
      await query(
        `UPDATE crawler.content_candidates
         SET content_type=$2,type_status='resolved',type_source=$3,content_key=NULL,
             detail_status='api_pending',api_status='pending',missing_fields=$4::text[],
             result_json=$5::jsonb,error_message=NULL,updated_at=now()
         WHERE candidate_id=$1`,
        [
          row.candidate_id,
          contentType,
          typeSource,
          classifiedResolution.missingFields,
          JSON.stringify(apiResultJson),
        ],
      );
      await persistFullVideoDisposition(row, {
        storageAction,
        classification,
        access: normalized.access,
        detail: normalized.detail,
      });
      await enqueueYoutubeApiFallback(row, classifiedResolution.missingFields);
      return {
        candidate_id: row.candidate_id,
        video_id: row.source_content_id,
        content_type: contentType,
        api_missing: classifiedResolution.missingFields,
        error: null,
        classified_only: true,
      };
    }
    const terminal = classifiedResolution.action === "terminal";
    await query(
      `UPDATE crawler.content_candidates
       SET content_type=$2,type_status='resolved',type_source=$3,content_key=NULL,
           detail_status=$4,api_status=$5,missing_fields=$6::text[],result_json=$7::jsonb,
           error_message=$8,finished_at=CASE WHEN $4='unavailable' THEN now() ELSE finished_at END,
           updated_at=now()
       WHERE candidate_id=$1`,
      [
        row.candidate_id,
        contentType,
        typeSource,
        terminal ? "unavailable" : "failed",
        terminal ? "unavailable" : "not_needed",
        classifiedResolution.missingFields,
        JSON.stringify({
          ...resultJson,
          classified_only: true,
          terminal_reason: terminal ? accessStatus : null,
        }),
        accessError.message,
      ],
    );
    await persistFullVideoDisposition(row, {
      storageAction,
      classification,
      access: normalized.access,
      detail: normalized.detail,
    });
    return {
      candidate_id: row.candidate_id,
      video_id: row.source_content_id,
      content_type: contentType,
      api_missing: [],
      missing_fields: classifiedResolution.missingFields,
      error: accessError,
      retryable: !terminal,
      classified_only: true,
    };
  }

  if (storageAction.kind === "update_access") {
    const terminal = normalizeResolvedDetail(normalized.detail, {
      afterApi: true,
      access: normalized.access,
    });
    const stored = await updateExistingContentAccessFromCandidate(row, terminal);
    if (!stored?.content_key) {
      const missingExisting = new Error(`known content identity disappeared for ${row.source_content_id}`);
      await query(
        `UPDATE crawler.content_candidates
         SET content_type=NULL,type_status='unresolved',type_source=NULL,detail_status='failed',
             api_status='not_needed',missing_fields=$2::text[],result_json=$3::jsonb,
             error_message=$4,updated_at=now()
         WHERE candidate_id=$1`,
        [row.candidate_id, candidateMissing, JSON.stringify(resultJson), missingExisting.message],
      );
      await persistFullVideoDisposition(row, {
        storageAction: { kind: "unresolved" },
        classification,
        access: normalized.access,
        detail: normalized.detail,
        error: missingExisting,
      });
      return {
        candidate_id: row.candidate_id,
        video_id: row.source_content_id,
        content_type: null,
        api_missing: candidateMissing,
        error: missingExisting,
        retryable: false,
      };
    }
    const terminalJson = {
      ...resultJson,
      detail: terminal.detail,
      terminal_reason: normalized.access.access_status,
      preserved_content_type: true,
    };
    await query(
      `UPDATE crawler.content_candidates
       SET content_type=$2,type_status='resolved',type_source=$3,content_key=$4,
           detail_status='done',api_status='not_needed',missing_fields=$5::text[],
           result_json=$6::jsonb,error_message=NULL,finished_at=now(),updated_at=now()
       WHERE candidate_id=$1`,
      [
        row.candidate_id,
        stored.content_type,
        stored.content_type_source,
        stored.content_key,
        apiMissing,
        JSON.stringify(terminalJson),
      ],
    );
    await persistFullVideoDisposition(row, {
      storageAction,
      classification,
      access: normalized.access,
      detail: terminal.detail,
    });
    return {
      candidate_id: row.candidate_id,
      video_id: row.source_content_id,
      content_type: stored.content_type,
      api_missing: [],
      missing_fields: apiMissing,
      error: null,
      partial: apiMissing.length > 0,
      access_only: true,
    };
  }

  const candidate = {
    ...row,
    content_type: contentType,
    type_source: typeSource,
    type_authoritative: classification?.authoritative === true,
  };
  let contentKey = await upsertContentFromCandidate(candidate, normalized);
  const resolutionAction = detailResolutionAction({
    error: apiMissing.length > 0 ? detailError : null,
    attemptNumber,
    maxAttempts,
    missingFields: apiMissing,
    apiFallbackMode: settings.apiFallbackMode,
    accessStatus: normalized.access.access_status,
    apiAlreadyAttempted: Object.prototype.hasOwnProperty.call(row.result_json ?? {}, "api_detail"),
  });
  if (resolutionAction === "done") {
    await query(
      `UPDATE crawler.content_candidates
       SET content_type=$2,type_status='resolved',type_source=$3,content_key=$4,
           detail_status='done',api_status='not_needed',missing_fields='{}'::text[],
           result_json=$5::jsonb,error_message=NULL,finished_at=now(),updated_at=now()
       WHERE candidate_id=$1`,
      [row.candidate_id, contentType, typeSource, contentKey, JSON.stringify(resultJson)],
    );
    await persistFullVideoDisposition(row, {
      storageAction,
      classification,
      access: normalized.access,
      detail: normalized.detail,
    });
    return { candidate_id: row.candidate_id, video_id: row.source_content_id, content_type: contentType, api_missing: [], error: null };
  }

  if (resolutionAction === "api") {
    const apiResultJson = {
      ...resultJson,
      api_trigger: {
        mode: "emergency",
        reason: detailError ? "scrape_error" : "scrape_incomplete",
        attempt: attemptNumber,
        missing_fields: apiMissing,
      },
    };
    await query(
      `UPDATE crawler.content_candidates
       SET content_type=$2,type_status='resolved',type_source=$3,content_key=$4,
           detail_status='api_pending',api_status='pending',missing_fields=$5::text[],
           result_json=$6::jsonb,error_message=NULL,updated_at=now()
       WHERE candidate_id=$1`,
      [row.candidate_id, contentType, typeSource, contentKey, apiMissing, JSON.stringify(apiResultJson)],
    );
    await persistFullVideoDisposition(row, {
      storageAction,
      classification,
      access: normalized.access,
      detail: normalized.detail,
    });
    await enqueueYoutubeApiFallback(row, apiMissing);
    return { candidate_id: row.candidate_id, video_id: row.source_content_id, content_type: contentType, api_missing: apiMissing, error: null };
  }

  const terminal = normalizeResolvedDetail(normalized.detail, { afterApi: true, access: normalized.access });
  contentKey = await upsertContentFromCandidate(candidate, terminal);
  const terminalJson = {
    ...resultJson,
    detail: terminal.detail,
    terminal_reason: accessIsTerminalWithoutApi(normalized.access) ? normalized.access.access_status : "partial_fields",
  };
  const partialError = detailError
    ? String(detailError?.message ?? detailError)
    : `partial fields: ${apiMissing.join(", ")}`;
  await query(
    `UPDATE crawler.content_candidates
     SET content_type=$2,type_status='resolved',type_source=$3,content_key=$4,
         detail_status='done',api_status='not_needed',missing_fields=$5::text[],
         result_json=$6::jsonb,error_message=$7,finished_at=now(),updated_at=now()
     WHERE candidate_id=$1`,
    [row.candidate_id, contentType, typeSource, contentKey, apiMissing, JSON.stringify(terminalJson), partialError],
  );
  await persistFullVideoDisposition(row, {
    storageAction,
    classification,
    access: normalized.access,
    detail: terminal.detail,
  });
  return {
    candidate_id: row.candidate_id,
    video_id: row.source_content_id,
    content_type: contentType,
    api_missing: [],
    missing_fields: apiMissing,
    error: null,
    partial: true,
  };
}

async function processContentDetailRun({
  runId,
  channelId,
  signal = null,
  executionMode = "detail_queue",
  finalize = true,
  publishedAtRequiredPrecision = null,
  apiFallbackMode = null,
  contentMaxAgeDays = null,
}) {
  if (!runId || !channelId) throw new Error("run_id and channel_id are required");
  await query("UPDATE crawler.channel_runs SET detail_status='running',status='waiting_detail',updated_at=now() WHERE run_id=$1", [runId]);
  const rows = await query(
    `SELECT candidate.*,run.started_at AS crawl_started_at,
            known.content_key AS known_content_key,
            known.content_type AS known_content_type,
            known.content_type_source AS known_content_type_source,
            run.result_json#>>'{publication_repair,content_max_age_days}'
              AS repair_content_max_age_days
     FROM crawler.content_candidates candidate
     JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
     LEFT JOIN crawler.contents known
       ON known.channel_id=candidate.channel_id
      AND known.source_content_id=candidate.source_content_id
     WHERE candidate.run_id=$1
       AND (
         candidate.detail_status NOT IN ('done','unavailable','api_pending')
         OR (
           candidate.detail_status IN ('done','unavailable')
           AND candidate.disposition IS NULL
         )
       )
     ORDER BY candidate.position ASC`,
    [runId],
  );
  const crawlSettings = await getCrawlSettingsV2();
  const apiSettings = await getYoutubeApiSettingsV2();
  const settings = {
    detailMaxAttempts: crawlSettings.detailMaxAttempts,
    detailConcurrency: crawlSettings.detailConcurrency,
    contentMaxAgeDays: intValue(
      contentMaxAgeDays ?? rows.rows[0]?.repair_content_max_age_days,
      crawlSettings.contentMaxAgeDays,
      0,
      3650,
    ),
    publishedAtRequiredPrecision: ["date_only", "second"].includes(publishedAtRequiredPrecision)
      ? publishedAtRequiredPrecision
      : crawlSettings.publishedAtRequiredPrecision,
    apiFallbackMode: ["disabled", "emergency"].includes(apiFallbackMode)
      ? apiFallbackMode
      : apiSettings.fallbackMode,
  };
  const execution = await processWithOrderedPrefetch({
    items: rows.rows,
    concurrency: settings.detailConcurrency,
    signal,
    shouldPrefetch: (row) => shouldPrefetchYoutubeJsDetail(row, settings),
    prefetch: (row) => captureYoutubeJsDetail(row.source_content_id, { signal }),
    process: (row, youtubeJsDetail) => processOneCandidate(row, settings, {
      youtubeJsDetail,
      signal,
    }),
    stopAfter: contentDetailBatchStopReason,
  });
  const results = [...execution.results];
  const processed = execution.processed;
  const cancelledApiTasks = await cancelResolvedYoutubeApiTasks(runId);
  await saveJsonRaw({
    objectType: "youtube_content_detail_batch_json",
    entityType: "channel_run",
    entityId: runId,
    source: executionMode === "channel_inline" ? "channel_inline_detail_v2" : "content_detail_v2",
    payload: { channel_id: channelId, run_id: runId, execution_mode: executionMode, results },
    metadata: {
      channel_id: channelId,
      run_id: runId,
      count: results.length,
      execution_mode: executionMode,
      extractor_version: "v4_first_success",
      detail_concurrency: settings.detailConcurrency,
    },
  });
  const summary = await updateRunDetailStatus(runId);
  const youtubeRequestRows = await query(
    `SELECT COALESCE(sum(COALESCE((result_json#>>'{detail,youtubejs_request_count}')::int,0)),0)::int AS request_count
     FROM crawler.content_candidates
     WHERE run_id=$1`,
    [runId],
  );
  const youtubeJsDetailRequestCount = Number(youtubeRequestRows.rows[0]?.request_count ?? 0);
  const retryableFailureError = contentDetailFailureError("content detail failure", results).cause ?? null;
  await query(
    `UPDATE crawler.channel_runs
     SET result_json=jsonb_set(
           result_json,
           '{youtube_request_counts}',
           COALESCE(result_json->'youtube_request_counts','{}'::jsonb)
             || jsonb_build_object('content_detail',$2::int),
           true
         ) || jsonb_build_object('detail_concurrency',$3::int),
         updated_at=now()
     WHERE run_id=$1`,
    [runId, youtubeJsDetailRequestCount, settings.detailConcurrency],
  );
  if (finalize && migrationActivityCanFinalize(summary.migration_activity_gate)) {
    await queueFinalize(channelId, runId, "content-detail-updated");
  }
  return {
    ok: true,
    channel_id: channelId,
    run_id: runId,
    execution_mode: executionMode,
    detail_concurrency: settings.detailConcurrency,
    processed,
    youtubejs_request_count: youtubeJsDetailRequestCount,
    cancelled_api_tasks: cancelledApiTasks,
    ...(retryableFailureError ? { retryable_failure_error: retryableFailureError } : {}),
    ...summary,
  };
}

export async function processContentDetailBatchV2(job) {
  const runId = text(job.data?.run_id);
  const channelId = text(job.data?.channel_id);
  const result = await processContentDetailRun({
    runId,
    channelId,
    signal: currentChannelExecutionAbortSignal(),
    publishedAtRequiredPrecision: text(job.data?.published_at_required_precision),
    apiFallbackMode: text(job.data?.api_fallback_mode),
    contentMaxAgeDays: job.data?.content_max_age_days,
  });
  const summary = result;
  if (Number(summary.failed) > 0) {
    throw contentDetailFailureError(
      `${summary.failed} content candidates failed before API fallback`,
      [summary.retryable_failure_error],
    );
  }
  return result;
}

export async function processCheckpointRepairV2(job) {
  const repairRunId = text(job.data?.run_id);
  const targetRunId = text(job.data?.checkpoint_target_run_id)
    ?? text(job.data?.repair_parent_run_id);
  const channelId = text(job.data?.channel_id);
  const repairRound = integer(job.data?.repair_round);
  if (!repairRunId || !targetRunId || !channelId || !repairRound) {
    throw new Error("checkpoint repair requires run_id, target Run, channel_id and repair_round");
  }
  if (repairRunId === targetRunId) {
    throw new Error("checkpoint repair cannot reuse an exhausted Business Run");
  }
  const prepared = await prepareCheckpointRepairCandidates(query, {
    targetRunId,
    repairRunId,
    repairRound,
  });
  try {
    const result = await processContentDetailRun({
      runId: targetRunId,
      channelId,
      signal: currentChannelExecutionAbortSignal(),
      executionMode: "checkpoint_repair",
      finalize: true,
      publishedAtRequiredPrecision: text(job.data?.published_at_required_precision),
      apiFallbackMode: text(job.data?.api_fallback_mode) ?? "emergency",
      contentMaxAgeDays: job.data?.content_max_age_days,
    });
    if (Number(result.failed ?? 0) > 0) {
      throw contentDetailFailureError(
        `${result.failed} content candidates failed during checkpoint repair`,
        [result.retryable_failure_error],
      );
    }
    await finishCheckpointRepairExecution(query, {
      repairRunId,
      targetRunId,
      status: "done",
      summary: result,
    });
    return {
      ...result,
      checkpoint_repair: true,
      repair_run_id: repairRunId,
      target_run_id: targetRunId,
      prepared_candidates: prepared.prepared_count,
    };
  } catch (error) {
    await finishCheckpointRepairExecution(query, {
      repairRunId,
      targetRunId,
      status: "failed",
      summary: { prepared_candidates: prepared.prepared_count },
      error: error?.message ?? error,
    });
    throw error;
  }
}

function mergeApiDetail(existing, apiDetail) {
  return mergeDetail(existing, apiDetail);
}

async function reserveYoutubeApiRequest(dailyRequestLimit, requestedVideoCount) {
  if (dailyRequestLimit <= 0) return null;
  const rows = await query(
    `INSERT INTO crawler.youtube_api_daily_usage (
       usage_date,request_count,requested_video_count,created_at,updated_at
     ) VALUES (CURRENT_DATE,1,$2,now(),now())
     ON CONFLICT (usage_date) DO UPDATE
     SET request_count=crawler.youtube_api_daily_usage.request_count+1,
         requested_video_count=crawler.youtube_api_daily_usage.requested_video_count+EXCLUDED.requested_video_count,
         updated_at=now()
     WHERE crawler.youtube_api_daily_usage.request_count < $1
     RETURNING usage_date,request_count,requested_video_count`,
    [dailyRequestLimit, requestedVideoCount],
  );
  return rows.rows[0] ?? null;
}

async function deferYoutubeApiBatchForDailyLimit({ batchId, taskIds, candidateIds, dailyRequestLimit }) {
  const nextRetrySql = "date_trunc('day', now()) + interval '1 day 5 minutes'";
  await query(
    `UPDATE crawler.youtube_api_batches
     SET status='done',
         result_json=result_json || $2::jsonb,
         error_message=NULL,finished_at=now(),updated_at=now()
     WHERE batch_id=$1`,
    [batchId, JSON.stringify({ deferred: true, reason: "daily_request_limit_reached", daily_request_limit: dailyRequestLimit })],
  );
  await query(
    `UPDATE crawler.youtube_api_tasks
     SET status='pending',error_message=NULL,next_retry_at=${nextRetrySql},updated_at=now()
     WHERE task_id=ANY($1::bigint[])`,
    [taskIds],
  );
  await query(
    `UPDATE crawler.content_candidates
     SET api_status='pending',error_message=NULL,updated_at=now()
     WHERE candidate_id=ANY($1::bigint[])`,
    [candidateIds],
  );
}

function youtubeApiTaskNeedsCommentThreads(task) {
  return (Array.isArray(task?.missing_fields) ? task.missing_fields : [])
    .includes("comments_first_page");
}

async function fetchCommentThreadsForApiTask(task, settings, {
  preferredKeyIndex = null,
  totalCount = null,
} = {}) {
  const keyIndices = [
    ...(Number.isInteger(preferredKeyIndex) ? [preferredKeyIndex] : []),
    ...settings.apiKeys.map((_, index) => index),
  ].filter((index, position, values) => values.indexOf(index) === position);
  let requestAttempts = 0;
  let lastError = null;
  for (const index of keyIndices) {
    const usage = await reserveYoutubeApiRequest(settings.dailyRequestLimit, 1);
    if (!usage) {
      return { deferred: true, requestAttempts, lastError };
    }
    requestAttempts += 1;
    try {
      const result = await fetchVideoCommentThreadsDataApi(
        task.source_content_id,
        settings.apiKeys[index],
        { timeoutMs: settings.timeoutMs, totalCount },
      );
      return { result, keyIndex: index, requestAttempts, deferred: false, lastError: null };
    } catch (error) {
      lastError = error;
    }
  }
  return { result: null, keyIndex: null, requestAttempts, deferred: false, lastError };
}

async function deferYoutubeApiTaskForDailyLimit(task, dailyRequestLimit) {
  const nextRetrySql = "date_trunc('day', now()) + interval '1 day 5 minutes'";
  await query(
    `UPDATE crawler.youtube_api_tasks
     SET status='pending',error_message=NULL,next_retry_at=${nextRetrySql},
         result_json=COALESCE(result_json,'{}'::jsonb)
           || jsonb_build_object(
                'deferred',true,
                'reason','daily_request_limit_reached',
                'daily_request_limit',$2::int,
                'api_verification',
                COALESCE(result_json->'api_verification','{}'::jsonb)
                  || jsonb_build_object(
                       'comment_threads',jsonb_build_object(
                         'status','deferred',
                         'reason','daily_request_limit_reached',
                         'checked_at',now()
                       )
                     )
              ),
         updated_at=now()
     WHERE task_id=$1`,
    [task.task_id, dailyRequestLimit],
  );
  await query(
    `UPDATE crawler.content_candidates
     SET api_status='pending',error_message=NULL,updated_at=now()
     WHERE candidate_id=ANY($1::bigint[])`,
    [task.candidate_ids ?? []],
  );
}

async function failYoutubeApiCommentTask(task, error) {
  const message = String(error?.message ?? error ?? "commentThreads.list failed");
  await query(
    `UPDATE crawler.youtube_api_tasks
     SET status='failed',error_message=$2,next_retry_at=now()+interval '5 minutes',
         result_json=COALESCE(result_json,'{}'::jsonb)
           || jsonb_build_object(
                'api_verification',
                COALESCE(result_json->'api_verification','{}'::jsonb)
                  || jsonb_build_object(
                       'comment_threads',jsonb_build_object(
                         'status','failed','error',$2::text,'checked_at',now()
                       )
                     )
              ),
         updated_at=now()
     WHERE task_id=$1`,
    [task.task_id, message],
  );
  await query(
    `UPDATE crawler.content_candidates
     SET api_status='failed',error_message=$2,updated_at=now()
     WHERE candidate_id=ANY($1::bigint[])`,
    [task.candidate_ids ?? [], message],
  );
}

export async function processDataApiBatchV2(job) {
  const batchId = text(job.data?.batch_id) ?? String(job.id);
  const taskIds = [...new Set((job.data?.task_ids ?? []).map(Number).filter(Number.isFinite))].slice(0, 50);
  if (taskIds.length === 0) return { ok: true, skipped: true, reason: "no tasks" };
  const taskRows = await query("SELECT * FROM crawler.youtube_api_tasks WHERE task_id=ANY($1::bigint[]) ORDER BY task_id", [taskIds]);
  const videoIds = taskRows.rows.map((row) => row.source_content_id);
  const storedReplay = job.data?.stored_evidence_replay
    ? storedDataApiReplayResult({ jobData: job.data, tasks: taskRows.rows })
    : null;
  const settings = await getYoutubeApiSettingsV2();
  const crawlSettings = await getCrawlSettingsV2();
  const candidateIds = taskRows.rows.flatMap((row) => row.candidate_ids ?? []);
  const markBatchFailed = async (error) => {
    const message = String(error?.message ?? error);
    await query(
      `UPDATE crawler.youtube_api_batches SET status='failed',error_message=$2,finished_at=now(),updated_at=now() WHERE batch_id=$1`,
      [batchId, message],
    );
    await query(
      `UPDATE crawler.youtube_api_tasks
       SET status='failed',error_message=$2,next_retry_at=now()+interval '5 minutes',updated_at=now()
       WHERE task_id=ANY($1::bigint[])`,
      [taskIds, message],
    );
    await query(
      `UPDATE crawler.content_candidates
       SET api_status='failed',error_message=$2,updated_at=now()
       WHERE candidate_id=ANY($1::bigint[])`,
      [candidateIds, message],
    );
  };
  if (!storedReplay && settings.apiKeys.length === 0) {
    const error = new Error("youtube data api key missing");
    await markBatchFailed(error);
    throw error;
  }
  await query(
    `UPDATE crawler.youtube_api_batches SET status='running',started_at=now(),updated_at=now() WHERE batch_id=$1`,
    [batchId],
  );
  await query(
    `UPDATE crawler.youtube_api_tasks SET status='running',attempts=attempts+1,updated_at=now() WHERE task_id=ANY($1::bigint[])`,
    [taskIds],
  );
  await query(
    `UPDATE crawler.content_candidates SET api_status='running',updated_at=now()
     WHERE candidate_id=ANY($1::bigint[])`,
    [candidateIds],
  );
  let apiResult = storedReplay;
  let keyIndex = null;
  let lastError = null;
  let requestAttempts = storedReplay?.requestAttempts ?? 0;
  if (!storedReplay) {
    for (let index = 0; index < settings.apiKeys.length; index += 1) {
      const usage = await reserveYoutubeApiRequest(settings.dailyRequestLimit, videoIds.length);
      if (!usage) {
        await deferYoutubeApiBatchForDailyLimit({
          batchId,
          taskIds,
          candidateIds,
          dailyRequestLimit: settings.dailyRequestLimit,
        });
        return {
          ok: true,
          deferred: true,
          reason: "daily_request_limit_reached",
          daily_request_limit: settings.dailyRequestLimit,
        };
      }
      requestAttempts += 1;
      try {
        apiResult = await fetchVideoDataApiDetails(videoIds, settings.apiKeys[index], { timeoutMs: settings.timeoutMs });
        keyIndex = index;
        break;
      } catch (error) {
        lastError = error;
      }
    }
  }
  if (!apiResult) {
    await markBatchFailed(lastError);
    throw lastError;
  }
  if (!storedReplay) {
    await saveJsonRaw({
      objectType: "youtube_data_api_batch_json",
      entityType: "youtube_api_batch",
      entityId: batchId,
      source: "youtube_data_api_videos_list",
      payload: apiResult.raw,
      metadata: { batch_id: batchId, requested_count: videoIds.length, returned_count: apiResult.returnedCount },
    });
  }

  const affectedRuns = new Map();
  const taskOutcomes = { done: 0, unavailable: 0, failed: 0, deferred: 0 };
  for (const task of taskRows.rows) {
    const apiDetail = apiResult.detailsById.get(task.source_content_id) ?? {};
    let commentApiResult = null;
    if (youtubeApiTaskNeedsCommentThreads(task) && !unfinishedLiveReason(apiDetail)) {
      const commentFetch = await fetchCommentThreadsForApiTask(task, settings, {
        preferredKeyIndex: keyIndex,
        totalCount: integer(apiDetail.comment_count),
      });
      requestAttempts += commentFetch.requestAttempts;
      if (commentFetch.deferred) {
        await deferYoutubeApiTaskForDailyLimit(task, settings.dailyRequestLimit);
        taskOutcomes.deferred += 1;
        continue;
      }
      if (!commentFetch.result) {
        await failYoutubeApiCommentTask(task, commentFetch.lastError);
        taskOutcomes.failed += 1;
        continue;
      }
      commentApiResult = commentFetch.result;
      await saveJsonRaw({
        objectType: "youtube_data_api_comment_threads_json",
        entityType: "content",
        entityId: task.source_content_id,
        source: "youtube_data_api_comment_threads",
        payload: commentApiResult.raw,
        metadata: {
          batch_id: batchId,
          source_content_id: task.source_content_id,
          status: commentApiResult.status,
        },
      });
    }
    const candidates = await query(
      `SELECT candidate.*,run.started_at AS crawl_started_at,
              known.content_key AS known_content_key,
              known.content_type AS known_content_type,
              known.content_type_source AS known_content_type_source,
              run.result_json#>>'{publication_repair,content_max_age_days}'
                AS repair_content_max_age_days
       FROM crawler.content_candidates candidate
       JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
       LEFT JOIN crawler.contents known
         ON known.channel_id=candidate.channel_id
        AND known.source_content_id=candidate.source_content_id
       WHERE candidate.candidate_id=ANY($1::bigint[])`,
      [task.candidate_ids],
    );
    const taskMissing = new Set();
    for (const candidate of candidates.rows) {
      const resultJson = candidate.result_json ?? {};
      const existingDetail = resultJson.detail ?? {};
      const mergedDetail = mergeApiDetail(
        mergeApiDetail(existingDetail, apiDetail),
        commentApiResult?.detail ?? {},
      );
      const classification = resolveYoutubeContentType({
        videoId: candidate.source_content_id,
        upload: resultJson.flat ?? {
          video_id: candidate.source_content_id,
          content_type: candidate.content_type,
          type_source: candidate.type_source,
        },
        detail: mergedDetail,
      });
      const confirmedClassification = classification?.authoritative === true ? classification : null;
      const contentType = confirmedClassification?.content_type ?? null;
      const typeSource = confirmedClassification?.source ?? null;
      const apiAccess = accessFromDetail(mergedDetail, "unknown");
      const previousAccess = resultJson.access;
      const resolvedAccess = apiAccess.access_status !== "unknown"
        ? apiAccess
        : previousAccess?.access_status && previousAccess.access_status !== "unknown"
          ? previousAccess
          : apiAccess;
      const normalized = normalizeResolvedDetail(mergedDetail, { afterApi: true, access: resolvedAccess });
      const storageAction = fullVideoStorageAction({
        candidate,
        classification,
        access: normalized.access,
      });
      const state = {
        ...resultJson,
        detail: normalized.detail,
        classification,
        access: normalized.access,
        api_detail: apiDetail,
        api_comments: commentApiResult
          ? {
              status: commentApiResult.status,
              source: "youtube_data_api_comment_threads",
            }
          : null,
      };
      const apiLiveReason = unfinishedLiveReason(normalized.detail);
      if (apiLiveReason) {
        await excludeUnfinishedLiveCandidate(
          candidate,
          normalized.detail,
          "youtube_data_api",
          apiLiveReason,
        );
        affectedRuns.set(candidate.run_id, candidate.channel_id);
        continue;
      }
      if (storageAction.kind === "update_access") {
        const stored = await updateExistingContentAccessFromCandidate(candidate, normalized);
        const terminalMissing = terminalApiMissingFields(
          missingApiFields(normalized.detail),
          normalized.access,
        );
        terminalMissing.forEach((field) => taskMissing.add(field));
        await query(
          `UPDATE crawler.content_candidates
           SET content_type=$2,type_status='resolved',type_source=$3,content_key=$4,
               detail_status='done',api_status='not_needed',missing_fields=$5::text[],
               result_json=$6::jsonb,error_message=NULL,finished_at=now(),updated_at=now()
           WHERE candidate_id=$1`,
          [
            candidate.candidate_id,
            stored?.content_type ?? candidate.known_content_type,
            stored?.content_type_source ?? candidate.known_content_type_source,
            stored?.content_key ?? candidate.known_content_key,
            terminalMissing,
            JSON.stringify({ ...state, preserved_content_type: true }),
          ],
        );
        await persistFullVideoDisposition(candidate, {
          storageAction,
          classification,
          access: normalized.access,
          detail: normalized.detail,
        });
      } else if (storageAction.kind === "classified_only") {
        const terminalMissing = terminalApiMissingFields(
          missingApiFields(normalized.detail),
          normalized.access,
        );
        terminalMissing.forEach((field) => taskMissing.add(field));
        await query(
          `UPDATE crawler.content_candidates
           SET content_type=$2,type_status='resolved',type_source=$3,content_key=NULL,
               detail_status='unavailable',api_status='unavailable',missing_fields=$4::text[],
               result_json=$5::jsonb,error_message=$6,finished_at=now(),updated_at=now()
           WHERE candidate_id=$1`,
          [
            candidate.candidate_id,
            contentType,
            typeSource,
            terminalMissing,
            JSON.stringify({ ...state, classified_only: true }),
            `content access ${normalized.access.access_status || "unknown"} after detail and api`,
          ],
        );
        await persistFullVideoDisposition(candidate, {
          storageAction,
          classification,
          access: normalized.access,
          detail: normalized.detail,
        });
      } else if (!contentType) {
        const terminalMissing = [...new Set([
          "content_type",
          ...terminalApiMissingFields(missingApiFields(normalized.detail), normalized.access),
        ])];
        terminalMissing.forEach((field) => taskMissing.add(field));
        await query(
          `UPDATE crawler.content_candidates
           SET type_status='unavailable',detail_status='unavailable',api_status='unavailable',
               missing_fields=$2::text[],result_json=$3::jsonb,error_message='type unresolved after detail and api',
               finished_at=now(),updated_at=now()
           WHERE candidate_id=$1`,
          [candidate.candidate_id, terminalMissing, JSON.stringify(state)],
        );
        await persistFullVideoDisposition(candidate, {
          storageAction,
          classification,
          access: normalized.access,
          detail: normalized.detail,
        });
      } else {
        const terminalMissing = terminalApiMissingFields(
          missingApiFields(normalized.detail),
          normalized.access,
        );
        terminalMissing.forEach((field) => taskMissing.add(field));
        const apiReferenceAt = candidate.crawl_started_at ?? Date.now();
        const contentMaxAgeDays = intValue(
          candidate.repair_content_max_age_days,
          crawlSettings.contentMaxAgeDays,
          0,
          3650,
        );
        const apiWindow = classifyContentWindow(
          normalized.detail,
          contentMaxAgeDays,
          apiReferenceAt,
        );
        if (contentMaxAgeDays > 0 && apiWindow.relation === "outside") {
          await excludeCandidateByAge(
            candidate,
            normalized.detail,
            detailAgeDays(normalized.detail, apiReferenceAt),
            contentMaxAgeDays,
            "youtube_data_api",
            apiWindow,
          );
          affectedRuns.set(candidate.run_id, candidate.channel_id);
          continue;
        }
        const contentKey = await upsertContentFromCandidate({
          ...candidate,
          content_type: contentType,
          type_source: typeSource,
          type_authoritative: classification?.authoritative === true,
        }, normalized);
        await query(
          `UPDATE crawler.content_candidates
           SET content_type=$2,type_status='resolved',type_source=$3,content_key=$4,
               detail_status='done',api_status=$5,missing_fields=$6::text[],
               result_json=$7::jsonb,error_message=NULL,finished_at=now(),updated_at=now()
           WHERE candidate_id=$1`,
          [
            candidate.candidate_id,
            contentType,
            typeSource,
            contentKey,
            terminalMissing.length > 0 ? "unavailable" : "done",
            terminalMissing,
            JSON.stringify(state),
          ],
        );
        await persistFullVideoDisposition(candidate, {
          storageAction,
          classification,
          access: normalized.access,
          detail: normalized.detail,
        });
      }
      affectedRuns.set(candidate.run_id, candidate.channel_id);
    }
    const taskStatus = apiResult.detailsById.has(task.source_content_id) && taskMissing.size === 0
      ? "done"
      : "unavailable";
    taskOutcomes[taskStatus] += 1;
    await query(
      `UPDATE crawler.youtube_api_tasks
       SET status=$2,missing_fields=$3::text[],result_json=$4::jsonb,
           error_message=NULL,next_retry_at=NULL,finished_at=now(),updated_at=now()
       WHERE task_id=$1`,
      [
        task.task_id,
        taskStatus,
        [...taskMissing],
        JSON.stringify({
          ...youtubeApiTaskResultEvidence({
          apiDetail,
          detailReturned: apiResult.detailsById.has(task.source_content_id),
          commentApiResult,
          }),
          ...(storedReplay
            ? {
                stored_data_api_evidence_recovery: {
                  ...(task.result_json?.stored_data_api_evidence_recovery ?? {}),
                  replay_applied: true,
                  replay_batch_id: batchId,
                },
              }
            : {}),
        }),
      ],
    );
  }
  const batchStatus = taskOutcomes.failed > 0 ? "failed" : "done";
  const batchError = taskOutcomes.failed > 0
    ? `${taskOutcomes.failed} YouTube API task(s) require retry`
    : null;
  await query(
    `UPDATE crawler.youtube_api_batches
     SET status=$2,key_index=$3,result_json=$4::jsonb,error_message=$5,finished_at=now(),updated_at=now()
     WHERE batch_id=$1`,
    [
      batchId,
      batchStatus,
      keyIndex,
      JSON.stringify({
        requested_count: videoIds.length,
        returned_count: apiResult.returnedCount,
        request_attempts: requestAttempts,
        task_outcomes: taskOutcomes,
        ...(storedReplay ? { stored_evidence_replay: storedReplay.replay } : {}),
      }),
      batchError,
    ],
  );
  for (const [runId, channelId] of affectedRuns) {
    const summary = await updateRunDetailStatus(runId);
    if (migrationActivityCanFinalize(summary.migration_activity_gate)) {
      await queueFinalize(channelId, runId, "youtube-data-api-complete");
    }
  }
  return {
    ok: true,
    batch_id: batchId,
    requested_count: videoIds.length,
    returned_count: apiResult.returnedCount,
    request_attempts: requestAttempts,
    key_index: keyIndex,
    task_outcomes: taskOutcomes,
    ...(storedReplay ? { stored_evidence_replay: storedReplay.replay } : {}),
  };
}

async function loadFirstPartyIdentityByChannel(channelIds) {
  const ids = [...new Set((channelIds ?? []).map(String).filter(Boolean))];
  if (ids.length === 0) return new Map();
  const channelRows = await query(
    `SELECT channel_id, title, handle, summary, about_description
     FROM crawler.channels
     WHERE channel_id=ANY($1::text[])`,
    [ids],
  );
  const contentRows = await query(
    `SELECT channel_id, source_content_id, title, description, published_at, comments_first_page
     FROM (
       SELECT channel_id, source_content_id, title, description, published_at, comments_first_page,
              row_number() OVER (
                PARTITION BY channel_id
                ORDER BY published_at DESC NULLS LAST, last_seen_at DESC
              ) AS rn
       FROM crawler.contents
       WHERE channel_id=ANY($1::text[])
         AND content_type IN ('video', 'short', 'live')
     ) ranked
     WHERE rn <= 8`,
    [ids],
  );
  const videosByChannel = new Map();
  for (const row of contentRows.rows) {
    const list = videosByChannel.get(row.channel_id) ?? [];
    list.push(row);
    videosByChannel.set(row.channel_id, list);
  }
  return new Map(channelRows.rows.map((row) => [
    row.channel_id,
    {
      title: row.title,
      handle: row.handle,
      summary: row.summary,
      about_description: row.about_description,
      videos: videosByChannel.get(row.channel_id) ?? [],
    },
  ]));
}

export async function processAgentBatchV2(job) {
  let channelIds = [...new Set((job.data?.channel_ids ?? []).map(String).filter(Boolean))];
  if (channelIds.length === 0) return { ok: true, skipped: true, reason: "no channels" };
  const incrementalAgent = isIncrementalAgentJob(job);
  const requestedConfigId = Number(job.data?.agent_config_id);
  const requestedConfig = Number.isFinite(requestedConfigId) && requestedConfigId > 0
    ? await getAgentConfigById(requestedConfigId)
    : null;
  const localConfig = await getLocalOfflineAgentConfig();
  const selectedConfig = selectAgentConfigForJob({ job, requestedConfig, localConfig });
  const localExecution = isLocalOfflineAgentConfig(selectedConfig);
  const llm = localExecution
    ? { apiKeys: [], baseUrl: null }
    : await getAgentLlmSettings(selectedConfig);
  const agentConfig = {
    ...selectedConfig,
    endpoint: selectedConfig.endpoint || llm.baseUrl || null,
  };
  const config = agentConfig;
  const incrementalClaim = incrementalAgent
    ? await incrementalAgentResultStore.claim(job)
    : null;
  if (incrementalAgent && incrementalClaim.requests.length === 0) {
    return { ok: true, skipped: true, reason: "incremental requests already settled" };
  }
  if (incrementalAgent) {
    channelIds = incrementalClaim.requests.map((request) => request.channel_id);
  }
  const requestedRows = await query(
    `SELECT channel_id,channel_url,handle,title,summary,about_description,
            country,country_source,country_code,country_canonical_name,
            agent_status,latest_run_id
     FROM crawler.channels
     WHERE channel_id=ANY($1::text[]) AND status='active'
     ORDER BY array_position($1::text[],channel_id)`,
    [channelIds],
  );
  const completedRows = incrementalAgent
    ? []
    : requestedRows.rows.filter((row) => row.agent_status === "done");
  for (const row of completedRows) {
    await queueFinalize(row.channel_id, row.latest_run_id, "agent-already-complete");
  }
  const rows = {
    rows: incrementalAgent
      ? requestedRows.rows
      : requestedRows.rows.filter((row) => row.agent_status !== "done"),
  };
  if (rows.rows.length === 0) return { ok: true, skipped: true, reason: "already complete" };
  await markAgentChannelsRunning({
    withTransaction,
    channelIds: rows.rows.map((row) => row.channel_id),
    forceRefresh: incrementalAgent,
  });
  let channels = [];
  if (!localExecution) {
    const identityByChannel = await loadFirstPartyIdentityByChannel(
      rows.rows.map((row) => row.channel_id),
    );
    channels = rows.rows.map((row) => ({
      channel_id: row.channel_id,
      input_url: normalChannelUrl(row),
      country: normalizeCrawlerCountry(row.country),
      country_source: row.country_source,
      country_code: row.country_code,
      country_canonical_name: row.country_canonical_name,
      country_required: countryRequiresAgent(row),
      first_party_identity: identityByChannel.get(row.channel_id) ?? {
        title: row.title,
        handle: row.handle,
        summary: row.summary,
        about_description: row.about_description,
        videos: [],
      },
    }));
  }
  const channelById = new Map((localExecution ? rows.rows.map((row) => ({
    channel_id: row.channel_id,
    input_url: normalChannelUrl(row),
    country_required: false,
  })) : channels).map((item) => [item.channel_id, item]));
  let result;
  try {
    result = localExecution
      ? await localOfflineProfileExecutor.execute({
        config,
        channelIds: rows.rows.map((row) => row.channel_id),
      })
      : await runAgentBatch({ config, apiKeys: llm.apiKeys, channels });
  } catch (error) {
    if (incrementalAgent) {
      for (const request of incrementalClaim.requests) {
        await incrementalAgentResultStore.fail({
          batchId: incrementalClaim.batchId,
          request,
          row: channelById.get(request.channel_id) ?? {
            input_url: `https://www.youtube.com/channel/${encodeURIComponent(request.channel_id)}`,
            country_required: true,
          },
          agentConfig,
          error,
        });
      }
    }
    throw error;
  }
  const errorByChannel = new Map(result.errors.map((item) => [item.channel_id, item.error]));
  const requestByChannel = new Map(
    (incrementalClaim?.requests ?? []).map((request) => [request.channel_id, request]),
  );
  for (const row of rows.rows) {
    const resolved = result.results.get(row.channel_id);
    if (resolved) {
      if (incrementalAgent) {
        await incrementalAgentResultStore.complete({
          batchId: incrementalClaim.batchId,
          request: requestByChannel.get(row.channel_id),
          resolved,
          row: channelById.get(row.channel_id),
          agentConfig,
        });
      } else {
        const promptVariant = resolved.execution_variant === "local_offline"
          ? "local_offline"
          : resolved.country_required ? "country_required" : "country_resolved";
        const publicationRun = buildAgentPublicationRun({
          agentConfig,
          agentModel: resolved.agent_model ?? agentConfig.model,
          promptVariant,
          executionVariant: resolved.execution_variant ?? null,
          runtimeIdentity: resolved.execution_variant === "local_offline"
            ? resolved.metrics?.profile_processing_context
            : null,
          inputContentIds: resolved.input_content_ids ?? [],
          taxonomyVersion: process.env.AGENT_TAXONOMY_VERSION || AGENT_TAXONOMY_VERSION,
        });
        await persistAgentChannelSuccess({
          withTransaction,
          channelId: row.channel_id,
          inputUrl: normalChannelUrl(row),
          metrics: resolved.metrics,
          publicationRun,
        });
        await queueFinalize(
          row.channel_id,
          resolved.source_latest_run_id ?? row.latest_run_id,
          "agent-complete",
        );
      }
    } else {
      const error = errorByChannel.get(row.channel_id) ?? "agent returned no result";
      if (incrementalAgent) {
        await incrementalAgentResultStore.fail({
          batchId: incrementalClaim.batchId,
          request: requestByChannel.get(row.channel_id),
          row: channelById.get(row.channel_id),
          agentConfig,
          error,
        });
      } else {
        await persistAgentChannelFailure({
          withTransaction,
          channelId: row.channel_id,
          inputUrl: normalChannelUrl(row),
          agentModel: agentConfig.model ?? null,
          agentConfigId: agentConfig.config_id,
          promptTemplateId: localExecution ? null : agentConfig.prompt_template_id ?? null,
          promptHash: localExecution ? null : agentConfig.prompt_hash ?? null,
          promptVariant: localExecution
            ? "local_offline"
            : hasResolvedCrawlerCountry(row) ? "country_resolved" : "country_required",
          errorMessage: error,
        });
      }
    }
  }
  if (result.errors.length > 0 && !incrementalAgent) {
    throw new Error(`agent failed for ${result.errors.length}/${rows.rows.length} channels`);
  }
  return {
    ok: result.errors.length === 0,
    partial: incrementalAgent && result.errors.length > 0,
    incremental: incrementalAgent,
    channel_count: rows.rows.length,
    country_required_count: localExecution
      ? 0
      : channels.filter((item) => item.country_required).length,
    country_resolved_count: localExecution
      ? rows.rows.length
      : channels.filter((item) => !item.country_required).length,
    applied_count: result.results.size,
    agent_config_id: agentConfig.config_id,
    agent_config_name: agentConfig.name,
  };
}

function contentForProfile(row) {
  return {
    content_id: row.source_content_id,
    video_id: row.source_content_id,
    content_type: row.content_type,
    type: row.content_type === "short" ? "shorts" : row.content_type === "live" ? "lives" : "videos",
    position: row.position,
    title: row.title,
    url: row.url,
    description: row.description,
    description_status: row.description_status,
    description_source: row.description_source,
    hashtags: row.hashtags,
    keywords: row.keywords,
    published_at: row.published_at,
    published_at_status: row.published_at_status,
    published_at_precision: row.published_at_precision,
    published_at_source: row.published_at_source,
    duration_seconds: row.duration_seconds,
    length_text: row.length_text,
    duration_status: row.duration_status,
    duration_source: row.duration_source,
    view_count_text: row.view_count_text,
    view_count_status: row.view_count_status,
    view_count_source: row.view_count_source,
    like_count: row.like_count,
    like_count_status: row.like_count_status,
    like_count_source: row.like_count_source,
    comment_count: row.comment_count,
    comment_count_status: row.comment_count_status,
    comments_disabled: row.comments_disabled,
    comment_count_source: row.comment_count_source,
    is_members_only: row.is_members_only,
    access_status: row.access_status,
    access_status_source: row.access_status_source,
    live_scheduled_at: row.live_scheduled_at,
    live_started_at: row.live_started_at,
    live_ended_at: row.live_ended_at,
    extractor_version: row.extractor_version,
  };
}

function groupContents(rows) {
  const grouped = { videos: [], shorts: [], lives: [] };
  for (const row of rows) {
    const item = contentForProfile(row);
    if (row.content_type === "short") grouped.shorts.push(item);
    else if (row.content_type === "live") grouped.lives.push(item);
    else grouped.videos.push(item);
  }
  return grouped;
}

function countNames(items) {
  const counts = {};
  for (const item of items) counts[item] = (counts[item] ?? 0) + 1;
  return counts;
}

function crawlerCountryMetric(channel) {
  if (!hasResolvedCrawlerCountry(channel)) return null;
  return {
    value: normalizeCrawlerCountry(channel.country),
    country_code: channel.country_code
      ?? canonicalizeCrawlerCountry(channel.country).code,
    canonical_name: channel.country_canonical_name
      ?? canonicalizeCrawlerCountry(channel.country).name,
    reason: null,
    source: "crawler",
    evidence: ["YouTube About country field"],
    confidence: "high",
    source_urls: [normalChannelUrl(channel)],
  };
}

export async function processFinalizeV2(job) {
  const channelId = text(job.data?.channel_id);
  if (!channelId) throw new Error("channel_id is required");
  const channelRows = await query("SELECT * FROM crawler.channels WHERE channel_id=$1 LIMIT 1", [channelId]);
  let channel = channelRows.rows[0];
  if (!channel) throw new Error(`channel not found: ${channelId}`);
  if (!isFinalizableChannelStatus(channel.status)) {
    return {
      ok: true,
      skipped: true,
      skip_reason: channel.status === "removed" ? "channel_removed" : "channel_inactive",
      channel_id: channelId,
      requested_run_id: text(job.data?.run_id) ?? channel.latest_run_id ?? null,
    };
  }
  const runId = text(job.data?.run_id) ?? channel.latest_run_id;
  if (!isCurrentChannelRun(channel, runId)) {
    return {
      ok: true,
      skipped: true,
      skip_reason: "stale_run",
      channel_id: channelId,
      requested_run_id: runId,
      latest_run_id: channel.latest_run_id ?? null,
    };
  }
  const runRows = runId ? await query("SELECT * FROM crawler.channel_runs WHERE run_id=$1 LIMIT 1", [runId]) : { rows: [] };
  const run = runRows.rows[0] ?? null;
  const candidates = runId
    ? await query("SELECT * FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position", [runId])
    : { rows: [] };
  const contents = runId
    ? await query(
        `SELECT * FROM crawler.contents WHERE channel_id=$1 AND run_id=$2 ORDER BY position ASC NULLS LAST`,
        [channelId, runId],
      )
    : { rows: [] };
  let agent = await query(
    `SELECT * FROM crawler.agent_profiles
     WHERE channel_id=$1 AND agent_mode='basic' AND status='success' LIMIT 1`,
    [channelId],
  );
  let agentProfile = agent.rows[0] ?? null;
  const defaultPublicationAsOf = agentProfile?.updated_at
    ?? run?.updated_at
    ?? run?.started_at
    ?? new Date();
  const publicationContext = finalizePublicationContext({
    job,
    run,
    runId,
    defaultAsOf: defaultPublicationAsOf,
  });
  const publicationAsOf = publicationContext.asOf;
  const publicationRevisionType = publicationContext.revisionType;
  const initialObservations = await recordInitialFullObservations({
    withTransaction,
    channelId,
    runId,
    observedAt: publicationAsOf,
    revisionType: publicationRevisionType,
    repairId: publicationContext.repairId,
  });
  [channel, agent] = await Promise.all([
    query("SELECT * FROM crawler.channels WHERE channel_id=$1 LIMIT 1", [channelId])
      .then((result) => result.rows[0]),
    query(
      `SELECT * FROM crawler.agent_profiles
       WHERE channel_id=$1 AND agent_mode='basic' AND status='success' LIMIT 1`,
      [channelId],
    ),
  ]);
  if (!channel) throw new Error(`channel disappeared during finalize: ${channelId}`);
  agentProfile = agent.rows[0] ?? null;
  const sourceRevision = finalizeSourceRevision({
    channel,
    run: runRows.rows[0] ?? null,
    candidates: candidates.rows,
    contents: contents.rows,
    agent: agentProfile,
  });
  const existingFinalized = await query(
    "SELECT run_id,status,quality_json FROM crawler.finalized_profiles WHERE channel_id=$1 LIMIT 1",
    [channelId],
  );
  if (finalizedProfileIsCurrent(
    existingFinalized.rows[0],
    runId,
    sourceRevision,
    initialObservations.outcomes,
  )) {
    const committed = await withTransaction((client) => commitFinalizedProfile(client, {
      channelId,
      runId,
      status: existingFinalized.rows[0].status,
      deduplicated: true,
      publicationAsOf,
      publicationRevisionType,
    }));
    return {
      ok: true,
      deduplicated: true,
      channel_id: channelId,
      run_id: runId,
      status: existingFinalized.rows[0].status,
      source_revision: sourceRevision,
      publication_status: committed.publication?.status ?? null,
    };
  }

  const openDetail = candidates.rows.filter((item) => ["queued", "running", "failed"].includes(item.detail_status));
  const openApi = candidates.rows.filter((item) => ["pending", "queued", "running", "failed"].includes(item.api_status));
  const unavailable = candidates.rows.filter((item) => item.detail_status === "unavailable");
  const ageExcluded = candidates.rows.filter((item) => item.result_json?.scope?.status === "excluded");
  const missingFields = candidates.rows.flatMap((item) => item.missing_fields ?? []);
  const missingChannelFields = [];
  if (!channel.title) missingChannelFields.push("title");
  if (channel.subscriber_count == null && !channel.subscriber_count_text) missingChannelFields.push("subscriber_count");
  const agentFields = agent.rows[0]?.metrics_json?.audience_profile_agent ?? {};
  const requiredAgentFields = [
    "creator_gender", "creator_age_range", "creator_language", "audience_region",
    "audience_age_gender", "audience_language",
    "active_subscriber_ratio", "channel_tags", "channel_categories",
  ];
  if (!hasResolvedCrawlerCountry(channel)) requiredAgentFields.unshift("country");
  const missingAgentFields = requiredAgentFields.filter((field) => !agentFields[field]);
  const initialObservationOutcomes = initialObservations.outcomes ?? {};
  const incompleteSourceObservationKinds = ["about", "video", "agent"]
    .filter((kind) => initialObservationOutcomes[kind] !== "complete");
  const status = resolveFinalizeStatus({
    channelStatus: channel.status,
    openDetailCount: openDetail.length,
    openApiCount: openApi.length,
    hasAgent: agent.rows.length > 0,
    missingAgentFieldCount: missingAgentFields.length,
    missingChannelFieldCount: missingChannelFields.length,
    missingContentFieldCount: missingFields.length,
    unavailableCount: unavailable.length,
    incompleteSourceObservationCount: incompleteSourceObservationKinds.length,
  });

  const existingFinalizedRow = existingFinalized.rows[0] ?? null;
  if (!finalizeStatusCanAdvance(
    existingFinalizedRow?.status,
    status,
    String(existingFinalizedRow?.run_id ?? "") === String(runId ?? ""),
  )) {
    await withTransaction((client) => synchronizeFinalizedRun(client, {
      runId,
      finalizedStatus: existingFinalizedRow.status,
    }));
    return {
      ok: true,
      skipped: true,
      skip_reason: "finalize_status_regression",
      channel_id: channelId,
      run_id: runId,
      retained_status: existingFinalizedRow.status,
      attempted_status: status,
    };
  }

  const metrics = structuredClone(agent.rows[0]?.metrics_json ?? {});
  const countryMetric = crawlerCountryMetric(channel);
  if (countryMetric) {
    metrics.audience_profile_agent = { ...(metrics.audience_profile_agent ?? {}), country: countryMetric };
  }
  const groupedContents = groupContents(contents.rows);
  const quality = {
    review_status: "unreviewed",
    publish_status: "not_published",
    source_revision: sourceRevision,
    quality_status: status,
    publish_ready: status === "ready_auto" && missingChannelFields.length === 0 && missingFields.length === 0 && unavailable.length === 0,
    data_complete: status === "ready_auto" && missingChannelFields.length === 0 && missingFields.length === 0 && unavailable.length === 0,
    expected_content_count: Number(run?.expected_content_count ?? candidates.rows.length),
    candidate_count: candidates.rows.length,
    classified_content_count: contents.rows.length,
    unavailable_candidate_count: unavailable.length,
    age_excluded_candidate_count: ageExcluded.length,
    detail_open_count: openDetail.length,
    api_open_count: openApi.length,
    missing_channel_fields: missingChannelFields,
    missing_content_fields: countNames(missingFields),
    missing_agent_fields: missingAgentFields,
    initial_observations: {
      recorded: initialObservations.recorded,
      reason: initialObservations.reason,
      kinds: Object.keys(initialObservations.observations ?? {}),
      outcomes: initialObservationOutcomes,
      incomplete_kinds: incompleteSourceObservationKinds,
    },
    missing_items: candidates.rows
      .filter((item) => (item.missing_fields?.length ?? 0) > 0 || item.detail_status === "failed")
      .map((item) => ({
        video_id: item.source_content_id,
        position: item.position,
        fields: item.missing_fields,
        detail_status: item.detail_status,
        api_status: item.api_status,
        error: item.error_message,
      })),
    quality_checked_at: new Date().toISOString(),
  };
  const profile = {
    channel,
    contents: groupedContents,
    metrics,
    agent_profile: agent.rows[0] ?? null,
    quality,
    finalized_reason: job.data?.reason ?? null,
  };
  let rawObject = null;
  if (isSuccessfulPublicationFinalize(status)) {
    rawObject = await saveJsonRaw({
      objectType: "youtube_final_profile_json",
      entityType: "channel",
      entityId: channelId,
      source: "finalize_v2",
      payload: profile,
      metadata: { channel_id: channelId, run_id: runId, status },
    });
  }
  const committed = await withTransaction((client) => commitFinalizedProfile(client, {
    channelId,
    runId,
    status,
    profile,
    quality: { ...quality, raw_object_path: rawObject?.object_path ?? null },
    publicationAsOf,
    publicationRevisionType,
  }));
  if (!committed.applied) {
    return {
      ok: true,
      skipped: true,
      skip_reason: committed.skip_reason,
      channel_id: channelId,
      requested_run_id: runId,
      latest_run_id: committed.latest_run_id,
      retained_status: committed.retained_status,
    };
  }
  return {
    ok: true,
    channel_id: channelId,
    run_id: runId,
    status,
    source_revision: sourceRevision,
    candidate_count: candidates.rows.length,
    content_count: contents.rows.length,
    publication_status: committed.publication?.status ?? null,
  };
}
