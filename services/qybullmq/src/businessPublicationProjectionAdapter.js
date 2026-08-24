import { observationFactsHash } from "./crawlObservationStore.js";

export const BUSINESS_PROJECTION_ADAPTER_VERSION = "business-publication-projection-v4";
export const BUSINESS_PROJECTION_METRIC_VERSION = "publication-current-metric-v1";

const CONTENT_KIND = Object.freeze({
  video: "videos",
  short: "shorts",
  live: "lives",
});
const FACT_KEYS = Object.freeze([
  "country",
  "creator_language",
  "creator_gender",
  "creator_age_range",
  "audience_region",
  "audience_language",
  "audience_age_gender",
  "active_subscriber_ratio",
  "channel_categories",
  "channel_tags",
]);
const SCOPES = Object.freeze(["all", "last30d", "last90d", "lives", "shorts", "videos"]);
const PLACEHOLDER_METRICS = Object.freeze([
  "subscriber_growth",
  "new_followers_yesterday",
  "view_growth",
  "active_fan_trend",
  "trend_series",
]);

export class BusinessPublicationProjectionContractError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = "BusinessPublicationProjectionContractError";
    this.code = code;
    this.details = details;
  }
}

function fail(code, message, details) {
  throw new BusinessPublicationProjectionContractError(code, message, details);
}

function object(value, field, { nullable = false } = {}) {
  if (nullable && value == null) return null;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    fail("projection_payload_invalid", `${field} must be an object`);
  }
  return value;
}

function text(value, field, { nullable = false } = {}) {
  if (nullable && value == null) return null;
  const output = String(value ?? "").trim();
  if (!output) fail("projection_payload_invalid", `${field} is required`);
  return output;
}

function optionalText(value) {
  if (value == null) return null;
  return String(value).trim() || null;
}

function timestamp(value, field, { nullable = false } = {}) {
  if (nullable && (value == null || value === "")) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    fail("projection_payload_invalid", `${field} must be a timestamp`);
  }
  return parsed.toISOString();
}

function date(value, field, { nullable = false } = {}) {
  if (nullable && (value == null || value === "")) return null;
  const match = String(value ?? "").match(/^(\d{4}-\d{2}-\d{2})/);
  if (!match) fail("projection_payload_invalid", `${field} must be an ISO date`);
  return match[1];
}

function nonnegativeInteger(value, field, { nullable = false } = {}) {
  if (nullable && (value == null || value === "")) return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    fail("projection_payload_invalid", `${field} must be a non-negative safe integer`);
  }
  return parsed;
}

function finiteNonnegative(value, field, { nullable = false } = {}) {
  if (nullable && (value == null || value === "")) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    fail("projection_payload_invalid", `${field} must be a non-negative number`);
  }
  return parsed;
}

function optionalBoolean(value, field) {
  if (value == null) return null;
  if (typeof value !== "boolean") {
    fail("projection_payload_invalid", `${field} must be boolean or null`);
  }
  return value;
}

function youtubeBusinessEmailState(value, field) {
  const source = object(value, field);
  const hasAvailable = Object.hasOwn(source, "youtube_business_email_available");
  const hasObservedAt = Object.hasOwn(source, "youtube_business_email_observed_at");
  if (!hasAvailable && !hasObservedAt) {
    return { available: null, observedAt: null };
  }
  if (hasAvailable !== hasObservedAt) {
    fail(
      "projection_payload_invalid",
      `${field} business email availability and observation time must both be present`,
    );
  }
  const available = optionalBoolean(
    source.youtube_business_email_available,
    `${field}.youtube_business_email_available`,
  );
  const observedAt = timestamp(
    source.youtube_business_email_observed_at,
    `${field}.youtube_business_email_observed_at`,
    { nullable: true },
  );
  if ((available === null) !== (observedAt === null)) {
    fail(
      "projection_payload_invalid",
      `${field} business email availability and observation time must both be known or unknown`,
    );
  }
  return { available, observedAt };
}

function list(value, field) {
  if (!Array.isArray(value)) fail("projection_payload_invalid", `${field} must be an array`);
  return value;
}

function identifier(prefix, ...parts) {
  const digest = observationFactsHash(parts).slice("sha256:".length, "sha256:".length + 32);
  return `${prefix}_${digest}`;
}

function normalizedMetricStatus(statusValue, value) {
  if (value == null) return "unavailable";
  const status = optionalText(statusValue);
  if (status === "exact") return "exact";
  if (status === "estimated" || status === "approximate" || status === "recovered") {
    return "approximate";
  }
  fail("projection_payload_invalid", `unsupported Channel metric status: ${status}`);
}

function verifiedValue(payload) {
  const status = optionalText(payload.is_verified_status);
  const value = optionalBoolean(payload.is_verified, "channel.is_verified");
  if (status === "verified" && value === true) return { value: true, status: "verified" };
  if (new Set(["not_verified", "observed_false"]).has(status) && value === false) {
    return { value: false, status: "not_verified" };
  }
  if (status === "unknown" && value == null) return { value: null, status: "unknown" };
  fail("projection_payload_invalid", "Channel verification value and status disagree");
}

function channelSnapshotFromCurrent({ row, batchId, snapshotId, capturedAt, versionVector }) {
  const payload = object(row.payload_json, "result.entity_current.payload_json");
  const channelId = text(payload.channel_id, "channel.channel_id");
  const sourceObservedAt = timestamp(
    row.source_observed_at,
    "result.entity_current.source_observed_at",
  );
  const verified = verifiedValue(payload);
  const businessEmail = youtubeBusinessEmailState(payload, "channel");
  const subscriberCount = nonnegativeInteger(
    payload.subscriber_count,
    "channel.subscriber_count",
    { nullable: true },
  );
  const totalViewCount = nonnegativeInteger(
    payload.total_view_count,
    "channel.total_view_count",
    { nullable: true },
  );
  const videoCount = nonnegativeInteger(
    payload.total_video_count,
    "channel.total_video_count",
    { nullable: true },
  );
  const joinedDate = date(payload.joined_date, "channel.joined_date", { nullable: true });
  const avatar = list(payload.avatar, "channel.avatar")
    .map((item) => object(item, "channel.avatar item"))
    .sort((left, right) => Number(left.position ?? 0) - Number(right.position ?? 0));
  const joinedStatus = joinedDate == null
    ? "unavailable"
    : payload.joined_date_status === "exact" ? "exact" : "approximate";
  return {
    id: snapshotId,
    channel_id: channelId,
    import_batch_id: batchId,
    captured_at: capturedAt,
    channel_observed_at: sourceObservedAt,
    title: text(payload.title, "channel.title"),
    handle: optionalText(payload.handle),
    country_text: optionalText(payload.country_name ?? payload.country_code),
    avatar_url: optionalText(avatar[0]?.url),
    description: optionalText(payload.description),
    is_verified: verified.value,
    is_verified_status: verified.status,
    youtube_business_email_available: businessEmail.available,
    youtube_business_email_observed_at: businessEmail.observedAt,
    subscriber_count_text: subscriberCount == null ? null : String(subscriberCount),
    view_count_text: totalViewCount == null ? null : String(totalViewCount),
    video_count_text: videoCount == null ? null : String(videoCount),
    joined_date_text: optionalText(payload.joined_date_raw),
    subscriber_count: subscriberCount,
    subscriber_count_observed_at: subscriberCount == null ? null : sourceObservedAt,
    total_view_count: totalViewCount,
    total_view_count_observed_at: totalViewCount == null ? null : sourceObservedAt,
    video_count: videoCount,
    video_count_observed_at: videoCount == null ? null : sourceObservedAt,
    joined_date: joinedDate,
    raw_channel: {
      adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
      publication_stream_id: row.publication_stream_id,
      active_revision_id: row.active_revision_id,
      active_sequence: Number(row.active_sequence),
      result_hash: row.result_hash,
      source_observed_at: sourceObservedAt,
      projected_at: capturedAt,
      version_vector: versionVector,
      payload,
    },
    parse_status: {
      adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
      source: "result.entity_current",
      carried_forward: false,
    },
    channel_url: optionalText(payload.canonical_url),
    source_status: optionalText(payload.lifecycle_status ?? row.lifecycle_status),
    source_reject_reason: null,
    source_priority: null,
    source_ready_for_agent: null,
    source_agent_status: null,
    source_latest_run_id: null,
    source_created_at: timestamp(row.created_at ?? row.activated_at, "entity created_at"),
    source_updated_at: timestamp(row.updated_at ?? row.activated_at, "entity updated_at"),
    subscriber_count_status: normalizedMetricStatus(
      payload.subscriber_count_status,
      subscriberCount,
    ),
    total_view_count_status: normalizedMetricStatus(
      payload.total_view_count_status,
      totalViewCount,
    ),
    video_count_status: normalizedMetricStatus(payload.total_video_count_status, videoCount),
    joined_date_status: joinedStatus,
    candidate_last_published_at: null,
    candidate_last_published_status: null,
    candidate_last_published_source: null,
    candidate_last_published_date: null,
  };
}

function channelSnapshotFromPrevious({ row, batchId, snapshotId, capturedAt, versionVector }) {
  const previous = object(row, "previous channel snapshot");
  const businessEmail = youtubeBusinessEmailState(previous, "previous channel snapshot");
  const sourceObservedAt = timestamp(
    previous.channel_observed_at
      ?? previous.raw_channel?.source_observed_at
      ?? previous.captured_at,
    "previous channel source_observed_at",
  );
  return {
    ...previous,
    id: snapshotId,
    import_batch_id: batchId,
    captured_at: capturedAt,
    channel_observed_at: sourceObservedAt,
    youtube_business_email_available: businessEmail.available,
    youtube_business_email_observed_at: businessEmail.observedAt,
    subscriber_count_observed_at: previous.subscriber_count == null
      ? null
      : timestamp(
        previous.subscriber_count_observed_at ?? sourceObservedAt,
        "previous subscriber_count_observed_at",
      ),
    total_view_count_observed_at: previous.total_view_count == null
      ? null
      : timestamp(
        previous.total_view_count_observed_at ?? sourceObservedAt,
        "previous total_view_count_observed_at",
      ),
    video_count_observed_at: previous.video_count == null
      ? null
      : timestamp(
        previous.video_count_observed_at ?? sourceObservedAt,
        "previous video_count_observed_at",
      ),
    raw_channel: {
      adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
      carried_forward_from_snapshot_id: previous.id,
      source_observed_at: sourceObservedAt,
      projected_at: capturedAt,
      version_vector: versionVector,
      previous_raw_channel: previous.raw_channel ?? {},
    },
    parse_status: {
      adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
      source: "active_public_snapshot",
      carried_forward: true,
    },
  };
}

function linksFromCurrent(payload, channelId, snapshotId) {
  return list(payload.links, "channel.links").map((value, index) => {
    const link = object(value, "channel link");
    const url = text(link.target_url, "channel link target_url");
    const linkType = text(link.link_type, "channel link link_type");
    const source = "publication_projection";
    return {
      id: identifier("publication_link", snapshotId, linkType, url, source),
      channel_id: channelId,
      channel_snapshot_id: snapshotId,
      link_type: linkType,
      url,
      title: optionalText(link.title),
      source,
      raw_link: {
        display_url: optionalText(link.display_url),
        favicon_url: optionalText(link.favicon_url),
        position: nonnegativeInteger(link.position ?? index, "channel link position"),
        purpose: optionalText(link.purpose),
      },
    };
  });
}

function linksFromPrevious(rows, channelId, snapshotId) {
  return rows.map((value) => {
    const link = object(value, "previous channel link");
    const source = text(link.source, "previous channel link source");
    const linkType = text(link.link_type, "previous channel link link_type");
    const url = text(link.url, "previous channel link url");
    return {
      ...link,
      id: identifier("publication_link", snapshotId, linkType, url, source),
      channel_id: channelId,
      channel_snapshot_id: snapshotId,
      raw_link: {
        ...(object(link.raw_link ?? {}, "previous channel link raw_link")),
        carried_forward_from_link_id: link.id,
      },
    };
  });
}

function countValue(payload, key, statusKey, observedAtKey) {
  const sourceStatus = optionalText(payload[statusKey]);
  const value = nonnegativeInteger(payload[key], `content.${key}`, { nullable: true });
  if (key === "comment_count" && sourceStatus === "disabled") {
    if (value != null && value !== 0) {
      fail(
        "projection_payload_invalid",
        "disabled Comments require comment_count=0, comment_count_status=disabled, and comments_disabled=true",
      );
    }
    return {
      value: 0,
      status: "exact",
      observedAt: timestamp(payload[observedAtKey], `content.${observedAtKey}`),
    };
  }
  if (value == null) return { value: null, status: "unavailable", observedAt: null };
  if (!sourceStatus) text(payload[statusKey], `content.${statusKey}`);
  let status;
  if (key === "view_count") {
    status = new Set(["exact", "estimated", "recovered"]).has(sourceStatus)
      ? sourceStatus
      : "unavailable";
  } else {
    status = sourceStatus === "stale"
      ? "stale"
      : (sourceStatus === "exact"
          || sourceStatus.startsWith("zero_")
          || (key === "comment_count" && sourceStatus === "disabled"))
        ? "exact"
        : "unavailable";
  }
  if (status === "unavailable") return { value: null, status, observedAt: null };
  const observedAt = timestamp(payload[observedAtKey], `content.${observedAtKey}`);
  return { value, status, observedAt };
}

function contentFromCurrent(row, channelId, snapshotId, index) {
  const payload = object(row.payload_json, "result.content_current.payload_json");
  if (text(payload.content_id, "content.content_id") !== text(row.content_id, "content_id")) {
    fail("projection_identity_mismatch", "Content row and payload identities disagree");
  }
  const contentKind = CONTENT_KIND[text(payload.kind, "content.kind")];
  if (!contentKind) fail("projection_payload_invalid", `unsupported Content kind: ${payload.kind}`);
  const publishedStatus = text(payload.published_at_status, "content.published_at_status");
  const publishedAt = publishedStatus === "exact"
    ? timestamp(payload.published_at, "content.published_at")
    : publishedStatus === "estimated"
      ? timestamp(payload.published_at, "content.published_at", { nullable: true })
      : null;
  const publishedDate = date(payload.published_date, "content.published_date", { nullable: true });
  const view = countValue(payload, "view_count", "view_count_status", "view_count_observed_at");
  const like = countValue(payload, "like_count", "like_count_status", "like_count_observed_at");
  const commentsDisabled = optionalBoolean(
    payload.comments_disabled,
    "content.comments_disabled",
  );
  const comment = countValue(
    payload,
    "comment_count",
    "comment_count_status",
    "comment_count_observed_at",
  );
  const sourceCommentStatus = text(payload.comment_count_status, "content.comment_count_status");
  if ((sourceCommentStatus === "disabled") !== (commentsDisabled === true)
      || (commentsDisabled === true && comment.value !== 0)) {
    fail(
      "projection_payload_invalid",
      "disabled Comments require comment_count=0, comment_count_status=disabled, and comments_disabled=true",
    );
  }
  const duration = nonnegativeInteger(
    payload.duration_seconds,
    "content.duration_seconds",
    { nullable: true },
  );
  const sourceDurationStatus = optionalText(payload.duration_status);
  const durationStatus = duration == null
    ? "unavailable"
    : sourceDurationStatus === "stale" ? "stale" : "exact";
  const descriptionStatus = text(payload.description_status, "content.description_status");
  const position = nonnegativeInteger(
    row.position ?? payload.position ?? index + 1,
    "content.position",
  );
  const contentId = text(payload.content_id, "content.content_id");
  return {
    identity: {
      video_id: contentId,
      channel_id: channelId,
      url: optionalText(payload.url),
      first_seen_at: timestamp(row.created_at ?? row.activated_at, "content created_at"),
      last_seen_at: timestamp(row.updated_at ?? row.activated_at, "content updated_at"),
    },
    snapshot: {
      id: identifier("publication_content", snapshotId, contentId),
      channel_snapshot_id: snapshotId,
      video_id: contentId,
      content_kind: contentKind,
      title: text(payload.title, "content.title"),
      thumbnail_url: optionalText(payload.thumbnail_url),
      published_text: publishedDate,
      published_date: publishedDate,
      view_count_text: view.value == null ? null : String(view.value),
      view_count: view.value,
      like_count: like.value,
      comment_count: comment.value,
      length_text: duration == null ? null : String(duration),
      duration_seconds: duration,
      url: optionalText(payload.url),
      raw_item: {
        adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
        active_revision_id: row.active_revision_id,
        active_sequence: Number(row.active_sequence),
        item_hash: row.item_hash,
        payload,
      },
      source_content_key: optionalText(payload.content_key),
      source_run_id: null,
      source_content_type: text(payload.kind, "content.kind"),
      published_at: publishedAt,
      published_at_status: publishedStatus,
      published_at_source: optionalText(payload.published_at_source),
      is_recent: true,
      is_canonical: true,
      view_count_status: view.status,
      source_first_seen_at: timestamp(row.created_at ?? row.activated_at, "content created_at"),
      source_last_seen_at: timestamp(row.updated_at ?? row.activated_at, "content updated_at"),
      source_last_enriched_at: timestamp(row.activated_at, "content activated_at"),
      parse_status: {
        adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
        source: "result.content_current",
      },
      like_count_status: like.status,
      comment_count_status: comment.status,
      duration_status: durationStatus,
      view_count_observed_at: view.observedAt,
      like_count_observed_at: like.observedAt,
      comment_count_observed_at: comment.observedAt,
      source_url: optionalText(payload.url),
      channel_id: channelId,
      comments_disabled: commentsDisabled,
      is_members_only: Boolean(payload.is_members_only),
      access_status: payload.access_status === "private"
        ? "unavailable"
        : text(payload.access_status, "content.access_status"),
      access_status_source: optionalText(payload.access_status_source),
      source_position: position,
      published_at_precision: optionalText(payload.published_at_precision),
      duration_source: optionalText(payload.duration_source),
      view_count_source: optionalText(payload.view_count_source),
      like_count_source: optionalText(payload.like_count_source),
      comment_count_source: optionalText(payload.comment_count_source),
      live_scheduled_at: timestamp(payload.live_scheduled_at, "live_scheduled_at", { nullable: true }),
      live_started_at: timestamp(payload.live_started_at, "live_started_at", { nullable: true }),
      live_ended_at: timestamp(payload.live_ended_at, "live_ended_at", { nullable: true }),
      extractor_version: optionalText(payload.extractor_version),
      description: payload.description == null ? null : String(payload.description),
      description_status: descriptionStatus,
      description_source: optionalText(payload.description_source),
      hashtags: list(payload.hashtags, "content.hashtags").map(String),
      keywords: list(payload.keywords, "content.keywords").map(String),
    },
  };
}

function contentFromPrevious(rowValue, channelId, snapshotId, index) {
  const row = object(rowValue, "previous content snapshot");
  const contentId = text(row.video_id, "previous content video_id");
  return {
    identity: {
      video_id: contentId,
      channel_id: channelId,
      url: optionalText(row.item_url ?? row.url),
      first_seen_at: timestamp(
        row.item_first_seen_at ?? row.source_first_seen_at ?? row.captured_at,
        "previous content first_seen_at",
      ),
      last_seen_at: timestamp(
        row.item_last_seen_at ?? row.source_last_seen_at ?? row.captured_at,
        "previous content last_seen_at",
      ),
    },
    snapshot: {
      ...row,
      id: identifier("publication_content", snapshotId, contentId),
      channel_snapshot_id: snapshotId,
      channel_id: channelId,
      source_position: row.source_position ?? index + 1,
      raw_item: {
        adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
        carried_forward_from_content_snapshot_id: row.id,
        previous_raw_item: row.raw_item ?? {},
      },
      parse_status: {
        adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
        source: "active_public_snapshot",
        carried_forward: true,
      },
    },
  };
}

function categoryValue(value) {
  if (Array.isArray(value)) return value;
  const input = object(value, "channel_categories value");
  const output = [];
  const levelOne = optionalText(input.level_1);
  if (levelOne) output.push(levelOne);
  for (const item of Array.isArray(input.level_2) ? input.level_2 : []) {
    const normalized = optionalText(item);
    if (normalized && !output.includes(normalized)) output.push(normalized);
  }
  return output;
}

function factsFromCurrent(row, channelId, snapshotId) {
  if (row.is_retracted) return [];
  const payload = object(row.payload_json, "result.agent_current.payload_json");
  const facts = object(payload.facts, "agent.facts");
  return FACT_KEYS.map((fieldKey) => {
    const fact = object(facts[fieldKey], `agent fact ${fieldKey}`);
    const value = fieldKey === "channel_categories" ? categoryValue(fact.value) : fact.value;
    if (value === undefined) fail("projection_payload_invalid", `agent fact ${fieldKey} has no value`);
    return {
      id: identifier("publication_fact", snapshotId, fieldKey),
      channel_id: channelId,
      channel_snapshot_id: snapshotId,
      field_key: fieldKey,
      value_json: value,
      source: text(fact.source, `agent fact ${fieldKey} source`),
      confidence: text(fact.confidence, `agent fact ${fieldKey} confidence`),
      evidence: list(fact.evidence, `agent fact ${fieldKey} evidence`),
      source_urls: list(fact.source_urls, `agent fact ${fieldKey} source_urls`).map(String),
      reason: optionalText(fact.reason),
      provenance: {
        adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
        publication_stream_id: row.publication_stream_id,
        active_revision_id: row.active_revision_id,
        active_sequence: Number(row.active_sequence),
        result_hash: row.result_hash,
        agent_model: payload.agent_model,
        agent_config_id: payload.agent_config_id,
        prompt_template_id: payload.prompt_template_id,
        prompt_hash: payload.prompt_hash,
        output_hash: payload.output_hash,
      },
    };
  });
}

function factsFromPrevious(rows, channelId, snapshotId) {
  return rows.map((value) => {
    const row = object(value, "previous profile fact");
    const fieldKey = text(row.field_key, "previous fact field_key");
    return {
      ...row,
      id: identifier("publication_fact", snapshotId, fieldKey),
      channel_id: channelId,
      channel_snapshot_id: snapshotId,
      provenance: {
        ...(object(row.provenance ?? {}, "previous fact provenance")),
        adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
        carried_forward_from_fact_id: row.id,
      },
    };
  });
}

function scopeContents(contents, scope, capturedAt) {
  if (scope === "all") return contents;
  if (new Set(["videos", "shorts", "lives"]).has(scope)) {
    return contents.filter((item) => item.content_kind === scope);
  }
  const days = scope === "last30d" ? 30 : 90;
  const cutoff = new Date(new Date(capturedAt).getTime() - days * 86400000);
  return contents.filter((item) => {
    const instant = item.published_at ?? (item.published_date ? `${item.published_date}T00:00:00.000Z` : null);
    return instant && new Date(instant) >= cutoff;
  });
}

function roundedCoverage(sampleSize, populationSize) {
  if (populationSize === 0) return 0;
  return Number((sampleSize / populationSize).toFixed(8));
}

function metricStatus(items, field, sampleSize, populationSize) {
  if (sampleSize === 0) return { status: "unavailable", reason: "No eligible observations in this scope." };
  const statusField = `${field}_status`;
  const allExact = sampleSize === populationSize
    && items.filter((item) => item[field] != null).every((item) => item[statusField] === "exact");
  return allExact
    ? { status: "exact", reason: null }
    : { status: "estimated", reason: "Metric includes incomplete or non-exact observations." };
}

function percentileMedian(values) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function baseMetric({ snapshotId, channelId, scope, key, capturedAt, populationSize, sampleSize }) {
  return {
    id: identifier("publication_metric", snapshotId, key, scope),
    channel_id: channelId,
    channel_snapshot_id: snapshotId,
    metric_key: key,
    scope,
    value_numeric: null,
    value_json: null,
    formula_version: BUSINESS_PROJECTION_METRIC_VERSION,
    status_reason: null,
    computed_at: capturedAt,
    sample_size: sampleSize,
    population_size: populationSize,
    coverage: roundedCoverage(sampleSize, populationSize),
    value_status: "unavailable",
    numerator_numeric: null,
    denominator_numeric: null,
    baseline_snapshot_id: null,
    actual_interval_seconds: null,
    requested_window_days: scope === "last30d" ? 30 : scope === "last90d" ? 90 : null,
  };
}

function aggregateMetric({ snapshotId, channelId, scope, capturedAt, items, field, operation }) {
  const populationSize = items.length;
  const eligible = items.filter((item) => item[field] != null);
  const sampleSize = eligible.length;
  const key = operation === "sum"
    ? "observed_content_views"
    : `${operation}_${field.replace("_count", "s")}`;
  const metric = baseMetric({
    snapshotId,
    channelId,
    scope,
    key,
    capturedAt,
    populationSize,
    sampleSize,
  });
  const quality = metricStatus(items, field, sampleSize, populationSize);
  metric.value_status = quality.status;
  metric.status_reason = quality.reason;
  metric.formula_version = operation === "sum"
    ? `sum(non-null ${field});${BUSINESS_PROJECTION_METRIC_VERSION}`
    : `${operation}(non-null ${field});${BUSINESS_PROJECTION_METRIC_VERSION}`;
  if (sampleSize === 0) return metric;
  const values = eligible.map((item) => finiteNonnegative(item[field], field));
  const sum = values.reduce((total, value) => total + value, 0);
  if (operation === "sum") {
    metric.value_numeric = sum;
    metric.numerator_numeric = sum;
    metric.denominator_numeric = 1;
  } else if (operation === "average") {
    metric.value_numeric = sum / sampleSize;
    metric.numerator_numeric = sum;
    metric.denominator_numeric = sampleSize;
  } else {
    metric.value_numeric = percentileMedian(values);
  }
  return metric;
}

function ratioMetric({ snapshotId, channelId, scope, capturedAt, sourceMetric, subscriberCount, subscriberStatus }) {
  const key = ({
    average_views: "views_subscribers_ratio",
    average_likes: "likes_subscribers_ratio",
    average_comments: "comments_subscribers_ratio",
  })[sourceMetric.metric_key];
  if (!key) fail("projection_metric_invalid", `unsupported ratio source: ${sourceMetric.metric_key}`);
  const metric = baseMetric({
    snapshotId,
    channelId,
    scope,
    key,
    capturedAt,
    populationSize: sourceMetric.population_size,
    sampleSize: sourceMetric.sample_size,
  });
  metric.formula_version = `100*${sourceMetric.metric_key}/subscribers;${BUSINESS_PROJECTION_METRIC_VERSION}`;
  if (sourceMetric.value_numeric == null || subscriberCount == null || subscriberCount <= 0) {
    metric.status_reason = subscriberCount === 0
      ? "Subscriber denominator is zero."
      : "Average observation or subscriber denominator is unavailable.";
    return metric;
  }
  metric.value_numeric = 100 * sourceMetric.value_numeric / subscriberCount;
  metric.numerator_numeric = sourceMetric.value_numeric;
  metric.denominator_numeric = subscriberCount;
  if (sourceMetric.value_status === "exact" && subscriberStatus === "exact") {
    metric.value_status = "exact";
  } else {
    metric.value_status = "estimated";
    metric.status_reason = "Ratio includes an estimated average or subscriber denominator.";
  }
  return metric;
}

function engagementMetric({ snapshotId, channelId, scope, capturedAt, items }) {
  const eligible = items.filter((item) => (
    item.view_count != null && item.like_count != null && item.comment_count != null
  ));
  const metric = baseMetric({
    snapshotId,
    channelId,
    scope,
    key: "engagement_rate_by_views",
    capturedAt,
    populationSize: items.length,
    sampleSize: eligible.length,
  });
  metric.formula_version = `100*sum(likes+comments)/sum(views),complete-case;${BUSINESS_PROJECTION_METRIC_VERSION}`;
  const denominator = eligible.reduce((sum, item) => sum + Number(item.view_count), 0);
  if (eligible.length === 0 || denominator <= 0) {
    metric.status_reason = "No complete engagement observations with a positive view denominator.";
    return metric;
  }
  const numerator = eligible.reduce(
    (sum, item) => sum + Number(item.like_count) + Number(item.comment_count),
    0,
  );
  metric.value_numeric = 100 * numerator / denominator;
  metric.numerator_numeric = numerator;
  metric.denominator_numeric = denominator;
  const exact = eligible.length === items.length && eligible.every((item) => (
    item.view_count_status === "exact"
    && item.like_count_status === "exact"
    && item.comment_count_status === "exact"
  ));
  metric.value_status = exact ? "exact" : "estimated";
  metric.status_reason = exact ? null : "Engagement includes incomplete or non-exact observations.";
  return metric;
}

export function calculateBusinessProjectionMetrics({
  channelId,
  snapshotId,
  capturedAt,
  contents,
  subscriberCount,
  subscriberStatus,
}) {
  const metrics = [];
  for (const scope of SCOPES) {
    const scoped = scopeContents(contents, scope, capturedAt);
    metrics.push({
      ...baseMetric({
        snapshotId,
        channelId,
        scope,
        key: "content_count",
        capturedAt,
        populationSize: scoped.length,
        sampleSize: scoped.length,
      }),
      value_numeric: scoped.length,
      formula_version: `count(recent canonical content);${BUSINESS_PROJECTION_METRIC_VERSION}`,
      value_status: "exact",
      numerator_numeric: scoped.length,
      denominator_numeric: 1,
    });
    const views = ["sum", "average", "median"].map((operation) => aggregateMetric({
      snapshotId, channelId, scope, capturedAt, items: scoped, field: "view_count", operation,
    }));
    const likes = ["average", "median"].map((operation) => aggregateMetric({
      snapshotId, channelId, scope, capturedAt, items: scoped, field: "like_count", operation,
    }));
    const comments = ["average", "median"].map((operation) => aggregateMetric({
      snapshotId, channelId, scope, capturedAt, items: scoped, field: "comment_count", operation,
    }));
    metrics.push(...views, ...likes, ...comments);
    metrics.push(engagementMetric({ snapshotId, channelId, scope, capturedAt, items: scoped }));
    metrics.push(ratioMetric({
      snapshotId,
      channelId,
      scope,
      capturedAt,
      sourceMetric: views[1],
      subscriberCount,
      subscriberStatus,
    }));
    metrics.push(ratioMetric({
      snapshotId,
      channelId,
      scope,
      capturedAt,
      sourceMetric: likes[0],
      subscriberCount,
      subscriberStatus,
    }));
    metrics.push(ratioMetric({
      snapshotId,
      channelId,
      scope,
      capturedAt,
      sourceMetric: comments[0],
      subscriberCount,
      subscriberStatus,
    }));
  }
  for (const key of PLACEHOLDER_METRICS) {
    metrics.push({
      ...baseMetric({
        snapshotId,
        channelId,
        scope: "all",
        key,
        capturedAt,
        populationSize: 0,
        sampleSize: 0,
      }),
      formula_version: `requires-history;${BUSINESS_PROJECTION_METRIC_VERSION}`,
      value_status: "placeholder",
      status_reason: "Historical comparison is not available in this projection batch.",
    });
  }
  return metrics;
}

function latestPublication(contents) {
  const candidates = contents
    .filter((item) => item.published_date)
    .sort((left, right) => {
      const leftValue = left.published_at ?? `${left.published_date}T00:00:00.000Z`;
      const rightValue = right.published_at ?? `${right.published_date}T00:00:00.000Z`;
      return rightValue.localeCompare(leftValue);
    });
  const latest = candidates[0];
  if (!latest) return null;
  return {
    at: latest.published_at_status === "exact" ? latest.published_at : null,
    date: latest.published_date,
    status: latest.published_at_status === "exact" ? "exact" : "date_exact",
    source: latest.published_at_source ?? "publication_projection",
  };
}

function projectionCapturedAt(current, previous, fallback) {
  const values = [
    current.channel?.activated_at,
    current.video?.activated_at,
    current.agent?.activated_at,
    previous.snapshot?.captured_at,
    fallback,
  ].filter(Boolean).map((value) => timestamp(value, "projection timestamp"));
  return values.sort().at(-1);
}

export function buildBusinessPublicationProjection(inputValue) {
  const input = object(inputValue, "projection input");
  const channelId = text(input.channelId, "channelId");
  const batchId = text(input.batchId, "batchId");
  const current = object(input.current ?? {}, "current");
  const previous = object(input.previous ?? {}, "previous");
  const versionVector = object(input.versionVector, "versionVector");
  const capturedAt = projectionCapturedAt(current, previous, input.capturedAt);

  if (current.channel?.is_retracted || current.channel?.lifecycle_status === "removed") {
    return {
      action: "remove",
      channelId,
      versionVector,
      capturedAt,
      projectionHash: observationFactsHash({ channel_id: channelId, action: "remove", versionVector }),
    };
  }
  if (!current.channel && !previous.snapshot) {
    fail("projection_channel_missing", `Channel ${channelId} has no Current or active public snapshot`);
  }

  const snapshotId = identifier("publication_snapshot", batchId, channelId);
  const snapshot = current.channel
    ? channelSnapshotFromCurrent({
      row: current.channel,
      batchId,
      snapshotId,
      capturedAt,
      versionVector,
    })
    : channelSnapshotFromPrevious({
      row: previous.snapshot,
      batchId,
      snapshotId,
      capturedAt,
      versionVector,
    });
  if (snapshot.channel_id !== channelId) {
    fail("projection_identity_mismatch", "Channel Current identity differs from projection target");
  }

  const links = current.channel
    ? linksFromCurrent(current.channel.payload_json, channelId, snapshotId)
    : linksFromPrevious(previous.links ?? [], channelId, snapshotId);
  const contentPairs = current.video
    ? (current.contents ?? []).map((row, index) => contentFromCurrent(row, channelId, snapshotId, index))
    : (previous.contents ?? []).map((row, index) => contentFromPrevious(row, channelId, snapshotId, index));
  const contents = contentPairs.map((item) => item.snapshot);
  const contentItems = contentPairs.map((item) => item.identity);
  const facts = current.agent
    ? factsFromCurrent(current.agent, channelId, snapshotId)
    : factsFromPrevious(previous.facts ?? [], channelId, snapshotId);
  const latest = latestPublication(contents);
  if (latest) {
    snapshot.candidate_last_published_at = latest.at;
    snapshot.candidate_last_published_date = latest.date;
    snapshot.candidate_last_published_status = latest.status;
    snapshot.candidate_last_published_source = latest.source;
  }
  const metrics = calculateBusinessProjectionMetrics({
    channelId,
    snapshotId,
    capturedAt,
    contents,
    subscriberCount: snapshot.subscriber_count,
    subscriberStatus: snapshot.subscriber_count_status,
  });
  const projectionHash = observationFactsHash({
    adapter_version: BUSINESS_PROJECTION_ADAPTER_VERSION,
    version_vector: versionVector,
    snapshot,
    links,
    contents,
    facts,
    metrics,
  });
  return {
    action: "upsert",
    channelId,
    versionVector,
    capturedAt,
    snapshot,
    links,
    contentItems,
    contents,
    facts,
    metrics,
    projectionHash,
  };
}
