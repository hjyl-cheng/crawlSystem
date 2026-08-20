import { createHash } from "node:crypto";
import { normalizeAboutCurrentIdentity } from "./aboutCurrent.js";
import { incrementalAgentEventPayload } from "./incrementalAgentResultStore.js";
import { VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL } from "./publicationContract.js";

export const BASELINE_BUNDLE_FORMAT = "crawler-observation-ndjson-v1";
const RESOLVED_METRIC_STATUSES = new Set(["exact", "estimated"]);
const UNRESOLVED_METRIC_STATUSES = new Set(["unavailable", "unresolved"]);
const CONTENT_TYPES = new Set(["video", "short", "live"]);
const PUBLISHED_PRECISIONS = new Set(["second", "date_only", "unknown"]);

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function safeCount(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function sha256Bytes(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function compareCanonicalText(left, right) {
  return Buffer.compare(Buffer.from(String(left), "utf8"), Buffer.from(String(right), "utf8"));
}

function timestamp(value) {
  if (value == null) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function currentIdentity(row = {}) {
  return normalizeAboutCurrentIdentity({
    title: row.title,
    handle: row.handle,
    avatar_url: row.avatar_url,
    keywords: row.keywords,
    available_tabs: row.available_tabs,
    description: row.summary,
  });
}

function businessEmailCurrent(row = {}) {
  if (row.youtube_business_email_observed_at == null) {
    return { available: null, status: "unknown" };
  }
  if (row.youtube_business_email_available === true) {
    return { available: true, status: "available" };
  }
  if (row.youtube_business_email_available === false) {
    return { available: false, status: "not_available" };
  }
  return { available: null, status: "unknown" };
}

function baselineMetric(row, name) {
  const storedStatus = text(row?.[`${name}_status`]);
  const storedValue = safeCount(row?.[name]);
  const resolved = RESOLVED_METRIC_STATUSES.has(storedStatus) && storedValue !== null;
  return {
    value: resolved ? storedValue : null,
    text: text(row?.[`${name}_text`]),
    status: resolved
      ? storedStatus
      : UNRESOLVED_METRIC_STATUSES.has(storedStatus) ? storedStatus : "unresolved",
    source: text(row?.[`${name}_source`]),
  };
}

export function aboutBaseline(row = {}) {
  const subscriber = baselineMetric(row, "subscriber_count");
  const views = baselineMetric(row, "total_view_count");
  const videos = baselineMetric(row, "total_video_count");
  const payload = {
    subscriber_count: subscriber.value,
    subscriber_count_status: subscriber.status,
    total_view_count: views.value,
    total_view_count_status: views.status,
    total_video_count: videos.value,
    total_video_count_status: videos.status,
  };
  const resolvedCount = [subscriber, views, videos]
    .filter((metric) => RESOLVED_METRIC_STATUSES.has(metric.status)).length;
  const outcome = resolvedCount === 3 ? "complete" : "partial";
  const businessEmail = businessEmailCurrent(row);
  return {
    about: {
      outcome,
      outcome_reason_code: resolvedCount === 3
        ? "migration_baseline_about_complete"
        : resolvedCount > 0
          ? "migration_baseline_about_partial"
          : "migration_baseline_about_unavailable",
      subscriber_count: subscriber.value,
      subscriber_count_text: subscriber.text,
      subscriber_count_status: subscriber.status,
      subscriber_count_source: subscriber.source,
      total_view_count: views.value,
      total_view_count_text: views.text,
      total_view_count_status: views.status,
      total_view_count_source: views.source,
      total_video_count: videos.value,
      total_video_count_text: videos.text,
      total_video_count_status: videos.status,
      total_video_count_source: videos.source,
      facts_hash: sha256Bytes(JSON.stringify(payload)),
    },
    current: {
      aboutDescription: row.about_description ?? null,
      country: row.country ?? null,
      joinedDateText: row.joined_date_text ?? null,
      joinedAt: row.joined_at ?? null,
      joinedAtPrecision: row.joined_at_precision ?? "unknown",
      externalLinks: row.external_links ?? [],
      youtubeBusinessEmailAvailable: businessEmail.available,
      youtubeBusinessEmailStatus: businessEmail.status,
      identity: currentIdentity(row),
    },
    payload,
  };
}

export function discoveryBaseline({ identityCount = 0, entries = [], scanProof = null } = {}) {
  const identityTotal = Math.max(0, Number(identityCount) || 0);
  const firstSeenLimit = Math.min(identityTotal, 30);
  const proof = scanProof && typeof scanProof === "object" && !Array.isArray(scanProof)
    ? scanProof
    : null;
  const proofItems = safeCount(proof?.selected_count);
  const proofPages = safeCount(proof?.pages);
  const proofParseGaps = safeCount(proof?.parse_gap_count);
  const proofTerminal = text(proof?.terminal_condition);
  const proofComplete = proofParseGaps === 0
    && [
      "qualified_item_limit",
      "age_boundary_crossed",
      "list_end",
      VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL,
    ].includes(proofTerminal);
  const itemCount = proof ? (proofItems ?? identityTotal) : firstSeenLimit;
  const firstSeen = entries
    .filter((entry) => text(entry.video_id) && CONTENT_TYPES.has(text(entry.content_type)))
    .slice(0, firstSeenLimit)
    .map((entry, index) => {
      const publishedAt = timestamp(entry.first_published_at ?? entry.published_at);
      const requestedPrecision = text(
        entry.first_published_at_precision ?? entry.published_at_precision,
      );
      return {
        video_id: text(entry.video_id),
        position: index + 1,
        content_type: text(entry.content_type),
        published_at: publishedAt,
        published_at_precision: publishedAt && PUBLISHED_PRECISIONS.has(requestedPrecision)
          ? requestedPrecision
          : "unknown",
      };
    })
    .sort((left, right) => compareCanonicalText(left.published_at, right.published_at));
  const truncated = identityTotal >= 30;
  const payload = {
    pages: proof ? proofPages : itemCount > 0 ? 1 : 0,
    items: itemCount,
    anchor_matched: false,
    stop_reason: proofComplete
      ? proofTerminal
      : text(proof?.stop_reason) ?? (truncated ? "max_items" : "list_end"),
    parse_gap_count: proof ? proofParseGaps : 0,
    first_seen: firstSeen,
    first_seen_count: firstSeen.length,
    detail_success_count: proof
      ? safeCount(proof.detail_success_count) ?? firstSeen.length
      : firstSeen.length,
    detail_failure_count: proof ? safeCount(proof.detail_failure_count) : 0,
    ...(proof
      ? {
          inspected_count: safeCount(proof.inspected_count),
          requested_limit: safeCount(proof.requested_limit),
          content_max_age_days: safeCount(proof.content_max_age_days),
          scan_policy_version: text(proof.scan_policy_version),
          terminal_condition: proofTerminal,
          qualified_count: safeCount(proof.coverage?.qualified_count),
          excluded_count: safeCount(proof.coverage?.excluded_count),
          age_boundary_crossed: proof.coverage?.age_boundary_crossed === true,
        }
      : {}),
  };
  return {
    outcome: proof ? (proofComplete ? "complete" : "partial") : truncated ? "partial" : "complete",
    payload,
  };
}

export function recentSamplingBaseline({ recentCount = 0, staleCount = 0 } = {}) {
  const recent = Math.max(0, Number(recentCount) || 0);
  const stale = Math.min(recent, Math.max(0, Number(staleCount) || 0));
  return {
    outcome: "complete",
    payload: {
      recent_count: recent,
      stale_ratio: recent > 0 ? Number((stale / recent).toFixed(6)) : 0,
      selected_count: 0,
      success_count: 0,
      failure_count: 0,
      next_count: 0,
      comparable_view_count: 0,
      view_changed_count: 0,
      view_delta_total: 0,
      engagement_changed_count: 0,
    },
  };
}

export function agentBaseline(row = {}) {
  const metrics = row.metrics_json && typeof row.metrics_json === "object"
    ? row.metrics_json
    : {};
  const persistedAgentVersionHash = text(row.agent_version_hash);
  const payload = incrementalAgentEventPayload(metrics, {
    provider: row.provider ?? null,
    model: row.agent_model ?? row.config_model ?? null,
    agentConfigId: row.agent_config_id ?? null,
    promptTemplateId: row.prompt_template_id ?? null,
    promptHash: row.prompt_hash ?? null,
    promptVariant: row.prompt_variant ?? null,
    taxonomyVersion: row.taxonomy_version ?? "qy-taxonomy-v1",
    tools: Array.isArray(row.tools_json) ? row.tools_json : [],
    agentVersionHash: persistedAgentVersionHash,
  });
  payload.agent_version_hash = persistedAgentVersionHash;
  const inputContentIds = [...new Set(
    (Array.isArray(row.input_content_ids) ? row.input_content_ids : [])
      .map(text)
      .filter(Boolean),
  )].sort(compareCanonicalText);
  payload.input_content_count = inputContentIds.length;
  payload.input_content_hash = row.input_content_hash ?? null;
  payload.category_level_2 = [...new Set(
    payload.category_level_2.map(text).filter(Boolean),
  )];
  const ratio = Number(payload.active_subscriber_ratio);
  payload.active_subscriber_ratio = Number.isSafeInteger(ratio) && ratio >= 0 && ratio <= 100
    ? ratio
    : null;
  return {
    current_hash: payload.output_hash,
    input_content_hash: payload.input_content_hash,
    agent_version_hash: payload.agent_version_hash,
    outcome: "complete",
    payload: { ...payload, fulfilled_plan_count: 1 },
  };
}

export function serializeBaselineEvents(events, {
  expectedChannelCount,
  exportedAt,
} = {}) {
  const maximumObservedAt = new Date(exportedAt).getTime();
  if (!Number.isFinite(maximumObservedAt)) throw new TypeError("exportedAt must be a timestamp");
  const sorted = [...events].sort((left, right) => (
    compareCanonicalText(left.channel_id, right.channel_id)
    || compareCanonicalText(left.observation_kind, right.observation_kind)
    || Number(left.kind_sequence) - Number(right.kind_sequence)
  ));
  const eventIds = new Set();
  const observationIds = new Set();
  const sequences = new Map();
  const channels = new Set();
  const channelsWithFacts = new Set();
  for (const event of sorted) {
    if (!text(event.event_id) || eventIds.has(event.event_id)) {
      throw new TypeError("Baseline event_id values must be present and unique");
    }
    if (!text(event.observation_id) || observationIds.has(event.observation_id)) {
      throw new TypeError("Baseline observation_id values must be present and unique");
    }
    const channelId = text(event.channel_id);
    const kind = text(event.observation_kind);
    const sequence = Number(event.kind_sequence);
    const key = `${channelId}\u0000${kind}`;
    const expected = (sequences.get(key) ?? 0) + 1;
    if (!channelId || !kind || !Number.isSafeInteger(sequence) || sequence !== expected) {
      throw new TypeError(`Baseline sequence must be contiguous from 1 for ${channelId}/${kind}`);
    }
    const observedAt = new Date(event.observed_at).getTime();
    if (!Number.isFinite(observedAt)) {
      throw new TypeError("Baseline event observed_at must be a timestamp");
    }
    if (observedAt > maximumObservedAt) {
      throw new TypeError("Baseline event is newer than exportedAt");
    }
    eventIds.add(event.event_id);
    observationIds.add(event.observation_id);
    sequences.set(key, sequence);
    channels.add(channelId);
    if (event.outcome !== "failed") channelsWithFacts.add(channelId);
  }
  if (channels.size !== Number(expectedChannelCount)) {
    throw new TypeError(
      `Baseline Channel count mismatch: expected ${expectedChannelCount}, got ${channels.size}`,
    );
  }
  if (channelsWithFacts.size !== channels.size) {
    throw new TypeError("every Baseline Channel requires a non-failed Observation");
  }
  const bytes = Buffer.from(sorted.map((event) => JSON.stringify(event)).join("\n") + "\n");
  return {
    bytes,
    eventCount: sorted.length,
    channelCount: channels.size,
    eventsSha256: sha256Bytes(bytes),
  };
}

export function baselineManifest({
  baselineVersion,
  sourceDatabase,
  sourceSnapshotId,
  exportedAt,
  eventsFile,
  eventCount,
  channelCount,
  byteCount,
  eventsSha256,
}) {
  const manifest = {
    schema_version: 1,
    bundle_format: BASELINE_BUNDLE_FORMAT,
    baseline_version: text(baselineVersion),
    source_database: text(sourceDatabase),
    source_schema: "crawler",
    source_snapshot_id: text(sourceSnapshotId),
    exported_at: new Date(exportedAt).toISOString(),
    events_file: text(eventsFile),
    event_count: Number(eventCount),
    channel_count: Number(channelCount),
    byte_count: Number(byteCount),
    events_sha256: text(eventsSha256),
  };
  if (!manifest.baseline_version || !manifest.source_database || !manifest.source_snapshot_id) {
    throw new TypeError("Baseline Manifest identity fields are required");
  }
  if (!manifest.events_file || !Number.isSafeInteger(manifest.event_count)
      || manifest.event_count <= 0 || !Number.isSafeInteger(manifest.channel_count)
      || manifest.channel_count <= 0 || !Number.isSafeInteger(manifest.byte_count)
      || manifest.byte_count <= 0 || !/^sha256:[0-9a-f]{64}$/.test(manifest.events_sha256 ?? "")) {
    throw new TypeError("Baseline Manifest counts and event digest are invalid");
  }
  return manifest;
}

export function baselineFileSha256(value) {
  return sha256Bytes(value);
}
