import { observationFactsHash } from "./crawlObservationStore.js";
import { publicationResultHash } from "./publicationResultHash.js";

const HASH = /^sha256:[0-9a-f]{64}$/;

const CHANNEL_V1_KEYS = new Set([
  "channel_id",
  "title",
  "canonical_url",
  "vanity_channel_url",
  "handle",
  "avatar",
  "rss_url",
  "keywords",
  "is_family_safe",
  "is_verified",
  "is_verified_status",
  "has_videos",
  "has_shorts",
  "has_live_streams",
  "description",
  "subscriber_count",
  "subscriber_count_status",
  "total_video_count",
  "total_video_count_status",
  "total_view_count",
  "total_view_count_status",
  "joined_date",
  "joined_date_status",
  "joined_date_raw",
  "country_code",
  "country_name",
  "links",
  "lifecycle_status",
]);
const CHANNEL_V2_KEYS = new Set([
  ...CHANNEL_V1_KEYS,
  "youtube_business_email_available",
  "youtube_business_email_observed_at",
]);
const AGENT_KEYS = new Set([
  "channel_id",
  "agent_mode",
  "input_url",
  "facts",
  "agent_model",
  "agent_config_id",
  "prompt_template_id",
  "prompt_hash",
  "prompt_variant",
  "agent_version_hash",
  "output_hash",
  "input_content_ids",
  "input_content_hash",
  "taxonomy_version",
]);
const AGENT_FACT_KEYS = new Set([
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
const AGENT_FACT_VALUE_KEYS = new Set([
  "value",
  "confidence",
  "evidence",
  "source_urls",
  "reason",
  "source",
]);
const VIDEO_BOOTSTRAP_KEYS = new Set([
  "channel_id",
  "window_policy",
  "window_proof",
  "items",
  "result_hash",
]);
const VIDEO_DELTA_KEYS = new Set([
  "channel_id",
  "window_policy",
  "window_proof",
  "upserts",
  "window_exits",
  "retractions",
  "result_hash",
]);
const VIDEO_POLICY_KEYS = new Set([
  "policy_version",
  "as_of",
  "cutoff_at",
  "cutoff_date",
  "max_age_days",
  "max_items",
]);
const VIDEO_PROOF_KEYS = new Set([
  "complete",
  "terminal_condition",
  "catalog_candidate_count",
  "qualified_count",
  "selected_count",
  "excluded_count",
  "latest_scan_items",
  "latest_scan_pages",
  "latest_scan_stop_reason",
  "latest_scan_detail_failure_count",
]);
const VIDEO_ITEM_KEYS = new Set([
  "position",
  "item_hash",
  "content_id",
  "content_key",
  "kind",
  "title",
  "url",
  "thumbnail_url",
  "published_at",
  "published_date",
  "published_at_precision",
  "published_at_status",
  "published_at_source",
  "duration_seconds",
  "duration_status",
  "duration_source",
  "view_count",
  "view_count_status",
  "view_count_source",
  "view_count_observed_at",
  "like_count",
  "like_count_status",
  "like_count_source",
  "like_count_observed_at",
  "comment_count",
  "comment_count_status",
  "comment_count_source",
  "comment_count_observed_at",
  "comments_disabled",
  "description",
  "description_status",
  "description_source",
  "hashtags",
  "keywords",
  "access_status",
  "access_status_source",
  "is_members_only",
  "live_scheduled_at",
  "live_started_at",
  "live_ended_at",
  "extractor_version",
]);
const VIDEO_EXIT_KEYS = new Set(["content_id", "reason"]);
const VIDEO_EXIT_REASONS = new Set(["aged_out", "outside_limit"]);
const VIDEO_RETRACTION_REASONS = new Set([
  "source_deleted",
  "source_private",
  "source_unavailable",
  "policy_removed",
]);

export class BusinessPublicationContractError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "BusinessPublicationContractError";
    this.code = code;
  }
}

function fail(code, message) {
  throw new BusinessPublicationContractError(code, message);
}

function object(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    fail("payload_contract_invalid", `${field} must be an object`);
  }
  return value;
}

function exactKeys(value, expected, field, contractVersion = 1) {
  const actual = Object.keys(object(value, field));
  const missing = [...expected].filter((key) => !Object.hasOwn(value, key));
  const extras = actual.filter((key) => !expected.has(key));
  if (missing.length > 0 || extras.length > 0) {
    fail(
      "payload_contract_invalid",
      `${field} keys differ from Contract V${contractVersion}`
        + `${missing.length > 0 ? `; missing=${missing.sort().join(",")}` : ""}`
        + `${extras.length > 0 ? `; unsupported=${extras.sort().join(",")}` : ""}`,
    );
  }
}

function optionalBoolean(value, field) {
  if (value == null) return null;
  if (typeof value !== "boolean") fail("payload_contract_invalid", `${field} must be boolean or null`);
  return value;
}

function optionalTimestamp(value, field) {
  if (value == null) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) fail("payload_contract_invalid", `${field} must be a timestamp or null`);
  return parsed.toISOString();
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) fail("payload_contract_invalid", `${field} is required`);
  return output;
}

function hash(value, field) {
  const output = requiredText(value, field);
  if (!HASH.test(output)) fail("payload_contract_invalid", `${field} must be a SHA-256 hash`);
  return output;
}

function matchChannel(envelope) {
  if (requiredText(envelope.payload.channel_id, "payload.channel_id") !== envelope.channel_id) {
    fail("payload_channel_mismatch", "Payload Channel ID does not match the Envelope");
  }
}

function matchResultHash(envelope, actual) {
  if (actual !== envelope.result_hash) {
    fail("result_hash_mismatch", "Payload result does not match Envelope result_hash");
  }
}

function validateChannel(envelope) {
  const contractVersion = Number(envelope.contract_version);
  if (![1, 2].includes(contractVersion)) {
    fail("payload_contract_invalid", `Unsupported Channel Contract V${envelope.contract_version}`);
  }
  if (envelope.revision_type === "retraction") {
    exactKeys(
      envelope.payload,
      new Set(["channel_id", "retraction"]),
      "channel retraction payload",
      contractVersion,
    );
    object(envelope.payload.retraction, "channel retraction payload.retraction");
  } else {
    exactKeys(
      envelope.payload,
      contractVersion === 2 ? CHANNEL_V2_KEYS : CHANNEL_V1_KEYS,
      "channel payload",
      contractVersion,
    );
    if (!Array.isArray(envelope.payload.avatar)
        || !Array.isArray(envelope.payload.keywords)
        || !Array.isArray(envelope.payload.links)) {
      fail("payload_contract_invalid", "Channel avatar, keywords, and links must be arrays");
    }
    if (contractVersion === 2) {
      const available = optionalBoolean(
        envelope.payload.youtube_business_email_available,
        "channel.youtube_business_email_available",
      );
      const observedAt = optionalTimestamp(
        envelope.payload.youtube_business_email_observed_at,
        "channel.youtube_business_email_observed_at",
      );
      if ((available === null) !== (observedAt === null)) {
        fail(
          "payload_contract_invalid",
          "Channel business email availability and observation time must both be known or unknown",
        );
      }
    }
  }
  matchChannel(envelope);
  matchResultHash(envelope, observationFactsHash(envelope.payload));
}

function validateAgent(envelope) {
  if (Number(envelope.contract_version) !== 1) {
    fail("payload_contract_invalid", "Agent supports only Contract V1");
  }
  if (envelope.revision_type === "retraction") {
    exactKeys(envelope.payload, new Set(["channel_id", "retraction"]), "agent retraction payload");
    object(envelope.payload.retraction, "agent retraction payload.retraction");
  } else {
    exactKeys(envelope.payload, AGENT_KEYS, "agent payload");
    exactKeys(envelope.payload.facts, AGENT_FACT_KEYS, "agent payload.facts");
    for (const [name, fact] of Object.entries(envelope.payload.facts)) {
      exactKeys(fact, AGENT_FACT_VALUE_KEYS, `agent payload.facts.${name}`);
    }
    if (!Array.isArray(envelope.payload.input_content_ids)) {
      fail("payload_contract_invalid", "Agent input_content_ids must be an array");
    }
    if (envelope.payload.input_content_ids.some((value) => (
      typeof value !== "string" || value.trim() !== value || value.length === 0
    ))) {
      fail("payload_contract_invalid", "Agent input_content_ids must contain non-empty strings");
    }
    const canonicalInputIds = [...new Set(envelope.payload.input_content_ids)].sort((left, right) => (
      Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"))
    ));
    if (canonicalInputIds.length !== envelope.payload.input_content_ids.length
        || canonicalInputIds.some((value, index) => value !== envelope.payload.input_content_ids[index])) {
      fail("payload_contract_invalid", "Agent input_content_ids must be unique and sorted");
    }
    if (observationFactsHash(canonicalInputIds) !== hash(
      envelope.payload.input_content_hash,
      "agent payload.input_content_hash",
    )) {
      fail("payload_contract_invalid", "Agent input_content_hash does not match input_content_ids");
    }
  }
  matchChannel(envelope);
  matchResultHash(envelope, observationFactsHash(envelope.payload));
}

function videoItemHash(item) {
  const businessValue = Object.fromEntries(Object.entries(item).filter(([key]) => (
    key !== "position"
    && key !== "item_hash"
    && !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
  return observationFactsHash(businessValue);
}

function validateVideoItems(items, field, { contiguous = false } = {}) {
  if (!Array.isArray(items)) fail("payload_contract_invalid", `${field} must be an array`);
  const contentIds = new Set();
  const positions = new Set();
  let previousPosition = 0;
  for (const [index, item] of items.entries()) {
    exactKeys(item, VIDEO_ITEM_KEYS, `${field} item`);
    const contentId = requiredText(item.content_id, `${field}.content_id`);
    const position = Number(item.position);
    if (!Number.isSafeInteger(position) || position < 1) {
      fail("payload_contract_invalid", `${field}.position must be a positive integer`);
    }
    if (position <= previousPosition) {
      fail("payload_contract_invalid", `${field} positions must be ordered`);
    }
    if (contiguous && position !== index + 1) {
      fail("payload_contract_invalid", `${field} positions must be contiguous and ordered`);
    }
    previousPosition = position;
    if (contentIds.has(contentId) || positions.has(position)) {
      fail("payload_contract_invalid", `${field} contains duplicate Content IDs or positions`);
    }
    contentIds.add(contentId);
    positions.add(position);
    const publishableAccess = (["public", "unlisted"].includes(item.access_status)
        && item.is_members_only === false)
      || (item.access_status === "members_only" && item.is_members_only === true);
    if (!publishableAccess) {
      fail("payload_contract_invalid", `${field} may contain only public, unlisted, or members-only Content`);
    }
    if (hash(item.item_hash, `${field}.item_hash`) !== videoItemHash(item)) {
      fail("payload_contract_invalid", `${field}.item_hash does not match the Item Payload`);
    }
  }
}

function validateVideoPolicy(envelope) {
  const policy = envelope.payload.window_policy;
  exactKeys(policy, VIDEO_POLICY_KEYS, "video payload.window_policy");
  exactKeys(envelope.payload.window_proof, VIDEO_PROOF_KEYS, "video payload.window_proof");
  if (policy.policy_version !== envelope.policy_version
      || Number(policy.max_age_days) !== 90
      || Number(policy.max_items) !== 30) {
    fail("payload_contract_invalid", "Video Window Policy does not match Contract V1");
  }
  if (envelope.payload.window_proof.complete !== true) {
    fail("payload_contract_invalid", "Video Window Proof must be complete");
  }
}

function validateVideoRemovals(items, field, reasons) {
  if (!Array.isArray(items)) fail("payload_contract_invalid", `${field} must be an array`);
  const contentIds = new Set();
  for (const item of items) {
    exactKeys(item, VIDEO_EXIT_KEYS, `${field} item`);
    const contentId = requiredText(item.content_id, `${field}.content_id`);
    if (contentIds.has(contentId)) {
      fail("payload_contract_invalid", `${field} contains duplicate Content IDs`);
    }
    contentIds.add(contentId);
    if (!reasons.has(item.reason)) {
      fail("payload_contract_invalid", `${field}.reason is not allowed by Contract V1`);
    }
  }
  return contentIds;
}

function validateVideo(envelope) {
  if (Number(envelope.contract_version) !== 1) {
    fail("payload_contract_invalid", "Video supports only Contract V1");
  }
  const bootstrap = envelope.revision_type === "bootstrap";
  exactKeys(
    envelope.payload,
    bootstrap ? VIDEO_BOOTSTRAP_KEYS : VIDEO_DELTA_KEYS,
    bootstrap ? "video bootstrap payload" : "video delta payload",
  );
  matchChannel(envelope);
  validateVideoPolicy(envelope);
  matchResultHash(envelope, hash(envelope.payload.result_hash, "video payload.result_hash"));
  if (bootstrap) {
    if (envelope.payload.items.length > 30) {
      fail("payload_contract_invalid", "Video bootstrap cannot contain more than 30 Items");
    }
    validateVideoItems(envelope.payload.items, "video payload.items", { contiguous: true });
    matchResultHash(envelope, publicationResultHash("video", envelope.payload));
    return;
  }
  validateVideoItems(envelope.payload.upserts, "video payload.upserts");
  const upserts = new Set(envelope.payload.upserts.map((item) => item.content_id));
  const exits = validateVideoRemovals(
    envelope.payload.window_exits,
    "video payload.window_exits",
    VIDEO_EXIT_REASONS,
  );
  const retractions = validateVideoRemovals(
    envelope.payload.retractions,
    "video payload.retractions",
    VIDEO_RETRACTION_REASONS,
  );
  for (const contentId of upserts) {
    if (exits.has(contentId) || retractions.has(contentId)) {
      fail("payload_contract_invalid", "Video Delta actions overlap for one Content ID");
    }
  }
  for (const contentId of exits) {
    if (retractions.has(contentId)) {
      fail("payload_contract_invalid", "Video Delta actions overlap for one Content ID");
    }
  }
}

export function validateBusinessPublicationEnvelope(envelopeValue) {
  const envelope = object(envelopeValue, "envelope");
  if (envelope.domain === "channel") validateChannel(envelope);
  else if (envelope.domain === "video") validateVideo(envelope);
  else if (envelope.domain === "agent") validateAgent(envelope);
  else fail("payload_contract_invalid", `Unsupported Publication Domain: ${envelope.domain}`);
  return envelope;
}
