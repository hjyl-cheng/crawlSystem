import { observationFactsHash } from "../../src/crawlObservationStore.js";
import {
  AGENT_TAXONOMY_VERSION,
  PUBLICATION_POLICY_VERSION,
  VIDEO_WINDOW_POLICY_VERSION,
} from "../../src/publicationContract.js";
import { publicationResultHash } from "../../src/publicationResultHash.js";

const AGENT_FACTS = [
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
];

function channelPayload(channelId) {
  return {
    channel_id: channelId,
    title: "Publication release fixture",
    canonical_url: `https://www.youtube.com/channel/${channelId}`,
    vanity_channel_url: null,
    handle: null,
    avatar: [],
    rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
    keywords: [],
    is_family_safe: true,
    is_verified: false,
    is_verified_status: "observed_false",
    has_videos: false,
    has_shorts: false,
    has_live_streams: false,
    description: "Publication release contract fixture",
    subscriber_count: 1,
    subscriber_count_status: "exact",
    total_video_count: 0,
    total_video_count_status: "exact",
    total_view_count: 0,
    total_view_count_status: "exact",
    joined_date: "2020-01-01",
    joined_date_status: "exact",
    joined_date_raw: "Joined Jan 1, 2020",
    country_code: "US",
    country_name: "United States",
    links: [],
    lifecycle_status: "active",
  };
}

function videoPayload(channelId) {
  const payload = {
    channel_id: channelId,
    window_policy: {
      policy_version: VIDEO_WINDOW_POLICY_VERSION,
      as_of: "2026-07-27T09:00:00.000Z",
      cutoff_at: "2026-04-28T09:00:00.000Z",
      cutoff_date: "2026-04-28",
      max_age_days: 90,
      max_items: 30,
    },
    window_proof: {
      complete: true,
      terminal_condition: "list_end_confirmed",
      catalog_candidate_count: 0,
      qualified_count: 0,
      selected_count: 0,
      excluded_count: 0,
      latest_scan_items: 0,
      latest_scan_pages: 1,
      latest_scan_stop_reason: "list_end",
      latest_scan_detail_failure_count: 0,
    },
    items: [],
    result_hash: null,
  };
  payload.result_hash = publicationResultHash("video", payload);
  return payload;
}

function agentPayload(channelId) {
  return {
    channel_id: channelId,
    agent_mode: "fixture",
    input_url: `https://www.youtube.com/channel/${channelId}`,
    facts: Object.fromEntries(AGENT_FACTS.map((name) => [name, {
      value: null,
      confidence: null,
      evidence: [],
      source_urls: [],
      reason: "fixture",
      source: "integration-test",
    }])),
    agent_model: "fixture-model",
    agent_config_id: "fixture-config",
    prompt_template_id: "fixture-template",
    prompt_hash: `sha256:${"1".repeat(64)}`,
    prompt_variant: "fixture",
    agent_version_hash: `sha256:${"2".repeat(64)}`,
    output_hash: `sha256:${"3".repeat(64)}`,
    input_content_ids: [],
    input_content_hash: observationFactsHash([]),
    taxonomy_version: AGENT_TAXONOMY_VERSION,
  };
}

export function publicationPayloadFixture(domain, channelId) {
  if (domain === "channel") return channelPayload(channelId);
  if (domain === "video") return videoPayload(channelId);
  if (domain === "agent") return agentPayload(channelId);
  throw new TypeError(`unsupported Publication fixture domain: ${domain}`);
}

export function publicationPayloadResultHash(domain, payload) {
  return domain === "video" ? payload.result_hash : observationFactsHash(payload);
}

export function publicationPolicyVersionFixture(domain) {
  return domain === "video" ? VIDEO_WINDOW_POLICY_VERSION : PUBLICATION_POLICY_VERSION;
}
