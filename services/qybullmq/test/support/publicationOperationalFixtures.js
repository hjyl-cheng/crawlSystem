import {
  AGENT_FACT_KEYS,
  buildAgentPublicationRun,
} from "../../src/agentPublicationCurrent.js";
import { observationFactsHash } from "../../src/crawlObservationStore.js";

const DEFAULT_OBSERVED_AT = "2026-07-28T03:10:00.000Z";
const AGENT_PROMPT = "Analyze every input URL and return all V1 Agent facts.";

function agentFactValue(field) {
  const values = {
    country: "Brazil",
    creator_language: "Portuguese",
    creator_gender: "brand_team",
    creator_age_range: 35,
    audience_region: [
      { region: "Brazil", percentage: 70 },
      { region: "Portugal", percentage: 10 },
      { region: "United States", percentage: 8 },
      { region: "Angola", percentage: 5 },
      { region: "Mozambique", percentage: 2 },
      { region: "Other", percentage: 5 },
    ],
    audience_language: [
      { language: "Portuguese", percentage: 90 },
      { language: "English", percentage: 7 },
      { language: "Other", percentage: 3 },
    ],
    audience_age_gender: [
      { age_range: "18-24", male: 8, female: 8 },
      { age_range: "25-34", male: 14, female: 14 },
      { age_range: "35-44", male: 10, female: 10 },
      { age_range: "45-54", male: 7, female: 7 },
      { age_range: "55-64", male: 4, female: 4 },
      { age_range: "65+", male: 7, female: 7 },
    ],
    active_subscriber_ratio: 0,
    channel_categories: {
      level_1: "Software & Internet",
      level_2: ["Artificial Intelligence"],
    },
    channel_tags: {
      tags: [
        "Artificial Intelligence",
        "Software",
        "Programming",
        "Technology",
        "Tutorials",
        "Machine Learning",
        "Developer Tools",
        "Product Reviews",
        "Industry News",
        "Digital Culture",
      ],
      top_5_distribution: [
        { tag: "Artificial Intelligence", percentage: 25 },
        { tag: "Software", percentage: 22 },
        { tag: "Programming", percentage: 18 },
        { tag: "Technology", percentage: 14 },
        { tag: "Tutorials", percentage: 11 },
        { tag: "Other", percentage: 10 },
      ],
    },
  };
  return values[field];
}

function source(kind, channelId, observedAt, {
  factsHash,
  resultSummary = {},
  stopReason = null,
} = {}) {
  const observationId = `${kind}-observation-${channelId}`;
  const runId = `${kind}-run-${channelId}`;
  return {
    channel_id: channelId,
    observation_kind: kind,
    cursor: {
      channel_id: channelId,
      observation_kind: kind,
      latest_sequence: "1",
      latest_observation_id: observationId,
      latest_observed_at: observedAt,
      latest_complete_observation_id: observationId,
      latest_complete_observed_at: observedAt,
      current_facts_hash: factsHash,
      source_cursor: stopReason ? { terminal_reason: stopReason } : {},
    },
    latest_observation: {
      observation_id: observationId,
      observed_at: observedAt,
      channel_id: channelId,
      observation_kind: kind,
      outcome: "complete",
      outcome_reason_code: `${kind}_complete`,
      facts_hash: factsHash,
    },
    complete_observation: {
      observation_id: observationId,
      observed_at: observedAt,
      channel_id: channelId,
      run_id: runId,
      observation_kind: kind,
      kind_sequence: "1",
      outcome: "complete",
      outcome_reason_code: `${kind}_complete`,
      facts_hash: factsHash,
      crawler_version: "integration-test",
      extractor_versions: {},
      result_summary_json: {
        ...(kind === "video" ? {
          discovery: {
            items: 0,
            pages: 1,
            stop_reason: stopReason,
            parse_gap_count: 0,
            detail_failure_count: 0,
          },
        } : {}),
        ...resultSummary,
      },
    },
    run: {
      run_id: runId,
      channel_id: channelId,
      status: "done",
      crawl_mode: "full",
      plan_id: null,
      policy_version: "v16-rule-6",
      crawler_version: "integration-test",
    },
  };
}

export function completePublicationOperationalFixture(channelId, {
  observedAt = DEFAULT_OBSERVED_AT,
} = {}) {
  const aboutHash = `sha256:${"a".repeat(64)}`;
  const channel = {
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    title: "Publication Ready",
    handle: "@publicationready",
    avatar_url: "https://yt3.example/avatar.jpg",
    keywords: ["publication", "testing"],
    available_tabs: ["videos", "shorts", "live"],
    about_description: "A complete channel description.",
    country: "Brazil",
    country_code: "BR",
    country_canonical_name: "Brazil",
    joined_date_text: "Joined Jan 1, 2020",
    joined_at: "2020-01-01",
    joined_at_precision: "date_only",
    subscriber_count: "1000",
    subscriber_count_text: "1K subscribers",
    subscriber_count_status: "exact",
    subscriber_count_source: "youtube_about",
    total_view_count: "50000",
    total_view_count_text: "50,000 views",
    total_view_count_status: "exact",
    total_view_count_source: "youtube_about",
    total_video_count: "50",
    total_video_count_text: "50 videos",
    total_video_count_status: "exact",
    total_video_count_source: "youtube_about",
    is_verified: true,
    is_verified_status: "verified",
    external_links: [{
      title: "Website",
      display_url: "example.com",
      url: "https://example.com/",
    }],
    status: "active",
    source_json: {
      channel_header: {
        rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
        vanity_channel_url: "https://www.youtube.com/@publicationready",
        is_family_safe: true,
      },
    },
    about_last_observed_at: observedAt,
    about_current_hash: aboutHash,
    about_identity_last_observed_at: observedAt,
    about_identity_current_hash: `sha256:${"b".repeat(64)}`,
  };
  const agentConfig = {
    config_id: 7,
    provider: "openai-compatible",
    model: "agent-test",
    prompt_template_id: 10,
    template_text: AGENT_PROMPT,
    tools_json: [{ type: "web_search" }],
  };
  const metrics = {
    audience_profile_agent: Object.fromEntries(AGENT_FACT_KEYS.map((field) => [field, {
      value: agentFactValue(field),
      confidence: "high",
      evidence: [`Evidence for ${field}`],
      source_urls: [`https://www.youtube.com/channel/${channelId}`],
      reason: null,
      source: "crawler",
    }])),
  };
  const agentRun = buildAgentPublicationRun({
    agentConfig,
    agentModel: agentConfig.model,
    promptVariant: "country_resolved",
    inputContentIds: [],
  });
  const outputHash = observationFactsHash(metrics);
  const agent = {
    channel_id: channelId,
    agent_mode: "basic",
    input_url: `https://www.youtube.com/channel/${channelId}`,
    status: "success",
    metrics_json: metrics,
    ...agentRun,
    current_output_hash: outputHash,
    last_observation_id: `agent-observation-${channelId}`,
    last_observed_at: observedAt,
  };
  return {
    channel,
    contents: [],
    agent,
    agentConfig,
    sources: [
      source("about", channelId, observedAt, { factsHash: aboutHash }),
      source("video", channelId, observedAt, {
        factsHash: `sha256:${"c".repeat(64)}`,
        stopReason: "list_end",
      }),
      source("agent", channelId, observedAt, {
        factsHash: outputHash,
        resultSummary: {
          output_hash: outputHash,
          input_content_hash: agent.input_content_hash,
          agent_version_hash: agent.agent_version_hash,
        },
      }),
    ],
  };
}
