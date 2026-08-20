import assert from "node:assert/strict";
import test from "node:test";
import {
  AGENT_FACT_KEYS,
  buildAgentReadiness,
  buildChannelReadiness,
  generatePublicationReadinessReport,
} from "../src/publicationReadinessReport.js";
import { buildAgentPublicationRun } from "../src/agentPublicationCurrent.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { buildVideoPublicationItem } from "../src/videoPublicationCurrent.js";

const AS_OF = "2026-07-26T12:00:00.000Z";
const CHANNEL_ID = "UCpublication-ready";
const AGENT_PROMPT = "Analyze each input URL and return the V1 Agent facts.";

function agentConfig() {
  return {
    config_id: 7,
    provider: "openai-compatible",
    model: "agent-test",
    prompt_template_id: 10,
    template_text: AGENT_PROMPT,
    tools_json: [{ type: "web_search" }],
  };
}

function domainSource(kind, {
  channelId = CHANNEL_ID,
  observationId = `${kind}-observation`,
  factsHash = `sha256:${kind.padEnd(64, "0").slice(0, 64)}`,
  stopReason = null,
  observedAt = "2026-07-26T10:00:00.000Z",
  resultSummary = {},
} = {}) {
  const runId = `${kind}-run`;
  return {
    channel_id: channelId,
    observation_kind: kind,
    cursor: {
      channel_id: channelId,
      observation_kind: kind,
      latest_sequence: "2",
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
      kind_sequence: "2",
      outcome: "complete",
      outcome_reason_code: `${kind}_complete`,
      facts_hash: factsHash,
      crawler_version: "qy-v16",
      extractor_versions: {},
      result_summary_json: {
        ...(kind === "video" ? {
          discovery: {
            items: 1,
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
      crawl_mode: "incremental",
      plan_id: `${kind}-plan`,
      policy_version: "v16-rule-6",
      crawler_version: "qy-v16",
    },
  };
}

function channelRow(overrides = {}) {
  const aboutHash = "sha256:" + "a".repeat(64);
  return {
    channel_id: CHANNEL_ID,
    channel_url: `https://www.youtube.com/channel/${CHANNEL_ID}`,
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
      url: "https://example.com/?utm_source=youtube",
    }],
    status: "active",
    source_json: {
      channel_header: {
        rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${CHANNEL_ID}`,
        vanity_channel_url: "https://www.youtube.com/@publicationready",
        is_family_safe: true,
      },
    },
    about_last_observed_at: "2026-07-26T10:00:00.000Z",
    about_current_hash: aboutHash,
    profile_last_observed_at: "2026-07-26T10:00:00.000Z",
    profile_current_hash: "sha256:" + "b".repeat(64),
    ...overrides,
  };
}

function videoRow(index = 1, overrides = {}) {
  const contentId = `video-${String(index).padStart(2, "0")}`;
  const row = {
    channel_id: CHANNEL_ID,
    content_key: `${CHANNEL_ID}:video:${contentId}`,
    source_content_id: contentId,
    content_type: "video",
    title: `Video ${index}`,
    url: `https://www.youtube.com/watch?v=${contentId}`,
    thumbnail_url: `https://i.ytimg.com/vi/${contentId}/hqdefault.jpg`,
    published_at: `2026-07-${String(Math.max(1, 26 - index)).padStart(2, "0")}T08:00:00.000Z`,
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtube_player",
    duration_seconds: 120,
    duration_status: "exact",
    duration_source: "youtube_player",
    view_count: 1000 + index,
    view_count_status: "exact",
    view_count_source: "youtube_player",
    like_count: 100,
    like_count_status: "exact",
    like_count_source: "youtube_player",
    comment_count: 10,
    comment_count_status: "exact",
    comment_count_source: "youtube_next",
    comments_disabled: false,
    description: "",
    description_status: "empty",
    description_source: "youtube_player",
    hashtags: ["test"],
    keywords: ["publication"],
    access_status: "public",
    access_status_source: "youtube_player",
    is_members_only: false,
    extractor_version: "youtubei.js@test",
    run_id: "video-run",
    last_observation_id: "video-observation",
    playlist_last_seen_at: "2026-07-26T10:00:00.000Z",
    player_last_observed_at: "2026-07-26T10:00:00.000Z",
    ...overrides,
  };
  if (!Object.prototype.hasOwnProperty.call(overrides, "publication_item_hash")) {
    row.publication_item_hash = buildVideoPublicationItem(row, {
      channelId: row.channel_id,
    }).item_hash;
  }
  return row;
}

function factValue(field) {
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

function agentMetrics() {
  return {
    audience_profile_agent: Object.fromEntries(AGENT_FACT_KEYS.map((field) => [field, {
      value: factValue(field),
      confidence: "high",
      evidence: [`Evidence for ${field}`],
      source_urls: [`https://www.youtube.com/channel/${CHANNEL_ID}`],
      reason: null,
      source: "crawler",
    }])),
  };
}

function agentRow(metrics = agentMetrics(), overrides = {}) {
  const run = buildAgentPublicationRun({
    agentConfig: agentConfig(),
    promptVariant: "country_resolved",
    inputContentIds: [],
  });
  return {
    channel_id: CHANNEL_ID,
    agent_mode: "basic",
    input_url: `https://www.youtube.com/channel/${CHANNEL_ID}`,
    status: "success",
    metrics_json: metrics,
    ...run,
    current_output_hash: observationFactsHash(metrics),
    last_observation_id: "agent-observation",
    last_observed_at: "2026-07-26T10:00:00.000Z",
    ...overrides,
  };
}

function agentSource(row) {
  return domainSource("agent", {
    factsHash: observationFactsHash(row.metrics_json),
    resultSummary: {
      output_hash: row.current_output_hash,
      input_content_hash: row.input_content_hash,
      agent_version_hash: row.agent_version_hash,
    },
  });
}

function completeFixture() {
  const channel = channelRow();
  const metrics = agentMetrics();
  const agent = agentRow(metrics);
  return {
    channel,
    contents: [videoRow()],
    agent,
    sources: [
      domainSource("about", { factsHash: channel.about_current_hash }),
      domainSource("video", { factsHash: "sha256:" + "v".repeat(64), stopReason: "list_end" }),
      agentSource(agent),
    ],
    business: {
      channel_id: CHANNEL_ID,
      search: { channel_id: CHANNEL_ID, watermark: "release-1", name: channel.title },
      snapshot: {
        channel_id: CHANNEL_ID,
        title: channel.title,
        handle: channel.handle,
        avatar_url: channel.avatar_url,
        description: channel.about_description,
        is_verified: true,
        subscriber_count: 1000,
        total_view_count: 50000,
        video_count: 50,
        joined_date: "2020-01-01",
      },
      link_count: 1,
      fact_keys: [...AGENT_FACT_KEYS],
      content_count: 1,
    },
  };
}

function queryFixtures(fixture) {
  const crawlerCalls = [];
  const businessCalls = [];
  return {
    crawlerCalls,
    businessCalls,
    async crawlerQuery(sql) {
      crawlerCalls.push(sql);
      if (sql.includes("publication-readiness:channels")) return { rows: [{ row: fixture.channel }] };
      if (sql.includes("publication-readiness:contents")) return { rows: fixture.contents.map((row) => ({ row })) };
      if (sql.includes("publication-readiness:agents")) {
        return { rows: [{ row: fixture.agent, config: agentConfig() }] };
      }
      if (sql.includes("publication-readiness:sources")) return { rows: fixture.sources };
      throw new Error(`unexpected Crawler query: ${sql}`);
    },
    async businessQuery(sql) {
      businessCalls.push(sql);
      if (sql.includes("publication-readiness:business-current")) return { rows: [fixture.business] };
      throw new Error(`unexpected Business query: ${sql}`);
    },
  };
}

test("a complete three-Domain Current is Baseline eligible with five bulk queries", async () => {
  const fixture = completeFixture();
  const queries = queryFixtures(fixture);
  const report = await generatePublicationReadinessReport({
    crawlerQuery: queries.crawlerQuery,
    businessQuery: queries.businessQuery,
    asOf: AS_OF,
  });

  assert.equal(queries.crawlerCalls.length, 4);
  assert.equal(queries.businessCalls.length, 1);
  assert.equal(report.summary.total_channels, 1);
  assert.equal(report.summary.baseline_eligible, 1);
  assert.equal(report.channels[0].baseline_eligible, true);
  assert.match(report.channels[0].domains.channel.result_hash, /^sha256:[0-9a-f]{64}$/);
  assert.equal(report.channels[0].domains.channel.payload.channel_id, CHANNEL_ID);
  assert.equal(report.channels[0].domains.video.payload.channel_id, CHANNEL_ID);
  assert.match(report.channels[0].domains.video.items[0].item_hash, /^sha256:[0-9a-f]{64}$/);
  assert.match(report.channels[0].domains.agent.result_hash, /^sha256:[0-9a-f]{64}$/);
});

test("invalid optional Links and unknown Verified are warnings, not a Channel-wide rejection", () => {
  const row = channelRow({
    is_verified: undefined,
    is_verified_status: undefined,
    external_links: [{ title: "Website", display_url: "example.com", url: null }],
  });
  const result = buildChannelReadiness({
    row,
    source: domainSource("about", { factsHash: row.about_current_hash }),
  });

  assert.equal(result.ready, true);
  assert.equal(result.links.valid_count, 0);
  assert.equal(result.warnings.some((item) => item.code === "channel_link_target_missing"), true);
  assert.equal(result.warnings.some((item) => item.field === "is_verified"), true);
});

test("Channel hashes ignore temporary image CDN query parameters", () => {
  const source = domainSource("about", { factsHash: channelRow().about_current_hash });
  const first = buildChannelReadiness({
    row: channelRow({ avatar_url: "https://yt3.googleusercontent.com/avatar?token=first" }),
    source,
  });
  const second = buildChannelReadiness({
    row: channelRow({ avatar_url: "https://yt3.googleusercontent.com/avatar?token=second" }),
    source,
  });
  assert.equal(first.ready, true);
  assert.equal(first.result_hash, second.result_hash);
});

test("a complete Domain Observation is publishable before its enclosing Run finishes", () => {
  const row = channelRow();
  const source = domainSource("about", { factsHash: row.about_current_hash });
  source.run.status = "running";

  const current = buildChannelReadiness({ row, source });

  assert.equal(current.ready, true);
  assert.equal(current.source_refs.run.status, "running");
});


test("Agent readiness checks every audit Key and accepts a zero ratio", () => {
  const completeMetrics = agentMetrics();
  const completeRow = agentRow(completeMetrics);
  const complete = buildAgentReadiness({
    row: completeRow,
    config: agentConfig(),
    source: agentSource(completeRow),
  });
  assert.equal(complete.ready, true);
  assert.equal(complete.facts.find((fact) => fact.field === "active_subscriber_ratio").complete, true);

  const incompleteMetrics = agentMetrics();
  delete incompleteMetrics.audience_profile_agent.country.reason;
  const incompleteRow = agentRow(incompleteMetrics);
  const incomplete = buildAgentReadiness({
    row: incompleteRow,
    config: agentConfig(),
    source: agentSource(incompleteRow),
  });
  assert.equal(incomplete.ready, false);
  assert.deepEqual(
    incomplete.facts.find((fact) => fact.field === "country").missing_keys,
    ["reason"],
  );
});

test("a Business field completeness regression blocks Baseline eligibility", async () => {
  const fixture = completeFixture();
  fixture.channel = channelRow({ avatar_url: null, source_json: {
    channel_header: {
      rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${CHANNEL_ID}`,
      vanity_channel_url: "https://www.youtube.com/@publicationready",
      is_family_safe: true,
    },
  } });
  fixture.sources[0] = domainSource("about", { factsHash: fixture.channel.about_current_hash });
  const queries = queryFixtures(fixture);
  const report = await generatePublicationReadinessReport({
    crawlerQuery: queries.crawlerQuery,
    businessQuery: queries.businessQuery,
    asOf: AS_OF,
  });

  assert.equal(report.summary.baseline_eligible, 0);
  assert.equal(report.channels[0].business_regression.passed, false);
  assert.equal(
    report.channels[0].business_regression.regressions.some((item) => item.field === "avatar_url"),
    true,
  );
  assert.equal(
    report.channels[0].readiness_reasons.some((item) => item.code === "business_field_completeness_regression"),
    true,
  );
});

test("large metric and collection drops block Baseline eligibility", async () => {
  const fixture = completeFixture();
  fixture.business.snapshot.subscriber_count = 3000;
  fixture.business.link_count = 3;
  fixture.business.content_count = 30;
  const queries = queryFixtures(fixture);
  const report = await generatePublicationReadinessReport({
    crawlerQuery: queries.crawlerQuery,
    businessQuery: queries.businessQuery,
    asOf: AS_OF,
  });

  assert.equal(report.channels[0].business_regression.passed, false);
  assert.deepEqual(
    report.channels[0].business_regression.regressions
      .filter((item) => item.reason === "large_drop")
      .map((item) => item.field),
    ["links", "subscriber_count", "video_window"],
  );
  assert.equal(report.policies.business_large_drop_ratio, 0.5);
  assert.equal(
    report.channels[0].readiness_reasons
      .some((item) => item.code === "business_field_large_drop_regression"),
    true,
  );
  assert.equal(report.channels[0].baseline_eligible, false);
});
