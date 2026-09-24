const CHANNEL_ID = "UCprojectionAdapterFixture";
const BATCH_ID = "publication_projection_fixture";
const CAPTURED_AT = "2026-07-30T12:00:00.000Z";
const FACT_KEYS = [
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

function currentRow(payload, overrides = {}) {
  return {
    channel_id: CHANNEL_ID,
    publication_stream_id: "7830def3-165b-4829-815c-b677faa7f9c6",
    active_sequence: 1,
    active_revision_id: "10000000-0000-4000-8000-000000000001",
    result_hash: `sha256:${"1".repeat(64)}`,
    payload_json: payload,
    source_observed_at: CAPTURED_AT,
    activated_at: CAPTURED_AT,
    created_at: CAPTURED_AT,
    updated_at: CAPTURED_AT,
    ...overrides,
  };
}

function channelPayload(overrides = {}) {
  return {
    channel_id: CHANNEL_ID,
    title: "Projection Fixture",
    canonical_url: `https://www.youtube.com/channel/${CHANNEL_ID}`,
    vanity_channel_url: null,
    handle: "@projectionfixture",
    avatar: [{ url: "https://example.test/avatar.jpg", position: 0 }],
    rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${CHANNEL_ID}`,
    keywords: ["fixture"],
    is_family_safe: true,
    is_verified: false,
    is_verified_status: "not_verified",
    has_videos: true,
    has_shorts: false,
    has_live_streams: false,
    description: "Projection adapter fixture",
    subscriber_count: 100,
    subscriber_count_status: "estimated",
    total_video_count: 2,
    total_video_count_status: "exact",
    total_view_count: 1000,
    total_view_count_status: "exact",
    joined_date: "2020-01-01",
    joined_date_status: "exact",
    joined_date_raw: "Joined Jan 1, 2020",
    country_code: "US",
    country_name: "United States",
    links: [{
      title: "Site",
      display_url: "example.test",
      target_url: "https://example.test/",
      favicon_url: null,
      position: 0,
      link_type: "website",
      purpose: "public_reference",
    }],
    lifecycle_status: "active",
    ...overrides,
  };
}

function contentPayload(overrides = {}) {
  return {
    content_id: "video-fixture-1",
    content_key: `${CHANNEL_ID}:video:video-fixture-1`,
    kind: "video",
    title: "Video fixture",
    url: "https://www.youtube.com/watch?v=video-fixture-1",
    thumbnail_url: null,
    published_at: "2026-07-29T10:00:00.000Z",
    published_date: "2026-07-29",
    published_at_precision: "second",
    published_at_status: "exact",
    published_at_source: "youtubejs",
    duration_seconds: 60,
    duration_status: "exact",
    duration_source: "youtubejs",
    view_count: 100,
    view_count_status: "exact",
    view_count_source: "youtubejs",
    view_count_observed_at: CAPTURED_AT,
    like_count: 10,
    like_count_status: "zero_from_empty",
    like_count_source: "youtubejs",
    like_count_observed_at: CAPTURED_AT,
    comment_count: 5,
    comment_count_status: "exact",
    comment_count_source: "youtubejs",
    comment_count_observed_at: CAPTURED_AT,
    comments_disabled: false,
    description: "Video fixture",
    description_status: "exact",
    description_source: "youtubejs",
    hashtags: ["#fixture"],
    keywords: ["fixture"],
    access_status: "public",
    access_status_source: "youtubejs",
    is_members_only: false,
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "fixture-v1",
    ...overrides,
  };
}

function agentPayload() {
  const values = {
    country: "United States",
    creator_language: "English",
    creator_gender: "brand_team",
    creator_age_range: 35,
    audience_region: [{ region: "United States", percentage: 80 }],
    audience_language: [{ language: "English", percentage: 90 }],
    audience_age_gender: [{ age_range: "25-34", male: 50, female: 50 }],
    active_subscriber_ratio: 20,
    channel_categories: { level_1: "News", level_2: ["Local News"] },
    channel_tags: { tags: ["local"] },
  };
  return {
    channel_id: CHANNEL_ID,
    facts: Object.fromEntries(FACT_KEYS.map((key) => [key, {
      value: values[key],
      confidence: "high",
      evidence: ["fixture"],
      source_urls: ["https://example.test/"],
      reason: null,
      source: "integration-test",
    }])),
    agent_model: "fixture-model",
    agent_config_id: 7,
    prompt_template_id: 10,
    prompt_hash: "a".repeat(64),
    output_hash: `sha256:${"2".repeat(64)}`,
  };
}

function versionVector() {
  return {
    channel: { sequence: 1, revision_id: "channel-revision", result_hash: "channel-hash" },
    video: { sequence: 1, revision_id: "video-revision", result_hash: "video-hash" },
    agent: { sequence: 1, revision_id: "agent-revision", result_hash: "agent-hash" },
  };
}

export function completeInput() {
  const content = contentPayload();
  return {
    channelId: CHANNEL_ID,
    batchId: BATCH_ID,
    capturedAt: CAPTURED_AT,
    versionVector: versionVector(),
    current: {
      channel: currentRow(channelPayload()),
      video: currentRow({}, {
        active_revision_id: "10000000-0000-4000-8000-000000000002",
        window_policy: {},
        window_proof: {},
      }),
      contents: [currentRow(content, {
        content_id: content.content_id,
        item_hash: `sha256:${"3".repeat(64)}`,
        position: 1,
      })],
      agent: currentRow(agentPayload(), {
        active_revision_id: "10000000-0000-4000-8000-000000000003",
      }),
    },
    previous: { snapshot: null, links: [], contents: [], facts: [] },
  };
}


export { CHANNEL_ID, BATCH_ID, CAPTURED_AT, FACT_KEYS, currentRow, channelPayload, contentPayload, agentPayload, versionVector };
