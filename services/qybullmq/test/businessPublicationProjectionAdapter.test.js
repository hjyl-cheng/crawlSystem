import assert from "node:assert/strict";
import test from "node:test";
import {
  buildBusinessPublicationProjection,
  calculateBusinessProjectionMetrics,
} from "../src/businessPublicationProjectionAdapter.js";

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

function completeInput() {
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

test("Projection Adapter keeps Channel metric time when a later Video activation triggers projection", () => {
  const input = completeInput();
  const channelObservedAt = "2026-07-30T00:36:11.330Z";
  const laterVideoActivation = "2026-07-31T00:35:15.369Z";
  input.current.channel.source_observed_at = channelObservedAt;
  input.current.channel.activated_at = "2026-07-30T00:36:12.019Z";
  input.current.video.activated_at = laterVideoActivation;
  input.capturedAt = laterVideoActivation;

  const result = buildBusinessPublicationProjection(input);

  assert.equal(result.capturedAt, laterVideoActivation);
  assert.equal(result.snapshot.captured_at, laterVideoActivation);
  assert.equal(result.snapshot.channel_observed_at, channelObservedAt);
  assert.equal(result.snapshot.subscriber_count_observed_at, channelObservedAt);
  assert.equal(result.snapshot.total_view_count_observed_at, channelObservedAt);
  assert.equal(result.snapshot.video_count_observed_at, channelObservedAt);
  assert.equal(result.snapshot.raw_channel.source_observed_at, channelObservedAt);
});

test("Projection Adapter rejects Channel metrics without a source Observation time", () => {
  const input = completeInput();
  delete input.current.channel.source_observed_at;
  assert.throws(
    () => buildBusinessPublicationProjection(input),
    /result\.entity_current\.source_observed_at must be a timestamp/,
  );
});

test("Projection Adapter carries the stored Channel Observation time into a later composite snapshot", () => {
  const original = buildBusinessPublicationProjection(completeInput()).snapshot;
  const input = completeInput();
  const channelObservedAt = "2026-07-29T00:36:11.330Z";
  input.current.channel = null;
  input.current.video.activated_at = "2026-07-31T00:35:15.369Z";
  input.previous.snapshot = {
    ...original,
    channel_observed_at: channelObservedAt,
    subscriber_count_observed_at: channelObservedAt,
    total_view_count_observed_at: channelObservedAt,
    video_count_observed_at: channelObservedAt,
    raw_channel: {
      ...original.raw_channel,
      source_observed_at: "2026-07-30T00:36:11.330Z",
    },
  };

  const result = buildBusinessPublicationProjection(input);

  assert.equal(result.snapshot.channel_observed_at, channelObservedAt);
  assert.equal(result.snapshot.subscriber_count_observed_at, channelObservedAt);
  assert.equal(result.snapshot.raw_channel.source_observed_at, channelObservedAt);
  assert.equal(result.snapshot.raw_channel.carried_forward_from_snapshot_id, original.id);
});

test("Projection Adapter builds a deterministic complete business snapshot", () => {
  const first = buildBusinessPublicationProjection(completeInput());
  const second = buildBusinessPublicationProjection(completeInput());
  assert.deepEqual(second, first);
  assert.equal(first.action, "upsert");
  assert.equal(first.snapshot.channel_id, CHANNEL_ID);
  assert.equal(first.snapshot.is_verified, false);
  assert.equal(first.snapshot.is_verified_status, "not_verified");
  assert.equal(first.snapshot.youtube_business_email_available, null);
  assert.equal(first.snapshot.youtube_business_email_observed_at, null);
  assert.equal(first.snapshot.subscriber_count_status, "approximate");
  assert.equal(first.links.length, 1);
  assert.equal(first.contents.length, 1);
  assert.equal(first.contents[0].content_kind, "videos");
  assert.equal(first.contents[0].like_count_status, "exact");
  assert.equal(first.facts.length, 10);
  assert.deepEqual(
    first.facts.find((fact) => fact.field_key === "channel_categories").value_json,
    ["News", "Local News"],
  );
  assert.equal(first.metrics.length, 77);
  assert.equal(first.snapshot.candidate_last_published_date, "2026-07-29");
});

test("Projection Adapter preserves unlisted access in the business snapshot", () => {
  const input = completeInput();
  input.current.contents[0].payload_json = contentPayload({
    access_status: "unlisted",
    access_status_source: "youtubejs_microformat",
  });

  const result = buildBusinessPublicationProjection(input);

  assert.equal(result.contents[0].access_status, "unlisted");
  assert.equal(result.contents[0].access_status_source, "youtubejs_microformat");
});

test("Projection Adapter publishes disabled comments as an exact zero", () => {
  const input = completeInput();
  input.current.contents[0].payload_json = contentPayload({
    comment_count: 0,
    comment_count_status: "disabled",
    comments_disabled: true,
  });

  const result = buildBusinessPublicationProjection(input);

  assert.deepEqual({
    comment_count: result.contents[0].comment_count,
    comment_count_status: result.contents[0].comment_count_status,
    comments_disabled: result.contents[0].comments_disabled,
  }, {
    comment_count: 0,
    comment_count_status: "exact",
    comments_disabled: true,
  });
});

test("Projection Adapter upgrades a legacy disabled null to an exact zero", () => {
  const input = completeInput();
  input.current.contents[0].payload_json = contentPayload({
    comment_count: null,
    comment_count_status: "disabled",
    comments_disabled: true,
  });

  const result = buildBusinessPublicationProjection(input);

  assert.equal(result.contents[0].comment_count, 0);
  assert.equal(result.contents[0].comment_count_status, "exact");
  assert.equal(result.contents[0].comments_disabled, true);
});

test("Projection Adapter rejects a nonzero disabled comment count", () => {
  const input = completeInput();
  input.current.contents[0].payload_json = contentPayload({
    comment_count: 1,
    comment_count_status: "disabled",
    comments_disabled: true,
  });

  assert.throws(
    () => buildBusinessPublicationProjection(input),
    /disabled Comments require comment_count=0/,
  );
});

test("Projection Adapter publishes a confirmed business email removal and carries it forward", () => {
  const availableInput = completeInput();
  availableInput.current.channel.payload_json = channelPayload({
    youtube_business_email_available: true,
    youtube_business_email_observed_at: "2026-07-30T11:00:00.000Z",
  });
  const available = buildBusinessPublicationProjection(availableInput);
  assert.equal(available.snapshot.youtube_business_email_available, true);
  assert.equal(
    available.snapshot.youtube_business_email_observed_at,
    "2026-07-30T11:00:00.000Z",
  );

  const removedInput = completeInput();
  removedInput.capturedAt = "2026-07-31T12:00:00.000Z";
  removedInput.current.channel.source_observed_at = "2026-07-31T11:00:00.000Z";
  removedInput.current.channel.payload_json = channelPayload({
    youtube_business_email_available: false,
    youtube_business_email_observed_at: "2026-07-31T11:00:00.000Z",
  });
  const removed = buildBusinessPublicationProjection(removedInput);
  assert.equal(removed.snapshot.youtube_business_email_available, false);
  assert.equal(
    removed.snapshot.youtube_business_email_observed_at,
    "2026-07-31T11:00:00.000Z",
  );

  const videoOnlyInput = completeInput();
  videoOnlyInput.capturedAt = "2026-08-01T12:00:00.000Z";
  videoOnlyInput.current.channel = null;
  videoOnlyInput.previous.snapshot = removed.snapshot;
  const videoOnly = buildBusinessPublicationProjection(videoOnlyInput);
  assert.equal(videoOnly.snapshot.youtube_business_email_available, false);
  assert.equal(
    videoOnly.snapshot.youtube_business_email_observed_at,
    "2026-07-31T11:00:00.000Z",
  );
});

test("Projection Adapter rejects incomplete business email state pairs", () => {
  const input = completeInput();
  input.current.channel.payload_json = channelPayload({
    youtube_business_email_available: true,
  });
  assert.throws(
    () => buildBusinessPublicationProjection(input),
    /business email availability and observation time must both be present/,
  );
});

test("Projection Adapter preserves unknown verification instead of coercing false", () => {
  const input = completeInput();
  input.current.channel.payload_json = channelPayload({
    is_verified: null,
    is_verified_status: "unknown",
  });
  const result = buildBusinessPublicationProjection(input);
  assert.equal(result.snapshot.is_verified, null);
  assert.equal(result.snapshot.is_verified_status, "unknown");
});

test("Projection Adapter carries forward absent Video and Agent domains", () => {
  const input = completeInput();
  input.current.video = null;
  input.current.contents = [];
  input.current.agent = null;
  input.previous.contents = [{
    id: "old-content",
    channel_snapshot_id: "old-snapshot",
    video_id: "old-video",
    content_kind: "shorts",
    title: "Old short",
    thumbnail_url: null,
    published_text: "2026-07-20",
    published_date: "2026-07-20",
    view_count_text: "10",
    view_count: 10,
    like_count: 1,
    comment_count: 0,
    length_text: "10",
    duration_seconds: 10,
    url: "https://www.youtube.com/shorts/old-video",
    raw_item: {},
    source_content_key: `${CHANNEL_ID}:short:old-video`,
    source_run_id: null,
    source_content_type: "short",
    published_at: "2026-07-20T10:00:00.000Z",
    published_at_status: "exact",
    published_at_source: "legacy",
    is_recent: true,
    is_canonical: true,
    view_count_status: "exact",
    source_first_seen_at: "2026-07-20T10:00:00.000Z",
    source_last_seen_at: "2026-07-20T10:00:00.000Z",
    source_last_enriched_at: "2026-07-20T10:00:00.000Z",
    parse_status: {},
    like_count_status: "exact",
    comment_count_status: "exact",
    duration_status: "exact",
    view_count_observed_at: "2026-07-20T10:00:00.000Z",
    like_count_observed_at: "2026-07-20T10:00:00.000Z",
    comment_count_observed_at: "2026-07-20T10:00:00.000Z",
    source_url: "https://www.youtube.com/shorts/old-video",
    channel_id: CHANNEL_ID,
    comments_disabled: false,
    is_members_only: false,
    access_status: "public",
    access_status_source: "legacy",
    source_position: 1,
    published_at_precision: "second",
    duration_source: "legacy",
    view_count_source: "legacy",
    like_count_source: "legacy",
    comment_count_source: "legacy",
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "legacy",
    description: "",
    description_status: "empty",
    description_source: "legacy",
    hashtags: [],
    keywords: [],
    captured_at: "2026-07-20T10:00:00.000Z",
    item_first_seen_at: "2026-07-20T10:00:00.000Z",
    item_last_seen_at: "2026-07-20T10:00:00.000Z",
  }];
  input.previous.facts = [{
    id: "old-fact",
    channel_id: CHANNEL_ID,
    channel_snapshot_id: "old-snapshot",
    field_key: "country",
    value_json: "Brazil",
    source: "legacy",
    confidence: "high",
    evidence: [],
    source_urls: [],
    reason: null,
    provenance: {},
  }];
  const result = buildBusinessPublicationProjection(input);
  assert.equal(result.contents[0].video_id, "old-video");
  assert.equal(result.contents[0].content_kind, "shorts");
  assert.equal(result.facts[0].value_json, "Brazil");
  assert.equal(result.contents[0].parse_status.carried_forward, true);
  assert.equal(result.facts[0].provenance.carried_forward_from_fact_id, "old-fact");
});

test("Projection Adapter emits a removal without manufacturing a tombstone snapshot", () => {
  const input = completeInput();
  input.current.channel.is_retracted = true;
  const result = buildBusinessPublicationProjection(input);
  assert.equal(result.action, "remove");
  assert.equal(Object.hasOwn(result, "snapshot"), false);
});

test("Projection metrics recompute exact averages, medians, ratios, and empty scopes", () => {
  const contents = [
    {
      content_kind: "videos",
      published_at: "2026-07-29T10:00:00.000Z",
      published_date: "2026-07-29",
      view_count: 100,
      view_count_status: "exact",
      like_count: 10,
      like_count_status: "exact",
      comment_count: 5,
      comment_count_status: "exact",
    },
    {
      content_kind: "shorts",
      published_at: "2026-07-28T10:00:00.000Z",
      published_date: "2026-07-28",
      view_count: 300,
      view_count_status: "exact",
      like_count: 30,
      like_count_status: "exact",
      comment_count: 15,
      comment_count_status: "exact",
    },
  ];
  const metrics = calculateBusinessProjectionMetrics({
    channelId: CHANNEL_ID,
    snapshotId: "snapshot",
    capturedAt: CAPTURED_AT,
    contents,
    subscriberCount: 100,
    subscriberStatus: "exact",
  });
  const find = (key, scope = "all") => metrics.find((row) => (
    row.metric_key === key && row.scope === scope
  ));
  assert.equal(find("average_views").value_numeric, 200);
  assert.equal(find("median_views").value_numeric, 200);
  assert.equal(find("views_subscribers_ratio").value_numeric, 200);
  assert.equal(find("engagement_rate_by_views").value_numeric, 15);
  assert.equal(find("content_count", "lives").value_numeric, 0);
  assert.equal(find("average_views", "lives").value_status, "unavailable");
});
