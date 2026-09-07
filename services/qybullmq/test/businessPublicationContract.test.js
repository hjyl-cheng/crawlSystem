import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import {
  BusinessPublicationContractError,
  validateBusinessPublicationEnvelope,
} from "../src/businessPublicationContract.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { publicationEnvelopeFromRow } from "../src/publicationTransport.js";

function channelPayload(channelId) {
  return {
    channel_id: channelId,
    title: "Business Contract",
    canonical_url: `https://www.youtube.com/channel/${channelId}`,
    vanity_channel_url: null,
    handle: null,
    avatar: [],
    rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
    keywords: [],
    is_family_safe: true,
    is_verified: false,
    is_verified_status: "observed_false",
    has_videos: true,
    has_shorts: false,
    has_live_streams: false,
    description: "Contract fixture",
    subscriber_count: 1,
    subscriber_count_status: "exact",
    total_video_count: 1,
    total_video_count_status: "exact",
    total_view_count: 1,
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

function channelEnvelope(payload = channelPayload("UCbusinesscontract"), overrides = {}) {
  return publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "bootstrap",
    channel_id: payload.channel_id,
    domain: "channel",
    data_sequence: 1,
    previous_data_sequence: null,
    operation: "replace",
    contract_version: 1,
    policy_version: "publication-policy-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: {},
    previous_result_hash: null,
    result_hash: observationFactsHash(payload),
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
    ...overrides,
  });
}

function channelV2Payload(channelId, overrides = {}) {
  return {
    ...channelPayload(channelId),
    youtube_business_email_available: true,
    youtube_business_email_observed_at: "2026-07-27T19:59:00.000Z",
    ...overrides,
  };
}

function videoItem(channelId, position = 5) {
  const payload = {
    content_id: "video-contract-1",
    content_key: `${channelId}:video-contract-1`,
    kind: "video",
    title: "Video Contract",
    url: "https://www.youtube.com/watch?v=video-contract-1",
    thumbnail_url: null,
    published_at: "2026-07-20T12:00:00.000Z",
    published_date: "2026-07-20",
    published_at_precision: "second",
    published_at_status: "exact",
    published_at_source: "youtubejs",
    duration_seconds: 60,
    duration_status: "exact",
    duration_source: "youtubejs",
    view_count: 10,
    view_count_status: "exact",
    view_count_source: "youtubejs",
    view_count_observed_at: "2026-07-27T20:00:00.000Z",
    like_count: 2,
    like_count_status: "exact",
    like_count_source: "youtubejs",
    like_count_observed_at: "2026-07-27T20:00:00.000Z",
    comment_count: 1,
    comment_count_status: "exact",
    comment_count_source: "youtubejs",
    comment_count_observed_at: "2026-07-27T20:00:00.000Z",
    comments_disabled: false,
    description: "Video contract fixture",
    description_status: "exact",
    description_source: "youtubejs",
    hashtags: [],
    keywords: [],
    access_status: "public",
    access_status_source: "youtubejs",
    is_members_only: false,
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "integration-test",
  };
  const businessValue = Object.fromEntries(Object.entries(payload).filter(([key]) => (
    !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
  return { position, item_hash: observationFactsHash(businessValue), ...payload };
}

function rehashVideoItem(item) {
  const businessValue = Object.fromEntries(Object.entries(item).filter(([key]) => (
    key !== "position"
    && key !== "item_hash"
    && !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
  return { ...item, item_hash: observationFactsHash(businessValue) };
}

test("Business Contract accepts an unknown optional comment count without comment bodies", () => {
  const channelId = "UCoptionalcomments";
  const item = rehashVideoItem({ ...videoItem(channelId), comment_count: null, comment_count_status: "unresolved" });
  assert.doesNotThrow(() => validateBusinessPublicationEnvelope(videoDeltaEnvelope(channelId, { upserts: [item] })));
  assert.equal(Object.hasOwn(item, "comments_first_page"), false);
});

function videoDeltaEnvelope(channelId, {
  upserts = [],
  retractions = [],
} = {}) {
  const resultHash = `sha256:${"e".repeat(64)}`;
  const payload = {
    channel_id: channelId,
    window_policy: {
      policy_version: "video-window-v1",
      as_of: "2026-07-27T20:00:00.000Z",
      cutoff_at: "2026-04-28T20:00:00.000Z",
      cutoff_date: "2026-04-28",
      max_age_days: 90,
      max_items: 30,
    },
    window_proof: {
      complete: true,
      terminal_condition: "list_end_confirmed",
      catalog_candidate_count: upserts.length + retractions.length,
      qualified_count: upserts.length,
      selected_count: upserts.length,
      excluded_count: retractions.length,
      latest_scan_items: upserts.length + retractions.length,
      latest_scan_pages: 1,
      latest_scan_stop_reason: "list_end",
      latest_scan_detail_failure_count: 0,
    },
    upserts,
    window_exits: [],
    retractions,
    result_hash: resultHash,
  };
  return publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "incremental",
    channel_id: channelId,
    domain: "video",
    data_sequence: 2,
    previous_data_sequence: 1,
    operation: "apply_window_delta",
    contract_version: 1,
    policy_version: "video-window-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: {},
    previous_result_hash: `sha256:${"d".repeat(64)}`,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
}

test("Business Contract validation independently checks the complete Channel Payload", () => {
  const valid = channelEnvelope();
  assert.equal(validateBusinessPublicationEnvelope(valid), valid);

  const incompletePayload = { ...valid.payload };
  delete incompletePayload.links;
  assert.throws(
    () => validateBusinessPublicationEnvelope(channelEnvelope(incompletePayload)),
    BusinessPublicationContractError,
  );

  assert.throws(
    () => validateBusinessPublicationEnvelope({
      ...valid,
      result_hash: `sha256:${"f".repeat(64)}`,
    }),
    (error) => error.code === "result_hash_mismatch",
  );
  assert.throws(
    () => validateBusinessPublicationEnvelope({
      ...valid,
      channel_id: "UCdifferent",
    }),
    (error) => error.code === "payload_channel_mismatch",
  );
});

test("Business Contract accepts Channel V1 and V2 while enforcing V2 email state pairs", () => {
  const legacy = channelEnvelope();
  assert.equal(validateBusinessPublicationEnvelope(legacy), legacy);

  const v2Payload = channelV2Payload("UCbusinesscontractv2");
  const v2 = channelEnvelope(v2Payload, { contract_version: 2 });
  assert.equal(validateBusinessPublicationEnvelope(v2), v2);

  const removedPayload = channelV2Payload("UCbusinesscontractremoved", {
    youtube_business_email_available: false,
  });
  const removed = channelEnvelope(removedPayload, { contract_version: 2 });
  assert.equal(validateBusinessPublicationEnvelope(removed), removed);

  const unknownPayload = channelV2Payload("UCbusinesscontractunknown", {
    youtube_business_email_available: null,
    youtube_business_email_observed_at: null,
  });
  const unknown = channelEnvelope(unknownPayload, { contract_version: 2 });
  assert.equal(validateBusinessPublicationEnvelope(unknown), unknown);

  assert.throws(
    () => validateBusinessPublicationEnvelope(channelEnvelope({
      ...v2Payload,
      youtube_business_email_observed_at: null,
    }, { contract_version: 2 })),
    /must both be known or unknown/,
  );
  assert.throws(
    () => validateBusinessPublicationEnvelope(channelEnvelope(v2Payload)),
    /Contract V1/,
  );
});

test("Business Contract accepts sparse canonical positions in Video Delta upserts", () => {
  const channelId = "UCbusinessvideodelta";
  const resultHash = `sha256:${"c".repeat(64)}`;
  const payload = {
    channel_id: channelId,
    window_policy: {
      policy_version: "video-window-v1",
      as_of: "2026-07-27T20:00:00.000Z",
      cutoff_at: "2026-04-28T20:00:00.000Z",
      cutoff_date: "2026-04-28",
      max_age_days: 90,
      max_items: 30,
    },
    window_proof: {
      complete: true,
      terminal_condition: "list_end_confirmed",
      catalog_candidate_count: 5,
      qualified_count: 5,
      selected_count: 5,
      excluded_count: 0,
      latest_scan_items: 5,
      latest_scan_pages: 1,
      latest_scan_stop_reason: "list_end",
      latest_scan_detail_failure_count: 0,
    },
    upserts: [videoItem(channelId, 5)],
    window_exits: [],
    retractions: [],
    result_hash: resultHash,
  };
  const envelope = publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "incremental",
    channel_id: channelId,
    domain: "video",
    data_sequence: 2,
    previous_data_sequence: 1,
    operation: "apply_window_delta",
    contract_version: 1,
    policy_version: "video-window-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: {},
    previous_result_hash: `sha256:${"b".repeat(64)}`,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
  assert.equal(validateBusinessPublicationEnvelope(envelope), envelope);
});

test("Business Contract accepts an unlisted Video upsert with a valid Item hash", () => {
  const channelId = "UCbusinessunlisted";
  const resultHash = `sha256:${"d".repeat(64)}`;
  const item = { ...videoItem(channelId, 1), access_status: "unlisted" };
  const businessValue = Object.fromEntries(Object.entries(item).filter(([key]) => (
    key !== "position"
    && key !== "item_hash"
    && !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
  item.item_hash = observationFactsHash(businessValue);
  const payload = {
    channel_id: channelId,
    window_policy: {
      policy_version: "video-window-v1",
      as_of: "2026-07-27T20:00:00.000Z",
      cutoff_at: "2026-04-28T20:00:00.000Z",
      cutoff_date: "2026-04-28",
      max_age_days: 90,
      max_items: 30,
    },
    window_proof: {
      complete: true,
      terminal_condition: "list_end_confirmed",
      catalog_candidate_count: 1,
      qualified_count: 1,
      selected_count: 1,
      excluded_count: 0,
      latest_scan_items: 1,
      latest_scan_pages: 1,
      latest_scan_stop_reason: "list_end",
      latest_scan_detail_failure_count: 0,
    },
    upserts: [item],
    window_exits: [],
    retractions: [],
    result_hash: resultHash,
  };
  const envelope = publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "incremental",
    channel_id: channelId,
    domain: "video",
    data_sequence: 2,
    previous_data_sequence: 1,
    operation: "apply_window_delta",
    contract_version: 1,
    policy_version: "video-window-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: {},
    previous_result_hash: `sha256:${"c".repeat(64)}`,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });

  assert.equal(validateBusinessPublicationEnvelope(envelope), envelope);
});

test("Business Contract still rejects a private Video upsert", () => {
  const channelId = "UCbusinessprivate";
  const item = rehashVideoItem({
    ...videoItem(channelId, 1),
    access_status: "private",
  });

  assert.throws(
    () => validateBusinessPublicationEnvelope(videoDeltaEnvelope(channelId, {
      upserts: [item],
    })),
    (error) => error.code === "payload_contract_invalid" && /public/.test(error.message),
  );
});

test("Business Contract rejects a source_unlisted Video Retraction", () => {
  const channelId = "UCbusinessretraction";
  const resultHash = `sha256:${"e".repeat(64)}`;
  const payload = {
    channel_id: channelId,
    window_policy: {
      policy_version: "video-window-v1",
      as_of: "2026-07-27T20:00:00.000Z",
      cutoff_at: "2026-04-28T20:00:00.000Z",
      cutoff_date: "2026-04-28",
      max_age_days: 90,
      max_items: 30,
    },
    window_proof: {
      complete: true,
      terminal_condition: "list_end_confirmed",
      catalog_candidate_count: 1,
      qualified_count: 0,
      selected_count: 0,
      excluded_count: 1,
      latest_scan_items: 1,
      latest_scan_pages: 1,
      latest_scan_stop_reason: "list_end",
      latest_scan_detail_failure_count: 0,
    },
    upserts: [],
    window_exits: [],
    retractions: [{ content_id: "unlisted-video", reason: "source_unlisted" }],
    result_hash: resultHash,
  };
  const envelope = publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "incremental",
    channel_id: channelId,
    domain: "video",
    data_sequence: 2,
    previous_data_sequence: 1,
    operation: "apply_window_delta",
    contract_version: 1,
    policy_version: "video-window-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: {},
    previous_result_hash: `sha256:${"d".repeat(64)}`,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });

  assert.throws(
    () => validateBusinessPublicationEnvelope(envelope),
    (error) => error.code === "payload_contract_invalid" && /reason/.test(error.message),
  );
});

test("Business Contract accepts coherent members-only Video upserts", () => {
  const channelId = "UCbusinessmembersonly";
  const item = rehashVideoItem({
    ...videoItem(channelId, 1),
    access_status: "members_only",
    is_members_only: true,
  });
  const envelope = videoDeltaEnvelope(channelId, { upserts: [item] });

  assert.equal(validateBusinessPublicationEnvelope(envelope), envelope);

  const inconsistentItem = rehashVideoItem({ ...item, is_members_only: false });
  assert.throws(
    () => validateBusinessPublicationEnvelope(videoDeltaEnvelope(channelId, {
      upserts: [inconsistentItem],
    })),
    (error) => error.code === "payload_contract_invalid" && /members-only/.test(error.message),
  );
});

test("Business Contract accepts explicit private and unavailable Video Retractions", () => {
  const channelId = "UCbusinessaccessretractions";
  const envelope = videoDeltaEnvelope(channelId, {
    retractions: [
      { content_id: "private-video", reason: "source_private" },
      { content_id: "unavailable-video", reason: "source_unavailable" },
    ],
  });

  assert.equal(validateBusinessPublicationEnvelope(envelope), envelope);
});
