import assert from "node:assert/strict";
import test from "node:test";
import { validateBusinessPublicationEnvelope } from "../src/businessPublicationContract.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  buildPublicationRecoveryBootstrap,
  buildVideoRecoverySnapshot,
  classifyRecoverablePublicationDeadLetter,
} from "../src/publicationDeadLetterRecovery.js";

const CHANNEL_ID = "UCrecoveryfixture";
const OLD_STREAM_ID = "11111111-1111-4111-8111-111111111111";
const NEW_STREAM_ID = "22222222-2222-4222-8222-222222222222";
const REVISION_ID = "33333333-3333-4333-8333-333333333333";

function videoItem(contentId, position, accessStatus = "public", membersOnly = false) {
  const item = {
    position,
    content_id: contentId,
    content_key: `${CHANNEL_ID}:${contentId}`,
    kind: "video",
    title: contentId,
    url: `https://www.youtube.com/watch?v=${contentId}`,
    thumbnail_url: null,
    published_at: "2026-08-01T00:00:00.000Z",
    published_date: "2026-08-01",
    published_at_precision: "second",
    published_at_status: "exact",
    published_at_source: "youtube_player",
    duration_seconds: 60,
    duration_status: "exact",
    duration_source: "youtube_player",
    view_count: 10,
    view_count_status: "exact",
    view_count_source: "youtube_player",
    view_count_observed_at: "2026-08-03T00:00:00.000Z",
    like_count: 2,
    like_count_status: "exact",
    like_count_source: "youtube_player",
    like_count_observed_at: "2026-08-03T00:00:00.000Z",
    comment_count: 1,
    comment_count_status: "exact",
    comment_count_source: "youtube_next",
    comment_count_observed_at: "2026-08-03T00:00:00.000Z",
    comments_disabled: false,
    description: "fixture",
    description_status: "exact",
    description_source: "youtube_player",
    hashtags: [],
    keywords: [],
    access_status: accessStatus,
    access_status_source: "youtube_player",
    is_members_only: membersOnly,
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "fixture",
  };
  const businessValue = Object.fromEntries(Object.entries(item).filter(([key]) => (
    key !== "position"
    && !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
  return { ...item, item_hash: observationFactsHash(businessValue) };
}

function videoCurrent() {
  return {
    domain: "video",
    contract_version: 1,
    policy_version: "video-window-v1",
    readiness_status: "ready",
    data_sequence: 3,
    current_revision_id: "44444444-4444-4444-8444-444444444444",
    result_hash: `sha256:${"a".repeat(64)}`,
    complete_observed_at: "2026-08-03T00:00:00.000Z",
    source_refs: { observation_id: "video-observation" },
    payload_json: {
      channel_id: CHANNEL_ID,
      window_policy: {
        policy_version: "video-window-v1",
        as_of: "2026-08-03T00:00:00.000Z",
        cutoff_at: "2026-05-05T00:00:00.000Z",
        cutoff_date: "2026-05-05",
        max_age_days: 90,
        max_items: 30,
      },
      window_proof: {
        complete: true,
        terminal_condition: "qualified_item_limit",
        catalog_candidate_count: 3,
        qualified_count: 3,
        selected_count: 3,
        excluded_count: 0,
        latest_scan_items: 3,
        latest_scan_pages: 1,
        latest_scan_stop_reason: "anchor_matched",
        latest_scan_detail_failure_count: 0,
      },
      items: [
        videoItem("public-video", 1),
        videoItem("unknown-video", 2, "unknown"),
        videoItem("members-video", 3, "members_only", true),
      ],
    },
  };
}

test("Video dead-letter recovery keeps public and members-only Content", () => {
  const snapshot = buildVideoRecoverySnapshot(videoCurrent());

  assert.deepEqual(snapshot.removed_content_ids, ["unknown-video"]);
  assert.deepEqual(snapshot.payload.items.map((item) => [
    item.content_id,
    item.position,
    item.access_status,
    item.is_members_only,
  ]), [
    ["public-video", 1, "public", false],
    ["members-video", 2, "members_only", true],
  ]);
  assert.equal(snapshot.payload.window_proof.selected_count, 2);
  assert.equal(snapshot.payload.window_proof.qualified_count, 2);
  assert.equal(snapshot.payload.window_proof.excluded_count, 1);
  assert.match(snapshot.result_hash, /^sha256:[0-9a-f]{64}$/);
});

test("a recovery Video Bootstrap passes the unchanged strict Business Contract", () => {
  const recovery = buildPublicationRecoveryBootstrap({
    current: videoCurrent(),
    channelId: CHANNEL_ID,
    oldStreamId: OLD_STREAM_ID,
    newStreamId: NEW_STREAM_ID,
    revisionId: REVISION_ID,
    occurredAt: "2026-08-03T04:00:00.000Z",
    evidenceHash: `sha256:${"b".repeat(64)}`,
    deadRevisionIds: ["55555555-5555-4555-8555-555555555555"],
    historicalRetractions: [{ content_id: "unknown-video", reason: "policy_removed" }],
  });

  assert.equal(recovery.envelope.revision_type, "bootstrap");
  assert.equal(recovery.envelope.data_sequence, 1);
  assert.deepEqual(recovery.removed_content_ids, ["unknown-video"]);
  assert.deepEqual(
    recovery.envelope.source.dead_letter_recovery.historical_retractions,
    [{ content_id: "unknown-video", reason: "policy_removed" }],
  );
  assert.doesNotThrow(() => validateBusinessPublicationEnvelope(recovery.envelope));
});

test("only supported Video contract upgrades are auto-recoverable", () => {
  assert.deepEqual(classifyRecoverablePublicationDeadLetter({
    domain: "video",
    status: "dead_letter",
    receipt_status: "rejected",
    error_code: "payload_contract_invalid",
    error_message: "video payload.upserts may contain only public Content",
    members_only_item_count: 2,
    non_publishable_item_count: 0,
  }), { recoverable: true, reason: "video_members_only_contract_upgrade" });
  assert.deepEqual(classifyRecoverablePublicationDeadLetter({
    domain: "video",
    status: "dead_letter",
    receipt_status: "rejected",
    error_code: "payload_contract_invalid",
    error_message: "video payload.retractions.reason is not allowed by Contract V1",
    retraction_reason_count: 2,
    unsupported_retraction_reason_count: 0,
  }), { recoverable: true, reason: "video_retraction_contract_upgrade" });
  assert.deepEqual(classifyRecoverablePublicationDeadLetter({
    domain: "video",
    status: "dead_letter",
    receipt_status: "rejected",
    error_code: "payload_contract_invalid",
    error_message: "video payload.item_hash does not match",
    members_only_item_count: 0,
    non_publishable_item_count: 0,
  }), { recoverable: false, reason: "unsupported_dead_letter" });
  assert.deepEqual(classifyRecoverablePublicationDeadLetter({
    domain: "video",
    status: "dead_letter",
    receipt_status: "rejected",
    error_code: "payload_contract_invalid",
    error_message: "video payload.upserts may contain only public Content",
    members_only_item_count: 1,
    non_publishable_item_count: 1,
  }), { recoverable: false, reason: "unsupported_dead_letter" });
});

test("only an explicitly targeted active Video position quarantine is recoverable", () => {
  const activationQuarantine = {
    domain: "video",
    status: "delivered",
    receipt_status: "accepted",
    explicit_target: true,
    business_receive_status: "accepted",
    business_validation_status: "quarantined",
    business_activation_status: "quarantined",
    quarantine_status: "open",
    quarantine_issue_code: "video_current_invalid",
    quarantine_message: "Active Video positions must be contiguous from 1",
  };

  assert.deepEqual(classifyRecoverablePublicationDeadLetter(activationQuarantine), {
    recoverable: true,
    reason: "video_position_gap_activation_quarantine",
  });
  assert.deepEqual(classifyRecoverablePublicationDeadLetter({
    ...activationQuarantine,
    explicit_target: false,
  }), { recoverable: false, reason: "unsupported_publication_failure" });
  assert.deepEqual(classifyRecoverablePublicationDeadLetter({
    ...activationQuarantine,
    quarantine_message: "Video Current cannot be canonicalized: unrelated corruption",
  }), { recoverable: false, reason: "unsupported_publication_failure" });
});
