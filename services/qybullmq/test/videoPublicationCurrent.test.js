import assert from "node:assert/strict";
import test from "node:test";
import {
  buildVideoPublicationCurrent,
  buildVideoPublicationItem,
} from "../src/videoPublicationCurrent.js";

const AS_OF = "2026-07-26T12:00:00.000Z";
const CHANNEL_ID = "UCvideo-publication";
const OBSERVATION_ID = "video-observation";

function source(options = {}) {
  const stopReason = options.stopReason ?? "list_end";
  const parseGapCount = Object.prototype.hasOwnProperty.call(options, "parseGapCount")
    ? options.parseGapCount
    : 0;
  const factsHash = "sha256:" + "v".repeat(64);
  return {
    cursor: {
      channel_id: CHANNEL_ID,
      observation_kind: "video",
      latest_sequence: "2",
      latest_observation_id: OBSERVATION_ID,
      latest_observed_at: "2026-07-26T10:00:00.000Z",
      latest_complete_observation_id: OBSERVATION_ID,
      latest_complete_observed_at: "2026-07-26T10:00:00.000Z",
      current_facts_hash: factsHash,
      source_cursor: stopReason ? {
        terminal_reason: stopReason,
        ...(options.matchedAnchorId ? { matched_anchor_id: options.matchedAnchorId } : {}),
      } : {},
    },
    latest_observation: {
      observation_id: OBSERVATION_ID,
      observed_at: "2026-07-26T10:00:00.000Z",
      outcome: "complete",
      outcome_reason_code: "video_complete",
    },
    complete_observation: {
      observation_id: OBSERVATION_ID,
      observed_at: "2026-07-26T10:00:00.000Z",
      channel_id: CHANNEL_ID,
      run_id: "video-run",
      observation_kind: "video",
      kind_sequence: "2",
      outcome: "complete",
      outcome_reason_code: "video_complete",
      facts_hash: factsHash,
      crawler_version: "qy-v16",
      extractor_versions: {},
      result_summary_json: {
        discovery: {
          items: 1,
          pages: 1,
          stop_reason: stopReason,
          anchor_matched: stopReason === "anchor_matched" && Boolean(options.matchedAnchorId),
          ...(parseGapCount === undefined ? {} : { parse_gap_count: parseGapCount }),
          detail_failure_count: 0,
        },
      },
    },
    run: {
      run_id: "video-run",
      channel_id: CHANNEL_ID,
      status: "done",
      crawl_mode: "incremental",
      plan_id: "video-plan",
      policy_version: "v16-rule-6",
      crawler_version: "qy-v16",
    },
  };
}

function boundedGapSource() {
  const value = source({ stopReason: "gap_abandoned_latest_30" });
  const gapAbandonment = {
    policy_version: "latest-30-on-catchup-limit-v1",
    source_stop_reason: "catchup_limit",
    scanned_item_count: 150,
    first_page_item_count: 100,
    catch_up_item_count: 50,
    catch_up_item_limit: 50,
    selected_item_count: 30,
  };
  Object.assign(value.cursor.source_cursor, {
    matched_anchor_id: null,
    crossed_anchor_ids: [],
    gap_abandonment: { ...gapAbandonment },
  });
  value.complete_observation.outcome_reason_code = "video_cycle_gap_abandoned_latest_30";
  Object.assign(value.complete_observation.result_summary_json.discovery, {
    items: 30,
    anchor_matched: false,
    gap_abandonment: { ...gapAbandonment },
  });
  return value;
}

function initialCandidateLimitSource(overrides = {}) {
  const value = source({ stopReason: "candidate_limit_processed" });
  value.complete_observation.outcome_reason_code = "initial_full_video_complete";
  Object.assign(value.complete_observation.result_summary_json.discovery, {
    items: 30,
    detail_success_count: 4,
    detail_failure_count: 0,
  });
  Object.assign(value.run, {
    crawl_mode: "full",
    result_json: {
      upload_scan: {
        pages: 1,
        stop_reason: "max_items",
        terminal_reason: "max_items",
        selected_count: 30,
        inspected_count: 30,
        parse_gap_count: 0,
        requested_limit: 30,
        content_max_age_days: 90,
        ...overrides,
      },
    },
  });
  return value;
}

function videoRow(id = "video-01", overrides = {}) {
  return {
    channel_id: CHANNEL_ID,
    content_key: `${CHANNEL_ID}:video:${id}`,
    source_content_id: id,
    content_type: "video",
    title: `Video ${id}`,
    url: `https://www.youtube.com/watch?v=${id}`,
    thumbnail_url: `https://i.ytimg.com/vi/${id}/hqdefault.jpg`,
    published_at: "2026-07-20T08:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtube_player",
    duration_seconds: 120,
    duration_status: "exact",
    duration_source: "youtube_player",
    view_count: 1000,
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
    last_observation_id: OBSERVATION_ID,
    playlist_last_seen_at: "2026-07-26T10:00:00.000Z",
    player_last_observed_at: "2026-07-26T10:00:00.000Z",
    ...overrides,
  };
}

function withItemHash(row) {
  return {
    ...row,
    publication_item_hash: buildVideoPublicationItem(row, {
      channelId: row.channel_id,
    }).item_hash,
  };
}

test("an unconfirmed optional comment count does not exclude the video or publish a guessed count", () => {
  const row = videoRow("optional-comments", {
    comment_count: 17,
    comment_count_status: "unresolved",
    comments_first_page: { surface: "absent", returned_count: 0, comments: [] },
  });
  const result = buildVideoPublicationItem(row, { channelId: CHANNEL_ID });
  assert.equal(result.ready, true);
  assert.equal(result.payload.comment_count, null);
  assert.equal(result.payload.comment_count_status, "unresolved");
  assert.equal(row.comment_count, 17);
});

test("Video Item hash contains business state and excludes transport audit noise", () => {
  const first = buildVideoPublicationItem(videoRow(), { channelId: CHANNEL_ID });
  const second = buildVideoPublicationItem(videoRow("video-01", {
    thumbnail_url: "https://i.ytimg.com/vi/video-01/hqdefault.jpg?temporary=two",
    view_count_source: "youtube_data_api",
    player_last_observed_at: "2026-07-26T11:00:00.000Z",
    extractor_version: "youtubei.js@next",
  }), { channelId: CHANNEL_ID });
  const changedStatus = buildVideoPublicationItem(videoRow("video-01", {
    view_count_status: "estimated",
  }), { channelId: CHANNEL_ID });

  assert.equal(first.ready, true);
  assert.equal(first.item_hash, second.item_hash);
  assert.notEqual(first.item_hash, changedStatus.item_hash);
  assert.equal(first.payload.published_at_status, "exact");
});

test("Video Item rejects missing metric provenance and inconsistent disabled Comments", () => {
  const missingSource = buildVideoPublicationItem(videoRow("missing-source", {
    view_count_source: null,
  }), { channelId: CHANNEL_ID });
  const missingObservedAt = buildVideoPublicationItem(videoRow("missing-observed-at", {
    player_last_observed_at: null,
    next_last_observed_at: null,
    last_enriched_at: null,
    last_seen_at: null,
  }), { channelId: CHANNEL_ID });
  const invalidComments = buildVideoPublicationItem(videoRow("invalid-comments", {
    comment_count: null,
    comment_count_status: "disabled",
    comments_disabled: false,
  }), { channelId: CHANNEL_ID });
  const invalidDisabledCount = buildVideoPublicationItem(videoRow("invalid-disabled-count", {
    comment_count: 1,
    comment_count_status: "disabled",
    comments_disabled: true,
  }), { channelId: CHANNEL_ID });
  const missingAccessSource = buildVideoPublicationItem(videoRow("missing-access-source", {
    access_status_source: null,
  }), { channelId: CHANNEL_ID });
  const missingDescriptionSource = buildVideoPublicationItem(videoRow("missing-description-source", {
    description_source: null,
  }), { channelId: CHANNEL_ID });
  const invalidAccess = buildVideoPublicationItem(videoRow("invalid-access", {
    access_status: "members_only",
    is_members_only: false,
  }), { channelId: CHANNEL_ID });

  assert.equal(missingSource.ready, false);
  assert.equal(missingObservedAt.ready, false);
  assert.equal(missingAccessSource.ready, false);
  assert.equal(missingDescriptionSource.ready, false);
  assert.equal(
    invalidComments.issues.some((item) => item.code === "video_item_comment_state_invalid"),
    true,
  );
  assert.equal(
    invalidDisabledCount.issues.some(
      (item) => item.code === "video_item_comment_state_invalid",
    ),
    true,
  );
  assert.equal(
    invalidAccess.issues.some((item) => item.code === "video_item_access_state_invalid"),
    true,
  );
});

test("Video Item publishes disabled comments as an authoritative zero", () => {
  const disabledComments = buildVideoPublicationItem(videoRow("comments-disabled", {
    comment_count: 0,
    comment_count_status: "disabled",
    comment_count_source: "youtubejs_comments",
    comments_disabled: true,
  }), { channelId: CHANNEL_ID });

  assert.equal(disabledComments.ready, true);
  assert.deepEqual({
    comment_count: disabledComments.payload.comment_count,
    comment_count_status: disabledComments.payload.comment_count_status,
    comments_disabled: disabledComments.payload.comments_disabled,
  }, {
    comment_count: 0,
    comment_count_status: "disabled",
    comments_disabled: true,
  });
});

test("Video Current requires the deterministic Item hash to be persisted", () => {
  const row = videoRow();
  const ready = buildVideoPublicationCurrent({
    rows: [withItemHash(row)],
    source: source(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const missing = buildVideoPublicationCurrent({
    rows: [row],
    source: source(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const mismatch = buildVideoPublicationCurrent({
    rows: [{ ...row, publication_item_hash: "sha256:" + "a".repeat(64) }],
    source: source(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(ready.ready, true);
  assert.equal(ready.items[0].payload.content_id, "video-01");
  assert.equal(ready.payload.channel_id, CHANNEL_ID);
  assert.equal(ready.payload.items[0].content_id, "video-01");
  assert.equal(ready.payload.items[0].item_hash, ready.items[0].item_hash);
  assert.equal(missing.ready, false);
  assert.equal(missing.issues.some((item) => item.code === "video_item_hash_missing"), true);
  assert.equal(mismatch.issues.some((item) => item.code === "video_item_hash_mismatch"), true);
});

test("Video Current publishes unlisted Content into the business window", () => {
  const row = withItemHash(videoRow("unlisted-video", {
    access_status: "unlisted",
    access_status_source: "youtubejs_microformat",
  }));
  const current = buildVideoPublicationCurrent({
    rows: [row],
    source: source(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(current.ready, true);
  assert.equal(current.items[0].payload.access_status, "unlisted");
  assert.deepEqual(current.exclusions, []);
});

test("Video Current still excludes private and unavailable Content", () => {
  const current = buildVideoPublicationCurrent({
    rows: [
      videoRow("private-video", {
        access_status: "private",
        access_status_source: "youtubejs_playability",
      }),
      videoRow("unavailable-video", {
        access_status: "unavailable",
        access_status_source: "youtubejs_playability",
      }),
    ],
    source: source(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.deepEqual(current.items, []);
  assert.deepEqual(current.exclusions, [
    { content_id: "private-video", reason_code: "source_private" },
    { content_id: "unavailable-video", reason_code: "source_unavailable" },
  ]);
});

test("Video Window accepts only a qualified limit, crossed age boundary, or confirmed list end", () => {
  const anchorSource = source({ stopReason: "anchor_matched" });
  const rows = Array.from({ length: 30 }, (_, index) => withItemHash(videoRow(
    `video-${String(index + 1).padStart(2, "0")}`,
    { published_at: new Date(Date.parse(AS_OF) - ((index + 1) * 3600000)).toISOString() },
  )));
  const underLimit = buildVideoPublicationCurrent({
    rows: rows.slice(0, 29),
    source: anchorSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const qualifiedLimit = buildVideoPublicationCurrent({
    rows,
    source: anchorSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const ageBoundary = buildVideoPublicationCurrent({
    rows: [
      withItemHash(videoRow("recent")),
      withItemHash(videoRow("old", { published_at: "2026-04-01T00:00:00.000Z" })),
    ],
    source: anchorSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(underLimit.ready, false);
  assert.equal(underLimit.window_proof.terminal_condition, null);
  assert.equal(qualifiedLimit.ready, true);
  assert.equal(qualifiedLimit.window_proof.terminal_condition, "qualified_item_limit");
  assert.equal(ageBoundary.ready, true);
  assert.equal(ageBoundary.window_proof.terminal_condition, "age_boundary_crossed");
});

test("Video Window accepts only fully audited bounded gap abandonment evidence", () => {
  const rows = Array.from({ length: 20 }, (_, index) => withItemHash(videoRow(
    `gap-video-${String(index + 1).padStart(2, "0")}`,
    { published_at: new Date(Date.parse(AS_OF) - ((index + 1) * 3600000)).toISOString() },
  )));
  const complete = buildVideoPublicationCurrent({
    rows,
    source: boundedGapSource(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const parseGapSource = boundedGapSource();
  parseGapSource.complete_observation.result_summary_json.discovery.parse_gap_count = 1;
  const parseGap = buildVideoPublicationCurrent({
    rows,
    source: parseGapSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const unexhaustedSource = boundedGapSource();
  unexhaustedSource.cursor.source_cursor.gap_abandonment.catch_up_item_count = 49;
  unexhaustedSource.complete_observation.result_summary_json.discovery
    .gap_abandonment.catch_up_item_count = 49;
  const unexhausted = buildVideoPublicationCurrent({
    rows,
    source: unexhaustedSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const mismatchedSource = boundedGapSource();
  mismatchedSource.cursor.source_cursor.gap_abandonment.scanned_item_count = 149;
  const mismatched = buildVideoPublicationCurrent({
    rows,
    source: mismatchedSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(complete.ready, true);
  assert.equal(complete.window_proof.terminal_condition, "bounded_gap_abandonment");
  for (const rejected of [parseGap, unexhausted, mismatched]) {
    assert.equal(rejected.ready, false);
    assert.equal(rejected.window_proof.terminal_condition, null);
    assert.equal(
      rejected.issues.some((item) => item.code === "video_window_termination_unproven"),
      true,
    );
  }
});

test("Video Window carries a trusted baseline through an exact incremental anchor", () => {
  const anchorId = "trusted-anchor";
  const current = buildVideoPublicationCurrent({
    rows: [withItemHash(videoRow(anchorId, {
      first_seen_at: "2026-07-25T09:00:00.000Z",
    }))],
    source: source({ stopReason: "anchor_matched", matchedAnchorId: anchorId }),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
    previousCurrent: {
      data_sequence: "1",
      current_revision_id: "11111111-1111-4111-8111-111111111111",
      result_hash: "sha256:" + "1".repeat(64),
      complete_observed_at: "2026-07-25T10:00:00.000Z",
      payload_json: {
        window_proof: {
          complete: true,
          terminal_condition: "list_end_confirmed",
        },
      },
    },
  });

  assert.equal(current.ready, true);
  assert.equal(current.window_proof.terminal_condition, "trusted_anchor_continuity");
});

test("Video Window accepts a fully processed Initial Full candidate limit", () => {
  const rows = Array.from({ length: 4 }, (_, index) => (
    withItemHash(videoRow(`recent-${index + 1}`))
  ));
  const complete = buildVideoPublicationCurrent({
    rows,
    source: initialCandidateLimitSource(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const incomplete = buildVideoPublicationCurrent({
    rows,
    source: initialCandidateLimitSource({ inspected_count: 29 }),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(complete.ready, true);
  assert.equal(complete.window_proof.terminal_condition, "candidate_limit_processed");
  assert.equal(incomplete.ready, false);
  assert.equal(incomplete.window_proof.terminal_condition, null);
  assert.equal(
    incomplete.issues.some((item) => item.code === "video_window_termination_unproven"),
    true,
  );
});

test("Video Window accepts a repaired Initial Full candidate limit on the same Full Run", () => {
  const repairedSource = initialCandidateLimitSource();
  repairedSource.complete_observation.outcome_reason_code = "repair_video_complete";
  const unrelatedSource = initialCandidateLimitSource();
  unrelatedSource.complete_observation.outcome_reason_code = "video_complete";
  const current = buildVideoPublicationCurrent({
    rows: Array.from({ length: 4 }, (_, index) => (
      withItemHash(videoRow(`repaired-recent-${index + 1}`))
    )),
    source: repairedSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });
  const unrelated = buildVideoPublicationCurrent({
    rows: Array.from({ length: 4 }, (_, index) => (
      withItemHash(videoRow(`unrelated-recent-${index + 1}`))
    )),
    source: unrelatedSource,
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(current.ready, true);
  assert.equal(current.window_proof.terminal_condition, "candidate_limit_processed");
  assert.equal(unrelated.ready, false);
  assert.equal(unrelated.window_proof.terminal_condition, null);
});

test("Video Window rejects an anchor first seen after the trusted baseline", () => {
  const anchorId = "newer-anchor";
  const current = buildVideoPublicationCurrent({
    rows: [withItemHash(videoRow(anchorId, {
      first_seen_at: "2026-07-25T11:00:00.000Z",
    }))],
    source: source({ stopReason: "anchor_matched", matchedAnchorId: anchorId }),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
    previousCurrent: {
      data_sequence: "1",
      current_revision_id: "11111111-1111-4111-8111-111111111111",
      result_hash: "sha256:" + "1".repeat(64),
      complete_observed_at: "2026-07-25T10:00:00.000Z",
      payload_json: { window_proof: { complete: true } },
    },
  });

  assert.equal(current.ready, false);
  assert.equal(current.window_proof.terminal_condition, null);
  assert.equal(
    current.issues.some((item) => item.code === "video_window_termination_unproven"),
    true,
  );
});

test("Video Window applies instant and date-only 90-day cutoffs exactly", () => {
  const rows = [
    withItemHash(videoRow("instant-cutoff", { published_at: "2026-04-27T12:00:00.000Z" })),
    withItemHash(videoRow("instant-inside", { published_at: "2026-04-27T12:00:01.000Z" })),
    withItemHash(videoRow("date-cutoff", {
      published_at: "2026-04-27T00:00:00.000Z",
      published_at_precision: "date_only",
    })),
    withItemHash(videoRow("date-inside", {
      published_at: "2026-04-28T00:00:00.000Z",
      published_at_precision: "date_only",
    })),
  ];
  const current = buildVideoPublicationCurrent({
    rows,
    source: source(),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.deepEqual(
    current.items.map((item) => item.payload.content_id),
    ["date-inside", "instant-inside"],
  );
  assert.deepEqual(
    current.exclusions.map((item) => item.content_id),
    ["date-cutoff", "instant-cutoff"],
  );
});

test("list end is not proof when parse-gap evidence is absent", () => {
  const current = buildVideoPublicationCurrent({
    rows: [withItemHash(videoRow())],
    source: source({ stopReason: "list_end", parseGapCount: undefined }),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(current.ready, false);
  assert.equal(current.window_proof.terminal_condition, null);
  assert.equal(current.issues.some((item) => item.code === "video_window_termination_unproven"), true);
});

test("Video Window accepts audited Full Repair age-boundary evidence", () => {
  const current = buildVideoPublicationCurrent({
    rows: [withItemHash(videoRow("recent"))],
    source: source({ stopReason: "age_boundary_crossed" }),
    channelId: CHANNEL_ID,
    asOf: AS_OF,
  });

  assert.equal(current.ready, true);
  assert.equal(current.window_proof.terminal_condition, "age_boundary_crossed");
});
