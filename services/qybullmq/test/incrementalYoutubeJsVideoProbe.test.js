import assert from "node:assert/strict";
import test from "node:test";
import {
  incrementalYoutubeJsVideoProbeAttemptOutcome,
  probeIncrementalYoutubeJsVideoFetch,
} from "../src/incrementalYoutubeJsVideo.js";
import {
  postgresReadOnlyQuery,
  selectIncrementalYoutubeJsVideoProbePlan,
} from "../src/incrementalYoutubeJsVideoProbe.js";

function plan() {
  return {
    plan_id: "11111111-1111-4111-8111-111111111111",
    plan_day: "2026-09-03",
    channel_id: "UCprobe",
    task_mask: { about: true, video: true, agent: false },
    capacity: { factor: 1, player_cap: 2, next_cap: 1, version: "capacity-v1" },
    planner_config_version: "video-plan-1",
  };
}

function publicDetail(videoId) {
  return {
    id: videoId,
    title: `Title ${videoId}`,
    thumbnail_url: `https://i.ytimg.com/vi/${videoId}/default.jpg`,
    published_at: "2026-09-02T12:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "second",
    published_at_source: "youtubejs_player_microformat",
    duration_seconds: 60,
    view_count: 100,
    like_count: 5,
    comment_count: 1,
    comment_count_status: "exact",
    comments_disabled: false,
    comments_first_page: { total_count: 1, returned_count: 1, comments: [] },
    description: "Observed description",
    description_status: "exact",
    description_observed: true,
    hashtags: [],
    hashtags_observed: true,
    keywords: [],
    keywords_observed: true,
    access_status: "public",
    content_type_signals: {
      source: "youtubei_player",
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
    extractor_version: "youtubei.js@test",
    source: "youtubejs_get_info",
  };
}

function readOnlyProbeQuery({ includeRecent = true } = {}) {
  const statements = [];
  const query = async (sql) => {
    statements.push(sql);
    assert.doesNotMatch(sql, /\b(?:INSERT|UPDATE|DELETE|MERGE|LOCK|TRUNCATE)\b/i);
    if (sql.includes("FROM crawler.channel_domain_cursors")) {
      return {
        rows: [{
          anchors: [{ video_id: "stored-video", published_at: "2026-09-01T00:00:00.000Z" }],
        }],
      };
    }
    if (sql.includes("source_content_id=ANY($2::text[])")) {
      return { rows: [{ video_id: "stored-video" }] };
    }
    if (sql.includes("SELECT DISTINCT ON (candidate.source_content_id)")) return { rows: [] };
    if (sql.includes("candidate.disposition IN ('deferred','terminal_excluded')")) return { rows: [] };
    if (sql.includes("candidate.first_seen_ledger_status='pending'")) return { rows: [] };
    if (sql.includes("FROM crawler.settings")) return { rows: [{ mode: "clock" }] };
    if (sql.includes("WITH observed_uploads AS")) {
      return {
        rows: includeRecent ? [{
          content_key: "UCprobe:video:stored-video",
          channel_id: "UCprobe",
          source_content_id: "stored-video",
          content_type: "video",
          published_at: "2026-09-01T00:00:00.000Z",
          sampling_published_at: "2026-09-01T00:00:00.000Z",
          player_last_observed_at: null,
          next_last_observed_at: null,
          video_change_probability: null,
          enrich_pending: false,
          player_enrich_open: false,
          player_enrich_leased: false,
          player_enrich_terminal_waiting: false,
          player_enrich_retry_waiting: false,
        }] : [],
      };
    }
    throw new Error(`unexpected probe query: ${sql}`);
  };
  return { query, statements };
}

function snapshot() {
  return {
    async scanUploads(options) {
      assert.deepEqual(options.anchors, [{ id: "stored-video", published_day: "2026-09-01" }]);
      return {
        playlist_id: "UUprobe",
        entries: [
          {
            id: "new-video",
            position: 1,
            title: "New upload",
            content_type: "video",
            published_day: "2026-09-02",
          },
          {
            id: "stored-video",
            position: 2,
            title: "Stored upload",
            content_type: "video",
            published_day: "2026-09-01",
          },
        ],
        pages: 1,
        item_count: 2,
        parse_gap_count: 0,
        anchor_matched: true,
        matched_anchor_id: "stored-video",
        crossed_anchor_ids: [],
        stop_reason: "anchor_matched",
        terminal_reason: "anchor_matched",
        complete: true,
      };
    },
  };
}

test("probe only borrows an inactive Channel Clock ID and synthesizes manual inputs", async () => {
  const statements = [];
  const selected = await selectIncrementalYoutubeJsVideoProbePlan(async (sql) => {
    statements.push(sql);
    if (sql.includes("FROM feature_clock.channel_clock_state clock")) {
      return {
        rows: [{
          channel_id: "UCclockstate",
          lifecycle_status: "active",
          video_due_at: "2026-09-06T03:00:00.000Z",
          video_due_day: new Date("2026-09-06T00:00:00.000Z"),
          video_last_complete_at: "2026-09-01T03:00:00.000Z",
          video_last_outcome: "complete",
          clock_version: 7,
        }],
      };
    }
    throw new Error(`unexpected SQL: ${sql}`);
  }, {
    planDay: "2026-09-03",
    clockStateFallback: {
      capacityFactor: 1,
      playerCap: 1,
      nextCap: 0,
      capacityVersion: "read-only-probe-v1",
      plannerConfigVersion: "video-plan-1",
    },
  });

  assert.equal(statements.length, 1);
  assert.doesNotMatch(statements[0], /FROM feature_clock\.daily_channel_plans plan/);
  assert.match(statements[0], /NOT EXISTS/);
  assert.match(statements[0], /active_plan\.status IN \('dispatching','dispatched','running'\)/);
  assert.equal(selected.clock.source, "channel_clock_state");
  assert.equal(selected.clock.formal_daily_plan, false);
  assert.equal(selected.clock.due_day, "2026-09-06");
  assert.match(selected.clock.note, /neither read nor claimed/);
  assert.equal(selected.plan.channel_id, "UCclockstate");
  assert.equal(selected.plan.plan_mode, "manual/read_only_probe");
  assert.equal(selected.plan.formal_daily_plan, false);
  assert.deepEqual(selected.plan.task_mask, { about: false, video: true, agent: false });
  assert.deepEqual(selected.plan.capacity, {
    factor: 1,
    player_cap: 1,
    next_cap: 0,
    version: "read-only-probe-v1",
  });
});

test("PostgreSQL probe wrapper puts every query in its own verified read-only transaction", async () => {
  const statements = [];
  const client = {
    async query(sql, params = []) {
      statements.push([sql, params]);
      if (sql === "SHOW transaction_read_only") {
        return { rows: [{ transaction_read_only: "on" }] };
      }
      if (sql === "SELECT $1::int AS value") return { rows: [{ value: params[0] }] };
      return { rows: [] };
    },
  };
  const query = postgresReadOnlyQuery(client);
  assert.deepEqual(await query("SELECT $1::int AS value", [7]), { rows: [{ value: 7 }] });
  assert.deepEqual(statements, [
    ["BEGIN TRANSACTION READ ONLY", []],
    ["SHOW transaction_read_only", []],
    ["SELECT $1::int AS value", [7]],
    ["COMMIT", []],
  ]);
});

test("read-only probe follows Phase A then Phase B with Detail concurrency fixed at one", async () => {
  const database = readOnlyProbeQuery();
  const order = [];
  let active = 0;
  let maximumActive = 0;
  const report = await probeIncrementalYoutubeJsVideoFetch({
    plan: plan(),
    query: database.query,
    getChannelSnapshot: async () => snapshot(),
    fetchDetail: async (videoId) => {
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      order.push(videoId);
      await new Promise((resolve) => setImmediate(resolve));
      active -= 1;
      return publicDetail(videoId);
    },
    detailLimitPerPhase: 1,
    now: () => new Date("2026-09-03T02:00:00.000Z"),
  });

  assert.equal(report.writes_performed, false);
  assert.equal(report.plan.formal_daily_plan, false);
  assert.equal(report.scan.complete, true);
  assert.deepEqual(report.targets.items, [
    { phase: "first_seen", ordinal: 0, video_id: "new-video" },
    { phase: "recent", ordinal: 0, video_id: "stored-video" },
  ]);
  assert.deepEqual(order, ["new-video", "stored-video"]);
  assert.equal(maximumActive, 1);
  assert.equal(report.verification.captured_count, 2);
  assert.equal(report.verification.pending_count, 0);
  assert.equal(report.verification.fetch_flow_ok, true);
  assert.equal(report.verification.detail_surface_observed, true);
  assert.equal(report.phases[0].results[0].field_status.title, "exact");
  assert.equal(report.phases[0].results[0].detail.description, "Observed description");
  assert.deepEqual(incrementalYoutubeJsVideoProbeAttemptOutcome(report), {
    kind: "managed_work_complete",
    businessState: "terminal",
    result: { report_ready: true, fetch_flow_ok: true },
  });
  assert.ok(database.statements.length > 0);
});

test("read-only probe leaves a 429 target pending and stops before Phase B", async () => {
  const database = readOnlyProbeQuery();
  const requested = [];
  const routeFailure = Object.assign(new Error("429 Too Many Requests"), { status: 429 });
  const report = await probeIncrementalYoutubeJsVideoFetch({
    plan: plan(),
    query: database.query,
    getChannelSnapshot: async () => snapshot(),
    fetchDetail: async (videoId) => {
      requested.push(videoId);
      throw routeFailure;
    },
    detailLimitPerPhase: 1,
    now: () => new Date("2026-09-03T02:00:00.000Z"),
  });

  assert.deepEqual(requested, ["new-video"]);
  assert.equal(report.phases[0].results[0].status, "pending");
  assert.equal(report.phases[0].results[0].field_status.title, "unobserved");
  assert.equal(
    report.halted.executor_action,
    "request_bounded_diagnostic_route_switch",
  );
  assert.equal(report.halted.production_executor_action, "throw_and_retry_same_pending_item");
  assert.equal(report.halted.retry_suppressed, false);
  assert.equal(report.halted.failure.decision.kind, "youtube_rate_limited");
  assert.equal(report.verification.fetch_flow_ok, false);
  assert.equal(report.verification.automatic_retry_or_route_switch_requested, true);
  assert.equal(report.verification.pending_count, 1);
  assert.deepEqual(incrementalYoutubeJsVideoProbeAttemptOutcome(report), {
    kind: "retryable_network_failure",
    observation: "youtube_rate_limited",
    source: "youtube_managed_request",
    failedStage: "first_seen",
    checkpointPersisted: true,
  });
  assert.equal(
    report.flow.find((step) => step.step === "phase_b_recent_detail").state,
    "skipped_after_retryable_failure",
  );
});

test("read-only probe reports a Scan route failure without retrying, switching, or fetching Detail", async () => {
  const database = readOnlyProbeQuery();
  let channelAttempts = 0;
  let detailAttempts = 0;
  const routeFailure = Object.assign(new Error("CONNECT upstream failed: i/o timeout"), {
    code: "FINGERPRINT_PROXY_TRANSPORT",
    failureKind: "proxy_transport",
    source: "fingerprint_gateway",
  });
  const report = await probeIncrementalYoutubeJsVideoFetch({
    plan: plan(),
    query: database.query,
    getChannelSnapshot: async () => {
      channelAttempts += 1;
      throw routeFailure;
    },
    fetchDetail: async () => {
      detailAttempts += 1;
      return publicDetail("unexpected");
    },
    detailLimitPerPhase: 1,
    now: () => new Date("2026-09-03T02:00:00.000Z"),
  });

  assert.equal(channelAttempts, 1);
  assert.equal(detailAttempts, 0);
  assert.equal(report.writes_performed, false);
  assert.equal(report.scan.complete, false);
  assert.equal(report.scan.failure.decision.kind, "proxy_transport");
  assert.equal(
    report.flow.find((step) => step.step === "uploads_scan").state,
    "failed_route_switch_requested",
  );
  assert.equal(report.halted.phase, "uploads_scan");
  assert.equal(
    report.halted.executor_action,
    "request_bounded_diagnostic_route_switch",
  );
  assert.equal(report.halted.production_executor_action,
    "classify_and_retry_according_to_youtube_failure_policy");
  assert.equal(report.halted.retry_suppressed, false);
  assert.equal(report.verification.fetch_flow_ok, false);
  assert.equal(report.verification.automatic_retry_or_route_switch_requested, true);
  assert.deepEqual(report.targets.items, []);
  assert.deepEqual(report.phases, []);
  assert.equal(
    report.flow.find((step) => step.step === "phase_a_first_seen_detail").state,
    "skipped_after_scan_failure",
  );
  assert.deepEqual(incrementalYoutubeJsVideoProbeAttemptOutcome(report), {
    kind: "retryable_network_failure",
    observation: "proxy_transport",
    source: "fingerprint_gateway",
    failedStage: "uploads_scan",
    checkpointPersisted: true,
  });
});
