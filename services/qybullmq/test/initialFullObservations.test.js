import assert from "node:assert/strict";
import test from "node:test";
import {
  initialFullVideoBaseline,
  isPublicationGapChildRepairRun,
  recordInitialFullObservations,
} from "../src/initialFullObservations.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";

function publicationReadyVideo(id, overrides = {}) {
  const channelId = "UCpublication-repair";
  return {
    channel_id: channelId,
    content_key: `${channelId}:video:${id}`,
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
    hashtags: [],
    keywords: [],
    access_status: "public",
    access_status_source: "youtube_player",
    is_members_only: false,
    player_last_observed_at: "2026-07-26T10:00:00.000Z",
    ...overrides,
  };
}

test("initial Full Video baseline is derived only from retained Contents", () => {
  const baseline = initialFullVideoBaseline([
    {
      source_content_id: "recent-video",
      content_type: "video",
      published_at: "2026-07-20T00:00:00Z",
      published_at_precision: "second",
      is_recent: true,
      player_last_observed_at: "2026-07-20T00:00:00Z",
    },
  ], "2026-07-21T00:00:00Z");
  assert.equal(baseline.discovery.payload.first_seen_count, 1);
  assert.equal(baseline.discovery.payload.first_seen[0].video_id, "recent-video");
  assert.equal(baseline.recentSampling.payload.recent_count, 1);
  assert.equal(baseline.recentSampling.payload.stale_ratio, 0);
});

test("initial Full Video baseline counts the UTC 30-day pool instead of the 90-day retention flag", () => {
  const baseline = initialFullVideoBaseline([
    {
      source_content_id: "inside-window",
      content_type: "video",
      published_at: "2026-07-20T00:00:00Z",
      published_at_precision: "second",
      is_recent: false,
      player_last_observed_at: "2026-07-20T00:00:00Z",
    },
    {
      source_content_id: "at-window-boundary",
      content_type: "video",
      published_at: "2026-06-21T00:00:00Z",
      published_at_precision: "second",
      is_recent: true,
      player_last_observed_at: "2026-06-21T00:00:00Z",
    },
    {
      source_content_id: "outside-window",
      content_type: "video",
      published_at: "2026-06-20T23:59:59Z",
      published_at_precision: "second",
      is_recent: true,
      player_last_observed_at: "2026-06-20T23:59:59Z",
    },
    {
      source_content_id: "unknown-publication-time",
      content_type: "video",
      published_at: null,
      published_at_precision: "unknown",
      is_recent: true,
      player_last_observed_at: null,
    },
  ], "2026-07-21T00:00:00Z");

  assert.equal(baseline.discovery.payload.first_seen_count, 4);
  assert.equal(baseline.recentSampling.payload.recent_count, 2);
  assert.equal(baseline.recentSampling.payload.stale_ratio, 0.5);
});

test("initial Full Video baseline deduplicates Contents by YouTube Video ID", () => {
  const baseline = initialFullVideoBaseline([
    {
      source_content_id: "duplicate-video",
      content_type: "video",
      published_at: null,
      published_at_precision: null,
      is_recent: true,
      player_last_observed_at: "2026-07-20T00:00:00Z",
    },
    {
      source_content_id: "duplicate-video",
      content_type: "live",
      published_at: "2026-07-20T00:00:00Z",
      published_at_precision: "second",
      is_recent: true,
      player_last_observed_at: null,
    },
  ], "2026-07-21T00:00:00Z");

  assert.equal(baseline.discovery.payload.first_seen_count, 1);
  assert.deepEqual(baseline.discovery.payload.first_seen, [{
    video_id: "duplicate-video",
    position: 1,
    content_type: "live",
    published_at: "2026-07-20T00:00:00.000Z",
    published_at_precision: "second",
  }]);
  assert.equal(baseline.recentSampling.payload.recent_count, 1);
  assert.equal(baseline.recentSampling.payload.stale_ratio, 0);
  assert.equal(baseline.knownIdentityCount, 1);
  assert.deepEqual(baseline.anchorVideoIds, ["duplicate-video"]);
});

test("Publication Repair marks a capped deep scan Complete only after 30 qualified items", () => {
  const baseline = initialFullVideoBaseline(
    Array.from({ length: 30 }, (_, index) => publicationReadyVideo(`video-${index + 1}`)),
    "2026-07-26T12:00:00Z",
    {
      channelId: "UCpublication-repair",
      uploadScan: {
        requested_limit: 100,
        content_max_age_days: 90,
        selected_count: 100,
        inspected_count: 100,
        pages: 4,
        parse_gap_count: 0,
        stop_reason: "max_items",
        terminal_reason: "max_items",
        scan_policy_version: "publication-video-window-repair-v1",
      },
    },
  );

  assert.equal(baseline.discovery.outcome, "complete");
  assert.equal(baseline.discovery.payload.stop_reason, "qualified_item_limit");
  assert.equal(baseline.discovery.payload.qualified_count, 30);
  assert.equal(baseline.discovery.payload.items, 100);
});

test("Publication Repair preserves age-boundary proof after old Candidates leave Contents", () => {
  const baseline = initialFullVideoBaseline(
    [publicationReadyVideo("recent")],
    "2026-07-26T12:00:00Z",
    {
      channelId: "UCpublication-repair",
      uploadScan: {
        requested_limit: 100,
        content_max_age_days: 90,
        selected_count: 12,
        inspected_count: 12,
        pages: 1,
        parse_gap_count: 0,
        stop_reason: "max_items",
        terminal_reason: "max_items",
        scan_policy_version: "publication-video-window-repair-v1",
      },
      ageBoundaryObserved: true,
    },
  );

  assert.equal(baseline.discovery.outcome, "complete");
  assert.equal(baseline.discovery.payload.stop_reason, "age_boundary_crossed");
  assert.equal(baseline.discovery.payload.age_boundary_crossed, true);
});

test("Publication Repair keeps an exhausted or parse-gapped scan Partial", () => {
  const rows = Array.from({ length: 29 }, (_, index) => publicationReadyVideo(`video-${index + 1}`));
  const scan = {
    requested_limit: 100,
    content_max_age_days: 90,
    selected_count: 100,
    inspected_count: 100,
    pages: 4,
    parse_gap_count: 0,
    stop_reason: "max_items",
    terminal_reason: "max_items",
    scan_policy_version: "publication-video-window-repair-v1",
  };
  const exhausted = initialFullVideoBaseline(rows, "2026-07-26T12:00:00Z", {
    channelId: "UCpublication-repair",
    uploadScan: scan,
  });
  const parseGap = initialFullVideoBaseline(
    [...rows, publicationReadyVideo("video-30")],
    "2026-07-26T12:00:00Z",
    {
      channelId: "UCpublication-repair",
      uploadScan: { ...scan, parse_gap_count: 1, stop_reason: "parse_gap" },
    },
  );

  assert.equal(exhausted.discovery.outcome, "partial");
  assert.equal(exhausted.discovery.payload.terminal_condition, null);
  assert.equal(parseGap.discovery.outcome, "partial");
  assert.equal(parseGap.discovery.payload.stop_reason, "parse_gap");
});

test("a shorter collection window cannot prove the 90-day Publication list end", () => {
  const baseline = initialFullVideoBaseline(
    [publicationReadyVideo("recent")],
    "2026-07-26T12:00:00Z",
    {
      channelId: "UCpublication-repair",
      uploadScan: {
        requested_limit: 100,
        content_max_age_days: 30,
        selected_count: 1,
        inspected_count: 1,
        pages: 1,
        parse_gap_count: 0,
        stop_reason: "list_end",
        terminal_reason: "list_end",
      },
    },
  );

  assert.equal(baseline.discovery.outcome, "partial");
  assert.equal(baseline.discovery.payload.terminal_condition, null);
});

test("initial Full observations publish only About, Video, and Agent after Agent completion", async () => {
  const calls = [];
  const inputContentHash = observationFactsHash([]);
  const agentVersionHash = `sha256:${"d".repeat(64)}`;
  const pendingAbout = {
    idempotencyKey: "about:run:full:attempt:1",
    channelId: "UCfull",
    runId: "run:full",
    observedAt: "2026-07-21T10:00:00Z",
    triggerReason: "initial_full",
    about: {},
    current: {
      profile: {
        title: "Full Channel",
        handle: "@full-channel",
        avatar_url: null,
        keywords: [],
        available_tabs: ["videos"],
        summary: null,
      },
    },
  };
  const client = {
    async query(sql, params = []) {
      calls.push({ type: "query", sql, params });
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: {
              channel_id: "UCfull",
              status: "active",
              agent_status: "done",
              title: "Full Channel",
            },
            run: {
              run_id: "run:full",
              channel_id: "UCfull",
              candidate_id: 42,
              crawl_mode: "full",
              detail_status: "done",
              started_at: "2026-07-21T09:00:00Z",
              result_json: { pending_initial_about_observation: pendingAbout },
            },
          }],
        };
      }
      if (sql.includes("SELECT to_jsonb(content)")) return { rows: [] };
      if (sql.includes("FROM crawler.contents")) {
        return {
          rows: [{
            source_content_id: "video-1",
            content_type: "video",
            published_at: "2026-07-20T00:00:00Z",
            published_at_precision: "second",
            is_recent: true,
            player_last_observed_at: "2026-07-21T10:00:00Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.agent_profiles")) {
        return {
          rows: [{
            channel_id: "UCfull",
            status: "success",
            metrics_json: {},
            input_content_ids: [],
            input_content_hash: inputContentHash,
            taxonomy_version: "qy-taxonomy-v1",
            agent_version_hash: agentVersionHash,
            observed_at: "2026-07-21T11:00:00Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.crawl_observations")) return { rows: [] };
      return { rowCount: 1, rows: [] };
    },
  };
  const recordAbout = async (_client, command) => {
    calls.push({ type: "observation", kind: "about", command });
    return { observation_id: "about-observation", outcome: "partial" };
  };
  const recordObservation = async (_client, command) => {
    calls.push({ type: "observation", kind: command.observationKind, command });
    const prepared = await command.prepare({
      client,
      observationId: command.observationKind + "-observation",
    });
    calls.push({ type: "prepared", kind: command.observationKind, prepared });
    return {
      observation_id: command.observationKind + "-observation",
      outcome: prepared.outcome,
    };
  };

  const result = await recordInitialFullObservations({
    withTransaction: (action) => action(client),
    channelId: "UCfull",
    runId: "run:full",
    observedAt: "2026-07-21T11:00:00Z",
    recordAbout,
    recordObservation,
  });

  assert.equal(result.recorded, true);
  assert.deepEqual(result.outcomes, {
    about: "partial",
    video: "complete",
    agent: "complete",
  });
  const contentSelect = calls.find((call) => (
    call.type === "query" && call.sql.includes("SELECT content.*")
  ));
  assert.match(contentSelect.sql, /FROM crawler\.contents content/);
  assert.deepEqual(
    calls.filter((call) => call.type === "observation").map((call) => call.kind),
    ["about", "video", "agent"],
  );
  assert.equal(
    calls.find((call) => call.type === "observation" && call.kind === "about")
      .command.publicationReconcile,
    false,
  );
  assert.equal(
    calls.some((call) => (
      call.type === "query"
      && call.sql.includes("result_json-'pending_initial_about_observation'")
      && call.params[1] === "about-observation"
    )),
    true,
  );
  const agentObservation = calls.find((call) => (
    call.type === "prepared" && call.kind === "agent"
  )).prepared;
  assert.deepEqual(agentObservation.resultSummary, {
    fulfilled_plan_count: 1,
    baseline: true,
    output_hash: observationFactsHash({}),
    input_content_hash: inputContentHash,
    agent_version_hash: agentVersionHash,
  });
  const videoObservation = calls.find((call) => (
    call.type === "prepared" && call.kind === "video"
  )).prepared;
  assert.deepEqual(videoObservation.resultSummary.discovery, {
    pages: 1,
    items: 1,
    anchor_matched: false,
    stop_reason: "list_end",
    parse_gap_count: 0,
    first_seen_count: 1,
    detail_success_count: 1,
    detail_failure_count: 0,
  });
});

test("a Repair records new traceable Observations despite existing Full Crawl kinds", async () => {
  const calls = [];
  const inputContentHash = observationFactsHash([]);
  const agentVersionHash = `sha256:${"e".repeat(64)}`;
  const client = {
    async query(sql) {
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: {
              channel_id: "UCrepair",
              status: "active",
              agent_status: "done",
              title: "Repair Channel",
            },
            run: {
              run_id: "run:repair",
              channel_id: "UCrepair",
              candidate_id: 43,
              crawl_mode: "full",
              detail_status: "done",
              started_at: "2026-07-21T09:00:00Z",
              result_json: {},
            },
          }],
        };
      }
      if (sql.includes("SELECT to_jsonb(content)")) return { rows: [] };
      if (sql.includes("FROM crawler.contents")) {
        return {
          rows: [{
            content_key: "UCrepair:video:repaired-video",
            source_content_id: "repaired-video",
            content_type: "video",
            published_at: "2026-07-20T00:00:00Z",
            published_at_precision: "second",
            player_last_observed_at: "2026-07-21T10:00:00Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.agent_profiles")) {
        return {
          rows: [{
            channel_id: "UCrepair",
            status: "success",
            metrics_json: {},
            input_content_ids: [],
            input_content_hash: inputContentHash,
            taxonomy_version: "qy-taxonomy-v1",
            agent_version_hash: agentVersionHash,
            observed_at: "2026-07-21T11:00:00Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.crawl_observations")) {
        return {
          rows: ["about", "video", "agent"].map((observation_kind) => ({
            observation_id: `old-${observation_kind}`,
            observation_kind,
          })),
        };
      }
      if (sql.includes("UPDATE crawler.contents") && sql.includes("RETURNING content_key")) {
        return { rows: [{ content_key: "UCrepair:video:repaired-video" }] };
      }
      return { rowCount: 1, rows: [] };
    },
  };
  const recordAbout = async (_client, command) => {
    calls.push(command);
    return { observation_id: "repair-about" };
  };
  const recordObservation = async (_client, command) => {
    calls.push(command);
    await command.prepare({
      client,
      observationId: `repair-${command.observationKind}`,
    });
    return { observation_id: `repair-${command.observationKind}` };
  };

  const result = await recordInitialFullObservations({
    withTransaction: (action) => action(client),
    channelId: "UCrepair",
    runId: "run:repair",
    observedAt: "2026-07-21T11:00:00Z",
    revisionType: "repair",
    repairId: "repair-job-7",
    recordAbout,
    recordObservation,
  });

  assert.equal(result.recorded, true);
  assert.deepEqual(calls.map((command) => command.observationKind ?? "about"), [
    "about",
    "video",
    "agent",
  ]);
  assert.deepEqual(calls.map((command) => command.triggerReason), ["repair", "repair", "repair"]);
  assert.deepEqual(calls.map((command) => command.idempotencyKey), [
    "repair-full:run:repair:repair-job-7:about",
    "repair-full:run:repair:repair-job-7:video",
    "repair-full:run:repair:repair-job-7:agent",
  ]);
  assert.equal(calls[0].publicationReconcile, false);
});

test("Repair Finalize upgrades a partial Video observation once for complete evidence", async () => {
  const inputContentHash = observationFactsHash([]);
  const agentVersionHash = `sha256:${"7".repeat(64)}`;
  const baseVideoKey = "repair-full:run:repair-upgrade:repair-job-upgrade:video";
  const stored = new Map([
    [
      "repair-full:run:repair-upgrade:repair-job-upgrade:about",
      { observation_id: "old-about", outcome: "complete" },
    ],
    [baseVideoKey, { observation_id: "old-video", outcome: "partial" }],
    [
      "repair-full:run:repair-upgrade:repair-job-upgrade:agent",
      { observation_id: "old-agent", outcome: "complete" },
    ],
  ]);
  const created = [];
  const videoKeys = [];
  const client = {
    async query(sql) {
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: {
              channel_id: "UCrepair-upgrade",
              status: "active",
              agent_status: "done",
              subscriber_count: 100,
              subscriber_count_status: "exact",
              total_view_count: 1000,
              total_view_count_status: "exact",
              total_video_count: 1,
              total_video_count_status: "exact",
            },
            run: {
              run_id: "run:repair-upgrade",
              channel_id: "UCrepair-upgrade",
              candidate_id: 44,
              crawl_mode: "full",
              detail_status: "done",
              started_at: "2026-08-18T09:00:00Z",
              result_json: {},
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.contents")) {
        return {
          rows: [{
            content_key: "UCrepair-upgrade:video:recovered-video",
            channel_id: "UCrepair-upgrade",
            source_content_id: "recovered-video",
            content_type: "video",
            published_at: "2026-08-17T00:00:00Z",
            published_at_precision: "second",
            player_last_observed_at: "2026-08-18T09:30:00Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.content_candidates")) return { rows: [] };
      if (sql.includes("FROM crawler.agent_profiles")) {
        return {
          rows: [{
            channel_id: "UCrepair-upgrade",
            status: "success",
            metrics_json: {},
            input_content_ids: [],
            input_content_hash: inputContentHash,
            taxonomy_version: "qy-taxonomy-v1",
            agent_version_hash: agentVersionHash,
            observed_at: "2026-08-18T10:00:00Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.crawl_observations")) {
        return {
          rows: [
            { observation_id: "old-about", observation_kind: "about", outcome: "complete" },
            { observation_id: "old-video", observation_kind: "video", outcome: "partial" },
            { observation_id: "old-agent", observation_kind: "agent", outcome: "complete" },
          ],
        };
      }
      if (sql.includes("UPDATE crawler.contents") && sql.includes("RETURNING content_key")) {
        return { rows: [{ content_key: "UCrepair-upgrade:video:recovered-video" }] };
      }
      return { rowCount: 1, rows: [] };
    },
  };
  const duplicate = (command) => {
    const existing = stored.get(command.idempotencyKey);
    return existing ? { ...existing, duplicate: true } : null;
  };
  const recordAbout = async (_client, command) => duplicate(command);
  const recordObservation = async (_client, command) => {
    if (command.observationKind === "video") videoKeys.push(command.idempotencyKey);
    const existing = duplicate(command);
    if (existing) return existing;
    const prepared = await command.prepare({
      client,
      observationId: `new-${command.observationKind}`,
    });
    const result = {
      observation_id: `new-${command.observationKind}`,
      outcome: prepared.outcome,
      duplicate: false,
    };
    stored.set(command.idempotencyKey, result);
    created.push({ key: command.idempotencyKey, outcome: prepared.outcome });
    return result;
  };
  const input = {
    withTransaction: (action) => action(client),
    channelId: "UCrepair-upgrade",
    runId: "run:repair-upgrade",
    observedAt: "2026-08-18T10:00:00Z",
    revisionType: "repair",
    repairId: "repair-job-upgrade",
    recordAbout,
    recordObservation,
  };

  const first = await recordInitialFullObservations(input);
  const second = await recordInitialFullObservations(input);

  assert.equal(first.outcomes.video, "complete");
  assert.equal(second.outcomes.video, "complete");
  assert.equal(created.length, 1);
  assert.deepEqual(created.map((entry) => entry.outcome), ["complete"]);
  assert.equal(videoKeys[0], videoKeys[1]);
  assert.match(
    videoKeys[0],
    /^repair-full:run:repair-upgrade:repair-job-upgrade:video:evidence:sha256:[a-f0-9]{64}$/,
  );
});

test("Repair Finalize reuses the staged About command after reconciliation consumes it", async () => {
  const aboutCommands = [];
  const inputContentHash = observationFactsHash([]);
  const agentVersionHash = `sha256:${"f".repeat(64)}`;
  let pendingAboutVisible = true;
  let persistedAboutObservationId = null;
  const pendingAbout = {
    idempotencyKey: "staged-about-key",
    channelId: "UCrepair",
    runId: "run:repair",
    observedAt: "2026-07-27T12:10:08.405Z",
    triggerReason: "repair",
    about: {},
    current: {
      profile: {
        title: "Repair Channel",
        handle: "@repair-channel",
        avatar_url: null,
        keywords: [],
        available_tabs: ["videos"],
        summary: null,
      },
    },
  };
  const client = {
    async query(sql) {
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: {
              channel_id: "UCrepair",
              status: "active",
              agent_status: "done",
              title: "Repair Channel",
              handle: "@repair-channel",
              keywords: [],
              available_tabs: ["videos"],
            },
            run: {
              run_id: "run:repair",
              channel_id: "UCrepair",
              candidate_id: 43,
              crawl_mode: "full",
              detail_status: "done",
              started_at: "2026-07-27T12:10:08.405Z",
              result_json: pendingAboutVisible
                ? { pending_initial_about_observation: pendingAbout }
                : { initial_about_observation_id: persistedAboutObservationId },
            },
          }],
        };
      }
      if (sql.includes("SELECT to_jsonb(content)")) return { rows: [] };
      if (sql.includes("FROM crawler.contents")) return { rows: [] };
      if (sql.includes("FROM crawler.agent_profiles")) {
        return {
          rows: [{
            channel_id: "UCrepair",
            status: "success",
            metrics_json: {},
            input_content_ids: [],
            input_content_hash: inputContentHash,
            taxonomy_version: "qy-taxonomy-v1",
            agent_version_hash: agentVersionHash,
            observed_at: "2026-07-27T12:10:08.405Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.crawl_observations")) {
        return {
          rows: persistedAboutObservationId
            ? [{ observation_id: persistedAboutObservationId, observation_kind: "about" }]
            : [],
        };
      }
      if (sql.includes("result_json-'pending_initial_about_observation'")) {
        pendingAboutVisible = false;
      }
      return { rowCount: 1, rows: [] };
    },
  };
  const recordAbout = async (_client, command) => {
    aboutCommands.push(structuredClone(command));
    persistedAboutObservationId = "repair-about";
    return { observation_id: "repair-about" };
  };
  const recordObservation = async (_client, command) => ({
    observation_id: `repair-${command.observationKind}`,
  });
  const input = {
    withTransaction: (action) => action(client),
    channelId: "UCrepair",
    runId: "run:repair",
    observedAt: "2026-07-27T12:10:08.405Z",
    revisionType: "repair",
    repairId: "publication-readiness-repair-20260727-v1",
    recordAbout,
    recordObservation,
  };

  await recordInitialFullObservations(input);
  await recordInitialFullObservations(input);

  assert.equal(aboutCommands.length, 1);
});

test("a Publication Gap child Repair does not require a Promotion candidate_id", () => {
  assert.equal(isPublicationGapChildRepairRun({
    candidate_id: null,
    result_json: {
      final_repair: { mode: "channel", parent_run_id: "run:promotion", rounds: 1 },
      publication_gap_repair: {
        status: "required",
        domains: ["channel"],
        root_run_id: "run:promotion",
      },
    },
  }, "repair"), true);
  assert.equal(isPublicationGapChildRepairRun({
    candidate_id: null,
    result_json: {
      final_repair: { mode: "channel", parent_run_id: "run:promotion", rounds: 1 },
      publication_gap_repair: {
        status: "required",
        domains: ["channel"],
        root_run_id: "run:promotion",
      },
    },
  }, "incremental"), false);
  assert.equal(isPublicationGapChildRepairRun({
    candidate_id: null,
    result_json: {
      final_repair: { mode: "channel", parent_run_id: "run:promotion", rounds: 1 },
    },
  }, "repair"), false);
});

test("Publication Gap child Repair observations record without a Promotion candidate_id", async () => {
  const recorded = [];
  const client = {
    async query(sql) {
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: {
              channel_id: "UCchild-gap",
              status: "active",
              agent_status: "done",
              subscriber_count: 100,
              subscriber_count_status: "estimated",
              total_view_count: 1000,
              total_view_count_status: "exact",
              total_video_count: 30,
              total_video_count_status: "exact",
            },
            run: {
              run_id: "run:child",
              channel_id: "UCchild-gap",
              candidate_id: null,
              crawl_mode: "full",
              detail_status: "done",
              started_at: "2026-08-17T09:01:13.000Z",
              result_json: {
                final_repair: {
                  mode: "channel",
                  rounds: 1,
                  parent_run_id: "run:promotion",
                },
                publication_gap_repair: {
                  status: "required",
                  domains: ["channel"],
                  root_run_id: "run:promotion",
                },
              },
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.contents")) return { rows: [] };
      if (sql.includes("FROM crawler.content_candidates")) return { rows: [] };
      if (sql.includes("FROM crawler.agent_profiles")) {
        return {
          rows: [{
            channel_id: "UCchild-gap",
            status: "success",
            metrics_json: {},
            input_content_ids: [],
            input_content_hash: "sha256:input",
            taxonomy_version: "qy-taxonomy-v1",
            agent_version_hash: `sha256:${"a".repeat(64)}`,
            observed_at: "2026-08-17T09:01:13.000Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.crawl_observations")) return { rows: [] };
      return { rowCount: 1, rows: [] };
    },
  };

  const result = await recordInitialFullObservations({
    withTransaction: (action) => action(client),
    channelId: "UCchild-gap",
    runId: "run:child",
    observedAt: "2026-08-17T09:02:09.000Z",
    revisionType: "repair",
    repairId: "final-repair:run:promotion:1",
    recordAbout: async (_client, command) => {
      recorded.push(command.observationKind ?? "about");
      return { observation_id: "child-about", outcome: "complete" };
    },
    recordObservation: async (_client, command) => {
      recorded.push(command.observationKind);
      return { observation_id: `child-${command.observationKind}`, outcome: "complete" };
    },
  });

  assert.equal(result.recorded, true);
  assert.deepEqual(recorded, ["about", "video", "agent"]);
  assert.deepEqual(result.outcomes, {
    about: "complete",
    video: "complete",
    agent: "complete",
  });
});

test("an ordinary Full Run without candidate_id is still incomplete", async () => {
  const result = await recordInitialFullObservations({
    withTransaction: (action) => action({
      async query() {
        return {
          rows: [{
            channel: { channel_id: "UCplain", status: "active", agent_status: "done" },
            run: {
              run_id: "run:plain",
              channel_id: "UCplain",
              candidate_id: null,
              crawl_mode: "full",
              detail_status: "done",
              result_json: {},
            },
          }],
        };
      },
    }),
    channelId: "UCplain",
    runId: "run:plain",
    revisionType: "repair",
    repairId: "final-repair:run:plain:1",
    recordAbout: async () => assert.fail("ordinary Full Run without candidate_id must not record"),
    recordObservation: async () => assert.fail("ordinary Full Run without candidate_id must not record"),
  });

  assert.deepEqual(result, {
    recorded: false,
    reason: "full_crawl_not_complete",
    observations: {},
  });
});

test("initial Full observations keep staged About data while Agent is incomplete", async () => {
  const calls = [];
  const client = {
    async query(sql) {
      calls.push(sql);
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: { channel_id: "UCfull", status: "active", agent_status: "pending" },
            run: {
              run_id: "run:full",
              channel_id: "UCfull",
              candidate_id: 42,
              crawl_mode: "full",
              detail_status: "done",
              result_json: {
                pending_initial_about_observation: {
                  channelId: "UCfull",
                  runId: "run:full",
                },
              },
            },
          }],
        };
      }
      return { rows: [] };
    },
  };

  const result = await recordInitialFullObservations({
    withTransaction: (action) => action(client),
    channelId: "UCfull",
    runId: "run:full",
    recordAbout: async () => assert.fail("About must stay staged"),
    recordObservation: async () => assert.fail("no domain may publish before Agent"),
  });

  assert.deepEqual(result, {
    recorded: false,
    reason: "full_crawl_not_complete",
    observations: {},
  });
  assert.equal(calls.length, 1);
});

test("dormant initial Full observations consume About and reuse the Activity Gate Video", async () => {
  const calls = [];
  const pendingAbout = {
    idempotencyKey: "about:run:dormant:attempt:1",
    channelId: "UCdormant",
    runId: "run:dormant",
    observedAt: "2026-07-28T10:00:00Z",
    triggerReason: "initial_full",
    about: {},
    current: {},
  };
  const client = {
    async query(sql, params = []) {
      calls.push({ type: "query", sql, params });
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: {
              channel_id: "UCdormant",
              status: "dormant",
              agent_status: "skipped",
              title: "Dormant Channel",
            },
            run: {
              run_id: "run:dormant",
              channel_id: "UCdormant",
              candidate_id: 84,
              crawl_mode: "full",
              detail_status: "done",
              started_at: "2026-07-28T09:00:00Z",
              result_json: { pending_initial_about_observation: pendingAbout },
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.contents")) return { rows: [] };
      if (sql.includes("FROM crawler.content_candidates")) return { rows: [] };
      if (sql.includes("FROM crawler.agent_profiles")) return { rows: [] };
      if (sql.includes("FROM crawler.crawl_observations")) {
        return { rows: [{ observation_id: "gate-video", observation_kind: "video" }] };
      }
      return { rowCount: 1, rows: [] };
    },
  };
  const result = await recordInitialFullObservations({
    withTransaction: (action) => action(client),
    channelId: "UCdormant",
    runId: "run:dormant",
    observedAt: "2026-07-28T10:00:00Z",
    recordAbout: async (_client, command) => {
      calls.push({ type: "observation", kind: "about", command });
      return { observation_id: "dormant-about" };
    },
    recordObservation: async () => assert.fail("Video must be reused and Agent must stay skipped"),
  });

  assert.equal(result.recorded, true);
  assert.deepEqual(Object.keys(result.observations), ["about"]);
  assert.equal(calls.some((call) => call.type === "observation" && call.kind === "about"), true);
  assert.equal(calls.some((call) => (
    call.type === "query"
    && call.sql.includes("result_json-'pending_initial_about_observation'")
    && call.params[1] === "dormant-about"
  )), true);
});

test("About-only Repair replaces About and reuses existing Video and Agent observations", async () => {
  const aboutCommands = [];
  let pendingAboutConsumed = false;
  const pendingAbout = {
    idempotencyKey: "about:run:about-gap:attempt:4",
    channelId: "UCabout-gap",
    runId: "run:about-gap",
    observedAt: "2026-08-16T10:00:00.000Z",
    triggerReason: "repair",
    about: { outcome: "complete" },
    current: {},
  };
  const client = {
    async query(sql) {
      if (sql.includes("row_to_json(channel)")) {
        return {
          rows: [{
            channel: {
              channel_id: "UCabout-gap",
              status: "active",
              agent_status: "done",
            },
            run: {
              run_id: "run:about-gap",
              channel_id: "UCabout-gap",
              candidate_id: 42,
              crawl_mode: "full",
              detail_status: "done",
              started_at: "2026-08-16T09:00:00.000Z",
              result_json: {
                publication_gap_repair_execution: { scope: "about_only" },
                pending_initial_about_observation: pendingAbout,
              },
            },
          }],
        };
      }
      if (sql.includes("FROM crawler.contents")) return { rows: [] };
      if (sql.includes("FROM crawler.content_candidates")) return { rows: [] };
      if (sql.includes("FROM crawler.agent_profiles")) {
        return {
          rows: [{
            channel_id: "UCabout-gap",
            status: "success",
            metrics_json: {},
            input_content_ids: [],
            input_content_hash: "sha256:input",
            taxonomy_version: "qy-taxonomy-v1",
            agent_version_hash: `sha256:${"a".repeat(64)}`,
            observed_at: "2026-08-16T09:00:00.000Z",
          }],
        };
      }
      if (sql.includes("FROM crawler.crawl_observations")) {
        return {
          rows: [
            { observation_id: "old-about", observation_kind: "about", outcome: "partial" },
            { observation_id: "existing-video", observation_kind: "video", outcome: "complete" },
            { observation_id: "existing-agent", observation_kind: "agent", outcome: "complete" },
          ],
        };
      }
      if (sql.includes("result_json-'pending_initial_about_observation'")) {
        pendingAboutConsumed = true;
      }
      return { rowCount: 1, rows: [] };
    },
  };

  const result = await recordInitialFullObservations({
    withTransaction: (action) => action(client),
    channelId: "UCabout-gap",
    runId: "run:about-gap",
    observedAt: "2026-08-16T10:00:00.000Z",
    revisionType: "repair",
    repairId: "publication-gap-about-only-1",
    recordAbout: async (_client, command) => {
      aboutCommands.push(command);
      return { observation_id: "new-about", outcome: "complete" };
    },
    recordObservation: async (_client, command) => {
      assert.fail(`About-only Repair must not rewrite ${command.observationKind}`);
    },
  });

  assert.equal(pendingAboutConsumed, true);
  assert.equal(aboutCommands.length, 1);
  assert.equal(aboutCommands[0].idempotencyKey, pendingAbout.idempotencyKey);
  assert.deepEqual(Object.keys(result.observations), ["about"]);
  assert.deepEqual(result.outcomes, {
    about: "complete",
    video: "complete",
    agent: "complete",
  });
});
