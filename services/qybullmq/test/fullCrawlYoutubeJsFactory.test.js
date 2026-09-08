import assert from "node:assert/strict";
import test from "node:test";
import { createFullCrawlYoutubeJsExecutor } from "../src/fullCrawlYoutubeJsFactory.js";
import { YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT, YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";

const OBSERVED_AT = "2026-09-04T00:00:00.000Z";

test("completed About-only publication repair is replayed without refetching or restaging", async () => {
  const value = job();
  Object.assign(value.data, { publication_gap_domains: ["channel"],
    publication_gap_root_run_id: value.data.run_id, publication_gap_scope: "about_only",
    require_complete_about_metrics: true });
  const state = checkpointState("handoff");
  state.run.publication_finalized_status = "ready_auto";
  state.run.result_json.publication_gap_repair_execution = { scope: "about_only", status: "staged" };
  const unexpected = async () => assert.fail("completed repair must not execute again");
  const execute = createFullCrawlYoutubeJsExecutor({
    store: { loadSettings: async () => ({}), restore: async () => state, completeAboutOnlyRepair: unexpected },
    youtube: { fetchChannel: unexpected, fetchUploads: unexpected, fetchDetail: unexpected },
    handoff: { fetchCompleted: unexpected },
  });
  assert.equal((await execute(value)).already_complete, true);
});

for (const complete of [true, false]) {
  test(`About-only repair fetches fresh About despite completed Full Crawl checkpoint (complete=${complete})`, async () => {
    const repairJob = job();
    Object.assign(repairJob.data, {
      publication_gap_domains: ["channel"],
      publication_gap_root_run_id: repairJob.data.run_id,
      publication_gap_scope: "about_only",
      require_complete_about_metrics: true,
    });
    const calls = [];
    const executor = createFullCrawlYoutubeJsExecutor({
      clock: () => OBSERVED_AT,
      store: {
        loadSettings: async () => ({}),
        restore: async () => checkpointState("handoff", { fetch: { status: "complete", stored_count: 30 } }),
        async completeAboutOnlyRepair(receivedJob, { aboutObservation, enqueueFinalize }) {
          assert.equal(receivedJob, repairJob);
          assert.equal(aboutObservation.about.total_view_count, 123456);
          assert.equal(aboutObservation.triggerReason, "repair");
          calls.push("stage_about");
          await enqueueFinalize({ channelId: "UCfull", runId: "run-full-1", reason: "publication-gap-about-only" });
          return { scope: "about_only", candidate_count: 30, about_outcome: "complete" };
        },
      },
      youtube: {
        async fetchChannel() {
          calls.push("fetch_about");
          const snapshot = channelSnapshot();
          Object.assign(snapshot.metadata, {
            total_view_count: complete ? 123456 : null,
            view_count_text: complete ? null : "unsupported views format",
            view_count_source: "youtube_about",
            total_video_count: 30,
            video_count_source: "youtube_about",
          });
          return snapshot;
        },
        fetchUploads: async () => assert.fail("must not refetch uploads"),
        fetchDetail: async () => assert.fail("must not refetch video details"),
      },
      handoff: { fetchCompleted: async ({ reason }) => { assert.equal(reason, "publication-gap-about-only"); calls.push("finalize"); } },
    });
    if (complete) {
      const result = await executor(repairJob, { resumeMode: "resume" });
      assert.equal(result.scope, "about_only");
      assert.deepEqual(calls, ["fetch_about", "stage_about", "finalize"]);
    } else {
      await assert.rejects(executor(repairJob), { code: "publication_gap_about_incomplete" });
      assert.deepEqual(calls, ["fetch_about"]);
    }
  });
}

function job() {
  return {
    id: "channel-snapshot__batch-1__UCfull__g1",
    name: "channel-snapshot",
    queueName: "youtube-channel-crawl",
    attemptsStarted: 1,
    data: {
      channel_id: "UCfull",
      candidate_id: 41,
      run_id: "run-full-1",
      business_run_key: "full:41:g1",
      dispatch_batch_id: "batch-1",
      dispatch_generation: 1,
      fetch_contract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
    },
    async updateProgress() {},
  };
}

function target(videoId, position) {
  return {
    video_id: videoId,
    position,
    url: `https://www.youtube.com/watch?v=${videoId}`,
    title: `Video ${position}`,
    published_at: "2026-09-03T00:00:00.000Z",
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "youtubejs_player_microformat",
  };
}

function checkpointState(phase, {
  candidates = [],
  fetch = null,
} = {}) {
  return {
    phase,
    identity: {
      channelId: "UCfull",
      candidateId: 41,
      runId: "run-full-1",
      businessRunKey: "full:41:g1",
      dispatchBatchId: "batch-1",
      candidateAttemptFence: {
        candidateId: 41,
        dispatchGeneration: 1,
        jobId: "channel-snapshot__batch-1__UCfull__g1",
        bullmqAttempt: 1,
      },
    },
    channel: {
      source_json: { channel_header: { available_tabs: ["Videos"] } },
    },
    run: {
      content_limit: 2,
      started_at: OBSERVED_AT,
      result_json: {
        content_max_age_days: 90,
        migration_activity_gate: { required: false, decision: "not_required" },
      },
    },
    candidates,
    fetch,
  };
}

function channelSnapshot() {
  return {
    about_requested: true,
    about_observed: true,
    metadata: {
      channel_id: "UCfull",
      channel_url: "https://www.youtube.com/channel/UCfull",
      handle: "@full",
      title: "Full Channel",
      country: "Brazil",
      subscriber_count: 10_000,
      subscriber_count_text: "10,000",
      subscriber_count_source: "youtube_about",
    },
    raw: { engine: "youtubejs", request_counts: { get_channel: 1, get_about: 1 } },
  };
}

function publicDetail(videoId) {
  return {
    id: videoId,
    title: `Detail ${videoId}`,
    published_at: "2026-09-03T00:00:00.000Z",
    view_count_text: "100",
    duration_seconds: 60,
    access_status: "public",
    access_status_source: "youtubejs_player",
    playability_kind: "content",
    comments_disabled: true,
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
  };
}

function scriptedFixture({ phase = "admission", candidates = [] } = {}) {
  const calls = [];
  let state = checkpointState(phase, { candidates });
  let detailQueue = candidates
    .filter((candidate) => !["done", "unavailable"].includes(candidate.detail_status))
    .map((candidate) => ({ ...candidate }));
  const store = {
    async loadSettings() {
      calls.push("store:settings");
      return { minSubscriberCount: 1000, channelContentLimit: 2, contentMaxAgeDays: 90 };
    },
    async restore() {
      calls.push(`store:restore:${state.phase}`);
      return state;
    },
    async beginAdmission() {
      calls.push("store:begin-admission");
    },
    async commitAdmission() {
      calls.push("store:commit-admission");
      state = checkpointState("uploads");
      return { committed: true, existing: false };
    },
    async settleTerminalCheckpoint() {
      calls.push("store:settle-terminal-checkpoint");
    },
    async commitUploads(_job, { targets }) {
      calls.push("store:commit-uploads");
      detailQueue = targets.map((entry, index) => ({
        candidate_id: index + 1,
        detail_status: "queued",
        target: entry,
      }));
      state = checkpointState(detailQueue.length > 0 ? "detail" : "close_fetch", {
        candidates: detailQueue,
      });
      return { committed: true };
    },
    async claimDetailExecution() {
      calls.push("store:claim-execution");
      return { claimed: true };
    },
    async claimNextDetail() {
      const candidate = detailQueue.shift() ?? null;
      calls.push(candidate
        ? `store:claim-detail:${candidate.target.video_id}`
        : "store:claim-detail:none");
      return candidate;
    },
    async commitDetail(_fence, candidate) {
      calls.push(`store:commit-detail:${candidate.target.video_id}`);
    },
    async closeFetch() {
      calls.push("store:close-fetch");
      const selectedCount = state.candidates.length;
      state = checkpointState("handoff", {
        candidates: state.candidates,
        fetch: { status: "complete", selected_count: selectedCount },
      });
      return {
        receipt: { status: "complete", selected_count: selectedCount },
        candidateCount: selectedCount,
        migrationActivity: { decision: "not_required" },
      };
    },
  };
  const youtube = {
    async fetchChannel() {
      calls.push("youtube:channel");
      return channelSnapshot();
    },
    async fetchUploads() {
      calls.push("youtube:uploads");
      return {
        playlist_id: "UUfull",
        entries: [target("video-1", 1), target("video-2", 2)],
        activity_evidence_complete: true,
        scan: {
          complete: true,
          stop_reason: "limit",
          terminal_reason: "limit",
          pages: 1,
          inspected_count: 2,
          parse_gap_count: 0,
        },
      };
    },
    async fetchDetail(videoId, options) {
      assert.equal(options.optionalComments, true);
      assert.equal(options.strictRequiredSurfaces, true);
      calls.push(`youtube:detail:${videoId}`);
      return publicDetail(videoId);
    },
  };
  const handoff = {
    async candidateSettled() {
      calls.push("handoff:candidate");
    },
    async fetchCompleted() {
      calls.push("handoff:fetch");
    },
  };
  return {
    calls,
    store,
    youtube,
    handoff,
    setState(value) { state = value; },
  };
}

function executor(fixture) {
  return createFullCrawlYoutubeJsExecutor({
    store: fixture.store,
    youtube: fixture.youtube,
    handoff: fixture.handoff,
    videoApiFallback: fixture.videoApiFallback,
    clock: () => OBSERVED_AT,
    locale: "en",
  });
}

test("v3 video fallback returns through the original Detail checkpoint and handoff", async () => {
  const fixture = scriptedFixture();
  let fallbackCalls = 0;
  fixture.youtube.fetchDetail = async () => { throw new Error("exhausted"); };
  fixture.videoApiFallback = async request => {
    fallbackCalls += 1;
    assert.equal(request.consumer, "full");
    assert.equal(request.runId, "run-full-1");
    assert.equal(request.optionalComments, true);
    return request.validate(publicDetail(request.videoId));
  };
  const input = job();
  input.data.fetch_contract = YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT;
  const result = await executor(fixture)(input);
  assert.equal(result.ok, true);
  assert.equal(fallbackCalls, 2);
  assert.ok(fixture.calls.includes("store:commit-detail:video-1"));
  assert.ok(fixture.calls.includes("handoff:fetch"));
});

test("frozen v2 runs never enter the v3 API fallback", async () => {
  const fixture = scriptedFixture();
  fixture.videoApiFallback = async () => assert.fail("v2 fallback is forbidden");
  assert.equal((await executor(fixture)(job())).ok, true);
});

test("initial execution commits every phase and serial Detail observations", async () => {
  const fixture = scriptedFixture();
  const result = await executor(fixture)(job());

  assert.deepEqual(result.executed_phases, [
    "admission",
    "uploads",
    "detail",
    "close_fetch",
    "handoff",
  ]);
  assert.equal(result.detail_processed, 2);
  assert.deepEqual(
    fixture.calls.filter((call) => call.startsWith("youtube:")),
    ["youtube:channel", "youtube:uploads", "youtube:detail:video-1", "youtube:detail:video-2"],
  );
  assert.ok(
    fixture.calls.indexOf("store:commit-detail:video-1")
      < fixture.calls.indexOf("youtube:detail:video-2"),
  );
});

test("Uploads checkpoint recovery does not fetch Channel again", async () => {
  const fixture = scriptedFixture({ phase: "uploads" });
  const result = await executor(fixture)(job(), { resumeMode: "network_attempt_resume" });

  assert.equal(result.resumed, true);
  assert.equal(fixture.calls.includes("youtube:channel"), false);
  assert.equal(fixture.calls.includes("youtube:uploads"), true);
  assert.equal(fixture.calls.filter((call) => call.startsWith("youtube:detail:")).length, 2);
});

test("v1 and v2 both recover actual comment request failures before storing a video", async () => {
  for (const contract of [YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT, YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT]) {
    const fixture = scriptedFixture({ phase: "uploads" });
    fixture.youtube.fetchDetail = async (id, options) => {
      assert.equal(options.optionalComments, contract.executor_version === 2);
      return { ...publicDetail(id), comments_disabled: false, youtubejs_comments_error: "comments timeout" };
    };
    const value = job();
    value.data.fetch_contract = contract;
    await assert.rejects(executor(fixture)(value), error => error.required_surface === "comments");
    assert.equal(fixture.calls.some(call => call.startsWith("store:commit-detail:")), false);
  }
});

test("Admission handoff failure is replayed without fetching Channel again", async () => {
  const fixture = scriptedFixture();
  const handoffError = new Error("discovery wakeup unavailable");
  fixture.handoff.candidateSettled = async () => {
    fixture.calls.push("handoff:candidate:failed");
    throw handoffError;
  };

  await assert.rejects(executor(fixture)(job()), handoffError);
  fixture.handoff.candidateSettled = async () => {
    fixture.calls.push("handoff:candidate:replayed");
  };
  await executor(fixture)(job(), { resumeMode: "network_attempt_resume" });

  assert.equal(
    fixture.calls.filter((call) => call === "youtube:channel").length,
    1,
  );
  assert.equal(fixture.calls.includes("handoff:candidate:replayed"), true);
});

test("Detail checkpoint recovery fetches only unfinished Candidates", async () => {
  const fixture = scriptedFixture({
    phase: "detail",
    candidates: [
      { candidate_id: 1, detail_status: "done", target: target("video-1", 1) },
      { candidate_id: 2, detail_status: "failed", target: target("video-2", 2) },
    ],
  });
  const result = await executor(fixture)(job(), { resumeMode: "network_attempt_resume" });

  assert.equal(result.detail_processed, 1);
  assert.equal(fixture.calls.includes("youtube:channel"), false);
  assert.equal(fixture.calls.includes("youtube:uploads"), false);
  assert.deepEqual(
    fixture.calls.filter((call) => call.startsWith("youtube:detail:")),
    ["youtube:detail:video-2"],
  );
});

test("Fetch completion recovery replays only the idempotent handoff", async () => {
  const fixture = scriptedFixture();
  fixture.setState(checkpointState("handoff", {
    candidates: [{ candidate_id: 1, detail_status: "done", target: target("video-1", 1) }],
    fetch: { status: "complete", selected_count: 1 },
  }));
  const result = await executor(fixture)(job(), { resumeMode: "network_attempt_resume" });

  assert.deepEqual(result.executed_phases, ["handoff"]);
  assert.equal(fixture.calls.some((call) => call.startsWith("youtube:")), false);
  assert.equal(fixture.calls.includes("store:claim-execution"), false);
  assert.equal(fixture.calls.includes("store:close-fetch"), false);
  assert.equal(fixture.calls.at(-1), "handoff:fetch");
});

test("a terminal Candidate recovery also settles its reserved Business Run", async () => {
  const fixture = scriptedFixture();
  fixture.setState({
    ...checkpointState("terminal"),
    binding: { status: "reserved" },
    terminalResult: {
      ok: true,
      skipped: true,
      skip_reason: "subscriber_count_below_minimum",
    },
  });

  const result = await executor(fixture)(job(), { resumeMode: "network_attempt_resume" });

  assert.equal(result.skip_reason, "subscriber_count_below_minimum");
  assert.equal(fixture.calls.includes("store:settle-terminal-checkpoint"), true);
  assert.equal(fixture.calls.includes("handoff:candidate"), true);
  assert.equal(fixture.calls.some((call) => call.startsWith("youtube:")), false);
});

test("a failed Detail request leaves the current Candidate uncommitted", async () => {
  const fixture = scriptedFixture({
    phase: "detail",
    candidates: [{ candidate_id: 1, detail_status: "queued", target: target("video-1", 1) }],
  });
  const routeError = Object.assign(new Error("proxy connection reset"), { code: "ECONNRESET" });
  fixture.youtube.fetchDetail = async () => {
    fixture.calls.push("youtube:detail:video-1");
    throw routeError;
  };

  await assert.rejects(executor(fixture)(job()), routeError);
  assert.equal(fixture.calls.includes("store:commit-detail:video-1"), false);
  assert.equal(fixture.calls.includes("store:close-fetch"), false);
});

for (const handle of [null, undefined, "@full"]) {
  test(`optional channel handle survives admission and fetch (handle=${handle})`, async () => {
    const fixture = scriptedFixture();
    const snapshot = channelSnapshot();
    snapshot.metadata.handle = handle;
    fixture.youtube.fetchChannel = async () => snapshot;
    const commit = fixture.store.commitAdmission;
    fixture.store.commitAdmission = async (...args) => {
      assert.equal(args[1].metadata.handle, handle);
      return commit(...args);
    };
    assert.equal((await executor(fixture)(job())).ok, true);
    assert.ok(fixture.calls.includes("store:commit-admission"));
    assert.ok(fixture.calls.includes("handoff:fetch"));
  });
}

for (const field of ["title", "channel_url", "channel_id"]) {
  test(`optional handle does not weaken channel identity validation (${field})`, async () => {
    const fixture = scriptedFixture();
    const snapshot = channelSnapshot();
    snapshot.metadata.handle = null;
    snapshot.metadata[field] = null;
    fixture.youtube.fetchChannel = async () => snapshot;
    await assert.rejects(executor(fixture)(job()));
    assert.ok(!fixture.calls.includes("store:commit-admission"));
  });
}
