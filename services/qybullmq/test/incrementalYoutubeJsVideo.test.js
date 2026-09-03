import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL,
  INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL,
  captureIncrementalYoutubeJsVideoCheckpointPhase,
  fetchIncrementalYoutubeJsVideoDetail,
  incrementalYoutubeJsVideoCycleKey,
  incrementalYoutubeJsVideoFieldStatus,
  incrementalYoutubeJsVideoTargetHash,
} from "../src/incrementalYoutubeJsVideo.js";

test("an active Item claim is awaited without consuming a retry", async () => {
  const claimToken = "11111111-1111-4111-8111-111111111111";
  let claimCalls = 0;
  let stateCalls = 0;
  let detailCalls = 0;
  const client = {
    async query(sql) {
      if (String(sql).includes("SELECT status,first_seen_checkpoint_status")) {
        return { rows: [{ status: "fetching", first_seen_checkpoint_status: "complete" }] };
      }
      if (sql === INCREMENTAL_YOUTUBEJS_VIDEO_CLAIM_SQL) {
        claimCalls += 1;
        if (claimCalls === 1 || claimCalls === 3) return { rows: [] };
        return {
          rows: [{
            run_id: "run-claim-wait",
            cycle_key: "base",
            phase: "recent",
            ordinal: 0,
            video_id: "video-claim-wait",
            target_json: {},
            status: "claimed",
            claim_token: claimToken,
            claim_expires_at: new Date(Date.now() + 30_000),
            attempt_count: 2,
          }],
        };
      }
      if (sql === INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL) {
        return { rows: [{ video_id: "video-claim-wait" }] };
      }
      throw new Error(`unexpected transaction query: ${String(sql).slice(0, 80)}`);
    },
  };
  const stateQuery = async (sql) => {
    assert.match(sql, /active_claim_wait_ms/);
    stateCalls += 1;
    return stateCalls === 1
      ? {
          rows: [{
            total_count: 1,
            settled_count: 0,
            active_claim_count: 1,
            active_claim_wait_ms: 1,
          }],
        }
      : {
          rows: [{
            total_count: 1,
            settled_count: 1,
            active_claim_count: 0,
            active_claim_wait_ms: 0,
          }],
        };
  };

  const result = await captureIncrementalYoutubeJsVideoCheckpointPhase({
    runId: "run-claim-wait",
    cycleKey: "base",
    phase: "recent",
    query: stateQuery,
    withTransaction: async (action) => action(client),
    fetchDetail: async (videoId) => {
      detailCalls += 1;
      assert.equal(videoId, "video-claim-wait");
      return detail(videoId);
    },
    signal: null,
  });

  assert.deepEqual(result, {
    total: 1,
    settled: 1,
    activeClaims: 0,
    activeClaimWaitMs: 0,
  });
  assert.equal(claimCalls, 3);
  assert.equal(stateCalls, 2);
  assert.equal(detailCalls, 1);
});

function detail(videoId = "video-1") {
  return {
    id: videoId,
    title: "A title",
    thumbnail_url: "https://i.ytimg.com/example.jpg",
    published_at: "2026-09-01T00:00:00.000Z",
    published_at_status: "exact",
    duration_seconds: 61,
    view_count: 10,
    view_count_text: "10",
    like_count: 2,
    comment_count: 0,
    comment_count_status: "exact",
    comments_disabled: false,
    comments_first_page: { total_count: 0, returned_count: 0, comments: [] },
    description: "",
    description_observed: true,
    hashtags: [],
    hashtags_observed: true,
    keywords: ["one"],
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
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "youtubei.js@test",
    source: "youtubejs_get_info",
  };
}

test("cycle key is stable within a Run and changes only with committed recovery marker keys", () => {
  assert.equal(incrementalYoutubeJsVideoCycleKey({}), "base");
  const first = incrementalYoutubeJsVideoCycleKey({
    controlled_recoveries: {
      recover_partial: { operation_id: "recover_partial", prepared_at: "first" },
      recover_cap: { operation_id: "recover_cap", attempts_made_before: 1 },
    },
  });
  const sameKeys = incrementalYoutubeJsVideoCycleKey({
    controlled_recoveries: {
      recover_cap: { operation_id: "recover_cap", attempts_made_before: 99 },
      recover_partial: { operation_id: "recover_partial", prepared_at: "later" },
    },
    execution_attempt_id: "ignored",
  });
  assert.equal(first, sameKeys);
  assert.match(first, /^recovery:[a-f0-9]{64}$/);
  assert.notEqual(first, incrementalYoutubeJsVideoCycleKey({
    controlled_recoveries: {
      recover_partial: { operation_id: "recover_partial" },
    },
  }));
});

test("cycle key rejects malformed or conflicting recovery markers", () => {
  assert.throws(
    () => incrementalYoutubeJsVideoCycleKey({ controlled_recoveries: [] }),
    /must be an object/,
  );
  assert.throws(
    () => incrementalYoutubeJsVideoCycleKey({ controlled_recoveries: null }),
    /must be an object/,
  );
  assert.throws(
    () => incrementalYoutubeJsVideoCycleKey({
      controlled_recoveries: { recover_partial: { operation_id: "another_operation" } },
    }),
    /invalid controlled recovery marker/,
  );
});

test("target hash is order-independent but includes phase, ordinal, id, and target snapshot", () => {
  const items = [
    { phase: "recent", ordinal: 0, video_id: "stored", target_json: { score: 0.8 } },
    { phase: "first_seen", ordinal: 0, video_id: "new", target_json: { position: 1 } },
  ];
  assert.equal(
    incrementalYoutubeJsVideoTargetHash(items),
    incrementalYoutubeJsVideoTargetHash([...items].reverse()),
  );
  assert.notEqual(
    incrementalYoutubeJsVideoTargetHash(items),
    incrementalYoutubeJsVideoTargetHash([
      items[0],
      { ...items[1], target_json: { position: 2 } },
    ]),
  );
});

test("field status distinguishes observed empty, disabled, unavailable, and parser gap", () => {
  const statuses = incrementalYoutubeJsVideoFieldStatus(detail());
  assert.equal(statuses.description, "empty");
  assert.equal(statuses.hashtags, "empty");
  assert.equal(statuses.keywords, "exact");
  assert.equal(statuses.comment_count, "exact");
  assert.equal(statuses.access_status, "exact");
  assert.equal(statuses.live_started_at, "unobserved");

  const disabled = incrementalYoutubeJsVideoFieldStatus({
    ...detail(),
    comment_count: 0,
    comment_count_status: "disabled",
    comments_disabled: true,
  });
  assert.equal(disabled.comment_count, "disabled");
  assert.equal(disabled.comments_first_page, "disabled");

  const parserError = new Error("comments parser changed");
  parserError.required_surface = "comments";
  const partial = incrementalYoutubeJsVideoFieldStatus(detail(), parserError);
  assert.equal(partial.title, "exact");
  assert.equal(partial.comment_count, "parser_gap");
  assert.equal(partial.comments_first_page, "parser_gap");

  const unavailable = incrementalYoutubeJsVideoFieldStatus({
    id: "private",
    access_status: "private",
  });
  assert.equal(unavailable.access_status, "unavailable");
  assert.equal(unavailable.title, "unobserved");
});

test("field status records a normalized YouTubeJS numeric view count as exact", () => {
  const observed = detail();
  delete observed.view_count;
  observed.view_count_text = "375";
  const beforeNormalization = incrementalYoutubeJsVideoFieldStatus(observed);
  assert.equal(beforeNormalization.view_count, "unobserved");

  observed.view_count = 375;
  observed.view_count_status = "exact";
  const normalized = incrementalYoutubeJsVideoFieldStatus(observed);
  assert.equal(normalized.view_count, "exact");
});

test("incremental detail fetch uses one strict YouTubeJS request and never falls back", async () => {
  let calls = 0;
  const value = await fetchIncrementalYoutubeJsVideoDetail("video-1", {
    fetchYoutubeJs: async (videoId, options) => {
      calls += 1;
      assert.equal(videoId, "video-1");
      assert.equal(options.strictRequiredSurfaces, true);
      return detail(videoId);
    },
  });
  assert.equal(calls, 1);
  assert.equal(value.id, "video-1");

  const original = new Error("detail failed");
  await assert.rejects(
    fetchIncrementalYoutubeJsVideoDetail("video-2", {
      fetchYoutubeJs: async () => {
        calls += 1;
        throw original;
      },
    }),
    (error) => error === original,
  );
  assert.equal(calls, 2);
});

test("incremental detail fetch preserves terminal facts and checkpoints parser gaps as partial", async () => {
  const incomplete = {
    ...detail("parser-gap"),
    title: null,
  };
  await assert.rejects(
    fetchIncrementalYoutubeJsVideoDetail("parser-gap", {
      fetchYoutubeJs: async () => incomplete,
    }),
    (error) => {
      assert.equal(error.name, "YoutubeJsRequiredSurfaceError");
      assert.equal(error.required_surface, "player");
      assert.equal(error.partial_detail, incomplete);
      return true;
    },
  );

  const privateDetail = {
    id: "private-video",
    access_status: "private",
    availability: "private",
    youtubejs_comments_error: "comments are not available",
    extractor_version: "youtubei.js@test",
    source: "youtubejs_get_info",
  };
  assert.equal(
    await fetchIncrementalYoutubeJsVideoDetail("private-video", {
      fetchYoutubeJs: async () => privateDetail,
    }),
    privateDetail,
  );
});

test("new executor source has no legacy import, yt-dlp fallback, or attempt identity", async () => {
  const source = await readFile(new URL("../src/incrementalYoutubeJsVideo.js", import.meta.url), "utf8");
  assert.doesNotMatch(source, /from ["']\.\/incrementalVideo\.js["']/);
  assert.doesNotMatch(source, /yt[_-]?dlp|ytdlp/i);
  assert.doesNotMatch(source, /executionAttemptId|execution_attempt_id/);
  assert.match(source, /video:\$\{runId\}:youtubejs:\$\{cycleKey\}/);
  assert.match(source, /detail_concurrency|captureCheckpointPhase|status='pending'/);
});

test("Worker refuses the checkpoint executor when YouTubeJS Detail is disabled", async () => {
  const source = await readFile(new URL("../src/worker.js", import.meta.url), "utf8");
  assert.match(source, /youtubeJsDetailEnabled/);
  assert.match(
    source,
    /incrementalVideoExecutorMode === "youtubejs_checkpoint_v1" && !youtubeJsDetailEnabled\(\)/,
  );
  assert.match(
    source,
    /INCREMENTAL_VIDEO_EXECUTOR=youtubejs_checkpoint_v1 requires YOUTUBEJS_EXTRACTOR_MODE=full/,
  );
});
