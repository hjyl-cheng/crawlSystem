import assert from "node:assert/strict";
import test from "node:test";
import { createVideoDetailApiFallback, withVideoFallbackExecution, mergeVideoApiEvidence } from "../src/videoDetailApiFallback.js";
import { projectVideoDetail } from "../src/videoDetailEvidence.js";
import { validateYoutubeJsVideoDetail } from "../src/youtubeJsVideoDetailContract.js";
import { retryableRotaFailure } from "../src/managedWorkerExecution.js";
import { selectYoutubeFailure } from "../src/youtubeFailurePolicy.js";
import { videoApiResultError } from "../src/videoApiBatchRequests.js";
import { fetchIncrementalYoutubeJsVideoDetail } from "../src/incrementalYoutubeJsVideo.js";

const partial = { content_type_signals: { source: "youtubejs_player", canonical_url: "https://www.youtube.com/shorts/video1" } };
const api = { title: "API title", published_at: "2026-09-01T00:00:00Z", view_count_text: "100",
  duration_seconds: 60, privacy_status: "public", comments_disabled: true, source: "youtube_data_api_videos_list" };
const parserError = () => Object.assign(new Error("required player surface incomplete"), {
  name: "YoutubeJsRequiredSurfaceError", partial_detail: partial,
});
const networkError = () => Object.assign(new Error("connect ECONNRESET"), { code: "ECONNRESET" });
test("exact API counts replace unresolved or estimated scraper count statuses", () => {
  const facts = projectVideoDetail(mergeVideoApiEvidence("video1", {
    view_count_status: "estimated", like_count_status: "unresolved", like_count: null,
  }, { ...api, like_count: 17 }));
  assert.equal(facts.view_count_status, "exact");
  assert.equal(facts.like_count_status, "exact");
  assert.equal(facts.like_count, 17);
});
function harness({ existing = null, settings = {}, result = api } = {}) {
  const requests = [];
  const fallback = createVideoDetailApiFallback({
    query: async () => ({ rows: existing ? [existing] : [] }),
    withTransaction: async fn => fn({}),
    loadSettings: async () => ({ apiKeys: ["test-key"], dailyRequestLimit: 50, fallbackMode: "emergency", ...settings }),
    request: async (_, value) => { requests.push(value); }, wait: async () => result,
  });
  const options = { videoId: "video1", requestId: "request1", runId: "run1", consumer: "full", optionalComments: true,
    fetch: async () => { throw parserError(); },
    validate: detail => validateYoutubeJsVideoDetail("video1", detail, { optionalComments: true }) };
  return { fallback, options, requests };
}

for (const [consumer, detailMode] of [["full", "full"], ["incremental", "full"], ["incremental", "metrics"]]) {
  test(`${consumer} ${detailMode} comments failures use batch comment recovery only at the managed budget limit`, async () => {
    const comments = { version: 1, sort: "TOP_COMMENTS", total_count: 12, returned_count: 0, comments: [] };
    const { fallback, options, requests } = harness({ result: { ...api, comments_disabled: false,
      comment_count: 12, comment_count_status: "exact", comments_first_page: comments } });
    const error = Object.assign(new Error("required comments surface failed", { cause: networkError() }), {
      name: "YoutubeJsRequiredSurfaceError", required_surface: "comments",
      partial_detail: { ...partial, youtubejs_comments_error: "connect ECONNRESET" },
    });
    const call = () => fallback({ ...options, consumer, detailMode, optionalComments: true,
      fetch: async () => { throw error; } });
    await assert.rejects(withVideoFallbackExecution({ getBudget: async () => ({ business_tasks_used: 4, business_tasks_limit: 9 }) }, call), e => e === error);
    assert.equal(requests.length, 0);
    const resolved = await withVideoFallbackExecution({ getBudget: async () => ({ business_tasks_used: 9, business_tasks_limit: 9 }) }, call);
    assert.equal(requests.length, 1);
    assert.equal(requests[0].requireComments, true);
    assert.equal(resolved.detail.youtubejs_comments_error, null);
    assert.equal(resolved.detail.comment_count, 12);
  });
}
test("exhausted player parsing uses batch evidence and preserves authoritative Shorts", async () => {
  const { fallback, options, requests } = harness();
  const value = await fallback(options);
  assert.equal(value.classification.content_type, "short");
  assert.equal(value.classification.authoritative, true);
  assert.equal(value.detail.title, "API title");
  assert.equal(value.detail.video_detail_fallback.source, "youtube_data_api_batch");
  assert.equal(requests.length, 1);
});
test("successful YouTubeJS details never request API", async () => {
  const { fallback, options, requests } = harness();
  assert.equal(await fallback({ ...options, fetch: async () => "ok", validate: x => x }), "ok");
  assert.equal(requests.length, 0);
});
for (const attempt of [1, 2, 3]) test(`network checkpoint attempt ${attempt} respects the route retry cap`, async () => {
  const { fallback, options, requests } = harness();
  const error = networkError();
  const invoke = () => fallback({ ...options, detailMode: "metrics", attempt, fetch: async () => { throw error; }, validate: x => x });
  if (attempt < 3) await assert.rejects(invoke, candidate => candidate === error);
  else assert.equal((await invoke()).title, "API title");
  assert.equal(requests.length, attempt === 3 ? 1 : 0);
});
test("authoritative final business budget permits fallback before another route is rejected", async () => {
  const { fallback, options } = harness();
  const detail = await withVideoFallbackExecution({ getBudget: async () => ({ business_tasks_used: 8, business_tasks_limit: 8 }) },
    () => fallback({ ...options, detailMode: "metrics", fetch: async () => { throw networkError(); }, validate: x => x }));
  assert.equal(detail.title, "API title");
});
test("control-plane retries and final Bull attempt cannot exhaust the network budget", async () => {
  const { fallback, options, requests } = harness();
  const error = networkError();
  await assert.rejects(withVideoFallbackExecution({ lastJobAttempt: true,
    getBudget: async () => ({ business_tasks_used: 4, business_tasks_limit: 9 }) },
  () => fallback({ ...options, attempt: 5, detailMode: "metrics", fetch: async () => { throw error; } })), x => x === error);
  assert.equal(requests.length, 0);
});
test("exhausted full detail requests API even without type evidence and preserves unresolved type", async () => {
  const { fallback, options, requests } = harness();
  const result = await withVideoFallbackExecution({ getBudget: async () => ({ business_tasks_used: 9, business_tasks_limit: 9 }) },
    () => fallback({ ...options, fetch: async () => { throw networkError(); } }));
  assert.equal(requests.length, 1);
  assert.notEqual(result.classification?.authoritative, true);
  assert.equal(result.detail.title, "API title");
});

for (const code of ["CONTENT_DETAIL_EXECUTION_FENCE_STALE", "CANDIDATE_ATTEMPT_FENCE_STALE", "23505"]) {
  test(`internal failure ${code} never enters API fallback`, async () => {
    const { fallback, options, requests } = harness();
    const error = Object.assign(new Error("internal failure"), { code });
    await assert.rejects(fallback({ ...options, attempt: 9, fetch: async () => { throw error; } }), x => x === error);
    assert.equal(requests.length, 0);
  });
}
test("API metadata leaves video type unresolved without inventing one", async () => {
  const { fallback, options } = harness();
  const result = await fallback({ ...options, fetch: async () => { const error = parserError(); error.partial_detail = {}; throw error; } });
  assert.notEqual(result.classification?.authoritative, true);
});

test("resuming a durable request never fetches YouTubeJS again", async () => {
  const { fallback, options, requests } = harness({ existing: { run_id: "run1", source_content_id: "video1", consumer: "full", partial_detail: partial } });
  const result = await fallback({ ...options, fetch: async () => assert.fail("must consume durable API result") });
  assert.equal(result.detail.title, "API title");
  assert.equal(requests.length, 0);
});
test("disabled fallback preserves original error", async () => {
  const { fallback, options, requests } = harness({ settings: { fallbackMode: "disabled" } });
  await assert.rejects(fallback(options), { name: "YoutubeJsRequiredSurfaceError" });
  assert.equal(requests.length, 0);
});
test("unresolved API failures never inherit earlier proxy failures", () => {
  const error = videoApiResultError("API result unresolved");
  error.channel_execution_attempt = { failure_decisions: [{ kind: "proxy_transport" }] };
  assert.equal(retryableRotaFailure(error), null);
  assert.equal(selectYoutubeFailure({ error }).decision.retry_mode, "none");
});
for (const phase of ["first_seen", "recent"]) test(`incremental ${phase} consumes the same fallback`, async () => {
  const { fallback, requests } = harness();
  const result = await fetchIncrementalYoutubeJsVideoDetail("video1", {
    phase, fetchYoutubeJs: async () => { throw parserError(); }, videoApiFallback: fallback,
    checkpoint: { run_id: "run1", cycle_key: "cycle1", attempt_count: 3 },
  });
  assert.equal(result.title, "API title");
  assert.equal(requests[0].consumer, "incremental");
  assert.equal(requests[0].requireComments, phase === "first_seen");
});

test("resuming an untyped durable API result does not restart network collection", async () => {
  const { fallback, options, requests } = harness({ existing: { run_id: "run1", source_content_id: "video1", consumer: "full", partial_detail: {} } });
  const result = await fallback({ ...options, fetch: async () => assert.fail("API result must be reused") });
  assert.equal(requests.length, 0);
  assert.notEqual(result.classification?.authoritative, true);
});
