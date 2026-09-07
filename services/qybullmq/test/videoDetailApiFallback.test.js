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
test("full detail with no type evidence keeps the network failure and never spends API quota", async () => {
  const { fallback, options, requests } = harness();
  const error = networkError();
  await assert.rejects(fallback({ ...options, attempt: 9, fetch: async () => { throw error; } }), x => x === error);
  assert.equal(requests.length, 0);
});
for (const code of ["CONTENT_DETAIL_EXECUTION_FENCE_STALE", "CANDIDATE_ATTEMPT_FENCE_STALE", "23505"]) {
  test(`internal failure ${code} never enters API fallback`, async () => {
    const { fallback, options, requests } = harness();
    const error = Object.assign(new Error("internal failure"), { code });
    await assert.rejects(fallback({ ...options, attempt: 9, fetch: async () => { throw error; } }), x => x === error);
    assert.equal(requests.length, 0);
  });
}
test("API metadata cannot invent video type", async () => {
  const { fallback, options } = harness();
  await assert.rejects(fallback({ ...options, fetch: async () => { const error = parserError(); error.partial_detail = {}; throw error; } }),
    { name: "YoutubeJsRequiredSurfaceError" });
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
