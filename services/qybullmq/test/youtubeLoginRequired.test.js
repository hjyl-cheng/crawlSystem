import assert from "node:assert/strict";
import test from "node:test";
import { fetchYoutubeJsVideoInfoWithTerminalFallback } from "../src/youtubeJs.js";
import { createVideoDetailApiFallback } from "../src/videoDetailApiFallback.js";
import { validateYoutubeJsVideoDetail } from "../src/youtubeJsVideoDetailContract.js";
import { resolveCollectedVideoOutcome } from "../src/collectedVideoOutcome.js";
import { isPleaseSignInResponse, loginRequiredDetail, loadLoginRequiredExclusion } from "../src/youtubeLoginRequired.js";
import { scannedVideoDispositionWork } from "../src/incrementalVideoBatchPlan.js";
import { resolveVideoDisposition, videoDispositionEligibleForImmediateRepair } from "../src/videoDisposition.js";
import { contentEnrichDetailOutcome } from "../src/contentEnrichPolicy.js";
import { applyIncrementalVideoDetail } from "../src/incrementalYoutubeJsVideo.js";

for (const throws of [false, true]) {
  test(`repeated Please sign in is excluded without API or route failure (${throws ? "exception" : "response"})`, async () => {
    const calls = [];
    const client = { getInfo: async (_id, { client }) => {
      calls.push(client);
      const surface = { status: "LOGIN_REQUIRED", reason: "Please sign in" };
      if (throws) throw Object.assign(new Error(surface.reason), { info: surface });
      return { playability_status: surface };
    } };
    const fallback = createVideoDetailApiFallback({
      query: async () => ({ rows: [] }),
      loadSettings: async () => assert.fail("login exclusion must not request API settings"),
    });
    const observation = await fallback({
      videoId: "login-video", runId: "run", requestId: "request", consumer: "full", attempt: 9,
      fetch: async () => (await fetchYoutubeJsVideoInfoWithTerminalFallback(client, "login-video")).detail,
      validate: detail => validateYoutubeJsVideoDetail("login-video", detail),
    });
    const { storageAction, disposition } = resolveCollectedVideoOutcome({
      candidate: { known_content_key: "existing", known_content_type: "video" },
      ...observation, observedAt: "2026-09-16T00:00:00Z",
    });
    assert.deepEqual(calls, ["WEB", "IOS"]);
    assert.equal(observation.access.access_status, "unknown");
    assert.equal(disposition.kind, "terminal_excluded");
    assert.equal(disposition.reason_code, "login_required");
    assert.equal(disposition.next_attempt_at, "infinity");
    assert.equal(disposition.retryable, false);
    assert.equal(storageAction.kind, "unresolved");
  });
}

test("only the exact login response qualifies; age, bot and ambiguous errors do not", () => {
  for (const reason of ["Please sign in", " Please sign in. ", "PLEASE SIGN IN!"]) {
    assert.equal(isPleaseSignInResponse("LOGIN_REQUIRED", reason), true);
  }
  for (const [status, reason] of [
    ["ERROR", "Please sign in"], [null, "Please sign in"],
    ["LOGIN_REQUIRED", "Sign in to confirm your age"],
    ["LOGIN_REQUIRED", "Sign in to confirm you're not a bot"],
    ["LOGIN_REQUIRED", "Please sign in to confirm you're not a bot"],
  ]) assert.equal(isPleaseSignInResponse(status, reason), false);
});

test("permanent exclusions survive later discovery and low-frequency repair", () => {
  for (const value of ["infinity", Infinity]) {
    const candidate = { disposition: "terminal_excluded", next_attempt_at: value,
      result_json: { disposition: { reason_code: "login_required" } } };
    assert.deepEqual(scannedVideoDispositionWork([{ id: "v" }], new Map([["v", candidate]]),
      "2099-01-01T00:00:00Z"), { workEntries: [], pendingDeferredVideoIds: [] });
    assert.equal(videoDispositionEligibleForImmediateRepair(candidate), false);
  }
  const disposition = resolveVideoDisposition({ priorDisposition: {
    kind: "terminal_excluded", reason_code: "login_required",
  }, observedAt: "2026-09-16", error: new Error("incomplete scan") });
  assert.equal(disposition.next_attempt_at, "infinity");
});

test("stored detail stays untouched and enrichment stops without a dead letter", async () => {
  const detail = loginRequiredDetail("v", []);
  const outcome = contentEnrichDetailOutcome({ task_id: "t" }, detail, new Date("2026-09-16"));
  assert.equal(outcome.kind, "terminal");
  assert.equal(outcome.next_retry_at, "infinity");
  assert.equal(outcome.access_status, null);
  const result = await applyIncrementalVideoDetail({ query: () => assert.fail("must not write contents") }, {
    row: { content_key: "stored", title: "Original", access_status: "public", view_count: 100 },
    detail, observedAt: "2026-09-16",
  });
  assert.equal(result.excluded, true);
});

test("cached exclusion can be released by an explicit finite retry time", async () => {
  const detail = loginRequiredDetail("v", []);
  const row = { disposition: "terminal_excluded", next_attempt_at: Infinity,
    result_json: { detail, disposition: { reason_code: "login_required" } } };
  const query = async () => ({ rows: [row] });
  assert.equal(await loadLoginRequiredExclusion(query, "channel", "v"), detail);
  row.next_attempt_at = new Date();
  assert.equal(await loadLoginRequiredExclusion(query, "channel", "v"), null);
});
