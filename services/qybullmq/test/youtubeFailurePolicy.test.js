import assert from "node:assert/strict";
import test from "node:test";
import {
  annotateYoutubeFailure,
  decideYoutubeFailure,
  shouldReportProxyFailure,
  youtubeFailureText,
} from "../src/youtubeFailurePolicy.js";

test("plain 403 is a token or client problem until stronger evidence exists", () => {
  const result = decideYoutubeFailure({ status: 403, body: "Forbidden" });
  assert.deepEqual(result, {
    kind: "token_or_client",
    retry_mode: "route_or_token",
    proxy_action: "none",
    client_action: "refresh_or_fallback",
    terminal: false,
    status: 403,
  });
  assert.equal(shouldReportProxyFailure({ status: 403, body: "Forbidden" }), false);
});

test("explicit rate limits and bot challenges require a new identity", () => {
  assert.equal(decideYoutubeFailure({ status: 429 }).kind, "youtube_rate_limited");
  assert.equal(decideYoutubeFailure({ body: "Sign in to confirm you're not a bot" }).kind, "youtube_challenge");
  assert.equal(shouldReportProxyFailure({ status: 429 }), true);
});

test("proxy transport failures are separate from upstream timeouts", () => {
  const proxy = decideYoutubeFailure({ error: new Error("Tunnel connection failed: 502") });
  const upstream = decideYoutubeFailure({ error: new Error("YouTube request timed out") });
  assert.equal(proxy.kind, "proxy_transport");
  assert.equal(proxy.proxy_action, "cooldown_network");
  assert.equal(upstream.kind, "upstream_transient");
  assert.equal(upstream.proxy_action, "none");
});

test("content and PostgreSQL contract failures do not retry", () => {
  assert.equal(decideYoutubeFailure({ error: new Error("HTTP Error 404: video not found") }).retry_mode, "none");
  const databaseError = Object.assign(new Error("violates check constraint"), { code: "23514" });
  const database = decideYoutubeFailure({ error: databaseError });
  assert.equal(database.kind, "database_contract");
  assert.equal(database.terminal, true);
});

test("nested transport evidence is retained", () => {
  const error = new TypeError("fetch failed", {
    cause: Object.assign(new Error("SSL wrong version number"), { code: "ERR_SSL_WRONG_VERSION_NUMBER" }),
  });
  assert.equal(decideYoutubeFailure({ error }).kind, "proxy_transport");
  assert.match(youtubeFailureText(error), /ERR_SSL_WRONG_VERSION_NUMBER/);
});

test("structured adapter evidence survives an outer generic error message", () => {
  const error = annotateYoutubeFailure(new Error("request failed"), {
    status: 429,
    source: "youtubejs_player",
    targetUrl: "https://www.youtube.com/youtubei/v1/player",
  });
  const result = decideYoutubeFailure({ error });
  assert.equal(result.kind, "youtube_rate_limited");
  assert.equal(error.youtube_failure_evidence.source, "youtubejs_player");
});
