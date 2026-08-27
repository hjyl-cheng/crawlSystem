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

test("a nested structured Fingerprint proxy failure does not depend on error text", () => {
  const gatewayError = Object.assign(new Error("gateway request failed"), {
    failureKind: "proxy_transport",
    code: "FINGERPRINT_PROXY_TRANSPORT",
    curlCode: 35,
    youtube_failure_evidence: {
      source: "fingerprint_gateway",
      status: null,
      body: "opaque gateway failure",
    },
  });
  const error = new Error("channel snapshot failed", { cause: gatewayError });
  const result = decideYoutubeFailure({ error });
  assert.deepEqual(result, {
    kind: "proxy_transport",
    retry_mode: "new_identity",
    proxy_action: "cooldown_network",
    client_action: "none",
    terminal: false,
    status: null,
    evidence: {
      failure_kind: "proxy_transport",
      code: "FINGERPRINT_PROXY_TRANSPORT",
      source: "fingerprint_gateway",
    },
  });
});

test("structured Fingerprint upstream failures keep the same identity", () => {
  const error = Object.assign(new Error("gateway request failed"), {
    failure_kind: "upstream_transient",
    code: "FINGERPRINT_UPSTREAM_TRANSIENT",
    youtube_failure_evidence: { source: "fingerprint_gateway" },
  });
  const result = decideYoutubeFailure({ error });
  assert.equal(result.kind, "upstream_transient");
  assert.equal(result.retry_mode, "same_identity");
  assert.equal(result.proxy_action, "none");
  assert.equal(result.evidence.source, "fingerprint_gateway");
});

test("structured failure evidence never borrows source from a wrapper", () => {
  const gatewayError = Object.assign(new Error("gateway request failed"), {
    failureKind: "proxy_transport",
    code: "FINGERPRINT_PROXY_TRANSPORT",
  });
  const error = Object.assign(new Error("unrelated wrapper"), {
    cause: gatewayError,
    youtube_failure_evidence: { source: "unrelated_wrapper" },
  });

  assert.equal(decideYoutubeFailure({ error }).evidence.source, null);
});

test("an unmarked generic TLS error is not blamed on the proxy", () => {
  const result = decideYoutubeFailure({
    error: new Error("SSLError curl_code=35"),
  });
  assert.notEqual(result.kind, "proxy_transport");
  assert.equal(result.proxy_action, "none");
});
