import assert from "node:assert/strict";
import test from "node:test";
import {
  annotateYoutubeFailure,
  decideYoutubeFailure,
  selectYoutubeFailure,
  shouldReportProxyFailure,
  youtubeFailureEvidence,
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
  const transport = annotateYoutubeFailure(
    Object.assign(new Error("SSL wrong version number"), { code: "ERR_SSL_WRONG_VERSION_NUMBER" }),
    { source: "youtube_fetch_transport" },
  );
  const error = new TypeError("fetch failed", { cause: transport });
  assert.equal(decideYoutubeFailure({ error }).kind, "proxy_transport");
  assert.match(youtubeFailureText(error), /ERR_SSL_WRONG_VERSION_NUMBER/);
});

test("trusted source on a wrapper cannot borrow TLS text from its cause", () => {
  const error = annotateYoutubeFailure(new TypeError("fetch failed", {
    cause: Object.assign(new Error("SSL wrong version number"), {
      code: "ERR_SSL_WRONG_VERSION_NUMBER",
    }),
  }), { source: "youtube_fetch_transport" });
  const result = decideYoutubeFailure({ error });

  assert.equal(result.kind, "upstream_transient");
  assert.equal(result.retry_mode, "same_identity");
  assert.equal(result.proxy_action, "none");
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

test("trusted Fingerprint legacy SSLError text rotates the proxy", () => {
  const gatewayError = Object.assign(
    new Error("fingerprint gateway request failed: SSLError curl_code=35"),
    { youtube_failure_evidence: { source: "fingerprint_gateway" } },
  );
  const result = decideYoutubeFailure({
    error: new Error("channel snapshot failed", { cause: gatewayError }),
  });

  assert.equal(result.kind, "proxy_transport");
  assert.equal(result.retry_mode, "new_identity");
  assert.equal(result.proxy_action, "cooldown_network");
});

test("trusted Fingerprint legacy proxy_transport text rotates the proxy", () => {
  const error = annotateYoutubeFailure(
    new Error("fingerprint gateway proxy_transport: connection failed"),
    { source: "fingerprint_gateway" },
  );
  const result = decideYoutubeFailure({ error });

  assert.equal(result.kind, "proxy_transport");
  assert.equal(result.proxy_action, "cooldown_network");
});

test("untrusted legacy Fingerprint-shaped text does not rotate the proxy", () => {
  for (const message of [
    "fingerprint gateway request failed: SSLError curl_code=35",
    "fingerprint gateway proxy_transport: connection failed",
  ]) {
    const result = decideYoutubeFailure({ error: new Error(message) });
    assert.notEqual(result.kind, "proxy_transport", message);
    assert.equal(result.proxy_action, "none", message);
  }
});

test("plain SSL routines text without trusted transport evidence does not rotate the proxy", () => {
  const result = decideYoutubeFailure({
    error: new Error("error:0A00010B:SSL routines::wrong version number"),
  });
  assert.notEqual(result.kind, "proxy_transport");
  assert.equal(result.proxy_action, "none");
});

test("trusted source metadata cannot be combined with TLS text from an AggregateError sibling", () => {
  const sourceOnly = annotateYoutubeFailure(new Error("managed request metadata"), {
    source: "youtubejs_fetch",
  });
  const unrelatedTls = new Error("error:0A00010B:SSL routines::wrong version number");
  const result = decideYoutubeFailure({
    error: new AggregateError([sourceOnly, unrelatedTls], "parallel request failures"),
  });

  assert.equal(result.kind, "upstream_transient");
  assert.equal(result.retry_mode, "same_identity");
  assert.equal(result.proxy_action, "none");
});

test("structured failure evidence does not borrow HTTP status from an AggregateError sibling", () => {
  const proxyFailure = Object.assign(new Error("gateway failed"), {
    failureKind: "proxy_transport",
    code: "FINGERPRINT_PROXY_TRANSPORT",
    youtube_failure_evidence: { source: "fingerprint_gateway" },
  });
  const missingContent = annotateYoutubeFailure(new Error("video missing"), {
    status: 404,
    source: "youtubejs_player",
  });
  const aggregate = new AggregateError([proxyFailure, missingContent], "parallel failures");
  const result = decideYoutubeFailure({ error: aggregate });

  assert.equal(result.kind, "proxy_transport");
  assert.equal(result.status, null);
  assert.equal(result.evidence.source, "fingerprint_gateway");
  const evidence = youtubeFailureEvidence(aggregate);
  assert.equal(evidence.status, null);
  assert.equal(evidence.source, "fingerprint_gateway");
});

test("nested Fingerprint evidence does not borrow an outer YouTube HTTP status", () => {
  const gatewayFailure = Object.assign(
    new Error("fingerprint gateway request failed: SSLError curl_code=35"),
    { youtube_failure_evidence: { source: "fingerprint_gateway" } },
  );
  const outerFailure = annotateYoutubeFailure(
    new Error("YouTube response was not found", { cause: gatewayFailure }),
    { status: 404, source: "youtubejs_player" },
  );
  const result = decideYoutubeFailure({ error: outerFailure });

  assert.equal(result.kind, "proxy_transport");
  assert.equal(result.status, null);
  assert.equal(result.evidence.source, "fingerprint_gateway");
  const evidence = youtubeFailureEvidence(outerFailure);
  assert.equal(evidence.status, null);
  assert.equal(evidence.source, "fingerprint_gateway");
});

test("failure selection returns one complete cause node", () => {
  const rateLimit = annotateYoutubeFailure(new Error("Too many requests"), {
    status: 429,
    body: "YouTube rate limit",
    source: "youtubejs_player",
    targetUrl: "https://www.youtube.com/youtubei/v1/player",
  });
  const wrapper = annotateYoutubeFailure(
    new Error("snapshot failed", { cause: rateLimit }),
    { source: "unrelated_wrapper" },
  );

  const selected = selectYoutubeFailure({ error: wrapper });
  const evidence = youtubeFailureEvidence(wrapper);
  assert.equal(selected.node, rateLimit);
  assert.equal(selected.decision.kind, "youtube_rate_limited");
  assert.equal(selected.evidence.error, rateLimit);
  assert.equal(evidence.error, rateLimit);
  assert.equal(evidence.status, 429);
  assert.equal(evidence.body, "YouTube rate limit");
  assert.equal(evidence.source, "youtubejs_player");
  assert.equal(evidence.target_url, "https://www.youtube.com/youtubei/v1/player");
});
