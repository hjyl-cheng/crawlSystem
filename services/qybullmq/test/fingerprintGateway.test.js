import assert from "node:assert/strict";
import test from "node:test";
import {
  decodeGatewayMetadata,
  encodeGatewayMetadata,
  FingerprintGateway,
  FingerprintGatewayError,
} from "../src/fingerprintGateway.js";

test("fingerprint gateway metadata survives an HTTP-header round trip", () => {
  const value = { url: "https://www.youtube.com/?q=ola", headers: { accept: "application/json" } };
  assert.deepEqual(decodeGatewayMetadata(encodeGatewayMetadata(value)), value);
});

test("fingerprint gateway configures isolated profiles and reconstructs the target response", async () => {
  const calls = [];
  const fetchFn = async (url, init = {}) => {
    calls.push({ url: String(url), init });
    if (String(url).endsWith("/v1/profiles/configure")) {
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    }
    return new Response("target-body", {
      status: 200,
      headers: {
        "x-fingerprint-response-status": "201",
        "x-fingerprint-response-headers": encodeGatewayMetadata({ "content-type": "text/plain", "x-target": "yes" }),
      },
    });
  };
  const gateway = new FingerprintGateway({ fetchFn });
  gateway.start = async () => {};
  const chrome = {
    profile_id: "chrome-profile",
    engine: "youtubejs_chrome",
    impersonate_target: "chrome136",
    user_agent: "Chrome UA",
    fingerprint_json: { max_connections: 2 },
    cookie_state: { cookies: [] },
  };
  const safari = {
    profile_id: "safari-profile",
    engine: "ytdlp_safari",
    impersonate_target: "safari",
    user_agent: "Safari UA",
    fingerprint_json: { max_connections: 1 },
    cookie_state: { cookies: [] },
  };

  await gateway.prepare({
    proxyUrl: "http://slot:secret@rota:8000",
    profileGroup: { profile_group_id: "group-1", profile_revision: 1, clients: { chrome, safari } },
  });
  const response = await gateway.fetch(chrome, "https://www.youtube.com/youtubei/v1/browse", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{}",
    timeoutMs: 4321,
  });

  const configured = JSON.parse(calls[0].init.body);
  assert.deepEqual(configured.profiles.map((profile) => profile.profile_id), ["chrome-profile", "safari-profile"]);
  assert.equal(calls[1].init.headers["x-fingerprint-timeout-ms"], "4321");
  assert.equal(decodeGatewayMetadata(calls[1].init.headers["x-fingerprint-headers"])["user-agent"], "Chrome UA");
  assert.equal(response.status, 201);
  assert.equal(response.headers.get("x-target"), "yes");
  assert.equal(await response.text(), "target-body");
});

test("fingerprint gateway distinguishes local transport failures from target HTTP responses", async () => {
  const gateway = new FingerprintGateway({
    fetchFn: async () => new Response("local failure", { status: 500 }),
  });
  gateway.start = async () => {};

  await assert.rejects(
    gateway.fetch({ profile_id: "chrome", user_agent: "UA" }, "https://www.youtube.com"),
    /gateway request failed HTTP 500/,
  );
});

test("an invalid target HTTP status becomes structured evidence instead of a Response RangeError", async () => {
  const rawTargetBody = Buffer.from([0xff, 0x00, 0x41]);
  const gateway = new FingerprintGateway({
    fetchFn: async () => ({
      status: 200,
      headers: new Headers({
        "x-fingerprint-response-status": "700",
        "x-fingerprint-response-headers": encodeGatewayMetadata({
          "content-type": "text/plain",
          "x-target-evidence": "invalid-status",
        }),
      }),
      async arrayBuffer() { return rawTargetBody; },
      body: null,
    }),
  });
  gateway.start = async () => {};

  await assert.rejects(
    gateway.fetch(
      { profile_id: "chrome", user_agent: "UA" },
      "https://www.youtube.com/watch?v=invalid-status",
    ),
    (error) => {
      assert.equal(error instanceof RangeError, false);
      assert.equal(error instanceof FingerprintGatewayError, true);
      assert.equal(error.code, "FINGERPRINT_INVALID_TARGET_STATUS");
      assert.equal(error.failureKind, "invalid_target_status");
      assert.equal(error.gatewayStatus, 200);
      assert.equal(error.targetStatusRaw, "700");
      assert.equal(error.detail, rawTargetBody.toString("utf8"));
      assert.equal(error.targetBodySampleBase64, rawTargetBody.toString("base64"));
      assert.deepEqual(error.targetHeaders, {
        "content-type": "text/plain",
        "x-target-evidence": "invalid-status",
      });
      assert.equal(error.youtube_failure_evidence.source, "fingerprint_gateway");
      assert.equal(
        error.youtube_failure_evidence.target_url,
        "https://www.youtube.com/watch?v=invalid-status",
      );
      return true;
    },
  );
});

test("invalid target status remains structured when target header metadata is malformed", async () => {
  const gateway = new FingerprintGateway({
    fetchFn: async () => ({
      status: 200,
      headers: new Headers({
        "x-fingerprint-response-status": "999",
        "x-fingerprint-response-headers": "not-base64url-json",
      }),
      async arrayBuffer() { return Buffer.from("raw-invalid-response"); },
      body: null,
    }),
  });
  gateway.start = async () => {};

  await assert.rejects(
    gateway.fetch({ profile_id: "chrome", user_agent: "UA" }, "https://www.youtube.com"),
    (error) => error instanceof FingerprintGatewayError
      && error.code === "FINGERPRINT_INVALID_TARGET_STATUS"
      && error.targetStatusRaw === "999"
      && error.targetHeadersRaw === "not-base64url-json",
  );
});
