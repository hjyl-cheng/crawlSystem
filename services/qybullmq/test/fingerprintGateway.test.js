import assert from "node:assert/strict";
import test from "node:test";
import {
  decodeGatewayMetadata,
  encodeGatewayMetadata,
  FingerprintGateway,
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
