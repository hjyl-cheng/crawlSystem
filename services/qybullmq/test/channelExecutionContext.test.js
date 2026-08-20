import assert from "node:assert/strict";
import test from "node:test";
import {
  ChannelExecutionMetrics,
  channelExecutionMetrics,
  recordChannelExecutionRequest,
  runWithChannelExecution,
  assertChannelExecutionIdentity,
  currentChannelExecution,
  currentClientProfile,
  ProxyIdentityChangedError,
} from "../src/channelExecutionContext.js";

const expectedProxy = {
  proxy_id: 7,
  proxy_address_hash: "hash-a",
  slot_name: "bullmq-channel-01",
};

test("channel execution context survives asynchronous work and exposes only its client profile", async () => {
  let currentProxy = { ...expectedProxy };
  const chrome = { profile_id: "chrome-1" };

  await runWithChannelExecution({
    proxy: expectedProxy,
    get_proxy_snapshot: () => currentProxy,
    profile_group: { clients: { youtubejs_chrome: chrome } },
  }, async () => {
    await Promise.resolve();
    assert.equal(currentChannelExecution().proxy.proxy_id, 7);
    assert.equal(currentClientProfile("youtubejs_chrome"), chrome);
    assert.equal(assertChannelExecutionIdentity().proxy_id, 7);
  });

  assert.equal(currentChannelExecution(), null);
});

test("channel execution context rejects proxy address drift even when proxy id is unchanged", async () => {
  let currentProxy = { ...expectedProxy };

  await runWithChannelExecution({
    proxy: expectedProxy,
    get_proxy_snapshot: () => currentProxy,
    profile_group: { clients: {} },
  }, async () => {
    currentProxy = { ...expectedProxy, proxy_address_hash: "hash-b" };
    assert.throws(() => assertChannelExecutionIdentity(), ProxyIdentityChangedError);
  });
});

test("channel execution metrics aggregate requests in memory and retain bounded failure evidence", async () => {
  const metrics = new ChannelExecutionMetrics();
  await runWithChannelExecution({ metrics }, async () => {
    recordChannelExecutionRequest({
      engine: "youtubejs",
      client: "WEB",
      status: 200,
      durationMs: 12.4,
    });
    recordChannelExecutionRequest({
      engine: "youtubejs",
      client: "WEB",
      status: 429,
      durationMs: 20,
      ok: false,
      body: "Too many requests",
      source: "youtubejs_player",
    });
    const snapshot = channelExecutionMetrics();
    assert.equal(snapshot.request_count, 2);
    assert.equal(snapshot.failure_count, 1);
    assert.equal(snapshot.by_engine.youtubejs.clients.WEB, 2);
    assert.equal(snapshot.by_engine.youtubejs.statuses[429], 1);
    assert.equal(snapshot.failure_evidence[0].source, "youtubejs_player");
  });
});
