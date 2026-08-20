import assert from "node:assert/strict";
import test from "node:test";
import { currentProxyIdentity, runWithProxyIdentity } from "../src/proxyIdentity.js";

test("proxy report identity remains bound to the active job across async work", async () => {
  const previousId = process.env.YOUTUBE_PROXY_ID;
  const previousUrl = process.env.YOUTUBE_PROXY_URL;
  process.env.YOUTUBE_PROXY_ID = "99";
  process.env.YOUTUBE_PROXY_URL = "http://environment:secret@proxy:8000";
  try {
    await runWithProxyIdentity({ proxy_id: 7, slot_name: "bullmq-channel-03" }, async () => {
      await Promise.resolve();
      assert.deepEqual(currentProxyIdentity(), {
        proxy_id: 7,
        proxy_user: "bullmq-channel-03",
        proxy_url: "http://environment:secret@proxy:8000",
        dispatcher: null,
        abort_signal: null,
        managed_request_tracker: null,
        slot_name: "bullmq-channel-03",
        lease_id: null,
        route_generation: null,
        network_identity_key: null,
        profile_epoch: null,
        identity_policy_id: null,
      });
    });
    assert.deepEqual(currentProxyIdentity(), {
      proxy_id: 99,
      proxy_user: "environment",
      proxy_url: "http://environment:secret@proxy:8000",
      dispatcher: null,
      abort_signal: null,
      managed_request_tracker: null,
      slot_name: null,
      lease_id: null,
      route_generation: null,
      network_identity_key: null,
      profile_epoch: null,
      identity_policy_id: null,
    });
  } finally {
    if (previousId === undefined) delete process.env.YOUTUBE_PROXY_ID;
    else process.env.YOUTUBE_PROXY_ID = previousId;
    if (previousUrl === undefined) delete process.env.YOUTUBE_PROXY_URL;
    else process.env.YOUTUBE_PROXY_URL = previousUrl;
  }
});

test("workers without a proxy slot use an empty execution identity", async () => {
  await runWithProxyIdentity(null, async () => {
    assert.deepEqual(currentProxyIdentity({ proxyUrl: "", proxyUser: "", proxyId: null }), {
      proxy_id: null,
      proxy_user: null,
      proxy_url: null,
      dispatcher: null,
      abort_signal: null,
      managed_request_tracker: null,
      slot_name: null,
      lease_id: null,
      route_generation: null,
      network_identity_key: null,
      profile_epoch: null,
      identity_policy_id: null,
    });
  });
});

test("managed execution identity preserves the complete Route tuple", async () => {
  await runWithProxyIdentity({
    proxy_id: 17,
    proxy_user: "bullmq-channel-09",
    proxy_url: "http://bullmq-channel-09:secret@rota:8000",
    slot_name: "bullmq-channel-09",
    lease_id: "lease-17",
    route_generation: 4,
    network_identity_key: "network-17",
    profile_epoch: 2,
    identity_policy_id: "qy-br-channel-anonymous-v1",
  }, async () => assert.deepEqual(currentProxyIdentity(), {
    proxy_id: 17,
    proxy_user: "bullmq-channel-09",
    proxy_url: "http://bullmq-channel-09:secret@rota:8000",
    dispatcher: null,
    abort_signal: null,
    managed_request_tracker: null,
    slot_name: "bullmq-channel-09",
    lease_id: "lease-17",
    route_generation: 4,
    network_identity_key: "network-17",
    profile_epoch: 2,
    identity_policy_id: "qy-br-channel-anonymous-v1",
  }));
});

test("managed execution identity preserves Rota's initial profile epoch zero", async () => {
  await runWithProxyIdentity({
    proxy_user: "bullmq-channel-01",
    slot_name: "bullmq-channel-01",
    lease_id: "lease-initial",
    route_generation: 1,
    network_identity_key: "network-initial",
    profile_epoch: 0,
    identity_policy_id: "qy-br-channel-anonymous-v1",
  }, async () => {
    assert.equal(currentProxyIdentity().profile_epoch, 0);
  });
});
