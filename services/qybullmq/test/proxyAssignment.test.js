import assert from "node:assert/strict";
import test from "node:test";
import {
  proxyAddressHash,
  proxyAssignmentKey,
  sameProxyAssignment,
} from "../src/proxyAssignment.js";

test("proxy address hash is stable for the same normalized Rota assignment", () => {
  const first = proxyAddressHash({ proxyId: 17, protocol: "HTTP", address: " Proxy.Example:9000 " });
  const second = proxyAddressHash({ proxyId: "17", protocol: "http", address: "proxy.example:9000" });

  assert.equal(first, second);
  assert.match(first, /^[a-f0-9]{64}$/);
  assert.equal(proxyAddressHash({ proxyId: 17, protocol: "http", address: "" }), null);
});

test("proxy assignment identity requires both proxy id and address hash", () => {
  const identity = { proxy_id: 17, proxy_address_hash: "address-a" };

  assert.equal(proxyAssignmentKey(identity), "17:address-a");
  assert.equal(sameProxyAssignment(identity, { proxyId: 17, proxyAddressHash: "address-a" }), true);
  assert.equal(sameProxyAssignment(identity, { proxy_id: 17, proxy_address_hash: "address-b" }), false);
  assert.equal(sameProxyAssignment(null, null), false);
});

test("Rota v2 assignment identity is fenced by Lease, Route, Policy and Profile epoch", () => {
  const assignment = {
    slot_name: "bullmq-channel-01",
    lease_id: "lease-1",
    route_generation: 7,
    identity_policy_id: "qy-br-channel-anonymous-v1",
    network_identity_key: "net-1",
    profile_epoch: 2,
  };

  assert.match(proxyAssignmentKey(assignment), /^v2:/);
  assert.equal(sameProxyAssignment(assignment, { ...assignment }), true);
  assert.equal(sameProxyAssignment(assignment, { ...assignment, route_generation: 8 }), false);
  assert.equal(sameProxyAssignment(assignment, { ...assignment, profile_epoch: 3 }), false);
});
