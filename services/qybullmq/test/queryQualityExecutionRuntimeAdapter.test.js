import assert from "node:assert/strict";
import test from "node:test";
import { QueryQualityExecutionRuntimeAdapter } from "../src/queryQualityExecutionRuntimeAdapter.js";

test("Query Quality Runtime rejects a Discover Role Assignment", async () => {
  const adapter = new QueryQualityExecutionRuntimeAdapter({
    createDispatcher: () => ({ close: async () => {} }),
  });
  await assert.rejects(adapter.acquire({
    assignment: {
      role: "discover",
      slot_name: "discover-01",
      lease_id: "lease-1",
      route_generation: 1,
      network_identity_key: "net-1",
      profile_epoch: 1,
      identity_policy_id: "qy-br-discover-anonymous-v1",
      identity_policy_version: 1,
      identity_policy_hash: "sha256:discover",
    },
    policy: {
      role: "discover",
      id: "qy-br-discover-anonymous-v1",
      version: 1,
      hash: "sha256:discover",
    },
    proxyUrl: "http://rota:8000/",
  }), /query_quality Runtime Assignment conflicts/);
});
