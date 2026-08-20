import assert from "node:assert/strict";
import test from "node:test";
import { currentProxyIdentity, runManagedProxyRequest } from "../src/proxyIdentity.js";
import { DiscoverExecutionRuntimeAdapter } from "../src/discoverExecutionRuntimeAdapter.js";

const policy = Object.freeze({
  id: "qy-br-discover-anonymous-v1",
  version: 1,
  hash: "sha256:discover",
  role: "discover",
});

function assignment(overrides = {}) {
  return {
    role: "discover",
    slot_name: "discover-01",
    lease_id: "lease-1",
    route_generation: 1,
    network_identity_key: "net-1",
    profile_epoch: 1,
    identity_policy_id: policy.id,
    identity_policy_version: policy.version,
    identity_policy_hash: policy.hash,
    proxy_user: "discover-user-1",
    ...overrides,
  };
}

function fixture() {
  const dispatchers = [];
  const adapter = new DiscoverExecutionRuntimeAdapter({
    createDispatcher(proxyUrl) {
      const value = {
        proxyUrl,
        closed: false,
        async close() { this.closed = true; },
      };
      dispatchers.push(value);
      return value;
    },
  });
  return { adapter, dispatchers };
}

test("Discover Runtime scopes proxy state and quiesces managed requests", async () => {
  const { adapter, dispatchers } = fixture();
  const runtime = await adapter.acquire({
    assignment: assignment(),
    policy,
    proxyUrl: "http://user:secret@rota:8000/",
    task: { task_id: "task-1" },
    prepared: { businessRunId: "discover-page:1" },
    abortSignal: new AbortController().signal,
  });

  let releaseRequest;
  const pending = new Promise((resolve) => { releaseRequest = resolve; });
  const execution = runtime.execute({}, async () => {
    assert.equal(currentProxyIdentity().network_identity_key, "net-1");
    return runManagedProxyRequest(() => pending);
  });
  await Promise.resolve();
  const quiescing = adapter.quiesce(runtime);
  releaseRequest("done");

  assert.equal(await execution, "done");
  assert.deepEqual(await quiescing, { active_managed_requests: 0 });
  await adapter.retire(runtime);
  assert.equal(dispatchers[0].closed, true);
});

test("Discover Runtime retires an idle connection before accepting a new Route", async () => {
  const { adapter, dispatchers } = fixture();
  const first = await adapter.acquire({
    assignment: assignment(), policy, proxyUrl: "http://rota-a:8000/",
    abortSignal: new AbortController().signal,
  });
  await adapter.quiesce(first);
  const second = await adapter.acquire({
    assignment: assignment({ route_generation: 2, network_identity_key: "net-2" }),
    policy,
    proxyUrl: "http://rota-b:8000/",
    abortSignal: new AbortController().signal,
    reusableRuntime: first,
  });

  assert.notEqual(second, first);
  assert.equal(dispatchers[0].closed, true);
  assert.equal(dispatchers.length, 2);
});
