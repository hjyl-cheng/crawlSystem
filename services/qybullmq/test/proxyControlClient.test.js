import assert from "node:assert/strict";
import test from "node:test";
import {
  ProxyControlClient,
  ProxyControlRequestError,
} from "../src/proxyControlClient.js";

function response(payload, status = 200, headers = new Map()) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name) => headers.get(name.toLowerCase()) ?? null },
    async text() { return JSON.stringify(payload); },
  };
}

function fixture(handler = async () => response({ ok: true }), overrides = {}) {
  const calls = [];
  const client = new ProxyControlClient({
    controlUrl: "http://rota/api/v1/proxy-control",
    token: "control-token-secret",
    fetchImpl: async (url, init) => {
      calls.push({ url, init, body: init.body ? JSON.parse(init.body) : null });
      return handler(url, init, calls.length);
    },
    sleepImpl: async () => {},
    ...overrides,
  });
  return { client, calls };
}

test("worker v2 commands use the exact authenticated Rota endpoints", async () => {
  const { client, calls } = fixture(async (url) => response(
    url.endsWith("/capacity") ? { ok: true, active: 8 } : { ok: true },
  ));
  const claim = {
    claim_request_id: "claim-1",
    protocol_version: 2,
    role: "channel",
    worker_id: "worker-1",
    worker_instance_id: "instance-1",
    identity_policy_id: "qy-br-channel-anonymous-v1",
    identity_policy_version: 1,
  };
  const leaseFence = {
    slot_name: "bullmq-channel-01",
    worker_id: "worker-1",
    worker_instance_id: "instance-1",
    lease_id: "lease-1",
  };
  await client.claim(claim);
  await client.renew({ renew_request_id: "renew-1", ...leaseFence, known_route_generation: 3 });
  await client.beginTask({
    ...leaseFence,
    route_generation: 3,
    attempt_request_id: "attempt-1",
    business_run_id: "run-1",
    job_execution_id: "queue:job:1",
    task_kind: "channel_full",
  });
  await client.observe({
    ...leaseFence,
    route_generation: 3,
    task_id: "task-1",
    business_run_id: "run-1",
    observation_id: "observation-1",
    kind: "youtube_challenge",
    source: "youtubejs",
    occurred_at: "2026-08-13T00:00:00.000Z",
  });
  await client.completeTask({
    completion_request_id: "complete-1",
    ...leaseFence,
    route_generation: 3,
    task_id: "task-1",
    business_run_id: "run-1",
    outcome: "failed",
    duration_ms: 100,
    business_complete: false,
    observation_ids: ["observation-1"],
    attempt_quiesced: true,
    active_managed_requests: 0,
  });
  await client.release({
    release_request_id: "release-1",
    ...leaseFence,
    known_route_generation: 3,
    reason: "worker_shutdown",
  });
  const capacity = await client.capacity();

  assert.equal(capacity.active, 8);
  assert.deepEqual(calls.map((call) => call.url), [
    "http://rota/api/v1/proxy-control/claim",
    "http://rota/api/v1/proxy-control/renew",
    "http://rota/api/v1/proxy-control/tasks/begin",
    "http://rota/api/v1/proxy-control/tasks/observe",
    "http://rota/api/v1/proxy-control/tasks/complete",
    "http://rota/api/v1/proxy-control/release",
    "http://rota/api/v1/proxy-control/capacity",
  ]);
  assert.deepEqual(calls[0].body, claim);
  assert.equal(calls[6].init.method, "GET");
  assert.equal(calls[6].body, null);
  assert.ok(calls.every((call) => call.init.headers.authorization === "Bearer control-token-secret"));
  assert.equal("swap" in client, false);
});

test("control conflicts retain the Rota status and machine code", async () => {
  const { client } = fixture(async () => response({
    code: "LEASE_CONFLICT",
    error: "proxy control lease conflict",
  }, 409));
  await assert.rejects(
    client.renew({}),
    (error) => error instanceof ProxyControlRequestError
      && error.status === 409
      && error.code === "LEASE_CONFLICT"
      && error.retryable === false,
  );
});

test("temporary Route ineligibility preserves the machine code for Adapter deferral", async () => {
  const { client } = fixture(async () => response({
    code: "ROUTE_NOT_READY",
    error: "proxy control route is not ready",
  }, 409));
  await assert.rejects(
    client.beginTask({}),
    (error) => error instanceof ProxyControlRequestError
      && error.status === 409
      && error.code === "ROUTE_NOT_READY"
      && error.retryable === false,
  );
});

test("retryable control failures replay the identical idempotent request", async () => {
  const request = { renew_request_id: "renew-stable", lease_id: "lease-1" };
  const { client, calls } = fixture(async (_url, _init, count) => (
    count === 1
      ? response({ code: "TEMPORARY", error: "try again" }, 503)
      : response({ ok: true, lease_id: "lease-1" })
  ));
  const result = await client.renew(request);
  assert.equal(result.lease_id, "lease-1");
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].body, request);
  assert.deepEqual(calls[1].body, request);
});

test("transport failures are bounded and remain distinguishable", async () => {
  const { client, calls } = fixture(async () => {
    throw new Error("connection reset");
  }, { maxAttempts: 2 });
  await assert.rejects(
    client.claim({ claim_request_id: "claim-transport" }),
    (error) => error instanceof ProxyControlRequestError
      && error.status === null
      && error.retryable === true,
  );
  assert.equal(calls.length, 2);
});
