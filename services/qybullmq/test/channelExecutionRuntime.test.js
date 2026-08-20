import assert from "node:assert/strict";
import test from "node:test";
import { ChannelExecutionRuntime } from "../src/channelExecutionRuntime.js";
import { currentClientProfile, ProxyIdentityChangedError } from "../src/channelExecutionContext.js";

const proxy = {
  proxy_id: 7,
  proxy_address_hash: "hash-a",
  slot_name: "bullmq-channel-01",
  proxy_user: "bullmq-channel-01",
};

const profileGroup = {
  profile_group_id: "group-7-1",
  profile_revision: 1,
  clients: {
    youtubejs_chrome: { profile_id: "chrome-7-1", engine: "youtubejs_chrome" },
    ytdlp_safari: { profile_id: "safari-7-1", engine: "ytdlp_safari" },
  },
};

function job(id = "job-1") {
  return {
    id,
    queueName: "youtube-channel-crawl",
    attemptsMade: 0,
    data: { channel_id: "UCtest", run_id: "run-test" },
  };
}

function runtimeFixture(overrides = {}) {
  const calls = { attempts: [], checkpoints: [], finishes: [], snapshots: 0 };
  const profileStore = {
    async loadOrCreate() { return profileGroup; },
    async beginAttempt(input) { calls.attempts.push(input); return "attempt-1"; },
    async checkpointCookies(groupId, state) { calls.checkpoints.push({ groupId, state }); },
    async finishAttempt(attemptId, value) { calls.finishes.push({ attemptId, value }); },
    ...overrides.profileStore,
  };
  const gateway = {
    async prepare() {},
    async snapshot() {
      calls.snapshots += 1;
      return { cookies: [{ name: "chrome", value: "chrome-cookie-secret" }] };
    },
    async close() {},
    ...overrides.gateway,
  };
  const runtime = new ChannelExecutionRuntime({
    profileStore,
    gateway,
    acquireYtDlp: async () => ({ enabled: true, mode: "persistent" }),
    releaseYtDlp: async () => ({
      pid: 12,
      cookie_state: { primary: { cookies: [{ name: "safari", value: "safari-cookie-secret" }] } },
    }),
    acquireYoutube: async () => ({ enabled: true, mode: "full" }),
    releaseYoutube: async () => ({ duration_ms: 10 }),
    ...overrides.runtime,
  });
  return { runtime, calls };
}

function context(currentProxy) {
  return {
    job: job(),
    proxy,
    getProxySnapshot: () => currentProxy.value,
    proxyUrl: "http://slot:secret@rota:8000",
    workerId: "qy-channel-01",
    language: "en",
    country: "BR",
    timezone: "America/Sao_Paulo",
  };
}

test("channel runtime binds both client profiles and checkpoints cookies without logging them", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();
  const output = await runtime.run(context(currentProxy), async () => {
    assert.equal(currentClientProfile("youtubejs_chrome").profile_id, "chrome-7-1");
    return { ok: true };
  });

  assert.deepEqual(output.result, { ok: true });
  assert.equal(output.execution.profile_group_id, "group-7-1");
  assert.equal(calls.checkpoints.length, 1);
  assert.equal(calls.finishes[0].value.status, "success");
  assert.equal(calls.finishes[0].value.result.youtube_requests.request_count, 0);
  assert.deepEqual(calls.finishes[0].value.result.failure_decisions, []);
  assert.doesNotMatch(JSON.stringify(calls.finishes[0].value.result), /cookie-secret/);
  assert.equal(runtime.activeAttemptId, null);
});

test("channel runtime opens a Rota v2 session at initial profile epoch zero", async () => {
  const initialProxy = {
    slot_name: "bullmq-channel-01",
    proxy_user: "bullmq-channel-01-g1",
    lease_id: "lease-initial",
    route_generation: 1,
    network_identity_key: "network-initial",
    profile_epoch: 0,
    identity_policy_id: "qy-br-channel-anonymous-v1",
  };
  let loaded = null;
  const { runtime } = runtimeFixture({
    profileStore: {
      async loadOrCreate(input) {
        loaded = input;
        return profileGroup;
      },
    },
  });

  const output = await runtime.run({
    ...context({ value: initialProxy }),
    proxy: initialProxy,
    getProxySnapshot: () => initialProxy,
  }, async () => ({ ok: true }));

  assert.deepEqual(output.result, { ok: true });
  assert.equal(loaded.profileEpoch, 0);
});

test("channel runtime links an immutable Incremental Plan through prepared Business Run metadata", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();
  const incrementalJob = {
    ...job(),
    queueName: "youtube-channel-incremental",
    data: { channel_id: "UCtest", plan_id: "plan-1" },
  };

  await runtime.run({
    ...context(currentProxy),
    job: incrementalJob,
    prepared: { businessRunId: "incremental:plan-1" },
  }, async () => ({ ok: true }));

  assert.equal(calls.attempts[0].runId, "incremental:plan-1");
  assert.deepEqual(incrementalJob.data, { channel_id: "UCtest", plan_id: "plan-1" });
});

test("channel runtime aborts and skips cookie checkpoint when proxy identity drifts", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();

  await assert.rejects(
    runtime.run(context(currentProxy), async () => {
      currentProxy.value = { ...proxy, proxy_address_hash: "hash-b" };
      return { ok: true };
    }),
    ProxyIdentityChangedError,
  );

  assert.equal(calls.snapshots, 0);
  assert.equal(calls.checkpoints.length, 0);
  assert.equal(calls.finishes[0].value.status, "aborted");
  assert.equal(calls.finishes[0].value.identityChanged, true);
  assert.equal(calls.finishes[0].value.result.failure_decisions[0].kind, "unknown");
});

test("channel runtime rejects a second attempt while the first is still preparing", async () => {
  let releaseLoad;
  const loading = new Promise((resolve) => { releaseLoad = resolve; });
  const { runtime } = runtimeFixture({
    profileStore: { async loadOrCreate() { return loading; } },
  });
  const currentProxy = { value: { ...proxy } };
  const first = runtime.run(context(currentProxy), async () => ({ ok: true }));
  await Promise.resolve();

  await assert.rejects(runtime.run({ ...context(currentProxy), job: job("job-2") }, async () => ({})), /already active/);
  releaseLoad(profileGroup);
  await first;
});
