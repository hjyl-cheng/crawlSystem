import assert from "node:assert/strict";
import test from "node:test";
import { ChannelExecutionRuntimeAdapter } from "../src/channelExecutionRuntimeAdapter.js";

const policy = Object.freeze({
  id: "qy-br-channel-anonymous-v1",
  version: 1,
  hash: "sha256:channel",
  role: "channel",
  youtube_language: "pt-BR",
  youtube_country: "BR",
  browser_profile_timezone: "America/Sao_Paulo",
});

const assignment = Object.freeze({
  role: "channel",
  workload_scope: "qy-production",
  slot_name: "channel-01",
  lease_id: "lease-1",
  route_generation: 3,
  network_identity_key: "net-3",
  profile_epoch: 2,
  identity_policy_id: policy.id,
  identity_policy_version: policy.version,
  identity_policy_hash: policy.hash,
  proxy_user: "channel-user-3",
});

test("Channel Runtime passes only the frozen anonymous identity into the existing channel session", async () => {
  const calls = [];
  const legacyRuntime = {
    async run(options, callback) {
      calls.push(options);
      return { result: await callback(), execution: { attempt_id: "attempt-1" } };
    },
    async close() {},
  };
  const adapter = new ChannelExecutionRuntimeAdapter({
    runtime: legacyRuntime,
    workerId: "qy-channel-01",
  });
  const prepared = { businessRunId: "run-1" };
  const runtime = await adapter.acquire({
    assignment,
    policy,
    proxyUrl: "http://user:secret@rota:8000/",
    task: { task_id: "task-1", attempt_number: 1 },
    prepared,
    abortSignal: new AbortController().signal,
  });
  const result = await runtime.execute({
    job: { id: "job-1", queueName: "youtube-channel-crawl", data: { channel_id: "UC1" } },
    prepared,
  }, async () => ({ kind: "managed_work_complete", businessState: "terminal" }));

  assert.equal(result.kind, "managed_work_complete");
  assert.equal(calls[0].proxy.network_identity_key, "net-3");
  assert.equal(calls[0].proxy.proxy_id, undefined);
  assert.equal(calls[0].language, "pt-BR");
  assert.equal(calls[0].task.task_id, "task-1");
  assert.deepEqual(await adapter.quiesce(runtime), { active_managed_requests: 0 });
});

test("Channel Runtime accepts Rota's initial profile epoch zero", async () => {
  const legacyRuntime = {
    async run(_options, callback) {
      return { result: await callback(), execution: { attempt_id: "attempt-initial" } };
    },
    async close() {},
  };
  const adapter = new ChannelExecutionRuntimeAdapter({
    runtime: legacyRuntime,
    workerId: "qy-channel-01",
  });

  const runtime = await adapter.acquire({
    assignment: { ...assignment, profile_epoch: 0 },
    policy,
    proxyUrl: "http://user:secret@rota:8000/",
    task: { task_id: "task-initial", attempt_number: 1 },
    prepared: { businessRunId: "run-initial" },
    abortSignal: new AbortController().signal,
  });

  assert.equal(runtime.assignment.profile_epoch, 0);
});
