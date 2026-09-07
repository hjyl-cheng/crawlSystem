import assert from "node:assert/strict";
import test from "node:test";
import { ChannelExecutionRuntime } from "../src/channelExecutionRuntime.js";
import {
  currentClientProfile,
  ProxyIdentityChangedError,
  recordChannelExecutionRequest,
} from "../src/channelExecutionContext.js";
import { YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT } from "../src/fullCrawlFetchContract.js";

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
    attemptsStarted: 1,
    data: { channel_id: "UCtest", run_id: "run-test", dispatch_generation: 3 },
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

test("YouTubeJS Full Crawl acquires only YouTubeJS and checkpoints no yt-dlp state", async () => {
  const currentProxy = { value: { ...proxy } };
  const leases = [];
  const releases = [];
  const { runtime, calls } = runtimeFixture({
    runtime: {
      acquireYtDlp: async () => {
        leases.push("ytdlp");
        return { enabled: true };
      },
      releaseYtDlp: async () => {
        releases.push("ytdlp");
        return { cookie_state: { unexpected: true } };
      },
      acquireYoutube: async () => {
        leases.push("youtubejs");
        return { enabled: true, mode: "full" };
      },
      releaseYoutube: async () => {
        releases.push("youtubejs");
        return { duration_ms: 10 };
      },
    },
  });
  const output = await runtime.run({
    ...context(currentProxy),
    prepared: {
      workloadKind: "channel_full",
      fetchContract: YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
    },
  }, async () => ({ ok: true }));

  assert.deepEqual(leases, ["youtubejs"]);
  assert.deepEqual(releases, ["youtubejs"]);
  assert.equal(calls.checkpoints.length, 1);
  assert.equal(calls.checkpoints[0].state.ytdlp_safari, undefined);
  assert.equal(output.sessions.ytdlp, null);
  assert.equal(output.sessions.youtubejs.mode, "full");
});

test("channel runtime writes the persisted dispatch generation into execution audit", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();

  await runtime.run(context(currentProxy), async () => ({ ok: true }));

  assert.equal(calls.attempts[0].dispatchGeneration, 3);
});

test("channel runtime gives a stalled BullMQ restart a distinct zero-based audit attempt", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();
  const stalledRestart = context(currentProxy);
  stalledRestart.job = {
    ...stalledRestart.job,
    attemptsMade: 0,
    attemptsStarted: 2,
  };

  await runtime.run(stalledRestart, async () => ({ ok: true }));

  assert.equal(calls.attempts[0].jobAttempt, 1);
});

test("channel runtime rejects a missing dispatch generation before audit starts", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();
  const invalid = context(currentProxy);
  delete invalid.job.data.dispatch_generation;

  await assert.rejects(
    runtime.run(invalid, async () => ({ ok: true })),
    /dispatch_generation must be a positive integer/,
  );
  assert.equal(calls.attempts.length, 0);
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
    data: { channel_id: "UCtest", plan_id: "plan-1", dispatch_generation: 4 },
  };

  await runtime.run({
    ...context(currentProxy),
    job: incrementalJob,
    prepared: { businessRunId: "incremental:plan-1" },
  }, async () => ({ ok: true }));

  assert.equal(calls.attempts[0].runId, "incremental:plan-1");
  assert.deepEqual(incrementalJob.data, {
    channel_id: "UCtest", plan_id: "plan-1", dispatch_generation: 4,
  });
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
  assert.deepEqual(calls.finishes[0].value.result.failure_decisions, []);
});

test("channel runtime terminates yt-dlp when proxy identity drifts during YouTube cleanup", async () => {
  const currentProxy = { value: { ...proxy } };
  let ytdlpReleaseOptions = null;
  const { runtime, calls } = runtimeFixture({
    runtime: {
      releaseYoutube: async () => {
        currentProxy.value = { ...proxy, proxy_address_hash: "hash-cleanup" };
        return { duration_ms: 10 };
      },
      releaseYtDlp: async (options) => {
        ytdlpReleaseOptions = options;
        return null;
      },
    },
  });

  await assert.rejects(
    runtime.run(context(currentProxy), async () => ({ ok: true })),
    ProxyIdentityChangedError,
  );

  assert.equal(ytdlpReleaseOptions.cancelled, true);
  assert.equal(ytdlpReleaseOptions.reason instanceof ProxyIdentityChangedError, true);
  assert.equal(calls.checkpoints.length, 0);
  assert.equal(calls.finishes[0].value.status, "aborted");
  assert.equal(calls.finishes[0].value.identityChanged, true);
});

test("channel runtime invalidates a released yt-dlp daemon when identity drifts during release", async () => {
  const currentProxy = { value: { ...proxy } };
  const ytdlpReleaseOptions = [];
  const { runtime, calls } = runtimeFixture({
    runtime: {
      releaseYtDlp: async (options) => {
        ytdlpReleaseOptions.push(options);
        if (ytdlpReleaseOptions.length === 1) {
          currentProxy.value = { ...proxy, proxy_address_hash: "hash-ytdlp-cleanup" };
          return {
            pid: 12,
            cookie_state: { primary: { cookies: [{ name: "SID", value: "stale" }] } },
          };
        }
        return null;
      },
    },
  });

  await assert.rejects(
    runtime.run(context(currentProxy), async () => ({ ok: true })),
    ProxyIdentityChangedError,
  );

  assert.equal(ytdlpReleaseOptions.length, 2);
  assert.equal(ytdlpReleaseOptions[0].cancelled, false);
  assert.equal(ytdlpReleaseOptions[1].cancelled, true);
  assert.equal(ytdlpReleaseOptions[1].reason instanceof ProxyIdentityChangedError, true);
  assert.equal(calls.checkpoints.length, 0);
  assert.equal(calls.finishes[0].value.status, "aborted");
  assert.equal(calls.finishes[0].value.identityChanged, true);
});

test("channel runtime propagates Rota cancellation raised during YouTube cleanup", async () => {
  const currentProxy = { value: { ...proxy } };
  const controller = new AbortController();
  const reason = new Error("Rota cancelled during YouTube cleanup");
  let ytdlpReleaseOptions = null;
  const { runtime, calls } = runtimeFixture({
    runtime: {
      releaseYoutube: async () => {
        controller.abort(reason);
        return { duration_ms: 10 };
      },
      releaseYtDlp: async (options) => {
        ytdlpReleaseOptions = options;
        return null;
      },
    },
  });

  await assert.rejects(
    runtime.run({
      ...context(currentProxy),
      abortSignal: controller.signal,
    }, async () => ({ ok: true })),
    (error) => error === reason,
  );

  assert.equal(ytdlpReleaseOptions.cancelled, true);
  assert.equal(ytdlpReleaseOptions.reason, reason);
  assert.equal(calls.checkpoints.length, 0);
  assert.equal(calls.finishes[0].value.status, "aborted");
  assert.equal(calls.finishes[0].value.error, reason);
});

test("channel runtime makes an in-progress yt-dlp release cancellable", async () => {
  const currentProxy = { value: { ...proxy } };
  const controller = new AbortController();
  const reason = new Error("Rota cancelled during yt-dlp release");
  const releaseOptions = [];
  const { runtime, calls } = runtimeFixture({
    runtime: {
      releaseYtDlp: async (options) => {
        releaseOptions.push(options);
        if (releaseOptions.length === 1) controller.abort(reason);
        if (options.signal?.aborted) throw options.signal.reason;
        return null;
      },
    },
  });

  await assert.rejects(
    runtime.run({
      ...context(currentProxy),
      abortSignal: controller.signal,
    }, async () => ({ ok: true })),
    (error) => error === reason,
  );

  assert.equal(releaseOptions[0].cancelled, false);
  assert.equal(releaseOptions[0].signal, controller.signal);
  assert.equal(calls.checkpoints.length, 0);
  assert.equal(calls.finishes[0].value.status, "aborted");
});

test("channel runtime preserves cancellation reasons that cannot carry attempt metadata", async () => {
  const reasons = [
    "Rota cancelled as text",
    "",
    Object.freeze(new Error("immutable Rota cancellation")),
  ];
  for (const reason of reasons) {
    const currentProxy = { value: { ...proxy } };
    const controller = new AbortController();
    const { runtime, calls } = runtimeFixture();
    const notRejected = Symbol("not rejected");
    let caught = notRejected;

    try {
      await runtime.run({
        ...context(currentProxy),
        abortSignal: controller.signal,
      }, async () => {
        controller.abort(reason);
      });
    } catch (error) {
      caught = error;
    }

    assert.equal(caught, reason);
    assert.equal(calls.finishes[0].value.status, "aborted");
    assert.equal(calls.finishes[0].value.error, reason);
  }
});

test("channel runtime records Rota cancellation as aborted without failure evidence", async () => {
  const currentProxy = { value: { ...proxy } };
  const controller = new AbortController();
  const reason = new Error("Rota cancelled channel execution");
  let releaseOptions = null;
  const { runtime, calls } = runtimeFixture({
    runtime: {
      releaseYtDlp: async (options) => {
        releaseOptions = options;
        return null;
      },
    },
  });

  await assert.rejects(
    runtime.run({
      ...context(currentProxy),
      abortSignal: controller.signal,
    }, async () => {
      controller.abort(reason);
      return { ok: true };
    }),
    (error) => error === reason,
  );

  const finished = calls.finishes[0].value;
  assert.equal(finished.status, "aborted");
  assert.equal(finished.identityChanged, false);
  assert.equal(finished.error, reason);
  assert.equal(releaseOptions.cancelled, true);
  assert.equal(releaseOptions.reason, reason);
  assert.deepEqual(finished.result.youtube_requests.failure_evidence, []);
  assert.deepEqual(finished.result.failure_decisions, []);
});

test("channel runtime preserves real failure evidence recorded before Rota cancellation", async () => {
  const currentProxy = { value: { ...proxy } };
  const controller = new AbortController();
  const reason = new Error("Rota cancelled after upstream failure");
  const { runtime, calls } = runtimeFixture();

  await assert.rejects(
    runtime.run({
      ...context(currentProxy),
      abortSignal: controller.signal,
    }, async () => {
      recordChannelExecutionRequest({
        engine: "youtube_http",
        client: "WEB",
        status: 503,
        ok: false,
        error: new Error("upstream returned HTTP 503"),
        source: "pre_cancel_network",
      });
      controller.abort(reason);
    }),
    (error) => error === reason,
  );

  const finished = calls.finishes[0].value;
  assert.equal(finished.status, "aborted");
  assert.equal(finished.result.youtube_requests.failure_evidence.length, 1);
  assert.equal(finished.result.failure_decisions.length, 1);
  assert.equal(finished.result.failure_decisions[0].evidence.source, "pre_cancel_network");
  assert.equal(
    finished.result.failure_decisions.some((decision) => (
      decision.evidence.source === "channel_attempt"
    )),
    false,
  );
});

test("channel runtime preserves the source attached to a nested structured failure", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();
  const gatewayError = Object.assign(new Error("gateway request failed"), {
    failureKind: "proxy_transport",
    code: "FINGERPRINT_PROXY_TRANSPORT",
    youtube_failure_evidence: { source: "fingerprint_gateway" },
  });

  let failure = null;
  try {
    await runtime.run(context(currentProxy), async () => {
      throw new Error("channel snapshot failed", { cause: gatewayError });
    });
  } catch (error) {
    failure = error;
  }

  assert.ok(failure);
  const decision = failure.channel_execution_attempt.failure_decisions.find(
    (item) => item.kind === "proxy_transport",
  );
  assert.equal(decision.evidence.source, "fingerprint_gateway");
  assert.equal(
    calls.finishes[0].value.result.failure_decisions[0].evidence.source,
    "fingerprint_gateway",
  );
});

test("channel runtime persists Decision and Evidence from the same nested failure", async () => {
  const currentProxy = { value: { ...proxy } };
  const { runtime, calls } = runtimeFixture();
  const rateLimit = Object.assign(new Error("HTTP 429"), {
    youtube_failure_evidence: { status: 429, source: "youtubejs_player" },
  });
  const wrapper = Object.assign(new Error("channel snapshot failed", { cause: rateLimit }), {
    youtube_failure_evidence: { source: "unrelated_wrapper" },
  });

  await assert.rejects(
    runtime.run(context(currentProxy), async () => { throw wrapper; }),
    (error) => error === wrapper,
  );

  const persisted = calls.finishes[0].value.result.failure_decisions.find(
    (item) => item.kind === "youtube_rate_limited",
  );
  assert.equal(persisted.evidence.source, "youtubejs_player");
  assert.equal(persisted.evidence.status, 429);
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
