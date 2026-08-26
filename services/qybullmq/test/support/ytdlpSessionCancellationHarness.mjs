import { writeFile } from "node:fs/promises";

const outputPath = process.argv[2];
const scenario = process.argv[3];
globalThis.__ytdlpSessionCancellationState = {
  scenario,
  spawnedPids: [],
  killedPids: [],
  closedPids: [],
  commands: [],
  cancelRequest: () => {},
  cancelStart: () => {},
  cancelConfigure: () => {},
  cancelAcquire: () => {},
  cancelRelease: () => {},
  releaseClose: () => {},
};

const {
  acquirePersistentYtDlp,
  closePersistentYtDlp,
  persistentVideoDetail,
  releasePersistentYtDlp,
} = await import("../../src/ytdlpSession.js");
const {
  ChannelExecutionMetrics,
  runWithChannelExecution,
} = await import("../../src/channelExecutionContext.js");

const profile = {
  profile_id: "test-safari",
  fingerprint_json: { ytdlp_target: "safari-17" },
  cookie_state: { cookies: [] },
};
const acquire = (channelId, options = {}) => acquirePersistentYtDlp(channelId, "en", {
  profile,
  proxyUrl: "http://proxy.test:8080",
  ...options,
});
const state = globalThis.__ytdlpSessionCancellationState;
const nextTurn = () => new Promise((resolve) => setImmediate(resolve));
let output;

try {
  if (scenario === "pre_cancelled_acquire") {
    const controller = new AbortController();
    const reason = new Error("pre-cancelled acquire");
    controller.abort(reason);
    let caught = null;
    try {
      await acquire("UCpre", { signal: controller.signal });
    } catch (error) {
      caught = error;
    }
    output = { reason_preserved: caught === reason };
  } else if ([
    "cancel_during_start",
    "cancel_during_configure",
    "cancel_during_acquire_response",
  ].includes(scenario)) {
    const controller = new AbortController();
    const reason = new Error(`cancel persistent acquire during ${scenario}`);
    state.cancelStart = () => controller.abort(reason);
    state.cancelConfigure = () => controller.abort(reason);
    state.cancelAcquire = () => controller.abort(reason);
    let caught = null;
    try {
      await acquire("UCphase", { signal: controller.signal });
    } catch (error) {
      caught = error;
    }
    const originalPid = state.spawnedPids[0] ?? null;
    const { persistentYtDlpState } = await import("../../src/ytdlpSession.js");
    output = {
      reason_preserved: caught === reason,
      original_pid_exited: state.closedPids.includes(originalPid),
      active_channel: persistentYtDlpState().active_channel,
    };
  } else if (scenario === "pre_cancelled_active_request") {
    const first = await acquire("UCpreactive");
    const controller = new AbortController();
    const reason = new Error("pre-cancel active yt-dlp request");
    controller.abort(reason);
    let caught = null;
    try {
      await persistentVideoDetail("https://www.youtube.com/watch?v=pre-cancel", {
        signal: controller.signal,
      });
    } catch (error) {
      caught = error;
    }
    await releasePersistentYtDlp();
    output = {
      reason_preserved: caught === reason,
      original_pid_exited: state.closedPids.includes(first.process_pid),
      spawn_count_after_release: state.spawnedPids.length,
    };
  } else if (["cancel_active_request", "cancel_active_request_delayed_close"].includes(scenario)) {
    const first = await acquire("UCfirst");
    const controller = new AbortController();
    const reason = new Error("cancel active yt-dlp request");
    state.cancelRequest = () => controller.abort(reason);
    const requestOutcome = persistentVideoDetail("https://www.youtube.com/watch?v=cancel", {
      timeoutMs: 100,
      signal: controller.signal,
    }).then(
      (value) => ({ value, error: null }),
      (error) => ({ value: null, error }),
    );
    let settled = false;
    void requestOutcome.then(() => { settled = true; });
    await nextTurn();
    await nextTurn();
    const requestSettledBeforeClose = settled;
    const originalClosedBeforeRelease = state.closedPids.includes(first.process_pid);
    const nextAcquire = scenario === "cancel_active_request_delayed_close"
      ? acquire("UCsecond")
      : null;
    await nextTurn();
    const spawnCountBeforeClose = state.spawnedPids.length;
    state.releaseClose();
    const { error: caught } = await requestOutcome;
    const spawnCountAfterAbort = state.spawnedPids.length;
    await releasePersistentYtDlp();
    const spawnCountAfterRelease = state.spawnedPids.length;
    const second = nextAcquire ? await nextAcquire : await acquire("UCsecond");
    output = {
      reason_preserved: caught === reason,
      original_pid: first.process_pid,
      original_pid_exited: state.closedPids.includes(first.process_pid),
      original_pid_closed_before_release: originalClosedBeforeRelease,
      request_settled_before_close: requestSettledBeforeClose,
      spawn_count_before_close: spawnCountBeforeClose,
      spawn_count_after_abort: spawnCountAfterAbort,
      spawn_count_after_release: spawnCountAfterRelease,
      next_acquire_pid: second.process_pid,
      spawn_count_after_next_acquire: state.spawnedPids.length,
    };
    await releasePersistentYtDlp();
  } else if (scenario === "cancelled_release") {
    const acquired = await acquire("UCcancelledrelease");
    await releasePersistentYtDlp({
      cancelled: true,
      reason: new Error("channel cancelled outside yt-dlp"),
    });
    output = {
      original_pid_exited: state.closedPids.includes(acquired.process_pid),
    };
  } else if (scenario === "cancel_during_release") {
    const acquired = await acquire("UCreleaseabort");
    const controller = new AbortController();
    const reason = new Error("cancel normal persistent release");
    state.cancelRelease = () => controller.abort(reason);
    const startedAt = Date.now();
    let caught = null;
    let released = null;
    try {
      released = await releasePersistentYtDlp({ signal: controller.signal });
    } catch (error) {
      caught = error;
    }
    output = {
      reason_preserved: caught === reason,
      released,
      elapsed_ms: Date.now() - startedAt,
      original_pid_exited: state.closedPids.includes(acquired.process_pid),
      release_commands: state.commands.filter((command) => command === "release").length,
    };
  } else if (scenario === "timeout_then_recover") {
    const first = await acquire("UCtimeoutrecovery");
    const metrics = new ChannelExecutionMetrics();
    const proxy = { proxy_id: 1, proxy_address_hash: "same" };
    const result = await runWithChannelExecution({
      proxy,
      get_proxy_snapshot: () => proxy,
      metrics,
    }, () => persistentVideoDetail("https://www.youtube.com/watch?v=timeout-recovery", {
      timeoutMs: 10,
    }));
    const snapshot = metrics.snapshot();
    output = {
      result_pid: result?.pid ?? null,
      first_pid: first.process_pid,
      spawn_count_after_recovery: state.spawnedPids.length,
      request_count: snapshot.request_count,
      failure_count: snapshot.failure_count,
      failure_evidence: snapshot.failure_evidence,
    };
  } else if (scenario === "listener_cleanup") {
    const controller = new AbortController();
    const originalAdd = controller.signal.addEventListener.bind(controller.signal);
    const originalRemove = controller.signal.removeEventListener.bind(controller.signal);
    let listenerAdds = 0;
    let listenerRemoves = 0;
    controller.signal.addEventListener = (...args) => {
      listenerAdds += 1;
      return originalAdd(...args);
    };
    controller.signal.removeEventListener = (...args) => {
      listenerRemoves += 1;
      return originalRemove(...args);
    };
    await acquire("UClisten", { signal: controller.signal });
    await persistentVideoDetail("https://www.youtube.com/watch?v=listen", {
      signal: controller.signal,
    });
    await releasePersistentYtDlp();
    output = { listener_adds: listenerAdds, listener_removes: listenerRemoves };
  } else if (scenario === "close_killed_child") {
    const acquired = await acquire("UCkilled");
    state.lastChild.kill("SIGKILL");
    const spawnCountBeforeClose = state.spawnedPids.length;
    await closePersistentYtDlp();
    output = {
      spawn_count_before_close: spawnCountBeforeClose,
      original_pid_exited: state.closedPids.includes(acquired.process_pid),
    };
  } else {
    const first = await acquire("UCreuse1");
    const released = await releasePersistentYtDlp();
    const second = await acquire("UCreuse2");
    const releasedSecond = await releasePersistentYtDlp();
    output = {
      cookie_state: released.cookie_state,
      second_cookie_state: releasedSecond.cookie_state,
      first_pid: first.process_pid,
      second_pid: second.process_pid,
      spawn_count_before_close: state.spawnedPids.length,
    };
  }
} finally {
  await closePersistentYtDlp();
}

await writeFile(outputPath, JSON.stringify({
  ...output,
  spawn_count: state.spawnedPids.length,
  commands: state.commands,
  killed_pids: state.killedPids,
  closed_pids: state.closedPids,
}), "utf8");
