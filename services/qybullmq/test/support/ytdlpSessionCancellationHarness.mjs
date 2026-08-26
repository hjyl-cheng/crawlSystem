import { writeFile } from "node:fs/promises";

const outputPath = process.argv[2];
const scenario = process.argv[3];
globalThis.__ytdlpSessionCancellationState = {
  scenario,
  spawnedPids: [],
  killedPids: [],
  commands: [],
  cancelRequest: () => {},
  cancelStart: () => {},
  cancelConfigure: () => {},
  cancelAcquire: () => {},
};

const {
  acquirePersistentYtDlp,
  closePersistentYtDlp,
  persistentVideoDetail,
  releasePersistentYtDlp,
} = await import("../../src/ytdlpSession.js");

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
  } else if (scenario.startsWith("cancel_during_")) {
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
      original_pid_exited: state.killedPids.includes(originalPid),
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
      original_pid_exited: state.killedPids.includes(first.process_pid),
      spawn_count_after_release: state.spawnedPids.length,
    };
  } else if (scenario === "cancel_active_request") {
    const first = await acquire("UCfirst");
    const controller = new AbortController();
    const reason = new Error("cancel active yt-dlp request");
    state.cancelRequest = () => controller.abort(reason);
    let caught = null;
    try {
      await persistentVideoDetail("https://www.youtube.com/watch?v=cancel", {
        timeoutMs: 100,
        signal: controller.signal,
      });
    } catch (error) {
      caught = error;
    }
    const spawnCountAfterAbort = state.spawnedPids.length;
    await releasePersistentYtDlp();
    const spawnCountAfterRelease = state.spawnedPids.length;
    const second = await acquire("UCsecond");
    output = {
      reason_preserved: caught === reason,
      original_pid: first.process_pid,
      original_pid_exited: state.killedPids.includes(first.process_pid),
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
      original_pid_exited: state.killedPids.includes(acquired.process_pid),
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
      original_pid_exited: state.killedPids.includes(acquired.process_pid),
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
}), "utf8");
