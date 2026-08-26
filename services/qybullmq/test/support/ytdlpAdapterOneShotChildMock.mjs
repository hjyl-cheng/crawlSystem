import { EventEmitter } from "node:events";

export function spawn() {
  const state = globalThis.__ytdlpAdapterCancellationState;
  state.spawnCount += 1;
  const child = new EventEmitter();
  child.killed = false;
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.stdin = {
    end() {
      if (state.scenario.endsWith("_active_one_shot")) {
        state.cancel();
        return;
      }
      setImmediate(() => {
        child.stdout.emit("data", JSON.stringify({ ok: false, error: "one-shot should not run" }));
        child.emit("close", 0, null);
      });
    },
  };
  child.kill = () => {
    child.killed = true;
    state.oneShotKilled = true;
    if (state.scenario.endsWith("_active_one_shot")) child.emit("close", null, "SIGKILL");
    return true;
  };
  return child;
}
