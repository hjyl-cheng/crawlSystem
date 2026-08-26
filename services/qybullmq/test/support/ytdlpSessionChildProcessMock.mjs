import { EventEmitter } from "node:events";

let nextPid = 4100;

class FakeChild extends EventEmitter {
  constructor(state) {
    super();
    this.state = state;
    this.pid = nextPid += 1;
    this.killed = false;
    this.closed = false;
    this.stdout = new EventEmitter();
    this.stderr = new EventEmitter();
    this.stdin = {
      writable: true,
      write: (line, callback) => this.write(line, callback),
    };
    state.lastChild = this;
    state.spawnedPids.push(this.pid);
    setImmediate(() => {
      if (this.killed) return;
      if (state.scenario === "cancel_during_start") {
        state.cancelStart();
        return;
      }
      this.stdout.emit("data", `${JSON.stringify({ event: "ready", pid: this.pid })}\n`);
    });
  }

  write(line, callback) {
    if (this.killed) {
      callback?.(new Error("fake child stdin is closed"));
      return false;
    }
    const request = JSON.parse(String(line));
    this.state.commands.push(request.command);
    callback?.(null);
    if (
      request.command === "video_detail"
      && ["cancel_active_request", "cancel_active_request_delayed_close"].includes(this.state.scenario)
    ) {
      this.state.cancelRequest();
      return true;
    }
    if (request.command === "video_detail" && this.state.scenario === "timeout_then_recover") {
      if (this.pid === this.state.spawnedPids[0]) return true;
    }
    if (request.command === "release" && this.state.scenario === "cancel_during_release") {
      this.state.cancelRelease();
      setTimeout(() => {
        if (this.killed) return;
        this.stdout.emit("data", `${JSON.stringify({
          request_id: request.request_id,
          ok: true,
          result: { cookie_state: { cookies: [] }, pid: this.pid },
        })}\n`);
      }, 50);
      return true;
    }
    if (request.command === "configure" && this.state.scenario === "cancel_during_configure") {
      this.state.cancelConfigure();
      return true;
    }
    setImmediate(() => {
      if (this.killed) return;
      const result = request.command === "release"
        ? { cookie_state: { cookies: [{ name: "SID", value: "persisted" }] }, pid: this.pid }
        : { pid: this.pid, uptime_ms: 12, channels_processed: 1 };
      this.stdout.emit("data", `${JSON.stringify({
        request_id: request.request_id,
        ok: true,
        result,
      })}\n`);
      if (request.command === "acquire" && this.state.scenario === "cancel_during_acquire_response") {
        this.state.cancelAcquire();
      }
      if (request.command === "shutdown") setImmediate(() => this.kill("SIGTERM"));
    });
    return true;
  }

  kill(signal = "SIGTERM") {
    if (this.killed) return false;
    this.killed = true;
    this.stdin.writable = false;
    this.state.killedPids.push(this.pid);
    const close = () => {
      if (this.closed) return;
      this.closed = true;
      this.state.closedPids.push(this.pid);
      this.emit("close", null, signal);
    };
    if (
      this.state.scenario === "cancel_active_request_delayed_close"
      && this.pid === this.state.spawnedPids[0]
    ) {
      this.state.releaseClose = close;
    } else {
      setImmediate(close);
    }
    return true;
  }
}

export function spawn() {
  return new FakeChild(globalThis.__ytdlpSessionCancellationState);
}
