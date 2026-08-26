import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { nanoid } from "nanoid";
import {
  assertChannelExecutionIdentity,
  currentChannelExecution,
  currentChannelExecutionAbortSignal,
  ProxyIdentityChangedError,
  recordChannelExecutionRequest,
} from "./channelExecutionContext.js";
import { combineAbortSignals, throwIfAborted } from "./abortSignal.js";
import { fingerprintTransportRequired } from "./fingerprintFetch.js";
import { annotateYoutubeFailure } from "./youtubeFailurePolicy.js";

const SCRIPT_PATH = fileURLToPath(new URL("../scripts/ytdlp_session.py", import.meta.url));
const PYTHON_BIN = String(process.env.YTDLP_PYTHON_BIN || "python3");
const START_TIMEOUT_MS = Math.max(1000, Number(process.env.YTDLP_SESSION_START_TIMEOUT_MS || 15000));
const STOP_TIMEOUT_MS = Math.max(500, Number(process.env.YTDLP_SESSION_STOP_TIMEOUT_MS || 3000));

class YtDlpSessionRemoteError extends Error {
  constructor(message) {
    super(message);
    this.name = "YtDlpSessionRemoteError";
    this.remote = true;
  }
}

class YtDlpSessionCommandTimeoutError extends Error {
  constructor(command, timeoutMs) {
    super(`yt-dlp session command ${command} timed out after ${timeoutMs}ms`);
    this.name = "YtDlpSessionCommandTimeoutError";
    this.code = "YTDLP_SESSION_COMMAND_TIMEOUT";
    this.command = command;
    this.timeoutMs = timeoutMs;
  }
}

function waitForSignal(promise, signal, onAbort) {
  if (!signal) return promise;
  throwIfAborted(signal);
  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal.removeEventListener("abort", handleAbort);
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      callback(value);
    };
    const handleAbort = () => {
      if (settled) return;
      try {
        onAbort?.(signal.reason);
      } finally {
        finish(reject, signal.reason);
      }
    };
    signal.addEventListener("abort", handleAbort, { once: true });
    Promise.resolve(promise).then(
      (value) => finish(resolve, value),
      (error) => finish(reject, error),
    );
    if (signal.aborted) handleAbort();
  });
}

class YtDlpDaemon {
  constructor() {
    this.child = null;
    this.pending = new Map();
    this.stdoutBuffer = "";
    this.stderrBuffer = "";
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
    this.proxyUrl = null;
    this.startedAt = null;
    this.readyInfo = null;
    this.stopping = false;
    this.profileKey = null;
    this.profileId = null;
    this.exitPromise = null;
    this.exitResolve = null;
    this.terminationRequested = false;
    this.terminationReason = null;
  }

  async start({ signal = null } = {}) {
    throwIfAborted(signal);
    if (this.terminationRequested) throw this.terminationReason;
    if (this.child && !this.stopping && !this.child.killed && this.readyPromise) {
      await waitForSignal(
        this.readyPromise,
        signal,
        (reason) => this.terminate(reason),
      );
      return this.readyInfo;
    }
    if (this.child && this.exitPromise) {
      await waitForSignal(this.exitPromise, signal);
      throwIfAborted(signal);
      if (this.terminationRequested) throw this.terminationReason;
    }
    this.proxyUrl = null;
    this.startedAt = Date.now();
    this.stopping = false;
    this.stdoutBuffer = "";
    this.stderrBuffer = "";
    this.readyInfo = null;
    this.readyPromise = new Promise((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });
    const child = spawn(PYTHON_BIN, ["-u", SCRIPT_PATH], {
      stdio: ["pipe", "pipe", "pipe"],
      env: { ...process.env },
    });
    this.child = child;
    this.exitPromise = new Promise((resolve) => {
      this.exitResolve = resolve;
    });
    child.stdout.on("data", (chunk) => this.#onStdout(chunk));
    child.stderr.on("data", (chunk) => {
      this.stderrBuffer = `${this.stderrBuffer}${chunk.toString()}`.slice(-16000);
    });
    child.on("error", (error) => this.#onChildError(error, child));
    child.on("exit", (code, signal) => {
      const suffix = this.stderrBuffer.trim() ? `: ${this.stderrBuffer.trim().slice(-1000)}` : "";
      this.#onExit(new Error(`yt-dlp session exited code=${code} signal=${signal}${suffix}`), child);
    });
    child.on("close", (code, signal) => {
      const suffix = this.stderrBuffer.trim() ? `: ${this.stderrBuffer.trim().slice(-1000)}` : "";
      this.#onExit(new Error(`yt-dlp session exited code=${code} signal=${signal}${suffix}`), child);
    });
    const timer = setTimeout(() => {
      const error = new Error(`yt-dlp session start timeout after ${START_TIMEOUT_MS}ms`);
      void this.terminate(error);
    }, START_TIMEOUT_MS);
    try {
      const readyPromise = this.readyPromise;
      await waitForSignal(
        readyPromise,
        signal,
        (reason) => this.terminate(reason),
      );
      return this.readyInfo;
    } finally {
      clearTimeout(timer);
    }
  }

  #onStdout(chunk) {
    this.stdoutBuffer += chunk.toString();
    while (true) {
      const newline = this.stdoutBuffer.indexOf("\n");
      if (newline < 0) break;
      const line = this.stdoutBuffer.slice(0, newline).trim();
      this.stdoutBuffer = this.stdoutBuffer.slice(newline + 1);
      if (!line) continue;
      let message;
      try {
        message = JSON.parse(line);
      } catch (error) {
        void this.terminate(new Error(`yt-dlp session returned invalid JSON: ${error?.message || error}`));
        return;
      }
      if (message.event === "ready") {
        this.readyInfo = {
          ...message,
          startup_ms: Date.now() - this.startedAt,
        };
        this.readyResolve?.(this.readyInfo);
        continue;
      }
      const requestId = String(message.request_id || "");
      const pending = this.pending.get(requestId);
      if (!pending) continue;
      if (message.ok) {
        pending.resolve(message.result);
      } else {
        pending.reject(new YtDlpSessionRemoteError(String(message.error || "yt-dlp session command failed")));
      }
    }
  }

  #rejectWork(error) {
    this.readyReject?.(error);
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
  }

  #onChildError(error, child) {
    if (child !== this.child) return;
    this.#rejectWork(error);
  }

  #onExit(error, exitedChild = this.child) {
    if (exitedChild && exitedChild !== this.child) return;
    if (!this.child && !this.readyPromise) return;
    this.#rejectWork(error);
    const resolveExit = this.exitResolve;
    this.child = null;
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
    this.readyInfo = null;
    this.profileKey = null;
    this.profileId = null;
    this.proxyUrl = null;
    this.exitPromise = null;
    this.exitResolve = null;
    resolveExit?.();
  }

  terminate(reason = new Error("yt-dlp session terminated")) {
    const child = this.child;
    const exitPromise = this.exitPromise;
    this.stopping = true;
    if (!this.terminationRequested) {
      this.terminationRequested = true;
      this.terminationReason = reason;
    }
    if (!child && !this.readyPromise) return exitPromise ?? Promise.resolve();
    this.#rejectWork(reason);
    if (child && !child.killed) {
      try {
        child.kill("SIGKILL");
      } catch {
        // The process is already gone.
      }
    }
    return exitPromise ?? Promise.resolve();
  }

  async request(command, payload = {}, timeoutMs = 90000, { signal = null } = {}) {
    throwIfAborted(signal);
    await this.start({ signal });
    return this.requestExisting(command, payload, timeoutMs, { signal });
  }

  async requestExisting(command, payload = {}, timeoutMs = 90000, { signal = null } = {}) {
    throwIfAborted(signal);
    const child = this.child;
    if (!child || child.killed || !this.readyPromise || !this.readyInfo) {
      throw new Error("yt-dlp session process is not running");
    }
    if (!child.stdin?.writable) throw new Error("yt-dlp session stdin is not writable");
    const requestId = nanoid(12);
    return new Promise((resolve, reject) => {
      let settled = false;
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", handleAbort);
      };
      const finish = (callback, value) => {
        if (settled) return;
        settled = true;
        this.pending.delete(requestId);
        cleanup();
        callback(value);
      };
      const timer = setTimeout(() => {
        const error = new YtDlpSessionCommandTimeoutError(command, timeoutMs);
        finish(reject, error);
        void this.terminate(error);
      }, timeoutMs);
      const handleAbort = () => {
        finish(reject, signal.reason);
        void this.terminate(signal.reason);
      };
      this.pending.set(requestId, {
        resolve: (value) => finish(resolve, value),
        reject: (error) => finish(reject, error),
        cleanup,
        command,
      });
      signal?.addEventListener("abort", handleAbort, { once: true });
      if (signal?.aborted) {
        handleAbort();
        return;
      }
      try {
        child.stdin.write(
          `${JSON.stringify({ request_id: requestId, command, payload })}\n`,
          (error) => {
            if (error) finish(reject, error);
          },
        );
      } catch (error) {
        finish(reject, error);
      }
    });
  }

  async configure(config, { signal = null } = {}) {
    throwIfAborted(signal);
    await this.start({ signal });
    const key = JSON.stringify([
      config.profile_id,
      config.proxy_url,
      config.impersonate_target,
      config.user_agent,
      config.visitor_data,
      config.timezone,
    ]);
    if (this.profileKey === key) return this.readyInfo;
    const result = await this.requestExisting("configure", config, 30000, { signal });
    throwIfAborted(signal);
    this.profileKey = key;
    this.profileId = config.profile_id;
    this.proxyUrl = config.proxy_url;
    return result;
  }

  async stop() {
    const child = this.child;
    if (!child || child.killed || !this.readyPromise || !this.readyInfo) {
      await this.terminate();
      return;
    }
    this.stopping = true;
    this.terminationRequested = true;
    this.terminationReason = new Error("yt-dlp session stopped");
    try {
      await this.requestExisting("shutdown", {}, STOP_TIMEOUT_MS);
      const exitPromise = this.exitPromise;
      if (!exitPromise) return;
      let timer;
      try {
        await Promise.race([
          exitPromise,
          new Promise((_, reject) => {
            timer = setTimeout(() => {
              reject(new Error(`yt-dlp session shutdown timed out after ${STOP_TIMEOUT_MS}ms`));
            }, STOP_TIMEOUT_MS);
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
    } catch (error) {
      await this.terminate(error);
    }
  }
}

let daemon = null;
let activeLease = null;
let activeProfileConfig = null;
let disabledReason = null;
let daemonExitBarrier = Promise.resolve();

function commandSource(command) {
  if (command === "video_detail") return "yt_dlp_detail";
  if (command === "channel_uploads") return "yt_dlp_uploads";
  if (command === "channel_metadata") return "yt_dlp_channel_metadata";
  return `yt_dlp_${command}`;
}

function recordCommandResult(command, result, startedAt) {
  const attempts = Array.isArray(result?.attempt_timings_ms) ? result.attempt_timings_ms : null;
  if (attempts?.length) {
    for (const attempt of attempts) {
      const error = attempt.ok === false
        ? annotateYoutubeFailure(new Error(String(attempt.error || "yt-dlp attempt failed")), {
            source: commandSource(command),
            client: attempt.client,
          })
        : null;
      recordChannelExecutionRequest({
        engine: "yt_dlp",
        client: attempt.client || "web_safari",
        durationMs: attempt.duration_ms,
        ok: attempt.ok !== false,
        error,
        source: commandSource(command),
      });
    }
    return;
  }
  recordChannelExecutionRequest({
    engine: "yt_dlp",
    client: result?.client || "web_safari",
    status: 200,
    durationMs: Date.now() - startedAt,
    source: commandSource(command),
  });
}

function recordCommandFailure(command, error, startedAt) {
  const annotated = annotateYoutubeFailure(error, {
    source: commandSource(command),
    client: "web_safari",
  });
  recordChannelExecutionRequest({
    engine: "yt_dlp",
    client: "web_safari",
    durationMs: Date.now() - startedAt,
    ok: false,
    error: annotated,
    source: commandSource(command),
  });
  return annotated;
}

function enabledByEnvironment() {
  const value = String(process.env.YTDLP_PERSISTENT_POOL_ENABLED || "false").trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(value);
}

function profileConfig(profile, proxyUrl, language) {
  if (!profile?.profile_id || !proxyUrl) throw new Error("yt-dlp profile and proxy URL are required");
  return {
    profile_id: profile.profile_id,
    proxy_url: String(proxyUrl),
    impersonate_target: profile.fingerprint_json?.ytdlp_target || profile.impersonate_target,
    user_agent: profile.user_agent,
    visitor_data: profile.visitor_data,
    cookie_state: profile.cookie_state || { cookies: [] },
    language: String(language || "pt-BR"),
    timezone: String(profile.timezone || "America/Sao_Paulo"),
  };
}

function trackDaemonExit(exitPromise) {
  const prior = daemonExitBarrier;
  const barrier = Promise.all([prior, Promise.resolve(exitPromise)]).then(() => undefined);
  daemonExitBarrier = barrier;
  return barrier;
}

function terminateDaemon(current, reason) {
  if (!current) return daemonExitBarrier;
  return trackDaemonExit(current.terminate(reason));
}

async function clearAndTerminateDaemon(reason) {
  const current = daemon;
  daemon = null;
  activeLease = null;
  await terminateDaemon(current, reason);
}

async function abortSessionIfNeeded(signal) {
  if (!signal?.aborted) return;
  await clearAndTerminateDaemon(signal.reason);
  throw signal.reason;
}

async function ensureDaemon(config = activeProfileConfig, { signal = null } = {}) {
  await daemonExitBarrier;
  throwIfAborted(signal);
  if (!enabledByEnvironment()) return null;
  if (!config) throw new Error("yt-dlp fingerprint profile is not configured");
  const nextKey = JSON.stringify([
    config.profile_id,
    config.proxy_url,
    config.impersonate_target,
    config.user_agent,
    config.visitor_data,
    config.timezone,
  ]);
  if (daemon && daemon.profileKey !== nextKey && !activeLease) {
    const previous = daemon;
    daemon = null;
    await terminateDaemon(previous, new Error("yt-dlp fingerprint profile changed"));
    throwIfAborted(signal);
  }
  if (!daemon) daemon = new YtDlpDaemon();
  await daemon.start({ signal });
  await daemon.configure(config, { signal });
  throwIfAborted(signal);
  disabledReason = null;
  return daemon;
}

async function restartDaemon({ signal = null } = {}) {
  throwIfAborted(signal);
  const previous = daemon;
  daemon = null;
  const previousLease = activeLease;
  await terminateDaemon(previous, new Error("restarting yt-dlp session"));
  throwIfAborted(signal);
  if (!previousLease) return ensureDaemon(activeProfileConfig, { signal });
  const next = await ensureDaemon(activeProfileConfig, { signal });
  await next.request("acquire", previousLease, 15000, { signal });
  throwIfAborted(signal);
  return next;
}

async function requestWithRecovery(command, payload, timeoutMs, { signal = null } = {}) {
  const effectiveSignal = combineAbortSignals(signal, currentChannelExecutionAbortSignal());
  await abortSessionIfNeeded(effectiveSignal);
  const startedAt = Date.now();
  const active = activeLease;
  const strict = fingerprintTransportRequired() && Boolean(currentChannelExecution());
  if (!active || !enabledByEnvironment()) {
    if (strict) throw new Error("persistent yt-dlp fingerprint session is required for channel execution");
    return null;
  }
  assertChannelExecutionIdentity();
  let current;
  try {
    current = await ensureDaemon(activeProfileConfig, { signal: effectiveSignal });
    const result = await current.request(
      command,
      { ...payload, lease_id: active.lease_id },
      timeoutMs,
      { signal: effectiveSignal },
    );
    throwIfAborted(effectiveSignal);
    assertChannelExecutionIdentity();
    recordCommandResult(command, result, startedAt);
    return result;
  } catch (error) {
    if (effectiveSignal?.aborted) {
      await clearAndTerminateDaemon(effectiveSignal.reason);
      throw effectiveSignal.reason;
    }
    if (error?.code === "YTDLP_SESSION_COMMAND_TIMEOUT") {
      recordCommandFailure(command, error, startedAt);
    }
    if (error instanceof ProxyIdentityChangedError) throw error;
    if (error?.remote) throw recordCommandFailure(command, error, startedAt);
    try {
      current = await restartDaemon({ signal: effectiveSignal });
      const result = await current.request(
        command,
        { ...payload, lease_id: active.lease_id },
        timeoutMs,
        { signal: effectiveSignal },
      );
      throwIfAborted(effectiveSignal);
      assertChannelExecutionIdentity();
      recordCommandResult(command, result, startedAt);
      return result;
    } catch (retryError) {
      if (effectiveSignal?.aborted) {
        await clearAndTerminateDaemon(effectiveSignal.reason);
        throw effectiveSignal.reason;
      }
      const recordedRetryTimeout = retryError?.code === "YTDLP_SESSION_COMMAND_TIMEOUT"
        ? recordCommandFailure(command, retryError, startedAt)
        : null;
      if (retryError instanceof ProxyIdentityChangedError) throw retryError;
      if (retryError?.remote) throw recordCommandFailure(command, retryError, startedAt);
      disabledReason = String(retryError?.message || retryError);
      await clearAndTerminateDaemon(retryError);
      if (strict) throw recordedRetryTimeout ?? recordCommandFailure(command, retryError, startedAt);
      return null;
    }
  }
}

export function persistentYtDlpEnabled() {
  return enabledByEnvironment();
}

export async function warmPersistentYtDlp() {
  return enabledByEnvironment()
    ? { enabled: true, mode: "deferred_until_profile" }
    : { enabled: false, mode: "one_shot" };
}

export async function acquirePersistentYtDlp(channelId, language = "pt-BR", {
  profile,
  proxyUrl,
  signal = null,
} = {}) {
  const effectiveSignal = combineAbortSignals(signal, currentChannelExecutionAbortSignal());
  await abortSessionIfNeeded(effectiveSignal);
  if (!enabledByEnvironment()) return { enabled: false, mode: "one_shot" };
  if (activeLease) throw new Error(`yt-dlp process is already leased by ${activeLease.channel_id}`);
  activeProfileConfig = profileConfig(profile, proxyUrl, language);
  const lease = {
    lease_id: `channel:${channelId}:${Date.now()}:${nanoid(6)}`,
    channel_id: String(channelId),
    language: String(language || "pt-BR"),
  };
  try {
    const current = await ensureDaemon(activeProfileConfig, { signal: effectiveSignal });
    const stats = await current.request("acquire", lease, 15000, { signal: effectiveSignal });
    throwIfAborted(effectiveSignal);
    activeLease = lease;
    return {
      enabled: true,
      mode: "persistent",
      lease_id: lease.lease_id,
      process_pid: stats?.pid ?? current.readyInfo?.pid ?? null,
      process_uptime_ms: stats?.uptime_ms ?? null,
      channels_processed: stats?.channels_processed ?? null,
    };
  } catch (error) {
    if (effectiveSignal?.aborted) {
      await clearAndTerminateDaemon(effectiveSignal.reason);
      throw effectiveSignal.reason;
    }
    activeLease = null;
    disabledReason = String(error?.message || error);
    return { enabled: false, mode: "one_shot", error: disabledReason };
  }
}

export async function releasePersistentYtDlp({ cancelled = false, reason = null, signal = null } = {}) {
  if (cancelled || signal?.aborted) {
    await clearAndTerminateDaemon(
      signal?.aborted ? signal.reason : reason ?? new Error("yt-dlp channel execution cancelled"),
    );
    return null;
  }
  const lease = activeLease;
  activeLease = null;
  const current = daemon;
  if (!lease || !current) return null;
  try {
    const result = await current.requestExisting(
      "release",
      { lease_id: lease.lease_id },
      15000,
      { signal },
    );
    throwIfAborted(signal);
    return result;
  } catch (error) {
    if (daemon === current) daemon = null;
    await terminateDaemon(current, signal?.aborted ? signal.reason : error);
    if (signal?.aborted) throw signal.reason;
    return { error: String(error?.message || error), restarted: true };
  }
}

export async function closePersistentYtDlp() {
  activeLease = null;
  activeProfileConfig = null;
  const current = daemon;
  daemon = null;
  if (!current) {
    await daemonExitBarrier;
    return;
  }
  await trackDaemonExit(current.stop());
}

export async function persistentChannelMetadata(url, { timeoutMs = 90000, signal = null } = {}) {
  return requestWithRecovery("channel_metadata", { url }, timeoutMs, { signal });
}

export async function persistentChannelUploads(channelId, limit, {
  timeoutMs = 180000,
  signal = null,
} = {}) {
  return requestWithRecovery("channel_uploads", { channel_id: channelId, limit }, timeoutMs, { signal });
}

export async function persistentVideoDetail(url, { timeoutMs = 90000, signal = null } = {}) {
  return requestWithRecovery("video_detail", { url }, timeoutMs, { signal });
}

export function persistentYtDlpState() {
  return {
    enabled: enabledByEnvironment(),
    active_channel: activeLease?.channel_id ?? null,
    process_pid: daemon?.readyInfo?.pid ?? null,
    process_started_at: daemon?.startedAt ? new Date(daemon.startedAt).toISOString() : null,
    profile_id: daemon?.profileId ?? null,
    proxy_url_matches: daemon && activeProfileConfig ? daemon.proxyUrl === activeProfileConfig.proxy_url : null,
    disabled_reason: disabledReason,
  };
}
