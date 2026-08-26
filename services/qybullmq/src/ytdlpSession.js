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
  }

  async start({ signal = null } = {}) {
    throwIfAborted(signal);
    if (this.child && !this.child.killed && this.readyPromise) {
      await waitForSignal(
        this.readyPromise,
        signal,
        (reason) => this.terminate(reason),
      );
      return this.readyInfo;
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
    child.stdout.on("data", (chunk) => this.#onStdout(chunk));
    child.stderr.on("data", (chunk) => {
      this.stderrBuffer = `${this.stderrBuffer}${chunk.toString()}`.slice(-16000);
    });
    child.on("error", (error) => this.#onExit(error, child));
    child.on("close", (code, signal) => {
      const suffix = this.stderrBuffer.trim() ? `: ${this.stderrBuffer.trim().slice(-1000)}` : "";
      this.#onExit(new Error(`yt-dlp session exited code=${code} signal=${signal}${suffix}`), child);
    });
    const timer = setTimeout(() => {
      const error = new Error(`yt-dlp session start timeout after ${START_TIMEOUT_MS}ms`);
      this.terminate(error);
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
        this.terminate(new Error(`yt-dlp session returned invalid JSON: ${error?.message || error}`));
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

  #onExit(error, exitedChild = this.child) {
    if (exitedChild && this.child && exitedChild !== this.child) return;
    if (!this.child && !this.readyPromise) return;
    this.readyReject?.(error);
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
    this.child = null;
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
    this.readyInfo = null;
    this.profileKey = null;
    this.profileId = null;
    this.proxyUrl = null;
  }

  terminate(reason = new Error("yt-dlp session terminated")) {
    const child = this.child;
    if (!child && !this.readyPromise) return;
    this.stopping = true;
    if (child && !child.killed) {
      try {
        child.kill("SIGKILL");
      } catch {
        // The process is already gone.
      }
    }
    this.#onExit(reason, child);
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
        const error = new Error(`yt-dlp session command ${command} timed out after ${timeoutMs}ms`);
        finish(reject, error);
        this.terminate(error);
      }, timeoutMs);
      const handleAbort = () => {
        finish(reject, signal.reason);
        this.terminate(signal.reason);
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
      this.terminate();
      return;
    }
    this.stopping = true;
    try {
      await this.requestExisting("shutdown", {}, STOP_TIMEOUT_MS);
    } catch (error) {
      this.terminate(error);
    }
  }
}

let daemon = null;
let activeLease = null;
let activeProfileConfig = null;
let disabledReason = null;

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

function clearAndTerminateDaemon(reason) {
  const current = daemon;
  daemon = null;
  activeLease = null;
  current?.terminate(reason);
}

function abortSessionIfNeeded(signal) {
  if (!signal?.aborted) return;
  clearAndTerminateDaemon(signal.reason);
  throw signal.reason;
}

async function ensureDaemon(config = activeProfileConfig, { signal = null } = {}) {
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
    daemon.terminate(new Error("yt-dlp fingerprint profile changed"));
    daemon = null;
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
  previous?.terminate(new Error("restarting yt-dlp session"));
  const previousLease = activeLease;
  if (!previousLease) return ensureDaemon(activeProfileConfig, { signal });
  const next = await ensureDaemon(activeProfileConfig, { signal });
  await next.request("acquire", previousLease, 15000, { signal });
  throwIfAborted(signal);
  return next;
}

async function requestWithRecovery(command, payload, timeoutMs, { signal = null } = {}) {
  const effectiveSignal = combineAbortSignals(signal, currentChannelExecutionAbortSignal());
  abortSessionIfNeeded(effectiveSignal);
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
      clearAndTerminateDaemon(effectiveSignal.reason);
      throw effectiveSignal.reason;
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
        clearAndTerminateDaemon(effectiveSignal.reason);
        throw effectiveSignal.reason;
      }
      if (retryError instanceof ProxyIdentityChangedError) throw retryError;
      if (retryError?.remote) throw recordCommandFailure(command, retryError, startedAt);
      disabledReason = String(retryError?.message || retryError);
      clearAndTerminateDaemon(retryError);
      if (strict) throw recordCommandFailure(command, retryError, startedAt);
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
  abortSessionIfNeeded(effectiveSignal);
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
      clearAndTerminateDaemon(effectiveSignal.reason);
      throw effectiveSignal.reason;
    }
    activeLease = null;
    disabledReason = String(error?.message || error);
    return { enabled: false, mode: "one_shot", error: disabledReason };
  }
}

export async function releasePersistentYtDlp({ cancelled = false, reason = null } = {}) {
  if (cancelled) {
    clearAndTerminateDaemon(reason ?? new Error("yt-dlp channel execution cancelled"));
    return null;
  }
  const lease = activeLease;
  activeLease = null;
  const current = daemon;
  if (!lease || !current) return null;
  try {
    return await current.requestExisting("release", { lease_id: lease.lease_id }, 15000);
  } catch (error) {
    if (daemon === current) daemon = null;
    current.terminate(error);
    return { error: String(error?.message || error), restarted: true };
  }
}

export async function closePersistentYtDlp() {
  activeLease = null;
  activeProfileConfig = null;
  const current = daemon;
  daemon = null;
  if (!current) return;
  await current.stop();
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
