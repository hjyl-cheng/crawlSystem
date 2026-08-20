import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { nanoid } from "nanoid";
import {
  assertChannelExecutionIdentity,
  currentChannelExecution,
  ProxyIdentityChangedError,
  recordChannelExecutionRequest,
} from "./channelExecutionContext.js";
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

  async start() {
    if (this.child && !this.child.killed && this.readyPromise) {
      await this.readyPromise;
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
    child.on("error", (error) => this.#onExit(error));
    child.on("close", (code, signal) => {
      const suffix = this.stderrBuffer.trim() ? `: ${this.stderrBuffer.trim().slice(-1000)}` : "";
      this.#onExit(new Error(`yt-dlp session exited code=${code} signal=${signal}${suffix}`));
    });
    const timer = setTimeout(() => {
      this.readyReject?.(new Error(`yt-dlp session start timeout after ${START_TIMEOUT_MS}ms`));
      this.#terminate();
    }, START_TIMEOUT_MS);
    try {
      await this.readyPromise;
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
        this.#onExit(new Error(`yt-dlp session returned invalid JSON: ${error?.message || error}`));
        this.#terminate();
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
      this.pending.delete(requestId);
      clearTimeout(pending.timer);
      if (message.ok) {
        pending.resolve(message.result);
      } else {
        pending.reject(new YtDlpSessionRemoteError(String(message.error || "yt-dlp session command failed")));
      }
    }
  }

  #onExit(error) {
    if (!this.child && !this.readyPromise) return;
    this.readyReject?.(error);
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    this.child = null;
    this.readyPromise = null;
    this.readyResolve = null;
    this.readyReject = null;
    this.readyInfo = null;
  }

  #terminate() {
    if (!this.child) return;
    try {
      this.child.kill("SIGKILL");
    } catch {
      // The process is already gone.
    }
  }

  async request(command, payload = {}, timeoutMs = 90000) {
    await this.start();
    if (!this.child?.stdin?.writable) throw new Error("yt-dlp session stdin is not writable");
    const requestId = nanoid(12);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(new Error(`yt-dlp session command ${command} timed out after ${timeoutMs}ms`));
        this.#terminate();
      }, timeoutMs);
      this.pending.set(requestId, { resolve, reject, timer, command });
      this.child.stdin.write(`${JSON.stringify({ request_id: requestId, command, payload })}\n`, (error) => {
        if (!error) return;
        const pending = this.pending.get(requestId);
        if (!pending) return;
        this.pending.delete(requestId);
        clearTimeout(timer);
        reject(error);
      });
    });
  }

  async configure(config) {
    await this.start();
    const key = JSON.stringify([
      config.profile_id,
      config.proxy_url,
      config.impersonate_target,
      config.user_agent,
      config.visitor_data,
      config.timezone,
    ]);
    if (this.profileKey === key) return this.readyInfo;
    const result = await this.request("configure", config, 30000);
    this.profileKey = key;
    this.profileId = config.profile_id;
    this.proxyUrl = config.proxy_url;
    return result;
  }

  async stop() {
    const child = this.child;
    if (!child) return;
    this.stopping = true;
    try {
      await this.request("shutdown", {}, STOP_TIMEOUT_MS);
    } catch {
      this.#terminate();
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

async function ensureDaemon(config = activeProfileConfig) {
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
    await daemon.stop();
    daemon = null;
  }
  if (!daemon) daemon = new YtDlpDaemon();
  await daemon.start();
  await daemon.configure(config);
  disabledReason = null;
  return daemon;
}

async function restartDaemon() {
  if (daemon) await daemon.stop();
  daemon = null;
  const previousLease = activeLease;
  if (!previousLease) return ensureDaemon(activeProfileConfig);
  const next = await ensureDaemon(activeProfileConfig);
  await next.request("acquire", previousLease, 15000);
  return next;
}

async function requestWithRecovery(command, payload, timeoutMs) {
  const startedAt = Date.now();
  const active = activeLease;
  const strict = fingerprintTransportRequired() && Boolean(currentChannelExecution());
  if (!active || !enabledByEnvironment()) {
    if (strict) throw new Error("persistent yt-dlp fingerprint session is required for channel execution");
    return null;
  }
  assertChannelExecutionIdentity();
  let current = await ensureDaemon();
  try {
    const result = await current.request(command, { ...payload, lease_id: active.lease_id }, timeoutMs);
    assertChannelExecutionIdentity();
    recordCommandResult(command, result, startedAt);
    return result;
  } catch (error) {
    if (error instanceof ProxyIdentityChangedError) throw error;
    if (error?.remote) throw recordCommandFailure(command, error, startedAt);
    try {
      current = await restartDaemon();
      const result = await current.request(command, { ...payload, lease_id: active.lease_id }, timeoutMs);
      assertChannelExecutionIdentity();
      recordCommandResult(command, result, startedAt);
      return result;
    } catch (retryError) {
      if (retryError instanceof ProxyIdentityChangedError) throw retryError;
      if (retryError?.remote) throw recordCommandFailure(command, retryError, startedAt);
      disabledReason = String(retryError?.message || retryError);
      activeLease = null;
      if (daemon) await daemon.stop();
      daemon = null;
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

export async function acquirePersistentYtDlp(channelId, language = "pt-BR", { profile, proxyUrl } = {}) {
  if (!enabledByEnvironment()) return { enabled: false, mode: "one_shot" };
  if (activeLease) throw new Error(`yt-dlp process is already leased by ${activeLease.channel_id}`);
  activeProfileConfig = profileConfig(profile, proxyUrl, language);
  const lease = {
    lease_id: `channel:${channelId}:${Date.now()}:${nanoid(6)}`,
    channel_id: String(channelId),
    language: String(language || "pt-BR"),
  };
  try {
    const current = await ensureDaemon(activeProfileConfig);
    const stats = await current.request("acquire", lease, 15000);
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
    activeLease = null;
    disabledReason = String(error?.message || error);
    return { enabled: false, mode: "one_shot", error: disabledReason };
  }
}

export async function releasePersistentYtDlp() {
  const lease = activeLease;
  activeLease = null;
  if (!lease || !daemon) return null;
  try {
    return await daemon.request("release", { lease_id: lease.lease_id }, 15000);
  } catch (error) {
    await daemon.stop();
    daemon = null;
    return { error: String(error?.message || error), restarted: true };
  }
}

export async function closePersistentYtDlp() {
  activeLease = null;
  activeProfileConfig = null;
  if (!daemon) return;
  await daemon.stop();
  daemon = null;
}

export async function persistentChannelMetadata(url, { timeoutMs = 90000 } = {}) {
  return requestWithRecovery("channel_metadata", { url }, timeoutMs);
}

export async function persistentChannelUploads(channelId, limit, { timeoutMs = 180000 } = {}) {
  return requestWithRecovery("channel_uploads", { channel_id: channelId, limit }, timeoutMs);
}

export async function persistentVideoDetail(url, { timeoutMs = 90000 } = {}) {
  return requestWithRecovery("video_detail", { url }, timeoutMs);
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
