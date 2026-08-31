import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { fetch as undiciFetch } from "undici";

const SCRIPT_PATH = fileURLToPath(new URL("../scripts/fingerprint_gateway.py", import.meta.url));
const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "content-length",
  "host",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

function numberEnv(name, fallback, minimum = 1) {
  const value = Number.parseInt(String(process.env[name] ?? ""), 10);
  return Number.isFinite(value) ? Math.max(minimum, value) : fallback;
}

function safeJson(text) {
  try {
    return text ? JSON.parse(text) : {};
  } catch {
    return {};
  }
}

async function responseEvidence(response, limit = 2048) {
  const bytes = typeof response?.arrayBuffer === "function"
    ? Buffer.from(await response.arrayBuffer())
    : Buffer.from(await response.text());
  return Object.freeze({
    detail: bytes.toString("utf8"),
    bodySampleBase64: bytes.subarray(0, limit).toString("base64"),
  });
}

function isMissingTargetHttpStatus(value) {
  return value === 0 || value === "0";
}

export class FingerprintGatewayError extends Error {
  constructor({ gatewayStatus, payload = {}, detail = "", targetUrl = null } = {}) {
    const originalFailureKind = String(payload.failure_kind || "unknown");
    const errorType = String(payload.error_type || "FingerprintGatewayError");
    const curlCode = Number.isInteger(Number(payload.curl_code)) ? Number(payload.curl_code) : null;
    const evidence = curlCode == null ? errorType : `${errorType} curl_code=${curlCode}`;
    const normalizedDetail = String(detail).slice(0, 2000);
    const targetStatusRaw = payload.target_status_raw ?? null;
    const missingTargetHttp = isMissingTargetHttpStatus(targetStatusRaw);
    const failureKind = originalFailureKind === "invalid_target_status" && missingTargetHttp
      ? "proxy_transport"
      : originalFailureKind;
    const targetHeaders = payload.target_response_headers
      && typeof payload.target_response_headers === "object"
      && !Array.isArray(payload.target_response_headers)
      ? { ...payload.target_response_headers }
      : {};
    const targetHeadersRaw = payload.target_response_headers_raw ?? null;
    super(
      missingTargetHttp
        ? `fingerprint gateway proxy_transport: ${errorType} target_status_raw=${String(targetStatusRaw)}`
        : failureKind === "invalid_target_status"
        ? `fingerprint gateway invalid target HTTP status: ${String(targetStatusRaw)}`
        : failureKind === "unknown"
        ? `fingerprint gateway request failed HTTP ${gatewayStatus}: ${String(detail).slice(0, 500)}`
        : `fingerprint gateway ${failureKind}: ${evidence}`,
    );
    this.name = "FingerprintGatewayError";
    this.code = failureKind === "proxy_transport"
      ? "FINGERPRINT_PROXY_TRANSPORT"
      : failureKind === "upstream_transient"
        ? "FINGERPRINT_UPSTREAM_TRANSIENT"
        : failureKind === "invalid_target_status"
          ? "FINGERPRINT_INVALID_TARGET_STATUS"
        : "FINGERPRINT_GATEWAY_ERROR";
    this.curlCode = curlCode;
    this.gatewayStatus = Number(gatewayStatus) || null;
    this.failureKind = failureKind;
    this.sessionReset = payload.session_reset === true;
    this.detail = normalizedDetail;
    this.gatewayPayload = { ...payload };
    this.targetStatusRaw = targetStatusRaw;
    this.targetHeaders = targetHeaders;
    this.targetHeadersRaw = targetHeadersRaw;
    this.targetBodySampleBase64 = payload.target_body_sample_base64 ?? null;
    this.youtube_failure_evidence = {
      status: null,
      body: originalFailureKind === "invalid_target_status" || missingTargetHttp
        ? JSON.stringify({
          error_type: errorType,
          target_status_raw: targetStatusRaw,
          target_response_headers: targetHeaders,
          target_response_headers_raw: targetHeadersRaw,
          target_body_sample_base64: payload.target_body_sample_base64 ?? null,
          detail: normalizedDetail,
        })
        : evidence,
      source: "fingerprint_gateway",
      target_url: targetUrl ? String(targetUrl) : null,
      client: null,
    };
  }
}

export function encodeGatewayMetadata(value) {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

export function decodeGatewayMetadata(value, fallback = null) {
  if (!value) return fallback;
  return JSON.parse(Buffer.from(String(value), "base64url").toString("utf8"));
}

function filteredHeaders(value) {
  const output = {};
  const headers = new Headers(value || {});
  for (const [name, headerValue] of headers.entries()) {
    if (!HOP_BY_HOP_HEADERS.has(name.toLowerCase())) output[name] = headerValue;
  }
  return output;
}

async function requestParts(input, init = {}) {
  const requestLike = typeof input === "object" && input !== null && !(input instanceof URL);
  const url = new URL(requestLike ? input.url : input).toString();
  const method = String(init.method || (requestLike ? input.method : "GET") || "GET").toUpperCase();
  const headers = filteredHeaders(init.headers || (requestLike ? input.headers : {}));
  let body = init.body;
  if (body === undefined && requestLike && !["GET", "HEAD"].includes(method)) {
    body = await input.clone().arrayBuffer();
  }
  if (body === undefined || body === null || ["GET", "HEAD"].includes(method)) body = undefined;
  else if (typeof body === "string" || Buffer.isBuffer(body) || body instanceof Uint8Array || body instanceof ArrayBuffer) {
    body = Buffer.from(body);
  } else {
    throw new Error(`unsupported fingerprint request body: ${body?.constructor?.name || typeof body}`);
  }
  return { url, method, headers, body };
}

export class FingerprintGateway {
  constructor({
    host = "127.0.0.1",
    port = numberEnv("FINGERPRINT_GATEWAY_PORT", 3099),
    python = String(process.env.FINGERPRINT_PYTHON_BIN || "python3"),
    fetchFn = undiciFetch,
    spawnFn = spawn,
  } = {}) {
    this.host = host;
    this.port = port;
    this.python = python;
    this.fetchFn = fetchFn;
    this.spawnFn = spawnFn;
    this.child = null;
    this.startPromise = null;
    this.stderr = "";
    this.configurationKey = null;
  }

  baseUrl() {
    return `http://${this.host}:${this.port}`;
  }

  async start() {
    if (this.child && !this.child.killed) return;
    if (this.startPromise) return this.startPromise;
    this.startPromise = (async () => {
      const child = this.spawnFn(this.python, ["-u", SCRIPT_PATH, "--host", this.host, "--port", String(this.port)], {
        stdio: ["ignore", "ignore", "pipe"],
        env: { ...process.env },
      });
      this.child = child;
      child.stderr?.on("data", (chunk) => {
        this.stderr = `${this.stderr}${chunk.toString()}`.slice(-16000);
      });
      child.once("exit", () => {
        if (this.child === child) {
          this.child = null;
          this.configurationKey = null;
        }
      });
      child.once("error", (error) => {
        this.stderr = `${this.stderr}\n${error.message}`.slice(-16000);
      });
      for (let attempt = 0; attempt < 60; attempt += 1) {
        if (!this.child) break;
        try {
          const response = await this.fetchFn(`${this.baseUrl()}/health`, {
            signal: AbortSignal.timeout(500),
          });
          if (response.ok) return;
        } catch {
          // The local gateway is still starting.
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      if (this.child === child && !child.killed) child.kill("SIGTERM");
      throw new Error(`fingerprint gateway failed to start${this.stderr ? `: ${this.stderr.slice(-1000)}` : ""}`);
    })().finally(() => {
      this.startPromise = null;
    });
    return this.startPromise;
  }

  async prepare({ proxyUrl, profileGroup }) {
    await this.start();
    const profiles = Object.values(profileGroup?.clients || {});
    if (!proxyUrl || profiles.length === 0) throw new Error("fingerprint gateway requires proxy URL and client profiles");
    const key = `${proxyUrl}|${profileGroup.profile_group_id}|${profileGroup.profile_revision}`;
    if (this.configurationKey === key) return;
    const response = await this.fetchFn(`${this.baseUrl()}/v1/profiles/configure`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        proxy_url: proxyUrl,
        profiles: profiles.map((profile) => ({
          profile_id: profile.profile_id,
          engine: profile.engine,
          impersonate_target: profile.impersonate_target,
          user_agent: profile.user_agent,
          cookie_state: profile.cookie_state || { cookies: [] },
          max_connections: Number(profile.fingerprint_json?.max_connections || 1),
        })),
      }),
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) throw new Error(`fingerprint gateway configure failed HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
    this.configurationKey = key;
  }

  async fetch(profile, input, init = {}) {
    if (!profile?.profile_id) throw new Error("fingerprint client profile is required");
    await this.start();
    const request = await requestParts(input, init);
    request.headers["user-agent"] = profile.user_agent;
    const response = await this.fetchFn(`${this.baseUrl()}/v1/fetch/${encodeURIComponent(profile.profile_id)}`, {
      method: "POST",
      headers: {
        "content-type": "application/octet-stream",
        "x-fingerprint-url": encodeGatewayMetadata(request.url),
        "x-fingerprint-method": request.method,
        "x-fingerprint-headers": encodeGatewayMetadata(request.headers),
        "x-fingerprint-redirect": String(init.redirect || "follow"),
        "x-fingerprint-timeout-ms": String(
          Math.max(1000, Number(init.timeoutMs) || numberEnv("YOUTUBEJS_TIMEOUT_MS", 30000, 1000)),
        ),
      },
      body: request.body,
      signal: init.signal,
    });
    const rawTargetStatus = response.headers.get("x-fingerprint-response-status");
    if (!rawTargetStatus) {
      const { detail } = await responseEvidence(response);
      const payload = safeJson(detail);
      throw new FingerprintGatewayError({
        gatewayStatus: response.status,
        payload: {
          ...payload,
          failure_kind: isMissingTargetHttpStatus(payload.target_status_raw)
            ? "proxy_transport"
            : (payload.failure_kind || "unknown"),
        },
        detail,
        targetUrl: request.url,
      });
    }
    const rawTargetHeaders = response.headers.get("x-fingerprint-response-headers");
    const targetStatus = /^\d{3}$/.test(rawTargetStatus) ? Number(rawTargetStatus) : null;
    if (!Number.isInteger(targetStatus) || targetStatus < 200 || targetStatus > 599) {
      let targetHeaders = {};
      try {
        targetHeaders = decodeGatewayMetadata(rawTargetHeaders, {});
      } catch {
        // The raw metadata remains part of the structured invalid-status evidence.
      }
      const { detail, bodySampleBase64 } = await responseEvidence(response);
      throw new FingerprintGatewayError({
        gatewayStatus: response.status,
        payload: {
          error: "fingerprint target returned an invalid HTTP status",
          error_type: "InvalidTargetHttpStatus",
          failure_kind: isMissingTargetHttpStatus(rawTargetStatus)
            ? "proxy_transport"
            : "invalid_target_status",
          target_status_raw: rawTargetStatus,
          target_response_headers: targetHeaders,
          target_response_headers_raw: rawTargetHeaders,
          target_body_sample_base64: bodySampleBase64,
        },
        detail,
        targetUrl: request.url,
      });
    }
    const targetHeaders = decodeGatewayMetadata(rawTargetHeaders, {});
    const body = [101, 204, 205, 304].includes(targetStatus) ? null : response.body;
    return new Response(body, {
      status: targetStatus,
      headers: targetHeaders,
    });
  }

  async snapshot(profile) {
    if (!profile?.profile_id || !this.child) return profile?.cookie_state || { cookies: [] };
    const response = await this.fetchFn(
      `${this.baseUrl()}/v1/profiles/${encodeURIComponent(profile.profile_id)}/snapshot`,
      { signal: AbortSignal.timeout(5000) },
    );
    if (!response.ok) throw new Error(`fingerprint gateway snapshot failed HTTP ${response.status}`);
    return response.json();
  }

  async close() {
    const child = this.child;
    this.child = null;
    this.configurationKey = null;
    if (!child) return;
    try {
      await this.fetchFn(`${this.baseUrl()}/shutdown`, {
        method: "POST",
        signal: AbortSignal.timeout(1000),
      });
    } catch {
      // The child is terminated below if graceful shutdown did not complete.
    }
    if (!child.killed) child.kill("SIGTERM");
  }
}

let defaultGateway = null;

export function fingerprintGateway() {
  if (!defaultGateway) defaultGateway = new FingerprintGateway();
  return defaultGateway;
}
