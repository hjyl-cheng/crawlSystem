import { AsyncLocalStorage } from "node:async_hooks";
import { sameProxyAssignment } from "./proxyAssignment.js";
import { youtubeFailureEvidence, youtubeFailureText } from "./youtubeFailurePolicy.js";

const executionStorage = new AsyncLocalStorage();

function bounded(value, maxLength = 500) {
  const text = String(value ?? "");
  return text.length <= maxLength ? text : text.slice(0, maxLength);
}

function emptyEngineMetrics() {
  return {
    requests: 0,
    failures: 0,
    duration_ms: 0,
    clients: {},
    statuses: {},
  };
}

export class ChannelExecutionMetrics {
  constructor() {
    this.requests = 0;
    this.failures = 0;
    this.durationMs = 0;
    this.byEngine = {};
    this.failureEvidence = [];
  }

  recordRequest({
    engine = "unknown",
    client = null,
    status = null,
    durationMs = 0,
    ok = true,
    error = null,
    body = "",
    source = "",
    targetUrl = null,
  } = {}) {
    const engineName = String(engine || "unknown");
    const metrics = this.byEngine[engineName] ?? emptyEngineMetrics();
    const duration = Math.max(0, Number(durationMs) || 0);
    const statusKey = Number.isInteger(Number(status)) ? String(Number(status)) : null;
    const clientName = String(client || "unknown");
    this.requests += 1;
    this.durationMs += duration;
    metrics.requests += 1;
    metrics.duration_ms += duration;
    metrics.clients[clientName] = (metrics.clients[clientName] || 0) + 1;
    if (statusKey) metrics.statuses[statusKey] = (metrics.statuses[statusKey] || 0) + 1;
    if (!ok) {
      this.failures += 1;
      metrics.failures += 1;
      this.recordFailure({ error, status, body, source, targetUrl, client: clientName, engine: engineName });
    }
    this.byEngine[engineName] = metrics;
  }

  recordFailure({ error = null, engine = "unknown", ...overrides } = {}) {
    const evidence = youtubeFailureEvidence(error, overrides);
    this.failureEvidence.push({
      engine: String(engine || "unknown"),
      client: evidence.client,
      status: evidence.status == null ? null : Number(evidence.status),
      source: bounded(evidence.source, 120),
      target_url: bounded(evidence.target_url, 500) || null,
      body: bounded(evidence.body),
      error_name: error?.name || null,
      error_code: error?.code || error?.cause?.code || null,
      error_message: bounded(youtubeFailureText(error)),
    });
    if (this.failureEvidence.length > 20) this.failureEvidence.shift();
  }

  markFailure(evidence = {}) {
    const engineName = String(evidence.engine || "unknown");
    const metrics = this.byEngine[engineName] ?? emptyEngineMetrics();
    this.failures += 1;
    metrics.failures += 1;
    this.byEngine[engineName] = metrics;
    this.recordFailure({ ...evidence, engine: engineName });
  }

  snapshot() {
    return {
      request_count: this.requests,
      failure_count: this.failures,
      duration_ms: Math.round(this.durationMs),
      by_engine: Object.fromEntries(Object.entries(this.byEngine).map(([engine, value]) => [
        engine,
        {
          ...value,
          duration_ms: Math.round(value.duration_ms),
          clients: { ...value.clients },
          statuses: { ...value.statuses },
        },
      ])),
      failure_evidence: this.failureEvidence.map((item) => ({ ...item })),
    };
  }
}

export class ProxyIdentityChangedError extends Error {
  constructor(expected, actual) {
    super(`proxy identity changed during channel attempt: expected ${expected?.proxy_id ?? "none"}, received ${actual?.proxy_id ?? "none"}`);
    this.name = "ProxyIdentityChangedError";
    this.code = "PROXY_IDENTITY_CHANGED";
    this.expected = expected || null;
    this.actual = actual || null;
  }
}

export function runWithChannelExecution(context, callback) {
  return executionStorage.run(Object.freeze({ ...context }), callback);
}

export function currentChannelExecution() {
  return executionStorage.getStore() ?? null;
}

export function currentClientProfile(engine) {
  return currentChannelExecution()?.profile_group?.clients?.[engine] ?? null;
}

export function recordChannelExecutionRequest(event) {
  currentChannelExecution()?.metrics?.recordRequest(event);
}

export function recordChannelExecutionFailure(evidence) {
  currentChannelExecution()?.metrics?.markFailure(evidence);
}

export function channelExecutionMetrics() {
  return currentChannelExecution()?.metrics?.snapshot?.() ?? null;
}

export function currentChannelExecutionAbortSignal() {
  return currentChannelExecution()?.abort_signal ?? null;
}

export function assertChannelExecutionIdentity() {
  const context = currentChannelExecution();
  if (!context) return null;
  const actual = context.get_proxy_snapshot?.() ?? context.proxy;
  if (!sameProxyAssignment(context.proxy, actual)) {
    throw new ProxyIdentityChangedError(context.proxy, actual);
  }
  return actual;
}
