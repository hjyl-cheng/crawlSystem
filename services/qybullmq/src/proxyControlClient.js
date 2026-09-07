const defaultFetch = (...args) => globalThis.fetch(...args);

function safeJson(text) {
  try {
    return text ? JSON.parse(text) : {};
  } catch {
    return {};
  }
}

function retryableStatus(status) {
  return status === 429 || status >= 500;
}

function retryDelayMs(response, attempt) {
  const raw = response?.headers?.get?.("retry-after");
  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.min(5000, seconds * 1000);
  return Math.min(1000, 100 * (2 ** Math.max(0, attempt - 1)));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class ProxyControlRequestError extends Error {
  constructor(message, {
    status = null,
    code = null,
    payload = null,
    retryable = false,
    cause = null,
  } = {}) {
    super(message, cause ? { cause } : undefined);
    this.name = "ProxyControlRequestError";
    this.status = status;
    this.code = code;
    this.payload = payload;
    this.retryable = retryable;
  }
}

export class ProxyControlClient {
  constructor({
    controlUrl = process.env.ROTA_PROXY_CONTROL_URL,
    token = process.env.ROTA_PROXY_CONTROL_TOKEN,
    fetchImpl = defaultFetch,
    timeoutMs = Number(process.env.ROTA_PROXY_CONTROL_TIMEOUT_MS || 5000),
    maxAttempts = Number(process.env.ROTA_PROXY_CONTROL_MAX_ATTEMPTS || 3),
    sleepImpl = sleep,
  } = {}) {
    this.controlUrl = String(controlUrl || "").trim().replace(/\/+$/, "");
    this.token = String(token || "");
    this.fetchImpl = fetchImpl;
    this.timeoutMs = Math.max(100, Number(timeoutMs) || 5000);
    this.maxAttempts = Math.max(1, Math.min(5, Number(maxAttempts) || 3));
    this.sleepImpl = sleepImpl;
  }

  claim(request) {
    return this.#command("claim", request);
  }

  renew(request) {
    return this.#command("renew", request);
  }

  beginTask(request) {
    return this.#command("tasks/begin", request);
  }

  observe(request) {
    return this.#command("tasks/observe", request);
  }

  completeTask(request) {
    return this.#command("tasks/complete", request);
  }

  release(request) {
    return this.#command("release", request);
  }

  capacity() {
    return this.#command("capacity", null, { method: "GET" });
  }
  businessRunBudget(businessRunId) {
    return this.#command(`business-runs/${encodeURIComponent(businessRunId)}/budget`, null, { method: "GET" });
  }

  async close() {}

  async #command(path, payload, { method = "POST" } = {}) {
    if (!this.controlUrl) {
      throw new ProxyControlRequestError("Rota proxy control URL is not configured");
    }
    if (this.token.length < 12) {
      throw new ProxyControlRequestError("Rota proxy control token is not configured");
    }
    const encoded = payload === null ? null : JSON.stringify(payload);
    let lastError = null;
    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), this.timeoutMs);
      let response = null;
      try {
        response = await this.fetchImpl(`${this.controlUrl}/${path}`, {
          method,
          headers: {
            authorization: `Bearer ${this.token}`,
            ...(encoded === null ? {} : { "content-type": "application/json" }),
          },
          ...(encoded === null ? {} : { body: encoded }),
          signal: controller.signal,
        });
        const text = await response.text();
        const parsed = safeJson(text);
        if (response.ok && parsed.ok !== false) return parsed;
        lastError = new ProxyControlRequestError(
          parsed.error || `Rota proxy control returned HTTP ${response.status}`,
          {
            status: response.status,
            code: parsed.code ?? null,
            payload: parsed,
            retryable: retryableStatus(response.status),
          },
        );
      } catch (error) {
        if (error instanceof ProxyControlRequestError) {
          lastError = error;
        } else {
          const timedOut = controller.signal.aborted;
          lastError = new ProxyControlRequestError(
            timedOut ? "Rota proxy control request timed out" : "Rota proxy control request failed",
            { retryable: true, cause: error },
          );
        }
      } finally {
        clearTimeout(timer);
      }
      if (!lastError.retryable || attempt >= this.maxAttempts) throw lastError;
      await this.sleepImpl(retryDelayMs(response, attempt));
    }
    throw lastError;
  }
}

let defaultClient = null;

export function proxyControlClient() {
  if (!defaultClient) defaultClient = new ProxyControlClient();
  return defaultClient;
}

export async function closeProxyControlClient() {
  if (!defaultClient) return;
  await defaultClient.close();
  defaultClient = null;
}
