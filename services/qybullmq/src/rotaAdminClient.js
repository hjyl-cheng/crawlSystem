function trimBaseUrl(value) {
  return String(value || "").replace(/\/+$/, "");
}

function truncate(value, maximum = 500) {
  const text = String(value ?? "");
  return text.length > maximum ? text.slice(0, maximum) : text;
}

export class RotaAdminClient {
  constructor({
    baseUrl,
    username,
    password,
    fetchImpl = globalThis.fetch,
    timeoutMs = 15_000,
  } = {}) {
    this.baseUrl = trimBaseUrl(baseUrl);
    this.username = String(username || "").trim();
    this.password = String(password || "");
    this.fetchImpl = fetchImpl;
    this.timeoutMs = Math.max(1_000, Number(timeoutMs) || 15_000);
    this.token = null;
    this.loginPromise = null;
    if (!this.baseUrl) throw new Error("Rota API base URL is required");
    if (!this.username || !this.password) throw new Error("Rota admin credentials are required");
    if (typeof this.fetchImpl !== "function") throw new TypeError("fetchImpl must be a function");
  }

  async login() {
    if (this.loginPromise) return this.loginPromise;
    this.loginPromise = this.#login().finally(() => {
      this.loginPromise = null;
    });
    return this.loginPromise;
  }

  async #login() {
    const response = await this.fetchImpl(`${this.baseUrl}/api/v1/auth/login`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username: this.username, password: this.password }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const payload = await this.#payload(response);
    if (!response.ok || !payload.token) {
      throw new Error(`Rota login failed with HTTP ${response.status}`);
    }
    this.token = String(payload.token);
    return this.token;
  }

  async request(path, {
    method = "GET",
    body,
    expected = [200],
    retryAuth = true,
    timeoutMs = this.timeoutMs,
  } = {}) {
    if (!this.token) await this.login();
    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      method,
      headers: {
        authorization: `Bearer ${this.token}`,
        ...(body === undefined ? {} : { "content-type": "application/json" }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(Math.max(1_000, Number(timeoutMs) || this.timeoutMs)),
    });
    if (response.status === 401 && retryAuth) {
      this.token = null;
      await this.login();
      return this.request(path, { method, body, expected, retryAuth: false, timeoutMs });
    }
    const payload = await this.#payload(response);
    if (!expected.includes(response.status)) {
      throw new Error(`${method} ${path} returned ${response.status}: ${truncate(JSON.stringify(payload))}`);
    }
    return payload;
  }

  async testProxy(proxyId, { timeoutMs = 75_000 } = {}) {
    const id = Number(proxyId);
    if (!Number.isInteger(id) || id <= 0) throw new TypeError("positive proxyId is required");
    return this.request(`/api/v1/proxies/${id}/test`, {
      method: "POST",
      timeoutMs,
    });
  }

  async refreshProxyUser(username) {
    const value = String(username || "").trim();
    if (!value) throw new TypeError("proxy username is required");
    return this.request("/api/v1/proxy-users/refresh", {
      method: "POST",
      body: { username: value },
    });
  }

  async #payload(response) {
    const raw = await response.text();
    if (!raw) return {};
    try {
      return JSON.parse(raw);
    } catch {
      return { raw };
    }
  }
}
