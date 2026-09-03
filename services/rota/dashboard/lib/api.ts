import {
  Proxy,
  ProxiesResponse,
  DashboardStats,
  ProxyCapacity,
  ChartResponse,
  LogEntry,
  LogsResponse,
  SystemMetrics,
  Settings,
  GeoIPStatus,
  AuthResponse,
  AddProxyRequest,
  UpdateProxyRequest,
  BulkTagRequest,
  BulkProxyRequest,
  BulkDeleteRequest,
  ProxyTestResult,
  ProxySource,
  CreateSourceRequest,
  UpdateSourceRequest,
  ProxyPool,
  PoolProxy,
  GeoSummaryItem,
  GeoCityItem,
  HCJob,
  CreatePoolRequest,
  ProxyUser,
  CreateProxyUserRequest,
  UpdateProxyUserRequest,
  PoolAlertRule,
  CreatePoolAlertRuleRequest,
} from "./types"

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001"

function getDefaultApiBaseUrl(): string {
  if (typeof window !== "undefined") {
    return window.location.origin
  }

  return API_BASE_URL
}

class ApiClient {
  private baseUrl: string
  private token: string | null = null

  constructor(baseUrl: string = getDefaultApiBaseUrl()) {
    this.baseUrl = baseUrl
    // Load token from localStorage if available
    if (typeof window !== "undefined") {
      this.token = localStorage.getItem("auth_token")
    }
  }

  setToken(token: string) {
    this.token = token
    if (typeof window !== "undefined") {
      localStorage.setItem("auth_token", token)
    }
  }

  clearToken() {
    this.token = null
    if (typeof window !== "undefined") {
      localStorage.removeItem("auth_token")
    }
  }

  private getHeaders(): HeadersInit {
    const headers: HeadersInit = {
      "Content-Type": "application/json",
    }
    if (this.token) {
      headers["Authorization"] = `Bearer ${this.token}`
    }
    return headers
  }

  private async request<T>(
    endpoint: string,
    options: RequestInit = {},
    redirectOnUnauthorized = true
  ): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`
    const response = await fetch(url, {
      ...options,
      headers: {
        ...this.getHeaders(),
        ...options.headers,
      },
    })

    if (response.status === 401) {
      this.clearToken()
      if (redirectOnUnauthorized && typeof window !== "undefined") {
        window.location.href = "/login?reason=session_expired"
        throw new Error("Session expired. Please log in again.")
      }
    }

    if (!response.ok) {
      const error = await response.json().catch(() => ({
        error: `HTTP ${response.status}: ${response.statusText}`,
      }))
      throw new Error(error.error || error.message || "Request failed")
    }

    // Handle 204 No Content
    if (response.status === 204) {
      return {} as T
    }

    return response.json()
  }

  private async requestBlob(endpoint: string): Promise<Blob> {
    const response = await fetch(`${this.baseUrl}${endpoint}`, {
      headers: this.getHeaders(),
    })

    if (response.status === 401) {
      this.clearToken()
      if (typeof window !== "undefined") {
        window.location.href = "/login?reason=session_expired"
      }
      throw new Error("Session expired. Please log in again.")
    }
    if (!response.ok) {
      throw new Error(`Export failed: HTTP ${response.status}`)
    }

    return response.blob()
  }

  // Authentication
  async login(username: string, password: string): Promise<AuthResponse> {
    const response = await this.request<AuthResponse>("/api/v1/auth/login", {
      method: "POST",
      body: JSON.stringify({ username, password }),
    }, false)
    this.setToken(response.token)
    return response
  }

  async getAdminInfo(options: { redirectOnUnauthorized?: boolean } = {}): Promise<{ username: string }> {
    return this.request(
      "/api/v1/auth/me",
      {},
      options.redirectOnUnauthorized ?? true,
    )
  }

  async changePassword(opts: {
    current_password: string
    new_password: string
    new_username?: string
  }): Promise<{ message: string; username: string; token: string }> {
    const res = await this.request<{ message: string; username: string; token: string }>(
      "/api/v1/auth/change-password",
      { method: "POST", body: JSON.stringify(opts) }
    )
    // Update stored token so session stays valid after username/password change
    this.setToken(res.token)
    return res
  }

  // Dashboard
  async getDashboardStats(): Promise<DashboardStats> {
    return this.request<DashboardStats>("/api/v1/dashboard/stats")
  }

  async getProxyCapacity(): Promise<ProxyCapacity> {
    return this.request<ProxyCapacity>("/api/v1/dashboard/proxy-capacity")
  }

  async getResponseTimeChart(interval: string = "4h"): Promise<ChartResponse> {
    return this.request<ChartResponse>(
      `/api/v1/dashboard/charts/response-time?interval=${interval}`
    )
  }

  async getSuccessRateChart(interval: string = "4h"): Promise<ChartResponse> {
    return this.request<ChartResponse>(
      `/api/v1/dashboard/charts/success-rate?interval=${interval}`
    )
  }

  // Proxies
  async getProxies(params?: {
    page?: number
    limit?: number
    search?: string
    status?: string
    protocol?: string
    tag?: string
    sort?: string
    order?: "asc" | "desc"
  }): Promise<ProxiesResponse> {
    const searchParams = new URLSearchParams()
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined) {
          searchParams.append(key, value.toString())
        }
      })
    }
    const query = searchParams.toString()
    return this.request<ProxiesResponse>(
      `/api/v1/proxies${query ? `?${query}` : ""}`
    )
  }

  async addProxy(proxy: AddProxyRequest): Promise<Proxy> {
    return this.request<Proxy>("/api/v1/proxies", {
      method: "POST",
      body: JSON.stringify(proxy),
    })
  }

  async updateProxy(id: number, proxy: UpdateProxyRequest): Promise<Proxy> {
    return this.request<Proxy>(`/api/v1/proxies/${id}`, {
      method: "PUT",
      body: JSON.stringify(proxy),
    })
  }

  async deleteProxy(id: number): Promise<void> {
    return this.request<void>(`/api/v1/proxies/${id}`, {
      method: "DELETE",
    })
  }

  async bulkAddProxies(request: BulkProxyRequest): Promise<{
    created: number
    failed: number
    results: Array<{ address: string; status: string; id?: string }>
  }> {
    return this.request("/api/v1/proxies/bulk", {
      method: "POST",
      body: JSON.stringify(request),
    })
  }

  async bulkDeleteProxies(request: BulkDeleteRequest): Promise<{
    deleted: number
    message: string
  }> {
    return this.request("/api/v1/proxies/bulk-delete", {
      method: "POST",
      body: JSON.stringify(request),
    })
  }

  async bulkTagProxies(request: BulkTagRequest): Promise<{
    updated: number
    message: string
  }> {
    return this.request("/api/v1/proxies/bulk-tags", {
      method: "POST",
      body: JSON.stringify(request),
    })
  }

  async archiveProxy(id: number, reason = "manual"): Promise<{ archived: number }> {
    return this.request(`/api/v1/proxies/${id}/archive`, {
      method: "POST",
      body: JSON.stringify({ reason }),
    })
  }

  async restoreProxy(id: number): Promise<{ restored: number; status: "idle" }> {
    return this.request(`/api/v1/proxies/${id}/restore`, { method: "POST" })
  }

  async bulkArchiveProxies(ids: number[], reason = "manual"): Promise<{ archived: number }> {
    return this.request("/api/v1/proxies/bulk-archive", {
      method: "POST",
      body: JSON.stringify({ ids, reason }),
    })
  }

  async bulkRestoreProxies(ids: number[]): Promise<{ restored: number; status: "idle" }> {
    return this.request("/api/v1/proxies/bulk-restore", {
      method: "POST",
      body: JSON.stringify({ ids }),
    })
  }

  async deleteAllProxies(): Promise<{ deleted: number }> {
    return this.request("/api/v1/proxies", { method: "DELETE" })
  }

  async testProxy(id: number): Promise<ProxyTestResult> {
    return this.request<ProxyTestResult>(`/api/v1/proxies/${id}/test`, {
      method: "POST",
    })
  }

  async exportProxies(format: "txt" | "json" | "csv" = "txt", status?: string): Promise<Blob> {
    const params = new URLSearchParams({ format })
    if (status) params.append("status", status)
    return this.requestBlob(`/api/v1/proxies/export?${params.toString()}`)
  }

  async reloadProxies(): Promise<{ status: string; message: string }> {
    return this.request("/api/v1/proxies/reload", {
      method: "POST",
    })
  }

  // Logs
  async getLogs(params?: {
    page?: number
    limit?: number
    level?: string
    search?: string
    source?: string
    start_time?: string
    end_time?: string
  }): Promise<LogsResponse> {
    const searchParams = new URLSearchParams()
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined) {
          searchParams.append(key, value.toString())
        }
      })
    }
    const query = searchParams.toString()
    return this.request<LogsResponse>(`/api/v1/logs${query ? `?${query}` : ""}`)
  }

  async exportLogs(format: "txt" | "json" = "txt", params?: {
    level?: string
    source?: string
    start_time?: string
    end_time?: string
  }): Promise<Blob> {
    const searchParams = new URLSearchParams({ format })
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined) {
          searchParams.append(key, value.toString())
        }
      })
    }

    return this.requestBlob(`/api/v1/logs/export?${searchParams.toString()}`)
  }

  // System Metrics
  async getSystemMetrics(): Promise<SystemMetrics> {
    return this.request<SystemMetrics>("/api/v1/metrics/system")
  }

  // Settings
  async getSettings(): Promise<Settings> {
    return this.request<Settings>("/api/v1/settings")
  }

  async updateSettings(settings: Partial<Settings>): Promise<{
    message: string
    config: Settings
  }> {
    return this.request("/api/v1/settings", {
      method: "PUT",
      body: JSON.stringify(settings),
    })
  }

  async resetSettings(): Promise<{
    message: string
    config: Settings
  }> {
    return this.request("/api/v1/settings/reset", {
      method: "POST",
    })
  }

  async getGeoIPStatus(): Promise<GeoIPStatus> {
    return this.request("/api/v1/settings/geoip/status")
  }

  async updateGeoIPDatabase(): Promise<{ message: string; status: GeoIPStatus }> {
    return this.request("/api/v1/settings/geoip/update-db", { method: "POST" })
  }

  // ── Proxy Sources ─────────────────────────────────────────────────────────
  async getSources(): Promise<{ sources: ProxySource[] }> {
    return this.request("/api/v1/sources")
  }

  async createSource(req: CreateSourceRequest): Promise<ProxySource> {
    return this.request("/api/v1/sources", { method: "POST", body: JSON.stringify(req) })
  }

  async updateSource(id: number, req: UpdateSourceRequest): Promise<ProxySource> {
    return this.request(`/api/v1/sources/${id}`, { method: "PUT", body: JSON.stringify(req) })
  }

  async deleteSource(id: number): Promise<void> {
    return this.request(`/api/v1/sources/${id}`, { method: "DELETE" })
  }

  async fetchSourceNow(id: number): Promise<{ source: ProxySource; imported: number }> {
    return this.request(`/api/v1/sources/${id}/fetch`, { method: "POST" })
  }

  async enrichGeo(): Promise<{ enriched: number }> {
    return this.request("/api/v1/sources/enrich-geo", { method: "POST" })
  }

  // ── Proxy Pools ───────────────────────────────────────────────────────────
  async getPools(): Promise<{ pools: ProxyPool[] }> {
    return this.request("/api/v1/pools")
  }

  async getPool(id: number): Promise<ProxyPool> {
    return this.request(`/api/v1/pools/${id}`)
  }

  async createPool(req: CreatePoolRequest): Promise<ProxyPool> {
    return this.request("/api/v1/pools", { method: "POST", body: JSON.stringify(req) })
  }

  async updatePool(id: number, req: Partial<CreatePoolRequest>): Promise<ProxyPool> {
    return this.request(`/api/v1/pools/${id}`, { method: "PUT", body: JSON.stringify(req) })
  }

  async deletePool(id: number): Promise<void> {
    return this.request(`/api/v1/pools/${id}`, { method: "DELETE" })
  }

  async getPoolProxies(id: number): Promise<{ proxies: PoolProxy[] }> {
    return this.request(`/api/v1/pools/${id}/proxies`)
  }

  async addPoolProxies(id: number, proxyIds: number[]): Promise<{ added: number }> {
    return this.request(`/api/v1/pools/${id}/proxies`, {
      method: "POST",
      body: JSON.stringify({ proxy_ids: proxyIds }),
    })
  }

  async removePoolProxies(id: number, proxyIds: number[]): Promise<{ removed: number }> {
    return this.request(`/api/v1/pools/${id}/proxies`, {
      method: "DELETE",
      body: JSON.stringify({ proxy_ids: proxyIds }),
    })
  }

  async syncPool(id: number): Promise<{ synced: number }> {
    return this.request(`/api/v1/pools/${id}/sync`, { method: "POST" })
  }

  async healthCheckPool(
    id: number,
    url?: string,
    workers?: number
  ): Promise<{ job_id: string; pool_id: number; status: string }> {
    return this.request(`/api/v1/pools/${id}/health-check`, {
      method: "POST",
      body: JSON.stringify({ url: url ?? "", workers: workers ?? 20 }),
    })
  }

  async getHealthCheckJob(poolId: number, jobId: string): Promise<HCJob> {
    return this.request(`/api/v1/pools/${poolId}/health-check/${jobId}`)
  }

  async getHealthCheckJobs(poolId: number): Promise<{ jobs: HCJob[] }> {
    return this.request(`/api/v1/pools/${poolId}/health-check/jobs`)
  }

  async getGeoSummary(): Promise<{ geo: GeoSummaryItem[] }> {
    return this.request("/api/v1/pools/geo-summary")
  }

  async getGeoByCountry(): Promise<{ geo: GeoSummaryItem[] }> {
    return this.request("/api/v1/pools/geo-countries")
  }

  async getGeoCities(countryCode: string): Promise<{ cities: GeoCityItem[] }> {
    return this.request(`/api/v1/pools/geo-cities/${countryCode}`)
  }

  // ── Proxy Users ───────────────────────────────────────────────────────────
  async getProxyUsers(): Promise<{ users: ProxyUser[] }> {
    return this.request("/api/v1/proxy-users")
  }

  async getProxyUser(id: number): Promise<ProxyUser> {
    return this.request(`/api/v1/proxy-users/${id}`)
  }

  async createProxyUser(req: CreateProxyUserRequest): Promise<ProxyUser> {
    return this.request("/api/v1/proxy-users", { method: "POST", body: JSON.stringify(req) })
  }

  async updateProxyUser(id: number, req: UpdateProxyUserRequest): Promise<ProxyUser> {
    return this.request(`/api/v1/proxy-users/${id}`, { method: "PUT", body: JSON.stringify(req) })
  }

  async deleteProxyUser(id: number): Promise<void> {
    return this.request(`/api/v1/proxy-users/${id}`, { method: "DELETE" })
  }

  // ── Pool Export ──────────────────────────────────────────────────────────
  async exportPool(poolId: number, format: "txt" | "csv" = "txt"): Promise<Blob> {
    return this.requestBlob(`/api/v1/pools/${poolId}/export?format=${format}`)
  }

  // ── Alert Rules ──────────────────────────────────────────────────────────
  async getAlertRules(poolId: number): Promise<PoolAlertRule[]> {
    const data = await this.request<{ rules: PoolAlertRule[] }>(`/api/v1/pools/${poolId}/alert-rules`)
    return data.rules
  }

  async createAlertRule(poolId: number, req: CreatePoolAlertRuleRequest): Promise<PoolAlertRule> {
    return this.request(`/api/v1/pools/${poolId}/alert-rules`, {
      method: "POST",
      body: JSON.stringify(req),
    })
  }

  async updateAlertRule(poolId: number, ruleId: number, req: CreatePoolAlertRuleRequest): Promise<PoolAlertRule> {
    return this.request(`/api/v1/pools/${poolId}/alert-rules/${ruleId}`, {
      method: "PUT",
      body: JSON.stringify(req),
    })
  }

  async deleteAlertRule(poolId: number, ruleId: number): Promise<void> {
    return this.request(`/api/v1/pools/${poolId}/alert-rules/${ruleId}`, { method: "DELETE" })
  }

  // ── ISP / Tag lists ──────────────────────────────────────────────────────
  async getISPList(q?: string): Promise<string[]> {
    const qs = q ? `?q=${encodeURIComponent(q)}` : ""
    const data = await this.request<{ isps: string[] }>(`/api/v1/pools/isp-list${qs}`)
    return data.isps
  }

  async getTagList(): Promise<string[]> {
    const data = await this.request<{ tags: string[] }>("/api/v1/pools/tag-list")
    return data.tags
  }

  private getWebSocketBaseUrl(): string {
    return this.baseUrl.replace(/^http/, "ws")
  }

  // The dashboard socket owns reconnects because each reconnect replaces the
  // underlying WebSocket. Callers only need a stable close handle.
  createDashboardWebSocket(onMessage: (data: DashboardStats) => void): { close: () => void } {
    let socket: WebSocket | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null
    let reconnectAttempts = 0
    let closed = false

    const scheduleReconnect = () => {
      if (closed || reconnectTimer) return
      const delay = Math.min(1000 * 2 ** Math.min(reconnectAttempts, 5), 30000)
      reconnectAttempts++
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null
        connect()
      }, delay)
    }

    const connect = () => {
      if (closed) return
      const wsUrl = this.getWebSocketBaseUrl()

      try {
        socket = new WebSocket(`${wsUrl}/ws/dashboard${this.token ? `?token=${this.token}` : ""}`)
      } catch {
        scheduleReconnect()
        return
      }

      socket.onopen = () => {
        reconnectAttempts = 0
      }
      socket.onmessage = (event) => {
        if (typeof event.data !== "string") return
        try {
          const message: unknown = JSON.parse(event.data)
          if (
            typeof message === "object" &&
            message !== null &&
            "type" in message &&
            message.type === "stats_update" &&
            "data" in message
          ) {
            onMessage(message.data as DashboardStats)
          }
        } catch {
          // Ignore malformed frames and keep the live stream running.
        }
      }
      socket.onclose = scheduleReconnect
    }

    connect()

    return {
      close: () => {
        closed = true
        if (reconnectTimer) clearTimeout(reconnectTimer)
        reconnectTimer = null
        socket?.close()
      },
    }
  }

  createLogsWebSocket(
    onMessage: (log: LogEntry) => void,
    levels?: string[],
    source?: string
  ): WebSocket {
    const wsUrl = this.getWebSocketBaseUrl()
    const ws = new WebSocket(`${wsUrl}/ws/logs${this.token ? `?token=${this.token}` : ""}`)

    ws.onopen = () => {
      if (levels && levels.length > 0 || source) {
        ws.send(JSON.stringify({
          action: "filter",
          levels: levels || [],
          source: source || ""
        }))
      }
    }

    ws.onmessage = (event) => {
      if (typeof event.data !== "string") return
      try {
        onMessage(JSON.parse(event.data) as LogEntry)
      } catch {
        // Ignore malformed frames and keep the stream running.
      }
    }

    return ws
  }
}

// Export singleton instance
export const api = new ApiClient()

// Export class for custom instances
export { ApiClient }
