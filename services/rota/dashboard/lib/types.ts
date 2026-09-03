// API Response Types

export type ProxyProtocol =
  | "http"
  | "https"
  | "socks4"
  | "socks4a"
  | "socks5"
  | "vless"
  | "vmess"
  | "trojan"
  | "shadowsocks"

export type SourceProtocol = "auto" | ProxyProtocol

export interface Proxy {
  id: number
  address: string
  protocol: ProxyProtocol
  status: "active" | "failed" | "idle" | "archived"
  requests: number
  success_rate: number
  avg_response_time: number
  last_check?: string
  failed_since?: string
  failure_episode_kind?: "hard_unreachable" | "soft_unreachable" | "youtube_unusable"
  next_health_check_at?: string
  last_health_check_at?: string
  last_health_success_at?: string
  base_health_status?: "not_run" | "passed" | "failed"
  youtube_health_status?: "not_run" | "passed" | "failed"
  archived_at?: string
  archive_reason?: string
  tags: string[]
  username?: string
  country_code?: string
  country_name?: string
  geo_updated_at?: string
  created_at: string
  updated_at: string
}

export interface ProxiesResponse {
  proxies: Proxy[]
  pagination: {
    page: number
    limit: number
    total: number
    total_pages: number
  }
}

export interface DashboardStats {
  active_proxies: number
  failed_proxies: number
  pending_proxies: number
  archived_proxies: number
  total_proxies: number
  total_requests: number
  avg_success_rate: number
  avg_response_time: number
  request_growth: number
  success_rate_growth: number
  response_time_delta: number
}

export interface ProxyRoleCapacity {
  identity_policy_id?: string
  identity_policy_version?: number
  identity_policy_hash?: string
  desired: number
  provisioned: number
  eligible: number
  assigned: number
  ready: number
  claimed: number
  reserve: number
}

export interface ProxyCapacity {
  workload_scope: string
  catalog_version: number
  catalog_digest: string
  ok: boolean
  active: number
  cooldown: number
  total: number
  archived: number
  running: number
  reserve: number
  minimum_reserve: number
  reserve_below_minimum: boolean
  roles: Record<string, ProxyRoleCapacity>
}

export interface ChartDataPoint {
  time: string
  value?: number
  success?: number
  failure?: number
}

export interface ChartResponse {
  data: ChartDataPoint[]
}

export interface LogEntry {
  id: string
  timestamp: string
  level: "info" | "warning" | "error" | "success"
  message: string
  details?: string
  metadata?: Record<string, unknown>
}

export interface LogsResponse {
  logs: LogEntry[]
  pagination: {
    page: number
    limit: number
    total: number
    total_pages: number
  }
}

export interface SystemMetrics {
  memory: {
    total: number
    used: number
    available: number
    percentage: number
  }
  cpu: {
    percentage: number
    cores: number
  }
  disk: {
    total: number
    used: number
    free: number
    percentage: number
  }
  runtime: {
    goroutines: number
    threads: number
    gc_pause_count: number
    mem_alloc: number
    mem_sys: number
  }
}

export interface Settings {
  authentication: {
    enabled: boolean
    username: string
    password: string
  }
  rotation: {
    method: "random" | "roundrobin" | "least_conn" | "time_based"
    time_based?: {
      interval: number
    }
    remove_unhealthy: boolean
    fallback: boolean
    fallback_max_retries: number
    follow_redirect: boolean
    timeout: number
    retries: number
    allowed_protocols: string[]
    max_response_time: number
    min_success_rate: number
  }
  rate_limit: {
    enabled: boolean
    interval: number
    max_requests: number
  }
  healthcheck: {
    timeout: number
    workers: number
    base_url: string
    base_status: number
    url: string
    status: number
    headers: string[]
    strict_tls: boolean
  }
  proxy_lifecycle: {
    auto_archive_enabled: boolean
    active_recheck_minutes: number
    hard_unreachable_after_hours: number
    soft_unreachable_after_hours: number
    youtube_unusable_after_hours: number
  }
  log_retention: {
    enabled: boolean
    retention_days: number
    compression_after_days: number
    cleanup_interval_hours: number
  }
  geoip: {
    provider: "local" | "maxmind"
    maxmind_license_key: string
    maxmind_db_path: string
    maxmind_url: string
    auto_update: boolean
    update_interval_hours: number
    last_updated_at?: string
  }
}

export interface GeoIPStatus {
  provider: "local" | "maxmind"
  configured: boolean
  database_loaded: boolean
  database_path: string
  active_database_path?: string
  database_type?: string
  database_build_time?: string
  source?: "local" | "managed"
  license_configured: boolean
  auto_update: boolean
  update_interval_hours: number
  last_updated_at?: string
  updating: boolean
  last_error?: string
}

export interface AuthResponse {
  token: string
  user: {
    username: string
  }
}

export interface ApiError {
  error: string
  details?: string
}

// Request Types
export interface AddProxyRequest {
  address: string
  protocol: ProxyProtocol
  username?: string
  password?: string
  tags?: string[]
}

export interface UpdateProxyRequest {
  address?: string
  protocol?: ProxyProtocol
  username?: string
  password?: string
  tags?: string[]
}

export interface BulkTagRequest {
  ids: number[]
  add?: string[]
  remove?: string[]
}

export interface BulkProxyRequest {
  proxies: AddProxyRequest[]
}

export interface BulkDeleteRequest {
  ids: number[]
}

export interface ProxyTestResult {
  id: number
  address: string
  status: "active" | "failed" | "idle"
  response_time?: number
  error?: string
  tested_at: string
  duration?: number // Alias for response_time for better clarity
}

// ── Proxy Sources ──────────────────────────────────────────────────────────
export interface ProxySource {
  id: number
  name: string
  url: string
  protocol: SourceProtocol
  enabled: boolean
  interval_minutes: number
  last_fetched_at?: string
  last_count: number        // newly imported on last fetch
  last_total: number        // total lines returned on last fetch
  last_supported: number    // valid supported lines on last fetch
  last_skipped: number      // invalid or unsupported lines on last fetch
  active_count: number      // source members currently in active lifecycle state
  last_error?: string
  cleanup_enabled: boolean
  cleanup_days: number
  default_tags: string[]
  created_at: string
  updated_at: string
}

export interface CreateSourceRequest {
  name: string
  url: string
  protocol: SourceProtocol
  enabled: boolean
  interval_minutes: number
  default_tags: string[]
}

export interface UpdateSourceRequest {
  name?: string
  url?: string
  protocol?: SourceProtocol
  enabled?: boolean
  interval_minutes?: number
  default_tags?: string[]
}

// ── Proxy Pools ────────────────────────────────────────────────────────────
export interface ProxyPool {
  id: number
  name: string
  description: string
  country_code?: string
  region_name?: string
  city_name?: string
  rotation_method: "roundrobin" | "random" | "stick"
  stick_count: number
  health_check_url: string
  health_check_cron: string
  health_check_enabled: boolean
  auto_sync: boolean
  sync_mode: "auto" | "manual"
  enabled: boolean
  total_proxies: number
  active_proxies: number
  failed_proxies: number
  geo_filters?: GeoFilter[]
  isp_filters?: string[]
  tag_filters?: string[]
  created_at: string
  updated_at: string
}

export interface PoolAlertRule {
  id: number
  pool_id: number
  enabled: boolean
  min_active_proxies: number
  webhook_url: string
  webhook_method: "POST" | "GET"
  last_fired_at?: string
  cooldown_minutes: number
  created_at: string
  updated_at: string
}

export interface CreatePoolAlertRuleRequest {
  enabled: boolean
  min_active_proxies: number
  webhook_url: string
  webhook_method?: "POST" | "GET"
  cooldown_minutes?: number
}

export interface PoolProxy {
  proxy_id: number
  address: string
  protocol: string
  status: string
  country_code?: string
  country_name?: string
  region_name?: string
  city_name?: string
  isp?: string
  requests: number
  success_rate: number
  avg_response_time: number
  last_check?: string
  added_at: string
}

export interface GeoSummaryItem {
  country_code: string
  country_name: string
  region_name: string
  city_name: string
  total: number
  active: number
}

export interface GeoCityItem {
  city_name: string
  region_name: string
  total: number
  active: number
}

export interface GeoFilter {
  country_code: string
  city_name?: string
}

export interface PoolHealthCheckResult {
  pool_id: number
  pool_name: string
  checked: number
  active: number
  failed: number
  results: ProxyTestResult[]
  started_at: string
  finished_at: string
}

export type HCJobStatus = "pending" | "running" | "done" | "failed"

export interface HCJob {
  id: string
  pool_id: number
  pool_name: string
  status: HCJobStatus
  progress: number
  total: number
  active: number
  failed: number
  check_url: string
  workers: number
  error?: string
  started_at: string
  updated_at: string
  finished_at?: string
  results?: ProxyTestResult[]
}

// ── Proxy Users ────────────────────────────────────────────────────────────
export interface ProxyUser {
  id: number
  username: string
  enabled: boolean
  main_pool_id?: number
  main_pool_name?: string
  fallback_pool_ids: number[]
  max_retries: number
  requests_per_minute: number
  created_at: string
  updated_at: string
}

export interface CreateProxyUserRequest {
  username: string
  password: string
  enabled: boolean
  main_pool_id?: number | null
  fallback_pool_ids: number[]
  max_retries: number
  requests_per_minute?: number
}

export interface UpdateProxyUserRequest {
  password?: string
  enabled?: boolean
  main_pool_id?: number | null
  fallback_pool_ids?: number[]
  max_retries?: number
  requests_per_minute?: number
}

export interface CreatePoolRequest {
  name: string
  description?: string
  country_code?: string
  region_name?: string
  city_name?: string
  geo_filters?: GeoFilter[]
  isp_filters?: string[]
  tag_filters?: string[]
  rotation_method: "roundrobin" | "random" | "stick"
  stick_count: number
  health_check_url?: string
  health_check_cron?: string
  health_check_enabled: boolean
  auto_sync: boolean
  sync_mode?: "auto" | "manual"
  enabled: boolean
}

export interface ProxyWithTags {
  id: number
  address: string
  protocol: string
  status: string
  tags: string[]
  country_code?: string
  country_name?: string
  city_name?: string
  isp?: string
}
