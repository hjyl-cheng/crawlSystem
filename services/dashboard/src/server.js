import express from "express";
import morgan from "morgan";
import pg from "pg";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { Queue } from "bullmq";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import {
  crawlerContentExportSelect,
  crawlerContentExportTables,
  crawlerContentFilteredTables,
} from "./exportPolicy.js";
import {
  CLOCK_KINDS,
  CLOCK_LABELS_TEXT,
  clockMaskTotal,
  dailyClockExecutionProgress,
  dailyClockPlanPresentation,
  normalizeClockFilter,
} from "./clockPresentation.js";
import {
  PUBLICATION_COMPARISON_MAX_CHANNELS,
  comparePublicationChannels,
  normalizePublicationComparisonChannelIds,
} from "./publicationComparison.js";
import {
  MIGRATION_FINALIZED_SQL,
} from "./migrationCompletion.js";
import {
  assertCrawlerDashboardIdentity,
  assertMigrationSourceIdentity,
  filterAndPageMigrationCandidates,
  mergeMigrationCandidates,
  migrationReadModelStats,
} from "./migrationTopology.js";
import { loadChannelCurrentContent } from "./channelCurrentContent.js";

const { Pool } = pg;

function optionalEnvironmentValue(name) {
  const direct = String(process.env[name] ?? "").trim();
  const filePath = String(process.env[`${name}_FILE`] ?? "").trim();
  if (direct && filePath) throw new Error(`${name} and ${name}_FILE cannot both be set`);
  if (!filePath) return direct || null;
  const value = readFileSync(filePath, "utf8").trim();
  if (!value) throw new Error(`${name}_FILE is empty`);
  return value;
}

const port = Number(process.env.PORT || 3000);
const basePath = process.env.BULL_BOARD_BASE_PATH || "/queues";
const bullmqQueuesUrl = process.env.BULLMQ_QUEUES_URL || "https://qybullmq.example.test/queues";
const crawlerApiUrl = String(process.env.CRAWLER_API_URL || "http://127.0.0.1:3000").replace(/\/+$/, "");
const rotaProxyControlUrl = String(
  process.env.ROTA_PROXY_CONTROL_URL
  || process.env.PROXY_RECONCILER_URL
  || "http://rota-core:8001/api/v1/proxy-control",
).replace(/\/+$/, "");
const minioConsoleUrl = process.env.MINIO_CONSOLE_URL || "https://qyminio.example.test";
const proxyDashboardUrl = process.env.PROXY_DASHBOARD_URL || "https://qyproxy.example.test/dashboard";
const defaultChannelContentLimit = Number(process.env.YOUTUBE_CHANNEL_CONTENT_LIMIT || 30);
const defaultContentMaxAgeDays = Number(process.env.YOUTUBE_CONTENT_MAX_AGE_DAYS || 90);
const defaultMinSubscriberCount = Number(process.env.MIN_SUBSCRIBER_COUNT || 1000);
const defaultDiscoverStopMinQualifiedRatio = Number(process.env.DISCOVER_STOP_MIN_QUALIFIED_RATIO || (1 / 3));
const defaultYoutubeDataApiKeys = parseApiKeys(process.env.YOUTUBE_DATA_API_KEYS || process.env.YOUTUBE_DATA_API_KEY || "");
const defaultAgentBaseUrl = String(process.env.AGENT_BASE_URL || process.env.OPENAI_BASE_URL || "https://api.openai.com/v1").trim();
const defaultAgentApiKeys = parseApiKeys(process.env.AGENT_API_KEYS || process.env.AGENT_API_KEY || process.env.OPENAI_API_KEY || "");
const querySchedulerStatuses = new Set(["stopped", "running", "paused", "finishing", "repairing"]);
const defaultDiscoverBacklogLimit = 3;
const defaultQueryQualityMinScore = Number(process.env.QUERY_QUALITY_MIN_SCORE || 60);
const queryQualityTaskChunkSize = 3;
const agentModelOptions = [
  ["rules-agent-v1", "rules-agent-v1"],
  ["gpt-4.1", "gpt-4.1"],
  ["gpt-4.1-mini", "gpt-4.1-mini"],
  ["gpt-4o", "gpt-4o"],
  ["gpt-4o-mini", "gpt-4o-mini"],
  ["deepseek-chat", "deepseek-chat"],
  ["deepseek-reasoner", "deepseek-reasoner"],
  ["gemini-2.5-pro", "gemini-2.5-pro"],
  ["gemini-2.5-flash", "gemini-2.5-flash"],
  ["gemini-3.5-flash", "gemini-3.5-flash"],
  ["grok-4.3", "grok-4.3"],
  ["qwen-max", "qwen-max"],
  ["qwen-plus", "qwen-plus"],
];

const queueNames = [
  "youtube-query-quality",
  "youtube-discover-page",
  "youtube-channel-crawl",
  "youtube-content-detail",
  "youtube-data-api-batch",
  "youtube-agent-batch",
  "youtube-finalize",
];

const redisOptions = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT || 6379),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || [
    "postgres://",
    encodeURIComponent(process.env.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(process.env.POSTGRES_PASSWORD || "bullmq"),
    "@",
    process.env.POSTGRES_HOST || "127.0.0.1",
    ":",
    process.env.POSTGRES_PORT || "5432",
    "/",
    process.env.POSTGRES_DB || "bullmq_crawler",
  ].join(""),
  max: Number(process.env.POSTGRES_POOL_MAX || 8),
});

const expectedCrawlerDatabase = String(process.env.EXPECTED_CRAWLER_DATABASE || "").trim();
const forbiddenCrawlerDatabase = String(
  process.env.FORBIDDEN_CRAWLER_DATABASE || "bullmq_crawler_migration",
).trim();
const migrationSourceId = String(process.env.MIGRATION_SOURCE_ID || "").trim();
const expectedMigrationDatabase = String(
  process.env.EXPECTED_MIGRATION_DATABASE || process.env.MIGRATION_POSTGRES_DB || "",
).trim();
const expectedMigrationDatabaseOid = String(
  process.env.EXPECTED_MIGRATION_DATABASE_OID || "",
).trim();
const expectedMigrationDatabaseUser = String(
  process.env.EXPECTED_MIGRATION_DATABASE_USER || process.env.MIGRATION_POSTGRES_USER || "",
).trim();

const migrationDatabaseUrl = optionalEnvironmentValue("MIGRATION_DATABASE_URL");
const migrationDatabaseConfigured = Boolean(
  migrationDatabaseUrl || process.env.MIGRATION_POSTGRES_HOST,
);
const migrationPool = migrationDatabaseConfigured
  ? new Pool({
      connectionString: migrationDatabaseUrl || [
        "postgres://",
        encodeURIComponent(process.env.MIGRATION_POSTGRES_USER || ""),
        ":",
        encodeURIComponent(process.env.MIGRATION_POSTGRES_PASSWORD || ""),
        "@",
        process.env.MIGRATION_POSTGRES_HOST,
        ":",
        process.env.MIGRATION_POSTGRES_PORT || "5432",
        "/",
        process.env.MIGRATION_POSTGRES_DB || "",
      ].join(""),
      max: Number(process.env.MIGRATION_POSTGRES_POOL_MAX || 4),
      application_name: "newcrawler-dashboard-migration-readonly",
      options: "-c timezone=UTC",
    })
  : null;

const businessAuditDatabaseUrl = optionalEnvironmentValue(
  "BUSINESS_PUBLICATION_AUDIT_DATABASE_URL",
);
const businessAuditPool = businessAuditDatabaseUrl
  ? new Pool({
      connectionString: businessAuditDatabaseUrl,
      max: Number(process.env.BUSINESS_PUBLICATION_AUDIT_POOL_MAX || 3),
      options: "-c timezone=UTC",
    })
  : null;
const expectedBusinessAuditDatabase = String(
  process.env.EXPECTED_BUSINESS_DATABASE || "",
).trim();
const expectedBusinessAuditRole = String(
  process.env.EXPECTED_BUSINESS_AUDITOR_ROLE || "business_publication_auditor",
).trim();

const queues = Object.fromEntries(
  queueNames.map((name) => [
    name,
    new Queue(name, {
      connection: redisOptions,
      defaultJobOptions: {
        attempts: 3,
        backoff: { type: "exponential", delay: 5000 },
        removeOnComplete: { age: 86400, count: 10000 },
        removeOnFail: { age: 604800, count: 20000 },
      },
    }),
  ]),
);
const pipelineExecutionQueues = Object.entries(queues)
  .filter(([name]) => name !== "youtube-query-quality")
  .map(([, queue]) => queue);

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath(basePath);
createBullBoard({
  queues: queueNames.map((name) => new BullMQAdapter(queues[name])),
  serverAdapter,
});

let schemaReady;

function h(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function intValue(value, def, min = 0, max = 1_000_000) {
  const n = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function numberValue(value, def, min = 0, max = 1_000_000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function jsonInt(value, def, min = 0, max = 1_000_000) {
  const n = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function jsonNumber(value, def, min = 0, max = 1_000_000) {
  const n = Number(value);
  if (!Number.isFinite(n)) return def;
  return Math.max(min, Math.min(max, n));
}

function parseApiKeys(value) {
  const rawKeys = Array.isArray(value)
    ? value
    : String(value ?? "").split(/[\s,;]+/);
  return Array.from(new Set(rawKeys
    .map((key) => String(key ?? "").trim())
    .filter(Boolean)));
}

function parsePositiveIds(value, maxItems = 500) {
  const values = Array.isArray(value) ? value : [value];
  const ids = [];
  const seen = new Set();
  for (const item of values) {
    const text = String(item ?? "").trim();
    if (!/^\d+$/.test(text)) continue;
    const id = Number(text);
    if (!Number.isSafeInteger(id) || id <= 0 || seen.has(id)) continue;
    seen.add(id);
    ids.push(id);
    if (ids.length >= maxItems) break;
  }
  return ids;
}

function keyTail(key) {
  const text = String(key ?? "").trim();
  return text ? text.slice(-4) : "";
}

function statusClass(status) {
  if (["active", "done", "success", "ready_auto", "accepted", "scored", "scored_discover"].includes(status)) return "good";
  if (["queued", "running", "finishing", "repairing", "pending", "pending_detail", "pending_api", "pending_agent", "ready_partial", "api_pending", "paused", "unscored", "scored_partial", "scored_fallback"].includes(status)) return "warn";
  if (["failed", "error", "rejected", "removed"].includes(status)) return "bad";
  return "muted-pill";
}

const DORMANT_REASON = "no_published_content_within_90_days";

function isDormantChannel(channel) {
  return channel?.status === "dormant"
    || (channel?.status === "rejected" && channel?.reject_reason === DORMANT_REASON);
}

function addChannelStatusFilter(where, args, status, alias = "c") {
  if (!status || status === "all") return;
  if (status === "tracked") {
    where.push(`(
      ${alias}.status IN ('active','dormant')
      OR (${alias}.status='rejected' AND ${alias}.reject_reason='${DORMANT_REASON}')
    )`);
    return;
  }
  if (status === "dormant") {
    where.push(`(
      ${alias}.status='dormant'
      OR (${alias}.status='rejected' AND ${alias}.reject_reason='${DORMANT_REASON}')
    )`);
    return;
  }
  args.push(status);
  where.push(`${alias}.status = $${args.length}`);
}

function qualityStatusLabel(status) {
  const text = String(status || "unscored");
  return text.startsWith("scored") ? "scored" : text;
}

function queryScheduleState(term, scheduler) {
  const score = Number(term?.quality_score);
  const minScore = Number(scheduler?.query_quality_min_score || 0);
  const qualityStatus = String(term?.quality_status || "");
  if (!Number.isFinite(score) || ["unscored", "failed"].includes(qualityStatus)) {
    return { label: "未评分", className: "bad", title: qualityStatus || "unscored" };
  }
  if (score < minScore) {
    return { label: "分数不足", className: "warn", title: `quality_score ${score.toFixed(2)} < ${minScore}` };
  }
  const next = term?.next_crawl_at ? new Date(term.next_crawl_at) : null;
  if (next && Number.isFinite(next.getTime()) && next.getTime() > Date.now()) {
    return { label: "待到期", className: "muted-pill", title: `next_crawl_at ${timeText(term.next_crawl_at)} 北京时间` };
  }
  return { label: "可调度", className: "good", title: `quality_score ${score.toFixed(2)} >= ${minScore}` };
}

function fmtInt(value) {
  const n = Number(value ?? 0);
  return Number.isFinite(n) ? new Intl.NumberFormat("en-US").format(n) : "0";
}

function fmtBytes(value) {
  const bytes = Number(value ?? 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / (1024 ** index)).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
}

function fmtDurationMs(value) {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return "-";
  return milliseconds < 1000 ? `${Math.round(milliseconds)} ms` : `${(milliseconds / 1000).toFixed(1)} s`;
}

function fmtResponseDurationMs(value) {
  if (value == null || value === "") return "-";
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return "-";
  if (milliseconds < 60000) return fmtDurationMs(milliseconds);
  const seconds = Math.round(milliseconds / 1000);
  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function countStatusNote(status) {
  const text = String(status ?? "");
  if (!text || text === "exact" || text === "unresolved") return "";
  return `<div class="note">${h(text)}</div>`;
}

function countValue(value) {
  return value == null ? "-" : fmtInt(value);
}

function aboutMetricCell(value, status) {
  const labels = {
    exact: "精确",
    estimated: "估算",
    unavailable: "未获取",
    unresolved: "未解析",
  };
  const normalized = String(status || "unavailable");
  return `<strong class="mono">${h(countValue(value))}</strong><div class="note">${h(labels[normalized] || normalized)}</div>`;
}

function aboutObservationResult(item) {
  const labels = { complete: "完整", partial: "部分", failed: "失败" };
  const className = item.outcome === "complete"
    ? "good"
    : item.outcome === "partial" ? "warn" : "bad";
  return `<span class="pill ${className}">${h(labels[item.outcome] || item.outcome)}</span><div class="note mono">${h(item.outcome_reason_code || "-")}</div>`;
}

function aboutTriggerText(value) {
  const labels = {
    initial_full: "首次抓取",
    clock_due: "Clock 到期",
    retry: "重试",
    manual: "手动",
    repair: "修复",
    migration_baseline: "迁移基线",
  };
  const normalized = String(value || "");
  return labels[normalized] || normalized || "-";
}

function commentCountValue(item) {
  if (item?.comments_disabled) return '<span class="pill muted-pill">评论关闭</span>';
  return countValue(item?.comment_count);
}

function liveTimeNote(item) {
  const parts = [];
  if (item?.live_scheduled_at) parts.push(`预约 ${timeText(item.live_scheduled_at)}`);
  if (item?.live_started_at) parts.push(`开始 ${timeText(item.live_started_at)}`);
  if (item?.live_ended_at) parts.push(`结束 ${timeText(item.live_ended_at)}`);
  return parts.length > 0 ? `<div class="note">${h(`${parts.join(" · ")} · 北京时间`)}</div>` : "";
}

function accessBadge(item) {
  const status = String(item?.access_status ?? "unknown");
  if (item?.is_members_only || status === "members_only") {
    return `<span class="pill warn">会员/付费</span><div class="note">${h(item?.access_status_source || "tab_badge")}</div>`;
  }
  if (status === "public") return `<span class="pill good">公开</span>`;
  if (status && status !== "unknown") return `<span class="pill bad">${h(status)}</span>`;
  return `<span class="muted">-</span>`;
}

function completenessSummary(channel) {
  if (channel?.status === "removed") {
    const labels = {
      community_guidelines: "违反社区准则，平台已移除",
      channel_not_found: "频道不存在或频道主已删除",
      owner_closed: "频道主已关闭账号",
      copyright_termination: "版权侵权导致账号终止",
      account_terminated: "账号已被 YouTube 终止",
    };
    const reason = String(channel.removed_reason || channel.reject_reason || "channel_removed");
    return `<span class="pill bad">已封禁/移除</span><div class="note">${h(labels[reason] || reason)}</div>`;
  }
  if (isDormantChannel(channel)) {
    return '<span class="pill muted-pill">休眠</span><div class="note">90 天内无已发布内容，仅安排 Video 复查</div>';
  }
  if (channel?.status === "rejected") {
    const labels = {
      subscriber_count_below_minimum: "订阅数低于门槛",
      subscriber_count_unknown: "订阅数检查失败",
      channel_unavailable: "频道不可用",
      manual_excluded_news_channel: "人工排除",
    };
    const reason = String(channel.reject_reason || "rejected");
    return `<span class="pill bad">不符合条件</span><div class="note mono">${h(labels[reason] || reason)}</div>`;
  }
  const quality = channel?.quality_json && typeof channel.quality_json === "object" ? channel.quality_json : {};
  const parts = [];
  for (const field of quality.missing_channel_fields || []) parts.push(`${field}(1)`);
  for (const [field, count] of Object.entries(quality.missing_content_fields || {})) {
    if (Number(count) > 0) parts.push(`${field}(${count})`);
  }
  for (const field of quality.missing_agent_fields || []) parts.push(`agent.${field}(1)`);
  if (Number(quality.detail_open_count) > 0) parts.push(`Detail(${quality.detail_open_count})`);
  if (Number(quality.api_open_count) > 0) parts.push(`API(${quality.api_open_count})`);
  if (Number(quality.unavailable_candidate_count) > 0) parts.push(`不可用(${quality.unavailable_candidate_count})`);
  if (parts.length === 0 && (quality.data_complete || quality.publish_ready)) return `<span class="pill good">完整</span>`;
  if (parts.length === 0) {
    const runStatus = String(channel?.run_status || "");
    const detailStatus = String(channel?.run_detail_status || "");
    if (["queued", "running"].includes(runStatus)) return `<span class="pill warn">频道抓取中</span>`;
    if (runStatus === "waiting_detail" || ["queued", "running", "api_pending"].includes(detailStatus)) {
      return `<span class="pill warn">内容补全中</span>`;
    }
    if (runStatus === "waiting_agent" || ["pending", "queued", "running"].includes(String(channel?.agent_status || ""))) {
      return `<span class="pill warn">Agent 处理中</span>`;
    }
    return `<span class="pill warn">待 Finalize</span>`;
  }
  return `<span class="pill warn">缺 ${fmtInt(parts.length)} 类</span><div class="note mono">${h(parts.slice(0, 4).join(" · "))}${parts.length > 4 ? " …" : ""}</div>`;
}

function jsonText(value) {
  return JSON.stringify(value ?? null, null, 2);
}

function scriptJson(value) {
  return JSON.stringify(value ?? null)
    .replaceAll("<", "\\u003c")
    .replaceAll(">", "\\u003e")
    .replaceAll("&", "\\u0026")
    .replaceAll("\u2028", "\\u2028")
    .replaceAll("\u2029", "\\u2029");
}

function csvCell(value) {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function quoteIdent(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function downloadTimestamp() {
  return new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}

async function writeJsonLine(res, value) {
  if (!res.write(`${JSON.stringify(value)}\n`)) {
    await new Promise((resolve) => res.once("drain", resolve));
  }
}

const dashboardTimeFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Asia/Shanghai",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

function timeText(value) {
  if (!value) return "-";
  const ts = new Date(value);
  if (Number.isNaN(ts.getTime())) return "-";
  const parts = Object.fromEntries(
    dashboardTimeFormatter.formatToParts(ts)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`;
}

function utcDayText(value) {
  if (!value) return "-";
  const text = String(value);
  const isoDay = text.match(/^\d{4}-\d{2}-\d{2}/)?.[0];
  if (isoDay) return isoDay;
  const ts = new Date(value);
  if (Number.isNaN(ts.getTime())) return "-";
  return [
    ts.getUTCFullYear(),
    String(ts.getUTCMonth() + 1).padStart(2, "0"),
    String(ts.getUTCDate()).padStart(2, "0"),
  ].join("-");
}

function dailyClockDurationText(value, { estimate = false } = {}) {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return estimate ? "计算中" : "-";
  const totalMinutes = estimate
    ? Math.max(1, Math.ceil(milliseconds / 60_000))
    : Math.floor(milliseconds / 60_000);
  if (!estimate && totalMinutes === 0) {
    return `${Math.max(0, Math.round(milliseconds / 1000))}秒`;
  }
  const days = Math.floor(totalMinutes / (24 * 60));
  const hours = Math.floor((totalMinutes % (24 * 60)) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `${days}天${hours > 0 ? `${hours}小时` : ""}`;
  if (hours > 0) return `${hours}小时${minutes > 0 ? `${minutes}分钟` : ""}`;
  return `${totalMinutes}分钟`;
}

function timeOnlyText(value) {
  const text = timeText(value);
  return text === "-" ? text : text.slice(11);
}

function publishedTimeText(item) {
  if (!item?.published_at) return "-";
  const value = timeText(item.published_at);
  if (value === "-") return value;
  return item.published_at_precision === "date_only"
    ? `${value.slice(0, 10)} 北京时间`
    : `${value} 北京时间`;
}

function publishedPrecisionText(item) {
  const precision = item?.published_at_precision || "unknown";
  const source = item?.published_at_source || "-";
  return precision === "date_only"
    ? `date_only（时分秒未知） · ${source}`
    : `${precision} · ${source}`;
}

function videoTextMetadata(item) {
  const description = typeof item?.description === "string"
    ? item.description.replace(/\s+/g, " ").trim()
    : "";
  const preview = description.length > 180 ? `${description.slice(0, 180)}...` : description;
  const status = item?.description_status || "unresolved";
  const hashtags = Array.isArray(item?.hashtags) ? item.hashtags : [];
  return `<div>${h(preview || (status === "empty" ? "空描述" : "-"))}</div><div class="note">${h(status)} · ${h(item?.description_source || "-")}</div>${hashtags.length > 0 ? `<div class="note">${hashtags.map((tag) => h(tag)).join(" · ")}</div>` : ""}`;
}

function safeJobId(...parts) {
  const id = parts
    .flat()
    .map((part) => String(part ?? "").trim())
    .filter(Boolean)
    .map((part) => part.replace(/[^a-zA-Z0-9_-]+/g, "_").replace(/^_+|_+$/g, ""))
    .filter(Boolean)
    .join("__");
  return (id || "job").slice(0, 240);
}

async function db(sql, params = []) {
  await ensureSchema();
  return pool.query(sql, params);
}

async function migrationRead(sql, params = []) {
  if (!migrationPool) throw new Error("迁移数据库未配置");
  const client = await migrationPool.connect();
  try {
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    await client.query("SET LOCAL statement_timeout=10000");
    const identityResult = await client.query(
      `SELECT current_database() AS database_name,
              database_state.oid::text AS database_oid,
              current_user AS database_user,
              current_setting('default_transaction_read_only')
                AS default_transaction_read_only,
              current_setting('transaction_read_only') AS transaction_read_only,
              to_regclass('crawler.channel_candidates') IS NOT NULL AS candidates_ready,
              to_regclass('crawler.channels') IS NOT NULL AS channels_ready,
              (
                has_table_privilege(current_user,'crawler.channel_candidates','INSERT')
                OR has_any_column_privilege(current_user,'crawler.channel_candidates','INSERT')
                OR has_table_privilege(current_user,'crawler.channel_candidates','UPDATE')
                OR has_any_column_privilege(current_user,'crawler.channel_candidates','UPDATE')
                OR has_table_privilege(current_user,'crawler.channel_candidates','DELETE')
                OR has_table_privilege(current_user,'crawler.channel_candidates','TRUNCATE')
              ) AS candidate_write,
              (
                has_table_privilege(current_user,'crawler.channels','INSERT')
                OR has_any_column_privilege(current_user,'crawler.channels','INSERT')
                OR has_table_privilege(current_user,'crawler.channels','UPDATE')
                OR has_any_column_privilege(current_user,'crawler.channels','UPDATE')
                OR has_table_privilege(current_user,'crawler.channels','DELETE')
                OR has_table_privilege(current_user,'crawler.channels','TRUNCATE')
              ) AS channel_write
       FROM pg_database database_state
       WHERE database_state.datname=current_database()`,
    );
    assertMigrationSourceIdentity(identityResult.rows[0], {
      expectedDatabase: expectedMigrationDatabase,
      expectedDatabaseOid: expectedMigrationDatabaseOid,
      expectedUser: expectedMigrationDatabaseUser,
      targetDatabase: expectedCrawlerDatabase,
    });
    const result = await client.query(sql, params);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function publicationComparisonData(channelIdsValue) {
  if (!businessAuditPool) throw new Error("业务数据库只读对账连接未配置");
  const channelIds = normalizePublicationComparisonChannelIds(channelIdsValue);
  const crawlerClient = await pool.connect();
  let businessClient;
  try {
    businessClient = await businessAuditPool.connect();
    await crawlerClient.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    await businessClient.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    await crawlerClient.query("SET LOCAL statement_timeout='20s'");
    await businessClient.query("SET LOCAL statement_timeout='20s'");
    const report = await comparePublicationChannels({
      crawlerClient,
      businessClient,
      channelIds,
      destination: String(process.env.PUBLICATION_DESTINATION || "business").trim(),
      expectedBusinessDatabase: expectedBusinessAuditDatabase,
      expectedBusinessRole: expectedBusinessAuditRole,
    });
    await crawlerClient.query("COMMIT");
    await businessClient.query("COMMIT");
    return report;
  } catch (error) {
    await Promise.all([
      crawlerClient.query("ROLLBACK").catch(() => {}),
      businessClient?.query("ROLLBACK").catch(() => {}),
    ]);
    throw error;
  } finally {
    crawlerClient.release();
    businessClient?.release();
  }
}

async function ensureSchema() {
  if (schemaReady) return schemaReady;
  schemaReady = (async () => {
    const client = await pool.connect();
    try {
      const identityResult = await client.query(
        `SELECT current_database() AS database_name,current_user AS database_user,
                current_setting('transaction_read_only') AS transaction_read_only,
                identity.database_kind AS identity_kind,
                identity.database_name AS identity_database
         FROM crawler.database_identity identity
         WHERE identity.singleton=true`,
      );
      assertCrawlerDashboardIdentity(identityResult.rows[0], {
        expectedDatabase: expectedCrawlerDatabase,
        forbiddenDatabase: forbiddenCrawlerDatabase,
      });
      if (String(process.env.SKIP_SCHEMA_MIGRATION || "").toLowerCase() === "true") return;
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock(781137208)");
      await client.query(`
        CREATE SCHEMA IF NOT EXISTS crawler;

        CREATE TABLE IF NOT EXISTS crawler.query_sets (
          query_set_id BIGSERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          description TEXT,
          status TEXT NOT NULL DEFAULT 'active'
            CHECK (status IN ('active', 'paused', 'archived')),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_query_sets_name
        ON crawler.query_sets (lower(name));

        CREATE TABLE IF NOT EXISTS crawler.query_terms (
          query_id BIGSERIAL PRIMARY KEY,
          query_set_id BIGINT REFERENCES crawler.query_sets(query_set_id) ON DELETE SET NULL,
          query_text TEXT NOT NULL,
          language TEXT,
          country TEXT,
          category TEXT,
          status TEXT NOT NULL DEFAULT 'active'
            CHECK (status IN ('active', 'paused', 'exhausted', 'archived')),
          priority INTEGER NOT NULL DEFAULT 100,
          next_crawl_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          crawl_interval_sec INTEGER NOT NULL DEFAULT 1296000,
          quality_score NUMERIC(5, 2),
          quality_status TEXT NOT NULL DEFAULT 'unscored',
          quality_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          quality_checked_at TIMESTAMPTZ,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        ALTER TABLE crawler.query_terms
        ADD COLUMN IF NOT EXISTS query_set_id BIGINT REFERENCES crawler.query_sets(query_set_id) ON DELETE SET NULL;

        ALTER TABLE crawler.query_terms
        ADD COLUMN IF NOT EXISTS quality_score NUMERIC(5, 2);

        ALTER TABLE crawler.query_terms
        ADD COLUMN IF NOT EXISTS quality_status TEXT NOT NULL DEFAULT 'unscored';

        ALTER TABLE crawler.query_terms
        ADD COLUMN IF NOT EXISTS quality_json JSONB NOT NULL DEFAULT '{}'::jsonb;

        ALTER TABLE crawler.query_terms
        ADD COLUMN IF NOT EXISTS quality_checked_at TIMESTAMPTZ;

        CREATE INDEX IF NOT EXISTS idx_crawler_query_terms_set
        ON crawler.query_terms (query_set_id, status, priority DESC, query_id ASC);

        CREATE INDEX IF NOT EXISTS idx_crawler_query_terms_quality
        ON crawler.query_terms (quality_status, quality_score DESC NULLS LAST);

        CREATE TABLE IF NOT EXISTS crawler.query_quality_batches (
          quality_batch_id TEXT PRIMARY KEY,
          status TEXT NOT NULL DEFAULT 'queued'
            CHECK (status IN ('queued', 'running', 'cancel_requested', 'cancelled', 'done', 'failed')),
          total_count INTEGER NOT NULL DEFAULT 0,
          processed_count INTEGER NOT NULL DEFAULT 0,
          scored_count INTEGER NOT NULL DEFAULT 0,
          fallback_count INTEGER NOT NULL DEFAULT 0,
          failed_count INTEGER NOT NULL DEFAULT 0,
          cancelled_count INTEGER NOT NULL DEFAULT 0,
          options_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          error_message TEXT,
          started_at TIMESTAMPTZ,
          finished_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_crawler_query_quality_batches_status
        ON crawler.query_quality_batches (status, created_at ASC);

        CREATE TABLE IF NOT EXISTS crawler.query_quality_tasks (
          quality_task_id BIGSERIAL PRIMARY KEY,
          quality_batch_id TEXT NOT NULL REFERENCES crawler.query_quality_batches(quality_batch_id) ON DELETE CASCADE,
          query_id BIGINT NOT NULL REFERENCES crawler.query_terms(query_id) ON DELETE CASCADE,
          status TEXT NOT NULL DEFAULT 'queued'
            CHECK (status IN ('queued', 'running', 'scored', 'fallback', 'failed', 'cancelled')),
          attempts INTEGER NOT NULL DEFAULT 0,
          result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          error_message TEXT,
          started_at TIMESTAMPTZ,
          finished_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          UNIQUE (quality_batch_id, query_id)
        );

        CREATE INDEX IF NOT EXISTS idx_crawler_query_quality_tasks_claim
        ON crawler.query_quality_tasks (quality_batch_id, status, quality_task_id);

        CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_query_terms_scope
        ON crawler.query_terms (
          lower(query_text),
          COALESCE(language, ''),
          COALESCE(country, ''),
          COALESCE(category, '')
        );

        CREATE TABLE IF NOT EXISTS crawler.raw_objects (
          raw_object_id BIGSERIAL PRIMARY KEY,
          bucket TEXT NOT NULL,
          object_key TEXT NOT NULL,
          object_path TEXT NOT NULL UNIQUE,
          object_type TEXT NOT NULL,
          entity_type TEXT,
          entity_id TEXT,
          source TEXT,
          content_type TEXT,
          content_hash TEXT,
          size_bytes BIGINT,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_crawler_raw_objects_entity
        ON crawler.raw_objects (entity_type, entity_id, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_crawler_raw_objects_type
        ON crawler.raw_objects (object_type, created_at DESC);

        ALTER TABLE crawler.raw_objects ADD COLUMN IF NOT EXISTS content_encoding TEXT;
        ALTER TABLE crawler.raw_objects ADD COLUMN IF NOT EXISTS original_size_bytes BIGINT;
        ALTER TABLE crawler.raw_objects ADD COLUMN IF NOT EXISTS stored_size_bytes BIGINT;

        CREATE TABLE IF NOT EXISTS crawler.settings (
          setting_key TEXT PRIMARY KEY,
          value_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        ALTER TABLE crawler.agent_configs
        ADD COLUMN IF NOT EXISTS max_workers INTEGER NOT NULL DEFAULT 1;

        INSERT INTO crawler.query_sets (name, description)
        VALUES ('default', 'Default query set for BullMQ crawler')
        ON CONFLICT (lower(name)) DO NOTHING;

        UPDATE crawler.query_terms
        SET query_set_id = (SELECT query_set_id FROM crawler.query_sets WHERE lower(name) = 'default' LIMIT 1)
        WHERE query_set_id IS NULL;

      `);
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  })().catch((error) => {
    schemaReady = null;
    throw error;
  });
  return schemaReady;
}

async function queueStats() {
  const rows = [];
  for (const name of queueNames) {
    const counts = await queues[name].getJobCounts(
      "waiting",
      "active",
      "delayed",
      "failed",
      "completed",
      "paused",
      "waiting-children",
      "prioritized",
    );
    rows.push({ name, counts });
  }
  return rows;
}

async function discoverQueueBacklog() {
  const counts = await queues["youtube-discover-page"].getJobCounts(
    "waiting",
    "active",
    "delayed",
    "prioritized",
    "paused",
  );
  return Object.values(counts).reduce((sum, count) => sum + Number(count || 0), 0);
}

async function downstreamPipelineWorkCount(pipelineCycleId) {
  if (!pipelineCycleId) return 0;
  let queueBacklog = 0;
  for (const name of queueNames.filter((item) => !["youtube-query-quality", "youtube-discover-page"].includes(item))) {
    const jobs = await queues[name].getJobs(["waiting", "active", "delayed", "prioritized", "paused"], 0, 9999, true);
    queueBacklog += jobs.filter((job) => job.data?.pipeline_cycle_id === pipelineCycleId).length;
  }
  const result = await db(`
    SELECT count(*)::int AS total
    FROM crawler.channels c
    JOIN crawler.channel_runs r ON r.run_id=c.latest_run_id
    WHERE c.status='active'
      AND r.result_json->>'pipeline_cycle_id'=$1
  `, [pipelineCycleId]);
  return queueBacklog + Number(result.rows[0]?.total || 0);
}

async function storageHealth() {
  const endpoint = String(process.env.S3_ENDPOINT || "").trim();
  const bucket = String(process.env.S3_BUCKET || "").trim();
  if (!endpoint || !bucket) return { ok: false, configured: false, bucket };
  try {
    const response = await fetch(`${endpoint.replace(/\/$/, "")}/minio/health/ready`, { signal: AbortSignal.timeout(3000) });
    return { ok: response.ok, configured: true, bucket, endpoint, status: response.status };
  } catch (error) {
  return { ok: false, configured: true, bucket, endpoint, error: error?.message || String(error) };
  }
}

async function crawlerOperationalMetrics() {
  const [storageRows, storageTypeRows, finalizeRows, runtimeRows, connectionRows] = await Promise.all([
    db(`SELECT count(*)::int AS objects,
               COALESCE(sum(COALESCE(stored_size_bytes,size_bytes,0)),0)::bigint AS stored_bytes,
               COALESCE(sum(COALESCE(original_size_bytes,size_bytes,0)),0)::bigint AS original_bytes,
               count(*) FILTER (WHERE created_at>=CURRENT_DATE)::int AS objects_today,
               COALESCE(sum(COALESCE(stored_size_bytes,size_bytes,0))
                 FILTER (WHERE created_at>=CURRENT_DATE),0)::bigint AS stored_today
        FROM crawler.raw_objects`),
    db(`SELECT object_type,count(*)::int AS objects,
               COALESCE(sum(COALESCE(stored_size_bytes,size_bytes,0)),0)::bigint AS stored_bytes
        FROM crawler.raw_objects
        GROUP BY object_type
        ORDER BY sum(COALESCE(stored_size_bytes,size_bytes,0)) DESC
        LIMIT 6`),
    db(`WITH per_channel AS (
          SELECT entity_key,count(*)::int AS runs
          FROM crawler.task_events
          WHERE queue_name='youtube-finalize' AND status='completed'
          GROUP BY entity_key
        )
        SELECT
          (SELECT count(*)::int FROM crawler.finalized_profiles) AS profiles,
          (SELECT count(*)::int
           FROM crawler.finalized_profiles f
           JOIN crawler.channels c USING(channel_id)
           WHERE f.run_id IS DISTINCT FROM c.latest_run_id) AS stale_profiles,
          COALESCE((SELECT round(avg(runs),2) FROM per_channel),0) AS average_runs,
          COALESCE((SELECT max(runs) FROM per_channel),0)::int AS maximum_runs`),
    db(`WITH recent AS (
          SELECT (payload_json->>'duration_ms')::numeric AS total_ms
          FROM crawler.task_events
          WHERE queue_name='youtube-channel-crawl' AND status='completed'
            AND payload_json ? 'duration_ms'
          ORDER BY created_at DESC
          LIMIT 500
        )
        SELECT count(*)::int AS samples,round(avg(total_ms))::bigint AS average_ms,
               round(percentile_cont(.5) WITHIN GROUP (ORDER BY total_ms))::bigint AS p50_ms,
               round(percentile_cont(.95) WITHIN GROUP (ORDER BY total_ms))::bigint AS p95_ms
        FROM recent`),
    db(`SELECT count(*) FILTER (WHERE state='active')::int AS active,
               count(*) FILTER (WHERE state='idle')::int AS idle,
               current_setting('max_connections')::int AS maximum
        FROM pg_stat_activity
        WHERE datname=current_database()`),
  ]);
  let proxy = { ok: false, roles: {}, reserve: 0, active: 0, cooldown: 0 };
  try {
    const response = await fetch(`${rotaProxyControlUrl}/capacity`, { signal: AbortSignal.timeout(3000) });
    if (response.ok) proxy = await response.json();
  } catch {
    // The rest of the crawler metrics remain useful while the reconciler restarts.
  }
  return {
    storage: storageRows.rows[0] || {},
    storageTypes: storageTypeRows.rows,
    finalize: finalizeRows.rows[0] || {},
    runtime: runtimeRows.rows[0] || {},
    connections: connectionRows.rows[0] || {},
    proxy,
  };
}

function normalizeCrawlSettings(value = {}) {
  return {
    channel_content_limit: jsonInt(value.channel_content_limit, defaultChannelContentLimit, 1, 100),
    content_max_age_days: jsonInt(value.content_max_age_days, defaultContentMaxAgeDays, 0, 3650),
    min_subscriber_count: jsonInt(value.min_subscriber_count, defaultMinSubscriberCount, 0, 1_000_000_000),
    discover_stop_min_qualified_ratio: jsonNumber(
      value.discover_stop_min_qualified_ratio,
      defaultDiscoverStopMinQualifiedRatio,
      0,
      1,
    ),
    detail_max_attempts: jsonInt(value.detail_max_attempts, 3, 1, 10),
    detail_concurrency: jsonInt(value.detail_concurrency, 2, 1, 4),
    published_at_required_precision: "date_only",
  };
}

function normalizeQueryScheduler(value = {}) {
  const status = querySchedulerStatuses.has(String(value.status || "")) ? String(value.status) : "stopped";
  const querySetId = Number(value.query_set_id);
  const queryQualityMinScore = Number(value.query_quality_min_score);
  return {
    status,
    query_set_id: Number.isFinite(querySetId) && querySetId > 0 ? Math.floor(querySetId) : null,
    query_quality_min_score: Number.isFinite(queryQualityMinScore)
      ? Math.max(0, Math.min(100, Math.floor(queryQualityMinScore)))
      : 0,
    chunk_size: jsonInt(value.chunk_size, 3, 1, 100),
    max_discover_backlog: jsonInt(value.max_discover_backlog, defaultDiscoverBacklogLimit, 1, 20),
    started_at: value.started_at || null,
    paused_at: value.paused_at || null,
    stopped_at: value.stopped_at || null,
    completed_at: value.completed_at || null,
    stop_reason: value.stop_reason || null,
    paused_from_status: value.paused_from_status || null,
    pipeline_cycle_id: value.pipeline_cycle_id || null,
    updated_at: value.updated_at || null,
    updated_by: value.updated_by || null,
  };
}

async function ensureQueryScheduler() {
  const existing = await db("SELECT value_json FROM crawler.settings WHERE setting_key = 'query_scheduler' LIMIT 1");
  const settings = normalizeQueryScheduler(existing.rows[0]?.value_json || {});
  await db(`
    INSERT INTO crawler.settings (setting_key, value_json, updated_at)
    VALUES ('query_scheduler', $1::jsonb, now())
    ON CONFLICT (setting_key) DO NOTHING
  `, [JSON.stringify(settings)]);
  return settings;
}

async function saveQueryScheduler(settings) {
  const normalized = normalizeQueryScheduler({
    ...settings,
    updated_at: new Date().toISOString(),
    updated_by: "dashboard",
  });
  await db(`
    INSERT INTO crawler.settings (setting_key, value_json, updated_at)
    VALUES ('query_scheduler', $1::jsonb, now())
    ON CONFLICT (setting_key)
    DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = now()
  `, [JSON.stringify(normalized)]);
  return normalized;
}

function queryDuePredicate(queryAlias, querySetParam, qualityScoreParam) {
  return `
    ${queryAlias}.next_crawl_at <= now()
    AND (${querySetParam}::bigint IS NULL OR ${queryAlias}.query_set_id = ${querySetParam}::bigint)
    AND ${queryAlias}.quality_score IS NOT NULL
    AND ${queryAlias}.quality_status NOT IN ('unscored', 'failed')
    AND COALESCE(${queryAlias}.quality_score, 0) >= ${qualityScoreParam}::numeric
    AND NOT EXISTS (
      SELECT 1
      FROM crawler.query_pages qp
      WHERE qp.query_id = ${queryAlias}.query_id
        AND qp.status IN ('queued', 'running')
    )
    AND NOT EXISTS (
      SELECT 1
      FROM crawler.query_pages qp
      WHERE qp.query_id = ${queryAlias}.query_id
        AND qp.status = 'done'
        AND qp.should_continue = true
        AND qp.result_json ? 'next_continuation_token'
        AND COALESCE(qp.result_json #>> '{yt_config,apiKey}', '') <> ''
        AND right(qp.page_id, length(':page:' || qp.page_no::text)) = ':page:' || qp.page_no::text
        AND NOT EXISTS (
          SELECT 1
          FROM crawler.query_pages qp2
          WHERE qp2.page_id = regexp_replace(
            qp.page_id,
            ':page:' || qp.page_no::text || '$',
            ':page:' || (qp.page_no + 1)::text
          )
        )
    )
    AND NOT EXISTS (
      SELECT 1
      FROM crawler.query_pages qp
      WHERE qp.query_id = ${queryAlias}.query_id
        AND qp.status = 'failed'
        AND qp.result_json ? 'parser_contract_error'
    )
    AND NOT EXISTS (
      SELECT 1
      FROM crawler.query_pages qp
      WHERE qp.query_id = ${queryAlias}.query_id
        AND qp.status = 'failed'
        AND qp.updated_at > now() - interval '10 minutes'
    )`;
}

async function querySchedulerWorkCounts(querySetId = null, queryQualityMinScore = 0) {
  const normalizedQuerySetId = Number.isFinite(Number(querySetId)) && Number(querySetId) > 0
    ? Math.floor(Number(querySetId))
    : null;
  const normalizedQualityMinScore = Number.isFinite(Number(queryQualityMinScore))
    ? Math.max(0, Math.min(100, Math.floor(Number(queryQualityMinScore))))
    : 0;
  const args = [normalizedQuerySetId, normalizedQualityMinScore];
  const eligible = await db(`
    SELECT count(*)::bigint AS total
    FROM crawler.query_terms qt
    WHERE ($1::bigint IS NULL OR qt.query_set_id = $1::bigint)
      AND qt.quality_score IS NOT NULL
      AND qt.quality_status NOT IN ('unscored', 'failed')
      AND COALESCE(qt.quality_score, 0) >= $2::numeric
  `, args);
  const due = await db(`
    SELECT count(*)::bigint AS total
    FROM crawler.query_terms qt
    WHERE ${queryDuePredicate("qt", "$1", "$2")}
  `, args);
  const resumable = await db(`
    SELECT count(*)::bigint AS total
    FROM crawler.query_pages qp
    JOIN crawler.query_terms qt ON qt.query_id = qp.query_id
    WHERE qp.status = 'done'
      AND qp.should_continue = true
      AND qp.result_json ? 'next_continuation_token'
      AND COALESCE(qp.result_json #>> '{yt_config,apiKey}', '') <> ''
      AND right(qp.page_id, length(':page:' || qp.page_no::text)) = ':page:' || qp.page_no::text
      AND NOT EXISTS (
        SELECT 1
        FROM crawler.query_pages qp2
        WHERE qp2.page_id = regexp_replace(
          qp.page_id,
          ':page:' || qp.page_no::text || '$',
          ':page:' || (qp.page_no + 1)::text
        )
      )
      AND ($1::bigint IS NULL OR qt.query_set_id = $1::bigint)
      AND qt.quality_score IS NOT NULL
      AND qt.quality_status NOT IN ('unscored', 'failed')
      AND COALESCE(qt.quality_score, 0) >= $2::numeric
  `, args);
  const scoring = await db(`
    SELECT count(*)::bigint AS total
    FROM crawler.query_quality_tasks task
    JOIN crawler.query_quality_batches batch USING (quality_batch_id)
    JOIN crawler.query_terms term ON term.query_id=task.query_id
    WHERE task.status IN ('queued','running')
      AND batch.status IN ('queued','running')
      AND ($1::bigint IS NULL OR term.query_set_id=$1::bigint)
  `, args.slice(0, 1));
  return {
    eligible: Number(eligible.rows[0]?.total || 0),
    due: Number(due.rows[0]?.total || 0),
    resumable: Number(resumable.rows[0]?.total || 0),
    scoring: Number(scoring.rows[0]?.total || 0),
  };
}

function normalizeYoutubeApiSettings(value = {}) {
  const apiKeys = parseApiKeys(value.api_keys?.length ? value.api_keys : (value.api_key || defaultYoutubeDataApiKeys));
  return {
    api_keys: apiKeys,
    timeout_ms: jsonInt(value.timeout_ms, 12000, 1000, 60000),
    batch_size: jsonInt(value.batch_size, 50, 1, 50),
    daily_request_limit: jsonInt(value.daily_request_limit, 500, 0, 10000),
    fallback_mode: value.fallback_mode === "disabled" ? "disabled" : "emergency",
  };
}

function publicYoutubeApiSettings(settings) {
  const apiKeys = parseApiKeys(settings.api_keys);
  return {
    key_count: apiKeys.length,
    key_tails: apiKeys.map((key) => key.slice(-4)),
    timeout_ms: settings.timeout_ms,
    batch_size: settings.batch_size,
    daily_request_limit: settings.daily_request_limit,
    fallback_mode: settings.fallback_mode,
  };
}

function normalizeAgentLlmSettings(value = {}, config = {}) {
  const apiKeys = parseApiKeys(value.api_keys?.length ? value.api_keys : (value.api_key || defaultAgentApiKeys));
  const baseUrl = String(value.base_url || config.endpoint || defaultAgentBaseUrl || "").trim();
  return {
    base_url: baseUrl,
    api_keys: apiKeys,
  };
}

function publicAgentLlmSettings(settings) {
  const apiKeys = parseApiKeys(settings.api_keys);
  return {
    base_url: settings.base_url || "",
    key_count: apiKeys.length,
    key_tails: apiKeys.map((key) => keyTail(key)),
  };
}

function agentLlmSettingKey(configId) {
  const id = Number(configId);
  return Number.isFinite(id) && id > 0 ? `agent_llm:${Math.floor(id)}` : "agent_llm";
}

async function ensureCrawlSettings() {
  const existing = await db("SELECT value_json FROM crawler.settings WHERE setting_key = 'crawl' LIMIT 1");
  const settings = normalizeCrawlSettings(existing.rows[0]?.value_json || {});
  await db(`
    INSERT INTO crawler.settings (setting_key, value_json, updated_at)
    VALUES ('crawl', $1::jsonb, now())
    ON CONFLICT (setting_key) DO NOTHING
  `, [JSON.stringify(settings)]);
  return settings;
}

async function ensureYoutubeApiSettings({ includeSecret = false } = {}) {
  const existing = await db("SELECT value_json FROM crawler.settings WHERE setting_key = 'youtube_api' LIMIT 1");
  const settings = normalizeYoutubeApiSettings(existing.rows[0]?.value_json || {});
  await db(`
    INSERT INTO crawler.settings (setting_key, value_json, updated_at)
    VALUES ('youtube_api', $1::jsonb, now())
    ON CONFLICT (setting_key)
    DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = now()
  `, [JSON.stringify(settings)]);
  return includeSecret ? settings : publicYoutubeApiSettings(settings);
}

async function ensureAgentLlmSettings({ includeSecret = false, config = null } = {}) {
  const settingKey = agentLlmSettingKey(config?.config_id);
  const existing = await db("SELECT value_json FROM crawler.settings WHERE setting_key = $1 LIMIT 1", [settingKey]);
  const fallback = settingKey === "agent_llm"
    ? null
    : await db("SELECT value_json FROM crawler.settings WHERE setting_key = 'agent_llm' LIMIT 1");
  const settings = normalizeAgentLlmSettings(existing.rows[0]?.value_json || fallback?.rows?.[0]?.value_json || {}, config || {});
  await db(`
    INSERT INTO crawler.settings (setting_key, value_json, updated_at)
    VALUES ($1, $2::jsonb, now())
    ON CONFLICT (setting_key)
    DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = now()
  `, [settingKey, JSON.stringify(settings)]);
  return includeSecret ? settings : publicAgentLlmSettings(settings);
}

function agentConnectionEndpoint(config, baseUrl = "") {
  const endpoint = String(config?.endpoint || baseUrl || "").trim();
  if (!endpoint) return null;
  const provider = String(config?.provider || "").toLowerCase();
  if (/:(?:stream)?generateContent\/?$/i.test(endpoint)) return endpoint;
  if (provider.includes("gemini")) {
    const model = String(config?.model || "gemini-2.5-flash").trim();
    if (/\/models\/[^/]+\/?$/i.test(endpoint)) return `${endpoint.replace(/\/$/, "")}:generateContent`;
    return `${endpoint.replace(/\/$/, "")}/models/${encodeURIComponent(model)}:generateContent`;
  }
  if (/\/chat\/completions\/?$/i.test(endpoint) || /\/responses\/?$/i.test(endpoint)) return endpoint;
  return `${endpoint.replace(/\/$/, "")}/chat/completions`;
}

function compactAgentTestText(value, maxLength = 240) {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  return text.length > maxLength ? `${text.slice(0, maxLength)}...` : text;
}

function agentTestResponseText(data) {
  if (typeof data === "string") return data;
  if (typeof data?.output_text === "string") return data.output_text;
  if (typeof data?.text === "string") return data.text;
  const content = data?.choices?.[0]?.message?.content;
  if (typeof content === "string") return content;
  if (Array.isArray(content)) return content.map((part) => part?.text ?? part?.content ?? "").join("");
  const parts = data?.candidates?.[0]?.content?.parts;
  if (Array.isArray(parts)) return parts.map((part) => part?.text ?? "").join("");
  if (Array.isArray(data?.output)) {
    return data.output.flatMap((item) => item?.content ?? []).map((part) => part?.text ?? "").join("");
  }
  return JSON.stringify(data ?? "");
}

function safeAgentTestError(error, apiKeys = []) {
  let message = String(error?.message || error || "Agent 连接测试失败");
  for (const apiKey of apiKeys) {
    if (apiKey) message = message.replaceAll(apiKey, "[REDACTED]");
  }
  message = message.replace(/Bearer\s+\S+/gi, "Bearer [REDACTED]");
  return compactAgentTestText(message, 500);
}

async function testAgentConnection(config, llmSettings) {
  const provider = String(config?.provider || "").toLowerCase();
  if (provider === "rules") throw new Error("rules 配置不会调用外部模型");
  const endpoint = agentConnectionEndpoint(config, llmSettings?.base_url);
  if (!endpoint) throw new Error("Agent BaseURL 为空");
  const apiKeys = parseApiKeys(llmSettings?.api_keys);
  const keys = apiKeys.length > 0 ? apiKeys : [null];
  const timeoutMs = Math.min(60000, Math.max(5000, Number(config?.timeout_ms) || 30000));
  const model = String(config?.model || "gpt-4o-mini").trim();
  let lastError = null;

  for (let index = 0; index < keys.length; index += 1) {
    const apiKey = keys[index];
    const isGemini = provider.includes("gemini") || endpoint.includes("generativelanguage.googleapis.com");
    const useResponses = /\/responses\/?$/i.test(endpoint);
    const body = isGemini
      ? {
          contents: [{ role: "user", parts: [{ text: "hello" }] }],
          generationConfig: { temperature: 0.2, maxOutputTokens: 16 },
        }
      : useResponses
        ? { model, input: "hello", max_output_tokens: 16 }
        : {
            model,
            messages: [{ role: "user", content: "hello" }],
            temperature: 0.2,
            stream: false,
            max_tokens: 16,
          };
    const headers = {
      "content-type": "application/json",
      ...(isGemini && apiKey ? { "x-goog-api-key": apiKey } : {}),
      ...(!isGemini && apiKey ? { authorization: /^bearer\s+/i.test(apiKey) ? apiKey : `Bearer ${apiKey}` } : {}),
    };

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs),
      });
      const rawBody = await response.text();
      if (!response.ok) {
        const detail = compactAgentTestText(rawBody, 300);
        const requestError = new Error(`Agent HTTP ${response.status}${detail ? `: ${detail}` : ""}`);
        requestError.status = response.status;
        requestError.endpoint = endpoint;
        throw requestError;
      }
      let payload = rawBody;
      try {
        payload = JSON.parse(rawBody);
      } catch {
        // A successful compatible endpoint may return plain text or SSE.
      }
      return {
        endpoint,
        response_preview: compactAgentTestText(agentTestResponseText(payload) || "连接成功"),
      };
    } catch (error) {
      error.endpoint = error.endpoint || endpoint;
      lastError = error;
      const tryNextKey = index < keys.length - 1 && [401, 403, 429].includes(Number(error?.status));
      if (!tryNextKey) break;
    }
  }

  const safeError = new Error(safeAgentTestError(lastError, apiKeys));
  safeError.endpoint = lastError?.endpoint || endpoint;
  throw safeError;
}

async function recordAgentConnectionTest(config, result) {
  const inserted = await db(`
    INSERT INTO crawler.task_events (
      queue_name,job_id,job_name,entity_key,status,payload_json,error_message
    ) VALUES (
      'agent-config-test',$1,'agent-config-hello',$2,$3,$4::jsonb,$5
    )
    RETURNING created_at
  `, [
    `agent-config-test:${config.config_id}:${randomUUID()}`,
    `agent-config:${config.config_id}`,
    result.ok ? "completed" : "failed",
    JSON.stringify({
      agent_config_id: Number(config.config_id),
      agent_config_name: config.name,
      provider: config.provider,
      model: config.model,
      endpoint: result.endpoint || null,
      duration_ms: result.duration_ms,
      response_preview: result.response_preview || null,
    }),
    result.error || null,
  ]);
  return inserted.rows[0]?.created_at || new Date().toISOString();
}

async function recalculateContentRecency(recentDays) {
  const result = await db(`
    UPDATE crawler.contents
    SET is_recent = CASE
          WHEN $1::int <= 0 THEN true
          WHEN published_at IS NULL THEN is_recent
          ELSE (published_at AT TIME ZONE 'UTC')::date >= ((now() AT TIME ZONE 'UTC')::date - $1::int)
        END
    WHERE published_at IS NOT NULL
    RETURNING content_key
  `, [recentDays]);
  return result.rowCount || 0;
}

async function channelSummaryRows({
  limit = 100,
  offset = 0,
  search = "",
  channelStatus = "active",
  agentStatus = "",
  finalStatus = "",
} = {}) {
  const where = [];
  const args = [];
  if (search) {
    args.push(`%${search}%`);
    where.push(`(c.channel_id ILIKE $${args.length} OR c.channel_url ILIKE $${args.length} OR c.handle ILIKE $${args.length} OR c.title ILIKE $${args.length})`);
  }
  addChannelStatusFilter(where, args, channelStatus);
  if (agentStatus) {
    args.push(agentStatus);
    where.push(`c.agent_status = $${args.length}`);
  }
  if (finalStatus) {
    args.push(finalStatus);
    where.push(`COALESCE(fp.status, 'pending') = $${args.length}`);
  }
  if (!finalStatus && channelStatus === "active") {
    where.push(MIGRATION_FINALIZED_SQL);
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await db(`
    SELECT
      c.channel_id,
      c.channel_url,
      COALESCE(c.handle, '') AS handle,
      COALESCE(c.title, '') AS title,
      c.subscriber_count,
      c.status,
      c.reject_reason,
      c.dormant_reason,
      c.dormant_since,
      c.dormant_recheck_day,
      c.dormant_last_probe_at,
      c.dormant_cycle,
      c.removed_reason,
      c.removed_at,
      c.removed_source,
      c.removed_evidence,
      c.ready_for_agent,
      c.agent_status,
      c.latest_run_id,
      c.created_at,
      c.updated_at,
      candidate.avatar_url,
      clock.about_due_at,
      clock.about_due_day,
      COALESCE(about_snapshot.observed_at, clock.about_last_complete_at) AS about_last_observed_at,
      clock.video_due_at,
      clock.video_due_day,
      clock.agent_due_at,
      clock.agent_due_day,
      clock.about_tier,
      clock.video_tier,
      clock.agent_tier,
      clock.clock_version,
      active_clock_plan.plan_id AS clock_plan_id,
      active_clock_plan.status AS clock_plan_status,
      active_clock_plan.scheduled_at AS clock_plan_scheduled_at,
      active_clock_plan.run_about AS clock_plan_run_about,
      active_clock_plan.run_video AS clock_plan_run_video,
      active_clock_plan.run_agent AS clock_plan_run_agent,
      COALESCE(fp.status, 'pending') AS final_status,
      COALESCE(fp.quality_json, '{}'::jsonb) AS quality_json,
      fp.finalized_at,
      r.status AS run_status,
      r.detail_status AS run_detail_status,
      (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id)::bigint AS content_count,
      (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id AND ct.content_type = 'video')::bigint AS video_count,
      (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id AND ct.content_type = 'short')::bigint AS short_count,
      (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id AND ct.content_type = 'live')::bigint AS live_count
    FROM crawler.channels c
    LEFT JOIN LATERAL (
      SELECT COALESCE(
               NULLIF(candidate.snapshot_json #>> '{channel_header,avatar_url}', ''),
               NULLIF(candidate.avatar_url, '')
             ) AS avatar_url
      FROM crawler.channel_candidates candidate
      WHERE candidate.channel_id = c.channel_id
      ORDER BY candidate.accepted_at DESC NULLS LAST, candidate.created_at DESC
      LIMIT 1
    ) candidate ON true
    LEFT JOIN crawler.finalized_profiles fp ON fp.channel_id = c.channel_id
    LEFT JOIN crawler.channel_runs r ON r.run_id = c.latest_run_id
    LEFT JOIN feature_clock.channel_clock_state clock ON clock.channel_id = c.channel_id
    LEFT JOIN LATERAL (
      SELECT snapshot.observed_at
      FROM crawler.channel_about_metric_snapshots snapshot
      WHERE snapshot.channel_id = c.channel_id
      ORDER BY snapshot.observed_at DESC, snapshot.observation_id DESC
      LIMIT 1
    ) about_snapshot ON true
    LEFT JOIN feature_clock.daily_channel_plans active_clock_plan
      ON active_clock_plan.channel_id = c.channel_id
     AND active_clock_plan.status IN ('planned', 'dispatching', 'dispatched', 'running')
    ${whereSql}
    ORDER BY c.created_at DESC
    LIMIT $${args.length + 1} OFFSET $${args.length + 2}
  `, [...args, limit, offset]);
  return rows.rows;
}

async function migrationChannelListData(req) {
  const limit = intValue(req.query.limit, 500, 1, 500);
  const offset = intValue(req.query.offset, 0, 0, 1_000_000);
  const search = String(req.query.q || "").trim();
  const requestedCandidateStatus = String(req.query.channel_status || "all").trim();
  const candidateStatuses = new Set(["all", "discovered", "queued", "validating", "failed", "finishing"]);
  const channelStatus = candidateStatuses.has(requestedCandidateStatus) ? requestedCandidateStatus : "all";
  const agentStatus = String(req.query.agent_status || "").trim();
  const finalStatus = String(req.query.final_status || "").trim();
  const filters = { limit, offset, search, channelStatus, agentStatus, finalStatus };
  if (!migrationPool) {
    return { configured: false, available: false, channels: [], total: 0, stats: { total: 0 }, filters };
  }

  try {
    const args = [];
    const sourceWhere = ["candidate.source_json->>'source'='legacy_results_db'"];
    if (search) {
      args.push(`%${search}%`);
      sourceWhere.push(`(
        candidate.channel_id ILIKE $${args.length}
        OR candidate.channel_url ILIKE $${args.length}
        OR candidate.handle ILIKE $${args.length}
        OR candidate.title ILIKE $${args.length}
      )`);
    }
    const sourceWhereSql = sourceWhere.join(" AND ");
    const sourceRows = await migrationRead(`WITH ranked_source AS (
        SELECT
          candidate.*,
          row_number() OVER (
            PARTITION BY candidate.channel_id
            ORDER BY candidate.priority DESC,candidate.candidate_id DESC
          ) AS channel_rank
        FROM crawler.channel_candidates candidate
        WHERE ${sourceWhereSql}
      )
        SELECT
          candidate_id,
          candidate.dispatch_batch_id,
          candidate.channel_id,
          candidate.channel_url,candidate.handle,candidate.title,candidate.avatar_url,
          candidate.search_subscriber_count,candidate.snapshot_json,candidate.source_json,
          candidate.source_json #>> '{legacy_import,country}' AS legacy_country,
          candidate.source_json #>> '{legacy_import,target_reason}' AS legacy_target_reason,
          candidate.source_json #>> '{legacy_import,br_evidence_score}' AS legacy_evidence_score,
          candidate.source_json #>> '{legacy_import,br_evidence_reasons}' AS legacy_evidence_reasons,
          candidate.source_json #>> '{legacy_import,discovered_at}' AS legacy_discovered_at,
          candidate.source_json->>'source_rowid' AS source_rowid,
          candidate.created_at,
          candidate.updated_at
        FROM ranked_source candidate
        WHERE candidate.channel_rank=1
        ORDER BY candidate.priority DESC,candidate.candidate_id
      `, args);
    const sourceCandidateRows = sourceRows.rows;
    const targetCandidateRows = [];
    for (const sourceChunk of chunksOf(sourceCandidateRows, 500)) {
      const sourceCandidateIds = sourceChunk.map((row) => String(row.candidate_id));
      const channelIds = sourceChunk.map((row) => row.channel_id);
      const targetRows = await db(`
          SELECT intent.migration_intent_id,intent.source_candidate_id::text,
                 intent.target_candidate_id,intent.channel_id,
                 candidate.status AS target_candidate_status,
                 channel.status AS target_channel_status,
                 channel.registry_promotion_candidate_id,
                 channel.reject_reason AS target_reject_reason,
                 channel.agent_status,channel.latest_run_id,
                 COALESCE(finalized.status,'pending') AS final_status,
                 COALESCE(finalized.quality_json,'{}'::jsonb) AS quality_json,
                 run.status AS run_status,run.detail_status AS run_detail_status,
                 (SELECT count(*) FROM crawler.contents content
                  WHERE content.channel_id=intent.channel_id
                    AND content.run_id=channel.latest_run_id)::bigint AS content_count,
                 (SELECT count(*) FROM crawler.contents content
                  WHERE content.channel_id=intent.channel_id
                    AND content.run_id=channel.latest_run_id
                    AND content.content_type='video')::bigint AS video_count,
                 (SELECT count(*) FROM crawler.contents content
                  WHERE content.channel_id=intent.channel_id
                    AND content.run_id=channel.latest_run_id
                    AND content.content_type='short')::bigint AS short_count,
                 (SELECT count(*) FROM crawler.contents content
                  WHERE content.channel_id=intent.channel_id
                    AND content.run_id=channel.latest_run_id
                    AND content.content_type='live')::bigint AS live_count,
                 GREATEST(intent.updated_at,COALESCE(candidate.updated_at,intent.updated_at),
                          COALESCE(channel.updated_at,intent.updated_at)) AS updated_at
          FROM crawler.migration_channel_intents intent
          LEFT JOIN crawler.channel_candidates candidate
            ON candidate.candidate_id=intent.target_candidate_id
          LEFT JOIN crawler.channels channel ON channel.channel_id=intent.channel_id
          LEFT JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
          LEFT JOIN crawler.finalized_profiles finalized ON finalized.channel_id=intent.channel_id
          WHERE intent.source_id=$1
            AND (intent.source_candidate_id=ANY($2::bigint[])
                 OR intent.channel_id=ANY($3::text[]))
        `, [migrationSourceId, sourceCandidateIds, channelIds]);
      targetCandidateRows.push(...targetRows.rows);
    }
    const mergedChannels = mergeMigrationCandidates(sourceCandidateRows, targetCandidateRows);
    const page = filterAndPageMigrationCandidates(mergedChannels, {
      channelStatus,
      agentStatus,
      finalStatus,
      offset,
      limit,
    });
    return {
      configured: true,
      available: true,
      channels: page.rows,
      total: page.total,
      stats: migrationReadModelStats(sourceCandidateRows.length, mergedChannels),
      filters,
    };
  } catch (error) {
    console.error("migration channel database read failed", error?.message || String(error));
    return {
      configured: true,
      available: false,
      channels: [],
      total: 0,
      stats: { total: 0 },
      filters,
      error: "迁移数据库当前不可用",
    };
  }
}

async function migrationChannelDetailData(channelId) {
  if (!migrationPool) throw new Error("迁移数据库未配置");
  const candidateRows = await migrationRead(`
    SELECT
      candidate.*,
      candidate.source_json #>> '{legacy_import,country}' AS legacy_country,
      candidate.source_json #>> '{legacy_import,target_reason}' AS legacy_target_reason,
      candidate.source_json #>> '{legacy_import,br_evidence_score}' AS legacy_evidence_score,
      candidate.source_json #>> '{legacy_import,br_evidence_reasons}' AS legacy_evidence_reasons,
      candidate.source_json #>> '{legacy_import,discovered_at}' AS legacy_discovered_at,
      candidate.source_json->>'source_rowid' AS source_rowid,
      candidate.source_json->>'source_database' AS source_database,
      candidate.source_json->>'source_database_sha256' AS source_database_sha256
    FROM crawler.channel_candidates candidate
    WHERE candidate.channel_id=$1
      AND candidate.source_json->>'source'='legacy_results_db'
    ORDER BY candidate.candidate_id DESC
    LIMIT 1
  `, [channelId]);
  if (candidateRows.rows.length === 0) return null;

  const candidate = candidateRows.rows[0];
  const [sourceRows, batchRows] = await Promise.all([
    migrationRead(`
      SELECT page_id,query_text,rank_position,discovery_strategy,source_json,created_at
      FROM crawler.channel_candidate_sources
      WHERE candidate_id=$1
      ORDER BY candidate_source_id
    `, [candidate.candidate_id]),
    migrationRead(`
      SELECT dispatch_batch_id,status,discovered_candidate_count,result_json,started_at,finished_at
      FROM crawler.query_dispatch_batches
      WHERE dispatch_batch_id=$1
      LIMIT 1
    `, [candidate.dispatch_batch_id]),
  ]);
  const [pipelineData, intentRows] = await Promise.all([
    channelDetailDataFrom(db, channelId),
    db(`
      SELECT intent.*,intent.source_candidate_id::text AS source_candidate_id,
             candidate.status AS target_candidate_status,
             channel.status AS target_channel_status,
             channel.registry_promotion_candidate_id,
             channel.reject_reason AS target_reject_reason,
             channel.agent_status,channel.latest_run_id,
             COALESCE(finalized.status,'pending') AS final_status,
             COALESCE(finalized.quality_json,'{}'::jsonb) AS quality_json,
             run.status AS run_status,run.detail_status AS run_detail_status,
             GREATEST(intent.updated_at,COALESCE(candidate.updated_at,intent.updated_at),
                      COALESCE(channel.updated_at,intent.updated_at)) AS updated_at
      FROM crawler.migration_channel_intents intent
      LEFT JOIN crawler.channel_candidates candidate
        ON candidate.candidate_id=intent.target_candidate_id
      LEFT JOIN crawler.channels channel ON channel.channel_id=intent.channel_id
      LEFT JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
      LEFT JOIN crawler.finalized_profiles finalized ON finalized.channel_id=intent.channel_id
      WHERE intent.source_id=$1
        AND (intent.source_candidate_id=$2::bigint OR intent.channel_id=$3)
      LIMIT 1
    `, [migrationSourceId, candidate.candidate_id, channelId]),
  ]);
  const targetState = intentRows.rows[0] || null;
  const merged = mergeMigrationCandidate(candidate, targetState);
  const avatarUrl = candidate.snapshot_json?.channel_header?.avatar_url || candidate.avatar_url || null;
  const detail = pipelineData || {
    channel: {
      channel_id: candidate.channel_id,
      channel_url: candidate.channel_url,
      handle: candidate.handle,
      title: candidate.title,
      subscriber_count: candidate.search_subscriber_count,
      status: merged.status,
      reject_reason: merged.reject_reason,
      agent_status: merged.agent_status,
      final_status: merged.final_status,
      latest_run_id: merged.latest_run_id,
      profile_json: {},
      quality_json: {},
      source_json: candidate.source_json || {},
      created_at: candidate.created_at,
      updated_at: candidate.updated_at,
    },
    runs: [],
    contentStats: [],
    contents: [],
    candidates: [],
    aboutSnapshots: [],
  };
  return {
    ...detail,
    channel: {
      ...detail.channel,
      channel_url: candidate.channel_url,
      handle: candidate.handle,
      title: candidate.title,
      avatar_url: avatarUrl,
    },
    candidate,
    targetCandidate: targetState,
    migrationIntent: targetState,
    sources: sourceRows.rows,
    batch: batchRows.rows[0] || null,
    candidateOnly: !pipelineData,
  };
}

async function migrateChannel(channelId, candidateId) {
  const response = await fetch(
    `${crawlerApiUrl}/api/migration/channels/${encodeURIComponent(channelId)}`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ candidate_id: Number(candidateId) }),
      signal: AbortSignal.timeout(15000),
    },
  );
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) {
    const error = new Error(payload.error || `Crawler API 返回 ${response.status}`);
    error.code = payload.code || "crawler_api_error";
    throw error;
  }
  return payload;
}

async function migrateChannelBatch(selection) {
  const response = await fetch(
    `${crawlerApiUrl}/api/migration/channels/batch`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ selection }),
      signal: AbortSignal.timeout(30000),
    },
  );
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) {
    const error = new Error(payload.error || `Crawler API 返回 ${response.status}`);
    error.code = payload.code || "crawler_api_error";
    throw error;
  }
  return payload;
}

function utcDayOffset(offset = 0, now = new Date()) {
  const day = new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate() + offset,
  ));
  return day.toISOString().slice(0, 10);
}

function dailyClockScopeSql() {
  return `
    WITH bounds AS (
      SELECT $1::date AS target_day,
             (now() AT TIME ZONE 'UTC')::date AS today_utc
    ), scope AS (
      SELECT
        clock.channel_id,
        COALESCE(channel.channel_url,'https://www.youtube.com/channel/' || clock.channel_id) AS channel_url,
        COALESCE(channel.handle,'') AS handle,
        COALESCE(channel.title,'') AS title,
        channel.avatar_url,
        channel.subscriber_count,
        bounds.target_day,
        bounds.today_utc,
        plan.plan_id,
        plan.plan_day,
        COALESCE(plan.status,'unplanned') AS plan_status,
        plan.scheduled_at,
        plan.execution_deadline_at,
        plan.completed_at,
        outbox.status AS dispatch_status,
        run.run_id,
        run.status AS crawler_run_status,
        run.started_at AS crawler_started_at,
        run.finished_at AS crawler_finished_at,
        GREATEST(plan.completed_at,run.finished_at) AS effective_completed_at,
        run.result_json #>> '{domains,about,outcome}' AS crawler_about_outcome,
        run.result_json #>> '{domains,video,outcome}' AS crawler_video_outcome,
        run.result_json #>> '{domains,agent,outcome}' AS crawler_agent_outcome,
        CASE WHEN plan.plan_id IS NOT NULL THEN plan.run_about
             ELSE clock.about_due_day <= bounds.target_day END AS run_about,
        CASE WHEN plan.plan_id IS NOT NULL THEN plan.run_video
             ELSE clock.video_due_day <= bounds.target_day END AS run_video,
        CASE WHEN plan.plan_id IS NOT NULL THEN plan.run_agent
             ELSE clock.agent_due_day <= bounds.target_day END AS run_agent,
        clock.about_due_day,
        clock.video_due_day,
        clock.agent_due_day,
        clock.about_tier,
        clock.video_tier,
        clock.agent_tier,
        plan.due_day AS plan_due_day,
        clock.channel_next_run_day AS current_due_day,
        clock.channel_next_run_day AS due_day,
        clock.dispatch_slot,
        COALESCE(plan.estimated_request_cost,clock.estimated_request_cost) AS estimated_request_cost,
        COALESCE(plan.source_clock_version,clock.clock_version) AS source_clock_version,
        COALESCE(plan.policy_version,clock.policy_version) AS policy_version,
        plan.planner_config_version,
        plan.capacity_version
      FROM feature_clock.channel_clock_state clock
      CROSS JOIN bounds
      LEFT JOIN LATERAL (
        SELECT candidate.*
        FROM feature_clock.daily_channel_plans candidate
        WHERE candidate.channel_id=clock.channel_id
          AND (
            candidate.plan_day=bounds.target_day
            OR (
              bounds.target_day=bounds.today_utc
              AND candidate.status IN ('planned','dispatching','dispatched','running')
            )
          )
        ORDER BY
          (candidate.plan_day=bounds.target_day) DESC,
          candidate.plan_day DESC,
          candidate.created_at DESC
        LIMIT 1
      ) plan ON true
      LEFT JOIN feature_clock.dispatch_outbox outbox ON outbox.plan_id=plan.plan_id
      LEFT JOIN crawler.channel_runs run ON run.plan_id=plan.plan_id
      LEFT JOIN crawler.channels channel ON channel.channel_id=clock.channel_id
      WHERE clock.lifecycle_status='active'
        AND COALESCE(channel.status,'active')='active'
        AND (plan.plan_id IS NOT NULL
         OR (
           (
             (
               bounds.target_day=bounds.today_utc
               AND clock.channel_next_run_day <= bounds.target_day
             )
             OR (
               bounds.target_day>bounds.today_utc
               AND clock.channel_next_run_day = bounds.target_day
             )
           )
           AND NOT EXISTS (
             SELECT 1
             FROM feature_clock.daily_channel_plans active_plan
             WHERE active_plan.channel_id=clock.channel_id
               AND active_plan.status IN ('planned','dispatching','dispatched','running')
           )
         ))
    )`;
}

async function dailyClockListData(req) {
  const day = req.query.day === "tomorrow" ? "tomorrow" : "today";
  const targetDay = utcDayOffset(day === "tomorrow" ? 1 : 0);
  const limit = intValue(req.query.limit, 100, 1, 500);
  const offset = intValue(req.query.offset, 0, 0, 1_000_000);
  const search = String(req.query.q || "").trim();
  const clock = normalizeClockFilter(req.query.clock);
  const requestedStatus = String(req.query.status || "all").trim();
  const statuses = new Set([
    "all", "unplanned", "active", "terminal", "planned", "dispatching",
    "dispatched", "running", "succeeded", "partial", "failed", "recovered",
    "unrecovered", "cancelled",
  ]);
  const status = statuses.has(requestedStatus) ? requestedStatus : "all";
  const args = [targetDay];
  const where = [];
  if (search) {
    args.push(`%${search}%`);
    where.push(`(
      scope.channel_id ILIKE $${args.length}
      OR scope.handle ILIKE $${args.length}
      OR scope.title ILIKE $${args.length}
    )`);
  }
  if (clock !== "all") where.push(`scope.run_${clock}=true`);
  if (status === "active") {
    where.push("scope.plan_status IN ('planned','dispatching','dispatched','running')");
  } else if (status === "terminal") {
    where.push("scope.plan_status IN ('succeeded','partial','failed','cancelled')");
  } else if (status === "recovered") {
    where.push("scope.plan_status='failed' AND scope.crawler_run_status='done'");
  } else if (status === "unrecovered") {
    where.push("scope.plan_status='failed' AND COALESCE(scope.crawler_run_status,'')<>'done'");
  } else if (status !== "all") {
    args.push(status);
    where.push(`scope.plan_status=$${args.length}`);
  }
  const whereSql = where.length > 0 ? `WHERE ${where.join(" AND ")}` : "";
  const scopeSql = dailyClockScopeSql();

  try {
    const [stats, total, rows] = await Promise.all([
      pool.query(`${scopeSql}
        SELECT
          count(*)::int AS total,
          count(*) FILTER (WHERE plan_id IS NOT NULL)::int AS frozen,
          count(*) FILTER (WHERE plan_id IS NULL)::int AS unplanned,
          count(*) FILTER (WHERE plan_status IN ('planned','dispatching','dispatched','running'))::int AS active,
          count(*) FILTER (WHERE plan_status='succeeded')::int AS succeeded,
          count(*) FILTER (WHERE plan_status='failed' AND crawler_run_status='done')::int AS recovered,
          count(*) FILTER (WHERE plan_status='partial')::int AS partial,
          count(*) FILTER (WHERE plan_status='failed' AND COALESCE(crawler_run_status,'')<>'done')::int AS unrecovered,
          count(*) FILTER (WHERE plan_status='cancelled')::int AS cancelled,
          count(*) FILTER (WHERE due_day < target_day)::int AS overdue,
          count(*) FILTER (WHERE run_about)::int AS about,
          count(*) FILTER (WHERE run_video)::int AS video,
          count(*) FILTER (WHERE run_agent)::int AS agent,
          min(crawler_started_at) AS execution_started_at,
          max(effective_completed_at) FILTER (
            WHERE plan_status IN ('succeeded','partial','failed','cancelled')
          ) AS execution_completed_at,
          count(*) FILTER (
            WHERE effective_completed_at >= now() - interval '5 minutes'
              AND (
                plan_status IN ('succeeded','partial')
                OR (plan_status='failed' AND crawler_run_status='done')
              )
          )::int AS recent_completed,
          CASE
            WHEN min(crawler_started_at) IS NULL THEN 0
            ELSE LEAST(
              300,
              GREATEST(0,EXTRACT(EPOCH FROM (now() - min(crawler_started_at))))
            )
          END AS recent_window_seconds
        FROM scope`, [targetDay]),
      pool.query(`${scopeSql} SELECT count(*)::int AS total FROM scope ${whereSql}`, args),
      pool.query(`${scopeSql}
        SELECT * FROM scope
        ${whereSql}
        ORDER BY
          due_day,dispatch_slot,channel_id
        LIMIT $${args.length + 1} OFFSET $${args.length + 2}`,
      [...args, limit, offset]),
    ]);
    return {
      available: true,
      day,
      targetDay,
      generatedAt: new Date().toISOString(),
      channels: rows.rows,
      total: total.rows[0]?.total ?? 0,
      stats: stats.rows[0] || {},
      filters: { limit, offset, search, clock, status },
    };
  } catch (error) {
    console.error("daily clock database read failed", error?.message || String(error));
    return {
      available: false,
      day,
      targetDay,
      generatedAt: new Date().toISOString(),
      channels: [],
      total: 0,
      stats: {},
      filters: { limit, offset, search, clock, status },
      error: "每日 Clock 数据当前不可用",
    };
  }
}

async function channelListData(req) {
  const limit = intValue(req.query.limit, 100, 1, 500);
  const offset = intValue(req.query.offset, 0, 0, 1_000_000);
  const search = String(req.query.q || "").trim();
  const requestedChannelStatus = String(req.query.channel_status || "active").trim();
  const normalizedChannelStatus = requestedChannelStatus === "rejected"
    ? "dormant"
    : requestedChannelStatus === "tracked"
      ? "active"
      : requestedChannelStatus;
  const channelStatus = ["active", "dormant", "removed", "all"].includes(normalizedChannelStatus)
    ? normalizedChannelStatus
    : "active";
  const agentStatus = String(req.query.agent_status || "").trim();
  const finalStatus = String(req.query.final_status || "").trim();

  const where = [];
  const args = [];
  if (search) {
    args.push(`%${search}%`);
    where.push(`(c.channel_id ILIKE $${args.length} OR c.channel_url ILIKE $${args.length} OR c.handle ILIKE $${args.length} OR c.title ILIKE $${args.length})`);
  }
  addChannelStatusFilter(where, args, channelStatus);
  if (agentStatus) {
    args.push(agentStatus);
    where.push(`c.agent_status = $${args.length}`);
  }
  if (finalStatus) {
    args.push(finalStatus);
    where.push(`COALESCE(fp.status, 'pending') = $${args.length}`);
  }
  if (!finalStatus && channelStatus === "active") {
    where.push(MIGRATION_FINALIZED_SQL);
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const total = await db(`
    SELECT count(*)::int AS total
    FROM crawler.channels c
    LEFT JOIN crawler.finalized_profiles fp ON fp.channel_id = c.channel_id
    ${whereSql}
  `, args);
  const stats = await db(`
    SELECT
      count(*)::bigint AS total,
      count(*) FILTER (
        WHERE c.status = 'active'
          AND fp.status IN ('ready_auto','ready_partial')
      )::bigint AS active,
      count(*) FILTER (
        WHERE c.status = 'active'
          AND COALESCE(fp.status,'pending') NOT IN ('ready_auto','ready_partial')
      )::bigint AS migration_pending,
      count(*) FILTER (
        WHERE c.status='dormant'
           OR (c.status='rejected' AND c.reject_reason='${DORMANT_REASON}')
      )::bigint AS dormant,
      count(*) FILTER (
        WHERE c.status='rejected'
          AND c.reject_reason IS DISTINCT FROM '${DORMANT_REASON}'
      )::bigint AS rejected,
      count(*) FILTER (WHERE c.status = 'removed')::bigint AS removed,
      count(*) FILTER (WHERE c.agent_status = 'done')::bigint AS agent_done,
      count(*) FILTER (WHERE fp.status = 'ready_auto')::bigint AS final_done
    FROM crawler.channels c
    LEFT JOIN crawler.finalized_profiles fp ON fp.channel_id = c.channel_id
  `);

  return {
    channels: await channelSummaryRows({ limit, offset, search, channelStatus, agentStatus, finalStatus }),
    total: total.rows[0]?.total ?? 0,
    stats: stats.rows[0] || { total: 0, agent_done: 0, final_done: 0 },
    filters: { limit, offset, search, channelStatus, agentStatus, finalStatus },
    notice: req.query.notice || "",
    error: req.query.error || "",
  };
}

async function queryDashboardData(req) {
  const limit = intValue(req.query.limit, 50, 1, 200);
  const offset = intValue(req.query.offset, 0, 0, 1_000_000);
  const setId = intValue(req.query.set_id, 0, 0, 1_000_000_000);
  const search = String(req.query.q || "").trim();
  const language = String(req.query.language || "").trim();
  const country = String(req.query.country || "").trim();
  const scheduler = await ensureQueryScheduler();
  const effectiveSetId = scheduler.query_set_id ?? (setId > 0 ? setId : null);

  const args = [effectiveSetId, scheduler.query_quality_min_score];
  const where = [queryDuePredicate("qt", "$1", "$2")];
  if (search) {
    args.push(`%${search}%`);
    where.push(`qt.query_text ILIKE $${args.length}`);
  }
  if (language) {
    args.push(language);
    where.push(`COALESCE(qt.language, '') = $${args.length}`);
  }
  if (country) {
    args.push(country);
    where.push(`COALESCE(qt.country, '') = $${args.length}`);
  }
  const whereSql = `WHERE ${where.join(" AND ")}`;

  const sets = await db(`
    SELECT
      qs.query_set_id,
      qs.name,
      COALESCE(qs.description, '') AS description,
      qs.status,
      count(qt.query_id)::bigint AS term_count,
      count(qt.query_id) FILTER (
        WHERE qt.quality_score IS NOT NULL
          AND qt.quality_status NOT IN ('unscored', 'failed')
          AND COALESCE(qt.quality_score, 0) >= $1::numeric
      )::bigint AS eligible_count,
      qs.updated_at
    FROM crawler.query_sets qs
    LEFT JOIN crawler.query_terms qt ON qt.query_set_id = qs.query_set_id
    WHERE qs.status <> 'archived'
    GROUP BY qs.query_set_id
    ORDER BY qs.query_set_id ASC
  `, [scheduler.query_quality_min_score]);

  const total = await db(`SELECT count(*)::int AS total FROM crawler.query_terms qt ${whereSql}`, args);
  const terms = await db(`
    SELECT
      qt.query_id,
      COALESCE(qt.query_set_id, 0) AS query_set_id,
      COALESCE(qs.name, '') AS query_set_name,
      qt.query_text,
      COALESCE(qt.language, '') AS language,
      COALESCE(qt.country, '') AS country,
      COALESCE(qt.category, '') AS category,
      qt.priority,
      qt.quality_score,
      qt.quality_status,
      qt.quality_checked_at,
      qt.next_crawl_at,
      qt.updated_at
    FROM crawler.query_terms qt
    LEFT JOIN crawler.query_sets qs ON qs.query_set_id = qt.query_set_id
    ${whereSql}
    ORDER BY qt.priority DESC, qt.quality_score DESC, qt.query_id ASC
    LIMIT $${args.length + 1} OFFSET $${args.length + 2}
  `, [...args, limit, offset]);

  const totals = sets.rows.reduce(
    (acc, row) => {
      acc.total += Number(row.term_count || 0);
      acc.eligible += Number(row.eligible_count || 0);
      return acc;
    },
    { total: 0, eligible: 0, due: 0 },
  );
  const discoverCounts = await queues["youtube-discover-page"].getJobCounts(
    "waiting",
    "active",
    "delayed",
    "prioritized",
    "paused",
  );
  const schedulerWorkCounts = await querySchedulerWorkCounts(scheduler.query_set_id, scheduler.query_quality_min_score);
  totals.eligible = schedulerWorkCounts.eligible;
  totals.due = schedulerWorkCounts.due;

  return {
    sets: sets.rows,
    terms: terms.rows,
    total: total.rows[0]?.total ?? 0,
    totals,
    scheduler,
    discoverCounts,
    scheduler_due_count: schedulerWorkCounts.due,
    resumable_count: schedulerWorkCounts.resumable,
    filters: { limit, offset, setId, search, language, country },
    notice: req.query.notice || "",
    error: req.query.error || "",
  };
}

function layout({ title, active, body, mainClass = "" }) {
  const embedded = String(active || "").startsWith("external-");
  const mainClasses = ["main", embedded ? "embed-main" : "", mainClass].filter(Boolean).join(" ");
  const nav = [
    ["/queries", "Query 词库", "queries"],
    ["/daily-clocks", "每日 Clock", "daily-clocks"],
    ["/channels", "频道列表", "channels"],
    ["/migration-channels", "迁移频道列表", "migration-channels"],
  ];
  const externalNav = [
    ["/external/queues", "队列监控", "external-queues"],
    ["/external/minio", "新 MinIO", "external-minio"],
    ["/external/proxy", "IP 代理池", "external-proxy"],
  ];
  const configNav = [
    ["/agent", "Agent 配置", "agent"],
    ["/crawler", "爬虫配置", "crawler"],
    ["/youtube-api", "YouTube API", "youtube-api"],
  ];
  return `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${h(title)} - Crawler Dashboard</title>
  <style>
    :root { --bg:#e9eeeb; --shell:#f6f8f7; --panel:#fff; --line:#dde8e1; --soft:#f7faf8; --text:#101713; --muted:#738077; --primary:#127849; --dark:#0b4d32; --red:#d84343; --amber:#dda126; --blue:#2555d8; }
    * { box-sizing: border-box; }
    html, body { width:100%; height:100%; overflow:hidden; }
    body { margin:0; min-height:100vh; background:var(--bg); color:var(--text); font:14px/1.42 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif; }
    a { color:inherit; text-decoration:none; }
    .shell { width:min(1760px, calc(100% - 48px)); height:calc(100vh - 48px); margin:24px auto; padding:18px; display:flex; gap:18px; background:rgba(255,255,255,.82); border-radius:28px; overflow:hidden; box-shadow:0 18px 42px rgba(20,35,28,.08); }
    .side { width:266px; flex:0 0 auto; background:#fff; border-radius:18px; padding:24px 18px; overflow:auto; }
    .brand { display:flex; align-items:center; gap:10px; padding:8px 12px 28px; }
    .mark { width:40px; height:40px; border-radius:999px; border:3px solid var(--primary); color:var(--primary); display:grid; place-items:center; font-weight:850; font-size:11px; }
    .brand-title { font-size:18px; font-weight:850; }
    .brand-sub { color:var(--muted); font-size:12px; margin-top:3px; }
    .nav-label { padding:0 12px 12px; color:#8f9994; font-size:12px; font-weight:800; text-transform:uppercase; }
    .nav { display:grid; gap:6px; margin-top:24px; }
    .nav a { min-height:44px; border-radius:8px; display:flex; align-items:center; gap:10px; padding:10px 12px; color:#8f9b95; font-size:15px; font-weight:700; }
    .nav a:hover, .nav a.active { background:#eef7f2; color:var(--text); }
    .logout-form { margin:24px 12px 0; }
    .logout-button { width:100%; min-height:42px; border:1px solid var(--line); border-radius:8px; background:#fff; color:var(--muted); font:inherit; font-weight:750; cursor:pointer; }
    .logout-button:hover { border-color:var(--primary); color:var(--dark); background:#eef7f2; }
    .dot { width:9px; height:9px; border-radius:3px; background:#a8b4ae; }
    .active .dot { background:var(--primary); box-shadow:0 0 0 4px rgba(18,120,73,.12); }
    .main { flex:1; min-width:0; height:100%; padding:24px; background:var(--shell); border-radius:20px; overflow:auto; }
    .main.embed-main { overflow:hidden; display:flex; flex-direction:column; }
    .main.agent-main { overflow:hidden; display:flex; flex-direction:column; }
    .agent-main > .topbar,.agent-main > .alert,.agent-main > .grid { flex:0 0 auto; }
    .agent-main > .table-panel { flex:1 1 0; min-height:0; display:flex; flex-direction:column; overflow:hidden; }
    .agent-main > .table-panel .table-tools { flex:0 0 auto; }
    .agent-main > .table-panel .table-scroll { flex:1 1 auto; min-height:0; overflow:auto; }
    .agent-main > .table-panel thead th { position:sticky; top:0; z-index:2; }
    .topbar { min-height:86px; display:flex; align-items:center; justify-content:space-between; gap:18px; margin-bottom:20px; }
    .eyebrow { color:var(--primary); font-size:12px; font-weight:850; text-transform:uppercase; margin-bottom:6px; }
    h1,h2,h3 { margin:0; letter-spacing:0; }
    h1 { font-size:34px; line-height:1.08; font-weight:850; }
    h2 { font-size:18px; }
    h3 { font-size:14px; }
    .sub { margin-top:10px; color:var(--muted); max-width:860px; font-size:15px; }
    .toolbar { display:flex; gap:10px; align-items:center; flex-wrap:wrap; justify-content:flex-end; }
    .btn { display:inline-flex; align-items:center; justify-content:center; gap:8px; min-height:42px; padding:10px 16px; border:1px solid rgba(18,120,73,.45); border-radius:8px; background:#fff; color:var(--dark); font-weight:750; cursor:pointer; white-space:nowrap; }
    .btn:hover { background:#e8f5ee; border-color:var(--primary); }
    .btn:disabled { opacity:.5; cursor:not-allowed; background:#f1f5f3; border-color:var(--line); color:var(--muted); }
    .btn-primary { background:linear-gradient(135deg,var(--dark),var(--primary)); color:#fff; border-color:var(--primary); }
    .btn-danger { border-color:var(--red); color:#fff; background:var(--red); }
    .grid { display:grid; gap:14px; }
    .grid-2 { grid-template-columns:repeat(2,minmax(0,1fr)); }
    .grid-3 { grid-template-columns:repeat(3,minmax(0,1fr)); }
    .grid-4 { grid-template-columns:repeat(4,minmax(0,1fr)); }
    .panel,.metric,.table-panel { min-width:0; background:var(--panel); border:1px solid rgba(227,236,231,.85); border-radius:8px; box-shadow:0 8px 22px rgba(20,35,28,.06); }
    .panel { padding:20px; }
    .metric { padding:20px 22px; min-height:130px; display:flex; flex-direction:column; justify-content:space-between; }
    .metric-label { font-size:15px; font-weight:750; }
    .metric-value { font-size:42px; font-weight:850; line-height:1.02; margin-top:20px; }
    .metric-foot,.muted,.note { color:var(--muted); font-size:13px; }
    .metric-blue { background:linear-gradient(135deg,#0a4a30,#16864f); color:#fff; }
    .metric-blue .metric-foot { color:#c7eca7; }
    .panel-head { display:flex; justify-content:space-between; align-items:center; gap:12px; padding-bottom:16px; margin-bottom:16px; border-bottom:1px solid #edf3ef; }
    .field { display:grid; gap:6px; min-width:0; }
    label { color:var(--muted); font-size:12px; font-weight:800; }
    input,select,textarea { width:100%; min-height:42px; padding:9px 12px; border:1px solid var(--line); border-radius:8px; background:#fff; color:var(--text); font:inherit; }
    .check-row { min-height:42px; display:flex; align-items:center; gap:10px; color:var(--text); font-weight:750; }
    .check-row input { width:auto; min-height:0; }
    textarea { min-height:120px; resize:vertical; }
    .form-grid { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; align-items:end; }
    .form-grid-5 { display:grid; grid-template-columns:1fr 1.5fr .7fr .7fr .7fr; gap:12px; align-items:end; }
    .filters { display:grid; grid-template-columns:minmax(220px,1.4fr) minmax(150px,.8fr) minmax(120px,.6fr) minmax(120px,.6fr) minmax(100px,.5fr) auto; gap:12px; align-items:end; }
    .migration-filters { grid-template-columns:minmax(240px,1fr) minmax(100px,.25fr) auto; }
    .batch-migration-form { display:flex; align-items:flex-end; gap:8px; }
    .batch-migration-form .field { min-width:112px; }
    .migration-table-scroll { max-height:760px; }
    .migration-table-scroll thead th { position:sticky; top:0; z-index:1; }
    .migration-channel { display:flex; align-items:flex-start; gap:10px; min-width:250px; }
    .migration-avatar { width:42px; height:42px; flex:0 0 42px; border-radius:50%; object-fit:cover; border:1px solid var(--line); background:#f4f7f5; }
    .migration-avatar-empty { display:grid; place-items:center; color:var(--muted); font-size:11px; font-weight:800; }
    .query-filters { grid-template-columns:minmax(220px,1.4fr) minmax(150px,.8fr) minmax(120px,.6fr) minmax(120px,.6fr) auto; }
    .pager-form { display:flex; align-items:center; gap:8px; }
    .pager-form label { white-space:nowrap; }
    .pager-form select { width:auto; min-width:82px; min-height:32px; padding:5px 28px 5px 10px; }
    .table-scroll { overflow:auto; }
    table { width:100%; border-collapse:collapse; }
    th,td { padding:12px 16px; border-bottom:1px solid #edf3ef; text-align:left; vertical-align:middle; }
    th { color:var(--muted); background:var(--soft); font-size:13px; font-weight:850; white-space:nowrap; }
    .query-select-cell { width:48px; padding-left:14px; padding-right:8px; text-align:center; }
    .query-select { width:16px; height:16px; min-height:0; padding:0; accent-color:var(--primary); cursor:pointer; }
    .channel-select-cell { width:48px; padding-left:14px; padding-right:8px; text-align:center; }
    .channel-select { width:16px; height:16px; min-height:0; padding:0; accent-color:var(--primary); cursor:pointer; }
    .comparison-ratio { min-width:112px; font-variant-numeric:tabular-nums; }
    .comparison-ratio strong { font-size:16px; }
    .comparison-issues { min-width:250px; }
    .comparison-issues summary { color:var(--dark); font-weight:800; cursor:pointer; }
    .comparison-issues ul { margin:10px 0 0; padding-left:18px; color:var(--muted); }
    .comparison-issues li + li { margin-top:5px; }
    .query-row-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .daily-clock-filters { grid-template-columns:minmax(220px,1.4fr) minmax(130px,.65fr) minmax(150px,.75fr) minmax(100px,.45fr) auto; }
    .clock-mask { display:flex; align-items:center; gap:6px; flex-wrap:wrap; min-width:260px; }
    .clock-chip { display:inline-flex; align-items:center; gap:5px; min-height:28px; padding:4px 8px; border:1px solid; border-radius:6px; font-size:12px; font-weight:800; white-space:nowrap; }
    .clock-about { color:#1d4f9b; background:#eef5ff; border-color:#b8d1f3; }
    .clock-video { color:#8a5700; background:#fff8e8; border-color:#efd08c; }
    .clock-agent { color:#9f2f3d; background:#fff1f3; border-color:#efb7bf; }
    .clock-chip-time { font-weight:650; opacity:.8; }
    .channel-clock-times { display:grid; gap:5px; min-width:390px; }
    .channel-clock-line { display:grid; grid-template-columns:62px minmax(230px,1fr) auto; align-items:center; gap:7px; }
    .channel-clock-line .clock-chip { min-width:62px; justify-content:center; }
    .channel-clock-time-stack { display:grid; gap:2px; min-width:0; }
    .channel-clock-at { white-space:nowrap; font-size:12px; }
    .channel-clock-last { white-space:nowrap; font-size:11px; }
    .query-delete-modal { width:min(620px, calc(100vw - 48px)); }
    .query-delete-list { max-height:180px; margin:12px 0 0; padding-left:22px; overflow:auto; color:var(--muted); overflow-wrap:anywhere; }
    .agent-test-cell { min-width:210px; max-width:280px; }
    .agent-test-head { display:flex; align-items:center; gap:8px; }
    .agent-test-preview { min-height:18px; max-width:260px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .agent-status-toggle { font:inherit; cursor:pointer; }
    .agent-status-toggle:disabled { cursor:not-allowed; opacity:.6; }
    .num { text-align:right; font-variant-numeric:tabular-nums; }
    .mono { font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace; }
    .pill { display:inline-flex; align-items:center; justify-content:center; min-height:24px; padding:3px 9px; border-radius:999px; font-size:12px; font-weight:800; border:1px solid transparent; white-space:nowrap; }
    .good { color:#0b4d32; background:#eef9f3; border-color:#b7e7ca; }
    .warn { color:#92400e; background:#fffbeb; border-color:#fde68a; }
    .bad { color:#b91c1c; background:#fef2f2; border-color:#fecaca; }
    .muted-pill { color:#475569; background:#f1f5f9; border-color:#dbe3ed; }
    .alert { padding:12px 14px; border-radius:8px; border:1px solid var(--line); background:#fff; color:var(--text); font-weight:700; margin-bottom:16px; }
    .alert-good { border-color:#b7e7ca; background:#eef9f3; color:#0b4d32; }
    .alert-bad { border-color:#fecaca; background:#fef2f2; color:#991b1b; }
    .mt { margin-top:20px; }
    .inline-form { display:inline; }
    .small-btn { min-height:32px; padding:6px 10px; font-size:12px; }
    .json { min-height:220px; font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace; font-size:12px; white-space:pre-wrap; }
    .modal { width:min(920px, calc(100vw - 48px)); max-height:calc(100vh - 72px); padding:0; border:1px solid var(--line); border-radius:10px; background:#fff; color:var(--text); box-shadow:0 24px 70px rgba(16,23,19,.24); }
    .modal-wide { width:min(1320px, calc(100vw - 48px)); }
    .modal::backdrop { background:rgba(16,23,19,.42); }
    .modal-head { display:flex; align-items:center; justify-content:space-between; gap:16px; padding:20px; border-bottom:1px solid #edf3ef; }
    .modal-body { padding:20px; }
    .modal-body-scroll { max-height:calc(100vh - 190px); overflow:auto; }
    .modal-actions { display:flex; justify-content:flex-end; gap:10px; padding:16px 20px 20px; }
    .table-tools { padding:20px; border-bottom:1px solid #edf3ef; }
    .table-tools .panel-head { padding:0 0 16px; margin:0 0 16px; }
    .detail-page { min-width:0; overflow-x:hidden; }
    .detail-page .table-scroll { overflow-x:hidden; }
    .detail-page table { table-layout:fixed; }
    .detail-page th,.detail-page td { white-space:normal; overflow-wrap:anywhere; }
    .detail-page .json { overflow-wrap:anywhere; }
    .detail-overview-grid { grid-template-columns:minmax(0,1.4fr) minmax(300px,.6fr); }
    .detail-overview-grid > .panel { display:flex; flex-direction:column; }
    .detail-overview-grid > .panel > .table-scroll,
    .detail-overview-grid > .panel > .json { flex:1 1 auto; min-height:0; }
    .detail-overview-grid textarea.json { height:auto; resize:vertical; }
    .source-table th:nth-child(1) { width:22%; }
    .source-table th:nth-child(2) { width:20%; }
    .source-table th:nth-child(3) { width:24%; }
    .source-table th:nth-child(4) { width:34%; }
    .stat-table th:not(:first-child), .stat-table td:not(:first-child) { text-align:right; }
    .detail-page .about-snapshot-scroll { overflow-x:auto; }
    .about-snapshot-table { min-width:1000px; }
    .about-snapshot-table th:nth-child(1) { width:16%; }
    .about-snapshot-table th:nth-child(2),
    .about-snapshot-table th:nth-child(3),
    .about-snapshot-table th:nth-child(4) { width:11%; }
    .about-snapshot-table th:nth-child(5) { width:14%; }
    .about-snapshot-table th:nth-child(6) { width:12%; }
    .about-snapshot-table th:nth-child(7) { width:25%; }
    .content-table th:nth-child(1) { width:8%; }
    .content-table th:nth-child(2) { width:34%; }
    .content-table th:nth-child(3) { width:18%; }
    .content-table th:nth-child(4) { width:10%; }
    .content-table th:nth-child(5) { width:10%; }
    .content-table th:nth-child(6) { width:10%; }
    .content-table th:nth-child(7) { width:10%; }
    .embed-page { height:100%; min-height:0; display:flex; flex-direction:column; }
    .embed-page .topbar { flex:0 0 auto; min-height:72px; margin-bottom:14px; }
    .iframe-panel { height:calc(100vh - 196px); min-height:640px; padding:0; overflow:hidden; }
    .embed-page .iframe-panel { flex:1 1 auto; height:auto; min-height:0; }
    .embed-frame { width:100%; height:100%; border:0; display:block; background:#fff; }
    @media (max-width:1100px) {
      html,body { height:auto; min-height:100%; overflow:auto; }
      .shell { width:100%; height:auto; min-height:100vh; margin:0; padding:0; border-radius:0; display:block; overflow:visible; }
      .side { position:sticky; top:0; z-index:20; width:100%; padding:10px 14px; border-bottom:1px solid var(--line); border-radius:0; display:flex; align-items:center; gap:6px; overflow-x:auto; overflow-y:hidden; }
      .brand { flex:0 0 auto; padding:0 12px 0 0; }
      .mark { width:34px; height:34px; }
      .brand-title { font-size:14px; white-space:nowrap; }
      .brand-sub,.nav-label { display:none; }
      .nav { flex:0 0 auto; display:flex; gap:4px; margin:0; }
      .nav a { min-height:38px; padding:8px 10px; font-size:13px; white-space:nowrap; }
      .logout-form { flex:0 0 auto; margin:0 0 0 4px; }
      .logout-button { min-height:38px; padding:0 12px; white-space:nowrap; }
      .main { height:auto; min-height:calc(100vh - 58px); padding:18px 16px; border-radius:0; overflow:visible; }
      .main.embed-main { min-height:calc(100vh - 58px); overflow:hidden; }
      .topbar { align-items:flex-start; flex-direction:column; }
      .topbar .toolbar { justify-content:flex-start; }
      h1 { font-size:28px; }
      .grid-2,.grid-3,.grid-4,.detail-overview-grid,.form-grid,.form-grid-5,.filters { grid-template-columns:1fr; }
      table { min-width:900px; }
      .detail-page table { min-width:0; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="side">
      <div class="brand">
        <div class="mark">CD</div>
        <div><div class="brand-title">Crawler Dashboard</div><div class="brand-sub">BullMQ 新爬虫后台</div></div>
      </div>
      <nav class="nav">
        <div class="nav-label">控制台</div>
        ${nav.map(([href, label, key]) => `<a class="${active === key ? "active" : ""}" href="${href}"><span class="dot"></span><span>${label}</span></a>`).join("")}
      </nav>
      <nav class="nav">
        <div class="nav-label">外部链接</div>
        ${externalNav.map(([href, label, key]) => `<a class="${active === key ? "active" : ""}" href="${href}"><span class="dot"></span><span>${label}</span></a>`).join("")}
      </nav>
      <nav class="nav">
        <div class="nav-label">配置项</div>
        ${configNav.map(([href, label, key]) => `<a class="${active === key ? "active" : ""}" href="${href}"><span class="dot"></span><span>${label}</span></a>`).join("")}
      </nav>
      <form class="logout-form" method="post" action="/auth/logout">
        <button class="logout-button" type="submit">退出登录</button>
      </form>
    </aside>
    <main class="${h(mainClasses)}">${body}</main>
  </div>
<script>
  function openModal(id) {
    const modal = document.getElementById(id);
    if (modal && typeof modal.showModal === "function") {
      modal.showModal();
      const input = modal.querySelector("input:not([type='hidden']), textarea, select");
      if (input) input.focus();
    }
  }
  document.addEventListener("click", (event) => {
    if (event.target && event.target.tagName === "DIALOG") event.target.close();
  });
</script>
</body>
</html>`;
}

function redirectWith(req, res, params) {
  const back = new URL(req.get("referer") || "/queries", "http://local");
  back.searchParams.delete("notice");
  back.searchParams.delete("error");
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && String(value) !== "") {
      back.searchParams.set(key, String(value));
    }
  }
  res.redirect(303, back.pathname + back.search);
}

function queryPage(data) {
  const { sets, terms, filters, totals, scheduler, discoverCounts } = data;
  const prev = new URLSearchParams();
  const next = new URLSearchParams();
  for (const params of [prev, next]) {
    if (filters.search) params.set("q", filters.search);
    if (filters.setId) params.set("set_id", String(filters.setId));
    if (filters.language) params.set("language", filters.language);
    if (filters.country) params.set("country", filters.country);
    params.set("limit", String(filters.limit));
  }
  prev.set("offset", String(Math.max(0, filters.offset - filters.limit)));
  next.set("offset", String(filters.offset + filters.limit));
  const pageStart = data.total === 0 ? 0 : filters.offset + 1;
  const pageEnd = Math.min(filters.offset + filters.limit, data.total);
  const limitOptions = [10, 20, 50, 100]
    .map((value) => `<option value="${value}" ${Number(filters.limit) === value ? "selected" : ""}>${value}</option>`)
    .join("");
  const paginationHiddenInputs = `
    <input type="hidden" name="q" value="${h(filters.search)}">
    <input type="hidden" name="set_id" value="${h(filters.setId || 0)}">
    <input type="hidden" name="language" value="${h(filters.language)}">
    <input type="hidden" name="country" value="${h(filters.country)}">
    <input type="hidden" name="offset" value="0">`;
  const setOptions = sets.map((set) => `<option value="${h(set.query_set_id)}" ${filters.setId === Number(set.query_set_id) ? "selected" : ""}>${h(set.name)}</option>`).join("");
  const activeSetOptions = sets
    .filter((set) => set.status === "active")
    .map((set) => `<option value="${h(set.query_set_id)}">${h(set.name)}</option>`)
    .join("");
  const schedulerSetOptions = [
    `<option value="" ${scheduler.query_set_id ? "" : "selected"}>全部分组</option>`,
    ...sets
      .filter((set) => set.status === "active")
      .map((set) => `<option value="${h(set.query_set_id)}" ${Number(scheduler.query_set_id) === Number(set.query_set_id) ? "selected" : ""}>${h(set.name)}</option>`),
  ].join("");
  const discoverBacklog = ["waiting", "active", "delayed", "prioritized"]
    .reduce((sum, key) => sum + Number(discoverCounts?.[key] || 0), 0);
  const schedulerSetName = scheduler.query_set_id
    ? (sets.find((set) => Number(set.query_set_id) === Number(scheduler.query_set_id))?.name || `#${scheduler.query_set_id}`)
    : "全部分组";
  const listScopeLabel = scheduler.query_set_id
    ? schedulerSetName
    : filters.setId
    ? (sets.find((set) => Number(set.query_set_id) === Number(filters.setId))?.name || `#${filters.setId}`)
    : schedulerSetName;
  const querySetFilterHtml = scheduler.query_set_id
    ? `<div class="field"><label>分组</label><div class="mono">${h(schedulerSetName)}</div><input type="hidden" name="set_id" value="0"></div>`
    : `<div class="field"><label>分组</label><select name="set_id"><option value="0">全部分组</option>${setOptions}</select></div>`;
  const schedulerStatusText = scheduler.stop_reason === "parser_contract_error"
    ? "解析器错误待修复"
    : scheduler.status === "running"
      ? "Discover 运行中"
      : scheduler.status === "finishing"
      ? "自动收尾中"
      : scheduler.status === "repairing"
        ? "自动补缺中"
        : scheduler.status === "paused"
          ? "已暂停"
          : scheduler.stop_reason === "pipeline_complete"
            ? "已完成"
            : "已结束";
  const schedulerConfigSummary = `
    <div class="form-grid">
      <div class="field"><label>运行分组</label><div class="mono">${h(schedulerSetName)}</div></div>
      <div class="field"><label>Query 质量分</label><div class="mono">>= ${fmtInt(scheduler.query_quality_min_score)}</div></div>
      <div class="field"><label>每次切片 Query 数</label><div class="mono">${fmtInt(scheduler.chunk_size)}</div></div>
      <div class="field"><label>Discover 队列上限</label><div class="mono">${fmtInt(scheduler.max_discover_backlog)}</div></div>
      <div class="field"><label>当前状态</label><div><span class="pill ${statusClass(scheduler.status)}">${h(schedulerStatusText)}</span></div></div>
    </div>`;
  const schedulerControlHtml = scheduler.status === "stopped"
    ? `<form id="scheduler-start-form" method="post" action="/queries/scheduler/start" class="form-grid">
        <div class="field"><label>运行分组</label><select name="query_set_id" data-summary="set">${schedulerSetOptions}</select></div>
        <div class="field"><label>Query 质量分</label><input name="query_quality_min_score" data-summary="quality" type="number" min="0" max="100" step="1" value="${h(scheduler.query_quality_min_score)}"></div>
        <div class="field"><label>每次切片 Query 数</label><input name="chunk_size" type="number" min="1" max="100" value="${h(scheduler.chunk_size)}"></div>
        <div class="field"><label>Discover 队列上限</label><input name="max_discover_backlog" data-summary="max-discover" type="number" min="1" max="20" value="${h(scheduler.max_discover_backlog)}"></div>
        <button class="btn btn-primary" type="submit">开始</button>
      </form>`
    : `${schedulerConfigSummary}
      <div class="toolbar mt" style="justify-content:flex-start;">
        ${scheduler.status === "paused"
          ? `<form method="post" action="/queries/scheduler/resume" class="inline-form"><button class="btn btn-primary" type="submit">继续</button></form>`
          : `<form method="post" action="/queries/scheduler/pause" class="inline-form"><button class="btn" type="submit">暂停</button></form>`}
        <form method="post" action="/queries/scheduler/stop" class="inline-form"><button class="btn btn-danger" type="submit">结束</button></form>
      </div>`;

  return layout({
    title: "Query 词库",
    active: "queries",
    body: `
<div class="topbar">
  <div>
    <div class="eyebrow">Discovery Library</div>
    <h1>Query 词库</h1>
    <div class="sub">管理新爬虫发现词库，只写入 crawler.query_terms。</div>
  </div>
  <div class="toolbar">
    <a class="btn" href="/queries">刷新</a>
    <button class="btn" type="button" onclick="openModal('set-modal')">新增分组</button>
    <button class="btn" type="button" onclick="openModal('query-modal')">新增 Query</button>
    <button class="btn" type="button" onclick="openModal('import-modal')">批量导入</button>
  </div>
</div>
${data.notice ? `<div class="alert alert-good">${h(data.notice)}</div>` : ""}
${data.error ? `<div class="alert alert-bad">${h(data.error)}</div>` : ""}

<section class="grid grid-3">
  <div class="metric metric-blue"><div><div class="metric-label">Due Query</div><div class="metric-value" data-metric-value="due">${fmtInt(totals.due)}</div></div><div class="metric-foot">可立即调度</div></div>
  <div class="metric"><div><div class="metric-label">达标 Query</div><div class="metric-value" data-metric-value="eligible">${fmtInt(totals.eligible)}</div></div><div class="metric-foot">质量分 >= <span data-metric-value="quality">${fmtInt(scheduler.query_quality_min_score)}</span></div></div>
  <div class="metric"><div><div class="metric-label">Query 总数</div><div class="metric-value">${fmtInt(totals.total)}</div></div><div class="metric-foot">当前可立即调度 ${fmtInt(data.total)} 条 · ${fmtInt(sets.length)} 个分组</div></div>
</section>

<section class="panel mt">
  <div class="panel-head">
    <div>
      <h2>Query 调度控制</h2>
      <div class="note">开始后自动完成 Discover、频道抓取、当前批次审计补缺、Agent 和 Finalize；暂停/结束保留现场。</div>
    </div>
    <span class="pill ${statusClass(scheduler.status)}">${h(schedulerStatusText)}</span>
  </div>
  ${schedulerControlHtml}
  <div id="scheduler-summary" class="note mt">
    当前范围：<span data-summary-value="set">${h(schedulerSetName)}</span>
    · Discover backlog <span data-summary-value="discover">${fmtInt(discoverBacklog)}</span> / <span data-summary-value="max-discover">${fmtInt(scheduler.max_discover_backlog)}</span>
    · Query 质量 >= <span data-summary-value="quality">${fmtInt(scheduler.query_quality_min_score)}</span>
    · 达标 <span data-summary-value="eligible">${fmtInt(totals.eligible)}</span>
    · 当前范围 due <span data-summary-value="due">${fmtInt(data.scheduler_due_count)}</span>
    · 待恢复 continuation <span data-summary-value="resumable">${fmtInt(data.resumable_count)}</span>
    · 更新时间（北京时间） <span data-summary-value="updated">${timeText(scheduler.updated_at)}</span>
  </div>
</section>

<script>
(() => {
  const form = document.getElementById("scheduler-start-form");
  const summary = document.getElementById("scheduler-summary");
  if (!form || !summary) return;
  const setInput = form.elements.query_set_id;
  const qualityInput = form.elements.query_quality_min_score;
  const chunkInput = form.elements.chunk_size;
  const maxInput = form.elements.max_discover_backlog;
  const text = (key, value) => {
    const node = summary.querySelector('[data-summary-value="' + key + '"]');
    if (node) node.textContent = value;
  };
  const fmt = (value) => new Intl.NumberFormat("en-US").format(Number(value || 0));
  const bounded = (value, low, high) => Math.max(low, Math.min(high, Math.floor(Number(value || 0))));
  let seq = 0;
  let timer = null;
  function metric(key, value) {
    const node = document.querySelector('[data-metric-value="' + key + '"]');
    if (node) node.textContent = value;
  }
  function setLoadingState(loading, reloadList) {
    if (!loading) return;
    text("eligible", "更新中...");
    text("due", "更新中...");
    text("resumable", "更新中...");
    text("updated", "更新中...");
    metric("eligible", "...");
    metric("due", "...");
    if (!reloadList) return;
    const listStatus = document.querySelector("[data-query-list-status]");
    if (listStatus) listStatus.textContent = "正在重新加载可调度 Query...";
    const listBody = document.querySelector("[data-query-list-body]");
    if (listBody) {
      listBody.style.opacity = "0.5";
      listBody.style.pointerEvents = "none";
    }
  }
  function payload() {
    const quality = bounded(qualityInput.value, 0, 100);
    const maxDiscover = bounded(maxInput.value, 1, 20);
    return {
      query_set_id: setInput.value || null,
      query_quality_min_score: quality,
      chunk_size: bounded(chunkInput.value, 1, 100),
      max_discover_backlog: maxDiscover,
    };
  }
  function updateLocalSummary() {
    const data = payload();
    const selected = setInput.options[setInput.selectedIndex];
    text("set", selected ? selected.textContent : "全部分组");
    text("quality", fmt(data.query_quality_min_score));
    text("max-discover", fmt(data.max_discover_backlog));
    updateQueryRows(data.query_quality_min_score);
    metric("quality", fmt(data.query_quality_min_score));
  }
  function updatePill(node, label, className, title) {
    node.className = "pill query-schedule-pill " + className;
    node.textContent = label;
    node.title = title;
  }
  function updateQueryRows(minScore) {
    const now = Date.now();
    document.querySelectorAll(".query-schedule-pill").forEach((node) => {
      const score = Number(node.dataset.qualityScore || "NaN");
      const qualityStatus = String(node.dataset.qualityStatus || "");
      if (!Number.isFinite(score) || qualityStatus === "unscored" || qualityStatus === "failed") {
        updatePill(node, "未评分", "bad", qualityStatus || "unscored");
        return;
      }
      if (score < minScore) {
        updatePill(node, "分数不足", "warn", "quality_score " + score.toFixed(2) + " < " + minScore);
        return;
      }
      const nextText = node.dataset.nextCrawlAt || "";
      const nextMs = nextText ? Date.parse(nextText) : NaN;
      if (Number.isFinite(nextMs) && nextMs > now) {
        updatePill(node, "待到期", "muted-pill", "next_crawl_at " + nextText.replace("T", " ").slice(0, 19));
        return;
      }
      updatePill(node, "可调度", "good", "quality_score " + score.toFixed(2) + " >= " + minScore);
    });
  }
  let lastSaved = JSON.stringify(payload());
  async function refreshSummary() {
    window.clearTimeout(timer);
    updateLocalSummary();
    const data = payload();
    const serialized = JSON.stringify(data);
    if (serialized === lastSaved) return;
    const previous = JSON.parse(lastSaved);
    const shouldReloadList = previous.query_set_id !== data.query_set_id
      || previous.query_quality_min_score !== data.query_quality_min_score;
    const currentSeq = ++seq;
    setLoadingState(true, shouldReloadList);
    try {
      const response = await fetch("/queries/scheduler/draft", {
        method: "POST",
        headers: { accept: "application/json", "content-type": "application/json" },
        body: serialized,
      });
      const payload = await response.json();
      if (currentSeq !== seq) return;
      if (!payload.ok) throw new Error(payload.error || "scheduler draft update failed");
      lastSaved = serialized;
      text("discover", fmt(payload.discover_backlog));
      text("eligible", fmt(payload.eligible));
      text("due", fmt(payload.due));
      text("resumable", fmt(payload.resumable));
      text("updated", payload.updated_at || "-");
      metric("eligible", fmt(payload.eligible));
      metric("due", fmt(payload.due));
      metric("quality", fmt(data.query_quality_min_score));
      if (shouldReloadList) {
        const url = new URL(window.location.href);
        url.searchParams.set("offset", "0");
        window.location.assign(url.pathname + url.search);
      }
    } catch {
      if (currentSeq !== seq) return;
      text("updated", "刷新失败");
      metric("eligible", "!");
      metric("due", "!");
      if (shouldReloadList) {
        const listStatus = document.querySelector("[data-query-list-status]");
        if (listStatus) listStatus.textContent = "可调度 Query 刷新失败";
      }
    }
  }
  function scheduleRefresh() {
    updateLocalSummary();
    window.clearTimeout(timer);
    timer = window.setTimeout(refreshSummary, 650);
  }
  function commitOnEnter(event) {
    if (event.key === "Enter") {
      event.preventDefault();
      event.currentTarget.blur();
    }
  }
  for (const input of [qualityInput, chunkInput, maxInput]) {
    input.addEventListener("input", scheduleRefresh);
    input.addEventListener("blur", refreshSummary);
    input.addEventListener("keydown", commitOnEnter);
  }
  setInput.addEventListener("change", refreshSummary);
  updateLocalSummary();
})();
</script>

<section class="table-panel mt">
  <div class="table-tools">
    <div class="panel-head">
      <div><h2>可立即调度 Query</h2><div class="note" data-query-list-status>当前范围 ${h(listScopeLabel)} · ${fmtInt(pageStart)}-${fmtInt(pageEnd)} / ${fmtInt(data.total)}</div></div>
      <div class="toolbar">
        <span id="query-selected-count" class="note">已选 0 条</span>
        <button id="query-bulk-delete" class="btn btn-danger small-btn" type="button" disabled>批量删除</button>
        <form method="get" action="/queries" class="pager-form">
          ${paginationHiddenInputs}
          <label for="query-page-limit">每页</label>
          <select id="query-page-limit" name="limit" onchange="this.form.submit()">${limitOptions}</select>
        </form>
        ${filters.offset > 0 ? `<a class="btn small-btn" href="/queries?${h(prev.toString())}">上一页</a>` : `<button class="btn small-btn" type="button" disabled>上一页</button>`}
        ${pageEnd < data.total ? `<a class="btn small-btn" href="/queries?${h(next.toString())}">下一页</a>` : `<button class="btn small-btn" type="button" disabled>下一页</button>`}
      </div>
    </div>
    <form method="get" action="/queries" class="filters query-filters">
      <div class="field"><label>搜索</label><input name="q" value="${h(filters.search)}" placeholder="query text"></div>
      ${querySetFilterHtml}
      <div class="field"><label>语言</label><input name="language" value="${h(filters.language)}" placeholder="pt-BR"></div>
      <div class="field"><label>国家</label><input name="country" value="${h(filters.country)}" placeholder="BR"></div>
      <input type="hidden" name="limit" value="${h(filters.limit)}">
      <input type="hidden" name="offset" value="0">
      <button class="btn" type="submit">筛选</button>
    </form>
  </div>
  <div class="table-scroll">
    <table>
      <thead><tr><th class="query-select-cell"><input id="query-select-all" class="query-select" type="checkbox" aria-label="选择当前页全部 Query"></th><th>分组</th><th>Query</th><th>语言 / 国家</th><th>质量</th><th>优先级</th><th>调度判断</th><th>Next Crawl（北京时间）</th><th>操作</th></tr></thead>
      <tbody data-query-list-body>
      ${terms.map((term) => {
        const scheduleState = queryScheduleState(term, scheduler);
        return `
        <tr>
          <td class="query-select-cell"><input class="query-select" type="checkbox" value="${h(term.query_id)}" data-query-select data-query-text="${h(term.query_text)}" aria-label="选择 Query ${h(term.query_text)}"></td>
          <td>${h(term.query_set_name || "-")}</td>
          <td><strong>${h(term.query_text)}</strong></td>
          <td>${h(term.language || "-")} / ${h(term.country || "-")}</td>
          <td>
            <div class="mono">${term.quality_score == null ? "-" : Number(term.quality_score).toFixed(2)}</div>
            <span class="pill ${statusClass(qualityStatusLabel(term.quality_status))}" title="${h(term.quality_status || "unscored")}">${h(qualityStatusLabel(term.quality_status))}</span>
          </td>
          <td class="num">${fmtInt(term.priority)}</td>
          <td><span class="pill ${scheduleState.className} query-schedule-pill" data-quality-score="${h(term.quality_score == null ? "" : Number(term.quality_score).toFixed(2))}" data-quality-status="${h(term.quality_status || "")}" data-next-crawl-at="${h(term.next_crawl_at || "")}" title="${h(scheduleState.title)}">${h(scheduleState.label)}</span></td>
          <td class="mono">${timeText(term.next_crawl_at)}</td>
          <td>
            <div class="query-row-actions">
              <form class="inline-form" method="post" action="/queries/terms/${h(term.query_id)}/due"><button class="btn small-btn" type="submit">设为 due</button></form>
              <button class="btn btn-danger small-btn" type="button" data-query-delete data-query-id="${h(term.query_id)}" data-query-text="${h(term.query_text)}">删除</button>
            </div>
          </td>
        </tr>`;
      }).join("")}
      ${terms.length === 0 ? `<tr><td colspan="9" class="muted">当前没有可立即调度的 Query。</td></tr>` : ""}
      </tbody>
    </table>
  </div>
</section>

<dialog id="query-delete-modal" class="modal query-delete-modal">
  <div class="modal-head">
    <div><h2>确认删除 Query</h2><div class="note">此操作不可撤销</div></div>
    <button class="btn small-btn" type="button" data-query-delete-close>关闭</button>
  </div>
  <form id="query-delete-form" method="post" action="/queries/terms/delete">
    <div id="query-delete-inputs"></div>
    <div class="modal-body">
      <div id="query-delete-summary" class="alert alert-bad"></div>
      <div class="note">评分任务会随 Query 删除；历史 Discover 页面和频道来源会保留，但不再关联该 Query。</div>
      <ul id="query-delete-list" class="query-delete-list"></ul>
    </div>
    <div class="modal-actions">
      <button class="btn" type="button" data-query-delete-close>取消</button>
      <button id="query-delete-confirm" class="btn btn-danger" type="submit">永久删除</button>
    </div>
  </form>
</dialog>
<script>
(() => {
  const modal = document.getElementById("query-delete-modal");
  const form = document.getElementById("query-delete-form");
  const inputs = document.getElementById("query-delete-inputs");
  const summary = document.getElementById("query-delete-summary");
  const list = document.getElementById("query-delete-list");
  const confirmButton = document.getElementById("query-delete-confirm");
  const bulkButton = document.getElementById("query-bulk-delete");
  const selectedCount = document.getElementById("query-selected-count");
  const selectAll = document.getElementById("query-select-all");
  const rowCheckboxes = Array.from(document.querySelectorAll("[data-query-select]"));

  function selectedRows() {
    return rowCheckboxes
      .filter((checkbox) => checkbox.checked)
      .map((checkbox) => ({ id: checkbox.value, text: checkbox.dataset.queryText || ("Query #" + checkbox.value) }));
  }

  function syncSelection() {
    const selected = selectedRows().length;
    if (selectedCount) selectedCount.textContent = "已选 " + selected + " 条";
    if (bulkButton) bulkButton.disabled = selected === 0;
    if (selectAll) {
      selectAll.disabled = rowCheckboxes.length === 0;
      selectAll.checked = rowCheckboxes.length > 0 && selected === rowCheckboxes.length;
      selectAll.indeterminate = selected > 0 && selected < rowCheckboxes.length;
    }
  }

  function openDeleteModal(rows) {
    if (!modal || !form || !inputs || !summary || !list || rows.length === 0) return;
    inputs.replaceChildren();
    list.replaceChildren();
    rows.forEach((row) => {
      const input = document.createElement("input");
      input.type = "hidden";
      input.name = "query_ids";
      input.value = row.id;
      inputs.appendChild(input);
    });
    rows.slice(0, 20).forEach((row) => {
      const item = document.createElement("li");
      item.textContent = row.text;
      list.appendChild(item);
    });
    if (rows.length > 20) {
      const item = document.createElement("li");
      item.textContent = "另有 " + (rows.length - 20) + " 条";
      list.appendChild(item);
    }
    summary.textContent = rows.length === 1
      ? "确定永久删除这条 Query？"
      : "确定永久删除选中的 " + rows.length + " 条 Query？";
    if (confirmButton) {
      confirmButton.disabled = false;
      confirmButton.textContent = "永久删除";
    }
    modal.showModal();
  }

  rowCheckboxes.forEach((checkbox) => checkbox.addEventListener("change", syncSelection));
  if (selectAll) {
    selectAll.addEventListener("change", () => {
      rowCheckboxes.forEach((checkbox) => { checkbox.checked = selectAll.checked; });
      syncSelection();
    });
  }
  if (bulkButton) bulkButton.addEventListener("click", () => openDeleteModal(selectedRows()));
  document.querySelectorAll("[data-query-delete]").forEach((button) => {
    button.addEventListener("click", () => openDeleteModal([{
      id: button.dataset.queryId,
      text: button.dataset.queryText || ("Query #" + button.dataset.queryId),
    }]));
  });
  document.querySelectorAll("[data-query-delete-close]").forEach((button) => {
    button.addEventListener("click", () => modal?.close());
  });
  if (form) {
    form.addEventListener("submit", () => {
      if (!confirmButton) return;
      confirmButton.disabled = true;
      confirmButton.textContent = "删除中...";
    });
  }
  syncSelection();
})();
</script>

<dialog id="set-modal" class="modal">
  <div class="modal-head"><div><h2>新增分组</h2><div class="note">只属于新爬虫库</div></div><button class="btn small-btn" type="button" onclick="document.getElementById('set-modal').close()">关闭</button></div>
  <form method="post" action="/queries/sets">
    <div class="modal-body form-grid">
      <div class="field"><label>分组名称</label><input name="name" required placeholder="queryList/manual"></div>
      <div class="field"><label>说明</label><input name="description" placeholder="手工补充词"></div>
    </div>
    <div class="modal-actions"><button class="btn" type="button" onclick="document.getElementById('set-modal').close()">取消</button><button class="btn btn-primary" type="submit">新增分组</button></div>
  </form>
</dialog>

<dialog id="query-modal" class="modal">
  <div class="modal-head"><div><h2>新增 Query</h2><div class="note">默认 pt-BR / BR</div></div><button class="btn small-btn" type="button" onclick="document.getElementById('query-modal').close()">关闭</button></div>
  <form method="post" action="/queries/terms">
    <div class="modal-body form-grid-5">
      <div class="field"><label>分组</label><select name="query_set_id">${activeSetOptions}</select></div>
      <div class="field"><label>Query 词</label><input name="query_text" required placeholder="maquiagem brasileira"></div>
      <div class="field"><label>语言</label><input name="language" value="pt-BR"></div>
      <div class="field"><label>国家</label><input name="country" value="BR"></div>
      <div class="field"><label>优先级</label><input name="priority" type="number" value="100"></div>
    </div>
    <div class="modal-actions"><button class="btn" type="button" onclick="document.getElementById('query-modal').close()">取消</button><button class="btn btn-primary" type="submit">新增</button></div>
  </form>
</dialog>

<dialog id="import-modal" class="modal">
  <div class="modal-head"><div><h2>批量导入</h2><div class="note">每行一个 query，或 CSV 第一列为 query_text。所有分数都会入库；评分完成前不会调度，是否进入 Discover 由上方 Query 质量分控制。</div></div><button id="import-close" class="btn small-btn" type="button">关闭</button></div>
  <form id="import-form" method="post" action="/queries/import" class="grid">
    <div class="modal-body grid">
      <div class="form-grid">
        <div class="field"><label>分组</label><select name="query_set_id">${activeSetOptions}</select></div>
        <div class="field"><label>语言</label><input name="language" value="pt-BR"></div>
        <div class="field"><label>国家</label><input name="country" value="BR"></div>
        <div class="field"><label>优先级</label><input name="priority" type="number" value="100"></div>
      </div>
      <textarea name="queries" required placeholder="maquiagem para pele madura&#10;skincare brasil&#10;beleza feminina"></textarea>
      <progress id="import-progress" max="100" value="0" style="display:none;width:100%;height:14px;"></progress>
      <div id="import-status" class="alert alert-good" style="display:none;margin-bottom:0;"></div>
    </div>
    <div class="modal-actions"><button id="import-cancel" class="btn" type="button">取消</button><button id="import-submit" class="btn btn-primary" type="submit">导入</button></div>
  </form>
</dialog>
<script>
(() => {
  const form = document.getElementById("import-form");
  if (!form) return;
  const modal = document.getElementById("import-modal");
  const button = document.getElementById("import-submit");
  const cancelButton = document.getElementById("import-cancel");
  const closeButton = document.getElementById("import-close");
  const status = document.getElementById("import-status");
  const progress = document.getElementById("import-progress");
  let importing = false;
  let activeBatchId = null;
  let pollTimer = null;
  let pollFailures = 0;
  let cancellationRequested = false;

  function parseQueries(raw) {
    const seen = new Set();
    const rows = [];
    String(raw || "").split(/\\r?\\n/).forEach((line) => {
      const first = line.trim().split(",")[0].trim().replace(/^"|"$/g, "");
      if (!first || /^query(_text)?$/i.test(first)) return;
      const key = first.toLowerCase();
      if (seen.has(key)) return;
      seen.add(key);
      rows.push(first);
    });
    return rows;
  }

  function setStatus(text, tone) {
    if (!status) return;
    status.style.display = "block";
    status.className = "alert " + (tone === "bad" ? "alert-bad" : "alert-good");
    status.textContent = text;
  }

  function setProgress(processed, total) {
    if (!progress) return;
    progress.style.display = "block";
    progress.max = Math.max(1, Number(total || 0));
    progress.value = Math.min(progress.max, Math.max(0, Number(processed || 0)));
  }

  function finishControls(label) {
    importing = false;
    if (pollTimer) window.clearTimeout(pollTimer);
    pollTimer = null;
    if (button) {
      button.disabled = false;
      button.textContent = label || "重新导入";
    }
    if (cancelButton) {
      cancelButton.disabled = false;
      cancelButton.textContent = "关闭";
    }
    if (closeButton) closeButton.disabled = false;
  }

  function batchStatusText(batch) {
    return "评分进度：" + Number(batch.processed_count || 0) + " / " + Number(batch.total_count || 0)
      + "；正常评分 " + Number(batch.scored_count || 0)
      + "；fallback " + Number(batch.fallback_count || 0)
      + "；失败 " + Number(batch.failed_count || 0)
      + "；取消 " + Number(batch.cancelled_count || 0);
  }

  async function pollImport() {
    if (!activeBatchId) {
      cancellationRequested = true;
      if (cancelButton) {
        cancelButton.disabled = true;
        cancelButton.textContent = "取消中...";
      }
      setStatus("正在等待服务器返回批次 ID，随后立即取消。", "bad");
      return;
    }
    try {
      const response = await fetch("/queries/import/" + encodeURIComponent(activeBatchId), {
        headers: { accept: "application/json" },
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "读取导入进度失败");
      const batch = payload.batch || {};
      pollFailures = 0;
      setProgress(batch.processed_count, batch.total_count);
      setStatus(batchStatusText(batch), batch.status === "failed" ? "bad" : "good");
      if (batch.status === "done") {
        finishControls("导入完成");
        setStatus("导入完成。" + batchStatusText(batch) + "。页面即将刷新。", "good");
        window.setTimeout(() => {
          window.location.href = "/queries?notice=" + encodeURIComponent(
            "Query 导入与评分完成：" + Number(batch.total_count || 0) + " 条，fallback " + Number(batch.fallback_count || 0) + " 条"
          );
        }, 700);
        return;
      }
      if (batch.status === "cancelled") {
        finishControls("重新导入");
        setStatus("导入已取消。" + batchStatusText(batch) + "；已写入的 Query 保留，未评分 Query 不会被调度。", "bad");
        return;
      }
      if (batch.status === "failed") {
        finishControls("重新导入");
        setStatus("评分批次失败。" + batchStatusText(batch) + (batch.error_message ? "；" + batch.error_message : ""), "bad");
        return;
      }
      pollTimer = window.setTimeout(pollImport, 1000);
    } catch (error) {
      pollFailures += 1;
      setStatus("暂时无法读取进度，正在重试（" + pollFailures + "）：" + (error?.message || error), "bad");
      pollTimer = window.setTimeout(pollImport, Math.min(5000, 1000 + pollFailures * 500));
    }
  }

  async function cancelImport() {
    if (!importing) {
      if (modal) modal.close();
      return;
    }
    if (!activeBatchId) {
      cancellationRequested = true;
      if (cancelButton) {
        cancelButton.disabled = true;
        cancelButton.textContent = "取消中...";
      }
      setStatus("正在等待服务器返回批次 ID，随后立即取消。", "bad");
      return;
    }
    if (cancelButton) {
      cancelButton.disabled = true;
      cancelButton.textContent = "取消中...";
    }
    setStatus("正在取消评分任务；已经发出的网络请求可能自然结束，但不会再写入评分。", "bad");
    try {
      const response = await fetch("/queries/import/" + encodeURIComponent(activeBatchId) + "/cancel", {
        method: "POST",
        headers: { accept: "application/json", "content-type": "application/json" },
        body: "{}",
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "取消失败");
      if (cancelButton) cancelButton.textContent = "取消中...";
      if (!pollTimer) pollTimer = window.setTimeout(pollImport, 100);
    } catch (error) {
      if (cancelButton) {
        cancelButton.disabled = false;
        cancelButton.textContent = "重新取消";
      }
      setStatus("取消请求失败：" + (error?.message || error), "bad");
    }
  }

  if (cancelButton) cancelButton.addEventListener("click", cancelImport);
  if (closeButton) closeButton.addEventListener("click", cancelImport);
  if (modal) {
    modal.addEventListener("click", (event) => {
      if (event.target === modal && importing) {
        event.stopImmediatePropagation();
        cancelImport();
      }
    }, true);
    modal.addEventListener("cancel", (event) => {
      if (importing) {
        event.preventDefault();
        cancelImport();
      }
    });
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (importing) return;
    const queries = parseQueries(form.elements.queries?.value || "");
    if (queries.length === 0) {
      setStatus("没有解析到可导入的 query。", "bad");
      return;
    }
    importing = true;
    activeBatchId = null;
    pollFailures = 0;
    cancellationRequested = false;
    setProgress(0, queries.length);
    if (button) {
      button.disabled = true;
      button.textContent = "导入中...";
    }
    if (cancelButton) cancelButton.textContent = "取消导入";
    if (closeButton) closeButton.disabled = false;
    const formData = new FormData(form);
    try {
      setStatus("正在写入 " + queries.length + " 条 Query 并创建评分任务...", "good");
      const response = await fetch("/queries/import-start", {
        method: "POST",
        headers: { accept: "application/json", "content-type": "application/json" },
        body: JSON.stringify({
          queries,
          query_set_id: Number(formData.get("query_set_id") || 0),
          language: String(formData.get("language") || "pt-BR"),
          country: String(formData.get("country") || "BR"),
          category: String(formData.get("category") || ""),
          priority: Number(formData.get("priority") || 100),
        }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload.ok === false) throw new Error(payload.error || "创建导入批次失败：" + response.status);
      activeBatchId = payload.quality_batch_id;
      setProgress(0, payload.total_count || queries.length);
      if (cancellationRequested) {
        await cancelImport();
        return;
      }
      setStatus("已写入 " + Number(payload.total_count || queries.length) + " 条 Query，正在评分...", "good");
      pollTimer = window.setTimeout(pollImport, 200);
    } catch (error) {
      finishControls("重新导入");
      setStatus("导入启动失败：" + (error?.message || error), "bad");
    }
  });
})();
</script>
`,
  });
}

function dailyClockMaskCell(row) {
  const kinds = CLOCK_KINDS;
  const terminal = ["succeeded", "partial", "failed", "cancelled"]
    .includes(String(row.plan_status || ""));
  const chips = kinds
    .filter(([kind]) => Boolean(row[`run_${kind}`]))
    .map(([kind, label]) => {
      const dueTime = row.plan_id && !terminal
        ? ""
        : `<span class="clock-chip-time">${h(utcDayText(row[`${kind}_due_day`]))}</span>`;
      return `<span class="clock-chip clock-${kind}">${label}${dueTime}</span>`;
    });
  return `<div class="clock-mask">${chips.join("") || '<span class="muted">无</span>'}</div>`;
}

function dailyClockPlanStatus(row) {
  const presentation = dailyClockPlanPresentation(row);
  const notes = [];
  if (presentation.recovered) notes.push("原 Plan 失败（历史）");
  if (row.dispatch_status) notes.push(`Outbox ${row.dispatch_status}`);
  if (row.crawler_run_status) notes.push(`最新 Run ${row.crawler_run_status}`);
  return `<span class="pill ${presentation.className}">${h(presentation.label)}</span>${notes.length > 0 ? `<div class="note mono">${h(notes.join(" · "))}</div>` : ""}`;
}

function dailyClockMetricCards(data, stats, progress) {
  if (data.day === "today" && progress.state === "running") {
    const eta = dailyClockDurationText(progress.etaMs, { estimate: true });
    const etaFoot = progress.ratePerMinute == null
      ? "最近速度样本不足，预计时间计算中"
      : `预计 ${timeText(progress.estimatedCompletionAt)} 完成 · 近 5 分钟 ${progress.ratePerMinute.toFixed(1)} 个/分钟`;
    return `<section class="grid grid-4">
  <div class="metric metric-blue"><div><div class="metric-label">剩余频道</div><div class="metric-value">${fmtInt(progress.remaining)}</div></div><div class="metric-foot">待生成 ${fmtInt(stats.unplanned)} · 活动 Plan ${fmtInt(stats.active)}</div></div>
  <div class="metric"><div><div class="metric-label">预计剩余时间</div><div class="metric-value" style="font-size:32px;">${h(eta)}</div></div><div class="metric-foot">${h(etaFoot)}</div></div>
  <div class="metric"><div><div class="metric-label">已完成频道</div><div class="metric-value">${fmtInt(progress.completed)}</div></div><div class="metric-foot">已处理 ${fmtInt(progress.finished)} · 部分 ${fmtInt(progress.partial)} · 失败 ${fmtInt(progress.failed)} · 取消 ${fmtInt(progress.cancelled)}</div></div>
  <div class="metric"><div><div class="metric-label">已运行</div><div class="metric-value" style="font-size:32px;">${h(dailyClockDurationText(progress.elapsedMs))}</div></div><div class="metric-foot">开始 ${h(timeText(progress.startedAt))} 北京时间</div></div>
</section>`;
  }

  if (data.day === "today" && progress.state === "completed") {
    const incomplete = progress.partial + progress.failed + progress.cancelled;
    return `<section class="grid grid-4">
  <div class="metric metric-blue"><div><div class="metric-label">完成频道</div><div class="metric-value">${fmtInt(progress.completed)}</div></div><div class="metric-foot">本日共处理 ${fmtInt(progress.finished)} / ${fmtInt(progress.total)}</div></div>
  <div class="metric"><div><div class="metric-label">实际总用时</div><div class="metric-value" style="font-size:32px;">${h(dailyClockDurationText(progress.elapsedMs))}</div></div><div class="metric-foot">第一个频道开始至最后一个频道结束</div></div>
  <div class="metric"><div><div class="metric-label">完成时间</div><div class="metric-value" style="font-size:32px;">${h(timeOnlyText(progress.completedAt))}</div></div><div class="metric-foot">${h(timeText(progress.completedAt))} 北京时间</div></div>
  <div class="metric"><div><div class="metric-label">非完整结果</div><div class="metric-value">${fmtInt(incomplete)}</div></div><div class="metric-foot">部分 ${fmtInt(progress.partial)} · 失败 ${fmtInt(progress.failed)} · 取消 ${fmtInt(progress.cancelled)}</div></div>
</section>`;
  }

  return `<section class="grid grid-4">
  <div class="metric metric-blue"><div><div class="metric-label">${data.day === "today" ? "今日" : "明日"}调度范围</div><div class="metric-value">${fmtInt(stats.total)}</div></div><div class="metric-foot">${h(data.targetDay)} 调度批次 · 当前筛选 ${fmtInt(data.total)} 条</div></div>
  <div class="metric"><div><div class="metric-label">实际 Plan</div><div class="metric-value">${fmtInt(stats.frozen)}</div></div><div class="metric-foot">活动 ${fmtInt(stats.active)} · 原始成功 ${fmtInt(stats.succeeded)} · 已恢复 ${fmtInt(stats.recovered)} · 当前仍失败 ${fmtInt(stats.unrecovered)}</div></div>
  <div class="metric"><div><div class="metric-label">待生成计划</div><div class="metric-value">${fmtInt(stats.unplanned)}</div></div><div class="metric-foot">当前仍逾期 ${fmtInt(stats.overdue)} · 按最早 Due 排队</div></div>
  <div class="metric"><div><div class="metric-label">Clock Mask</div><div class="metric-value">${fmtInt(clockMaskTotal(stats))}</div></div><div class="metric-foot">A ${fmtInt(stats.about)} · V ${fmtInt(stats.video)} · G ${fmtInt(stats.agent)}</div></div>
</section>`;
}

function dailyClockListPage(data) {
  const { filters, stats } = data;
  const previous = new URLSearchParams();
  const next = new URLSearchParams();
  for (const params of [previous, next]) {
    params.set("day", data.day);
    if (filters.search) params.set("q", filters.search);
    params.set("clock", filters.clock);
    params.set("status", filters.status);
    params.set("limit", String(filters.limit));
  }
  previous.set("offset", String(Math.max(0, filters.offset - filters.limit)));
  next.set("offset", String(filters.offset + filters.limit));
  const hasPrevious = filters.offset > 0;
  const hasNext = filters.offset + data.channels.length < Number(data.total || 0);
  const clockOptions = [
    ["all", "全部 Clock"],
    ...CLOCK_KINDS,
  ];
  const statusOptions = [
    ["all", "全部状态"],
    ["unplanned", "待生成计划"],
    ["active", "活动 Plan"],
    ["planned", "等待容量 / 已分配"],
    ["dispatching", "投递中"],
    ["dispatched", "已投递"],
    ["running", "执行中"],
    ["terminal", "已结束"],
    ["succeeded", "已完成"],
    ["partial", "部分完成"],
    ["failed", "原 Plan 失败"],
    ["recovered", "已恢复"],
    ["unrecovered", "当前仍失败"],
    ["cancelled", "已取消"],
  ];
  const rows = data.channels.map((channel) => {
    const dueDay = utcDayText(channel.due_day);
    const planDueDay = utcDayText(channel.plan_due_day);
    const terminal = ["succeeded", "partial", "failed", "cancelled"]
      .includes(String(channel.plan_status || ""));
    const isOverdue = dueDay !== "-" && dueDay < data.targetDay;
    const originalDue = planDueDay !== "-" && planDueDay !== dueDay
      ? `<div class="note mono">本次原到期 ${h(planDueDay)} UTC</div>`
      : "";
    const execution = channel.scheduled_at
      ? `<strong class="mono">${h(timeText(channel.scheduled_at))}</strong><div class="note">北京时间${channel.execution_deadline_at ? ` · 截止 ${h(timeText(channel.execution_deadline_at))}` : ""}</div>`
      : channel.plan_id
        ? '<span class="muted">等待动态分配</span><div class="note">安全窗口 00:30～21:30 UTC</div>'
        : '<span class="muted">待生成 Plan</span>';
    return `<tr>
      <td>${channelIdentityCell(channel, { showAvatar: false })}</td>
      <td>${dailyClockMaskCell(channel)}</td>
      <td><span class="mono">${h(dueDay)}</span>${isOverdue ? '<div><span class="pill bad">逾期</span></div>' : ""}${originalDue}</td>
      <td>${execution}</td>
      <td>${dailyClockPlanStatus(channel)}</td>
      <td><div class="mono">Clock v${h(channel.source_clock_version)}</div><div class="note mono">${h(channel.policy_version || "-")}</div></td>
      <td class="mono">${h(channel.plan_id || "待生成")}</td>
      <td><a class="btn small-btn" href="/channels/${encodeURIComponent(channel.channel_id)}">查看频道</a></td>
    </tr>`;
  }).join("");

  const progress = dailyClockExecutionProgress(stats, {
    day: data.day,
    now: data.generatedAt,
  });
  const metricCards = dailyClockMetricCards(data, stats, progress);
  const autoRefresh = data.available && progress.state === "running"
    ? `<script>window.setTimeout(() => window.location.reload(), 30000);</script>`
    : "";

  const table = data.available
    ? `${metricCards}
<section class="table-panel mt">
  <div class="table-tools">
    <div class="panel-head">
      <div><h2>${h(data.targetDay)} UTC 调度日</h2><div class="note">Clock 显示 UTC 到期日 · 执行时间显示北京时间</div></div>
      <div class="toolbar">
        ${hasPrevious ? `<a class="btn small-btn" href="/daily-clocks?${h(previous.toString())}">上一页</a>` : ""}
        ${hasNext ? `<a class="btn small-btn" href="/daily-clocks?${h(next.toString())}">下一页</a>` : ""}
      </div>
    </div>
    <form method="get" action="/daily-clocks" class="filters daily-clock-filters">
      <input type="hidden" name="day" value="${h(data.day)}">
      <div class="field"><label>搜索</label><input name="q" value="${h(filters.search)}" placeholder="channel / handle / title"></div>
      <div class="field"><label>Clock</label><select name="clock">${clockOptions.map(([value, label]) => `<option value="${h(value)}" ${filters.clock === value ? "selected" : ""}>${h(label)}</option>`).join("")}</select></div>
      <div class="field"><label>Plan 状态</label><select name="status">${statusOptions.map(([value, label]) => `<option value="${h(value)}" ${filters.status === value ? "selected" : ""}>${h(label)}</option>`).join("")}</select></div>
      <div class="field"><label>每页</label><input name="limit" type="number" min="1" max="500" value="${h(filters.limit)}"></div>
      <input type="hidden" name="offset" value="0">
      <button class="btn" type="submit">筛选</button>
    </form>
  </div>
  <div class="table-scroll">
    <table>
      <thead><tr><th>频道</th><th>本次 Clock / 执行后下次到期</th><th>当前最早到期日（UTC）</th><th>执行时间（北京时间）</th><th>Plan 状态</th><th>版本</th><th>Plan ID</th><th>操作</th></tr></thead>
      <tbody>${rows || '<tr><td colspan="8" class="muted">当前范围没有要调度的 Channel。</td></tr>'}</tbody>
    </table>
  </div>
</section>`
    : `<div class="alert alert-bad">${h(data.error || "每日 Clock 数据当前不可用")}</div>`;

  return layout({
    title: "每日 Clock",
    active: "daily-clocks",
    body: `<div class="topbar">
  <div>
    <div class="eyebrow">Daily Clock</div>
    <h1>每日 Clock</h1>
    <div class="sub">${h(data.targetDay)} UTC 调度日 · 更新时间 ${h(timeText(data.generatedAt))} 北京时间</div>
  </div>
  <div class="toolbar">
    <a class="btn ${data.day === "today" ? "btn-primary" : ""}" href="/daily-clocks?day=today">今天</a>
    <a class="btn ${data.day === "tomorrow" ? "btn-primary" : ""}" href="/daily-clocks?day=tomorrow">明天</a>
    <a class="btn" href="/daily-clocks?day=${h(data.day)}">刷新</a>
  </div>
</div>
${table}
${autoRefresh}`,
  });
}

function migrationCandidateSummary(channel) {
  const labels = {
    discovered: "待迁移",
    queued: "待迁移",
    validating: "验证中",
    finishing: "待收尾",
    accepted: "已通过",
    rejected: "已拒绝",
    existing: "已存在",
    failed: "失败",
  };
  const status = String(channel?.status || "discovered");
  return `<span class="pill ${statusClass(status)}">${h(labels[status] || status)}</span><div class="note mono">${h(status)}</div>`;
}

function channelIdentityCell(channel, { showAvatar = true } = {}) {
  const avatar = channel.avatar_url
    ? `<img class="migration-avatar" src="${h(channel.avatar_url)}" alt="" loading="lazy">`
    : `<div class="migration-avatar migration-avatar-empty" aria-hidden="true">-</div>`;
  return `<div class="migration-channel">${showAvatar ? avatar : ""}<div><strong>${h(channel.title || channel.handle || channel.channel_id)}</strong><div class="note">${h(channel.handle || channel.channel_id)}</div><div class="note mono">${h(channel.channel_id)}</div><a class="note" href="${h(channel.channel_url)}" target="_blank" rel="noreferrer">打开 YouTube</a></div></div>`;
}

function channelClockTimesCell(channel) {
  if (channel?.status === "removed") {
    return '<span class="pill bad">已永久停用</span><div class="note">不再生成增量 Clock</div>';
  }
  if (isDormantChannel(channel)) {
    const recheckDay = utcDayText(channel.dormant_recheck_day);
    if (recheckDay === "-") {
      return '<span class="pill muted-pill">休眠</span><div class="note">Video 复查尚未初始化</div>';
    }
    const overdue = recheckDay < utcDayOffset(0);
    return `<div class="channel-clock-times"><div class="channel-clock-line">
      <span class="clock-chip clock-video">Video 复查</span>
      <span class="channel-clock-time-stack">
        <span class="mono channel-clock-at">${h(recheckDay)} UTC</span>
        <span class="note channel-clock-last">上次探测 ${h(timeText(channel.dormant_last_probe_at))} 北京时间</span>
      </span>
      <span class="pill ${overdue ? "bad" : "muted-pill"}">${overdue ? "复查逾期" : "待复查"}</span>
    </div></div>`;
  }
  const clocks = CLOCK_KINDS;
  const hasClockState = clocks.some(([kind]) => Boolean(channel[`${kind}_due_day`]));
  if (!hasClockState) return '<span class="muted">Clock 尚未初始化</span>';

  const planLabels = {
    planned: "等待调度",
    dispatching: "投递中",
    dispatched: "已投递",
    running: "执行中",
  };
  const planStatus = String(channel.clock_plan_status || "");
  const todayUtc = utcDayOffset();
  const rows = clocks.map(([kind, label]) => {
    const dueDay = utcDayText(channel[`${kind}_due_day`]);
    const includedInActivePlan = Boolean(
      channel.clock_plan_id
      && channel[`clock_plan_run_${kind}`],
    );
    const assigned = includedInActivePlan && Boolean(channel.clock_plan_scheduled_at);
    const displayTime = assigned ? timeText(channel.clock_plan_scheduled_at) : dueDay;
    const stateLabel = includedInActivePlan
      ? assigned
        ? (planLabels[planStatus] || planStatus || "已排期")
        : "等待容量"
      : dueDay !== "-" && dueDay < todayUtc
        ? "逾期待排"
        : dueDay === todayUtc
          ? "今日到期"
          : "待到期";
    const stateClass = includedInActivePlan
      ? (planStatus === "running" || !assigned ? "warn" : "good")
      : dueDay !== "-" && dueDay < todayUtc
        ? "bad"
        : "muted-pill";
    const timeTitle = assigned
      ? "Dispatcher 分配的实际执行时间（北京时间）"
      : "Clock 权威到期日（UTC）";
    const lastAboutCapture = kind === "about"
      ? timeText(channel.about_last_observed_at)
      : null;
    const lastAboutHtml = kind === "about"
      ? `<span class="note channel-clock-last">上次抓取 ${h(lastAboutCapture)} 北京时间</span>`
      : "";
    return `<div class="channel-clock-line">
      <span class="clock-chip clock-${kind}">${label}</span>
      <span class="channel-clock-time-stack">
        <span class="mono channel-clock-at" title="${h(timeTitle)}">${h(displayTime)}${assigned ? "" : " UTC"}</span>
        ${lastAboutHtml}
      </span>
      <span class="pill ${stateClass}">${h(stateLabel)}</span>
    </div>`;
  }).join("");
  return `<div class="channel-clock-times">${rows}</div>`;
}

function channelTableRows(channels, {
  detailBasePath = "/channels",
  emptyText = "暂无频道。",
  showAvatar = true,
  migrationActions = false,
  showClocks = false,
  selectable = false,
} = {}) {
  const rows = channels.map((channel) => {
    const candidateOnly = Boolean(channel.is_candidate_only);
    const migrationIncomplete = channel.migration_incomplete === true;
    const completeness = candidateOnly || migrationIncomplete
      ? migrationCandidateSummary({ status: channel.candidate_status || channel.status })
      : completenessSummary(channel);
    const candidateStatus = String(channel.candidate_status || channel.status || "");
    const canMigrate = ["discovered", "failed"].includes(candidateStatus);
    const dormant = !candidateOnly && !migrationIncomplete && isDormantChannel(channel);
    const migrateLabel = migrationIncomplete ? "待收尾" : "迁移";
    const migrateAction = migrationActions
      ? `<form class="inline-form" method="post" action="/migration-channels/${encodeURIComponent(channel.channel_id)}/migrate">
           <input type="hidden" name="candidate_id" value="${h(channel.candidate_id)}">
           <button class="btn btn-primary small-btn" type="submit" ${canMigrate ? "" : "disabled"}>${migrateLabel}</button>
         </form>`
      : "";
    return `<tr>
      ${selectable ? `<td class="channel-select-cell"><input class="channel-select" type="checkbox" name="channel_id" value="${h(channel.channel_id)}" aria-label="选择 ${h(channel.title || channel.channel_id)}"></td>` : ""}
      <td>${channelIdentityCell(channel, { showAvatar })}</td>
      <td>${channel.subscriber_count == null ? '<span class="muted">未获取</span>' : fmtInt(channel.subscriber_count)}</td>
      <td>总 ${fmtInt(channel.content_count)}<div class="note">video ${fmtInt(channel.video_count)} · short ${fmtInt(channel.short_count)} · live ${fmtInt(channel.live_count)}</div></td>
      ${showClocks ? `<td>${channelClockTimesCell(channel)}</td>` : ""}
      <td>${completeness}</td>
      <td>${dormant ? '<span class="muted">-</span>' : `<span class="pill ${statusClass(channel.agent_status || "pending")}">${h(channel.agent_status || "pending")}</span>`}</td>
      <td>${dormant ? '<span class="muted">-</span>' : `<span class="pill ${statusClass(channel.final_status || "pending")}">${h(channel.final_status || "pending")}</span>`}</td>
      <td class="mono">${timeText(channel.updated_at)}</td>
      <td><div class="query-row-actions">${migrateAction}<a class="btn small-btn" href="${h(detailBasePath)}/${encodeURIComponent(channel.channel_id)}">查看详情</a></div></td>
    </tr>`;
  }).join("");
  const columnCount = (showClocks ? 9 : 8) + (selectable ? 1 : 0);
  return rows || `<tr><td colspan="${columnCount}" class="muted">${h(emptyText)}</td></tr>`;
}

function migrationChannelListPage(migration) {
  const { filters } = migration;
  const previous = new URLSearchParams();
  const next = new URLSearchParams();
  for (const params of [previous, next]) {
    if (filters.search) params.set("q", filters.search);
    params.set("channel_status", filters.channelStatus);
    if (filters.agentStatus) params.set("agent_status", filters.agentStatus);
    if (filters.finalStatus) params.set("final_status", filters.finalStatus);
    params.set("limit", String(filters.limit));
  }
  previous.set("offset", String(Math.max(0, filters.offset - filters.limit)));
  next.set("offset", String(filters.offset + filters.limit));
  const hasPrevious = filters.offset > 0;
  const hasNext = filters.offset + migration.channels.length < Number(migration.total || 0);
  const candidateOptions = [
    ["all", "全部未迁移"],
    ["discovered", "待迁移"],
    ["queued", "已入队"],
    ["validating", "验证中"],
    ["failed", "失败"],
    ["finishing", "待收尾"],
  ];
  const agentOptions = ["", "pending", "queued", "running", "done", "failed", "skipped"];
  const finalOptions = ["", "pending", "pending_detail", "pending_api", "pending_agent", "ready_auto", "ready_partial", "failed"];
  const batchOptions = [
    ["100", "100"],
    ["1000", "1000"],
    ["2000", "2000"],
  ];

  const table = migration.available
    ? `
<section class="grid grid-3">
  <div class="metric metric-blue"><div><div class="metric-label">迁移待办</div><div class="metric-value">${fmtInt(migration.stats.total)}</div></div><div class="metric-foot">待迁移 ${fmtInt(migration.stats.discovered)} · 待收尾 ${fmtInt(migration.stats.finishing)} · 失败待重试 ${fmtInt(migration.stats.failed)} · 当前筛选 ${fmtInt(migration.total)} 条</div></div>
  <div class="metric"><div><div class="metric-label">迁移完成</div><div class="metric-value">${fmtInt(migration.stats.migration_done)}</div></div><div class="metric-foot">已成功进入爬虫数据库</div></div>
  <div class="metric"><div><div class="metric-label">Final 完成</div><div class="metric-value">${fmtInt(migration.stats.final_done)}</div></div><div class="metric-foot">ready_auto / ready_partial</div></div>
</section>
<section class="table-panel mt">
  <div class="table-tools">
    <div class="panel-head">
      <div><h2>迁移待办频道</h2><div class="note">待迁移或待收尾共 ${fmtInt(migration.stats.total)} 条 · 当前筛选 ${fmtInt(migration.total)} 条</div></div>
      <div class="toolbar">
        ${hasPrevious ? `<a class="btn small-btn" href="/migration-channels?${h(previous.toString())}">上一页</a>` : ""}
        ${hasNext ? `<a class="btn small-btn" href="/migration-channels?${h(next.toString())}">下一页</a>` : ""}
      </div>
    </div>
    <form method="get" action="/migration-channels" class="filters">
      <div class="field"><label>搜索</label><input name="q" value="${h(filters.search)}" placeholder="channel / handle / title"></div>
      <div class="field"><label>频道状态</label><select name="channel_status">${candidateOptions.map(([value, label]) => `<option value="${h(value)}" ${filters.channelStatus === value ? "selected" : ""}>${h(label)}</option>`).join("")}</select></div>
      <div class="field"><label>Agent</label><select name="agent_status">${agentOptions.map((status) => `<option value="${h(status)}" ${filters.agentStatus === status ? "selected" : ""}>${status || "全部"}</option>`).join("")}</select></div>
      <div class="field"><label>Final</label><select name="final_status">${finalOptions.map((status) => `<option value="${h(status)}" ${filters.finalStatus === status ? "selected" : ""}>${status || "全部"}</option>`).join("")}</select></div>
      <div class="field"><label>每页</label><input name="limit" type="number" min="1" max="500" value="${h(filters.limit)}"></div>
      <input type="hidden" name="offset" value="0">
      <button class="btn" type="submit">筛选</button>
    </form>
  </div>
  <div class="table-scroll">
    <table>
      <thead><tr><th>频道</th><th>订阅</th><th>内容</th><th>完整性</th><th>Agent</th><th>Final</th><th>更新时间（北京时间）</th><th>操作</th></tr></thead>
      <tbody>
      ${channelTableRows(migration.channels, { detailBasePath: "/migration-channels", emptyText: "暂无未迁移频道。", showAvatar: false, migrationActions: true })}
      </tbody>
    </table>
  </div>
</section>`
    : `<div class="alert alert-bad">${h(migration.error || (migration.configured ? "迁移数据库当前不可用" : "迁移数据库未配置"))}</div>`;

  return layout({
    title: "迁移频道列表",
    active: "migration-channels",
    body: `
<div class="topbar">
  <div>
    <div class="eyebrow">Migration Channels</div>
    <h1>未迁移频道列表</h1>
    <div class="sub">显示尚未开始、正在处理、失败待重试，以及已进入频道注册表但尚未完成 Finalize 的 legacy 迁移任务。</div>
  </div>
  <div class="toolbar">
    <form class="batch-migration-form" method="post" action="/migration-channels/batch-migrate" onsubmit="return confirm('确认按所选数量启动完整迁移吗？');">
      <div class="field">
        <label for="batch-migration-selection">迁移数量</label>
        <select id="batch-migration-selection" name="selection">
          ${batchOptions.map(([value, label]) => `<option value="${value}" ${value === "100" ? "selected" : ""}>${label}</option>`).join("")}
        </select>
      </div>
      <button class="btn btn-primary" type="submit">批量迁移</button>
    </form>
    <a class="btn" href="/migration-channels">刷新</a>
    <a class="btn" href="/channels">频道列表</a>
  </div>
</div>
${migration.notice ? `<div class="alert alert-good">${h(migration.notice)}</div>` : ""}
${migration.error ? `<div class="alert alert-bad">${h(migration.error)}</div>` : ""}
${table}`,
  });
}

function migrationChannelDetailPage(data) {
  const candidate = data.candidate;
  const discoveredAt = Number(candidate.legacy_discovered_at);
  const discoveredText = Number.isFinite(discoveredAt) && discoveredAt > 0
    ? timeText(new Date(discoveredAt * 1000))
    : "-";
  const infoRows = [
    ["channel_id", candidate.channel_id],
    ["channel_url", candidate.channel_url],
    ["handle", candidate.handle || "-"],
    ["title", candidate.title || "-"],
    ["avatar_url", data.channel.avatar_url || "源库无头像字段"],
    ["search_subscriber_count", candidate.search_subscriber_count ?? "-"],
    ["candidate_status", candidate.status],
    ["reject_reason", candidate.reject_reason || "-"],
    ["error_message", candidate.error_message || "-"],
    ["legacy_country", candidate.legacy_country || "-"],
    ["legacy_target_reason", candidate.legacy_target_reason || "-"],
    ["legacy_evidence_score", candidate.legacy_evidence_score || "-"],
    ["legacy_evidence_reasons", candidate.legacy_evidence_reasons || "-"],
    ["legacy_discovered_at（北京时间）", discoveredText],
    ["source_rowid", candidate.source_rowid || "-"],
    ["candidate_id", candidate.candidate_id],
    ["dispatch_batch_id", candidate.dispatch_batch_id],
    ["dispatch_batch_status", data.batch?.status || "-"],
    ["source_database", candidate.source_database || "-"],
    ["source_database_sha256", candidate.source_database_sha256 || "-"],
  ];
  const extraSections = `
<section class="grid detail-overview-grid mt">
  <div class="panel">
    <div class="panel-head"><div><h2>迁移来源字段</h2><div class="note">旧库字段与 crawler.channel_candidates 映射</div></div></div>
    <div class="table-scroll"><table class="source-table"><thead><tr><th>字段</th><th>值</th></tr></thead><tbody>${infoRows.map(([field, value]) => `<tr><td class="mono">${h(field)}</td><td class="mono">${h(value)}</td></tr>`).join("")}</tbody></table></div>
  </div>
  <div class="panel">
    <div class="panel-head"><div><h2>旧库描述</h2><div class="note">crawler.channel_candidates.description</div></div></div>
    <textarea class="json" readonly>${h(candidate.description || "")}</textarea>
  </div>
</section>
<section class="grid grid-2 mt">
  <div class="panel">
    <div class="panel-head"><div><h2>候选来源</h2><div class="note">crawler.channel_candidate_sources</div></div></div>
    <div class="table-scroll"><table><thead><tr><th>策略</th><th>页面</th><th>源位置</th></tr></thead><tbody>${data.sources.map((source) => `<tr><td class="mono">${h(source.discovery_strategy)}</td><td class="mono">${h(source.page_id || "-")}</td><td class="mono">${h(source.rank_position ?? "-")}</td></tr>`).join("")}</tbody></table></div>
  </div>
  <div class="panel">
    <div class="panel-head"><div><h2>原始来源</h2><div class="note">crawler.channel_candidates.source_json</div></div></div>
    <textarea class="json" readonly>${h(jsonText(candidate.source_json || {}))}</textarea>
  </div>
</section>`;
  return channelDetailPage(data, {
    titlePrefix: "迁移频道详情",
    active: "migration-channels",
    eyebrow: "Migration Channel Review",
    databaseName: "bullmq_crawler_migration",
    listHref: "/migration-channels",
    listLabel: "返回迁移频道列表",
    allowActions: false,
    extraSections,
  });
}

function channelListPage(data) {
  const { channels, filters, stats } = data;
  const prev = new URLSearchParams();
  const next = new URLSearchParams();
  for (const params of [prev, next]) {
    if (filters.search) params.set("q", filters.search);
    params.set("channel_status", filters.channelStatus);
    if (filters.agentStatus) params.set("agent_status", filters.agentStatus);
    if (filters.finalStatus) params.set("final_status", filters.finalStatus);
    params.set("limit", String(filters.limit));
  }
  prev.set("offset", String(Math.max(0, filters.offset - filters.limit)));
  next.set("offset", String(filters.offset + filters.limit));
  const agentOptions = ["", "pending", "queued", "running", "done", "failed", "skipped"];
  const finalOptions = ["", "pending", "pending_detail", "pending_api", "pending_agent", "ready_auto", "ready_partial", "failed"];
  const channelOptions = [
    ["active", "正常"],
    ["dormant", "休眠"],
    ["removed", "已封禁/移除"],
    ["all", "全部状态"],
  ];

  return layout({
    title: "频道列表",
    active: "channels",
    body: `
<div class="topbar">
  <div>
    <div class="eyebrow">Channel Review</div>
    <h1>频道列表</h1>
    <div class="sub">审核新爬虫已抓取频道，查看字段来源和最终 profile。来源数据库：<span class="mono">bullmq_crawler</span>，schema：<span class="mono">crawler</span>。</div>
  </div>
  <div class="toolbar">
    <a class="btn" href="/channels">刷新</a>
    <a class="btn" href="/exports/channels.csv">导出频道 CSV</a>
    <a class="btn" href="/exports/finalized-profiles.jsonl">导出 Profile JSONL</a>
    <a class="btn btn-primary" href="/exports/crawler-content.sql">导出抓取内容 SQL</a>
  </div>
</div>
${data.notice ? `<div class="alert alert-good">${h(data.notice)}</div>` : ""}
${data.error ? `<div class="alert alert-bad">${h(data.error)}</div>` : ""}

<section class="grid grid-3">
  <div class="metric metric-blue"><div><div class="metric-label">正常频道</div><div class="metric-value">${fmtInt(stats.active)}</div></div><div class="metric-foot">迁移待收尾 ${fmtInt(stats.migration_pending)} · 休眠 ${fmtInt(stats.dormant)} · 不符合条件 ${fmtInt(stats.rejected)} · 移除 ${fmtInt(stats.removed)} · 当前筛选 ${fmtInt(data.total)} 条</div></div>
  <div class="metric"><div><div class="metric-label">Agent 完成</div><div class="metric-value">${fmtInt(stats.agent_done)}</div></div><div class="metric-foot">crawler.channels.agent_status = done</div></div>
  <div class="metric"><div><div class="metric-label">Final 完成</div><div class="metric-value">${fmtInt(stats.final_done)}</div></div><div class="metric-foot">crawler.finalized_profiles.status = done</div></div>
</section>

<section class="table-panel mt">
  <div class="table-tools">
    <div class="panel-head">
      <div><h2>频道审核</h2><div class="note">字段来源：crawler.channels、crawler.contents、crawler.agent_profiles、crawler.finalized_profiles、crawler.raw_objects、feature_clock.channel_clock_state</div></div>
      <div class="toolbar">
        <button id="publication-compare-button" class="btn btn-primary small-btn" type="submit" form="publication-compare-form" disabled title="一次最多比对 ${PUBLICATION_COMPARISON_MAX_CHANNELS} 个频道">批量比对</button>
        <a class="btn small-btn" href="/channels?${h(prev.toString())}">上一页</a>
        <a class="btn small-btn" href="/channels?${h(next.toString())}">下一页</a>
      </div>
    </div>
    <form method="get" action="/channels" class="filters">
      <div class="field"><label>搜索</label><input name="q" value="${h(filters.search)}" placeholder="channel / handle / title"></div>
      <div class="field"><label>频道状态</label><select name="channel_status">${channelOptions.map(([value, label]) => `<option value="${h(value)}" ${filters.channelStatus === value ? "selected" : ""}>${h(label)}</option>`).join("")}</select></div>
      <div class="field"><label>Agent</label><select name="agent_status">${agentOptions.map((s) => `<option value="${h(s)}" ${filters.agentStatus === s ? "selected" : ""}>${s || "全部"}</option>`).join("")}</select></div>
      <div class="field"><label>Final</label><select name="final_status">${finalOptions.map((s) => `<option value="${h(s)}" ${filters.finalStatus === s ? "selected" : ""}>${s || "全部"}</option>`).join("")}</select></div>
      <div class="field"><label>每页</label><input name="limit" type="number" min="1" max="500" value="${h(filters.limit)}"></div>
      <input type="hidden" name="offset" value="0">
      <button class="btn" type="submit">筛选</button>
    </form>
  </div>
  <form id="publication-compare-form" method="post" action="/channels/publication-compare">
    <div class="table-scroll">
      <table>
        <thead><tr><th class="channel-select-cell"><input id="channel-select-all" class="channel-select" type="checkbox" aria-label="选择本页频道"></th><th>频道</th><th>订阅</th><th>内容</th><th>Clock 到期日 / 执行时间</th><th>完整性</th><th>Agent</th><th>Final</th><th>更新时间（北京时间）</th><th>操作</th></tr></thead>
        <tbody>
        ${channelTableRows(channels, { showClocks: true, selectable: true })}
        </tbody>
      </table>
    </div>
  </form>
</section>
<script>
(() => {
  const maximum = ${PUBLICATION_COMPARISON_MAX_CHANNELS};
  const selectAll = document.getElementById("channel-select-all");
  const button = document.getElementById("publication-compare-button");
  const checkboxes = Array.from(document.querySelectorAll(".channel-select[name='channel_id']"));
  const update = () => {
    const count = checkboxes.filter((input) => input.checked).length;
    if (button) {
      button.disabled = count === 0;
      button.textContent = count > 0 ? "批量比对 (" + count + ")" : "批量比对";
    }
    if (selectAll) {
      selectAll.checked = checkboxes.length > 0 && count === checkboxes.length;
      selectAll.indeterminate = count > 0 && count < checkboxes.length;
    }
  };
  for (const checkbox of checkboxes) {
    checkbox.addEventListener("change", () => {
      const selected = checkboxes.filter((input) => input.checked);
      if (selected.length > maximum) {
        checkbox.checked = false;
        window.alert("一次最多比对 " + maximum + " 个频道");
      }
      update();
    });
  }
  selectAll?.addEventListener("change", () => {
    checkboxes.forEach((checkbox, index) => {
      checkbox.checked = selectAll.checked && index < maximum;
    });
    update();
  });
  update();
})();
</script>`,
  });
}

function publicationComparisonStatus(status) {
  if (status === "matched") return { label: "完全一致", className: "good" };
  if (status === "pending") return { label: "投递中", className: "warn" };
  if (status === "not_published") return { label: "尚未发布", className: "muted-pill" };
  return { label: "发现差异", className: "bad" };
}

function publicationComparisonRatio(matched, total, note = "") {
  const complete = Number(total) > 0 && Number(matched) === Number(total);
  const empty = Number(total) === 0;
  return `<div class="comparison-ratio"><strong class="${empty ? "muted" : complete ? "good" : "bad"}">${fmtInt(matched)} / ${fmtInt(total)}</strong>${note ? `<div class="note">${h(note)}</div>` : ""}</div>`;
}

function publicationComparisonPage(report) {
  const rows = report.channels.map((channel) => {
    const status = publicationComparisonStatus(channel.status);
    const issues = channel.issues.length > 0
      ? `<details class="comparison-issues"><summary>${fmtInt(channel.issues.length)} 项</summary><ul>${channel.issues.map((value) => `<li><span class="pill ${value.severity === "error" ? "bad" : "warn"}">${value.severity === "error" ? "错误" : "提示"}</span> ${h(value.message)}</li>`).join("")}</ul></details>`
      : '<span class="pill good">无缺口</span>';
    return `<tr>
      <td><div><strong>${h(channel.title || channel.channel_id)}</strong></div><div class="note mono">${h(channel.handle || channel.channel_id)}</div><div class="note mono">${h(channel.channel_id)}</div></td>
      <td><span class="pill ${status.className}">${status.label}</span></td>
      <td>${publicationComparisonRatio(
        channel.about.matched_snapshots,
        channel.about.revision_snapshots,
        `原始 ${channel.about.raw_observations} · 启用前 ${channel.about.pre_bootstrap_observations} · 无变化 ${channel.about.no_change_observations}`,
      )}</td>
      <td>${publicationComparisonRatio(
        channel.revisions.matched,
        channel.revisions.delivered,
        `Baseline 覆盖 ${channel.revisions.baseline_covered} · 投递中 ${channel.revisions.pending} · Dead ${channel.revisions.dead_letter}`,
      )}</td>
      <td>${publicationComparisonRatio(channel.history.represented, channel.history.activated)}</td>
      <td>${publicationComparisonRatio(
        channel.current.matched_domains,
        3,
        channel.current.live_vector_match ? "业务查询 Current 一致" : "业务查询 Current 未匹配",
      )}</td>
      <td><div class="mono">${h(channel.current.snapshot_id || "-")}</div><div class="note mono">${h(channel.current.watermark || "-")}</div></td>
      <td>${issues}</td>
    </tr>`;
  }).join("");
  return layout({
    title: "业务库批量对账",
    active: "channels",
    body: `
<div class="topbar">
  <div>
    <div class="eyebrow">Publication Audit</div>
    <h1>业务库批量对账</h1>
    <div class="sub">生成时间 ${h(timeText(report.generated_at))} 北京时间</div>
  </div>
  <div class="toolbar"><a class="btn" href="/channels">返回频道列表</a></div>
</div>
<section class="grid grid-4">
  <div class="metric metric-blue"><div><div class="metric-label">已选择</div><div class="metric-value">${fmtInt(report.total)}</div></div><div class="metric-foot">只读双库对账</div></div>
  <div class="metric"><div><div class="metric-label">完全一致</div><div class="metric-value">${fmtInt(report.matched)}</div></div><div class="metric-foot">历史与 Current 均匹配</div></div>
  <div class="metric"><div><div class="metric-label">发现差异</div><div class="metric-value">${fmtInt(report.mismatch)}</div></div><div class="metric-foot">需要查看具体缺口</div></div>
  <div class="metric"><div><div class="metric-label">投递中 / 未发布</div><div class="metric-value">${fmtInt(report.pending + report.not_published)}</div></div><div class="metric-foot">投递中 ${fmtInt(report.pending)} · 未发布 ${fmtInt(report.not_published)}</div></div>
</section>
<section class="table-panel mt">
  <div class="table-tools"><div class="panel-head"><div><h2>逐频道结果</h2><div class="note">About 比较订阅数、总播放量、视频数和观测时间；其他域使用不可变 Revision、Hash、Activation、Version Vector 与 Current Cursor。</div></div></div></div>
  <div class="table-scroll">
    <table>
      <thead><tr><th>频道</th><th>结论</th><th>About 快照</th><th>双库 Revision</th><th>历史表示</th><th>三域 Current</th><th>当前业务快照</th><th>详情</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>
</section>`,
  });
}

function iframePage({ title, active, src, description, refreshHref }) {
  return layout({
    title,
    active,
    body: `
<div class="embed-page">
<div class="topbar">
  <div>
    <div class="eyebrow">Embedded Console</div>
    <h1>${h(title)}</h1>
    <div class="sub">${h(description)}</div>
  </div>
  <div class="toolbar">
    <a class="btn" href="${h(refreshHref)}">刷新</a>
  </div>
</div>
<section class="panel iframe-panel">
  <iframe class="embed-frame" src="${h(src)}" title="${h(title)}"></iframe>
</section>
</div>`,
  });
}

const fieldSources = [
  ["channel_id", "bullmq_crawler", "crawler.channels", "频道主键"],
  ["channel_url", "bullmq_crawler", "crawler.channels", "YouTube 频道 URL"],
  ["handle", "bullmq_crawler", "crawler.channels", "YouTube handle"],
  ["title", "bullmq_crawler", "crawler.channels", "频道标题"],
  ["subscriber_count", "bullmq_crawler", "crawler.channels", "订阅数"],
  ["GetAbout snapshots", "bullmq_crawler", "crawler.channel_about_metric_snapshots", "每次 About 抓取的订阅数、总播放量、总视频数及字段状态"],
  ["latest_run_id", "bullmq_crawler", "crawler.channels", "最近抓取 run"],
  ["content candidates", "bullmq_crawler", "crawler.content_candidates", "uploads 最新 N 条、类型识别、Detail/API 状态和缺失字段"],
  ["content detail", "bullmq_crawler", "crawler.contents", "已分类内容：日期、时长、播放、互动、会员状态及字段来源"],
  ["thumbnail_url", "bullmq_crawler", "crawler.contents", "视频封面图 URL"],
  ["YouTube API", "bullmq_crawler", "crawler.youtube_api_tasks", "仅处理 Detail 后仍缺失的字段，跨频道批量请求"],
  ["agent metrics", "bullmq_crawler", "crawler.agent_profiles", "Agent 画像指标：metrics_json"],
  ["final profile", "bullmq_crawler", "crawler.finalized_profiles", "最终审核/发布候选 JSON：profile_json / quality_json / status"],
];

async function channelDetailDataFrom(queryDb, channelId) {
  const channel = await queryDb(`
    SELECT c.*, fp.status AS final_status, fp.profile_json, fp.quality_json, fp.finalized_at
    FROM crawler.channels c
    LEFT JOIN crawler.finalized_profiles fp ON fp.channel_id = c.channel_id
    WHERE c.channel_id = $1
    LIMIT 1
  `, [channelId]);
  if (channel.rows.length === 0) return null;

  const runs = await queryDb(`
    SELECT run_id, status, crawl_mode, started_at, finished_at, error_message, result_json
    FROM crawler.channel_runs
    WHERE channel_id = $1
    ORDER BY created_at DESC
    LIMIT 10
  `, [channelId]);

  const { contentStats, contents } = await loadChannelCurrentContent(queryDb, channelId);

  const candidates = await queryDb(`
    SELECT candidate_id,source_content_id,position,title,source_url,content_type,type_status,type_source,
           detail_status,api_status,missing_fields,attempts,error_message,result_json,updated_at
    FROM crawler.content_candidates
    WHERE channel_id=$1 AND run_id=$2
    ORDER BY position ASC
  `, [channelId, channel.rows[0].latest_run_id]);

  const aboutSnapshots = await queryDb(`
    SELECT observation.observation_id,observation.kind_sequence,
           observation.observed_at,observation.run_id,
           observation.trigger_reason,observation.outcome,
           observation.outcome_reason_code,observation.crawler_version,
           observation.extractor_versions,
           snapshot.subscriber_count,snapshot.total_view_count,snapshot.total_video_count,
           snapshot.subscriber_count_status,snapshot.total_view_count_status,
           snapshot.total_video_count_status,snapshot.facts_hash,
           (snapshot.observation_id IS NOT NULL) AS snapshot_written
    FROM crawler.crawl_observations observation
    LEFT JOIN crawler.channel_about_metric_snapshots snapshot
      ON snapshot.observation_id=observation.observation_id
    WHERE observation.channel_id=$1
      AND observation.observation_kind='about'
    ORDER BY observation.observed_at DESC,observation.kind_sequence DESC
  `, [channelId]);

  return {
    channel: channel.rows[0],
    runs: runs.rows,
    contentStats,
    contents,
    candidates: candidates.rows,
    aboutSnapshots: aboutSnapshots.rows,
  };
}

async function channelDetailData(channelId) {
  return channelDetailDataFrom(db, channelId);
}

function channelDetailPage(data, options = {}) {
  const channel = data.channel;
  const profile = channel.profile_json || {};
  const quality = channel.quality_json || {};
  const titlePrefix = options.titlePrefix || "频道详情";
  const active = options.active || "channels";
  const eyebrow = options.eyebrow || "Channel Review";
  const databaseName = options.databaseName || "bullmq_crawler";
  const listHref = options.listHref || "/channels";
  const listLabel = options.listLabel || "返回频道列表";
  const allowActions = options.allowActions !== false;
  const sourceRows = fieldSources.map(([field, _database, table, description]) => [field, databaseName, table, description]);
  const actionButtons = allowActions && channel.status === "active"
    ? `<a class="btn" href="/youtube-api">YouTube API 配置</a>
       <form class="inline-form" method="post" action="/channels/${h(encodeURIComponent(channel.channel_id))}/requeue-missing"><button class="btn btn-primary" type="submit">补全缺失字段</button></form>
       <a class="btn" href="/exports/finalized-profiles.jsonl">导出 Profile JSONL</a>`
    : "";
  const dataComplete = Boolean(quality.data_complete ?? quality.publish_ready)
    && Object.keys(quality.missing_content_fields || {}).length === 0
    && Number(quality.unavailable_candidate_count || 0) === 0;
  const removedAlert = channel.status === "removed"
    ? `<div class="alert alert-bad">该频道已封禁/移除，后续 ${CLOCK_LABELS_TEXT} Clock 调度均已永久停用。原因：${h(channel.removed_reason || "channel_removed")}；识别时间：${h(timeText(channel.removed_at))} 北京时间。</div>`
    : "";
  return layout({
    title: `${titlePrefix} - ${channel.title || channel.handle || channel.channel_id}`,
    active,
    body: `
<div class="detail-page">
${removedAlert}
<div class="topbar">
  <div>
    <div class="eyebrow">${h(eyebrow)}</div>
    <h1>${h(channel.title || channel.handle || channel.channel_id)}</h1>
    <div class="sub">审核新爬虫结构化数据。来源数据库：<span class="mono">${h(databaseName)}</span>，schema：<span class="mono">crawler</span>。</div>
  </div>
  <div class="toolbar">
    <a class="btn" href="${h(listHref)}">${h(listLabel)}</a>
    <a class="btn" href="${h(channel.channel_url)}" target="_blank" rel="noreferrer">打开 YouTube</a>
    ${actionButtons}
  </div>
</div>
${data.notice ? `<div class="alert alert-good">${h(data.notice)}</div>` : ""}
${data.error ? `<div class="alert alert-bad">${h(data.error)}</div>` : ""}

<section class="grid grid-4">
  <div class="metric metric-blue"><div><div class="metric-label">订阅数</div><div class="metric-value">${channel.subscriber_count == null ? "未获取" : fmtInt(channel.subscriber_count)}</div></div><div class="metric-foot">${h(channel.handle || "-")}</div></div>
  <div class="metric"><div><div class="metric-label">Agent</div><div class="metric-value" style="font-size:34px;">${h(channel.agent_status)}</div></div><div class="metric-foot">crawler.channels.agent_status</div></div>
  <div class="metric"><div><div class="metric-label">Final</div><div class="metric-value" style="font-size:34px;">${h(channel.final_status || "pending")}</div></div><div class="metric-foot">crawler.finalized_profiles.status</div></div>
  <div class="metric"><div><div class="metric-label">Run</div><div class="metric-value" style="font-size:24px;">${h(channel.latest_run_id || "-")}</div></div><div class="metric-foot">crawler.channels.latest_run_id</div></div>
</section>

<section class="table-panel mt">
  <div class="table-tools"><div class="panel-head"><div><h2>GetAbout 抓取历史</h2><div class="note">${fmtInt(data.aboutSnapshots.length)} 次 About Observation，按抓取时间倒序</div></div></div></div>
  <div class="table-scroll about-snapshot-scroll">
    <table class="about-snapshot-table">
      <thead><tr><th>抓取时间（北京时间）</th><th>订阅数</th><th>总播放量</th><th>总视频数</th><th>结果</th><th>触发来源</th><th>Run / 版本</th></tr></thead>
      <tbody>
      ${data.aboutSnapshots.map((item) => `<tr>
        <td><strong class="mono">${h(timeText(item.observed_at))}</strong><div class="note">序号 #${fmtInt(item.kind_sequence)}</div></td>
        <td>${aboutMetricCell(item.subscriber_count, item.subscriber_count_status)}</td>
        <td>${aboutMetricCell(item.total_view_count, item.total_view_count_status)}</td>
        <td>${aboutMetricCell(item.total_video_count, item.total_video_count_status)}</td>
        <td>${aboutObservationResult(item)}${item.snapshot_written ? "" : '<div class="note">本次未生成数值快照</div>'}</td>
        <td>${h(aboutTriggerText(item.trigger_reason))}<div class="note mono">${h(item.trigger_reason || "-")}</div></td>
        <td><div class="mono">${h(item.run_id || "-")}</div><div class="note mono">${h(item.crawler_version || "-")}</div></td>
      </tr>`).join("")}
      ${data.aboutSnapshots.length === 0 ? '<tr><td colspan="7" class="muted">暂无 GetAbout 抓取记录。</td></tr>' : ""}
      </tbody>
    </table>
  </div>
</section>

<section class="grid detail-overview-grid mt">
  <div class="panel">
    <div class="panel-head"><div><h2>字段来源</h2><div class="note">用于审核和后续映射到业务库</div></div></div>
    <div class="table-scroll">
      <table class="source-table">
        <thead><tr><th>字段</th><th>数据库</th><th>表</th><th>说明</th></tr></thead>
        <tbody>${sourceRows.map((row) => `<tr>${row.map((cell) => `<td class="mono">${h(cell)}</td>`).join("")}</tr>`).join("")}</tbody>
      </table>
    </div>
  </div>
  <div class="panel">
    <div class="panel-head"><div><h2>Final Profile</h2><div class="note">crawler.finalized_profiles.profile_json</div></div></div>
    <textarea class="json" readonly>${h(jsonText(profile))}</textarea>
  </div>
</section>

<section class="panel mt">
  <div class="panel-head"><div><h2>完整性检查</h2><div class="note">按当前 run 实时汇总，精确显示缺失字段及所属阶段</div></div><span class="pill ${dataComplete ? "good" : "warn"}">${dataComplete ? "完整" : h(channel.final_status || "pending")}</span></div>
  <div class="form-grid">
    <div class="field"><label>期望 / 已分类</label><div class="mono">${fmtInt(quality.expected_content_count)} / ${fmtInt(quality.classified_content_count)}</div></div>
    <div class="field"><label>Detail / API 待处理</label><div class="mono">${fmtInt(quality.detail_open_count)} / ${fmtInt(quality.api_open_count)}</div></div>
    <div class="field"><label>超期排除</label><div class="mono">${fmtInt(quality.age_excluded_candidate_count)}</div></div>
    <div class="field"><label>频道字段缺失</label><div class="mono">${h((quality.missing_channel_fields || []).join(", ") || "无")}</div></div>
    <div class="field"><label>Agent 字段缺失</label><div class="mono">${h((quality.missing_agent_fields || []).join(", ") || "无")}</div></div>
    <div class="field" style="grid-column:span 2;"><label>内容字段缺失</label><div class="mono">${h(Object.entries(quality.missing_content_fields || {}).map(([field, count]) => `${field}(${count})`).join(" · ") || "无")}</div></div>
  </div>
</section>

<section class="table-panel mt">
  <div class="table-tools"><div class="panel-head"><div><h2>候选处理状态</h2><div class="note">crawler.content_candidates，包含尚未完成类型识别或等待 API 的内容</div></div></div></div>
  <div class="table-scroll">
    <table>
      <thead><tr><th>#</th><th>内容</th><th>类型</th><th>Detail</th><th>API</th><th>缺失字段</th><th>错误</th></tr></thead>
      <tbody>
        ${data.candidates.map((item) => {
          const excluded = item.result_json?.scope?.status === "excluded";
          const detailLabel = excluded ? "excluded_age" : item.detail_status;
          return `<tr><td class="num">${fmtInt(item.position)}</td><td><a href="${h(item.source_url || `https://www.youtube.com/watch?v=${item.source_content_id}`)}" target="_blank" rel="noreferrer">${h(item.title || item.source_content_id)}</a><div class="note mono">${h(item.source_content_id)}</div></td><td>${h(item.content_type || item.type_status)}<div class="note">${h(item.type_source || "-")}</div></td><td><span class="pill ${excluded ? "muted" : statusClass(item.detail_status)}">${h(detailLabel)}</span><div class="note">attempt ${fmtInt(item.attempts)}</div></td><td><span class="pill ${statusClass(item.api_status)}">${h(item.api_status)}</span></td><td class="mono">${h((item.missing_fields || []).join(", ") || "-")}</td><td class="note">${h(excluded ? `>${fmtInt(item.result_json.scope.max_age_days)} 天` : (item.error_message || "-"))}</td></tr>`;
        }).join("")}
        ${data.candidates.length === 0 ? `<tr><td colspan="7" class="muted">暂无候选内容。</td></tr>` : ""}
      </tbody>
    </table>
  </div>
</section>

<section class="table-panel mt">
  <div class="table-tools"><div class="panel-head"><div><h2>内容统计</h2><div class="note">crawler.contents</div></div></div></div>
  <div class="table-scroll">
    <table class="stat-table">
      <thead><tr><th>类型</th><th>总数</th><th>秒级时间</th><th>有时长</th><th>描述已判定</th><th>有互动/播放</th><th>会员/付费</th></tr></thead>
      <tbody>${data.contentStats.map((row) => `<tr><td>${h(row.content_type)}</td><td class="num">${fmtInt(row.total)}</td><td class="num">${fmtInt(row.exact_second)}</td><td class="num">${fmtInt(row.has_length)}</td><td class="num">${fmtInt(row.description_resolved)}</td><td class="num">${fmtInt(row.has_stats)}</td><td class="num">${fmtInt(row.members_only)}</td></tr>`).join("")}</tbody>
    </table>
  </div>
</section>

<section class="table-panel mt">
    <div class="table-tools"><div class="panel-head"><div><h2>当前内容</h2><div class="note">频道当前唯一内容目录，按发布时间倒序</div></div></div></div>
  <div class="table-scroll">
    <table class="content-table">
      <thead><tr><th>类型</th><th>标题</th><th>描述 / 话题</th><th>发布时间（北京时间）</th><th>访问</th><th>时长</th><th>播放</th><th>Like/Comment</th></tr></thead>
      <tbody>
      ${data.contents.map((item) => `<tr><td>${h(item.content_type)}</td><td><a href="${h(item.url || "#")}" target="_blank" rel="noreferrer">${h(item.title || item.source_content_id)}</a><div class="note mono">${h(item.source_content_id)} · ${h(item.extractor_version || "-")}</div></td><td>${videoTextMetadata(item)}</td><td>${h(publishedTimeText(item))}<div class="note">${h(publishedPrecisionText(item))}</div>${liveTimeNote(item)}</td><td>${accessBadge(item)}</td><td>${h(item.length_text || "-")}${countStatusNote(item.duration_status)}<div class="note">${h(item.duration_source || "-")}</div></td><td>${h(item.view_count_text || "-")}${countStatusNote(item.view_count_status)}<div class="note">${h(item.view_count_source || "-")}</div></td><td>${countValue(item.like_count)} / ${commentCountValue(item)}<div class="note">${h(item.like_count_status)} / ${h(item.comment_count_status)}</div><div class="note">${h(item.like_count_source || "-")} / ${h(item.comment_count_source || "-")}</div></td></tr>`).join("")}
      ${data.contents.length === 0 ? `<tr><td colspan="8" class="muted">暂无已分类内容。</td></tr>` : ""}
      </tbody>
    </table>
  </div>
</section>

${options.extraSections || ""}
</div>`,
  });
}

async function getActiveAgentConfig() {
  const row = await db(`
    SELECT c.*, t.name AS prompt_name, t.version AS prompt_version, t.template_text, t.output_schema_json, t.status AS prompt_status
    FROM crawler.agent_configs c
    LEFT JOIN crawler.agent_prompt_templates t ON t.template_id = c.prompt_template_id
    WHERE c.enabled = true
    ORDER BY c.config_id ASC
    LIMIT 1
  `);
  return row.rows[0] || null;
}

async function listAgentConfigs() {
  const rows = await db(`
    WITH latest_job_events AS (
      SELECT DISTINCT ON (job_id) job_id,status,payload_json
      FROM crawler.task_events
      WHERE queue_name='youtube-agent-batch'
        AND status IN ('started','completed','failed')
      ORDER BY job_id,event_id DESC
    ), config_stats AS (
      SELECT (payload_json->>'agent_config_id')::bigint AS config_id,
             count(*) FILTER (WHERE status='completed')::int AS succeeded_jobs,
             count(*) FILTER (WHERE status='failed')::int AS failed_jobs,
             count(*) FILTER (
               WHERE status='completed' AND COALESCE(payload_json->>'duration_ms','') ~ '^\\d+$'
             )::int AS response_samples,
             round(avg(
               CASE
                 WHEN status='completed' AND COALESCE(payload_json->>'duration_ms','') ~ '^\\d+$'
                   THEN (payload_json->>'duration_ms')::numeric
                 ELSE NULL
               END
             ))::bigint AS average_response_ms
      FROM latest_job_events
      WHERE status IN ('completed','failed')
        AND COALESCE(payload_json->>'agent_config_id','') ~ '^\\d+$'
      GROUP BY (payload_json->>'agent_config_id')::bigint
    ), latest_connection_tests AS (
      SELECT DISTINCT ON ((payload_json->>'agent_config_id')::bigint)
             (payload_json->>'agent_config_id')::bigint AS config_id,
             status,payload_json,error_message,created_at
      FROM crawler.task_events
      WHERE queue_name='agent-config-test'
        AND status IN ('completed','failed')
        AND COALESCE(payload_json->>'agent_config_id','') ~ '^\\d+$'
      ORDER BY (payload_json->>'agent_config_id')::bigint,event_id DESC
    )
    SELECT c.*, t.name AS prompt_name, t.version AS prompt_version,
           t.template_text, t.output_schema_json, t.status AS prompt_status,
           COALESCE(stats.succeeded_jobs,0)::int AS succeeded_jobs,
           COALESCE(stats.failed_jobs,0)::int AS failed_jobs,
           COALESCE(stats.response_samples,0)::int AS response_samples,
           stats.average_response_ms,
           connection.status AS connection_test_status,
           connection.payload_json->>'duration_ms' AS connection_test_duration_ms,
           connection.payload_json->>'response_preview' AS connection_test_response_preview,
           connection.payload_json->>'endpoint' AS connection_test_endpoint,
           connection.error_message AS connection_test_error,
           connection.created_at AS connection_test_at
    FROM crawler.agent_configs c
    LEFT JOIN crawler.agent_prompt_templates t ON t.template_id = c.prompt_template_id
    LEFT JOIN config_stats stats ON stats.config_id = c.config_id
    LEFT JOIN latest_connection_tests connection ON connection.config_id = c.config_id
    ORDER BY c.enabled DESC, c.config_id ASC
  `);
  const configs = [];
  for (const config of rows.rows) {
    configs.push({
      ...config,
      llm_settings: await ensureAgentLlmSettings({ config }),
    });
  }
  return configs;
}

async function getAgentConfigById(configId) {
  const id = intValue(configId, 0, 1);
  if (!id) return null;
  const row = await db(`
    SELECT c.*, t.name AS prompt_name, t.version AS prompt_version, t.template_text, t.output_schema_json, t.status AS prompt_status
    FROM crawler.agent_configs c
    LEFT JOIN crawler.agent_prompt_templates t ON t.template_id = c.prompt_template_id
    WHERE c.config_id = $1
    LIMIT 1
  `, [id]);
  return row.rows[0] || null;
}

function modelSelectHtml(currentModel) {
  const model = String(currentModel || "rules-agent-v1");
  const modelKnown = agentModelOptions.some(([value]) => value === model);
  return [
    ...agentModelOptions,
    ...(!modelKnown ? [[model, model]] : []),
    ["__custom__", "自定义模型"],
  ].map(([value, label]) => `<option value="${h(value)}" ${value === model ? "selected" : ""}>${h(label)}</option>`).join("");
}

async function ensureAgentConfig() {
  const existing = await getActiveAgentConfig();
  if (existing) return existing;
  const template = await db(`
    INSERT INTO crawler.agent_prompt_templates (name, version, template_text, output_schema_json, status, is_default)
    VALUES ('Dashboard Agent Prompt', 1, 'Return strict JSON for each input_url.', '{}'::jsonb, 'active', true)
    RETURNING template_id
  `);
  await db(`
    INSERT INTO crawler.agent_configs (name, provider, model, prompt_template_id, batch_size, min_batch_size, enabled, is_default)
    VALUES ('default', 'rules', 'rules-agent-v1', $1, 50, 50, true, false)
    ON CONFLICT (name) DO UPDATE SET enabled = true
  `, [template.rows[0].template_id]);
  return getActiveAgentConfig();
}

async function agentRuntimeState() {
  const queue = queues["youtube-agent-batch"];
  const [workerCount, globalConcurrency, activeJobs, outstandingJobs] = await Promise.all([
    queue.getWorkersCount(),
    queue.getGlobalConcurrency(),
    queue.getJobs(["active"], 0, 9999, true),
    queue.getJobs(["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"], 0, 9999, true),
  ]);
  const countByConfig = (jobs) => jobs.reduce((counts, job) => {
    const configId = Number(job.data?.agent_config_id);
    if (Number.isFinite(configId) && configId > 0) counts.set(configId, (counts.get(configId) || 0) + 1);
    return counts;
  }, new Map());
  return {
    workerCount,
    globalConcurrency,
    activeCount: activeJobs.length,
    outstandingCount: outstandingJobs.length,
    activeByConfig: countByConfig(activeJobs),
    outstandingByConfig: countByConfig(outstandingJobs),
  };
}

async function agentPage(req) {
  const activeConfig = await ensureAgentConfig();
  const configs = await listAgentConfigs();
  const runtime = await agentRuntimeState();
  const configuredCapacity = configs
    .filter((config) => config.enabled)
    .reduce((total, config) => total + Number(config.max_workers || 1), 0);
  const providerOptionsHtml = ["rules", "openai-compatible", "openai", "deepseek", "gemini", "qwen"]
    .map((provider) => `<option value="${h(provider)}">${h(provider)}</option>`)
    .join("");
  const newConfig = {
    config_id: null,
    name: "",
    provider: activeConfig.provider || "openai-compatible",
    model: activeConfig.model || "grok-4.3",
    endpoint: activeConfig.endpoint || defaultAgentBaseUrl,
    batch_size: activeConfig.batch_size || 50,
    max_workers: 1,
    enabled: true,
    timeout_ms: activeConfig.timeout_ms || 120000,
    max_retries: activeConfig.max_retries || 2,
    tools_json: activeConfig.tools_json || [{ type: "web_search" }],
    template_text: activeConfig.template_text || "",
    prompt_name: "New Agent Prompt",
    prompt_version: 1,
    prompt_status: "draft",
    llm_base_url: activeConfig.endpoint || defaultAgentBaseUrl,
    key_count: 0,
    key_tails: [],
  };
  const cannotDeleteReason = "至少保留一个配置";
  const enabledConfigCount = configs.filter((item) => item.enabled).length;
  const clientConfigs = configs.map((item) => ({
    config_id: Number(item.config_id),
    name: item.name || "",
    provider: item.provider || "rules",
    model: item.model || "rules-agent-v1",
    endpoint: item.endpoint || "",
    batch_size: Number(item.batch_size || 50),
    max_workers: Number(item.max_workers || 1),
    enabled: Boolean(item.enabled),
    timeout_ms: Number(item.timeout_ms || 120000),
    max_retries: Number(item.max_retries || 2),
    prompt_name: item.prompt_name || "",
    prompt_version: Number(item.prompt_version || 1),
    prompt_status: item.prompt_status || "",
    template_text: item.template_text || "",
    tools_json: item.tools_json || [{ type: "web_search" }],
    llm_base_url: item.llm_settings?.base_url || item.endpoint || "",
    key_count: Number(item.llm_settings?.key_count || 0),
    key_tails: item.llm_settings?.key_tails || [],
  }));
  const rowsHtml = configs.map((item) => {
    const activeWorkers = Number(runtime.activeByConfig.get(Number(item.config_id)) || 0);
    const outstandingWorkers = Number(runtime.outstandingByConfig.get(Number(item.config_id)) || 0);
    const succeededJobs = Number(item.succeeded_jobs || 0);
    const failedJobs = Number(item.failed_jobs || 0);
    const terminalJobs = succeededJobs + failedJobs;
    const successRate = terminalJobs > 0 ? (succeededJobs * 100) / terminalJobs : null;
    const successRateClass = successRate == null ? "muted-pill" : successRate >= 90 ? "good" : successRate >= 70 ? "warn" : "bad";
    const connectionTestStatus = String(item.connection_test_status || "");
    const connectionTestOk = connectionTestStatus === "completed";
    const connectionTestLabel = connectionTestStatus ? (connectionTestOk ? "可用" : "失败") : "未测试";
    const connectionTestClass = connectionTestStatus ? (connectionTestOk ? "good" : "bad") : "muted-pill";
    const connectionTestDetail = connectionTestStatus
      ? `${fmtResponseDurationMs(item.connection_test_duration_ms)} · ${timeText(item.connection_test_at)} 北京时间`
      : "尚未测试";
    const connectionTestPreview = connectionTestOk
      ? String(item.connection_test_response_preview || "连接成功")
      : String(item.connection_test_error || "");
    const deleteDisabled = configs.length <= 1 || outstandingWorkers > 0;
    const toggleDisabled = Boolean(item.enabled) && enabledConfigCount <= 1;
    const toggleSubmit = toggleDisabled
      ? "false"
      : item.enabled
        ? "confirm('确认停用这个 Agent 渠道？停用后不再派发新任务，已有任务会继续执行。')"
        : "true";
    return `<tr>
      <td class="mono">#${h(item.config_id)}</td>
      <td><strong>${h(item.name)}</strong></td>
      <td>
        <form class="inline-form" method="post" action="/agent/configs/${h(item.config_id)}/toggle" onsubmit="return ${toggleSubmit};">
          <button class="pill agent-status-toggle ${item.enabled ? "good" : "muted-pill"}" type="submit" ${toggleDisabled ? "disabled" : ""} title="${h(toggleDisabled ? "至少保留一个启用的 Agent 渠道" : (item.enabled ? "点击停用" : "点击启用"))}" aria-label="${h(item.enabled ? `停用 ${item.name}` : `启用 ${item.name}`)}">${item.enabled ? "启用" : "停用"}</button>
        </form>
      </td>
      <td class="agent-test-cell" data-agent-test-cell="${h(item.config_id)}" aria-live="polite">
        <div class="agent-test-head"><span class="pill ${connectionTestClass}" data-agent-test-badge>${connectionTestLabel}</span><button class="btn small-btn" type="button" data-agent-test="${h(item.config_id)}">测试</button></div>
        <div class="note" data-agent-test-detail>${h(connectionTestDetail)}</div>
        <div class="note agent-test-preview" data-agent-test-preview title="${h(connectionTestPreview)}">${h(connectionTestPreview)}</div>
      </td>
      <td class="num">${fmtInt(item.batch_size)}</td>
      <td class="num">${fmtInt(activeWorkers)} / ${fmtInt(item.max_workers || 1)}<div class="note">待处理 ${fmtInt(outstandingWorkers)}</div></td>
      <td class="num"><strong>${h(fmtResponseDurationMs(item.average_response_ms))}</strong><div class="note">${fmtInt(item.response_samples)} 个成功样本</div></td>
      <td class="num"><span class="pill ${successRateClass}">${successRate == null ? "-" : `${successRate.toFixed(1)}%`}</span><div class="note">${fmtInt(succeededJobs)} 成功 / ${fmtInt(failedJobs)} 失败</div></td>
      <td>
        <div class="toolbar" style="justify-content:flex-start;">
          <button class="btn small-btn" type="button" onclick="openAgentConfigModal('edit', ${h(Number(item.config_id))})">编辑</button>
          <form class="inline-form" method="post" action="/agent/configs/${h(item.config_id)}/delete" onsubmit="return ${deleteDisabled ? "false" : "confirm('确认删除这个 Agent Config？')"};">
            <button class="btn btn-danger small-btn" type="submit" ${deleteDisabled ? "disabled" : ""} title="${h(deleteDisabled ? (outstandingWorkers > 0 ? "仍有该配置的待处理任务" : cannotDeleteReason) : "删除配置")}">删除</button>
          </form>
        </div>
      </td>
    </tr>`;
  }).join("");
  return layout({
    title: "Agent 配置",
    active: "agent",
    mainClass: "agent-main",
    body: `
<div class="topbar">
  <div>
    <div class="eyebrow">Agent Settings</div>
    <h1>Agent 配置</h1>
    <div class="sub">所有启用配置同时参与 Agent 调度；Batch Size 控制单次频道数，最大 Worker 控制该渠道的并发请求上限。</div>
  </div>
  <div class="toolbar"><button class="btn btn-primary" type="button" onclick="openAgentConfigModal('create')">新建配置</button><a class="btn" href="/queries">Query 词库</a></div>
</div>
${req.query.notice ? `<div class="alert alert-good">${h(req.query.notice)}</div>` : ""}
${req.query.error ? `<div class="alert alert-bad">${h(req.query.error)}</div>` : ""}

<section class="grid grid-3">
  <div class="metric metric-blue"><div><div class="metric-label">Agent Worker</div><div class="metric-value">${fmtInt(runtime.workerCount)}</div></div><div class="metric-foot">队列并发上限 ${fmtInt(runtime.globalConcurrency)}</div></div>
  <div class="metric"><div><div class="metric-label">配置容量</div><div class="metric-value">${fmtInt(configuredCapacity)}</div></div><div class="metric-foot">所有启用配置最大 Worker 合计</div></div>
  <div class="metric"><div><div class="metric-label">Agent 请求</div><div class="metric-value">${fmtInt(runtime.activeCount)}</div></div><div class="metric-foot">待处理 ${fmtInt(runtime.outstandingCount)}</div></div>
</section>

<section class="table-panel mt">
  <div class="table-tools"><div class="panel-head"><div><h2>Agent Config 列表</h2><div class="note">启用的配置会按最大 Worker 容量同时领取新 Agent 任务；统计基于任务事件保留期内每个 Job 的最新状态。</div></div></div></div>
  <div class="table-scroll">
    <table>
      <thead><tr><th>ID</th><th>名称</th><th>状态</th><th>连通测试</th><th>Batch</th><th>Active / Max</th><th>平均返回时间</th><th>成功率</th><th>操作</th></tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table>
  </div>
</section>

<dialog id="agent-config-modal" class="modal modal-wide">
  <div class="modal-head">
    <div><h2 id="agent-modal-title">Agent Config</h2><div id="agent-modal-note" class="note">Provider、BaseURL、SK、model、batch 配置</div></div>
    <button class="btn small-btn" type="button" onclick="document.getElementById('agent-config-modal').close()">关闭</button>
  </div>
  <form method="post" action="/agent">
    <input type="hidden" name="config_id" id="agent_config_id">
    <input type="hidden" name="mode" id="agent_mode" value="update">
    <div class="modal-body modal-body-scroll grid">
      <section class="panel">
        <div class="form-grid">
          <div class="field"><label>名称</label><input name="name" id="agent_name" placeholder="例如 grok-primary"></div>
          <div class="field"><label>Provider</label><select name="provider" id="agent_provider">${providerOptionsHtml}</select></div>
          <div class="field"><label>Model</label><select name="model_select" id="agent_model_select">${modelSelectHtml("rules-agent-v1")}</select></div>
          <div class="field"><label>自定义模型</label><input name="model_custom" id="agent_model_custom" placeholder="选择自定义时填写"></div>
          <div class="field"><label>Batch Size（每批最多）</label><input name="batch_size" id="agent_batch_size" type="number" min="1" max="50"></div>
          <div class="field"><label>最大 Worker</label><input name="max_workers" id="agent_max_workers" type="number" min="1" max="100"></div>
          <label class="check-row"><input type="checkbox" name="enabled" id="agent_enabled">参与 Agent 调度</label>
        </div>
        <div class="form-grid mt">
          <div class="field" style="grid-column:span 2;"><label>BaseURL</label><input name="base_url" id="agent_base_url" placeholder="https://api.openai.com/v1"></div>
          <div class="field" style="grid-column:span 2;"><label>当前 SK</label><input id="agent_current_sk" readonly></div>
          <div class="field"><label>Timeout MS</label><input name="timeout_ms" id="agent_timeout_ms" type="number" min="5000" max="3600000" step="1000"></div>
          <div class="field"><label>Max Retries</label><input name="max_retries" id="agent_max_retries" type="number"></div>
          <label class="check-row"><input type="checkbox" name="clear_agent_api_keys" id="agent_clear_keys">清空当前 SK</label>
        </div>
        <div class="field mt">
          <label>新增/替换 SK</label>
          <textarea name="agent_api_keys" id="agent_api_keys" class="json" style="min-height:120px;" autocomplete="off"></textarea>
        </div>
      </section>
      <section class="grid grid-2">
        <div class="panel">
          <div class="panel-head"><div><h2>Prompt Template</h2><div id="agent_prompt_meta" class="note"></div></div></div>
          <textarea name="template_text" id="agent_template_text" class="json" style="min-height:420px;"></textarea>
        </div>
        <div class="panel">
          <div class="panel-head"><div><h2>Tools JSON</h2><div class="note">必须是合法 JSON</div></div></div>
          <textarea name="tools_json" id="agent_tools_json" class="json"></textarea>
        </div>
      </section>
    </div>
    <div class="modal-actions">
      <button class="btn" type="button" onclick="document.getElementById('agent-config-modal').close()">取消</button>
      <button class="btn btn-primary" id="agent_submit_btn" type="submit">保存修改</button>
    </div>
  </form>
</dialog>

<script>
  const agentConfigs = ${scriptJson(clientConfigs)};
  const newAgentConfig = ${scriptJson(newConfig)};
  function agentField(id) { return document.getElementById(id); }
  function setAgentValue(id, value) {
    const field = agentField(id);
    if (field) field.value = value == null ? "" : String(value);
  }
  function setAgentSelect(selectId, value) {
    const select = agentField(selectId);
    if (!select) return;
    const text = value == null ? "" : String(value);
    const option = Array.from(select.options).find((item) => item.value === text);
    if (option) select.value = text;
    else select.value = "__custom__";
  }
  function setAgentModel(value) {
    const text = value == null ? "" : String(value);
    const select = agentField("agent_model_select");
    const custom = agentField("agent_model_custom");
    const option = select ? Array.from(select.options).find((item) => item.value === text) : null;
    if (option) {
      select.value = text;
      if (custom) custom.value = "";
    } else {
      if (select) select.value = "__custom__";
      if (custom) custom.value = text;
    }
  }
  function skText(config) {
    const tails = Array.isArray(config.key_tails) ? config.key_tails : [];
    return tails.length > 0 ? tails.map((tail) => "****" + tail).join(" / ") : "未配置，LLM Agent 不会调用模型";
  }
  function openAgentConfigModal(mode, configId) {
    const isCreate = mode === "create";
    const config = isCreate ? { ...newAgentConfig } : agentConfigs.find((item) => Number(item.config_id) === Number(configId));
    if (!config) return;
    setAgentValue("agent_config_id", isCreate ? "" : config.config_id);
    setAgentValue("agent_mode", isCreate ? "create" : "update");
    setAgentValue("agent_name", config.name || "");
    setAgentSelect("agent_provider", config.provider || "rules");
    setAgentModel(config.model || "rules-agent-v1");
    setAgentValue("agent_batch_size", config.batch_size || 50);
    setAgentValue("agent_max_workers", config.max_workers || 1);
    setAgentValue("agent_base_url", config.llm_base_url || config.endpoint || "");
    setAgentValue("agent_current_sk", isCreate ? "新建配置暂无 SK" : skText(config));
    setAgentValue("agent_timeout_ms", config.timeout_ms || 120000);
    setAgentValue("agent_max_retries", config.max_retries || 2);
    setAgentValue("agent_api_keys", "");
    setAgentValue("agent_template_text", config.template_text || "");
    setAgentValue("agent_tools_json", JSON.stringify(config.tools_json || [{ type: "web_search" }], null, 2));
    if (agentField("agent_clear_keys")) agentField("agent_clear_keys").checked = false;
    if (agentField("agent_enabled")) agentField("agent_enabled").checked = config.enabled !== false;
    const keyCount = Number(config.key_count || 0);
    if (agentField("agent_api_keys")) {
      agentField("agent_api_keys").placeholder = keyCount > 0 ? "留空则保留当前 SK；如需替换，按一行一个 sk 粘贴" : "一行一个 sk";
    }
    if (agentField("agent-modal-title")) agentField("agent-modal-title").textContent = isCreate ? "新建 Agent Config" : "编辑 Agent Config";
    if (agentField("agent-modal-note")) agentField("agent-modal-note").textContent = isCreate ? "创建后启用即可与其他配置同时工作" : "正在编辑 config #" + config.config_id;
    if (agentField("agent_prompt_meta")) agentField("agent_prompt_meta").textContent = (config.prompt_name || "Prompt") + " v" + (config.prompt_version || 1) + " · " + (config.prompt_status || "");
    if (agentField("agent_submit_btn")) agentField("agent_submit_btn").textContent = isCreate ? "创建配置" : "保存修改";
    openModal("agent-config-modal");
  }
  function formatAgentTestDuration(value) {
    const milliseconds = Number(value);
    if (!Number.isFinite(milliseconds) || milliseconds < 0) return "-";
    if (milliseconds < 1000) return Math.round(milliseconds) + " ms";
    if (milliseconds < 60000) return (milliseconds / 1000).toFixed(1) + " s";
    const seconds = Math.round(milliseconds / 1000);
    return Math.floor(seconds / 60) + "m " + (seconds % 60) + "s";
  }
  function formatAgentTestTime(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime())
      ? "刚刚"
      : date.toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false }) + " 北京时间";
  }
  async function runAgentConnectionTest(button) {
    const configId = button.getAttribute("data-agent-test");
    const cell = button.closest("[data-agent-test-cell]");
    const badge = cell?.querySelector("[data-agent-test-badge]");
    const detail = cell?.querySelector("[data-agent-test-detail]");
    const preview = cell?.querySelector("[data-agent-test-preview]");
    button.disabled = true;
    button.textContent = "测试中";
    if (badge) {
      badge.className = "pill warn";
      badge.textContent = "测试中";
    }
    if (detail) detail.textContent = "正在发送 hello...";
    if (preview) {
      preview.textContent = "";
      preview.title = "";
    }
    try {
      const response = await fetch("/agent/configs/" + encodeURIComponent(configId) + "/test", {
        method: "POST",
        headers: { accept: "application/json" },
      });
      const payload = await response.json();
      const ok = Boolean(payload.ok);
      const message = ok ? (payload.response_preview || "连接成功") : (payload.error || "连接测试失败");
      if (badge) {
        badge.className = "pill " + (ok ? "good" : "bad");
        badge.textContent = ok ? "可用" : "失败";
      }
      if (detail) detail.textContent = formatAgentTestDuration(payload.duration_ms) + " · " + formatAgentTestTime(payload.tested_at);
      if (preview) {
        preview.textContent = message;
        preview.title = message;
      }
    } catch (error) {
      const message = String(error?.message || error || "连接测试失败");
      if (badge) {
        badge.className = "pill bad";
        badge.textContent = "失败";
      }
      if (detail) detail.textContent = "请求失败";
      if (preview) {
        preview.textContent = message;
        preview.title = message;
      }
    } finally {
      button.disabled = false;
      button.textContent = "测试";
    }
  }
  document.querySelectorAll("[data-agent-test]").forEach((button) => {
    button.addEventListener("click", () => runAgentConnectionTest(button));
  });
</script>`,
  });
}

async function crawlerConfigPage(req) {
  const crawlSettings = await ensureCrawlSettings();
  const operations = await crawlerOperationalMetrics();
  const discoverStopMinQualifiedPercent = Math.round(Number(crawlSettings.discover_stop_min_qualified_ratio) * 100);
  const channelProxy = operations.proxy.roles?.channel || {};
  const storageTypeRows = operations.storageTypes.map((item) => `
    <tr><td>${h(item.object_type)}</td><td class="num">${fmtInt(item.objects)}</td><td class="num">${h(fmtBytes(item.stored_bytes))}</td></tr>
  `).join("");
  return layout({
    title: "爬虫配置",
    active: "crawler",
    body: `
<div class="topbar">
  <div>
    <div class="eyebrow">Crawler Settings</div>
    <h1>爬虫配置</h1>
    <div class="sub">控制频道筛选、Discover 翻页止损、每频道内容数量和 Detail 抓取重试。</div>
  </div>
  <div class="toolbar"><a class="btn" href="/crawler">刷新</a></div>
</div>
${req.query.notice ? `<div class="alert alert-good">${h(req.query.notice)}</div>` : ""}
${req.query.error ? `<div class="alert alert-bad">${h(req.query.error)}</div>` : ""}
<form method="post" action="/crawler" class="grid">
  <section class="panel">
    <div class="panel-head"><div><h2>抓取规则</h2><div class="note">内容同时受最大条数和发布时间窗口限制，频道三个 Tab 提供精确类型。</div></div></div>
    <div class="form-grid">
      <div class="field"><label>最低粉丝量</label><input name="min_subscriber_count" type="number" min="0" max="1000000000" value="${h(crawlSettings.min_subscriber_count)}"></div>
      <div class="field"><label>停止翻页触发合格率 %</label><input name="discover_stop_min_qualified_percent" type="number" min="0" max="100" step="1" value="${h(discoverStopMinQualifiedPercent)}"></div>
      <div class="field"><label>每频道最新上传条数</label><input name="channel_content_limit" type="number" min="1" max="100" value="${h(crawlSettings.channel_content_limit)}"></div>
      <div class="field"><label>内容最长时间（天）</label><input name="content_max_age_days" type="number" min="0" max="3650" value="${h(crawlSettings.content_max_age_days)}"></div>
      <div class="field"><label>Detail 最大抓取轮次</label><input name="detail_max_attempts" type="number" min="1" max="10" value="${h(crawlSettings.detail_max_attempts)}"></div>
      <div class="field"><label>单频道 Detail 并发</label><input name="detail_concurrency" type="number" min="1" max="4" value="${h(crawlSettings.detail_concurrency)}"></div>
      <div class="field"><label>发布时间精度</label><input readonly value="yt-dlp 优先获取秒级时间，缺失时保留日期精度"></div>
      <div class="field"><label>当前规则</label><input readonly value="${h(`粉丝 >= ${fmtInt(crawlSettings.min_subscriber_count)}；合格率 < ${discoverStopMinQualifiedPercent}% 停止；最多 ${fmtInt(crawlSettings.channel_content_limit)} 条且不超过 ${fmtInt(crawlSettings.content_max_age_days)} 天；Detail ${fmtInt(crawlSettings.detail_max_attempts)} 轮 / 并发 ${fmtInt(crawlSettings.detail_concurrency)}`)}"></div>
      <div class="field"><label>生效范围</label><input readonly value="保存后对新开始的 Channel Job 生效，不批量重抓已有频道"></div>
    </div>
    <div class="toolbar mt" style="justify-content:flex-start;"><button class="btn btn-primary" type="submit">保存爬虫配置</button></div>
  </section>
	</form>
<section class="grid grid-4 mt">
  <div class="metric metric-blue"><div><div class="metric-label">Channel 出口</div><div class="metric-value">${fmtInt(channelProxy.ready)}</div></div><div class="metric-foot">健康 ${fmtInt(operations.proxy.active)} · 备用 ${fmtInt(operations.proxy.reserve)} · 冷却 ${fmtInt(operations.proxy.cooldown)}</div></div>
  <div class="metric"><div><div class="metric-label">频道 P50 / P95</div><div class="metric-value" style="font-size:28px;">${h(fmtDurationMs(operations.runtime.p50_ms))} / ${h(fmtDurationMs(operations.runtime.p95_ms))}</div></div><div class="metric-foot">最近 ${fmtInt(operations.runtime.samples)} 个完成任务</div></div>
  <div class="metric"><div><div class="metric-label">MinIO 索引占用</div><div class="metric-value" style="font-size:32px;">${h(fmtBytes(operations.storage.stored_bytes))}</div></div><div class="metric-foot">${fmtInt(operations.storage.objects)} 个对象 · 今日 ${h(fmtBytes(operations.storage.stored_today))}</div></div>
  <div class="metric"><div><div class="metric-label">Finalize</div><div class="metric-value" style="font-size:32px;">${h(Number(operations.finalize.average_runs || 0).toFixed(2))}x</div></div><div class="metric-foot">陈旧 Run ${fmtInt(operations.finalize.stale_profiles)} · 最高 ${fmtInt(operations.finalize.maximum_runs)}x</div></div>
</section>
<section class="table-panel mt">
  <div class="table-tools"><div class="panel-head"><div><h2>运行与存储</h2><div class="note">PostgreSQL 连接 ${fmtInt(operations.connections.active)} active / ${fmtInt(operations.connections.idle)} idle / ${fmtInt(operations.connections.maximum)} max</div></div></div></div>
  <div class="table-wrap"><table><thead><tr><th>MinIO 对象类型</th><th class="num">对象数</th><th class="num">存储占用</th></tr></thead><tbody>${storageTypeRows || '<tr><td colspan="3" class="empty">暂无对象</td></tr>'}</tbody></table></div>
</section>`,
  });
}

async function youtubeApiConfigPage(req) {
  const settings = await ensureYoutubeApiSettings();
  const usageRows = await db(`
    SELECT request_count,requested_video_count,requested_channel_count,updated_at
    FROM crawler.youtube_api_daily_usage
    WHERE usage_date=CURRENT_DATE
  `);
  const pendingRows = await db(`
    SELECT count(*)::int AS count
    FROM crawler.youtube_api_tasks
    WHERE status IN ('pending','queued','running','failed')
  `);
  const pendingChannelRows = await db(`
    SELECT count(*)::int AS count
    FROM crawler.youtube_channel_api_tasks
    WHERE status IN ('pending','running','failed')
  `);
  const usage = usageRows.rows[0] || {
    request_count: 0,
    requested_video_count: 0,
    requested_channel_count: 0,
    updated_at: null,
  };
  const remainingRequests = Math.max(0, settings.daily_request_limit - Number(usage.request_count || 0));
  return layout({
    title: "YouTube API 配置",
    active: "youtube-api",
    body: `
<div class="topbar">
	  <div>
	    <div class="eyebrow">YouTube Data API</div>
	    <h1>YouTube API 配置</h1>
		    <div class="sub">官方 API 仅作灾难兜底。Video 缺字段时批量调用 <span class="mono">videos.list</span>；Channel About 缺少必要指标时共享批量调用 <span class="mono">channels.list</span>。</div>
	  </div>
  <div class="toolbar"><a class="btn" href="/channels">频道列表</a></div>
</div>
${req.query.notice ? `<div class="alert alert-good">${h(req.query.notice)}</div>` : ""}
${req.query.error ? `<div class="alert alert-bad">${h(req.query.error)}</div>` : ""}
<form method="post" action="/youtube-api" class="grid">
	  <section class="panel">
	    <div class="panel-head">
	      <div><h2>API Key 列表</h2><div class="note">配置存储在 crawler.settings(setting_key = youtube_api)。页面不会回显完整 key。</div></div>
	      <span class="pill ${settings.key_count > 0 ? "good" : "warn"}">${settings.key_count > 0 ? `${settings.key_count} keys` : "not ready"}</span>
	    </div>
	    <div class="form-grid">
	      <div class="field" style="grid-column:span 2;"><label>当前 Key</label><input readonly value="${h(settings.key_count > 0 ? settings.key_tails.map((tail) => `****${tail}`).join(" / ") : "未配置，脚本不会调用 YouTube Data API")}"></div>
		      <div class="field"><label>超时 MS</label><input name="timeout_ms" type="number" min="1000" max="60000" value="${h(settings.timeout_ms)}"></div>
		      <div class="field"><label>Batch Size</label><input name="batch_size" type="number" min="1" max="50" value="${h(settings.batch_size)}"></div>
		      <div class="field"><label>每日请求上限</label><input name="daily_request_limit" type="number" min="0" max="10000" value="${h(settings.daily_request_limit)}"></div>
		      <div class="field"><label>Fallback 模式</label><select name="fallback_mode"><option value="emergency" ${settings.fallback_mode === "emergency" ? "selected" : ""}>仅灾难兜底</option><option value="disabled" ${settings.fallback_mode === "disabled" ? "selected" : ""}>完全禁用</option></select></div>
		      <div class="field"><label>今日请求</label><input readonly value="${h(`${fmtInt(usage.request_count)} / ${fmtInt(settings.daily_request_limit)}，剩余 ${fmtInt(remainingRequests)}`)}"></div>
		      <div class="field"><label>今日请求视频 ID</label><input readonly value="${h(fmtInt(usage.requested_video_count))}"></div>
		      <div class="field"><label>今日请求频道 ID</label><input readonly value="${h(fmtInt(usage.requested_channel_count))}"></div>
		      <div class="field"><label>待补全视频</label><input readonly value="${h(fmtInt(pendingRows.rows[0]?.count || 0))}"></div>
		      <div class="field"><label>待补全频道</label><input readonly value="${h(fmtInt(pendingChannelRows.rows[0]?.count || 0))}"></div>
	      <label class="check-row"><input type="checkbox" name="clear_api_keys">清空当前 Key 列表</label>
	    </div>
	    <div class="field mt">
	      <label>新增/替换 Key 列表</label>
	      <textarea name="api_keys" class="json" style="min-height:180px;" autocomplete="off" placeholder="${settings.key_count > 0 ? "留空则保留当前 key 列表；如需替换，按一行一个 key 粘贴" : "一行一个 YouTube Data API key"}"></textarea>
	    </div>
	    <div class="toolbar mt" style="justify-content:flex-start;"><button class="btn btn-primary" type="submit">保存 YouTube API 配置</button></div>
	  </section>
  <section class="panel">
    <div class="panel-head"><div><h2>当前策略</h2><div class="note">最大限度减少 API 额度消耗</div></div></div>
    <div class="grid">
		      <div>1. uploads playlist 确定最新内容，频道三个 Tab 确定类型，Detail 用一次 yt-dlp 同时获取完整字段。</div>
		      <div>2. 网络失败和缺字段先重新连接 Rota 抓取；达到最大抓取轮次后才允许进入独立 API 队列。</div>
		      <div>3. Video 请求 <span class="mono">snippet,contentDetails,statistics,status,liveStreamingDetails</span>；Channel About 请求会跨 Worker 合并，单批最多 50 个频道。</div>
		      <div>4. 每一次请求都会原子计入每日上限；达到上限后任务保留到次日继续，不会丢失。</div>
	    </div>
  </section>
</form>`,
  });
}

async function requeueMissingForChannel(channelId) {
  const channelRows = await db("SELECT latest_run_id,agent_status FROM crawler.channels WHERE channel_id=$1 LIMIT 1", [channelId]);
  const channel = channelRows.rows[0];
  if (!channel?.latest_run_id) throw new Error("频道没有可补全的 run");
  const detailRows = await db(
    `UPDATE crawler.content_candidates
     SET content_type=CASE WHEN type_status IN ('unresolved','unavailable') THEN NULL ELSE content_type END,
         type_status=CASE WHEN type_status IN ('unresolved','unavailable') THEN 'unresolved' ELSE type_status END,
         type_source=CASE WHEN type_status IN ('unresolved','unavailable') THEN NULL ELSE type_source END,
         detail_status='queued',api_status='not_needed',missing_fields='{}'::text[],
         error_message=NULL,finished_at=NULL,updated_at=now()
     WHERE channel_id=$1 AND run_id=$2
       AND (detail_status='failed' OR type_status IN ('unresolved','unavailable'))
     RETURNING candidate_id,source_content_id`,
    [channelId, channel.latest_run_id],
  );
  if (detailRows.rowCount > 0) {
    await db(
      `UPDATE crawler.youtube_api_tasks
       SET missing_fields=array_remove(missing_fields,'content_type'),
           status=CASE
             WHEN cardinality(array_remove(missing_fields,'content_type'))=0 THEN 'unavailable'
             ELSE 'pending'
           END,
           next_retry_at=NULL,updated_at=now()
       WHERE source_content_id=ANY($1::text[])
         AND missing_fields @> ARRAY['content_type']::text[]`,
      [detailRows.rows.map((row) => row.source_content_id)],
    );
  }
  const apiRows = await db(
    `UPDATE crawler.content_candidates
     SET api_status='pending',detail_status='api_pending',error_message=NULL,updated_at=now()
     WHERE channel_id=$1 AND run_id=$2
       AND api_status IN ('failed','unavailable')
       AND type_status='resolved'
       AND content_type IS NOT NULL
       AND NOT (missing_fields @> ARRAY['content_type']::text[])
     RETURNING candidate_id,source_content_id,missing_fields`,
    [channelId, channel.latest_run_id],
  );
  for (const item of apiRows.rows) {
    await db(
      `INSERT INTO crawler.youtube_api_tasks (source_content_id,status,missing_fields,candidate_ids,updated_at)
       VALUES ($1,'pending',$2::text[],ARRAY[$3]::bigint[],now())
       ON CONFLICT (source_content_id) DO UPDATE
       SET status='pending',
           candidate_ids=COALESCE((
             SELECT array_agg(DISTINCT c.candidate_id ORDER BY c.candidate_id)
             FROM crawler.content_candidates c
             WHERE c.source_content_id=EXCLUDED.source_content_id
               AND c.detail_status='api_pending'
               AND c.api_status IN ('pending','queued','running')
           ),EXCLUDED.candidate_ids),
           missing_fields=COALESCE((
             SELECT array_agg(DISTINCT field ORDER BY field)
             FROM crawler.content_candidates c
             CROSS JOIN LATERAL unnest(c.missing_fields) AS fields(field)
             WHERE c.source_content_id=EXCLUDED.source_content_id
               AND c.detail_status='api_pending'
               AND c.api_status IN ('pending','queued','running')
           ),EXCLUDED.missing_fields),
           error_message=NULL,next_retry_at=NULL,finished_at=NULL,
           created_at=now(),updated_at=now()`,
      [item.source_content_id, item.missing_fields || [], item.candidate_id],
    );
  }
  if (detailRows.rowCount > 0) {
    await queues["youtube-channel-crawl"].add(
      "channel-detail-repair",
      { channel_id: channelId, run_id: channel.latest_run_id, reason: "manual-requeue" },
      { jobId: safeJobId("channel-detail-repair", channel.latest_run_id, Date.now()) },
    );
  }
  let agentQueued = 0;
  if (channel.agent_status === "failed") {
    await db(
      `UPDATE crawler.channels
       SET agent_status='pending',agent_next_retry_at=NULL,agent_error_message=NULL,updated_at=now()
       WHERE channel_id=$1`,
      [channelId],
    );
    agentQueued = 1;
  }
  await queues["youtube-finalize"].add(
    "finalize-channel",
    { channel_id: channelId, run_id: channel.latest_run_id, reason: "manual-requeue" },
    { jobId: safeJobId("finalize", channel.latest_run_id, Date.now()) },
  );
  return { detail: detailRows.rowCount, api: apiRows.rowCount, agent: agentQueued };
}

const app = express();
app.use(morgan("combined"));
app.use(express.urlencoded({ extended: true, limit: "5mb" }));
app.use(express.json({ limit: "5mb" }));
app.use((req, res, next) => {
  const controlled = String(process.env.CONTROLLED_MIGRATION_ONLY || "").toLowerCase() === "true";
  const readMethod = req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS";
  if (!controlled || readMethod || req.path.startsWith("/migration-channels")) return next();
  return res.status(423).type("text").send(
    "Non-Migration writes are disabled during the controlled canary.",
  );
});

app.get("/", (_req, res) => res.redirect("/queries"));

app.get("/health", async (_req, res) => {
  try {
    await db("SELECT 1");
    res.json({ ok: true, db: "ok", storage: await storageHealth(), queues: await queueStats() });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/queries", async (req, res, next) => {
  try {
    res.type("html").send(queryPage(await queryDashboardData(req)));
  } catch (error) {
    next(error);
  }
});

app.get("/daily-clocks", async (req, res, next) => {
  try {
    res.type("html").send(dailyClockListPage(await dailyClockListData(req)));
  } catch (error) {
    next(error);
  }
});

app.get("/channels", async (req, res, next) => {
  try {
    res.type("html").send(channelListPage(await channelListData(req)));
  } catch (error) {
    next(error);
  }
});

app.post("/channels/publication-compare", async (req, res) => {
  try {
    const report = await publicationComparisonData(req.body?.channel_id);
    res.type("html").send(publicationComparisonPage(report));
  } catch (error) {
    redirectWith(req, res, { error: `业务库比对失败：${error?.message || String(error)}` });
  }
});

app.get("/migration-channels", async (req, res, next) => {
  try {
    const data = await migrationChannelListData(req);
    data.notice = String(req.query.notice || "");
    data.error = String(req.query.error || "");
    res.type("html").send(migrationChannelListPage(data));
  } catch (error) {
    next(error);
  }
});

app.post("/migration-channels/batch-migrate", async (req, res) => {
  try {
    const result = await migrateChannelBatch(req.body?.selection);
    const notice = result.created
      ? `已启动批量迁移：${fmtInt(result.target_count)} 个频道，后台将持续执行完整流程`
      : "当前没有可迁移的频道";
    return redirectWith(req, res, { notice });
  } catch (error) {
    return redirectWith(req, res, { error: `批量迁移启动失败：${error?.message || String(error)}` });
  }
});

app.post("/migration-channels/:channelId/migrate", async (req, res) => {
  try {
    const result = await migrateChannel(req.params.channelId, req.body?.candidate_id);
    const notice = result.created
      ? `已提交迁移：${result.channel_id}，完整流程已进入队列`
      : `该频道已在迁移中：${result.channel_id}`;
    return redirectWith(req, res, { notice });
  } catch (error) {
    return redirectWith(req, res, { error: `迁移启动失败：${error?.message || String(error)}` });
  }
});

app.get("/migration-channels/:channelId", async (req, res, next) => {
  try {
    const data = await migrationChannelDetailData(req.params.channelId);
    if (!data) {
      res.status(404).type("html").send(layout({
        title: "迁移频道不存在",
        active: "migration-channels",
        body: `<div class="alert alert-bad">迁移频道不存在：${h(req.params.channelId)}</div>`,
      }));
      return;
    }
    res.type("html").send(migrationChannelDetailPage(data));
  } catch (error) {
    next(error);
  }
});

app.get("/channels/:channelId", async (req, res, next) => {
  try {
    const data = await channelDetailData(req.params.channelId);
    if (!data) {
      res.status(404).type("html").send(layout({
        title: "频道不存在",
        active: "channels",
        body: `<div class="alert alert-bad">频道不存在：${h(req.params.channelId)}</div>`,
      }));
      return;
    }
    data.notice = req.query.notice || "";
    data.error = req.query.error || "";
    res.type("html").send(channelDetailPage(data));
  } catch (error) {
    next(error);
  }
});

app.post("/channels/:channelId/requeue-missing", async (req, res, next) => {
  try {
    const result = await requeueMissingForChannel(req.params.channelId);
    return redirectWith(req, res, { notice: `已重新投递缺失项：Detail ${result.detail}，YouTube API ${result.api}，Agent ${result.agent}` });
  } catch (error) {
    next(error);
  }
});

app.get("/exports/channels.csv", async (_req, res, next) => {
  try {
    const rows = await db(`
      SELECT
        c.channel_id,
        c.channel_url,
        COALESCE(c.handle, '') AS handle,
        COALESCE(c.title, '') AS title,
        c.subscriber_count,
        c.agent_status,
        COALESCE(fp.status, 'pending') AS final_status,
        fp.finalized_at,
        (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id)::bigint AS content_count,
        (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id AND ct.content_type = 'video')::bigint AS video_count,
        (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id AND ct.content_type = 'short')::bigint AS short_count,
        (SELECT count(*) FROM crawler.contents ct WHERE ct.channel_id = c.channel_id AND ct.content_type = 'live')::bigint AS live_count,
        c.created_at,
        c.updated_at
      FROM crawler.channels c
      LEFT JOIN crawler.finalized_profiles fp ON fp.channel_id = c.channel_id
      WHERE c.status = 'active'
      ORDER BY c.created_at DESC
    `);
    const headers = [
      "channel_id",
      "channel_url",
      "handle",
      "title",
      "subscriber_count",
      "agent_status",
      "final_status",
      "finalized_at",
      "content_count",
      "video_count",
      "short_count",
      "live_count",
      "created_at",
      "updated_at",
    ];
    const lines = [
      headers.join(","),
      ...rows.rows.map((row) => headers.map((key) => csvCell(row[key])).join(",")),
    ];
    res.setHeader("content-type", "text/csv; charset=utf-8");
    res.setHeader("content-disposition", 'attachment; filename="crawler_channels.csv"');
    res.send(`${lines.join("\n")}\n`);
  } catch (error) {
    next(error);
  }
});

app.get("/exports/finalized-profiles.jsonl", async (_req, res, next) => {
  try {
    const rows = await db(`
      SELECT
        fp.channel_id,
        fp.run_id,
        fp.status,
        fp.profile_json,
        fp.quality_json,
        fp.finalized_at,
        fp.updated_at
      FROM crawler.finalized_profiles fp
      JOIN crawler.channels c ON c.channel_id = fp.channel_id
      WHERE c.status = 'active'
      ORDER BY fp.updated_at DESC
    `);
    res.setHeader("content-type", "application/x-ndjson; charset=utf-8");
    res.setHeader("content-disposition", 'attachment; filename="crawler_finalized_profiles.jsonl"');
    for (const row of rows.rows) {
      res.write(`${JSON.stringify({
        source_database: "bullmq_crawler",
        source_schema: "crawler",
        source_table: "finalized_profiles",
        ...row,
      })}\n`);
    }
    res.end();
  } catch (error) {
    next(error);
  }
});

async function exportCrawlerContentJsonl(res) {
  const exportedAt = new Date().toISOString();
  const batchSize = 1000;
  await ensureSchema();
  res.setHeader("content-type", "application/x-ndjson; charset=utf-8");
  res.setHeader("content-disposition", `attachment; filename="crawler_content_${downloadTimestamp()}.jsonl"`);
  await writeJsonLine(res, {
    type: "export_manifest",
    source_database: "bullmq_crawler",
    source_schema: "crawler",
    export_scope: "crawler_content",
    exported_at: exportedAt,
    format: "jsonl",
    table_count: crawlerContentExportTables.length,
    tables: crawlerContentExportTables,
    excluded_tables: ["settings", "agent_configs", "agent_prompt_templates", "controller_ticks", "task_events"],
    batch_size: batchSize,
  });
  for (const tableName of crawlerContentExportTables) {
    const selectSql = crawlerContentExportSelect(tableName);
    const countRows = await pool.query(
      `SELECT count(*)::bigint AS row_count FROM (${selectSql}) export_rows`,
    );
    const rowCount = Number(countRows.rows[0]?.row_count ?? 0);
    await writeJsonLine(res, {
      type: "table_start",
      source_database: "bullmq_crawler",
      source_schema: "crawler",
      source_table: tableName,
      row_count: rowCount,
    });
    for (let offset = 0; offset < rowCount; offset += batchSize) {
      const rows = await pool.query(`${selectSql} OFFSET $1 LIMIT $2`, [offset, batchSize]);
      for (const row of rows.rows) {
        await writeJsonLine(res, {
          type: "table_row",
          source_database: "bullmq_crawler",
          source_schema: "crawler",
          source_table: tableName,
          row,
        });
      }
    }
    await writeJsonLine(res, {
      type: "table_end",
      source_database: "bullmq_crawler",
      source_schema: "crawler",
      source_table: tableName,
      row_count: rowCount,
    });
  }
  await writeJsonLine(res, {
    type: "export_end",
    source_database: "bullmq_crawler",
    source_schema: "crawler",
    export_scope: "crawler_content",
    exported_at: exportedAt,
    finished_at: new Date().toISOString(),
  });
  res.end();
}

function postgresCliArgs(command) {
  return [
    "--host", process.env.POSTGRES_HOST || "127.0.0.1",
    "--port", String(process.env.POSTGRES_PORT || "5432"),
    "--username", process.env.POSTGRES_USER || "bullmq",
    "--dbname", process.env.POSTGRES_DB || "bullmq_crawler",
    "--no-psqlrc",
    "--quiet",
    "--set", "ON_ERROR_STOP=1",
    "--command", command,
  ];
}

function pipeChildOutput(child, res, program) {
  return new Promise((resolve, reject) => {
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      if (!res.write(chunk)) {
        child.stdout.pause();
        res.once("drain", () => child.stdout.resume());
      }
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(stderr.trim() || `${program} exited with code ${code}`));
    });
  });
}

async function writeFilteredTableCopy(res, tableName) {
  const columns = await pool.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema='crawler' AND table_name=$1
     ORDER BY ordinal_position`,
    [tableName],
  );
  if (columns.rowCount === 0) throw new Error(`Crawler export table not found: ${tableName}`);
  const columnList = columns.rows.map(({ column_name: columnName }) => quoteIdent(columnName)).join(", ");
  res.write(`\nCOPY crawler.${quoteIdent(tableName)} (${columnList}) FROM stdin;\n`);
  const copySql = `COPY (${crawlerContentExportSelect(tableName)}) TO STDOUT`;
  const child = spawn("psql", postgresCliArgs(copySql), {
    env: {
      ...process.env,
      PGPASSWORD: process.env.POSTGRES_PASSWORD || "bullmq",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  await pipeChildOutput(child, res, "psql");
  res.write("\\.\n");
}

async function exportCrawlerContentSqlDump(res) {
  await ensureSchema();
  const filteredTables = crawlerContentFilteredTables();
  const args = [
    "--host", process.env.POSTGRES_HOST || "127.0.0.1",
    "--port", String(process.env.POSTGRES_PORT || "5432"),
    "--username", process.env.POSTGRES_USER || "bullmq",
    "--dbname", process.env.POSTGRES_DB || "bullmq_crawler",
    "--schema", "crawler",
    "--no-owner",
    "--no-privileges",
    "--section", "pre-data",
    "--section", "data",
    ...crawlerContentExportTables.flatMap((tableName) => ["--table", `crawler.${tableName}`]),
    ...filteredTables.flatMap((tableName) => ["--exclude-table-data", `crawler.${tableName}`]),
  ];
  res.setHeader("content-type", "application/sql; charset=utf-8");
  res.setHeader("content-disposition", `attachment; filename="crawler_content_${downloadTimestamp()}.sql"`);
  res.write([
    "-- PostgreSQL dump for BullMQ crawler content tables",
    `-- Exported at: ${new Date().toISOString()}`,
    "-- Scope: crawler content only",
    `-- Tables: ${crawlerContentExportTables.map((tableName) => `crawler.${tableName}`).join(", ")}`,
    "-- Excluded: crawler.settings, crawler.agent_configs, crawler.agent_prompt_templates, crawler.controller_ticks, crawler.task_events",
    "CREATE SCHEMA IF NOT EXISTS crawler;",
    "",
  ].join("\n"));

  const child = spawn("pg_dump", args, {
    env: {
      ...process.env,
      PGPASSWORD: process.env.POSTGRES_PASSWORD || "bullmq",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  try {
    await pipeChildOutput(child, res, "pg_dump");
    for (const tableName of filteredTables) await writeFilteredTableCopy(res, tableName);
    res.end();
  } catch (error) {
    const message = error?.message || String(error);
    res.write(`\n-- Crawler export failed: ${message.replaceAll("\n", " ")}\n`);
    res.end();
  }
}

app.get("/exports/crawler-content.sql", async (_req, res, next) => {
  try {
    await exportCrawlerContentSqlDump(res);
  } catch (error) {
    next(error);
  }
});

app.get("/exports/crawler-content.jsonl", async (_req, res, next) => {
  try {
    await exportCrawlerContentJsonl(res);
  } catch (error) {
    if (res.headersSent) {
      await writeJsonLine(res, {
        type: "export_error",
        source_database: "bullmq_crawler",
        source_schema: "crawler",
        export_scope: "crawler_content",
        error: error.message,
      });
      res.end();
      return;
    }
    next(error);
  }
});

app.get("/exports/crawler-full.jsonl", async (_req, res, next) => {
  try {
    await exportCrawlerContentJsonl(res);
  } catch (error) {
    if (res.headersSent) {
      await writeJsonLine(res, {
        type: "export_error",
        source_database: "bullmq_crawler",
        source_schema: "crawler",
        export_scope: "crawler_content",
        error: error.message,
      });
      res.end();
      return;
    }
    next(error);
  }
});

app.get("/external/queues", (_req, res) => {
  res.type("html").send(iframePage({
    title: "队列监控",
    active: "external-queues",
    src: bullmqQueuesUrl,
    refreshHref: "/external/queues",
    description: "嵌入 BullMQ 队列监控页面，查看新爬虫 Redis 队列状态。",
  }));
});

app.get("/external/minio", (_req, res) => {
  res.type("html").send(iframePage({
    title: "新 MinIO",
    active: "external-minio",
    src: minioConsoleUrl,
    refreshHref: "/external/minio",
    description: "嵌入 crawler-raw 对象存储控制台。",
  }));
});

app.get("/external/proxy", (_req, res) => {
  res.type("html").send(iframePage({
    title: "IP 代理池",
    active: "external-proxy",
    src: proxyDashboardUrl,
    refreshHref: "/external/proxy",
    description: "嵌入代理池监控页面，查看当前 IP、代理可用性和请求状态。",
  }));
});

app.post("/queries/sets", async (req, res) => {
  const name = String(req.body.name || "").trim();
  if (!name) return redirectWith(req, res, { error: "分组名称不能为空" });
  await db(`
    INSERT INTO crawler.query_sets (name, description, updated_at)
    VALUES ($1, $2, now())
    ON CONFLICT (lower(name)) DO UPDATE SET description = EXCLUDED.description, status = 'active', updated_at = now()
  `, [name, req.body.description || null]);
  return redirectWith(req, res, { notice: "分组已保存" });
});

function parseImportQueryTexts(raw) {
  const lines = String(raw || "").split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const seen = new Set();
  const queryTexts = [];
  let parsed = 0;
  for (const line of lines) {
    const first = line.split(",")[0].trim().replace(/^"|"$/g, "");
    if (!first || /^query(_text)?$/i.test(first)) continue;
    parsed += 1;
    const key = first.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    queryTexts.push(first);
  }
  return { parsed, queryTexts };
}

function chunksOf(values, size) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) chunks.push(values.slice(index, index + size));
  return chunks;
}

function queryQualityJobId(qualityBatchId, taskIds) {
  return safeJobId("query-quality", qualityBatchId, taskIds.join("-"));
}

async function enqueueQueryQualityTasks(qualityBatchId, taskIds) {
  const chunks = chunksOf(taskIds.map(Number).filter(Number.isFinite), queryQualityTaskChunkSize);
  if (chunks.length === 0) return 0;
  await queues["youtube-query-quality"].addBulk(chunks.map((ids) => ({
    name: "score-query-quality",
    data: { quality_batch_id: qualityBatchId, quality_task_ids: ids },
    opts: { jobId: queryQualityJobId(qualityBatchId, ids) },
  })));
  return chunks.length;
}

async function refreshQueryQualityImport(qualityBatchId) {
  const result = await db(
    `WITH stats AS (
       SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status IN ('scored','fallback','failed','cancelled'))::int AS processed,
              count(*) FILTER (WHERE status='scored')::int AS scored,
              count(*) FILTER (WHERE status='fallback')::int AS fallback,
              count(*) FILTER (WHERE status='failed')::int AS failed,
              count(*) FILTER (WHERE status='cancelled')::int AS cancelled,
              count(*) FILTER (WHERE status IN ('queued','running'))::int AS open
       FROM crawler.query_quality_tasks
       WHERE quality_batch_id=$1
     )
     UPDATE crawler.query_quality_batches batch
     SET total_count=stats.total,processed_count=stats.processed,scored_count=stats.scored,
         fallback_count=stats.fallback,failed_count=stats.failed,cancelled_count=stats.cancelled,
         status=CASE
           WHEN stats.open=0 AND batch.status IN ('cancel_requested','cancelled') THEN 'cancelled'
           WHEN stats.open=0 AND stats.failed>0 AND stats.scored+stats.fallback=0 THEN 'failed'
           WHEN stats.open=0 THEN 'done'
           WHEN batch.status='queued' AND stats.processed>0 THEN 'running'
           ELSE batch.status
         END,
         started_at=CASE WHEN stats.processed>0 THEN COALESCE(batch.started_at,now()) ELSE batch.started_at END,
         finished_at=CASE WHEN stats.open=0 THEN COALESCE(batch.finished_at,now()) ELSE batch.finished_at END,
         updated_at=now()
     FROM stats
     WHERE batch.quality_batch_id=$1
     RETURNING batch.*`,
    [qualityBatchId],
  );
  return result.rows[0] ?? null;
}

async function createQueryQualityImport({
  queryTexts,
  parsedCount,
  querySetId,
  language,
  country,
  category,
  priority,
  minSubscriberCount,
}) {
  if (queryTexts.length === 0) throw new Error("没有解析到可导入的 Query");
  const qualityBatchId = randomUUID();
  const options = {
    language,
    country,
    min_subscriber_count: minSubscriberCount,
    include_video_search: true,
    parsed_count: parsedCount,
    query_set_id: querySetId || null,
    category: category || null,
    priority,
  };
  await ensureSchema();
  const client = await pool.connect();
  let taskIds = [];
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_quality_batches (
         quality_batch_id,status,total_count,options_json,updated_at
       ) VALUES ($1,'queued',$2,$3::jsonb,now())`,
      [qualityBatchId, queryTexts.length, JSON.stringify(options)],
    );
    const taskRows = await client.query(
      `WITH input AS (
         SELECT query_text,ordinality::int AS input_order
         FROM jsonb_array_elements_text($1::jsonb) WITH ORDINALITY AS item(query_text,ordinality)
       ), upserted AS (
         INSERT INTO crawler.query_terms (
           query_set_id,query_text,language,country,category,status,priority,
           quality_score,quality_status,quality_json,quality_checked_at,next_crawl_at,updated_at
         )
         SELECT NULLIF($2::bigint,0),query_text,NULLIF($3,''),NULLIF($4,''),NULLIF($5,''),'active',$6,
                NULL,'unscored',jsonb_build_object('quality_batch_id',$7::text,'status','queued'),NULL,now(),now()
         FROM input
         ORDER BY input_order
         ON CONFLICT (lower(query_text),COALESCE(language,''),COALESCE(country,''),COALESCE(category,''))
         DO UPDATE SET query_set_id=COALESCE(EXCLUDED.query_set_id,crawler.query_terms.query_set_id),
                       status='active',priority=EXCLUDED.priority,
                       quality_score=NULL,quality_status='unscored',quality_json=EXCLUDED.quality_json,
                       quality_checked_at=NULL,next_crawl_at=LEAST(crawler.query_terms.next_crawl_at,EXCLUDED.next_crawl_at),
                       updated_at=now()
         RETURNING query_id
       )
       INSERT INTO crawler.query_quality_tasks (quality_batch_id,query_id,status,updated_at)
       SELECT $7,query_id,'queued',now()
       FROM upserted
       RETURNING quality_task_id`,
      [JSON.stringify(queryTexts), querySetId, language, country, category || "", priority, qualityBatchId],
    );
    taskIds = taskRows.rows.map((row) => Number(row.quality_task_id));
    if (taskIds.length !== queryTexts.length) {
      throw new Error(`评分任务数量不一致：期望 ${queryTexts.length}，实际 ${taskIds.length}`);
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  let enqueueError = null;
  try {
    await enqueueQueryQualityTasks(qualityBatchId, taskIds);
  } catch (error) {
    enqueueError = error?.message || String(error);
    await db(
      "UPDATE crawler.query_quality_batches SET error_message=$2,updated_at=now() WHERE quality_batch_id=$1",
      [qualityBatchId, enqueueError],
    );
  }
  return {
    quality_batch_id: qualityBatchId,
    parsed_count: parsedCount,
    total_count: taskIds.length,
    enqueue_error: enqueueError,
  };
}

async function startQueryImport(body = {}) {
  const rawQueries = Array.isArray(body.queries) ? body.queries.join("\n") : String(body.queries || "");
  const { parsed, queryTexts } = parseImportQueryTexts(rawQueries);
  const crawlSettings = await ensureCrawlSettings();
  return createQueryQualityImport({
    queryTexts,
    parsedCount: parsed,
    querySetId: jsonInt(body.query_set_id, 0, 0, 1_000_000_000),
    language: String(body.language || "pt-BR"),
    country: String(body.country || "BR"),
    category: String(body.category || ""),
    priority: jsonInt(body.priority, 100, 0, 10000),
    minSubscriberCount: crawlSettings.min_subscriber_count,
  });
}

app.post("/queries/terms", async (req, res) => {
  const text = String(req.body.query_text || "").trim();
  if (!text) return redirectWith(req, res, { error: "Query 词不能为空" });
  try {
    const result = await startQueryImport({ ...req.body, queries: [text] });
    return redirectWith(req, res, { notice: `Query 已保存并进入评分队列（批次 ${result.quality_batch_id}）；评分前不会进入 Discover` });
  } catch (error) {
    return redirectWith(req, res, { error: error?.message || String(error) });
  }
});

app.post("/queries/import", async (req, res) => {
  try {
    const result = await startQueryImport(req.body);
    return redirectWith(req, res, { notice: `已写入 ${result.total_count} 条 Query 并进入评分队列；任何分数都会保留，评分前不会调度` });
  } catch (error) {
    return redirectWith(req, res, { error: error?.message || String(error) });
  }
});

async function importStartHandler(req, res) {
  try {
    const result = await startQueryImport(req.body);
    res.status(202).json({ ok: true, ...result });
  } catch (error) {
    res.status(400).json({ ok: false, error: error?.message || String(error) });
  }
}

app.post("/queries/import-start", importStartHandler);
app.post("/queries/import-batch", importStartHandler);

app.get("/queries/import/:qualityBatchId", async (req, res) => {
  try {
    const batch = await refreshQueryQualityImport(String(req.params.qualityBatchId || ""));
    if (!batch) return res.status(404).json({ ok: false, error: "导入批次不存在" });
    return res.json({ ok: true, batch });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error?.message || String(error) });
  }
});

app.post("/queries/import/:qualityBatchId/cancel", async (req, res) => {
  const qualityBatchId = String(req.params.qualityBatchId || "");
  try {
    const current = await db(
      "SELECT status FROM crawler.query_quality_batches WHERE quality_batch_id=$1 LIMIT 1",
      [qualityBatchId],
    );
    if (current.rows.length === 0) return res.status(404).json({ ok: false, error: "导入批次不存在" });
    if (!["done", "failed", "cancelled"].includes(current.rows[0].status)) {
      await db(
        `UPDATE crawler.query_quality_batches
         SET status='cancel_requested',updated_at=now()
         WHERE quality_batch_id=$1`,
        [qualityBatchId],
      );
      await db(
        `UPDATE crawler.query_quality_tasks
         SET status='cancelled',finished_at=now(),updated_at=now()
         WHERE quality_batch_id=$1 AND status IN ('queued','running')
         RETURNING quality_task_id`,
        [qualityBatchId],
      );
      const allTasks = await db(
        "SELECT quality_task_id FROM crawler.query_quality_tasks WHERE quality_batch_id=$1 ORDER BY quality_task_id",
        [qualityBatchId],
      );
      await Promise.all(chunksOf(allTasks.rows.map((row) => Number(row.quality_task_id)), queryQualityTaskChunkSize).map(async (ids) => {
        const job = await queues["youtube-query-quality"].getJob(queryQualityJobId(qualityBatchId, ids));
        if (!job) return;
        try {
          await job.remove();
        } catch {
          // Active jobs observe cancel_requested before persisting their scores.
        }
      }));
    }
    const batch = await refreshQueryQualityImport(qualityBatchId);
    return res.json({ ok: true, batch });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error?.message || String(error) });
  }
});

async function deleteQueryTerms(queryIds) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const terms = await client.query(
      `SELECT term.query_id,term.query_text,
              EXISTS (
                SELECT 1 FROM crawler.query_quality_tasks task
                WHERE task.query_id=term.query_id AND task.status IN ('queued','running')
              ) AS quality_busy,
              EXISTS (
                SELECT 1 FROM crawler.query_pages page
                WHERE page.query_id=term.query_id AND page.status IN ('queued','running')
              ) AS discover_busy
       FROM crawler.query_terms term
       WHERE term.query_id=ANY($1::bigint[])
       ORDER BY term.query_id
       FOR UPDATE`,
      [queryIds],
    );
    if (terms.rows.length === 0) throw new Error("所选 Query 已不存在或已被删除");

    const busy = terms.rows.filter((term) => term.quality_busy || term.discover_busy);
    if (busy.length > 0) {
      const preview = busy.slice(0, 3).map((term) => term.query_text).join("、");
      const suffix = busy.length > 3 ? ` 等 ${busy.length} 条` : "";
      throw new Error(`运行中的 Query 不能删除：${preview}${suffix}`);
    }

    const batchRows = await client.query(
      `SELECT DISTINCT quality_batch_id
       FROM crawler.query_quality_tasks
       WHERE query_id=ANY($1::bigint[])`,
      [terms.rows.map((term) => term.query_id)],
    );
    const deleted = await client.query(
      `DELETE FROM crawler.query_terms
       WHERE query_id=ANY($1::bigint[])
       RETURNING query_id,query_text`,
      [terms.rows.map((term) => term.query_id)],
    );

    for (const row of batchRows.rows) {
      await client.query(
        `WITH stats AS (
           SELECT count(*)::int AS total,
                  count(*) FILTER (WHERE status IN ('scored','fallback','failed','cancelled'))::int AS processed,
                  count(*) FILTER (WHERE status='scored')::int AS scored,
                  count(*) FILTER (WHERE status='fallback')::int AS fallback,
                  count(*) FILTER (WHERE status='failed')::int AS failed,
                  count(*) FILTER (WHERE status='cancelled')::int AS cancelled,
                  count(*) FILTER (WHERE status IN ('queued','running'))::int AS open
           FROM crawler.query_quality_tasks
           WHERE quality_batch_id=$1
         )
         UPDATE crawler.query_quality_batches batch
         SET total_count=stats.total,processed_count=stats.processed,scored_count=stats.scored,
             fallback_count=stats.fallback,failed_count=stats.failed,cancelled_count=stats.cancelled,
             status=CASE
               WHEN stats.open=0 AND batch.status IN ('cancel_requested','cancelled') THEN 'cancelled'
               WHEN stats.open=0 AND stats.failed>0 AND stats.scored+stats.fallback=0 THEN 'failed'
               WHEN stats.open=0 THEN 'done'
               WHEN batch.status='queued' AND stats.processed>0 THEN 'running'
               ELSE batch.status
             END,
             started_at=CASE WHEN stats.processed>0 THEN COALESCE(batch.started_at,now()) ELSE batch.started_at END,
             finished_at=CASE WHEN stats.open=0 THEN COALESCE(batch.finished_at,now()) ELSE batch.finished_at END,
             updated_at=now()
         FROM stats
         WHERE batch.quality_batch_id=$1`,
        [row.quality_batch_id],
      );
    }

    await client.query("COMMIT");
    return { deleted: deleted.rows, requestedCount: queryIds.length };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

app.post("/queries/terms/delete", async (req, res) => {
  const queryIds = parsePositiveIds(req.body.query_ids);
  if (queryIds.length === 0) return redirectWith(req, res, { error: "请选择要删除的 Query" });
  try {
    const result = await deleteQueryTerms(queryIds);
    const missingCount = result.requestedCount - result.deleted.length;
    const notice = missingCount > 0
      ? `已永久删除 ${result.deleted.length} 条 Query；${missingCount} 条已不存在`
      : `已永久删除 ${result.deleted.length} 条 Query`;
    return redirectWith(req, res, { notice });
  } catch (error) {
    return redirectWith(req, res, { error: error?.message || String(error) });
  }
});

app.post("/queries/terms/:id/due", async (req, res) => {
  const id = intValue(req.params.id, 0, 1);
  await db("UPDATE crawler.query_terms SET next_crawl_at = now(), updated_at = now() WHERE query_id = $1", [id]);
  return redirectWith(req, res, { notice: "已设为 due" });
});

app.get("/queries/scheduler/work-counts", async (req, res) => {
  const querySetId = intValue(req.query.query_set_id, 0, 0, 1_000_000_000);
  const queryQualityMinScore = intValue(req.query.query_quality_min_score, 0, 0, 100);
  const counts = await querySchedulerWorkCounts(querySetId > 0 ? querySetId : null, queryQualityMinScore);
  res.json({
    ok: true,
    query_set_id: querySetId > 0 ? querySetId : null,
    query_quality_min_score: queryQualityMinScore,
    eligible: counts.eligible,
    due: counts.due,
    resumable: counts.resumable,
    scoring: counts.scoring,
    discover_backlog: await discoverQueueBacklog(),
    updated_at: timeText(new Date()),
  });
});

app.post("/queries/scheduler/draft", async (req, res) => {
  const current = await ensureQueryScheduler();
  const querySetId = intValue(req.body.query_set_id, 0, 0, 1_000_000_000);
  const next = await saveQueryScheduler({
    ...current,
    query_set_id: querySetId > 0 ? querySetId : null,
    query_quality_min_score: intValue(req.body.query_quality_min_score, current.query_quality_min_score || 0, 0, 100),
    chunk_size: intValue(req.body.chunk_size, current.chunk_size || 3, 1, 100),
    max_discover_backlog: intValue(req.body.max_discover_backlog, current.max_discover_backlog || defaultDiscoverBacklogLimit, 1, 20),
  });
  const counts = await querySchedulerWorkCounts(next.query_set_id, next.query_quality_min_score);
  res.json({
    ok: true,
    scheduler: next,
    eligible: counts.eligible,
    due: counts.due,
    resumable: counts.resumable,
    scoring: counts.scoring,
    discover_backlog: await discoverQueueBacklog(),
    updated_at: timeText(next.updated_at),
  });
});

app.post("/queries/scheduler/:action", async (req, res) => {
  const action = String(req.params.action || "").trim();
  const current = await ensureQueryScheduler();
  const now = new Date().toISOString();
  let next = current;
  let notice = "";

  if (action === "start") {
    const querySetId = intValue(req.body.query_set_id, 0, 0, 1_000_000_000);
    const normalizedQuerySetId = querySetId > 0 ? querySetId : null;
    const queryQualityMinScore = intValue(req.body.query_quality_min_score, current.query_quality_min_score || 0, 0, 100);
    const chunkSize = intValue(req.body.chunk_size, current.chunk_size || 3, 1, 100);
    const maxDiscoverBacklog = intValue(req.body.max_discover_backlog, current.max_discover_backlog || defaultDiscoverBacklogLimit, 1, 20);
    const draft = await saveQueryScheduler({
      ...current,
      query_set_id: normalizedQuerySetId,
      query_quality_min_score: queryQualityMinScore,
      chunk_size: chunkSize,
      max_discover_backlog: maxDiscoverBacklog,
    });
    const continuingCycle = Boolean(
      current.pipeline_cycle_id
      && !current.completed_at
      && current.stop_reason === "user_requested"
    );
    const pipelineCycleId = continuingCycle
      ? current.pipeline_cycle_id
      : `pipeline:${randomUUID()}`;
    const workCounts = await querySchedulerWorkCounts(normalizedQuerySetId, queryQualityMinScore);
    const discoverBacklog = await discoverQueueBacklog();
    const discoverWork = workCounts.due + workCounts.resumable + discoverBacklog;
    const downstreamWork = continuingCycle ? await downstreamPipelineWorkCount(pipelineCycleId) : 0;
    if (discoverWork + downstreamWork + workCounts.scoring <= 0) {
      return redirectWith(req, res, {
        error: `当前范围没有满足质量分 >= ${fmtInt(queryQualityMinScore)} 的 due query、可恢复页面，Discover 队列也为空，未启动调度。可以先降低 Query 质量分、点击某个 Query 的“设为 due”，或等待 next_crawl_at 到期。`,
      });
    }
    next = await saveQueryScheduler({
      ...draft,
      status: discoverWork > 0 || workCounts.scoring > 0 ? "running" : "finishing",
      query_set_id: normalizedQuerySetId,
      query_quality_min_score: queryQualityMinScore,
      chunk_size: chunkSize,
      max_discover_backlog: maxDiscoverBacklog,
      started_at: continuingCycle ? (current.started_at || now) : now,
      paused_at: null,
      stopped_at: null,
      completed_at: null,
      stop_reason: null,
      paused_from_status: null,
      pipeline_cycle_id: pipelineCycleId,
    });
    await Promise.all(Object.values(queues).map((queue) => queue.resume()));
    notice = discoverWork > 0
      ? `Query 调度已开始：质量分 >= ${fmtInt(next.query_quality_min_score)}，每次切片 ${fmtInt(next.chunk_size)} 个`
      : workCounts.scoring > 0
        ? `Query 调度已开始：正在等待 ${fmtInt(workCounts.scoring)} 条 Query 完成评分，随后自动按质量分调度`
      : "自动收尾已开始：继续处理已有 Channel、Agent、补缺和 Finalize 现场";
  } else if (action === "resume") {
    const resumeStatus = ["finishing", "repairing"].includes(current.paused_from_status)
      ? current.paused_from_status
      : "running";
    if (resumeStatus === "running") {
      const workCounts = await querySchedulerWorkCounts(current.query_set_id, current.query_quality_min_score);
      const discoverBacklog = await discoverQueueBacklog();
      if (workCounts.due + workCounts.resumable + workCounts.scoring + discoverBacklog <= 0) {
        return redirectWith(req, res, {
          error: `当前范围没有满足质量分 >= ${fmtInt(current.query_quality_min_score)} 的 due query、可恢复页面，Discover 队列也为空，未继续调度。可以先降低 Query 质量分、点击某个 Query 的“设为 due”，或等待 next_crawl_at 到期。`,
        });
      }
    }
    next = await saveQueryScheduler({
      ...current,
      status: resumeStatus,
      paused_at: null,
      stopped_at: null,
      paused_from_status: null,
    });
    await Promise.all(Object.values(queues).map((queue) => queue.resume()));
    notice = resumeStatus === "running"
      ? `Query 调度已继续：质量分 >= ${fmtInt(next.query_quality_min_score)}，每次切片 ${fmtInt(next.chunk_size)} 个`
      : "自动收尾已继续：从暂停现场恢复";
  } else if (action === "pause") {
    next = await saveQueryScheduler({
      ...current,
      status: "paused",
      paused_at: now,
      paused_from_status: ["running", "finishing", "repairing"].includes(current.status) ? current.status : "running",
    });
    await Promise.all(pipelineExecutionQueues.map((queue) => queue.pause()));
    notice = "流水线已暂停：活动 Job 完成后停止领取，队列和数据库现场已保留";
  } else if (action === "stop") {
    next = await saveQueryScheduler({
      ...current,
      status: "stopped",
      stopped_at: now,
      completed_at: null,
      stop_reason: "user_requested",
      paused_from_status: null,
    });
    if (current.pipeline_cycle_id) {
      await db(
        `UPDATE crawler.query_dispatch_batches
         SET status=CASE WHEN status='completed' THEN status ELSE 'stopped' END,
             finished_at=CASE WHEN status='completed' THEN finished_at ELSE now() END,
             updated_at=now()
         WHERE dispatch_batch_id=$1`,
        [current.pipeline_cycle_id],
      );
    }
    await Promise.all(pipelineExecutionQueues.map((queue) => queue.pause()));
    notice = "流水线已结束：现场保留，后续开始会按当前 query/page 状态继续";
  } else {
    return redirectWith(req, res, { error: `未知调度动作：${action}` });
  }

  return redirectWith(req, res, { notice });
});

app.post("/queries/seed-due", async (req, res) => {
  const limit = intValue(req.body.limit, 1, 1, 100);
  const rows = await db(`
    SELECT *
    FROM crawler.query_terms
    WHERE next_crawl_at <= now()
      AND quality_score IS NOT NULL
      AND quality_status NOT IN ('unscored', 'failed')
    ORDER BY priority DESC, next_crawl_at ASC, query_id ASC
    LIMIT $1
  `, [limit]);
  let created = 0;
  for (const row of rows.rows) {
    const pageNo = 1;
    const discoveryRunId = `query:${row.query_id}:run:${Date.now()}:${safeJobId(row.query_text).slice(0, 24)}`;
    const pageId = `${discoveryRunId}:page:${pageNo}`;
    await queues["youtube-discover-page"].add(
      "discover-page",
      {
        query_id: row.query_id,
        query_text: row.query_text,
        language: row.language,
        country: row.country,
        category: row.category,
        page_no: pageNo,
        page_id: pageId,
        discovery_run_id: discoveryRunId,
      },
      { jobId: safeJobId("discover-page", pageId), priority: row.priority },
    );
    await db(
      `INSERT INTO crawler.query_pages (
         page_id, query_id, query_text, page_no, status, priority, result_json, updated_at
       )
       VALUES ($1, $2, $3, $4, 'queued', $5, $6::jsonb, now())
       ON CONFLICT (page_id) DO NOTHING`,
      [
        pageId,
        row.query_id,
        row.query_text,
        pageNo,
        row.priority,
        JSON.stringify({ manual_seed: true, discovery_run_id: discoveryRunId }),
      ],
    );
    created += 1;
  }
  return redirectWith(req, res, { notice: `已投递 ${created} 个 due query` });
});

app.get("/agent", async (req, res, next) => {
  try {
    res.type("html").send(await agentPage(req));
  } catch (error) {
    next(error);
  }
});

app.get("/crawler", async (req, res, next) => {
  try {
    res.type("html").send(await crawlerConfigPage(req));
  } catch (error) {
    next(error);
  }
});

app.get("/youtube-api", async (req, res, next) => {
  try {
    res.type("html").send(await youtubeApiConfigPage(req));
  } catch (error) {
    next(error);
  }
});

app.post("/agent", async (req, res) => {
  let toolsJson;
  try {
    toolsJson = JSON.parse(req.body.tools_json || "[]");
  } catch {
    return redirectWith(req, res, { error: "Tools JSON 不合法" });
  }

  const mode = req.body.mode === "create" ? "create" : "update";
  const configId = intValue(req.body.config_id, 0, 1);
  const currentConfig = mode === "create" ? null : await getAgentConfigById(configId);
  if (mode !== "create" && !currentConfig) return redirectWith(req, res, { error: "Agent Config 不存在" });

  const activeConfig = await ensureAgentConfig();
  const baseConfig = currentConfig || activeConfig;
  const currentLlmSettings = mode === "create"
    ? normalizeAgentLlmSettings({}, baseConfig)
    : await ensureAgentLlmSettings({ includeSecret: true, config: currentConfig });
  const modelSelect = String(req.body.model_select || "").trim();
  const modelCustom = String(req.body.model_custom || "").trim();
  const model = modelSelect === "__custom__" ? modelCustom : (modelSelect || "rules-agent-v1");
  if (!model) return redirectWith(req, res, { error: "Model 不能为空" });

  const name = String(req.body.name || currentConfig?.name || "").trim();
  if (!name) return redirectWith(req, res, { error: "名称不能为空" });
  const batchSize = intValue(req.body.batch_size, 50, 1, 50);
  const maxWorkers = intValue(req.body.max_workers, 1, 1, 100);
  const enabled = req.body.enabled === "on";
  if (mode !== "create" && !enabled) {
    const alternatives = await db(
      "SELECT count(*)::int AS count FROM crawler.agent_configs WHERE enabled=true AND config_id<>$1",
      [configId],
    );
    if (Number(alternatives.rows[0]?.count || 0) === 0) {
      return redirectWith(req, res, { error: "至少保留一个启用的 Agent Config" });
    }
  }
  const baseUrl = String(req.body.base_url || "").trim() || currentLlmSettings.base_url || defaultAgentBaseUrl;
  const postedAgentKeys = parseApiKeys(req.body.agent_api_keys || "");
  const clearAgentKeys = req.body.clear_agent_api_keys === "on";
  const agentLlmSettings = {
    base_url: baseUrl,
    api_keys: clearAgentKeys ? [] : (postedAgentKeys.length > 0 ? postedAgentKeys : parseApiKeys(currentLlmSettings.api_keys)),
  };

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    let savedConfigId = configId;
    let promptTemplateId = currentConfig?.prompt_template_id || null;
    if (mode === "create") {
      const promptName = `Agent Prompt - ${name}`;
      const version = await client.query(
        "SELECT COALESCE(MAX(version), 0) + 1 AS next_version FROM crawler.agent_prompt_templates WHERE name = $1",
        [promptName],
      );
      const template = await client.query(
        `INSERT INTO crawler.agent_prompt_templates (name, version, template_text, output_schema_json, status, is_default, updated_at)
         VALUES ($1, $2, $3, '{}'::jsonb, 'active', false, now())
         RETURNING template_id`,
        [promptName, Number(version.rows[0]?.next_version || 1), req.body.template_text || ""],
      );
      promptTemplateId = template.rows[0].template_id;
      const inserted = await client.query(
        `INSERT INTO crawler.agent_configs (
           name, provider, model, endpoint, secret_ref, prompt_template_id,
           batch_size, min_batch_size, max_workers, timeout_ms, max_retries, tools_json,
           enabled, is_default, updated_at
         )
         VALUES ($1, $2, $3, NULLIF($4, ''), NULL, $5, $6, $6, $7, $8, $9, $10::jsonb, $11, false, now())
         RETURNING config_id`,
        [
          name,
          req.body.provider || "rules",
          model,
          baseUrl,
          promptTemplateId,
          batchSize,
          maxWorkers,
          intValue(req.body.timeout_ms, 120000, 5000, 3600000),
          intValue(req.body.max_retries, 2, 0, 10),
          JSON.stringify(toolsJson),
          enabled,
        ],
      );
      savedConfigId = inserted.rows[0].config_id;
    } else {
      if (promptTemplateId) {
        await client.query(
          `UPDATE crawler.agent_prompt_templates
           SET template_text = $1, updated_at = now()
           WHERE template_id = $2`,
          [req.body.template_text || "", promptTemplateId],
        );
      }
      await client.query(
        `UPDATE crawler.agent_configs
         SET name = $1,
             provider = $2,
             model = $3,
             endpoint = NULLIF($4, ''),
             batch_size = $5,
             min_batch_size = $5,
             max_workers = $6,
             timeout_ms = $7,
             max_retries = $8,
             tools_json = $9::jsonb,
             enabled = $10,
             updated_at = now()
         WHERE config_id = $11`,
        [
          name,
          req.body.provider || "rules",
          model,
          baseUrl,
          batchSize,
          maxWorkers,
          intValue(req.body.timeout_ms, 120000, 5000, 3600000),
          intValue(req.body.max_retries, 2, 0, 10),
          JSON.stringify(toolsJson),
          enabled,
          savedConfigId,
        ],
      );
    }

    const settingKey = agentLlmSettingKey(savedConfigId);
    await client.query(
      `INSERT INTO crawler.settings (setting_key, value_json, updated_at)
       VALUES ($1, $2::jsonb, now())
       ON CONFLICT (setting_key)
       DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = now()`,
      [settingKey, JSON.stringify(agentLlmSettings)],
    );
    await client.query(
      `UPDATE crawler.agent_configs
       SET secret_ref = NULLIF($1, ''), updated_at = now()
       WHERE config_id = $2`,
      [agentLlmSettings.api_keys.length > 0 ? `crawler.settings:${settingKey}.api_keys` : "", savedConfigId],
    );
    await client.query("COMMIT");
    return res.redirect(303, `/agent?notice=${encodeURIComponent(`Agent 配置已保存：model=${model}，batch=${batchSize}，max workers=${maxWorkers}，SK=${agentLlmSettings.api_keys.length} 个`)}`);
  } catch (error) {
    await client.query("ROLLBACK");
    return redirectWith(req, res, { error: error.message });
  } finally {
    client.release();
  }
});

app.post("/agent/configs/:id/test", async (req, res) => {
  res.set("cache-control", "no-store");
  const id = intValue(req.params.id, 0, 1);
  if (!id) return res.status(400).json({ ok: false, error: "Agent Config ID 不合法" });
  const config = await getAgentConfigById(id);
  if (!config) return res.status(404).json({ ok: false, error: "Agent Config 不存在" });

  const startedAt = Date.now();
  let result;
  try {
    const llmSettings = await ensureAgentLlmSettings({ includeSecret: true, config });
    const tested = await testAgentConnection(config, llmSettings);
    result = {
      ok: true,
      duration_ms: Date.now() - startedAt,
      endpoint: tested.endpoint,
      response_preview: tested.response_preview,
      error: null,
    };
  } catch (error) {
    result = {
      ok: false,
      duration_ms: Date.now() - startedAt,
      endpoint: error?.endpoint || agentConnectionEndpoint(config, config.endpoint),
      response_preview: null,
      error: safeAgentTestError(error),
    };
  }

  try {
    result.tested_at = await recordAgentConnectionTest(config, result);
  } catch (error) {
    return res.status(500).json({
      ok: false,
      duration_ms: result.duration_ms,
      error: `连接测试已执行，但结果保存失败：${safeAgentTestError(error)}`,
      tested_at: new Date().toISOString(),
    });
  }
  return res.status(result.ok ? 200 : 502).json(result);
});

app.post("/agent/configs/:id/toggle", async (req, res) => {
  const id = intValue(req.params.id, 0, 1);
  if (!id) return redirectWith(req, res, { error: "Agent Config ID 不合法" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("LOCK TABLE crawler.agent_configs IN SHARE ROW EXCLUSIVE MODE");
    const found = await client.query(
      "SELECT config_id, name, enabled FROM crawler.agent_configs WHERE config_id = $1 LIMIT 1",
      [id],
    );
    if (found.rows.length === 0) throw new Error("Agent Config 不存在");

    const config = found.rows[0];
    const nextEnabled = !config.enabled;
    if (!nextEnabled) {
      const enabledRows = await client.query(
        "SELECT count(*)::int AS count FROM crawler.agent_configs WHERE enabled=true AND config_id<>$1",
        [id],
      );
      if (Number(enabledRows.rows[0]?.count || 0) === 0) {
        throw new Error("至少保留一个启用的 Agent Config");
      }
    }

    await client.query(
      "UPDATE crawler.agent_configs SET enabled=$2, updated_at=now() WHERE config_id=$1",
      [id, nextEnabled],
    );
    await client.query("COMMIT");
    const notice = nextEnabled
      ? `Agent 渠道已启用：${config.name}`
      : `Agent 渠道已停用：${config.name}，后续不再派发新任务`;
    return redirectWith(req, res, { notice });
  } catch (error) {
    await client.query("ROLLBACK");
    return redirectWith(req, res, { error: error.message });
  } finally {
    client.release();
  }
});

app.post("/agent/configs/:id/delete", async (req, res) => {
  const id = intValue(req.params.id, 0, 1);
  const jobs = await queues["youtube-agent-batch"].getJobs(
    ["waiting", "active", "delayed", "prioritized", "paused", "waiting-children"],
    0,
    9999,
    true,
  );
  if (jobs.some((job) => Number(job.data?.agent_config_id) === id)) {
    return redirectWith(req, res, { error: "该配置仍有待处理 Agent 任务，暂时不能删除" });
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const countRows = await client.query("SELECT count(*)::int AS count FROM crawler.agent_configs");
    if (Number(countRows.rows[0]?.count || 0) <= 1) throw new Error("至少保留一个 Agent Config");
    const found = await client.query("SELECT config_id, enabled FROM crawler.agent_configs WHERE config_id = $1 LIMIT 1", [id]);
    if (found.rows.length === 0) throw new Error("Agent Config 不存在");
    if (found.rows[0].enabled) {
      const enabledRows = await client.query(
        "SELECT count(*)::int AS count FROM crawler.agent_configs WHERE enabled=true AND config_id<>$1",
        [id],
      );
      if (Number(enabledRows.rows[0]?.count || 0) === 0) throw new Error("至少保留一个启用的 Agent Config");
    }
    await client.query("DELETE FROM crawler.settings WHERE setting_key = $1", [agentLlmSettingKey(id)]);
    await client.query("DELETE FROM crawler.agent_configs WHERE config_id = $1", [id]);
    await client.query("COMMIT");
    return res.redirect(303, `/agent?notice=${encodeURIComponent("Agent Config 已删除")}`);
  } catch (error) {
    await client.query("ROLLBACK");
    return redirectWith(req, res, { error: error.message });
  } finally {
    client.release();
  }
});

app.post("/crawler", async (req, res) => {
  const crawlSettings = {
    channel_content_limit: intValue(req.body.channel_content_limit, defaultChannelContentLimit, 1, 100),
    content_max_age_days: intValue(req.body.content_max_age_days, defaultContentMaxAgeDays, 0, 3650),
    min_subscriber_count: intValue(req.body.min_subscriber_count, defaultMinSubscriberCount, 0, 1_000_000_000),
    detail_max_attempts: intValue(req.body.detail_max_attempts, 3, 1, 10),
    detail_concurrency: intValue(req.body.detail_concurrency, 2, 1, 4),
    published_at_required_precision: "date_only",
    discover_stop_min_qualified_ratio: numberValue(
      req.body.discover_stop_min_qualified_percent,
      defaultDiscoverStopMinQualifiedRatio * 100,
      0,
      100,
    ) / 100,
  };
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.settings (setting_key, value_json, updated_at)
       VALUES ('crawl', $1::jsonb, now())
       ON CONFLICT (setting_key)
       DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = now()`,
      [JSON.stringify(crawlSettings)],
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    return redirectWith(req, res, { error: error.message });
  } finally {
    client.release();
  }
  return redirectWith(req, res, { notice: `爬虫配置已保存：最低粉丝量 ${fmtInt(crawlSettings.min_subscriber_count)}，合格率 < ${Math.round(crawlSettings.discover_stop_min_qualified_ratio * 100)}% 停止，每频道最多 ${fmtInt(crawlSettings.channel_content_limit)} 条且不超过 ${fmtInt(crawlSettings.content_max_age_days)} 天，Detail 最多 ${fmtInt(crawlSettings.detail_max_attempts)} 轮，并发 ${fmtInt(crawlSettings.detail_concurrency)}` });
});

app.post("/youtube-api", async (req, res) => {
  const current = await ensureYoutubeApiSettings({ includeSecret: true });
  const postedKeys = parseApiKeys(req.body.api_keys || "");
  const clearKeys = req.body.clear_api_keys === "on";
  const settings = {
    api_keys: clearKeys ? [] : (postedKeys.length > 0 ? postedKeys : parseApiKeys(current.api_keys)),
    timeout_ms: intValue(req.body.timeout_ms, current.timeout_ms || 12000, 1000, 60000),
    batch_size: intValue(req.body.batch_size, current.batch_size || 50, 1, 50),
    daily_request_limit: intValue(req.body.daily_request_limit, current.daily_request_limit ?? 500, 0, 10000),
    fallback_mode: req.body.fallback_mode === "disabled" ? "disabled" : "emergency",
  };
  try {
    await db(`
      INSERT INTO crawler.settings (setting_key, value_json, updated_at)
      VALUES ('youtube_api', $1::jsonb, now())
      ON CONFLICT (setting_key)
      DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = now()
    `, [JSON.stringify(settings)]);
  } catch (error) {
    return redirectWith(req, res, { error: error.message });
  }
  const status = settings.api_keys.length > 0 ? `已配置 ${settings.api_keys.length} 个 key，脚本会自动启用` : "未配置 key，脚本会跳过 YouTube Data API";
  return redirectWith(req, res, { notice: `YouTube API 配置已保存：${status}，${settings.fallback_mode === "emergency" ? "仅灾难兜底" : "已禁用自动兜底"}，Batch Size=${settings.batch_size}，每日最多 ${settings.daily_request_limit} 次请求` });
});

app.use(basePath, serverAdapter.getRouter());

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(500).type("html").send(layout({
    title: "Error",
    active: "",
    body: `<div class="alert alert-bad">${h(error.message || String(error))}</div>`,
  }));
});

async function shutdown(signal) {
  console.log(`received ${signal}, shutting down`);
  await Promise.all(Object.values(queues).map((queue) => queue.close()));
  await Promise.all([
    pool.end(),
    migrationPool ? migrationPool.end() : Promise.resolve(),
    businessAuditPool ? businessAuditPool.end() : Promise.resolve(),
  ]);
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

app.listen(port, async () => {
  await ensureSchema();
  console.log(`crawler dashboard listening on :${port}`);
});
