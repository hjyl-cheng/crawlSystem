export const CLOCK_KINDS = Object.freeze([
  Object.freeze(["about", "About"]),
  Object.freeze(["video", "Video"]),
  Object.freeze(["agent", "Agent"]),
]);

export const CLOCK_LABELS_TEXT = CLOCK_KINDS.map(([, label]) => label).join("、");

const CLOCK_FILTERS = new Set(["all", ...CLOCK_KINDS.map(([kind]) => kind)]);

export function normalizeClockFilter(value) {
  const requested = String(value ?? "all").trim();
  return CLOCK_FILTERS.has(requested) ? requested : "all";
}

export function clockMaskTotal(stats = {}) {
  return CLOCK_KINDS.reduce(
    (total, [kind]) => total + (Number(stats[kind]) || 0),
    0,
  );
}

function nonNegativeNumber(value) {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? Math.max(0, number) : 0;
}

function timestampMs(value) {
  if (!value) return null;
  const milliseconds = new Date(value).getTime();
  return Number.isFinite(milliseconds) ? milliseconds : null;
}

export function dailyClockExecutionProgress(stats = {}, {
  day = "today",
  now = new Date(),
  minRateSamples = 3,
  minRateWindowMs = 30_000,
} = {}) {
  const total = nonNegativeNumber(stats.total);
  const unplanned = nonNegativeNumber(stats.unplanned);
  const active = nonNegativeNumber(stats.active);
  const succeeded = nonNegativeNumber(stats.succeeded);
  const recovered = nonNegativeNumber(stats.recovered);
  const partial = nonNegativeNumber(stats.partial);
  const failed = nonNegativeNumber(stats.unrecovered);
  const cancelled = nonNegativeNumber(stats.cancelled);
  const completed = succeeded + recovered;
  const finished = completed + partial + failed + cancelled;
  const remaining = unplanned + active;
  const startedAtMs = timestampMs(stats.execution_started_at);
  const completedAtMs = timestampMs(stats.execution_completed_at);
  const nowMs = timestampMs(now) ?? Date.now();
  const hasStarted = startedAtMs != null || active > 0 || finished > 0;

  let state = "pending";
  if (day !== "today") state = "pending";
  else if (total === 0) state = "idle";
  else if (!hasStarted) state = "pending";
  else if (remaining > 0) state = "running";
  else state = "completed";

  let elapsedMs = null;
  if (startedAtMs != null && state === "running") {
    elapsedMs = Math.max(0, nowMs - startedAtMs);
  } else if (startedAtMs != null && completedAtMs != null && state === "completed") {
    elapsedMs = Math.max(0, completedAtMs - startedAtMs);
  }

  const recentCompleted = nonNegativeNumber(stats.recent_completed);
  const rateWindowMs = nonNegativeNumber(stats.recent_window_seconds) * 1000;
  const rateIsStable = state === "running"
    && recentCompleted >= minRateSamples
    && rateWindowMs >= minRateWindowMs;
  const ratePerMinute = rateIsStable
    ? recentCompleted / (rateWindowMs / 60_000)
    : null;
  const etaMs = ratePerMinute > 0
    ? (remaining / ratePerMinute) * 60_000
    : null;

  return {
    state,
    total,
    remaining,
    completed,
    finished,
    partial,
    failed,
    cancelled,
    recentCompleted,
    ratePerMinute,
    etaMs,
    estimatedCompletionAt: etaMs == null
      ? null
      : new Date(nowMs + etaMs).toISOString(),
    elapsedMs,
    startedAt: startedAtMs == null ? null : new Date(startedAtMs).toISOString(),
    completedAt: completedAtMs == null ? null : new Date(completedAtMs).toISOString(),
  };
}

const PLAN_STATUS_PRESENTATION = Object.freeze({
  unplanned: Object.freeze({ label: "待生成计划", className: "muted-pill" }),
  dispatching: Object.freeze({ label: "投递中", className: "warn" }),
  dispatched: Object.freeze({ label: "已投递", className: "warn" }),
  running: Object.freeze({ label: "执行中", className: "warn" }),
  succeeded: Object.freeze({ label: "已完成", className: "good" }),
  partial: Object.freeze({ label: "部分完成", className: "warn" }),
  failed: Object.freeze({ label: "失败", className: "bad" }),
  cancelled: Object.freeze({ label: "已取消", className: "muted-pill" }),
});

export function dailyClockPlanPresentation(row = {}) {
  const originalStatus = String(row.plan_status || "unplanned");
  const dueKinds = CLOCK_KINDS
    .map(([kind]) => kind)
    .filter((kind) => Boolean(row[`run_${kind}`]));
  const outcomes = dueKinds
    .map((kind) => String(row[`crawler_${kind}_outcome`] || ""))
    .filter(Boolean);

  if (originalStatus === "failed" && row.crawler_run_status === "done") {
    if (outcomes.includes("failed")) {
      return {
        className: "bad",
        label: "失败",
        originalStatus,
        recovered: false,
      };
    }
    if (outcomes.includes("partial")) {
      return {
        className: "warn",
        label: "已恢复（部分完成）",
        originalStatus,
        recovered: true,
      };
    }
    if (dueKinds.length > 0 && dueKinds.every((kind) => (
      row[`crawler_${kind}_outcome`] === "complete"
    ))) {
      return {
        className: "good",
        label: "已恢复",
        originalStatus,
        recovered: true,
      };
    }
    return {
      className: "warn",
      label: "已恢复（原计划失败）",
      originalStatus,
      recovered: true,
    };
  }

  if (originalStatus === "planned") {
    return {
      className: "warn",
      label: row.scheduled_at ? "已分配待投递" : "等待容量",
      originalStatus,
      recovered: false,
    };
  }

  const presentation = PLAN_STATUS_PRESENTATION[originalStatus] || {
    label: originalStatus,
    className: "warn",
  };
  return {
    ...presentation,
    originalStatus,
    recovered: false,
  };
}
