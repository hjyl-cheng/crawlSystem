import {
  activeVideoActivity,
  buildDormantLifecycle,
  DORMANT_REASON,
  dormantVideoActivity,
  DORMANT_WINDOW_DAYS,
  evaluateVideoActivity,
} from "./channelDormancy.js";
import {
  classifyPublicationWindow,
  normalizePublicationEvidence,
  PUBLICATION_TIME_CLASSIFIER_VERSION,
} from "./publicationTimeEvidence.js";

export const INCREMENTAL_VIDEO_ACTIVITY_POLICY_VERSION = "incremental-video-activity-v4";

function boundedPositiveInteger(value, fallback, maximum) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, maximum);
}

export const INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_SCAN_DEFAULTS = Object.freeze({
  rowLimit: boundedPositiveInteger(
    process.env.INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_ROW_LIMIT,
    1000,
    10000,
  ),
  pageSize: boundedPositiveInteger(
    process.env.INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_PAGE_SIZE,
    200,
    1000,
  ),
  timeBudgetMs: boundedPositiveInteger(
    process.env.INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_TIME_BUDGET_MS,
    500,
    5000,
  ),
});

const EVIDENCE_PAGE_SAVEPOINT = "video_activity_evidence_page";
const EVIDENCE_CURSOR = "video_activity_evidence_cursor";

async function queryEvidenceStatement(client, sql, params, timeoutMs, originalTimeout) {
  await client.query(`SAVEPOINT ${EVIDENCE_PAGE_SAVEPOINT}`);
  try {
    await client.query(
      "SELECT set_config('statement_timeout',$1,true)",
      [`${Math.max(1, Math.ceil(timeoutMs))}ms`],
    );
    const result = await client.query(sql, params);
    await client.query(
      "SELECT set_config('statement_timeout',$1,true)",
      [originalTimeout],
    );
    await client.query(`RELEASE SAVEPOINT ${EVIDENCE_PAGE_SAVEPOINT}`);
    return { result, timedOut: false };
  } catch (error) {
    await client.query(`ROLLBACK TO SAVEPOINT ${EVIDENCE_PAGE_SAVEPOINT}`);
    await client.query(`RELEASE SAVEPOINT ${EVIDENCE_PAGE_SAVEPOINT}`);
    if (error?.code === "57014") return { result: null, timedOut: true };
    throw error;
  }
}

function canonicalStatus(row) {
  if (row?.status === "rejected" && row?.reject_reason === DORMANT_REASON) return "dormant";
  return String(row?.status ?? "");
}

export function classifyStoredVideoActivity(rows, {
  observedAt,
  maxAgeDays = DORMANT_WINDOW_DAYS,
} = {}) {
  const relationCounts = {
    inside: 0,
    outside: 0,
    after_as_of: 0,
    cutoff_overlap: 0,
    unresolved: 0,
  };
  const unresolvedByStatusCounts = {
    relative: 0,
    estimated: 0,
    unavailable: 0,
    unresolved: 0,
  };
  const seen = new Set();
  let recent = 0;
  let uncertain = 0;
  for (const row of Array.isArray(rows) ? rows : []) {
    const identity = String(row?.source_content_id ?? "").trim();
    if (identity && seen.has(identity)) continue;
    if (identity) seen.add(identity);
    const contentType = String(row?.content_type ?? "").trim();
    if (!["video", "short", "live"].includes(contentType)) continue;
    const unfinishedLive = contentType === "live"
      && row?.live_ended_at == null
      && row?.duration_seconds == null;
    if (unfinishedLive) {
      uncertain += 1;
      relationCounts.unresolved += 1;
      unresolvedByStatusCounts.unresolved += 1;
      continue;
    }
    const publication = normalizePublicationEvidence(row);
    const window = classifyPublicationWindow(publication, {
      asOf: observedAt,
      maxAgeDays,
    });
    relationCounts[window.relation] += 1;
    if (window.relation === "inside") recent += 1;
    else if (["after_as_of", "cutoff_overlap", "unresolved"].includes(window.relation)) {
      uncertain += 1;
      if (window.relation === "unresolved") {
        unresolvedByStatusCounts[publication.published_at_status] += 1;
      }
    }
  }
  return {
    recentPublishedContentCount: recent,
    uncertainContentCount: uncertain,
    relationCounts,
    unresolvedByStatusCounts,
    classifierVersion: PUBLICATION_TIME_CLASSIFIER_VERSION,
    policyVersion: INCREMENTAL_VIDEO_ACTIVITY_POLICY_VERSION,
  };
}

async function loadStoredVideoActivityEvidence(client, {
  channelId,
  rowLimit,
  pageSize,
  timeBudgetMs,
  monotonicNow,
}) {
  const effectiveRowLimit = boundedPositiveInteger(
    rowLimit,
    INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_SCAN_DEFAULTS.rowLimit,
    10000,
  );
  const effectivePageSize = Math.min(
    boundedPositiveInteger(
      pageSize,
      INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_SCAN_DEFAULTS.pageSize,
      1000,
    ),
    effectiveRowLimit,
  );
  const effectiveTimeBudgetMs = boundedPositiveInteger(
    timeBudgetMs,
    INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_SCAN_DEFAULTS.timeBudgetMs,
    5000,
  );
  const clock = typeof monotonicNow === "function" ? monotonicNow : () => performance.now();
  const startedAt = Number(clock());
  const rows = [];
  let bufferedRows = [];
  let pageCount = 0;
  let elapsedMs = 0;
  let complete = false;
  let stopReason = "complete";
  let truncatedCount = 0;
  const timeoutResult = await client.query(
    "SELECT current_setting('statement_timeout') AS statement_timeout",
  );
  const originalTimeout = String(timeoutResult.rows?.[0]?.statement_timeout ?? "0");
  let cursorOpen = false;

  try {
    const declaration = await queryEvidenceStatement(
      client,
      `DECLARE ${EVIDENCE_CURSOR} NO SCROLL CURSOR FOR
       SELECT source_content_id,content_type,live_ended_at,duration_seconds,
              published_at,published_at_status,published_at_precision,published_at_source
       FROM crawler.contents
       WHERE channel_id=$1
         AND content_type IN ('video','short','live')
       ORDER BY source_content_id`,
      [channelId],
      effectiveTimeBudgetMs,
      originalTimeout,
    );
    let observedNow = Number(clock());
    elapsedMs = Number.isFinite(startedAt) && Number.isFinite(observedNow)
      ? Math.max(0, observedNow - startedAt)
      : 0;
    if (declaration.timedOut) {
      elapsedMs = Math.max(elapsedMs, effectiveTimeBudgetMs);
      stopReason = "time_budget";
    } else {
      cursorOpen = true;
      while (rows.length < effectiveRowLimit) {
        const remainingBudgetMs = effectiveTimeBudgetMs - elapsedMs;
        if (remainingBudgetMs <= 0) {
          truncatedCount = bufferedRows.length;
          stopReason = "time_budget";
          break;
        }
        const acceptedLimit = Math.min(effectivePageSize, effectiveRowLimit - rows.length);
        const fetchCount = Math.max(1, acceptedLimit + 1 - bufferedRows.length);
        const page = await queryEvidenceStatement(
          client,
          `FETCH FORWARD ${fetchCount} FROM ${EVIDENCE_CURSOR}`,
          [],
          remainingBudgetMs,
          originalTimeout,
        );
        pageCount += 1;
        observedNow = Number(clock());
        elapsedMs = Number.isFinite(startedAt) && Number.isFinite(observedNow)
          ? Math.max(0, observedNow - startedAt)
          : 0;
        if (page.timedOut) {
          elapsedMs = Math.max(elapsedMs, effectiveTimeBudgetMs);
          truncatedCount = bufferedRows.length;
          stopReason = "time_budget";
          break;
        }
        const fetchedRows = Array.isArray(page.result?.rows) ? page.result.rows : [];
        const pageRows = [...bufferedRows, ...fetchedRows];
        const acceptedRows = pageRows.slice(0, acceptedLimit);
        bufferedRows = pageRows.slice(acceptedLimit);
        rows.push(...acceptedRows);

        if (elapsedMs >= effectiveTimeBudgetMs) {
          truncatedCount = bufferedRows.length;
          stopReason = "time_budget";
          break;
        }
        if (pageRows.length <= acceptedLimit) {
          complete = true;
          stopReason = "complete";
          break;
        }
        if (rows.length >= effectiveRowLimit) {
          truncatedCount = bufferedRows.length;
          stopReason = "row_limit";
          break;
        }
      }
    }
  } finally {
    if (cursorOpen) await client.query(`CLOSE ${EVIDENCE_CURSOR}`);
  }

  return {
    rows,
    complete,
    rowLimit: effectiveRowLimit,
    pageSize: effectivePageSize,
    timeBudgetMs: effectiveTimeBudgetMs,
    pageCount,
    elapsedMs,
    truncatedCount,
    truncatedCountIsLowerBound: !complete && truncatedCount > 0,
    stopReason,
  };
}

export async function applyVideoActivityLifecycle(client, {
  channelId,
  observedAt,
  discoveryComplete,
  runActivityEvidence = [],
  evidenceScanRowLimit = INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_SCAN_DEFAULTS.rowLimit,
  evidenceScanPageSize = INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_SCAN_DEFAULTS.pageSize,
  evidenceScanTimeBudgetMs = INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_SCAN_DEFAULTS.timeBudgetMs,
  monotonicNow = () => performance.now(),
}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const observed = new Date(observedAt);
  if (Number.isNaN(observed.getTime())) throw new TypeError("observedAt must be a timestamp");
  const channelRows = await client.query(
    `SELECT status,reject_reason,dormant_reason,dormant_since,dormant_recheck_day,
            dormant_last_probe_at,dormant_cycle
     FROM crawler.channels
     WHERE channel_id=$1
     FOR NO KEY UPDATE`,
    [channelId],
  );
  const channel = channelRows.rows[0];
  if (!channel) throw new Error(`Channel not found while applying Video activity: ${channelId}`);
  const currentStatus = canonicalStatus(channel);
  if (!["active", "dormant"].includes(currentStatus)) {
    throw new Error(`Channel lifecycle does not permit Video activity: ${channel.status}`);
  }

  const evidenceScan = await loadStoredVideoActivityEvidence(client, {
    channelId,
    rowLimit: evidenceScanRowLimit,
    pageSize: evidenceScanPageSize,
    timeBudgetMs: evidenceScanTimeBudgetMs,
    monotonicNow,
  });
  const evidence = classifyStoredVideoActivity([
    ...evidenceScan.rows,
    ...(Array.isArray(runActivityEvidence) ? runActivityEvidence : []),
  ], {
    observedAt: observed,
  });
  const recent = evidence.recentPublishedContentCount;
  const uncertain = evidence.uncertainContentCount;
  const evidenceComplete = discoveryComplete === true && evidenceScan.complete;
  const evidenceMetrics = {
    evidence_complete: evidenceComplete,
    evidence_scan_complete: evidenceScan.complete,
    evidence_scan_rows: evidenceScan.rows.length,
    evidence_scan_page_count: evidenceScan.pageCount,
    evidence_scan_elapsed_ms: evidenceScan.elapsedMs,
    evidence_scan_truncated_count: evidenceScan.truncatedCount,
    evidence_scan_truncated_count_is_lower_bound: evidenceScan.truncatedCountIsLowerBound,
    evidence_scan_stop_reason: evidenceScan.stopReason,
    evidence_scan_row_limit: evidenceScan.rowLimit,
    evidence_scan_page_size: evidenceScan.pageSize,
    evidence_scan_time_budget_ms: evidenceScan.timeBudgetMs,
  };
  const decision = evaluateVideoActivity({
    recentPublishedContentCount: recent,
    uncertainContentCount: uncertain,
    discoveryComplete: evidenceComplete,
  });

  if (decision.decision === "inconclusive") {
    if (currentStatus === "dormant") {
      await client.query(
        `UPDATE crawler.channels
         SET dormant_last_probe_at=$2,updated_at=now()
         WHERE channel_id=$1 AND status IN ('dormant','rejected')`,
        [channelId, observed.toISOString()],
      );
    }
    return {
      activity: null,
      lifecycle_status: currentStatus,
      recent_published_content_count: recent,
      uncertain_content_count: uncertain,
      classifier_version: evidence.classifierVersion,
      policy_version: evidence.policyVersion,
      relation_counts: evidence.relationCounts,
      unresolved_by_status_counts: evidence.unresolvedByStatusCounts,
      ...evidenceMetrics,
      conclusive: false,
    };
  }

  if (decision.decision === "active") {
    await client.query(
      `UPDATE crawler.channels
       SET status='active',reject_reason=NULL,
           dormant_reason=NULL,dormant_since=NULL,dormant_recheck_day=NULL,
           dormant_last_probe_at=NULL,dormant_cycle=0,updated_at=now()
       WHERE channel_id=$1 AND status<>'removed'`,
      [channelId],
    );
    return {
      activity: activeVideoActivity(recent),
      lifecycle_status: "active",
      recent_published_content_count: recent,
      uncertain_content_count: uncertain,
      classifier_version: evidence.classifierVersion,
      policy_version: evidence.policyVersion,
      relation_counts: evidence.relationCounts,
      unresolved_by_status_counts: evidence.unresolvedByStatusCounts,
      ...evidenceMetrics,
      conclusive: true,
      transitioned: currentStatus !== "active",
    };
  }

  const dormantState = buildDormantLifecycle({
    channelId,
    observedAt: observed,
    dormantSince: currentStatus === "dormant" ? channel.dormant_since : null,
    dormantCycle: currentStatus === "dormant" ? channel.dormant_cycle : 0,
  });
  await client.query(
    `UPDATE crawler.channels
     SET status='dormant',reject_reason=NULL,dormant_reason=$2,
         dormant_since=$3,dormant_recheck_day=$4::date,
         dormant_last_probe_at=$5,dormant_cycle=$6,updated_at=now()
     WHERE channel_id=$1 AND status<>'removed'`,
    [
      channelId,
      DORMANT_REASON,
      dormantState.dormant_since,
      dormantState.dormant_recheck_day,
      dormantState.dormant_last_probe_at,
      dormantState.dormant_cycle,
    ],
  );
  await client.query(
    `UPDATE crawler.agent_refresh_requests
     SET status='cancelled',last_error='channel_dormant',finished_at=now(),updated_at=now()
     WHERE channel_id=$1 AND status IN ('pending','queued','failed')`,
    [channelId],
  );
  return {
    activity: dormantVideoActivity(dormantState),
    lifecycle_status: "dormant",
    recent_published_content_count: 0,
    uncertain_content_count: 0,
    classifier_version: evidence.classifierVersion,
    policy_version: evidence.policyVersion,
    relation_counts: evidence.relationCounts,
    unresolved_by_status_counts: evidence.unresolvedByStatusCounts,
    ...evidenceMetrics,
    conclusive: true,
    transitioned: currentStatus !== "dormant",
    dormant_recheck_day: dormantState.dormant_recheck_day,
  };
}
