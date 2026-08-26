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

export const INCREMENTAL_VIDEO_ACTIVITY_POLICY_VERSION = "incremental-video-activity-v2";

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

export async function applyVideoActivityLifecycle(client, {
  channelId,
  observedAt,
  discoveryComplete,
  runActivityEvidence = [],
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

  const evidenceRows = await client.query(
    `SELECT source_content_id,content_type,live_ended_at,duration_seconds,
            published_at,published_at_status,published_at_precision,published_at_source
     FROM crawler.contents
     WHERE channel_id=$1
       AND content_type IN ('video','short','live')
     ORDER BY source_content_id`,
    [channelId],
  );
  const evidence = classifyStoredVideoActivity([
    ...(Array.isArray(runActivityEvidence) ? runActivityEvidence : []),
    ...evidenceRows.rows,
  ], {
    observedAt: observed,
  });
  const recent = evidence.recentPublishedContentCount;
  const uncertain = evidence.uncertainContentCount;
  const decision = evaluateVideoActivity({
    recentPublishedContentCount: recent,
    uncertainContentCount: uncertain,
    discoveryComplete,
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
    conclusive: true,
    transitioned: currentStatus !== "dormant",
    dormant_recheck_day: dormantState.dormant_recheck_day,
  };
}
