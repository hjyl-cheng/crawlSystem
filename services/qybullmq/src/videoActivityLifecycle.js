import {
  activeVideoActivity,
  buildDormantLifecycle,
  DORMANT_REASON,
  dormantVideoActivity,
  DORMANT_WINDOW_DAYS,
  evaluateVideoActivity,
} from "./channelDormancy.js";

function canonicalStatus(row) {
  if (row?.status === "rejected" && row?.reject_reason === DORMANT_REASON) return "dormant";
  return String(row?.status ?? "");
}

export async function applyVideoActivityLifecycle(client, {
  channelId,
  observedAt,
  discoveryComplete,
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
     FOR UPDATE`,
    [channelId],
  );
  const channel = channelRows.rows[0];
  if (!channel) throw new Error(`Channel not found while applying Video activity: ${channelId}`);
  const currentStatus = canonicalStatus(channel);
  if (!["active", "dormant"].includes(currentStatus)) {
    throw new Error(`Channel lifecycle does not permit Video activity: ${channel.status}`);
  }

  const referenceDay = observed.toISOString().slice(0, 10);
  const recentRows = await client.query(
    `SELECT count(DISTINCT source_content_id)::int AS recent_published_content_count
     FROM crawler.contents
     WHERE channel_id=$1
       AND (
         content_type IN ('video','short')
         OR (
           content_type='live'
           AND (live_ended_at IS NOT NULL OR duration_seconds IS NOT NULL)
         )
       )
       AND published_at IS NOT NULL
       AND (published_at AT TIME ZONE 'UTC')::date
             BETWEEN ($2::date-($3::int-1)) AND $2::date`,
    [channelId, referenceDay, DORMANT_WINDOW_DAYS],
  );
  const recent = Number(recentRows.rows[0]?.recent_published_content_count ?? 0);
  const decision = evaluateVideoActivity({
    recentPublishedContentCount: recent,
    uncertainContentCount: 0,
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
    conclusive: true,
    transitioned: currentStatus !== "dormant",
    dormant_recheck_day: dormantState.dormant_recheck_day,
  };
}

