import {
  buildDormantLifecycle,
  DORMANT_REASON,
  dormantVideoActivity,
} from "./channelDormancy.js";
import { recordCrawlerObservation } from "./crawlObservationStore.js";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveInteger(value, fallback = 100000) {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

export async function loadLegacyDormantBackfillCandidates(client, {
  limit = 100000,
  channelId = null,
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const normalizedChannelId = channelId == null ? null : requiredText(channelId, "channelId");
  const result = await client.query(
    `SELECT channel.channel_id,channel.title,channel.latest_run_id,
            run.started_at AS run_started_at,
            (SELECT count(*)::int
             FROM crawler.content_candidates candidate
             WHERE candidate.channel_id=channel.channel_id) AS candidate_count
     FROM crawler.channels channel
     LEFT JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
     WHERE (
         channel.status='active'
         OR (
           channel.status='rejected'
           AND channel.reject_reason='${DORMANT_REASON}'
         )
       )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.contents content
         WHERE content.channel_id=channel.channel_id
       )
       AND ($2::text IS NULL OR channel.channel_id=$2)
     ORDER BY channel.created_at,channel.channel_id
     LIMIT $1`,
    [positiveInteger(limit), normalizedChannelId],
  );
  return result.rows;
}

export async function applyLegacyDormantBackfill(client, {
  channelId,
  observedAt = new Date(),
  crawlerVersion = String(process.env.CRAWLER_VERSION || "qy-v16"),
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const normalizedChannelId = requiredText(channelId, "channelId");
  const observed = new Date(observedAt);
  if (Number.isNaN(observed.getTime())) throw new TypeError("observedAt must be a timestamp");
  const recordedAt = observed.toISOString();
  const locked = await client.query(
    `SELECT channel.channel_id,channel.latest_run_id,
            channel.dormant_since,channel.dormant_cycle,
            run.started_at AS run_started_at,
            (SELECT count(*)::int
             FROM crawler.content_candidates candidate
             WHERE candidate.channel_id=channel.channel_id) AS candidate_count
     FROM crawler.channels channel
     LEFT JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
     WHERE channel.channel_id=$1
       AND (
         channel.status='active'
         OR (
           channel.status='rejected'
           AND channel.reject_reason='${DORMANT_REASON}'
         )
       )
       AND NOT EXISTS (
         SELECT 1
         FROM crawler.contents content
         WHERE content.channel_id=channel.channel_id
       )
     FOR UPDATE OF channel`,
    [normalizedChannelId],
  );
  const channel = locked.rows[0];
  if (!channel) {
    return {
      applied: false,
      channelId: normalizedChannelId,
      reason: "not_eligible",
    };
  }

  const dormantState = buildDormantLifecycle({
    channelId: normalizedChannelId,
    observedAt: recordedAt,
    dormantSince: channel.dormant_since,
    dormantCycle: Number(channel.dormant_cycle ?? 0),
  });
  await client.query(
    `UPDATE crawler.channels
     SET status='dormant',reject_reason=NULL,
         dormant_reason=$2,dormant_since=$3,dormant_recheck_day=$4::date,
         dormant_last_probe_at=$5,dormant_cycle=$6,
         ready_for_agent=false,
         agent_status=CASE WHEN agent_status='done' THEN 'done' ELSE 'skipped' END,
         updated_at=now()
     WHERE channel_id=$1
       AND (
         status='active'
         OR (status='rejected' AND reject_reason='${DORMANT_REASON}')
       )`,
    [
      normalizedChannelId,
      DORMANT_REASON,
      dormantState.dormant_since,
      dormantState.dormant_recheck_day,
      dormantState.dormant_last_probe_at,
      dormantState.dormant_cycle,
    ],
  );
  await client.query(
    `UPDATE crawler.agent_refresh_requests
     SET status='cancelled',last_error='channel_dormant',
         finished_at=COALESCE(finished_at,now()),updated_at=now()
     WHERE channel_id=$1 AND status IN ('pending','queued','failed')`,
    [normalizedChannelId],
  );

  const candidateCount = Number(channel.candidate_count ?? 0);
  const storedDecision = {
    required: true,
    decision: "dormant",
    reason: DORMANT_REASON,
    max_age_days: 90,
    reference_day: recordedAt.slice(0, 10),
    recent_published_content_count: 0,
    uncertain_content_count: 0,
    evidence_source: "legacy_zero_content_backfill",
    inspected_content_count: candidateCount,
    evaluated_at: recordedAt,
    dormant_since: dormantState.dormant_since,
    dormant_recheck_day: dormantState.dormant_recheck_day,
    dormant_cycle: dormantState.dormant_cycle,
  };
  const observation = await recordCrawlerObservation(client, {
    idempotencyKey: `legacy-dormant-backfill:v1:${normalizedChannelId}`,
    observationKind: "video",
    channelId: normalizedChannelId,
    runId: channel.latest_run_id,
    observedAt: recordedAt,
    planId: null,
    planDay: null,
    triggerReason: "migration_baseline",
    scheduledAt: channel.run_started_at,
    startedAt: recordedAt,
    finishedAt: recordedAt,
    crawlerVersion,
    extractorVersions: { legacy_backfill: "v1" },
    command: {
      source: "legacy_zero_content_backfill",
      run_id: channel.latest_run_id,
      candidate_count: candidateCount,
    },
    prepare: async () => ({
      outcome: "complete",
      outcomeReasonCode: "legacy_zero_content_video_dormant",
      resultSummary: {
        known_identity_count: 0,
        recent_count: 0,
        stale_ratio: 0,
        baseline: true,
        lifecycle_status: "dormant",
        evidence_candidate_count: candidateCount,
      },
      payload: {
        discovery: {
          outcome: "complete",
          payload: {
            pages: 0,
            items: 0,
            anchor_matched: false,
            stop_reason: "list_end",
            parse_gap_count: 0,
            first_seen: [],
            first_seen_count: 0,
            detail_success_count: 0,
            detail_failure_count: 0,
          },
        },
        recent_sampling: {
          outcome: "complete",
          payload: {
            recent_count: 0,
            stale_ratio: 0,
            selected_count: 0,
            success_count: 0,
            failure_count: 0,
            next_count: 0,
            comparable_view_count: 0,
            view_changed_count: 0,
            view_delta_total: 0,
            engagement_changed_count: 0,
          },
        },
        activity: dormantVideoActivity(dormantState),
      },
      anchorVideoIds: [],
      sourceCursor: {
        source: "legacy_zero_content_backfill",
        known_identity_count: 0,
        evidence_candidate_count: candidateCount,
      },
    }),
  });

  if (channel.latest_run_id) {
    await client.query(
      `UPDATE crawler.channel_runs
       SET result_json=result_json || jsonb_build_object(
             'migration_activity_gate',$2::jsonb,
             'legacy_dormant_backfill_observation_id',$3::text
           ),updated_at=now()
       WHERE run_id=$1`,
      [channel.latest_run_id, JSON.stringify(storedDecision), observation.observation_id],
    );
  }
  return {
    applied: true,
    channelId: normalizedChannelId,
    observationId: observation.observation_id,
    eventId: observation.event_id,
    dormantRecheckDay: dormantState.dormant_recheck_day,
  };
}
