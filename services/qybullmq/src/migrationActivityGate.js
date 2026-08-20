import { evaluateMigrationActivity } from "./migrationActivityPolicy.js";
import {
  buildDormantLifecycle,
  DORMANT_REASON,
  dormantVideoActivity,
} from "./channelDormancy.js";
import { recordCrawlerObservation } from "./crawlObservationStore.js";

function boundedInteger(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

export async function applyMigrationActivityGate(client, {
  runId,
  detailStatus,
  evaluatedAt = new Date(),
  activityEvidence = null,
}) {
  const runRows = await client.query(
    `SELECT run.*,channel.status AS channel_status,
            channel.dormant_reason,channel.dormant_since,
            channel.dormant_recheck_day,channel.dormant_last_probe_at,
            channel.dormant_cycle
     FROM crawler.channel_runs run
     JOIN crawler.channels channel ON channel.channel_id=run.channel_id
     WHERE run.run_id=$1
     FOR UPDATE OF run,channel`,
    [runId],
  );
  const run = runRows.rows[0];
  if (!run) throw new Error("channel run not found: " + runId);
  if (run.channel_status === "removed") {
    return {
      decision: "rejected",
      activate: false,
      dormant: false,
      reject: true,
      reason: "channel_removed",
      recentPublishedContentCount: 0,
      uncertainContentCount: 0,
      maxAgeDays: 90,
      candidateId: run.candidate_id == null ? null : Number(run.candidate_id),
      dispatchBatchId: optionalText(run.result_json?.dispatch_batch_id),
    };
  }
  const configured = run.result_json?.migration_activity_gate ?? {};
  const required = configured.required === true;
  const maxAgeDays = boundedInteger(configured.max_age_days, 90, 1, 3650);
  const terminalDecision = ["passed", "inconclusive", "dormant", "rejected"].includes(configured.decision);
  if (terminalDecision) {
    return {
      decision: configured.decision,
      activate: ["passed", "inconclusive"].includes(configured.decision),
      dormant: configured.decision === "dormant",
      reject: configured.decision === "rejected",
      reason: configured.reason ?? null,
      recentPublishedContentCount: Number(configured.recent_published_content_count ?? 0),
      uncertainContentCount: Number(configured.uncertain_content_count ?? 0),
      maxAgeDays,
      dormantSince: configured.dormant_since ?? null,
      dormantRecheckDay: configured.dormant_recheck_day ?? null,
      dormantCycle: Number(configured.dormant_cycle ?? 0),
      candidateId: run.candidate_id == null ? null : Number(run.candidate_id),
      dispatchBatchId: optionalText(run.result_json?.dispatch_batch_id),
    };
  }
  if (!required) {
    return {
      ...evaluateMigrationActivity({ required: false, detailStatus, maxAgeDays }),
      candidateId: run.candidate_id == null ? null : Number(run.candidate_id),
      dispatchBatchId: optionalText(run.result_json?.dispatch_batch_id),
    };
  }

  const evidenceReferenceDay = optionalText(activityEvidence?.referenceDay);
  const referenceDay = activityEvidence?.complete === true
      && /^\d{4}-\d{2}-\d{2}$/.test(evidenceReferenceDay ?? "")
    ? evidenceReferenceDay
    : new Date(run.started_at ?? evaluatedAt).toISOString().slice(0, 10);
  let evidence;
  if (activityEvidence?.complete === true) {
    evidence = {
      recent_published_content_count: boundedInteger(
        activityEvidence.recentPublishedContentCount,
        0,
        0,
        1_000_000,
      ),
      uncertain_content_count: boundedInteger(
        activityEvidence.uncertainContentCount,
        0,
        0,
        1_000_000,
      ),
    };
  } else {
    const evidenceRows = await client.query(
      `SELECT
         count(DISTINCT content.source_content_id) FILTER (
           WHERE (
                  content.content_type IN ('video','short')
                  OR (
                    content.content_type='live'
                    AND (content.live_ended_at IS NOT NULL OR content.duration_seconds IS NOT NULL)
                  )
                 )
             AND content.published_at IS NOT NULL
             AND (content.published_at AT TIME ZONE 'UTC')::date
                   BETWEEN ($2::date-($3::int-1)) AND $2::date
         )::int AS recent_published_content_count,
         count(candidate.candidate_id) FILTER (
           WHERE COALESCE(candidate.result_json#>>'{scope,status}','')<>'excluded'
             AND (
               candidate.detail_status='unavailable'
               OR candidate.content_key IS NULL
               OR content.published_at IS NULL
               OR (
                 content.content_type='live'
                 AND content.live_ended_at IS NULL
                 AND content.duration_seconds IS NULL
               )
             )
         )::int AS uncertain_content_count
       FROM crawler.content_candidates candidate
       LEFT JOIN crawler.contents content
         ON content.content_key=candidate.content_key
        AND content.channel_id=candidate.channel_id
       WHERE candidate.run_id=$1`,
      [runId, referenceDay, maxAgeDays],
    );
    evidence = evidenceRows.rows[0] ?? {};
  }
  const decision = evaluateMigrationActivity({
    required,
    detailStatus,
    recentPublishedContentCount: evidence.recent_published_content_count,
    uncertainContentCount: evidence.uncertain_content_count,
    maxAgeDays,
  });
  const recordedAt = new Date(evaluatedAt).toISOString();
  const dormantState = decision.dormant
    ? buildDormantLifecycle({
        channelId: run.channel_id,
        observedAt: recordedAt,
        dormantSince: run.channel_status === "dormant" ? run.dormant_since : null,
        dormantCycle: run.channel_status === "dormant" ? run.dormant_cycle : 0,
      })
    : null;
  const storedDecision = {
    required: true,
    decision: decision.decision,
    reason: decision.reason,
    max_age_days: decision.maxAgeDays,
    reference_day: referenceDay,
    recent_published_content_count: decision.recentPublishedContentCount,
    uncertain_content_count: decision.uncertainContentCount,
    evaluated_at: recordedAt,
    ...(activityEvidence?.complete === true
      ? {
          evidence_source: optionalText(activityEvidence.source) ?? "uploads_publication_dates",
          inspected_content_count: boundedInteger(
            activityEvidence.inspectedContentCount,
            0,
            0,
            1_000_000,
          ),
          excluded_upcoming_count: boundedInteger(
            activityEvidence.excludedUpcomingCount,
            0,
            0,
            1_000_000,
          ),
          newest_published_day: optionalText(activityEvidence.newestPublishedDay),
        }
      : {}),
    ...(dormantState
      ? {
          dormant_since: dormantState.dormant_since,
          dormant_recheck_day: dormantState.dormant_recheck_day,
          dormant_cycle: dormantState.dormant_cycle,
        }
      : {}),
  };

  if (decision.activate) {
    await client.query(
      `UPDATE crawler.channels
       SET status='active',reject_reason=NULL,
           dormant_reason=NULL,dormant_since=NULL,dormant_recheck_day=NULL,
           dormant_last_probe_at=NULL,dormant_cycle=0,
           ready_for_agent=true,
           agent_status=CASE
             WHEN agent_status IN ('done','queued','running') THEN agent_status
             ELSE 'pending'
           END,
           updated_at=now()
       WHERE channel_id=$1 AND status<>'removed'`,
      [run.channel_id],
    );
    await client.query(
      `UPDATE crawler.channel_runs
       SET result_json=result_json
             || jsonb_build_object('migration_activity_gate',$2::jsonb),
           updated_at=now()
       WHERE run_id=$1`,
      [runId, JSON.stringify(storedDecision)],
    );
  } else if (decision.dormant) {
    await client.query(
      `UPDATE crawler.channel_candidates
       SET status='accepted',reject_reason=NULL,error_message=NULL,
           accepted_at=COALESCE(accepted_at,now()),
           snapshot_json=snapshot_json || jsonb_build_object(
             'migration_activity_gate',$2::jsonb
           ),
           validation_finished_at=now(),updated_at=now()
       WHERE candidate_id=$1`,
      [run.candidate_id, JSON.stringify(storedDecision)],
    );
    await client.query(
      `UPDATE crawler.channels
       SET status='dormant',reject_reason=NULL,dormant_reason=$2,
           dormant_since=$3,dormant_recheck_day=$4::date,
           dormant_last_probe_at=$5,dormant_cycle=$6,
           ready_for_agent=false,
           agent_status=CASE WHEN agent_status='done' THEN 'done' ELSE 'skipped' END,
           updated_at=now()
       WHERE channel_id=$1 AND status<>'removed'`,
      [
        run.channel_id,
        DORMANT_REASON,
        dormantState.dormant_since,
        dormantState.dormant_recheck_day,
        dormantState.dormant_last_probe_at,
        dormantState.dormant_cycle,
      ],
    );
    const videoObservation = await recordCrawlerObservation(client, {
      idempotencyKey: `initial-full:${runId}:video`,
      observationKind: "video",
      channelId: run.channel_id,
      runId,
      observedAt: recordedAt,
      planId: null,
      planDay: null,
      triggerReason: "initial_full",
      scheduledAt: run.started_at ?? null,
      startedAt: run.started_at ?? recordedAt,
      finishedAt: recordedAt,
      crawlerVersion: String(process.env.CRAWLER_VERSION || "qy-v16"),
      extractorVersions: { full_crawl: "qy-v2" },
      command: {
        source: "migration_activity_gate",
        run_id: runId,
        recent_published_content_count: 0,
      },
      prepare: async () => ({
        outcome: "complete",
        outcomeReasonCode: "initial_full_video_dormant",
        resultSummary: {
          known_identity_count: 0,
          recent_count: 0,
          stale_ratio: 0,
          baseline: true,
          lifecycle_status: "dormant",
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
          source: "migration_activity_gate",
          known_identity_count: 0,
        },
      }),
    });
    await client.query(
      `UPDATE crawler.channel_runs
       SET status='done',detail_status='done',
           result_json=result_json || jsonb_build_object(
             'migration_activity_gate',$2::jsonb,
             'initial_video_observation_id',$3::text
           ),
           finished_at=COALESCE(finished_at,now()),updated_at=now()
       WHERE run_id=$1`,
      [runId, JSON.stringify(storedDecision), videoObservation.observation_id],
    );
  }
  return {
    ...decision,
    dormantSince: dormantState?.dormant_since ?? null,
    dormantRecheckDay: dormantState?.dormant_recheck_day ?? null,
    dormantCycle: dormantState?.dormant_cycle ?? 0,
    candidateId: run.candidate_id == null ? null : Number(run.candidate_id),
    dispatchBatchId: optionalText(run.result_json?.dispatch_batch_id),
  };
}
