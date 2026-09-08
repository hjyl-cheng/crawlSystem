import { EMPTY_UPLOADS_REASON } from "./youtubeUploadsCountry.js";
import {
  evaluateMigrationActivity,
  evaluateMigrationUploadsActivity,
  MIGRATION_ACTIVITY_POLICY_VERSION,
} from "./migrationActivityPolicy.js";
import { PUBLICATION_TIME_CLASSIFIER_VERSION } from "./publicationTimeEvidence.js";
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

function storedMetricCount(run, name) {
  return boundedInteger(
    run?.result_json?.migration_activity_metrics?.[name],
    0,
    0,
    1_000_000,
  );
}

function initialActivityEvidence(run, activityEvidence) {
  if (activityEvidence?.complete === true) {
    return {
      evidence_complete: true,
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
  }
  return run?.result_json?.migration_activity_initial_evidence ?? null;
}

function candidateActivityEvidence(rows) {
  const entries = [];
  const recoveredRelationCounts = {
    inside: 0,
    outside: 0,
    after_as_of: 0,
    cutoff_overlap: 0,
    unresolved: 0,
  };
  for (const row of Array.isArray(rows) ? rows : []) {
    const scopeStatus = optionalText(row.scope_status);
    const scopeReason = optionalText(row.scope_reason);
    if (scopeStatus === "excluded") {
      if (scopeReason === "live_in_progress") {
        entries.push({
          video_id: optionalText(row.source_content_id),
          content_type: "live",
          is_live: true,
          published_at: null,
          published_at_status: "unresolved",
          published_at_precision: "unknown",
          published_at_source: null,
        });
      } else if (
        ["older_than_max_age", "after_chronological_age_cutoff"].includes(scopeReason)
        && optionalText(row.scope_relation) === "outside"
      ) {
        recoveredRelationCounts.outside += 1;
      }
      continue;
    }
    const contentType = optionalText(row.content_type);
    const completedLive = contentType === "live"
      && (row.live_ended_at != null || row.duration_seconds != null);
    const usableContent = ["video", "short"].includes(contentType) || completedLive;
    const unavailable = row.detail_status === "unavailable" || !row.content_key || !usableContent;
    entries.push({
      video_id: optionalText(row.source_content_id),
      content_type: contentType,
      is_live: contentType === "live" && !completedLive,
      published_at: unavailable ? null : row.published_at,
      published_at_status: unavailable ? "unresolved" : row.published_at_status,
      published_at_precision: unavailable ? "unknown" : row.published_at_precision,
      published_at_source: unavailable ? null : row.published_at_source,
    });
  }
  return { entries, recoveredRelationCounts };
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
  const emptyUploads = run.result_json?.full_crawl?.uploads?.document?.empty_uploads;
  const emptyDormant = emptyUploads?.outcome === "dormant";
  const required = configured.required === true || emptyDormant;
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
      referenceAt: configured.reference_at ?? null,
      classifierVersion: configured.classifier_version ?? null,
      policyVersion: configured.policy_version ?? null,
      evidenceComplete: configured.evidence_complete === true,
      detailsRequestedDueToUnresolvedCount: Number(
        configured.details_requested_due_to_unresolved_count ?? 0,
      ),
      dormantReversedAfterDetailCount: Number(
        configured.dormant_reversed_after_detail_count ?? 0,
      ),
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
  const evidenceReferenceAt = new Date(activityEvidence?.referenceAt ?? "");
  const fallbackReferenceAt = new Date(run.started_at ?? evaluatedAt);
  const referenceAt = activityEvidence?.complete === true && !Number.isNaN(evidenceReferenceAt.getTime())
    ? evidenceReferenceAt
    : fallbackReferenceAt;
  const referenceDay = activityEvidence?.complete === true
      && /^\d{4}-\d{2}-\d{2}$/.test(evidenceReferenceDay ?? "")
    ? evidenceReferenceDay
    : referenceAt.toISOString().slice(0, 10);
  const initialEvidence = initialActivityEvidence(run, activityEvidence);
  const evidenceComplete = activityEvidence?.complete === true
    || initialEvidence?.evidence_complete === true;
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
      classifier_version: optionalText(activityEvidence.classifierVersion)
        ?? PUBLICATION_TIME_CLASSIFIER_VERSION,
      policy_version: optionalText(activityEvidence.policyVersion)
        ?? MIGRATION_ACTIVITY_POLICY_VERSION,
      relation_counts: activityEvidence.relationCounts ?? null,
      unresolved_by_status_counts: activityEvidence.unresolvedByStatusCounts ?? null,
      evidence_complete: true,
    };
  } else {
    const evidenceRows = await client.query(
      `SELECT candidate.source_content_id,candidate.detail_status,candidate.content_key,
              candidate.result_json#>>'{scope,status}' AS scope_status,
              candidate.result_json#>>'{scope,reason}' AS scope_reason,
              candidate.result_json#>>'{scope,relation}' AS scope_relation,
              COALESCE(content.content_type,candidate.content_type) AS content_type,
              content.live_ended_at,content.duration_seconds,
              content.published_at,content.published_at_status,
              content.published_at_precision,content.published_at_source
       FROM crawler.content_candidates candidate
       LEFT JOIN crawler.contents content
         ON content.content_key=candidate.content_key
        AND content.channel_id=candidate.channel_id
       WHERE candidate.run_id=$1
       ORDER BY candidate.candidate_id`,
      [runId],
    );
    const candidateEvidence = candidateActivityEvidence(evidenceRows.rows);
    const classified = evaluateMigrationUploadsActivity({
      required: true,
      entries: candidateEvidence.entries,
      evidenceComplete,
      maxAgeDays,
      observedAt: referenceAt,
    });
    const relationCounts = Object.fromEntries(
      Object.keys(classified.relationCounts).map((relation) => [
        relation,
        Number(classified.relationCounts[relation] ?? 0)
          + Number(candidateEvidence.recoveredRelationCounts[relation] ?? 0),
      ]),
    );
    evidence = {
      recent_published_content_count: classified.recentPublishedContentCount,
      uncertain_content_count: classified.uncertainContentCount,
      classifier_version: classified.classifierVersion,
      policy_version: classified.policyVersion,
      relation_counts: relationCounts,
      unresolved_by_status_counts: classified.unresolvedByStatusCounts,
      evidence_complete: evidenceComplete,
    };
  }
  const decision = evaluateMigrationActivity({
    required,
    detailStatus,
    evidenceComplete: evidence.evidence_complete === true,
    recentPublishedContentCount: evidence.recent_published_content_count,
    uncertainContentCount: evidence.uncertain_content_count,
    maxAgeDays,
  });
  const recordedAt = new Date(evaluatedAt).toISOString();
  const detailsRequestedDueToUnresolvedCount = storedMetricCount(
    run,
    "details_requested_due_to_unresolved_count",
  );
  const dormantReversedAfterDetailCount = decision.decision === "passed"
      && detailsRequestedDueToUnresolvedCount > 0
      && initialEvidence?.evidence_complete === true
      && boundedInteger(initialEvidence.recent_published_content_count, 0, 0, 1_000_000) === 0
      && boundedInteger(initialEvidence.uncertain_content_count, 0, 0, 1_000_000) > 0
    ? 1
    : 0;
  const dormantState = decision.dormant
    ? buildDormantLifecycle({
        channelId: run.channel_id,
        reason: emptyDormant ? EMPTY_UPLOADS_REASON : DORMANT_REASON,
        observedAt: recordedAt,
        dormantSince: run.channel_status === "dormant" ? run.dormant_since : null,
        dormantCycle: run.channel_status === "dormant" ? run.dormant_cycle : 0,
      })
    : null;
  const storedDecision = {
    required: true,
    decision: decision.decision,
    reason: emptyDormant ? EMPTY_UPLOADS_REASON : decision.reason,
    ...(emptyDormant ? { empty_uploads: emptyUploads } : {}),
    max_age_days: decision.maxAgeDays,
    reference_day: referenceDay,
    reference_at: referenceAt.toISOString(),
    recent_published_content_count: decision.recentPublishedContentCount,
    uncertain_content_count: decision.uncertainContentCount,
    evidence_complete: evidence.evidence_complete === true,
    classifier_version: evidence.classifier_version ?? PUBLICATION_TIME_CLASSIFIER_VERSION,
    policy_version: evidence.policy_version ?? MIGRATION_ACTIVITY_POLICY_VERSION,
    relation_counts: evidence.relation_counts,
    unresolved_by_status_counts: evidence.unresolved_by_status_counts,
    details_requested_due_to_unresolved_count: detailsRequestedDueToUnresolvedCount,
    dormant_reversed_after_detail_count: dormantReversedAfterDetailCount,
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
             || jsonb_build_object('migration_activity_gate',$2::jsonb)
             || jsonb_build_object(
                  'migration_activity_metrics',
                  COALESCE(result_json->'migration_activity_metrics','{}'::jsonb)
                    || jsonb_build_object('dormant_reversed_after_detail_count',$3::int)
                ),
           updated_at=now()
       WHERE run_id=$1`,
      [runId, JSON.stringify(storedDecision), dormantReversedAfterDetailCount],
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
        emptyDormant ? EMPTY_UPLOADS_REASON : DORMANT_REASON,
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
           ) || jsonb_build_object(
             'migration_activity_metrics',
             COALESCE(result_json->'migration_activity_metrics','{}'::jsonb)
               || jsonb_build_object('dormant_reversed_after_detail_count',$4::int)
           ),
           finished_at=COALESCE(finished_at,now()),updated_at=now()
       WHERE run_id=$1`,
      [
        runId,
        JSON.stringify(storedDecision),
        videoObservation.observation_id,
        dormantReversedAfterDetailCount,
      ],
    );
  }
  return {
    ...decision,
    reason: emptyDormant ? EMPTY_UPLOADS_REASON : decision.reason,
    dormantSince: dormantState?.dormant_since ?? null,
    dormantRecheckDay: dormantState?.dormant_recheck_day ?? null,
    dormantCycle: dormantState?.dormant_cycle ?? 0,
    detailsRequestedDueToUnresolvedCount,
    dormantReversedAfterDetailCount,
    candidateId: run.candidate_id == null ? null : Number(run.candidate_id),
    dispatchBatchId: optionalText(run.result_json?.dispatch_batch_id),
  };
}
