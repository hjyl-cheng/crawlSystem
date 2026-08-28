import { retractPublicationChannel } from "./publicationReconciler.js";
import { normalizeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";

const TERMINAL_EVIDENCE_MAX_LENGTH = 2000;

function errorText(error) {
  const values = [
    error?.message ?? error,
    error?.cause?.message,
  ].map((value) => String(value ?? "").trim()).filter(Boolean);
  return [...new Set(values)].join(": ").slice(0, TERMINAL_EVIDENCE_MAX_LENGTH);
}

export function classifyTerminalChannelError(error) {
  const evidence = errorText(error);
  if (!evidence) return null;

  let removedReason = null;
  if (
    /(?:channel|account).{0,120}(?:removed|terminated).{0,160}community guidelines/i
      .test(evidence)
  ) {
    removedReason = "community_guidelines";
  } else if (
    /(?:channel|account).{0,120}(?:removed|terminated).{0,200}copyright infringement/i
      .test(evidence)
  ) {
    removedReason = "copyright_termination";
  } else if (/\bthis channel does not exist\b/i.test(evidence)) {
    removedReason = "channel_not_found";
  } else if (/\bthis account has been closed\b/i.test(evidence)) {
    removedReason = "owner_closed";
  } else if (/\bthis account has been terminated\b/i.test(evidence)) {
    removedReason = "account_terminated";
  }

  if (!removedReason) return null;
  return {
    failure_kind: "channel_removed",
    removed_reason: removedReason,
    removed_source: "youtube_alert",
    evidence,
  };
}

function renderedText(value) {
  if (typeof value === "string") return value.trim();
  if (typeof value?.simpleText === "string") return value.simpleText.trim();
  if (Array.isArray(value?.runs)) {
    return value.runs.map((run) => String(run?.text ?? "")).join("").trim();
  }
  return "";
}

export function terminalChannelEvidenceFromInitialData(initialData) {
  if (!initialData || typeof initialData !== "object") return null;
  const alerts = Array.isArray(initialData.alerts) ? initialData.alerts : [];
  for (const alert of alerts) {
    const renderer = alert?.alertRenderer;
    if (String(renderer?.type ?? "").toUpperCase() !== "ERROR") continue;
    const message = renderedText(renderer.text);
    const terminal = classifyTerminalChannelError(new Error(message));
    if (terminal) return terminal;
  }
  return null;
}

function timestamp(value) {
  const parsed = value == null ? new Date() : new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError("observedAt must be a valid timestamp");
  return parsed.toISOString();
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

export async function markChannelRemoved(client, {
  channelId,
  candidateId = null,
  candidateAttemptFence = null,
  runId = null,
  runStatus = "skipped",
  terminal,
  observedAt = new Date(),
} = {}) {
  if (!client || typeof client.query !== "function") throw new TypeError("client is required");
  const normalizedChannelId = requiredText(channelId, "channelId");
  if (terminal?.failure_kind !== "channel_removed") {
    throw new TypeError("terminal channel removal evidence is required");
  }
  const requestedReason = requiredText(terminal.removed_reason, "terminal.removed_reason");
  const requestedSource = requiredText(terminal.removed_source, "terminal.removed_source");
  const requestedEvidence = requiredText(terminal.evidence, "terminal.evidence")
    .slice(0, TERMINAL_EVIDENCE_MAX_LENGTH);
  const requestedAt = timestamp(observedAt);

  const channel = await client.query(
    `UPDATE crawler.channels
     SET status='removed',reject_reason=COALESCE(removed_reason,$2),ready_for_agent=false,
         dormant_reason=NULL,dormant_since=NULL,dormant_recheck_day=NULL,
         dormant_last_probe_at=NULL,dormant_cycle=0,
         agent_status=CASE WHEN agent_status='done' THEN 'done' ELSE 'skipped' END,
         agent_next_retry_at=NULL,agent_error_message=NULL,
         removed_reason=COALESCE(removed_reason,$2),
         removed_at=COALESCE(removed_at,$3::timestamptz),
         removed_source=COALESCE(removed_source,$4),
         removed_evidence=COALESCE(removed_evidence,$5),updated_at=now()
     WHERE channel_id=$1
     RETURNING channel_id,removed_reason,removed_at,removed_source,removed_evidence`,
    [normalizedChannelId, requestedReason, requestedAt, requestedSource, requestedEvidence],
  );
  const storedChannel = channel.rows[0];
  const removedReason = requiredText(
    storedChannel?.removed_reason ?? requestedReason,
    "stored removed_reason",
  );
  const removedAt = timestamp(storedChannel?.removed_at ?? requestedAt);
  const removedSource = requiredText(
    storedChannel?.removed_source ?? requestedSource,
    "stored removed_source",
  );
  const evidence = requiredText(
    storedChannel?.removed_evidence ?? requestedEvidence,
    "stored removed_evidence",
  ).slice(0, TERMINAL_EVIDENCE_MAX_LENGTH);

  let candidate = { rowCount: 0, rows: [] };
  if (candidateId != null) {
    const normalizedCandidateId = Number(candidateId);
    if (!Number.isSafeInteger(normalizedCandidateId) || normalizedCandidateId <= 0) {
      throw new TypeError("candidateId must be a positive integer");
    }
    const attemptFence = candidateAttemptFence == null
      ? null
      : normalizeChannelCandidateAttemptFence(candidateAttemptFence, {
          candidateId: normalizedCandidateId,
        });
    candidate = await client.query(
      `UPDATE crawler.channel_candidates AS candidate
       SET status=CASE WHEN promotion.is_registry_promotion
                    THEN candidate.status ELSE 'rejected' END,
           reject_reason=CASE WHEN promotion.is_registry_promotion
                           THEN candidate.reject_reason ELSE $2 END,
           error_message=CASE WHEN promotion.is_registry_promotion
                           THEN candidate.error_message ELSE $3 END,
           snapshot_json=(COALESCE(snapshot_json,'{}'::jsonb)-'parser_contract_error')
             || jsonb_build_object(
                  'terminal_channel',jsonb_build_object(
                    'removed_reason',$2::text,
                    'removed_at',$4::timestamptz,
                    'removed_source',$5::text,
                    'evidence',$3::text
                  )
                ),
           next_retry_at=NULL,validation_finished_at=COALESCE(validation_finished_at,now()),
           updated_at=now()
       FROM (
         SELECT EXISTS (
           SELECT 1 FROM crawler.channels AS channel
           WHERE channel.registry_promotion_candidate_id=$1
         ) AS is_registry_promotion
       ) AS promotion
       WHERE candidate.candidate_id=$1
         AND (
           $6::bigint IS NULL
           OR (
             candidate.snapshot_dispatch_generation=$6
             AND candidate.snapshot_active_job_id=$7
             AND candidate.snapshot_active_job_attempt=$8
           )
         )
       RETURNING candidate.candidate_id`,
      [
        normalizedCandidateId,
        removedReason,
        evidence,
        removedAt,
        removedSource,
        attemptFence?.dispatchGeneration ?? null,
        attemptFence?.jobId ?? null,
        attemptFence?.bullmqAttempt ?? null,
      ],
    );
  }

  await client.query(
    `UPDATE crawler.agent_refresh_requests
     SET status='cancelled',batch_id=NULL,queued_at=NULL,finished_at=COALESCE(finished_at,$2::timestamptz),
         next_retry_at='infinity'::timestamptz,last_error=$3,updated_at=now()
     WHERE channel_id=$1 AND status IN ('pending','queued','running','failed')`,
    [normalizedChannelId, removedAt, `Channel removed: ${removedReason}`],
  );

  await client.query(
    `UPDATE crawler.channel_runs
     SET status='skipped',detail_status='failed',
         error_message=COALESCE(error_message,$3),
         result_json=COALESCE(result_json,'{}'::jsonb)
           || jsonb_build_object(
                'terminal_channel',jsonb_build_object(
                  'removed_reason',$4::text,
                  'removed_at',$5::timestamptz,
                  'removed_source',$6::text,
                  'evidence',$3::text
                )
              ),
         finished_at=COALESCE(finished_at,$5::timestamptz),updated_at=now()
     WHERE channel_id=$1
       AND run_id IS DISTINCT FROM $2::text
       AND status IN ('queued','running','waiting_pages','waiting_detail','waiting_agent','finalizing')`,
    [normalizedChannelId, runId, evidence, removedReason, removedAt, removedSource],
  );

  if (runId != null) {
    const normalizedRunId = requiredText(runId, "runId");
    if (!new Set(["failed", "skipped"]).has(runStatus)) {
      throw new TypeError("runStatus must be failed or skipped");
    }
    const runStatusSql = runStatus === "failed" ? "status='failed'" : "status='skipped'";
    await client.query(
      `UPDATE crawler.channel_runs
       SET ${runStatusSql},detail_status='failed',error_message=$2,
           result_json=COALESCE(result_json,'{}'::jsonb)
             || jsonb_build_object(
                  'terminal_channel',jsonb_build_object(
                    'removed_reason',$3::text,
                    'removed_at',$4::timestamptz,
                    'removed_source',$5::text,
                    'evidence',$2::text
                  )
                ),
           finished_at=COALESCE(finished_at,$4::timestamptz),updated_at=now()
       WHERE run_id=$1`,
      [normalizedRunId, evidence, removedReason, removedAt, removedSource],
    );
  }

  const publication = await retractPublicationChannel(client, {
    channelId: normalizedChannelId,
    reasonCode: removedReason,
    removedAt,
    source: removedSource,
    evidence,
  });

  return {
    removed: Number(channel.rowCount || 0) > 0 || Number(candidate.rowCount || 0) > 0,
    channel_id: normalizedChannelId,
    removed_reason: removedReason,
    removed_at: removedAt,
    removed_source: removedSource,
    publication,
  };
}
