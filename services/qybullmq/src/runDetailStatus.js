import { reconcileDispatchBatchCandidateState } from "./dispatchBatchCandidateState.js";
import { applyMigrationActivityGate } from "./migrationActivityGate.js";

export async function reconcileRunDetailStatus(client, runId) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const normalizedRunId = String(runId ?? "").trim();
  if (!normalizedRunId) throw new TypeError("runId is required");
  const rows = await client.query(
    `SELECT
       count(*)::int AS total,
       count(*) FILTER (WHERE detail_status IN ('done','unavailable'))::int AS terminal,
       count(*) FILTER (WHERE api_status IN ('pending','queued','running','failed'))::int AS api_open,
       count(*) FILTER (WHERE detail_status = 'failed')::int AS failed,
       count(*) FILTER (
         WHERE detail_status IN ('done','unavailable')
           AND disposition IS NULL
       )::int AS undisposed,
       count(*) FILTER (
         WHERE detail_status IN ('done','unavailable')
           AND cardinality(missing_fields)>0
       )::int AS partial,
       count(*) FILTER (WHERE result_json->'scope'->>'status' = 'excluded')::int AS excluded,
       count(*) FILTER (WHERE result_json->'scope'->>'reason' IN ('older_than_max_age','after_chronological_age_cutoff'))::int AS age_excluded,
       count(*) FILTER (WHERE result_json->'scope'->>'reason' = 'upcoming_live')::int AS upcoming_excluded,
       count(*) FILTER (WHERE result_json->'scope'->>'reason' = 'live_in_progress')::int AS live_in_progress_excluded,
       count(*) FILTER (
         WHERE result_json#>>'{detail_request,reason_code}'='initial_publication_unresolved'
       )::int AS details_requested_due_to_unresolved_count
     FROM crawler.content_candidates
     WHERE run_id=$1`,
    [normalizedRunId],
  );
  const summary = rows.rows[0] ?? {};
  const undisposed = Number(summary.undisposed ?? 0);
  const dispositionError = undisposed > 0
    ? new Error(
        `${undisposed} terminal content candidate${undisposed === 1 ? "" : "s"} has no disposition`,
      )
    : null;
  const status = Number(summary.failed) > 0 || dispositionError
    ? "failed"
    : Number(summary.api_open) > 0
      ? "api_pending"
      : Number(summary.terminal) >= Number(summary.total)
        ? "done"
        : "running";
  await client.query(
    `UPDATE crawler.channel_runs
     SET detail_status=$2,
         status=CASE WHEN $2='done' THEN 'waiting_agent' WHEN $2='failed' THEN 'waiting_detail' ELSE 'waiting_detail' END,
         expected_content_count=GREATEST($3::int-$4::int,0),
         result_json=result_json || jsonb_build_object(
           'excluded_count',$4::int,
           'age_excluded_count',$5::int,
           'upcoming_live_excluded_count',$6::int,
           'live_in_progress_excluded_count',$7::int,
           'undisposed_content_count',$8::int,
           'retained_content_count',GREATEST($3::int-$4::int,0)
         ) || jsonb_build_object(
           'migration_activity_metrics',
           COALESCE(result_json->'migration_activity_metrics','{}'::jsonb)
             || jsonb_build_object(
               'details_requested_due_to_unresolved_count',$10::int
             )
         ),
         error_message=CASE
           WHEN $8::int>0 THEN $9
           WHEN $2='failed' THEN error_message
           ELSE NULL
         END,
         updated_at=now()
     WHERE run_id=$1`,
    [
      normalizedRunId,
      status,
      Number(summary.total ?? 0),
      Number(summary.excluded ?? 0),
      Number(summary.age_excluded ?? 0),
      Number(summary.upcoming_excluded ?? 0),
      Number(summary.live_in_progress_excluded ?? 0),
      undisposed,
      dispositionError?.message ?? null,
      Number(summary.details_requested_due_to_unresolved_count ?? 0),
    ],
  );
  const migrationActivity = await applyMigrationActivityGate(client, {
    runId: normalizedRunId,
    detailStatus: status,
  });
  if (migrationActivity?.reject && migrationActivity.dispatchBatchId) {
    await reconcileDispatchBatchCandidateState(
      client.query.bind(client),
      migrationActivity.dispatchBatchId,
    );
  }
  if (dispositionError) throw dispositionError;
  return {
    ...summary,
    status: migrationActivity.reject ? "skipped" : status,
    migration_activity_gate: migrationActivity,
  };
}
