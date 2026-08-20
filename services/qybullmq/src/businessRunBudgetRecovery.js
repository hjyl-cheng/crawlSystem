import { UnrecoverableError } from "bullmq";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

export function isBusinessRunBudgetExhausted(error) {
  for (let current = error; current; current = current?.cause) {
    const code = text(current?.code)?.toUpperCase();
    const reason = text(current?.reason)?.toLowerCase();
    if (code === "BUSINESS_RUN_BUDGET_EXHAUSTED"
        || code === "BUSINESS_RUN_BUDGET"
        || reason === "business_run_budget_exhausted"
        || reason === "business_run_budget") return true;
    if (current?.cause === current) break;
  }
  return false;
}

export async function recordBusinessRunBudgetExhaustion(query, job, {
  source = "rota_begin_task",
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const executionRunId = text(job?.data?.run_id);
  const targetRunId = text(job?.data?.checkpoint_target_run_id)
    ?? (job?.name === "channel-checkpoint-repair" ? text(job?.data?.repair_parent_run_id) : null)
    ?? executionRunId;
  if (!executionRunId || !targetRunId) {
    return { recorded: false, reason: "run_identity_missing" };
  }
  const evidence = {
    status: "business_run_budget_exhausted",
    source: text(source) ?? "rota_begin_task",
    queue_name: text(job?.queueName),
    job_id: text(job?.id),
    job_name: text(job?.name),
    repair_round: Number(job?.data?.repair_round ?? 0) || null,
  };
  const execution = await query(
    `UPDATE crawler.channel_runs
     SET status='failed',detail_status='failed',
         error_message='Rota Business Run budget exhausted',
         result_json=jsonb_set(
           COALESCE(result_json,'{}'::jsonb),
           '{proxy_control}',
           COALESCE(result_json->'proxy_control','{}'::jsonb)
             || $2::jsonb
             || jsonb_build_object('observed_at',now()),
           true
         ),
         finished_at=COALESCE(finished_at,now()),updated_at=now()
     WHERE run_id=$1
     RETURNING run_id`,
    [executionRunId, JSON.stringify(evidence)],
  );
  if (targetRunId !== executionRunId) {
    await query(
      `UPDATE crawler.channel_runs
       SET status='failed',detail_status='failed',
           error_message='Checkpoint repair Business Run budget exhausted',
           result_json=jsonb_set(
             jsonb_set(
               COALESCE(result_json,'{}'::jsonb),
               '{proxy_control}',
               COALESCE(result_json->'proxy_control','{}'::jsonb)
                 || $2::jsonb
                 || jsonb_build_object(
                      'exhausted_execution_run_id',$3::text,
                      'observed_at',now()
                    ),
               true
             ),
             '{checkpoint_repair}',
             COALESCE(result_json->'checkpoint_repair','{}'::jsonb)
               || jsonb_build_object(
                    'last_budget_exhausted_run_id',$3::text,
                    'last_budget_exhausted_at',now()
                  ),
             true
           ),
           finished_at=NULL,updated_at=now()
       WHERE run_id=$1
       RETURNING run_id`,
      [targetRunId, JSON.stringify(evidence), executionRunId],
    );
  }
  return {
    recorded: execution.rowCount === 1,
    execution_run_id: executionRunId,
    target_run_id: targetRunId,
  };
}

export async function terminateExhaustedBusinessRun(query, job, error) {
  if (!isBusinessRunBudgetExhausted(error)) return false;
  const recorded = await recordBusinessRunBudgetExhaustion(query, job);
  if (!recorded.recorded) {
    throw new Error("Rota Business Run budget exhausted but the Crawler Run was not recorded");
  }
  const terminal = new UnrecoverableError("Rota Business Run budget exhausted; checkpoint repair required");
  terminal.code = "BUSINESS_RUN_BUDGET_EXHAUSTED";
  terminal.recovery = recorded;
  throw terminal;
}
