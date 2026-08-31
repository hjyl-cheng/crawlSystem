import { UnrecoverableError } from "bullmq";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function optionalPositiveInteger(value, field) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

export class BusinessRunBudgetRecoveryError extends Error {
  constructor(message) {
    super(message);
    this.name = "BusinessRunBudgetRecoveryError";
    this.code = "BUSINESS_RUN_BUDGET_RECOVERY_FAILED";
  }
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

export async function recordBusinessRunBudgetExhaustion(client, job, {
  source = "rota_begin_task",
} = {}) {
  if (!client || typeof client.query !== "function") throw new TypeError("transaction client is required");
  const requestedRunId = text(job?.data?.run_id);
  const requestedBusinessRunKey = text(job?.data?.business_run_key);
  if (!requestedRunId && !requestedBusinessRunKey) {
    throw new BusinessRunBudgetRecoveryError("Business Run identity is missing from the BullMQ Job");
  }
  const bindings = await client.query(
    `SELECT business_run_key,business_run_id,status,terminal_reason,channel_id,candidate_id
     FROM crawler.business_run_bindings
     WHERE ($1::text IS NOT NULL AND business_run_key=$1)
        OR ($2::text IS NOT NULL AND business_run_id=$2)
     FOR UPDATE`,
    [requestedBusinessRunKey, requestedRunId],
  );
  if (bindings.rowCount !== 1) {
    throw new BusinessRunBudgetRecoveryError("Business Run Binding could not be resolved uniquely");
  }
  const binding = bindings.rows[0];
  if ((requestedBusinessRunKey && binding.business_run_key !== requestedBusinessRunKey)
      || (requestedRunId && binding.business_run_id !== requestedRunId)) {
    throw new BusinessRunBudgetRecoveryError("BullMQ Job conflicts with its Business Run Binding");
  }
  const terminalReason = "proxy_control_business_run_budget_exhausted";
  if (binding.status === "terminal" && binding.terminal_reason !== terminalReason) {
    throw new BusinessRunBudgetRecoveryError(
      "Business Run Binding already has a different terminal reason",
    );
  }

  const jobCandidateId = optionalPositiveInteger(job?.data?.candidate_id, "job.data.candidate_id");
  const jobDispatchGeneration = optionalPositiveInteger(
    job?.data?.dispatch_generation,
    "job.data.dispatch_generation",
  );
  const jobId = text(job?.id);
  const jobAttempt = optionalPositiveInteger(job?.attemptsStarted, "job.attemptsStarted");
  const bindingCandidateId = optionalPositiveInteger(binding.candidate_id, "binding.candidate_id");
  if (jobCandidateId && bindingCandidateId && jobCandidateId !== bindingCandidateId) {
    throw new BusinessRunBudgetRecoveryError("BullMQ Job conflicts with the Binding Candidate");
  }
  const candidateId = bindingCandidateId ?? jobCandidateId;
  const requestedChannelId = text(job?.data?.channel_id);
  if (requestedChannelId && text(binding.channel_id) !== requestedChannelId) {
    throw new BusinessRunBudgetRecoveryError("BullMQ Job conflicts with the Binding Channel");
  }
  let lockedCandidate = null;
  if (candidateId) {
    const candidate = await client.query(
      `SELECT candidate_id,status,channel_id,snapshot_dispatch_generation,
              snapshot_active_job_id,snapshot_active_job_attempt
       FROM crawler.channel_candidates
       WHERE candidate_id=$1
       FOR UPDATE`,
      [candidateId],
    );
    if (candidate.rowCount !== 1
        || (text(binding.channel_id) && candidate.rows[0].channel_id !== binding.channel_id)) {
      throw new BusinessRunBudgetRecoveryError("Binding Candidate could not be locked consistently");
    }
    lockedCandidate = candidate.rows[0];
    const candidateDispatchGeneration = Number(lockedCandidate.snapshot_dispatch_generation);
    if (jobDispatchGeneration === null) {
      throw new BusinessRunBudgetRecoveryError(
        "job.data.dispatch_generation is required for Candidate budget recovery",
      );
    }
    if (candidateDispatchGeneration !== jobDispatchGeneration) {
      throw new BusinessRunBudgetRecoveryError(
        `Candidate dispatch generation changed: expected ${jobDispatchGeneration}, got ${candidateDispatchGeneration}`,
      );
    }
    if (!jobId || jobAttempt === null) {
      throw new BusinessRunBudgetRecoveryError(
        "BullMQ Job activation identity is required for Candidate budget recovery",
      );
    }
    if (text(lockedCandidate.snapshot_active_job_id) !== jobId
        || Number(lockedCandidate.snapshot_active_job_attempt) !== jobAttempt) {
      throw new BusinessRunBudgetRecoveryError(
        "Candidate activation changed before Business Run budget recovery",
      );
    }
    if (["rejected", "existing"].includes(lockedCandidate.status)) {
      throw new BusinessRunBudgetRecoveryError(
        `Business Run Candidate is already terminal: ${lockedCandidate.status}`,
      );
    }
  }

  const executionRunId = binding.business_run_id;
  const targetRunId = text(job?.data?.checkpoint_target_run_id)
    ?? (job?.name === "channel-checkpoint-repair" ? text(job?.data?.repair_parent_run_id) : null)
    ?? executionRunId;
  const runIds = [...new Set([executionRunId, targetRunId])].sort();
  const lockedRuns = await client.query(
    `SELECT run_id,status,detail_status
     FROM crawler.channel_runs
     WHERE run_id=ANY($1::text[])
     ORDER BY run_id
     FOR UPDATE`,
    [runIds],
  );
  const runsById = new Map(lockedRuns.rows.map((row) => [row.run_id, row]));
  const executionRun = runsById.get(executionRunId) ?? null;
  const targetRun = runsById.get(targetRunId) ?? null;
  if (binding.status === "materialized" && !executionRun) {
    throw new BusinessRunBudgetRecoveryError("Materialized Business Run has no Channel Run");
  }
  if (binding.status === "reserved" && executionRun) {
    throw new BusinessRunBudgetRecoveryError("Reserved Business Run already has a Channel Run");
  }
  if (targetRunId !== executionRunId && !targetRun) {
    throw new BusinessRunBudgetRecoveryError("Checkpoint target Channel Run is missing");
  }
  for (const run of runsById.values()) {
    if (["done", "skipped"].includes(run.status)) {
      throw new BusinessRunBudgetRecoveryError(
        `Channel Run is already terminal: ${run.run_id} (${run.status})`,
      );
    }
  }
  const evidence = {
    status: "business_run_budget_exhausted",
    source: text(source) ?? "rota_begin_task",
    queue_name: text(job?.queueName),
    job_id: text(job?.id),
    job_attempt: jobAttempt,
    job_name: text(job?.name),
    repair_round: Number(job?.data?.repair_round ?? 0) || null,
    business_run_key: binding.business_run_key,
    business_run_id: binding.business_run_id,
  };
  const execution = await client.query(
    `UPDATE crawler.channel_runs
     SET status='failed',detail_status='failed',
         error_message='Rota Business Run budget exhausted',
         result_json=jsonb_set(
           COALESCE(result_json,'{}'::jsonb),
           '{proxy_control}',
           COALESCE(result_json->'proxy_control','{}'::jsonb)
             || $2::jsonb
             || jsonb_build_object(
                  'observed_at',
                  COALESCE(result_json->'proxy_control'->'observed_at',to_jsonb(now()))
                ),
           true
         ),
         finished_at=COALESCE(finished_at,now()),updated_at=now()
     WHERE run_id=$1 AND status NOT IN ('done','skipped')
     RETURNING run_id`,
    [executionRunId, JSON.stringify(evidence)],
  );
  if (executionRun && execution.rowCount !== 1) {
    throw new BusinessRunBudgetRecoveryError("Business Run Channel Run changed during termination");
  }
  if (targetRunId !== executionRunId) {
    const target = await client.query(
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
                      'observed_at',
                      COALESCE(result_json->'proxy_control'->'observed_at',to_jsonb(now()))
                    ),
               true
             ),
             '{checkpoint_repair}',
             COALESCE(result_json->'checkpoint_repair','{}'::jsonb)
               || jsonb_build_object(
                    'last_budget_exhausted_run_id',$3::text,
                    'last_budget_exhausted_at',
                    COALESCE(
                      result_json->'checkpoint_repair'->'last_budget_exhausted_at',
                      to_jsonb(now())
                    )
                  ),
             true
           ),
           finished_at=NULL,updated_at=now()
       WHERE run_id=$1 AND status NOT IN ('done','skipped')
       RETURNING run_id`,
      [targetRunId, JSON.stringify(evidence), executionRunId],
    );
    if (target.rowCount !== 1) {
      throw new BusinessRunBudgetRecoveryError(
        "Checkpoint target Channel Run changed during termination",
      );
    }
  }
  const terminalBinding = await client.query(
    `UPDATE crawler.business_run_bindings
     SET status='terminal',terminal_reason=COALESCE(terminal_reason,$3),updated_at=now()
     WHERE business_run_key=$1 AND business_run_id=$2
       AND (
         status IN ('reserved','materialized')
         OR (status='terminal' AND terminal_reason=$3)
       )
     RETURNING business_run_key,business_run_id,status,terminal_reason`,
    [binding.business_run_key, binding.business_run_id, terminalReason],
  );
  if (terminalBinding.rowCount !== 1) {
    throw new BusinessRunBudgetRecoveryError("Business Run Binding could not be terminated");
  }

  let candidateRecorded = false;
  if (candidateId) {
    const candidate = await client.query(
      `UPDATE crawler.channel_candidates
       SET status=CASE
             WHEN status IN ('accepted','rejected','existing') THEN status
             ELSE 'failed'
           END,
           error_message=CASE
             WHEN status IN ('accepted','rejected','existing') THEN error_message
             ELSE 'Rota Business Run budget exhausted'
           END,
           snapshot_json=jsonb_set(
             COALESCE(snapshot_json,'{}'::jsonb),
             '{proxy_control}',
             COALESCE(snapshot_json->'proxy_control','{}'::jsonb)
               || $2::jsonb
               || jsonb_build_object(
                    'observed_at',
                    COALESCE(snapshot_json->'proxy_control'->'observed_at',to_jsonb(now()))
                  ),
             true
           ),
           snapshot_active_job_id=NULL,
           snapshot_active_job_attempt=NULL,
           next_retry_at=NULL,
           validation_finished_at=COALESCE(validation_finished_at,now()),
           updated_at=now()
       WHERE candidate_id=$1
         AND snapshot_dispatch_generation=$3
         AND snapshot_active_job_id=$4
         AND snapshot_active_job_attempt=$5
         AND status NOT IN ('rejected','existing')
       RETURNING candidate_id,status`,
      [candidateId, JSON.stringify(evidence), jobDispatchGeneration, jobId, jobAttempt],
    );
    if (candidate.rowCount !== 1) {
      throw new BusinessRunBudgetRecoveryError("Business Run Candidate could not be terminated");
    }
    candidateRecorded = true;
  }
  let retryResolved = 0;
  if (candidateRecorded) {
    const resolvedRetry = await client.query(
      `UPDATE crawler.migration_system_retry_items retry
       SET status='resolved',
           resolution='retry_job_terminal_business_run_budget_exhausted',
           resolved_at=COALESCE(resolved_at,now()),updated_at=now()
       FROM crawler.migration_channel_intents intent
       WHERE retry.candidate_id=$1
         AND retry.status='dispatched'
         AND retry.retry_dispatch_generation=$2
         AND retry.migration_intent_id=intent.migration_intent_id
         AND intent.target_candidate_id=$1
         AND intent.dispatch_attempts=$2
       RETURNING retry.system_retry_id`,
      [candidateId, jobDispatchGeneration],
    );
    retryResolved = Number(resolvedRetry.rowCount ?? 0);
  }
  return {
    recorded: true,
    execution_run_id: executionRunId,
    target_run_id: targetRunId,
    business_run_key: binding.business_run_key,
    run_materialized: execution.rowCount === 1,
    binding_recorded: true,
    candidate_recorded: candidateRecorded,
    retry_resolved: retryResolved,
  };
}

export async function terminateExhaustedBusinessRun(withTransaction, job, error) {
  if (!isBusinessRunBudgetExhausted(error)) return false;
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  const recorded = await withTransaction((client) => (
    recordBusinessRunBudgetExhaustion(client, job)
  ));
  const terminal = new UnrecoverableError("Rota Business Run budget exhausted; terminal state recorded");
  terminal.code = "BUSINESS_RUN_BUDGET_EXHAUSTED";
  terminal.recovery = recorded;
  terminal.business_run_terminal = true;
  throw terminal;
}
