function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function integerOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function nonNegativeInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return parsed;
}

function textOrNull(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function displayValue(value) {
  return value === null || value === undefined || value === "" ? "-" : value;
}

export async function loadRotaBusinessRunBudget({
  fetchImpl = globalThis.fetch,
  controlUrl,
  controlToken,
  businessRunId,
  timeoutMs = 3000,
} = {}) {
  if (typeof fetchImpl !== "function") throw new TypeError("fetchImpl is required");
  const normalizedControlUrl = textOrNull(controlUrl)?.replace(/\/+$/, "");
  const normalizedControlToken = textOrNull(controlToken);
  const normalizedBusinessRunId = textOrNull(businessRunId);
  if (!normalizedControlUrl) throw new TypeError("controlUrl is required");
  if (!normalizedControlToken) throw new TypeError("controlToken is required");
  if (!normalizedBusinessRunId) throw new TypeError("businessRunId is required");

  const response = await fetchImpl(
    `${normalizedControlUrl}/business-runs/${encodeURIComponent(normalizedBusinessRunId)}/budget`,
    {
      headers: { authorization: `Bearer ${normalizedControlToken}` },
      signal: AbortSignal.timeout(Math.max(100, Number(timeoutMs) || 3000)),
    },
  );
  if (!response?.ok) {
    throw new Error(`Rota Business Run budget returned HTTP ${Number(response?.status) || 0}`);
  }
  const payload = await response.json();
  if (payload?.ok !== true || textOrNull(payload.business_run_id) !== normalizedBusinessRunId) {
    throw new TypeError("Rota Business Run budget response has an invalid identity");
  }
  const businessTasksUsed = nonNegativeInteger(
    payload.business_tasks_used,
    "business_tasks_used",
  );
  const businessTasksLimit = positiveInteger(
    payload.business_tasks_limit,
    "business_tasks_limit",
  );
  const executionTasksUsed = nonNegativeInteger(
    payload.execution_tasks_used,
    "execution_tasks_used",
  );
  const executionTasksLimit = positiveInteger(
    payload.execution_tasks_limit,
    "execution_tasks_limit",
  );
  if (businessTasksUsed > businessTasksLimit || executionTasksUsed > executionTasksLimit) {
    throw new TypeError("Rota Business Run budget response exceeds its persisted limit");
  }
  const budgetExhaustedAt = textOrNull(payload.budget_exhausted_at);
  if (budgetExhaustedAt && !Number.isFinite(Date.parse(budgetExhaustedAt))) {
    throw new TypeError("budget_exhausted_at must be an ISO timestamp or null");
  }
  return {
    available: true,
    workload_scope: textOrNull(payload.workload_scope),
    business_run_id: normalizedBusinessRunId,
    business_tasks_used: businessTasksUsed,
    business_tasks_limit: businessTasksLimit,
    current_execution_id: textOrNull(payload.current_execution_id),
    execution_tasks_used: executionTasksUsed,
    execution_tasks_limit: executionTasksLimit,
    budget_exhausted_at: budgetExhaustedAt,
  };
}

export async function loadMigrationRunDiagnostics({ read, candidateId } = {}) {
  if (typeof read !== "function") throw new TypeError("Target read is required");
  const normalizedCandidateId = positiveInteger(candidateId, "candidateId");
  const result = await read(
    `SELECT candidate.candidate_id::text,
            candidate.snapshot_dispatch_generation::text,
            binding.status AS binding_status,
            binding.terminal_reason,
            binding.business_run_key,
            binding.business_run_id,
            latest.attempt_number::text AS latest_rota_attempt,
            latest.job_attempt::text AS latest_bullmq_attempt_zero_based,
            latest.job_id AS latest_job_id,
            latest.status AS latest_attempt_status
     FROM crawler.channel_candidates candidate
     LEFT JOIN LATERAL (
       SELECT current_binding.status,current_binding.terminal_reason,
              current_binding.business_run_key,current_binding.business_run_id
       FROM crawler.business_run_bindings current_binding
       WHERE current_binding.candidate_id=candidate.candidate_id
       ORDER BY current_binding.created_at DESC,current_binding.business_run_key DESC
       LIMIT 1
     ) binding ON true
     LEFT JOIN LATERAL (
       SELECT attempt.attempt_number,attempt.job_attempt,attempt.job_id,attempt.status
       FROM crawler.channel_execution_attempts attempt
       WHERE attempt.business_run_id=binding.business_run_id
         AND attempt.channel_id=candidate.channel_id
       ORDER BY attempt.started_at DESC,attempt.attempt_id DESC
       LIMIT 1
     ) latest ON true
     WHERE candidate.candidate_id=$1
     LIMIT 1`,
    [normalizedCandidateId],
  );
  const row = result.rows[0];
  if (!row) return null;
  const bullmqAttemptZeroBased = integerOrNull(row.latest_bullmq_attempt_zero_based);
  return {
    candidate_id: positiveInteger(row.candidate_id, "candidate.candidate_id"),
    dispatch_generation: integerOrNull(row.snapshot_dispatch_generation),
    binding_status: textOrNull(row.binding_status),
    terminal_reason: textOrNull(row.terminal_reason),
    business_run_key: textOrNull(row.business_run_key),
    business_run_id: textOrNull(row.business_run_id),
    rota_attempt: integerOrNull(row.latest_rota_attempt),
    bullmq_attempt: bullmqAttemptZeroBased == null ? null : bullmqAttemptZeroBased + 1,
    latest_job_id: textOrNull(row.latest_job_id),
    latest_attempt_status: textOrNull(row.latest_attempt_status),
  };
}

export function renderMigrationRunDiagnostics(diagnostics) {
  if (!diagnostics) return "";
  const budget = diagnostics.budget?.available === true ? diagnostics.budget : null;
  const rows = [
    ["Business Run Binding", diagnostics.binding_status],
    ["terminal reason", diagnostics.terminal_reason],
    ["business_run_key", diagnostics.business_run_key],
    ["business_run_id", diagnostics.business_run_id],
    ["Dispatch generation", diagnostics.dispatch_generation],
    ["BullMQ attempt", diagnostics.bullmq_attempt],
    ["Rota attempt", diagnostics.rota_attempt],
    ["latest Job ID", diagnostics.latest_job_id],
    ["latest attempt status", diagnostics.latest_attempt_status],
    ["Rota budget status", budget ? "available" : "unavailable"],
    [
      "Business Run budget",
      budget ? `${budget.business_tasks_used} / ${budget.business_tasks_limit}` : null,
    ],
    ["current Execution", budget?.current_execution_id],
    [
      "Execution budget",
      budget ? `${budget.execution_tasks_used} / ${budget.execution_tasks_limit}` : null,
    ],
    ["budget_exhausted_at", budget?.budget_exhausted_at],
  ];
  return `<section class="panel mt migration-run-diagnostics">
    <div class="panel-head"><div><h2>迁移执行诊断</h2><div class="note">Crawler 执行记录与 Rota 权威预算</div></div></div>
    <div class="table-scroll"><table class="source-table"><thead><tr><th>字段</th><th>值</th></tr></thead><tbody>${rows
      .map(([field, value]) => `<tr><td class="mono">${escapeHtml(field)}</td><td class="mono">${escapeHtml(displayValue(value))}</td></tr>`)
      .join("")}</tbody></table></div>
  </section>`;
}
