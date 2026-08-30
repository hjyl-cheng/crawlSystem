function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function responsePayload(response) {
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload.ok === false) {
    const error = new Error(payload.error || `Crawler API returned ${response.status}`);
    error.code = payload.code || "crawler_api_error";
    throw error;
  }
  return payload;
}

export async function loadMigrationSystemRetries({
  read,
  limit = 100,
} = {}) {
  if (typeof read !== "function") throw new TypeError("Crawler read is required");
  const normalizedLimit = Math.max(1, Math.min(500, Number(limit) || 100));
  const result = await read(
    `SELECT retry.system_retry_id,retry.migration_intent_id,retry.candidate_id,
            retry.failure_code,retry.failure_category,retry.failure_evidence,
            retry.failed_dispatch_batch_id,retry.failed_dispatch_generation,
            retry.failed_job_id,retry.failed_job_attempt,
            retry.status,retry.retry_dispatch_generation,retry.requested_at,
            retry.dispatched_at,retry.resolved_at,retry.resolution,
            intent.channel_id,candidate.channel_url,candidate.status AS candidate_status,
            candidate.snapshot_dispatch_generation,
            candidate.snapshot_active_job_id,candidate.snapshot_active_job_attempt,
            candidate.dispatch_batch_id,candidate.pipeline_cycle_id
     FROM crawler.migration_system_retry_items retry
     JOIN crawler.migration_channel_intents intent
       ON intent.migration_intent_id=retry.migration_intent_id
     JOIN crawler.channel_candidates candidate
       ON candidate.candidate_id=retry.candidate_id
     WHERE retry.status=ANY($1::text[])
     ORDER BY retry.requested_at ASC,retry.system_retry_id ASC
     LIMIT $2`,
    [["retrying", "pending", "dispatched"], normalizedLimit],
  );
  const items = result.rows.map((row) => ({
    ...row,
    failure_type: "retryable_system_failure",
    system_retry_id: Number(row.system_retry_id),
    migration_intent_id: Number(row.migration_intent_id),
    candidate_id: Number(row.candidate_id),
    failed_dispatch_generation: Number(row.failed_dispatch_generation),
    failed_job_attempt: Number(row.failed_job_attempt),
    retry_dispatch_generation: row.retry_dispatch_generation == null
      ? null
      : Number(row.retry_dispatch_generation),
    snapshot_dispatch_generation: row.snapshot_dispatch_generation == null
      ? null
      : Number(row.snapshot_dispatch_generation),
    snapshot_active_job_attempt: row.snapshot_active_job_attempt == null
      ? null
      : Number(row.snapshot_active_job_attempt),
  }));
  return {
    count: items.length,
    items,
  };
}

export async function loadMigrationSystemRetriesSafely(options = {}) {
  try {
    return {
      available: true,
      ...(await loadMigrationSystemRetries(options)),
      error: null,
    };
  } catch {
    return {
      available: false,
      count: 0,
      items: [],
      error: "系统失败重试清单当前不可用",
    };
  }
}

export async function requestMigrationSystemRetry({
  crawlerApiUrl,
  systemRetryId,
  fetchImpl = fetch,
} = {}) {
  const baseUrl = requiredText(crawlerApiUrl, "crawlerApiUrl").replace(/\/+$/, "");
  const retryId = Number(systemRetryId);
  if (!Number.isSafeInteger(retryId) || retryId <= 0) {
    throw new TypeError("systemRetryId must be a positive integer");
  }
  const response = await fetchImpl(
    `${baseUrl}/api/migration/system-retries/${retryId}/retry`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}",
      signal: AbortSignal.timeout(15000),
    },
  );
  return responsePayload(response);
}

export function renderMigrationSystemRetries(value = []) {
  const state = Array.isArray(value)
    ? { available: true, items: value, error: null }
    : {
      available: value?.available !== false,
      items: Array.isArray(value?.items) ? value.items : [],
      error: value?.error || null,
    };
  const { items } = state;
  const rows = items.map((item) => {
    const retryId = Number(item.system_retry_id);
    const status = String(item.status ?? "");
    const retryConfirmation = status === "pending"
      ? "确认按 Candidate Fence 分配 G+1 重试吗？"
      : "确认重新投递已分配的 G+1 重试吗？";
    const action = ["pending", "dispatched"].includes(status)
      ? `<form class="inline-form" method="post" action="/migration-channels/system-retries/${retryId}/retry" onsubmit="return confirm('${retryConfirmation}');"><button class="btn btn-primary small-btn" type="submit">受控重试</button></form>`
      : `<span class="pill ${status === "dispatched" ? "good" : "warn"}">${escapeHtml(status)}</span>`;
    return `<tr>
      <td><strong class="mono">${escapeHtml(item.failure_type || "retryable_system_failure")}</strong><div class="note mono">${escapeHtml(item.failure_category)} / ${escapeHtml(item.failure_code)}</div></td>
      <td><a class="mono" href="/migration-channels/${encodeURIComponent(item.channel_id)}">${escapeHtml(item.channel_id)}</a></td>
      <td class="mono">${escapeHtml(item.candidate_id)}</td>
      <td class="mono">${escapeHtml(item.failed_job_attempt)}</td>
      <td class="mono">G${escapeHtml(item.failed_dispatch_generation)}</td>
      <td class="mono">${item.retry_dispatch_generation == null ? "-" : `G${escapeHtml(item.retry_dispatch_generation)}`}</td>
      <td>${action}</td>
    </tr>`;
  }).join("");
  return `<section class="table-panel mt">
    <div class="table-tools"><div class="panel-head"><div><h2>系统失败待重试</h2><div class="note">与后续迁移批次隔离，重试由 Candidate Fence 原子分配新 generation。</div></div><span class="pill ${items.length > 0 ? "warn" : "good"}">${items.length}</span></div></div>
    <div class="table-scroll"><table><thead><tr><th>失败类型</th><th>频道</th><th>Candidate</th><th>attempt</th><th>失败代</th><th>重试代</th><th>操作</th></tr></thead><tbody>${rows || `<tr><td colspan="7" class="muted">${escapeHtml(state.available ? "当前没有系统失败待重试项。" : state.error)}</td></tr>`}</tbody></table></div>
  </section>`;
}
