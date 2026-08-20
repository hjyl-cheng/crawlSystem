import { recordBusinessRunBudgetExhaustion } from "./businessRunBudgetRecovery.js";
import { queuesByRole } from "./queues.js";

export const LEGACY_BUDGET_EXHAUSTION_REASON =
  "proxy control business run budget exhausted";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function fail(message) {
  throw new Error(`Legacy budget evidence rejected: ${message}`);
}

async function validatedJob(queue, { runId, jobId }) {
  if (!queue || typeof queue.getJob !== "function") {
    throw new TypeError("the Channel Crawl BullMQ queue is required");
  }
  const job = await queue.getJob(jobId);
  if (!job) fail(`Job is missing: ${jobId}`);
  const state = await job.getState();
  if (String(job.id) !== jobId) fail("Job ID mismatch");
  if (job.queueName !== queuesByRole.channelCrawl) fail("Job queue mismatch");
  if (!["channel-crawl-repair", "channel-detail-repair"].includes(job.name)) {
    fail("Job type is not a final repair");
  }
  if (state !== "failed") fail(`Job state is ${state}`);
  if (String(job.failedReason ?? "").trim() !== LEGACY_BUDGET_EXHAUSTION_REASON) {
    fail("failedReason is not the exact Rota budget exhaustion reason");
  }
  if (String(job.data?.run_id ?? "").trim() !== runId) fail("execution Run ID mismatch");
  if (String(job.data?.repair_parent_run_id ?? "").trim() !== runId) {
    fail("repair parent Run ID mismatch");
  }
  if (!Number.isSafeInteger(Number(job.data?.repair_round))
      || Number(job.data.repair_round) <= 0) fail("repair round is invalid");
  if (!Number.isSafeInteger(Number(job.attemptsMade)) || Number(job.attemptsMade) <= 0) {
    fail("attempt history is missing");
  }
  return job;
}

export async function recoverLegacyBusinessRunBudgetEvidence({
  query,
  queue,
  runId,
  jobId,
  apply = false,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const normalizedRunId = requiredText(runId, "runId");
  const normalizedJobId = requiredText(jobId, "jobId");
  const job = await validatedJob(queue, {
    runId: normalizedRunId,
    jobId: normalizedJobId,
  });
  const result = await query(
    `SELECT run_id,status,detail_status,result_json
     FROM crawler.channel_runs
     WHERE run_id=$1
     LIMIT 1`,
    [normalizedRunId],
  );
  const run = result.rows?.[0];
  if (!run) fail(`Run is missing: ${normalizedRunId}`);
  if (run.status !== "failed" || run.detail_status !== "failed") {
    fail("Run is not failed/failed");
  }
  if (String(run.result_json?.final_repair?.job_id ?? "") !== normalizedJobId) {
    fail("Run final repair Job ID mismatch");
  }
  if (run.result_json?.proxy_control?.status === "business_run_budget_exhausted") {
    return {
      action: "already_recorded",
      applied: false,
      run_id: normalizedRunId,
      job_id: normalizedJobId,
    };
  }
  if (!apply) {
    return {
      action: "record_budget_exhaustion",
      applied: false,
      run_id: normalizedRunId,
      job_id: normalizedJobId,
    };
  }
  const recorded = await recordBusinessRunBudgetExhaustion(query, job, {
    source: "bullmq_failed_job_reconciliation",
  });
  if (!recorded.recorded || recorded.execution_run_id !== normalizedRunId) {
    fail("Run evidence update did not match the requested Run");
  }
  return {
    action: "recorded_budget_exhaustion",
    applied: true,
    run_id: normalizedRunId,
    job_id: normalizedJobId,
  };
}
