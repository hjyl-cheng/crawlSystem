#!/usr/bin/env node
// Recover only original snapshots that were incorrectly failed while awaiting API.
import { parseArgs } from "node:util";
import { readFile } from "node:fs/promises";
import { Queue } from "bullmq";
import { query, withTransaction, closeDb } from "../src/db.js";
import { redisOptions, queuesByRole, bullmqPrefix } from "../src/queues.js";
import { retryFullCrawlSnapshotJob } from "../src/finalRepairJobRecovery.js";
import { mergeVideoApiEvidence } from "../src/videoDetailApiFallback.js";
import { validateFullCrawlYoutubeJsDetail } from "../src/fullCrawlYoutubeJsModel.js";

const { values } = parseArgs({ options: { "candidate-ids": { type: "string" }, "prior-timeout-plan": { type: "string" }, apply: { type: "boolean", default: false } } });
// A later automatic retry can replace the original timeout with budget exhaustion.
// Require the operator's previously validated dry-run evidence for that exact Job.
const priorPlans = values["prior-timeout-plan"]
  ? (await readFile(values["prior-timeout-plan"], "utf8")).split("\n").filter(line => line.startsWith("{")).map(line => JSON.parse(line))
  : [];
const ids = String(values["candidate-ids"] ?? "").split(",").map(Number);
if (!ids.length || ids.some(id => !Number.isSafeInteger(id) || id <= 0)) throw new Error("Explicit positive --candidate-ids required");
const queue = new Queue(queuesByRole.channelCrawl, { connection: redisOptions, ...(bullmqPrefix ? { prefix: bullmqPrefix } : {}) });
const timeoutMessage = "Data API fallback is still pending; durable request retained for recovery";
try {
  for (const id of [...new Set(ids)]) {
    const candidate = (await query("SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1", [id])).rows[0];
    const run = (await query("SELECT * FROM crawler.channel_runs WHERE candidate_id=$1 ORDER BY started_at DESC,created_at DESC LIMIT 1", [id])).rows[0];
    if (!candidate || !run) throw new Error(`Missing candidate/run: ${id}`);
    const prepared = run.result_json?.video_api_timeout_recovery?.operation === "api-continuation-20260909";
    const priorPlan = priorPlans.find(plan => plan.candidate_id === id && plan.run_id === run.run_id
      && plan.job_id === run.result_json?.job_id && plan.apply === false && plan.failure_finished_at);
    const supersededTimeout = run.status === "failed" && run.error_message === "Rota Business Run budget exhausted"
      && priorPlan != null;
    if (prepared && run.status !== "failed") {
      const existingJob = await queue.getJob(run.result_json.job_id);
      const skippedBudget = existingJob?.returnvalue?.skip_reason === "proxy_control_business_run_budget_exhausted";
      if (!existingJob || (await existingJob.getState() !== "failed" && !skippedBudget)) {
        console.log(JSON.stringify({ candidate_id: id, run_id: run.run_id, action: "already_recovering", status: run.status }));
        continue;
      }
    }
    if (candidate.status !== "accepted" || candidate.snapshot_active_job_id != null
        || (!supersededTimeout && !(prepared && run.status === "waiting_detail")
          && ![timeoutMessage, `${timeoutMessage}: VIDEO_API_FALLBACK_UNRESOLVED`].includes(run.error_message))
        || run.publication_finalized_at != null) throw new Error(`Target no longer matches API timeout failure: ${id}`);
    const requests = (await query(`SELECT r.* FROM crawler.youtube_api_detail_requests r
      JOIN crawler.content_candidates c ON c.run_id=r.run_id AND c.source_content_id=r.source_content_id
      WHERE r.run_id=$1 AND c.detail_status NOT IN ('done','unavailable') ORDER BY r.created_at`, [run.run_id])).rows;
    if (!requests.length || requests.some(row => row.status !== "done" || row.consumer !== "full")) {
      throw new Error(`API evidence is not ready: ${id}`);
    }
    for (const row of requests) validateFullCrawlYoutubeJsDetail(row.source_content_id,
      mergeVideoApiEvidence(row.source_content_id, row.partial_detail, row.detail_json), { optionalComments: true });
    if (supersededTimeout && !requests.some(row => row.request_id === priorPlan.request_id)) {
      throw new Error(`Prior timeout API identity changed: ${id}`);
    }
    const job = await queue.getJob(run.result_json.job_id);
    const jobState = job ? await job.getState() : null;
    const skippedBudget = prepared && jobState === "completed"
      && job.returnvalue?.skip_reason === "proxy_control_business_run_budget_exhausted";
    if (!job || (jobState !== "failed" && !skippedBudget) || job.data.run_id !== run.run_id
        || Number(job.data.candidate_id) !== id
        || Number(job.data.dispatch_generation) !== Number(candidate.snapshot_dispatch_generation)) {
      throw new Error(`Original failed snapshot identity changed: ${id}`);
    }
    const plan = { candidate_id: id, run_id: run.run_id, job_id: job.id,
      request_id: requests[0].request_id, failure_finished_at: run.finished_at, apply: values.apply };
    console.log(JSON.stringify(plan));
    if (!values.apply) continue;
    await job.updateData({ ...job.data, video_api_continuation: { request_id: plan.request_id } });
    await withTransaction(async client => {
      const locked = (await client.query("SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE", [id])).rows[0];
      if (locked.status !== "accepted" || locked.snapshot_active_job_id != null
          || Number(locked.snapshot_dispatch_generation) !== Number(job.data.dispatch_generation)) {
        throw new Error(`Candidate ownership changed: ${id}`);
      }
      const binding = (await client.query(`SELECT * FROM crawler.business_run_bindings
        WHERE business_run_id=$1 FOR UPDATE`, [run.run_id])).rows[0];
      if (!binding || Number(binding.candidate_id) !== id || binding.channel_id !== run.channel_id) {
        throw new Error(`Business binding identity changed: ${id}`);
      }
      if (binding.status === "terminal") {
        if (!priorPlan || binding.terminal_reason !== "proxy_control_business_run_budget_exhausted"
            || !binding.materialized_at) throw new Error(`Unexpected terminal binding: ${id}`);
        await client.query(`UPDATE crawler.business_run_bindings SET status='materialized',terminal_reason=NULL,
          updated_at=now() WHERE business_run_id=$1`, [run.run_id]);
      }
      if (prepared) return;
      const updated = await client.query(`UPDATE crawler.channel_runs SET status='waiting_detail',detail_status='queued',
        error_message=NULL,finished_at=NULL,updated_at=now(),
        result_json=result_json || jsonb_build_object('video_api_timeout_recovery',$3::jsonb)
        WHERE run_id=$1 AND status='failed' AND error_message=$2 RETURNING run_id`,
      [run.run_id, run.error_message, JSON.stringify({ operation: "api-continuation-20260909", original_error: run.error_message,
        original_finished_at: run.finished_at, prior_timeout_plan: priorPlan ?? null,
        requested_at: new Date().toISOString(), request_id: plan.request_id })]);
      if (updated.rowCount !== 1) throw new Error(`Run changed before recovery: ${id}`);
    });
    const recovered = await retryFullCrawlSnapshotJob(queue, { run, candidate });
    console.log(JSON.stringify({ candidate_id: id, action: recovered.action }));
  }
} finally { await queue.close(); await closeDb(); }
