import {
  classifyTerminalChannelError,
  markChannelRemoved,
} from "./channelLifecycle.js";
import { recordCrawlerObservation } from "./crawlObservationStore.js";
import { validateIncrementalJob } from "./incrementalPlan.js";
import { isVideoExecutionRecoveryPending } from "./videoExecutionRecovery.js";

const DOMAIN_ORDER = ["about", "video", "agent"];
const COMPLETED_STATES = new Set(["complete", "partial", "queued"]);

function resultJson(value) {
  if (!value) return {};
  return typeof value === "string" ? JSON.parse(value) : value;
}

export function classifyIncrementalFailure(error) {
  if (classifyTerminalChannelError(error)) return "channel_removed";
  const message = String(error?.message || error || "").toLowerCase();
  if (/removed.*community guidelines|terminated.*community guidelines/.test(message)) {
    return "channel_removed";
  }
  if (/channel.*(?:unavailable|not found)|this page isn't available/.test(message)) {
    return "channel_unavailable";
  }
  if (/timed?\s*out|timeout/.test(message)) return "timeout";
  if (/proxy|tunnel|socks|bot.challenge|not a bot|captcha/.test(message)) {
    return "proxy_failure";
  }
  if (/parser|contract/.test(message)) return "parser_failure";
  return "crawler_failure";
}

function failedDomain(plan, run) {
  const domains = resultJson(run.result_json).domains ?? {};
  return DOMAIN_ORDER.find((domain) => (
    plan.task_mask[domain] === true
    && !COMPLETED_STATES.has(String(domains[domain]?.status || ""))
  )) ?? null;
}

function genericFailurePayload(domain, failureKind, attempts, removedReason = null) {
  if (domain === "agent") return { failed_plan_count: 1 };
  return {
    failure_kind: failureKind,
    attempt_count: attempts,
    ...(removedReason ? { removed_reason: removedReason } : {}),
  };
}

export async function recordIncrementalTerminalFailure({
  job,
  error,
  attempts,
  maxAttempts = attempts,
  permanent = false,
  withTransaction,
  recordGeneric = recordCrawlerObservation,
  markRemoved = markChannelRemoved,
  crawlerVersion = String(process.env.CRAWLER_VERSION || "qy-v16"),
}) {
  // A superseded execution no longer owns the Plan. The outer queue failure
  // callback must not publish a failed Observation for its replacement.
  if (error?.code === "CONTENT_DETAIL_EXECUTION_FENCE_STALE") {
    return { recorded: false, reason: "execution_superseded" };
  }
  if (isVideoExecutionRecoveryPending(error)) {
    return { recorded: false, reason: "execution_recovery_pending" };
  }
  const attemptCount = Math.max(1, Number(attempts) || 1);
  const maximum = Math.max(1, Number(maxAttempts) || 1);
  const terminalChannel = classifyTerminalChannelError(error);
  if (!permanent && !terminalChannel && attemptCount < maximum) {
    return { recorded: false, reason: "retry_pending" };
  }
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");

  const plan = validateIncrementalJob(job);
  const failureKind = terminalChannel?.failure_kind ?? classifyIncrementalFailure(error);
  const removedReason = terminalChannel?.removed_reason ?? null;
  const message = String(error?.message || error || "incremental job failed").slice(0, 2000);
  return withTransaction(async (client) => {
    const rows = await client.query(
      `SELECT run_id,result_json,finished_at
       FROM crawler.channel_runs
       WHERE plan_id=$1
       FOR UPDATE`,
      [plan.plan_id],
    );
    const run = rows.rows[0];
    if (!run) return { recorded: false, reason: "run_not_found" };
    const domain = failedDomain(plan, run);
    if (!domain) return { recorded: false, reason: "no_failed_domain" };

    const observedAt = new Date(run.finished_at || Date.now()).toISOString();
    if (terminalChannel) {
      await markRemoved(client, {
        channelId: plan.channel_id,
        runId: run.run_id,
        runStatus: "failed",
        terminal: terminalChannel,
        observedAt,
      });
    }
    const common = {
      idempotencyKey: `terminal-failure:${domain}:${run.run_id}`,
      channelId: plan.channel_id,
      runId: run.run_id,
      observedAt,
      planId: plan.plan_id,
      planDay: plan.plan_day,
      triggerReason: "clock_due",
      scheduledAt: plan.scheduled_at,
      startedAt: null,
      finishedAt: observedAt,
      crawlerVersion,
      extractorVersions: {},
      errorClass: error?.name || "Error",
      errorMessage: message,
    };

    const payload = genericFailurePayload(
      domain,
      failureKind,
      attemptCount,
      removedReason,
    );
    const observation = await recordGeneric(client, {
      ...common,
      observationKind: domain,
      command: payload,
      prepare: async () => ({
        outcome: "failed",
        outcomeReasonCode: `${domain}_${failureKind}`,
        resultSummary: payload,
        payload,
        errorClass: common.errorClass,
        errorMessage: message,
      }),
    });
    return {
      recorded: true,
      domain,
      failure_kind: failureKind,
      removed_reason: removedReason,
      observation,
    };
  });
}
