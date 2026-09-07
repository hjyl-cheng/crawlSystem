#!/usr/bin/env node

import { parseArgs } from "node:util";
import pg from "pg";
import { ChannelExecutionRuntimeAdapter } from "../src/channelExecutionRuntimeAdapter.js";
import { databaseUrl } from "../src/databaseConnection.js";
import { closeDb } from "../src/db.js";
import { dynamicRotaProxyConfig } from "../src/fixedProxyConfig.js";
import { resolveWorkerIdentityPolicy } from "../src/identityPolicyCatalog.js";
import {
  incrementalYoutubeJsVideoProbeAttemptOutcome,
  probeIncrementalYoutubeJsVideoFetch,
} from "../src/incrementalYoutubeJsVideo.js";
import {
  postgresReadOnlyQuery,
  selectIncrementalYoutubeJsVideoProbePlan,
} from "../src/incrementalYoutubeJsVideoProbe.js";
import { buildManagedDiagnosticJob } from "../src/managedDiagnosticJob.js";
import { closeProxyControlClient, proxyControlClient } from "../src/proxyControlClient.js";
import {
  RotaExecutionBudgetExhaustedError,
  RotaSlotAdapter,
} from "../src/rotaSlotAdapter.js";
import { closeYoutubeJs, openYoutubeJsChannel } from "../src/youtubeJs.js";

const { Client } = pg;

function utcDay() {
  return new Date().toISOString().slice(0, 10);
}

function detailLimit(value) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0 || parsed > 10) {
    throw new TypeError("--detail-limit-per-phase must be an integer between 0 and 10");
  }
  return parsed;
}

function routeSwitchLimit(value) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0 || parsed > 5) {
    throw new TypeError("--max-route-switches must be an integer between 0 and 5");
  }
  return parsed;
}

function optionsFromArgs(argv = process.argv.slice(2)) {
  const { values } = parseArgs({
    args: argv,
    options: {
      "channel-id": { type: "string" },
      "plan-day": { type: "string", default: utcDay() },
      "detail-limit-per-phase": { type: "string", default: "1" },
      "max-route-switches": { type: "string", default: "2" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  return {
    help: values.help === true,
    channelId: String(values["channel-id"] ?? "").trim() || null,
    planDay: String(values["plan-day"]),
    detailLimitPerPhase: detailLimit(values["detail-limit-per-phase"]),
    maxRouteSwitches: routeSwitchLimit(values["max-route-switches"]),
  };
}

function usage() {
  return [
    "Usage: node scripts/probeIncrementalYoutubeJsVideo.mjs [options]",
    "",
    "Options:",
    "  --channel-id <UC...>             Use one inactive Channel Clock ID as a sample",
    "  --plan-day <YYYY-MM-DD>          Defaults to the current UTC day",
    "  --detail-limit-per-phase <0..10> Defaults to 1 (at most two Details total)",
    "  --max-route-switches <0..5>      Defaults to 2 (at most three routes total)",
    "  -h, --help                       Show this help",
    "",
    "The crawler database is queried through verified read-only transactions. The probe",
    "only borrows a channel ID from Channel Clock and never reads or claims a Daily Plan.",
    "It does not create",
    "Batch, Item, Observation, Cursor, Outbox, Lifecycle, or Publication records.",
    "Channels with dispatching/dispatched/running Plans are always excluded. The selected",
    "ID receives a separate Video-only manual/read_only_probe input (player cap 1, next cap 0).",
  ].join("\n");
}

function createRotaSlot({ maxRouteSwitches }) {
  const workerId = String(
    process.env.INCREMENTAL_VIDEO_PROBE_WORKER_ID
      || `incremental-video-probe-${process.pid}`,
  ).trim();
  const proxyPassword = String(process.env.ROTA_BULLMQ_PROXY_PASSWORD || "");
  const dynamicProxy = dynamicRotaProxyConfig({
    slotRole: "channel",
    controlUrl: process.env.ROTA_PROXY_CONTROL_URL,
    controlToken: process.env.ROTA_PROXY_CONTROL_TOKEN,
    proxyPassword,
  });
  if (!dynamicProxy) throw new Error("Incremental Video probe requires a Rota channel slot");
  const resolvedPolicy = resolveWorkerIdentityPolicy({
    role: "channel",
    policyId: process.env.ROTA_IDENTITY_POLICY_ID,
    expectedWorkloadScope: process.env.ROTA_WORKLOAD_SCOPE_EXPECTED,
  });
  return new RotaSlotAdapter({
    client: proxyControlClient(),
    role: "channel",
    workerId,
    resolvedPolicy,
    proxyBaseUrl: String(process.env.ROTA_PROXY_BASE_URL || "http://youtube-rota-qy-core:8000"),
    proxyPassword,
    identityRuntime: new ChannelExecutionRuntimeAdapter({ workerId }),
    maxRouteSwitchesPerExecution: maxRouteSwitches,
  });
}

async function openReadOnlyClient() {
  const client = new Client({
    connectionString: databaseUrl(),
    application_name: "incremental-youtubejs-video-read-only-probe",
  });
  await client.connect();
  const query = postgresReadOnlyQuery(client);
  const guard = (await query(
    `SELECT current_database() AS database_name,
            current_user AS database_user`,
  )).rows[0] ?? {};
  return { client, query, guard };
}

const options = optionsFromArgs();
if (options.help) {
  console.log(usage());
  process.exit(0);
}
// This is process-local. The probe must exercise the YouTubeJS Detail surface even
// when the production worker image currently runs in channel-only extractor mode.
process.env.YOUTUBEJS_EXTRACTOR_MODE = "full";

let database = null;
let rotaSlot = null;
let report = null;
let routeExecutionError = null;
const routeAttempts = [];
try {
  database = await openReadOnlyClient();
  const selected = await selectIncrementalYoutubeJsVideoProbePlan(
    database.query,
    {
      channelId: options.channelId,
      planDay: options.planDay,
      clockStateFallback: {
        capacityFactor: 1,
        playerCap: 1,
        nextCap: 0,
        capacityVersion: "read-only-probe-v1",
        plannerConfigVersion: String(
          process.env.INCREMENTAL_VIDEO_PLANNER_VERSION || "video-plan-1",
        ),
      },
    },
  );
  console.log(JSON.stringify({
    event: "incremental_youtubejs_video_probe.plan_selected",
    database: {
      name: database.guard.database_name,
      user: database.guard.database_user,
      read_only: true,
    },
    clock: selected.clock,
    detail_limit_per_phase: options.detailLimitPerPhase,
    max_route_switches: options.maxRouteSwitches,
  }));

  rotaSlot = createRotaSlot({ maxRouteSwitches: options.maxRouteSwitches });
  const policy = rotaSlot.policy;
  await rotaSlot.start();
  const diagnostic = buildManagedDiagnosticJob({
    kind: "incremental_video_probe",
    channelId: selected.plan.channel_id,
    runId: `incremental-video-probe:${selected.plan.plan_id}`,
  });
  const job = Object.freeze({ ...diagnostic, attemptsStarted: 1 });
  try {
    await rotaSlot.executeJob(job, {
      prepare: async () => ({
        kind: "ready",
        businessRunId: `incremental-video-probe:${selected.plan.plan_id}:${Date.now()}`,
        workloadKind: "channel_full",
        identityPolicyId: policy.id,
        identityPolicyVersion: policy.version,
        identityPolicyHash: policy.hash,
        initialResumeMode: "initial",
      }),
      executeAttempt: async (_prepared, attempt) => {
        report = await probeIncrementalYoutubeJsVideoFetch({
          plan: selected.plan,
          query: database.query,
          getChannelSnapshot: () => openYoutubeJsChannel(selected.plan.channel_id, {
            includeAbout: false,
          }),
          detailLimitPerPhase: options.detailLimitPerPhase,
        });
        routeAttempts.push({
          attempt_number: Number(attempt.number),
          resume_mode: String(attempt.resumeMode),
          route_generation: Number(attempt.routeGeneration),
          scan_state: report.flow.find((step) => step.step === "uploads_scan")?.state ?? null,
          halted_phase: report.halted?.phase ?? null,
          failure_kind: report.halted?.failure?.decision?.kind ?? null,
          fetch_flow_ok: report.verification.fetch_flow_ok === true,
          detail_surface_observed: report.verification.detail_surface_observed === true,
        });
        return incrementalYoutubeJsVideoProbeAttemptOutcome(report);
      },
    });
  } catch (error) {
    if (!(error instanceof RotaExecutionBudgetExhaustedError) || !report) throw error;
    routeExecutionError = {
      name: String(error.name || "RotaExecutionBudgetExhaustedError"),
      message: String(error.message || error),
    };
  }
} finally {
  await rotaSlot?.close().catch(() => null);
  await closeYoutubeJs().catch(() => null);
  await closeProxyControlClient().catch(() => null);
  await database?.client.end().catch(() => null);
  await closeDb().catch(() => null);
}

if (!report) throw new Error("Incremental Video probe did not produce a report");
report.runtime = {
  probe_data_queries_read_only: true,
  clock_plan_state_writes: 0,
  incremental_fetch_fact_writes: 0,
  diagnostic_execution_telemetry: true,
  rota_control_telemetry: true,
  route_attempt_limit: options.maxRouteSwitches + 1,
  executed_route_attempts: routeAttempts.length,
  completed_route_switches: Math.max(0, routeAttempts.length - 1),
  retryable_route_failures: routeAttempts.filter((attempt) => [
    "proxy_transport",
    "youtube_rate_limited",
    "youtube_challenge",
  ].includes(attempt.failure_kind)).length,
  route_budget_exhausted: routeExecutionError != null,
  route_execution_error: routeExecutionError,
};
report.diagnostic_route_attempts = routeAttempts;
console.log(JSON.stringify({
  event: "incremental_youtubejs_video_probe.done",
  report,
}, null, 2));
if (!report.verification.fetch_flow_ok || !report.verification.detail_surface_observed) {
  process.exitCode = 1;
}
