import { createHash } from "node:crypto";

export const SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES = Object.freeze([
  "ready_auto",
  "ready_partial",
]);
export const FINALIZABLE_CHANNEL_STATUSES = Object.freeze([
  "active",
  "dormant",
]);
const SUCCESSFUL_PUBLICATION_FINALIZE_STATUS_SET = new Set(
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
);
const FINALIZABLE_CHANNEL_STATUS_SET = new Set(FINALIZABLE_CHANNEL_STATUSES);

export function isSuccessfulPublicationFinalize(status) {
  return SUCCESSFUL_PUBLICATION_FINALIZE_STATUS_SET.has(String(status ?? ""));
}

export function isFinalizableChannelStatus(status) {
  return FINALIZABLE_CHANNEL_STATUS_SET.has(String(status ?? ""));
}

function timestamp(value) {
  if (value == null) return null;
  if (value instanceof Date) return value.toISOString();
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString();
}

function sortedStrings(values) {
  return [...(values ?? [])].map(String).sort();
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

export function representedFinalizeRunIds(jobs = []) {
  return [...new Set((Array.isArray(jobs) ? jobs : [])
    .map((job) => text(job?.data?.run_id))
    .filter(Boolean))].sort();
}

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

export function finalizePublicationContext({
  job = {},
  run = null,
  runId = null,
  defaultAsOf,
} = {}) {
  const data = record(job.data);
  const result = record(run?.result_json);
  const publicationRepair = record(result.publication_repair);
  const finalRepair = record(result.final_repair);
  const publicationGapRepairExecution = record(result.publication_gap_repair_execution);
  const aboutOnlyPublicationGap = publicationGapRepairExecution.scope === "about_only"
    && (
      publicationGapRepairExecution.status === "staged"
      || text(data.reason) === "publication-gap-about-only"
    );
  const repairing = Boolean(
    text(data.repair_batch_id)
    || text(data.repair_reason)
    || Number(data.repair_round) > 0
    || result.publication_repair
    || result.final_repair
    || aboutOnlyPublicationGap,
  );
  if (!repairing) {
    return { revisionType: "incremental", repairId: null, asOf: defaultAsOf };
  }

  const normalizedRunId = text(runId) ?? text(run?.run_id);
  const round = Number(finalRepair.rounds ?? data.repair_round);
  const parentRunId = text(finalRepair.parent_run_id) ?? text(data.repair_parent_run_id);
  const roundId = Number.isSafeInteger(round) && round > 0
    ? `final-repair:${parentRunId ?? normalizedRunId ?? "unknown"}:${round}`
    : null;
  const repairId = text(data.repair_batch_id)
    ?? text(publicationRepair.batch_id)
    ?? text(finalRepair.job_id)
    ?? (aboutOnlyPublicationGap
      ? `publication-gap-about-only:${normalizedRunId ?? "unknown"}`
      : null)
    ?? roundId
    ?? text(job.id)
    ?? normalizedRunId;
  if (!repairId) throw new TypeError("a stable Repair identity is required");

  return {
    revisionType: "repair",
    repairId,
    asOf: text(publicationRepair.prepared_at)
      ?? text(finalRepair.queued_at)
      ?? run?.started_at
      ?? (job.timestamp == null ? null : new Date(Number(job.timestamp)).toISOString())
      ?? defaultAsOf,
  };
}

export function isCurrentChannelRun(channel, runId) {
  const requested = String(runId ?? "").trim();
  const latest = String(channel?.latest_run_id ?? "").trim();
  return Boolean(requested && latest && requested === latest);
}

export function finalizeSourceRevision({ channel, run, candidates = [], contents = [], agent = null }) {
  const source = {
    channel: {
      channel_id: channel?.channel_id ?? null,
      latest_run_id: channel?.latest_run_id ?? null,
      status: channel?.status ?? null,
      agent_status: channel?.agent_status ?? null,
      updated_at: timestamp(channel?.updated_at),
    },
    run: {
      run_id: run?.run_id ?? null,
      detail_status: run?.detail_status ?? null,
      expected_content_count: Number(run?.expected_content_count ?? 0),
    },
    candidates: candidates.map((item) => ({
      candidate_id: Number(item.candidate_id),
      content_type: item.content_type ?? null,
      content_key: item.content_key ?? null,
      detail_status: item.detail_status ?? null,
      api_status: item.api_status ?? null,
      missing_fields: sortedStrings(item.missing_fields),
      updated_at: timestamp(item.updated_at),
    })),
    contents: contents.map((item) => ({
      content_key: item.content_key ?? null,
      content_type: item.content_type ?? null,
      source_content_id: item.source_content_id ?? null,
      revision_at: timestamp(item.last_enriched_at ?? item.last_seen_at),
    })),
    agent: agent
      ? {
          status: agent.status ?? null,
          prompt_hash: agent.prompt_hash ?? null,
          attempts: Number(agent.attempts ?? 0),
          updated_at: timestamp(agent.updated_at),
        }
      : null,
  };
  return createHash("sha256").update(JSON.stringify(source)).digest("hex");
}

export function finalizeDispatchRevision(state) {
  return createHash("sha256").update(JSON.stringify(state ?? {})).digest("hex");
}

export function finalizedProfileIsCurrent(existing, runId, sourceRevision, observationOutcomes = null) {
  if (String(existing?.run_id ?? "") !== String(runId ?? "")) return false;
  if (String(existing?.quality_json?.source_revision ?? "") !== String(sourceRevision ?? "")) {
    return false;
  }
  if (observationOutcomes && typeof observationOutcomes === "object" && !Array.isArray(observationOutcomes)) {
    const existingOutcomes = record(existing?.quality_json?.initial_observations).outcomes;
    const recordedOutcomes = record(existingOutcomes);
    const nextOutcomes = record(observationOutcomes);
    for (const kind of ["about", "video", "agent"]) {
      if (String(recordedOutcomes[kind] ?? "") !== String(nextOutcomes[kind] ?? "")) {
        return false;
      }
    }
  }
  return true;
}

export function finalizeStatusCanAdvance(existingStatus, nextStatus, sameRun = true) {
  if (!sameRun) return true;
  const current = String(existingStatus ?? "");
  const next = String(nextStatus ?? "");
  if (current === "ready_auto") return next === "ready_auto";
  if (current === "ready_partial") return next === "ready_partial" || next === "ready_auto";
  return true;
}

export function resolveFinalizeStatus({
  channelStatus,
  openDetailCount = 0,
  openApiCount = 0,
  hasAgent = false,
  missingAgentFieldCount = 0,
  missingChannelFieldCount = 0,
  missingContentFieldCount = 0,
  unavailableCount = 0,
  incompleteSourceObservationCount = 0,
} = {}) {
  if (Number(openDetailCount) > 0) return "pending_detail";
  if (Number(openApiCount) > 0) return "pending_api";
  if (channelStatus === "dormant") return "ready_partial";
  if (!hasAgent || Number(missingAgentFieldCount) > 0) return "pending_agent";
  if (
    Number(missingChannelFieldCount) > 0
    || Number(missingContentFieldCount) > 0
    || Number(unavailableCount) > 0
    || Number(incompleteSourceObservationCount) > 0
  ) return "ready_partial";
  return "ready_auto";
}

export function finalizedRunState(finalizedStatus) {
  const status = String(finalizedStatus ?? "");
  if (isSuccessfulPublicationFinalize(status)) {
    return { status: "done", terminal: true };
  }
  if (status === "pending_agent") return { status: "waiting_agent", terminal: false };
  return { status: "waiting_detail", terminal: false };
}
