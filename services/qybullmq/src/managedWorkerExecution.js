import { queuesByRole } from "./queues.js";
import { decideYoutubeFailure } from "./youtubeFailurePolicy.js";

const RETRYABLE_ROUTE_FAILURES = new Set([
  "proxy_transport",
  "youtube_rate_limited",
  "youtube_challenge",
]);

const ROLE_QUEUES = Object.freeze({
  channel: Object.freeze(new Set([
    queuesByRole.channelCrawl,
    queuesByRole.channelIncremental,
  ])),
  discover: Object.freeze(new Set([queuesByRole.discoverPage])),
  query_quality: Object.freeze(new Set([queuesByRole.queryQuality])),
});

const MANAGED_QUEUES = new Set(
  Object.values(ROLE_QUEUES).flatMap((queues) => [...queues]),
);

function normalizedQueues(values) {
  return [...new Set((values ?? []).map((value) => String(value ?? "").trim()).filter(Boolean))];
}

function decisionPriority(decision) {
  if (["youtube_rate_limited", "youtube_challenge"].includes(decision?.kind)) return 3;
  if (decision?.kind === "proxy_transport") return 2;
  return 0;
}

function failureDecisions(error) {
  const recorded = Array.isArray(error?.channel_execution_attempt?.failure_decisions)
    ? error.channel_execution_attempt.failure_decisions
    : [];
  return [
    ...recorded,
    error?.youtube_failure_decision,
    decideYoutubeFailure({ error }),
  ].filter(Boolean);
}

export function validateWorkerQueueConfiguration({
  role = "",
  enabledQueues = [],
  fixedProxy = false,
} = {}) {
  const normalizedRole = String(role ?? "").trim().toLowerCase();
  const queues = normalizedQueues(enabledQueues);
  if (queues.length === 0) throw new Error("WORKER_QUEUES must contain at least one queue");

  if (normalizedRole) {
    const allowed = ROLE_QUEUES[normalizedRole];
    if (!allowed) throw new Error(`unsupported managed Worker role: ${normalizedRole}`);
    const invalid = queues.filter((queueName) => !allowed.has(queueName));
    if (invalid.length > 0) {
      throw new Error(
        `PROXY_SLOT_ROLE=${normalizedRole} cannot consume queues: ${invalid.join(",")}`,
      );
    }
    return Object.freeze({ managed: true, role: normalizedRole, queues: Object.freeze(queues) });
  }

  if (queues.includes(queuesByRole.contentDetail)) {
    throw new Error("standalone youtube-content-detail requires a designed managed Role");
  }
  const managed = queues.filter((queueName) => MANAGED_QUEUES.has(queueName));
  if (managed.length > 0 && !fixedProxy) {
    throw new Error(`Rota-managed queues require PROXY_SLOT_ROLE: ${managed.join(",")}`);
  }
  if (fixedProxy) {
    const invalid = managed.filter((queueName) => !ROLE_QUEUES.channel.has(queueName));
    if (invalid.length > 0) {
      throw new Error(`fixed proxy compatibility mode cannot consume queues: ${invalid.join(",")}`);
    }
  }
  return Object.freeze({ managed: false, role: null, queues: Object.freeze(queues) });
}

export function retryableRotaFailure(error) {
  const selected = failureDecisions(error)
    .filter((decision) => RETRYABLE_ROUTE_FAILURES.has(decision?.kind))
    .sort((left, right) => decisionPriority(right) - decisionPriority(left))[0] ?? null;
  if (!selected) return null;
  return Object.freeze({
    observation: selected.kind,
    source: String(
      selected?.evidence?.source
        ?? error?.youtube_failure_evidence?.source
        ?? "youtube_managed_request",
    ),
  });
}

export function managedFailedStage(job) {
  if (job?.queueName === queuesByRole.channelCrawl) return "channel_full";
  if (job?.queueName === queuesByRole.channelIncremental) return "channel_incremental";
  if (job?.queueName === queuesByRole.discoverPage) return "discover_page";
  if (job?.queueName === queuesByRole.queryQuality) return "query_quality_chunk";
  return "managed_workload";
}

export function managedBusinessState(job, result) {
  if (job?.queueName === queuesByRole.discoverPage && result?.qualification_pending === true) {
    return "waiting_downstream";
  }
  return "terminal";
}

export async function executeManagedWorkerAttempt({
  job,
  prepared,
  attempt,
  execute,
  persistRetryableCheckpoint,
} = {}) {
  if (typeof execute !== "function") throw new TypeError("execute is required");
  if (typeof persistRetryableCheckpoint !== "function") {
    throw new TypeError("persistRetryableCheckpoint is required");
  }
  try {
    const result = await execute({
      resumeMode: attempt?.resumeMode ?? prepared?.initialResumeMode ?? "initial",
      prepared,
    });
    return {
      kind: "managed_work_complete",
      businessState: managedBusinessState(job, result),
      result,
    };
  } catch (error) {
    const failure = retryableRotaFailure(error);
    if (!failure) throw error;
    const checkpointPersisted = await persistRetryableCheckpoint({
      job,
      prepared,
      attempt,
      error,
      failure,
    });
    if (checkpointPersisted !== true) {
      throw new Error("managed retry checkpoint was not durably persisted", { cause: error });
    }
    return {
      kind: "retryable_network_failure",
      observation: failure.observation,
      source: failure.source,
      failedStage: managedFailedStage(job),
      checkpointPersisted: true,
    };
  }
}

export function managedQueuesForRole(role) {
  return ROLE_QUEUES[String(role ?? "").trim().toLowerCase()] ?? null;
}
