import { isVideoApiHandoff } from "./videoApiContinuation.js";
import { isVideoExecutionRecoveryPending } from "./videoExecutionRecovery.js";
import { prepareDormantUploadsProbe, pendingUploadsDormancy, dormantUploadsScan } from "./youtubeUploadsCountry.js";
import { executeIncrementalAbout } from "./incrementalAbout.js";
import { enqueueIncrementalAgent } from "./incrementalAgent.js";
import { incrementalDomainState } from "./incrementalRunStore.js";
import { executeIncrementalYoutubeJsVideo } from "./incrementalYoutubeJsVideo.js";
import { validateIncrementalJob } from "./incrementalPlan.js";
import { openYoutubeJsChannel } from "./youtubeJs.js";

const SESSION_DOMAINS = ["about", "video"];

function completed(state) {
  return ["complete", "partial", "queued"].includes(state);
}

function normalizeLifecycleStatus(value) {
  const status = String(value ?? "").trim();
  return ["active", "dormant"].includes(status) ? status : null;
}

function storedDomainResult(run, domain) {
  const raw = run?.result_json;
  const result = typeof raw === "string" ? JSON.parse(raw) : raw;
  return result?.domains?.[domain] ?? null;
}

function domainResult(result, domain) {
  return {
    outcome: result?.outcome ?? (result?.queued ? "queued" : "complete"),
    observation_id: result?.observation_id ?? null,
    event_id: result?.event_id ?? null,
    kind_sequence: result?.kind_sequence ?? null,
    duplicate: result?.duplicate === true,
    ...(result?.reservation_cleanup_deferred === true
      ? { reservation_cleanup_deferred: true }
      : {}),
    ...(domain === "video"
      ? {
          lifecycle_status: normalizeLifecycleStatus(result?.lifecycle_status),
          dormant_recheck_day: result?.dormant_recheck_day ?? null,
        }
      : {}),
  };
}

async function observationLifecycleStatus(query, {
  runId,
  observationId = null,
  allowUnavailable = false,
}) {
  if (typeof query !== "function") {
    if (allowUnavailable) return null;
    throw new TypeError("query is required to restore incremental Video lifecycle status");
  }
  const result = await query(
    `SELECT result_summary_json #>> '{activity,lifecycle_status}' AS lifecycle_status
     FROM crawler.crawl_observations
     WHERE run_id=$1
       AND observation_kind='video'
       AND ($2::uuid IS NULL OR observation_id=$2::uuid)
     ORDER BY kind_sequence DESC
     LIMIT 1`,
    [runId, observationId],
  );
  const lifecycleStatus = normalizeLifecycleStatus(result?.rows?.[0]?.lifecycle_status);
  if (lifecycleStatus == null && !allowUnavailable) {
    throw new Error(`incremental Video lifecycle status is unavailable for ${runId}`);
  }
  return lifecycleStatus;
}

export class IncrementalChannelRunner {
  constructor({
    runStore,
    agentBacklog,
    withTransaction,
    query,
    about = executeIncrementalAbout,
    video = executeIncrementalYoutubeJsVideo,
    agent = enqueueIncrementalAgent,
    openChannel = openYoutubeJsChannel,
  }) {
    if (!runStore || typeof runStore.claim !== "function") throw new TypeError("runStore is required");
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.runStore = runStore;
    this.agentBacklog = agentBacklog;
    this.withTransaction = withTransaction;
    this.query = query;
    this.executors = { about, video, agent };
    this.openChannel = openChannel;
  }

  async execute(job) {
    const claimedPlan = validateIncrementalJob(job);
    const claim = await this.runStore.claim(job.data);
    const plan = claimedPlan;
    const run = claim.run;
    const runId = run.run_id;
    if (claim.terminal) {
      return {
        ok: true,
        duplicate: true,
        terminal: true,
        run_id: runId,
        status: run.status,
      };
    }

    const startedAt = new Date().toISOString();
    let snapshotPromise = null;
    let sessionOpened = false;
    const getChannelSnapshot = () => {
      if (!snapshotPromise) {
        snapshotPromise = (async () => {
          const stored = plan.task_mask.video ? await this.query(
            "SELECT country_code,country_source,total_video_count,status,source_json->'uploads_recheck' AS uploads_recheck FROM crawler.channels WHERE channel_id=$1",
            [plan.channel_id],
          ) : { rows: [] };
          const channel = stored.rows[0] ?? {};
          const pendingDormant = pendingUploadsDormancy(
            channel.country_source === "youtube_about" ? channel.country_code : null,
          );
          if (plan.task_mask.video && pendingDormant) return {
            scanUploads: async ({ anchors = [] } = {}) => dormantUploadsScan(plan.channel_id, anchors, pendingDormant),
          };
          if (plan.plan_mode === "dormant_probe" && channel.status === "dormant") {
            const decision = prepareDormantUploadsProbe(channel.uploads_recheck?.country);
            if (decision) return {
              scanUploads: async ({ anchors = [] } = {}) => dormantUploadsScan(plan.channel_id, anchors, decision),
            };
          }
          sessionOpened = true;
          return this.openChannel(plan.channel_id, {
            includeAbout: plan.task_mask.about,
            uploadsCountry: channel.country_source === "youtube_about" ? channel.country_code : null,
            uploadsVideoCount: channel.total_video_count ?? null,
            dormantUploadsCountry: channel.status === "dormant" ? channel.uploads_recheck?.country : null,
          });
        })();
      }
      return snapshotPromise;
    };
    const context = {
      plan,
      runId,
      startedAt,
      withTransaction: this.withTransaction,
      query: this.query,
      getChannelSnapshot,
      agentBacklog: this.agentBacklog,
    };
    let activeDomain = null;
    const storedVideoResult = storedDomainResult(run, "video") ?? {};
    let lifecycleStatus = normalizeLifecycleStatus(storedVideoResult.lifecycle_status);
    const results = {};
    try {
      for (const domain of SESSION_DOMAINS) {
        if (!plan.task_mask[domain] || completed(incrementalDomainState(run, domain))) continue;
        const executor = this.executors[domain];
        if (typeof executor !== "function") {
          throw new Error(`incremental ${domain} executor is not configured`);
        }
        activeDomain = domain;
        await this.runStore.markDomain(runId, domain, "running", { started_at: startedAt });
        const result = await executor(context);
        if (result?.outcome === "failed") {
          throw new Error(`incremental ${domain} returned a failed Observation`);
        }
        const outcome = result?.outcome === "partial" ? "partial" : "complete";
        results[domain] = result;
        if (domain === "video") {
          lifecycleStatus = normalizeLifecycleStatus(result?.lifecycle_status);
        }
        await this.runStore.markDomain(runId, domain, outcome, domainResult(result, domain));
      }

      if (plan.task_mask.agent && !completed(incrementalDomainState(run, "agent"))) {
        activeDomain = "agent";
        if (plan.task_mask.video && lifecycleStatus == null) {
          const videoOutcome = results.video?.outcome
            ?? storedVideoResult.outcome
            ?? storedVideoResult.status
            ?? null;
          lifecycleStatus = await observationLifecycleStatus(this.query, {
            runId,
            observationId: results.video?.observation_id
              ?? storedVideoResult.observation_id
              ?? null,
            allowUnavailable: videoOutcome === "partial",
          });
        }
        const result = lifecycleStatus === "dormant"
          ? { queued: false, outcome: "skipped", reason: "channel_dormant" }
          : await this.executors.agent(context);
        results.agent = result;
        await this.runStore.markDomain(
          runId,
          "agent",
          result?.queued === false ? "skipped" : "queued",
          domainResult(result, "agent"),
        );
      }
      const waitingForAgent = plan.task_mask.agent && results.agent?.queued !== false;
      await this.runStore.finish(runId, { waitingForAgent });
      return {
        ok: true,
        duplicate: false,
        resumed: claim.resumed,
        run_id: runId,
        status: waitingForAgent ? "waiting_agent" : "done",
        executed_domains: Object.keys(results),
        session_opened: sessionOpened,
      };
    } catch (error) {
      if (error?.code === "UPLOADS_COUNTRY_RECHECK" || error?.code === "CONTENT_DETAIL_EXECUTION_FENCE_STALE"
        || isVideoApiHandoff(error) || isVideoExecutionRecoveryPending(error)) throw error;
      if (activeDomain) {
        await this.runStore.markDomain(runId, activeDomain, "failed", {
          error: String(error?.message || error).slice(0, 1000),
        }).catch(() => {});
      }
      await this.runStore.fail(runId, error).catch(() => {});
      throw error;
    }
  }
}
