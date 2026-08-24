import { executeIncrementalAbout } from "./incrementalAbout.js";
import { enqueueIncrementalAgent } from "./incrementalAgent.js";
import { incrementalDomainState } from "./incrementalRunStore.js";
import { executeIncrementalVideo } from "./incrementalVideo.js";
import { validateIncrementalJob } from "./incrementalPlan.js";
import { openYoutubeJsChannel } from "./youtubeJs.js";

const SESSION_DOMAINS = ["about", "video"];

function completed(state) {
  return ["complete", "partial", "queued"].includes(state);
}

function domainResult(result) {
  return {
    outcome: result?.outcome ?? (result?.queued ? "queued" : "complete"),
    observation_id: result?.observation_id ?? null,
    event_id: result?.event_id ?? null,
    kind_sequence: result?.kind_sequence ?? null,
    duplicate: result?.duplicate === true,
    ...(result?.reservation_cleanup_deferred === true
      ? { reservation_cleanup_deferred: true }
      : {}),
  };
}

export class IncrementalChannelRunner {
  constructor({
    runStore,
    agentBacklog,
    withTransaction,
    query,
    about = executeIncrementalAbout,
    video = executeIncrementalVideo,
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
    const getChannelSnapshot = () => {
      if (!snapshotPromise) {
        snapshotPromise = this.openChannel(plan.channel_id, {
          includeAbout: plan.task_mask.about,
        });
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
    let lifecycleStatus = null;
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
        if (domain === "video") lifecycleStatus = result?.lifecycle_status ?? null;
        await this.runStore.markDomain(runId, domain, outcome, domainResult(result));
      }

      if (plan.task_mask.agent && !completed(incrementalDomainState(run, "agent"))) {
        activeDomain = "agent";
        const result = lifecycleStatus === "dormant"
          ? { queued: false, outcome: "skipped", reason: "channel_dormant" }
          : await this.executors.agent(context);
        results.agent = result;
        await this.runStore.markDomain(
          runId,
          "agent",
          result?.queued === false ? "skipped" : "queued",
          domainResult(result),
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
        session_opened: snapshotPromise !== null,
      };
    } catch (error) {
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
