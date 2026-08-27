import { BusinessRunBindingStore } from "./businessRunBindingStore.js";
import { incrementalRunId, validateIncrementalPlan } from "./incrementalPlan.js";
import { publicationGapRepairJobIntent } from "./publicationGapRepairExecution.js";
import { queuesByRole } from "./queues.js";
import { materializeCheckpointRepairRun } from "./checkpointRepair.js";

const LEGACY_INCREMENTAL_RUNTIME_KEYS = Object.freeze([
  "run_id",
  "business_run_key",
  "identity_policy_id",
  "identity_policy_version",
  "identity_policy_hash",
]);
const LEGACY_INCREMENTAL_RUNTIME_KEY_SET = new Set(LEGACY_INCREMENTAL_RUNTIME_KEYS);

function required(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function positiveInteger(value, field) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) throw new TypeError(`${field} is required`);
  return number;
}

function optionalInteger(value) {
  if (value == null || value === "") return null;
  return positiveInteger(value, "integer value");
}

function retryAt(delayMs = 60_000) {
  return new Date(Date.now() + delayMs).toISOString();
}

function policyFields(resolvedPolicy) {
  const policy = resolvedPolicy?.policy;
  if (!policy) throw new TypeError("resolvedPolicy is required");
  return {
    policy,
    identityPolicyId: required(policy.id, "policy.id"),
    identityPolicyVersion: positiveInteger(policy.version, "policy.version"),
    identityPolicyHash: required(policy.hash, "policy.hash"),
  };
}

function initialResumeMode(job, resumed = false) {
  return Number(job?.attemptsMade ?? 0) > 0 || resumed
    ? "bullmq_redelivery_resume"
    : "initial";
}

async function restoreFrozenIncrementalPlan(job, fields) {
  const data = job?.data ?? {};
  const presentRuntimeKeys = LEGACY_INCREMENTAL_RUNTIME_KEYS.filter((key) => (
    Object.prototype.hasOwnProperty.call(data, key)
  ));
  if (presentRuntimeKeys.length === 0) return data;
  if (presentRuntimeKeys.length !== LEGACY_INCREMENTAL_RUNTIME_KEYS.length) {
    throw new TypeError("legacy Incremental runtime metadata is incomplete");
  }

  const planPayload = Object.fromEntries(
    Object.entries(data).filter(([key]) => !LEGACY_INCREMENTAL_RUNTIME_KEY_SET.has(key)),
  );
  const plan = validateIncrementalPlan(planPayload);
  const expectedRunId = incrementalRunId(plan.plan_id);
  const expectedBusinessRunKey = `incremental-plan:${plan.plan_id}`;
  if (data.run_id !== expectedRunId
      || data.business_run_key !== expectedBusinessRunKey
      || data.identity_policy_id !== fields.identityPolicyId
      || Number(data.identity_policy_version) !== fields.identityPolicyVersion
      || data.identity_policy_hash !== fields.identityPolicyHash) {
    throw new TypeError("legacy Incremental runtime metadata conflicts with the frozen Plan");
  }
  if (typeof job.updateData !== "function") {
    throw new TypeError("legacy Incremental runtime metadata requires BullMQ updateData");
  }
  await job.updateData(planPayload);
  job.data = planPayload;
  return planPayload;
}

async function bindRunToJob(job, runId, businessRunKey, extraData = {}) {
  const data = {
    ...(job.data ?? {}),
    ...extraData,
    run_id: runId,
    business_run_key: businessRunKey,
  };
  if (typeof job.updateData === "function"
      && (
        job.data?.run_id !== runId
        || job.data?.business_run_key !== businessRunKey
        || Object.entries(extraData).some(([key, value]) => job.data?.[key] !== value)
      )) {
    await job.updateData(data);
  }
  job.data = data;
}

async function cachePolicyOnJob(job, fields) {
  const data = {
    ...(job.data ?? {}),
    identity_policy_id: fields.identityPolicyId,
    identity_policy_version: fields.identityPolicyVersion,
    identity_policy_hash: fields.identityPolicyHash,
  };
  if (typeof job.updateData === "function"
      && (job.data?.identity_policy_id !== fields.identityPolicyId
        || Number(job.data?.identity_policy_version) !== fields.identityPolicyVersion
        || job.data?.identity_policy_hash !== fields.identityPolicyHash)) {
    await job.updateData(data);
  }
  job.data = data;
}

function ready({ businessRunId, workloadKind, fields, resumed = false, job, extra = {} }) {
  return {
    kind: "ready",
    job,
    businessRunId,
    workloadKind,
    identityPolicyId: fields.identityPolicyId,
    identityPolicyVersion: fields.identityPolicyVersion,
    identityPolicyHash: fields.identityPolicyHash,
    initialResumeMode: initialResumeMode(job, resumed),
    ...extra,
  };
}

function bindingIdentity(job) {
  const data = job?.data ?? {};
  const channelId = required(data.channel_id, "job.data.channel_id");
  const candidateId = optionalInteger(data.candidate_id);
  const explicitRunId = String(data.run_id ?? "").trim() || null;
  const cachedBusinessRunKey = String(data.business_run_key ?? "").trim() || null;
  const repairBatchId = String(data.repair_batch_id ?? "").trim() || null;
  const repairParentRunId = String(data.repair_parent_run_id ?? "").trim() || null;
  const repairRound = Number(data.repair_round ?? 0);
  const fullIntentId = String(data.full_intent_id ?? "").trim() || null;
  const retryIntentId = String(data.retry_intent_id ?? "").trim() || null;
  const recoveryBusinessRunId = String(data.recovery_business_run_id ?? "").trim() || null;

  if (retryIntentId) {
    if (candidateId == null) throw new TypeError("Recovery Intent requires candidate_id");
    if (!recoveryBusinessRunId) {
      throw new TypeError("Recovery Intent requires recovery_business_run_id");
    }
    return {
      businessRunKey: `full-candidate:${candidateId}:recovery:${retryIntentId}`,
      runKind: "full",
      explicitRunId: recoveryBusinessRunId,
      candidateId,
      fullIntentId: retryIntentId,
      retryIntentId,
    };
  }

  if (job.name === "channel-full-repair") {
    if (!repairBatchId) throw new TypeError("channel-full-repair requires repair_batch_id");
    return {
      businessRunKey: `full-repair:${repairBatchId}:${channelId}`,
      runKind: "full_repair",
      explicitRunId,
      candidateId,
      fullIntentId: repairBatchId,
    };
  }
  if (candidateId != null) {
    return {
      businessRunKey: `full-candidate:${candidateId}`,
      runKind: "full",
      explicitRunId: null,
      candidateId,
      fullIntentId,
    };
  }
  if (repairParentRunId && Number.isSafeInteger(repairRound) && repairRound > 0) {
    return {
      businessRunKey: `full-repair:auto:${repairParentRunId}:${repairRound}:${channelId}`,
      runKind: "full_repair",
      explicitRunId: null,
      candidateId: null,
      fullIntentId: `${repairParentRunId}:${repairRound}`,
    };
  }
  if (fullIntentId) {
    return {
      businessRunKey: `full-intent:${fullIntentId}`,
      runKind: "full",
      explicitRunId: null,
      candidateId: null,
      fullIntentId,
    };
  }
  if (explicitRunId) {
    return {
      businessRunKey: cachedBusinessRunKey ?? `explicit-run:${explicitRunId}`,
      runKind: "full",
      explicitRunId,
      candidateId,
      fullIntentId,
    };
  }
  throw new TypeError("channel Full job has no persistent Business Run intent");
}

export class ProxyBusinessRunPreparer {
  constructor({
    queryFn,
    withTransaction,
    incrementalRunStore,
    resolvedPolicy,
    bindingStore = new BusinessRunBindingStore({ withTransaction }),
  } = {}) {
    if (typeof queryFn !== "function") throw new TypeError("queryFn is required");
    this.query = queryFn;
    this.incrementalRunStore = incrementalRunStore;
    this.resolvedPolicy = resolvedPolicy;
    this.bindingStore = bindingStore;
    this.withTransaction = withTransaction;
  }

  async prepareChannel(job) {
    const fields = policyFields(this.resolvedPolicy);
    if (job.queueName === queuesByRole.contentEnrich) {
      const jobId = required(job.id, "job.id");
      const channelId = required(job.data?.channel_id, "job.data.channel_id");
      const tasks = Array.isArray(job.data?.tasks) ? job.data.tasks : [];
      if (tasks.length === 0) throw new TypeError("Content Enrich tasks are required");
      for (const task of tasks) {
        required(task?.task_id, "job.data.tasks[].task_id");
        positiveInteger(task?.dispatch_generation, "job.data.tasks[].dispatch_generation");
      }
      const businessRunId = `content-enrich:${jobId}`;
      if (businessRunId.length > 255) throw new TypeError("Content Enrich Business Run ID is too long");
      return ready({
        businessRunId,
        workloadKind: "content_enrich",
        fields,
        resumed: Number(job.attemptsMade ?? 0) > 0,
        job,
        extra: { channelId },
      });
    }
    if (job.queueName === queuesByRole.channelIncremental) {
      if (!this.incrementalRunStore) throw new TypeError("incrementalRunStore is required");
      const planPayload = await restoreFrozenIncrementalPlan(job, fields);
      const claimed = await this.incrementalRunStore.claim(planPayload);
      if (claimed.terminal) {
        return { kind: "skip", reason: "incremental_run_terminal", result: claimed.run };
      }
      const businessRunKey = `incremental-plan:${required(planPayload.plan_id, "plan_id")}`;
      const resolved = await this.bindingStore.resolve({
        businessRunKey,
        explicitBusinessRunId: claimed.run.run_id,
        requestedStatus: "materialized",
        runKind: "incremental",
        channelId: claimed.run.channel_id,
        planId: String(planPayload.plan_id),
        policy: fields.policy,
        intent: {
          plan_payload_hash: claimed.run?.result_json?.plan_payload_hash ?? null,
          task_mask: planPayload.task_mask ?? {},
          plan_mode: planPayload.plan_mode ?? "standard",
        },
      });
      await this.#freezeRunPolicy(claimed.run.run_id, fields);
      return ready({
        businessRunId: claimed.run.run_id,
        workloadKind: "channel_incremental",
        fields,
        resumed: claimed.resumed,
        job,
        extra: { binding: resolved.binding, businessRunKey, incrementalClaim: claimed },
      });
    }

    if (job.queueName !== queuesByRole.channelCrawl) {
      throw new TypeError(`unsupported Channel queue: ${job.queueName}`);
    }
    const identity = bindingIdentity(job);
    const checkpointRepair = job.name === "channel-checkpoint-repair";
    const checkpointTargetRunId = checkpointRepair
      ? required(job.data?.repair_parent_run_id, "channel-checkpoint-repair repair_parent_run_id")
      : null;
    const checkpointRepairRound = checkpointRepair
      ? positiveInteger(job.data?.repair_round, "channel-checkpoint-repair repair_round")
      : null;
    const publicationGapIntent = publicationGapRepairJobIntent(job.data);
    const channelId = required(job.data?.channel_id, "job.data.channel_id");
    const cachedBusinessRunKey = String(job.data?.business_run_key ?? "").trim();
    if (identity.retryIntentId) {
      const recovery = await this.query(
        `SELECT retry_intent_id,candidate_id,new_business_run_id,new_business_run_key,
                new_job_id,dispatch_generation,status
         FROM crawler.migration_retry_intents
         WHERE retry_intent_id=$1
         LIMIT 1`,
        [identity.retryIntentId],
      );
      const row = recovery.rows[0];
      if (!row
          || Number(row.candidate_id) !== identity.candidateId
          || row.new_business_run_id !== identity.explicitRunId
          || row.new_business_run_key !== identity.businessRunKey
          || row.new_job_id !== String(job.id)
          || Number(row.dispatch_generation) !== Number(job.data?.dispatch_generation)
          || !["requested", "dispatched", "running"].includes(row.status)) {
        throw new TypeError(`Recovery Intent identity mismatch: ${identity.retryIntentId}`);
      }
    }
    const lifecycle = await this.query(
      `SELECT channel.status AS channel_status,channel.removed_reason,
              channel.registry_promotion_run_id,channel.registry_promotion_candidate_id,
              candidate.status AS candidate_status,candidate.reject_reason
       FROM (SELECT $1::text AS channel_id,$2::bigint AS candidate_id) input
       LEFT JOIN crawler.channels channel ON channel.channel_id=input.channel_id
       LEFT JOIN crawler.channel_candidates candidate
         ON candidate.candidate_id=input.candidate_id AND candidate.channel_id=input.channel_id`,
      [channelId, identity.candidateId],
    );
    const state = lifecycle.rows[0] ?? {};
    const repairParentRunId = String(job.data?.repair_parent_run_id ?? "").trim();
    const registryPromotionRunId = String(state.registry_promotion_run_id ?? "").trim();
    const registryPromotionCandidateId = Number(state.registry_promotion_candidate_id);
    if (
      publicationGapIntent?.strategy === "child_run"
      && publicationGapIntent.rootRunId !== registryPromotionRunId
    ) {
      throw new TypeError(
        "Publication Gap Child root_run_id does not match the Channel Promotion Run",
      );
    }
    const promotionRepairContinuation = job.name === "channel-crawl-repair"
      && repairParentRunId
      && repairParentRunId === registryPromotionRunId
      && publicationGapIntent?.strategy !== "child_run"
      && Number.isSafeInteger(registryPromotionCandidateId)
      && registryPromotionCandidateId > 0;
    if (promotionRepairContinuation) {
      identity.explicitRunId = registryPromotionRunId;
      identity.candidateId = registryPromotionCandidateId;
    }
    if (state.channel_status === "removed" && identity.candidateId == null) {
      return { kind: "skip", reason: "channel_removed", result: { channel_id: channelId, skipped: true } };
    }
    if (["rejected", "existing"].includes(state.candidate_status)) {
      return {
        kind: "skip",
        reason: state.reject_reason || `candidate_${state.candidate_status}`,
        result: { channel_id: channelId, candidate_id: identity.candidateId, skipped: true },
      };
    }
    if (!identity.explicitRunId && identity.candidateId != null
        && state.candidate_status === "accepted" && state.registry_promotion_run_id) {
      identity.explicitRunId = String(state.registry_promotion_run_id);
    }
    const explicitExists = identity.explicitRunId
      ? await this.query(
          `SELECT run.run_id,binding.business_run_key,binding.business_run_id,
                  binding.status AS binding_status,binding.identity_policy_id,
                  binding.identity_policy_version,binding.identity_policy_hash
           FROM crawler.channel_runs run
           LEFT JOIN crawler.business_run_bindings binding
             ON binding.business_run_id=run.run_id
           WHERE run.run_id=$1 AND run.channel_id=$2
           LIMIT 1`,
          [identity.explicitRunId, channelId],
        )
      : { rows: [] };
    if (identity.explicitRunId && explicitExists.rows.length === 0
        && job.name !== "channel-full-repair" && !identity.retryIntentId) {
      throw new TypeError(`explicit Channel Run does not exist: ${identity.explicitRunId}`);
    }
    const attached = explicitExists.rows[0]?.business_run_key
      ? explicitExists.rows[0]
      : null;
    if (attached) {
      if (cachedBusinessRunKey
          && cachedBusinessRunKey !== attached.business_run_key
          && !promotionRepairContinuation) {
        throw new TypeError(
          `cached Business Run identity conflicts: ${cachedBusinessRunKey} != ${attached.business_run_key}`,
        );
      }
      this.#assertRowPolicy(attached, fields);
      const binding = {
        business_run_key: attached.business_run_key,
        business_run_id: attached.business_run_id,
        status: attached.binding_status,
      };
      if (binding.status === "terminal") {
        return { kind: "skip", reason: "business_run_terminal", result: { skipped: true } };
      }
      await bindRunToJob(
        job,
        binding.business_run_id,
        binding.business_run_key,
        promotionRepairContinuation
          ? { candidate_id: identity.candidateId }
          : checkpointRepair ? { checkpoint_target_run_id: checkpointTargetRunId } : {},
      );
      await cachePolicyOnJob(job, fields);
      return ready({
        businessRunId: binding.business_run_id,
        workloadKind: "channel_full",
        fields,
        resumed: true,
        job,
        extra: {
          binding,
          businessRunKey: binding.business_run_key,
          ...(checkpointRepair ? { checkpointTargetRunId } : {}),
        },
      });
    }
    if (cachedBusinessRunKey && cachedBusinessRunKey !== identity.businessRunKey) {
      throw new TypeError(
        `cached Business Run identity conflicts: ${cachedBusinessRunKey} != ${identity.businessRunKey}`,
      );
    }
    const resolved = await this.bindingStore.resolve({
      businessRunKey: identity.businessRunKey,
      explicitBusinessRunId: identity.explicitRunId,
      requestedStatus: explicitExists.rows.length > 0 ? "materialized" : "reserved",
      runKind: identity.runKind,
      channelId,
      candidateId: identity.candidateId,
      fullIntentId: identity.fullIntentId,
      policy: fields.policy,
      intent: {
        job_name: required(job.name, "job.name"),
        crawl_mode: String(job.data?.crawl_mode || "full"),
        repair_batch_id: job.data?.repair_batch_id ?? null,
        repair_parent_run_id: job.data?.repair_parent_run_id ?? null,
        repair_round: Number(job.data?.repair_round ?? 0),
        repair_version: job.data?.repair_version ?? job.data?.repair_scan_policy_version ?? null,
        publication_gap_domains: publicationGapIntent?.domains ?? null,
        publication_gap_root_run_id: publicationGapIntent?.rootRunId ?? null,
        publication_gap_scope: publicationGapIntent?.scope ?? null,
        ...(checkpointRepair ? { checkpoint_target_run_id: checkpointTargetRunId } : {}),
      },
    });
    if (resolved.terminal) {
      return { kind: "skip", reason: resolved.binding.terminal_reason, result: { skipped: true } };
    }
    if (checkpointRepair) {
      if (typeof this.withTransaction !== "function") {
        throw new TypeError("checkpoint repair requires withTransaction");
      }
      await this.withTransaction((client) => materializeCheckpointRepairRun(client, {
        repairRunId: resolved.binding.business_run_id,
        targetRunId: checkpointTargetRunId,
        businessRunKey: identity.businessRunKey,
        channelId,
        repairRound: checkpointRepairRound,
        jobId: job.id,
      }));
    }
    await bindRunToJob(
      job,
      resolved.binding.business_run_id,
      identity.businessRunKey,
      checkpointRepair ? { checkpoint_target_run_id: checkpointTargetRunId } : {},
    );
    await cachePolicyOnJob(job, fields);
    return ready({
      businessRunId: resolved.binding.business_run_id,
      workloadKind: "channel_full",
      fields,
      resumed: !resolved.created,
      job,
      extra: {
        binding: resolved.binding,
        businessRunKey: identity.businessRunKey,
        ...(checkpointRepair ? { checkpointTargetRunId } : {}),
      },
    });
  }

  async prepareDiscover(job) {
    const fields = policyFields(this.resolvedPolicy);
    const pageId = required(job.data?.page_id, "job.data.page_id");
    const rows = await this.query("SELECT * FROM crawler.query_pages WHERE page_id=$1 LIMIT 1", [pageId]);
    const page = rows.rows[0];
    if (!page) throw new TypeError(`Discover Page Intent does not exist: ${pageId}`);
    required(page.page_intent_hash, "query_pages.page_intent_hash");
    this.#assertRowPolicy(page, fields);
    if (page.managed_fetch_status === "done" || ["skipped"].includes(page.status)) {
      return {
        kind: "skip",
        reason: "discover_managed_fetch_complete",
        result: { ok: true, page_id: pageId, managed_fetch_complete: true },
      };
    }
    if (page.dispatch_status === "terminal") {
      return { kind: "skip", reason: page.dispatch_reason || "discover_page_terminal", result: null };
    }
    return ready({
      businessRunId: `discover-page:${pageId}`,
      workloadKind: "discover_page",
      fields,
      resumed: page.managed_fetch_status !== "pending",
      job,
      extra: { page },
    });
  }

  async prepareQueryQuality(job) {
    const fields = policyFields(this.resolvedPolicy);
    const chunkId = required(job.data?.quality_chunk_id, "job.data.quality_chunk_id");
    const rows = await this.query(
      `SELECT chunk.*,
              COALESCE(array_agg(member.quality_task_id ORDER BY member.member_ordinal)
                FILTER (WHERE member.quality_task_id IS NOT NULL),'{}'::bigint[]) AS quality_task_ids
       FROM crawler.query_quality_chunks chunk
       LEFT JOIN crawler.query_quality_chunk_members member
         ON member.quality_chunk_id=chunk.quality_chunk_id
       WHERE chunk.quality_chunk_id=$1
       GROUP BY chunk.quality_chunk_id`,
      [chunkId],
    );
    const chunk = rows.rows[0];
    if (!chunk) throw new TypeError(`Query Quality Chunk does not exist: ${chunkId}`);
    required(chunk.chunk_intent_hash, "query_quality_chunks.chunk_intent_hash");
    this.#assertRowPolicy(chunk, fields);
    const storedMembers = (chunk.quality_task_ids ?? []).map(Number);
    const jobMembers = Array.isArray(job.data?.quality_task_ids)
      ? job.data.quality_task_ids.map(Number)
      : null;
    if (jobMembers && (jobMembers.length !== storedMembers.length
        || jobMembers.some((value, index) => value !== storedMembers[index]))) {
      throw new TypeError("Query Quality Chunk members conflict with the BullMQ payload");
    }
    if (["done", "cancelled"].includes(chunk.status)) {
      return { kind: "skip", reason: `query_quality_chunk_${chunk.status}`, result: chunk };
    }
    return ready({
      businessRunId: `query-quality:${chunk.quality_batch_id}:${chunk.quality_chunk_id}`,
      workloadKind: "query_quality_chunk",
      fields,
      resumed: chunk.status !== "pending" && chunk.status !== "queued",
      job,
      extra: { chunk: { ...chunk, quality_task_ids: storedMembers } },
    });
  }

  async #freezeRunPolicy(runId, fields) {
    const updated = await this.query(
      `UPDATE crawler.channel_runs
       SET identity_policy_id=COALESCE(identity_policy_id,$2),
           identity_policy_version=COALESCE(identity_policy_version,$3),
           identity_policy_hash=COALESCE(identity_policy_hash,$4),updated_at=now()
       WHERE run_id=$1
         AND (identity_policy_id IS NULL OR identity_policy_id=$2)
         AND (identity_policy_version IS NULL OR identity_policy_version=$3)
         AND (identity_policy_hash IS NULL OR identity_policy_hash=$4)
       RETURNING run_id`,
      [runId, fields.identityPolicyId, fields.identityPolicyVersion, fields.identityPolicyHash],
    );
    if (updated.rowCount !== 1) throw new TypeError(`Channel Run Identity Policy conflict: ${runId}`);
  }

  #assertRowPolicy(row, fields) {
    if (row.identity_policy_id !== fields.identityPolicyId
        || Number(row.identity_policy_version) !== fields.identityPolicyVersion
        || row.identity_policy_hash !== fields.identityPolicyHash) {
      const error = new Error("business Intent is assigned to a different Identity Policy");
      error.code = "POLICY_UNAVAILABLE";
      error.retryAt = retryAt();
      throw error;
    }
  }
}
