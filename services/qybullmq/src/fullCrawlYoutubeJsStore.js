import { activeChannelCandidateAttemptFence } from "./channelCandidateAttemptFence.js";
import {
  beginChannelCandidateValidation,
  lockChannelCandidateAttempt,
  markChannelCandidateAlreadyPromoted,
  recordAcceptedChannelCandidateSnapshot,
  rejectChannelCandidateAdmission,
} from "./channelCandidateAttemptMutations.js";
import { claimChannelRegistryPromotion } from "./channelRegistryPromotion.js";
import { markChannelRemoved } from "./channelLifecycle.js";
import { prepareChannelRun } from "./channelRunBinding.js";
import {
  materializeBusinessRunBinding,
  terminateBusinessRunBinding,
} from "./businessRunBindingStore.js";
import {
  claimContentDetailExecution,
  lockContentDetailExecution,
} from "./contentDetailExecutionFence.js";
import {
  assertSameFullCrawlFetchContract,
  readFullCrawlFetchContractFromIntent,
  isYoutubeJsFullCrawlFetchContract,
  normalizeFullCrawlFetchContract,
  fullCrawlFetchContractId,
} from "./fullCrawlFetchContract.js";
import {
  fullCrawlTargetHash,
  fullCrawlUploadsHash,
  normalizeFullCrawlTargets,
} from "./fullCrawlYoutubeJsModel.js";
import {
  fullVideoStorageAction,
  updateExistingFullVideoAccess,
  upsertFullVideoContent,
} from "./fullVideoContentStore.js";
import { reconcileFullCrawlAgentState } from "./fullCrawlAgentState.js";
import { reconcileRunDetailStatus } from "./runDetailStatus.js";
import { resolveYoutubeContentType } from "./youtubeContentType.js";
import { resolveVideoDisposition } from "./videoDisposition.js";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function positiveInteger(value, field) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return number;
}

function boundedInteger(value, fallback, min, max) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function objectValue(value) {
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function jobIdentity(job) {
  const data = job?.data ?? {};
  if (!isYoutubeJsFullCrawlFetchContract(data.fetch_contract)) {
    throw new TypeError("Full Crawl Store requires a supported YouTubeJS contract");
  }
  return Object.freeze({
    fetchContract: normalizeFullCrawlFetchContract(data.fetch_contract),
    channelId: text(data.channel_id) ?? (() => { throw new TypeError("channel_id is required"); })(),
    candidateId: positiveInteger(data.candidate_id, "candidate_id"),
    runId: text(data.run_id) ?? (() => { throw new TypeError("run_id is required"); })(),
    businessRunKey: text(data.business_run_key)
      ?? (() => { throw new TypeError("business_run_key is required"); })(),
    dispatchBatchId: text(data.dispatch_batch_id) ?? text(data.pipeline_cycle_id),
    candidateAttemptFence: activeChannelCandidateAttemptFence(job),
  });
}

export class FullCrawlYoutubeJsCheckpointError extends Error {
  constructor(message, { runId = null, phase = null, cause = null } = {}) {
    super(message, cause == null ? undefined : { cause });
    this.name = "FullCrawlYoutubeJsCheckpointError";
    this.code = "FULL_CRAWL_YOUTUBEJS_CHECKPOINT_CONFLICT";
    this.run_id = runId;
    this.phase = phase;
  }
}

export class FullCrawlYoutubeJsExecutionStaleError extends Error {
  constructor(runId) {
    super(`Full Crawl YouTubeJS execution Fence is stale: ${runId}`);
    this.name = "FullCrawlYoutubeJsExecutionStaleError";
    this.code = "CONTENT_DETAIL_EXECUTION_FENCE_STALE";
    this.run_id = runId;
  }
}

function checkpointError(message, identity, phase = null) {
  return new FullCrawlYoutubeJsCheckpointError(message, {
    runId: identity?.runId ?? null,
    phase,
  });
}

function targetFromCandidate(row) {
  const stored = objectValue(row?.result_json).full_crawl_target;
  if (!stored || typeof stored !== "object" || Array.isArray(stored)) {
    throw new FullCrawlYoutubeJsCheckpointError(
      `Full Crawl Candidate target evidence is missing: ${row?.candidate_id ?? "unknown"}`,
      { runId: row?.run_id ?? null, phase: "uploads" },
    );
  }
  const target = normalizeFullCrawlTargets([stored])[0];
  if (target.video_id !== text(row.source_content_id)
      || target.position !== Number(row.position)
      || target.source_url !== text(row.source_url)) {
    throw new FullCrawlYoutubeJsCheckpointError(
      `Full Crawl Candidate target evidence conflicts: ${row.candidate_id}`,
      { runId: row.run_id, phase: "uploads" },
    );
  }
  return target;
}

function assertUploadsCheckpoint(run, candidates, identity) {
  const result = objectValue(run?.result_json);
  const receipt = objectValue(result.full_crawl).uploads;
  if (!receipt) {
    if (candidates.length > 0) {
      throw checkpointError("Full Crawl Candidates exist without an Uploads receipt", identity, "uploads");
    }
    return null;
  }
  const document = objectValue(receipt.document);
  if (document.version !== 1 || !Array.isArray(document.entries)) {
    throw checkpointError("Full Crawl Uploads document is malformed", identity, "uploads");
  }
  let uploadsHash;
  let documentTargetHash;
  try {
    uploadsHash = fullCrawlUploadsHash(document);
    documentTargetHash = fullCrawlTargetHash(document.entries);
  } catch (cause) {
    throw new FullCrawlYoutubeJsCheckpointError(
      "Full Crawl Uploads document cannot be canonicalized",
      { runId: identity?.runId ?? null, phase: "uploads", cause },
    );
  }
  if (uploadsHash !== text(receipt.uploads_hash)) {
    throw checkpointError("Full Crawl Uploads hash conflicts with its receipt", identity, "uploads");
  }
  const targets = candidates.map(targetFromCandidate);
  const targetHash = fullCrawlTargetHash(targets);
  if (documentTargetHash !== text(receipt.target_hash)
      || targetHash !== text(receipt.target_hash)
      || candidates.length !== Number(receipt.selected_count)) {
    throw checkpointError("Full Crawl target set conflicts with its receipt", identity, "uploads");
  }
  const apiCandidate = candidates.find((candidate) => candidate.api_status !== "not_needed");
  if (apiCandidate) {
    throw checkpointError(
      `YouTubeJS Full Crawl cannot contain Data API work: ${apiCandidate.candidate_id}`,
      identity,
      "detail",
    );
  }
  return { receipt, document, targets };
}

function assertFetchCheckpoint(run, candidates, uploads, identity) {
  const fetch = objectValue(objectValue(run?.result_json).full_crawl).fetch;
  if (!fetch || fetch.status !== "complete") return null;
  if (!uploads) {
    throw checkpointError("Full Crawl fetch receipt exists without Uploads", identity, "close_fetch");
  }
  const open = candidates.find((candidate) => (
    !["done", "unavailable"].includes(candidate.detail_status)
    || candidate.disposition == null
  ));
  if (run.detail_status !== "done" || open) {
    throw checkpointError("Full Crawl fetch receipt conflicts with unfinished Detail work", identity, "close_fetch");
  }
  if (text(fetch.uploads_hash) !== text(uploads?.receipt?.uploads_hash)
      || text(fetch.target_hash) !== text(uploads?.receipt?.target_hash)
      || Number(fetch.selected_count) !== Number(uploads.receipt.selected_count)) {
    throw checkpointError("Full Crawl fetch receipt hashes conflict with Uploads", identity, "close_fetch");
  }
  return fetch;
}

function checkpointPhase({ binding, candidate, channel, run, candidates, uploads, fetch }) {
  if (binding.status === "terminal") return "terminal";
  if (["rejected", "existing"].includes(candidate.status)) return "terminal";
  if (channel?.status === "removed") return "terminal";
  if (!run) return channel && candidate.status !== "accepted" ? "existing" : "admission";
  if (!uploads) return "uploads";
  if (fetch) return "handoff";
  const open = candidates.some((row) => !["done", "unavailable"].includes(row.detail_status));
  return open ? "detail" : "close_fetch";
}

function terminalResult(identity, { binding, candidate, channel }) {
  const reason = binding.status === "terminal"
    ? binding.terminal_reason ?? "business_run_terminal"
    : channel?.status === "removed"
      ? channel.removed_reason ?? "channel_removed"
      : candidate.reject_reason ?? `candidate_${candidate.status}`;
  return {
    ok: true,
    skipped: true,
    skip_reason: reason,
    channel_id: identity.channelId,
    candidate_id: identity.candidateId,
    run_id: null,
    candidate_count: 0,
    fetch_contract: fullCrawlFetchContractId(identity.fetchContract),
  };
}

function checkpointState(identity, rows) {
  const { binding, candidate, channel, run, candidates } = rows;
  if (!binding) throw checkpointError("Full Crawl Business Run binding is missing", identity);
  if (binding.business_run_id !== identity.runId
      || binding.channel_id !== identity.channelId
      || Number(binding.candidate_id) !== identity.candidateId
      || binding.run_kind !== "full") {
    throw checkpointError("Full Crawl Business Run identity conflicts", identity);
  }
  const bindingContract = readFullCrawlFetchContractFromIntent(binding.intent_json);
  if (!bindingContract.explicit
      || !isYoutubeJsFullCrawlFetchContract(bindingContract.contract)) {
    throw checkpointError("Business Run is not bound to a supported YouTubeJS contract", identity);
  }
  assertSameFullCrawlFetchContract(bindingContract.contract, identity.fetchContract);
  if (!candidate
      || Number(candidate.candidate_id) !== identity.candidateId
      || candidate.channel_id !== identity.channelId) {
    throw checkpointError("Full Crawl Channel Candidate identity conflicts", identity);
  }
  if (run) {
    if (run.run_id !== identity.runId
        || run.channel_id !== identity.channelId
        || Number(run.candidate_id) !== identity.candidateId
        || run.crawl_mode !== "full") {
      throw checkpointError("Full Crawl Channel Run identity conflicts", identity);
    }
    assertSameFullCrawlFetchContract(
      bindingContract.contract,
      objectValue(run.result_json).fetch_contract,
    );
    if (channel?.registry_promotion_run_id !== identity.runId
        || Number(channel?.registry_promotion_candidate_id) !== identity.candidateId) {
      throw checkpointError("Full Crawl Channel Registry promotion conflicts", identity);
    }
  } else if (candidates.length > 0) {
    throw checkpointError("Full Crawl Detail Candidates exist without a Channel Run", identity);
  }
  const uploads = run ? assertUploadsCheckpoint(run, candidates, identity) : null;
  const fetch = run ? assertFetchCheckpoint(run, candidates, uploads, identity) : null;
  const phase = checkpointPhase({ binding, candidate, channel, run, candidates, uploads, fetch });
  return Object.freeze({
    identity,
    binding,
    candidate,
    channel,
    run,
    candidates,
    uploads,
    fetch,
    phase,
    terminalResult: phase === "terminal" ? terminalResult(identity, rows) : null,
  });
}

function settingsFallback(environment) {
  return {
    minSubscriberCount: boundedInteger(environment.MIN_SUBSCRIBER_COUNT, 1000, 0, 1_000_000_000),
    channelContentLimit: boundedInteger(environment.YOUTUBE_CHANNEL_CONTENT_LIMIT, 30, 1, 100),
    contentMaxAgeDays: boundedInteger(environment.YOUTUBE_CONTENT_MAX_AGE_DAYS, 90, 0, 3650),
  };
}

export class FullCrawlYoutubeJsStore {
  constructor({ query, withTransaction, environment = process.env } = {}) {
    if (typeof query !== "function") throw new TypeError("query is required");
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.query = query;
    this.withTransaction = withTransaction;
    this.environment = environment;
  }

  async loadSettings() {
    const fallback = settingsFallback(this.environment);
    try {
      const result = await this.query(
        "SELECT value_json FROM crawler.settings WHERE setting_key='crawl' LIMIT 1",
      );
      const value = objectValue(result.rows[0]?.value_json);
      return Object.freeze({
        minSubscriberCount: boundedInteger(
          value.min_subscriber_count,
          fallback.minSubscriberCount,
          0,
          1_000_000_000,
        ),
        channelContentLimit: boundedInteger(
          value.channel_content_limit,
          fallback.channelContentLimit,
          1,
          100,
        ),
        contentMaxAgeDays: boundedInteger(
          value.content_max_age_days,
          fallback.contentMaxAgeDays,
          0,
          3650,
        ),
      });
    } catch {
      return Object.freeze(fallback);
    }
  }

  async restore(job) {
    const identity = jobIdentity(job);
    const rows = await this.withTransaction(async (client) => {
      const bindingRows = await client.query(
        `SELECT * FROM crawler.business_run_bindings
         WHERE business_run_key=$1 AND business_run_id=$2
         FOR SHARE`,
        [identity.businessRunKey, identity.runId],
      );
      const candidateRows = await client.query(
        "SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 AND channel_id=$2 FOR SHARE",
        [identity.candidateId, identity.channelId],
      );
      const channelRows = await client.query(
        "SELECT * FROM crawler.channels WHERE channel_id=$1 FOR SHARE",
        [identity.channelId],
      );
      const runRows = await client.query(
        "SELECT * FROM crawler.channel_runs WHERE run_id=$1 AND channel_id=$2 FOR SHARE",
        [identity.runId, identity.channelId],
      );
      const contentRows = runRows.rowCount === 0
        ? { rows: [] }
        : await client.query(
            "SELECT * FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position,candidate_id",
            [identity.runId],
          );
      return {
        binding: bindingRows.rows[0] ?? null,
        candidate: candidateRows.rows[0] ?? null,
        channel: channelRows.rows[0] ?? null,
        run: runRows.rows[0] ?? null,
        candidates: contentRows.rows,
      };
    });
    return checkpointState(identity, rows);
  }

  async beginAdmission(job) {
    const identity = jobIdentity(job);
    return beginChannelCandidateValidation(this.query, identity.candidateAttemptFence);
  }

  async rejectAdmission(job, { reason, sourceJson }) {
    const identity = jobIdentity(job);
    return this.withTransaction(async (client) => {
      await rejectChannelCandidateAdmission(
        client.query.bind(client),
        identity.candidateAttemptFence,
        { reason, sourceJson },
      );
      await terminateBusinessRunBinding(client, {
        businessRunKey: identity.businessRunKey,
        businessRunId: identity.runId,
        reason,
      });
      return terminalResult(identity, {
        binding: { status: "terminal", terminal_reason: reason },
        candidate: { status: "rejected", reject_reason: reason },
        channel: null,
      });
    });
  }

  async settleExistingChannel(job) {
    const identity = jobIdentity(job);
    return this.withTransaction(async (client) => {
      await markChannelCandidateAlreadyPromoted(
        client.query.bind(client),
        identity.candidateAttemptFence,
      );
      await terminateBusinessRunBinding(client, {
        businessRunKey: identity.businessRunKey,
        businessRunId: identity.runId,
        reason: "channel_already_promoted",
      });
      return terminalResult(identity, {
        binding: { status: "terminal", terminal_reason: "channel_already_promoted" },
        candidate: { status: "existing", reject_reason: "channel_already_promoted" },
        channel: null,
      });
    });
  }

  async settleTerminalCheckpoint(job, { reason }) {
    const identity = jobIdentity(job);
    return this.withTransaction(async (client) => terminateBusinessRunBinding(client, {
      businessRunKey: identity.businessRunKey,
      businessRunId: identity.runId,
      reason,
    }));
  }

  async settleTerminalChannel(job, terminal, observedAt = new Date()) {
    const identity = jobIdentity(job);
    return this.withTransaction(async (client) => {
      await markChannelRemoved(client, {
        channelId: identity.channelId,
        candidateId: identity.candidateId,
        candidateAttemptFence: identity.candidateAttemptFence,
        terminal,
        observedAt,
      });
      await terminateBusinessRunBinding(client, {
        businessRunKey: identity.businessRunKey,
        businessRunId: identity.runId,
        reason: terminal.removed_reason ?? "channel_removed",
      });
      return terminalResult(identity, {
        binding: { status: "terminal", terminal_reason: terminal.removed_reason },
        candidate: { status: "rejected", reject_reason: terminal.removed_reason },
        channel: { status: "removed", removed_reason: terminal.removed_reason },
      });
    });
  }

  async commitAdmission(job, {
    metadata,
    sourceJson,
    aboutObservation,
    observedAt,
    settings,
  }) {
    const identity = jobIdentity(job);
    return this.withTransaction(async (client) => {
      await lockChannelCandidateAttempt(
        client.query.bind(client),
        identity.candidateAttemptFence,
      );
      const candidateRows = await client.query(
        "SELECT * FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE",
        [identity.candidateId],
      );
      const candidate = candidateRows.rows[0];
      if (!candidate || candidate.channel_id !== identity.channelId) {
        throw checkpointError("Channel Candidate disappeared during Admission", identity, "admission");
      }

      let promotion;
      if (candidate.status === "accepted") {
        const updated = await client.query(
          `UPDATE crawler.channels
           SET channel_url=COALESCE($2,channel_url),handle=COALESCE($3,handle),
               title=COALESCE($4,title),country=COALESCE($5,country),
               country_source=CASE WHEN $5::text IS NULL THEN country_source ELSE 'youtube_about' END,
               country_code=COALESCE($6,country_code),
               country_canonical_name=COALESCE($7,country_canonical_name),
               subscriber_count=COALESCE($8::bigint,subscriber_count),
               subscriber_count_text=COALESCE($9,subscriber_count_text),
               status='active',reject_reason=NULL,
               dormant_reason=NULL,dormant_since=NULL,dormant_recheck_day=NULL,
               dormant_last_probe_at=NULL,dormant_cycle=0,
               source_json=source_json || $10::jsonb,updated_at=now()
           WHERE channel_id=$1 AND registry_promotion_candidate_id=$11
             AND registry_promotion_run_id=$12
           RETURNING channel_id`,
          [
            identity.channelId,
            metadata.channel_url,
            metadata.handle,
            metadata.title,
            metadata.country,
            metadata.country_code,
            metadata.country_canonical_name,
            metadata.subscriber_count,
            metadata.subscriber_count_text,
            JSON.stringify(sourceJson),
            identity.candidateId,
            identity.runId,
          ],
        );
        if (updated.rowCount !== 1) {
          throw checkpointError("Accepted Candidate promotion conflicts during Admission", identity, "admission");
        }
        await recordAcceptedChannelCandidateSnapshot(
          client.query.bind(client),
          identity.candidateAttemptFence,
          { sourceJson },
        );
        promotion = { promoted: true, status: "promoted" };
      } else {
        promotion = await claimChannelRegistryPromotion(client, {
          candidateId: identity.candidateId,
          runId: identity.runId,
          channelId: identity.channelId,
          channelUrl: metadata.channel_url,
          handle: metadata.handle,
          title: metadata.title,
          country: metadata.country,
          countryCode: metadata.country_code,
          countryCanonicalName: metadata.country_canonical_name,
          subscriberCount: metadata.subscriber_count,
          subscriberCountText: metadata.subscriber_count_text,
          readyForAgent: false,
          sourceJson,
          candidateAttemptFence: identity.candidateAttemptFence,
        });
      }
      if (!promotion.promoted) {
        await terminateBusinessRunBinding(client, {
          businessRunKey: identity.businessRunKey,
          businessRunId: identity.runId,
          reason: "channel_already_promoted",
        });
        return { committed: false, existing: true };
      }

      await reconcileFullCrawlAgentState(client.query.bind(client), {
        channelId: identity.channelId,
        eligible: false,
      });
      const migrationGateRequired = job.data?.reject_if_no_recent_content === true;
      const runResult = {
        job_id: String(job.id),
        pipeline_cycle_id: text(job.data?.pipeline_cycle_id),
        dispatch_batch_id: identity.dispatchBatchId,
        candidate_id: identity.candidateId,
        query_id: job.data?.query_id ?? null,
        query_text: text(job.data?.query_text),
        content_max_age_days: settings.contentMaxAgeDays,
        migration_activity_gate: {
          required: migrationGateRequired,
          decision: migrationGateRequired ? "pending" : "not_required",
          max_age_days: settings.contentMaxAgeDays,
        },
        full_crawl: {
          admission: {
            status: "committed",
            observed_at: new Date(observedAt).toISOString(),
            source: "youtubejs",
          },
        },
        ...(aboutObservation == null
          ? {}
          : { pending_initial_about_observation: aboutObservation }),
      };
      await prepareChannelRun(client, {
        runId: identity.runId,
        channelId: identity.channelId,
        candidateId: identity.candidateId,
        crawlMode: "full",
        contentLimit: settings.channelContentLimit,
        resultJson: runResult,
      });
      await materializeBusinessRunBinding(client, {
        businessRunKey: identity.businessRunKey,
        businessRunId: identity.runId,
      });
      return { committed: true, existing: false };
    });
  }

  async commitUploads(job, {
    document,
    targets,
    activityEvidence,
    observedAt,
  }) {
    const identity = jobIdentity(job);
    const normalizedTargets = normalizeFullCrawlTargets(targets);
    const uploadsHash = fullCrawlUploadsHash(document);
    const targetHash = fullCrawlTargetHash(normalizedTargets);
    if (document?.version !== 1
        || !Array.isArray(document.entries)
        || fullCrawlTargetHash(document.entries) !== targetHash) {
      throw checkpointError(
        "Uploads document and selected target set conflict",
        identity,
        "uploads",
      );
    }
    return this.withTransaction(async (client) => {
      await lockChannelCandidateAttempt(
        client.query.bind(client),
        identity.candidateAttemptFence,
      );
      const runRows = await client.query(
        "SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE",
        [identity.runId],
      );
      const run = runRows.rows[0];
      if (!run || run.channel_id !== identity.channelId) {
        throw checkpointError("Channel Run disappeared during Uploads", identity, "uploads");
      }
      assertSameFullCrawlFetchContract(
        identity.fetchContract,
        objectValue(run.result_json).fetch_contract,
      );
      const existingCandidates = await client.query(
        "SELECT * FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position,candidate_id FOR UPDATE",
        [identity.runId],
      );
      const existingReceipt = objectValue(objectValue(run.result_json).full_crawl).uploads;
      if (existingReceipt) {
        const checkpoint = assertUploadsCheckpoint(
          run,
          existingCandidates.rows,
          identity,
        );
        if (checkpoint.receipt.uploads_hash !== uploadsHash
            || checkpoint.receipt.target_hash !== targetHash) {
          throw checkpointError("Uploads replay changed the frozen target set", identity, "uploads");
        }
        return { committed: false, uploadsHash, targetHash };
      }
      if (existingCandidates.rowCount > 0) {
        throw checkpointError("Candidates exist before the Uploads checkpoint", identity, "uploads");
      }

      await client.query(
        "UPDATE crawler.contents SET is_recent=false WHERE channel_id=$1",
        [identity.channelId],
      );
      for (const target of normalizedTargets) {
        const discoveryClassification = resolveYoutubeContentType({
          videoId: target.video_id,
          upload: target,
        });
        await client.query(
          `INSERT INTO crawler.content_candidates (
             run_id,channel_id,source_content_id,position,title,source_url,thumbnail_url,
             content_type,type_status,type_source,detail_status,api_status,result_json,updated_at
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,NULL,'unresolved',NULL,'queued','not_needed',$8::jsonb,now())`,
          [
            identity.runId,
            identity.channelId,
            target.video_id,
            target.position,
            target.title,
            target.source_url,
            target.thumbnail_url,
            JSON.stringify({
              flat: target,
              full_crawl_target: target,
              discovery_classification: discoveryClassification,
            }),
          ],
        );
      }
      const initialEvidence = activityEvidence == null ? null : {
        evidence_complete: activityEvidence.evidenceComplete === true,
        decision: activityEvidence.decision,
        max_age_days: activityEvidence.maxAgeDays,
        reference_day: activityEvidence.referenceDay,
        reference_at: activityEvidence.referenceAt,
        recent_published_content_count: activityEvidence.recentPublishedContentCount,
        uncertain_content_count: activityEvidence.uncertainContentCount,
        inspected_content_count: activityEvidence.inspectedContentCount,
        excluded_upcoming_count: activityEvidence.excludedUpcomingCount,
        newest_published_day: activityEvidence.newestPublishedDay,
        classifier_version: activityEvidence.classifierVersion,
        policy_version: activityEvidence.policyVersion,
        relation_counts: activityEvidence.relationCounts,
        unresolved_by_status_counts: activityEvidence.unresolvedByStatusCounts,
      };
      const receipt = {
        outcome: Number(document.parse_gap_count ?? 0) === 0 ? "complete" : "partial",
        uploads_hash: uploadsHash,
        target_hash: targetHash,
        selected_count: normalizedTargets.length,
        evidence_complete: document.activity_evidence_complete === true,
        observed_at: new Date(observedAt).toISOString(),
        document,
      };
      await client.query(
        `UPDATE crawler.channel_runs
         SET status='waiting_detail',detail_status=$2,expected_content_count=$3,
             result_json=jsonb_set(
               COALESCE(result_json,'{}'::jsonb),
               '{full_crawl,uploads}',
               $4::jsonb,
               true
             ) || CASE
               WHEN $5::jsonb IS NULL THEN '{}'::jsonb
               ELSE jsonb_build_object('migration_activity_initial_evidence',$5::jsonb)
             END,
             updated_at=now()
         WHERE run_id=$1`,
        [
          identity.runId,
          normalizedTargets.length > 0 ? "queued" : "done",
          normalizedTargets.length,
          JSON.stringify(receipt),
          initialEvidence == null ? null : JSON.stringify(initialEvidence),
        ],
      );
      return { committed: true, uploadsHash, targetHash, receipt };
    });
  }

  async claimDetailExecution(fence) {
    return this.withTransaction(async (client) => {
      const claimed = await claimContentDetailExecution(client, fence);
      if (!claimed) return null;
      await client.query(
        `UPDATE crawler.content_candidates
         SET detail_status='queued',
             result_json=result_json || jsonb_build_object(
               'full_crawl_recovered_at',now()
             ),
             updated_at=now()
         WHERE run_id=$1 AND detail_status='running'`,
        [fence.runId],
      );
      return claimed;
    });
  }

  async claimNextDetail(fence) {
    return this.withTransaction(async (client) => {
      if (!(await lockContentDetailExecution(client, fence))) {
        throw new FullCrawlYoutubeJsExecutionStaleError(fence.runId);
      }
      const selected = await client.query(
        `SELECT candidate.*,
                known.content_key AS known_content_key,
                known.content_type AS known_content_type,
                known.content_type_source AS known_content_type_source,
                run.started_at AS crawl_started_at,
                run.result_json->>'content_max_age_days' AS content_max_age_days
         FROM crawler.content_candidates candidate
         JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
         LEFT JOIN LATERAL (
           SELECT content.content_key,content.content_type,content.content_type_source
           FROM crawler.contents content
           WHERE content.channel_id=candidate.channel_id
             AND content.source_content_id=candidate.source_content_id
           ORDER BY content.last_seen_at DESC NULLS LAST
           LIMIT 1
         ) known ON true
         WHERE candidate.run_id=$1 AND candidate.detail_status IN ('queued','failed')
         ORDER BY candidate.position,candidate.candidate_id
         LIMIT 1
         FOR UPDATE OF candidate`,
        [fence.runId],
      );
      const row = selected.rows[0];
      if (!row) return null;
      const claimed = await client.query(
        `UPDATE crawler.content_candidates
         SET detail_status='running',attempts=attempts+1,error_message=NULL,updated_at=now()
         WHERE candidate_id=$1 AND detail_status IN ('queued','failed')
         RETURNING *`,
        [row.candidate_id],
      );
      if (claimed.rowCount !== 1) {
        throw checkpointError("Detail Candidate claim conflicted", { runId: fence.runId }, "detail");
      }
      return { ...row, ...claimed.rows[0], target: targetFromCandidate(claimed.rows[0]) };
    });
  }

  async commitDetail(fence, row, {
    detail,
    access,
    classification,
    terminalReason = null,
    window = null,
    observedAt,
    locale,
  }) {
    return this.withTransaction(async (client) => {
      if (!(await lockContentDetailExecution(client, fence))) {
        throw new FullCrawlYoutubeJsExecutionStaleError(fence.runId);
      }
      const lockedRows = await client.query(
        "SELECT * FROM crawler.content_candidates WHERE candidate_id=$1 AND run_id=$2 FOR UPDATE",
        [row.candidate_id, fence.runId],
      );
      const locked = lockedRows.rows[0];
      if (!locked || locked.detail_status !== "running") {
        throw checkpointError(`Detail Candidate commit conflicted: ${row.candidate_id}`, {
          runId: fence.runId,
        }, "detail");
      }
      const authoritative = classification?.authoritative === true;
      const storageCandidate = {
        ...locked,
        known_content_key: row.known_content_key ?? null,
        known_content_type: row.known_content_type ?? null,
        known_content_type_source: row.known_content_type_source ?? null,
        content_type: authoritative ? classification.content_type : null,
        type_source: authoritative ? classification.source : null,
        type_authoritative: authoritative,
      };
      const storageAction = terminalReason
        ? { kind: "unresolved" }
        : fullVideoStorageAction({ candidate: storageCandidate, classification, access });
      const state = { detail, access, classification };
      let contentKey = null;
      if (storageAction.kind === "update_access") {
        contentKey = (await updateExistingFullVideoAccess(client, {
          candidate: storageCandidate,
          state,
          access,
        }))?.content_key ?? storageAction.content_key;
      } else if (storageAction.kind === "upsert") {
        contentKey = await upsertFullVideoContent(client, {
          candidate: storageCandidate,
          state,
          access,
          locale,
        });
      }
      const disposition = resolveVideoDisposition({
        storageAction,
        classification,
        access,
        detail,
        terminalReason,
        priorDisposition: objectValue(locked.result_json).disposition,
        observedAt,
      });
      const excluded = disposition.kind === "terminal_excluded";
      const scope = excluded
        ? {
            status: "excluded",
            reason: terminalReason ?? disposition.reason_code,
            source: terminalReason ? "youtubejs_uploads_or_player" : "youtubejs_player",
            ...(window == null
              ? {}
              : {
                  relation: window.relation,
                  relation_reason_code: window.reason_code,
                  classifier_version: window.classifier_version,
                  basis: window.basis,
                  precision: window.precision,
                  evidence_source: window.source,
                }),
          }
        : { status: disposition.kind === "deferred" ? "deferred" : "included" };
      const terminalLive = ["upcoming_live", "live_in_progress"].includes(terminalReason);
      const contentType = authoritative
        ? classification.content_type
        : terminalLive ? "live" : locked.content_type;
      const typeStatus = authoritative || terminalLive
        ? "resolved"
        : excluded ? "unavailable" : "unresolved";
      const typeSource = authoritative
        ? classification.source
        : terminalLive ? text(row.target?.type_source) ?? "youtubejs_uploads_live_flag" : locked.type_source;
      const detailStatus = ["private", "unavailable"].includes(access?.access_status)
        ? "unavailable"
        : "done";
      const resultJson = {
        ...objectValue(locked.result_json),
        detail,
        access,
        classification,
        scope,
        disposition,
        extractor: {
          source: "youtubejs",
          client: text(detail?.youtubejs_client),
          version: text(detail?.extractor_version),
          request_count: Number(detail?.youtubejs_request_count ?? 0),
        },
        full_crawl_detail: {
          status: "committed",
          observed_at: new Date(observedAt).toISOString(),
          terminal_reason: terminalReason,
        },
      };
      const updated = await client.query(
        `UPDATE crawler.content_candidates
         SET content_type=$2,type_status=$3,type_source=$4,content_key=$5,
             detail_status=$6,api_status='not_needed',missing_fields='{}'::text[],
             result_json=$7::jsonb,error_message=NULL,disposition=$8,next_attempt_at=$9,
             finished_at=now(),updated_at=now()
         WHERE candidate_id=$1 AND run_id=$10 AND detail_status='running'
         RETURNING candidate_id`,
        [
          locked.candidate_id,
          contentType,
          typeStatus,
          typeSource,
          contentKey,
          detailStatus,
          JSON.stringify(resultJson),
          disposition.kind,
          disposition.next_attempt_at,
          fence.runId,
        ],
      );
      if (updated.rowCount !== 1) {
        throw checkpointError(`Detail Candidate CAS failed: ${locked.candidate_id}`, {
          runId: fence.runId,
        }, "detail");
      }
      return { candidateId: Number(locked.candidate_id), disposition, contentKey };
    });
  }

  async closeFetch(fence, { completedAt }) {
    return this.withTransaction(async (client) => {
      if (!(await lockContentDetailExecution(client, fence))) {
        throw new FullCrawlYoutubeJsExecutionStaleError(fence.runId);
      }
      const runRows = await client.query(
        "SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE",
        [fence.runId],
      );
      const run = runRows.rows[0];
      const candidateRows = await client.query(
        "SELECT * FROM crawler.content_candidates WHERE run_id=$1 ORDER BY position,candidate_id FOR UPDATE",
        [fence.runId],
      );
      const identity = { runId: fence.runId };
      const uploads = assertUploadsCheckpoint(run, candidateRows.rows, identity);
      if (!uploads) throw checkpointError("Close Fetch requires an Uploads checkpoint", identity, "close_fetch");
      const open = candidateRows.rows.find((candidate) => (
        !["done", "unavailable"].includes(candidate.detail_status)
        || candidate.disposition == null
      ));
      if (open) {
        throw checkpointError(`Close Fetch found unfinished Candidate: ${open.candidate_id}`, identity, "close_fetch");
      }
      const detail = await reconcileRunDetailStatus(client, fence.runId);
      if (detail.status !== "done") {
        throw checkpointError(`Close Fetch Detail status is ${detail.status}`, identity, "close_fetch");
      }
      const migrationActivity = detail.migration_activity_gate;
      if (migrationActivity?.decision === "not_required") {
        await reconcileFullCrawlAgentState(client.query.bind(client), {
          channelId: fence.channelId,
          eligible: true,
        });
      }
      const counts = candidateRows.rows.reduce((output, candidate) => {
        const kind = candidate.disposition;
        if (kind === "stored") output.stored += 1;
        else if (kind === "terminal_excluded") output.excluded += 1;
        else if (kind === "deferred") output.deferred += 1;
        return output;
      }, { stored: 0, excluded: 0, deferred: 0 });
      const receipt = {
        status: "complete",
        completed_at: new Date(completedAt).toISOString(),
        uploads_hash: uploads.receipt.uploads_hash,
        target_hash: uploads.receipt.target_hash,
        selected_count: candidateRows.rowCount,
        stored_count: counts.stored,
        excluded_count: counts.excluded,
        deferred_count: counts.deferred,
      };
      await client.query(
        `UPDATE crawler.channel_runs
         SET result_json=jsonb_set(
               COALESCE(result_json,'{}'::jsonb),
               '{full_crawl,fetch}',
               $2::jsonb,
               true
             ),updated_at=now()
         WHERE run_id=$1`,
        [fence.runId, JSON.stringify(receipt)],
      );
      return {
        receipt,
        detail,
        migrationActivity,
        candidateCount: candidateRows.rowCount,
        ...counts,
      };
    });
  }
}
