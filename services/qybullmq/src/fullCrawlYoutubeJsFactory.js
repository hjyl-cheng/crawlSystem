import { buildAboutObservation } from "./aboutObservation.js";
import { parseYoutubeAboutCountry } from "./agentCountryPolicy.js";
import { currentChannelExecution, currentChannelExecutionAbortSignal } from "./channelExecutionContext.js";
import { evaluateChannelQualification } from "./channelQualification.js";
import { classifyTerminalChannelError } from "./channelLifecycle.js";
import { contentDetailExecutionFence } from "./contentDetailExecutionFence.js";
import { classifyContentWindow } from "./contentWindow.js";
import {
  isLiveInProgress,
  isUpcomingLiveDetail,
} from "./detailPolicy.js";
import {
  fullCrawlFetchContractId,
  isYoutubeJsFullCrawlFetchContract,
  YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID,
  YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT_ID,
} from "./fullCrawlFetchContract.js";
import {
  classifyFullCrawlTargetBeforeDetail,
  fullCrawlDetailAccess,
  fullCrawlTargetDetail,
  fullCrawlUploadsDocument,
  validateFullCrawlYoutubeJsDetail,
} from "./fullCrawlYoutubeJsModel.js";
import { evaluateMigrationUploadsActivity } from "./migrationActivityPolicy.js";
import { ParserContractError } from "./localizedParsing.js";
import { publicationGapRepairJobIntent } from "./publicationGapRepairExecution.js";
import { resolveYoutubeContentType } from "./youtubeContentType.js";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function nowIso(clock) {
  const value = new Date(clock());
  if (Number.isNaN(value.getTime())) throw new TypeError("Full Crawl clock returned an invalid timestamp");
  return value.toISOString();
}

function requiredSurfaceError(message, surface, cause = null) {
  const error = cause == null ? new Error(message) : new Error(message, { cause });
  error.name = "YoutubeJsRequiredSurfaceError";
  error.required_surface = surface;
  return error;
}

function validateChannelSnapshot(snapshot, { channelId, locale }) {
  if (!snapshot || typeof snapshot !== "object" || Array.isArray(snapshot)) {
    throw requiredSurfaceError(`YouTube.js Channel snapshot is missing for ${channelId}`, "channel");
  }
  if (snapshot.about_requested !== true) {
    throw requiredSurfaceError(`YouTube.js About was not requested for ${channelId}`, "channel_about");
  }
  if (snapshot.about_error) {
    throw requiredSurfaceError(
      `YouTube.js About failed for ${channelId}: ${snapshot.about_error.message ?? snapshot.about_error}`,
      "channel_about",
      snapshot.about_error,
    );
  }
  if (snapshot.about_observed !== true) {
    throw requiredSurfaceError(`YouTube.js About was not observed for ${channelId}`, "channel_about");
  }
  const metadata = snapshot.metadata ?? {};
  if (text(metadata.channel_id) !== channelId) {
    throw requiredSurfaceError(`YouTube.js Channel identity conflicts for ${channelId}`, "channel");
  }
  // YouTube may omit a handle (including on auto-generated Topic channels).
  // The channel ID and canonical URL identify the channel without one.
  for (const field of ["title", "channel_url"]) {
    if (!text(metadata[field])) {
      throw new ParserContractError({
        field,
        value: metadata[field],
        locale,
        source: "youtubejs_channel_about",
        reason: `required_channel_${field}_not_observed`,
        context: { channel_id: channelId },
      });
    }
  }
  if (!Number.isSafeInteger(Number(metadata.subscriber_count))
      || Number(metadata.subscriber_count) < 0) {
    throw new ParserContractError({
      field: "subscriber_count",
      value: metadata.subscriber_count_text,
      locale,
      source: "youtubejs_channel_about",
      reason: "required_subscriber_count_not_observed",
      context: { channel_id: channelId },
    });
  }
  return metadata;
}

function normalizeAdmission(snapshot, job, settings, { locale, observedAt, startedAt }) {
  const channelId = text(job.data?.channel_id);
  const metadata = { ...validateChannelSnapshot(snapshot, { channelId, locale }) };
  const country = parseYoutubeAboutCountry(metadata.country, { channel_id: channelId });
  metadata.country = country.raw;
  metadata.country_code = country.code;
  metadata.country_canonical_name = country.name;
  const qualification = evaluateChannelQualification({
    subscriberCount: Number(metadata.subscriber_count),
    minSubscriberCount: job.data?.min_subscriber_count ?? settings.minSubscriberCount,
    required: job.data?.enforce_min_subscribers === true,
  });
  const executionAttemptId = currentChannelExecution()?.attempt_id
    ?? `job-attempt:${Number(job.attemptsStarted ?? 1)}`;
  const aboutObservation = buildAboutObservation({ ...snapshot, metadata }, {
    executionAttemptId,
    locale,
    channelId,
    runId: job.data.run_id,
    observedAt,
    planId: null,
    planDay: null,
    triggerReason: "initial_full",
    scheduledAt: job.data?.scheduled_at ?? null,
    startedAt,
    crawlerVersion: text(process.env.CRAWLER_VERSION) ?? "qy-v16",
  });
  const sourceJson = {
    channel_header: metadata,
    country_observation: country,
    channel_extractor: "youtubejs",
    qualification: { ...qualification, checked_at: observedAt },
    dispatch_batch_id: job.data?.dispatch_batch_id ?? job.data?.pipeline_cycle_id ?? null,
    candidate_id: Number(job.data.candidate_id),
    full_crawl_fetch_contract: fullCrawlFetchContractId(job.data.fetch_contract),
    youtube_request_counts: snapshot.raw?.request_counts ?? null,
  };
  return { metadata, qualification, aboutObservation, sourceJson };
}

function hasChannelContent(state) {
  const tabs = state.channel?.source_json?.channel_header?.available_tabs;
  return Number(state.run?.result_json?.pending_initial_about_observation?.about?.total_video_count) > 0
    || Number(state.channel?.total_video_count) > 0
    || (Array.isArray(tabs) ? tabs.length > 0 : true);
}

function uploadsSettings(state, fallback) {
  return {
    channelContentLimit: Number(state.run?.content_limit ?? fallback.channelContentLimit),
    contentMaxAgeDays: Number(
      state.run?.result_json?.content_max_age_days ?? fallback.contentMaxAgeDays,
    ),
    observedAt: state.run?.started_at ?? new Date().toISOString(),
  };
}

function migrationActivityRequired(state, job) {
  return state.run?.result_json?.migration_activity_gate?.required === true
    || job.data?.reject_if_no_recent_content === true;
}

function detailTerminalReason(detail, contentMaxAgeDays, observedAt) {
  if (isUpcomingLiveDetail(detail)) return { terminalReason: "upcoming_live", window: null };
  if (isLiveInProgress(detail)) return { terminalReason: "live_in_progress", window: null };
  const window = classifyContentWindow(detail, contentMaxAgeDays, observedAt);
  return {
    terminalReason: Number(contentMaxAgeDays) > 0 && window.relation === "outside"
      ? "outside_content_window"
      : null,
    window,
  };
}

async function updateProgress(job, value) {
  if (typeof job?.updateProgress === "function") await job.updateProgress(value);
}

function completedResult({ state, closeResult, executedPhases, phaseTimingsMs, startedAt, resumed, fetchContractId }) {
  const receipt = closeResult?.receipt ?? state.fetch ?? {};
  const migrationActivity = closeResult?.migrationActivity
    ?? state.run?.result_json?.migration_activity_gate
    ?? null;
  return {
    ok: true,
    channel_id: state.identity.channelId,
    candidate_id: state.identity.candidateId,
    run_id: state.identity.runId,
    resumed,
    skipped: migrationActivity?.decision === "dormant",
    skip_reason: migrationActivity?.decision === "dormant"
      ? migrationActivity.reason ?? "migration_activity_dormant"
      : null,
    candidate_count: Number(receipt.selected_count ?? closeResult?.candidateCount ?? 0),
    detail_processed: Number(closeResult?.detailProcessed ?? 0),
    detail_status: "done",
    migration_activity_gate: migrationActivity,
    fetch_contract: fetchContractId,
    executed_phases: executedPhases,
    phase_timings_ms: {
      ...phaseTimingsMs,
      total: Date.now() - startedAt,
    },
  };
}

export function createFullCrawlYoutubeJsExecutor({
  store,
  youtube,
  videoApiFallback = null,
  handoff = {},
  clock = () => new Date(),
  locale = "en",
} = {}) {
  if (!store || typeof store.restore !== "function") throw new TypeError("store is required");
  if (!youtube || typeof youtube.fetchChannel !== "function"
      || typeof youtube.fetchUploads !== "function"
      || typeof youtube.fetchDetail !== "function") {
    throw new TypeError("YouTubeJS Adapter is required");
  }

  return async function executeFullCrawlYoutubeJs(job, { resumeMode = "initial" } = {}) {
    if (!isYoutubeJsFullCrawlFetchContract(job?.data?.fetch_contract)) {
      throw new TypeError("Full Crawl YouTubeJS executor requires a supported YouTubeJS contract");
    }
    const fetchContractId = fullCrawlFetchContractId(job.data.fetch_contract);
    const optionalComments = [YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT_ID, YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT_ID].includes(fetchContractId);
    const startedAtMs = Date.now();
    const startedAt = nowIso(clock);
    const phaseTimingsMs = {};
    const executedPhases = [];
    const settings = await store.loadSettings();
    let state = await store.restore(job);
    const initialPhase = state.phase;
    const resumed = resumeMode !== "initial" || initialPhase !== "admission";
    const notifyCandidateSettled = () => handoff.candidateSettled?.({
      candidateId: state.identity.candidateId,
      dispatchBatchId: state.identity.dispatchBatchId,
    });

    if (state.phase === "terminal") {
      if (state.binding?.status !== "terminal") {
        await store.settleTerminalCheckpoint(job, {
          reason: state.terminalResult.skip_reason,
        });
      }
      await notifyCandidateSettled();
      return state.terminalResult;
    }
    if (state.phase === "existing") {
      const result = await store.settleExistingChannel(job);
      await notifyCandidateSettled();
      return result;
    }
    const repairIntent = publicationGapRepairJobIntent(job.data);
    if (repairIntent?.scope === "about_only") {
      if (state.phase !== "handoff") {
        throw new Error("About-only repair requires a completed Full Crawl checkpoint");
      }
      if (state.run?.publication_finalized_status === "ready_auto"
          && state.run?.result_json?.publication_gap_repair_execution?.scope === "about_only") {
        return { ok: true, repaired: true, already_complete: true, scope: "about_only",
          channel_id: state.identity.channelId, run_id: state.identity.runId };
      }
      const snapshot = await youtube.fetchChannel(state.identity.channelId, {
        includeAbout: true,
        signal: currentChannelExecutionAbortSignal(),
      });
      const admission = normalizeAdmission(snapshot, job, settings, {
        locale, observedAt: nowIso(clock), startedAt,
      });
      if (admission.aboutObservation.about.outcome !== "complete") {
        const error = new Error("About-only Publication Gap repair requires complete About metrics");
        error.code = "publication_gap_about_incomplete";
        throw error;
      }
      const repair = await store.completeAboutOnlyRepair(job, {
        aboutObservation: { ...admission.aboutObservation, triggerReason: "repair" },
        enqueueFinalize: (target) => handoff.fetchCompleted({
          ...target, candidateAttemptFence: state.identity.candidateAttemptFence,
        }),
      });
      return {
        ok: true, repaired: true, channel_id: state.identity.channelId,
        run_id: state.identity.runId, candidate_id: state.identity.candidateId,
        ...repair,
      };
    }
    if (state.phase !== "admission") await notifyCandidateSettled();

    if (state.phase === "admission") {
      const phaseStartedAt = Date.now();
      executedPhases.push("admission");
      await store.beginAdmission(job);
      let snapshot;
      try {
        snapshot = await youtube.fetchChannel(state.identity.channelId, {
          includeAbout: true,
          signal: currentChannelExecutionAbortSignal(),
        });
      } catch (error) {
        const terminal = classifyTerminalChannelError(error);
        if (!terminal) throw error;
        const result = await store.settleTerminalChannel(job, terminal, nowIso(clock));
        await notifyCandidateSettled();
        return result;
      }
      const observedAt = nowIso(clock);
      const admission = normalizeAdmission(snapshot, job, settings, {
        locale,
        observedAt,
        startedAt,
      });
      if (!admission.qualification.qualified) {
        const result = await store.rejectAdmission(job, {
          reason: admission.qualification.reason,
          sourceJson: admission.sourceJson,
        });
        await notifyCandidateSettled();
        return result;
      }
      const committed = await store.commitAdmission(job, {
        metadata: admission.metadata,
        sourceJson: admission.sourceJson,
        aboutObservation: admission.aboutObservation,
        observedAt,
        settings,
      });
      await notifyCandidateSettled();
      if (committed.existing) return {
        ok: true,
        skipped: true,
        skip_reason: "channel_already_promoted",
        channel_id: state.identity.channelId,
        candidate_id: state.identity.candidateId,
        run_id: null,
        candidate_count: 0,
        fetch_contract: fetchContractId,
      };
      phaseTimingsMs.admission = Date.now() - phaseStartedAt;
      state = await store.restore(job);
    }

    if (state.phase === "uploads") {
      const phaseStartedAt = Date.now();
      executedPhases.push("uploads");
      const frozen = uploadsSettings(state, settings);
      const uploads = await youtube.fetchUploads(
        state.identity.channelId,
        frozen.channelContentLimit,
        {
          hasContent: hasChannelContent(state),
          country: state.channel?.source_json?.country_observation != null
            ? state.channel.source_json.country_observation.code
            : state.channel?.country_source === "youtube_about" ? state.channel.country_code : null,
          locale,
          now: new Date(frozen.observedAt).getTime(),
          signal: currentChannelExecutionAbortSignal(),
        },
      );
      const document = fullCrawlUploadsDocument(uploads);
      const required = migrationActivityRequired(state, job) || document.empty_uploads?.outcome === "dormant";
      const activity = evaluateMigrationUploadsActivity({
        required,
        entries: document.entries,
        evidenceComplete: document.activity_evidence_complete,
        maxAgeDays: frozen.contentMaxAgeDays,
        observedAt: frozen.observedAt,
      });
      const targets = activity.dormant ? [] : document.entries;
      await store.commitUploads(job, {
        document,
        targets,
        activityEvidence: required ? activity : null,
        observedAt: nowIso(clock),
      });
      phaseTimingsMs.uploads = Date.now() - phaseStartedAt;
      state = await store.restore(job);
    }

    if (state.phase === "handoff") {
      executedPhases.push("handoff");
      await handoff.fetchCompleted?.({
        channelId: state.identity.channelId,
        runId: state.identity.runId,
        candidateAttemptFence: state.identity.candidateAttemptFence,
        reason: "channel-full-fetch-replayed",
      });
      return completedResult({
        fetchContractId,
        state,
        closeResult: null,
        executedPhases,
        phaseTimingsMs,
        startedAt: startedAtMs,
        resumed,
      });
    }

    const detailFence = contentDetailExecutionFence(job, {
      executionMode: "channel_inline",
      candidateAttemptFence: state.identity.candidateAttemptFence,
    });
    if (!(await store.claimDetailExecution(detailFence))) {
      const error = new Error(`Full Crawl Detail execution was superseded: ${state.identity.runId}`);
      error.code = "CONTENT_DETAIL_EXECUTION_FENCE_STALE";
      throw error;
    }

    let detailProcessed = 0;
    if (state.phase === "detail") {
      const phaseStartedAt = Date.now();
      executedPhases.push("detail");
      const frozen = uploadsSettings(state, settings);
      while (true) {
        const candidate = await store.claimNextDetail(detailFence);
        if (!candidate) break;
        const preflight = classifyFullCrawlTargetBeforeDetail(candidate.target, {
          contentMaxAgeDays: frozen.contentMaxAgeDays,
          observedAt: frozen.observedAt,
        });
        let observation;
        let terminalReason = preflight.terminalReason;
        let window = preflight.window ?? null;
        if (terminalReason) {
          const detail = preflight.detail ?? fullCrawlTargetDetail(candidate.target);
          observation = {
            detail,
            access: fullCrawlDetailAccess(detail),
            classification: resolveYoutubeContentType({
              videoId: candidate.target.video_id,
              upload: candidate.target,
              detail,
            }),
          };
        } else {
          const fetch = () => youtube.fetchDetail(candidate.target.video_id, {
            signal: currentChannelExecutionAbortSignal(),
            strictRequiredSurfaces: true,
            optionalComments,
            detailMode: "full",
            requireContentType: true,
          });
          const validate = detail => validateFullCrawlYoutubeJsDetail(candidate.target.video_id, detail, { optionalComments });
          observation = videoApiFallback && fetchContractId === YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT_ID
            ? await videoApiFallback({ videoId: candidate.target.video_id,
              runId: state.identity.runId,
              requestId: JSON.stringify(["full", state.identity.runId, candidate.target.video_id]),
              consumer: "full", attempt: candidate.attempts, optionalComments,
              signal: currentChannelExecutionAbortSignal(), fetch, validate })
            : validate(await fetch());
          const terminal = detailTerminalReason(
            observation.detail,
            frozen.contentMaxAgeDays,
            frozen.observedAt,
          );
          terminalReason = terminal.terminalReason;
          window = terminal.window;
        }
        await store.commitDetail(detailFence, candidate, {
          ...observation,
          terminalReason,
          window,
          observedAt: nowIso(clock),
          locale,
        });
        detailProcessed += 1;
        await updateProgress(job, {
          stage: "full_crawl_youtubejs_detail",
          run_id: state.identity.runId,
          processed: detailProcessed,
          total: state.candidates.length,
        });
      }
      phaseTimingsMs.detail = Date.now() - phaseStartedAt;
    }

    const closeStartedAt = Date.now();
    executedPhases.push("close_fetch");
    const closeResult = await store.closeFetch(detailFence, { completedAt: nowIso(clock) });
    closeResult.detailProcessed = detailProcessed;
    phaseTimingsMs.close_fetch = Date.now() - closeStartedAt;

    const handoffStartedAt = Date.now();
    executedPhases.push("handoff");
    await handoff.fetchCompleted?.({
      channelId: state.identity.channelId,
      runId: state.identity.runId,
      candidateAttemptFence: state.identity.candidateAttemptFence,
      reason: closeResult.migrationActivity?.decision === "dormant"
        ? "migration-activity-dormant"
        : "channel-full-fetch-complete",
    });
    phaseTimingsMs.handoff = Date.now() - handoffStartedAt;
    return completedResult({
      fetchContractId,
      state,
      closeResult,
      executedPhases,
      phaseTimingsMs,
      startedAt: startedAtMs,
      resumed,
    });
  };
}
