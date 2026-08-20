import {
  aboutBaseline,
  agentBaseline,
  discoveryBaseline,
  recentSamplingBaseline,
} from "./baselineBundle.js";
import { recordAboutObservation } from "./aboutObservationStore.js";
import {
  observationFactsHash,
  recordCrawlerObservation,
} from "./crawlObservationStore.js";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";
import { VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL } from "./publicationContract.js";
import {
  evaluateVideoPublicationWindowCoverage,
  VIDEO_WINDOW_MAX_AGE_DAYS,
  VIDEO_WINDOW_MAX_ITEMS,
} from "./videoPublicationCurrent.js";

function iso(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError("observedAt must be a timestamp");
  return parsed.toISOString();
}

const PUBLISHED_PRECISION_RANK = Object.freeze({ unknown: 0, date_only: 1, second: 2 });
const CONTENT_TYPE_RANK = Object.freeze({ video: 0, short: 1, live: 2 });

function timestampMs(value) {
  if (value == null) return null;
  const parsed = new Date(value).getTime();
  return Number.isNaN(parsed) ? null : parsed;
}

function contentEvidence(row) {
  const publishedAt = timestampMs(row?.published_at);
  return [
    publishedAt == null ? 0 : 1,
    publishedAt == null ? 0 : (PUBLISHED_PRECISION_RANK[row?.published_at_precision] ?? 0),
    CONTENT_TYPE_RANK[row?.content_type] ?? -1,
    timestampMs(row?.player_last_observed_at) ?? -1,
  ];
}

function hasBetterContentEvidence(candidate, current) {
  const candidateEvidence = contentEvidence(candidate);
  const currentEvidence = contentEvidence(current);
  for (let index = 0; index < candidateEvidence.length; index += 1) {
    if (candidateEvidence[index] !== currentEvidence[index]) {
      return candidateEvidence[index] > currentEvidence[index];
    }
  }
  return false;
}

function uniqueVideoContents(contents) {
  const identities = new Map();
  for (const row of contents) {
    const videoId = String(row?.source_content_id ?? "").trim();
    if (!videoId) continue;
    const current = identities.get(videoId);
    if (!current) {
      identities.set(videoId, { ...row, source_content_id: videoId });
      continue;
    }
    const preferred = hasBetterContentEvidence(row, current) ? row : current;
    const currentObservedMs = timestampMs(current.player_last_observed_at) ?? -1;
    const candidateObservedMs = timestampMs(row.player_last_observed_at) ?? -1;
    identities.set(videoId, {
      ...preferred,
      source_content_id: videoId,
      player_last_observed_at: candidateObservedMs > currentObservedMs
        ? row.player_last_observed_at
        : current.player_last_observed_at,
    });
  }
  return [...identities.values()];
}

function genericCommand({
  run,
  channelId,
  observationKind,
  observedAt,
  crawlerVersion,
  revisionType,
  repairId,
  evidenceHash = null,
  prepare,
}) {
  const repairing = revisionType === "repair";
  const repairKey = `repair-full:${run.run_id}:${repairId}:${observationKind}`;
  return {
    idempotencyKey: repairing
      ? (evidenceHash == null ? repairKey : `${repairKey}:evidence:${evidenceHash}`)
      : `initial-full:${run.run_id}:${observationKind}`,
    observationKind,
    channelId,
    runId: run.run_id,
    observedAt,
    planId: null,
    planDay: null,
    triggerReason: repairing ? "repair" : "initial_full",
    scheduledAt: run.started_at ?? null,
    startedAt: run.started_at ?? observedAt,
    finishedAt: observedAt,
    crawlerVersion,
    extractorVersions: { full_crawl: "qy-v2" },
    command: {
      source: repairing ? "full_crawl_repair_finalize" : "full_crawl_finalize",
      run_id: run.run_id,
      ...(repairing ? { repair_id: repairId } : {}),
    },
    prepare,
  };
}

function fullOutcomeReason(revisionType, domain, outcome) {
  const prefix = revisionType === "repair" ? "repair" : "initial_full";
  const suffix = outcome === "complete" ? "complete" : "partial";
  return `${prefix}_${domain}_${suffix}`;
}

function resolvedOutcome(...values) {
  return values.find((value) => ["complete", "partial", "failed"].includes(value)) ?? null;
}

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function videoBaselineEvidenceHash(baseline) {
  return observationFactsHash({
    outcome: baseline.outcome,
    discovery: baseline.discovery,
    recent_sampling: baseline.recentSampling,
    known_identity_count: baseline.knownIdentityCount,
    window_coverage: baseline.windowCoverage,
    anchor_video_ids: baseline.anchorVideoIds,
  });
}

export function isPublicationGapChildRepairRun(run, revisionType) {
  if (revisionType !== "repair") return false;
  const result = record(run?.result_json);
  const gap = record(result.publication_gap_repair);
  const finalRepair = record(result.final_repair);
  return gap.status === "required"
    && optionalText(gap.root_run_id) != null
    && finalRepair.mode === "channel"
    && optionalText(finalRepair.parent_run_id) != null;
}

function nonnegativeInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function fullCrawlScanProof({
  contents,
  channelId,
  observedAt,
  uploadScan,
  ageBoundaryObserved,
  detailFailureCount,
}) {
  if (!uploadScan || typeof uploadScan !== "object" || Array.isArray(uploadScan)) return null;
  const parseGapCount = nonnegativeInteger(uploadScan.parse_gap_count);
  const evaluatedCoverage = evaluateVideoPublicationWindowCoverage({
    rows: contents,
    channelId,
    asOf: observedAt,
  });
  const coverage = {
    ...evaluatedCoverage,
    age_boundary_crossed: evaluatedCoverage.age_boundary_crossed || ageBoundaryObserved,
  };
  const collectionCoversWindow = nonnegativeInteger(uploadScan.content_max_age_days)
    === VIDEO_WINDOW_MAX_AGE_DAYS;
  const requestedLimit = nonnegativeInteger(uploadScan.requested_limit);
  const selectedCount = nonnegativeInteger(uploadScan.selected_count);
  const inspectedCount = nonnegativeInteger(uploadScan.inspected_count);
  const detailFailures = nonnegativeInteger(detailFailureCount) ?? 0;
  const candidateLimitProcessed = collectionCoversWindow
    && requestedLimit === VIDEO_WINDOW_MAX_ITEMS
    && selectedCount === requestedLimit
    && inspectedCount !== null
    && inspectedCount >= requestedLimit
    && detailFailures === 0
    && (uploadScan.stop_reason === "max_items" || uploadScan.terminal_reason === "max_items");
  let terminalCondition = null;
  if (parseGapCount === 0) {
    if (coverage.qualified_count >= VIDEO_WINDOW_MAX_ITEMS) {
      terminalCondition = "qualified_item_limit";
    } else if (candidateLimitProcessed) {
      terminalCondition = VIDEO_INITIAL_CANDIDATE_LIMIT_TERMINAL;
    } else if (collectionCoversWindow && coverage.age_boundary_crossed) {
      terminalCondition = "age_boundary_crossed";
    } else if (collectionCoversWindow && (
      uploadScan.stop_reason === "list_end"
      || uploadScan.terminal_reason === "list_end"
    )) {
      terminalCondition = "list_end";
    }
  }
  return {
    pages: nonnegativeInteger(uploadScan.pages),
    inspected_count: nonnegativeInteger(uploadScan.inspected_count),
    selected_count: nonnegativeInteger(uploadScan.selected_count),
    requested_limit: nonnegativeInteger(uploadScan.requested_limit),
    content_max_age_days: nonnegativeInteger(uploadScan.content_max_age_days),
    scan_policy_version: String(uploadScan.scan_policy_version ?? "").trim() || null,
    parse_gap_count: parseGapCount,
    stop_reason: String(uploadScan.stop_reason ?? "").trim() || null,
    terminal_condition: terminalCondition,
    detail_success_count: contents.length,
    detail_failure_count: detailFailures,
    coverage,
  };
}

export function initialFullVideoBaseline(contents = [], observedAt = new Date(), {
  channelId = null,
  uploadScan = null,
  ageBoundaryObserved = false,
  detailFailureCount = 0,
} = {}) {
  const observedMs = new Date(observedAt).getTime();
  const recentWindowStartMs = observedMs - (30 * 86400000);
  const identities = uniqueVideoContents(contents);
  const entries = identities.map((row) => ({
    video_id: row.source_content_id,
    content_type: row.content_type,
    first_published_at: row.published_at,
    first_published_at_precision: row.published_at_precision,
  }));
  const scanProof = uploadScan == null
    ? null
    : fullCrawlScanProof({
        contents: identities,
        channelId,
        observedAt,
        uploadScan,
        ageBoundaryObserved,
        detailFailureCount,
      });
  const discovery = discoveryBaseline({
    identityCount: identities.length,
    entries,
    scanProof,
  });
  const recent = identities.filter((row) => {
    const publishedMs = timestampMs(row.published_at);
    return publishedMs != null && publishedMs >= recentWindowStartMs;
  });
  const staleCount = recent.filter((row) => (
    row.player_last_observed_at == null
    || observedMs - new Date(row.player_last_observed_at).getTime() >= 7 * 86400000
  )).length;
  const recentSampling = recentSamplingBaseline({
    recentCount: recent.length,
    staleCount,
  });
  return {
    outcome: discovery.outcome === "complete"
      && recentSampling.outcome === "complete" ? "complete" : "partial",
    discovery,
    recentSampling,
    knownIdentityCount: identities.length,
    windowCoverage: scanProof?.coverage ?? null,
    anchorVideoIds: identities
      .filter((row) => timestampMs(row.published_at) != null)
      .slice(0, 20)
      .map((row) => row.source_content_id),
  };
}

export async function recordInitialFullObservations({
  withTransaction,
  channelId,
  runId,
  observedAt,
  crawlerVersion = String(process.env.CRAWLER_VERSION || "qy-v16"),
  revisionType = "incremental",
  repairId = null,
  recordAbout = recordAboutObservation,
  recordObservation = recordCrawlerObservation,
}) {
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (!channelId || !runId) throw new TypeError("channelId and runId are required");
  if (!new Set(["incremental", "repair"]).has(revisionType)) {
    throw new TypeError("revisionType must be incremental or repair");
  }
  const normalizedRepairId = revisionType === "repair"
    ? String(repairId ?? "").trim()
    : null;
  if (revisionType === "repair" && !normalizedRepairId) {
    throw new TypeError("repairId is required for Repair observations");
  }
  return withTransaction(async (client) => {
    const sourceRows = await client.query(
      `SELECT row_to_json(channel) AS channel,row_to_json(run) AS run
       FROM crawler.channels channel
       JOIN crawler.channel_runs run ON run.channel_id=channel.channel_id
       WHERE channel.channel_id=$1 AND run.run_id=$2
       FOR UPDATE OF channel,run`,
      [channelId, runId],
    );
    const source = sourceRows.rows[0];
    if (!source) throw new Error("full Crawl source not found: " + channelId + "/" + runId);
    const channel = source.channel;
    const run = source.run;
    const activeComplete = channel.status === "active" && channel.agent_status === "done";
    const dormantComplete = channel.status === "dormant"
      && ["done", "skipped"].includes(channel.agent_status);
    const hasPromotionCandidate = run.candidate_id != null;
    const publicationGapChildRepair = isPublicationGapChildRepairRun(run, revisionType);
    if (
      run.crawl_mode !== "full"
      || (!hasPromotionCandidate && !publicationGapChildRepair)
      || run.detail_status !== "done"
      || (!activeComplete && !dormantComplete)
    ) {
      return { recorded: false, reason: "full_crawl_not_complete", observations: {} };
    }

    const contentRows = await client.query(
      `SELECT content.*
       FROM crawler.contents content
       WHERE content.channel_id=$1 AND content.run_id=$2
         AND content_type IN ('video','short','live')
       ORDER BY published_at DESC NULLS LAST,source_content_id`,
      [channelId, runId],
    );
    const candidateProofRows = await client.query(
      `SELECT detail_status,missing_fields,result_json->'scope' AS scope
       FROM crawler.content_candidates
       WHERE run_id=$1
       ORDER BY position`,
      [runId],
    );
    const agentRows = await client.query(
      `SELECT profile.channel_id,profile.metrics_json,profile.agent_model,
              profile.agent_config_id,profile.prompt_template_id,profile.prompt_hash,
              profile.prompt_variant,profile.input_content_ids,profile.input_content_hash,
              profile.taxonomy_version,profile.agent_version_hash,
              config.provider,config.model AS config_model,
              config.tools_json,
              COALESCE(profile.updated_at,run.updated_at,run.started_at) AS observed_at
       FROM crawler.agent_profiles profile
       LEFT JOIN crawler.agent_configs config ON config.config_id=profile.agent_config_id
       JOIN crawler.channel_runs run ON run.run_id=$2
       WHERE profile.channel_id=$1
         AND profile.agent_mode='basic' AND profile.status='success'
       LIMIT 1`,
      [channelId, runId],
    );
    const observationRows = await client.query(
      `SELECT observation_id,observation_kind,outcome
       FROM crawler.crawl_observations
       WHERE channel_id=$1 AND run_id=$2`,
      [channelId, runId],
    );
    const agent = agentRows.rows[0];
    if (!agent && !dormantComplete) {
      return { recorded: false, reason: "full_crawl_agent_missing", observations: {} };
    }
    const eventTime = iso(observedAt ?? agent?.observed_at ?? run.updated_at ?? run.started_at);
    const aboutOnlyRepair = revisionType === "repair"
      && run.result_json?.publication_gap_repair_execution?.scope === "about_only";
    const existing = revisionType === "repair" && !aboutOnlyRepair
      ? new Map()
      : new Map(observationRows.rows.map((row) => [row.observation_kind, row.observation_id]));
    const outcomes = revisionType === "repair" && !aboutOnlyRepair
      ? new Map()
      : new Map(observationRows.rows.map((row) => [row.observation_kind, row.outcome]));
    const observations = {};
    const pendingAbout = run.result_json?.pending_initial_about_observation;
    if (pendingAbout != null && (
      typeof pendingAbout !== "object"
      || Array.isArray(pendingAbout)
      || String(pendingAbout.channelId ?? "") !== String(channelId)
      || String(pendingAbout.runId ?? "") !== String(runId)
    )) {
      throw new Error("pending initial About observation does not match its Full Crawl Run");
    }
    const persistedAboutObservationId = String(
      run.result_json?.initial_about_observation_id ?? "",
    ).trim() || null;
    if (aboutOnlyRepair) {
      for (const kind of ["video", "agent"]) {
        const reusable = observationRows.rows.some((row) => (
          row.observation_kind === kind && row.outcome === "complete"
        ));
        if (!reusable) {
          throw new Error(`About-only Publication Gap repair requires a complete ${kind} observation`);
        }
      }
      if (pendingAbout) {
        existing.delete("about");
        outcomes.delete("about");
      } else if (!persistedAboutObservationId) {
        throw new Error("About-only Publication Gap repair has no staged or persisted About observation");
      }
    }
    if (revisionType === "repair" && !pendingAbout && persistedAboutObservationId) {
      const persistedAbout = observationRows.rows.find((row) => (
        row.observation_kind === "about"
        && String(row.observation_id) === persistedAboutObservationId
      ));
      if (!persistedAbout) {
        throw new Error("persisted initial About observation is missing from its Full Crawl Run");
      }
      existing.set("about", persistedAboutObservationId);
      outcomes.set("about", persistedAbout.outcome);
    }

    if (!existing.has("about")) {
      if (pendingAbout) {
        observations.about = await recordAbout(client, {
          ...pendingAbout,
          ...(revisionType === "repair" && !aboutOnlyRepair
            ? {
                idempotencyKey: `repair-full:${runId}:${normalizedRepairId}:about`,
                triggerReason: "repair",
                about: {
                  ...pendingAbout.about,
                  outcome_reason_code: fullOutcomeReason(
                    revisionType,
                    "about",
                    pendingAbout.about?.outcome,
                  ),
                },
              }
            : {}),
          publicationReconcile: false,
        });
        outcomes.set("about", resolvedOutcome(
          observations.about?.outcome,
          pendingAbout.about?.outcome,
        ));
      } else {
        const baseline = aboutBaseline(channel);
        baseline.about.outcome_reason_code = fullOutcomeReason(
          revisionType,
          "about",
          baseline.about.outcome,
        );
        observations.about = await recordAbout(client, {
          idempotencyKey: revisionType === "repair"
            ? `repair-full:${runId}:${normalizedRepairId}:about`
            : `initial-full:${runId}:about`,
          channelId,
          runId,
          observedAt: eventTime,
          planId: null,
          planDay: null,
          triggerReason: revisionType === "repair" ? "repair" : "initial_full",
          scheduledAt: run.started_at ?? null,
          startedAt: run.started_at ?? eventTime,
          finishedAt: eventTime,
          crawlerVersion,
          extractorVersions: { full_crawl: "qy-v2" },
          publicationReconcile: false,
          about: baseline.about,
          current: baseline.current,
        });
        outcomes.set("about", resolvedOutcome(observations.about?.outcome, baseline.about.outcome));
      }
    }

    if (!existing.has("video")) {
      const ageBoundaryObserved = candidateProofRows.rows.some((row) => (
        ["older_than_max_age", "after_chronological_age_cutoff"].includes(row.scope?.reason)
      ));
      const detailFailureCount = candidateProofRows.rows.filter((row) => (
        row.detail_status === "unavailable"
        || (Array.isArray(row.missing_fields) && row.missing_fields.length > 0)
      )).length;
      const baseline = initialFullVideoBaseline(contentRows.rows, eventTime, {
        channelId,
        uploadScan: run.result_json?.upload_scan ?? null,
        ageBoundaryObserved,
        detailFailureCount,
      });
      const upgradesPartialRepair = revisionType === "repair"
        && baseline.outcome === "complete"
        && observationRows.rows.some((row) => (
          row.observation_kind === "video" && row.outcome === "partial"
        ));
      observations.video = await recordObservation(client, genericCommand({
        run,
        channelId,
        observationKind: "video",
        observedAt: eventTime,
        crawlerVersion,
        revisionType,
        repairId: normalizedRepairId,
        evidenceHash: upgradesPartialRepair ? videoBaselineEvidenceHash(baseline) : null,
        prepare: async ({ client: transactionClient, observationId }) => {
          const boundContents = await transactionClient.query(
            `UPDATE crawler.contents
             SET last_observation_id=$3
             WHERE channel_id=$1 AND run_id=$2
             RETURNING content_key`,
            [channelId, runId, observationId],
          );
          await refreshVideoPublicationItemHashes(
            transactionClient,
            boundContents.rows.map((row) => row.content_key),
          );
          return {
            outcome: baseline.outcome,
            outcomeReasonCode: fullOutcomeReason(revisionType, "video", baseline.outcome),
            resultSummary: {
              known_identity_count: baseline.knownIdentityCount,
              recent_count: baseline.recentSampling.payload.recent_count,
              stale_ratio: baseline.recentSampling.payload.stale_ratio,
              baseline: true,
              discovery: {
                pages: baseline.discovery.payload.pages,
                items: baseline.discovery.payload.items,
                anchor_matched: baseline.discovery.payload.anchor_matched,
                stop_reason: baseline.discovery.payload.stop_reason,
                parse_gap_count: baseline.discovery.payload.parse_gap_count,
                first_seen_count: baseline.discovery.payload.first_seen_count,
                detail_success_count: baseline.discovery.payload.detail_success_count,
                detail_failure_count: baseline.discovery.payload.detail_failure_count,
              },
            },
            payload: {
              discovery: {
                outcome: baseline.discovery.outcome,
                payload: baseline.discovery.payload,
              },
              recent_sampling: {
                outcome: baseline.recentSampling.outcome,
                payload: baseline.recentSampling.payload,
              },
            },
            anchorVideoIds: baseline.anchorVideoIds,
            sourceCursor: {
              source: revisionType === "repair"
                ? "full_crawl_repair_finalize"
                : "full_crawl_finalize",
              known_identity_count: baseline.knownIdentityCount,
              terminal_reason: baseline.discovery.payload.stop_reason,
            },
          };
        },
      }));
      outcomes.set("video", resolvedOutcome(observations.video?.outcome, baseline.outcome));
    }

    if (!existing.has("agent") && agent) {
      const baseline = agentBaseline(agent);
      observations.agent = await recordObservation(client, genericCommand({
        run,
        channelId,
        observationKind: "agent",
        observedAt: eventTime,
        crawlerVersion,
        revisionType,
        repairId: normalizedRepairId,
        prepare: async ({ client: transactionClient, observationId }) => {
          await transactionClient.query(
            `UPDATE crawler.agent_profiles
             SET last_observation_id=$2,last_observed_at=$3,current_output_hash=$4
             WHERE channel_id=$1 AND agent_mode='basic' AND status='success'`,
            [channelId, observationId, eventTime, baseline.current_hash],
          );
          return {
            outcome: baseline.outcome,
            outcomeReasonCode: fullOutcomeReason(revisionType, "agent", baseline.outcome),
            resultSummary: {
              fulfilled_plan_count: 1,
              baseline: true,
              output_hash: baseline.current_hash,
              input_content_hash: baseline.input_content_hash,
              agent_version_hash: baseline.agent_version_hash,
            },
            payload: baseline.payload,
          };
        },
      }));
      outcomes.set("agent", resolvedOutcome(observations.agent?.outcome, baseline.outcome));
    }
    if (pendingAbout) {
      const aboutObservationId = observations.about?.observation_id ?? existing.get("about");
      await client.query(
        `UPDATE crawler.channel_runs
         SET result_json=(result_json-'pending_initial_about_observation')
               || jsonb_build_object('initial_about_observation_id',$2::text),
             updated_at=now()
         WHERE run_id=$1`,
        [runId, aboutObservationId],
      );
    }
    return {
      recorded: Object.keys(observations).length > 0,
      reason: null,
      observations,
      outcomes: Object.fromEntries(
        ["about", "video", "agent"]
          .filter((kind) => resolvedOutcome(outcomes.get(kind)) != null)
          .map((kind) => [kind, outcomes.get(kind)]),
      ),
    };
  });
}
