function requiredQuery(value) {
  if (typeof value !== "function") throw new TypeError("query is required");
  return value;
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function optionalText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function record(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

const PUBLICATION_GAP_DOMAIN_ORDER = Object.freeze(["channel", "video"]);
const PUBLICATION_GAP_DOMAIN_SET = new Set(PUBLICATION_GAP_DOMAIN_ORDER);
const PUBLICATION_GAP_JOB_FIELDS = Object.freeze([
  "publication_gap_domains",
  "publication_gap_root_run_id",
  "publication_gap_scope",
  "require_complete_about_metrics",
]);

export function normalizePublicationGapDomains(value, {
  required = false,
  field = "publication_gap_domains",
} = {}) {
  if (value == null) {
    if (required) throw new TypeError(`${field} is required`);
    return [];
  }
  if (!Array.isArray(value)) throw new TypeError(`${field} must be an array`);
  const normalized = value.map((domain) => String(domain ?? "").trim()).filter(Boolean);
  const invalid = normalized.filter((domain) => !PUBLICATION_GAP_DOMAIN_SET.has(domain));
  if (invalid.length > 0) {
    throw new TypeError(`${field} contains unsupported domains: ${[...new Set(invalid)].join(", ")}`);
  }
  const domains = PUBLICATION_GAP_DOMAIN_ORDER.filter((domain) => normalized.includes(domain));
  if (required && domains.length === 0) throw new TypeError(`${field} is required`);
  return domains;
}

export function buildPublicationGapRepairTarget(rowValue = {}) {
  const row = record(rowValue);
  const runId = requiredText(row.run_id, "Publication Gap run_id");
  const domains = normalizePublicationGapDomains(row.repair_domains, {
    required: true,
    field: "Publication Gap repair_domains",
  });
  const registryPromotionRunId = optionalText(row.registry_promotion_run_id);
  const inheritedRootRunId = optionalText(row.publication_gap_root_run_id);
  const rootRunId = inheritedRootRunId ?? registryPromotionRunId ?? runId;
  if (registryPromotionRunId && rootRunId !== registryPromotionRunId) {
    throw new TypeError("Publication Gap root_run_id conflicts with the immutable Promotion Run");
  }
  const requiresAbout = domains.includes("channel");
  const common = {
    publication_gap_domains: domains,
    publication_gap_root_run_id: rootRunId,
    ...(requiresAbout ? { require_complete_about_metrics: true } : {}),
  };

  if (optionalText(row.publication_finalized_status) === "ready_partial") {
    const candidateId = positiveInteger(row.candidate_id, "candidate_id");
    if (runId !== rootRunId) {
      throw new TypeError("a ready_partial Publication Gap must resume the Promotion Run");
    }
    return {
      strategy: "resume_run",
      run_id: runId,
      candidate_id: candidateId,
      ...common,
      ...(domains.length === 1 && requiresAbout
        ? { publication_gap_scope: "about_only" }
        : {}),
    };
  }

  return {
    strategy: "child_run",
    repair_parent_run_id: rootRunId,
    ...common,
  };
}

export function publicationGapRepairJobIntent(dataValue = {}) {
  const data = record(dataValue);
  const hasGapEvidence = PUBLICATION_GAP_JOB_FIELDS.some((field) => (
    Object.prototype.hasOwnProperty.call(data, field)
  ));
  if (!hasGapEvidence) return null;

  const domains = normalizePublicationGapDomains(data.publication_gap_domains, { required: true });
  const rootRunId = requiredText(
    data.publication_gap_root_run_id,
    "publication_gap_root_run_id",
  );
  const parentRunId = optionalText(data.repair_parent_run_id);
  const runId = optionalText(data.run_id);
  const scope = optionalText(data.publication_gap_scope);
  const requiresAbout = domains.includes("channel");
  if (requiresAbout && data.require_complete_about_metrics !== true) {
    throw new TypeError("a Channel Publication Gap requires complete About metrics");
  }
  if (!requiresAbout && data.require_complete_about_metrics === true) {
    throw new TypeError("complete About metrics cannot be required without a Channel gap");
  }

  if (parentRunId) {
    if (parentRunId !== rootRunId) {
      throw new TypeError("Publication Gap Child parent_run_id must equal the Promotion root_run_id");
    }
    if (scope) throw new TypeError("Publication Gap Child cannot use a resume scope");
    return Object.freeze({
      strategy: "child_run",
      rootRunId,
      parentRunId,
      domains,
      requiresAbout,
      scope: null,
    });
  }

  if (!runId || runId !== rootRunId) {
    throw new TypeError("Publication Gap Resume must target the Promotion root_run_id");
  }
  positiveInteger(data.candidate_id, "candidate_id");
  if (scope && scope !== "about_only") {
    throw new TypeError(`unsupported Publication Gap resume scope: ${scope}`);
  }
  if (scope === "about_only" && !(domains.length === 1 && requiresAbout)) {
    throw new TypeError("About-only Publication Gap Resume requires only the Channel domain");
  }
  return Object.freeze({
    strategy: "resume_run",
    rootRunId,
    parentRunId: null,
    domains,
    requiresAbout,
    scope,
  });
}

export function isAboutOnlyPublicationGapRepair(data = {}) {
  try {
    const intent = publicationGapRepairJobIntent(data);
    return intent?.strategy === "resume_run" && intent.scope === "about_only";
  } catch {
    return false;
  }
}

export function deferAboutObservationUntilRepairFinalize(jobData = {}, runResultJson = {}) {
  return isAboutOnlyPublicationGapRepair(jobData)
    || Boolean(record(runResultJson).publication_repair);
}

export async function completeAboutOnlyPublicationGapRepair(queryValue, {
  jobData,
  runId,
  channelId,
  aboutOutcome,
  aboutObservationCommand,
  enqueueFinalize,
} = {}) {
  const query = requiredQuery(queryValue);
  if (!isAboutOnlyPublicationGapRepair(jobData)) {
    throw new TypeError("an explicit About-only Publication Gap repair is required");
  }
  if (typeof enqueueFinalize !== "function") {
    throw new TypeError("enqueueFinalize is required");
  }
  const normalizedRunId = requiredText(runId, "runId");
  const normalizedChannelId = requiredText(channelId, "channelId");
  const candidateId = positiveInteger(jobData.candidate_id, "candidate_id");
  if (String(jobData.run_id) !== normalizedRunId) {
    throw new TypeError("the About-only repair Run does not match jobData.run_id");
  }
  if (aboutOutcome !== "complete") {
    const error = new Error("About-only Publication Gap repair requires complete About metrics");
    error.code = "publication_gap_about_incomplete";
    throw error;
  }
  const command = record(aboutObservationCommand);
  if (
    String(command.channelId ?? "") !== normalizedChannelId
    || String(command.runId ?? "") !== normalizedRunId
    || command.about?.outcome !== "complete"
  ) {
    throw new TypeError("the complete About Observation command does not match the repair target");
  }

  const staged = await query(
    `/* publication-gap-repair:stage-about-only */
     WITH eligible AS (
       SELECT run.run_id,run.expected_content_count
       FROM crawler.channel_runs AS run
       JOIN crawler.channels AS channel
         ON channel.channel_id=run.channel_id
        AND channel.latest_run_id=run.run_id
        AND channel.registry_promotion_run_id=run.run_id
        AND channel.registry_promotion_candidate_id=run.candidate_id
       JOIN crawler.finalized_profiles AS finalized
         ON finalized.channel_id=channel.channel_id
        AND finalized.run_id=run.run_id
       WHERE run.run_id=$1
         AND run.channel_id=$2
         AND run.candidate_id=$3
         AND channel.status='active'
         AND channel.agent_status='done'
         AND run.crawl_mode='full'
         AND run.status='done'
         AND run.detail_status='done'
         AND run.publication_finalized_status='ready_partial'
         AND finalized.status='ready_partial'
         AND finalized.quality_json#>>'{initial_observations,outcomes,about}'='partial'
         AND finalized.quality_json#>>'{initial_observations,outcomes,video}'='complete'
         AND finalized.quality_json#>>'{initial_observations,outcomes,agent}'='complete'
         AND COALESCE((finalized.quality_json->>'unavailable_candidate_count')::int,0)=0
         AND COALESCE((finalized.quality_json->>'detail_open_count')::int,0)=0
         AND COALESCE((finalized.quality_json->>'api_open_count')::int,0)=0
         AND COALESCE((finalized.quality_json->>'classified_content_count')::int,-1)
               =COALESCE((finalized.quality_json->>'expected_content_count')::int,-2)
         AND finalized.quality_json->'missing_channel_fields'='[]'::jsonb
         AND finalized.quality_json->'missing_agent_fields'='[]'::jsonb
         AND finalized.quality_json->'missing_content_fields'='{}'::jsonb
         AND (
           finalized.quality_json#>'{publication_initial_package}' IS NULL
           OR (
             finalized.quality_json#>>'{publication_initial_package,status}'='not_ready'
             AND EXISTS (
               SELECT 1
               FROM jsonb_array_elements(
                 COALESCE(
                   finalized.quality_json#>'{publication_initial_package,domains}',
                   '[]'::jsonb
                 )
               ) AS domain
               WHERE domain->>'domain'='channel'
                 AND domain->>'readiness_status'<>'ready'
             )
             AND NOT EXISTS (
               SELECT 1
               FROM jsonb_array_elements(
                 COALESCE(
                   finalized.quality_json#>'{publication_initial_package,domains}',
                   '[]'::jsonb
                 )
               ) AS domain
               WHERE domain->>'domain' IN ('video','agent')
                 AND domain->>'readiness_status'<>'ready'
             )
           )
         )
       FOR UPDATE OF run
     )
     UPDATE crawler.channel_runs AS run
     SET result_json=jsonb_set(
           jsonb_set(
             run.result_json,
             '{pending_initial_about_observation}',
             $5::jsonb,
             true
           ),
           '{publication_gap_repair_execution}',
           jsonb_build_object(
             'scope','about_only',
             'status','staged',
             'staged_at',now(),
             'about_outcome',$4::text
           ),
           true
         ),
         updated_at=now()
     FROM eligible
     WHERE run.run_id=eligible.run_id
     RETURNING eligible.expected_content_count`,
    [
      normalizedRunId,
      normalizedChannelId,
      candidateId,
      aboutOutcome,
      JSON.stringify(command),
    ],
  );
  if (staged.rowCount !== 1) {
    const error = new Error("About-only Publication Gap repair preconditions no longer hold");
    error.code = "publication_gap_about_precondition_failed";
    throw error;
  }

  await enqueueFinalize({
    channelId: normalizedChannelId,
    runId: normalizedRunId,
    reason: "publication-gap-about-only",
  });
  return {
    scope: "about_only",
    candidate_count: Number(staged.rows[0]?.expected_content_count ?? 0),
    about_outcome: aboutOutcome,
  };
}
