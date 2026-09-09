import { buildChannelPublicationCurrent } from "./channelPublicationCurrent.js";
import {
  AGENT_FACT_KEYS,
  buildAgentPublicationCurrent,
} from "./agentPublicationCurrent.js";
import { observationFactsHash } from "./crawlObservationStore.js";
import { buildPublicationSourceTrace } from "./publicationSourceTrace.js";
import {
  AGENT_TAXONOMY_VERSION,
  PUBLICATION_CONTRACT_VERSION,
  PUBLICATION_POLICY_VERSION,
  VIDEO_WINDOW_POLICY_VERSION,
} from "./publicationContract.js";
import { buildVideoPublicationCurrent } from "./videoPublicationCurrent.js";

export {
  AGENT_FACT_KEYS,
  AGENT_TAXONOMY_VERSION,
  PUBLICATION_CONTRACT_VERSION,
  PUBLICATION_POLICY_VERSION,
  VIDEO_WINDOW_POLICY_VERSION,
};

export const PUBLICATION_READINESS_REPORT_VERSION = "publication-readiness-report-v1";
export const BUSINESS_LARGE_DROP_RATIO = 0.5;

export const CHANNELS_SQL = `/* publication-readiness:channels */
SELECT to_jsonb(channel) AS row
FROM crawler.channels channel
WHERE ($1::text[] IS NULL OR channel.channel_id=ANY($1::text[]))
ORDER BY channel.channel_id`;

export const CONTENTS_SQL = `/* publication-readiness:contents */
SELECT to_jsonb(content) - 'raw_json' AS row
FROM crawler.contents content
LEFT JOIN crawler.channel_domain_cursors video_cursor
  ON video_cursor.channel_id=content.channel_id
 AND video_cursor.observation_kind='video'
WHERE ($1::text[] IS NULL OR content.channel_id=ANY($1::text[]))
  AND (
    content.published_at IS NULL
    OR content.published_at >= $2::timestamptz - interval '91 days'
    OR content.last_observation_id=video_cursor.latest_complete_observation_id
  )
ORDER BY content.channel_id,content.published_at DESC NULLS LAST,
         content.source_content_id,content.content_key`;

export const AGENTS_SQL = `/* publication-readiness:agents */
SELECT DISTINCT ON (profile.channel_id)
       to_jsonb(profile) AS row,
       jsonb_build_object(
         'config_id',config.config_id,
         'provider',config.provider,
         'model',config.model,
         'prompt_template_id',config.prompt_template_id,
         'tools_json',config.tools_json,
         'template_text',prompt.template_text
       ) AS config
FROM crawler.agent_profiles profile
LEFT JOIN crawler.agent_configs config ON config.config_id=profile.agent_config_id
LEFT JOIN crawler.agent_prompt_templates prompt
  ON prompt.template_id=config.prompt_template_id
WHERE ($1::text[] IS NULL OR profile.channel_id=ANY($1::text[]))
ORDER BY profile.channel_id,
         (profile.agent_mode='basic') DESC,
         (profile.status='success') DESC,
         profile.updated_at DESC`;

export const SOURCES_SQL = `/* publication-readiness:sources */
SELECT cursor.channel_id,cursor.observation_kind,
       to_jsonb(cursor) AS cursor,
       to_jsonb(latest_observation) AS latest_observation,
       to_jsonb(complete_observation) AS complete_observation,
       to_jsonb(run) AS run,
       CASE WHEN cursor.observation_kind='video'
              AND run.result_json#>>'{fetch_contract,executor_id}'='youtubejs_full'
         THEN COALESCE((SELECT jsonb_agg(jsonb_build_object(
           'detail_status',candidate.detail_status,'disposition',candidate.disposition,
           'source_content_id',candidate.source_content_id,'position',candidate.position,
           'source_url',candidate.source_url,'target',candidate.result_json->'full_crawl_target'
         ) ORDER BY candidate.position)
         FROM crawler.content_candidates candidate
         WHERE candidate.run_id=run.run_id AND candidate.channel_id=cursor.channel_id), '[]'::jsonb)
         ELSE NULL END AS full_crawl_candidates
FROM crawler.channel_domain_cursors cursor
LEFT JOIN crawler.crawl_observations latest_observation
  ON latest_observation.observation_id=cursor.latest_observation_id
LEFT JOIN crawler.crawl_observations complete_observation
  ON complete_observation.observation_id=cursor.latest_complete_observation_id
LEFT JOIN crawler.channel_runs run ON run.run_id=complete_observation.run_id
WHERE ($1::text[] IS NULL OR cursor.channel_id=ANY($1::text[]))
  AND cursor.observation_kind IN ('about','video','agent')
ORDER BY cursor.channel_id,cursor.observation_kind`;

const BUSINESS_SQL = `/* publication-readiness:business-current */
WITH storage_state AS (
  SELECT COALESCE((
    SELECT read_mode FROM publication.creator_search_storage_state
    WHERE singleton=true
  ),'legacy') AS read_mode
), active_search AS (
  SELECT search.*
  FROM public.creator_search_live search
  CROSS JOIN storage_state state
  WHERE state.read_mode='live'
    AND ($1::text[] IS NULL OR search.channel_id=ANY($1::text[]))
  UNION ALL
  SELECT search.*
  FROM public.creator_search_active active
  JOIN public.creator_search_current search ON search.watermark=active.watermark
  CROSS JOIN storage_state state
  WHERE state.read_mode='legacy'
    AND ($1::text[] IS NULL OR search.channel_id=ANY($1::text[]))
), active_snapshots AS (
  SELECT snapshot.*
  FROM public.channel_snapshots snapshot
  JOIN active_search search
    ON search.snapshot_id=snapshot.id AND search.channel_id=snapshot.channel_id
), links AS (
  SELECT link.channel_snapshot_id,count(*)::int AS link_count
  FROM public.channel_links link
  JOIN active_snapshots snapshot ON snapshot.id=link.channel_snapshot_id
  GROUP BY link.channel_snapshot_id
), facts AS (
  SELECT fact.channel_snapshot_id,
         array_agg(DISTINCT fact.field_key ORDER BY fact.field_key) AS fact_keys
  FROM public.channel_profile_facts fact
  JOIN active_snapshots snapshot ON snapshot.id=fact.channel_snapshot_id
  GROUP BY fact.channel_snapshot_id
), contents AS (
  SELECT content.channel_snapshot_id,count(*)::int AS content_count
  FROM public.content_snapshots content
  JOIN active_snapshots snapshot ON snapshot.id=content.channel_snapshot_id
  WHERE content.is_canonical=true
  GROUP BY content.channel_snapshot_id
)
SELECT search.channel_id,
       jsonb_build_object(
         'watermark',search.watermark,
         'name',search.name,
         'handle',search.handle,
         'avatar_url',search.avatar_url,
         'verified',search.verified,
         'subscribers',search.subscribers,
         'total_views',search.total_views,
         'channel_video_count',search.channel_video_count
       ) AS search,
       jsonb_build_object(
         'title',snapshot.title,
         'handle',snapshot.handle,
         'avatar_url',snapshot.avatar_url,
         'description',snapshot.description,
         'is_verified',snapshot.is_verified,
         'subscriber_count',snapshot.subscriber_count,
         'total_view_count',snapshot.total_view_count,
         'video_count',snapshot.video_count,
         'joined_date',snapshot.joined_date
       ) AS snapshot,
       COALESCE(links.link_count,0)::int AS link_count,
       COALESCE(facts.fact_keys,'{}'::text[]) AS fact_keys,
       COALESCE(contents.content_count,0)::int AS content_count
FROM active_search search
JOIN active_snapshots snapshot
  ON snapshot.id=search.snapshot_id AND snapshot.channel_id=search.channel_id
LEFT JOIN links ON links.channel_snapshot_id=snapshot.id
LEFT JOIN facts ON facts.channel_snapshot_id=snapshot.id
LEFT JOIN contents ON contents.channel_snapshot_id=snapshot.id
ORDER BY search.channel_id`;

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function integer(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function nonnegativeInteger(value) {
  const parsed = integer(value);
  return parsed !== null && parsed >= 0 ? parsed : null;
}

function timestamp(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function compareText(left, right) {
  return Buffer.compare(Buffer.from(String(left ?? ""), "utf8"), Buffer.from(String(right ?? ""), "utf8"));
}

function uniqueTextList(value, { sort = true } = {}) {
  const values = Array.isArray(value) ? value : [];
  const output = [...new Set(values.map(text).filter(Boolean))];
  return sort ? output.sort(compareText) : output;
}

function compactIssue(issue) {
  return Object.fromEntries(Object.entries(issue).filter(([, value]) => value !== undefined));
}

function issueKey(issue) {
  return [issue.domain, issue.code, issue.field, issue.content_id].map((value) => value ?? "").join("\u0000");
}

function sortedIssues(issues) {
  const unique = new Map();
  for (const item of issues) unique.set(issueKey(item), compactIssue(item));
  return [...unique.values()].sort((left, right) => (
    compareText(left.domain, right.domain)
    || compareText(left.code, right.code)
    || compareText(left.field, right.field)
    || compareText(left.content_id, right.content_id)
  ));
}

function resultRows(result, queryName) {
  if (!result || !Array.isArray(result.rows)) {
    throw new TypeError(`${queryName} query must return { rows: [] }`);
  }
  return result.rows;
}

function channelFilter(channelIds) {
  if (channelIds === null || channelIds === undefined) return null;
  if (!Array.isArray(channelIds)) throw new TypeError("channelIds must be an array when provided");
  return uniqueTextList(channelIds);
}

export function buildChannelReadiness({ row, source }) {
  const channel = object(row);
  const candidate = buildChannelPublicationCurrent(channel);
  const currentRefs = {
    observed_at: channel.about_last_observed_at,
    facts_hash: channel.about_current_hash,
    identity_observed_at: channel.about_identity_last_observed_at ?? channel.profile_last_observed_at,
    identity_hash: channel.about_identity_current_hash ?? channel.profile_current_hash,
  };
  const trace = buildPublicationSourceTrace(source, currentRefs, "channel");
  const issues = [...candidate.issues, ...trace.issues];
  if (!text(currentRefs.identity_hash) || !timestamp(currentRefs.identity_observed_at)) {
    issues.push({ domain: "channel", code: "channel_identity_current_ref_missing" });
  }
  const readinessIssues = sortedIssues(issues);
  const ready = readinessIssues.length === 0;
  return {
    ready,
    contract_version: candidate.contract_version,
    policy_version: candidate.policy_version,
    result_hash: ready ? candidate.result_hash : null,
    payload: candidate.payload,
    checks: candidate.checks,
    links: candidate.links,
    source_refs: trace,
    issues: readinessIssues,
    warnings: candidate.warnings,
    comparison_values: candidate.comparison_values,
  };
}

export function buildVideoReadiness(input) {
  const current = buildVideoPublicationCurrent(input);
  return {
    ...current,
    items: current.items.map((item) => ({
      position: item.position,
      content_id: item.payload.content_id,
      content_key: item.payload.content_key,
      kind: item.payload.kind,
      published_at: item.payload.published_at,
      published_date: item.payload.published_date,
      item_hash: item.item_hash,
      source_refs: item.source_refs,
    })),
  };
}

export function buildAgentReadiness(input) {
  return buildAgentPublicationCurrent(input);
}

function present(value) {
  if (value === null || value === undefined) return false;
  if (typeof value === "string") return Boolean(text(value));
  return true;
}

function compareValue(field, crawlerValue, businessValue, regressions, differences) {
  const crawlerPresent = present(crawlerValue);
  const businessPresent = present(businessValue);
  if (businessPresent && !crawlerPresent) {
    regressions.push({ field, crawler_complete: false, business_complete: true });
  } else if (crawlerPresent && businessPresent && observationFactsHash(crawlerValue) !== observationFactsHash(businessValue)) {
    differences.push({
      field,
      crawler_value_hash: observationFactsHash(crawlerValue),
      business_value_hash: observationFactsHash(businessValue),
    });
  }
}

function compareCount(field, crawlerValue, businessValue, regressions) {
  const crawlerCount = nonnegativeInteger(crawlerValue);
  const businessCount = nonnegativeInteger(businessValue);
  if (businessCount === null || businessCount === 0) return;
  if (crawlerCount === null || crawlerCount === 0) {
    regressions.push({ field, crawler_complete: false, business_complete: true });
    return;
  }
  if (crawlerCount / businessCount < BUSINESS_LARGE_DROP_RATIO) {
    regressions.push({
      field,
      reason: "large_drop",
      crawler_count: crawlerCount,
      business_count: businessCount,
    });
  }
}

function compareMetric(field, crawlerValue, businessValue, regressions, differences) {
  compareValue(field, crawlerValue, businessValue, regressions, differences);
  const crawlerCount = nonnegativeInteger(crawlerValue);
  const businessCount = nonnegativeInteger(businessValue);
  if (crawlerCount !== null
      && businessCount !== null
      && businessCount > 0
      && crawlerCount / businessCount < BUSINESS_LARGE_DROP_RATIO) {
    regressions.push({
      field,
      reason: "large_drop",
      crawler_count: crawlerCount,
      business_count: businessCount,
    });
  }
}

function businessRegression({ business, channel, video, agent }) {
  if (!business) {
    return {
      target_exists: false,
      passed: true,
      active_watermark: null,
      regressions: [],
      value_differences: [],
      target_profile_fact_count: 0,
      target_content_count: 0,
    };
  }
  const search = object(business.search);
  const snapshot = object(business.snapshot);
  const crawler = channel.comparison_values;
  const regressions = [];
  const differences = [];
  compareValue("title", crawler.title, snapshot.title ?? search.name, regressions, differences);
  compareValue("handle", crawler.handle, snapshot.handle ?? search.handle, regressions, differences);
  compareValue("avatar_url", crawler.avatar_url, snapshot.avatar_url ?? search.avatar_url, regressions, differences);
  compareValue("description", crawler.description, snapshot.description, regressions, differences);
  compareValue("is_verified", crawler.is_verified, snapshot.is_verified ?? search.verified, regressions, differences);
  compareMetric("subscriber_count", crawler.subscriber_count, snapshot.subscriber_count ?? search.subscribers, regressions, differences);
  compareMetric("total_view_count", crawler.total_view_count, snapshot.total_view_count ?? search.total_views, regressions, differences);
  compareMetric("total_video_count", crawler.total_video_count, snapshot.video_count ?? search.channel_video_count, regressions, differences);
  compareValue("joined_date", crawler.joined_date, snapshot.joined_date, regressions, differences);
  const businessLinkCount = nonnegativeInteger(business.link_count) ?? 0;
  compareCount("links", crawler.link_count, businessLinkCount, regressions);
  const completeAgentFacts = new Set(agent.facts.filter((fact) => fact.complete).map((fact) => fact.field));
  const businessFactKeys = uniqueTextList(business.fact_keys);
  for (const field of businessFactKeys) {
    if (!completeAgentFacts.has(field)) regressions.push({
      field: `agent.${field}`,
      crawler_complete: false,
      business_complete: true,
    });
  }
  const targetContentCount = nonnegativeInteger(business.content_count) ?? 0;
  compareCount("video_window", video.items.length, targetContentCount, regressions);
  return {
    target_exists: true,
    passed: regressions.length === 0,
    active_watermark: text(search.watermark),
    regressions: regressions.sort((left, right) => compareText(left.field, right.field)),
    value_differences: differences.sort((left, right) => compareText(left.field, right.field)),
    target_profile_fact_count: businessFactKeys.length,
    target_content_count: targetContentCount,
  };
}

function groupRows(rows, key) {
  const grouped = new Map();
  for (const wrapper of rows) {
    const row = object(wrapper.row ?? wrapper);
    const value = text(row[key]);
    if (!value) continue;
    const current = grouped.get(value) ?? [];
    current.push(row);
    grouped.set(value, current);
  }
  return grouped;
}

function oneRowMap(rows, key) {
  return new Map(rows.map((wrapper) => {
    const row = object(wrapper.row ?? wrapper);
    return [text(row[key]), { ...wrapper, row }];
  }).filter(([value]) => Boolean(value)));
}

function sourceMap(rows) {
  return new Map(rows.map((row) => [
    `${text(row.channel_id)}\u0000${text(row.observation_kind)}`,
    row,
  ]));
}

function reasonCounts(channels, { distinctChannels = false } = {}) {
  const counts = new Map();
  for (const channel of channels) {
    const codes = distinctChannels
      ? new Set(channel.readiness_reasons.map((reason) => reason.code))
      : channel.readiness_reasons.map((reason) => reason.code);
    for (const code of codes) {
      counts.set(code, (counts.get(code) ?? 0) + 1);
    }
  }
  return Object.fromEntries([...counts.entries()].sort(([left], [right]) => compareText(left, right)));
}

export async function generatePublicationReadinessReport({
  crawlerQuery,
  businessQuery,
  asOf = new Date(),
  channelIds = null,
}) {
  if (typeof crawlerQuery !== "function") throw new TypeError("crawlerQuery is required");
  if (typeof businessQuery !== "function") throw new TypeError("businessQuery is required");
  const asOfIso = timestamp(asOf);
  if (!asOfIso) throw new TypeError("asOf must be a valid timestamp");
  const filter = channelFilter(channelIds);
  const crawlerResults = (async () => [
    await crawlerQuery(CHANNELS_SQL, [filter]),
    await crawlerQuery(CONTENTS_SQL, [filter, asOfIso]),
    await crawlerQuery(AGENTS_SQL, [filter]),
    await crawlerQuery(SOURCES_SQL, [filter]),
  ])();
  const [[channelResult, contentResult, agentResult, sourceResult], businessResult] = await Promise.all([
    crawlerResults,
    businessQuery(BUSINESS_SQL, [filter]),
  ]);
  const channelRows = resultRows(channelResult, "channels");
  const contentRows = resultRows(contentResult, "contents");
  const agentRows = resultRows(agentResult, "agents");
  const sourceRows = resultRows(sourceResult, "sources");
  const businessRows = resultRows(businessResult, "business-current");
  const contents = groupRows(contentRows, "channel_id");
  const agents = oneRowMap(agentRows, "channel_id");
  const sources = sourceMap(sourceRows);
  const businesses = new Map(businessRows.map((row) => [text(row.channel_id), row]).filter(([id]) => Boolean(id)));
  const channels = [];
  const sourceIds = new Set();
  for (const wrapper of channelRows) {
    const row = object(wrapper.row ?? wrapper);
    const channelId = text(row.channel_id);
    if (!channelId) continue;
    sourceIds.add(channelId);
    const channel = buildChannelReadiness({
      row,
      source: sources.get(`${channelId}\u0000about`),
    });
    const video = buildVideoReadiness({
      rows: contents.get(channelId) ?? [],
      source: sources.get(`${channelId}\u0000video`),
      channelId,
      asOf: asOfIso,
    });
    const agentWrapper = agents.get(channelId) ?? {};
    const agent = buildAgentReadiness({
      row: agentWrapper.row,
      config: agentWrapper.config,
      source: sources.get(`${channelId}\u0000agent`),
    });
    const regression = businessRegression({
      business: businesses.get(channelId),
      channel,
      video,
      agent,
    });
    const issues = [
      ...channel.issues,
      ...video.issues,
      ...agent.issues,
      ...regression.regressions.map((item) => ({
        domain: "regression",
        code: item.reason === "large_drop"
          ? "business_field_large_drop_regression"
          : "business_field_completeness_regression",
        field: item.field,
      })),
    ];
    const readinessReasons = sortedIssues(issues);
    channels.push({
      channel_id: channelId,
      lifecycle_status: text(row.status),
      contract_version: PUBLICATION_CONTRACT_VERSION,
      policy_version: PUBLICATION_POLICY_VERSION,
      domains: { channel, video, agent },
      business_regression: regression,
      baseline_eligible: readinessReasons.length === 0,
      readiness_reasons: readinessReasons,
    });
  }
  channels.sort((left, right) => compareText(left.channel_id, right.channel_id));
  const targetOnly = [...businesses.keys()].filter((id) => !sourceIds.has(id)).sort(compareText);
  const sourceOnly = [...sourceIds].filter((id) => !businesses.has(id)).sort(compareText);
  return {
    report_version: PUBLICATION_READINESS_REPORT_VERSION,
    report_as_of: asOfIso,
    contract_version: PUBLICATION_CONTRACT_VERSION,
    policies: {
      publication: PUBLICATION_POLICY_VERSION,
      video_window: VIDEO_WINDOW_POLICY_VERSION,
      agent_taxonomy: AGENT_TAXONOMY_VERSION,
      business_large_drop_ratio: BUSINESS_LARGE_DROP_RATIO,
    },
    scope: {
      requested_channel_count: filter?.length ?? null,
      crawler_channel_count: channels.length,
      business_active_channel_count: businesses.size,
      source_only_channel_count: sourceOnly.length,
      target_only_channel_count: targetOnly.length,
      source_only_channel_ids: sourceOnly,
      target_only_channel_ids: targetOnly,
    },
    summary: {
      total_channels: channels.length,
      baseline_eligible: channels.filter((channel) => channel.baseline_eligible).length,
      not_ready: channels.filter((channel) => !channel.baseline_eligible).length,
      channel_ready: channels.filter((item) => item.domains.channel.ready).length,
      video_ready: channels.filter((item) => item.domains.video.ready).length,
      agent_ready: channels.filter((item) => item.domains.agent.ready).length,
      business_target_channels: channels.filter((item) => item.business_regression.target_exists).length,
      business_regression_passed: channels.filter((item) => (
        item.business_regression.target_exists && item.business_regression.passed
      )).length,
      business_regression_not_applicable: channels.filter((item) => !item.business_regression.target_exists).length,
      reason_counts: reasonCounts(channels, { distinctChannels: true }),
      reason_occurrence_counts: reasonCounts(channels),
    },
    channels,
  };
}
