function boundedNumber(value, fallback, minimum, maximum) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.max(minimum, Math.min(maximum, number));
}

function timestamp(value) {
  if (!value) return null;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function unitNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 && number <= 1 ? number : null;
}

export function incrementalVideoPlannerConfig(plan, env = process.env) {
  const supportedVersion = String(env.INCREMENTAL_VIDEO_PLANNER_VERSION || "video-plan-1").trim();
  if (plan.planner_config_version !== supportedVersion) {
    throw new Error(
      `unsupported Video Planner version ${plan.planner_config_version}; expected ${supportedVersion}`,
    );
  }
  return {
    version: supportedVersion,
    discoveryMaxPages: Math.floor(boundedNumber(
      env.INCREMENTAL_DISCOVERY_MAX_PAGES,
      20,
      1,
      100,
    )),
    discoveryCatchUpMaxItems: Math.floor(boundedNumber(
      env.INCREMENTAL_DISCOVERY_CATCH_UP_MAX_ITEMS,
      50,
      1,
      500,
    )),
    recentWindowDays: Math.floor(boundedNumber(
      env.INCREMENTAL_RECENT_WINDOW_DAYS,
      30,
      1,
      365,
    )),
    staleAfterDays: Math.floor(boundedNumber(
      env.INCREMENTAL_VIDEO_STALE_AFTER_DAYS,
      7,
      1,
      365,
    )),
    defaultCollectionPriority: boundedNumber(
      env.INCREMENTAL_DEFAULT_COLLECTION_PRIORITY,
      0.5,
      0,
      1,
    ),
    defaultChangeProbability: boundedNumber(
      env.INCREMENTAL_DEFAULT_CHANGE_PROBABILITY,
      0.5,
      0,
      1,
    ),
    defaultInteractionNeed: boundedNumber(
      env.INCREMENTAL_DEFAULT_INTERACTION_NEED,
      0.5,
      0,
      1,
    ),
    minimumRefreshScore: boundedNumber(
      env.INCREMENTAL_VIDEO_MIN_REFRESH_SCORE,
      0.55,
      0,
      1,
    ),
    changeEwmaAlpha: boundedNumber(
      env.INCREMENTAL_VIDEO_CHANGE_EWMA_ALPHA,
      0.4,
      0.01,
      1,
    ),
  };
}

export function orderedDiscoveryAnchors(rows = [], limit = 20) {
  const anchors = [];
  const seen = new Set();
  for (const row of rows) {
    const id = String(row?.video_id ?? row?.source_content_id ?? "").trim();
    if (!id || seen.has(id)) continue;
    const published = new Date(row?.published_at ?? row?.published_day ?? "");
    if (Number.isNaN(published.getTime())) continue;
    seen.add(id);
    anchors.push({ id, published_day: published.toISOString().slice(0, 10) });
    if (anchors.length >= Math.max(1, limit)) break;
  }
  return anchors;
}

function scoreRecent(row, { nowMs, staleAfterMs, config }) {
  const playerAt = timestamp(row.player_last_observed_at);
  const publishedAt = timestamp(row.published_at);
  const staleness = playerAt == null ? 1 : Math.min(1, Math.max(0, (nowMs - playerAt) / staleAfterMs));
  const freshness = publishedAt == null
    ? 0
    : Math.max(0, 1 - ((nowMs - publishedAt) / (30 * 86400000)));
  const learnedProbability = unitNumber(row.video_change_probability);
  const changeProbability = learnedProbability != null
    ? learnedProbability
    : (0.60 * config.defaultChangeProbability) + (0.40 * freshness);
  const nextAt = timestamp(row.next_last_observed_at);
  const nextStaleness = nextAt == null ? 1 : Math.min(1, Math.max(0, (nowMs - nextAt) / staleAfterMs));
  const playerScore = (0.60 * staleness) + (0.40 * changeProbability);
  const enrichPending = row.enrich_pending === true;
  return {
    ...row,
    staleness,
    change_probability: changeProbability,
    player_score: playerScore,
    next_score: (0.70 * nextStaleness) + (0.30 * freshness),
    enrich_pending: enrichPending,
    stale: staleness >= 1,
    refresh_candidate: enrichPending || playerAt == null || playerScore >= config.minimumRefreshScore,
  };
}

export function planRecentVideoSampling(rows, {
  plan,
  config,
  excludeVideoIds = [],
  now = new Date(),
} = {}) {
  const excluded = new Set(excludeVideoIds.map(String));
  const nowMs = new Date(now).getTime();
  const staleAfterMs = config.staleAfterDays * 86400000;
  const pool = rows.map((row) => scoreRecent(row, { nowMs, staleAfterMs, config }));
  const eligible = pool.filter((row) => !excluded.has(String(row.source_content_id)));
  const candidates = eligible.filter((row) => row.refresh_candidate);
  const pendingCandidates = candidates.filter((row) => row.enrich_pending);
  const scoredCandidates = candidates.filter((row) => !row.enrich_pending);
  const staleRatio = eligible.length === 0
    ? 0
    : eligible.filter((row) => row.stale).length / eligible.length;
  const demand = scoredCandidates.reduce((total, row) => total + row.player_score, 0);
  const coverageMultiplier = 0.50 + (0.50 * config.defaultCollectionPriority);
  const suggestedPlayer = candidates.length === 0
    ? 0
    : Math.min(
      candidates.length,
      pendingCandidates.length + Math.ceil(demand * coverageMultiplier),
    );
  const playerQuota = Math.min(
    candidates.length,
    suggestedPlayer,
    Math.floor(plan.capacity.player_cap * plan.capacity.factor),
  );
  const playerRows = [...candidates]
    .sort((left, right) => Number(right.enrich_pending) - Number(left.enrich_pending)
      || right.player_score - left.player_score
      || String(left.content_key).localeCompare(String(right.content_key)))
    .slice(0, playerQuota);
  const nextRatio = 0.10 + (0.25 * config.defaultInteractionNeed);
  const nextQuota = Math.min(
    playerRows.length,
    plan.capacity.next_cap,
    Math.ceil(playerRows.length * nextRatio),
  );
  const nextIds = new Set([...playerRows]
    .sort((left, right) => right.next_score - left.next_score
      || String(left.content_key).localeCompare(String(right.content_key)))
    .slice(0, nextQuota)
    .map((row) => String(row.source_content_id)));
  return {
    recent_count: pool.length,
    stale_ratio: Number(staleRatio.toFixed(6)),
    candidate_count: candidates.length,
    suggested_player_quota: suggestedPlayer,
    player_quota: playerRows.length,
    next_quota: nextIds.size,
    rows: playerRows.map((row) => ({
      ...row,
      collect_next: nextIds.has(String(row.source_content_id)),
    })),
  };
}
