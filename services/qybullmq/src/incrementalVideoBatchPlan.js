// Shared target policy: storage and reservations are supplied by the execution owner.
// Keep I/O order stable: the PostgreSQL adapter invokes this inside its existing
// fenced transaction. A node snapshot adapter must carry the same eligibility.
import { planRecentVideoSampling } from './incrementalVideoPlanner.js';
import { publicationEvidenceFromFields } from './publicationTimeEvidence.js';
function text(value) {
  if (value === null || value === undefined) return null;
  const output = String(value).trim();
  return output || null;
}
export function uploadsPublishedFacts(entry) {
  const evidence = publicationEvidenceFromFields({
    published_at: text(entry?.published_at) ?? text(entry?.published_day),
    published_at_status: entry?.published_at_status,
    published_at_precision: entry?.published_at_precision,
    published_at_source: entry?.published_at_source,
  });
  return evidence.published_at ? evidence : null;
}
function jsonCheckpointValue(value) {
  if (value == null) return null;
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new TypeError('checkpoint value is not JSON serializable');
  return JSON.parse(serialized);
}

function dispositionRecheckEntry(entry, prior) {
  return {
    ...entry,
    disposition_recheck: {
      candidate_id: Number(prior.candidate_id),
      prior_kind: text(prior.disposition),
      prior_reason_code: text(prior.result_json?.disposition?.reason_code),
      scheduled_at: prior.next_attempt_at == null
        ? null
        : new Date(prior.next_attempt_at).toISOString(),
    },
  };
}

export function scannedVideoDispositionWork(entries, priorByVideoId, observedAt, {
  allowDueRechecks = true,
} = {}) {
  const observedAtMs = Date.parse(observedAt);
  const pendingDeferredVideoIds = [];
  const workEntries = entries.flatMap((entry) => {
    const prior = priorByVideoId.get(entry.id);
    const priorKind = text(prior?.disposition);
    if (!["deferred", "terminal_excluded"].includes(priorKind)) return [entry];
    if (String(prior.next_attempt_at).toLowerCase() === "infinity") return [];
    if (!allowDueRechecks) {
      if (priorKind === "deferred") pendingDeferredVideoIds.push(entry.id);
      return [];
    }
    const nextAttemptAtMs = Date.parse(prior.next_attempt_at);
    const due = !Number.isFinite(nextAttemptAtMs) || nextAttemptAtMs <= observedAtMs;
    if (due) return [dispositionRecheckEntry(entry, prior)];
    if (priorKind === "deferred") pendingDeferredVideoIds.push(entry.id);
    return [];
  });
  return { workEntries, pendingDeferredVideoIds };
}

export function checkpointItems(discoveryEntries, samplingPlan) {
  const detailEligibleFirstSeen = discoveryEntries.filter(
    (entry) => entry.disposition_recheck
      || (entry.is_upcoming !== true && entry.is_live !== true),
  );
  const items = [
    ...detailEligibleFirstSeen.map((entry, ordinal) => ({
      phase: "first_seen",
      ordinal,
      video_id: entry.id,
      target_json: jsonCheckpointValue(entry),
    })),
    ...samplingPlan.rows.map((row, ordinal) => ({
      phase: "recent",
      ordinal,
      video_id: row.source_content_id,
      target_json: jsonCheckpointValue(row),
    })),
  ];
  const identities = new Set(items.map((item) => item.video_id));
  if (identities.size !== items.length) {
    throw new Error("Incremental YouTubeJS checkpoint target appears in more than one Phase");
  }
  return items;
}


export async function prepareIncrementalVideoBatch({ scan, observedAt, samplingPlanInput, config, state }) {
  const ids = scan.entries.map(entry => entry.id);
  const known = await state.knownVideoIds(ids);
  const latestDispositions = await state.latestVideoDispositions(ids.filter(id => !known.has(id)));
  const scannedWork = scannedVideoDispositionWork(
    scan.entries.filter(entry => !known.has(entry.id)), latestDispositions, observedAt,
    { allowDueRechecks: scan.complete === true },
  );
  let discoveryEntries = scannedWork.workEntries;
  let recoveredFirstSeen = [];
  let samplingPlan = { recent_count: 0, stale_ratio: 0, candidate_count: 0,
    suggested_player_quota: 0, player_quota: 0, next_quota: 0, rows: [] };
  if (scan.complete === true && !scan.empty_uploads) {
    recoveredFirstSeen = await state.pendingFirstSeen();
    discoveryEntries = [...discoveryEntries, ...await state.dueDispositions(scan.entries, observedAt)];
    const recentRows = await state.recentRows(scan.entries);
    const planned = planRecentVideoSampling(recentRows, {
      plan: samplingPlanInput, config,
      excludeVideoIds: discoveryEntries.map(entry => entry.id), now: new Date(observedAt),
    });
    samplingPlan = await state.reserveSampling(planned);
  }
  return { discoveryEntries, recoveredFirstSeen, samplingPlan,
    pendingDeferredVideoIds: scannedWork.pendingDeferredVideoIds,
    items: scan.complete === true ? checkpointItems(discoveryEntries, samplingPlan) : [] };
}

function integer(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : null;
}
export function projectDueVideoDispositionEntries(rows, scanEntries) {
  const scannedIds = new Set(scanEntries.map((entry) => text(entry?.id)).filter(Boolean));
  const maxPosition = scanEntries.reduce(
    (current, entry) => Math.max(current, integer(entry?.position) ?? 0),
    0,
  );
  return rows
    .filter((row) => !scannedIds.has(text(row.source_content_id)))
    .map((row, index) => {
      const flat = row.result_json?.flat ?? {};
      return {
        id: text(row.source_content_id),
        position: maxPosition + index + 1,
        title: text(row.title) ?? text(flat.title),
        thumbnail_url: text(row.thumbnail_url) ?? text(flat.thumbnail_url),
        published_day: text(flat.published_day),
        disposition_recheck: {
          candidate_id: Number(row.candidate_id),
          prior_kind: text(row.disposition),
          prior_reason_code: text(row.result_json?.disposition?.reason_code),
          scheduled_at: row.next_attempt_at == null
            ? null
            : new Date(row.next_attempt_at).toISOString(),
        },
      };
    })
    .filter((entry) => entry.id);
}
