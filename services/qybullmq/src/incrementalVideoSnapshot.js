import { prepareIncrementalVideoBatch, projectDueVideoDispositionEntries, uploadsPublishedFacts } from './incrementalVideoBatchPlan.js';

// SQL selects eligible stored candidates at dispatch time. Only undated content
// needs new scan evidence to decide its recent-window membership on the node.
export function recentRowsFromSnapshot(rows, scanEntries, { planDay, recentWindowDays }) {
  const publication = new Map();
  for (const entry of scanEntries) {
    const evidence = uploadsPublishedFacts(entry);
    if (!evidence) continue;
    const prior = publication.get(entry.id);
    if (!prior || Date.parse(evidence.published_at) > Date.parse(prior)) publication.set(entry.id, evidence.published_at);
  }
  const cutoff = Date.parse(`${planDay}T00:00:00Z`) - recentWindowDays * 86400000;
  return rows.map(row => {
    const published = row.stored_publication?.published_at ?? row.published_at ?? publication.get(row.source_content_id) ?? null;
    return { ...row, sampling_published_at: published, published_at: published };
  }).filter(row => row.enrich_pending || Date.parse(row.published_at) >= cutoff)
    .sort((a, b) => (Date.parse(b.published_at) || 0) - (Date.parse(a.published_at) || 0)
      || String(a.content_key).localeCompare(String(b.content_key)));
}

export async function planIncrementalVideoSnapshot(snapshot, scan) {
  if (snapshot?.version !== 1 || snapshot.kind !== 'incremental-video-snapshot'
      || snapshot.resumeBatch) throw new TypeError('fresh incremental video snapshot required');
  const known = new Set(snapshot.knownVideoIds);
  const dispositions = new Map(snapshot.latestDispositions.map(row => [row.source_content_id, row]));
  return prepareIncrementalVideoBatch({ scan, observedAt: snapshot.observedAt,
    samplingPlanInput: snapshot.samplingPlanInput, config: snapshot.config, state: {
      knownVideoIds: async ids => new Set(ids.filter(id => known.has(id))),
      latestVideoDispositions: async ids => new Map(ids.filter(id => dispositions.has(id)).map(id => [id, dispositions.get(id)])),
      pendingFirstSeen: async () => snapshot.pendingFirstSeen,
      dueDispositions: async entries => projectDueVideoDispositionEntries(snapshot.dueDispositionRows, entries),
      recentRows: async entries => recentRowsFromSnapshot(snapshot.recentRows, entries, {
        planDay: snapshot.samplingPlanInput.plan_day, recentWindowDays: snapshot.config.recentWindowDays,
      }),
      // Every returned row already contains its centrally reserved fence. This
      // callback does not create a second task owner on the collecting node.
      reserveSampling: async planned => planned,
    } });
}
