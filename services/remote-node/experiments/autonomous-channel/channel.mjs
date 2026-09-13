// Not imported by any production entry point. No DB, queue, credentials or
// YouTube network connection is created by this experiment.
import { prepareIncrementalVideoBatch } from '../../../qybullmq/src/incrementalVideoBatchPlan.js';
import { incrementalVideoPlannerConfig } from '../../../qybullmq/src/incrementalVideoPlanner.js';
import { fetchIncrementalYoutubeJsVideoDetail, normalizeProbeScan } from '../../../qybullmq/src/incrementalYoutubeJsVideo.js';
import { createVideoDetailApiFallback, withVideoFallbackExecution } from '../../../qybullmq/src/videoDetailApiFallback.js';
import { selectYoutubeFailure } from '../../../qybullmq/src/youtubeFailurePolicy.js';
import { videoApiPendingError } from '../../../qybullmq/src/videoApiContinuation.js';
import { channelSnapshotWire, toChannelWire } from '../../../qybullmq/src/remoteNodes/channelWire.js';
import { digest } from './journal.mjs';

// State is an immutable dispatch snapshot adapter, not preselected video IDs.
// Discovery is performed on the node; the shared policy selects targets there.
export async function collectChannel({ input, journal, youtube, signal,
  afterCheckpoint = async () => {}, getBudget = null }) {
  signal?.throwIfAborted();
  const { plan, runId, cycleKey, observedAt, anchors } = input;
  if (input.version !== 1 || !Number.isSafeInteger(input.generation) || input.generation < 1
      || !plan?.plan_id || !plan.channel_id || !runId || !cycleKey
      || !Number.isFinite(Date.parse(observedAt))) throw new Error('INVALID_CHANNEL_INPUT');
  // An immutable input fingerprint prevents reuse under a different generation,
  // changed policy, or changed channel snapshot.
  await journal.put('input', input);
  if (journal.get('result')) return journal.get('result');
  const config = input.config ?? incrementalVideoPlannerConfig(plan);
  if (config.version !== plan.planner_config_version) throw new Error('PLANNER_VERSION_CONFLICT');
  let snapshot;
  const open = async () => {
    signal?.throwIfAborted();
    snapshot ??= await youtube.openChannel(plan.channel_id, { about: plan.task_mask.about, signal });
    return snapshot;
  };
  if (!journal.get('about')) {
    await journal.put('about', channelSnapshotWire(await open()));
    await afterCheckpoint('about');
  }
  if (plan.task_mask.video) {
    if (!journal.get('scan')) {
      const raw = await (await open()).scanUploads({ anchors,
        maxPages: config.discoveryMaxPages, catchUpMaxItems: config.discoveryCatchUpMaxItems });
      signal?.throwIfAborted();
      await journal.put('scan', toChannelWire(normalizeProbeScan(raw, anchors, config)));
      await afterCheckpoint('scan');
    }
    if (!journal.get('batch')) {
      const batch = await prepareIncrementalVideoBatch({ scan: journal.get('scan'), observedAt,
        samplingPlanInput: { ...plan, capacity: { ...plan.capacity, factor: 1,
          player_cap: Math.floor(plan.capacity.player_cap * plan.capacity.factor) } }, config,
        state: isolatedSnapshotState(input.snapshot) });
      await journal.put('batch', batch);
      await afterCheckpoint('batch');
    }
    for (const item of journal.get('batch').items) {
      signal?.throwIfAborted();
      const key = `detail:${item.phase}:${item.video_id}`;
      if (journal.get(key)?.status === 'api_pending') break;
      if (journal.get(key)) continue;
      // Reuse the existing parser retry/API eligibility policy. Its persistence
      // adapter is a local outbox; this worker neither owns API keys nor waits
      // for API execution. No database call occurs inside the detail loop.
      const fallback = createVideoDetailApiFallback({
        query: async () => ({ rows: [] }),
        withTransaction: async () => { throw new Error('UNEXPECTED_CENTRAL_TRANSACTION'); },
        loadSettings: async () => ({ ...input.apiPolicy,
          apiKeys: input.apiPolicy.enabled ? ['CENTER_OWNS_CREDENTIALS'] : [],
          fallbackMode: input.apiPolicy.enabled ? 'enabled' : 'disabled' }),
        request: async (_transaction, request) => { await journal.put(`api:${key}`, toChannelWire(request)); },
        wait: async (_query, requestId) => { throw videoApiPendingError(requestId); },
      });
      let outcome;
      // Crash after writing the API outbox must not fetch the same video again.
      if (journal.get(`api:${key}`)) {
        outcome = { status: 'api_pending', request: journal.get(`api:${key}`) };
      } else {
        try {
          const invoke = () => fetchIncrementalYoutubeJsVideoDetail(item.video_id, {
            fetchYoutubeJs: youtube.fetchDetail, signal, phase: item.phase, target: item.target_json,
            checkpoint: { run_id: runId, cycle_key: cycleKey, attempt_count: 1 }, videoApiFallback: fallback,
          });
          // Optional managed Rota budget uses the existing async context.
          const detail = await withVideoFallbackExecution({ getBudget }, invoke);
          signal?.throwIfAborted();
          outcome = { status: 'captured', detail: toChannelWire(detail) };
        } catch (error) {
          signal?.throwIfAborted();
          if (error.code === 'VIDEO_API_PENDING') {
            outcome = { status: 'api_pending', request: journal.get(`api:${key}`) };
          } else {
            const failure = selectYoutubeFailure({ error }).decision;
            if (!['content_terminal', 'parser_runtime'].includes(failure.kind)) throw error;
            outcome = { status: 'settled_error', error: toChannelWire(error) };
          }
        }
      }
      await journal.put(key, outcome);
      await afterCheckpoint(key);
      // Preserve the first-seen barrier. The center must settle this pending
      // item before issuing a continuation for the unfinished targets.
      if (outcome.status === 'api_pending') break;
    }
  }
  signal?.throwIfAborted();
  const batch = journal.get('batch');
  const items = (batch?.items ?? []).map(item => ({ ...item,
    ...(journal.get(`detail:${item.phase}:${item.video_id}`) ?? { status: 'pending' }) }));
  const result = { version: 1, plan_id: plan.plan_id, channel_id: plan.channel_id,
    run_id: runId, cycle_key: cycleKey, generation: input.generation,
    input_sha256: digest(JSON.stringify(input)), observed_at: observedAt,
    state: items.some(item => item.status === 'api_pending') ? 'api_pending' : 'collected',
    about: journal.get('about'), scan: journal.get('scan') ?? null, batch: batch ?? null, items };
  await journal.put('result', result);
  return result;
}

// Trial fixtures carry a complete immutable snapshot. Production must build it
// under the real execution fence and reserve ContentEnrich ownership centrally.
// Publication fallback from new scan evidence is deliberately unsupported here
// until the SQL eligibility projection is extracted and parity-tested.
export function isolatedSnapshotState(snapshot) {
  if (snapshot?.reservation_model !== 'isolated-no-concurrent-enrich') throw new Error('SNAPSHOT_RESERVATIONS_UNVERIFIED');
  if (snapshot.recentRows.some(row => !row.published_at)) throw new Error('SNAPSHOT_PUBLICATION_PROJECTION_UNSUPPORTED');
  const known = new Set(snapshot.knownIds);
  const dispositions = new Map(snapshot.dispositions);
  return {
    knownVideoIds: async ids => new Set(ids.filter(id => known.has(id))),
    latestVideoDispositions: async ids => new Map(ids.filter(id => dispositions.has(id)).map(id => [id, dispositions.get(id)])),
    pendingFirstSeen: async () => snapshot.pendingFirstSeen,
    dueDispositions: async entries => snapshot.dueEntries.filter(entry => !entries.some(scanned => scanned.id === entry.id)),
    recentRows: async () => snapshot.recentRows,
    reserveSampling: async planned => planned,
  };
}

export function resultChunks(result, maxBytes = 512 * 1024) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 512 * 1024) throw new Error('INVALID_CHUNK_LIMIT');
  const bytes = Buffer.from(JSON.stringify(result));
  const manifest = { version: 1, plan_id: result.plan_id, generation: result.generation,
    input_sha256: result.input_sha256, sha256: digest(bytes), bytes: bytes.length,
    count: Math.ceil(bytes.length / maxBytes) };
  const chunks = [];
  for (let index = 0; index < manifest.count; index++) {
    const part = bytes.subarray(index * maxBytes, (index + 1) * maxBytes);
    chunks.push({ index, sha256: digest(part), data: part.toString('base64') });
  }
  return { manifest, chunks };
}

// A durable center-inbox model, NOT the production business transaction/fence.
// One channel per journal. ACK means all bytes were fsynced, not Plan finalized.
export class ChannelInbox {
  constructor({ journal, assertOwner, maxBytes = 32 * 1024 * 1024 }) {
    Object.assign(this, { journal, assertOwner, maxBytes });
  }
  async receive(manifest, chunk) {
    // Exact replay after a lost acknowledgement is accepted even after lease
    // handoff, but a new result from an old owner must still fail the fence.
    const receipt = this.journal.get('receipt');
    if (receipt) {
      if (JSON.stringify(this.journal.get('manifest')) !== JSON.stringify(manifest)) throw new Error('RESULT_CONFLICT');
      return receipt;
    }
    this.assertOwner(manifest);
    if (manifest.version !== 1 || !Number.isSafeInteger(manifest.bytes) || manifest.bytes < 1
        || manifest.bytes > this.maxBytes || !Number.isSafeInteger(manifest.count)
        || manifest.count < 1 || manifest.count > 4096
        || !Number.isSafeInteger(chunk.index) || chunk.index < 0 || chunk.index >= manifest.count
        || typeof chunk.data !== 'string' || chunk.data.length > 700000) throw new Error('INVALID_MANIFEST');
    const bytes = Buffer.from(chunk.data, 'base64');
    if (bytes.length > 512 * 1024 || digest(bytes) !== chunk.sha256) throw new Error('CHUNK_CORRUPT');
    await this.journal.put('manifest', manifest);
    const receivedBytes = [...this.journal.records.entries()]
      .filter(([key]) => key.startsWith('chunk:') && key !== `chunk:${chunk.index}`)
      .reduce((total, [, row]) => total + Buffer.from(row.value.data, 'base64').length, bytes.length);
    if (receivedBytes > manifest.bytes) throw new Error('RESULT_SIZE_CONFLICT');
    await this.journal.put(`chunk:${chunk.index}`, chunk);
    const parts = Array.from({ length: manifest.count }, (_, index) => this.journal.get(`chunk:${index}`));
    if (parts.some(part => !part)) return { durable: false, chunk_received: chunk.index };
    const resultBytes = Buffer.concat(parts.map(part => Buffer.from(part.data, 'base64')));
    if (resultBytes.length !== manifest.bytes || digest(resultBytes) !== manifest.sha256) throw new Error('RESULT_CORRUPT');
    const result = JSON.parse(resultBytes);
    if (result.plan_id !== manifest.plan_id || result.generation !== manifest.generation
        || result.input_sha256 !== manifest.input_sha256) throw new Error('RESULT_IDENTITY_CONFLICT');
    this.assertOwner(manifest);
    // Receipt + intact chunks are the durable result; do not write another copy.
    return this.journal.put('receipt', { durable: true, state: 'received',
      sha256: manifest.sha256, plan_id: manifest.plan_id, generation: manifest.generation });
  }
}
