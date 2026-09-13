import { fetchIncrementalYoutubeJsVideoDetail, normalizeProbeScan } from '../incrementalYoutubeJsVideo.js';
import { planIncrementalVideoSnapshot } from '../incrementalVideoSnapshot.js';
import { createVideoDetailApiFallback, withVideoFallbackExecution } from '../videoDetailApiFallback.js';
import { videoApiPendingError } from '../videoApiContinuation.js';
import { selectYoutubeFailure } from '../youtubeFailurePolicy.js';
import { channelSnapshotWire, toChannelWire } from './channelWire.js';
import { hash } from './protocol.js';
import { wholeChannelBytes } from './wholeChannelProtocol.js';

// Called inside the existing managed Rota/YouTube runtime. Only local journal
// writes occur between detail requests; no center commands or business SQL.
export async function collectWholeChannel({ input, youtube, journal, signal }) {
  signal.throwIfAborted();
  await journal.put('input', input);
  if (journal.get('result')) return journal.get('result');
  const plan = input.plan;
  const video = input.video;
  let snapshot, stage = 'open_channel', activeItem = null;
  const open = async () => snapshot ??= await youtube.openChannel(plan.channel_id, { ...input.channelOptions, signal });
  let targets = video?.resumeBatch?.items ?? [];
  try {
    if (input.collectAbout && input.previousEvidence?.about && !journal.get('about')) await journal.put('about', input.previousEvidence.about);
    if (input.collectAbout && !journal.get('about')) await journal.put('about', channelSnapshotWire(await open()));
    if (video) {
      stage = 'scan_uploads';
      if (!journal.get('scan')) {
        const scan = video.resumeBatch?.scan_json ?? input.previousEvidence?.scan ?? normalizeProbeScan(await (await open()).scanUploads({
          anchors: video.anchors, maxPages: video.config.discoveryMaxPages,
          catchUpMaxItems: video.config.discoveryCatchUpMaxItems,
        }), video.anchors, video.config);
        await journal.put('scan', toChannelWire(scan));
      }
      if (!journal.get('targets')) {
        targets = video.resumeBatch?.items ?? (await planIncrementalVideoSnapshot(video, journal.get('scan'))).items;
        await journal.put('targets', targets);
      }
      targets = journal.get('targets');
      stage = 'video_detail';
      for (const item of targets) {
        signal.throwIfAborted();
        activeItem = item;
        const key = `detail:${item.phase}:${item.video_id}`;
        if (['captured', 'settled_error'].includes(item.status)) continue;
        if (journal.get(key)?.status === 'api_pending') break;
        if (journal.get(key)) continue;
        const cached = input.previousEvidence?.items.find(prior => prior.video_id === item.video_id && prior.phase === item.phase);
        if (cached?.status === 'captured') {
          // Revalidate against today's frozen target requirements. A prior
          // metrics-only result cannot satisfy a newly required full repair.
          let detail;
          try { detail = await fetchIncrementalYoutubeJsVideoDetail(item.video_id, { signal, phase:item.phase,
            target:item.target_json, fetchYoutubeJs:async()=>cached.detail }); }
          catch { signal.throwIfAborted(); }
          if (detail) { await journal.put(key, {status:'captured',detail:toChannelWire(detail)}); continue; }
        }
        let lastError;
        const fallback = createVideoDetailApiFallback({
          query: async () => ({ rows: [] }),
          withTransaction: async () => { throw new Error('UNEXPECTED_NODE_SQL'); },
          loadSettings: async () => ({ fallbackMode: input.apiPolicy.enabled ? 'emergency' : 'disabled',
            apiKeys: input.apiPolicy.available ? ['central-only'] : [], dailyRequestLimit: input.apiPolicy.dailyRequestLimit }),
          request: async (_transaction, request) => journal.put(`api:${key}`, toChannelWire(request)),
          wait: async (_query, requestId) => { throw videoApiPendingError(requestId); },
        });
        let outcome;
        if (journal.get(`api:${key}`)) outcome = { status: 'api_pending', api_request: journal.get(`api:${key}`) };
        else try {
          const detail = await withVideoFallbackExecution({ getBudget: async () => input.networkBudget }, () => fetchIncrementalYoutubeJsVideoDetail(item.video_id, {
            signal, phase: item.phase, target: item.target_json, videoApiFallback: fallback,
            checkpoint: { run_id: video.runId, cycle_key: video.cycleKey, attempt_count: Number(item.attempt_count ?? 0) + 1 },
            fetchYoutubeJs: async (id, options) => {
              try { return await youtube.fetchDetail(id, options); } catch (error) { lastError = error; throw error; }
            },
          }));
          outcome = { status: 'captured', detail: toChannelWire(detail) };
        } catch (error) {
          signal.throwIfAborted();
          if (error.code === 'VIDEO_API_PENDING') outcome = { status: 'api_pending', api_request: journal.get(`api:${key}`), error: lastError ? toChannelWire(lastError) : null };
          else if (['content_terminal', 'parser_runtime'].includes(selectYoutubeFailure({ error }).decision.kind)) {
            outcome = { status: 'settled_error', error: toChannelWire(error) };
          } else throw error;
        }
        await journal.put(key, outcome);
        if (outcome.status === 'api_pending') break;
      }
    }
  } catch (error) {
    signal.throwIfAborted();
    await journal.put('failure', { stage, video_id: activeItem?.video_id ?? null,
      phase: activeItem?.phase ?? null, error: toChannelWire(error) });
  }
  signal.throwIfAborted();
  return sealWholeChannelResult(input, journal, targets);
}

async function sealWholeChannelResult(input, journal, targets) {
  const plan = input.plan;
  const failure = journal.get('failure') ?? null;
  const result = { version: 1, plan_id: plan.plan_id, channel_id: plan.channel_id,
    generation: input.generation, input_sha256: hash(wholeChannelBytes(input)),
    about: journal.get('about') ?? null, scan: journal.get('scan') ?? null, failure,
    items: targets.map(item => ({ phase: item.phase, ordinal: item.ordinal, video_id: item.video_id,
      ...(journal.get(`detail:${item.phase}:${item.video_id}`)
        ?? (journal.get(`api:detail:${item.phase}:${item.video_id}`)
          ? { status: 'api_pending', api_request: journal.get(`api:detail:${item.phase}:${item.video_id}`) }
          : ['captured', 'settled_error'].includes(item.status) ? { status: 'already_checkpointed' }
          : failure?.video_id === item.video_id && failure?.phase === item.phase
            ? { status: 'retryable', error: failure.error } : { status: 'pending' })) })),
  };
  await journal.put('result', result);
  return result;
}

// A restarted process cannot resurrect the managed browser identity. Return
// durable progress to its original owner before retiring that identity; the
// center resumes unfinished items under the existing execution-generation fence.
// This recovery path never calls YouTube or authorizes an expired upload.
export async function interruptWholeChannel(journal) {
  if (journal.get('result')) return journal.get('result');
  const input = journal.get('input');
  if (!input) throw new Error('WHOLE_CHANNEL_JOURNAL_INCOMPLETE');
  let targets = journal.get('targets') ?? input.video?.resumeBatch?.items ?? [];
  if (input.video && journal.get('scan') && !journal.get('targets')) {
    targets = input.video.resumeBatch?.items ?? (await planIncrementalVideoSnapshot(input.video, journal.get('scan'))).items;
    await journal.put('targets', targets);
  }
  const unfinished = targets.find(item => !['captured', 'settled_error'].includes(item.status)
    && !journal.get(`detail:${item.phase}:${item.video_id}`)
    && !journal.get(`api:detail:${item.phase}:${item.video_id}`));
  const apiPending = targets.some(item => journal.get(`detail:${item.phase}:${item.video_id}`)?.status === 'api_pending'
    || journal.get(`api:detail:${item.phase}:${item.video_id}`));
  const stage = input.collectAbout && !journal.get('about') ? 'open_channel'
    : input.video && !journal.get('scan') ? 'scan_uploads' : unfinished && !apiPending ? 'video_detail' : null;
  if (stage && !journal.get('failure')) await journal.put('failure', { stage,
    video_id: stage === 'video_detail' ? unfinished.video_id : null,
    phase: stage === 'video_detail' ? unfinished.phase : null,
    error: toChannelWire(Object.assign(new Error('Remote channel process interrupted; resume remaining checkpoints'),
      { code: 'REMOTE_CHANNEL_INTERRUPTED' })),
  });
  return sealWholeChannelResult(input, journal, targets);
}
