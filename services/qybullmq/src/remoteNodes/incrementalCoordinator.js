import { currentVideoFallbackBudget } from '../videoDetailApiFallback.js';
import { requestVideoApiDetail } from '../videoApiBatchRequests.js';
import { createIncrementalVideoDispatchSnapshot, adoptIncrementalVideoDispatchResult, renewIncrementalVideoDispatchSnapshot, releaseUnadoptedIncrementalVideoSnapshot } from '../incrementalYoutubeJsVideo.js';
import { createIncrementalTransactionScope } from './incrementalTransactionScope.js';
import { setTimeout as delay } from 'node:timers/promises';
import { IncrementalChannelRunner, incrementalDomainResult } from '../incrementalChannelRunner.js';
import { IncrementalRunStore } from '../incrementalRunStore.js';
import { IncrementalAgentBacklog } from '../incrementalAgentBacklog.js';
import { executeIncrementalAbout } from '../incrementalAbout.js';
import { executeIncrementalYoutubeJsVideo, fetchIncrementalYoutubeJsVideoDetail } from '../incrementalYoutubeJsVideo.js';
import { INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../incrementalPlan.js';
import { runWithChannelExecution } from '../channelExecutionContext.js';
import { assertVideoApiNetworkAllowed, isVideoApiHandoff, withVideoApiReplay } from '../videoApiContinuation.js';
import { withUploadsCountryExecution } from '../youtubeUploadsCountry.js';
import { planFromTask } from './channelPlanContract.js';
import { decodeResult } from './protocol.js';
import { wholeChannelBytes } from './wholeChannelProtocol.js';
import { fromChannelWire } from './channelWire.js';
import { assertRemoteIncrementalBusinessFence } from './incrementalBusinessFence.js';

// Reuse the actual clock/Plan runner. Only YouTube requests cross the transport;
// SQL, checkpoints, Agent, publication and API fallback stay on the center.
export async function runRemoteIncrementalPlan({ channelStore, lease, assertBusinessFence,
  createApiFallback = null, wholeChannels = null, loadWholeApiPolicy = null, pollMs = 100, signal = new AbortController().signal, apiReplayRequestId = null, onProgress = () => {} }) {
  if (typeof assertBusinessFence !== 'function') throw new TypeError('transactional business fence required');
  if (process.env.YOUTUBEJS_VIDEO_API_BATCH_FALLBACK === 'true' && !createApiFallback) {
    throw new TypeError('central API fallback must be provided when enabled');
  }
  const { task, coordinatorId, terminal } = apiReplayRequestId
    ? await channelStore.coordinateApiReplay(lease.task_id, apiReplayRequestId, assertBusinessFence)
    : await channelStore.coordinate(lease);
  if (terminal) return { ok: true, duplicate: true, terminal: true, ...task.applied_result };
  lease = { task_id: task.task_id, generation: task.generation };
  const plan = planFromTask(task);
  const scope = createIncrementalTransactionScope();
  const withTransaction = async (action) => {
    signal.throwIfAborted();
    if (scope.getStore()) return action(scope.getStore());
    const transaction = apiReplayRequestId ? channelStore.apiReplayTransaction.bind(channelStore) : channelStore.transaction.bind(channelStore);
    return transaction(lease, coordinatorId, assertBusinessFence,
      (client) => scope.run(client, () => action(client)));
  };
  const query = (sql, params) => withTransaction((client) => client.query(sql, params));
  const awaitCommand = async (commandId, requestSignal = signal, onPending = null) => {
    for (;;) {
      signal.throwIfAborted(); requestSignal?.throwIfAborted();
      const notification=channelStore.transportSignals?.watch(`task:${task.task_id}`,{timeoutMs:5000,signal:requestSignal??signal});
      try {
        const row = (await query(`SELECT state,payload_gzip FROM remote_ingestion.channel_commands
          WHERE command_id=$1`, [commandId])).rows[0];
        if (row.state === 'received') {
          onProgress('receiving',`command:${commandId}`);
          const { value } = await decodeResult(row.payload_gzip);
          if (value.outcome === 'failure') throw fromChannelWire(value.error);
          return fromChannelWire(value.data);
        }
        await onPending?.();
        if(notification)await notification.wait;
        else await delay(pollMs, null, { signal: requestSignal ?? signal });
      } finally { notification?.cancel(); }
    }
  };
  const request = async (operation, input, requestKey = 'once', requestSignal = signal) => {
    assertVideoApiNetworkAllowed();
    const commandId = await withTransaction((client) => channelStore.request(client, task, operation, input, requestKey));
    onProgress('collecting',operation);
    const result=await awaitCommand(commandId, requestSignal);
    onProgress('applying',operation);return result;
  };
  const runStore = new IncrementalRunStore({ withTransaction });
  const originalMarkDomain = runStore.markDomain.bind(runStore);
  runStore.markDomain = (runId, domain, status, detail) => withTransaction(async (client) => {
    if (status === 'failed') {
      const run = (await client.query('SELECT * FROM crawler.channel_runs WHERE run_id=$1 FOR UPDATE', [runId])).rows[0];
      // A lost COMMIT response must not erase a durably completed domain.
      if (['complete', 'partial', 'queued'].includes(run?.result_json?.domains?.[domain]?.status)) return run;
    }
    return originalMarkDomain(runId, domain, status, detail);
  });
  const originalFinish = runStore.finish.bind(runStore);
  runStore.finish = (runId, options) => withTransaction(async (client) => {
    const run = await originalFinish(runId, options);
    // Existing run completion and remote task completion share this commit.
    await channelStore.complete(client, task, { run_id: runId, status: run.status });
    return run;
  });
  const fallback = createApiFallback?.({ query, withTransaction }) ?? null;
  let whole = null;
  let wholePromise = null;
  const ensureWhole = channelOptions => {
    if (!wholeChannels || apiReplayRequestId) return null;
    return wholePromise ??= (async () => {
      const apiPolicy = loadWholeApiPolicy ? await loadWholeApiPolicy() : { enabled: false, available: false, dailyRequestLimit: 0 };
      const networkBudget = await currentVideoFallbackBudget();
      const prepared = await channelStore.transaction(lease, coordinatorId, assertBusinessFence, async client => {
        const previous = (await client.query('SELECT command_id,input_json FROM remote_ingestion.whole_channel_inputs WHERE task_id=$1 AND generation=$2', [task.task_id, task.generation])).rows[0];
        if (previous) return { commandId: previous.command_id, input: previous.input_json };
        // A newly admitted generation owns recovery. Release only old snapshot
        // reservations that never became part of an authoritative video batch.
        const older = (await client.query('SELECT input_json FROM remote_ingestion.whole_channel_inputs WHERE task_id=$1 AND generation<$2', [task.task_id, task.generation])).rows;
        for (const row of older) await releaseUnadoptedIncrementalVideoSnapshot(client, row.input_json?.video);
        const run = (await client.query('SELECT result_json FROM crawler.channel_runs WHERE run_id=$1', [`incremental:${plan.plan_id}`])).rows[0];
        const done = domain => ['complete','partial','queued'].includes(run?.result_json?.domains?.[domain]?.status);
        const input = { version: 1, generation: task.generation, plan, channelOptions,
          collectAbout: plan.task_mask.about && !done('about'), apiPolicy, networkBudget,
          video: plan.task_mask.video && !done('video') ? await createIncrementalVideoDispatchSnapshot({client,plan,runId:`incremental:${plan.plan_id}`}) : null };
        // A new managed owner may reuse evidence already durably received for
        // this immutable Plan. It never resumes the old lease or browser. Target
        // eligibility and all writes still belong to the new execution fence.
        const prior = (await client.query(`SELECT command_id FROM remote_ingestion.whole_channel_inputs
          WHERE task_id=$1 AND generation<$2 AND received_at IS NOT NULL AND input_json IS NOT NULL
          ORDER BY generation DESC LIMIT 1`, [task.task_id, task.generation])).rows[0];
        if (prior) {
          const previous = await wholeChannels.result(client, prior.command_id);
          if (!previous.result.failure && previous.result.items?.every(item => ['captured','already_checkpointed'].includes(item.status))
            && (!input.video || (previous.input.video?.runId === input.video.runId && previous.input.video.cycleKey === input.video.cycleKey))) {
            input.previousEvidence = { command_id:prior.command_id,
              about:input.collectAbout && previous.result.about?.about_observed ? previous.result.about : null,
              scan:input.video ? previous.result.scan : null,
              items:input.video ? previous.result.items.filter(item => item.status === 'captured') : [] };
          }
        }
        if (input.previousEvidence) {
          try { wholeChannelBytes(input); }
          catch(error) { if(error.code!=='WHOLE_CHANNEL_TOO_LARGE')throw error; delete input.previousEvidence; }
        }
        const commandId = await wholeChannels.prepare(client, task, input);
        return { commandId, input };
      }, { repeatableRead: true });
      let renewedAt = Date.now();
      onProgress('collecting','whole_channel');
      await awaitCommand(prepared.commandId, signal, async () => {
        if (prepared.input.video && Date.now() - renewedAt > 30000) {
          await withTransaction(client => renewIncrementalVideoDispatchSnapshot(client, prepared.input.video));
          renewedAt = Date.now();
        }
      });
      onProgress('applying','whole_channel');
      whole = await withTransaction(async client => {
        const received = await wholeChannels.result(client, prepared.commandId);
        const result = fromChannelWire(received.result);
        if (received.input.video) {
          await adoptIncrementalVideoDispatchResult(client, {plan, snapshot:received.input.video, result});
          for (const item of result.items ?? []) {
            if (item.status !== 'api_pending') continue;
            const request = item.api_request;
            const expected = JSON.stringify(['incremental', received.input.video.runId, received.input.video.cycleKey, item.phase, item.video_id]);
            if (!received.input.apiPolicy.enabled || !request || request.requestId !== expected
              || request.runId !== received.input.video.runId || request.videoId !== item.video_id || request.consumer !== 'incremental') throw new Error('WHOLE_CHANNEL_API_IDENTITY_CONFLICT');
            await requestVideoApiDetail(action => action(client), { requestId:expected,runId:request.runId,videoId:item.video_id,
              consumer:'incremental',partialDetail:request.partialDetail,requireComments:request.requireComments });
          }
        }
        return { input:received.input, result };
      });
      return whole;
    })();
  };
  const runner = new IncrementalChannelRunner({
    runStore, withTransaction, query,
    agentBacklog: new IncrementalAgentBacklog({ withTransaction }),
    about: async (context) => {
      const snapshot = await context.getChannelSnapshot();
      return withTransaction(async () => {
        const result = await executeIncrementalAbout({ ...context, getChannelSnapshot: async () => snapshot });
        if (result.outcome !== 'failed') {
          // About has no video-style finalized checkpoint. Commit its original
          // writer result and completed domain together, before returning.
          await runStore.markDomain(context.runId, 'about', result.outcome === 'partial' ? 'partial' : 'complete', result);
        }
        return result;
      });
    },
    openChannel: async (channelId, options) => {
      const collected = await ensureWhole(options);
      if (collected) {
        if (collected.result.failure?.stage === 'open_channel') throw collected.result.failure.error;
        return { ...(collected.result.about ?? {}), scanUploads: async () => {
          if (!collected.result.scan) throw collected.result.failure?.error ?? new Error('WHOLE_CHANNEL_SCAN_MISSING');
          return collected.result.scan;
        } };
      }
      const snapshot = await request('open_channel', { channel_id: channelId, options });
      return { ...snapshot, scanUploads: (scanOptions) => request('scan_uploads', { options: scanOptions, channel_options: options }) };
    },
    video: async (context) => {
      if (wholeChannels && !apiReplayRequestId) await context.getChannelSnapshot();
      const executeVideo = () => executeIncrementalYoutubeJsVideo({ ...context,
      // Publish the Observation and completed domain in the same transaction.
      // Feature may consume it immediately after commit; the next fenced
      // transaction must already see our own completion evidence.
      onFinalized: ({result}) => runStore.markDomain(context.runId, 'video',
        result.outcome === 'partial' ? 'partial' : 'complete', incrementalDomainResult(result, 'video')),
      fetchDetail: (videoId, options) => {
        let invocation = 0;
        return fetchIncrementalYoutubeJsVideoDetail(videoId, { ...options, videoApiFallback: fallback,
          fetchYoutubeJs: (id, { signal: detailSignal, ...detailOptions }) => {
            if (whole) {
              const item = whole.result.items.find(item => item.video_id === id && item.phase === options.phase);
              if (item?.status === 'captured') return item.detail;
              if (item?.error) throw item.error;
              throw new Error('WHOLE_CHANNEL_DETAIL_MISSING');
            }
            return request('video_detail', { video_id: id, options: detailOptions },
              `${options.checkpoint.cycle_key}:${options.phase}:${options.checkpoint.attempt_count}:${invocation++}`, detailSignal);
          },
        });
      },
    });
      const completeReceipt = whole && !whole.result.failure && whole.result.items.every(item =>
        ['captured', 'already_checkpointed'].includes(item.status));
      // All network evidence is durable. A single domain transaction reuses the
      // existing writers and holds the business fence until its atomic commit.
      // API/retry handoffs retain the original incremental checkpoint commits.
      return completeReceipt ? withTransaction(executeVideo) : executeVideo();
    },
  });
  try {
    const invoke = () => runWithChannelExecution({
      attempt_id: task.context.execution_attempt_id,
      abort_signal: signal,
    }, async () => {
      const result = await runner.execute({ id: plan.job_id, name: INCREMENTAL_JOB_NAME, queueName: INCREMENTAL_QUEUE, data: plan });
      if (result.terminal) await withTransaction((client) => channelStore.complete(client, task, result));
      return result;
    });
    const options = task.context.execution_options;
    return await (options ? withUploadsCountryExecution({ egressCountry: options.egress_country,
      recheck: options.uploads_country_recheck }, invoke) : invoke());
  } catch (error) {
    // Pending API/country work is owned by the center. Release the remote node;
    // never label this as a successful channel or hold a node waiting for API.
    const handoff = isVideoApiHandoff(error) || error.code === 'UPLOADS_COUNTRY_RECHECK';
    // Stop transport even if the original runner already committed run=failed.
    // This does not authorize another business write or change its outcome.
    await channelStore.store.transaction(async (client) => {
      const current = await channelStore.lock(client, lease, { coordinatorId, requireLive: !apiReplayRequestId });
      if (apiReplayRequestId && current.state !== 'received') throw new Error('API_REPLAY_ALREADY_SETTLED');
      if (apiReplayRequestId && error.code === 'VIDEO_API_NETWORK_REQUIRED') {
        await client.query('UPDATE remote_ingestion.tasks SET coordinator_until=NULL WHERE task_id=$1', [task.task_id]);
        return;
      }
      await client.query(`UPDATE remote_ingestion.tasks SET state=$2,last_error=$3,applied_result=$4,
        coordinator_until=NULL WHERE task_id=$1`, [task.task_id, handoff ? 'received' : 'failed',
        String(error.code || error.name || 'INCREMENTAL_FAILED').slice(0, 300),
        handoff ? { waiting_central: true, request_id: error.requestId ?? null, country: error.country ?? null } : null]);
    }).catch(() => {});
    throw error;
  } finally {
    // Invalidate this Plan's scope without affecting concurrent Plans.
    scope.disable();
  }
}

// Uses the original checkpoint runner under its existing no-network replay
// guard. Only when it requests a network step does the caller re-enter Rota.
export function runRemoteIncrementalApiReplay({ channelStore, taskId, requestId, assertCoordinator = null, ...options }) {
  if (typeof requestId !== 'string' || !requestId) throw new TypeError('API continuation request required');
  return withVideoApiReplay(() => runRemoteIncrementalPlan({ ...options, channelStore,
    lease: { task_id: taskId }, apiReplayRequestId: requestId,
    assertBusinessFence: async (client, task) => {
      if(assertCoordinator)await assertCoordinator(client);
      return assertRemoteIncrementalBusinessFence(client, task, { apiReplayRequestId: requestId });
    } }));
}
