import { AsyncLocalStorage } from 'node:async_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { IncrementalChannelRunner } from '../incrementalChannelRunner.js';
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
import { fromChannelWire } from './channelWire.js';
import { assertRemoteIncrementalBusinessFence } from './incrementalBusinessFence.js';

// Reuse the actual clock/Plan runner. Only YouTube requests cross the transport;
// SQL, checkpoints, Agent, publication and API fallback stay on the center.
export async function runRemoteIncrementalPlan({ channelStore, lease, assertBusinessFence,
  createApiFallback = null, pollMs = 100, signal = new AbortController().signal, apiReplayRequestId = null }) {
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
  const scope = new AsyncLocalStorage();
  const withTransaction = async (action) => {
    signal.throwIfAborted();
    if (scope.getStore()) return action(scope.getStore());
    const transaction = apiReplayRequestId ? channelStore.apiReplayTransaction.bind(channelStore) : channelStore.transaction.bind(channelStore);
    return transaction(lease, coordinatorId, assertBusinessFence,
      (client) => scope.run(client, () => action(client)));
  };
  const query = (sql, params) => withTransaction((client) => client.query(sql, params));
  const request = async (operation, input, requestKey = 'once', requestSignal = signal) => {
    assertVideoApiNetworkAllowed();
    const commandId = await withTransaction((client) => channelStore.request(client, task, operation, input, requestKey));
    for (;;) {
      signal.throwIfAborted(); requestSignal?.throwIfAborted();
      const row = (await query(`SELECT state,payload_gzip FROM remote_ingestion.channel_commands
        WHERE command_id=$1`, [commandId])).rows[0];
      if (row.state === 'received') {
        const { value } = await decodeResult(row.payload_gzip);
        if (value.outcome === 'failure') throw fromChannelWire(value.error);
        return fromChannelWire(value.data);
      }
      await delay(pollMs, null, { signal: requestSignal ?? signal });
    }
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
      const snapshot = await request('open_channel', { channel_id: channelId, options });
      return { ...snapshot, scanUploads: (scanOptions) => request('scan_uploads', { options: scanOptions, channel_options: options }) };
    },
    video: (context) => executeIncrementalYoutubeJsVideo({ ...context,
      fetchDetail: (videoId, options) => {
        let invocation = 0;
        return fetchIncrementalYoutubeJsVideoDetail(videoId, { ...options, videoApiFallback: fallback,
          fetchYoutubeJs: (id, { signal: detailSignal, ...detailOptions }) => request('video_detail', {
            video_id: id, options: detailOptions,
          }, `${options.checkpoint.cycle_key}:${options.phase}:${options.checkpoint.attempt_count}:${invocation++}`, detailSignal),
        });
      },
    }),
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
