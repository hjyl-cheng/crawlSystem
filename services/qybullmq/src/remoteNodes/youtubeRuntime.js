import { ChannelExecutionMetrics, runWithChannelExecution } from '../channelExecutionContext.js';
import { runWithProxyIdentity } from '../proxyIdentity.js';
import { ManagedRequestTracker } from '../executionRuntimeSupport.js';
import { withUploadsCountryExecution } from '../youtubeUploadsCountry.js';
import { FingerprintGateway } from '../fingerprintGateway.js';
import { acquireYoutubeJs, releaseYoutubeJs, closeYoutubeJs,
  openYoutubeJsChannel, fetchYoutubeJsVideoDetail } from '../youtubeJs.js';
import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { hash, RemoteProtocolError } from './protocol.js';
import { planFromTask } from './channelPlanContract.js';
import { sessionRequest, validateSession } from './youtubeSessionContract.js';

// YouTubeJS has a process-wide lease: exactly one Worker per process/container.
// The exported adapter uses the same collectors and fingerprint transport as
// local execution. No DB imports, identity generation or direct-network path.
let activeRuntime = false;
export function createRemoteYoutubeRuntime({ client, spool, gateway = new FingerprintGateway(),
  youtube = { acquire: acquireYoutubeJs, release: releaseYoutubeJs, close: closeYoutubeJs,
    openChannel: openYoutubeJsChannel, fetchDetail: fetchYoutubeJsVideoDetail } }) {
  const save = value => spool.save('youtube-session.json', Buffer.from(JSON.stringify(value)));
  const flush = async () => {
    const pending = await spool.read('youtube-session.json');
    if (!pending) return;
    if (!pending.checkpoint) {
      // The old process died while holding an identity. Report the interruption;
      // a new process must not resume that session with reset in-memory state.
      pending.checkpoint = { status: 'aborted', cookies: null, metrics: new ChannelExecutionMetrics().snapshot(),
        active_managed_requests: 0, error_code: 'REMOTE_YOUTUBE_PROCESS_INTERRUPTED' };
      await save(pending);
    }
    const ack = await client.youtubeCheckpoint(pending);
    if (ack.durable !== true || ack.route_id !== pending.request.route_id
      || ack.sha256 !== hash(canonicalIncrementalJson(pending.checkpoint))) throw new RemoteProtocolError('INVALID_YOUTUBE_CHECKPOINT_RECEIPT', 502);
    await spool.remove('youtube-session.json');
  };
  const withRuntime = async (route, invoke) => {
    if (activeRuntime) throw new Error('REMOTE_YOUTUBE_RUNTIME_BUSY');
    activeRuntime = true;
    const tracker = new ManagedRequestTracker(); const metrics = new ChannelExecutionMetrics();
    let bundle; let request; let started = false; let result; let failure; let cookies = null;
    const signal = route.signal;
    try {
      const proxyUrl = new URL(route.proxyUrl);
      if (proxyUrl.protocol !== 'http:' || !['127.0.0.1','[::1]'].includes(proxyUrl.hostname)
        || decodeURIComponent(proxyUrl.username) !== route.slot || !proxyUrl.password) throw new Error('LOCAL_ROTA_PROXY_REQUIRED');
      if (route.lease.worker_slot !== route.slot) throw new Error('WORKER_SLOT_MISMATCH');
      signal.throwIfAborted();
      await spool.init(); await flush();
      request = sessionRequest(route, route.lease, route.slot, route.bootId);
      bundle = validateSession(await client.youtubeSession(request), request);
      signal.throwIfAborted();
      await save({ request }); started = true;
      const group = bundle.profile_group;
      await gateway.prepare({ proxyUrl: route.proxyUrl, profileGroup: group });
      const transport = { fetch: (profile, input, init) => {
        const done = tracker.begin();
        return Promise.resolve().then(() => { signal.throwIfAborted(); return gateway.fetch(profile, input, init); }).finally(done);
      } };
      result = await runWithProxyIdentity({ ...bundle.proxy, proxy_url: route.proxyUrl,
        abort_signal: signal, managed_request_tracker: tracker }, () => runWithChannelExecution({
        attempt_id: bundle.attempt_id, proxy: bundle.proxy, get_proxy_snapshot: () => bundle.proxy,
        profile_group: group, fingerprint_gateway: transport, metrics, abort_signal: signal,
      }, async () => {
        const acquired = await youtube.acquire(planFromTask(route.lease).channel_id,
          { profile: group.clients.youtubejs_chrome, proxyUrl: route.proxyUrl });
        if (!acquired?.enabled) throw new Error('REMOTE_YOUTUBE_ACQUIRE_FAILED');
        signal.throwIfAborted();
        const options = bundle.execution_options;
        const output = await (options ? withUploadsCountryExecution({ egressCountry: options.egress_country,
          recheck: options.uploads_country_recheck }, invoke) : invoke());
        signal.throwIfAborted(); return output;
      }));
    } catch (error) { failure = error; }
    finally {
      const cleanup = async action => { try { await action(); } catch (error) { failure ??= error; } };
      // Stop admitting requests before dropping the relay. Always destroy both
      // session caches, including on acquisition errors and cancellation.
      await cleanup(() => tracker.quiesce());
      if (started) {
        await cleanup(() => youtube.release());
        if (!failure && !signal.aborted) await cleanup(async () => { cookies = await gateway.snapshot(bundle.profile_group.clients.youtubejs_chrome); });
        await cleanup(() => youtube.close());
        await cleanup(() => gateway.close());
        if (signal.aborted) failure = signal.reason;
        const status = signal.aborted ? 'aborted' : failure ? 'failed' : 'success';
        await cleanup(async () => {
          await save({ request, checkpoint: { status, cookies: status === 'success' ? cookies : null,
            metrics: metrics.snapshot(), active_managed_requests: 0,
            error_code: failure ? String(failure.code || failure.name || 'REMOTE_YOUTUBE_FAILED').slice(0, 200) : null } });
          await flush();
        });
      }
      activeRuntime = false;
    }
    if (failure) throw failure;
    return result;
  };
  withRuntime.recover = flush;
  return { withRuntime, youtube: { openChannel: youtube.openChannel, fetchDetail: youtube.fetchDetail } };
}
