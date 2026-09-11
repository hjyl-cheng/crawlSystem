import { createRemoteYoutubeRuntime } from './youtubeRuntime.js';
import { RemoteChannelNetworkSession } from './channelNetworkSession.js';
import { RemoteChannelPlanExecutor } from './channelPlanExecutor.js';

// One process/slot/spool, one whole-channel Plan at a time. Deployment readiness
// remains a central gate; creating this adapter does not activate a node.
export function createRemoteIncrementalWorker({ client, localRota, slot, spool,
  gateway, youtube, renewMs = 5000, pollMs = 100, timeoutMs = 15 * 60 * 1000 }) {
  const runtime = createRemoteYoutubeRuntime({ client, spool, gateway, youtube });
  const networkSession = new RemoteChannelNetworkSession({ client, localRota, slot, spool,
    withRuntime: runtime.withRuntime, renewMs });
  return new RemoteChannelPlanExecutor({ client, spool, youtube: runtime.youtube, networkSession, pollMs, timeoutMs });
}
