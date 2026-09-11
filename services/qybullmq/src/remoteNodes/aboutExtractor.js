import { RemoteProtocolError } from './protocol.js';

export const ABOUT_CAPABILITY = 'youtube.about.v1';

// The node supplies a local managed YouTubeJS session. No DB/Redis imports here.
// Real Rota/session lifecycle must be wired before enabling a remote deployment.
export function createRemoteAboutExtractor({ openChannel, withSession }) {
  if (typeof openChannel !== 'function' || typeof withSession !== 'function') {
    throw new TypeError('managed YouTubeJS session and channel extractor required');
  }
  return async (lease, { signal }) => {
    if (lease.capability !== ABOUT_CAPABILITY || !/^UC[A-Za-z0-9_-]{22}$/.test(lease.input?.channel_id)) {
      throw new RemoteProtocolError('UNSUPPORTED_ABOUT_TASK', 400);
    }
    return withSession(lease, { signal }, async () => {
      signal.throwIfAborted();
      const snapshot = await openChannel(lease.input.channel_id, { includeAbout: true });
      signal.throwIfAborted();
      return {
        observed_at: new Date().toISOString(),
        snapshot: {
          metadata: snapshot.metadata,
          about_requested: snapshot.about_requested,
          about_observed: snapshot.about_observed,
          about_error: snapshot.about_error ? {
            name: snapshot.about_error.name || 'Error',
            message: String(snapshot.about_error.message || snapshot.about_error).slice(0, 2000),
          } : null,
          raw: snapshot.raw,
        },
      };
    });
  };
}
