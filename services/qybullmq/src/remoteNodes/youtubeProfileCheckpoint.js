import { BrowserProfileStore } from '../browserProfileStore.js';
import { assertRemoteIncrementalBusinessFence } from './incrementalBusinessFence.js';
import { uuid, RemoteProtocolError } from './protocol.js';

// Center-only consumer. The original BrowserProfileStore performs cookie
// encryption/writes inside the same transaction as the ownership checks.
// Receiving a node checkpoint alone never invokes this method.
export function createRemoteYoutubeCheckpointConsumer({ sessions, profileSecret }) {
  if (typeof profileSecret !== 'string' || !profileSecret) throw new TypeError('central browser encryption secret required');
  const { routes, store } = sessions;
  return {
    async apply(bindingId, { client: transactionClient = null } = {}) {
      const apply = async client => {
        const binding = (await client.query('SELECT * FROM remote_ingestion.network_bindings WHERE binding_id=$1', [uuid(bindingId)])).rows[0];
        if (!binding) throw new RemoteProtocolError('NETWORK_BINDING_MISSING');
        const node = (await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE', [binding.node_id])).rows[0];
        if (!node || node.state === 'disabled') throw new RemoteProtocolError('UNAUTHORIZED', 401);
        const task = (await client.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1 FOR UPDATE', [binding.task_id])).rows[0];
        if (!task || task.node_id !== binding.node_id || task.worker_slot !== binding.slot || task.generation !== binding.generation) {
          throw new RemoteProtocolError('STALE_LEASE');
        }
        if (task.state !== 'applied' || !task.applied_result?.run_id) return { applied: false, reason: 'execution_not_successful' };
        const already = (await client.query('SELECT profile_applied_at FROM remote_ingestion.youtube_sessions WHERE binding_id=$1', [bindingId])).rows[0];
        if (already?.profile_applied_at) return { applied: true, replay: true };
        // Maintain node -> task -> business records -> local slot lock order.
        await assertRemoteIncrementalBusinessFence(client, task, { allowCompletedRun: true });
        const slot = (await client.query('SELECT binding_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2 FOR UPDATE', [binding.node_id, binding.slot])).rows[0];
        if (slot?.binding_id !== bindingId || binding.state !== 'retired' || binding.release_receipt?.in_flight !== 0) {
          throw new RemoteProtocolError('YOUTUBE_SESSION_NOT_QUIESCED');
        }
        const row = (await client.query('SELECT * FROM remote_ingestion.youtube_sessions WHERE binding_id=$1 FOR UPDATE', [bindingId])).rows[0];
        if (!row?.checkpoint_cipher) return { applied: false, reason: 'checkpoint_pending' };
        if (row.profile_applied_at) return { applied: true, replay: true };
        const checkpoint = routes.decrypt(row.checkpoint_cipher, `youtube-checkpoint:${bindingId}`);
        if (checkpoint.status !== 'success' || task.state !== 'applied') return { applied: false, reason: 'execution_not_successful' };
        const bundle = routes.decrypt(row.session_cipher, `youtube-session:${bindingId}`);
        const profile = bundle.profile_group;
        const attempt = (await client.query('SELECT * FROM crawler.channel_execution_attempts WHERE attempt_id=$1 FOR UPDATE', [task.context.execution_attempt_id])).rows[0];
        if (bundle.attempt_id !== attempt?.attempt_id || attempt.profile_group_id !== profile.profile_group_id
          || Number(attempt.profile_revision) !== profile.profile_revision
          || attempt.youtubejs_profile_id !== profile.clients.youtubejs_chrome.profile_id) throw new RemoteProtocolError('YOUTUBE_SESSION_PROFILE_MISMATCH');
        const current = (await client.query('SELECT status,profile_revision FROM crawler.browser_profile_groups WHERE profile_group_id=$1 FOR UPDATE', [profile.profile_group_id])).rows[0];
        if (current?.status !== 'active' || Number(current.profile_revision) !== profile.profile_revision) throw new RemoteProtocolError('YOUTUBE_SESSION_PROFILE_MISMATCH');
        const profiles = new BrowserProfileStore({ queryFn: client.query.bind(client), transactionFn: action => action(client), secret: profileSecret });
        await profiles.checkpointCookies(profile.profile_group_id, { youtubejs_chrome: checkpoint.cookies });
        await client.query(`UPDATE crawler.channel_execution_attempts SET result_json=COALESCE(result_json,'{}'::jsonb)
          || jsonb_build_object('remote_youtube_session',$2::jsonb),updated_at=clock_timestamp() WHERE attempt_id=$1`,
        [attempt.attempt_id, { binding_id: bindingId, youtube_requests: checkpoint.metrics }]);
        await client.query('UPDATE remote_ingestion.youtube_sessions SET profile_applied_at=clock_timestamp() WHERE binding_id=$1', [bindingId]);
        return { applied: true, replay: false };
      };
      return transactionClient ? apply(transactionClient) : store.transaction(apply);
    },
  };
}
