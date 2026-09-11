import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { hash, uuid, generation, RemoteProtocolError } from './protocol.js';
import { remoteExecutionOptions } from './executionContext.js';

export const SESSION_BYTES = 512 * 1024;
const fail = () => { throw new RemoteProtocolError('INVALID_YOUTUBE_SESSION', 400); };
const string = value => typeof value === 'string' && value.length > 0 && value.length <= 4096;

// Only the existing YouTubeJS profile is transported, never another engine or
// database, Rota management, or upstream proxy credentials.
export function youtubeProfileGroup(group, identity) {
  const client = group?.clients?.youtubejs_chrome;
  if (!client || client.engine !== 'youtubejs_chrome' || !string(group.profile_group_id)
    || !Number.isSafeInteger(group.profile_revision) || group.profile_revision < 1
    || !['profile_id', 'impersonate_target', 'user_agent', 'visitor_data', 'language', 'country', 'timezone'].every(key => string(client[key]))
    || !['network_identity_key', 'profile_epoch', 'identity_policy_id', 'identity_policy_version'].every(key => group[key] === identity[key])) fail();
  const profile = Object.fromEntries(['profile_id', 'engine', 'impersonate_target', 'user_agent', 'visitor_data',
    'language', 'country', 'timezone', 'fingerprint_json', 'cookie_state'].map(key => [key, client[key]]));
  if (!Array.isArray(profile.cookie_state?.cookies)) fail();
  const result = { profile_group_id: group.profile_group_id, profile_revision: group.profile_revision,
    ...Object.fromEntries(['network_identity_key', 'profile_epoch', 'identity_policy_id', 'identity_policy_version'].map(key => [key, group[key]])),
    clients: { youtubejs_chrome: profile } };
  if (Buffer.byteLength(JSON.stringify(result)) > SESSION_BYTES - 16384) fail();
  return structuredClone(result);
}

export function sessionRequest(route, lease, slot, bootId) {
  return { task_id: uuid(lease.task_id), generation: generation(lease.generation),
    slot, boot_id: bootId, epoch: route.epoch, route_id: uuid(route.route_id), identity_id: route.identity_id };
}

export function validateSession(bundle, request) {
  if (bundle?.version !== 1 || Object.keys(request).some(key => bundle[key] !== request[key])
    || !string(bundle.attempt_id) || !bundle.proxy || !bundle.identity
    || bundle.identity_id !== hash(canonicalIncrementalJson(bundle.identity))) fail();
  const proxy = bundle.proxy;
  if (!string(proxy.slot_name) || !string(proxy.lease_id) || !Number.isSafeInteger(proxy.route_generation)
    || proxy.route_generation < 1 || ['network_identity_key','profile_epoch','identity_policy_id'].some(key => proxy[key] !== bundle.identity[key])) fail();
  const options = bundle.execution_options ? remoteExecutionOptions(bundle.execution_options) : null;
  if (options && options.egress_country !== (bundle.identity.egress_country || null)) fail();
  return { ...bundle, ...(options ? { execution_options: options } : {}), profile_group: youtubeProfileGroup(bundle.profile_group, bundle.identity) };
}
