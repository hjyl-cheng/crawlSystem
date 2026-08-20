import { AsyncLocalStorage } from "node:async_hooks";

const proxyIdentityStorage = new AsyncLocalStorage();

function positiveInteger(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function nonnegativeInteger(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
}

function proxyUserFromUrl(proxyUrl) {
  if (!proxyUrl) return null;
  try {
    return new URL(proxyUrl).username || null;
  } catch {
    return null;
  }
}

function normalizeIdentity(identity = {}) {
  const value = identity ?? {};
  return {
    proxy_id: positiveInteger(value.proxy_id ?? value.proxyId),
    proxy_user: String(value.proxy_user ?? value.proxyUser ?? value.slot_name ?? "").trim() || null,
    proxy_url: String(value.proxy_url ?? value.proxyUrl ?? "").trim() || null,
    dispatcher: value.dispatcher ?? null,
    abort_signal: value.abort_signal ?? value.abortSignal ?? null,
    managed_request_tracker: value.managed_request_tracker ?? value.managedRequestTracker ?? null,
    slot_name: String(value.slot_name ?? value.slotName ?? "").trim() || null,
    lease_id: String(value.lease_id ?? value.leaseId ?? "").trim() || null,
    route_generation: positiveInteger(value.route_generation ?? value.routeGeneration),
    network_identity_key: String(
      value.network_identity_key ?? value.networkIdentityKey ?? "",
    ).trim() || null,
    profile_epoch: nonnegativeInteger(value.profile_epoch ?? value.profileEpoch),
    identity_policy_id: String(
      value.identity_policy_id ?? value.identityPolicyId ?? "",
    ).trim() || null,
  };
}

export function runWithProxyIdentity(identity, callback) {
  return proxyIdentityStorage.run(normalizeIdentity(identity), callback);
}

export function currentProxyIdentity({ proxyUrl = null, proxyUser = null, proxyId = null } = {}) {
  const active = proxyIdentityStorage.getStore() ?? {};
  return {
    proxy_id: positiveInteger(proxyId) ?? active.proxy_id ?? positiveInteger(process.env.YOUTUBE_PROXY_ID),
    proxy_user: String(proxyUser || active.proxy_user || proxyUserFromUrl(proxyUrl || process.env.YOUTUBE_PROXY_URL) || "").trim() || null,
    proxy_url: String(proxyUrl || active.proxy_url || process.env.YOUTUBE_PROXY_URL || "").trim() || null,
    dispatcher: active.dispatcher ?? null,
    abort_signal: active.abort_signal ?? null,
    managed_request_tracker: active.managed_request_tracker ?? null,
    slot_name: active.slot_name ?? null,
    lease_id: active.lease_id ?? null,
    route_generation: active.route_generation ?? null,
    network_identity_key: active.network_identity_key ?? null,
    profile_epoch: active.profile_epoch ?? null,
    identity_policy_id: active.identity_policy_id ?? null,
  };
}

export function currentManagedAbortSignal() {
  return proxyIdentityStorage.getStore()?.abort_signal ?? null;
}

export async function runManagedProxyRequest(callback) {
  const tracker = proxyIdentityStorage.getStore()?.managed_request_tracker ?? null;
  const done = tracker?.begin?.();
  try {
    return await callback();
  } finally {
    done?.();
  }
}
