import { createHash } from "node:crypto";

function normalized(value) {
  return String(value ?? "").trim().toLowerCase();
}

export function proxyAddressHash({ proxyId, protocol, address } = {}) {
  const id = Number(proxyId);
  const target = normalized(address);
  if (!Number.isInteger(id) || id <= 0 || !target) return null;
  return createHash("sha256")
    .update(`${id}|${normalized(protocol) || "http"}|${target}`)
    .digest("hex");
}

export function proxyAssignmentKey(identity = {}) {
  const slotName = String(identity?.slot_name ?? identity?.slotName ?? "").trim();
  const leaseId = String(identity?.lease_id ?? identity?.leaseId ?? "").trim();
  const routeGeneration = Number(identity?.route_generation ?? identity?.routeGeneration);
  const policyId = String(identity?.identity_policy_id ?? identity?.identityPolicyId ?? "").trim();
  const networkIdentityKey = String(
    identity?.network_identity_key ?? identity?.networkIdentityKey ?? "",
  ).trim();
  const profileEpoch = Number(identity?.profile_epoch ?? identity?.profileEpoch);
  if (slotName && leaseId && Number.isSafeInteger(routeGeneration) && routeGeneration > 0
      && policyId && networkIdentityKey && Number.isSafeInteger(profileEpoch) && profileEpoch >= 0) {
    return [
      "v2",
      slotName,
      leaseId,
      routeGeneration,
      policyId,
      networkIdentityKey,
      profileEpoch,
    ].join(":");
  }
  const proxyId = Number(identity?.proxy_id ?? identity?.proxyId);
  const addressHash = String(
    identity?.proxy_address_hash ?? identity?.proxyAddressHash ?? "",
  ).trim();
  if (!Number.isInteger(proxyId) || proxyId <= 0 || !addressHash) return null;
  return `${proxyId}:${addressHash}`;
}

export function sameProxyAssignment(left, right) {
  const leftKey = proxyAssignmentKey(left);
  return Boolean(leftKey && leftKey === proxyAssignmentKey(right));
}
