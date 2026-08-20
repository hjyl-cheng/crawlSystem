#!/usr/bin/env node

import { loadIdentityPolicyCatalog } from "../src/identityPolicyCatalog.js";
import { ProxyControlClient } from "../src/proxyControlClient.js";
import { normalizeRotaCapacity } from "../src/rotaCapacity.js";

const catalog = loadIdentityPolicyCatalog();
const expectedScope = String(process.env.ROTA_WORKLOAD_SCOPE_EXPECTED ?? "").trim();
if (!expectedScope || expectedScope !== catalog.workload_scope) {
  throw new Error("ROTA_WORKLOAD_SCOPE_EXPECTED must match the embedded Identity Policy catalog");
}

const client = new ProxyControlClient();
try {
  const capacity = normalizeRotaCapacity(await client.capacity(), { catalog });
  const mismatchedRoles = Object.entries(capacity.roles)
    .filter(([, value]) => value.desired !== value.provisioned)
    .map(([role]) => role);
  process.stdout.write(`${JSON.stringify({
    ok: mismatchedRoles.length === 0,
    capacity,
    resource_converged: mismatchedRoles.length === 0,
    mismatched_roles: mismatchedRoles,
  }, null, 2)}\n`);
  if (mismatchedRoles.length > 0) process.exitCode = 2;
} finally {
  await client.close();
}
