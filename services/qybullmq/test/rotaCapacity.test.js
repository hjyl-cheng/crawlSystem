import assert from "node:assert/strict";
import test from "node:test";
import { loadIdentityPolicyCatalog } from "../src/identityPolicyCatalog.js";
import { normalizeRotaCapacity } from "../src/rotaCapacity.js";

function capacityFixture(catalog = loadIdentityPolicyCatalog()) {
  const policy = (role) => [...catalog.policies.values()].find((value) => value.role === role);
  const role = (name, desired) => {
    const identity = policy(name);
    return {
      desired,
      provisioned: desired,
      eligible: 25,
      assigned: desired,
      ready: desired,
      claimed: 0,
      reserve: 25 - desired,
      identity_policy_id: identity?.id ?? "",
      identity_policy_version: identity?.version ?? 0,
      identity_policy_hash: identity?.hash ?? "",
    };
  };
  return {
    ok: true,
    workload_scope: catalog.workload_scope,
    catalog_version: catalog.catalog_version,
    catalog_digest: catalog.digest,
    active: 25,
    cooldown: 0,
    total: 25,
    reserve: 2,
    running: 23,
    minimum_reserve: 2,
    reserve_below_minimum: false,
    roles: {
      discover: role("discover", 2),
      channel: role("channel", 20),
      query_quality: role("query_quality", 1),
      detail: role("detail", 0),
    },
  };
}

test("Rota Capacity is accepted only with the same scope and Identity Policy catalog", () => {
  const catalog = loadIdentityPolicyCatalog();
  const normalized = normalizeRotaCapacity(capacityFixture(catalog), { catalog });
  assert.equal(normalized.roles.channel.ready, 20);
  assert.equal(normalized.roles.discover.ready, 2);
  assert.equal(normalized.roles.query_quality.ready, 1);
});

test("Rota Capacity fails closed on scope, catalog, and role policy drift", () => {
  const catalog = loadIdentityPolicyCatalog();
  const scopeDrift = capacityFixture(catalog);
  scopeDrift.workload_scope = "other-production";
  assert.throws(
    () => normalizeRotaCapacity(scopeDrift, { catalog }),
    /workload scope mismatch/,
  );

  const catalogDrift = capacityFixture(catalog);
  catalogDrift.catalog_digest = "sha256:stale";
  assert.throws(
    () => normalizeRotaCapacity(catalogDrift, { catalog }),
    /catalog does not match/,
  );

  const policyDrift = capacityFixture(catalog);
  policyDrift.roles.channel.identity_policy_hash = "sha256:stale";
  assert.throws(
    () => normalizeRotaCapacity(policyDrift, { catalog }),
    /Policy mismatch for role channel/,
  );
});
