function nonNegativeInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${field} must be a non-negative integer`);
  }
  return parsed;
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${field} is required`);
  return normalized;
}

function policiesByRole(catalog) {
  const result = new Map();
  for (const policy of catalog?.policies?.values?.() ?? []) {
    if (result.has(policy.role)) {
      throw new Error(`multiple Identity Policies are configured for role ${policy.role}`);
    }
    result.set(policy.role, policy);
  }
  return result;
}

export function normalizeRotaCapacity(payload, { catalog } = {}) {
  if (!payload || payload.ok === false) throw new Error("Rota Capacity is unavailable");
  const workloadScope = requiredText(payload.workload_scope, "capacity.workload_scope");
  if (workloadScope !== catalog?.workload_scope) {
    throw new Error(
      `Rota Capacity workload scope mismatch: ${workloadScope} != ${catalog?.workload_scope ?? "missing"}`,
    );
  }
  if (Number(payload.catalog_version) !== Number(catalog.catalog_version)
      || payload.catalog_digest !== catalog.digest) {
    throw new Error("Rota Capacity Identity Policy catalog does not match the Crawler catalog");
  }

  const expectedPolicies = policiesByRole(catalog);
  const roles = {};
  for (const role of ["discover", "channel", "query_quality", "detail"]) {
    const row = payload.roles?.[role];
    if (!row) throw new Error(`Rota Capacity is missing role ${role}`);
    const expected = expectedPolicies.get(role) ?? null;
    if (expected && (
      row.identity_policy_id !== expected.id
      || Number(row.identity_policy_version) !== Number(expected.version)
      || row.identity_policy_hash !== expected.hash
    )) {
      throw new Error(`Rota Capacity Identity Policy mismatch for role ${role}`);
    }
    roles[role] = Object.freeze({
      desired: nonNegativeInteger(row.desired, `capacity.roles.${role}.desired`),
      provisioned: nonNegativeInteger(row.provisioned, `capacity.roles.${role}.provisioned`),
      eligible: nonNegativeInteger(row.eligible, `capacity.roles.${role}.eligible`),
      assigned: nonNegativeInteger(row.assigned, `capacity.roles.${role}.assigned`),
      ready: nonNegativeInteger(row.ready, `capacity.roles.${role}.ready`),
      claimed: nonNegativeInteger(row.claimed, `capacity.roles.${role}.claimed`),
      reserve: nonNegativeInteger(row.reserve, `capacity.roles.${role}.reserve`),
      identity_policy_id: row.identity_policy_id ?? null,
      identity_policy_version: row.identity_policy_version ?? null,
      identity_policy_hash: row.identity_policy_hash ?? null,
    });
  }

  return Object.freeze({
    ok: true,
    workload_scope: workloadScope,
    catalog_version: Number(payload.catalog_version),
    catalog_digest: payload.catalog_digest,
    active: nonNegativeInteger(payload.active, "capacity.active"),
    cooldown: nonNegativeInteger(payload.cooldown, "capacity.cooldown"),
    total: nonNegativeInteger(payload.total, "capacity.total"),
    reserve: nonNegativeInteger(payload.reserve, "capacity.reserve"),
    running: nonNegativeInteger(payload.running, "capacity.running"),
    minimum_reserve: nonNegativeInteger(payload.minimum_reserve, "capacity.minimum_reserve"),
    reserve_below_minimum: payload.reserve_below_minimum === true,
    roles: Object.freeze(roles),
  });
}
