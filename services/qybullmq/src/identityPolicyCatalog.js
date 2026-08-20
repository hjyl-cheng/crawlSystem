import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

const catalogSource = process.env.ROTA_IDENTITY_POLICY_CATALOG_FILE
  || new URL(
    "../../rota/core/internal/proxycontrol/identity_policy_catalog.generated.json",
    import.meta.url,
  );
const validRoles = new Set(["channel", "discover", "query_quality"]);

function requiredString(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${field} is required`);
  return normalized;
}

function expectedPolicyHash(schemaVersion, policy) {
  const { hash: _hash, ...identity } = policy;
  return `sha256:${createHash("sha256").update(JSON.stringify({
    schema_version: schemaVersion,
    ...identity,
  })).digest("hex")}`;
}

export function loadIdentityPolicyCatalog({ bytes = readFileSync(catalogSource) } = {}) {
  let document;
  try {
    document = JSON.parse(Buffer.from(bytes).toString("utf8"));
  } catch (error) {
    throw new Error("cannot decode the identity policy catalog", { cause: error });
  }
  if (document?.schema_version !== 1 || !Number.isSafeInteger(document?.catalog_version)
      || document.catalog_version <= 0) {
    throw new Error("identity policy catalog header is invalid");
  }
  const workloadScope = requiredString(document.workload_scope, "catalog.workload_scope");
  const policies = new Map();
  for (const value of document.policies ?? []) {
    const id = requiredString(value.id, "policy.id");
    const role = requiredString(value.role, "policy.role").toLowerCase();
    if (!validRoles.has(role)) throw new Error(`identity policy ${id} has unsupported role ${role}`);
    if (policies.has(id)) throw new Error(`duplicate identity policy ${id}`);
    const expectedHash = expectedPolicyHash(document.schema_version, value);
    if (value.hash !== expectedHash) throw new Error(`identity policy ${id} hash is invalid`);
    const policy = Object.freeze({
      ...value,
      id,
      role,
      allowed_proxy_tags: Object.freeze([...(value.allowed_proxy_tags ?? [])]),
    });
    policies.set(id, policy);
  }
  if (policies.size === 0) throw new Error("identity policy catalog is empty");
  return Object.freeze({
    schema_version: document.schema_version,
    catalog_version: document.catalog_version,
    workload_scope: workloadScope,
    digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
    policies,
  });
}

export function resolveWorkerIdentityPolicy({
  role,
  policyId,
  expectedWorkloadScope,
  environment = process.env,
  catalog = loadIdentityPolicyCatalog(),
} = {}) {
  const normalizedRole = requiredString(role, "PROXY_SLOT_ROLE").toLowerCase();
  const normalizedPolicyId = requiredString(policyId, "ROTA_IDENTITY_POLICY_ID");
  const expectedScope = requiredString(expectedWorkloadScope, "ROTA_WORKLOAD_SCOPE_EXPECTED");
  if (catalog.workload_scope !== expectedScope) {
    throw new Error(`identity policy workload scope mismatch: ${catalog.workload_scope} != ${expectedScope}`);
  }
  const policy = catalog.policies.get(normalizedPolicyId);
  if (!policy || policy.role !== normalizedRole) {
    throw new Error(`identity policy ${normalizedPolicyId} is not valid for role ${normalizedRole}`);
  }
  const expectedEnvironment = {
    YOUTUBE_LANGUAGE: policy.youtube_language,
    YOUTUBE_CONTROL_LANGUAGE: policy.youtube_control_language,
    YOUTUBE_COUNTRY: policy.youtube_country,
    BROWSER_PROFILE_TIMEZONE: policy.browser_profile_timezone,
  };
  for (const [name, expected] of Object.entries(expectedEnvironment)) {
    const configured = String(environment[name] ?? "").trim();
    if (configured && configured !== expected) {
      throw new Error(`${name}=${configured} conflicts with identity policy ${policy.id}`);
    }
  }
  return Object.freeze({
    catalog_digest: catalog.digest,
    workload_scope: catalog.workload_scope,
    policy,
    environment: Object.freeze(expectedEnvironment),
  });
}
