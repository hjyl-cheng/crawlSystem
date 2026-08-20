import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  loadIdentityPolicyCatalog,
  resolveWorkerIdentityPolicy,
} from "../src/identityPolicyCatalog.js";

test("worker resolves the canonical BR channel identity policy", () => {
  const catalog = loadIdentityPolicyCatalog();
  const resolved = resolveWorkerIdentityPolicy({
    role: "channel",
    policyId: "qy-br-channel-anonymous-v1",
    expectedWorkloadScope: "qy-production",
    environment: {},
    catalog,
  });

  assert.equal(resolved.policy.hash.startsWith("sha256:"), true);
  assert.equal(resolved.policy.youtube_language, "pt-BR");
  assert.equal(resolved.policy.youtube_control_language, "en");
  assert.equal(resolved.policy.youtube_country, "BR");
  assert.equal(resolved.policy.browser_profile_timezone, "America/Sao_Paulo");
  assert.deepEqual(resolved.policy.allowed_proxy_tags, ["role:channel"]);
  assert.deepEqual(resolved.environment, {
    YOUTUBE_LANGUAGE: "pt-BR",
    YOUTUBE_CONTROL_LANGUAGE: "en",
    YOUTUBE_COUNTRY: "BR",
    BROWSER_PROFILE_TIMEZONE: "America/Sao_Paulo",
  });
});

test("worker fails closed on role, scope, or locale drift", () => {
  const base = {
    role: "channel",
    policyId: "qy-br-channel-anonymous-v1",
    expectedWorkloadScope: "qy-production",
    environment: {},
  };
  assert.throws(() => resolveWorkerIdentityPolicy({ ...base, role: "discover" }), /not valid for role/);
  assert.throws(() => resolveWorkerIdentityPolicy({
    ...base,
    expectedWorkloadScope: "qy-test",
  }), /workload scope mismatch/);
  assert.throws(() => resolveWorkerIdentityPolicy({
    ...base,
    environment: { YOUTUBE_COUNTRY: "US" },
  }), /conflicts with identity policy/);
});

test("worker rejects a modified generated policy", () => {
  const source = JSON.parse(readFile());
  source.policies[0].youtube_country = "US";
  assert.throws(
    () => loadIdentityPolicyCatalog({ bytes: Buffer.from(`${JSON.stringify(source)}\n`) }),
    /hash is invalid/,
  );
});

function readFile() {
  return String(readFileSync(
    new URL(
      "../../rota/core/internal/proxycontrol/identity_policy_catalog.generated.json",
      import.meta.url,
    ),
    "utf8",
  ));
}
