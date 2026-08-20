import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { publicationRuntimeRoleCommand } from "../scripts/managePublicationRuntimeRoles.mjs";
import {
  PUBLICATION_RUNTIME_ROLE_SPEC_HASH,
  publicationRuntimeRoleConfig,
  publicationRuntimeRoleConfirmation,
  publicationRuntimeRoleSpecifications,
} from "../src/publicationRuntimeRoleAdmin.js";

function environment(overrides = {}) {
  return {
    CRAWLER_ADMIN_DATABASE_URL: "postgres://crawler-admin@crawler/crawler_test",
    BUSINESS_ADMIN_DATABASE_URL: "postgres://business-admin@business/business_test",
    PUBLICATION_CRAWLER_PUBLISHER_DATABASE_URL:
      "postgres://publication_publisher:crawler-password@pgbouncer:6432/crawler_test",
    PUBLICATION_BUSINESS_INGRESS_DATABASE_URL:
      "postgres://business_publication_ingress:ingress-password@postgres:5432/business_test",
    PUBLICATION_BUSINESS_RECONCILER_DATABASE_URL:
      "postgres://business_publication_reconciler:reconciler-password@postgres:5432/business_test",
    EXPECTED_CRAWLER_DATABASE: "crawler_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "17",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "19",
    ...overrides,
  };
}

test("Runtime role config binds fixed identities without exposing passwords in confirmation", () => {
  const config = publicationRuntimeRoleConfig(environment());
  assert.equal(config.credentials.publication_publisher.role, "publication_publisher");
  assert.equal(config.credentials.business_publication_ingress.database, "business_test");
  const confirmation = publicationRuntimeRoleConfirmation(config);
  assert.match(confirmation, /^PROVISION_PUBLICATION_RUNTIME_ROLES:/);
  assert.ok(confirmation.includes(PUBLICATION_RUNTIME_ROLE_SPEC_HASH));
  assert.equal(confirmation.includes("crawler-password"), false);
  assert.equal(confirmation.includes("ingress-password"), false);
  assert.equal(confirmation.includes("reconciler-password"), false);
});

test("Runtime role config rejects wrong roles, databases, and ambiguous database identity", () => {
  assert.throws(() => publicationRuntimeRoleConfig(environment({
    PUBLICATION_CRAWLER_PUBLISHER_DATABASE_URL:
      "postgres://bullmq:password@pgbouncer:6432/crawler_test",
  })), /must use role publication_publisher/);
  assert.throws(() => publicationRuntimeRoleConfig(environment({
    PUBLICATION_BUSINESS_INGRESS_DATABASE_URL:
      "postgres://business_publication_ingress:password@postgres:5432/other_database",
  })), /must target database business_test/);
  assert.throws(() => publicationRuntimeRoleConfig(environment({
    EXPECTED_BUSINESS_DATABASE: "crawler_test",
  })), /must be different/);
});

test("Runtime role specifications grant only the SQL surfaces used by each process", () => {
  const specifications = publicationRuntimeRoleSpecifications();
  const publisher = specifications.source[0];
  assert.deepEqual(publisher.tables, {
    "publication.outbox": ["SELECT", "UPDATE"],
    "publication.revision": ["SELECT"],
  });
  const ingress = specifications.business.find((role) => (
    role.role === "business_publication_ingress"
  ));
  assert.deepEqual(ingress.tables["publication.inbox"], ["SELECT", "INSERT", "UPDATE"]);
  assert.deepEqual(
    ingress.tables["publication.channel_ownership"],
    ["SELECT", "INSERT"],
  );
  assert.deepEqual(
    ingress.denied["publication.channel_ownership"],
    ["UPDATE", "DELETE"],
  );
  assert.equal(Object.hasOwn(ingress.tables, "result.entity_current"), false);
  const reconciler = specifications.business.find((role) => (
    role.role === "business_publication_reconciler"
  ));
  assert.deepEqual(
    reconciler.tables["result.entity_current"],
    ["SELECT", "INSERT", "UPDATE"],
  );
  assert.deepEqual(reconciler.tables["publication.inbox"], ["SELECT"]);
});

test("Runtime role password is bound as a query parameter instead of SQL text", async () => {
  const source = await readFile(
    new URL("../src/publicationRuntimeRoleAdmin.js", import.meta.url),
    "utf8",
  );
  assert.match(source, /set_config\('publication\.runtime_role_password',\$1,true\)/);
  assert.match(source, /current_setting\('publication\.runtime_role_password'\)/);
  assert.doesNotMatch(source, /credentials\[[^\]]+\]\.password[^\n]+ALTER ROLE/);
});

test("Runtime role CLI is plan-only unless --apply is explicit", () => {
  assert.deepEqual(publicationRuntimeRoleCommand([]), { help: false, apply: false });
  assert.deepEqual(publicationRuntimeRoleCommand(["--apply"]), { help: false, apply: true });
  assert.throws(() => publicationRuntimeRoleCommand(["--all"]), /unknown option/);
  assert.throws(() => publicationRuntimeRoleCommand(["--apply", "--apply"]), /only/);
});
