import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

function serviceBlock(source, serviceName) {
  const marker = `  ${serviceName}:\n`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${serviceName} service is missing`);
  const remainder = source.slice(start + marker.length);
  const next = remainder.search(/^  [a-z0-9][a-z0-9-]*:\n/m);
  return next < 0 ? remainder : remainder.slice(0, next);
}

test("fresh Migration defaults cannot resolve either legacy Writer database", async () => {
  const [compose, environment, bootstrap] = await Promise.all([
    readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8"),
    readFile(new URL("../../../.env.example", import.meta.url), "utf8"),
    readFile(new URL("../../../scripts/bootstrap.sh", import.meta.url), "utf8"),
  ]);
  assert.match(environment, /^QY_DEPLOYMENT_MODE=fresh-migration$/m);
  assert.match(environment, /^QY_FRESH_CRAWLER_NETWORK=qy-newcrawler-crawler-runtime$/m);
  assert.match(environment, /^QY_FRESH_BUSINESS_NETWORK=qy-newcrawler-business-database$/m);
  assert.match(environment, /^CRAWLER_DB_NAME=newcrawler_crawler$/m);
  assert.match(environment, /^BUSINESS_DB_NAME=newcrawler_business$/m);
  assert.match(environment, /^MINIO_BUCKET=newcrawler-raw-20260824-v1$/m);
  assert.match(compose, /FORBIDDEN_CRAWLER_DATABASE: bullmq_crawler_migration/);
  assert.match(compose, /FORBIDDEN_BUSINESS_DATABASE: yewu_business/);
  assert.match(bootstrap, /newcrawler_crawler/);
  assert.match(bootstrap, /newcrawler_business/);
  assert.match(
    bootstrap,
    /postgresql:\/\/\$\{migration_db_user\}:\$\{migration_db_password\}@\$\{migration_db_host\}:\$\{migration_db_port\}\/\$\{migration_db_name\}"/,
  );
  assert.doesNotMatch(bootstrap, /default_transaction_read_only/);
});

test("fresh Migration overlay gives Source access only to API and Dashboard", async () => {
  const overlay = await readFile(
    new URL("../../../deploy/compose.fresh-migration.yml", import.meta.url),
    "utf8",
  );
  for (const service of ["qybullmq-api", "dashboard"]) {
    const block = serviceBlock(overlay, service);
    assert.match(block, /migration_source/);
    assert.match(block, /MIGRATION_DATABASE_URL_FILE: \/run\/secrets\/migration_database_url/);
    assert.match(block, /CONTROLLED_MIGRATION_ONLY: "true"/);
  }
  for (const service of [
    "controller",
    "worker-channel",
    "worker-data-api",
    "worker-agent",
    "worker-finalize",
    "publication-publisher",
    "business-publication-ingress",
    "business-publication-reconciler",
    "business-publication-projector",
  ]) {
    assert.doesNotMatch(serviceBlock(overlay, service), /migration_source/);
  }
  assert.match(overlay, /migration_source:\n    external: true/);
});

test("fresh Migration overlay isolates state, reuses only Rota, and disables automatic work", async () => {
  const [compose, overlay] = await Promise.all([
    readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8"),
    readFile(new URL("../../../deploy/compose.fresh-migration.yml", import.meta.url), "utf8"),
  ]);
  for (const service of [
    "worker-incremental",
    "worker-discover",
    "worker-query-quality",
    "feature-scheduler-daily",
    "feature-dispatch",
  ]) {
    assert.match(serviceBlock(overlay, service), /profiles: \[automatic-work-disabled-during-canary\]/);
  }
  for (const service of ["rota-db", "rota-core", "rota-dashboard"]) {
    assert.match(serviceBlock(overlay, service), /profiles: \[bundled-rota-disabled\]/);
  }
  assert.match(overlay, /qy_rota:\n    external: true/);
  assert.match(compose, /name: \$\{CRAWLER_POSTGRES_VOLUME_NAME:-qy-newcrawler-crawler-postgres-20260824-v1\}/);
  assert.match(compose, /name: \$\{BUSINESS_POSTGRES_VOLUME_NAME:-qy-newcrawler-business-postgres-20260824-v1\}/);
  assert.match(compose, /name: \$\{CRAWLER_REDIS_VOLUME_NAME:-qy-newcrawler-redis-20260824-v1\}/);
  assert.match(compose, /name: \$\{CRAWLER_MINIO_VOLUME_NAME:-qy-newcrawler-minio-20260824-v1\}/);
  assert.doesNotMatch(compose, /name: qy-newcrawler_(?:crawler|business)-postgres-data/);
  assert.match(
    compose,
    /name: \$\{QY_FRESH_CRAWLER_NETWORK:-qy-newcrawler-crawler-runtime\}/,
  );
  assert.match(
    compose,
    /name: \$\{QY_FRESH_BUSINESS_NETWORK:-qy-newcrawler-business-database\}/,
  );
});

test("fresh bootstrap gives each Publication process a dedicated database credential", async () => {
  const [compose, environment, bootstrap] = await Promise.all([
    readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8"),
    readFile(new URL("../../../.env.example", import.meta.url), "utf8"),
    readFile(new URL("../../../scripts/bootstrap.sh", import.meta.url), "utf8"),
  ]);
  const roles = [
    ["business-publication-ingress", "business_publication_ingress", "BUSINESS_PUBLICATION_INGRESS_DB_PASSWORD"],
    ["business-publication-reconciler", "business_publication_reconciler", "BUSINESS_PUBLICATION_RECONCILER_DB_PASSWORD"],
    ["business-publication-projector", "business_publication_projector", "BUSINESS_PUBLICATION_PROJECTOR_DB_PASSWORD"],
  ];
  assert.match(bootstrap, /write_secret crawler_publication_database_url/);
  for (const [service, role, passwordName] of roles) {
    assert.match(environment, new RegExp(`^${passwordName}=CHANGE_ME_${passwordName}$`, "m"));
    assert.match(bootstrap, new RegExp(`write_secret ${role}_database_url`));
    assert.match(bootstrap, new RegExp(`postgresql:\\/\\/${role}:\\$\\{[^}]+\\}@business-postgres:5432\\/\\$\\{business_db_name\\}`));
    const block = serviceBlock(compose, service);
    assert.match(block, new RegExp(`BUSINESS_DATABASE_URL_FILE: /run/secrets/${role}_database_url`));
    assert.match(block, new RegExp(`- ${role}_database_url`));
    assert.doesNotMatch(block, /business_database_url/);
  }
});

test("Publication runtime roles require an explicit dual-database plan and apply", async () => {
  const [compose, bootstrap] = await Promise.all([
    readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8"),
    readFile(new URL("../../../scripts/bootstrap.sh", import.meta.url), "utf8"),
  ]);
  assert.match(bootstrap, /write_secret crawler_admin_database_url/);
  assert.match(bootstrap, /write_secret business_admin_database_url/);
  const administrator = serviceBlock(compose, "publication-runtime-role-admin");
  assert.match(administrator, /profiles: \[manual-publication-runtime-role-admin\]/);
  assert.match(administrator, /managePublicationRuntimeRoles\.mjs/);
  assert.match(administrator, /CRAWLER_ADMIN_DATABASE_URL_FILE: \/run\/secrets\/crawler_admin_database_url/);
  assert.match(administrator, /BUSINESS_ADMIN_DATABASE_URL_FILE: \/run\/secrets\/business_admin_database_url/);
  assert.match(administrator, /PUBLICATION_CRAWLER_PUBLISHER_DATABASE_URL_FILE:/);
  assert.match(administrator, /PUBLICATION_BUSINESS_INGRESS_DATABASE_URL_FILE:/);
  assert.match(administrator, /PUBLICATION_BUSINESS_RECONCILER_DATABASE_URL_FILE:/);
  assert.match(administrator, /PUBLICATION_BUSINESS_PROJECTOR_DATABASE_URL_FILE:/);
  assert.doesNotMatch(administrator, /--apply/);
});

test("fresh Publication routing is bootstrapped only through an explicit dual-database plan and apply", async () => {
  const [compose, environment] = await Promise.all([
    readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8"),
    readFile(new URL("../../../.env.example", import.meta.url), "utf8"),
  ]);
  const administrator = serviceBlock(compose, "fresh-publication-bootstrap");
  assert.match(administrator, /profiles: \[manual-fresh-publication-bootstrap\]/);
  assert.match(administrator, /bootstrapFreshPublication\.mjs/);
  assert.match(administrator, /CRAWLER_ADMIN_DATABASE_URL_FILE: \/run\/secrets\/crawler_admin_database_url/);
  assert.match(administrator, /BUSINESS_ADMIN_DATABASE_URL_FILE: \/run\/secrets\/business_admin_database_url/);
  assert.match(administrator, /EXPECTED_CRAWLER_CHANNEL_COUNT: "0"/);
  assert.match(administrator, /EXPECTED_BUSINESS_CHANNEL_COUNT: "0"/);
  assert.match(administrator, /PUBLICATION_STREAM_ID: \$\{PUBLICATION_STREAM_ID:\?/);
  assert.match(administrator, /PUBLICATION_BOOTSTRAP_PROJECTION_MODE: \$\{PUBLICATION_BOOTSTRAP_PROJECTION_MODE:-online\}/);
  assert.doesNotMatch(administrator, /--apply/);
  assert.match(environment, /^PUBLICATION_STREAM_ID=CHANGE_ME_PUBLICATION_STREAM_ID$/m);
  assert.match(environment, /^PUBLICATION_BOOTSTRAP_PROJECTION_MODE=online$/m);
  assert.match(environment, /^PUBLICATION_WRITER_DEPLOYMENT_REF=sha256:CHANGE_ME_PUBLICATION_WRITER_DEPLOYMENT_DIGEST$/m);
  assert.match(environment, /^PUBLICATION_RUNTIME_DEPLOYMENT_REF=sha256:CHANGE_ME_PUBLICATION_RUNTIME_DEPLOYMENT_DIGEST$/m);
});
