import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

function serviceBlock(compose, serviceName) {
  const marker = `  ${serviceName}:\n`;
  const start = compose.indexOf(marker);
  assert.ok(start >= 0, `${serviceName} service is missing`);
  const remainder = compose.slice(start + marker.length);
  const next = remainder.search(/^  [a-z][a-z0-9-]*:\n/m);
  return next < 0 ? remainder : remainder.slice(0, next);
}

test("Publication Runtime Compose preserves the database and transport network boundaries", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.yml", import.meta.url),
    "utf8",
  );
  const ingress = serviceBlock(compose, "business-publication-ingress");
  const publisher = serviceBlock(compose, "publication-publisher");
  const reconciler = serviceBlock(compose, "business-publication-reconciler");

  assert.match(ingress, /networks: \[business_database, publication_transport\]/);
  assert.doesNotMatch(ingress, /networks:.*internal/);

  assert.match(publisher, /networks: \[internal, publication_transport\]/);
  assert.doesNotMatch(publisher, /networks:.*business_database/);
  assert.match(
    publisher,
    /BUSINESS_PUBLICATION_INGRESS_URL: http:\/\/business-publication-ingress:8081/,
  );
  assert.match(
    publisher,
    /BUSINESS_PUBLICATION_INGRESS_TRUSTED_HTTP_HOSTNAME: business-publication-ingress/,
  );
  assert.match(
    publisher,
    /PUBLICATION_PUBLISH_BATCH_SIZE: \$\{PUBLICATION_PUBLISH_BATCH_SIZE:-10\}/,
  );

  assert.match(reconciler, /networks: \[business_database\]/);
  assert.doesNotMatch(reconciler, /networks:.*internal|networks:.*publication_transport/);
  assert.match(compose, /publication_transport:\n    driver: bridge\n    internal: true/);
  for (const block of [ingress, publisher, reconciler]) {
    assert.doesNotMatch(block, /^\s+ports:/m);
  }
});

test("Publication Runtime Compose mounts credentials and token only as file-backed secrets", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.yml", import.meta.url),
    "utf8",
  );
  const ingress = serviceBlock(compose, "business-publication-ingress");
  const publisher = serviceBlock(compose, "publication-publisher");
  const reconciler = serviceBlock(compose, "business-publication-reconciler");

  assert.match(publisher, /DATABASE_URL_FILE: \/run\/secrets\/crawler_publication_database_url/);
  assert.match(publisher, /BUSINESS_PUBLICATION_INGRESS_TOKEN_FILE: \/run\/secrets\/business_publication_ingress_token/);
  assert.match(ingress, /BUSINESS_DATABASE_URL_FILE: \/run\/secrets\/business_database_url/);
  assert.match(ingress, /BUSINESS_PUBLICATION_INGRESS_TOKEN_FILE: \/run\/secrets\/business_publication_ingress_token/);
  assert.match(reconciler, /BUSINESS_DATABASE_URL_FILE: \/run\/secrets\/business_database_url/);

  for (const block of [ingress, publisher, reconciler]) {
    assert.doesNotMatch(
      block,
      /^\s+(?:DATABASE_URL|BUSINESS_DATABASE_URL|BUSINESS_PUBLICATION_INGRESS_TOKEN):/m,
    );
    assert.match(block, /read_only: true/);
    assert.match(block, /cap_drop: \[ALL\]/);
    assert.match(block, /security_opt: \[no-new-privileges:true\]/);
  }
});

test("shared QY Publication Runtime is deployed only from the pachongsys source tree", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.qy-publication-runtime.yml", import.meta.url),
    "utf8",
  );
  const launcher = await readFile(
    new URL("../../../scripts/publication-compose.sh", import.meta.url),
    "utf8",
  );

  assert.match(compose, /^name: bullmq-publication-runtime$/m);
  assert.match(
    compose,
    /image: qy-allpachong\/qybullmq:\$\{QYBULLMQ_IMAGE_TAG:\?QYBULLMQ_IMAGE_TAG must pin an immutable pachongsys image\}/,
  );

  const entries = new Map([
    ["business-publication-ingress", "runBusinessPublicationIngress.js"],
    ["publication-publisher", "runPublicationPublisher.js"],
    ["business-publication-reconciler", "runBusinessPublicationReconciler.js"],
    ["business-publication-projector", "runBusinessPublicationProjector.js"],
  ]);
  for (const [service, entrypoint] of entries) {
    const block = serviceBlock(compose, service);
    assert.match(block, new RegExp(`command: \\["node", "src/${entrypoint}"\\]`));
    assert.match(block, /<<: \*publication-runtime/);
  }

  const ingress = serviceBlock(compose, "business-publication-ingress");
  const publisher = serviceBlock(compose, "publication-publisher");
  assert.match(ingress, /BUSINESS_PUBLICATION_TLS_CERT_FILE: \/run\/secrets\/publication_ingress_tls_cert/);
  assert.match(ingress, /networks: \[business_database, publication_transport\]/);
  assert.match(publisher, /BUSINESS_PUBLICATION_INGRESS_URL: https:\/\/business-publication-ingress:8081/);
  assert.match(publisher, /networks: \[crawler_database, publication_transport\]/);
  assert.match(compose, /publication_transport:\n    internal: true/);

  assert.match(launcher, /deploy\/compose\.qy-publication-runtime\.yml/);
  assert.match(launcher, /--project-name bullmq-publication-runtime/);
  assert.match(launcher, /QYBULLMQ_IMAGE_TAG must pin an immutable pachongsys image/);
  assert.doesNotMatch(launcher, /\/tmp\/|dajian|FeatureEngine/);
});

test("Crawler PgBouncer authenticates only the operational, Feature, and Publisher roles", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.yml", import.meta.url),
    "utf8",
  );
  assert.match(compose, /AUTH_TYPE: scram-sha-256/);
  assert.match(
    compose,
    /AUTH_QUERY: .*usename IN \('bullmq','feature_user','publication_publisher'\)/,
  );
  assert.doesNotMatch(compose, /AUTH_QUERY:.*WHERE usename=\$\$1\s*$/m);
});

test("Business PostgreSQL 18 uses its version-aware data directory", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.yml", import.meta.url),
    "utf8",
  );
  const database = serviceBlock(compose, "business-postgres");
  assert.match(database, /image: postgres:18\.4-alpine/);
  assert.match(database, /business-postgres-data:\/var\/lib\/postgresql\n/);
  assert.doesNotMatch(database, /business-postgres-data:\/var\/lib\/postgresql\/data/);
});

test("Redis health checks authenticate with the expanded runtime password", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.yml", import.meta.url),
    "utf8",
  );
  const services = compose.slice(compose.indexOf("\nservices:\n"));
  const redis = serviceBlock(services, "redis");
  assert.match(redis, /REDISCLI_AUTH=\\"\$\$REDIS_PASSWORD\\" redis-cli ping/);
  assert.doesNotMatch(redis, /redis-cli -a '\$\$REDIS_PASSWORD'/);
});

test("runtime secrets are readable by their assigned non-root containers", async () => {
  const bootstrap = await readFile(
    new URL("../../../scripts/bootstrap.sh", import.meta.url),
    "utf8",
  );
  assert.match(bootstrap, /chmod 700 "\$\{SECRETS_DIR\}"/);
  assert.match(bootstrap, /chmod 444 "\$\{SECRETS_DIR\}\/\$\{name\}"/);
  assert.match(bootstrap, /chmod 600 "\$\{SECRETS_DIR\}\/qy_auth_initial_password"/);
});

test("database health gates wait for TCP and initialized schemas", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.yml", import.meta.url),
    "utf8",
  );
  const services = compose.slice(compose.indexOf("\nservices:\n"));
  const crawler = serviceBlock(services, "crawler-postgres");
  const business = serviceBlock(services, "business-postgres");
  for (const database of [crawler, business]) {
    assert.match(database, /psql -h 127\.0\.0\.1/);
    assert.doesNotMatch(database, /pg_isready/);
  }
  assert.match(crawler, /SELECT 1 FROM crawler\.channels LIMIT 0/);
  assert.match(business, /SELECT 1 FROM public\.channels LIMIT 0/);
});
