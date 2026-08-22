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

test("shared QY Feature bridge is owned by pachongsys and pins one immutable image", async () => {
  const compose = await readFile(
    new URL("../../../deploy/compose.qy-feature-bridge-runtime.yml", import.meta.url),
    "utf8",
  );
  const launcher = await readFile(
    new URL("../../../scripts/feature-bridge-compose.sh", import.meta.url),
    "utf8",
  );

  assert.match(compose, /^name: qy-feature-bridge-runtime$/m);
  assert.match(
    compose,
    /image: qy-allpachong\/qybullmq:\$\{QYBULLMQ_IMAGE_TAG:\?QYBULLMQ_IMAGE_TAG must pin an immutable pachongsys image\}/,
  );
  assert.match(
    compose,
    /image: qy-allpachong\/feature-engine:\$\{QY_FEATURE_ENGINE_IMAGE_TAG:\?QY_FEATURE_ENGINE_IMAGE_TAG must pin an immutable pachongsys image\}/,
  );

  const ingest = serviceBlock(compose, "feature-ingest");
  const relay = serviceBlock(compose, "feature-relay");
  const publisher = serviceBlock(compose, "crawler-outbox-publisher");
  assert.match(ingest, /command: \["feature-ingest"\]/);
  assert.match(ingest, /EXPECTED_FEATURE_DATABASE_USER: feature_user/);
  assert.match(ingest, /networks: \[feature_private, crawler\]/);
  assert.match(ingest, /feature_runtime_secrets:\/run\/qy-feature-secrets:ro/);
  assert.match(relay, /command: \["node", "src\/runFeatureRecalcRelay\.js"\]/);
  assert.match(relay, /networks: \[feature_private, crawler\]/);
  assert.match(relay, /feature_runtime_secrets:\/run\/qy-feature-secrets:ro/);
  assert.match(relay, /feature-ingest:\n\s+condition: service_healthy/);
  assert.match(publisher, /command: \["node", "src\/runCrawlerOutboxPublisher\.js"\]/);
  assert.match(publisher, /SKIP_SCHEMA_MIGRATION: "true"/);
  assert.match(publisher, /networks: \[crawler\]/);

  for (const block of [relay, publisher]) {
    assert.match(block, /<<: \*feature-bridge-runtime/);
    assert.doesNotMatch(block, /^\s+build:/m);
  }

  assert.match(compose, /name: \$\{QY_SHARED_CRAWLER_NETWORK:-bullmq-crawler\}/);
  assert.match(compose, /name: \$\{QY_SHARED_FEATURE_PRIVATE_NETWORK:-qy-feature-private\}/);
  assert.match(compose, /name: \$\{QY_SHARED_FEATURE_RUNTIME_SECRETS_VOLUME:-qy-feature-runtime-secrets\}/);
  assert.match(launcher, /deploy\/compose\.qy-feature-bridge-runtime\.yml/);
  assert.match(launcher, /--project-name qy-feature-bridge-runtime/);
  assert.match(launcher, /QYBULLMQ_IMAGE_TAG must pin an immutable pachongsys image/);
  assert.match(launcher, /QY_FEATURE_ENGINE_IMAGE_TAG must pin an immutable pachongsys image/);
  assert.doesNotMatch(launcher, /\/tmp\/|dajian|FeatureEngine/);
});
