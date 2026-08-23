import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Controller owns bounded Content Enrich dispatch behind a disabled-by-default rollout gate", async () => {
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");
  const controllerDispatch = await readFile(
    new URL("../src/controllerContentEnrichDispatch.js", import.meta.url),
    "utf8",
  );
  const compose = await readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8");
  const environment = await readFile(new URL("../../../.env.example", import.meta.url), "utf8");

  assert.match(source, /new ContentEnrichDispatcher\(/);
  assert.match(source, /CONTENT_ENRICH_DISPATCH_ENABLED/);
  assert.match(source, /CONTENT_ENRICH_QUEUE_HIGH_WATER/);
  assert.match(source, /dispatchContentEnrichForController\(\{[\s\S]*dispatcher:\s*contentEnrichDispatcher/);
  assert.match(source, /new ContentEnrichMonitor\(/);
  assert.match(source, /PostgresContentEnrichObservabilityRepository\(\{[\s\S]*queryFn:\s*query/);
  assert.match(source, /monitor:\s*contentEnrichMonitor/);
  assert.match(source, /queueCounts:\s*stats\[queuesByRole\.contentEnrich\]/);
  assert.match(source, /content_enrich_operational/);
  assert.match(controllerDispatch, /dispatcher\.dispatchAvailable\(\)/);
  assert.match(compose, /CONTENT_ENRICH_DISPATCH_ENABLED:\s*"?\$\{CONTENT_ENRICH_DISPATCH_ENABLED:-false\}"?/);
  assert.match(compose, /CONTENT_ENRICH_QUEUE_HIGH_WATER:/);
  assert.match(compose, /CONTENT_ENRICH_BACKLOG_ALERT_THRESHOLD:/);
  assert.match(compose, /CONTENT_ENRICH_QUEUED_AGE_ALERT_SECONDS:/);
  assert.match(environment, /^CONTENT_ENRICH_BACKLOG_ALERT_THRESHOLD=10000$/m);
  assert.match(environment, /^CONTENT_ENRICH_QUEUED_AGE_ALERT_SECONDS=86400$/m);
});

test("Worker routes only youtube-content-enrich Jobs through the dedicated Executor", async () => {
  const source = await readFile(new URL("../src/worker.js", import.meta.url), "utf8");
  const compose = await readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8");
  const environment = await readFile(new URL("../../../.env.example", import.meta.url), "utf8");

  assert.match(source, /new ContentEnrichExecutor\(/);
  assert.match(source, /maxAttempts:\s*Number\(process\.env\.CONTENT_ENRICH_MAX_ATTEMPTS/);
  assert.match(source, /case queuesByRole\.contentEnrich:[\s\S]*contentEnrichExecutor\.execute\(job\)/);
  assert.match(source, /job\.queueName === queuesByRole\.contentEnrich[\s\S]*content_enrich_checkpoint_persisted/);
  assert.match(compose, /CONTENT_ENRICH_MAX_ATTEMPTS:\s*\$\{CONTENT_ENRICH_MAX_ATTEMPTS:-8\}/);
  assert.match(environment, /^CONTENT_ENRICH_MAX_ATTEMPTS=8$/m);
});
