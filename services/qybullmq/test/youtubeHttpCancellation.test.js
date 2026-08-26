import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

function runScenario(scenario) {
  const loader = new URL("./support/youtubeHttpCancellationLoader.mjs", import.meta.url);
  const harness = new URL("./support/youtubeHttpCancellationHarness.mjs", import.meta.url);
  const directory = mkdtempSync(join(tmpdir(), "qy-youtube-http-cancel-"));
  const outputPath = join(directory, "result.json");
  try {
    const child = spawnSync(process.execPath, [
      "--no-warnings",
      "--experimental-loader",
      loader.pathname,
      harness.pathname,
      outputPath,
      scenario,
    ], {
      cwd: new URL("..", import.meta.url),
      encoding: "utf8",
      env: { ...process.env, NODE_TEST_CONTEXT: undefined },
    });
    assert.equal(child.status, 0, child.stderr || child.stdout);
    return JSON.parse(readFileSync(outputPath, "utf8"));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test("youtubeFetch does not record external cancellation as a transport failure", () => {
  const observed = runScenario("external_cancel");

  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.transport_aborted, true);
  assert.equal(observed.metrics.request_count, 0);
  assert.equal(observed.metrics.failure_count, 0);
  assert.deepEqual(observed.metrics.failure_evidence, []);
});

test("youtubeFetch gives cancellation priority over a concurrently completed response", () => {
  const observed = runScenario("external_cancel_after_response");

  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.transport_aborted, true);
  assert.equal(observed.metrics.request_count, 0);
  assert.equal(observed.metrics.failure_count, 0);
  assert.deepEqual(observed.metrics.failure_evidence, []);
});

test("youtubeFetch still records its internal timeout as a transport failure", () => {
  const observed = runScenario("internal_timeout");

  assert.equal(observed.transport_aborted, true);
  assert.equal(observed.metrics.request_count, 1);
  assert.equal(observed.metrics.failure_count, 1);
  assert.equal(observed.metrics.failure_evidence[0].source, "youtube_fetch_transport");
  assert.match(observed.error_message, /timeout 5ms/);
});
