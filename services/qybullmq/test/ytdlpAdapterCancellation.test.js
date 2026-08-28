import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

function runScenario(scenario) {
  const loader = new URL("./support/ytdlpAdapterCancellationLoader.mjs", import.meta.url);
  const harness = new URL("./support/ytdlpAdapterCancellationHarness.mjs", import.meta.url);
  const directory = mkdtempSync(join(tmpdir(), "qy-ytdlp-adapter-cancel-"));
  const outputPath = join(directory, "result.json");
  try {
    const env = { ...process.env };
    delete env.NODE_TEST_CONTEXT;
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
      env,
    });
    assert.equal(child.status, 0, child.stderr || child.stdout);
    return JSON.parse(readFileSync(outputPath, "utf8"));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

for (const adapter of ["uploads", "detail", "metadata"]) {
  test(`${adapter} cancellation after persistent null does not spawn a one-shot process`, () => {
    const observed = runScenario(`${adapter}_null`);

    assert.equal(observed.reason_preserved, true);
    assert.equal(observed.spawn_count, 0);
    assert.equal(observed.metrics.failure_count, 0);
  });
}

for (const adapter of ["uploads", "detail"]) {
  test(`${adapter} cancellation bypasses yt-dlp failure annotation`, () => {
    const observed = runScenario(`${adapter}_throw`);

    assert.equal(observed.reason_preserved, true);
    assert.equal(observed.spawn_count, 0);
    assert.equal(observed.metrics.failure_count, 0);
    assert.deepEqual(observed.metrics.failure_evidence, []);
  });
}

for (const adapter of ["uploads", "detail", "metadata"]) {
  test(`cancelling an active ${adapter} one-shot helper preserves the reason`, () => {
    const observed = runScenario(`${adapter}_active_one_shot`);

    assert.equal(observed.reason_preserved, true);
    assert.equal(observed.spawn_count, 1);
    assert.equal(observed.one_shot_killed, true);
    assert.equal(observed.metrics.failure_count, 0);
  });
}
