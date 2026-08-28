import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

function runScenario(scenario = "about_only") {
  const loader = new URL("./support/pipelineV2AboutOnlyLoader.mjs", import.meta.url);
  const harness = new URL("./support/pipelineV2AboutOnlyHarness.mjs", import.meta.url);
  const env = { ...process.env };
  delete env.NODE_TEST_CONTEXT;
  const directory = mkdtempSync(join(tmpdir(), "qy-pipeline-about-only-"));
  const outputPath = join(directory, "result.json");
  let child;
  try {
    child = spawnSync(process.execPath, [
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

test("an accepted Promotion with existing content reaches About-only repair before generic resume", () => {
    const observed = runScenario();
    assert.equal(observed.outcome, "resolved");
    assert.equal(observed.value.repaired, true);
    assert.equal(observed.value.resumed, undefined);
    assert.equal(observed.value.scope, "about_only");
    assert.equal(observed.value.candidate_count, 30);
    assert.equal(observed.value.about_outcome, "complete");
    assert.equal(observed.state.finalizeCalls.length, 1);
    assert.equal(observed.state.finalizeCalls[0][1].reason, "publication-gap-about-only");
    assert.equal(
      observed.state.queries.some((sql) => sql.includes("publication-gap-repair:stage-about-only")),
      true,
      "the real Pipeline call path must stage the About-only repair",
    );
    assert.equal(
      observed.state.queries.some((sql) => sql.includes("detail_status='running'")),
      false,
      "About-only repair must not enter the generic Content detail resume path",
    );
    assert.equal(
      observed.state.rawObjects.some((object) => object.objectType === "youtube_content_detail_batch_json"),
      false,
      "About-only repair must not regenerate a Content detail artifact",
    );
});

test("channel crawl does not fall back to legacy metadata after YouTube.js cancellation", () => {
  const observed = runScenario("youtubejs_channel_cancelled");

  assert.equal(observed.outcome, "rejected");
  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.state.legacyHeaderAttempts, 0);
});
