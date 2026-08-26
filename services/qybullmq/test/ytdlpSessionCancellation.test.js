import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

function runScenario(scenario) {
  const loader = new URL("./support/ytdlpSessionCancellationLoader.mjs", import.meta.url);
  const harness = new URL("./support/ytdlpSessionCancellationHarness.mjs", import.meta.url);
  const directory = mkdtempSync(join(tmpdir(), "qy-ytdlp-session-cancel-"));
  const outputPath = join(directory, "result.json");
  try {
    const env = { ...process.env, YTDLP_PERSISTENT_POOL_ENABLED: "true" };
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

test("a pre-cancelled persistent acquire preserves its reason without spawning Python", () => {
  const observed = runScenario("pre_cancelled_acquire");

  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.spawn_count, 0);
  assert.deepEqual(observed.commands, []);
});

for (const phase of ["start", "configure", "acquire_response"]) {
  test(`persistent acquire cancellation during ${phase} kills the original process`, () => {
    const observed = runScenario(`cancel_during_${phase}`);

    assert.equal(observed.reason_preserved, true);
    assert.equal(observed.spawn_count, 1);
    assert.equal(observed.original_pid_exited, true);
    assert.equal(observed.active_channel, null);
  });
}

test("request cancellation kills the daemon without recovery or release resurrection", () => {
  const observed = runScenario("cancel_active_request");

  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.original_pid_exited, true);
  assert.equal(observed.spawn_count_after_abort, 1);
  assert.equal(observed.spawn_count_after_release, 1);
  assert.equal(observed.next_acquire_pid > observed.original_pid, true);
  assert.equal(observed.spawn_count_after_next_acquire, 2);
  assert.equal(observed.commands.filter((command) => command === "video_detail").length, 1);
});

test("request cancellation waits for the old process to exit before settling or respawning", () => {
  const observed = runScenario("cancel_active_request_delayed_close");

  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.original_pid_closed_before_release, false);
  assert.equal(observed.request_settled_before_close, false);
  assert.equal(observed.spawn_count_before_close, 1);
  assert.equal(observed.original_pid_exited, true);
  assert.equal(observed.next_acquire_pid > observed.original_pid, true);
});

test("a request cancelled before entry terminates its existing lease without release", () => {
  const observed = runScenario("pre_cancelled_active_request");

  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.original_pid_exited, true);
  assert.equal(observed.spawn_count_after_release, 1);
  assert.equal(observed.commands.filter((command) => command === "video_detail").length, 0);
  assert.equal(observed.commands.filter((command) => command === "release").length, 0);
});

test("a cancelled channel release terminates an otherwise healthy idle daemon", () => {
  const observed = runScenario("cancelled_release");

  assert.equal(observed.original_pid_exited, true);
  assert.equal(observed.spawn_count, 1);
  assert.equal(observed.commands.filter((command) => command === "release").length, 0);
});

test("cancellation during normal release interrupts the command and preserves its reason", () => {
  const observed = runScenario("cancel_during_release");

  assert.equal(observed.reason_preserved, true);
  assert.equal(observed.released, null);
  assert.equal(observed.original_pid_exited, true);
  assert.equal(observed.release_commands, 1);
  assert.equal(observed.elapsed_ms < 500, true);
});

test("a recovered command timeout remains in channel failure evidence", () => {
  const observed = runScenario("timeout_then_recover");

  assert.equal(observed.result_pid > observed.first_pid, true);
  assert.equal(observed.spawn_count_after_recovery, 2);
  assert.equal(observed.request_count, 2);
  assert.equal(observed.failure_count, 1);
  assert.equal(observed.failure_evidence.length, 1);
  assert.equal(observed.failure_evidence[0].source, "yt_dlp_detail");
  assert.equal(observed.failure_evidence[0].error_code, "YTDLP_SESSION_COMMAND_TIMEOUT");
});

test("normal release returns cookie state and reuses the persistent process", () => {
  const observed = runScenario("normal_reuse");

  assert.deepEqual(observed.cookie_state, {
    cookies: [{ name: "SID", value: "persisted" }],
  });
  assert.equal(observed.first_pid, observed.second_pid);
  assert.equal(observed.spawn_count_before_close, 1);
  assert.equal(observed.commands.filter((command) => command === "release").length, 2);
});

test("successful persistent operations remove every abort listener they register", () => {
  const observed = runScenario("listener_cleanup");

  assert.equal(observed.listener_adds > 0, true);
  assert.equal(observed.listener_removes, observed.listener_adds);
});

test("closing a killed child before its close event does not spawn a replacement", () => {
  const observed = runScenario("close_killed_child");

  assert.equal(observed.spawn_count_before_close, 1);
  assert.equal(observed.spawn_count, 1);
  assert.equal(observed.original_pid_exited, true);
});
