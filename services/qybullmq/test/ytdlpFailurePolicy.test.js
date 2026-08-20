import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

const scriptPath = fileURLToPath(new URL("../scripts/ytdlp_session.py", import.meta.url));

function classifyFailures(messages) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.env.YTDLP_PYTHON_BIN || "python3", ["-u", scriptPath, "--classify-failure"], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`yt-dlp failure policy test timed out: ${stderr}`));
    }, 15000);
    child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
    child.on("error", reject);
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code !== 0) reject(new Error(`yt-dlp policy process exited ${code}: ${stderr}`));
      else resolve(JSON.parse(stdout));
    });
    child.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
    child.stdin.end(JSON.stringify(messages));
  });
}

test("yt-dlp only changes client for token/client failures", { timeout: 20000 }, async () => {
  const kinds = await classifyFailures([
    "Tunnel connection failed: 502 Bad Gateway",
    "The read operation timed out",
    "HTTP Error 403: Forbidden",
    "Sign in to confirm you're not a bot",
    "HTTP Error 404: Video not found",
  ]);
  assert.deepEqual(kinds, [
    "proxy_transport",
    "upstream_transient",
    "token_or_client",
    "youtube_challenge",
    "content_terminal",
  ]);
});
