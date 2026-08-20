import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repositoryRoot = fileURLToPath(new URL("../../..", import.meta.url));

test("image builds override stale component tags with the requested revision", async (t) => {
  const fixtureRoot = await mkdtemp(path.join(tmpdir(), "qy-build-images-"));
  t.after(() => rm(fixtureRoot, { force: true, recursive: true }));

  const scriptsDirectory = path.join(fixtureRoot, "scripts");
  const fakeBinDirectory = path.join(fixtureRoot, "fake-bin");
  await mkdir(scriptsDirectory, { recursive: true });
  await mkdir(fakeBinDirectory, { recursive: true });
  await mkdir(path.join(fixtureRoot, "runtime", "test", "env"), {
    recursive: true,
  });

  for (const script of ["build-images.sh", "compose.sh"]) {
    const target = path.join(scriptsDirectory, script);
    await copyFile(path.join(repositoryRoot, "scripts", script), target);
    await chmod(target, 0o755);
  }
  await writeFile(
    path.join(fixtureRoot, "runtime", "test", "env", "runtime.env"),
    "QY_DEPLOYMENT_MODE=bundled\n",
  );

  const fakeDocker = path.join(fakeBinDirectory, "docker");
  await writeFile(
    fakeDocker,
    '#!/usr/bin/env bash\nprintf "%s\\n" "$QYBULLMQ_IMAGE_TAG"\n',
  );
  await chmod(fakeDocker, 0o755);

  execFileSync("git", ["init", "--quiet"], { cwd: fixtureRoot });
  execFileSync("git", ["add", "."], { cwd: fixtureRoot });
  execFileSync(
    "git",
    [
      "-c",
      "user.name=QY Test",
      "-c",
      "user.email=qy-test@example.invalid",
      "commit",
      "--quiet",
      "-m",
      "fixture",
    ],
    { cwd: fixtureRoot },
  );

  const revision = execFileSync(
    "git",
    ["rev-parse", "--short=7", "HEAD"],
    { cwd: fixtureRoot, encoding: "utf8" },
  ).trim();
  const requestedTag = `pachongsys-${revision}-qybullmq`;
  const result = spawnSync(
    path.join(scriptsDirectory, "build-images.sh"),
    ["test", requestedTag, "qybullmq-api"],
    {
      cwd: fixtureRoot,
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${fakeBinDirectory}:${process.env.PATH}`,
        QYBULLMQ_IMAGE_TAG: "pachongsys-stale-qybullmq",
      },
    },
  );

  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.trim(), requestedTag);
});
