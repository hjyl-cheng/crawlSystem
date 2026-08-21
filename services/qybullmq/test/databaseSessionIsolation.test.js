import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { dirname, extname, join, relative, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const testDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(testDirectory, "../../..");
const scannedRoots = [
  "database",
  "deploy",
  "ops",
  "runtime",
  "scripts",
  "services",
];
const scannedExtensions = new Set([
  ".cjs",
  ".conf",
  ".env",
  ".example",
  ".ini",
  ".js",
  ".mjs",
  ".py",
  ".sh",
  ".sql",
  ".toml",
  ".yaml",
  ".yml",
]);
const skippedDirectories = new Set([
  ".git",
  ".venv",
  "coverage",
  "dist",
  "node_modules",
  "test",
  "tests",
]);

async function productionSourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const nested = await Promise.all(entries.map(async (entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (skippedDirectories.has(entry.name)) return [];
      return productionSourceFiles(path);
    }
    const dockerfile = entry.name === "Dockerfile" || entry.name.startsWith("Dockerfile.");
    return entry.isFile() && (dockerfile || scannedExtensions.has(extname(entry.name))) ? [path] : [];
  }));
  return nested.flat();
}

test("production code never enables session-level default transaction read-only mode", async () => {
  const settingName = ["default", "transaction", "read", "only"].join("_");
  const enablesDefaultReadOnly = new RegExp(
    `${settingName}\\s*(?:=|TO|,)\\s*(?:on|true|yes|1)`,
    "i",
  );
  const enablesSessionReadOnly = /SET\s+SESSION\s+CHARACTERISTICS\s+AS\s+TRANSACTION\s+READ\s+ONLY/i;
  const files = (await Promise.all(
    scannedRoots.map((directory) => productionSourceFiles(join(repositoryRoot, directory))),
  )).flat();
  const findings = [];

  for (const path of files) {
    const source = await readFile(path, "utf8");
    const lines = source.split("\n");
    lines.forEach((line, index) => {
      const unquoted = line.replace(/["'`]/g, "");
      if (enablesDefaultReadOnly.test(unquoted) || enablesSessionReadOnly.test(unquoted)) {
        findings.push(`${relative(repositoryRoot, path)}:${index + 1}`);
      }
    });
  }

  assert.deepEqual(findings, [], [
    "Session-level read-only settings leak across transaction-pooled PgBouncer backends.",
    "Use the PostgreSQL role's default permissions, or a transaction-scoped BEGIN ... READ ONLY.",
  ].join(" "));
});
