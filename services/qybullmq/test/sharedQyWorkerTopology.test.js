import assert from "node:assert/strict";
import { chmod, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import {
  rewriteDatabaseUrl,
  rewriteDatabaseUrlFile,
} from "../../../ops/rewrite-runtime-database-url.mjs";

function serviceBlock(source, service) {
  const marker = `  ${service}:\n`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${service} must be present in the shared QY worker overlay`);
  const remainder = source.slice(start + marker.length);
  const nextService = remainder.search(/^  [a-z0-9][a-z0-9-]*:\n/m);
  return nextService === -1 ? remainder : remainder.slice(0, nextService);
}

test("shared QY worker takeover includes the complete daily Clock runtime", async () => {
  const source = await readFile(new URL(
    "../../../deploy/compose.shared-qy-workers.yml",
    import.meta.url,
  ), "utf8");

  const scheduler = serviceBlock(source, "feature-scheduler-daily");
  assert.match(scheduler, /profiles:\s*!reset\s*\[\]/);
  assert.match(scheduler, /depends_on:\s*!reset\s*\{\}/);
  assert.match(scheduler, /networks:\s*!override\s*\[internal, qy_crawler\]/);

  const dispatch = serviceBlock(source, "feature-dispatch");
  assert.match(dispatch, /profiles:\s*!reset\s*\[\]/);
  assert.match(dispatch, /depends_on:\s*!reset\s*\{\}/);
  assert.match(dispatch, /REDIS_PASSWORD:\s*""/);
  assert.match(dispatch, /networks:\s*!override\s*\[internal, qy_crawler, qy_rota\]/);
});

test("shared adoption rewrites the file-backed Feature database endpoint", async (t) => {
  const directory = await mkdtemp(path.join(tmpdir(), "qy-shared-feature-url-"));
  const target = path.join(directory, "feature_database_url");
  t.after(async () => {
    const { rm } = await import("node:fs/promises");
    await rm(directory, { recursive: true, force: true });
  });
  await writeFile(
    target,
    "postgresql://feature_user:p%40ss@crawler-pgbouncer:6432/bundled_db\n",
    { mode: 0o600 },
  );
  await chmod(target, 0o600);

  await rewriteDatabaseUrlFile(target, {
    host: "bullmq-crawler-migration-pgbouncer",
    port: "6432",
    database: "bullmq_crawler_migration",
    expectedUser: "feature_user",
  });

  const rewritten = new URL((await readFile(target, "utf8")).trim());
  assert.equal(rewritten.hostname, "bullmq-crawler-migration-pgbouncer");
  assert.equal(rewritten.port, "6432");
  assert.equal(rewritten.pathname, "/bullmq_crawler_migration");
  assert.equal(decodeURIComponent(rewritten.password), "p@ss");
  assert.equal((await stat(target)).mode & 0o777, 0o600);

  const adoption = await readFile(new URL(
    "../../../ops/adopt-qy-shared-runtime.sh",
    import.meta.url,
  ), "utf8");
  assert.match(adoption, /rewrite-runtime-database-url\.mjs/);
  assert.match(adoption, /--expected-user feature_user/);
  assert.match(adoption, /QY_SOURCE_FEATURE_CONTAINER/);
  assert.match(adoption, /docker cp/);
  assert.ok(
    adoption.indexOf("docker cp") < adoption.indexOf("rewrite-runtime-database-url.mjs"),
    "the working shared credential must be copied before its endpoint is rewritten",
  );
});

test("database URL rewriting rejects a different database role", () => {
  assert.throws(
    () => rewriteDatabaseUrl(
      "postgresql://bullmq:secret@crawler-pgbouncer:6432/source",
      {
        host: "shared-pgbouncer",
        port: "6432",
        database: "target",
        expectedUser: "feature_user",
      },
    ),
    /user must be feature_user/,
  );
});
