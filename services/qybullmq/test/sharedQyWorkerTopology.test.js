import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

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
