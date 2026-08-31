import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";


test("Agent result writes never bypass the Publication writer transaction", async () => {
  const source = await readFile(new URL("../src/pipelineV2.js", import.meta.url), "utf8");
  const processor = source.match(
    /export async function processAgentBatchV2[\s\S]*?\n}\n\nfunction contentForProfile/,
  )?.[0];

  assert.ok(processor, "processAgentBatchV2 source was not found");
  assert.doesNotMatch(
    processor,
    /await query\(\s*`(?:INSERT INTO crawler\.agent_profiles|UPDATE crawler\.channels)/,
  );
});

test("a migration recovery Agent leaves Finalize dispatch to the fenced reconciler", async () => {
  const source = await readFile(new URL("../src/pipelineV2.js", import.meta.url), "utf8");
  const processor = source.match(
    /export async function processAgentBatchV2[\s\S]*?\n}\n\nfunction contentForProfile/,
  )?.[0];

  assert.ok(processor, "processAgentBatchV2 source was not found");
  const finalizeCalls = [...processor.matchAll(/await queueFinalize\([\s\S]*?\);/g)]
    .map((match) => match[0]);
  assert.equal(finalizeCalls.length, 2);
  for (const call of finalizeCalls) {
    const preceding = processor.slice(0, processor.indexOf(call)).slice(-120);
    assert.match(preceding, /if \(!recoveryFence\)/);
  }
});
