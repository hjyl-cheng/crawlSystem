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
