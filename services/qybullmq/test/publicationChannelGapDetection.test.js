import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { publicationGapRepairDomains } from "../src/publicationChannelOnboarding.js";

test("only Channel and Video readiness failures request a crawler repair", () => {
  assert.deepEqual(publicationGapRepairDomains({
    domains: [
      { domain: "channel", readiness_status: "not_ready" },
      { domain: "video", readiness_status: "ready" },
      { domain: "agent", readiness_status: "not_ready" },
    ],
  }), ["channel"]);
  assert.deepEqual(publicationGapRepairDomains({
    domains: [
      { domain: "channel", readiness_status: "not_ready" },
      { domain: "video", readiness_status: "not_ready" },
    ],
  }), ["channel", "video"]);
});

test("automatic onboarding records a source gap once and stops rescanning it", async () => {
  const source = await readFile(new URL("../src/publicationChannelOnboarding.js", import.meta.url), "utf8");
  assert.match(source, /recordPublicationGapRepairRequired/);
  assert.match(source, /publication_gap_repair/);
  assert.match(source, /quality_policy_refresh/);
  assert.match(source, /publication_gap_repair,status/);
});
